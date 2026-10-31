use crate::{
    calibrate,
    classes::{self, Class, Profile},
    farm::{BoxState, ClassState, Load},
    graph::Graph,
    protocol::{
        BoxStatus, ClassStatus, PROTOCOL, Ran, Reply, Status, Switching, Unit, UnitStatus, frame,
    },
    stats::{Origin, Prediction, Sample, Stats},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use compact_str::format_compact;
use log::{debug, error, info, warn};
use netidx::path::Path as NPath;
use netidx_core::atomic_id;
use poolshark::local::LPooled;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::CString,
    fs, io, mem,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::unix::AsyncFd,
    sync::{mpsc, oneshot, watch},
    task,
    time::{self, MissedTickBehavior},
};
use triomphe::Arc;

/// A waiting job's priority doubles every this many seconds, so jobs
/// little work waits on are not starved by a stream of ones a lot does.
const AGING_S: f64 = 10.;
const SAVE_EVERY: Duration = Duration::from_secs(60);
/// While jobs wait, look again this often, since predicted finish times
/// move as time passes.
const RECHECK: Duration = Duration::from_millis(250);
/// A job is placed only after it has waited this long, so the daemon sees
/// a wave of jobs cargo starts together before deciding where each goes.
const GATHER: Duration = Duration::from_millis(20);
/// Only jobs predicted to take this long on the fastest local class are
/// worth sending to another box.
const REMOTE_MIN_US: u64 = 1_000_000;
/// The predicted cost of running a job on another box: connecting,
/// sending inputs it lacks, and fetching outputs.
const REMOTE_OVERHEAD_US: u64 = 200_000;
/// While jobs run here and another box may be watching, count the free
/// jobserver tokens this often: codegen threads take and return them
/// without telling the scheduler.
const SAMPLE: Duration = Duration::from_millis(100);
/// A unit that failed remotely and then built locally this many times is
/// kept local.
const PIN_AFTER: u8 = 2;

atomic_id!(LayoutId);
atomic_id!(JobId);

/// What a job needs to run on another box.
pub struct Want {
    pub key: ArcStr,
    pub target: Option<ArcStr>,
}

pub enum Msg {
    Submit { unit: Unit, build: u32, remote: Option<Want>, reply: oneshot::Sender<Admission> },
    Finished { job: JobId, unit: Unit, ran: Option<Ran> },
    /// Another box offers this one a job for `class`.
    Offer { unit: Unit, class: u32, reply: oneshot::Sender<Answer> },
    /// The box a job was sent to took it.
    Accepted { job: JobId, version: u64 },
    RemoteDone { job: JobId, unit: Unit, outcome: RemoteOutcome },
    /// A unit failed on another box and then built here.
    Mismatch { unit: Unit },
    /// Another box's state changed, or it left.
    Farm { name: ArcStr, state: Option<(NPath, BoxState)> },
    Status { reply: oneshot::Sender<Status> },
    Profile { profile: Profile, recalibrate: bool },
    Calibrated { profile: Profile, result: Result<Vec<Class>> },
    Drain,
}

pub enum Admission {
    Slot(Slot),
    Remote(RemoteSlot),
    Restarting,
}

/// A class's cpus and jobserver, and the framed `Reply::Slot` for them.
pub struct Grant {
    pub frame: Bytes,
    pub cpus: Vec<u32>,
    pub jobserver: ArcStr,
}

/// Held while rustc runs on this box. Dropping it frees the slot.
pub struct Slot {
    pub grant: Arc<Grant>,
    job: JobId,
    unit: Unit,
    ran: Option<Ran>,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Slot {
    pub fn finish(mut self, ran: Ran) {
        self.ran = Some(ran);
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Finished {
            job: self.job,
            unit: self.unit.clone(),
            ran: self.ran.take(),
        });
    }
}

/// This box's answer to an offer: a slot if it took the job, and the
/// version of its published state that first reflects the answer.
pub struct Answer {
    pub slot: Option<Slot>,
    pub version: u64,
}

pub enum RemoteOutcome {
    Done(Ran),
    /// The box had no room, as its state at `version` will show.
    Busy { version: u64 },
    Failed,
}

/// Held while a job runs on another box. Dropping it without an outcome
/// counts as a failure.
pub struct RemoteSlot {
    pub jobs: NPath,
    pub class: u32,
    pub key: ArcStr,
    pub target: Option<ArcStr>,
    /// The crate files below the job, which it must send.
    pub crates: Vec<ArcStr>,
    job: JobId,
    unit: Unit,
    outcome: Option<RemoteOutcome>,
    tx: mpsc::UnboundedSender<Msg>,
}

impl RemoteSlot {
    pub fn unit(&self) -> &Unit {
        &self.unit
    }

    /// The box took the job; its state counts it from `version` on.
    pub fn accepted(&self, version: u64) {
        let _ = self.tx.send(Msg::Accepted { job: self.job, version });
    }

    pub fn finish(mut self, outcome: RemoteOutcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for RemoteSlot {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::RemoteDone {
            job: self.job,
            unit: self.unit.clone(),
            outcome: self.outcome.take().unwrap_or(RemoteOutcome::Failed),
        });
    }
}

/// A make-style jobserver FIFO with one token per CPU of a class. A
/// job's admission takes one token; its codegen threads take more.
struct Fifo {
    fd: AsyncFd<OwnedFd>,
    path: PathBuf,
    tokens: usize,
}

impl Fifo {
    fn create(path: PathBuf, tokens: usize) -> Result<Self> {
        let _ = fs::remove_file(&path);
        let cpath = CString::new(path.as_os_str().as_bytes())?;
        if unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) } != 0 {
            return Err(io::Error::last_os_error())
                .with_context(|| format_compact!("mkfifo {}", path.display()));
        }
        let fd = unsafe {
            libc::open(cpath.as_ptr(), libc::O_RDWR | libc::O_NONBLOCK | libc::O_CLOEXEC)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error())
                .with_context(|| format_compact!("opening {}", path.display()));
        }
        // The OwnedFd keeps the descriptor open for as long as the AsyncFd lives.
        let fd = unsafe { AsyncFd::register(OwnedFd::from_raw_fd(fd)) }.map_err(io::Error::from)?;
        let fifo = Self { fd, path, tokens };
        fifo.give(tokens);
        Ok(fifo)
    }

    /// Take up to `max` tokens without blocking.
    fn take(&self, max: usize) -> usize {
        let mut buf = [0u8; 64];
        let mut got = 0;
        while got < max {
            let want = (max - got).min(buf.len());
            let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), want) };
            if n <= 0 {
                break;
            }
            got += n as usize;
        }
        got
    }

    /// Tokens in the FIFO now.
    fn free(&self) -> u32 {
        let mut n: libc::c_int = 0;
        if unsafe { libc::ioctl(self.fd.as_raw_fd(), libc::FIONREAD, &mut n) } != 0 {
            warn!("counting jobserver tokens: {}", io::Error::last_os_error());
            return 0;
        }
        n as u32
    }

    fn give(&self, mut n: usize) {
        const TOKENS: [u8; 64] = [b'+'; 64];
        while n > 0 {
            let w = unsafe {
                libc::write(self.fd.as_raw_fd(), TOKENS.as_ptr().cast(), n.min(TOKENS.len()))
            };
            if w <= 0 {
                warn!("jobserver write failed: {}", io::Error::last_os_error());
                return;
            }
            n -= w as usize;
        }
    }
}

impl Drop for Fifo {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// One profile's classes and their jobservers. A profile switch makes a
/// new layout for new jobs; the old one lives until its jobs finish.
struct Layout {
    id: LayoutId,
    /// Fastest first.
    classes: Vec<Class>,
    fifos: Vec<Fifo>,
    grants: Vec<Arc<Grant>>,
    running: Vec<u32>,
}

impl Layout {
    fn new(classes: Vec<Class>, fifo_dir: &Path) -> Result<Self> {
        let id = LayoutId::new();
        let mut fifos = Vec::with_capacity(classes.len());
        let mut grants = Vec::with_capacity(classes.len());
        let mut buf = BytesMut::new();
        for (k, class) in classes.iter().enumerate() {
            let path = fifo_dir.join(format_compact!("class-{}-{k}", id.inner()).as_str());
            let jobserver = ArcStr::from(path.to_str().context("jobserver path is not utf-8")?);
            fifos.push(Fifo::create(path, class.cpus.len())?);
            let reply = Reply::Slot {
                cpus: class.cpus.clone(),
                jobserver: jobserver.clone(),
                filter_metadata: false,
            };
            frame(&mut buf, &reply)?;
            let frame = buf.split().freeze();
            grants.push(Arc::new(Grant { frame, cpus: class.cpus.clone(), jobserver }));
        }
        Ok(Self { id, running: vec![0; classes.len()], classes, fifos, grants })
    }

    fn idle(&self) -> bool {
        self.running.iter().all(|r| *r == 0)
    }

    fn release(&mut self, class: usize) {
        self.running[class] -= 1;
        let fifo = &self.fifos[class];
        if self.running[class] == 0 {
            // No job of this class is alive, so every token should be in
            // the FIFO. A rustc killed while holding tokens leaks them.
            let had = fifo.take(usize::MAX) + 1;
            fifo.give(fifo.tokens);
            if had != fifo.tokens {
                info!("class {class} jobserver had {had} of {} tokens, refilled", fifo.tokens);
            }
        } else {
            fifo.give(1);
        }
    }
}

enum Switch {
    No,
    /// The profile has no calibration. Admit nothing until every running
    /// job is done, then calibrate.
    Draining(Profile),
    Calibrating { profile: Profile, next: Option<(Profile, bool)> },
}

pub struct Setup {
    pub state_dir: PathBuf,
    pub fifo_dir: PathBuf,
    pub allowed: Vec<u32>,
    pub mem_limit_kb: u64,
    pub calibration_seconds: u64,
}

/// A job that may run on another box, with the crate files it reads.
struct Portable {
    key: ArcStr,
    target: Option<ArcStr>,
    crates: Vec<ArcStr>,
}

struct Waiting {
    unit: Unit,
    build: u32,
    pred: Prediction,
    rank_mcu: u64,
    portable: Option<Portable>,
    since: Instant,
    reply: oneshot::Sender<Admission>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    Local { layout: LayoutId, class: usize },
    /// `shown_in` is the version of the box's state that first counts the
    /// job, known once the box accepts it.
    Remote { remote: usize, class: usize, shown_in: Option<u64> },
}

struct Running {
    job: JobId,
    /// The build it is part of, by cargo's pid; None for another box's job.
    build: Option<u32>,
    place: Place,
    calib_us: u64,
    mem_kb: u64,
    cost_mcu: u64,
    rank_mcu: u64,
    start: Instant,
}

impl Running {
    /// Predicted µs left. A job past its prediction is expected to run
    /// for at least half again as long as it has.
    fn left_us(&self, now: Instant) -> u64 {
        let elapsed = now.duration_since(self.start).as_micros() as u64;
        time_us(self.cost_mcu, self.calib_us).saturating_sub(elapsed).max(elapsed / 2)
    }

    /// Its rank less the part of its own cost already done.
    fn rank_left_mcu(&self, now: Instant) -> u64 {
        let done = now.duration_since(self.start).as_micros() as u64 * 1000 / self.calib_us;
        self.rank_mcu.saturating_sub(done.min(self.cost_mcu))
    }
}

/// Another box as this one knows it.
struct RemoteBox {
    name: ArcStr,
    jobs: NPath,
    state: Option<BoxState>,
    /// Jobs sent to each class that its state doesn't count yet, as of the
    /// last dispatch.
    unseen: Vec<u32>,
    /// It answered busy, so its state is stale until this version.
    busy_until: Option<u64>,
}

impl RemoteBox {
    fn usable(&self, p: &Portable) -> Option<&BoxState> {
        let state = self.state.as_ref()?;
        let ready = state.protocol == PROTOCOL
            && self.busy_until.is_none_or(|v| state.version >= v)
            && state.toolchains.iter().any(|t| t.builds(&p.key, p.target.as_deref()));
        ready.then_some(state)
    }
}

/// Predicted µs for a job costing `cost_mcu` on a class calibrated at
/// `calib_us`.
fn time_us(cost_mcu: u64, calib_us: u64) -> u64 {
    cost_mcu * calib_us / 1000
}

pub struct Sched {
    setup: Setup,
    profile: Profile,
    layout: Layout,
    /// Layouts from earlier profiles that still have running jobs.
    retired: Vec<Layout>,
    switch: Switch,
    running: Vec<Running>,
    mem_running_kb: u64,
    queue: Vec<Waiting>,
    /// Jobs wait for classes whose FIFOs were empty at the last dispatch.
    empty: Vec<bool>,
    starved: bool,
    /// When the next job too young to place becomes old enough.
    gathering: Option<Instant>,
    remotes: Vec<RemoteBox>,
    /// Units that failed remotely and then built here, and how often.
    mismatches: HashMap<ArcStr, u8>,
    /// An offer was answered, so the next publish must send a new version.
    answered: bool,
    stats: Stats,
    graph: Graph,
    load: watch::Sender<Load>,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Sched {
    /// Start in `profile`. If it was never calibrated, calibrate before
    /// admitting anything. `load` receives this box's load as it changes.
    pub fn new(
        setup: Setup,
        profile: Profile,
        load: watch::Sender<Load>,
        tx: mpsc::UnboundedSender<Msg>,
    ) -> Result<Self> {
        let stats = Stats::load(&setup.state_dir.join("jobs.hdr")).unwrap_or_else(|e| {
            warn!("starting with empty job stats: {e:#}");
            Stats::new()
        });
        let graph = Graph::load(&setup.state_dir.join("graph")).unwrap_or_else(|e| {
            warn!("starting with an empty build graph: {e:#}");
            Graph::new()
        });
        let (classes, switch) = match classes::load(&setup.state_dir, &profile, &setup.allowed) {
            Ok(Some(classes)) => (classes, Switch::No),
            Ok(None) => (classes::fallback(&setup.allowed), Switch::Draining(profile.clone())),
            Err(e) => {
                warn!("{e:#}; calibrating {profile} again");
                (classes::fallback(&setup.allowed), Switch::Draining(profile.clone()))
            }
        };
        let layout = Layout::new(classes, &setup.fifo_dir)?;
        log_classes(&profile, &layout.classes);
        let mut sched = Self {
            setup,
            profile,
            empty: vec![false; layout.classes.len()],
            layout,
            retired: Vec::new(),
            switch,
            running: Vec::new(),
            mem_running_kb: 0,
            queue: Vec::new(),
            starved: false,
            gathering: None,
            remotes: Vec::new(),
            mismatches: HashMap::new(),
            answered: false,
            stats,
            graph,
            load,
            tx,
        };
        sched.publish();
        Ok(sched)
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Msg>) {
        enum Event {
            Msg(Option<Msg>),
            Recheck,
            Save,
        }
        let mut save = time::interval(SAVE_EVERY);
        let mut recheck = time::interval(RECHECK);
        recheck.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut sample = time::interval(SAMPLE);
        sample.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            self.advance();
            let starved = self.starved;
            let gathering = self.gathering;
            let watched = !self.layout.idle() && !self.load.is_closed();
            let event = tokio::select! {
                msg = rx.recv() => Event::Msg(msg),
                _ = std::future::poll_fn(|cx| self.poll_tokens(cx)), if starved => Event::Recheck,
                _ = recheck.tick(), if starved => Event::Recheck,
                _ = sample.tick(), if watched => Event::Recheck,
                _ = time::sleep_until(gathering.unwrap_or_else(Instant::now).into()), if gathering.is_some() => {
                    Event::Recheck
                }
                _ = save.tick() => Event::Save,
            };
            match event {
                Event::Msg(None) => break,
                Event::Msg(Some(msg)) => match self.handle(msg) {
                    Ok(true) => (),
                    Ok(false) => break,
                    Err(e) => {
                        error!("scheduler: {e:#}");
                        break;
                    }
                },
                Event::Recheck => (),
                Event::Save => self.save(),
            }
            self.dispatch();
            self.publish();
        }
        for w in self.queue.drain(..) {
            let _ = w.reply.send(Admission::Restarting);
        }
        self.save();
    }

    /// Ready when a FIFO that was empty gets a token. FIFOs the last
    /// dispatch gave tokens back to are not watched, or its own writes
    /// would wake it.
    fn poll_tokens(&self, cx: &mut TaskContext) -> Poll<()> {
        let mut ready = false;
        for (fifo, _) in self.layout.fifos.iter().zip(&self.empty).filter(|(_, e)| **e) {
            if let Poll::Ready(r) = fifo.fd.poll_read_ready(cx) {
                match r {
                    Ok(mut guard) => guard.clear_ready(),
                    Err(e) => warn!("polling jobserver: {e}"),
                }
                ready = true;
            }
        }
        if ready { Poll::Ready(()) } else { Poll::Pending }
    }

    /// False when the scheduler should stop.
    fn handle(&mut self, msg: Msg) -> Result<bool> {
        match msg {
            Msg::Submit { unit, build, remote, reply } => self.submit(unit, build, remote, reply),
            Msg::Finished { job, unit, ran } => self.finished(job, &unit, ran),
            Msg::Offer { unit, class, reply } => {
                let slot = self.offer(unit, class as usize);
                // The loop publishes once after each message, and the
                // publish after an answer always sends a new version.
                self.answered = true;
                let version = self.load.borrow().version + 1;
                // If the asker is gone the slot comes back through Drop.
                let _ = reply.send(Answer { slot, version });
            }
            Msg::Accepted { job, version } => {
                if let Some(r) = self.running.iter_mut().find(|r| r.job == job)
                    && let Place::Remote { shown_in, .. } = &mut r.place
                {
                    *shown_in = Some(version);
                }
            }
            Msg::RemoteDone { job, unit, outcome } => self.remote_done(job, &unit, outcome),
            Msg::Mismatch { unit } => *self.mismatches.entry(unit.exact).or_default() += 1,
            Msg::Farm { name, state } => self.farm(name, state),
            Msg::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Msg::Profile { profile, recalibrate } => self.switch_to(profile, recalibrate)?,
            Msg::Calibrated { profile, result } => self.calibrated(profile, result)?,
            Msg::Drain => return Ok(false),
        }
        Ok(true)
    }

    fn submit(&mut self, unit: Unit, build: u32, remote: Option<Want>, reply: oneshot::Sender<Admission>) {
        self.graph.record(&unit);
        let rank_mcu = self.graph.rank(&unit.id, &self.stats);
        let pred = self.stats.predict(&unit.exact, &unit.coarse);
        let pinned = self.mismatches.get(&unit.exact).is_some_and(|n| *n >= PIN_AFTER);
        let portable = match remote {
            Some(Want { key, target }) if !pinned => {
                self.graph.inputs(&unit.deps).map(|crates| Portable { key, target, crates })
            }
            _ => None,
        };
        self.queue.push(Waiting { unit, build, pred, rank_mcu, portable, since: Instant::now(), reply });
    }

    fn finished(&mut self, job: JobId, unit: &Unit, ran: Option<Ran>) {
        let Some(r) = self.take_running(job) else { return };
        let Place::Local { layout: id, class } = r.place else {
            warn!("job {job:?} finished locally but ran remotely");
            return;
        };
        let layout = if self.layout.id == id {
            &mut self.layout
        } else if let Some(l) = self.retired.iter_mut().find(|l| l.id == id) {
            l
        } else {
            warn!("job {job:?} finished in unknown layout {id:?}");
            return;
        };
        layout.release(class);
        self.retired.retain(|l| !l.idle());
        if let Some(ran) = ran {
            let origin = if r.build.is_some() { Origin::Here } else { Origin::Served };
            self.record(unit, &ran, r.calib_us, origin);
        }
    }

    fn take_running(&mut self, job: JobId) -> Option<Running> {
        let Some(i) = self.running.iter().position(|r| r.job == job) else {
            warn!("unknown job {job:?} finished");
            return None;
        };
        let r = self.running.swap_remove(i);
        self.mem_running_kb -= r.mem_kb;
        Some(r)
    }

    fn record(&mut self, unit: &Unit, ran: &Ran, calib_us: u64, origin: Origin) {
        debug!("{} used {} ms cpu in {} ms", unit.exact, ran.cpu_us / 1000, ran.wall_us / 1000);
        let sample = Sample {
            cost_mcu: ran.cpu_us * 1000 / calib_us,
            wall_ms: ran.wall_us / 1000,
            mem_kb: ran.maxrss_kb,
            origin,
        };
        self.stats.record(unit, sample);
    }

    fn remote_done(&mut self, job: JobId, unit: &Unit, outcome: RemoteOutcome) {
        let Some(r) = self.take_running(job) else { return };
        let Place::Remote { remote, .. } = r.place else { return };
        match outcome {
            RemoteOutcome::Done(ran) => {
                self.mismatches.remove(&unit.exact);
                self.record(unit, &ran, r.calib_us, Origin::Sent);
            }
            RemoteOutcome::Busy { version } => {
                let rb = &mut self.remotes[remote];
                info!("{} is busy", rb.name);
                rb.busy_until = Some(version);
            }
            RemoteOutcome::Failed => (),
        }
    }

    fn farm(&mut self, name: ArcStr, state: Option<(NPath, BoxState)>) {
        let i = match self.remotes.iter().position(|r| r.name == name) {
            Some(i) => i,
            None => {
                self.remotes.push(RemoteBox {
                    name,
                    jobs: NPath::root(),
                    state: None,
                    unseen: Vec::new(),
                    busy_until: None,
                });
                self.remotes.len() - 1
            }
        };
        let rb = &mut self.remotes[i];
        match state {
            Some((jobs, state)) => {
                let restarted = rb.state.as_ref().is_some_and(|old| state.version < old.version);
                if restarted {
                    rb.busy_until = None;
                }
                rb.unseen.resize(state.classes.len(), 0);
                rb.jobs = jobs;
                rb.state = Some(state);
            }
            None => {
                rb.state = None;
                rb.busy_until = None;
            }
        }
    }

    /// Why this box takes no offers now, whatever its free tokens.
    fn closed(&self) -> Option<&'static str> {
        if !matches!(self.switch, Switch::No) {
            Some("switching profile")
        } else if self.queue.iter().any(|w| !w.reply.is_closed()) {
            Some("local jobs are waiting")
        } else {
            None
        }
    }

    /// Admit a job another box offers, if a token is free in `class` and
    /// no job of this box's own is waiting.
    fn offer(&mut self, unit: Unit, class: usize) -> Option<Slot> {
        let refuse = |why: &str| {
            debug!("refusing {}: {why}", unit.exact);
            None
        };
        if let Some(why) = self.closed() {
            return refuse(why);
        }
        let class = class.min(self.layout.classes.len() - 1);
        let pred = self.stats.predict(&unit.exact, &unit.coarse);
        if !self.running.is_empty() && self.mem_running_kb + pred.mem_kb > self.setup.mem_limit_kb {
            return refuse("no memory");
        }
        if self.layout.fifos[class].take(1) == 0 {
            return refuse("no token");
        }
        let rank_mcu = pred.cost_mcu;
        Some(self.admit(unit, None, pred, rank_mcu, class, Instant::now()))
    }

    fn switch_to(&mut self, to: Profile, recalibrate: bool) -> Result<()> {
        if let Switch::Calibrating { profile, next } = &mut self.switch {
            // A calibration of `to` already under way is as fresh as a new one.
            *next = (*profile != to).then_some((to, recalibrate));
            return Ok(());
        }
        if to == self.profile && !recalibrate {
            self.switch = Switch::No;
            return Ok(());
        }
        if !recalibrate {
            match classes::load(&self.setup.state_dir, &to, &self.setup.allowed) {
                Ok(Some(classes)) => {
                    self.switch = Switch::No;
                    return self.apply(to, classes);
                }
                Ok(None) => (),
                Err(e) => warn!("{e:#}; calibrating {to} again"),
            }
        }
        info!("profile {to} needs calibration, waiting for running jobs to finish");
        self.switch = Switch::Draining(to);
        Ok(())
    }

    /// Start a pending calibration once nothing runs here.
    fn advance(&mut self) {
        let Switch::Draining(to) = &self.switch else { return };
        if self.running.iter().any(|r| matches!(r.place, Place::Local { .. })) {
            return;
        }
        let profile = to.clone();
        info!("calibrating profile {profile}");
        let (tx, cpus, seconds) =
            (self.tx.clone(), self.setup.allowed.clone(), self.setup.calibration_seconds);
        let name = profile.clone();
        task::spawn_blocking(move || {
            let result = calibrate::run(&cpus, seconds);
            let _ = tx.send(Msg::Calibrated { profile: name, result });
        });
        self.switch = Switch::Calibrating { profile, next: None };
    }

    fn calibrated(&mut self, profile: Profile, result: Result<Vec<Class>>) -> Result<()> {
        let next = match mem::replace(&mut self.switch, Switch::No) {
            Switch::Calibrating { next, .. } => next,
            other => {
                self.switch = other;
                None
            }
        };
        let classes = match result {
            Ok(classes) => {
                if let Err(e) = classes::save(&self.setup.state_dir, &profile, &classes) {
                    warn!("saving the calibration of {profile}: {e:#}");
                }
                classes
            }
            Err(e) => {
                error!("calibrating {profile}: {e:#}; using all cpus as one class");
                classes::fallback(&self.setup.allowed)
            }
        };
        self.apply(profile, classes)?;
        match next {
            Some((to, recalibrate)) => self.switch_to(to, recalibrate),
            None => Ok(()),
        }
    }

    fn apply(&mut self, profile: Profile, classes: Vec<Class>) -> Result<()> {
        let old = mem::replace(&mut self.layout, Layout::new(classes, &self.setup.fifo_dir)?);
        if !old.idle() {
            self.retired.push(old);
        }
        self.empty = vec![false; self.layout.classes.len()];
        if let Err(e) = classes::set_current(&self.setup.state_dir, &profile) {
            warn!("recording profile {profile}: {e:#}");
        }
        log_classes(&profile, &self.layout.classes);
        self.profile = profile;
        Ok(())
    }

    /// µs until a slot of local `class` is expected to free, from the jobs
    /// running in it, if any are.
    fn frees_in_us(&self, class: usize, now: Instant) -> Option<u64> {
        let here = Place::Local { layout: self.layout.id, class };
        self.running.iter().filter(|r| r.place == here).map(|r| r.left_us(now)).min()
    }

    fn dispatch(&mut self) {
        self.starved = false;
        self.gathering = None;
        self.queue.retain(|w| !w.reply.is_closed());
        if self.queue.is_empty() || !matches!(self.switch, Switch::No) {
            return;
        }
        let now = Instant::now();
        let young = |w: &Waiting| now.duration_since(w.since) < GATHER;
        self.gathering = self.queue.iter().filter(|w| young(w)).map(|w| w.since + GATHER).min();
        let prio = |w: &Waiting| {
            w.rank_mcu as f64 * (now.duration_since(w.since).as_secs_f64() / AGING_S).exp2()
        };
        self.queue.sort_by(|a, b| prio(b).total_cmp(&prio(a)));
        let idle = self.running.is_empty();
        let mut mem = self.mem_running_kb;
        let mut admissible: LPooled<Vec<usize>> = LPooled::take();
        // Stop at the first job that doesn't fit, so a big job at the front
        // is not starved by smaller ones behind it.
        for (i, w) in self.queue.iter().enumerate() {
            if mem + w.pred.mem_kb > self.setup.mem_limit_kb && !(idle && admissible.is_empty()) {
                break;
            }
            mem += w.pred.mem_kb;
            if !young(w) {
                admissible.push(i);
            }
        }
        if admissible.is_empty() {
            return;
        }
        // The work left in each build: the largest rank among its jobs,
        // counting the ones too young to place.
        let mut horizon: LPooled<HashMap<u32, u64>> = LPooled::take();
        for (build, rank) in self
            .queue
            .iter()
            .map(|w| (w.build, w.rank_mcu))
            .chain(self.running.iter().filter_map(|r| Some((r.build?, r.rank_left_mcu(now)))))
        {
            let h = horizon.entry(build).or_default();
            *h = (*h).max(rank);
        }
        let mut held: LPooled<Vec<usize>> = LPooled::take();
        held.extend(self.layout.fifos.iter().map(|f| f.take(admissible.len())));
        for (empty, held) in self.empty.iter_mut().zip(held.iter()) {
            *empty = *held == 0;
        }
        let mut frees_in: LPooled<Vec<u64>> = LPooled::take();
        frees_in.extend(
            (0..held.len()).map(|k| if held[k] > 0 { 0 } else { self.frees_in_us(k, now).unwrap_or(0) }),
        );
        // The soonest a job this dispatch places in each class finishes.
        let mut placed_us: LPooled<Vec<u64>> = LPooled::take();
        placed_us.resize(held.len(), u64::MAX);
        self.count_unseen();
        let fastest = self.layout.classes[0].calib_us;
        let mut cands: LPooled<Vec<Cand>> = LPooled::take();
        let mut granted: LPooled<Vec<(usize, Place)>> = LPooled::take();
        for &i in admissible.iter() {
            let w = &self.queue[i];
            cands.clear();
            cands.extend(self.layout.classes.iter().enumerate().map(|(k, c)| Cand {
                calib_us: c.calib_us,
                frees_in_us: frees_in[k],
                overhead_us: 0,
                place: Place::Local { layout: self.layout.id, class: k },
            }));
            if let Some(p) = &w.portable
                && (!w.pred.known || time_us(w.pred.cost_mcu, fastest) >= REMOTE_MIN_US)
            {
                for (r, rb) in self.remotes.iter().enumerate() {
                    let Some(state) = rb.usable(p) else { continue };
                    for (k, c) in state.classes.iter().enumerate() {
                        if rb.unseen.get(k).is_some_and(|n| *n < c.free) {
                            cands.push(Cand {
                                calib_us: c.calib_us,
                                frees_in_us: 0,
                                overhead_us: REMOTE_OVERHEAD_US,
                                place: Place::Remote { remote: r, class: k, shown_in: None },
                            });
                        }
                    }
                }
            }
            let local = self.layout.classes.len();
            let pick = if w.pred.known {
                let slack_mcu = horizon[&w.build].saturating_sub(w.rank_mcu);
                choose(&cands, local, w.pred.cost_mcu, slack_mcu, fastest)
            } else {
                anywhere_free(&cands, &held)
            };
            let cand = &cands[pick];
            match cand.place {
                Place::Remote { remote, class, .. } => {
                    self.remotes[remote].unseen[class] += 1;
                    granted.push((i, cand.place));
                }
                Place::Local { class, .. } if held[class] == 0 => {
                    // Jobs after this one see the queue it joins, so they
                    // don't all count on the same next free slot.
                    let c = &self.layout.classes[class];
                    frees_in[class] += time_us(w.pred.cost_mcu, c.calib_us) / c.cpus.len() as u64;
                    self.starved = true;
                }
                Place::Local { class, .. } => {
                    let c = &self.layout.classes[class];
                    placed_us[class] = placed_us[class].min(time_us(w.pred.cost_mcu, c.calib_us));
                    held[class] -= 1;
                    if held[class] == 0 {
                        let placed = placed_us[class];
                        frees_in[class] = self.frees_in_us(class, now).map_or(placed, |t| t.min(placed));
                    }
                    granted.push((i, cand.place));
                }
            }
        }
        for (fifo, n) in self.layout.fifos.iter().zip(held.iter()) {
            fifo.give(*n);
        }
        granted.sort_unstable_by_key(|&(i, _)| std::cmp::Reverse(i));
        for &(i, place) in granted.iter() {
            let w = self.queue.swap_remove(i);
            match place {
                Place::Local { class, .. } => {
                    let slot = self.admit(w.unit, Some(w.build), w.pred, w.rank_mcu, class, now);
                    // If the wrapper is gone the slot comes back through Drop.
                    let _ = w.reply.send(Admission::Slot(slot));
                }
                Place::Remote { remote, class, .. } => self.send_remote(w, remote, class, now),
            }
        }
    }

    /// Count the jobs sent to each box that its published state doesn't
    /// show yet.
    fn count_unseen(&mut self) {
        for rb in &mut self.remotes {
            rb.unseen.fill(0);
        }
        for r in &self.running {
            let Place::Remote { remote, class, shown_in } = r.place else { continue };
            let rb = &mut self.remotes[remote];
            let shown = shown_in.zip(rb.state.as_ref()).is_some_and(|(v, s)| s.version >= v);
            if !shown && let Some(n) = rb.unseen.get_mut(class) {
                *n += 1;
            }
        }
    }

    fn admit(
        &mut self,
        unit: Unit,
        build: Option<u32>,
        pred: Prediction,
        rank_mcu: u64,
        class: usize,
        now: Instant,
    ) -> Slot {
        let job = JobId::new();
        self.layout.running[class] += 1;
        self.mem_running_kb += pred.mem_kb;
        self.running.push(Running {
            job,
            build,
            place: Place::Local { layout: self.layout.id, class },
            calib_us: self.layout.classes[class].calib_us,
            mem_kb: pred.mem_kb,
            cost_mcu: pred.cost_mcu,
            rank_mcu,
            start: now,
        });
        Slot { grant: self.layout.grants[class].clone(), job, unit, ran: None, tx: self.tx.clone() }
    }

    fn send_remote(&mut self, w: Waiting, remote: usize, class: usize, now: Instant) {
        let rb = &self.remotes[remote];
        debug!("sending {} to {} class {class}", w.unit.exact, rb.name);
        let calib_us = rb.state.as_ref().map_or(1_000_000, |s| s.classes[class].calib_us);
        let job = JobId::new();
        self.running.push(Running {
            job,
            build: Some(w.build),
            place: Place::Remote { remote, class, shown_in: None },
            calib_us,
            mem_kb: 0,
            cost_mcu: w.pred.cost_mcu,
            rank_mcu: w.rank_mcu,
            start: now,
        });
        let p = w.portable.expect("remote placement needs a portable job");
        let slot = RemoteSlot {
            jobs: rb.jobs.clone(),
            class: class as u32,
            key: p.key,
            target: p.target,
            crates: p.crates,
            job,
            unit: w.unit,
            outcome: None,
            tx: self.tx.clone(),
        };
        // If the wrapper is gone the slot comes back through Drop.
        let _ = w.reply.send(Admission::Remote(slot));
    }

    /// Tell the farm this box's load as a new version, if it changed or an
    /// offer was answered since the last publish.
    fn publish(&mut self) {
        let answered = mem::take(&mut self.answered);
        let closed = self.closed().is_some();
        let (layout, profile) = (&self.layout, self.profile.name());
        self.load.send_if_modified(|load| {
            let mut changed =
                answered || load.profile != *profile || load.classes.len() != layout.classes.len();
            load.classes.resize(layout.classes.len(), ClassState { calib_us: 0, cpus: 0, free: 0 });
            for ((l, c), fifo) in load.classes.iter_mut().zip(&layout.classes).zip(&layout.fifos) {
                let free = if closed { 0 } else { fifo.free() };
                let now = ClassState { calib_us: c.calib_us, cpus: c.cpus.len() as u32, free };
                changed |= *l != now;
                *l = now;
            }
            if changed {
                load.profile = profile.clone();
                load.version += 1;
            }
            changed
        });
    }

    fn status(&mut self) -> Status {
        let mut ranks: LPooled<HashMap<ArcStr, u64>> = LPooled::take();
        for (key, rank) in self.graph.ranked(&self.stats) {
            let r = ranks.entry(key).or_default();
            *r = (*r).max(rank);
        }
        let mut units: Vec<UnitStatus> = self
            .stats
            .units()
            .map(|(key, info)| {
                let [here, sent, served] = info.runs;
                UnitStatus {
                    key: key.clone(),
                    here,
                    sent,
                    served,
                    cost_mcu: info.cost_mcu,
                    wall_ms: info.wall_ms,
                    mem_kb: info.mem_kb,
                    rank_mcu: ranks.get(key).copied(),
                }
            })
            .collect();
        units.sort_unstable_by(|a, b| b.rank_mcu.cmp(&a.rank_mcu).then(b.cost_mcu.cmp(&a.cost_mcu)));
        let boxes = self
            .remotes
            .iter()
            .enumerate()
            .filter_map(|(i, rb)| {
                let state = rb.state.as_ref()?;
                let there = |r: &&Running| matches!(r.place, Place::Remote { remote, .. } if remote == i);
                Some(BoxStatus {
                    name: rb.name.clone(),
                    profile: state.profile.clone(),
                    classes: state.classes.clone(),
                    sent: self.running.iter().filter(there).count() as u32,
                    compatible: state.protocol == PROTOCOL,
                })
            })
            .collect();
        Status {
            profile: self.profile.name().clone(),
            switching: match &self.switch {
                Switch::No => Switching::No,
                Switch::Draining(p) => Switching::Draining(p.name().clone()),
                Switch::Calibrating { profile, .. } => Switching::Calibrating(profile.name().clone()),
            },
            classes: self
                .layout
                .classes
                .iter()
                .zip(&self.layout.fifos)
                .enumerate()
                .map(|(k, (c, fifo))| {
                    let here = Place::Local { layout: self.layout.id, class: k };
                    let (mut running, mut serving) = (0, 0);
                    for r in self.running.iter().filter(|r| r.place == here) {
                        match r.build {
                            Some(_) => running += 1,
                            None => serving += 1,
                        }
                    }
                    let cpus = c.cpus.len() as u32;
                    ClassStatus { calib_us: c.calib_us, cpus, running, serving, free: fifo.free() }
                })
                .collect(),
            finishing: self.retired.iter().flat_map(|l| l.running.iter()).sum(),
            waiting: self.queue.len() as u32,
            mem_limit_kb: self.setup.mem_limit_kb,
            mem_running_kb: self.mem_running_kb,
            graph_nodes: self.graph.len() as u32,
            units,
            boxes,
        }
    }

    fn save(&mut self) {
        if self.stats.dirty {
            self.stats.rebuild_global();
            self.graph.invalidate();
            if let Err(e) = self.stats.save(&self.setup.state_dir.join("jobs.hdr")) {
                warn!("saving job stats: {e:#}");
            }
        }
        if self.graph.dirty
            && let Err(e) = self.graph.save(&self.setup.state_dir.join("graph"))
        {
            warn!("saving the build graph: {e:#}");
        }
    }
}

fn log_classes(profile: &Profile, classes: &[Class]) {
    for (k, c) in classes.iter().enumerate() {
        info!("{profile} class {k}: {} cpus, calibration {} ms", c.cpus.len(), c.calib_us / 1000);
    }
}

/// Somewhere a job could run.
struct Cand {
    calib_us: u64,
    /// µs until it has a slot.
    frees_in_us: u64,
    /// µs a job spends getting there and back.
    overhead_us: u64,
    place: Place,
}

/// Where a job runs. The first `local` candidates are this box's classes:
/// the job takes the slowest of them on which it still finishes within its
/// slack, so faster ones stay free for jobs more work waits on, or with no
/// slack the one where it finishes first, even if it must wait for it.
/// The rest are other boxes, which are spare capacity: the job goes to one
/// if it finishes there sooner than on its local choice. `ref_calib_us`
/// turns slack into time.
fn choose(cands: &[Cand], local: usize, cost_mcu: u64, slack_mcu: u64, ref_calib_us: u64) -> usize {
    let finish = |c: &Cand| c.frees_in_us + c.overhead_us + time_us(cost_mcu, c.calib_us);
    let here = &cands[..local];
    let first = here.iter().map(finish).min().unwrap_or(0);
    let deadline = first + time_us(slack_mcu, ref_calib_us);
    let pick = here
        .iter()
        .enumerate()
        .filter(|(_, c)| finish(c) <= deadline)
        .max_by(|(i, a), (j, b)| a.calib_us.cmp(&b.calib_us).then(j.cmp(i)))
        .map_or(0, |(i, _)| i);
    let away = cands[local..].iter().enumerate().min_by_key(|(_, c)| finish(c));
    match away {
        Some((j, c)) if finish(c) < finish(&cands[pick]) => local + j,
        _ => pick,
    }
}

/// Where a job with no history runs, since predictions say nothing about
/// it: the fastest local class with a token in `held`, else the fastest
/// other box with one free, else the local class expected to free first.
/// The first `held.len()` candidates are the local classes.
fn anywhere_free(cands: &[Cand], held: &[usize]) -> usize {
    let (here, away) = cands.split_at(held.len());
    if let Some(k) = held.iter().position(|n| *n > 0) {
        return k;
    }
    if let Some((j, _)) = away.iter().enumerate().min_by_key(|(_, c)| c.calib_us) {
        return held.len() + j;
    }
    here.iter().enumerate().min_by_key(|(_, c)| c.frees_in_us).map_or(0, |(k, _)| k)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::toolchain::Toolchain;
    use anyhow::bail;
    use netidx_core::pack::Pack;
    use std::{
        fs::{File, OpenOptions},
        io::{Read, Write},
    };

    fn class(calib_us: u64, cpus: &[u32]) -> Class {
        Class { calib_us, cpus: cpus.to_vec() }
    }

    fn unit(name: &str, deps: &[&str]) -> Unit {
        Unit {
            exact: ArcStr::from(name),
            coarse: ArcStr::from(name),
            id: ArcStr::from(name),
            deps: deps.iter().map(|d| crate::protocol::Dep { id: ArcStr::from(*d), file: None }).collect(),
        }
    }

    fn local(calib_us: u64, frees_in_us: u64) -> Cand {
        Cand { calib_us, frees_in_us, overhead_us: 0, place: Place::Local { layout: LayoutId::new(), class: 0 } }
    }

    #[test]
    fn choose_spends_slack_on_slower_classes() {
        // 1000 mcu takes 1, 2 or 4 ms. All free.
        let free = [local(1000, 0), local(2000, 0), local(4000, 0)];
        assert_eq!(choose(&free, 3, 1000, 0, 1000), 0);
        assert_eq!(choose(&free, 3, 1000, 1000, 1000), 1);
        assert_eq!(choose(&free, 3, 1000, 3000, 1000), 2);
        // The fast class frees in 5 ms: with no slack, start now on the middle one.
        assert_eq!(choose(&[local(1000, 5000), local(2000, 0), local(4000, 0)], 3, 1000, 0, 1000), 1);
        // Plenty of slack: wait 1 ms for the slow class rather than take a free fast one.
        assert_eq!(choose(&[local(1000, 0), local(2000, 0), local(4000, 1000)], 3, 1000, 10_000, 1000), 2);
        let remote = |calib_us| {
            let mut c = local(calib_us, 0);
            c.overhead_us = 200;
            c
        };
        // A remote class no faster than the local one loses to its overhead.
        assert_eq!(choose(&[local(1000, 0), remote(1000)], 1, 1000, 0, 1000), 0);
        // Slack keeps a job on a slow local class, but another box that
        // finishes it sooner wins anyway: it costs this box nothing.
        assert_eq!(choose(&[local(1000, 0), local(4000, 0), remote(1000)], 2, 1000, 10_000, 1000), 2);
    }

    #[test]
    fn anywhere_free_prefers_a_token_in_hand_then_another_box() {
        let cands = [local(1000, 0), local(4000, 0), local(500, 0), local(200, 0)];
        assert_eq!(anywhere_free(&cands, &[0, 1]), 1);
        assert_eq!(anywhere_free(&cands, &[0, 0]), 3, "the faster of the other boxes");
        let busy = [local(1000, 9000), local(4000, 3000)];
        assert_eq!(anywhere_free(&busy, &[0, 0]), 1, "the class that frees first");
    }

    struct Harness {
        tx: mpsc::UnboundedSender<Msg>,
        dir: PathBuf,
        load: watch::Receiver<Load>,
    }

    /// A scheduler in profile `default` with `classes`, job history from
    /// `costs`, and graph edges from `edges`, with its state in `dir`.
    fn sched(
        dir: &Path,
        classes: Vec<Class>,
        mem_limit_kb: u64,
        costs: &[(&str, u64, u64)],
        edges: &[(&str, &[&str])],
    ) -> Result<(Sched, mpsc::UnboundedReceiver<Msg>, watch::Receiver<Load>)> {
        let _ = fs::remove_dir_all(dir);
        fs::create_dir_all(dir)?;
        classes::save(dir, &Profile::DEFAULT, &classes)?;
        let setup = Setup {
            state_dir: dir.to_path_buf(),
            fifo_dir: dir.to_path_buf(),
            allowed: (0..4).collect(),
            mem_limit_kb,
            calibration_seconds: 4,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let (load_tx, load) = watch::channel(Load::default());
        let mut sched = Sched::new(setup, Profile::DEFAULT, load_tx, tx)?;
        for &(name, cost_mcu, mem_kb) in costs {
            for _ in 0..5 {
                let sample = Sample { cost_mcu, wall_ms: 1, mem_kb, origin: Origin::Here };
                sched.stats.record(&unit(name, &[]), sample);
            }
        }
        for &(name, deps) in edges {
            sched.graph.record(&unit(name, deps));
        }
        sched.stats.dirty = false;
        sched.graph.dirty = false;
        Ok((sched, rx, load))
    }

    fn scratch(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rustc-dist-sched-{name}-{}", std::process::id()))
    }

    impl Harness {
        fn start(
            name: &str,
            classes: Vec<Class>,
            mem_limit_kb: u64,
            costs: &[(&str, u64, u64)],
            edges: &[(&str, &[&str])],
        ) -> Result<Self> {
            let dir = scratch(name);
            let (sched, rx, load) = sched(&dir, classes, mem_limit_kb, costs, edges)?;
            let tx = sched.tx.clone();
            tokio::spawn(sched.run(rx));
            Ok(Self { tx, dir, load })
        }

        fn submit_with(&self, name: &str, remote: Option<Want>) -> oneshot::Receiver<Admission> {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Submit { unit: unit(name, &[]), build: 1, remote, reply }).unwrap();
            rx
        }

        fn submit(&self, name: &str) -> oneshot::Receiver<Admission> {
            self.submit_with(name, None)
        }

        fn profile(&self, name: &str) {
            let profile = Profile::new(name).unwrap();
            self.tx.send(Msg::Profile { profile, recalibrate: false }).unwrap();
        }

        async fn status(&self) -> Status {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Status { reply }).unwrap();
            rx.await.unwrap()
        }

        fn offer(&self, name: &str) -> oneshot::Receiver<Answer> {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Offer { unit: unit(name, &[]), class: 0, reply }).unwrap();
            rx
        }

        fn farm(&self, state: (NPath, BoxState)) {
            self.tx.send(Msg::Farm { name: "b".into(), state: Some(state) }).unwrap();
        }

        /// The current layout's jobserver, for a test with one class.
        fn fifo(&self) -> Result<File> {
            let path = fs::read_dir(&self.dir)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .find(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("class-")))
                .context("no jobserver")?;
            Ok(OpenOptions::new().read(true).write(true).open(path)?)
        }

        async fn published_free(&self, free: u32) -> Result<()> {
            let mut load = self.load.clone();
            time::timeout(Duration::from_secs(2), load.wait_for(|l| l.classes[0].free == free)).await??;
            Ok(())
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = self.tx.send(Msg::Drain);
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    /// The first cpu of the class a slot was granted. The tests give each
    /// class a distinct first cpu.
    fn cpu(slot: &Slot) -> u32 {
        let mut body = &slot.grant.frame[4..];
        match Reply::decode(&mut body).unwrap() {
            Reply::Slot { cpus, .. } => cpus[0],
            _ => panic!("not a slot"),
        }
    }

    async fn admitted(rx: oneshot::Receiver<Admission>) -> Result<Admission> {
        Ok(time::timeout(Duration::from_secs(5), rx).await??)
    }

    async fn slot(rx: oneshot::Receiver<Admission>) -> Result<Slot> {
        match admitted(rx).await? {
            Admission::Slot(s) => Ok(s),
            Admission::Remote(_) => bail!("sent remote"),
            Admission::Restarting => bail!("restarting"),
        }
    }

    async fn pending(rx: &mut oneshot::Receiver<Admission>) -> bool {
        time::timeout(Duration::from_millis(300), rx).await.is_err()
    }

    #[tokio::test]
    async fn work_that_gates_a_chain_gets_the_fast_class() -> Result<()> {
        let h = Harness::start(
            "chain",
            vec![class(1000, &[0]), class(3000, &[1])],
            u64::MAX,
            &[("gate", 1000, 1), ("after", 50_000, 1), ("leaf", 1000, 1)],
            &[("after", &["gate"])],
        )?;
        let leaf = h.submit("leaf");
        let gate = h.submit("gate");
        let (leaf, gate) = (slot(leaf).await?, slot(gate).await?);
        assert_eq!(cpu(&gate), 0, "the chain waits on gate");
        assert_eq!(cpu(&leaf), 1, "nothing waits on leaf");
        Ok(())
    }

    #[tokio::test]
    async fn a_small_job_waits_for_a_slow_core_while_bigger_work_runs() -> Result<()> {
        // gate takes 1 s, the work after it 50 s, small 10 or 20 ms.
        let h = Harness::start(
            "reserve",
            vec![class(1000, &[0, 2]), class(2000, &[1])],
            u64::MAX,
            &[("gate", 1_000_000, 1), ("after", 50_000_000, 1), ("small", 10_000, 1)],
            &[("after", &["gate"])],
        )?;
        let gate = slot(h.submit("gate")).await?;
        assert_eq!(cpu(&gate), 0);
        let first = slot(h.submit("small")).await?;
        assert_eq!(cpu(&first), 1);
        // A fast cpu is free, but the build has work coming, so the second
        // small job waits for the slow one.
        let mut second = h.submit("small");
        assert!(pending(&mut second).await);
        drop(first);
        assert_eq!(cpu(&slot(second).await?), 1);
        drop(gate);
        Ok(())
    }

    #[tokio::test]
    async fn jobs_stop_queueing_for_a_slow_class_once_the_queue_eats_their_slack() -> Result<()> {
        // gate's chain leaves each small job about 1 s of slack. A small job
        // takes 0.225 s on the fast class and 0.45 s on the slow one, which
        // has one cpu: running, one queued, and a third would finish too late.
        let h = Harness::start(
            "queue",
            vec![class(1000, &[0, 2, 3]), class(2000, &[1])],
            u64::MAX,
            &[("gate", 100_000, 1), ("after", 1_100_000, 1), ("small", 225_000, 1)],
            &[("after", &["gate"])],
        )?;
        let gate = slot(h.submit("gate")).await?;
        let first = slot(h.submit("small")).await?;
        assert_eq!(cpu(&first), 1);
        let mut queued = h.submit("small");
        let later: Vec<_> = (0..3).map(|_| h.submit("small")).collect();
        assert!(pending(&mut queued).await, "one more fits in the slow queue");
        for s in later {
            assert_eq!(cpu(&slot(s).await?), 0, "the rest go fast");
        }
        drop((gate, first));
        Ok(())
    }

    #[tokio::test]
    async fn a_critical_job_waits_for_a_much_faster_class() -> Result<()> {
        let h = Harness::start(
            "critical",
            vec![class(1000, &[0]), class(100_000, &[1])],
            u64::MAX,
            &[("a", 2_000_000, 1)],
            &[],
        )?;
        let first = slot(h.submit("a")).await?;
        assert_eq!(cpu(&first), 0);
        // The slow class is free, but it would take 200 s against 4.
        let mut second = h.submit("a");
        assert!(pending(&mut second).await);
        drop(first);
        assert_eq!(cpu(&slot(second).await?), 0);
        Ok(())
    }

    fn remote_box(calib_us: u64, cpus: u32, free: u32, version: u64) -> (NPath, BoxState) {
        let toolchains = vec![Toolchain { key: "c h".into(), targets: vec!["h".into()] }];
        let classes = vec![ClassState { calib_us, cpus, free }];
        let state = BoxState { protocol: PROTOCOL, profile: "default".into(), version, toolchains, classes };
        (NPath::from("/farm/b/jobs"), state)
    }

    fn want() -> Option<Want> {
        Some(Want { key: "c h".into(), target: None })
    }

    #[tokio::test]
    async fn a_big_job_goes_to_a_faster_box() -> Result<()> {
        // 5 s here, 1 s there.
        let h = Harness::start("remote", vec![class(1000, &[0])], u64::MAX, &[("big", 5_000_000, 1)], &[])?;
        h.farm(remote_box(200, 4, 4, 1));
        match admitted(h.submit_with("big", want())).await? {
            Admission::Remote(r) => {
                assert_eq!(r.jobs, NPath::from("/farm/b/jobs"));
                assert!(r.crates.is_empty());
                r.finish(RemoteOutcome::Busy { version: 3 });
            }
            _ => bail!("not sent remote"),
        }
        // Busy: what b published is stale until version 3.
        h.farm(remote_box(200, 4, 4, 2));
        assert!(matches!(admitted(h.submit_with("big", want())).await?, Admission::Slot(_)));
        h.farm(remote_box(200, 4, 4, 3));
        assert!(matches!(admitted(h.submit_with("big", want())).await?, Admission::Remote(_)));
        Ok(())
    }

    #[tokio::test]
    async fn remote_needs_a_matching_toolchain_and_a_free_slot() -> Result<()> {
        let h = Harness::start("nomatch", vec![class(1000, &[0, 1])], u64::MAX, &[("big", 5_000_000, 1)], &[])?;
        h.tx.send(Msg::Farm { name: "full".into(), state: Some(remote_box(200, 4, 0, 1)) })?;
        let other = Some(Want { key: "other h".into(), target: None });
        h.farm(remote_box(200, 4, 4, 1));
        assert!(matches!(admitted(h.submit_with("big", other)).await?, Admission::Slot(_)));
        let target = Some(Want { key: "c h".into(), target: Some("wasm32".into()) });
        assert!(matches!(admitted(h.submit_with("big", target)).await?, Admission::Slot(_)));
        Ok(())
    }

    #[tokio::test]
    async fn offers_are_refused_while_local_jobs_wait() -> Result<()> {
        let h = Harness::start("offer", vec![class(1000, &[0])], u64::MAX, &[], &[])?;
        let answer = h.offer("r").await?;
        let served = answer.slot.expect("a free slot");
        let st = h.status().await;
        assert_eq!((st.classes[0].running, st.classes[0].serving), (0, 1));
        let shown = h.load.borrow().clone();
        assert_eq!(shown.version, answer.version, "the publish after the answer has its version");
        assert_eq!(shown.classes[0].free, 0);
        let mut local = h.submit("l");
        assert!(pending(&mut local).await);
        let refused = h.offer("r").await?;
        assert!(refused.slot.is_none(), "a local job is waiting");
        assert_eq!(refused.version, answer.version + 1, "a refusal is published too");
        drop(served);
        slot(local).await?;
        Ok(())
    }

    #[tokio::test]
    async fn memory_limit_holds_jobs_back() -> Result<()> {
        let h = Harness::start("mem", vec![class(1000, &[0, 1, 2, 3])], 1000, &[("big", 10, 600)], &[])?;
        let first = slot(h.submit("big")).await?;
        let mut second = h.submit("big");
        assert!(pending(&mut second).await);
        drop(first);
        slot(second).await?;
        Ok(())
    }

    #[tokio::test]
    async fn small_jobs_do_not_starve_a_big_one() -> Result<()> {
        let h = Harness::start(
            "starve",
            vec![class(1000, &[0, 1, 2, 3])],
            1000,
            &[("big", 10_000, 950), ("small", 10, 100)],
            &[],
        )?;
        let running = slot(h.submit("small")).await?;
        let mut big = h.submit("big");
        let mut small = h.submit("small");
        assert!(pending(&mut big).await && pending(&mut small).await);
        drop(running);
        slot(big).await?;
        Ok(())
    }

    #[tokio::test]
    async fn published_tokens_leave_out_the_ones_codegen_threads_hold() -> Result<()> {
        let h = Harness::start("tokens", vec![class(1000, &[0, 1, 2, 3])], 1000, &[("big", 10, 600)], &[])?;
        let first = slot(h.submit("big")).await?;
        h.published_free(3).await?;
        // Its codegen threads take two more, then give them back.
        let mut fifo = h.fifo()?;
        fifo.read_exact(&mut [0u8; 2])?;
        h.published_free(1).await?;
        fifo.write_all(b"++")?;
        h.published_free(3).await?;
        // The second waits for memory with tokens free, and while it does
        // the box takes no offers.
        let mut second = h.submit("big");
        assert!(pending(&mut second).await);
        h.published_free(0).await?;
        drop(first);
        let second = slot(second).await?;
        h.published_free(3).await?;
        drop(second);
        Ok(())
    }

    #[tokio::test]
    async fn a_job_sent_away_counts_against_the_box_until_its_state_shows_it() -> Result<()> {
        // 5 s here, 1 s there.
        let h = Harness::start("unseen", vec![class(1000, &[0])], u64::MAX, &[("big", 5_000_000, 1)], &[])?;
        h.farm(remote_box(200, 4, 1, 1));
        let Admission::Remote(sent) = admitted(h.submit_with("big", want())).await? else {
            bail!("not sent remote");
        };
        let here = slot(h.submit_with("big", want())).await?;
        let mut third = h.submit_with("big", want());
        assert!(pending(&mut third).await, "b's one free token is taken");
        sent.accepted(5);
        h.farm(remote_box(200, 4, 1, 4));
        assert!(pending(&mut third).await, "version 4 doesn't count the job yet");
        h.farm(remote_box(200, 4, 0, 5));
        assert!(pending(&mut third).await, "version 5 counts it");
        h.farm(remote_box(200, 4, 1, 6));
        assert!(matches!(admitted(third).await?, Admission::Remote(_)), "another token freed");
        drop((sent, here));
        Ok(())
    }

    #[tokio::test]
    async fn a_wave_counts_the_jobs_it_just_placed() -> Result<()> {
        // 5 s here, 5.5 s there. Of two placed in one dispatch, the second
        // would wait 5 s for this box, so it goes to b.
        let dir = scratch("wave");
        let costs = [("big", 5_000_000, 1)];
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], u64::MAX, &costs, &[])?;
        let (jobs, state) = remote_box(1100, 4, 4, 1);
        s.farm("b".into(), Some((jobs, state)));
        let mut replies = [oneshot::channel(), oneshot::channel()].map(|(reply, rx)| {
            s.submit(unit("big", &[]), 1, want(), reply);
            rx
        });
        for w in &mut s.queue {
            w.since -= GATHER;
        }
        s.dispatch();
        let [a, b] = &mut replies;
        assert!(matches!((a.try_recv()?, b.try_recv()?), (Admission::Slot(_), Admission::Remote(_))));
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn jobs_with_no_history_go_wherever_a_token_is_free() -> Result<()> {
        let h = Harness::start("unknown", vec![class(1000, &[0])], u64::MAX, &[("small", 10_000, 1)], &[])?;
        h.farm(remote_box(1000, 4, 4, 1));
        let here = slot(h.submit_with("new", want())).await?;
        // Its predicted time says waiting here beats b's overhead, but it
        // goes at once, without waiting for the first to run over.
        let second = time::timeout(Duration::from_millis(200), h.submit_with("new", want())).await??;
        assert!(matches!(second, Admission::Remote(_)), "this box is full and b is not");
        let mut small = h.submit_with("small", want());
        assert!(pending(&mut small).await, "a job known to be small waits for this box");
        drop(here);
        Ok(())
    }

    #[tokio::test]
    async fn status_lists_every_unit_run_here_or_sent() -> Result<()> {
        let h = Harness::start("units", vec![class(1000, &[0])], u64::MAX, &[("big", 5_000_000, 1)], &[])?;
        let ran = || Ran { cpu_us: 1000, wall_us: 1000, maxrss_kb: 1 };
        h.offer("theirs").await?.slot.expect("a free slot").finish(ran());
        slot(h.submit("mine")).await?.finish(ran());
        h.farm(remote_box(200, 4, 4, 1));
        let Admission::Remote(r) = admitted(h.submit_with("big", want())).await? else {
            bail!("not sent remote");
        };
        r.finish(RemoteOutcome::Done(ran()));
        let st = h.status().await;
        let runs = |key: &str| {
            let u = st.units.iter().find(|u| u.key == key).unwrap();
            (u.here, u.sent, u.served, u.rank_mcu.is_some())
        };
        assert_eq!(runs("theirs"), (0, 0, 1, false), "not in this box's graph");
        assert_eq!(runs("mine"), (1, 0, 0, true));
        assert_eq!(runs("big"), (5, 1, 0, true));
        Ok(())
    }

    #[tokio::test]
    async fn leaked_tokens_are_refilled_when_the_class_is_idle() -> Result<()> {
        let h = Harness::start("leak", vec![class(1000, &[0, 1])], u64::MAX, &[], &[])?;
        let job = slot(h.submit("a")).await?;
        // A rustc that dies holding a codegen token never gives it back.
        let mut fifo = h.fifo()?;
        fifo.read_exact(&mut [0u8; 1])?;
        drop(fifo);
        drop(job);
        let a = slot(h.submit("a")).await?;
        let b = slot(h.submit("a")).await?;
        drop((a, b));
        Ok(())
    }

    #[tokio::test]
    async fn a_calibrated_profile_applies_to_new_jobs_at_once() -> Result<()> {
        let h = Harness::start("switch", vec![class(1000, &[0])], u64::MAX, &[], &[])?;
        classes::save(&h.dir, &Profile::new("battery")?, &[class(3000, &[0]), class(5000, &[1])])?;
        let old = slot(h.submit("a")).await?;
        h.profile("battery");
        // The old layout's only token is held, yet the new job runs.
        let new = slot(h.submit("a")).await?;
        let st = h.status().await;
        assert_eq!(st.profile.as_str(), "battery");
        assert_eq!(st.classes.len(), 2);
        assert_eq!(st.finishing, 1);
        let fifos = || {
            fs::read_dir(&h.dir).unwrap().filter(|e| {
                e.as_ref().unwrap().file_name().to_string_lossy().starts_with("class-")
            }).count()
        };
        assert_eq!(fifos(), 3);
        drop(old);
        assert_eq!(h.status().await.finishing, 0);
        assert_eq!(fifos(), 2, "the retired layout's fifo is removed");
        assert_eq!(fs::read_to_string(h.dir.join("profile"))?, "battery");
        drop(new);
        Ok(())
    }

    #[tokio::test]
    async fn an_uncalibrated_profile_holds_new_jobs_until_running_ones_finish() -> Result<()> {
        let h = Harness::start("drain", vec![class(1000, &[0, 1])], u64::MAX, &[], &[])?;
        let running = slot(h.submit("a")).await?;
        h.profile("never-seen");
        let mut waiting = h.submit("a");
        assert!(pending(&mut waiting).await, "a free token exists but the switch holds it");
        assert!(matches!(h.status().await.switching, Switching::Draining(_)));
        // Stop before the calibration itself would start.
        drop(h);
        drop(running);
        Ok(())
    }
}
