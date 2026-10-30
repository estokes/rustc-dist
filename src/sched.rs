use crate::{
    calibrate,
    classes::{self, Class, Profile},
    graph::Graph,
    protocol::{ClassStatus, Ran, Reply, Status, Switching, Unit, UnitStatus, frame},
    stats::{Prediction, Sample, Stats},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use compact_str::format_compact;
use log::{error, info, warn};
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
    sync::{mpsc, oneshot},
    task,
    time::{self, MissedTickBehavior},
};

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

atomic_id!(LayoutId);
atomic_id!(JobId);

pub enum Msg {
    Submit { unit: Unit, build: u32, reply: oneshot::Sender<Admission> },
    Finished { job: JobId, unit: Unit, ran: Option<Ran> },
    Status { reply: oneshot::Sender<Status> },
    Profile { profile: Profile, recalibrate: bool },
    Calibrated { profile: Profile, result: Result<Vec<Class>> },
    Drain,
}

pub enum Admission {
    Slot(Slot),
    Restarting,
}

/// Held while a wrapper runs rustc. Dropping it frees the slot.
pub struct Slot {
    /// The framed `Reply::Slot` for the wrapper.
    pub grant: Bytes,
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
    grants: Vec<Bytes>,
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
            frame(&mut buf, &Reply::Slot { cpus: class.cpus.clone(), jobserver })?;
            grants.push(buf.split().freeze());
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

struct Waiting {
    unit: Unit,
    build: u32,
    pred: Prediction,
    rank_mcu: u64,
    since: Instant,
    reply: oneshot::Sender<Admission>,
}

struct Running {
    job: JobId,
    build: u32,
    layout: LayoutId,
    class: usize,
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
    stats: Stats,
    graph: Graph,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Sched {
    /// Start in `profile`. If it was never calibrated, calibrate before
    /// admitting anything.
    pub fn new(setup: Setup, profile: Profile, tx: mpsc::UnboundedSender<Msg>) -> Result<Self> {
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
        Ok(Self {
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
            stats,
            graph,
            tx,
        })
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
        loop {
            self.advance();
            let starved = self.starved;
            let gathering = self.gathering;
            let event = tokio::select! {
                msg = rx.recv() => Event::Msg(msg),
                _ = std::future::poll_fn(|cx| self.poll_tokens(cx)), if starved => Event::Recheck,
                _ = recheck.tick(), if starved => Event::Recheck,
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
            Msg::Submit { unit, build, reply } => {
                self.graph.record(&unit);
                let rank_mcu = self.graph.rank(&unit.id, &self.stats);
                let pred = self.stats.predict(&unit.exact, &unit.coarse);
                self.queue.push(Waiting { unit, build, pred, rank_mcu, since: Instant::now(), reply });
            }
            Msg::Finished { job, unit, ran } => self.finished(job, &unit, ran),
            Msg::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Msg::Profile { profile, recalibrate } => self.switch_to(profile, recalibrate)?,
            Msg::Calibrated { profile, result } => self.calibrated(profile, result)?,
            Msg::Drain => return Ok(false),
        }
        Ok(true)
    }

    fn finished(&mut self, job: JobId, unit: &Unit, ran: Option<Ran>) {
        let Some(i) = self.running.iter().position(|r| r.job == job) else {
            warn!("unknown job {job:?} finished");
            return;
        };
        let r = self.running.swap_remove(i);
        self.mem_running_kb -= r.mem_kb;
        let layout = if self.layout.id == r.layout {
            &mut self.layout
        } else if let Some(l) = self.retired.iter_mut().find(|l| l.id == r.layout) {
            l
        } else {
            warn!("job {job:?} finished in unknown layout {:?}", r.layout);
            return;
        };
        layout.release(r.class);
        self.retired.retain(|l| !l.idle());
        if let Some(ran) = ran {
            let sample = Sample {
                cost_mcu: ran.cpu_us * 1000 / r.calib_us,
                wall_ms: ran.wall_us / 1000,
                mem_kb: ran.maxrss_kb,
            };
            self.stats.record(unit, sample);
        }
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

    /// Start a pending calibration once nothing is running.
    fn advance(&mut self) {
        let Switch::Draining(to) = &self.switch else { return };
        if !self.running.is_empty() {
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

    /// µs until a slot of `class` is expected to free, from the jobs
    /// running in it.
    fn frees_in_us(&self, class: usize, now: Instant) -> u64 {
        self.running
            .iter()
            .filter(|r| r.layout == self.layout.id && r.class == class)
            .map(|r| r.left_us(now))
            .min()
            .unwrap_or(0)
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
            .chain(self.running.iter().map(|r| (r.build, r.rank_left_mcu(now))))
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
        frees_in.extend((0..held.len()).map(|k| if held[k] > 0 { 0 } else { self.frees_in_us(k, now) }));
        let mut granted: LPooled<Vec<(usize, usize)>> = LPooled::take();
        for &i in admissible.iter() {
            let w = &self.queue[i];
            let slack_mcu = horizon[&w.build].saturating_sub(w.rank_mcu);
            let class = choose(&self.layout.classes, &frees_in, w.pred.cost_mcu, slack_mcu);
            if held[class] == 0 {
                // Jobs after this one see the queue it joins, so they don't
                // all count on the same next free slot.
                let c = &self.layout.classes[class];
                frees_in[class] += time_us(w.pred.cost_mcu, c.calib_us) / c.cpus.len() as u64;
                self.starved = true;
                continue;
            }
            held[class] -= 1;
            if held[class] == 0 {
                frees_in[class] = self.frees_in_us(class, now);
            }
            granted.push((i, class));
        }
        for (fifo, n) in self.layout.fifos.iter().zip(held.iter()) {
            fifo.give(*n);
        }
        granted.sort_unstable_by_key(|&(i, _)| std::cmp::Reverse(i));
        for &(i, class) in granted.iter() {
            let w = self.queue.swap_remove(i);
            self.grant(w, class, now);
        }
    }

    fn grant(&mut self, w: Waiting, class: usize, now: Instant) {
        let job = JobId::new();
        self.layout.running[class] += 1;
        self.mem_running_kb += w.pred.mem_kb;
        self.running.push(Running {
            job,
            build: w.build,
            layout: self.layout.id,
            class,
            calib_us: self.layout.classes[class].calib_us,
            mem_kb: w.pred.mem_kb,
            cost_mcu: w.pred.cost_mcu,
            rank_mcu: w.rank_mcu,
            start: now,
        });
        let slot = Slot {
            grant: self.layout.grants[class].clone(),
            job,
            unit: w.unit,
            ran: None,
            tx: self.tx.clone(),
        };
        // If the wrapper is gone the slot comes back through Drop.
        let _ = w.reply.send(Admission::Slot(slot));
    }

    fn status(&mut self) -> Status {
        let mut ranked = self.graph.ranked(&self.stats);
        ranked.sort_unstable_by_key(|(_, rank)| std::cmp::Reverse(*rank));
        let units = ranked
            .into_iter()
            .filter_map(|(key, rank_mcu)| {
                let info = self.stats.info(&key)?;
                Some(UnitStatus {
                    key,
                    runs: info.runs,
                    cost_mcu: info.cost_mcu,
                    wall_ms: info.wall_ms,
                    mem_kb: info.mem_kb,
                    rank_mcu,
                })
            })
            .take(20)
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
                .zip(&self.layout.running)
                .map(|(c, r)| ClassStatus {
                    calib_us: c.calib_us,
                    cpus: c.cpus.len() as u32,
                    running: *r,
                })
                .collect(),
            finishing: self.retired.iter().flat_map(|l| l.running.iter()).sum(),
            waiting: self.queue.len() as u32,
            mem_limit_kb: self.setup.mem_limit_kb,
            mem_running_kb: self.mem_running_kb,
            graph_nodes: self.graph.len() as u32,
            units,
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

/// The slowest class on which a job still finishes within its slack, so
/// faster classes stay free for jobs more work waits on. With no slack,
/// the class where it finishes first, even if it must wait for it.
/// Classes are fastest first; `frees_in` is µs until each has a slot.
fn choose(classes: &[Class], frees_in: &[u64], cost_mcu: u64, slack_mcu: u64) -> usize {
    let finish = |k: usize| frees_in[k] + time_us(cost_mcu, classes[k].calib_us);
    let first = (0..classes.len()).map(finish).min().unwrap_or(0);
    let slack = time_us(slack_mcu, classes[0].calib_us);
    (0..classes.len()).rev().find(|&k| finish(k) <= first + slack).unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::bail;
    use netidx_core::pack::Pack;
    use std::{fs::OpenOptions, io::Read};

    fn class(calib_us: u64, cpus: &[u32]) -> Class {
        Class { calib_us, cpus: cpus.to_vec() }
    }

    fn unit(name: &str, deps: &[&str]) -> Unit {
        Unit {
            exact: ArcStr::from(name),
            coarse: ArcStr::from(name),
            id: ArcStr::from(name),
            deps: deps.iter().map(|d| ArcStr::from(*d)).collect(),
        }
    }

    #[test]
    fn choose_spends_slack_on_slower_classes() {
        let classes = [class(1000, &[0]), class(2000, &[1]), class(4000, &[2])];
        // 1000 mcu takes 1, 2 or 4 ms. All free.
        assert_eq!(choose(&classes, &[0, 0, 0], 1000, 0), 0);
        assert_eq!(choose(&classes, &[0, 0, 0], 1000, 1000), 1);
        assert_eq!(choose(&classes, &[0, 0, 0], 1000, 3000), 2);
        // The fast class frees in 5 ms: with no slack, start now on the middle one.
        assert_eq!(choose(&classes, &[5000, 0, 0], 1000, 0), 1);
        // Plenty of slack: wait 1 ms for the slow class rather than take a free fast one.
        assert_eq!(choose(&classes, &[0, 0, 1000], 1000, 10_000), 2);
    }

    struct Harness {
        tx: mpsc::UnboundedSender<Msg>,
        dir: PathBuf,
    }

    impl Harness {
        /// A scheduler in profile `default` with `classes`, job history
        /// from `costs`, and graph edges from `edges`.
        fn start(
            name: &str,
            classes: Vec<Class>,
            mem_limit_kb: u64,
            costs: &[(&str, u64, u64)],
            edges: &[(&str, &[&str])],
        ) -> Result<Self> {
            let dir = std::env::temp_dir().join(format!("rustc-dist-sched-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir)?;
            classes::save(&dir, &Profile::DEFAULT, &classes)?;
            let setup = Setup {
                state_dir: dir.clone(),
                fifo_dir: dir.clone(),
                allowed: (0..4).collect(),
                mem_limit_kb,
                calibration_seconds: 4,
            };
            let (tx, rx) = mpsc::unbounded_channel();
            let mut sched = Sched::new(setup, Profile::DEFAULT, tx.clone())?;
            for &(name, cost_mcu, mem_kb) in costs {
                for _ in 0..5 {
                    sched.stats.record(&unit(name, &[]), Sample { cost_mcu, wall_ms: 1, mem_kb });
                }
            }
            for &(name, deps) in edges {
                sched.graph.record(&unit(name, deps));
            }
            sched.stats.dirty = false;
            sched.graph.dirty = false;
            tokio::spawn(sched.run(rx));
            Ok(Self { tx, dir })
        }

        fn submit(&self, name: &str) -> oneshot::Receiver<Admission> {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Submit { unit: unit(name, &[]), build: 1, reply }).unwrap();
            rx
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
        let mut body = &slot.grant[4..];
        match Reply::decode(&mut body).unwrap() {
            Reply::Slot { cpus, .. } => cpus[0],
            _ => panic!("not a slot"),
        }
    }

    async fn slot(rx: oneshot::Receiver<Admission>) -> Result<Slot> {
        match time::timeout(Duration::from_secs(5), rx).await?? {
            Admission::Slot(s) => Ok(s),
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
    async fn leaked_tokens_are_refilled_when_the_class_is_idle() -> Result<()> {
        let h = Harness::start("leak", vec![class(1000, &[0, 1])], u64::MAX, &[], &[])?;
        let job = slot(h.submit("a")).await?;
        // A rustc that dies holding a codegen token never gives it back.
        let fifo_path = fs::read_dir(&h.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("class-")))
            .unwrap();
        let mut fifo = OpenOptions::new().read(true).write(true).open(fifo_path)?;
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
