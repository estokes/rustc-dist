use crate::{
    calibrate,
    classes::{self, Class, Profile},
    farm::{BoxState, ClassState, Load},
    graph::Graph,
    protocol::{
        BoxStatus, ClassStatus, PROTOCOL, Ran, Reply, SpreadStatus, Status, Switching, Unit, UnitStatus,
        frame,
    },
    stats::{Origin, Prediction, Sample, Stats},
    sys::{self, BoxMemory},
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
/// While jobs run here, sample the box's memory and their footprints this
/// often. Placing a job samples again if the last sample is older.
const MEASURE: Duration = Duration::from_millis(250);
/// A class's calibrated rate counts as this much CPU time of jobs.
const RATE_PRIOR_US: f64 = 10_000_000.;
/// What a class's jobs say about its rate counts half as much after this
/// much more CPU time of its jobs.
const RATE_HALF_LIFE_US: f64 = 60_000_000.;
/// A job measured to keep fewer than this many hundredths of a thread busy
/// is light: it runs pinned to one class. Every other job, including one
/// never measured, is heavy and may run on every CPU.
const LIGHT_WIDTH_C: u64 = 150;

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
    /// A job's processes are `pid` and its descendants.
    Started { job: JobId, pid: u32 },
    /// Another box offers this one a job, pinned to `class` or, with none,
    /// on every CPU.
    Offer { unit: Unit, class: Option<u32>, reply: oneshot::Sender<Answer> },
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
    /// The job's processes are `pid` and its descendants.
    pub fn started(&self, pid: u32) {
        let _ = self.tx.send(Msg::Started { job: self.job, pid });
    }

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
    pub class: Option<u32>,
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

/// A class's instructions per CPU-µs: its calibration, then more and more
/// the jobs that ran on it.
struct Rate {
    instr: f64,
    cpu_us: f64,
}

impl Rate {
    fn seeded(rate: u64) -> Self {
        Self { instr: rate as f64 * RATE_PRIOR_US, cpu_us: RATE_PRIOR_US }
    }

    fn add(&mut self, instr: u64, cpu_us: u64) {
        let keep = 0.5f64.powf(cpu_us as f64 / RATE_HALF_LIFE_US);
        self.instr = self.instr * keep + instr as f64;
        self.cpu_us = self.cpu_us * keep + cpu_us as f64;
    }

    fn get(&self) -> u64 {
        ((self.instr / self.cpu_us) as u64).max(1)
    }
}

/// A heavy job's private jobserver, which the daemon lends tokens into
/// from its layout's classes. `lent` counts them by class, including the
/// token the job holds without reading one.
struct Spread {
    job: JobId,
    fifo: Fifo,
    lent: Vec<u32>,
    /// The most tokens it may hold: its codegen units.
    cap: u32,
    /// Its jobserver held a spare token at the last tick.
    idle: bool,
}

/// Take up to `want` tokens from `fifos` in `order`, counting them in `lent`.
fn borrow(fifos: &[Fifo], order: &[usize], want: u32, lent: &mut [u32]) -> u32 {
    let mut got = 0;
    for &k in order {
        if got == want {
            break;
        }
        let n = fifos[k].take((want - got) as usize) as u32;
        lent[k] += n;
        got += n;
    }
    got
}

/// Give `n` tokens counted in `lent` back to `fifos`, in reverse `order`.
fn repay(fifos: &[Fifo], order: &[usize], mut n: u32, lent: &mut [u32]) {
    for &k in order.iter().rev() {
        let r = n.min(lent[k]);
        fifos[k].give(r as usize);
        lent[k] -= r;
        n -= r;
    }
}

/// One profile's classes and their jobservers. A profile switch makes a
/// new layout for new jobs; the old one lives until its jobs finish.
struct Layout {
    id: LayoutId,
    /// Fastest at calibration first.
    classes: Vec<Class>,
    rates: Vec<Rate>,
    fifos: Vec<Fifo>,
    grants: Vec<Arc<Grant>>,
    /// Light jobs pinned to each class.
    running: Vec<u32>,
    spread: Vec<Spread>,
}

impl Layout {
    fn new(classes: Vec<Class>, fifo_dir: &Path, tokens_per_cpu: f64) -> Result<Self> {
        let id = LayoutId::new();
        let mut fifos = Vec::with_capacity(classes.len());
        let mut grants = Vec::with_capacity(classes.len());
        let mut buf = BytesMut::new();
        for (k, class) in classes.iter().enumerate() {
            let path = fifo_dir.join(format_compact!("class-{}-{k}", id.inner()).as_str());
            let jobserver = ArcStr::from(path.to_str().context("jobserver path is not utf-8")?);
            let tokens = (class.cpus.len() as f64 * tokens_per_cpu).round().max(1.) as usize;
            fifos.push(Fifo::create(path, tokens)?);
            let reply = Reply::Slot {
                cpus: class.cpus.clone(),
                jobserver: jobserver.clone(),
                filter_metadata: false,
            };
            frame(&mut buf, &reply)?;
            let frame = buf.split().freeze();
            grants.push(Arc::new(Grant { frame, cpus: class.cpus.clone(), jobserver }));
        }
        let rates = classes.iter().map(|c| Rate::seeded(c.rate)).collect();
        let running = vec![0; classes.len()];
        Ok(Self { id, running, classes, rates, fifos, grants, spread: Vec::new() })
    }

    fn idle(&self) -> bool {
        self.running.iter().all(|r| *r == 0) && self.spread.is_empty()
    }

    /// Tokens of `class` lent to heavy jobs.
    fn lent(&self, class: usize) -> u32 {
        self.spread.iter().map(|s| s.lent[class]).sum()
    }

    /// The fastest rate and the mean over CPUs: see `spread_rates`.
    fn spread_rates(&self) -> (u64, u64) {
        let cpus = self.classes.iter().map(|c| c.cpus.len() as u64);
        spread_rates(self.rates.iter().map(Rate::get).zip(cpus))
    }

    /// Classes by their current rate, fastest first.
    fn by_speed(&self) -> LPooled<Vec<usize>> {
        let mut order: LPooled<Vec<usize>> = LPooled::take();
        order.extend(0..self.classes.len());
        order.sort_by_key(|&k| std::cmp::Reverse(self.rates[k].get()));
        order
    }

    fn release(&mut self, class: usize) {
        self.running[class] -= 1;
        let lent = self.lent(class) as usize;
        let fifo = &self.fifos[class];
        if self.running[class] == 0 {
            // No light job of this class is alive, so every token not lent
            // should be in the FIFO. A rustc killed while holding tokens
            // leaks them.
            let had = fifo.take(usize::MAX) + 1;
            let should = fifo.tokens.saturating_sub(lent);
            fifo.give(should);
            if had != should {
                info!("class {class} jobserver had {had} of {should} tokens, refilled");
            }
        } else {
            fifo.give(1);
        }
    }

    /// Start a heavy job on `cpus`, holding a token already taken from
    /// `class`, with one spare in its own jobserver if any class has one.
    fn spread(
        &mut self,
        job: JobId,
        class: usize,
        cap: u32,
        fifo_dir: &Path,
        cpus: &[u32],
    ) -> Result<Arc<Grant>> {
        let path = fifo_dir.join(format_compact!("spread-{}-{}", self.id.inner(), job.inner()).as_str());
        let jobserver = ArcStr::from(path.to_str().context("jobserver path is not utf-8")?);
        let fifo = Fifo::create(path, 0)?;
        let mut buf = BytesMut::new();
        let reply = Reply::Slot { cpus: cpus.to_vec(), jobserver: jobserver.clone(), filter_metadata: false };
        frame(&mut buf, &reply)?;
        let mut lent = vec![0; self.classes.len()];
        lent[class] = 1;
        let spare = borrow(&self.fifos, &self.by_speed(), 1, &mut lent);
        fifo.give(spare as usize);
        self.spread.push(Spread { job, fifo, lent, cap: cap.max(1), idle: false });
        Ok(Arc::new(Grant { frame: buf.freeze(), cpus: cpus.to_vec(), jobserver }))
    }

    /// A heavy job ended: its tokens go back to the classes they came from.
    fn unspread(&mut self, job: JobId) {
        let Some(i) = self.spread.iter().position(|s| s.job == job) else { return };
        let s = self.spread.swap_remove(i);
        for (fifo, n) in self.fifos.iter().zip(&s.lent) {
            fifo.give(*n as usize);
        }
    }

    /// Lend each heavy job more tokens, fastest classes first and twice
    /// what it holds, once it has taken its spare; take back spares it has
    /// left unused for a tick, all but one. Rustc reads a token when it
    /// wants another thread, which the daemon can't see, so the spare is
    /// how a job asks.
    fn lend(&mut self) {
        let order = self.by_speed();
        let Self { fifos, spread, .. } = self;
        for s in spread.iter_mut() {
            let spare = s.fifo.free();
            let held: u32 = s.lent.iter().sum();
            if spare == 0 {
                s.idle = false;
                if held < s.cap {
                    let got = borrow(fifos, &order, held.min(s.cap - held), &mut s.lent);
                    s.fifo.give(got as usize);
                }
            } else {
                if s.idle && spare > 1 {
                    let back = s.fifo.take(spare as usize - 1) as u32;
                    repay(fifos, &order, back, &mut s.lent);
                }
                s.idle = true;
            }
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
    /// Measures the box's memory.
    pub memory: Box<dyn Fn() -> Result<BoxMemory> + Send + Sync>,
    pub tokens_per_cpu: f64,
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
    rank_mi: u64,
    portable: Option<Portable>,
    since: Instant,
    reply: oneshot::Sender<Admission>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    /// A light job pinned to `class`.
    Local { layout: LayoutId, class: usize },
    /// A heavy job on every CPU, its first token from `class`.
    Spread { layout: LayoutId, class: usize },
    /// On another box, pinned to `class` or, with none, on every CPU.
    /// `shown_in` is the version of the box's state that first counts the
    /// job, known once the box accepts it.
    Remote { remote: usize, class: Option<usize>, shown_in: Option<u64> },
}

impl Place {
    fn here(&self) -> bool {
        matches!(self, Place::Local { .. } | Place::Spread { .. })
    }
}

struct Running {
    job: JobId,
    /// The build it is part of, by cargo's pid; None for another box's job.
    build: Option<u32>,
    place: Place,
    /// Instructions per µs per thread where it runs, to estimate its cost
    /// on a box that can't count instructions.
    rate: u64,
    /// Its predicted run time.
    run_us: u64,
    /// Predicted peak memory; none for a job on another box.
    mem_kb: u64,
    cost_mi: u64,
    rank_mi: u64,
    start: Instant,
    /// The root of its processes, once it has started here.
    pid: Option<u32>,
    /// Its processes' RSS plus swap at the last sample, and the most seen.
    foot_kb: u64,
    peak_kb: u64,
}

impl Running {
    /// Predicted µs left. A job past its prediction is expected to run
    /// for at least half again as long as it has.
    fn left_us(&self, now: Instant) -> u64 {
        let elapsed = now.duration_since(self.start).as_micros() as u64;
        self.run_us.saturating_sub(elapsed).max(elapsed / 2)
    }

    /// Its rank less the part of its own cost already done.
    fn rank_left_mi(&self, now: Instant) -> u64 {
        let elapsed = now.duration_since(self.start).as_micros() as u64;
        let done = self.cost_mi * elapsed.min(self.run_us) / self.run_us.max(1);
        self.rank_mi.saturating_sub(done)
    }

    fn here(&self) -> bool {
        self.place.here()
    }
}

/// Another box as this one knows it.
struct RemoteBox {
    name: ArcStr,
    jobs: NPath,
    state: Option<BoxState>,
    /// Tokens each class has for jobs from here, as of the last dispatch:
    /// what its state shows free, less what jobs sent there that it doesn't
    /// show yet hold.
    avail: Vec<u32>,
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

/// Predicted µs for `cost_mi` million instructions at `rate` per µs.
fn time_us(cost_mi: u64, rate: u64) -> u64 {
    cost_mi * 1_000_000 / rate.max(1)
}

/// From each class's rate and CPUs: the fastest rate, for a heavy job's
/// front end, and the mean over CPUs, for its threads, which run anywhere.
fn spread_rates(classes: impl Iterator<Item = (u64, u64)>) -> (u64, u64) {
    let (mut fast, mut sum, mut cpus) = (1, 0, 0);
    for (rate, n) in classes {
        fast = fast.max(rate);
        sum += rate * n;
        cpus += n;
    }
    (fast, (sum / cpus.max(1)).max(1))
}

/// `spread_rates` of another box's published classes.
fn state_rates(classes: &[ClassState]) -> (u64, u64) {
    spread_rates(classes.iter().map(|c| (c.rate, c.cpus as u64)))
}

/// Whether a job runs pinned to one class.
fn light(pred: &Prediction) -> bool {
    pred.width_c.is_some_and(|w| w < LIGHT_WIDTH_C)
}

/// Threads a heavy job keeps busy after its front end: as measured, or
/// its codegen units if never measured.
fn width(pred: &Prediction, unit: &Unit) -> u64 {
    pred.width_c.map_or(unit.codegen_units as u64, |w| w.div_ceil(100)).max(1)
}

/// Predicted µs for a heavy job: its front end at `fast`, and the rest at
/// `rate` per thread on `threads`.
fn spread_us(pred: &Prediction, fast: u64, rate: u64, threads: u64) -> u64 {
    let front = pred.front_mi.unwrap_or(0).min(pred.cost_mi);
    time_us(front, fast) + time_us(pred.cost_mi - front, rate) / threads.max(1)
}

/// Take `n` of `avail` from the fastest of `classes` first, as a server
/// starts a heavy job: the token it holds, then a spare.
fn take_fastest(avail: &mut [u32], classes: &[ClassState], mut n: u32) {
    let mut order: LPooled<Vec<usize>> = LPooled::take();
    order.extend(0..avail.len().min(classes.len()));
    order.sort_by_key(|&k| std::cmp::Reverse(classes[k].rate));
    for &k in order.iter() {
        let t = n.min(avail[k]);
        avail[k] -= t;
        n -= t;
    }
}

pub struct Sched {
    setup: Setup,
    profile: Profile,
    layout: Layout,
    /// Layouts from earlier profiles that still have running jobs.
    retired: Vec<Layout>,
    switch: Switch,
    running: Vec<Running>,
    /// The box's memory at the last sample, less the footprints of jobs
    /// that finished since.
    mem: BoxMemory,
    measured: Option<Instant>,
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
        let layout = Layout::new(classes, &setup.fifo_dir, setup.tokens_per_cpu)?;
        log_classes(&profile, &layout.classes);
        let mut sched = Self {
            setup,
            profile,
            empty: vec![false; layout.classes.len()],
            layout,
            retired: Vec::new(),
            switch,
            running: Vec::new(),
            mem: BoxMemory::default(),
            measured: None,
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
            Sample,
            Measure,
            Save,
        }
        let mut save = time::interval(SAVE_EVERY);
        let mut recheck = time::interval(RECHECK);
        recheck.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut sample = time::interval(SAMPLE);
        sample.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut measure = time::interval(MEASURE);
        measure.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            self.advance();
            let starved = self.starved;
            let gathering = self.gathering;
            let sampling = !self.layout.spread.is_empty() || (!self.layout.idle() && !self.load.is_closed());
            let measuring = self.runs_here();
            let event = tokio::select! {
                msg = rx.recv() => Event::Msg(msg),
                _ = std::future::poll_fn(|cx| self.poll_tokens(cx)), if starved => Event::Recheck,
                _ = recheck.tick(), if starved => Event::Recheck,
                _ = sample.tick(), if sampling => Event::Sample,
                _ = measure.tick(), if measuring => Event::Measure,
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
                Event::Sample => self.layout.lend(),
                Event::Measure => self.measure(Instant::now()),
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
            Msg::Started { job, pid } => {
                if let Some(r) = self.running.iter_mut().find(|r| r.job == job) {
                    r.pid = Some(pid);
                }
            }
            Msg::Offer { unit, class, reply } => {
                let slot = self.offer(unit, class.map(|k| k as usize));
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
        let rank_mi = self.graph.rank(&unit.id, &self.stats);
        let pred = self.stats.predict(&unit.exact, &unit.coarse);
        let pinned = self.mismatches.get(&unit.exact).is_some_and(|n| *n >= PIN_AFTER);
        let portable = match remote {
            Some(Want { key, target }) if !pinned => {
                self.graph.inputs(&unit.deps).map(|crates| Portable { key, target, crates })
            }
            _ => None,
        };
        self.queue.push(Waiting { unit, build, pred, rank_mi, portable, since: Instant::now(), reply });
    }

    fn finished(&mut self, job: JobId, unit: &Unit, ran: Option<Ran>) {
        let Some(r) = self.take_running(job) else { return };
        let (Place::Local { layout: id, .. } | Place::Spread { layout: id, .. }) = r.place else {
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
        match r.place {
            Place::Local { class, .. } => {
                layout.release(class);
                // Only a job pinned to one class says how fast that class is.
                if let Some(ran) = &ran
                    && let Some(n) = ran.instructions
                {
                    layout.rates[class].add(n, ran.cpu_us);
                }
            }
            _ => layout.unspread(job),
        }
        self.retired.retain(|l| !l.idle());
        if let Some(ran) = ran {
            let origin = if r.build.is_some() { Origin::Here } else { Origin::Served };
            self.record(unit, &ran, r.rate, r.peak_kb, origin);
        }
    }

    fn take_running(&mut self, job: JobId) -> Option<Running> {
        let Some(i) = self.running.iter().position(|r| r.job == job) else {
            warn!("unknown job {job:?} finished");
            return None;
        };
        let r = self.running.swap_remove(i);
        self.mem.used_kb = self.mem.used_kb.saturating_sub(r.foot_kb);
        Some(r)
    }

    /// `rate` is that of the class it ran on, for a box that can't count
    /// instructions. `peak_kb` is the most RSS plus swap seen while it ran
    /// here, which `maxrss_kb` misses when its pages were swapped out.
    fn record(&mut self, unit: &Unit, ran: &Ran, rate: u64, peak_kb: u64, origin: Origin) {
        debug!("{} used {} ms cpu in {} ms", unit.exact, ran.cpu_us / 1000, ran.wall_us / 1000);
        let cost_mi = ran.instructions.unwrap_or(ran.cpu_us * rate) / 1_000_000;
        let (front_mi, width_c) = match ran.front {
            Some(f) => {
                let front_mi = cost_mi * f.cpu_us.min(ran.cpu_us) / ran.cpu_us.max(1);
                let rest_wall = ran.wall_us.saturating_sub(f.wall_us).max(1);
                (Some(front_mi), ran.cpu_us.saturating_sub(f.cpu_us) * 100 / rest_wall)
            }
            None => (None, ran.cpu_us * 100 / ran.wall_us.max(1)),
        };
        let sample = Sample {
            cost_mi,
            front_mi,
            width_c,
            wall_ms: ran.wall_us / 1000,
            mem_kb: ran.maxrss_kb.max(peak_kb),
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
                self.record(unit, &ran, r.rate, 0, Origin::Sent);
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
                    avail: Vec::new(),
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

    /// Admit a job another box offers, pinned to `class` or, with none,
    /// spread over every CPU, if a token is free and no job of this box's
    /// own is waiting.
    fn offer(&mut self, unit: Unit, class: Option<usize>) -> Option<Slot> {
        let refuse = |why: &str| {
            debug!("refusing {}: {why}", unit.exact);
            None
        };
        if let Some(why) = self.closed() {
            return refuse(why);
        }
        let pred = self.stats.predict(&unit.exact, &unit.coarse);
        let now = Instant::now();
        self.measure_if_stale(now);
        if self.runs_here() && pred.mem_kb > self.mem_free_kb() {
            return refuse("no memory");
        }
        let rank_mi = pred.cost_mi;
        match class {
            Some(k) => {
                let k = k.min(self.layout.classes.len() - 1);
                if self.layout.fifos[k].take(1) == 0 {
                    return refuse("no token");
                }
                Some(self.admit(unit, None, pred, rank_mi, k, now))
            }
            None => {
                let order = self.layout.by_speed();
                let Some(&k) = order.iter().find(|&&k| self.layout.fifos[k].take(1) == 1) else {
                    return refuse("no token");
                };
                Some(self.admit_spread(unit, None, pred, rank_mi, k, now))
            }
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

    /// Start a pending calibration once nothing runs here.
    fn advance(&mut self) {
        let Switch::Draining(to) = &self.switch else { return };
        if self.runs_here() {
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
        let layout = Layout::new(classes, &self.setup.fifo_dir, self.setup.tokens_per_cpu)?;
        let old = mem::replace(&mut self.layout, layout);
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

    /// µs until a token of local `class` is expected to free, from the
    /// jobs holding its tokens, if any do.
    fn frees_in_us(&self, class: usize, now: Instant) -> Option<u64> {
        let l = &self.layout;
        let holds = |r: &&Running| match r.place {
            Place::Local { layout, class: k } => layout == l.id && k == class,
            Place::Spread { layout, .. } => {
                layout == l.id && l.spread.iter().any(|s| s.job == r.job && s.lent[class] > 0)
            }
            Place::Remote { .. } => false,
        };
        self.running.iter().filter(holds).map(|r| r.left_us(now)).min()
    }

    /// Whether any job runs on this box, its own or another box's.
    fn runs_here(&self) -> bool {
        self.running.iter().any(Running::here)
    }

    /// Sample the box's memory and the footprint of each job running here.
    fn measure(&mut self, now: Instant) {
        match (self.setup.memory)() {
            Ok(mem) => self.mem = mem,
            Err(e) => warn!("measuring memory: {e:#}"),
        }
        for r in &mut self.running {
            if let Some(pid) = r.pid {
                r.foot_kb = sys::tree_footprint_kb(pid);
                r.peak_kb = r.peak_kb.max(r.foot_kb);
            }
        }
        self.measured = Some(now);
    }

    fn measure_if_stale(&mut self, now: Instant) {
        if self.measured.is_none_or(|t| now.duration_since(t) >= MEASURE) {
            self.measure(now);
        }
    }

    /// What running jobs are predicted to add to their footprints.
    fn mem_growth_kb(&self) -> u64 {
        self.running.iter().map(|r| r.mem_kb.saturating_sub(r.foot_kb)).sum()
    }

    /// Memory a new job here may take: the box's capacity less what it holds
    /// and what running jobs are predicted to add.
    fn mem_free_kb(&self) -> u64 {
        self.mem.capacity_kb.saturating_sub(self.mem.used_kb + self.mem_growth_kb())
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
            w.rank_mi as f64 * (now.duration_since(w.since).as_secs_f64() / AGING_S).exp2()
        };
        self.queue.sort_by(|a, b| prio(b).total_cmp(&prio(a)));
        let mut admissible: LPooled<Vec<usize>> = LPooled::take();
        admissible.extend((0..self.queue.len()).filter(|&i| !young(&self.queue[i])));
        if admissible.is_empty() {
            return;
        }
        self.measure_if_stale(now);
        // The work left in each build: the largest rank among its jobs,
        // counting the ones too young to place.
        let mut horizon: LPooled<HashMap<u32, u64>> = LPooled::take();
        for (build, rank) in self
            .queue
            .iter()
            .map(|w| (w.build, w.rank_mi))
            .chain(self.running.iter().filter_map(|r| Some((r.build?, r.rank_left_mi(now)))))
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
        self.count_available();
        let mut rates: LPooled<Vec<u64>> = LPooled::take();
        rates.extend(self.layout.rates.iter().map(Rate::get));
        let fastest = rates.iter().copied().max().unwrap_or(1);
        let (_, spread_rate) = self.layout.spread_rates();
        let mut mem_free = self.mem_free_kb();
        // With nothing running here, one job runs whatever its memory, so
        // the box can't sit idle behind a job bigger than it.
        let mut first_here = !self.runs_here();
        // Set once a job can neither fit here nor go to another box. Later
        // jobs don't run here either, so smaller ones can't starve it.
        let mut mem_blocked = false;
        let mut cands: LPooled<Vec<Cand>> = LPooled::take();
        let mut wide: LPooled<Vec<Wide>> = LPooled::take();
        let mut granted: LPooled<Vec<(usize, Place, u64)>> = LPooled::take();
        for &i in admissible.iter() {
            let w = &self.queue[i];
            let fits = !mem_blocked && (first_here || w.pred.mem_kb <= mem_free);
            let away = w
                .portable
                .as_ref()
                .filter(|_| !w.pred.known || time_us(w.pred.cost_mi, fastest) >= REMOTE_MIN_US);
            let picked = if light(&w.pred) {
                let local = if fits { rates.len() } else { 0 };
                cands.clear();
                cands.extend(rates[..local].iter().enumerate().map(|(k, &rate)| Cand {
                    rate,
                    frees_in_us: frees_in[k],
                    overhead_us: 0,
                    place: Place::Local { layout: self.layout.id, class: k },
                }));
                for (r, rb) in self.remotes.iter().enumerate() {
                    let Some(state) = away.and_then(|p| rb.usable(p)) else { continue };
                    for (k, c) in state.classes.iter().enumerate() {
                        if rb.avail.get(k).is_some_and(|n| *n > 0) {
                            cands.push(Cand {
                                rate: c.rate,
                                frees_in_us: 0,
                                overhead_us: REMOTE_OVERHEAD_US,
                                place: Place::Remote { remote: r, class: Some(k), shown_in: None },
                            });
                        }
                    }
                }
                (!cands.is_empty()).then(|| {
                    let slack_mi = horizon[&w.build].saturating_sub(w.rank_mi);
                    let c = &cands[choose(&cands, local, w.pred.cost_mi, slack_mi, fastest)];
                    let run_us = c.overhead_us + time_us(w.pred.cost_mi, c.rate);
                    (c.place, run_us)
                })
            } else {
                let threads = width(&w.pred, &w.unit);
                let tokens = held.iter().sum::<usize>() as u64;
                wide.clear();
                if fits {
                    // Its first token from the fastest class with one, or it
                    // waits for the class that frees first.
                    let first_free = frees_in.iter().copied().enumerate().min_by_key(|&(_, t)| t);
                    let (class, start_us) = match self.layout.by_speed().iter().find(|&&k| held[k] > 0) {
                        Some(&k) => (k, 0),
                        None => first_free.unwrap_or((0, 0)),
                    };
                    wide.push(Wide {
                        start_us,
                        run_us: spread_us(&w.pred, fastest, spread_rate, threads.min(tokens.max(1))),
                        capacity: spread_rate * tokens,
                        place: Place::Spread { layout: self.layout.id, class },
                    });
                }
                for (r, rb) in self.remotes.iter().enumerate() {
                    let Some(state) = away.and_then(|p| rb.usable(p)) else { continue };
                    let avail = rb.avail.iter().sum::<u32>() as u64;
                    if avail > 0 {
                        let (fast, rate) = state_rates(&state.classes);
                        wide.push(Wide {
                            start_us: 0,
                            run_us: REMOTE_OVERHEAD_US + spread_us(&w.pred, fast, rate, threads.min(avail)),
                            capacity: rate * avail,
                            place: Place::Remote { remote: r, class: None, shown_in: None },
                        });
                    }
                }
                (!wide.is_empty()).then(|| {
                    let c = &wide[pick_wide(&wide, w.pred.known)];
                    (c.place, c.run_us)
                })
            };
            let Some((place, run_us)) = picked else {
                mem_blocked = true;
                self.starved = true;
                continue;
            };
            if place.here() {
                // Memory is kept for a job waiting for a token too, so it
                // fits when one frees.
                mem_free = mem_free.saturating_sub(w.pred.mem_kb);
                first_here = false;
            }
            match place {
                Place::Remote { remote, class, .. } => {
                    let RemoteBox { state, avail, .. } = &mut self.remotes[remote];
                    match (class, state) {
                        (Some(k), _) => avail[k] -= 1,
                        (None, Some(state)) => take_fastest(avail, &state.classes, 2),
                        (None, None) => (),
                    }
                    granted.push((i, place, run_us));
                }
                Place::Local { class, .. } | Place::Spread { class, .. } if held[class] == 0 => {
                    // Jobs after this one see the queue it joins, so they
                    // don't all count on the same next free slot.
                    let cpus = self.layout.classes[class].cpus.len() as u64;
                    frees_in[class] += run_us / cpus;
                    self.starved = true;
                }
                Place::Local { class, .. } | Place::Spread { class, .. } => {
                    placed_us[class] = placed_us[class].min(run_us);
                    held[class] -= 1;
                    if held[class] == 0 {
                        let placed = placed_us[class];
                        frees_in[class] = self.frees_in_us(class, now).map_or(placed, |t| t.min(placed));
                    }
                    granted.push((i, place, run_us));
                }
            }
        }
        for (fifo, n) in self.layout.fifos.iter().zip(held.iter()) {
            fifo.give(*n);
        }
        granted.sort_unstable_by_key(|&(i, _, _)| std::cmp::Reverse(i));
        for &(i, place, _) in granted.iter() {
            let w = self.queue.swap_remove(i);
            // If the wrapper is gone a slot comes back through Drop.
            match place {
                Place::Local { class, .. } => {
                    let slot = self.admit(w.unit, Some(w.build), w.pred, w.rank_mi, class, now);
                    let _ = w.reply.send(Admission::Slot(slot));
                }
                Place::Spread { class, .. } => {
                    let slot = self.admit_spread(w.unit, Some(w.build), w.pred, w.rank_mi, class, now);
                    let _ = w.reply.send(Admission::Slot(slot));
                }
                Place::Remote { remote, class, .. } => self.send_remote(w, remote, class, now),
            }
        }
    }

    /// What each box has free for jobs from here: the free tokens its state
    /// shows, less those that jobs sent there it doesn't show yet hold.
    fn count_available(&mut self) {
        for rb in &mut self.remotes {
            rb.avail.clear();
            if let Some(state) = &rb.state {
                rb.avail.extend(state.classes.iter().map(|c| c.free));
            }
        }
        for r in &self.running {
            let Place::Remote { remote, class, shown_in } = r.place else { continue };
            let RemoteBox { state: Some(state), avail, .. } = &mut self.remotes[remote] else { continue };
            if shown_in.is_some_and(|v| state.version >= v) {
                continue;
            }
            match class {
                Some(k) => {
                    if let Some(n) = avail.get_mut(k) {
                        *n = n.saturating_sub(1);
                    }
                }
                None => take_fastest(avail, &state.classes, 2),
            }
        }
    }

    fn admit(
        &mut self,
        unit: Unit,
        build: Option<u32>,
        pred: Prediction,
        rank_mi: u64,
        class: usize,
        now: Instant,
    ) -> Slot {
        let job = JobId::new();
        self.layout.running[class] += 1;
        let rate = self.layout.rates[class].get();
        self.running.push(Running {
            job,
            build,
            place: Place::Local { layout: self.layout.id, class },
            rate,
            run_us: time_us(pred.cost_mi, rate),
            mem_kb: pred.mem_kb,
            cost_mi: pred.cost_mi,
            rank_mi,
            start: now,
            pid: None,
            foot_kb: 0,
            peak_kb: 0,
        });
        Slot { grant: self.layout.grants[class].clone(), job, unit, ran: None, tx: self.tx.clone() }
    }

    /// Start a heavy job here on every CPU, holding a token taken from
    /// `class`. If its jobserver can't be made, it runs pinned to `class`.
    fn admit_spread(
        &mut self,
        unit: Unit,
        build: Option<u32>,
        pred: Prediction,
        rank_mi: u64,
        class: usize,
        now: Instant,
    ) -> Slot {
        let job = JobId::new();
        let (fifo_dir, cpus) = (&self.setup.fifo_dir, &self.setup.allowed);
        let grant = match self.layout.spread(job, class, unit.codegen_units, fifo_dir, cpus) {
            Ok(grant) => grant,
            Err(e) => {
                warn!("starting {} on every cpu: {e:#}", unit.exact);
                return self.admit(unit, build, pred, rank_mi, class, now);
            }
        };
        let (fast, rate) = self.layout.spread_rates();
        let threads = width(&pred, &unit).min(cpus.len() as u64);
        self.running.push(Running {
            job,
            build,
            place: Place::Spread { layout: self.layout.id, class },
            rate,
            run_us: spread_us(&pred, fast, rate, threads),
            mem_kb: pred.mem_kb,
            cost_mi: pred.cost_mi,
            rank_mi,
            start: now,
            pid: None,
            foot_kb: 0,
            peak_kb: 0,
        });
        Slot { grant, job, unit, ran: None, tx: self.tx.clone() }
    }

    fn send_remote(&mut self, w: Waiting, remote: usize, class: Option<usize>, now: Instant) {
        let rb = &self.remotes[remote];
        debug!("sending {} to {} class {class:?}", w.unit.exact, rb.name);
        let classes = rb.state.as_ref().map_or(&[][..], |s| &s.classes[..]);
        let (rate, run_us) = match class {
            Some(k) => {
                let rate = classes.get(k).map_or(1, |c| c.rate);
                (rate, time_us(w.pred.cost_mi, rate))
            }
            None => {
                let (fast, rate) = state_rates(classes);
                let cpus: u64 = classes.iter().map(|c| c.cpus as u64).sum();
                (rate, spread_us(&w.pred, fast, rate, width(&w.pred, &w.unit).min(cpus)))
            }
        };
        let job = JobId::new();
        self.running.push(Running {
            job,
            build: Some(w.build),
            place: Place::Remote { remote, class, shown_in: None },
            rate,
            run_us,
            mem_kb: 0,
            cost_mi: w.pred.cost_mi,
            rank_mi: w.rank_mi,
            start: now,
            pid: None,
            foot_kb: 0,
            peak_kb: 0,
        });
        let p = w.portable.expect("remote placement needs a portable job");
        let slot = RemoteSlot {
            jobs: rb.jobs.clone(),
            class: class.map(|k| k as u32),
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
            load.classes.resize(layout.classes.len(), ClassState { rate: 0, cpus: 0, free: 0 });
            let classes = layout.classes.iter().zip(&layout.rates).zip(&layout.fifos);
            for (l, ((c, rate), fifo)) in load.classes.iter_mut().zip(classes) {
                let free = if closed { 0 } else { fifo.free() };
                let now = ClassState { rate: rate.get(), cpus: c.cpus.len() as u32, free };
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
                    cost_mi: info.cost_mi,
                    front_mi: info.front_mi,
                    width_c: info.width_c,
                    wall_ms: info.wall_ms,
                    mem_kb: info.mem_kb,
                    rank_mi: ranks.get(key).copied(),
                }
            })
            .collect();
        units.sort_unstable_by(|a, b| b.rank_mi.cmp(&a.rank_mi).then(b.cost_mi.cmp(&a.cost_mi)));
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
                    let rate = self.layout.rates[k].get();
                    let lent = self.layout.lent(k);
                    ClassStatus { rate, cpus, running, serving, lent, free: fifo.free() }
                })
                .collect(),
            spread: self.running.iter().filter(|r| matches!(r.place, Place::Spread { .. })).fold(
                SpreadStatus { running: 0, serving: 0 },
                |n, r| match r.build {
                    Some(_) => SpreadStatus { running: n.running + 1, ..n },
                    None => SpreadStatus { serving: n.serving + 1, ..n },
                },
            ),
            finishing: self
                .retired
                .iter()
                .map(|l| l.running.iter().sum::<u32>() + l.spread.len() as u32)
                .sum(),
            waiting: self.queue.len() as u32,
            mem_capacity_kb: self.mem.capacity_kb,
            mem_used_kb: self.mem.used_kb,
            mem_growth_kb: self.mem_growth_kb(),
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
        info!("{profile} class {k}: {} cpus, {:.2} G instructions/s", c.cpus.len(), c.rate as f64 / 1000.);
    }
}

/// Somewhere a job could run.
struct Cand {
    /// Instructions per µs.
    rate: u64,
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
/// if it finishes there sooner than on its local choice. `ref_rate` turns
/// slack into time.
fn choose(cands: &[Cand], local: usize, cost_mi: u64, slack_mi: u64, ref_rate: u64) -> usize {
    let finish = |c: &Cand| c.frees_in_us + c.overhead_us + time_us(cost_mi, c.rate);
    let here = &cands[..local];
    let first = here.iter().map(finish).min().unwrap_or(0);
    let deadline = first + time_us(slack_mi, ref_rate);
    let pick = here
        .iter()
        .enumerate()
        .filter(|(_, c)| finish(c) <= deadline)
        .min_by(|(i, a), (j, b)| a.rate.cmp(&b.rate).then(i.cmp(j)))
        .map_or(0, |(i, _)| i);
    let away = cands[local..].iter().enumerate().min_by_key(|(_, c)| finish(c));
    match away {
        Some((j, c)) if finish(c) < finish(&cands[pick]) => local + j,
        _ => pick,
    }
}

/// Somewhere a heavy job could run: this box, or another, on every CPU.
struct Wide {
    /// µs until it could start, and until it would finish after that.
    start_us: u64,
    run_us: u64,
    /// The instructions per µs its free tokens there are worth.
    capacity: u64,
    place: Place,
}

/// Where a heavy job runs: where it finishes soonest. With no history to
/// predict by, this box if it has a token free, else the other box with
/// the most free capacity, else this box when a token frees.
fn pick_wide(cands: &[Wide], known: bool) -> usize {
    if known {
        return cands.iter().enumerate().min_by_key(|(_, c)| c.start_us + c.run_us).map_or(0, |(i, _)| i);
    }
    let here = cands.iter().position(|c| c.place.here());
    if let Some(i) = here.filter(|&i| cands[i].start_us == 0) {
        return i;
    }
    let away = cands.iter().enumerate().filter(|(_, c)| !c.place.here());
    away.max_by_key(|(_, c)| c.capacity).map(|(i, _)| i).or(here).unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{protocol::Front, toolchain::Toolchain};
    use anyhow::bail;
    use netidx_core::pack::Pack;
    use std::{
        fs::{File, OpenOptions},
        io::{Read, Write},
        os::unix::fs::OpenOptionsExt,
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    /// The rate at which a million instructions take `calib_us` / 1000 µs.
    fn rate(calib_us: u64) -> u64 {
        1_000_000_000 / calib_us
    }

    fn class(calib_us: u64, cpus: &[u32]) -> Class {
        Class { rate: rate(calib_us), cpus: cpus.to_vec() }
    }

    fn unit(name: &str, deps: &[&str]) -> Unit {
        Unit {
            exact: ArcStr::from(name),
            coarse: ArcStr::from(name),
            id: ArcStr::from(name),
            deps: deps.iter().map(|d| crate::protocol::Dep { id: ArcStr::from(*d), file: None }).collect(),
            codegen_units: 16,
        }
    }

    fn local(calib_us: u64, frees_in_us: u64) -> Cand {
        let place = Place::Local { layout: LayoutId::new(), class: 0 };
        Cand { rate: rate(calib_us), frees_in_us, overhead_us: 0, place }
    }

    #[test]
    fn choose_spends_slack_on_slower_classes() {
        // 1000 mcu takes 1, 2 or 4 ms. All free.
        let free = [local(1000, 0), local(2000, 0), local(4000, 0)];
        assert_eq!(choose(&free, 3, 1000, 0, rate(1000)), 0);
        assert_eq!(choose(&free, 3, 1000, 1000, rate(1000)), 1);
        assert_eq!(choose(&free, 3, 1000, 3000, rate(1000)), 2);
        // The fast class frees in 5 ms: with no slack, start now on the middle one.
        assert_eq!(choose(&[local(1000, 5000), local(2000, 0), local(4000, 0)], 3, 1000, 0, rate(1000)), 1);
        // Plenty of slack: wait 1 ms for the slow class rather than take a free fast one.
        let slow_frees_soon = [local(1000, 0), local(2000, 0), local(4000, 1000)];
        assert_eq!(choose(&slow_frees_soon, 3, 1000, 10_000, rate(1000)), 2);
        let remote = |calib_us| {
            let mut c = local(calib_us, 0);
            c.overhead_us = 200;
            c
        };
        // A remote class no faster than the local one loses to its overhead.
        assert_eq!(choose(&[local(1000, 0), remote(1000)], 1, 1000, 0, rate(1000)), 0);
        // Slack keeps a job on a slow local class, but another box that
        // finishes it sooner wins anyway: it costs this box nothing.
        assert_eq!(choose(&[local(1000, 0), local(4000, 0), remote(1000)], 2, 1000, 10_000, rate(1000)), 2);
    }

    #[test]
    fn heavy_jobs_go_where_they_finish_first_or_where_tokens_are_free() {
        let wide = |start_us, run_us, capacity, here: bool| Wide {
            start_us,
            run_us,
            capacity,
            place: if here {
                Place::Spread { layout: LayoutId::new(), class: 0 }
            } else {
                Place::Remote { remote: 0, class: None, shown_in: None }
            },
        };
        // No history: a free token here, else the box with the most free capacity.
        assert_eq!(pick_wide(&[wide(0, 9000, 10, true), wide(0, 10, 100, false)], false), 0);
        let waiting = [wide(500, 10, 0, true), wide(0, 10, 10, false), wide(0, 10, 100, false)];
        assert_eq!(pick_wide(&waiting, false), 2);
        assert_eq!(pick_wide(&[wide(500, 10, 0, true)], false), 0, "it waits here");
        // History: wherever it finishes first.
        assert_eq!(pick_wide(&[wide(0, 9000, 10, true), wide(0, 1000, 100, false)], true), 1);
        assert_eq!(pick_wide(&[wide(0, 900, 10, true), wide(0, 1000, 100, false)], true), 0);
    }

    struct Harness {
        tx: mpsc::UnboundedSender<Msg>,
        dir: PathBuf,
        load: watch::Receiver<Load>,
    }

    /// A scheduler in profile `default` with `classes`, job history from
    /// `costs`, and graph edges from `edges`, with its state in `dir`.
    /// Box memory and tokens per CPU for a test scheduler.
    struct Room {
        memory: Box<dyn Fn() -> Result<BoxMemory> + Send + Sync>,
        tokens_per_cpu: f64,
    }

    /// A box with `capacity_kb` that nothing else uses, and a token per CPU.
    fn room(capacity_kb: u64) -> Room {
        Room { memory: Box::new(move || Ok(BoxMemory { capacity_kb, used_kb: 0 })), tokens_per_cpu: 1. }
    }

    fn sched(
        dir: &Path,
        classes: Vec<Class>,
        room: Room,
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
            memory: room.memory,
            tokens_per_cpu: room.tokens_per_cpu,
            calibration_seconds: 4,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let (load_tx, load) = watch::channel(Load::default());
        let mut sched = Sched::new(setup, Profile::DEFAULT, load_tx, tx)?;
        for &(name, cost_mi, mem_kb) in costs {
            for _ in 0..5 {
                let origin = Origin::Here;
                let sample = Sample { cost_mi, front_mi: None, width_c: 100, wall_ms: 1, mem_kb, origin };
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
            capacity_kb: u64,
            costs: &[(&str, u64, u64)],
            edges: &[(&str, &[&str])],
        ) -> Result<Self> {
            Self::start_with(name, classes, room(capacity_kb), costs, edges)
        }

        fn start_with(
            name: &str,
            classes: Vec<Class>,
            room: Room,
            costs: &[(&str, u64, u64)],
            edges: &[(&str, &[&str])],
        ) -> Result<Self> {
            let dir = scratch(name);
            let (sched, rx, load) = sched(&dir, classes, room, costs, edges)?;
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

        fn offer(&self, name: &str, class: Option<u32>) -> oneshot::Receiver<Answer> {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Offer { unit: unit(name, &[]), class, reply }).unwrap();
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
        let classes = vec![ClassState { rate: rate(calib_us), cpus, free }];
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
        let answer = h.offer("r", Some(0)).await?;
        let served = answer.slot.expect("a free slot");
        let st = h.status().await;
        assert_eq!((st.classes[0].running, st.classes[0].serving), (0, 1));
        let shown = h.load.borrow().clone();
        assert_eq!(shown.version, answer.version, "the publish after the answer has its version");
        assert_eq!(shown.classes[0].free, 0);
        let mut local = h.submit("l");
        assert!(pending(&mut local).await);
        let refused = h.offer("r", Some(0)).await?;
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
    async fn memory_other_processes_use_holds_jobs_back() -> Result<()> {
        let others = std::sync::Arc::new(AtomicU64::new(800));
        let used = others.clone();
        let memory =
            Box::new(move || Ok(BoxMemory { capacity_kb: 1000, used_kb: used.load(Ordering::Relaxed) }));
        let costs = [("small", 10, 100), ("big", 10, 600)];
        let room = Room { memory, tokens_per_cpu: 1. };
        let h = Harness::start_with("others", vec![class(1000, &[0, 1, 2, 3])], room, &costs, &[])?;
        let small = slot(h.submit("small")).await?;
        let mut big = h.submit("big");
        assert!(pending(&mut big).await, "800 used by others and 100 by small leave 100");
        // Nothing here changes; the box is measured again while big waits.
        others.store(0, Ordering::Relaxed);
        slot(big).await?;
        drop(small);
        Ok(())
    }

    #[tokio::test]
    async fn local_memory_does_not_hold_back_a_job_that_can_go_elsewhere() -> Result<()> {
        let costs = [("fat", 10, 900), ("big", 5_000_000, 400)];
        let h = Harness::start("elsewhere", vec![class(1000, &[0, 1])], 1000, &costs, &[])?;
        h.farm(remote_box(200, 4, 4, 1));
        let fat = slot(h.submit("fat")).await?;
        assert!(matches!(admitted(h.submit_with("big", want())).await?, Admission::Remote(_)));
        let mut stays = h.submit("big");
        assert!(pending(&mut stays).await, "it can't go elsewhere and doesn't fit here");
        drop(fat);
        slot(stays).await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_job_records_the_most_memory_seen_while_it_ran() -> Result<()> {
        let h = Harness::start("peak", vec![class(1000, &[0])], u64::MAX, &[], &[])?;
        let mut child = Command::new("sleep").arg("10").spawn()?;
        let job = slot(h.submit("a")).await?;
        job.started(child.id());
        time::sleep(MEASURE * 2).await;
        let foot = sys::tree_footprint_kb(child.id());
        // maxrss says 1 kB, as it would for a job swapped out at its peak.
        job.finish(Ran { cpu_us: 1000, wall_us: 1000, maxrss_kb: 1, instructions: None, front: None });
        let st = h.status().await;
        child.kill()?;
        child.wait()?;
        let a = st.units.iter().find(|u| u.key == "a").unwrap();
        assert!(a.mem_kb >= foot / 2 && foot > 0, "recorded {} kB for {foot} kB", a.mem_kb);
        Ok(())
    }

    #[tokio::test]
    async fn free_memory_counts_only_what_running_jobs_will_add() -> Result<()> {
        let dir = scratch("free");
        let costs = [("a", 10, 600)];
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room(10_000), &costs, &[])?;
        s.measure(Instant::now());
        let u = unit("a", &[]);
        let pred = s.stats.predict(&u.exact, &u.coarse);
        let p = pred.mem_kb;
        let slot = s.admit(u.clone(), Some(1), pred, 10, 0, Instant::now());
        assert_eq!(s.mem_free_kb(), 10_000 - p, "nothing sampled yet: all of it is to come");
        // The box's use includes what the job holds, so only the rest is to come.
        (s.running[0].foot_kb, s.mem.used_kb) = (p / 2, p / 2);
        assert_eq!(s.mem_free_kb(), 10_000 - p);
        (s.running[0].foot_kb, s.mem.used_kb) = (p + 300, p + 300);
        assert_eq!(s.mem_free_kb(), 10_000 - p - 300, "past its prediction");
        // Its footprint leaves the box's use with it, before the next sample.
        let job = s.running[0].job;
        s.finished(job, &u, None);
        assert_eq!(s.mem_free_kb(), 10_000);
        drop((slot, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_run_is_recorded_as_work_front_end_and_width() -> Result<()> {
        let dir = scratch("record");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        let u = unit("a", &[]);
        // 4 G instructions in 2 s of CPU over 1 s; the front end took 0.4 s
        // of each, so the rest kept 1.6 s of CPU busy in 0.6 s.
        let front = Some(Front { wall_us: 400_000, cpu_us: 400_000 });
        let instructions = Some(4_000_000_000);
        let ran = Ran { cpu_us: 2_000_000, wall_us: 1_000_000, maxrss_kb: 1, instructions, front };
        s.record(&u, &ran, 1, 0, Origin::Here);
        let info = |s: &Sched, key: &str| s.stats.units().find(|(k, _)| k.as_str() == key).unwrap().1;
        let near = |v: u64, want: u64| v.abs_diff(want) * 100 <= want;
        let a = info(&s, "a");
        let front_ok = a.front_mi.is_some_and(|f| near(f, 800));
        assert!(near(a.cost_mi, 4000) && front_ok, "{:?}", (a.cost_mi, a.front_mi));
        assert!(near(a.width_c, 266), "{}", a.width_c);
        // Without a count, work is CPU time at the class's rate.
        let ran = Ran { instructions: None, front: None, ..ran };
        s.record(&unit("b", &[]), &ran, 3000, 0, Origin::Here);
        let b = info(&s, "b");
        assert!(near(b.cost_mi, 6000) && near(b.width_c, 200), "{:?}", (b.cost_mi, b.width_c));
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn class_rates_learn_from_the_jobs_that_ran_there() -> Result<()> {
        let h = Harness::start("rates", vec![class(1000, &[0])], u64::MAX, &[("a", 10, 1)], &[])?;
        let seeded = h.status().await.classes[0].rate;
        assert_eq!(seeded, rate(1000));
        // Ten minutes of CPU at 3000 instructions per µs outweighs the seed.
        let job = slot(h.submit("a")).await?;
        let cpu_us = 600_000_000;
        let instructions = Some(cpu_us * 3000);
        job.finish(Ran { cpu_us, wall_us: cpu_us, maxrss_kb: 1, instructions, front: None });
        let learned = h.status().await.classes[0].rate;
        assert!(learned.abs_diff(3000) < 30, "{learned}");
        Ok(())
    }

    /// Five runs of a unit `width_c` hundredths of a thread wide.
    fn history(s: &mut Sched, name: &str, cost_mi: u64, width_c: u64) {
        for _ in 0..5 {
            let origin = Origin::Here;
            let sample = Sample { cost_mi, front_mi: None, width_c, wall_ms: 1, mem_kb: 1, origin };
            s.stats.record(&unit(name, &[]), sample);
        }
    }

    /// Submit jobs and place them in one dispatch, as if they had waited
    /// out the gathering window.
    fn place(s: &mut Sched, names: &[&str], portable: bool) -> Vec<oneshot::Receiver<Admission>> {
        let replies = names
            .iter()
            .map(|name| {
                let (reply, rx) = oneshot::channel();
                s.submit(unit(name, &[]), 1, portable.then(|| want().unwrap()), reply);
                rx
            })
            .collect();
        for w in &mut s.queue {
            w.since -= GATHER;
        }
        s.dispatch();
        replies
    }

    fn granted(rx: &mut oneshot::Receiver<Admission>) -> Slot {
        match rx.try_recv() {
            Ok(Admission::Slot(slot)) => slot,
            _ => panic!("no slot"),
        }
    }

    #[tokio::test]
    async fn light_jobs_are_pinned_and_heavy_ones_spread() -> Result<()> {
        let dir = scratch("spread");
        let classes = vec![class(1000, &[0, 1]), class(2000, &[2, 3])];
        let (mut s, _rx, _load) = sched(&dir, classes, room(u64::MAX), &[], &[])?;
        history(&mut s, "narrow", 1000, 100);
        history(&mut s, "wide", 1000, 400);
        let mut rx = place(&mut s, &["narrow", "wide", "never-run"], false);
        let slots: Vec<Slot> = rx.iter_mut().map(granted).collect();
        assert_eq!(slots[0].grant.cpus, vec![0, 1], "pinned to the fast class");
        for heavy in &slots[1..] {
            assert_eq!(heavy.grant.cpus, vec![0, 1, 2, 3]);
            assert!(heavy.grant.jobserver.contains("/spread-"), "{}", heavy.grant.jobserver);
        }
        drop((slots, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn heavy_jobs_borrow_tokens_as_they_take_them_and_give_back_what_they_leave() -> Result<()> {
        let dir = scratch("lend");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        let slot = granted(&mut place(&mut s, &["h"], false)[0]);
        let mut fifo = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(slot.grant.jobserver.as_str())?;
        let tokens = |s: &Sched| (s.layout.fifos[0].free(), s.layout.spread[0].fifo.free());
        assert_eq!(tokens(&s), (2, 1), "one held without reading, and a spare");
        // Codegen starts: it takes the spare and is lent twice what it holds.
        fifo.read_exact(&mut [0u8; 1])?;
        s.layout.lend();
        assert_eq!(tokens(&s), (0, 2));
        fifo.read_exact(&mut [0u8; 2])?;
        s.layout.lend();
        assert_eq!(s.layout.lent(0), 4, "the class had no more");
        // Three threads finish. Spares left unused a whole tick go back, but one.
        fifo.write_all(b"+++")?;
        s.layout.lend();
        assert_eq!(tokens(&s), (0, 3), "not a whole tick yet");
        s.layout.lend();
        assert_eq!(tokens(&s), (2, 1));
        // When it ends, everything it borrowed goes back.
        let path = PathBuf::from(slot.grant.jobserver.as_str());
        s.finished(slot.job, &slot.unit, None);
        assert_eq!(s.layout.fifos[0].free(), 4);
        assert!(!path.exists());
        drop((slot, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn refilling_a_class_leaves_out_tokens_lent_to_heavy_jobs() -> Result<()> {
        let dir = scratch("refill");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        history(&mut s, "narrow", 10, 100);
        let mut rx = place(&mut s, &["h", "narrow"], false);
        let (heavy, narrow) = (granted(&mut rx[0]), granted(&mut rx[1]));
        assert_eq!((s.layout.lent(0), s.layout.fifos[0].free()), (2, 1));
        // The class's last light job ends, so its jobserver is refilled, but
        // only with the tokens the heavy job doesn't hold.
        s.finished(narrow.job, &narrow.unit, None);
        assert_eq!(s.layout.fifos[0].free(), 2);
        drop((heavy, narrow, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn classes_hold_tokens_per_cpu() -> Result<()> {
        let dir = scratch("per-cpu");
        let room = Room { tokens_per_cpu: 1.5, ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1])], room, &[], &[])?;
        history(&mut s, "narrow", 10, 100);
        let mut rx = place(&mut s, &["narrow"; 4], false);
        let admitted: Vec<Slot> = rx[..3].iter_mut().map(granted).collect();
        assert!(rx[3].try_recv().is_err(), "two cpus hold three tokens");
        drop((admitted, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_wide_job_goes_to_the_box_with_the_most_capacity() -> Result<()> {
        let dir = scratch("capacity");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1])], room(u64::MAX), &[], &[])?;
        // b has 32 cpus each twice as fast as this box's 2.
        s.farm("b".into(), Some(remote_box(500, 32, 32, 1)));
        history(&mut s, "wide", 5_000_000, 1600);
        history(&mut s, "narrow", 5_000_000, 100);
        let mut rx = place(&mut s, &["wide", "narrow"], true);
        let class = |rx: &mut oneshot::Receiver<Admission>| match rx.try_recv() {
            Ok(Admission::Remote(r)) => Some(r.class),
            _ => None,
        };
        assert_eq!(class(&mut rx[0]), Some(None), "on every cpu there");
        assert_eq!(class(&mut rx[1]), Some(Some(0)), "pinned to a class there");
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn another_box_can_offer_a_job_for_every_cpu() -> Result<()> {
        let classes = vec![class(1000, &[0, 1]), class(2000, &[2, 3])];
        let h = Harness::start("offer-spread", classes, u64::MAX, &[], &[])?;
        let slot = h.offer("r", None).await?.slot.expect("a free token");
        assert_eq!(slot.grant.cpus, vec![0, 1, 2, 3]);
        assert!(slot.grant.jobserver.contains("/spread-"));
        let st = h.status().await;
        assert_eq!(st.spread.serving, 1);
        assert_eq!((st.classes[0].lent, st.classes[1].lent), (2, 0), "both from the fast class");
        drop(slot);
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
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &costs, &[])?;
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
        let ran = || Ran { cpu_us: 1000, wall_us: 1000, maxrss_kb: 1, instructions: None, front: None };
        h.offer("theirs", Some(0)).await?.slot.expect("a free slot").finish(ran());
        slot(h.submit("mine")).await?.finish(ran());
        h.farm(remote_box(200, 4, 4, 1));
        let Admission::Remote(r) = admitted(h.submit_with("big", want())).await? else {
            bail!("not sent remote");
        };
        r.finish(RemoteOutcome::Done(ran()));
        let st = h.status().await;
        let runs = |key: &str| {
            let u = st.units.iter().find(|u| u.key == key).unwrap();
            (u.here, u.sent, u.served, u.rank_mi.is_some())
        };
        assert_eq!(runs("theirs"), (0, 0, 1, false), "not in this box's graph");
        assert_eq!(runs("mine"), (1, 0, 0, true));
        assert_eq!(runs("big"), (5, 1, 0, true));
        Ok(())
    }

    #[tokio::test]
    async fn leaked_tokens_are_refilled_when_the_class_is_idle() -> Result<()> {
        let h = Harness::start("leak", vec![class(1000, &[0, 1])], u64::MAX, &[("a", 10, 1)], &[])?;
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
        let h = Harness::start("switch", vec![class(1000, &[0])], u64::MAX, &[("a", 10, 1)], &[])?;
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
