use crate::{
    blobs::{Hash, Hashes},
    calibrate,
    classes::{self, Class, Profile},
    farm::{BoxState, ClassState, Load},
    graph::Graph,
    protocol::{
        BoxClass, BoxStatus, ClassStatus, Measured, Ran, Reads, Reply, SpreadStatus, Status, Switching,
        Unit, UnitStatus, frame,
    },
    remote::{self, CrateFiles},
    stats::{self, Basis, Decay, Moving, Origin, PlaceId, Prediction, Run, Stats, Transfer},
    sys::{self, BoxMemory},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use compact_str::{CompactString, format_compact};
use log::{Level, debug, error, info, log_enabled, warn};
use netidx::path::Path as NPath;
use netidx_core::atomic_id;
use poolshark::local::LPooled;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    ffi::CString,
    fmt::Write as _,
    fs, io, mem,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::{
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
/// While a local class has a free slot, only jobs predicted to take this
/// long on the fastest local class are worth sending to another box.
const REMOTE_MIN_US: u64 = 1_000_000;
/// While jobs run here, count and lend each one's jobserver tokens this
/// often: codegen threads take and return them without telling the
/// scheduler.
const SAMPLE: Duration = Duration::from_millis(100);
/// A unit that failed remotely and then built locally this many times is
/// kept local.
const PIN_AFTER: u8 = 2;
/// Sample the box's memory, its load and the footprints of jobs running
/// here this often. Placing a job samples again if the last sample is
/// older.
const MEASURE: Duration = Duration::from_millis(250);
/// A job measured to hold fewer than this many hundredths of a token on
/// average after its front end is light: it runs pinned to one class.
/// Every other job, including one never measured, is heavy and may run on
/// every CPU.
const LIGHT_WIDTH_C: u64 = 150;
/// A box this box has sent nothing to for this long, while jobs wait here,
/// gets a heartbeat at most this often.
const HEARTBEAT_EVERY: Duration = Duration::from_secs(60);
/// A change in load this large, in hundredths of a task per CPU, is
/// published even if nothing else changed.
const LOAD_STEP_C: u32 = 25;
/// A class's next token, once predicted to free this share of the time
/// later or sooner than clients count down to, and at least
/// `FREES_IN_STEP_MS`, is published even if nothing else changed.
const FREES_IN_SHARE: f64 = 0.25;
const FREES_IN_STEP_MS: u64 = 250;
/// A sample of the box's runnable tasks counts `1/e` as much after this
/// long: long enough that one sample doesn't swing placement, short
/// enough to follow test runs starting and stopping.
const LOAD_TAU: Duration = Duration::from_secs(10);

atomic_id!(LayoutId);
atomic_id!(JobId);

/// What a job needs to run on another box: a toolchain, how it reads the
/// crates below it, and the directory their paths are relative to.
#[derive(Clone)]
pub struct Want {
    pub key: ArcStr,
    pub target: Option<ArcStr>,
    pub reads: Reads,
    pub cwd: ArcStr,
}

pub enum Msg {
    /// `remote` is where it may go besides here, or why nowhere.
    Submit { unit: Unit, build: u32, remote: Result<Want, &'static str>, reply: oneshot::Sender<Admission> },
    /// With how long its linker waited for files still on their way.
    Finished { job: JobId, unit: Unit, ran: Option<(Ran, u64)> },
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
    /// A heartbeat moved `kb` to the box `name` in `us`.
    Heartbeat { name: ArcStr, kb: u64, us: u64 },
    Drain,
}

/// A heartbeat for the daemon to send: `kb` to the box `name` listening at
/// `jobs`.
pub struct Heartbeat {
    pub name: ArcStr,
    pub jobs: NPath,
    pub kb: u64,
}

pub enum Admission {
    Slot(Slot),
    Remote(RemoteSlot),
    Restarting,
    /// The daemon can't run the job, and says why.
    Refused(ArcStr),
}

/// A job's cpus and jobserver, and the framed `Reply::Slot` for them.
pub struct Grant {
    pub frame: Bytes,
    pub cpus: Vec<u32>,
    pub jobserver: ArcStr,
}

/// What the scheduler measures of a job while its rustc runs here: the
/// jobserver tokens it holds over time, and the box's load.
pub struct Meter {
    start: Instant,
    /// Token-µs counted so far, the tokens held since, and when that was,
    /// in µs after `start`.
    tokens_us: AtomicU64,
    held: AtomicU32,
    counted_us: AtomicU64,
    /// The sum and count of load samples since its rustc started, and the
    /// latest sample, which stands for a job too short to be sampled.
    load_c: AtomicU64,
    samples: AtomicU32,
    last_c: AtomicU32,
}

impl Meter {
    /// A job holding the one token it starts with, on a box under `load_c`.
    fn new(load_c: u32, now: Instant) -> Self {
        Self {
            start: now,
            tokens_us: AtomicU64::new(0),
            held: AtomicU32::new(1),
            counted_us: AtomicU64::new(0),
            load_c: AtomicU64::new(0),
            samples: AtomicU32::new(0),
            last_c: AtomicU32::new(load_c),
        }
    }

    fn at_us(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.start).as_micros() as u64
    }

    /// Its rustc started: count tokens and load from now.
    fn restart(&self, now: Instant) {
        self.counted_us.store(self.at_us(now), Ordering::Relaxed);
        self.tokens_us.store(0, Ordering::Relaxed);
        self.load_c.store(0, Ordering::Relaxed);
        self.samples.store(0, Ordering::Relaxed);
    }

    /// It holds `held` tokens from `now` on.
    fn hold(&self, held: u32, now: Instant) {
        let at = self.at_us(now);
        let since = at.saturating_sub(self.counted_us.swap(at, Ordering::Relaxed));
        self.tokens_us.fetch_add(self.held.swap(held, Ordering::Relaxed) as u64 * since, Ordering::Relaxed);
    }

    fn sample(&self, load_c: u32) {
        self.load_c.fetch_add(load_c as u64, Ordering::Relaxed);
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.last_c.store(load_c, Ordering::Relaxed);
    }

    fn measured(&self, now: Instant) -> Measured {
        let since = self.at_us(now).saturating_sub(self.counted_us.load(Ordering::Relaxed));
        let tail = self.held.load(Ordering::Relaxed) as u64 * since;
        let load_c = match self.samples.load(Ordering::Relaxed) {
            0 => self.last_c.load(Ordering::Relaxed),
            n => (self.load_c.load(Ordering::Relaxed) / n as u64) as u32,
        };
        Measured { tokens_us: self.tokens_us.load(Ordering::Relaxed) + tail, load_c }
    }
}

/// Held while rustc runs on this box. Dropping it frees the slot.
pub struct Slot {
    pub grant: Arc<Grant>,
    job: JobId,
    unit: Unit,
    ran: Option<(Ran, u64)>,
    meter: Arc<Meter>,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Slot {
    /// The job's processes are `pid` and its descendants, and its rustc
    /// starts now.
    pub fn started(&self, pid: u32) {
        self.meter.restart(Instant::now());
        let _ = self.tx.send(Msg::Started { job: self.job, pid });
    }

    /// Its rustc succeeded. `linker_waited_us` is how long its linker
    /// waited for files still on their way.
    pub fn finish(mut self, ran: Ran, linker_waited_us: u64) {
        self.ran = Some((ran, linker_waited_us));
    }

    /// What the scheduler has measured of the job so far.
    pub fn measured(&self) -> Measured {
        self.meter.measured(Instant::now())
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

/// A job that ran on another box: what that box measured, how long its
/// linker waited for files, what moving them cost, and the crate files the
/// box holds after it, by hash with their size in kB.
pub struct SentRun {
    pub ran: Ran,
    pub measured: Measured,
    pub linker_waited_us: u64,
    pub transfer: Transfer,
    pub kept: Vec<(Hash, u64)>,
}

pub enum RemoteOutcome {
    Done(SentRun),
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

    /// A slot on the box at `jobs` for a job of `unit` that reads no
    /// crates, which tells `tx` how it went.
    #[cfg(test)]
    pub fn for_test(jobs: NPath, key: ArcStr, unit: Unit, tx: mpsc::UnboundedSender<Msg>) -> Self {
        Self { jobs, class: None, key, target: None, crates: Vec::new(), job: JobId::new(), unit, outcome: None, tx }
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

/// A job's make-style jobserver FIFO, which the daemon writes the tokens
/// it lends the job into.
struct Fifo {
    fd: OwnedFd,
    path: PathBuf,
}

impl Fifo {
    fn create(path: PathBuf) -> Result<Self> {
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
            let e = io::Error::last_os_error();
            let _ = fs::remove_file(&path);
            return Err(e).with_context(|| format_compact!("opening {}", path.display()));
        }
        Ok(Self { fd: unsafe { OwnedFd::from_raw_fd(fd) }, path })
    }

    /// Take up to `max` tokens without blocking.
    fn take(&self, max: u32) -> u32 {
        let mut buf = [0u8; 64];
        let mut got = 0;
        while got < max {
            let want = ((max - got) as usize).min(buf.len());
            let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), want) };
            if n <= 0 {
                break;
            }
            got += n as u32;
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

    fn give(&self, mut n: u32) {
        const TOKENS: [u8; 64] = [b'+'; 64];
        while n > 0 {
            let w = unsafe {
                libc::write(self.fd.as_raw_fd(), TOKENS.as_ptr().cast(), (n as usize).min(TOKENS.len()))
            };
            if w <= 0 {
                warn!("jobserver write failed: {}", io::Error::last_os_error());
                return;
            }
            n -= w as u32;
        }
    }
}

impl Drop for Fifo {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// A spare token in a job's jobserver that no class's token backs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Credit {
    None,
    /// In its jobserver, perhaps still unread.
    Spare,
    /// Its rustc took it before a class had a token to back it.
    Owed,
}

/// A job's jobserver, which the daemon lends tokens into: from its class
/// for a light job, from every class for a heavy one. `lent` counts them by
/// class, including the token the job holds without reading one. Rustc
/// reads a token when it wants another thread, which the daemon can't see,
/// so a spare in its jobserver is how it asks. The spare is lent on credit
/// and backed once taken, so a job that never asks ties up no token.
struct Lease {
    job: JobId,
    fifo: Fifo,
    lent: Vec<u32>,
    credit: Credit,
    /// The class a light job is pinned to; none for a heavy job.
    pinned: Option<usize>,
    /// The most tokens it may hold: its codegen units.
    cap: u32,
    /// Its jobserver held a token after the last tick.
    offered: bool,
    /// Its jobserver held a spare at the last tick.
    idle: bool,
    meter: Arc<Meter>,
}

impl Lease {
    /// Tokens its rustc holds, the one it started with included, when its
    /// jobserver holds `free`.
    fn in_use(&self, free: u32) -> u32 {
        let credit = u32::from(self.credit != Credit::None);
        (self.lent.iter().sum::<u32>() + credit).saturating_sub(free)
    }

    /// Tell its meter how many tokens it holds now.
    fn count(&self, now: Instant) {
        self.meter.hold(self.in_use(self.fifo.free()), now);
    }

    /// One tick: count its tokens; back a credit it took; if it took every
    /// token offered, lend it twice what it holds; take back spares left
    /// unused a whole tick, all but one, which stays on credit; and offer a
    /// spare on credit if it has none and `order`'s classes have a token.
    fn tick(&mut self, pool: &mut [u32], order: &[usize], now: Instant) {
        self.count(now);
        let mut free = self.fifo.free();
        let in_use = self.in_use(free);
        if self.credit == Credit::Owed && free > 0 {
            // A token it gave back pays what it owes.
            free -= self.fifo.take(1);
            self.credit = Credit::None;
        }
        let took_all = free == 0 && self.offered;
        if free == 0 && self.credit == Credit::Spare {
            self.credit = Credit::Owed;
        }
        if self.credit == Credit::Owed && borrow(pool, order, 1, &mut self.lent) == 1 {
            self.credit = Credit::None;
        }
        if took_all {
            self.idle = false;
            let got = borrow(pool, order, in_use.min(self.cap.saturating_sub(in_use)), &mut self.lent);
            self.fifo.give(got);
        } else if free > 0 {
            if self.idle {
                // The token left in its jobserver stays as a spare on credit,
                // so every token behind the spares it left goes back.
                let spare = u32::from(self.credit == Credit::Spare);
                self.fifo.take(free - 1);
                repay(pool, order, free - spare, &mut self.lent);
                self.credit = Credit::Spare;
            }
            self.idle = true;
        }
        let asking = in_use < self.cap && order.iter().any(|&k| pool[k] > 0);
        if self.fifo.free() == 0 && self.credit == Credit::None && asking {
            self.fifo.give(1);
            self.credit = Credit::Spare;
        }
        self.offered = self.fifo.free() > 0;
    }
}

/// Take up to `want` tokens from `pool` in `order`, counting them in `lent`.
fn borrow(pool: &mut [u32], order: &[usize], want: u32, lent: &mut [u32]) -> u32 {
    let mut got = 0;
    for &k in order {
        let n = (want - got).min(pool[k]);
        pool[k] -= n;
        lent[k] += n;
        got += n;
    }
    got
}

/// Give `n` tokens counted in `lent` back to `pool`, in reverse `order`.
fn repay(pool: &mut [u32], order: &[usize], mut n: u32, lent: &mut [u32]) {
    for &k in order.iter().rev() {
        let r = n.min(lent[k]);
        pool[k] += r;
        lent[k] -= r;
        n -= r;
    }
}

/// Where a box's jobs run, in the cost table: each class, for light jobs,
/// and all its CPUs, for heavy ones.
struct Places {
    classes: Vec<PlaceId>,
    all: PlaceId,
}

impl Places {
    /// `name`'s places in `profile`, whose classes are calibrated at
    /// `rates` instructions per µs with `cpus` CPUs each.
    fn of(stats: &mut Stats, name: &str, profile: &str, classes: impl Iterator<Item = (u64, u64)> + Clone) -> Self {
        let (fast, mean) = spread_rates(classes.clone());
        let classes = classes
            .enumerate()
            .map(|(k, (rate, _))| stats.place(&format_compact!("{name}/{profile}/{k}"), rate, rate))
            .collect();
        let all = stats.place(&format_compact!("{name}/{profile}/all"), fast, mean);
        Self { classes, all }
    }
}

/// One profile's classes, their tokens, and the jobs holding them. A
/// profile switch makes a new layout for new jobs; the old one lives until
/// its jobs finish, and lends them nothing more.
struct Layout {
    id: LayoutId,
    /// Fastest at calibration first.
    classes: Vec<Class>,
    /// Each class's tokens that no job holds.
    pool: Vec<u32>,
    /// Each class's tokens that jobs of earlier layouts, on the same CPUs,
    /// still hold, and what they hold beyond every class's tokens. Tokens
    /// those jobs give back pay `overdrawn`, then join `pool`.
    owed: Vec<u32>,
    overdrawn: u32,
    places: Places,
    leases: Vec<Lease>,
}

impl Layout {
    fn new(profile: &Profile, classes: Vec<Class>, tokens_per_cpu: f64, stats: &mut Stats) -> Self {
        let id = LayoutId::new();
        let pool = classes.iter().map(|c| (c.cpus.len() as f64 * tokens_per_cpu).round().max(1.) as u32).collect();
        let rates = classes.iter().map(|c| (c.rate, c.cpus.len() as u64));
        let places = Places::of(stats, "here", profile.name(), rates);
        for (k, c) in classes.iter().enumerate() {
            let factor = stats.factor(places.classes[k], 0);
            let rate = c.rate as f64 / 1000.;
            info!("{profile} class {k}: {} cpus, {rate:.2} G instructions/s calibrated, factor {factor:.2} idle", c.cpus.len());
        }
        let owed = vec![0; classes.len()];
        Self { id, pool, owed, overdrawn: 0, classes, places, leases: Vec::new() }
    }

    /// Jobs of earlier layouts hold `n` tokens on this layout's CPUs: take
    /// them from the slowest classes first, until those jobs give them back.
    fn owe(&mut self, mut n: u32) {
        let order = self.by_speed();
        for &k in order.iter().rev() {
            let t = n.min(self.pool[k]);
            self.pool[k] -= t;
            self.owed[k] += t;
            n -= t;
        }
        self.overdrawn += n;
    }

    /// Jobs of earlier layouts gave back `n` tokens: they pay what is
    /// overdrawn, then each class what it is owed, fastest first.
    fn repaid(&mut self, mut n: u32) {
        let t = n.min(self.overdrawn);
        self.overdrawn -= t;
        n -= t;
        let order = self.by_speed();
        for &k in order.iter() {
            let t = n.min(self.owed[k]);
            self.owed[k] -= t;
            self.pool[k] += t;
            n -= t;
        }
        if n > 0 {
            warn!("{n} tokens came back from jobs of an earlier profile that no class lent them");
        }
    }

    fn idle(&self) -> bool {
        self.leases.is_empty()
    }

    fn cpus(&self) -> u64 {
        self.classes.iter().map(|c| c.cpus.len() as u64).sum()
    }

    /// A unit's prediction where it runs fastest here, on the fastest
    /// class if it is `pinned` (light), else on every CPU, and the spot it
    /// takes.
    fn fastest(&self, stats: &Stats, exact: &ArcStr, coarse: &ArcStr, pinned: bool) -> (Prediction, Spot) {
        let at = if pinned { self.places.classes[self.by_speed()[0]] } else { self.places.all };
        let (pred, _) = stats.predict_at(exact, coarse, at);
        (pred, Spot::new(&pred, 1, at, pinned, self.cpus()))
    }

    /// A unit's predicted µs on this box idle, where it runs fastest.
    fn idle_us(&self, stats: &Stats, exact: &ArcStr, coarse: &ArcStr) -> u64 {
        let (pred, spot) = self.fastest(stats, exact, coarse, light(&stats.predict(exact, coarse)));
        stats.time_us(&pred, spot.at, 0, spot.threads)
    }

    /// Tokens of `class` lent to heavy jobs.
    fn lent(&self, class: usize) -> u32 {
        self.leases.iter().filter(|l| l.pinned.is_none()).map(|l| l.lent[class]).sum()
    }

    /// Classes by their calibrated rate, fastest first.
    fn by_speed(&self) -> LPooled<Vec<usize>> {
        let mut order: LPooled<Vec<usize>> = LPooled::take();
        order.extend(0..self.classes.len());
        order.sort_by_key(|&k| std::cmp::Reverse(self.classes[k].rate));
        order
    }

    /// Give `job`, which holds a token taken from class `first`, a jobserver
    /// of its own with a spare on credit, lending only from `pinned` if set.
    /// If the jobserver can't be made, the token goes back.
    #[allow(clippy::too_many_arguments)]
    fn lease(
        &mut self,
        job: JobId,
        pinned: Option<usize>,
        first: usize,
        cap: u32,
        fifo_dir: &Path,
        cpus: &[u32],
        meter: Arc<Meter>,
    ) -> Result<Arc<Grant>> {
        let path = fifo_dir.join(format_compact!("job-{}-{}", self.id.inner(), job.inner()).as_str());
        let made = path
            .to_str()
            .map(ArcStr::from)
            .context("jobserver path is not utf-8")
            .and_then(|jobserver| Ok((jobserver, Fifo::create(path)?)));
        let (jobserver, fifo) = match made {
            Ok(made) => made,
            Err(e) => {
                self.pool[first] += 1;
                return Err(e);
            }
        };
        let mut buf = BytesMut::new();
        let reply = Reply::Slot { cpus: cpus.to_vec(), jobserver: jobserver.clone(), filter_metadata: false };
        frame(&mut buf, &reply)?;
        let mut lent = vec![0; self.classes.len()];
        lent[first] = 1;
        let any_free = match pinned {
            Some(k) => self.pool[k] > 0,
            None => self.pool.iter().any(|&n| n > 0),
        };
        let credit = if any_free {
            fifo.give(1);
            Credit::Spare
        } else {
            Credit::None
        };
        self.leases.push(Lease {
            job,
            fifo,
            lent,
            credit,
            pinned,
            cap: cap.max(1),
            offered: any_free,
            idle: false,
            meter,
        });
        Ok(Arc::new(Grant { frame: buf.freeze(), cpus: cpus.to_vec(), jobserver }))
    }

    /// `job` ended: its tokens go back to the classes they came from, and
    /// what its meter measured comes back.
    fn release(&mut self, job: JobId, now: Instant) -> Option<Measured> {
        let i = self.leases.iter().position(|l| l.job == job)?;
        let l = self.leases.swap_remove(i);
        l.count(now);
        for (pool, n) in self.pool.iter_mut().zip(&l.lent) {
            *pool += n;
        }
        Some(l.meter.measured(now))
    }

    /// Tick every job's jobserver: see `Lease::tick`.
    fn lend(&mut self, now: Instant) {
        let order = self.by_speed();
        let Self { pool, leases, .. } = self;
        for l in leases.iter_mut() {
            match l.pinned {
                Some(k) => l.tick(pool, &[k], now),
                None => l.tick(pool, &order[..], now),
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
    /// Counts the tasks runnable on the box.
    pub runnable: Box<dyn Fn() -> Result<u32> + Send + Sync>,
    /// The hashes of files jobs sent to other boxes read.
    pub hashes: Arc<Hashes>,
    /// Where heartbeats to other boxes go to be sent.
    pub heartbeats: mpsc::UnboundedSender<Heartbeat>,
    pub tokens_per_cpu: f64,
    pub calibration_seconds: u64,
    pub decay: Decay,
}

/// A job that may run on another box, with the crate files it reads.
struct Portable {
    key: ArcStr,
    target: Option<ArcStr>,
    crates: Vec<ArcStr>,
    /// It links, so it sends every rlib below it.
    links: bool,
    /// The crate files it would send, by hash if one was taken since the
    /// file last changed, with their size in kB.
    inputs: Vec<(Option<Hash>, u64)>,
}

/// The crate files a job that reads `reads` sends of `crates`, paths
/// relative to `cwd`: see `Portable::inputs`.
fn inputs(hashes: &Hashes, cwd: &str, crates: &[ArcStr], reads: &Reads) -> Vec<(Option<Hash>, u64)> {
    let cwd = Path::new(cwd);
    let mut files = Vec::with_capacity(crates.len());
    for krate in crates {
        let mut add = |path: &Path| {
            if let Some((hash, len)) = hashes.known(path) {
                files.push((hash, len.div_ceil(1024)));
            }
        };
        match remote::crate_files(remote::absolute(cwd, krate), reads) {
            CrateFiles::One(path) => add(&path),
            CrateFiles::Late { rlib, meta } => {
                add(&rlib);
                add(&meta);
            }
        }
    }
    files
}

/// The crate files this box believes another box holds, by hash with
/// their size in kB and when a job last used them there, kept to the
/// box's cache size least recently used first, as the box keeps them.
#[derive(Default)]
struct Held {
    files: HashMap<Hash, (u64, u64)>,
    /// Uses, oldest first; one whose file was used again since is stale.
    order: VecDeque<(u64, Hash)>,
    kb: u64,
    uses: u64,
}

impl Held {
    /// A job there used `kept`, on a box that keeps `cap_kb`.
    fn used(&mut self, kept: &[(Hash, u64)], cap_kb: u64) {
        for &(hash, kb) in kept {
            self.uses += 1;
            let old = self.files.insert(hash, (kb, self.uses));
            self.kb = self.kb + kb - old.map_or(0, |(kb, _)| kb);
            self.order.push_back((self.uses, hash));
        }
        while self.kb > cap_kb
            && let Some((used, hash)) = self.order.pop_front()
        {
            if let Some(&(kb, last)) = self.files.get(&hash)
                && last == used
            {
                self.files.remove(&hash);
                self.kb -= kb;
            }
        }
        if self.order.len() > 2 * self.files.len() + 1024 {
            let files = &self.files;
            self.order.retain(|(used, hash)| files.get(hash).is_some_and(|&(_, last)| last == *used));
        }
    }

    /// The kB of `inputs` it doesn't hold.
    fn missing_kb(&self, inputs: &[(Option<Hash>, u64)]) -> u64 {
        inputs.iter().filter(|(hash, _)| hash.is_none_or(|h| !self.files.contains_key(&h))).map(|(_, kb)| kb).sum()
    }
}

/// A job of one of this box's builds: the build, by cargo's pid, and the
/// job's rank, the µs the build runs on once the job starts, `own_us` of
/// them its own (see `Graph::rank`).
#[derive(Clone, Copy)]
struct Build {
    pid: u32,
    rank_us: u64,
    own_us: u64,
}

struct Waiting {
    unit: Unit,
    build: Build,
    pred: Prediction,
    /// Where it may go besides here, or why nowhere.
    portable: Result<Portable, CompactString>,
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
    /// job, known once the box accepts it. Its files keep this box's link
    /// busy for `moves`.
    Remote { remote: usize, class: Option<usize>, shown_in: Option<u64>, moves: Moves },
}

/// How long a job sent to another box keeps this box's link busy with its
/// inputs, from when it is sent, and with its outputs, until it ends. Every
/// job this box sends shares the one link.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Moves {
    inputs_us: u64,
    outputs_us: u64,
}

/// When a job's files cross the link, in µs from now: two spans, or one
/// and an empty one.
type Spans = [(i64, i64); 2];

impl Moves {
    /// Its spans if it is sent at `sent` and ends at `end`, one if its
    /// inputs and outputs meet.
    fn spans(self, sent: i64, end: i64) -> Spans {
        let inputs = (sent, sent + self.inputs_us as i64);
        let outputs = ((end - self.outputs_us as i64).max(sent), end.max(sent));
        if outputs.0 <= inputs.1 { [(sent, inputs.1.max(outputs.1)), (0, 0)] } else { [inputs, outputs] }
    }
}

/// The µs spans `a` and `b` overlap.
fn overlap_us(a: &Spans, b: &Spans) -> u64 {
    a.iter().flat_map(|x| b.iter().map(move |y| (x.1.min(y.1) - x.0.max(y.0)).max(0) as u64)).sum()
}

/// What moving a job's files costs with the link shared: its own moving,
/// plus, while its files cross, the time the link spends on those of the
/// jobs `crossing` it, if it is sent at `start_us` and runs `run_us`.
fn shared(crossing: &[Spans], m: Moving, start_us: u64, run_us: u64) -> (u64, Moves) {
    let moves = Moves { inputs_us: m.inputs_us, outputs_us: m.outputs_us };
    let mine = moves.spans(start_us as i64, (start_us + m.copy_us + run_us) as i64);
    (m.copy_us + crossing.iter().map(|o| overlap_us(&mine, o)).sum::<u64>(), moves)
}

impl Place {
    fn here(&self) -> bool {
        matches!(self, Place::Local { .. } | Place::Spread { .. })
    }
}

struct Running {
    job: JobId,
    /// None for another box's job.
    build: Option<Build>,
    place: Place,
    /// Where it runs, in the cost table.
    at: PlaceId,
    /// Its predicted run time.
    run_us: u64,
    /// Predicted peak memory; none for a job on another box.
    mem_kb: u64,
    start: Instant,
    /// The root of its processes, once it has started here.
    pid: Option<u32>,
    /// The anonymous and shared memory its processes hold in RAM, plus
    /// their swap (`sys::tree_footprint_kb`), at the last sample, and the
    /// most seen.
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

    /// Its build, and its rank less the part of its own time already
    /// done, if it is one of this box's builds' jobs.
    fn rank_left_us(&self, now: Instant) -> Option<(u32, u64)> {
        let b = self.build?;
        let elapsed = now.duration_since(self.start).as_micros() as u64;
        let done = b.own_us * elapsed.min(self.run_us) / self.run_us.max(1);
        Some((b.pid, b.rank_us.saturating_sub(done)))
    }

    fn here(&self) -> bool {
        self.place.here()
    }
}

/// Another box as this one knows it.
struct RemoteBox {
    name: ArcStr,
    jobs: NPath,
    /// Its state, and its places in the cost table.
    state: Option<(BoxState, Places)>,
    /// Tokens each class has for jobs from here, as of the last dispatch:
    /// what its state shows free, less what jobs sent there that it doesn't
    /// show yet hold.
    avail: Vec<u32>,
    /// µs until the first of those jobs is predicted to end.
    unseen_left_us: Option<u64>,
    held: Held,
    /// When a job from here last ran there or a heartbeat was last sent
    /// there, and how many heartbeats it has had.
    used: Option<Instant>,
    heartbeat: Option<Instant>,
    heartbeats: u64,
    /// It answered busy, so its state is stale until this version.
    busy_until: Option<u64>,
    /// When its state arrived, which its predictions count from.
    seen: Instant,
}

impl RemoteBox {
    /// µs until a token of class `k`, which has none for jobs from here,
    /// is predicted to free: as its state says, or, if the tokens it shows
    /// free went to jobs from here it doesn't show yet, when the first of
    /// those ends.
    fn frees_in_us(&self, k: usize, now: Instant) -> Option<u64> {
        let (state, _) = self.state.as_ref()?;
        let class = state.classes.get(k)?;
        if class.free > 0 {
            return self.unseen_left_us;
        }
        let ms = class.frees_in_ms?;
        Some((ms as u64 * 1000).saturating_sub(now.duration_since(self.seen).as_micros() as u64))
    }

    /// Its state, if it can take `p`, else why not.
    fn usable(&self, p: &Portable) -> Result<&(BoxState, Places), &'static str> {
        let known = self.state.as_ref().ok_or("its state hasn't arrived")?;
        let state = &known.0;
        if self.busy_until.is_some_and(|v| state.version < v) {
            return Err("it answered busy");
        }
        if !state.toolchains.iter().any(|t| t.builds(&p.key, p.target.as_deref())) {
            return Err("it lacks the toolchain");
        }
        Ok(known)
    }
}

/// A run at `at` as `ran` and `m` measured it, less the time its linker
/// waited for files, holding the token the job started with.
fn timed(at: PlaceId, ran: &Ran, m: Measured, linker_waited_us: u64, mem_kb: u64, origin: Origin) -> Run {
    Run {
        place: at,
        load_c: m.load_c,
        wall_us: ran.wall_us.saturating_sub(linker_waited_us),
        front_us: ran.front.map(|f| f.wall_us),
        tokens_us: m.tokens_us.saturating_sub(linker_waited_us),
        mem_kb,
        origin,
    }
}

/// What `load` reads from `path`, or, if the file is there but can't be
/// read, `empty()` once the file is moved aside, so that saving doesn't
/// overwrite what it holds.
fn load_or_set_aside<T>(
    path: &Path,
    what: &str,
    load: impl FnOnce(&Path) -> Result<T>,
    empty: impl FnOnce() -> T,
) -> Result<T> {
    let e = match load(path) {
        Ok(t) => return Ok(t),
        Err(e) => e,
    };
    let aside = set_aside(path).with_context(|| format_compact!("moving unreadable {} aside: {e:#}", path.display()))?;
    error!("starting with an empty {what}: {} is unreadable, moved to {}: {e:#}", path.display(), aside.display());
    Ok(empty())
}

/// Rename `path` to the first of `PATH.unreadable`, `PATH.unreadable.2`,
/// ... that doesn't exist.
fn set_aside(path: &Path) -> Result<PathBuf> {
    let aside = (1u32..)
        .map(|n| {
            let mut name = path.as_os_str().to_owned();
            name.push(".unreadable");
            if n > 1 {
                name.push(format_compact!(".{n}").as_str());
            }
            PathBuf::from(name)
        })
        .find(|p| !p.exists())
        .context("no free name")?;
    fs::rename(path, &aside)?;
    Ok(aside)
}

/// Whether a class's next token, predicted to free in `now` ms, has moved
/// far enough from `shown`, published `since_ms` ago and counted down by
/// clients since, to publish it again.
fn frees_in_moved(shown: Option<u32>, since_ms: u64, now: Option<u32>) -> bool {
    match (shown, now) {
        (Some(shown), Some(now)) => {
            let (counted, now) = ((shown as u64).saturating_sub(since_ms), now as u64);
            let moved = counted.abs_diff(now);
            moved >= FREES_IN_STEP_MS && moved as f64 > FREES_IN_SHARE * counted.max(now) as f64
        }
        (shown, now) => shown.is_some() != now.is_some(),
    }
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

/// Whether a job runs pinned to one class.
fn light(pred: &Prediction) -> bool {
    pred.known && pred.width_c < LIGHT_WIDTH_C
}

/// Threads a job keeps busy after its front end: as measured, or if never
/// measured, every unit's median but no more than its `codegen_units`.
fn width(pred: &Prediction, codegen_units: u32) -> f64 {
    let width = pred.width_c as f64 / 100.;
    if pred.known { width.max(1.) } else { width.min(codegen_units as f64).max(1.) }
}

/// Hundredths of a task per CPU that `threads` add on `cpus` CPUs.
fn adds_c(threads: f64, cpus: u64) -> u32 {
    (threads * 100. / cpus.max(1) as f64).round() as u32
}

/// An average of samples in which each counts `1/e` as much after `tau`.
struct Ewma {
    tau: Duration,
    value: f64,
    at: Option<Instant>,
}

impl Ewma {
    fn new(tau: Duration) -> Self {
        Self { tau, value: 0., at: None }
    }

    fn add(&mut self, x: f64, now: Instant) {
        self.value = match self.at {
            None => x,
            Some(at) => {
                let a = 1. - (-now.saturating_duration_since(at).as_secs_f64() / self.tau.as_secs_f64()).exp();
                self.value + a * (x - self.value)
            }
        };
        self.at = Some(now);
    }
}

/// Where a job runs in the cost table, the threads it gets there, and the
/// load it adds, in hundredths of a task per CPU.
#[derive(Clone, Copy)]
struct Spot {
    at: PlaceId,
    threads: f64,
    adds: u32,
}

impl Spot {
    /// At `at`, pinned to a class or on every one of a box's `cpus`, for a
    /// job of `codegen_units`.
    fn new(pred: &Prediction, codegen_units: u32, at: PlaceId, pinned: bool, cpus: u64) -> Self {
        let width = width(pred, codegen_units);
        let threads = if pinned { width } else { width.min(cpus as f64) };
        Self { at, threads, adds: adds_c(if pinned { 1. } else { threads }, cpus) }
    }
}

/// Take `n` of `avail` from the fastest of `classes` first, as a server
/// starts a heavy job.
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
    /// Runnable tasks per CPU, in hundredths: their average over the
    /// samples, and that rounded, which decisions use.
    runnable: Ewma,
    load_c: u32,
    measured: Option<Instant>,
    queue: Vec<Waiting>,
    starved: bool,
    /// When the next job too young to place becomes old enough.
    gathering: Option<Instant>,
    remotes: Vec<RemoteBox>,
    /// Units that failed remotely and then built here, and how often.
    mismatches: HashMap<ArcStr, u8>,
    /// An offer was answered, so the next publish must send a new version.
    answered: bool,
    /// When the last version was published, which clients count its
    /// predictions down from.
    published: Instant,
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
        let decay = setup.decay;
        let jobs = setup.state_dir.join("jobs.hdr");
        let mut stats = load_or_set_aside(&jobs, "job stats", |p| Stats::load(p, decay), || Stats::new(decay))?;
        let graph = load_or_set_aside(&setup.state_dir.join("graph"), "build graph", Graph::load, Graph::new)?;
        let (classes, switch) = match classes::load(&setup.state_dir, &profile, &setup.allowed) {
            Ok(Some(classes)) => (classes, Switch::No),
            Ok(None) => (classes::fallback(&setup.allowed), Switch::Draining(profile.clone())),
            Err(e) => {
                warn!("{e:#}; calibrating {profile} again");
                (classes::fallback(&setup.allowed), Switch::Draining(profile.clone()))
            }
        };
        let layout = Layout::new(&profile, classes, setup.tokens_per_cpu, &mut stats);
        let mut sched = Self {
            setup,
            profile,
            layout,
            retired: Vec::new(),
            switch,
            running: Vec::new(),
            mem: BoxMemory::default(),
            runnable: Ewma::new(LOAD_TAU),
            load_c: 0,
            measured: None,
            queue: Vec::new(),
            starved: false,
            gathering: None,
            remotes: Vec::new(),
            mismatches: HashMap::new(),
            answered: false,
            published: Instant::now(),
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
            let sampling = self.layouts().any(|l| !l.idle());
            let event = tokio::select! {
                msg = rx.recv() => Event::Msg(msg),
                _ = recheck.tick(), if starved => Event::Recheck,
                _ = sample.tick(), if sampling => Event::Sample,
                _ = measure.tick() => Event::Measure,
                _ = time::sleep_until(gathering.unwrap_or_else(Instant::now).into()), if gathering.is_some() => {
                    Event::Recheck
                }
                _ = save.tick() => Event::Save,
            };
            self.stats.set_now(stats::now_s());
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
                Event::Sample => self.lend(Instant::now()),
                Event::Measure => self.measure(Instant::now()),
                Event::Save => {
                    self.save();
                }
            }
            self.dispatch();
            self.publish();
        }
        for w in self.queue.drain(..) {
            let _ = w.reply.send(Admission::Restarting);
        }
        if let Err(e) = self.save().await {
            error!("saving file hashes: {e}");
        }
    }

    fn layouts(&self) -> impl Iterator<Item = &Layout> {
        std::iter::once(&self.layout).chain(&self.retired)
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
            Msg::Mismatch { unit } => {
                let n = self.mismatches.entry(unit.exact.clone()).or_default();
                *n += 1;
                if *n == PIN_AFTER {
                    info!(
                        "keeping {} here until the daemon restarts: it failed on another box and then built here {n} times",
                        unit.exact
                    );
                }
            }
            Msg::Farm { name, state } => self.farm(name, state),
            Msg::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Msg::Profile { profile, recalibrate } => self.switch_to(profile, recalibrate)?,
            Msg::Calibrated { profile, result } => self.calibrated(profile, result)?,
            Msg::Heartbeat { name, kb, us } => {
                if let Some(rb) = self.remotes.iter().find(|r| r.name == name)
                    && let Some((_, places)) = &rb.state
                {
                    let mut at: LPooled<Vec<PlaceId>> = LPooled::take();
                    at.push(places.all);
                    at.extend(places.classes.iter().copied());
                    self.stats.heartbeat(&at, kb, us);
                }
            }
            Msg::Drain => return Ok(false),
        }
        Ok(true)
    }

    fn submit(&mut self, unit: Unit, pid: u32, remote: Result<Want, &'static str>, reply: oneshot::Sender<Admission>) {
        self.graph.record(&unit);
        let (layout, stats) = (&self.layout, &self.stats);
        let rank_us = self.graph.rank(&unit.id, &|exact, coarse| layout.idle_us(stats, exact, coarse));
        let build = Build { pid, rank_us, own_us: layout.idle_us(stats, &unit.exact, &unit.coarse) };
        let pred = self.stats.predict(&unit.exact, &unit.coarse);
        let pinned = self.mismatches.get(&unit.exact).is_some_and(|n| *n >= PIN_AFTER);
        let portable = match remote {
            Err(why) => Err(why.into()),
            Ok(_) if pinned => Err(format_compact!("it failed on another box and then built here {PIN_AFTER} times")),
            Ok(Want { key, target, reads, cwd }) => match self.graph.inputs(&unit.deps) {
                Ok(crates) => {
                    let inputs = inputs(&self.setup.hashes, &cwd, &crates, &reads);
                    Ok(Portable { key, target, crates, links: reads != Reads::Metadata, inputs })
                }
                Err(id) => Err(format_compact!("{id}, a crate below it, was never seen compiling")),
            },
        };
        self.queue.push(Waiting { unit, build, pred, portable, since: Instant::now(), reply });
    }

    fn finished(&mut self, job: JobId, unit: &Unit, ran: Option<(Ran, u64)>) {
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
        let measured = layout.release(job, Instant::now());
        self.reclaim();
        if let (Some((ran, linker_waited_us)), Some(m)) = (ran, measured) {
            debug!(
                "{} took {} ms holding {} tokens at load {}",
                unit.exact,
                ran.wall_us / 1000,
                m.tokens_us as f64 / ran.wall_us.max(1) as f64,
                m.load_c as f64 / 100.
            );
            let origin = if r.build.is_some() { Origin::Here } else { Origin::Served };
            let run = timed(r.at, &ran, m, linker_waited_us, ran.maxrss_kb.max(r.peak_kb), origin);
            self.stats.record(unit, &run);
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

    fn remote_done(&mut self, job: JobId, unit: &Unit, outcome: RemoteOutcome) {
        let Some(r) = self.take_running(job) else { return };
        let Place::Remote { remote, .. } = r.place else { return };
        self.remotes[remote].used = Some(Instant::now());
        match outcome {
            RemoteOutcome::Done(SentRun { ran, measured, linker_waited_us, transfer, kept }) => {
                self.mismatches.remove(&unit.exact);
                let run = timed(r.at, &ran, measured, linker_waited_us, ran.maxrss_kb, Origin::Sent(transfer));
                self.stats.record(unit, &run);
                let rb = &mut self.remotes[remote];
                if let Some((state, _)) = &rb.state {
                    rb.held.used(&kept, state.cache_gb << 20);
                }
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
                    unseen_left_us: None,
                    held: Held::default(),
                    used: None,
                    heartbeat: None,
                    heartbeats: 0,
                    busy_until: None,
                    seen: Instant::now(),
                });
                self.remotes.len() - 1
            }
        };
        let rb = &mut self.remotes[i];
        match state {
            Some((jobs, state)) => {
                let restarted = rb.state.as_ref().is_some_and(|(old, _)| state.version < old.version);
                if restarted {
                    rb.busy_until = None;
                }
                let rates = state.classes.iter().map(|c| (c.rate, c.cpus as u64));
                let places = Places::of(&mut self.stats, &rb.name, &state.profile, rates);
                rb.jobs = jobs;
                rb.state = Some((state, places));
                rb.seen = Instant::now();
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
        let id = self.layout.id;
        let (k, place) = match class {
            Some(k) => {
                let k = k.min(self.layout.classes.len() - 1);
                (k, Place::Local { layout: id, class: k })
            }
            None => {
                let pool = &self.layout.pool;
                let Some(&k) = self.layout.by_speed().iter().find(|&&k| pool[k] > 0) else {
                    return refuse("no token");
                };
                (k, Place::Spread { layout: id, class: k })
            }
        };
        if self.layout.pool[k] == 0 {
            return refuse("no token");
        }
        self.layout.pool[k] -= 1;
        let Estimate { spot, run_us, .. } = self.estimate(&unit, self.local(place), self.load_c, false);
        match self.admit(unit, None, pred, place, (spot.at, run_us), now) {
            Ok(slot) => Some(slot),
            Err(e) => {
                error!("{e:#}");
                None
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
                    self.apply(to, classes);
                    return Ok(());
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
        self.apply(profile, classes);
        match next {
            Some((to, recalibrate)) => self.switch_to(to, recalibrate),
            None => Ok(()),
        }
    }

    fn apply(&mut self, profile: Profile, classes: Vec<Class>) {
        let layout = Layout::new(&profile, classes, self.setup.tokens_per_cpu, &mut self.stats);
        let old = mem::replace(&mut self.layout, layout);
        if !old.idle() {
            self.retired.push(old);
        }
        for l in &mut self.retired {
            l.pool.fill(0);
        }
        let held = self.retired.iter().flat_map(|l| &l.leases).map(|l| l.lent.iter().sum::<u32>()).sum();
        self.layout.owe(held);
        self.graph.invalidate();
        if let Err(e) = classes::set_current(&self.setup.state_dir, &profile) {
            warn!("recording profile {profile}: {e:#}");
        }
        self.profile = profile;
    }

    /// µs until a token of local `class` is expected to free: when the
    /// first job holding one of its tokens ends, or, while the class is
    /// owed tokens that jobs of earlier layouts hold, when those jobs, in
    /// the order they are predicted to end, have given back what is
    /// overdrawn and what faster classes are owed, and one more (see
    /// `Layout::repaid`).
    fn frees_in_us(&self, class: usize, now: Instant) -> Option<u64> {
        let l = &self.layout;
        let left_us = |job: JobId| self.running.iter().find(|r| r.job == job).map(|r| r.left_us(now));
        let own = l.leases.iter().filter(|s| s.lent[class] > 0).filter_map(|s| left_us(s.job)).min();
        if l.owed[class] == 0 {
            return own;
        }
        let order = l.by_speed();
        let mut ahead = l.overdrawn + order.iter().take_while(|&&k| k != class).map(|&k| l.owed[k]).sum::<u32>();
        let mut ends: LPooled<Vec<(u64, u32)>> = LPooled::take();
        ends.extend(
            self.retired.iter().flat_map(|r| &r.leases).filter_map(|s| Some((left_us(s.job)?, s.lent.iter().sum()))),
        );
        ends.sort_unstable();
        let repaid = ends.iter().find_map(|&(us, n)| match n > ahead {
            true => Some(us),
            false => {
                ahead -= n;
                None
            }
        });
        own.into_iter().chain(repaid).min()
    }

    /// Tick every job's jobserver: see `Lease::tick`.
    fn lend(&mut self, now: Instant) {
        for l in std::iter::once(&mut self.layout).chain(&mut self.retired) {
            l.lend(now);
        }
        self.reclaim();
    }

    /// Tokens that jobs of earlier layouts gave back go to the current
    /// layout, and a layout with no jobs left goes.
    fn reclaim(&mut self) {
        let mut n = 0;
        for l in &mut self.retired {
            n += l.pool.iter().sum::<u32>();
            l.pool.fill(0);
        }
        self.layout.repaid(n);
        self.retired.retain(|l| !l.idle());
    }

    /// Whether any job runs on this box, its own or another box's.
    fn runs_here(&self) -> bool {
        self.running.iter().any(Running::here)
    }

    /// Sample the box's memory and load, and the footprint of each job
    /// running here.
    fn measure(&mut self, now: Instant) {
        match (self.setup.memory)() {
            Ok(mem) => self.mem = mem,
            Err(e) => warn!("measuring memory: {e:#}"),
        }
        match (self.setup.runnable)() {
            Ok(n) => {
                self.runnable.add(n as f64 * 100. / self.setup.allowed.len().max(1) as f64, now);
                self.load_c = self.runnable.value.round() as u32;
            }
            Err(e) => warn!("counting runnable tasks: {e:#}"),
        }
        for lease in std::iter::once(&self.layout).chain(&self.retired).flat_map(|l| &l.leases) {
            lease.meter.sample(self.load_c);
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

    /// Where a place on this box is in the cost table, whether a job there
    /// is pinned to a class, and the box's CPUs.
    fn local(&self, place: Place) -> (PlaceId, bool, u64) {
        let places = &self.layout.places;
        match place {
            Place::Local { class, .. } => (places.classes[class], true, self.layout.cpus()),
            Place::Spread { .. } | Place::Remote { .. } => (places.all, false, self.layout.cpus()),
        }
    }

    /// `unit` at `at`, pinned to a class or not, on a box of `cpus`: the
    /// spot it takes and µs it runs under `load_c` (see `run_us`), from
    /// its own runs there when it has any (see `Stats::predict_at`).
    fn estimate(&self, unit: &Unit, (at, pinned, cpus): (PlaceId, bool, u64), load_c: u32, waits: bool) -> Estimate {
        let (pred, basis) = self.stats.predict_at(&unit.exact, &unit.coarse, at);
        let spot = Spot::new(&pred, unit.codegen_units, at, pinned, cpus);
        Estimate { spot, run_us: self.run_us(&pred, &spot, load_c, waits), basis }
    }

    /// Predicted µs at `spot` on a box under `load_c`: with the job's own
    /// threads added if it starts now, and not if it waits for a token,
    /// since it then takes over from a job that ends.
    fn run_us(&self, pred: &Prediction, spot: &Spot, load_c: u32, waits: bool) -> u64 {
        let load_c = if waits { load_c } else { load_c + spot.adds };
        self.stats.time_us(pred, spot.at, load_c, spot.threads)
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
            w.build.rank_us as f64 * (now.duration_since(w.since).as_secs_f64() / AGING_S).exp2()
        };
        self.queue.sort_by(|a, b| prio(b).total_cmp(&prio(a)));
        let mut admissible: LPooled<Vec<usize>> = LPooled::take();
        admissible.extend((0..self.queue.len()).filter(|&i| !young(&self.queue[i])));
        if admissible.is_empty() {
            return;
        }
        self.measure_if_stale(now);
        // How long each build runs on: the largest rank among its jobs,
        // counting the ones too young to place.
        let mut horizon: LPooled<HashMap<u32, u64>> = LPooled::take();
        for (build, rank) in self
            .queue
            .iter()
            .map(|w| (w.build.pid, w.build.rank_us))
            .chain(self.running.iter().filter_map(|r| r.rank_left_us(now)))
        {
            let h = horizon.entry(build).or_default();
            *h = (*h).max(rank);
        }
        // The free tokens of each class, which this dispatch hands out; what
        // is left goes back.
        let mut held: LPooled<Vec<u32>> = LPooled::take();
        held.extend(self.layout.pool.iter().copied());
        let mut frees_in: LPooled<Vec<u64>> = LPooled::take();
        frees_in.extend(
            (0..held.len()).map(|k| if held[k] > 0 { 0 } else { self.frees_in_us(k, now).unwrap_or(0) }),
        );
        // The soonest a job this dispatch places in each class finishes.
        let mut placed_us: LPooled<Vec<u64>> = LPooled::take();
        placed_us.resize(held.len(), u64::MAX);
        self.count_available(now);
        let fastest = self.layout.by_speed()[0];
        let id = self.layout.id;
        // How much later than its state predicts each class of each box frees
        // a token, for the jobs this dispatch has set waiting for it.
        let mut queued: LPooled<HashMap<(usize, usize), u64>> = LPooled::take();
        // Each box's load, counting the jobs this dispatch places there.
        let mut load_here = self.load_c;
        let mut load_there: LPooled<Vec<u32>> = LPooled::take();
        load_there.extend(self.remotes.iter().map(|rb| rb.state.as_ref().map_or(0, |(s, _)| s.load_c)));
        let mut mem_free = self.mem_free_kb();
        // With nothing running here, one job runs whatever its memory, so
        // the box can't sit idle behind a job bigger than it.
        let mut first_here = !self.runs_here();
        // Set once a job can neither fit here nor go to another box. Later
        // jobs don't run here either, so smaller ones can't starve it.
        let mut mem_blocked = false;
        // When the files of the jobs this box has sent cross its link,
        // joined by those this dispatch sends.
        let mut crossing: LPooled<Vec<Spans>> = LPooled::take();
        crossing.extend(self.running.iter().filter_map(|r| match r.place {
            Place::Remote { moves, .. } => {
                let sent = -(now.duration_since(r.start).as_micros() as i64);
                Some(moves.spans(sent, r.left_us(now) as i64))
            }
            Place::Local { .. } | Place::Spread { .. } => None,
        }));
        let mut cands: LPooled<Vec<Cand>> = LPooled::take();
        let logging = log_enabled!(target: "placement", Level::Debug);
        let mut granted: LPooled<Vec<(usize, Place, u64, PlaceId)>> = LPooled::take();
        let queue_us = |queued: &HashMap<(usize, usize), u64>, r: usize, k: usize| {
            queued.get(&(r, k)).copied().unwrap_or(0)
        };
        for &i in admissible.iter() {
            let w = &self.queue[i];
            let fits = !mem_blocked && (first_here || w.pred.mem_kb <= mem_free);
            let waits_here = !fits || held.iter().all(|&n| n == 0);
            let fastest_at = self.layout.places.classes[fastest];
            let fastest_pred = self.stats.predict_at(&w.unit.exact, &w.unit.coarse, fastest_at).0;
            let fastest_us = self.stats.time_us(&fastest_pred, fastest_at, load_here, 1.);
            let away = match &w.portable {
                Err(why) => Err(why.as_str()),
                Ok(p) if !w.pred.known || waits_here || fastest_us >= REMOTE_MIN_US => Ok(p),
                Ok(_) => Err("it is short and a token is free here"),
            };
            let links = away.is_ok_and(|p| p.links);
            cands.clear();
            let chosen = if light(&w.pred) {
                let local = if fits { held.len() } else { 0 };
                for k in 0..local {
                    let place = Place::Local { layout: id, class: k };
                    let waits = held[k] == 0;
                    let load_c = if waits { self.load_c } else { load_here };
                    let Estimate { spot, run_us, basis } = self.estimate(&w.unit, self.local(place), load_c, waits);
                    cands.push(Cand { start_us: frees_in[k], copy_us: 0, run_us, spot, place, queue: None, basis });
                }
                for (r, rb) in self.remotes.iter().enumerate() {
                    let Ok((state, places)) = away.and_then(|p| rb.usable(p)) else { continue };
                    let cpus = state.classes.iter().map(|c| c.cpus as u64).sum();
                    let missing_kb = away.map_or(0, |p| rb.held.missing_kb(&p.inputs));
                    for (k, &at) in places.classes.iter().enumerate() {
                        // A busy class is worth waiting for if it finishes the
                        // job sooner than anywhere free.
                        let waits = rb.avail.get(k).is_none_or(|&n| n == 0);
                        let frees_in_us = match waits {
                            false => 0,
                            true => match rb.frees_in_us(k, now) {
                                Some(t) => t + queue_us(&queued, r, k),
                                None => continue,
                            },
                        };
                        let load_c = if waits { state.load_c } else { load_there[r] };
                        let Estimate { spot, run_us, basis } = self.estimate(&w.unit, (at, true, cpus), load_c, waits);
                        let moving = self.stats.moving(&w.unit, at, links, missing_kb);
                        let (copy_us, moves) = shared(&crossing, moving, frees_in_us, run_us);
                        let place = Place::Remote { remote: r, class: Some(k), shown_in: None, moves };
                        cands.push(Cand { start_us: frees_in_us, copy_us, run_us, spot, place, queue: None, basis });
                    }
                }
                (!cands.is_empty()).then(|| {
                    let idle_to_now = self.stats.factor(fastest_at, load_here) / self.stats.factor(fastest_at, 0);
                    let spare_idle_us = horizon[&w.build.pid].saturating_sub(w.build.rank_us);
                    choose(&cands, local, (spare_idle_us as f64 * idle_to_now) as u64)
                })
            } else {
                // Its front end runs first, on one thread, so the short jobs
                // holding tokens now are gone by the time its codegen wants
                // them: a box can give it as many threads as it has CPUs.
                if fits {
                    // Its first token from the fastest class with one, or it
                    // waits for the class that frees first.
                    let first_free = frees_in.iter().copied().enumerate().min_by_key(|&(_, t)| t);
                    let (class, start_us) = match self.layout.by_speed().iter().find(|&&k| held[k] > 0) {
                        Some(&k) => (k, 0),
                        None => first_free.unwrap_or((0, 0)),
                    };
                    let place = Place::Spread { layout: id, class };
                    let waits = held[class] == 0;
                    let load_c = if waits { self.load_c } else { load_here };
                    let Estimate { spot, run_us, basis } = self.estimate(&w.unit, self.local(place), load_c, waits);
                    cands.push(Cand { start_us, copy_us: 0, run_us, spot, place, queue: None, basis });
                }
                for (r, rb) in self.remotes.iter().enumerate() {
                    let Ok((state, places)) = away.and_then(|p| rb.usable(p)) else { continue };
                    let cpus = state.classes.iter().map(|c| c.cpus as u64).sum();
                    let waits = rb.avail.iter().all(|&n| n == 0);
                    let load_c = if waits { state.load_c } else { load_there[r] };
                    let Estimate { spot, run_us, basis } = self.estimate(&w.unit, (places.all, false, cpus), load_c, waits);
                    let missing_kb = away.map_or(0, |p| rb.held.missing_kb(&p.inputs));
                    let moving = self.stats.moving(&w.unit, places.all, links, missing_kb);
                    // A busy box is worth waiting for if it finishes the job
                    // sooner than anywhere free; with no history to say so,
                    // the job doesn't wait for one.
                    let start = match !waits {
                        true => Some((0, None)),
                        false if w.pred.known => (0..state.classes.len())
                            .filter_map(|k| Some((rb.frees_in_us(k, now)? + queue_us(&queued, r, k), Some(k))))
                            .min(),
                        false => None,
                    };
                    if let Some((start_us, queue)) = start {
                        let (copy_us, moves) = shared(&crossing, moving, start_us, run_us);
                        let place = Place::Remote { remote: r, class: None, shown_in: None, moves };
                        cands.push(Cand { start_us, copy_us, run_us, spot, place, queue, basis });
                    }
                }
                (!cands.is_empty()).then(|| pick_wide(&cands, w.pred.known))
            };
            let Some(chosen) = chosen else {
                mem_blocked = true;
                self.starved = true;
                continue;
            };
            let c = &cands[chosen];
            let (place, run_us, spot, queue) = (c.place, c.copy_us + c.run_us, c.spot, c.queue);
            let line = logging.then(|| self.placement(w, fastest_us, fits, away, &cands, chosen));
            if place.here() {
                // Memory is kept for a job waiting for a token too, so it
                // fits when one frees.
                mem_free = mem_free.saturating_sub(w.pred.mem_kb);
                first_here = false;
            }
            match place {
                Place::Remote { remote, class, moves, .. } => {
                    let RemoteBox { state, avail, .. } = &mut self.remotes[remote];
                    let waits = match class {
                        Some(k) => avail.get(k).is_none_or(|&n| n == 0),
                        None => avail.iter().all(|&n| n == 0),
                    };
                    if waits {
                        // It waits for a token there to free. Jobs after it
                        // see the queue it joins there.
                        if let (Some(k), Some((state, _))) = (class.or(queue), state.as_ref()) {
                            let cpus = state.classes.get(k).map_or(1, |c| c.cpus as u64).max(1);
                            *queued.entry((remote, k)).or_default() += run_us / cpus;
                        }
                        self.starved = true;
                        continue;
                    }
                    match (class, state) {
                        (Some(k), _) => avail[k] -= 1,
                        (None, Some((state, _))) => take_fastest(avail, &state.classes, 1),
                        (None, None) => (),
                    }
                    let unseen = &mut self.remotes[remote].unseen_left_us;
                    *unseen = Some(unseen.map_or(run_us, |t| t.min(run_us)));
                    load_there[remote] += spot.adds;
                    crossing.push(moves.spans(0, run_us as i64));
                    if let Some(line) = line {
                        debug!(target: "placement", "{}", line.as_str());
                    }
                    granted.push((i, place, run_us, spot.at));
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
                    load_here += spot.adds;
                    if let Some(line) = line {
                        debug!(target: "placement", "{}", line.as_str());
                    }
                    granted.push((i, place, run_us, spot.at));
                }
            }
        }
        self.layout.pool.copy_from_slice(&held);
        self.heartbeats(now, &granted);
        granted.sort_unstable_by_key(|&(i, ..)| std::cmp::Reverse(i));
        for &(i, place, run_us, at) in granted.iter() {
            let w = self.queue.swap_remove(i);
            // If the wrapper is gone a slot comes back through Drop.
            match place {
                Place::Local { .. } | Place::Spread { .. } => {
                    let admission = match self.admit(w.unit, Some(w.build), w.pred, place, (at, run_us), now) {
                        Ok(slot) => Admission::Slot(slot),
                        Err(e) => {
                            error!("{e:#}");
                            Admission::Refused(format_compact!("{e:#}").as_str().into())
                        }
                    };
                    let _ = w.reply.send(admission);
                }
                Place::Remote { remote, class, moves, .. } => self.send_remote(w, (remote, class, moves), at, run_us, now),
            }
        }
    }

    /// Where `w` goes and why: whether it ran before and its time on one
    /// thread of this box's fastest class; each place it could go, with the
    /// seconds until a slot, moving its files and running there, and what
    /// that run time comes from (its own runs there, its runs on that box,
    /// or an estimate from its size), the one it takes starred; and why it
    /// couldn't go anywhere else.
    fn placement(
        &self,
        w: &Waiting,
        fastest_us: u64,
        fits: bool,
        away: Result<&Portable, &str>,
        cands: &[Cand],
        chosen: usize,
    ) -> LPooled<String> {
        let mut line: LPooled<String> = LPooled::take();
        let s = |us: u64| us as f64 / 1e6;
        let ran = if w.pred.known { "ran before" } else { "never ran" };
        let _ = write!(line, "{} ({ran}; {:.2} s on one thread of the fastest class here):", w.unit.exact, s(fastest_us));
        for (i, c) in cands.iter().enumerate() {
            let star = if i == chosen { "*" } else { "" };
            let _ = match c.place {
                Place::Local { class, .. } => write!(line, " {star}here/{class}"),
                Place::Spread { .. } => write!(line, " {star}here/all"),
                Place::Remote { remote, class: Some(k), .. } => write!(line, " {star}{}/{k}", self.remotes[remote].name),
                Place::Remote { remote, class: None, .. } => write!(line, " {star}{}/all", self.remotes[remote].name),
            };
            let basis = match c.basis {
                Basis::Place => "own",
                Basis::Box => "box",
                Basis::Size => "est",
            };
            let _ = write!(line, " {:.2}+{:.2}+{:.2} [{basis}]", s(c.start_us), s(c.copy_us), s(c.run_us));
        }
        if !fits {
            line.push_str("; not here: its memory doesn't fit");
        }
        match away {
            Err(why) => {
                let _ = write!(line, "; no other box: {why}");
            }
            Ok(p) => {
                for (r, rb) in self.remotes.iter().enumerate() {
                    let why = match rb.usable(p) {
                        Err(why) => why,
                        Ok(_) if cands.iter().any(|c| matches!(c.place, Place::Remote { remote, .. } if remote == r)) => {
                            continue;
                        }
                        Ok(_) => "no token free, and none worth waiting for",
                    };
                    let _ = write!(line, "; not {}: {why}", rb.name);
                }
            }
        }
        line
    }

    /// While jobs wait here, send a heartbeat to each box that has a token
    /// free but has run nothing from here, nor had a heartbeat, for
    /// `HEARTBEAT_EVERY`, so its transfer stats stay current though its
    /// recent sends judged it badly: half a MB and a MB in turn.
    fn heartbeats(&mut self, now: Instant, granted: &[(usize, Place, u64, PlaceId)]) {
        let waiting = self.queue.len() > granted.len();
        if !waiting {
            return;
        }
        let quiet = |t: Option<Instant>| t.is_none_or(|t| now.duration_since(t) >= HEARTBEAT_EVERY);
        for (i, rb) in self.remotes.iter_mut().enumerate() {
            let busy = self.running.iter().any(|r| matches!(r.place, Place::Remote { remote, .. } if remote == i))
                || granted.iter().any(|(_, p, ..)| matches!(p, Place::Remote { remote, .. } if *remote == i));
            if rb.state.is_none() || busy || !rb.avail.iter().any(|&n| n > 0) || !quiet(rb.used) || !quiet(rb.heartbeat) {
                continue;
            }
            rb.heartbeat = Some(now);
            rb.heartbeats += 1;
            let kb = if rb.heartbeats % 2 == 1 { 512 } else { 1024 };
            let _ = self.setup.heartbeats.send(Heartbeat { name: rb.name.clone(), jobs: rb.jobs.clone(), kb });
        }
    }

    /// What each box has free for jobs from here: the free tokens its state
    /// shows, less those that jobs sent there it doesn't show yet hold.
    fn count_available(&mut self, now: Instant) {
        for rb in &mut self.remotes {
            rb.avail.clear();
            rb.unseen_left_us = None;
            if let Some((state, _)) = &rb.state {
                rb.avail.extend(state.classes.iter().map(|c| c.free));
            }
        }
        for r in &self.running {
            let Place::Remote { remote, class, shown_in, .. } = r.place else { continue };
            let RemoteBox { state: Some((state, _)), avail, unseen_left_us, .. } = &mut self.remotes[remote]
            else {
                continue;
            };
            if shown_in.is_some_and(|v| state.version >= v) {
                continue;
            }
            let left = r.left_us(now);
            *unseen_left_us = Some(unseen_left_us.map_or(left, |t| t.min(left)));
            match class {
                Some(k) => {
                    if let Some(n) = avail.get_mut(k) {
                        *n = n.saturating_sub(1);
                    }
                }
                None => take_fastest(avail, &state.classes, 1),
            }
        }
    }

    /// Start a job here at `place`, a class or every CPU, holding a token
    /// already taken from the class `place` names. It runs at `at` in the
    /// cost table and is predicted to take `run_us`.
    fn admit(
        &mut self,
        unit: Unit,
        build: Option<Build>,
        pred: Prediction,
        place: Place,
        (at, run_us): (PlaceId, u64),
        now: Instant,
    ) -> Result<Slot> {
        let job = JobId::new();
        let meter = Arc::new(Meter::new(self.load_c, now));
        let (pinned, first, cpus) = match place {
            Place::Local { class, .. } => (Some(class), class, self.layout.classes[class].cpus.clone()),
            Place::Spread { class, .. } => (None, class, self.setup.allowed.clone()),
            Place::Remote { .. } => unreachable!("admitting a remote place"),
        };
        let fifo_dir = &self.setup.fifo_dir;
        let grant = self
            .layout
            .lease(job, pinned, first, unit.codegen_units, fifo_dir, &cpus, meter.clone())
            .with_context(|| format_compact!("starting {}", unit.exact))?;
        self.running.push(Running {
            job,
            build,
            place,
            at,
            run_us,
            mem_kb: pred.mem_kb,
            start: now,
            pid: None,
            foot_kb: 0,
            peak_kb: 0,
        });
        Ok(Slot { grant, job, unit, ran: None, meter, tx: self.tx.clone() })
    }

    /// Send `w` to another box, where it runs at `at` and is predicted to
    /// take `run_us`, moving its files included.
    fn send_remote(
        &mut self,
        w: Waiting,
        (remote, class, moves): (usize, Option<usize>, Moves),
        at: PlaceId,
        run_us: u64,
        now: Instant,
    ) {
        let rb = &self.remotes[remote];
        debug!("sending {} to {} class {class:?}", w.unit.exact, rb.name);
        let job = JobId::new();
        self.running.push(Running {
            job,
            build: Some(w.build),
            place: Place::Remote { remote, class, shown_in: None, moves },
            at,
            run_us,
            mem_kb: 0,
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
        let now = Instant::now();
        let layout = &self.layout;
        let mut classes: LPooled<Vec<ClassState>> = LPooled::take();
        classes.extend(layout.classes.iter().zip(&layout.pool).enumerate().map(|(k, (c, &pool))| {
            let free = if closed { 0 } else { pool };
            let frees_in_ms = match (closed, free) {
                (true, _) => None,
                (false, 0) => self.frees_in_us(k, now).map(|us| (us / 1000).min(u32::MAX as u64) as u32),
                (false, _) => Some(0),
            };
            ClassState { rate: c.rate, cpus: c.cpus.len() as u32, free, frees_in_ms }
        }));
        let profile = self.profile.name();
        let load_c = self.load_c;
        let since_ms = now.saturating_duration_since(self.published).as_millis() as u64;
        let changed = self.load.send_if_modified(|load| {
            let key = |c: &ClassState| (c.rate, c.cpus, c.free);
            let changed = answered
                || load.profile != *profile
                || load.load_c.abs_diff(load_c) >= LOAD_STEP_C
                || load.classes.len() != classes.len()
                || load.classes.iter().zip(classes.iter()).any(|(shown, c)| {
                    key(shown) != key(c) || frees_in_moved(shown.frees_in_ms, since_ms, c.frees_in_ms)
                });
            if changed {
                load.profile = profile.clone();
                load.load_c = load_c;
                load.classes.clear();
                load.classes.extend(classes.drain(..));
                load.version += 1;
            }
            changed
        });
        if changed {
            self.published = now;
        }
    }

    fn status(&mut self) -> Status {
        self.measure_if_stale(Instant::now());
        let mut ranks: LPooled<HashMap<ArcStr, u64>> = LPooled::take();
        let (layout, stats) = (&self.layout, &self.stats);
        for (key, rank) in self.graph.ranked(&|exact, coarse| layout.idle_us(stats, exact, coarse)) {
            let r = ranks.entry(key).or_default();
            *r = (*r).max(rank);
        }
        let layout = &self.layout;
        let fastest = layout.by_speed()[0];
        // This box's fastest class, idle, in instructions per µs: speeds are
        // in hundredths of it, and work in ms on it.
        let base = layout.classes[fastest].rate as f64 / self.stats.factor(layout.places.classes[fastest], 0);
        let speed_c = |rate: u64, at: PlaceId, load_c: u32| {
            (rate as f64 / self.stats.factor(at, load_c) / base * 100.).round() as u32
        };
        let ms = |mi: u64| (mi as f64 * 1000. / base).round() as u64;
        let id = layout.id;
        let mut units: Vec<UnitStatus> = self
            .stats
            .units()
            .map(|(key, info)| {
                let [here, sent, served] = info.runs;
                let (pred, spot) = layout.fastest(&self.stats, key, key, light(&info.pred));
                UnitStatus {
                    key: key.clone(),
                    here,
                    sent,
                    served,
                    work_ms: ms(info.pred.cost_mi()),
                    front_ms: (info.pred.front_mi > 0).then(|| ms(info.pred.front_mi)),
                    width_c: info.pred.width_c,
                    now_ms: self.run_us(&pred, &spot, self.load_c, false) / 1000,
                    wall_ms: info.wall_ms,
                    mem_kb: info.pred.mem_kb,
                    copy_ms: info.copy_ms,
                    sent_kb: info.sent_kb,
                    rank_ms: ranks.get(key).map(|&r| r / 1000),
                }
            })
            .collect();
        units.sort_unstable_by(|a, b| b.rank_ms.cmp(&a.rank_ms).then(b.work_ms.cmp(&a.work_ms)));
        let boxes = self
            .remotes
            .iter()
            .enumerate()
            .filter_map(|(i, rb)| {
                let (state, places) = rb.state.as_ref()?;
                let there = |r: &&Running| matches!(r.place, Place::Remote { remote, .. } if remote == i);
                let classes = state
                    .classes
                    .iter()
                    .zip(&places.classes)
                    .map(|(c, &at)| BoxClass { cpus: c.cpus, speed_c: speed_c(c.rate, at, state.load_c), free: c.free })
                    .collect();
                let (_, mean) = spread_rates(state.classes.iter().map(|c| (c.rate, c.cpus as u64)));
                let copy_ms = |links| {
                    let mut at = std::iter::once(places.all).chain(places.classes.iter().copied());
                    at.find_map(|at| self.stats.sends(at, links).0).map(|us| us / 1000)
                };
                let (_, mb_per_s) = self.stats.sends(places.all, true);
                Some(BoxStatus {
                    name: rb.name.clone(),
                    profile: state.profile.clone(),
                    load_c: state.load_c,
                    classes,
                    threads: state.classes.iter().map(|c| c.cpus).sum(),
                    thread_speed_c: speed_c(mean, places.all, state.load_c),
                    library_copy_ms: copy_ms(false),
                    link_copy_ms: copy_ms(true),
                    mb_per_s: mb_per_s.round() as u32,
                    sent: self.running.iter().filter(there).count() as u32,
                })
            })
            .collect();
        let (_, mean) = spread_rates(layout.classes.iter().map(|c| (c.rate, c.cpus.len() as u64)));
        Status {
            profile: self.profile.name().clone(),
            switching: match &self.switch {
                Switch::No => Switching::No,
                Switch::Draining(p) => Switching::Draining(p.name().clone()),
                Switch::Calibrating { profile, .. } => Switching::Calibrating(profile.name().clone()),
            },
            load_c: self.load_c,
            base_rate: base.round() as u64,
            classes: layout
                .classes
                .iter()
                .enumerate()
                .map(|(k, c)| {
                    let here = Place::Local { layout: id, class: k };
                    let (mut running, mut serving) = (0, 0);
                    for r in self.running.iter().filter(|r| r.place == here) {
                        match r.build {
                            Some(_) => running += 1,
                            None => serving += 1,
                        }
                    }
                    let at = layout.places.classes[k];
                    ClassStatus {
                        cpus: c.cpus.len() as u32,
                        idle_speed_c: speed_c(c.rate, at, 0),
                        speed_c: speed_c(c.rate, at, self.load_c),
                        running,
                        serving,
                        lent: layout.lent(k),
                        free: layout.pool[k],
                    }
                })
                .collect(),
            spread: self.running.iter().filter(|r| matches!(r.place, Place::Spread { .. })).fold(
                SpreadStatus {
                    running: 0,
                    serving: 0,
                    threads: layout.cpus() as u32,
                    thread_speed_c: speed_c(mean, layout.places.all, self.load_c),
                },
                |n, r| match r.build {
                    Some(_) => SpreadStatus { running: n.running + 1, ..n },
                    None => SpreadStatus { serving: n.serving + 1, ..n },
                },
            ),
            finishing: self.retired.iter().map(|l| l.leases.len() as u32).sum(),
            waiting: self.queue.len() as u32,
            mem_capacity_kb: self.mem.capacity_kb,
            mem_used_kb: self.mem.used_kb,
            mem_growth_kb: self.mem_growth_kb(),
            graph_nodes: self.graph.len() as u32,
            units,
            boxes,
        }
    }

    /// Save the job stats and the build graph here, and the file hashes on
    /// a blocking thread, whose task it returns.
    fn save(&mut self) -> task::JoinHandle<()> {
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
        let (hashes, path) = (self.setup.hashes.clone(), self.setup.state_dir.join("hashes"));
        task::spawn_blocking(move || {
            if let Err(e) = hashes.save(&path) {
                warn!("saving file hashes: {e:#}");
            }
        })
    }
}

/// What `Sched::estimate` predicts of a job at a place.
struct Estimate {
    spot: Spot,
    run_us: u64,
    basis: Basis,
}

/// Somewhere a job could run.
struct Cand {
    /// µs until it has a slot, then moving its files, then running.
    start_us: u64,
    copy_us: u64,
    run_us: u64,
    spot: Spot,
    place: Place,
    /// The class of another box whose next free token a heavy job would
    /// wait for.
    queue: Option<usize>,
    basis: Basis,
}

impl Cand {
    fn finish_us(&self) -> u64 {
        self.start_us + self.copy_us + self.run_us
    }
}

/// Where a light job runs. The first `local` candidates are this box's
/// classes: the job takes the slowest of them on which it still finishes
/// within `slack_us`, so faster ones stay free for jobs more work waits on.
/// It takes one with a free slot if any will do, rather than wait for a
/// slower one while a faster one sits idle; with no slack it takes the one
/// where it finishes first, even if it must wait for it. The rest are other
/// boxes, which are spare capacity: the job goes to one if it finishes
/// there sooner than on its local choice.
fn choose(cands: &[Cand], local: usize, slack_us: u64) -> usize {
    let finish = Cand::finish_us;
    let here = &cands[..local];
    let first = here.iter().map(finish).min().unwrap_or(0);
    let deadline = first.saturating_add(slack_us);
    let slowest = |free_only: bool| {
        here.iter()
            .enumerate()
            .filter(|(_, c)| finish(c) <= deadline && (!free_only || c.start_us == 0))
            .max_by(|(i, a), (j, b)| a.run_us.cmp(&b.run_us).then(j.cmp(i)))
            .map(|(i, _)| i)
    };
    let pick = slowest(true).or_else(|| slowest(false)).unwrap_or(0);
    let away = cands[local..].iter().enumerate().min_by_key(|(_, c)| finish(c));
    match away {
        Some((j, c)) if finish(c) < finish(&cands[pick]) => local + j,
        _ => pick,
    }
}

/// Where a heavy job runs, on every CPU of this box or another: where it
/// finishes soonest. A job with no history has no size to say waiting
/// pays, so it goes where it finishes soonest of the places with a token
/// free, else waits for this box's.
fn pick_wide(cands: &[Cand], known: bool) -> usize {
    let soonest = |free_only: bool| {
        let open = cands.iter().enumerate().filter(|(_, c)| !free_only || c.start_us == 0);
        open.min_by_key(|(_, c)| c.finish_us()).map(|(i, _)| i)
    };
    let free = if known { None } else { soonest(true) };
    free.or_else(|| soonest(false)).unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::toolchain::Toolchain;
    use anyhow::bail;
    use netidx_core::pack::Pack;
    use std::{
        fs::OpenOptions,
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
            dir: Some(arcstr::literal!("/t")),
            deps: deps.iter().map(|d| crate::protocol::Dep { id: ArcStr::from(*d), file: None }).collect(),
            codegen_units: 16,
        }
    }

    fn spot() -> Spot {
        Spot { at: Stats::new(Decay::default()).place("t/0", 1, 1), threads: 1., adds: 0 }
    }

    fn local(run_us: u64, start_us: u64) -> Cand {
        let place = Place::Local { layout: LayoutId::new(), class: 0 };
        Cand { start_us, copy_us: 0, run_us, spot: spot(), place, queue: None, basis: Basis::Place }
    }

    #[test]
    fn choose_spends_slack_on_slower_classes() {
        // It takes 1, 2 or 4 ms. All free.
        let free = [local(1000, 0), local(2000, 0), local(4000, 0)];
        assert_eq!(choose(&free, 3, 0), 0);
        assert_eq!(choose(&free, 3, 1000), 1);
        assert_eq!(choose(&free, 3, 3000), 2);
        // The fast class frees in 5 ms: with no slack, start now on the middle one.
        assert_eq!(choose(&[local(1000, 5000), local(2000, 0), local(4000, 0)], 3, 0), 1);
        // Plenty of slack, but the slow class is busy: take the slowest free
        // one rather than wait for it while faster ones sit idle.
        let slow_frees_soon = [local(1000, 0), local(2000, 0), local(4000, 1000)];
        assert_eq!(choose(&slow_frees_soon, 3, 10_000), 1);
        // Every class busy: plenty of slack waits for the slow one.
        let all_busy = [local(1000, 500), local(2000, 500), local(4000, 1000)];
        assert_eq!(choose(&all_busy, 3, 10_000), 2);
        // No slack: wait for the fast class rather than take a free slow one
        // that finishes later.
        let fast_frees_soon = [local(1000, 500), local(2000, 10_000), local(4000, 0)];
        assert_eq!(choose(&fast_frees_soon, 3, 0), 0);
        let remote = |run_us| Cand { copy_us: 200, ..local(run_us, 0) };
        // A remote class no faster than the local one loses to moving files.
        assert_eq!(choose(&[local(1000, 0), remote(1000)], 1, 0), 0);
        // Slack keeps a job on a slow local class, but another box that
        // finishes it sooner wins anyway: it costs this box nothing.
        assert_eq!(choose(&[local(1000, 0), local(4000, 0), remote(1000)], 2, 10_000), 2);
    }

    #[test]
    fn heavy_jobs_go_where_they_finish_first() {
        let wide = |start_us, run_us, here: bool| Cand {
            start_us,
            copy_us: 0,
            run_us,
            spot: spot(),
            queue: None,
            basis: Basis::Place,
            place: if here {
                Place::Spread { layout: LayoutId::new(), class: 0 }
            } else {
                Place::Remote { remote: 0, class: None, shown_in: None, moves: Moves::default() }
            },
        };
        // No history: where it finishes soonest of the places with a token
        // free, else it waits here.
        assert_eq!(pick_wide(&[wide(0, 9000, true), wide(0, 10, false)], false), 1);
        assert_eq!(pick_wide(&[wide(0, 10, true), wide(0, 9000, false)], false), 0);
        assert_eq!(pick_wide(&[wide(500, 10, true), wide(0, 9000, false)], false), 1, "no size says waiting pays");
        assert_eq!(pick_wide(&[wide(500, 10, true)], false), 0, "it waits here");
        // History: wherever it finishes first, waiting included.
        assert_eq!(pick_wide(&[wide(0, 9000, true), wide(0, 1000, false)], true), 1);
        assert_eq!(pick_wide(&[wide(0, 900, true), wide(0, 1000, false)], true), 0);
        assert_eq!(pick_wide(&[wide(0, 9000, true), wide(500, 1000, false)], true), 1, "it waits for the other box");
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
        /// Counts the tasks runnable on the box, whatever runs there.
        runnable: Box<dyn Fn() -> Result<u32> + Send + Sync>,
        hashes: Arc<Hashes>,
        heartbeats: mpsc::UnboundedSender<Heartbeat>,
    }

    /// An idle box with `capacity_kb` that nothing else uses, and a token
    /// per CPU.
    fn room(capacity_kb: u64) -> Room {
        let memory = Box::new(move || Ok(BoxMemory { capacity_kb, used_kb: 0 }));
        Room {
            memory,
            tokens_per_cpu: 1.,
            runnable: Box::new(|| Ok(0)),
            hashes: Arc::new(Hashes::default()),
            heartbeats: mpsc::unbounded_channel().0,
        }
    }

    /// A scheduler on the state in `dir`, in the default profile.
    fn open(dir: &Path, room: Room) -> Result<(Sched, mpsc::UnboundedReceiver<Msg>, watch::Receiver<Load>)> {
        let setup = Setup {
            state_dir: dir.to_path_buf(),
            fifo_dir: dir.to_path_buf(),
            allowed: (0..4).collect(),
            memory: room.memory,
            runnable: room.runnable,
            hashes: room.hashes,
            heartbeats: room.heartbeats,
            tokens_per_cpu: room.tokens_per_cpu,
            calibration_seconds: 4,
            decay: Decay::default(),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let (load_tx, load) = watch::channel(Load::default());
        Ok((Sched::new(setup, Profile::DEFAULT, load_tx, tx)?, rx, load))
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
        let (mut sched, rx, load) = open(dir, room)?;
        for &(name, cost_mi, mem_kb) in costs {
            sched.stats.seed(&unit(name, &[]), 0, cost_mi, 100, mem_kb);
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

        fn submit_with(&self, name: &str, remote: Result<Want, &'static str>) -> oneshot::Receiver<Admission> {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Submit { unit: unit(name, &[]), build: 1, remote, reply }).unwrap();
            rx
        }

        fn submit(&self, name: &str) -> oneshot::Receiver<Admission> {
            self.submit_with(name, Err("the wrapper keeps it here"))
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
            Admission::Refused(why) => bail!("refused: {why}"),
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
    async fn a_job_with_time_to_spare_takes_the_slow_class_under_load_too() -> Result<()> {
        // big gates as much as it takes; leaf has 2 ms to spare at idle speed:
        // enough for the slow class, which takes 1 ms longer idle and 4.5 ms
        // longer at 4 tasks per cpu, when the build after leaf also takes 4.25
        // times as long.
        for runnable in [0, 16] {
            let dir = scratch(&format!("slack-{runnable}"));
            let room = Room { runnable: Box::new(move || Ok(runnable)), ..room(u64::MAX) };
            let classes = vec![class(1000, &[0, 1]), class(2000, &[2, 3])];
            let (mut s, _rx, _load) = sched(&dir, classes, room, &[("big", 3000, 1), ("leaf", 1000, 1)], &[])?;
            let mut rx = place(&mut s, &["big", "leaf"], false);
            let slots: Vec<Slot> = rx.iter_mut().map(granted).collect();
            assert_eq!((cpu(&slots[0]), cpu(&slots[1])), (0, 2), "{runnable} tasks runnable");
            drop((slots, s));
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_rank_counts_a_wide_job_at_the_threads_it_gets() -> Result<()> {
        let dir = scratch("rank-wide");
        let classes = vec![class(1000, &[0, 1, 2, 3])];
        let (mut s, _rx, _load) = sched(&dir, classes, room(u64::MAX), &[], &[("wide", &["gate"])])?;
        // gate takes 1 ms on a cpu; wide's 8 ms of work run on 4.
        history(&mut s, "gate", 1000, 100);
        history(&mut s, "wide", 8000, 400);
        let (reply, _rx) = oneshot::channel();
        s.submit(unit("gate", &[]), 1, Err("the wrapper keeps it here"), reply);
        let Build { rank_us, own_us, .. } = s.queue[0].build;
        assert_eq!((rank_us, own_us), (3000, 1000));
        let st = s.status();
        assert_eq!(st.units.iter().find(|u| u.key == "gate").and_then(|u| u.rank_ms), Some(3));
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_small_job_takes_a_free_fast_core_rather_than_wait_for_a_slow_one() -> Result<()> {
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
        assert_eq!(cpu(&first), 1, "with slack to spare, the slow cpu");
        let second = slot(h.submit("small")).await?;
        assert_eq!(cpu(&second), 0, "the slow cpu is busy and a fast one is free");
        drop((gate, first, second));
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
        let classes = vec![ClassState { rate: rate(calib_us), cpus, free, frees_in_ms: (free > 0).then_some(0) }];
        let state = BoxState { profile: "default".into(), version, toolchains, cache_gb: 16, load_c: 0, classes };
        (NPath::from("/farm/b/jobs"), state)
    }

    fn want() -> Result<Want, &'static str> {
        Ok(Want { key: "c h".into(), target: None, reads: Reads::Metadata, cwd: "/".into() })
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
    async fn short_jobs_go_away_once_no_local_slot_is_free() -> Result<()> {
        // 0.5 s here, 0.1 s there: too short to send while a slot is free here.
        let h = Harness::start("short", vec![class(1000, &[0])], u64::MAX, &[("small", 500_000, 1)], &[])?;
        h.farm(remote_box(200, 4, 4, 1));
        let here = slot(h.submit_with("small", want())).await?;
        match admitted(h.submit_with("small", want())).await? {
            Admission::Remote(r) => r.finish(RemoteOutcome::Busy { version: 2 }),
            _ => bail!("waited for the busy slot here"),
        }
        drop(here);
        // A box that would take longer than waiting here doesn't get it.
        let h = Harness::start("short-slow", vec![class(1000, &[0])], u64::MAX, &[("small", 500_000, 1)], &[])?;
        h.farm(remote_box(20_000, 4, 4, 1));
        let here = slot(h.submit_with("small", want())).await?;
        let mut waits = h.submit_with("small", want());
        assert!(pending(&mut waits).await);
        drop(here);
        slot(waits).await?;
        Ok(())
    }

    #[tokio::test]
    async fn short_jobs_go_away_when_memory_here_is_full() -> Result<()> {
        // A cpu is free here, but there is room for one job.
        let h = Harness::start("short-mem", vec![class(1000, &[0, 1])], 1000, &[("small", 500_000, 600)], &[])?;
        h.farm(remote_box(200, 4, 4, 1));
        let here = slot(h.submit_with("small", want())).await?;
        match admitted(h.submit_with("small", want())).await? {
            Admission::Remote(r) => r.finish(RemoteOutcome::Busy { version: 2 }),
            _ => bail!("ran here past its memory"),
        }
        drop(here);
        Ok(())
    }

    #[tokio::test]
    async fn remote_needs_a_matching_toolchain_and_a_free_slot() -> Result<()> {
        let h = Harness::start("nomatch", vec![class(1000, &[0, 1])], u64::MAX, &[("big", 5_000_000, 1)], &[])?;
        // "full" builds it in a fifth of the time, but has no token free and
        // says none will free.
        h.tx.send(Msg::Farm { name: "full".into(), state: Some(remote_box(200, 4, 0, 1)) })?;
        assert!(matches!(admitted(h.submit_with("big", want())).await?, Admission::Slot(_)));
        let other = Ok(Want { key: "other h".into(), target: None, reads: Reads::Metadata, cwd: "/".into() });
        h.farm(remote_box(200, 4, 4, 1));
        assert!(matches!(admitted(h.submit_with("big", other)).await?, Admission::Slot(_)));
        let target = Ok(Want { key: "c h".into(), target: Some("wasm32".into()), ..want().unwrap() });
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
        assert!(shown.classes[0].frees_in_ms.is_some(), "it predicts when the served job ends");
        let mut local = h.submit("l");
        assert!(pending(&mut local).await);
        let closed = h.load.borrow().clone();
        assert_eq!(closed.classes[0].frees_in_ms, None, "its own waiting job comes first");
        assert_eq!(closed.version, answer.version + 1, "and clients hear so at once");
        let refused = h.offer("r", Some(0)).await?;
        assert!(refused.slot.is_none(), "a local job is waiting");
        assert_eq!(refused.version, closed.version + 1, "a refusal is published too");
        drop(served);
        slot(local).await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_box_republishes_when_its_next_token_moves_from_what_clients_count_down_to() -> Result<()> {
        let dir = scratch("frees-in");
        let (mut s, _rx, load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        history(&mut s, "a", 10_000_000, 100);
        let slot = granted(&mut place(&mut s, &["a"], false)[0]);
        s.publish();
        let shown = || {
            let l = load.borrow();
            (l.version, l.classes[0].frees_in_ms.unwrap_or(u32::MAX))
        };
        let (version, ms) = shown();
        assert!((9_900..=10_000).contains(&ms), "it takes 10 s: {ms}");
        let later = |s: &mut Sched, by: Duration| {
            s.running[0].start -= by;
            s.published -= by;
            s.publish();
        };
        later(&mut s, Duration::from_secs(2));
        assert_eq!(shown(), (version, ms), "8 s left, as clients count down");
        // It runs 20 s over, and is expected to run 15 s more, where clients
        // count down to 0.
        later(&mut s, Duration::from_secs(28));
        let (republished, ms) = shown();
        assert_eq!(republished, version + 1);
        assert!((15_000..=15_100).contains(&ms), "half again as long as it has run: {ms}");
        drop((slot, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_prediction_is_republished_once_it_moves_a_share_and_a_step() {
        // Shown as 10 s, 2 s ago: clients count 8 s.
        assert!(!frees_in_moved(Some(10_000), 2_000, Some(8_000)));
        assert!(!frees_in_moved(Some(10_000), 2_000, Some(9_900)), "within a quarter");
        assert!(frees_in_moved(Some(10_000), 2_000, Some(11_000)));
        assert!(frees_in_moved(Some(10_000), 2_000, Some(5_000)));
        // Clients count down to 0, the box says 200 ms: too small a step.
        assert!(!frees_in_moved(Some(1_000), 5_000, Some(200)));
        assert!(frees_in_moved(Some(1_000), 5_000, Some(300)));
        assert!(frees_in_moved(None, 0, Some(0)), "it can say again");
        assert!(frees_in_moved(Some(0), 0, None));
    }

    #[tokio::test]
    async fn a_served_job_is_timed_without_its_linker_waiting() -> Result<()> {
        let h = Harness::start("linker-wait", vec![class(1000, &[0])], u64::MAX, &[], &[])?;
        let slot = h.offer("theirs", Some(0)).await?.slot.expect("a free slot");
        slot.finish(Ran { wall_us: 10_000_000, maxrss_kb: 1, front: None }, 9_000_000);
        let st = h.status().await;
        let u = st.units.iter().find(|u| u.key == "theirs").unwrap();
        assert!((990..=1010).contains(&u.wall_ms), "10 s, 9 of them waiting for rlibs: {}", u.wall_ms);
        Ok(())
    }

    #[tokio::test]
    async fn decisions_use_the_load_averaged_over_samples() -> Result<()> {
        let dir = scratch("load-average");
        let tasks = std::sync::Arc::new(AtomicU64::new(0));
        let seen = tasks.clone();
        let room = Room { runnable: Box::new(move || Ok(seen.load(Ordering::Relaxed) as u32)), ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room, &[], &[])?;
        let t0 = Instant::now();
        s.measure(t0);
        assert_eq!(s.load_c, 0);
        // One sample of 100 tasks per cpu barely moves it.
        tasks.store(400, Ordering::Relaxed);
        s.measure(t0 + MEASURE);
        assert!(s.load_c < 300, "{}", s.load_c);
        // Kept up for one time constant, it gets about 63% of the way.
        for i in 2..=40 {
            s.measure(t0 + MEASURE * i);
        }
        assert!((6000..6700).contains(&s.load_c), "{}", s.load_c);
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn status_shows_speeds_against_the_fastest_class_idle_and_work_in_seconds() -> Result<()> {
        let dir = scratch("status-speeds");
        // 4 tasks per cpu runnable; the second class is calibrated at half
        // speed, and b at five times.
        let room = Room { runnable: Box::new(|| Ok(16)), ..room(u64::MAX) };
        let classes = vec![class(1000, &[0, 1]), class(2000, &[2, 3])];
        let (mut s, _rx, _load) = sched(&dir, classes, room, &[("a", 3_000_000, 1)], &[])?;
        s.farm("b".into(), Some(remote_box(200, 4, 4, 1)));
        let st = s.status();
        assert_eq!(st.load_c, 400);
        let speeds = |c: &ClassStatus| (c.idle_speed_c, c.speed_c);
        assert_eq!((speeds(&st.classes[0]), speeds(&st.classes[1])), ((100, 25), (50, 13)));
        assert_eq!(st.boxes[0].classes[0].speed_c, 500);
        // 3 s of work on class 0 idle; now 4.25 times as long, with the
        // load its own thread adds.
        let a = st.units.iter().find(|u| u.key == "a").unwrap();
        assert_eq!((a.work_ms, a.now_ms), (3000, 12_750));
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn unreadable_state_is_moved_aside_before_starting_empty() -> Result<()> {
        let dir = scratch("unreadable");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        classes::save(&dir, &Profile::DEFAULT, &[class(1000, &[0])])?;
        for name in ["jobs.hdr", "graph"] {
            fs::write(dir.join(name), b"from a later version")?;
        }
        let (s, _rx, _load) = open(&dir, room(u64::MAX))?;
        assert_eq!((s.stats.units().count(), s.graph.len()), (0, 0));
        drop(s);
        for name in ["jobs.hdr", "graph"] {
            assert!(!dir.join(name).exists(), "{name} is still there to be saved over");
            assert_eq!(fs::read(dir.join(format!("{name}.unreadable")))?, b"from a later version");
        }
        // One set aside before is kept.
        fs::write(dir.join("graph"), b"another")?;
        drop(open(&dir, room(u64::MAX))?);
        assert_eq!(fs::read(dir.join("graph.unreadable"))?, b"from a later version");
        assert_eq!(fs::read(dir.join("graph.unreadable.2"))?, b"another");
        // A missing file is only empty.
        drop(open(&dir, room(u64::MAX))?);
        assert!(!dir.join("graph.unreadable.3").exists());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn status_measures_memory_before_any_job_runs() -> Result<()> {
        let h = Harness::start("status-mem", vec![class(1000, &[0])], 1000, &[], &[])?;
        assert_eq!(h.status().await.mem_capacity_kb, 1000);
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
        let room = Room { memory, runnable: Box::new(|| Ok(0)), ..room(0) };
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
        job.finish(Ran { wall_us: 1000, maxrss_kb: 1, front: None }, 0);
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
        s.layout.pool[0] -= 1;
        let place = Place::Local { layout: s.layout.id, class: 0 };
        let build = Build { pid: 1, rank_us: 10, own_us: 10 };
        let slot = s.admit(u.clone(), Some(build), pred, place, (s.layout.places.classes[0], 10), Instant::now())?;
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

    #[test]
    fn a_job_is_measured_at_the_load_while_its_rustc_ran() {
        let t0 = Instant::now();
        let m = Meter::new(100, t0);
        assert_eq!(m.measured(t0).load_c, 100, "not sampled yet: the load it was admitted at");
        // Its inputs arrive while the box gets busy.
        m.sample(900);
        m.sample(900);
        m.restart(t0 + Duration::from_secs(30));
        assert_eq!(m.measured(t0).load_c, 900, "too short to be sampled: the latest sample");
        m.sample(300);
        m.sample(500);
        assert_eq!(m.measured(t0).load_c, 400, "only what it saw once rustc started");
    }

    #[tokio::test]
    async fn a_job_counts_the_tokens_it_holds_over_time() -> Result<()> {
        let dir = scratch("tokens-held");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        let slot = granted(&mut place(&mut s, &["h"], false)[0]);
        let mut fifo = OpenOptions::new().read(true).write(true).open(slot.grant.jobserver.as_str())?;
        let t0 = s.layout.leases[0].meter.start;
        // It takes its spare, which the next tick sees: 1 token for 100 ms.
        // It is then lent 2 more, unread, so it holds 2 for 200 ms.
        fifo.read_exact(&mut [0u8; 1])?;
        s.layout.lend(t0 + Duration::from_millis(100));
        let measured = s.layout.release(slot.job, t0 + Duration::from_millis(300)).context("its lease")?;
        assert_eq!(measured.tokens_us, 500_000);
        assert_eq!(s.layout.pool[0], 4, "every token went back");
        drop((slot, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Run `name` once on box `b`, taking `wall_ms` there and `copy_ms`
    /// moving files, or return false if it ran here instead.
    fn run_on_b(s: &mut Sched, rx: &mut mpsc::UnboundedReceiver<Msg>, name: &str, wall_ms: u64, copy_ms: u64) -> bool {
        let r = match place(s, &[name], true)[0].try_recv() {
            Ok(Admission::Remote(r)) => r,
            Ok(Admission::Slot(_)) => return false,
            _ => panic!("not placed"),
        };
        let wall_us = wall_ms * 1000;
        let ran = Ran { wall_us, maxrss_kb: 1, front: None };
        let measured = Measured { tokens_us: wall_us, load_c: 0 };
        let transfer = Transfer { sent_kb: 1, received_kb: 0, copy_ms, links: false };
        r.finish(RemoteOutcome::Done(SentRun { ran, measured, linker_waited_us: 0, transfer, kept: vec![] }));
        while let Ok(msg) = rx.try_recv() {
            s.handle(msg).unwrap();
        }
        true
    }

    #[tokio::test]
    async fn a_box_slower_than_its_calibration_on_the_same_units_stops_getting_them() -> Result<()> {
        let dir = scratch("slow-box");
        let (mut s, mut rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        // b is calibrated five times as fast: 5 s here, 1 s there. It takes
        // 20, and once it has, that run predicts the next there.
        s.farm("b".into(), Some(remote_box(200, 4, 4, 1)));
        history(&mut s, "big", 5_000_000, 100);
        let sent = (0..30).take_while(|_| run_on_b(&mut s, &mut rx, "big", 20_000, 0)).count();
        assert_eq!(sent, 1);
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn the_time_a_unit_spent_moving_files_counts_against_sending_it() -> Result<()> {
        let dir = scratch("copy-counts");
        let (mut s, mut rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        // 5 s here, 1 s there, but its files took 10 s to move.
        s.farm("b".into(), Some(remote_box(200, 4, 4, 1)));
        history(&mut s, "big", 5_000_000, 100);
        assert!(run_on_b(&mut s, &mut rx, "big", 1000, 10_000));
        assert!(!run_on_b(&mut s, &mut rx, "big", 1000, 10_000), "it stays here");
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_jobs_files_cross_the_link_from_when_it_is_sent_and_until_it_ends() {
        let m = Moves { inputs_us: 100, outputs_us: 50 };
        assert_eq!(m.spans(0, 1000), [(0, 100), (950, 1000)]);
        assert_eq!(m.spans(0, 120), [(0, 120), (0, 0)], "they meet: one span");
        assert_eq!(m.spans(-50, 500), [(-50, 50), (450, 500)], "sent before now");
        assert_eq!(overlap_us(&m.spans(0, 1000), &m.spans(0, 1000)), 150);
        assert_eq!(overlap_us(&m.spans(0, 1000), &Moves::default().spans(0, 1000)), 0, "nothing to move");
    }

    #[tokio::test]
    async fn jobs_sent_at_once_share_the_link() -> Result<()> {
        let dir = scratch("share-link");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        // 2 s here, 0.4 s on b. Sends there move 10 µs a kB: "big" gets
        // 100 MB back, a second of the link; "small" gets 1 MB back.
        s.farm("b".into(), Some(remote_box(200, 32, 32, 1)));
        let at = s.remotes[0].state.as_ref().unwrap().1.classes[0];
        for (name, received_kb, copy_ms) in [("big", 100_000, 1000), ("small", 1000, 10)] {
            history(&mut s, name, 2_000_000, 100);
            for _ in 0..4 {
                let transfer = Transfer { sent_kb: 0, received_kb, copy_ms, links: false };
                let run = Run {
                    place: at,
                    load_c: 0,
                    wall_us: 400_000,
                    front_us: None,
                    tokens_us: 400_000,
                    mem_kb: 1,
                    origin: Origin::Sent(transfer),
                };
                s.stats.record(&unit(name, &[]), &run);
            }
        }
        let sent = |rx: Vec<oneshot::Receiver<Admission>>| {
            rx.into_iter().map(|mut rx| matches!(rx.try_recv(), Ok(Admission::Remote(_)))).collect::<Vec<_>>()
        };
        // The first big one goes: 1.4 s there against 2 here. The next would
        // get its 100 MB back while the first does, 2.4 s, so it stays.
        assert_eq!(sent(place(&mut s, &["big", "big", "big"], true)), [true, false, false]);
        // While the first is on its way, a fourth stays too.
        assert_eq!(sent(place(&mut s, &["big"], true)), [false]);
        // A megabyte each barely shares the link, even with the big one's.
        assert_eq!(sent(place(&mut s, &["small", "small", "small"], true)), [true, true, true]);
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_librarys_first_send_is_not_charged_what_links_took() -> Result<()> {
        let dir = scratch("copy-kinds");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        // 5 s here, 1 s there; binaries sent there took 10 s moving rlibs.
        s.farm("b".into(), Some(remote_box(200, 4, 4, 1)));
        history(&mut s, "lib", 5_000_000, 100);
        let at = s.remotes[0].state.as_ref().unwrap().1.classes[0];
        let transfer = Transfer { sent_kb: 300_000, received_kb: 0, copy_ms: 10_000, links: true };
        let run = Run {
            place: at,
            load_c: 0,
            wall_us: 1_000_000,
            front_us: None,
            tokens_us: 1_000_000,
            mem_kb: 1,
            origin: Origin::Sent(transfer),
        };
        s.stats.record(&unit("bin", &[]), &run);
        let mut rx = place(&mut s, &["lib"], true);
        assert!(matches!(rx[0].try_recv(), Ok(Admission::Remote(_))), "sent despite the binaries' 10 s");
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_job_goes_to_the_box_that_holds_its_inputs() -> Result<()> {
        let dir = scratch("follow-inputs");
        let hashes = Arc::new(Hashes::default());
        let room = Room { hashes: hashes.clone(), ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room, &[], &[])?;
        // Below the job, 50 MB of metadata, which c holds and b doesn't.
        let meta = dir.join("libdep.rmeta");
        std::fs::File::create(&meta)?.set_len(50 << 20)?;
        let (hash, len) = hashes.get(&meta)?;
        s.graph.record(&unit("dep", &[]));
        let mut job = unit("job", &[]);
        job.deps = vec![crate::protocol::Dep { id: "dep".into(), file: Some(meta.to_str().unwrap().into()) }];
        // 5 s here, 1 s on b or c.
        history(&mut s, "job", 5_000_000, 100);
        for (name, jobs) in [("b", "/farm/b/jobs"), ("c", "/farm/c/jobs")] {
            let (_, state) = remote_box(200, 4, 4, 1);
            s.farm(name.into(), Some((NPath::from(jobs), state)));
        }
        s.remotes[1].held.used(&[(hash, len.div_ceil(1024))], 16 << 20);
        let (reply, mut rx) = oneshot::channel();
        s.submit(job, 1, want(), reply);
        for w in &mut s.queue {
            w.since -= GATHER;
        }
        s.dispatch();
        match rx.try_recv() {
            Ok(Admission::Remote(r)) => assert_eq!(r.jobs, NPath::from("/farm/c/jobs")),
            _ => bail!("not sent"),
        }
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_placement_line_says_where_a_job_could_go_and_why_not_elsewhere() -> Result<()> {
        let dir = scratch("placement-line");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        for (name, toolchain) in [("b", true), ("c", false)] {
            let (jobs, mut state) = remote_box(200, 4, 4, 1);
            if !toolchain {
                state.toolchains.clear();
            }
            s.farm(name.into(), Some((jobs, state)));
        }
        let (reply, _rx) = oneshot::channel();
        s.submit(unit("job", &[]), 1, want(), reply);
        let (reply, _rx) = oneshot::channel();
        s.submit(unit("stays", &[]), 1, Err("the wrapper keeps it here"), reply);
        let cand = |start_us, copy_us, run_us, place, basis| Cand {
            start_us,
            copy_us,
            run_us,
            spot: spot(),
            place,
            queue: None,
            basis,
        };
        let here = Place::Spread { layout: s.layout.id, class: 0 };
        let b = Place::Remote { remote: 0, class: None, shown_in: None, moves: Moves::default() };
        let cands = [cand(0, 0, 5_000_000, here, Basis::Place), cand(250_000, 400_000, 1_000_000, b, Basis::Size)];
        let (job, stays) = (&s.queue[0], &s.queue[1]);
        let away = job.portable.as_ref().map_err(|why| anyhow::anyhow!("{why}"))?;
        assert_eq!(
            s.placement(job, 1_000_000, true, Ok(away), &cands, 1).as_str(),
            "job (never ran; 1.00 s on one thread of the fastest class here): here/all 0.00+0.00+5.00 [own] \
             *b/all 0.25+0.40+1.00 [est]; not c: it lacks the toolchain"
        );
        assert_eq!(
            s.placement(stays, 1_000_000, false, Err("it can't go to another box"), &cands[1..], 0).as_str(),
            "stays (never ran; 1.00 s on one thread of the fastest class here): *b/all 0.25+0.40+1.00 [est]; \
             not here: its memory doesn't fit; no other box: it can't go to another box"
        );
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_job_that_cant_go_to_another_box_says_why() -> Result<()> {
        let dir = scratch("why-here");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        let why = |s: &mut Sched, job: Unit| {
            let (reply, _rx) = oneshot::channel();
            s.submit(job, 1, want(), reply);
            let w = s.queue.pop().expect("queued");
            w.portable.err().map(|why| why.to_string())
        };
        let mut job = unit("job", &[]);
        job.deps = vec![crate::protocol::Dep { id: "dep-x1".into(), file: Some("/t/libdep-x1.rmeta".into()) }];
        assert_eq!(why(&mut s, job).as_deref(), Some("dep-x1, a crate below it, was never seen compiling"));
        assert_eq!(why(&mut s, unit("flaky", &[])), None);
        for _ in 0..PIN_AFTER {
            s.handle(Msg::Mismatch { unit: unit("flaky", &[]) })?;
        }
        assert_eq!(why(&mut s, unit("flaky", &[])).as_deref(), Some("it failed on another box and then built here 2 times"));
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn what_a_box_holds_is_kept_to_its_cache_size_least_recently_used_first() {
        let h = |n: u8| Hash([n; 16]);
        let mut held = Held::default();
        held.used(&[(h(1), 40), (h(2), 40)], 100);
        held.used(&[(h(1), 40)], 100);
        held.used(&[(h(3), 40)], 100);
        assert_eq!(held.missing_kb(&[(Some(h(1)), 40), (Some(h(2)), 40), (Some(h(3)), 40), (None, 5)]), 45);
    }

    #[tokio::test]
    async fn an_idle_box_gets_a_heartbeat_only_while_jobs_wait_here() -> Result<()> {
        let dir = scratch("heartbeat");
        let (heartbeats, mut sent) = mpsc::unbounded_channel();
        let room = Room { heartbeats, ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0])], room, &[], &[])?;
        // b is a hundred times slower: jobs wait for this box rather than go.
        s.farm("b".into(), Some(remote_box(100_000, 4, 4, 1)));
        history(&mut s, "big", 5_000_000, 100);
        let _first = granted(&mut place(&mut s, &["big"], true)[0]);
        assert!(sent.try_recv().is_err(), "no job waits");
        let mut second = place(&mut s, &["big"], true);
        assert!(second[0].try_recv().is_err(), "it waits here");
        let hb = sent.try_recv().context("a heartbeat")?;
        assert_eq!((hb.name.as_str(), hb.kb), ("b", 512));
        s.dispatch();
        assert!(sent.try_recv().is_err(), "at most one a minute");
        s.remotes[0].heartbeat = Some(Instant::now() - HEARTBEAT_EVERY);
        s.dispatch();
        assert_eq!(sent.try_recv().context("the next heartbeat")?.kb, 1024);
        // What it measured is a send there.
        s.handle(Msg::Heartbeat { name: "b".into(), kb: 1024, us: 50_000 })?;
        let at = s.remotes[0].state.as_ref().unwrap().1.all;
        assert_eq!(s.stats.moving(&unit("big", &[]), at, false, 1024).copy_us, 50_000);
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn what_places_learned_outlives_the_daemon_until_recalibration() -> Result<()> {
        let dir = scratch("learned");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0]), class(2000, &[1])], room(u64::MAX), &[], &[])?;
        let (fast, slow) = (s.layout.places.classes[0], s.layout.places.classes[1]);
        // The slow class is calibrated at half speed but takes six times as long.
        let run = |place, ms: u64| Run {
            place,
            load_c: 0,
            wall_us: ms * 1000,
            front_us: None,
            tokens_us: ms * 1000,
            mem_kb: 1,
            origin: Origin::Here,
        };
        for _ in 0..100 {
            s.stats.record(&unit("u", &[]), &run(fast, 1000));
            s.stats.record(&unit("u", &[]), &run(slow, 6000));
        }
        let ratio = |s: &Sched| s.stats.factor(slow, 0) / s.stats.factor(fast, 0);
        let learned = ratio(&s);
        assert!(learned > 2.5, "{learned}");
        s.save();
        drop(s);
        let (s, _rx, _load) = open(&dir, room(u64::MAX))?;
        assert_eq!(s.layout.places.classes[1], slow);
        assert!((ratio(&s) - learned).abs() < 0.01, "{} {learned}", ratio(&s));
        drop(s);
        classes::save(&dir, &Profile::DEFAULT, &[class(1000, &[0]), class(2500, &[1])])?;
        let (s, _rx, _load) = open(&dir, room(u64::MAX))?;
        assert_eq!(s.stats.factor(slow, 0), 1., "a new calibration starts over");
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn the_file_hashes_are_saved_before_the_scheduler_stops() -> Result<()> {
        let dir = scratch("hashes-saved");
        let (s, rx, _load) = sched(&dir, vec![class(1000, &[0])], room(u64::MAX), &[], &[])?;
        let (hashes, tx) = (s.setup.hashes.clone(), s.tx.clone());
        let file = dir.join("input");
        fs::write(&file, b"crate")?;
        let (hash, _) = hashes.get(&file)?;
        tx.send(Msg::Drain)?;
        s.run(rx).await;
        let saved = Hashes::load(&dir.join("hashes"))?;
        assert_eq!(saved.known(&file), Some((Some(hash), 5)));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// History for a unit of `cost_mi` that keeps `width_c` hundredths of a
    /// thread busy.
    fn history(s: &mut Sched, name: &str, cost_mi: u64, width_c: u64) {
        s.stats.seed(&unit(name, &[]), 0, cost_mi, width_c, 1);
    }

    /// Submit jobs and place them in one dispatch, as if they had waited
    /// out the gathering window.
    fn place(s: &mut Sched, names: &[&str], portable: bool) -> Vec<oneshot::Receiver<Admission>> {
        let replies = names
            .iter()
            .map(|name| {
                let (reply, rx) = oneshot::channel();
                s.submit(unit(name, &[]), 1, if portable { want() } else { Err("the wrapper keeps it here") }, reply);
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
    async fn a_job_never_run_is_as_wide_as_the_median_unit_not_its_codegen_units() -> Result<()> {
        let dir = scratch("never-run-width");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        // Every unit a second on one thread; a new one has 16 codegen units.
        for name in ["a", "b", "c"] {
            history(&mut s, name, 1_000_000, 100);
        }
        let place = Place::Spread { layout: s.layout.id, class: 0 };
        let known = s.estimate(&unit("a", &[]), s.local(place), 0, false).run_us;
        let new = s.estimate(&unit("new", &[]), s.local(place), 0, false).run_us;
        assert!(new.abs_diff(known) * 50 <= known, "a second, not a quarter of one on each cpu: {new} {known}");
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
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
        }
        for slot in &slots {
            assert!(slot.grant.jobserver.contains("/job-"), "a jobserver of its own: {}", slot.grant.jobserver);
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
        let tokens = |s: &Sched| (s.layout.pool[0], s.layout.leases[0].fifo.free());
        assert_eq!(tokens(&s), (3, 1), "one held without reading, and a spare on credit");
        // Codegen starts: it takes the spare, which is backed, and is lent
        // twice what it holds.
        fifo.read_exact(&mut [0u8; 1])?;
        s.layout.lend(Instant::now());
        assert_eq!(tokens(&s), (0, 2));
        fifo.read_exact(&mut [0u8; 2])?;
        s.layout.lend(Instant::now());
        assert_eq!(s.layout.lent(0), 4, "the class had no more");
        // Three threads finish. Spares left unused a whole tick go back, but
        // one, which stays on credit.
        fifo.write_all(b"+++")?;
        s.layout.lend(Instant::now());
        assert_eq!(tokens(&s), (0, 3), "not a whole tick yet");
        s.layout.lend(Instant::now());
        assert_eq!(tokens(&s), (3, 1));
        // When it ends, everything it borrowed goes back.
        let path = PathBuf::from(slot.grant.jobserver.as_str());
        s.finished(slot.job, &slot.unit, None);
        assert_eq!(s.layout.pool[0], 4);
        assert!(!path.exists());
        drop((slot, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_light_job_borrows_only_from_its_class() -> Result<()> {
        let dir = scratch("light-lease");
        let classes = vec![class(1000, &[0, 1]), class(2000, &[2, 3])];
        let (mut s, _rx, _load) = sched(&dir, classes, room(u64::MAX), &[], &[])?;
        history(&mut s, "narrow", 10, 100);
        let slot = granted(&mut place(&mut s, &["narrow"], false)[0]);
        let mut fifo = OpenOptions::new().read(true).write(true).open(slot.grant.jobserver.as_str())?;
        fifo.read_exact(&mut [0u8; 1])?;
        s.layout.lend(Instant::now());
        assert_eq!(&s.layout.pool[..], &[0, 2]);
        assert_eq!(&s.layout.leases[0].lent[..], &[2, 0]);
        drop((slot, s));
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
    async fn a_heavy_job_waits_for_a_busy_box_that_finishes_it_sooner() -> Result<()> {
        let dir = scratch("wait-for-box");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        history(&mut s, "wide", 5_000_000, 400);
        // b is five times as fast as this box but busy; c is free but slower.
        let busy = |frees_in_ms, version| {
            let (jobs, mut state) = remote_box(200, 32, 0, version);
            state.classes[0].frees_in_ms = Some(frees_in_ms);
            (jobs, state)
        };
        let (_, slow) = remote_box(5000, 36, 36, 1);
        s.farm("c".into(), Some((NPath::from("/farm/c/jobs"), slow)));
        // Here it takes 1.25 s on 4 cpus, which keep a token free after the
        // first, on c 6.45 s, on b 0.45 s once a token frees.
        s.farm("b".into(), Some(busy(10_000, 1)));
        let mut late = place(&mut s, &["wide"], true);
        assert!(matches!(late[0].try_recv(), Ok(Admission::Slot(_))), "b frees too late: here");
        s.farm("b".into(), Some(busy(300, 2)));
        let mut soon = place(&mut s, &["wide"], true);
        assert!(soon[0].try_recv().is_err(), "it waits for b");
        s.farm("b".into(), Some(remote_box(200, 32, 32, 3)));
        s.dispatch();
        match soon[0].try_recv() {
            Ok(Admission::Remote(r)) => {
                assert_eq!(r.jobs, NPath::from("/farm/b/jobs"));
                r.finish(RemoteOutcome::Busy { version: 4 });
            }
            _ => bail!("not sent to b once it had a token"),
        }
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_heavy_job_counts_a_box_by_its_cpus_not_its_free_tokens() -> Result<()> {
        let dir = scratch("cpus-not-tokens");
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1])], room(u64::MAX), &[], &[])?;
        history(&mut s, "wide", 5_000_000, 400);
        // b is as fast as this box and has one token free, but 32 cpus.
        s.farm("b".into(), Some(remote_box(1000, 32, 1, 1)));
        let mut rx = place(&mut s, &["wide"], true);
        assert!(matches!(rx[0].try_recv(), Ok(Admission::Remote(_))), "4 threads there beat 2 here");
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
        assert!(slot.grant.jobserver.contains("/job-"));
        let st = h.status().await;
        assert_eq!(st.spread.serving, 1);
        assert_eq!((st.classes[0].lent, st.classes[1].lent), (1, 0), "from the fast class");
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
        // Its codegen takes the spare and is lent more, then gives them back.
        let mut fifo = OpenOptions::new().read(true).write(true).open(first.grant.jobserver.as_str())?;
        fifo.read_exact(&mut [0u8; 1])?;
        h.published_free(0).await?;
        fifo.write_all(b"+")?;
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
        // 5 s here, 3.2 s there with its files: too long to wait for the
        // token the first takes.
        let h = Harness::start("unseen", vec![class(1000, &[0])], u64::MAX, &[("big", 5_000_000, 1)], &[])?;
        h.farm(remote_box(600, 4, 1, 1));
        let Admission::Remote(sent) = admitted(h.submit_with("big", want())).await? else {
            bail!("not sent remote");
        };
        let here = slot(h.submit_with("big", want())).await?;
        let mut third = h.submit_with("big", want());
        assert!(pending(&mut third).await, "b's one free token is taken");
        sent.accepted(5);
        h.farm(remote_box(600, 4, 1, 4));
        assert!(pending(&mut third).await, "version 4 doesn't count the job yet");
        h.farm(remote_box(600, 4, 0, 5));
        assert!(pending(&mut third).await, "version 5 counts it");
        h.farm(remote_box(600, 4, 1, 6));
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
    async fn a_wave_is_timed_as_dispatch_placed_it() -> Result<()> {
        let dir = scratch("wave-timed");
        // 2 tasks per cpu runnable; each job placed adds a quarter of one.
        let room = Room { runnable: Box::new(|| Ok(8)), ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room, &[], &[])?;
        history(&mut s, "narrow", 1_000_000, 100);
        let mut rx = place(&mut s, &["narrow"; 4], false);
        let slots: Vec<Slot> = rx.iter_mut().map(granted).collect();
        let mut run_us: Vec<u64> = s.running.iter().map(|r| r.run_us).collect();
        run_us.sort_unstable();
        assert_eq!(run_us, [2_250_000, 2_500_000, 2_750_000, 3_000_000], "1 s calibrated, counting the jobs before it");
        drop((slots, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A box `b` with one class, `free` tokens, and one predicted to free in
    /// `frees_in_ms`.
    fn busy_box(calib_us: u64, cpus: u32, free: u32, frees_in_ms: u32, version: u64) -> (NPath, BoxState) {
        let (jobs, mut state) = remote_box(calib_us, cpus, free, version);
        state.classes[0].frees_in_ms = Some(frees_in_ms);
        (jobs, state)
    }

    #[tokio::test]
    async fn light_jobs_wait_for_a_busy_box_rather_than_run_slower_here() -> Result<()> {
        let dir = scratch("wait-light");
        // Tests keep 12.5 tasks per cpu runnable here, so a job takes 12.75
        // times its calibrated 1 s, and more as more run. b is as fast and
        // idle but full: a token frees in 1.5 s, and each job takes 1.2 s
        // there, moving files included.
        let room = Room { runnable: Box::new(|| Ok(50)), ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room, &[], &[])?;
        s.farm("b".into(), Some(busy_box(1000, 1, 0, 1500, 1)));
        history(&mut s, "narrow", 1_000_000, 100);
        let mut rx = place(&mut s, &["narrow"; 12], true);
        // The tenth to wait would finish after 1.5 + 9 × 1.2 + 1.2 = 13.5 s:
        // it and the rest run here, on the 4 tokens free.
        let here = rx.iter_mut().map(|r| r.try_recv()).filter(|a| matches!(a, Ok(Admission::Slot(_)))).count();
        assert_eq!(here, 3, "nine wait for b, each behind the ones before it");
        assert_eq!(s.queue.len(), 9);
        // b frees a token: the first goes.
        s.farm("b".into(), Some(remote_box(1000, 1, 1, 2)));
        s.dispatch();
        assert_eq!(s.queue.len(), 8);
        drop(s);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_job_with_no_history_goes_where_it_runs_fastest_now() -> Result<()> {
        let dir = scratch("unknown-load");
        // A token is free here, but tests keep 12.5 tasks per cpu runnable.
        let room = Room { runnable: Box::new(|| Ok(50)), ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room, &[], &[])?;
        s.farm("b".into(), Some(remote_box(1000, 4, 4, 1)));
        // What other units take says a new one takes 5 s on a cpu.
        history(&mut s, "other", 5_000_000, 100);
        let mut rx = place(&mut s, &["new"], true);
        assert!(matches!(rx[0].try_recv(), Ok(Admission::Remote(_))), "b is idle");
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
        let ran = || Ran { wall_us: 1000, maxrss_kb: 1, front: None };
        h.offer("theirs", Some(0)).await?.slot.expect("a free slot").finish(ran(), 0);
        slot(h.submit("mine")).await?.finish(ran(), 0);
        h.farm(remote_box(200, 4, 4, 1));
        let Admission::Remote(r) = admitted(h.submit_with("big", want())).await? else {
            bail!("not sent remote");
        };
        let transfer = Transfer { sent_kb: 300, received_kb: 0, copy_ms: 40, links: false };
        let measured = Measured::default();
        r.finish(RemoteOutcome::Done(SentRun { ran: ran(), measured, linker_waited_us: 0, transfer, kept: vec![] }));
        let st = h.status().await;
        let unit = |key: &str| st.units.iter().find(|u| u.key == key).unwrap();
        let runs = |key: &str| {
            let u = unit(key);
            (u.here, u.sent, u.served, u.rank_ms.is_some())
        };
        assert_eq!(runs("theirs"), (0, 0, 1, false), "not in this box's graph");
        assert_eq!(runs("mine"), (1, 0, 0, true));
        assert_eq!(runs("big"), (0, 1, 0, true), "its history was seeded");
        // Histograms keep two significant figures.
        assert_eq!(unit("big").copy_ms, Some(40));
        assert!(unit("big").sent_kb.is_some_and(|kb| (297..=303).contains(&kb)), "{:?}", unit("big").sent_kb);
        assert_eq!(unit("mine").copy_ms, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_killed_rustc_leaks_no_tokens() -> Result<()> {
        let h = Harness::start("leak", vec![class(1000, &[0, 1])], u64::MAX, &[("a", 10, 1)], &[])?;
        let job = slot(h.submit("a")).await?;
        // A rustc that dies holding a codegen token never gives it back.
        let mut fifo = OpenOptions::new().read(true).write(true).open(job.grant.jobserver.as_str())?;
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
        // The old job holds a token on the cpus battery's classes cover, so
        // battery's slowest class is a token short until it ends.
        let new = slot(h.submit("a")).await?;
        assert_eq!(cpu(&new), 0);
        let mut third = h.submit("a");
        assert!(pending(&mut third).await, "no token is left");
        let st = h.status().await;
        assert_eq!(st.profile.as_str(), "battery");
        assert_eq!(st.classes.len(), 2);
        assert_eq!(st.finishing, 1);
        let fifos = || {
            fs::read_dir(&h.dir).unwrap().filter(|e| {
                e.as_ref().unwrap().file_name().to_string_lossy().starts_with("job-")
            }).count()
        };
        assert_eq!(fifos(), 2);
        drop(old);
        let third = slot(third).await?;
        assert_eq!(cpu(&third), 1, "the old job's token went to the class it was taken from");
        assert_eq!(h.status().await.finishing, 0);
        assert_eq!(fifos(), 2, "the finished job's jobserver is removed");
        assert_eq!(fs::read_to_string(h.dir.join("profile"))?, "battery");
        drop((new, third));
        Ok(())
    }

    #[tokio::test]
    async fn jobs_running_at_a_switch_borrow_nothing_more_and_what_they_leave_goes_to_the_new_classes() -> Result<()> {
        let dir = scratch("switch-tokens");
        // 12 tokens on 4 cpus; battery has 6 on its fast class and 6 on its slow one.
        let room = Room { tokens_per_cpu: 3., ..room(u64::MAX) };
        let (mut s, _rx, _load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room, &[], &[])?;
        classes::save(&dir, &Profile::new("battery")?, &[class(1000, &[0, 1]), class(2000, &[2, 3])])?;
        let slot = granted(&mut place(&mut s, &["h"], false)[0]);
        let mut fifo = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(slot.grant.jobserver.as_str())?;
        // Codegen takes every token it is lent, twice what it holds each tick,
        // till it holds 8.
        for n in [1, 2, 4] {
            fifo.read_exact(&mut vec![0u8; n])?;
            if n < 4 {
                s.lend(Instant::now());
            }
        }
        s.switch_to(Profile::new("battery")?, false)?;
        let tokens = |s: &Sched| (s.layout.pool.to_vec(), s.layout.owed.to_vec());
        assert_eq!(tokens(&s), (vec![4, 0], vec![2, 6]), "its 8 come off the slow class first");
        s.lend(Instant::now());
        assert_eq!(s.retired[0].leases[0].fifo.free(), 0, "the old classes' 4 free tokens are lent no more");
        // Five threads finish; a whole tick later they are back, and go to
        // battery's classes, fastest first.
        fifo.write_all(b"+++++")?;
        s.lend(Instant::now());
        s.lend(Instant::now());
        assert_eq!(tokens(&s), (vec![6, 3], vec![0, 3]));
        s.finished(slot.job, &slot.unit, None);
        assert_eq!(tokens(&s), (vec![6, 6], vec![0, 0]));
        assert!(s.retired.is_empty());
        drop((slot, s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_class_owed_tokens_frees_one_when_the_old_jobs_have_given_back_what_comes_first() -> Result<()> {
        let dir = scratch("switch-owed");
        let (mut s, _rx, load) = sched(&dir, vec![class(1000, &[0, 1, 2, 3])], room(u64::MAX), &[], &[])?;
        let names = ["a", "b", "c", "d"];
        for (name, secs) in names.iter().zip([1, 50, 100, 200]) {
            history(&mut s, name, secs * 1_000_000, 100);
        }
        let slots: Vec<Slot> = place(&mut s, &names, false).iter_mut().map(granted).collect();
        // Battery has a token on its fast class and one on its slow one;
        // the old jobs hold four.
        classes::save(&dir, &Profile::new("battery")?, &[class(1000, &[0]), class(2000, &[1])])?;
        s.switch_to(Profile::new("battery")?, false)?;
        let tokens = |s: &Sched| (s.layout.pool.to_vec(), s.layout.owed.to_vec(), s.layout.overdrawn);
        assert_eq!(tokens(&s), (vec![0, 0], vec![1, 1], 2));
        let run_us = |s: &Sched, i: usize| s.running.iter().find(|r| r.job == slots[i].job).unwrap().run_us;
        let near = |got: Option<u64>, want: u64| got.is_some_and(|us| us.abs_diff(want) < 100_000);
        // The first two tokens back pay what is overdrawn, the third the
        // fast class, the fourth the slow one.
        let now = Instant::now();
        assert!(near(s.frees_in_us(0, now), run_us(&s, 2)), "{:?}", s.frees_in_us(0, now));
        assert!(near(s.frees_in_us(1, now), run_us(&s, 3)), "{:?}", s.frees_in_us(1, now));
        s.publish();
        let shown = load.borrow().classes.iter().map(|c| c.frees_in_ms.unwrap_or(0) as u64 * 1000).collect::<Vec<_>>();
        assert!(near(Some(shown[0]), run_us(&s, 2)) && near(Some(shown[1]), run_us(&s, 3)), "{shown:?}");
        s.finished(slots[0].job, &slots[0].unit, None);
        assert_eq!(tokens(&s), (vec![0, 0], vec![1, 1], 1));
        let now = Instant::now();
        assert!(near(s.frees_in_us(0, now), run_us(&s, 2)), "the same jobs bring each class its token");
        assert!(near(s.frees_in_us(1, now), run_us(&s, 3)));
        drop((slots, s));
        fs::remove_dir_all(&dir)?;
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
        let shown = h.load.borrow().clone();
        assert_eq!(shown.profile.as_str(), "default", "the classes it shows are still default's");
        assert_eq!(shown.classes[0].free, 0, "it takes no offers meanwhile");
        // Stop before the calibration itself would start.
        drop(h);
        drop(running);
        Ok(())
    }
}
