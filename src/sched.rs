use crate::{
    calibrate,
    classes::{self, Class, Profile},
    protocol::{ClassStatus, Ran, Reply, Status, Switching, Unit, frame},
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
    task, time,
};

/// A waiting job's priority doubles every this many seconds, so light
/// jobs are not starved by a stream of heavy ones.
const AGING_S: f64 = 10.;
const SAVE_EVERY: Duration = Duration::from_secs(60);

atomic_id!(LayoutId);

pub enum Msg {
    Submit { unit: Unit, reply: oneshot::Sender<Admission> },
    Finished { layout: LayoutId, class: usize, mem_kb: u64, unit: Unit, ran: Option<Ran> },
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
    layout: LayoutId,
    class: usize,
    mem_kb: u64,
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
            layout: self.layout,
            class: self.class,
            mem_kb: self.mem_kb,
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
    /// job is done, so the calibration sees a quiet box.
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
    pred: Prediction,
    ideal: usize,
    since: Instant,
    reply: oneshot::Sender<Admission>,
}

pub struct Sched {
    setup: Setup,
    profile: Profile,
    layout: Layout,
    /// Layouts from earlier profiles that still have running jobs.
    retired: Vec<Layout>,
    switch: Switch,
    mem_running_kb: u64,
    queue: Vec<Waiting>,
    /// The last dispatch ran out of tokens with admissible jobs waiting.
    starved: bool,
    stats: Stats,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Sched {
    /// Start in `profile`. If it was never calibrated, calibrate before
    /// admitting anything.
    pub fn new(setup: Setup, profile: Profile, tx: mpsc::UnboundedSender<Msg>) -> Result<Self> {
        let stats_path = setup.state_dir.join("jobs.hdr");
        let stats = Stats::load(&stats_path).unwrap_or_else(|e| {
            warn!("starting with empty job stats: {e:#}");
            Stats::new()
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
            layout,
            retired: Vec::new(),
            switch,
            mem_running_kb: 0,
            queue: Vec::new(),
            starved: false,
            stats,
            tx,
        })
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Msg>) {
        enum Event {
            Msg(Option<Msg>),
            Tokens,
            Save,
        }
        let mut save = time::interval(SAVE_EVERY);
        loop {
            self.advance();
            let starved = self.starved;
            let event = tokio::select! {
                msg = rx.recv() => Event::Msg(msg),
                _ = std::future::poll_fn(|cx| self.poll_tokens(cx)), if starved => Event::Tokens,
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
                Event::Tokens => (),
                Event::Save => self.save(),
            }
            self.dispatch();
        }
        for w in self.queue.drain(..) {
            let _ = w.reply.send(Admission::Restarting);
        }
        self.save();
    }

    fn poll_tokens(&self, cx: &mut TaskContext) -> Poll<()> {
        let mut ready = false;
        for fifo in &self.layout.fifos {
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
            Msg::Submit { unit, reply } => {
                let pred = self.stats.predict(&unit);
                let ideal = ideal_class(&self.layout.classes, pred.percentile);
                self.queue.push(Waiting { unit, pred, ideal, since: Instant::now(), reply });
            }
            Msg::Finished { layout, class, mem_kb, unit, ran } => {
                self.finished(layout, class, mem_kb, &unit, ran)
            }
            Msg::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Msg::Profile { profile, recalibrate } => self.switch_to(profile, recalibrate)?,
            Msg::Calibrated { profile, result } => self.calibrated(profile, result)?,
            Msg::Drain => return Ok(false),
        }
        Ok(true)
    }

    fn finished(&mut self, id: LayoutId, class: usize, mem_kb: u64, unit: &Unit, ran: Option<Ran>) {
        self.mem_running_kb -= mem_kb;
        let layout = if self.layout.id == id {
            &mut self.layout
        } else if let Some(l) = self.retired.iter_mut().find(|l| l.id == id) {
            l
        } else {
            warn!("a job finished in unknown layout {id:?}");
            return;
        };
        layout.release(class);
        let calib_us = layout.classes[class].calib_us;
        self.retired.retain(|l| !l.idle());
        if let Some(ran) = ran {
            let sample = Sample {
                cost_mcu: ran.cpu_us * 1000 / calib_us,
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
        if !(self.layout.idle() && self.retired.is_empty()) {
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
        for w in &mut self.queue {
            w.ideal = ideal_class(&self.layout.classes, w.pred.percentile);
        }
        if let Err(e) = classes::set_current(&self.setup.state_dir, &profile) {
            warn!("recording profile {profile}: {e:#}");
        }
        log_classes(&profile, &self.layout.classes);
        self.profile = profile;
        Ok(())
    }

    fn dispatch(&mut self) {
        self.starved = false;
        self.queue.retain(|w| !w.reply.is_closed());
        if self.queue.is_empty() || !matches!(self.switch, Switch::No) {
            return;
        }
        let now = Instant::now();
        let prio = |w: &Waiting| {
            w.pred.cost_mcu as f64 * (now.duration_since(w.since).as_secs_f64() / AGING_S).exp2()
        };
        self.queue.sort_by(|a, b| prio(b).total_cmp(&prio(a)));
        let idle = self.layout.idle() && self.retired.is_empty();
        let mut mem = self.mem_running_kb;
        let mut admissible: LPooled<Vec<usize>> = LPooled::take();
        // Stop at the first job that doesn't fit, so a big job at the front
        // is not starved by smaller ones behind it.
        for (i, w) in self.queue.iter().enumerate() {
            if mem + w.pred.mem_kb > self.setup.mem_limit_kb && !(idle && admissible.is_empty()) {
                break;
            }
            mem += w.pred.mem_kb;
            admissible.push(i);
        }
        if admissible.is_empty() {
            return;
        }
        let mut held: LPooled<Vec<usize>> = LPooled::take();
        held.extend(self.layout.fifos.iter().map(|f| f.take(admissible.len())));
        let mut granted: LPooled<Vec<(usize, usize)>> = LPooled::take();
        for &i in admissible.iter() {
            let Some(class) = nearest(&held, self.queue[i].ideal) else {
                self.starved = true;
                break;
            };
            held[class] -= 1;
            granted.push((i, class));
        }
        for (fifo, n) in self.layout.fifos.iter().zip(held.iter()) {
            fifo.give(*n);
        }
        granted.sort_unstable_by_key(|&(i, _)| std::cmp::Reverse(i));
        for &(i, class) in granted.iter() {
            let w = self.queue.swap_remove(i);
            self.grant(w, class);
        }
    }

    fn grant(&mut self, w: Waiting, class: usize) {
        self.layout.running[class] += 1;
        self.mem_running_kb += w.pred.mem_kb;
        let slot = Slot {
            grant: self.layout.grants[class].clone(),
            layout: self.layout.id,
            class,
            mem_kb: w.pred.mem_kb,
            unit: w.unit,
            ran: None,
            tx: self.tx.clone(),
        };
        // If the wrapper is gone the slot comes back through Drop.
        let _ = w.reply.send(Admission::Slot(slot));
    }

    fn status(&self) -> Status {
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
            units: self.stats.top(20, |p| ideal_class(&self.layout.classes, p)),
        }
    }

    fn save(&mut self) {
        if !self.stats.dirty {
            return;
        }
        self.stats.rebuild_global();
        if let Err(e) = self.stats.save(&self.setup.state_dir.join("jobs.hdr")) {
            warn!("saving job stats: {e:#}");
        }
    }
}

fn log_classes(profile: &Profile, classes: &[Class]) {
    for (k, c) in classes.iter().enumerate() {
        info!("{profile} class {k}: {} cpus, calibration {} ms", c.cpus.len(), c.calib_us / 1000);
    }
}

/// The class whose share of the box, counted up from the slowest class,
/// covers `percentile`. Classes are fastest first.
pub fn ideal_class(classes: &[Class], percentile: f64) -> usize {
    let total: usize = classes.iter().map(|c| c.cpus.len()).sum();
    let mut covered = 0.;
    for (k, class) in classes.iter().enumerate().rev() {
        covered += class.cpus.len() as f64 * 100. / total as f64;
        if percentile <= covered {
            return k;
        }
    }
    0
}

/// The class with a free token closest to `ideal`, the faster on ties.
fn nearest(held: &[usize], ideal: usize) -> Option<usize> {
    (0..held.len()).filter(|&k| held[k] > 0).min_by_key(|&k| (k.abs_diff(ideal), k))
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::bail;
    use std::{fs::OpenOptions, io::Read};

    fn class(calib_us: u64, cpus: &[u32]) -> Class {
        Class { calib_us, cpus: cpus.to_vec() }
    }

    fn unit(name: &str) -> Unit {
        Unit { exact: ArcStr::from(name), coarse: ArcStr::from(name) }
    }

    #[test]
    fn bands_follow_cpu_share() {
        let laptop =
            [class(800, &[0, 1, 2, 3]), class(1200, &[4, 5, 6, 7, 8, 9, 10, 11]), class(1500, &[12, 13, 14, 15])];
        assert_eq!(ideal_class(&laptop, 0.), 2);
        assert_eq!(ideal_class(&laptop, 25.), 2);
        assert_eq!(ideal_class(&laptop, 26.), 1);
        assert_eq!(ideal_class(&laptop, 75.), 1);
        assert_eq!(ideal_class(&laptop, 76.), 0);
        assert_eq!(ideal_class(&laptop, 100.), 0);
    }

    #[test]
    fn nearest_prefers_ideal_then_faster() {
        assert_eq!(nearest(&[1, 1, 1], 1), Some(1));
        assert_eq!(nearest(&[1, 0, 1], 1), Some(0));
        assert_eq!(nearest(&[0, 0, 1], 0), Some(2));
        assert_eq!(nearest(&[0, 0, 0], 0), None);
    }

    struct Harness {
        tx: mpsc::UnboundedSender<Msg>,
        dir: PathBuf,
    }

    impl Harness {
        /// A scheduler in profile `default` with `classes`, and job
        /// history from `train`.
        fn start(name: &str, classes: Vec<Class>, mem_limit_kb: u64, train: &[(&str, u64, u64)]) -> Result<Self> {
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
            for &(name, cost_mcu, mem_kb) in train {
                for _ in 0..5 {
                    sched.stats.record(&unit(name), Sample { cost_mcu, wall_ms: 1, mem_kb });
                }
            }
            sched.stats.dirty = false;
            tokio::spawn(sched.run(rx));
            Ok(Self { tx, dir })
        }

        fn submit(&self, name: &str) -> oneshot::Receiver<Admission> {
            let (reply, rx) = oneshot::channel();
            self.tx.send(Msg::Submit { unit: unit(name), reply }).unwrap();
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

    async fn slot(rx: oneshot::Receiver<Admission>) -> Result<Slot> {
        match time::timeout(Duration::from_secs(5), rx).await?? {
            Admission::Slot(s) => Ok(s),
            Admission::Restarting => bail!("restarting"),
        }
    }

    async fn pending(rx: &mut oneshot::Receiver<Admission>) -> bool {
        time::timeout(Duration::from_millis(100), rx).await.is_err()
    }

    #[tokio::test]
    async fn heaviest_waiting_job_gets_the_fast_class() -> Result<()> {
        let h = Harness::start(
            "heavy",
            vec![class(1000, &[0]), class(2000, &[1])],
            u64::MAX,
            &[("heavy", 100_000, 1), ("light", 10, 1), ("mid", 1000, 1)],
        )?;
        let a = slot(h.submit("mid")).await?;
        let b = slot(h.submit("mid")).await?;
        let mut light = h.submit("light");
        let mut heavy = h.submit("heavy");
        assert!(pending(&mut light).await && pending(&mut heavy).await);
        let (fast, slow) = if a.class == 0 { (a, b) } else { (b, a) };
        drop(fast);
        let heavy = slot(heavy).await?;
        assert_eq!(heavy.class, 0);
        assert!(pending(&mut light).await);
        drop(slow);
        let light = slot(light).await?;
        assert_eq!(light.class, 1);
        drop(heavy);
        // A light job takes the fast class rather than leave it idle.
        assert_eq!(slot(h.submit("light")).await?.class, 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_limit_holds_jobs_back() -> Result<()> {
        let h = Harness::start("mem", vec![class(1000, &[0, 1, 2, 3])], 1000, &[("big", 10, 600)])?;
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
        let h = Harness::start("leak", vec![class(1000, &[0, 1])], u64::MAX, &[])?;
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
        let h = Harness::start("switch", vec![class(1000, &[0])], u64::MAX, &[])?;
        classes::save(&h.dir, &Profile::new("battery")?, &[class(3000, &[0]), class(5000, &[1])])?;
        let old = slot(h.submit("a")).await?;
        h.profile("battery");
        // The old layout's only token is held, yet the new job runs.
        let new = slot(h.submit("a")).await?;
        let st = h.status().await;
        assert_eq!(st.profile.as_str(), "battery");
        assert_eq!(st.classes.len(), 2);
        assert_eq!(st.finishing, 1);
        assert_ne!(old.layout, new.layout);
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
        let h = Harness::start("drain", vec![class(1000, &[0, 1])], u64::MAX, &[])?;
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
