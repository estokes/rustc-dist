use crate::{
    classes::Class,
    protocol::{ClassStatus, Ran, Reply, Status, Unit, frame},
    stats::{Prediction, Sample, Stats},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use compact_str::format_compact;
use log::{info, warn};
use poolshark::local::LPooled;
use std::{
    ffi::CString,
    fs, io,
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
    time,
};

/// A waiting job's priority doubles every this many seconds, so light
/// jobs are not starved by a stream of heavy ones.
const AGING_S: f64 = 10.;
const SAVE_EVERY: Duration = Duration::from_secs(60);

pub enum Msg {
    Submit { unit: Unit, reply: oneshot::Sender<Admission> },
    Finished { class: usize, mem_kb: u64, unit: Unit, ran: Option<Ran> },
    Status { reply: oneshot::Sender<Status> },
    Drain,
}

pub enum Admission {
    Slot(Slot),
    Restarting,
}

/// Held while a wrapper runs rustc. Dropping it frees the slot.
pub struct Slot {
    pub class: usize,
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
    tokens: usize,
}

impl Fifo {
    fn create(path: &Path, tokens: usize) -> Result<Self> {
        let _ = fs::remove_file(path);
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
        let fifo = Self { fd, tokens };
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

struct Waiting {
    unit: Unit,
    pred: Prediction,
    ideal: usize,
    since: Instant,
    reply: oneshot::Sender<Admission>,
}

pub struct Sched {
    /// Fastest first.
    classes: Vec<Class>,
    fifos: Vec<Fifo>,
    running: Vec<u32>,
    mem_running_kb: u64,
    mem_limit_kb: u64,
    queue: Vec<Waiting>,
    /// The last dispatch ran out of tokens with admissible jobs waiting.
    starved: bool,
    stats: Stats,
    stats_path: PathBuf,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Sched {
    /// Also returns, per class, the framed `Reply::Slot` a granted
    /// wrapper is sent.
    pub fn new(
        classes: Vec<Class>,
        fifo_dir: &Path,
        mem_limit_kb: u64,
        stats_path: PathBuf,
        tx: mpsc::UnboundedSender<Msg>,
    ) -> Result<(Self, Vec<Bytes>)> {
        let stats = Stats::load(&stats_path).unwrap_or_else(|e| {
            warn!("starting with empty job stats: {e:#}");
            Stats::new()
        });
        let mut fifos = Vec::with_capacity(classes.len());
        let mut grants = Vec::with_capacity(classes.len());
        let mut buf = BytesMut::new();
        for (k, class) in classes.iter().enumerate() {
            let path = fifo_dir.join(format_compact!("class-{k}").as_str());
            fifos.push(Fifo::create(&path, class.cpus.len())?);
            let jobserver = ArcStr::from(path.to_str().context("jobserver path is not utf-8")?);
            frame(&mut buf, &Reply::Slot { cpus: class.cpus.clone(), jobserver })?;
            grants.push(buf.split().freeze());
        }
        let sched = Self {
            running: vec![0; classes.len()],
            classes,
            fifos,
            mem_running_kb: 0,
            mem_limit_kb,
            queue: Vec::new(),
            starved: false,
            stats,
            stats_path,
            tx,
        };
        Ok((sched, grants))
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Msg>) {
        enum Event {
            Msg(Option<Msg>),
            Tokens,
            Save,
        }
        let mut save = time::interval(SAVE_EVERY);
        loop {
            let starved = self.starved;
            let event = tokio::select! {
                msg = rx.recv() => Event::Msg(msg),
                _ = std::future::poll_fn(|cx| self.poll_tokens(cx)), if starved => Event::Tokens,
                _ = save.tick() => Event::Save,
            };
            match event {
                Event::Msg(None) => break,
                Event::Msg(Some(msg)) => {
                    if !self.handle(msg) {
                        break;
                    }
                }
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
        for fifo in &self.fifos {
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
    fn handle(&mut self, msg: Msg) -> bool {
        match msg {
            Msg::Submit { unit, reply } => {
                let pred = self.stats.predict(&unit);
                let ideal = ideal_class(&self.classes, pred.percentile);
                self.queue.push(Waiting { unit, pred, ideal, since: Instant::now(), reply });
            }
            Msg::Finished { class, mem_kb, unit, ran } => self.finished(class, mem_kb, &unit, ran),
            Msg::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Msg::Drain => return false,
        }
        true
    }

    fn finished(&mut self, class: usize, mem_kb: u64, unit: &Unit, ran: Option<Ran>) {
        self.running[class] -= 1;
        self.mem_running_kb -= mem_kb;
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
        if let Some(ran) = ran {
            let sample = Sample {
                cost_mcu: ran.cpu_us * 1000 / self.classes[class].calib_us,
                wall_ms: ran.wall_us / 1000,
                mem_kb: ran.maxrss_kb,
            };
            self.stats.record(unit, sample);
        }
    }

    fn dispatch(&mut self) {
        self.starved = false;
        self.queue.retain(|w| !w.reply.is_closed());
        if self.queue.is_empty() {
            return;
        }
        let now = Instant::now();
        let prio = |w: &Waiting| {
            w.pred.cost_mcu as f64 * (now.duration_since(w.since).as_secs_f64() / AGING_S).exp2()
        };
        self.queue.sort_by(|a, b| prio(b).total_cmp(&prio(a)));
        let idle = self.running.iter().all(|r| *r == 0);
        let mut mem = self.mem_running_kb;
        let mut admissible: LPooled<Vec<usize>> = LPooled::take();
        // Stop at the first job that doesn't fit, so a big job at the front
        // is not starved by smaller ones behind it.
        for (i, w) in self.queue.iter().enumerate() {
            if mem + w.pred.mem_kb > self.mem_limit_kb && !(idle && admissible.is_empty()) {
                break;
            }
            mem += w.pred.mem_kb;
            admissible.push(i);
        }
        if admissible.is_empty() {
            return;
        }
        let mut held: LPooled<Vec<usize>> = LPooled::take();
        held.extend(self.fifos.iter().map(|f| f.take(admissible.len())));
        let mut granted: LPooled<Vec<(usize, usize)>> = LPooled::take();
        for &i in admissible.iter() {
            let Some(class) = nearest(&held, self.queue[i].ideal) else {
                self.starved = true;
                break;
            };
            held[class] -= 1;
            granted.push((i, class));
        }
        for (fifo, n) in self.fifos.iter().zip(held.iter()) {
            fifo.give(*n);
        }
        granted.sort_unstable_by_key(|&(i, _)| std::cmp::Reverse(i));
        for &(i, class) in granted.iter() {
            let w = self.queue.swap_remove(i);
            self.grant(w, class);
        }
    }

    fn grant(&mut self, w: Waiting, class: usize) {
        self.running[class] += 1;
        self.mem_running_kb += w.pred.mem_kb;
        let slot =
            Slot { class, mem_kb: w.pred.mem_kb, unit: w.unit, ran: None, tx: self.tx.clone() };
        // If the wrapper is gone the slot comes back through Drop.
        let _ = w.reply.send(Admission::Slot(slot));
    }

    fn status(&self) -> Status {
        Status {
            classes: self
                .classes
                .iter()
                .zip(&self.running)
                .map(|(c, r)| ClassStatus {
                    calib_us: c.calib_us,
                    cpus: c.cpus.len() as u32,
                    running: *r,
                })
                .collect(),
            waiting: self.queue.len() as u32,
            mem_limit_kb: self.mem_limit_kb,
            mem_running_kb: self.mem_running_kb,
            units: self.stats.top(20, |p| ideal_class(&self.classes, p)),
        }
    }

    fn save(&mut self) {
        if !self.stats.dirty {
            return;
        }
        self.stats.rebuild_global();
        if let Err(e) = self.stats.save(&self.stats_path) {
            warn!("saving job stats: {e:#}");
        }
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
    use std::fs::OpenOptions;
    use std::io::Read;

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
        fn start(name: &str, classes: Vec<Class>, mem_limit_kb: u64, train: &[(&str, u64, u64)]) -> Result<Self> {
            let dir = std::env::temp_dir().join(format!("rustc-dist-sched-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir)?;
            let (tx, rx) = mpsc::unbounded_channel();
            let (mut sched, _) = Sched::new(classes, &dir, mem_limit_kb, dir.join("jobs.hdr"), tx.clone())?;
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
        let mut fifo = OpenOptions::new().read(true).write(true).open(h.dir.join("class-0"))?;
        fifo.read_exact(&mut [0u8; 1])?;
        drop(fifo);
        drop(job);
        let a = slot(h.submit("a")).await?;
        let b = slot(h.submit("a")).await?;
        drop((a, b));
        Ok(())
    }
}
