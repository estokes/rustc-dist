use crate::{
    blobs::{Hashes, Store},
    browser,
    classes::{self, Profile},
    config::{self, Config},
    farm::{self, Load, Member},
    protocol::{self, Ask, Finished, Job, PROTOCOL, Reads, Reply, STOP, Unit},
    remote::{self, Event, Links, Moved, Outcome, ToServer},
    sandbox::{self, Server},
    sched::{Admission, Heartbeat, Msg, RemoteOutcome, RemoteSlot, Sched, SentRun, Setup, Want},
    stats::Transfer,
    sys,
    toolchain::{self, Keys},
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use compact_str::format_compact;
use log::{debug, error, info, warn};
use netidx::{
    config::Config as NetConfig,
    path::Path as NPath,
    publisher::{Publisher, PublisherBuilder},
    subscriber::SubscriberBuilder,
};
use std::{
    fs::{self, File, OpenOptions},
    mem,
    io::{self, ErrorKind},
    os::{
        fd::AsRawFd,
        unix::fs::{FileTypeExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy, sink},
    net::{UnixListener, UnixStream, unix::WriteHalf},
    signal::unix::{SignalKind, signal},
    sync::{mpsc, oneshot, watch},
    task, time,
};

/// How long a refused client may take to hang up.
const REFUSED_READ: Duration = Duration::from_secs(5);

/// What every connection needs.
struct Ctx {
    tx: mpsc::UnboundedSender<Msg>,
    stop: mpsc::Sender<()>,
    keys: Keys,
    hashes: triomphe::Arc<Hashes>,
    /// Set once this box has joined a farm.
    farm: OnceLock<Farm>,
    remote_links: bool,
    compression_level: i32,
}

/// This box's part in the farm.
struct Farm {
    links: Arc<Links>,
    /// Shutting it down leaves the farm.
    publisher: Publisher,
}

pub async fn run() -> Result<()> {
    if sys::running_as_root() {
        bail!("rustc-dist won't run its daemon as root: it runs other boxes' proc macros");
    }
    let cfg = config::load()?;
    let dir = config::runtime_dir();
    config::make_private_dir(&dir)?;
    let lock_path = config::lock_path();
    let Some(_lock) = lock(&lock_path).await? else {
        info!("another daemon holds {}", lock_path.display());
        return Ok(());
    };
    let sock_path = config::socket_path();
    let _ = fs::remove_file(&sock_path);
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format_compact!("binding {}", sock_path.display()))?;
    remove_stale_fifos(&dir);
    let state_dir = config::state_dir();
    let profile = classes::current(&state_dir);
    let hashes = triomphe::Arc::new(Hashes::load(&state_dir.join("hashes")).unwrap_or_else(|e| {
        warn!("starting with no file hashes: {e:#}");
        Hashes::default()
    }));
    let (heartbeats, heartbeat_rx) = mpsc::unbounded_channel();
    let setup = Setup {
        state_dir,
        fifo_dir: dir,
        allowed: sys::allowed_cpus()?,
        memory: {
            let share = cfg.compressed_swap_share;
            Box::new(move || sys::box_memory(share))
        },
        runnable: Box::new(sys::runnable),
        hashes: hashes.clone(),
        heartbeats,
        tokens_per_cpu: cfg.tokens_per_cpu,
        calibration_seconds: cfg.calibration_seconds,
        decay: cfg.decay(),
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let (load_tx, load) = watch::channel(Load::default());
    let sched = Sched::new(setup, profile, load_tx, tx.clone())?;
    let mut sched = tokio::spawn(sched.run(rx));
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let ctx = Arc::new(Ctx {
        tx: tx.clone(),
        stop: stop_tx,
        keys: Keys::default(),
        hashes,
        farm: OnceLock::new(),
        remote_links: cfg.remote_links,
        compression_level: cfg.compression_level,
    });
    // Wrappers are answered meanwhile, their jobs kept here until it's done.
    let joining = cfg.base.clone().map(|base| {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            match join(&cfg, &base, load, heartbeat_rx, ctx.tx.clone()).await {
                Ok(farm) => {
                    let _ = ctx.farm.set(farm);
                }
                Err(e) => error!("working alone, could not join the farm at {base}: {e:#}"),
            }
        })
    });
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    info!("listening on {}", sock_path.display());
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((sock, _)) => {
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve(sock, &ctx).await {
                            info!("a wrapper's connection: {e:#}");
                        }
                    });
                }
                Err(e) => warn!("accept: {e}"),
            },
            _ = &mut sched => {
                error!("the scheduler stopped");
                break;
            }
            _ = stop_rx.recv() => break,
            _ = term.recv() => break,
            _ = int.recv() => break,
        }
    }
    info!("shutting down");
    drop(listener);
    if let Some(joining) = joining {
        joining.abort();
    }
    if let Some(farm) = ctx.farm.get()
        && time::timeout(Duration::from_secs(2), farm.publisher.clone().shutdown()).await.is_err()
    {
        warn!("leaving the farm timed out; other boxes may see this one until the resolver forgets it");
    }
    let _ = fs::remove_file(&sock_path);
    let _ = tx.send(Msg::Drain);
    if !sched.is_finished() {
        sched.await?;
    }
    Ok(())
}

/// Publish this box under `base`, watch the others, take their jobs, and
/// send them the heartbeats the scheduler asks for.
async fn join(
    cfg: &Config,
    base: &str,
    load: watch::Receiver<Load>,
    heartbeats: mpsc::UnboundedReceiver<Heartbeat>,
    tx: mpsc::UnboundedSender<Msg>,
) -> Result<Farm> {
    let net = NetConfig::load_default().context("loading the netidx config")?;
    let publisher = PublisherBuilder::new(net.clone()).bind_cfg(cfg.bind).build().await?;
    let subscriber = SubscriberBuilder::new(net).build()?;
    let base = NPath::from(ArcStr::from(base));
    let name = ArcStr::from(cfg.name.clone().unwrap_or_else(hostname));
    let installed = task::spawn_blocking(toolchain::installed).await?;
    for t in &installed {
        info!("serving toolchain {} at {}", t.toolchain.key, t.sysroot.display());
    }
    let toolchains = installed.iter().map(|t| t.toolchain.clone()).collect();
    config::make_private_dir(&cfg.cache_dir)?;
    let (blobs, cap) = (cfg.cache_dir.join("blobs"), cfg.cache_gb << 30);
    let store = task::spawn_blocking(move || Store::open(blobs, cap)).await??;
    let server = Arc::new(Server::new(store, cfg.cache_dir.join("jobs"), installed, tx.clone(), cfg.compression_level)?);
    tokio::spawn(sandbox::listen(publisher.clone(), farm::jobs_path(&base, &name), server).await?);
    let status = browser::run(
        publisher.clone(),
        farm::status_path(&base, &name),
        farm::units_path(&base, &name),
        tx.clone(),
    );
    tokio::spawn(async move {
        if let Err(e) = status.await {
            error!("no longer publishing status: {e:#}");
        }
    });
    let member = Member {
        publisher: publisher.clone(),
        subscriber: subscriber.clone(),
        base,
        name,
        toolchains,
        cache_gb: cfg.cache_gb,
    };
    let links = Arc::new(Links::new(subscriber));
    tokio::spawn(send_heartbeats(links.clone(), heartbeats, tx.clone(), cfg.compression_level));
    tokio::spawn(async move {
        if let Err(e) = farm::run(member, load, tx).await {
            error!("left the farm: {e:#}");
        }
    });
    Ok(Farm { links, publisher })
}

fn hostname() -> String {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|h| h.trim().split('.').next().unwrap_or_default().replace('/', "_"))
        .unwrap_or_else(|_| "rustc-dist".into())
}

/// Jobserver FIFOs left by a daemon that died without cleaning up. The
/// daemon that holds the lock owns every FIFO in its runtime dir.
fn remove_stale_fifos(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|t| t.is_fifo()) {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// Take the daemon lock, waiting a few seconds for a daemon that is
/// shutting down. None if another daemon keeps it.
async fn lock(path: &Path) -> Result<Option<File>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format_compact!("opening {}", path.display()))?;
    for _ in 0..100 {
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(file));
        }
        let err = io::Error::last_os_error();
        if err.kind() != ErrorKind::WouldBlock {
            return Err(err).with_context(|| format_compact!("locking {}", path.display()));
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    Ok(None)
}

async fn serve(mut sock: UnixStream, ctx: &Ctx) -> Result<()> {
    let mut version = [0u8; 4];
    sock.read_exact(&mut version).await?;
    let version = u32::from_be_bytes(version);
    match version {
        PROTOCOL => (),
        STOP => {
            info!("asked to stop");
            let _ = ctx.stop.try_send(());
            return Ok(());
        }
        other => {
            warn!("refused a client that speaks protocol {other}; this daemon speaks {PROTOCOL}");
            sock.write_all(&protocol::refusal()).await?;
            // Take what it sends until it hangs up, so that its write
            // doesn't fail before it reads why.
            let _ = time::timeout(REFUSED_READ, copy(&mut sock, &mut sink())).await;
            return Ok(());
        }
    }
    let mut buf = BytesMut::new();
    match protocol::recv_async::<Ask>(&mut sock, &mut buf).await? {
        Ask::Probe => protocol::send_async(&mut sock, &mut buf, &Reply::Trivial).await,
        Ask::Compile { unit, build, job } => compile(sock, buf, ctx, unit, build, job).await,
        Ask::Status => {
            let (reply, status) = oneshot::channel();
            ctx.tx.send(Msg::Status { reply }).map_err(|_| anyhow!("scheduler stopped"))?;
            let status = status.await?;
            protocol::send_async(&mut sock, &mut buf, &Reply::Status(status)).await
        }
        Ask::Profile { name, recalibrate } => {
            let reply = match Profile::new(&name) {
                Ok(profile) => {
                    ctx.tx
                        .send(Msg::Profile { profile, recalibrate })
                        .map_err(|_| anyhow!("scheduler stopped"))?;
                    Reply::Done
                }
                Err(e) => Reply::Refused(format_compact!("{e:#}").as_str().into()),
            };
            protocol::send_async(&mut sock, &mut buf, &reply).await
        }
    }
}

/// Wait for the scheduler to place a job. None if cargo killed the
/// wrapper meanwhile.
async fn admit(
    sock: &UnixStream,
    ctx: &Ctx,
    unit: Unit,
    build: u32,
    remote: Result<Want, &'static str>,
) -> Result<Option<Admission>> {
    let (reply, admitted) = oneshot::channel();
    ctx.tx.send(Msg::Submit { unit, build, remote, reply }).map_err(|_| anyhow!("scheduler stopped"))?;
    unless_hung_up(sock, admitted).await?.map(|a| a.context("scheduler stopped")).transpose()
}

/// `work`'s output, or None if the wrapper hangs up first, whether or not
/// it read what it was sent. A wrapper sends nothing while it waits for a
/// slot or for a job on another box.
async fn unless_hung_up<T>(sock: &UnixStream, work: impl Future<Output = T>) -> Result<Option<T>> {
    tokio::pin!(work);
    loop {
        tokio::select! {
            done = &mut work => return Ok(Some(done)),
            readable = sock.readable() => {
                readable?;
                match sock.try_read(&mut [0u8; 1]) {
                    Ok(0) => return Ok(None),
                    Ok(_) => bail!("the wrapper sent data while it waited"),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => continue,
                    Err(e) if e.kind() == ErrorKind::ConnectionReset => return Ok(None),
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}

/// Send each heartbeat the scheduler asks for, and tell it how long each
/// took.
async fn send_heartbeats(
    links: Arc<Links>,
    mut asks: mpsc::UnboundedReceiver<Heartbeat>,
    tx: mpsc::UnboundedSender<Msg>,
    level: i32,
) {
    while let Some(Heartbeat { name, jobs, kb }) = asks.recv().await {
        let (links, tx) = (links.clone(), tx.clone());
        tokio::spawn(async move {
            match remote::heartbeat(&links, &jobs, kb, level).await {
                Ok(us) => {
                    debug!("heartbeat of {kb} kB to {name} took {} ms", us / 1000);
                    let _ = tx.send(Msg::Heartbeat { name, kb, us });
                }
                Err(e) => warn!("heartbeat to {name}: {e:#}"),
            }
        });
    }
}

async fn compile(
    mut sock: UnixStream,
    mut buf: BytesMut,
    ctx: &Ctx,
    unit: Unit,
    build: u32,
    job: Option<Job>,
) -> Result<()> {
    let job = job.map(triomphe::Arc::new);
    // Where it may go besides here, or why nowhere.
    let mut want = match (&job, ctx.farm.get()) {
        (None, _) => Err("the wrapper keeps it here"),
        (_, None) => Err("this box isn't in a farm"),
        (Some(job), _) if job.reads != Reads::Metadata && !ctx.remote_links => Err("remote_links is off"),
        (Some(job), Some(_)) => {
            let keys = &ctx.keys;
            let rustc = PathBuf::from(job.rustc.as_str());
            match task::block_in_place(|| keys.key(&rustc)) {
                Ok(key) => Ok(Want { key, target: job.target.clone(), reads: job.reads.clone(), cwd: job.cwd.clone() }),
                Err(e) => {
                    warn!("{e:#}");
                    Err("its toolchain is unknown")
                }
            }
        }
    };
    // Set once a remote run failed after passing cargo an rmeta notice.
    let mut filter_metadata = false;
    let mut failed_remotely = false;
    loop {
        let Some(admission) = admit(&sock, ctx, unit.clone(), build, want.clone()).await? else {
            return Ok(());
        };
        match admission {
            Admission::Restarting => {
                return protocol::send_async(&mut sock, &mut buf, &Reply::Restarting).await;
            }
            Admission::Refused(why) => {
                return protocol::send_async(&mut sock, &mut buf, &Reply::Refused(why)).await;
            }
            Admission::Slot(slot) => {
                if let Some(pid) = sock.peer_cred().ok().and_then(|c| c.pid()) {
                    slot.started(pid as u32);
                }
                if filter_metadata {
                    let reply = Reply::Slot {
                        cpus: slot.grant.cpus.clone(),
                        jobserver: slot.grant.jobserver.clone(),
                        filter_metadata,
                    };
                    protocol::send_async(&mut sock, &mut buf, &reply).await?;
                } else {
                    sock.write_all(&slot.grant.frame).await?;
                }
                if let Ok(Finished { ran, crates }) = protocol::recv_async(&mut sock, &mut buf).await {
                    slot.finish(ran, 0);
                    if failed_remotely {
                        let _ = ctx.tx.send(Msg::Mismatch { unit });
                    }
                    // Hashed now, while no job waits on them, they are
                    // priced and staged without reading them when a job
                    // that reads them goes to another box.
                    if ctx.farm.get().is_some() && !crates.is_empty() {
                        let hashes = ctx.hashes.clone();
                        task::spawn_blocking(move || {
                            for c in &crates {
                                if let Err(e) = hashes.get(Path::new(c.as_str())) {
                                    info!("hashing {c}: {e:#}");
                                }
                            }
                        });
                    }
                }
                return Ok(());
            }
            Admission::Remote(r) => {
                let job = job.as_ref().expect("only portable jobs go remote");
                match run_remote(&mut sock, &mut buf, ctx, job, r).await {
                    Ok(Remote::Done) => return Ok(()),
                    Ok(Remote::Gone) => {
                        debug!("{}: the wrapper went away; stopped it", unit.exact);
                        return Ok(());
                    }
                    Ok(Remote::Busy) => (),
                    Ok(Remote::Failed { forwarded_metadata }) => {
                        filter_metadata |= forwarded_metadata;
                        failed_remotely = true;
                        want = Err("it failed on another box just now");
                    }
                    Err(e) => {
                        warn!("{}: {e:#}", unit.exact);
                        want = Err("sending it failed just now");
                    }
                }
            }
        }
    }
}

enum Remote {
    Done,
    Busy,
    /// It failed there; build it here.
    Failed { forwarded_metadata: bool },
    /// The wrapper hung up, so the job was stopped.
    Gone,
}

async fn run_remote(
    sock: &mut UnixStream,
    buf: &mut BytesMut,
    ctx: &Ctx,
    job: &triomphe::Arc<Job>,
    mut r: RemoteSlot,
) -> Result<Remote> {
    let links = &ctx.farm.get().context("not in a farm")?.links;
    let start = Instant::now();
    let staging = task::spawn_blocking({
        let (job, crates, hashes) = (job.clone(), mem::take(&mut r.crates), ctx.hashes.clone());
        move || remote::stage(&job, &crates, &hashes)
    });
    let Some(staged) = unless_hung_up(sock, staging).await? else { return Ok(Remote::Gone) };
    let staged = staged??;
    let stage_us = start.elapsed().as_micros() as u64;
    let offer = ToServer::Offer { unit: r.unit().clone(), key: r.key.clone(), target: r.target.clone(), class: r.class };
    let (etx, mut events) = mpsc::unbounded_channel();
    let level = ctx.compression_level;
    let jobs = r.jobs.clone();
    let running = async {
        let linking = Instant::now();
        let link = links.get(&jobs).await?;
        let link_us = linking.elapsed().as_micros() as u64;
        let outcome = remote::run(&link, offer, job, &staged, &ctx.hashes, level, |ev| {
            etx.send(ev).map_err(|_| anyhow!("daemon connection gone"))
        });
        Ok::<_, anyhow::Error>((link_us, outcome.await?))
    };
    tokio::pin!(running);
    // Diagnostics wait for the exit, so a failure here leaves nothing for
    // cargo to see twice.
    let mut held: Vec<(bool, Bytes)> = Vec::new();
    let mut forwarded_metadata = false;
    let mut handle = async |ev: Event, sock: &mut WriteHalf<'_>, buf: &mut BytesMut| -> Result<()> {
        match ev {
            Event::Accepted { version } => {
                r.accepted(version);
                Ok(())
            }
            Event::Artifact(line) => {
                forwarded_metadata |= protocol::is_metadata_line(&line);
                protocol::send_async(sock, buf, &Reply::Output { stderr: true, data: line }).await
            }
            Event::Stderr(data) => {
                held.push((true, data));
                Ok(())
            }
            Event::Stdout(data) => {
                held.push((false, data));
                Ok(())
            }
        }
    };
    // XCR claude for claude: [bug/medium] Nothing here watches the wrapper while the job runs on the
    // other box: admit polls the socket for EOF, this loop doesn't, and `handle` writes to it only
    // on artifact lines. So a wrapper that dies (cargo interrupted) is noticed only at the job's
    // next artifact line: for a binary or test, the end of its link, as its dep-info line comes
    // early; for a library, the end of codegen after its rmeta. Until then the remote rustc runs
    // on, holding the other box's token and this box's RemoteSlot, so a build started right after
    // sees that box busy and may send the same unit again. A failed write then returns Err before
    // r.finish, so a run that succeeded is recorded as RemoteOutcome::Failed and logged as a
    // warning. DESIGN ("Wrapper protocol": a remote job is cancelled if its wrapper died) says
    // otherwise. Fix: an arm here like admit's `sock.readable()` that returns on EOF without
    // awaiting `running`; dropping it drops the exchange, whose Drop (remote::In) sends the server
    // Stop. Staging under block_in_place can't notice either, for up to tens of seconds on a busy
    // box. (daemon-01)
    // 2026-11-01 claude: admit and run_remote now wait through unless_hung_up, which returns None
    // on the wrapper's EOF: staging runs in spawn_blocking under it, and the relay of a remote
    // run's events is wrapped in it (the socket split so events can still be written). Returning
    // Remote::Gone drops running and so the exchange, whose Drop sends Stop; the RemoteSlot drops
    // as Failed, which records nothing. A failed write to the wrapper also ends the relay as Gone,
    // and a run that succeeded is finished as Done before its held output and Exit are written.
    // Test: daemon::test::waiting_ends_when_the_wrapper_hangs_up pins unless_hung_up; nothing
    // drives run_remote itself, as a RemoteSlot can only be made by the scheduler with a farm
    // behind it, so dropping the helper from run_remote would go unnoticed by the tests.
    // 2026-11-01 claude: The code holds, but no test fails without it: with run_remote's two
    // unless_hung_up calls (staging and relay) turned back into plain awaits in a scratch copy,
    // every daemon and e2e test passed. waiting_ends_when_the_wrapper_hangs_up pins only the helper.
    // The e2e harness can drive run_remote: e2e_test::server gives an in-process netidx with Links
    // and a publisher (enough for Ctx::farm), and endless() a job that runs until stopped. With a
    // #[cfg(test)] constructor for sched::RemoteSlot (its fields are private), a test can call
    // run_remote, drop the wrapper's end, and check that it returns Remote::Gone at once and that
    // serving(&b) drops to 0.
    // 2026-11-01 claude:
    // daemon::test::a_remote_job_ends_when_its_wrapper_hangs_up_while_it_is_staged_or_runs drives
    // run_remote against e2e_test::server (in-process netidx, Ctx::farm from its publisher and a
    // Links on its subscriber) with sched::RemoteSlot::for_test (a #[cfg(test)] constructor).
    // Staging: the job's last dep-info is a FIFO, so remote::stage blocks reading it; the wrapper
    // has hung up, and run_remote must return Remote::Gone within 5 s. Relay: e2e_test::endless runs
    // in the sandbox until the wrapper end is dropped; run_remote must return Gone within 5 s,
    // serving(&b) drop to 0 and the sandbox die. It fails with the staging unless_hung_up turned
    // into a plain await, and with the relay's (each checked). Writing it found a gap: a wrapper
    // that hangs up with output unread (here the early dep-info artifact line, as when cargo kills a
    // wrapper) gives the daemon ECONNRESET, not EOF, so unless_hung_up returned Err, compile logged
    // 'sending it failed just now' and queued the job again here. unless_hung_up now treats
    // ConnectionReset as a hang-up; waiting_ends_when_the_wrapper_hangs_up gained that case and
    // fails without it (checked), as does the run_remote test.
    let (from_wrapper, mut to_wrapper) = sock.split();
    let relay = async {
        loop {
            tokio::select! {
                outcome = &mut running => break Some(outcome),
                Some(ev) = events.recv() => {
                    if handle(ev, &mut to_wrapper, buf).await.is_err() {
                        break None;
                    }
                }
            }
        }
    };
    let Some(Some(outcome)) = unless_hung_up(from_wrapper.as_ref(), relay).await? else {
        // Dropping `running` drops its exchange, which stops the job there.
        return Ok(Remote::Gone);
    };
    let mut gone = false;
    while let Ok(ev) = events.try_recv() {
        gone = gone || handle(ev, &mut to_wrapper, buf).await.is_err();
    }
    drop(handle);
    match outcome {
        Ok((link_us, Outcome::Exit { code: 0, ran, measured, moved })) => {
            let Moved { bytes, wire_bytes, linker_waited_us, kept, steps } = moved;
            let copy_us = (start.elapsed().as_micros() as u64).saturating_sub(ran.wall_us) + linker_waited_us;
            let links = job.reads != Reads::Metadata;
            let transfer = Transfer { sent_kb: bytes / 1024, copy_ms: copy_us / 1000, links };
            let ms = |us: u64| us / 1000;
            info!(
                "{} on {}: compiled in {} ms, moving files took {} ms: staging {} (hashing {} MB {}), link {}, \
                 offer {}, manifest {}, sending {} kB as {} kB {}, setup and outputs {} ms, linker waited {} ms",
                r.unit().exact,
                r.jobs,
                ms(ran.wall_us),
                transfer.copy_ms,
                ms(stage_us),
                staged.hashed.0 >> 20,
                ms(staged.hashed.1),
                ms(link_us),
                ms(steps.offer_us),
                ms(steps.manifest_us),
                transfer.sent_kb,
                wire_bytes / 1024,
                ms(steps.send_us),
                ms(steps.run_us.saturating_sub(ran.wall_us)),
                ms(linker_waited_us)
            );
            r.finish(RemoteOutcome::Done(SentRun { ran, measured, linker_waited_us, transfer, kept }));
            let sent = async {
                for (stderr, data) in held {
                    protocol::send_async(&mut to_wrapper, buf, &Reply::Output { stderr, data }).await?;
                }
                protocol::send_async(&mut to_wrapper, buf, &Reply::Exit(0)).await
            };
            if gone || sent.await.is_err() {
                return Ok(Remote::Gone);
            }
            Ok(Remote::Done)
        }
        Ok((_, Outcome::Busy { version })) => {
            r.finish(RemoteOutcome::Busy { version });
            Ok(Remote::Busy)
        }
        Ok((_, Outcome::Exit { code, .. })) => {
            let mut stderr = held.iter().filter(|(stderr, _)| *stderr).map(|(_, line)| line);
            let why = stderr.clone().find_map(|line| protocol::error_message(line)).or_else(|| {
                stderr.next().map(|line| String::from_utf8_lossy(line).trim_end().into())
            });
            warn!(
                "{} exited {code} on {}, building it here: {}",
                r.unit().exact,
                r.jobs,
                why.as_deref().unwrap_or("it printed nothing")
            );
            r.finish(RemoteOutcome::Failed);
            Ok(Remote::Failed { forwarded_metadata })
        }
        Err(e) => {
            warn!("{} failed on {}, building it here: {e:#}", r.unit().exact, r.jobs);
            r.finish(RemoteOutcome::Failed);
            Ok(Remote::Failed { forwarded_metadata })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        e2e_test::{self, running, serving, until},
        protocol::OtherProtocol,
    };
    use std::{ffi::CString, future, io::Write, os::unix::ffi::OsStrExt, process};

    /// A connection's context, how it stops the daemon, and what it tells the
    /// scheduler.
    fn ctx() -> (Ctx, mpsc::Receiver<()>, mpsc::UnboundedReceiver<Msg>) {
        let (tx, msgs) = mpsc::unbounded_channel();
        let (stop, stopped) = mpsc::channel(1);
        let hashes = triomphe::Arc::new(Hashes::default());
        let keys = Keys::default();
        let ctx = Ctx { tx, stop, keys, hashes, farm: OnceLock::new(), remote_links: true, compression_level: 1 };
        (ctx, stopped, msgs)
    }

    #[tokio::test]
    async fn a_client_of_another_protocol_is_refused_and_the_daemon_stays() -> Result<()> {
        let (ctx, mut stopped, _msgs) = ctx();
        let (daemon, client) = UnixStream::pair()?;
        let mut client = client.into_std()?;
        client.set_nonblocking(false)?;
        let asked = task::spawn_blocking(move || {
            let mut buf = BytesMut::new();
            client.write_all(&(PROTOCOL + 1).to_be_bytes())?;
            protocol::send(&mut client, &mut buf, &Ask::Status)?;
            protocol::recv_reply(&mut client, &mut buf)
        });
        serve(daemon, &ctx).await?;
        let e = asked.await?.unwrap_err();
        assert_eq!(e.downcast_ref::<OtherProtocol>().map(|o| o.daemon), Some(PROTOCOL), "{e:#}");
        assert!(stopped.try_recv().is_err(), "the daemon keeps running");
        let (daemon, mut client) = UnixStream::pair()?;
        client.write_all(&STOP.to_be_bytes()).await?;
        serve(daemon, &ctx).await?;
        assert!(stopped.try_recv().is_ok(), "STOP stops a daemon of any version");
        Ok(())
    }

    #[tokio::test]
    async fn waiting_ends_when_the_wrapper_hangs_up() -> Result<()> {
        let (daemon, wrapper) = UnixStream::pair()?;
        assert_eq!(unless_hung_up(&daemon, async { 7 }).await?, Some(7));
        let hangs_up = async move {
            time::sleep(Duration::from_millis(50)).await;
            drop(wrapper);
        };
        let (waited, ()) = tokio::join!(unless_hung_up(&daemon, future::pending::<()>()), hangs_up);
        assert_eq!(waited?, None, "a wait for a slot, or for a job on another box, ends with its wrapper");
        let (mut daemon, wrapper) = UnixStream::pair()?;
        daemon.write_all(b"an artifact line\n").await?;
        let hangs_up_unread = async move {
            time::sleep(Duration::from_millis(50)).await;
            drop(wrapper);
        };
        let (waited, ()) = tokio::join!(unless_hung_up(&daemon, future::pending::<()>()), hangs_up_unread);
        assert_eq!(waited?, None, "a wrapper that hangs up with output unread is gone too");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_remote_job_ends_when_its_wrapper_hangs_up_while_it_is_staged_or_runs() -> Result<()> {
        let dir = scratch("remote-gone")?;
        let b = e2e_test::server(&dir.join("box")).await?;
        let (ctx, _stopped, _msgs) = ctx();
        let links = Arc::new(Links::new(b.net.subscriber().clone()));
        let _ = ctx.farm.set(Farm { links, publisher: b.net.publisher().clone() });
        let ws = dir.join("ws");
        let (job, unit) = e2e_test::endless(&ws)?;
        let key = toolchain::key_of(Path::new(job.rustc.as_str()))?;
        let job = triomphe::Arc::new(job);
        let slot = || RemoteSlot::for_test(b.jobs.clone(), key.clone(), unit.clone(), ctx.tx.clone());
        let gone = |r: Result<Result<Remote>, time::error::Elapsed>| matches!(r, Ok(Ok(Remote::Gone)));
        // Staging reads the last dep-info first: a FIFO, which holds it up
        // until something opens it for writing.
        let dep_info = remote::absolute(Path::new(job.cwd.as_str()), &job.paths.dep_info);
        mkfifo(&dep_info)?;
        let (mut sock, wrapper) = UnixStream::pair()?;
        let mut buf = BytesMut::new();
        drop(wrapper);
        let staged = time::timeout(Duration::from_secs(5), run_remote(&mut sock, &mut buf, &ctx, &job, slot())).await;
        let fifo = dep_info.clone();
        task::spawn_blocking(move || OpenOptions::new().write(true).open(fifo)).await??;
        fs::remove_file(&dep_info)?;
        assert!(gone(staged), "staging went on after the wrapper hung up");
        // Running there, which it does until stopped.
        let (mut sock, wrapper) = UnixStream::pair()?;
        let run = run_remote(&mut sock, &mut buf, &ctx, &job, slot());
        tokio::pin!(run);
        tokio::select! {
            _ = &mut run => bail!("the job ended before its sandbox started"),
            started = until(60, "starting the sandbox", || !running(&ws).is_empty()) => started?,
        }
        assert_eq!(serving(&b).await?, 1);
        drop(wrapper);
        // Hanging up without reading the dep-info line it was sent, as a
        // wrapper cargo kills may.
        assert!(gone(time::timeout(Duration::from_secs(5), run).await), "the job went on after the wrapper hung up");
        let mut tries = 0;
        while serving(&b).await? > 0 {
            tries += 1;
            assert!(tries < 100, "the box still serves the job");
            time::sleep(Duration::from_millis(100)).await;
        }
        until(10, "killing the sandbox", || running(&ws).is_empty()).await?;
        drop(b);
        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    fn scratch(name: &str) -> Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-{name}-{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn mkfifo(at: &Path) -> Result<()> {
        let path = CString::new(at.as_os_str().as_bytes())?;
        if unsafe { libc::mkfifo(path.as_ptr(), 0o600) } != 0 {
            return Err(io::Error::last_os_error()).with_context(|| format_compact!("mkfifo {}", at.display()));
        }
        Ok(())
    }

    #[test]
    fn a_dead_daemons_fifos_go_and_nothing_else() -> Result<()> {
        let dir = scratch("fifos")?;
        let fifo = dir.join("job-3-17");
        mkfifo(&fifo)?;
        fs::write(dir.join("lock"), "")?;
        remove_stale_fifos(&dir);
        assert!(!fifo.exists(), "the FIFO goes");
        assert!(dir.join("lock").exists(), "a file that isn't a FIFO stays");
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}

