use crate::{
    blobs::{Hashes, Store},
    browser,
    classes::{self, Profile},
    config::{self, Config},
    farm::{self, Load, Member},
    protocol::{self, Ask, Job, PROTOCOL, Ran, Reads, Reply, Unit},
    remote::{self, Event, Moved, Outcome, ToServer},
    sandbox::{self, Server},
    sched::{Admission, Msg, RemoteOutcome, RemoteSlot, Sched, SentRun, Setup, Want},
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
    subscriber::{Subscriber, SubscriberBuilder},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal::unix::{SignalKind, signal},
    sync::{mpsc, oneshot, watch},
    task, time,
};

/// What every connection needs.
struct Ctx {
    tx: mpsc::UnboundedSender<Msg>,
    stop: mpsc::Sender<()>,
    keys: Keys,
    hashes: triomphe::Arc<Hashes>,
    /// Present when this box is part of a farm.
    subscriber: Option<Subscriber>,
    remote_links: bool,
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
    let hashes = triomphe::Arc::new(Hashes::default());
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
        tokens_per_cpu: cfg.tokens_per_cpu,
        calibration_seconds: cfg.calibration_seconds,
        decay: cfg.decay(),
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let (load_tx, load) = watch::channel(Load::default());
    let sched = Sched::new(setup, profile, load_tx, tx.clone())?;
    let mut sched = tokio::spawn(sched.run(rx));
    let (subscriber, publisher) = match &cfg.base {
        None => (None, None),
        Some(base) => match join(&cfg, base, load, tx.clone()).await {
            Ok((sub, publisher)) => (Some(sub), Some(publisher)),
            Err(e) => {
                error!("working alone, could not join the farm at {base}: {e:#}");
                (None, None)
            }
        },
    };
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let ctx = Arc::new(Ctx {
        tx: tx.clone(),
        stop: stop_tx,
        keys: Keys::default(),
        hashes,
        subscriber,
        remote_links: cfg.remote_links,
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
                            debug!("connection: {e:#}");
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
    if let Some(publisher) = publisher
        && time::timeout(Duration::from_secs(2), publisher.shutdown()).await.is_err()
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

/// Publish this box under `base`, watch the others, and take their jobs.
/// Shutting down the publisher leaves the farm.
async fn join(
    cfg: &Config,
    base: &str,
    load: watch::Receiver<Load>,
    tx: mpsc::UnboundedSender<Msg>,
) -> Result<(Subscriber, Publisher)> {
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
    let store = Store::open(cfg.cache_dir.join("blobs"), cfg.cache_gb << 30)?;
    let server = Arc::new(Server::new(store, cfg.cache_dir.join("jobs"), installed, tx.clone())?);
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
    tokio::spawn(async move {
        if let Err(e) = farm::run(member, load, tx).await {
            error!("left the farm: {e:#}");
        }
    });
    Ok((subscriber, publisher))
}

fn hostname() -> String {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|h| h.trim().split('.').next().unwrap_or_default().replace('/', "_"))
        .unwrap_or_else(|_| "rustc-dist".into())
}

/// FIFOs left by a daemon that died without cleaning up.
fn remove_stale_fifos(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("class-") || name.starts_with("spread-") {
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
    if version != PROTOCOL {
        warn!("a client speaks protocol {version}, this daemon {PROTOCOL}; restarting");
        let _ = ctx.stop.try_send(());
        return Ok(());
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
        Ask::Shutdown => {
            let _ = ctx.stop.try_send(());
            protocol::send_async(&mut sock, &mut buf, &Reply::Restarting).await
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
async fn admit(sock: &mut UnixStream, ctx: &Ctx, unit: Unit, build: u32, remote: Option<Want>) -> Result<Option<Admission>> {
    let (reply, mut admitted) = oneshot::channel();
    ctx.tx.send(Msg::Submit { unit, build, remote, reply }).map_err(|_| anyhow!("scheduler stopped"))?;
    loop {
        tokio::select! {
            admission = &mut admitted => return Ok(Some(admission.context("scheduler stopped")?)),
            readable = sock.readable() => {
                readable?;
                match sock.try_read(&mut [0u8; 1]) {
                    Ok(0) => return Ok(None),
                    Ok(_) => bail!("wrapper sent data while waiting for a slot"),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
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
    let mut want = match (&job, &ctx.subscriber) {
        (Some(job), Some(_)) if job.reads == Reads::Metadata || ctx.remote_links => {
            let keys = &ctx.keys;
            let rustc = PathBuf::from(job.rustc.as_str());
            match task::block_in_place(|| keys.key(&rustc)) {
                Ok(key) => Some(key),
                Err(e) => {
                    warn!("{e:#}");
                    None
                }
            }
        }
        _ => None,
    };
    // Set once a remote run failed after passing cargo an rmeta notice.
    let mut filter_metadata = false;
    let mut failed_remotely = false;
    loop {
        let remote = want.clone().zip(job.as_ref()).map(|(key, j)| Want {
            key,
            target: j.target.clone(),
            reads: j.reads.clone(),
            cwd: j.cwd.clone(),
        });
        let Some(admission) = admit(&mut sock, ctx, unit.clone(), build, remote).await? else {
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
                if let Ok(ran) = protocol::recv_async::<Ran>(&mut sock, &mut buf).await {
                    slot.finish(ran, 0);
                    if failed_remotely {
                        let _ = ctx.tx.send(Msg::Mismatch { unit });
                    }
                }
                return Ok(());
            }
            Admission::Remote(r) => {
                let job = job.as_ref().expect("only portable jobs go remote");
                match run_remote(&mut sock, &mut buf, ctx, job, r).await {
                    Ok(Remote::Done) => return Ok(()),
                    Ok(Remote::Busy) => (),
                    Ok(Remote::Failed { forwarded_metadata }) => {
                        filter_metadata |= forwarded_metadata;
                        failed_remotely = true;
                        want = None;
                    }
                    Err(e) => {
                        warn!("{}: {e:#}", unit.exact);
                        want = None;
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
}

async fn run_remote(
    sock: &mut UnixStream,
    buf: &mut BytesMut,
    ctx: &Ctx,
    job: &Job,
    r: RemoteSlot,
) -> Result<Remote> {
    let subscriber = ctx.subscriber.as_ref().context("not in a farm")?;
    let start = Instant::now();
    let staged = task::block_in_place(|| remote::stage(job, &r.crates, &ctx.hashes))?;
    let offer = ToServer::Offer { unit: r.unit().clone(), key: r.key.clone(), target: r.target.clone(), class: r.class };
    let (etx, mut events) = mpsc::unbounded_channel();
    let running = remote::run(subscriber, r.jobs.clone(), offer, job, &staged, &ctx.hashes, |ev| {
        etx.send(ev).map_err(|_| anyhow!("daemon connection gone"))
    });
    tokio::pin!(running);
    // Diagnostics wait for the exit, so a failure here leaves nothing for
    // cargo to see twice.
    let mut held: Vec<(bool, Bytes)> = Vec::new();
    let mut forwarded_metadata = false;
    let mut handle = async |ev: Event, sock: &mut UnixStream, buf: &mut BytesMut| -> Result<()> {
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
    let outcome = loop {
        tokio::select! {
            outcome = &mut running => break outcome,
            Some(ev) = events.recv() => handle(ev, sock, buf).await?,
        }
    };
    while let Ok(ev) = events.try_recv() {
        handle(ev, sock, buf).await?;
    }
    drop(handle);
    match outcome {
        Ok(Outcome::Exit { code: 0, ran, measured, moved: Moved { bytes, linker_waited_us, kept } }) => {
            for (stderr, data) in held {
                protocol::send_async(sock, buf, &Reply::Output { stderr, data }).await?;
            }
            protocol::send_async(sock, buf, &Reply::Exit(0)).await?;
            let copy_us = (start.elapsed().as_micros() as u64).saturating_sub(ran.wall_us) + linker_waited_us;
            let links = job.reads != Reads::Metadata;
            let transfer = Transfer { sent_kb: bytes / 1024, copy_ms: copy_us / 1000, links };
            info!(
                "{} on {}: compiled in {} ms, moving files took {} ms ({} kB sent, linker waited {} ms)",
                r.unit().exact,
                r.jobs,
                ran.wall_us / 1000,
                transfer.copy_ms,
                transfer.sent_kb,
                linker_waited_us / 1000
            );
            r.finish(RemoteOutcome::Done(SentRun { ran, measured, linker_waited_us, transfer, kept }));
            Ok(Remote::Done)
        }
        Ok(Outcome::Busy { version }) => {
            r.finish(RemoteOutcome::Busy { version });
            Ok(Remote::Busy)
        }
        Ok(Outcome::Exit { code, .. }) => {
            debug!("{} exited {code} remotely, building it here", r.unit().exact);
            for (_, line) in held.iter().filter(|(stderr, _)| *stderr).take(5) {
                debug!("remote stderr: {}", String::from_utf8_lossy(line).trim_end());
            }
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

