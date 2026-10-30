use crate::{
    classes::{self, Profile},
    config,
    protocol::{self, Ask, PROTOCOL, Ran, Reply, Unit},
    sched::{Admission, Msg, Sched, Setup},
    sys,
};
use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use compact_str::format_compact;
use log::{debug, error, info, warn};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal::unix::{SignalKind, signal},
    sync::{mpsc, oneshot},
    time,
};

pub async fn run() -> Result<()> {
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
    let setup = Setup {
        state_dir,
        fifo_dir: dir,
        allowed: sys::allowed_cpus()?,
        mem_limit_kb: (sys::mem_total_kb()? as f64 * cfg.memory_fraction) as u64,
        calibration_seconds: cfg.calibration_seconds,
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let sched = Sched::new(setup, profile, tx.clone())?;
    let mut sched = tokio::spawn(sched.run(rx));
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    info!("listening on {}", sock_path.display());
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((sock, _)) => {
                    let (tx, stop) = (tx.clone(), stop_tx.clone());
                    tokio::spawn(async move {
                        if let Err(e) = serve(sock, tx, stop).await {
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
    let _ = fs::remove_file(&sock_path);
    let _ = tx.send(Msg::Drain);
    if !sched.is_finished() {
        sched.await?;
    }
    Ok(())
}

/// FIFOs left by a daemon that died without cleaning up.
fn remove_stale_fifos(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().starts_with("class-") {
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

async fn serve(
    mut sock: UnixStream,
    tx: mpsc::UnboundedSender<Msg>,
    stop: mpsc::Sender<()>,
) -> Result<()> {
    let mut version = [0u8; 4];
    sock.read_exact(&mut version).await?;
    let version = u32::from_be_bytes(version);
    if version != PROTOCOL {
        warn!("a client speaks protocol {version}, this daemon {PROTOCOL}; restarting");
        let _ = stop.try_send(());
        return Ok(());
    }
    let mut buf = BytesMut::new();
    match protocol::recv_async::<Ask>(&mut sock, &mut buf).await? {
        Ask::Probe => protocol::send_async(&mut sock, &mut buf, &Reply::Trivial).await,
        Ask::Compile(unit) => compile(sock, buf, tx, unit).await,
        Ask::Status => {
            let (reply, status) = oneshot::channel();
            tx.send(Msg::Status { reply }).map_err(|_| anyhow!("scheduler stopped"))?;
            let status = status.await?;
            protocol::send_async(&mut sock, &mut buf, &Reply::Status(status)).await
        }
        Ask::Shutdown => {
            let _ = stop.try_send(());
            protocol::send_async(&mut sock, &mut buf, &Reply::Restarting).await
        }
        Ask::Profile { name, recalibrate } => {
            let reply = match Profile::new(&name) {
                Ok(profile) => {
                    tx.send(Msg::Profile { profile, recalibrate })
                        .map_err(|_| anyhow!("scheduler stopped"))?;
                    Reply::Done
                }
                Err(e) => Reply::Refused(format_compact!("{e:#}").as_str().into()),
            };
            protocol::send_async(&mut sock, &mut buf, &reply).await
        }
    }
}

async fn compile(
    mut sock: UnixStream,
    mut buf: BytesMut,
    tx: mpsc::UnboundedSender<Msg>,
    unit: Unit,
) -> Result<()> {
    let (reply, mut admitted) = oneshot::channel();
    tx.send(Msg::Submit { unit, reply }).map_err(|_| anyhow!("scheduler stopped"))?;
    let admission = loop {
        tokio::select! {
            admission = &mut admitted => break admission.context("scheduler stopped")?,
            readable = sock.readable() => {
                readable?;
                match sock.try_read(&mut [0u8; 1]) {
                    // cargo killed the wrapper while it waited
                    Ok(0) => return Ok(()),
                    Ok(_) => bail!("wrapper sent data while waiting for a slot"),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    };
    match admission {
        Admission::Restarting => protocol::send_async(&mut sock, &mut buf, &Reply::Restarting).await,
        Admission::Slot(slot) => {
            sock.write_all(&slot.grant).await?;
            if let Ok(ran) = protocol::recv_async::<Ran>(&mut sock, &mut buf).await {
                slot.finish(ran);
            }
            Ok(())
        }
    }
}
