use crate::{
    classes, config,
    protocol::{self, Ask, PROTOCOL, Ran, Reply, Unit},
    sched::{Admission, Msg, Sched},
    sys,
};
use anyhow::{Context, Result, anyhow, bail};
use bytes::{Bytes, BytesMut};
use compact_str::format_compact;
use log::{debug, info, warn};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
    sync::Arc,
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
    let classes = classes::load(&sys::allowed_cpus()?)?;
    for (k, c) in classes.iter().enumerate() {
        info!("class {k}: {} cpus, calibration {} ms", c.cpus.len(), c.calib_us / 1000);
    }
    let mem_limit_kb = (sys::mem_total_kb()? as f64 * cfg.memory_fraction) as u64;
    let (tx, rx) = mpsc::unbounded_channel();
    let stats_path = config::state_dir().join("jobs.hdr");
    let (sched, grants) = Sched::new(classes, &dir, mem_limit_kb, stats_path, tx.clone())?;
    let grants: Arc<[Bytes]> = grants.into();
    let sched = tokio::spawn(sched.run(rx));
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    info!("listening on {}", sock_path.display());
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((sock, _)) => {
                    let (tx, grants, stop) = (tx.clone(), grants.clone(), stop_tx.clone());
                    tokio::spawn(async move {
                        if let Err(e) = serve(sock, tx, grants, stop).await {
                            debug!("connection: {e:#}");
                        }
                    });
                }
                Err(e) => warn!("accept: {e}"),
            },
            _ = stop_rx.recv() => break,
            _ = term.recv() => break,
            _ = int.recv() => break,
        }
    }
    info!("shutting down");
    drop(listener);
    let _ = fs::remove_file(&sock_path);
    let _ = tx.send(Msg::Drain);
    sched.await?;
    Ok(())
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
    grants: Arc<[Bytes]>,
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
        Ask::Compile(unit) => compile(sock, buf, tx, &grants, unit).await,
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
    }
}

async fn compile(
    mut sock: UnixStream,
    mut buf: BytesMut,
    tx: mpsc::UnboundedSender<Msg>,
    grants: &[Bytes],
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
            sock.write_all(&grants[slot.class]).await?;
            if let Ok(ran) = protocol::recv_async::<Ran>(&mut sock, &mut buf).await {
                slot.finish(ran);
            }
            Ok(())
        }
    }
}
