use crate::{
    config, sys,
    protocol::{self, Ask, PROTOCOL, Reply},
};
use anyhow::{Context, Result, bail};
use bytes::BytesMut;
use std::{
    env,
    fs::OpenOptions,
    io::{ErrorKind, Write},
    os::unix::{net::UnixStream, process::CommandExt},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// Send `ask` and return the daemon's reply, following the daemon
/// through restarts. With `spawn`, start the daemon if none is running.
pub fn request(ask: Ask, spawn: bool, buf: &mut BytesMut) -> Result<(UnixStream, Reply)> {
    exchange(&ask, spawn, buf)?.context("the rustc-dist daemon is not running")
}

/// Send `ask` to the daemon if one is running, and return its reply.
pub fn request_running(ask: Ask, buf: &mut BytesMut) -> Result<Option<Reply>> {
    Ok(exchange(&ask, false, buf)?.map(|(_, reply)| reply))
}

/// None if no daemon is running and `spawn` is false.
fn exchange(ask: &Ask, spawn: bool, buf: &mut BytesMut) -> Result<Option<(UnixStream, Reply)>> {
    // CR claude for claude: [bug/high] The 30 s deadline starts before the first attempt, and an
    // attempt (open) lasts until the daemon answers, which for a compile means until it has a slot,
    // or a remote job's first output. So a wrapper that queued for over 30 s and then gets
    // Reply::Restarting, which Sched::run sends every queued job when the daemon drains, or EOF
    // from a daemon that died, fails at once with "the rustc-dist daemon kept restarting" or
    // "talking to the rustc-dist daemon" instead of reconnecting. Restarting the daemon during a
    // big build (a deploy, systemctl restart, a client of another version: daemon::serve) then
    // fails every cargo with a wrapper queued that long. Fix: start the 30 s at the first failure,
    // and again after any attempt the daemon held for a while (longer than the 50 ms retry, say),
    // so only a run of quick failures gives up. A test with a fake daemon that answers Restarting
    // after the deadline would catch it. (client-01)
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let Some(mut sock) = connect(spawn)? else { return Ok(None) };
        // A daemon from another version closes without replying and restarts.
        match open(&mut sock, buf, ask) {
            Ok(Reply::Restarting) | Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(50))
            }
            Ok(Reply::Restarting) => bail!("the rustc-dist daemon kept restarting"),
            Err(e) => return Err(e.context("talking to the rustc-dist daemon")),
            Ok(reply) => return Ok(Some((sock, reply))),
        }
    }
}

fn open(sock: &mut UnixStream, buf: &mut BytesMut, ask: &Ask) -> Result<Reply> {
    sock.write_all(&PROTOCOL.to_be_bytes())?;
    protocol::send(sock, buf, ask)?;
    protocol::recv(sock, buf)
}

/// A connection to the daemon, if one is running.
fn running() -> Result<Option<UnixStream>> {
    match UnixStream::connect(config::socket_path()) {
        Ok(sock) => Ok(Some(sock)),
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused) => Ok(None),
        Err(e) => Err(e).context("connecting to the daemon"),
    }
}

/// Ask a running daemon to exit. Returns false if none was running.
pub fn shutdown() -> Result<bool> {
    let Some(mut sock) = running()? else { return Ok(false) };
    // Any daemon stops on this, even one from another version that can't reply.
    let _ = open(&mut sock, &mut BytesMut::new(), &Ask::Shutdown);
    Ok(true)
}

fn connect(spawn: bool) -> Result<Option<UnixStream>> {
    if let Some(sock) = running()? {
        return Ok(Some(sock));
    }
    if !spawn {
        return Ok(None);
    }
    spawn_daemon()?;
    for _ in 0..200 {
        thread::sleep(Duration::from_millis(25));
        if let Some(sock) = running()? {
            return Ok(Some(sock));
        }
    }
    bail!("the rustc-dist daemon did not start, see {}", config::log_path().display())
}

/// Start the daemon: through systemd if its user service is installed, so a
/// wrapper can't start a second daemon while systemd restarts its own, and
/// leave the service failing to take the lock; else on its own.
fn spawn_daemon() -> Result<()> {
    if config::systemd_unit().exists() {
        return sys::systemctl(&["start", "rustc-dist.service"]);
    }
    config::make_private_dir(&config::runtime_dir())?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::log_path())
        .context("opening the daemon log")?;
    let mut cmd = Command::new(env::current_exe().context("finding rustc-dist")?);
    cmd.arg("daemon")
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .process_group(0);
    // Cargo hands its jobserver pipe to the wrapper; the daemon must not keep it.
    unsafe {
        cmd.pre_exec(|| {
            libc::close_range(3, u32::MAX, libc::CLOSE_RANGE_CLOEXEC as i32);
            Ok(())
        });
    }
    cmd.spawn().context("starting the rustc-dist daemon")?;
    Ok(())
}
