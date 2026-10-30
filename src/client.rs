use crate::{
    config,
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
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut sock = connect(spawn)?;
        // A daemon from another version closes without replying and restarts.
        match open(&mut sock, buf, &ask) {
            Ok(Reply::Restarting) | Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(50))
            }
            Ok(Reply::Restarting) => bail!("the rustc-dist daemon kept restarting"),
            Err(e) => return Err(e.context("talking to the rustc-dist daemon")),
            Ok(reply) => return Ok((sock, reply)),
        }
    }
}

fn open(sock: &mut UnixStream, buf: &mut BytesMut, ask: &Ask) -> Result<Reply> {
    sock.write_all(&PROTOCOL.to_be_bytes())?;
    protocol::send(sock, buf, ask)?;
    protocol::recv(sock, buf)
}

/// Ask a running daemon to exit. Returns false if none was running.
pub fn shutdown() -> Result<bool> {
    let mut sock = match UnixStream::connect(config::socket_path()) {
        Ok(sock) => sock,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused) => {
            return Ok(false);
        }
        Err(e) => return Err(e).context("connecting to the daemon"),
    };
    // Any daemon stops on this, even one from another version that can't reply.
    let _ = open(&mut sock, &mut BytesMut::new(), &Ask::Shutdown);
    Ok(true)
}

fn connect(spawn: bool) -> Result<UnixStream> {
    let path = config::socket_path();
    match UnixStream::connect(&path) {
        Ok(sock) => return Ok(sock),
        Err(_) if spawn => spawn_daemon()?,
        Err(e) => return Err(e).context("the rustc-dist daemon is not running"),
    }
    for _ in 0..200 {
        thread::sleep(Duration::from_millis(25));
        if let Ok(sock) = UnixStream::connect(&path) {
            return Ok(sock);
        }
    }
    bail!("the rustc-dist daemon did not start, see {}", config::log_path().display())
}

fn spawn_daemon() -> Result<()> {
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
