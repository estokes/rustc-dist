use crate::{
    config, sys,
    protocol::{self, Ask, OtherProtocol, PROTOCOL, Reply, STOP},
};
use anyhow::{Context, Error, Result, anyhow, bail};
use bytes::BytesMut;
use compact_str::format_compact;
use std::{
    env,
    fs::OpenOptions,
    io::{ErrorKind, Read, Write},
    os::unix::{net::UnixStream, process::CommandExt},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// How long a run of failures to get an answer may go on, from its first.
const GIVE_UP: Duration = Duration::from_secs(30);
/// An ask the daemon held this long before failing was queued there, so
/// the failures before it don't count toward giving up.
const HELD: Duration = Duration::from_secs(1);
const RETRY: Duration = Duration::from_millis(50);
/// How many daemon restarts one ask follows.
pub const RESTARTS: u32 = 5;

/// The daemon restarts one ask has followed, at most `RESTARTS`.
#[derive(Default)]
pub struct Restarts(u32);

impl Restarts {
    /// Follow one more restart, which showed as `lost`, unless the ask has
    /// followed `RESTARTS` already.
    pub fn follow(&mut self, lost: Error) -> Result<()> {
        if self.0 == RESTARTS {
            return Err(lost.context(format_compact!("gave up after following {RESTARTS} restarts of the rustc-dist daemon")));
        }
        self.0 += 1;
        Ok(())
    }
}

/// Send `ask` and return the daemon's reply, following the daemon
/// through restarts. With `spawn`, start the daemon if none is running.
pub fn request(ask: &Ask, spawn: bool, buf: &mut BytesMut) -> Result<(UnixStream, Reply)> {
    request_after(ask, spawn, buf, &mut Restarts::default())
}

/// As `request`, for an ask that has followed `restarts` already.
pub fn request_after(ask: &Ask, spawn: bool, buf: &mut BytesMut, restarts: &mut Restarts) -> Result<(UnixStream, Reply)> {
    follow(|| connect(spawn), ask, buf, GIVE_UP, restarts)?.context("the rustc-dist daemon is not running")
}

/// Send `ask` to the daemon if one is running, and return its reply.
pub fn request_running(ask: &Ask, buf: &mut BytesMut) -> Result<Option<Reply>> {
    Ok(follow(|| connect(false), ask, buf, GIVE_UP, &mut Restarts::default())?.map(|(_, reply)| reply))
}

/// Ask through each connection `connect` makes until a daemon answers.
/// An ask the daemon held before it failed met a restart, which counts
/// toward `restarts`. It gives up once failures have gone on for `give_up`,
/// counting from the first since such a restart, so a restart is followed
/// however long the ask waited before it. None if `connect` finds no
/// daemon.
fn follow(
    mut connect: impl FnMut() -> Result<Option<UnixStream>>,
    ask: &Ask,
    buf: &mut BytesMut,
    give_up: Duration,
    restarts: &mut Restarts,
) -> Result<Option<(UnixStream, Reply)>> {
    let mut failing_since = None;
    loop {
        let Some(mut sock) = connect()? else { return Ok(None) };
        let asked = Instant::now();
        let failed = match open(&mut sock, buf, ask) {
            Ok(Reply::Restarting) => anyhow!("the rustc-dist daemon kept restarting"),
            Ok(reply) => return Ok(Some((sock, reply))),
            Err(e) if e.downcast_ref::<OtherProtocol>().is_some() => return Err(e),
            Err(e) => e.context("talking to the rustc-dist daemon"),
        };
        let now = Instant::now();
        if now - asked >= HELD {
            restarts.follow(failed)?;
            failing_since = Some(now);
        } else if now - *failing_since.get_or_insert(now) >= give_up {
            return Err(failed);
        }
        thread::sleep(RETRY);
    }
}

fn open(sock: &mut UnixStream, buf: &mut BytesMut, ask: &Ask) -> Result<Reply> {
    sock.write_all(&PROTOCOL.to_be_bytes())?;
    protocol::send(sock, buf, ask)?;
    protocol::recv_reply(sock, buf)
}

/// A connection to the daemon, if one is running.
fn running() -> Result<Option<UnixStream>> {
    match UnixStream::connect(config::socket_path()) {
        Ok(sock) => Ok(Some(sock)),
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused) => Ok(None),
        Err(e) => Err(e).context("connecting to the daemon"),
    }
}

/// Ask a running daemon of any version to exit. Returns false if none was
/// running.
pub fn shutdown() -> Result<bool> {
    let Some(mut sock) = running()? else { return Ok(false) };
    sock.write_all(&STOP.to_be_bytes()).context("asking the daemon to stop")?;
    // It hangs up once it has read that.
    let _ = sock.read(&mut [0u8; 1]);
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

#[cfg(test)]
mod test {
    use super::*;
    use std::iter;

    enum Says {
        Reply(Reply),
        HangsUp,
        Refuses,
    }

    /// A daemon that answers each connection with the next of `answers`,
    /// after holding the ask for its time.
    fn daemon(
        answers: impl Iterator<Item = (Duration, Says)> + Send + 'static,
    ) -> impl FnMut() -> Result<Option<UnixStream>> {
        let mut answers = answers;
        move || {
            let (hold, says) = answers.next().context("asked once too often")?;
            let (ours, mut theirs) = UnixStream::pair()?;
            thread::spawn(move || {
                let mut buf = BytesMut::new();
                let mut version = [0u8; 4];
                theirs.read_exact(&mut version)?;
                let _: Ask = protocol::recv(&mut theirs, &mut buf)?;
                thread::sleep(hold);
                match says {
                    Says::Reply(reply) => protocol::send(&mut theirs, &mut buf, &reply)?,
                    Says::HangsUp => (),
                    Says::Refuses => theirs.write_all(&protocol::refusal())?,
                }
                Ok::<_, anyhow::Error>(())
            });
            Ok(Some(ours))
        }
    }

    #[test]
    fn an_ask_queued_past_the_give_up_time_follows_a_restart() -> Result<()> {
        let answers = [
            (HELD + Duration::from_millis(200), Says::Reply(Reply::Restarting)),
            (Duration::ZERO, Says::HangsUp),
            (Duration::ZERO, Says::Reply(Reply::Done)),
        ];
        let reply = follow(daemon(answers.into_iter()), &Ask::Status, &mut BytesMut::new(), Duration::from_millis(300), &mut Restarts::default())?;
        assert!(matches!(reply, Some((_, Reply::Done))), "{reply:?}");
        Ok(())
    }

    #[test]
    fn a_daemon_that_dies_each_time_it_holds_the_ask_counts_toward_the_restarts_it_follows() {
        let mut restarts = Restarts::default();
        for _ in 1..RESTARTS {
            restarts.follow(anyhow!("an earlier restart")).unwrap();
        }
        let held = HELD + Duration::from_millis(50);
        let dying = daemon(iter::repeat_with(move || (held, Says::HangsUp)).take(3));
        let e = follow(dying, &Ask::Status, &mut BytesMut::new(), Duration::from_millis(300), &mut restarts).unwrap_err();
        assert!(format!("{e:#}").contains(&format!("gave up after following {RESTARTS} restarts")), "{e:#}");
    }

    #[test]
    fn a_run_of_quick_failures_gives_up() {
        let give_up = Duration::from_millis(300);
        let start = Instant::now();
        let hangs_up = daemon(iter::repeat_with(|| (Duration::ZERO, Says::HangsUp)));
        let e = follow(hangs_up, &Ask::Status, &mut BytesMut::new(), give_up, &mut Restarts::default()).unwrap_err();
        assert!(start.elapsed() >= give_up && start.elapsed() < HELD, "gave up after {:?}: {e:#}", start.elapsed());
        let start = Instant::now();
        let restarting = daemon(iter::repeat_with(|| (Duration::ZERO, Says::Reply(Reply::Restarting))));
        let e = follow(restarting, &Ask::Status, &mut BytesMut::new(), give_up, &mut Restarts::default()).unwrap_err();
        assert!(start.elapsed() >= give_up, "{e:#}");
        assert_eq!(e.to_string(), "the rustc-dist daemon kept restarting");
    }

    #[test]
    fn a_daemon_of_another_protocol_fails_the_ask_at_once() {
        let start = Instant::now();
        let refuses = daemon(iter::repeat_with(|| (Duration::ZERO, Says::Refuses)));
        let e = follow(refuses, &Ask::Status, &mut BytesMut::new(), Duration::from_secs(5), &mut Restarts::default()).unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(1), "retried for {:?}", start.elapsed());
        let other = e.downcast_ref::<OtherProtocol>().expect("the daemon's protocol");
        assert_eq!(other.daemon, PROTOCOL);
        assert!(e.to_string().contains(&format!("protocol {PROTOCOL}")), "{e}");
    }
}
