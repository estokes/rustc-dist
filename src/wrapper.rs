use crate::{
    argv::{self, Compile, Invocation},
    client::{self, Restarts},
    config,
    protocol::{self, Ask, Finished, Front, Job, Ran, Reply},
    remote, sys,
};
use anyhow::{Context, Error, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::BytesMut;
use compact_str::format_compact;
use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, BufRead, BufReader, Write},
    os::unix::{
        net::UnixStream,
        process::{CommandExt, ExitStatusExt, parent_id},
    },
    path::Path,
    process::{self, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

/// Environment another box must not inherit: jobservers, the wrapper
/// itself, and library preloading.
const LOCAL_ENV: &[&str] =
    &["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "PATH", "TMPDIR"];

/// The pause before asking again for a job a daemon restart lost after its
/// first reply.
const RERUN: Duration = Duration::from_millis(250);

pub fn run(args: Vec<OsString>) -> Result<()> {
    if args.is_empty() {
        bail!("cargo ran the wrapper without a compiler");
    }
    if env::var_os("RUSTC_DIST").is_some_and(|v| v == "off") || sys::running_as_root() {
        return Err(exec(&args));
    }
    let ask = match argv::classify(&args, env::var_os("OUT_DIR").as_deref()) {
        Invocation::Probe => Ask::Probe,
        Invocation::Compile(c) => {
            let build = parent_id();
            let job = job(&args, &c, build);
            Ask::Compile { unit: c.unit, build, job }
        }
    };
    let mut buf = BytesMut::new();
    let request = |ask: &Ask, buf: &mut BytesMut, restarts: &mut Restarts| client::request_after(ask, true, buf, restarts);
    match answer(&ask, &mut buf, request, &mut io::stderr(), &mut io::stdout())? {
        Answer::Trivial => Err(exec(&args)),
        Answer::Slot { mut sock, cpus, jobserver, filter_metadata } => {
            sys::pin(&cpus)?;
            let out = &mut io::stderr().lock();
            let status = compile(&mut sock, &mut buf, &args, &jobserver, filter_metadata, out)?;
            exit_as(status)
        }
        Answer::Exited(code) => process::exit(code),
    }
}

/// What the daemon said to do with a compile.
enum Answer {
    /// Run rustc as it is, without a slot.
    Trivial,
    /// Run it here, holding the slot `sock` stands for.
    Slot { sock: UnixStream, cpus: Vec<u32>, jobserver: ArcStr, filter_metadata: bool },
    /// It ran on another box and exited with this code.
    Exited(i32),
}

/// Ask with `request` until a daemon answers, printing a remote run's
/// output to `err` and `out` meanwhile. A daemon that goes away mid-job, or
/// sends the job back while it restarts, is asked again through `request`
/// after a pause, which follows the restart. Each such loss counts as a
/// restart the ask followed, as do those `request` follows. Cargo gets one
/// rmeta line, however many runs print one.
fn answer(
    ask: &Ask,
    buf: &mut BytesMut,
    mut request: impl FnMut(&Ask, &mut BytesMut, &mut Restarts) -> Result<(UnixStream, Reply)>,
    err: &mut impl Write,
    out: &mut impl Write,
) -> Result<Answer> {
    let mut printed_metadata = false;
    let mut restarts = Restarts::default();
    loop {
        let (mut sock, mut reply) = request(ask, buf, &mut restarts)?;
        let lost = loop {
            match reply {
                Reply::Trivial => return Ok(Answer::Trivial),
                Reply::Slot { cpus, jobserver, filter_metadata } => {
                    let filter_metadata = filter_metadata || printed_metadata;
                    return Ok(Answer::Slot { sock, cpus, jobserver, filter_metadata });
                }
                Reply::Output { stderr: true, data } => {
                    let metadata = protocol::is_metadata_line(&data);
                    if !(metadata && printed_metadata) {
                        err.write_all(&data)?;
                    }
                    printed_metadata |= metadata;
                }
                Reply::Output { stderr: false, data } => out.write_all(&data)?,
                Reply::Exit(code) => return Ok(Answer::Exited(code)),
                Reply::Refused(why) => bail!("the rustc-dist daemon can't run this compile: {why}"),
                Reply::Restarting => break anyhow!("the rustc-dist daemon sent the job back as it restarted"),
                Reply::Status(_) | Reply::Done => bail!("unexpected reply from the daemon"),
            }
            reply = match protocol::recv(&mut sock, buf) {
                Ok(reply) => reply,
                Err(e) if e.downcast_ref::<io::Error>().is_some() => break e.context("the rustc-dist daemon went away mid-job"),
                Err(e) => return Err(e.context("reading the daemon's reply")),
            };
        };
        restarts.follow(lost)?;
        thread::sleep(RERUN);
    }
}

/// What another box needs to run this compile, unless it must stay here.
fn job(args: &[OsString], c: &Compile, build: u32) -> Option<Job> {
    let portable = c.portable.as_ref()?;
    let cargo_cwd = fs::read_link(format_compact!("/proc/{build}/cwd").as_str()).ok()?;
    match config::project(&cargo_cwd) {
        Ok(p) if p.keeps_local(&c.name) => return None,
        Ok(_) => (),
        Err(e) => {
            eprintln!("rustc-dist: {e:#}");
            return None;
        }
    }
    let utf8 = |s: &OsString| s.to_str().map(ArcStr::from);
    let rustc = utf8(&args[0])?;
    let args = args[1..].iter().map(utf8).collect::<Option<Vec<_>>>()?;
    let env = env::vars_os()
        .filter_map(|(k, v)| Some((utf8(&k)?, utf8(&v)?)))
        .filter(|(k, _)| !LOCAL_ENV.contains(&k.as_str()) && !k.starts_with("LD_"))
        .collect();
    let cwd = env::current_dir().ok()?.to_str().map(ArcStr::from)?;
    let (paths, reads) = (portable.paths.clone(), portable.reads.clone());
    Some(Job { rustc, args, env, cwd, target: c.target.clone(), paths, reads })
}

fn exec(args: &[OsString]) -> Error {
    let err = Command::new(&args[0]).args(&args[1..]).exec();
    anyhow!(err).context(format_compact!("exec {}", Path::new(&args[0]).display()))
}

/// Run rustc on this box, passing its stderr to `out` so the front end can
/// be timed and the crate files it writes noted. With `filter_metadata`,
/// drop its rmeta notice, because cargo already got one from a remote run
/// that then failed.
fn compile(
    sock: &mut UnixStream,
    buf: &mut BytesMut,
    args: &[OsString],
    jobserver: &str,
    filter_metadata: bool,
    out: &mut impl Write,
) -> Result<ExitStatus> {
    let parent = process::id() as libc::pid_t;
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .env("CARGO_MAKEFLAGS", format_compact!("-j --jobserver-auth=fifo:{jobserver}").as_str())
        .stderr(Stdio::piped());
    // rustc must not outlive the wrapper that holds its slot.
    unsafe {
        cmd.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() != parent {
                return Err(io::Error::from_raw_os_error(libc::ESRCH));
            }
            Ok(())
        });
    }
    let start = Instant::now();
    let mut child =
        cmd.spawn().with_context(|| format_compact!("running {}", Path::new(&args[0]).display()))?;
    let mut front = None;
    let mut crates = Vec::new();
    let cwd = env::current_dir()?;
    let stderr = child.stderr.take().context("rustc's stderr")?;
    for line in BufReader::new(stderr).split(b'\n') {
        let line = line?;
        let metadata = protocol::is_metadata_line(&line);
        if metadata && front.is_none() {
            let wall_us = start.elapsed().as_micros() as u64;
            front = Some(Front { wall_us });
        }
        if protocol::is_artifact_line(&line)
            && let Ok(path) = protocol::artifact(&line)
            && remote::kept(&path)
            && let Some(path) = remote::absolute(&cwd, &path).to_str()
        {
            crates.push(ArcStr::from(path));
        }
        if !(metadata && filter_metadata) {
            out.write_all(&line)?;
            out.write_all(b"\n")?;
        }
    }
    let status = child.wait()?;
    if status.success() {
        let wall_us = start.elapsed().as_micros() as u64;
        let ran = Ran { wall_us, maxrss_kb: sys::children_maxrss_kb(), front };
        // A daemon that restarted meanwhile just misses this sample.
        let _ = protocol::send(sock, buf, &Finished { ran, crates });
    }
    Ok(status)
}

fn exit_as(status: ExitStatus) -> ! {
    if let Some(sig) = status.signal() {
        unsafe {
            libc::signal(sig, libc::SIG_DFL);
            libc::raise(sig);
        }
    }
    process::exit(status.code().unwrap_or(1))
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use std::iter;

    #[test]
    fn a_compile_reports_its_time_and_front_end() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-wrapper-{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        let mut src = String::new();
        for i in 0..200 {
            src.push_str(&format!("pub fn f{i}(v: &[u64]) -> u64 {{ v.iter().map(|x| x * {i}).sum() }}\n"));
        }
        fs::write(dir.join("lib.rs"), src)?;
        let args: Vec<OsString> = [
            "rustc", "--crate-name", "t", "--crate-type", "lib", "--edition", "2021",
            "--emit=dep-info,metadata,link", "--error-format=json", "--json=artifacts", "-C", "opt-level=2",
        ]
        .iter()
        .map(OsString::from)
        .chain([dir.join("lib.rs").into_os_string(), "--out-dir".into(), dir.clone().into_os_string()])
        .collect();
        let (mut ours, mut daemon) = UnixStream::pair()?;
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        let status = compile(&mut ours, &mut buf, &args, "/nonexistent", false, &mut out)?;
        assert!(status.success());
        let Finished { ran, crates } = protocol::recv(&mut daemon, &mut buf)?;
        let wrote = |f: &str| ArcStr::from(dir.join(f).to_str().unwrap());
        assert_eq!(crates, [wrote("libt.rmeta"), wrote("libt.rlib")], "the crate files, not the dep-info");
        let front = ran.front.context("an .rmeta line")?;
        assert!(0 < front.wall_us && front.wall_us < ran.wall_us && ran.maxrss_kb > 0, "{ran:?}");
        let forwarded = |out: &[u8]| out.split(|b| *b == b'\n').any(protocol::is_metadata_line);
        assert!(forwarded(&out), "cargo gets the .rmeta line");
        // After a remote run already sent cargo one, it is dropped, but the
        // front end is still timed.
        let mut out = Vec::new();
        assert!(compile(&mut ours, &mut buf, &args, "/nonexistent", true, &mut out)?.success());
        let Finished { ran, .. } = protocol::recv(&mut daemon, &mut buf)?;
        assert!(!forwarded(&out) && ran.front.is_some());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Daemons that each send their replies, in turn, and hang up.
    fn daemons(
        said: impl IntoIterator<Item = Vec<Reply>>,
    ) -> impl FnMut(&Ask, &mut BytesMut, &mut Restarts) -> Result<(UnixStream, Reply)> {
        let mut said = said.into_iter();
        move |_, buf, _| {
            let mut replies = said.next().context("asked once too often")?.into_iter();
            let first = replies.next().context("no reply")?;
            let (ours, mut theirs) = UnixStream::pair()?;
            for reply in replies {
                protocol::send(&mut theirs, buf, &reply)?;
            }
            Ok((ours, first))
        }
    }

    fn line(text: &str) -> Reply {
        Reply::Output { stderr: true, data: Bytes::from(format!("{text}\n")) }
    }

    const RMETA: &str = r#"{"$message_type":"artifact","artifact":"/t/liba.rmeta","emit":"metadata"}"#;
    const RLIB: &str = r#"{"$message_type":"artifact","artifact":"/t/liba.rlib","emit":"link"}"#;

    #[test]
    fn a_daemon_gone_mid_job_is_asked_again_and_cargo_gets_one_rmeta_line() -> Result<()> {
        let ask = Ask::Probe;
        let (mut err, mut out) = (Vec::new(), Vec::new());
        let gone_mid_job = daemons(vec![vec![line(RMETA)], vec![line(RMETA), line(RLIB), Reply::Exit(0)]]);
        let after_gone = answer(&ask, &mut BytesMut::new(), gone_mid_job, &mut err, &mut out)?;
        assert!(matches!(after_gone, Answer::Exited(0)));
        assert_eq!(String::from_utf8(err)?, format!("{RMETA}\n{RLIB}\n"));
        let slot = Reply::Slot { cpus: vec![0], jobserver: "/j".into(), filter_metadata: false };
        let sent_back = daemons(vec![vec![line(RMETA), Reply::Restarting], vec![slot]]);
        let after_restart = answer(&ask, &mut BytesMut::new(), sent_back, &mut Vec::new(), &mut out)?;
        assert!(
            matches!(after_restart, Answer::Slot { filter_metadata: true, .. }),
            "a rerun here after a restart drops the rmeta line cargo already has"
        );
        Ok(())
    }

    #[test]
    fn a_daemon_that_always_loses_the_job_after_its_first_reply_is_followed_only_so_far() -> Result<()> {
        let losing = || daemons(iter::repeat_with(|| vec![line(RMETA)]).take(client::RESTARTS as usize + 3));
        let (start, mut asked, mut err) = (Instant::now(), 0, Vec::new());
        let mut daemon = losing();
        let counted = |ask: &Ask, buf: &mut BytesMut, restarts: &mut Restarts| {
            asked += 1;
            daemon(ask, buf, restarts)
        };
        let Err(e) = answer(&Ask::Probe, &mut BytesMut::new(), counted, &mut err, &mut Vec::new()) else {
            bail!("answered")
        };
        let gave_up = format!("gave up after following {} restarts", client::RESTARTS);
        assert!(format!("{e:#}").contains(&gave_up), "{e:#}");
        assert_eq!(asked, client::RESTARTS + 1);
        assert!(start.elapsed() >= RERUN * client::RESTARTS, "asked again without a pause: {:?}", start.elapsed());
        assert_eq!(String::from_utf8(err)?, format!("{RMETA}\n"), "cargo gets one rmeta line");
        // Restarts the request followed, as while the ask was queued, count
        // toward the same limit.
        let (mut asked, mut daemon) = (0, losing());
        let queued_through_one = |ask: &Ask, buf: &mut BytesMut, restarts: &mut Restarts| {
            asked += 1;
            restarts.follow(anyhow!("lost while queued"))?;
            daemon(ask, buf, restarts)
        };
        let Err(e) = answer(&Ask::Probe, &mut BytesMut::new(), queued_through_one, &mut Vec::new(), &mut Vec::new())
        else {
            bail!("answered")
        };
        assert!(format!("{e:#}").contains(&gave_up), "{e:#}");
        assert_eq!(asked, client::RESTARTS / 2 + 1);
        Ok(())
    }
}
