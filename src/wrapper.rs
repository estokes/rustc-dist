use crate::{
    argv::{self, Compile, Invocation},
    client, config,
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
    time::Instant,
};

/// Environment another box must not inherit: jobservers, the wrapper
/// itself, and library preloading.
const LOCAL_ENV: &[&str] =
    &["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "PATH", "TMPDIR"];

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
    let (mut sock, mut reply) = client::request(ask, true, &mut buf)?;
    loop {
        match reply {
            Reply::Trivial => return Err(exec(&args)),
            Reply::Slot { cpus, jobserver, filter_metadata } => {
                sys::pin(&cpus)?;
                let out = &mut io::stderr().lock();
                let status = compile(&mut sock, &mut buf, &args, &jobserver, filter_metadata, out)?;
                exit_as(status)
            }
            Reply::Output { stderr: true, data } => io::stderr().write_all(&data)?,
            Reply::Output { stderr: false, data } => io::stdout().write_all(&data)?,
            Reply::Exit(code) => process::exit(code),
            Reply::Refused(why) => bail!("the rustc-dist daemon can't run this compile: {why}"),
            Reply::Status(_) | Reply::Restarting | Reply::Done => {
                bail!("unexpected reply from the daemon")
            }
        }
        // CR claude for eric: [bug/medium] Once the first reply is in, the wrapper can't follow a
        // daemon restart. A remote job whose daemon exits while it streams (main's shutdown_timeout
        // drops the connection tasks once daemon::run returns) ends here with "the daemon went away
        // mid-job", and one that failed on another box after forwarding its rmeta line, then met a
        // draining scheduler, gets Reply::Restarting, which this loop calls "unexpected reply from
        // the daemon". Either way cargo sees a failed compile, so any daemon restart (a deploy,
        // systemctl restart, a client of another version: daemon::serve) fails the builds that have
        // jobs on other boxes. The wrapper knows whether it printed an rmeta line already
        // (protocol::is_metadata_line on each Output), so it could ask again through
        // client::request and OR that into the Slot's filter_metadata. Rerun or fail is a policy
        // choice, hence for eric; see client-01 for the same failure before the first reply.
        // (wrapper-01)
        reply = protocol::recv(&mut sock, &mut buf).context("the daemon went away mid-job")?;
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
}
