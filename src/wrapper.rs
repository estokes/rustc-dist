use crate::{
    argv::{self, Compile, Invocation},
    client, config,
    protocol::{self, Ask, Job, Ran, Reply},
    remote::ARTIFACT,
    sys,
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
    if env::var_os("RUSTC_DIST").is_some_and(|v| v == "off") {
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
                let status = compile(&mut sock, &mut buf, &args, &jobserver, filter_metadata)?;
                exit_as(status)
            }
            Reply::Output { stderr: true, data } => io::stderr().write_all(&data)?,
            Reply::Output { stderr: false, data } => io::stdout().write_all(&data)?,
            Reply::Exit(code) => process::exit(code),
            Reply::Status(_) | Reply::Restarting | Reply::Done | Reply::Refused(_) => {
                bail!("unexpected reply from the daemon")
            }
        }
        reply = protocol::recv(&mut sock, &mut buf).context("the daemon went away mid-job")?;
    }
}

/// What another box needs to run this compile, unless it must stay here.
fn job(args: &[OsString], c: &Compile, build: u32) -> Option<Job> {
    let paths = c.portable.clone()?;
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
    Some(Job { rustc, args, env, cwd, target: c.target.clone(), paths })
}

fn exec(args: &[OsString]) -> Error {
    let err = Command::new(&args[0]).args(&args[1..]).exec();
    anyhow!(err).context(format_compact!("exec {}", Path::new(&args[0]).display()))
}

/// Run rustc on this box. With `filter_metadata`, drop its rmeta notice,
/// because cargo already got one from a remote run that then failed.
fn compile(
    sock: &mut UnixStream,
    buf: &mut BytesMut,
    args: &[OsString],
    jobserver: &str,
    filter_metadata: bool,
) -> Result<ExitStatus> {
    let parent = process::id() as libc::pid_t;
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .env("CARGO_MAKEFLAGS", format_compact!("-j --jobserver-auth=fifo:{jobserver}").as_str());
    if filter_metadata {
        cmd.stderr(Stdio::piped());
    }
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
    if let Some(stderr) = child.stderr.take() {
        let mut out = io::stderr().lock();
        for line in BufReader::new(stderr).split(b'\n') {
            let line = line?;
            let artifact = line.windows(ARTIFACT.len()).any(|w| w == ARTIFACT);
            if !(artifact && line.windows(METADATA.len()).any(|w| w == METADATA)) {
                out.write_all(&line)?;
                out.write_all(b"\n")?;
            }
        }
    }
    let status = child.wait()?;
    if status.success() {
        let wall_us = start.elapsed().as_micros() as u64;
        let usage = sys::children_usage();
        let ran = Ran { cpu_us: usage.cpu_us, wall_us, maxrss_kb: usage.maxrss_kb };
        // A daemon that restarted meanwhile just misses this sample.
        let _ = protocol::send(sock, buf, &ran);
    }
    Ok(status)
}

const METADATA: &[u8] = br#""emit":"metadata""#;

fn exit_as(status: ExitStatus) -> ! {
    if let Some(sig) = status.signal() {
        unsafe {
            libc::signal(sig, libc::SIG_DFL);
            libc::raise(sig);
        }
    }
    process::exit(status.code().unwrap_or(1))
}
