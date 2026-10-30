use crate::{
    argv::{self, Invocation},
    client,
    protocol::{self, Ask, Ran, Reply},
    sys,
};
use anyhow::{Context, Error, Result, anyhow, bail};
use bytes::BytesMut;
use compact_str::format_compact;
use std::{
    env,
    ffi::OsString,
    io,
    os::unix::{
        net::UnixStream,
        process::{CommandExt, ExitStatusExt},
    },
    path::Path,
    process::{self, Command, ExitStatus},
    time::Instant,
};

pub fn run(args: Vec<OsString>) -> Result<()> {
    if args.is_empty() {
        bail!("cargo ran the wrapper without a compiler");
    }
    if env::var_os("RUSTC_DIST").is_some_and(|v| v == "off") {
        return Err(exec(&args));
    }
    let ask = match argv::classify(&args) {
        Invocation::Probe => Ask::Probe,
        Invocation::Compile(unit) => Ask::Compile(unit),
    };
    let mut buf = BytesMut::new();
    let (mut sock, reply) = client::request(ask, true, &mut buf)?;
    match reply {
        Reply::Trivial => Err(exec(&args)),
        Reply::Slot { cpus, jobserver } => {
            sys::pin(&cpus)?;
            let status = compile(&mut sock, &mut buf, &args, &jobserver)?;
            exit_as(status)
        }
        Reply::Status(_) | Reply::Restarting => bail!("unexpected reply from the daemon"),
    }
}

fn exec(args: &[OsString]) -> Error {
    let err = Command::new(&args[0]).args(&args[1..]).exec();
    anyhow!(err).context(format_compact!("exec {}", Path::new(&args[0]).display()))
}

fn compile(
    sock: &mut UnixStream,
    buf: &mut BytesMut,
    args: &[OsString],
    jobserver: &str,
) -> Result<ExitStatus> {
    let parent = process::id() as libc::pid_t;
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .env("CARGO_MAKEFLAGS", format_compact!("-j --jobserver-auth=fifo:{jobserver}").as_str());
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
    let status = cmd
        .status()
        .with_context(|| format_compact!("running {}", Path::new(&args[0]).display()))?;
    if status.success() {
        let wall_us = start.elapsed().as_micros() as u64;
        let usage = sys::children_usage();
        let ran = Ran { cpu_us: usage.cpu_us, wall_us, maxrss_kb: usage.maxrss_kb };
        // A daemon that restarted meanwhile just misses this sample.
        let _ = protocol::send(sock, buf, &ran);
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
