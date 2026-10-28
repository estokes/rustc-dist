use crate::{
    argv, config,
    daemon::spawn_from_wrapper,
    protocol::{self, Job, ToWrapper},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use std::{
    env,
    ffi::OsString,
    io::Write,
    os::unix::process::CommandExt,
    path::PathBuf,
    process::Command,
    time::Duration,
};
use tokio::{net::UnixStream, time};

pub fn run(args: Vec<OsString>) -> Result<()> {
    if args.is_empty() {
        bail!("cargo invoked the wrapper with no arguments");
    }
    if early_local(&args)? {
        return exec_local(&args);
    }
    match tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(drive(args.clone()))? {
        Stop::Local => exec_local(&args),
        Stop::Code(code) => std::process::exit(code),
    }
}

enum Stop {
    Local,
    Code(i32),
}

fn early_local(args: &[OsString]) -> Result<bool> {
    if argv::has_incremental(args) {
        return Ok(true);
    }
    let cfg = match config::load_user() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("rustc-dist: {e:#}");
            return Ok(true);
        }
    };
    if cfg.base.is_none() {
        return Ok(true);
    }
    let start = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or(env::current_dir()?);
    let name = argv::crate_name(args);
    match config::local_crates(&start) {
        Ok(list) => {
            Ok(name.as_deref().is_some_and(|n| list.iter().any(|c| config::names_match(c, n))))
        }
        Err(e) => {
            eprintln!("rustc-dist: {e:#}");
            Ok(true)
        }
    }
}

fn exec_local(args: &[OsString]) -> Result<()> {
    let err = Command::new(&args[0]).args(&args[1..]).exec();
    Err(err).context("exec rustc")
}

async fn drive(args: Vec<OsString>) -> Result<Stop> {
    let mut sock = connect().await?;
    let argv = args
        .iter()
        .map(|a| a.to_str().context("argument is not utf-8").map(ArcStr::from))
        .collect::<Result<Vec<_>>>()?;
    let env = env::vars_os()
        .filter_map(|(k, v)| Some((ArcStr::from(k.to_str()?), ArcStr::from(v.to_str()?))))
        .collect();
    let cwd = env::current_dir()?;
    let cwd = ArcStr::from(cwd.to_str().context("cwd is not utf-8")?);
    protocol::write_msg(&mut sock, &Job { argv, env, cwd }).await?;
    loop {
        match protocol::read_msg::<ToWrapper>(&mut sock).await? {
            ToWrapper::RunLocal => return Ok(Stop::Local),
            ToWrapper::Stdout(bytes) => protocol::write_stdio(true, &bytes)?,
            ToWrapper::Stderr(bytes) => protocol::write_stdio(false, &bytes)?,
            ToWrapper::Exit(code) => return Ok(Stop::Code(code)),
            ToWrapper::Fail(msg) => {
                writeln!(std::io::stderr(), "rustc-dist: {msg}")?;
                return Ok(Stop::Code(1));
            }
        }
    }
}

async fn connect() -> Result<UnixStream> {
    let path = config::socket_path();
    if let Ok(sock) = UnixStream::connect(&path).await {
        return Ok(sock);
    }
    spawn_from_wrapper()?;
    for _ in 0..100 {
        if let Ok(sock) = UnixStream::connect(&path).await {
            return Ok(sock);
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    bail!("daemon did not open {}", path.display())
}
