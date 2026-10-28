use crate::{
    argv, config,
    farm::{self, FarmHandle},
    identity::{self, Identity},
    protocol::{self, Job, ToWrapper},
    remote::{self, Live, Outcome},
    stage,
};
use anyhow::{Context, Result, anyhow};
use arcstr::ArcStr;
use log::{info, warn};
use netidx::{config::Config, path::Path, subscriber::SubscriberBuilder};
use parking_lot::Mutex;
use std::{
    env, fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::mpsc,
    task::spawn_blocking,
    time,
};

const IDLE: Duration = Duration::from_secs(10 * 60);

struct Cached {
    path: PathBuf,
    mtime: SystemTime,
    identity: Identity,
}

pub async fn run() -> Result<()> {
    fs::create_dir_all(config::runtime_dir())?;
    init_log()?;
    let cfg = config::load_user().context("reading config")?;
    let Some(base) = cfg.base else {
        anyhow::bail!("no base path in config or RUSTC_DIST_BASE");
    };
    info!("daemon base {base}");
    let net = Config::load_default()?;
    let subscriber = SubscriberBuilder::new(net).build()?;
    let farm = farm::run(subscriber, Path::from(base)).await?;
    let sock_path = config::socket_path();
    let Some(listener) = bind_socket(&sock_path).await? else {
        return Ok(());
    };
    info!("listening on {}", sock_path.display());
    let cache = Arc::new(Mutex::new(None));
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let mut clients = 0usize;
    let mut idle_from = Instant::now();
    loop {
        let idle_left = IDLE.saturating_sub(idle_from.elapsed());
        tokio::select! {
            accepted = listener.accept() => {
                let (sock, _) = accepted?;
                clients += 1;
                let farm = farm.clone();
                let cache = cache.clone();
                let done = done_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(sock, farm, cache).await {
                        warn!("job: {e:#}");
                    }
                    let _ = done.send(());
                });
            }
            _ = done_rx.recv() => {
                clients = clients.saturating_sub(1);
                if clients == 0 {
                    idle_from = Instant::now();
                }
            }
            _ = time::sleep(idle_left), if clients == 0 => {
                info!("idle for {}s, exiting", IDLE.as_secs());
                break;
            }
        }
    }
    let _ = fs::remove_file(&sock_path);
    Ok(())
}

async fn bind_socket(path: &std::path::Path) -> Result<Option<UnixListener>> {
    match UnixListener::bind(path) {
        Ok(listener) => return Ok(Some(listener)),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {}
        Err(e) => return Err(e.into()),
    }
    if UnixStream::connect(path).await.is_ok() {
        info!("socket {} already taken", path.display());
        return Ok(None);
    }
    let _ = fs::remove_file(path);
    match UnixListener::bind(path) {
        Ok(listener) => Ok(Some(listener)),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            info!("socket {} already taken", path.display());
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

fn init_log() -> Result<()> {
    let path = config::log_path();
    let file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
    env_logger::Builder::from_env(env_logger::Env::new().filter_or("RUSTC_DIST_LOG", "info"))
        .target(env_logger::Target::Pipe(Box::new(file)))
        .init();
    Ok(())
}

async fn handle(mut sock: UnixStream, farm: FarmHandle, cache: Arc<Mutex<Option<Cached>>>) -> Result<()> {
    let job = protocol::read_msg::<Job>(&mut sock).await?;
    let args = arg_strs(&job.argv);
    if argv::has_incremental(&args) || local_crate(&job) {
        protocol::write_msg(&mut sock, &ToWrapper::RunLocal).await?;
        return Ok(());
    }
    let Some(rustc) = job.argv.first() else {
        protocol::write_msg(&mut sock, &ToWrapper::Fail(ArcStr::from("empty argv"))).await?;
        return Ok(());
    };
    let rustc = PathBuf::from(rustc.as_str());
    let identity = match probe(&cache, &rustc) {
        Ok(id) => id,
        Err(e) => {
            protocol::write_msg(&mut sock, &ToWrapper::Fail(ArcStr::from(format!("{e:#}")))).await?;
            return Ok(());
        }
    };
    let target = argv::job_target(&args, &identity.host);
    let argv = job.argv.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let env = job.env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
    let cwd = PathBuf::from(job.cwd.as_str());
    let staged = match spawn_blocking(move || stage::stage(&argv, &env, &cwd)).await {
        Ok(Ok(staged)) => staged,
        Ok(Err(e)) => {
            warn!("staging failed, compiling locally: {e:#}");
            protocol::write_msg(&mut sock, &ToWrapper::RunLocal).await?;
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    dispatch(&mut sock, &farm, &identity, &target, &staged).await
}

fn arg_strs(argv: &[ArcStr]) -> Vec<&str> {
    argv.iter().map(ArcStr::as_str).collect()
}

fn local_crate(job: &Job) -> bool {
    let Some(name) = argv::crate_name(&arg_strs(&job.argv)) else {
        return false;
    };
    let start = job
        .env
        .iter()
        .find(|(k, _)| k.as_str() == "CARGO_MANIFEST_DIR")
        .map(|(_, v)| PathBuf::from(v.as_str()))
        .unwrap_or_else(|| PathBuf::from(job.cwd.as_str()));
    match config::local_crates(&start) {
        Ok(list) => list.iter().any(|n| config::names_match(n, &name)),
        Err(e) => {
            warn!("local crate list: {e:#}");
            true
        }
    }
}

fn probe(cache: &Mutex<Option<Cached>>, rustc: &std::path::Path) -> Result<Identity> {
    let mtime = identity::mtime(rustc)?;
    let mut guard = cache.lock();
    if let Some(hit) = guard.as_ref() {
        if hit.path == rustc && hit.mtime == mtime {
            return Ok(hit.identity.clone());
        }
    }
    let id = identity::probe(rustc)?;
    info!(
        "toolchain {} {} {} {}",
        rustc.display(),
        id.release,
        id.commit,
        id.host
    );
    *guard = Some(Cached { path: rustc.to_path_buf(), mtime, identity: id.clone() });
    Ok(id)
}

async fn dispatch(
    sock: &mut UnixStream,
    farm: &FarmHandle,
    identity: &Identity,
    target: &str,
    staged: &stage::Staged,
) -> Result<()> {
    let mut skip = Vec::new();
    loop {
        let Some(reservation) = farm.reserve(identity, target, &skip).await else {
            info!("no server for {} {target}, compiling locally", identity.commit);
            protocol::write_msg(sock, &ToWrapper::RunLocal).await?;
            return Ok(());
        };
        let name = reservation.name.clone();
        let rpc = reservation.rpc.clone();
        info!("send {target} to {name}");
        match run_remote(sock, farm, rpc, staged).await? {
            RemoteEnd::Again => {
                skip.push(name);
                drop(reservation);
            }
            RemoteEnd::Finished => {
                drop(reservation);
                return Ok(());
            }
        }
    }
}

enum RemoteEnd {
    Again,
    Finished,
}

async fn run_remote(
    sock: &mut UnixStream,
    farm: &FarmHandle,
    rpc: Path,
    staged: &stage::Staged,
) -> Result<RemoteEnd> {
    let (tx, mut rx) = mpsc::channel(32);
    let compile = remote::compile(&farm.subscriber, rpc, staged, tx);
    tokio::pin!(compile);
    let mut sent = false;
    let mut live_open = true;
    loop {
        tokio::select! {
            biased;
            ev = rx.recv(), if live_open => {
                match ev {
                    Some(Live::Stdout(bytes)) => {
                        sent = true;
                        protocol::write_msg(sock, &ToWrapper::Stdout(bytes)).await?;
                    }
                    Some(Live::Stderr(bytes)) => {
                        sent = true;
                        protocol::write_msg(sock, &ToWrapper::Stderr(bytes)).await?;
                    }
                    None => live_open = false,
                }
            }
            result = &mut compile => {
                while let Ok(ev) = rx.try_recv() {
                    match ev {
                        Live::Stdout(bytes) => {
                            sent = true;
                            protocol::write_msg(sock, &ToWrapper::Stdout(bytes)).await?;
                        }
                        Live::Stderr(bytes) => {
                            sent = true;
                            protocol::write_msg(sock, &ToWrapper::Stderr(bytes)).await?;
                        }
                    }
                }
                return match result {
                    Ok(Outcome::Busy) => {
                        info!("server busy");
                        Ok(RemoteEnd::Again)
                    }
                    Ok(Outcome::Done(code)) => {
                        protocol::write_msg(sock, &ToWrapper::Exit(code)).await?;
                        Ok(RemoteEnd::Finished)
                    }
                    Err(e) if !sent => {
                        warn!("remote failed before output, trying another server: {e:#}");
                        Ok(RemoteEnd::Again)
                    }
                    Err(e) => {
                        protocol::write_msg(
                            sock,
                            &ToWrapper::Fail(ArcStr::from(format!("{e:#}"))),
                        )
                        .await?;
                        Ok(RemoteEnd::Finished)
                    }
                };
            }
        }
    }
}

pub fn spawn_from_wrapper() -> Result<()> {
    use std::os::unix::process::CommandExt;
    fs::create_dir_all(config::runtime_dir())?;
    let exe = env::current_exe().context("current exe")?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .process_group(0);
    cmd.spawn().map_err(|e| anyhow!(e)).context("starting daemon")?;
    Ok(())
}
