use crate::{
    blobs::{self, Hash, Store, Upload},
    link::{self, Gate},
    protocol::{self, Front, Ran},
    remote::{CHUNK, FileEntry, FromServer, Late, Rlib, SYSTEM, ToServer},
    sched::{Answer, Msg, Slot},
    sys,
    toolchain::Installed,
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use compact_str::format_compact;
use futures::{StreamExt, future::OptionFuture};
use log::{debug, warn};
use netidx::{
    path::Path as NPath,
    publisher::{PublishFlags, Publisher, Value},
};
use netidx_protocols::{
    channel::server::session,
    pack_channel::server::{Connection, singleton_with_flags},
};
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap, HashSet, hash_map::Entry},
    fs::{self, File},
    future::Future,
    io::{self, BufRead, BufReader, Read},
    mem,
    os::{
        fd::{FromRawFd, OwnedFd},
        unix::{
            fs::PermissionsExt,
            process::{CommandExt, ExitStatusExt},
        },
    },
    path::{Path, PathBuf},
    pin::pin,
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

/// Environment the sandbox sets or clears itself.
const DROPPED_ENV: &[&str] =
    &["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS", "PATH", "TMPDIR", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"];

/// What a box needs to run other boxes' jobs.
pub struct Server {
    pub store: Store,
    pub jobs_dir: PathBuf,
    pub toolchains: Vec<Installed>,
    pub tx: mpsc::UnboundedSender<Msg>,
    /// The stand-in linker, which waits for late rlibs.
    stand_in: PathBuf,
    next_job: AtomicU64,
}

impl Server {
    pub fn new(store: Store, jobs_dir: PathBuf, toolchains: Vec<Installed>, tx: mpsc::UnboundedSender<Msg>) -> Result<Self> {
        let _ = fs::remove_dir_all(&jobs_dir);
        fs::create_dir_all(&jobs_dir).with_context(|| format_compact!("creating {}", jobs_dir.display()))?;
        let stand_in = jobs_dir.join("linker");
        link::write_stand_in_linker(&stand_in)?;
        Ok(Self { store, jobs_dir, toolchains, tx, stand_in, next_job: AtomicU64::new(0) })
    }
}

/// How long a client has to take the channel it asked for.
const CONNECT: Duration = Duration::from_secs(30);

/// Publish the place other boxes send jobs, at `path`, and return what
/// takes them until the publisher stops. Each client connects on its own,
/// so one that stalls or botches its handshake holds up no other.
pub async fn listen(publisher: Publisher, path: NPath, server: Arc<Server>) -> Result<impl Future<Output = ()>> {
    let (tx, mut asks) = futures::channel::mpsc::channel(50);
    let listener =
        publisher.publish_with_flags_and_writes(PublishFlags::empty(), path.clone(), Value::from("channel"), Some(tx))?;
    publisher.flushed().await;
    Ok(async move {
        let _listener = listener;
        while let Some(mut batch) = asks.next().await {
            for ask in batch.drain(..) {
                let Some(reply) = ask.send_result else { continue };
                if !matches!(&ask.value, Value::String(s) if &**s == "connect") {
                    continue;
                }
                let (publisher, server, at) = (publisher.clone(), server.clone(), session(&path));
                tokio::spawn(async move {
                    let connected = time::timeout(CONNECT, async {
                        let mut channel = singleton_with_flags(&publisher, PublishFlags::empty(), None, at.clone()).await?;
                        reply.send(Value::from(at));
                        channel.wait_connected().await
                    });
                    match connected.await {
                        Ok(Ok(con)) => {
                            if let Err(e) = serve(server, con).await {
                                warn!("remote job: {e:#}");
                            }
                        }
                        Ok(Err(e)) => warn!("a client failed to connect: {e:#}"),
                        Err(_) => warn!("a client didn't connect within {} s", CONNECT.as_secs()),
                    }
                });
            }
        }
    })
}

/// Serve one remote job on `con`.
pub async fn serve(server: Arc<Server>, con: Connection) -> Result<()> {
    let ToServer::Offer { unit, key, target, class } = con.recv_one().await? else {
        bail!("expected an offer");
    };
    let Some(tc) = server.toolchains.iter().find(|t| t.toolchain.builds(&key, target.as_deref())) else {
        let why: ArcStr = format_compact!("no toolchain {key}").as_str().into();
        con.send_one(&FromServer::Failed(why)).await?;
        return Ok(());
    };
    let (reply, admitted) = oneshot::channel();
    server.tx.send(Msg::Offer { unit, class, reply }).map_err(|_| anyhow!("scheduler stopped"))?;
    let Answer { slot, version } = admitted.await?;
    let Some(slot) = slot else {
        con.send_one(&FromServer::Busy { version }).await?;
        return Ok(());
    };
    con.send_one(&FromServer::Accepted { version }).await?;
    match job(&server, &con, tc, &slot).await {
        Ok(Some((ran, linker_waited_us))) => slot.finish(ran, linker_waited_us),
        Ok(None) => (),
        Err(e) => {
            let why: ArcStr = format_compact!("{e:#}").as_str().into();
            let _ = con.send_one(&FromServer::Failed(why)).await;
            return Err(e);
        }
    }
    Ok(())
}

/// The Ran of a successful compile, and how long its linker waited for
/// late rlibs.
async fn job(server: &Server, con: &Connection, tc: &Installed, slot: &Slot) -> Result<Option<(Ran, u64)>> {
    let ToServer::Manifest { files, dirs, late } = con.recv_one().await? else {
        bail!("expected a manifest");
    };
    let rlibs = late.as_ref().map_or(&[][..], |l| &l.rlibs);
    for path in files.iter().map(|f| &f.path).chain(rlibs.iter().map(|r| &r.path)).chain(&dirs) {
        check(path)?;
    }
    if let Some(Late { linker, .. }) = &late {
        check_program(linker)?;
    }
    let store = &server.store;
    let _pins = store.pin(files.iter().map(|f| f.hash).chain(rlibs.iter().flat_map(|r| [r.hash, r.meta])));
    let mut now: HashSet<Hash> = files.iter().map(|f| f.hash).filter(|h| !store.has(h)).collect();
    let mut later = HashSet::new();
    for r in rlibs.iter().filter(|r| !store.has(&r.hash)) {
        later.insert(r.hash);
        if !store.has(&r.meta) {
            now.insert(r.meta);
        }
    }
    later.retain(|h| !now.contains(h));
    let need = FromServer::Need { now: now.iter().copied().collect(), later: later.iter().copied().collect() };
    con.send_one(&need).await?;
    receive(con, store, now, |_| Ok(())).await?;
    let ToServer::Run { args, env, cwd } = con.recv_one().await? else {
        bail!("expected run");
    };
    check(&cwd)?;
    store.evict();
    let n = server.next_job.fetch_add(1, Ordering::Relaxed);
    let root = JobDir(server.jobs_dir.join(format_compact!("{n}").as_str()));
    root.build(store, &files, rlibs, &later, &dirs, &cwd)?;
    let tops: BTreeSet<&str> = files
        .iter()
        .map(|f| &f.path)
        .chain(rlibs.iter().map(|r| &r.path))
        .chain(&dirs)
        .chain([&cwd])
        .filter_map(|p| p.split('/').nth(1))
        .collect();
    let linker = late.as_ref().map(|l| &l.linker).filter(|_| !later.is_empty());
    let mut cmd = sandbox(&root.0, &tops, tc, &slot.grant.jobserver, &env, &cwd)?;
    if let Some(linker) = linker {
        cmd.arg("--ro-bind").arg(&server.stand_in).arg(format_compact!("{}/{linker}", link::BIN).as_str());
        let path = format_compact!("{}:{PATH}", link::BIN);
        cmd.args(["--setenv", "PATH", path.as_str()]);
    }
    cmd.arg("--").arg(tc.rustc()).args(args.iter().map(|a| a.as_str()));
    let late = match linker {
        None => None,
        Some(_) => {
            let arrive = receive(con, store, later, |hash| {
                for r in rlibs.iter().filter(|r| r.hash == hash) {
                    blobs::replace(store, &hash, &root.at(&r.path))?;
                }
                Ok(())
            });
            Some((Gate::new()?, arrive))
        }
    };
    run(con, store, cmd, &root.0, slot, late).await
}

/// Receive chunks until every blob in `need` is stored, telling `stored`
/// of each as it is.
async fn receive(
    con: &Connection,
    store: &Store,
    mut need: HashSet<Hash>,
    mut stored: impl FnMut(Hash) -> Result<()>,
) -> Result<()> {
    let mut uploads: HashMap<Hash, Upload> = HashMap::new();
    while !need.is_empty() {
        let ToServer::Chunk { hash, data, last } = con.recv_one().await? else {
            bail!("expected a chunk");
        };
        if !need.contains(&hash) {
            bail!("unrequested blob {hash}");
        }
        let up = match uploads.entry(hash) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(store.upload(hash)?),
        };
        up.write(&data)?;
        if last {
            uploads.remove(&hash).expect("present").finish(store)?;
            need.remove(&hash);
            stored(hash)?;
        }
    }
    Ok(())
}

/// A program name the stand-in linker may take: a file name.
fn check_program(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
        bail!("refusing linker {name}");
    }
    Ok(())
}

/// A path the client may send: absolute, normalized, utf-8, and not in a
/// directory the sandbox shares with the host.
fn check(path: &str) -> Result<()> {
    let mut parts = path.split('/');
    let ok = parts.next() == Some("")
        && path.len() > 1
        && path.split('/').skip(1).all(|p| !p.is_empty() && p != "." && p != "..")
        && parts.next().is_some_and(|top| !SYSTEM.contains(&top));
    if !ok {
        bail!("refusing path {path}");
    }
    Ok(())
}

/// A job's copy of the client's paths. Removed when dropped.
struct JobDir(PathBuf);

impl JobDir {
    fn at(&self, path: &str) -> PathBuf {
        self.0.join(&path[1..])
    }

    /// Rlibs still to come get stand-ins made from their metadata.
    fn build(
        &self,
        store: &Store,
        files: &[FileEntry],
        rlibs: &[Rlib],
        later: &HashSet<Hash>,
        dirs: &[ArcStr],
        cwd: &str,
    ) -> Result<()> {
        for d in dirs.iter().map(|d| d.as_str()).chain([cwd]) {
            fs::create_dir_all(self.at(d))?;
        }
        let rlibs = rlibs.iter().map(|r| (&r.path, r.hash, later.contains(&r.hash).then_some(r.meta)));
        for (path, hash, stand_in) in files.iter().map(|f| (&f.path, f.hash, None)).chain(rlibs) {
            let dest = self.at(path);
            if let Some(dir) = dest.parent() {
                fs::create_dir_all(dir)?;
            }
            match stand_in {
                Some(meta) => link::write_stand_in_rlib(&store.path(&meta), &dest)?,
                None => blobs::place(store, &hash, &dest)?,
            }
        }
        Ok(())
    }
}

impl Drop for JobDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// What the sandbox puts on `PATH`.
const PATH: &str = "/usr/bin:/bin";

/// bwrap with no network, the host's system directories read only, the
/// job's tree at the client's paths, the toolchain, and the jobserver.
/// Its command follows `--`.
fn sandbox(
    root: &Path,
    tops: &BTreeSet<&str>,
    tc: &Installed,
    jobserver: &str,
    env: &[(ArcStr, ArcStr)],
    cwd: &str,
) -> Result<Command> {
    let mut cmd = Command::new("bwrap");
    // With rustc as pid 1, bwrap waits for it directly, so wait4 on bwrap
    // reports rustc's resource use; behind bwrap's own init it reports
    // almost none.
    cmd.args(["--unshare-all", "--as-pid-1", "--die-with-parent", "--new-session"]);
    cmd.args(["--ro-bind", "/usr", "/usr", "--ro-bind", "/etc", "/etc", "--proc", "/proc", "--dev", "/dev"]);
    for dir in ["bin", "lib", "lib64", "sbin"] {
        let host = format_compact!("/{dir}");
        match fs::symlink_metadata(host.as_str()) {
            Ok(m) if m.file_type().is_symlink() => {
                cmd.arg("--symlink").arg(fs::read_link(host.as_str())?).arg(host.as_str());
            }
            Ok(m) if m.is_dir() => {
                cmd.args(["--ro-bind", host.as_str(), host.as_str()]);
            }
            _ => (),
        }
    }
    if !tops.contains("tmp") {
        cmd.args(["--tmpfs", "/tmp"]);
    }
    for top in tops {
        cmd.arg("--bind").arg(root.join(top)).arg(format_compact!("/{top}").as_str());
    }
    cmd.arg("--ro-bind").arg(&tc.sysroot).arg(&tc.sysroot);
    cmd.args(["--bind", jobserver, jobserver, "--chdir", cwd, "--clearenv"]);
    for (k, v) in env {
        if !DROPPED_ENV.contains(&k.as_str()) && !k.starts_with("LD_") {
            cmd.args(["--setenv", k.as_str(), v.as_str()]);
        }
    }
    cmd.args(["--setenv", "PATH", PATH, "--setenv", "TMPDIR", "/tmp", "--setenv", "CARGO_MAKEFLAGS"]);
    cmd.arg(format_compact!("-j --jobserver-auth=fifo:{jobserver}").as_str());
    Ok(cmd)
}

enum Ev {
    Stderr(Bytes),
    Stdout(Bytes),
    Eof,
    /// With when it exited, which the loop may see later while it sends
    /// outputs.
    Exit(ExitStatus, libc::rusage, Instant),
}

/// Kills the sandbox if the job is abandoned. A pidfd can't hit a reused
/// pid.
struct Kill(Option<OwnedFd>);

impl Drop for Kill {
    fn drop(&mut self) {
        if let Some(fd) = &self.0 {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::syscall(libc::SYS_pidfd_send_signal, fd.as_raw_fd(), libc::SIGKILL, std::ptr::null::<()>(), 0)
            };
        }
    }
}

/// With `late`, a future stores late rlibs, and the gate holds the
/// linker until it has.
async fn run(
    con: &Connection,
    store: &Store,
    mut cmd: Command,
    root: &Path,
    slot: &Slot,
    late: Option<(Gate, impl Future<Output = Result<()>>)>,
) -> Result<Option<(Ran, u64)>> {
    let (mut gate, arrive) = late.unzip();
    let cpus = slot.grant.cpus.clone();
    let gate_fd = gate.as_ref().map(Gate::fd);
    cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    unsafe {
        cmd.pre_exec(move || {
            sys::pin(&cpus).map_err(io::Error::other)?;
            gate_fd.map_or(Ok(()), Gate::inherit)
        });
    }
    let start = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (spawned_tx, spawned) = oneshot::channel();
    // A thread of its own starts the sandbox and waits for it, for its
    // resource use.
    let xtx = tx.clone();
    thread::spawn(move || {
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                let _ = spawned_tx.send(Err(e));
                return;
            }
        };
        let pid = child.id() as libc::pid_t;
        let pipes = (child.stdout.take(), child.stderr.take());
        // wait4 reaps it; the Child must not try.
        mem::forget(child);
        let _ = spawned_tx.send(Ok((pid, pipes)));
        let mut status = 0;
        let mut ru: libc::rusage = unsafe { mem::zeroed() };
        if unsafe { libc::wait4(pid, &mut status, 0, &mut ru) } == pid {
            let _ = xtx.send(Ev::Exit(ExitStatus::from_raw(status), ru, Instant::now()));
        }
    });
    let (pid, (stdout, stderr)) = spawned.await?.context("starting bwrap")?;
    slot.started(pid as u32);
    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    let mut kill = Kill((pidfd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(pidfd as i32) }));
    let stderr = stderr.context("stderr")?;
    let etx = tx.clone();
    thread::spawn(move || {
        let mut r = BufReader::new(stderr);
        let mut line = Vec::new();
        while r.read_until(b'\n', &mut line).is_ok_and(|n| n > 0) {
            let _ = etx.send(Ev::Stderr(Bytes::from(mem::take(&mut line))));
        }
        let _ = etx.send(Ev::Eof);
    });
    let mut stdout = stdout.context("stdout")?;
    let otx = tx.clone();
    thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        while let Ok(n @ 1..) = stdout.read(&mut buf) {
            let _ = otx.send(Ev::Stdout(Bytes::copy_from_slice(&buf[..n])));
        }
        let _ = otx.send(Ev::Eof);
    });
    drop(tx);
    let mut eofs = 0;
    let mut exit = None;
    let mut front = None;
    let mut arriving = arrive.is_some();
    let mut arrive = pin!(OptionFuture::from(arrive));
    while eofs < 2 || exit.is_none() {
        tokio::select! {
            ev = rx.recv() => match ev.context("sandbox threads stopped")? {
                Ev::Stderr(line) => {
                    if protocol::is_artifact_line(&line) {
                        if front.is_none() && protocol::is_metadata_line(&line) {
                            let wall_us = start.elapsed().as_micros() as u64;
                            front = Some(Front { wall_us });
                        }
                        send_artifact(con, store, root, &line).await?;
                    }
                    con.send_one(&FromServer::Stderr(line)).await?;
                }
                Ev::Stdout(data) => con.send_one(&FromServer::Stdout(data)).await?,
                Ev::Eof => eofs += 1,
                Ev::Exit(status, ru, at) => exit = Some((status, ru, at)),
            },
            arrived = &mut arrive, if arriving => {
                arrived.context("guarded")??;
                arriving = false;
                if let Some(g) = &mut gate {
                    g.open().await?;
                }
            }
            asked = async { gate.as_mut().expect("guarded").ask().await },
                if gate.as_ref().is_some_and(Gate::listening) => asked?,
        }
    }
    kill.0 = None;
    let (status, ru, exited) = exit.expect("loop ends with an exit");
    let wall_us = exited.saturating_duration_since(start).as_micros() as u64;
    let ran = Ran { wall_us, maxrss_kb: ru.ru_maxrss as u64, front };
    let code = status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    debug!("sandbox exited {code}");
    let linker_waited_us = gate.map_or(0, |g| g.waited_us());
    con.send_one(&FromServer::Exit { code, ran, linker_waited_us, measured: slot.measured() }).await?;
    Ok((code == 0).then_some((ran, linker_waited_us)))
}

#[derive(Deserialize)]
struct Artifact {
    artifact: String,
}

/// Send the file an artifact line names, before the line. A crate file
/// stays in the store for jobs that read it later.
async fn send_artifact(con: &Connection, store: &Store, root: &Path, line: &[u8]) -> Result<()> {
    let a: Artifact = serde_json::from_slice(line).context("artifact line")?;
    check(&a.artifact)?;
    let host = root.join(&a.artifact[1..]);
    let mut file = File::open(&host).with_context(|| format_compact!("opening {}", host.display()))?;
    let mode = file.metadata()?.permissions().mode() & 0o777;
    let path: ArcStr = a.artifact.as_str().into();
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = file.read(&mut buf)?;
        let last = n < buf.len();
        hasher.update(&buf[..n]);
        let data = Bytes::copy_from_slice(&buf[..n]);
        con.send_one(&FromServer::Output { path: path.clone(), mode, data, last }).await?;
        if last {
            break;
        }
    }
    if [".rlib", ".rmeta", ".so"].iter().any(|ext| path.ends_with(ext)) {
        store.adopt(&host, &Hash(*hasher.finalize().as_bytes()))?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn checks_paths() {
        for ok in ["/home/eric/proj/a/src/lib.rs", "/tmp/t/deps", "/opt/x"] {
            assert!(check(ok).is_ok(), "{ok}");
        }
        for bad in ["relative", "/", "/home/../etc/passwd", "/home//x", "/usr/lib/x", "/etc/x", "/home/./x"] {
            assert!(check(bad).is_err(), "{bad}");
        }
    }
}
