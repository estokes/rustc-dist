use crate::{
    blobs::{self, Hash, Store, Upload},
    protocol::Ran,
    remote::{ARTIFACT, CHUNK, FileEntry, FromServer, SYSTEM, ToServer},
    sched::{Answer, Msg, Slot},
    sys,
    toolchain::Installed,
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use compact_str::format_compact;
use log::debug;
use netidx_protocols::pack_channel::server::Connection;
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{self, File},
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
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

/// Environment the sandbox sets or clears itself.
const DROPPED_ENV: &[&str] =
    &["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS", "PATH", "TMPDIR", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"];

/// What a box needs to run other boxes' jobs.
pub struct Server {
    pub store: Store,
    pub jobs_dir: PathBuf,
    pub toolchains: Vec<Installed>,
    pub tx: mpsc::UnboundedSender<Msg>,
    next_job: AtomicU64,
}

impl Server {
    pub fn new(store: Store, jobs_dir: PathBuf, toolchains: Vec<Installed>, tx: mpsc::UnboundedSender<Msg>) -> Result<Self> {
        let _ = fs::remove_dir_all(&jobs_dir);
        fs::create_dir_all(&jobs_dir).with_context(|| format_compact!("creating {}", jobs_dir.display()))?;
        Ok(Self { store, jobs_dir, toolchains, tx, next_job: AtomicU64::new(0) })
    }
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
        Ok(Some(ran)) => slot.finish(ran),
        Ok(None) => (),
        Err(e) => {
            let why: ArcStr = format_compact!("{e:#}").as_str().into();
            let _ = con.send_one(&FromServer::Failed(why)).await;
            return Err(e);
        }
    }
    Ok(())
}

/// The Ran of a successful compile.
async fn job(server: &Server, con: &Connection, tc: &Installed, slot: &Slot) -> Result<Option<Ran>> {
    let ToServer::Manifest { files, dirs } = con.recv_one().await? else {
        bail!("expected a manifest");
    };
    for path in files.iter().map(|f| &f.path).chain(&dirs) {
        check(path)?;
    }
    let mut need: HashSet<Hash> = HashSet::new();
    for f in &files {
        if !server.store.has(&f.hash) {
            need.insert(f.hash);
        }
    }
    con.send_one(&FromServer::Need(need.iter().copied().collect())).await?;
    let mut uploads: HashMap<Hash, Upload> = HashMap::new();
    while !need.is_empty() {
        let ToServer::Chunk { hash, data, last } = con.recv_one().await? else {
            bail!("expected a chunk");
        };
        if !need.contains(&hash) {
            bail!("unrequested blob {hash}");
        }
        let up = match uploads.entry(hash) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(server.store.upload(hash)?),
        };
        up.write(&data)?;
        if last {
            uploads.remove(&hash).expect("present").finish(&server.store)?;
            need.remove(&hash);
        }
    }
    let ToServer::Run { args, env, cwd } = con.recv_one().await? else {
        bail!("expected run");
    };
    check(&cwd)?;
    server.store.evict();
    let n = server.next_job.fetch_add(1, Ordering::Relaxed);
    let root = JobDir(server.jobs_dir.join(format_compact!("{n}").as_str()));
    root.build(&server.store, &files, &dirs, &cwd)?;
    let tops: BTreeSet<&str> = files
        .iter()
        .map(|f| &f.path)
        .chain(&dirs)
        .chain([&cwd])
        .filter_map(|p| p.split('/').nth(1))
        .collect();
    let mut cmd = sandbox(&root.0, &tops, tc, &slot.grant.jobserver, &env, &cwd)?;
    cmd.args(args.iter().map(|a| a.as_str()));
    run(con, cmd, &root.0, &slot.grant.cpus).await
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

    fn build(&self, store: &Store, files: &[FileEntry], dirs: &[ArcStr], cwd: &str) -> Result<()> {
        for d in dirs.iter().map(|d| d.as_str()).chain([cwd]) {
            fs::create_dir_all(self.at(d))?;
        }
        for f in files {
            let dest = self.at(&f.path);
            if let Some(dir) = dest.parent() {
                fs::create_dir_all(dir)?;
            }
            blobs::place(store, &f.hash, &dest)?;
        }
        Ok(())
    }
}

impl Drop for JobDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// bwrap with no network, the host's system directories read only, the
/// job's tree at the client's paths, the toolchain, and the jobserver.
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
    cmd.args(["--setenv", "PATH", "/usr/bin:/bin", "--setenv", "TMPDIR", "/tmp", "--setenv", "CARGO_MAKEFLAGS"]);
    cmd.arg(format_compact!("-j --jobserver-auth=fifo:{jobserver}").as_str());
    cmd.arg("--").arg(tc.rustc());
    Ok(cmd)
}

enum Ev {
    Stderr(Bytes),
    Stdout(Bytes),
    Eof,
    Exit(ExitStatus, libc::rusage),
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

async fn run(con: &Connection, mut cmd: Command, root: &Path, cpus: &[u32]) -> Result<Option<Ran>> {
    let cpus = cpus.to_vec();
    cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    unsafe {
        cmd.pre_exec(move || sys::pin(&cpus).map_err(io::Error::other));
    }
    let start = Instant::now();
    let mut child = cmd.spawn().context("starting bwrap")?;
    let pid = child.id() as libc::pid_t;
    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    let mut kill = Kill((pidfd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(pidfd as i32) }));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let stderr = child.stderr.take().context("stderr")?;
    let etx = tx.clone();
    thread::spawn(move || {
        let mut r = BufReader::new(stderr);
        let mut line = Vec::new();
        while r.read_until(b'\n', &mut line).is_ok_and(|n| n > 0) {
            let _ = etx.send(Ev::Stderr(Bytes::from(mem::take(&mut line))));
        }
        let _ = etx.send(Ev::Eof);
    });
    let mut stdout = child.stdout.take().context("stdout")?;
    let otx = tx.clone();
    thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        while let Ok(n @ 1..) = stdout.read(&mut buf) {
            let _ = otx.send(Ev::Stdout(Bytes::copy_from_slice(&buf[..n])));
        }
        let _ = otx.send(Ev::Eof);
    });
    thread::spawn(move || {
        let mut status = 0;
        let mut ru: libc::rusage = unsafe { mem::zeroed() };
        if unsafe { libc::wait4(pid, &mut status, 0, &mut ru) } == pid {
            let _ = tx.send(Ev::Exit(ExitStatus::from_raw(status), ru));
        }
    });
    // wait4 reaped it; the Child must not try.
    mem::forget(child);
    let mut eofs = 0;
    let mut exit = None;
    while eofs < 2 || exit.is_none() {
        match rx.recv().await.context("sandbox threads stopped")? {
            Ev::Stderr(line) => {
                if line.windows(ARTIFACT.len()).any(|w| w == ARTIFACT) {
                    send_artifact(con, root, &line).await?;
                }
                con.send_one(&FromServer::Stderr(line)).await?;
            }
            Ev::Stdout(data) => con.send_one(&FromServer::Stdout(data)).await?,
            Ev::Eof => eofs += 1,
            Ev::Exit(status, ru) => exit = Some((status, ru)),
        }
    }
    kill.0 = None;
    let (status, ru) = exit.expect("loop ends with an exit");
    let us = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
    let ran = Ran {
        cpu_us: us(ru.ru_utime) + us(ru.ru_stime),
        wall_us: start.elapsed().as_micros() as u64,
        maxrss_kb: ru.ru_maxrss as u64,
    };
    let code = status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    debug!("sandbox exited {code}");
    con.send_one(&FromServer::Exit { code, cpu_us: ran.cpu_us, wall_us: ran.wall_us, maxrss_kb: ran.maxrss_kb })
        .await?;
    Ok((code == 0).then_some(ran))
}

#[derive(Deserialize)]
struct Artifact {
    artifact: String,
}

/// Send the file an artifact line names, before the line.
async fn send_artifact(con: &Connection, root: &Path, line: &[u8]) -> Result<()> {
    let a: Artifact = serde_json::from_slice(line).context("artifact line")?;
    check(&a.artifact)?;
    let host = root.join(&a.artifact[1..]);
    let mut file = File::open(&host).with_context(|| format_compact!("opening {}", host.display()))?;
    let mode = file.metadata()?.permissions().mode() & 0o777;
    let path: ArcStr = a.artifact.as_str().into();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = file.read(&mut buf)?;
        let last = n < buf.len();
        let data = Bytes::copy_from_slice(&buf[..n]);
        con.send_one(&FromServer::Output { path: path.clone(), mode, data, last }).await?;
        if last {
            return Ok(());
        }
    }
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
