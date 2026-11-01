use crate::{
    blobs::{self, Claim, Coming, Hash, Hasher, Sending, Store, Upload, When},
    link::{self, Gate},
    protocol::{self, Front, Measured, Ran, Unit},
    remote::{self, ExchangeId, FileEntry, FromBox, FromServer, Late, Part, Rlib, SYSTEM, ToBox, ToServer},
    sched::{Answer, Msg, Slot},
    sys,
    toolchain::{Installed, Toolchains},
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use compact_str::format_compact;
use futures::{StreamExt, future::OptionFuture};
use log::{debug, info, warn};
use netidx::{
    path::Path as NPath,
    publisher::{PublishFlags, Publisher, Value},
};
use netidx_protocols::{
    channel::server::session,
    pack_channel::server::{Connection, singleton_with_flags},
};
use parking_lot::Mutex;
use poolshark::local::LPooled;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{self, File},
    future::Future,
    io::{self, BufRead, BufReader, Read},
    mem,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
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
    runtime::Handle,
    sync::{mpsc, oneshot},
    task::{self, AbortHandle, JoinSet},
    time,
};

/// Environment the sandbox sets or clears itself.
const DROPPED_ENV: &[&str] =
    &["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS", "PATH", "TMPDIR", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"];

/// What a box needs to run other boxes' jobs.
pub struct Server {
    pub store: Store,
    pub jobs_dir: PathBuf,
    /// Read again whenever rustup changes them.
    toolchains: Mutex<Toolchains>,
    pub tx: mpsc::UnboundedSender<Msg>,
    /// The stand-in linker, which waits for late rlibs.
    stand_in: PathBuf,
    next_job: AtomicU64,
    /// The zstd level it compresses outputs at, 0 for none.
    level: i32,
}

impl Server {
    pub fn new(
        store: Store,
        jobs_dir: PathBuf,
        toolchains: Toolchains,
        tx: mpsc::UnboundedSender<Msg>,
        level: i32,
    ) -> Result<Self> {
        let _ = fs::remove_dir_all(&jobs_dir);
        fs::create_dir_all(&jobs_dir).with_context(|| format_compact!("creating {}", jobs_dir.display()))?;
        let stand_in = jobs_dir.join("linker");
        link::write_stand_in_linker(&stand_in)?;
        let toolchains = Mutex::new(toolchains);
        Ok(Self { store, jobs_dir, toolchains, tx, stand_in, next_job: AtomicU64::new(0), level })
    }

    /// The toolchain `key` with `target`'s standard library, if this box has
    /// it now: rustup's toolchains are read again if they changed.
    fn toolchain(&self, key: &str, target: Option<&str>) -> Option<triomphe::Arc<Installed>> {
        let mut toolchains = self.toolchains.lock();
        toolchains.refresh();
        toolchains.find(key, target).cloned()
    }
}

/// How long a client has to take the channel it asked for.
const CONNECT: Duration = Duration::from_secs(30);

/// Publish the place other boxes send jobs, at `path`, and return what
/// takes them until the publisher stops. Each box's daemon connects once,
/// on its own, so one that stalls or botches its handshake holds up no
/// other.
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
                                info!("a client's channel closed: {e:#}");
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

/// Where a job sends to its client: its exchange on the client's channel.
struct Reply {
    con: Arc<Connection>,
    id: ExchangeId,
}

impl Reply {
    async fn send(&self, msg: FromServer) -> Result<()> {
        self.con.send_one(&FromBox { id: self.id, msg }).await
    }
}

/// What a job's client sends it after its offer.
type Inbox = mpsc::UnboundedReceiver<ToServer>;

async fn next(inbox: &mut Inbox) -> Result<ToServer> {
    inbox.recv().await.context("the client's channel closed")
}

/// Serve the exchanges another box's daemon opens on `con` until the
/// channel closes, which stops every job still running.
pub async fn serve(server: Arc<Server>, con: Connection) -> Result<()> {
    let con = Arc::new(con);
    let mut jobs: HashMap<ExchangeId, (mpsc::UnboundedSender<ToServer>, AbortHandle)> = HashMap::new();
    let mut running = JoinSet::new();
    loop {
        tokio::select! {
            m = con.recv_one::<ToBox>() => {
                let ToBox { id, msg } = m?;
                match msg {
                    ToServer::Heartbeat(data) => {
                        data.unpack()?;
                        con.send_one(&FromBox { id, msg: FromServer::Heartbeat }).await?;
                    }
                    ToServer::Offer { unit, key, target, class } => {
                        let (tx, inbox) = mpsc::unbounded_channel();
                        let reply = Reply { con: con.clone(), id };
                        let task = running.spawn(take(server.clone(), reply, inbox, unit, key, target, class));
                        if let Some((_, old)) = jobs.insert(id, (tx, task)) {
                            old.abort();
                            bail!("a second offer of exchange {id:?}");
                        }
                    }
                    ToServer::Stop => {
                        if let Some((_, task)) = jobs.remove(&id) {
                            task.abort();
                        }
                    }
                    // A job that ended may still get the late rlibs its
                    // client was sending.
                    msg => {
                        if let Some((tx, _)) = jobs.get(&id) {
                            let _ = tx.send(msg);
                        }
                    }
                }
            }
            Some(ended) = running.join_next_with_id() => {
                let task = ended.map_or_else(|e| e.id(), |(task, ())| task);
                jobs.retain(|_, (_, t)| t.id() != task);
            }
        }
    }
}

/// Take the job a client offered, if a slot is free, and run it, telling
/// the client if it fails.
async fn take(
    server: Arc<Server>,
    reply: Reply,
    inbox: Inbox,
    unit: Unit,
    key: ArcStr,
    target: Option<ArcStr>,
    class: Option<u32>,
) {
    let run = async {
        let Some(tc) = task::block_in_place(|| server.toolchain(&key, target.as_deref())) else {
            bail!("no toolchain {key}");
        };
        let (answer, admitted) = oneshot::channel();
        let exact = unit.exact.clone();
        server.tx.send(Msg::Offer { unit, class, reply: answer }).map_err(|_| anyhow!("scheduler stopped"))?;
        let Answer { slot, version } = admitted.await?;
        let Some(slot) = slot else {
            return reply.send(FromServer::Busy { version }).await;
        };
        reply.send(FromServer::Accepted { version }).await?;
        let Exited { code, ran, linker_waited_us, measured } = job(&server, &reply, inbox, &tc, slot).await?;
        if !tc.unchanged() {
            bail!("rustup changed the toolchain {key} while the job ran");
        }
        if code != 0 {
            info!("{exact}, another box's job, failed here");
        }
        reply.send(FromServer::Exit { code, ran, linker_waited_us, measured }).await
    };
    if let Err(e) = run.await {
        warn!("remote job: {e:#}");
        let why: ArcStr = format_compact!("{e:#}").as_str().into();
        let _ = reply.send(FromServer::Failed(why)).await;
    }
}

/// How a job's rustc exited: its code and run, how long its linker waited
/// for late rlibs, and what the scheduler measured of it.
struct Exited {
    code: i32,
    ran: Ran,
    linker_waited_us: u64,
    measured: Measured,
}

/// Run a job the box took in `slot`, and return once its rustc has exited
/// and the late rlibs other jobs wait for are in.
async fn job(server: &Arc<Server>, reply: &Reply, mut inbox: Inbox, tc: &Installed, slot: Slot) -> Result<Exited> {
    let ToServer::Manifest { files, dirs, late } = next(&mut inbox).await? else {
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
    // What the job needs before rustc starts, and late rlibs: those its
    // client sends, and those another job's client is already sending.
    let (mut send_now, mut send_later) = (HashMap::new(), HashMap::new());
    let (mut coming_now, mut coming_later) = (Vec::new(), Vec::new());
    task::block_in_place(|| {
        let mut claimed: LPooled<HashSet<Hash>> = LPooled::take();
        let mut claim = |hash: Hash, when, send: &mut HashMap<Hash, Option<Sending>>, coming: &mut Vec<Coming>| {
            if claimed.insert(hash) {
                match store.claim(&hash, when) {
                    Claim::Have => (),
                    Claim::Send(s) => drop(send.insert(hash, Some(s))),
                    Claim::SendToo => drop(send.insert(hash, None)),
                    Claim::Coming(c) => coming.push(c),
                }
            }
        };
        for f in &files {
            claim(f.hash, When::Now, &mut send_now, &mut coming_now);
        }
        for r in rlibs.iter().filter(|r| !store.has(&r.hash)) {
            claim(r.meta, When::Now, &mut send_now, &mut coming_now);
            claim(r.hash, When::Late, &mut send_later, &mut coming_later);
        }
    });
    let need = FromServer::Need { now: send_now.keys().copied().collect(), later: send_later.keys().copied().collect() };
    reply.send(need).await?;
    let mut now = Uploads::new(send_now);
    while !now.done() {
        now.take(store, next(&mut inbox).await?)?;
    }
    let ToServer::Run { args, env, cwd } = next(&mut inbox).await? else {
        bail!("expected run");
    };
    check(&cwd)?;
    let later: LPooled<HashSet<Hash>> = send_later.keys().copied().chain(coming_later.iter().map(Coming::hash)).collect();
    let sent_later = send_later.len();
    // From Run on the client sends only late rlibs, which a task of their
    // own receives into the store whatever the job is doing, so no chunk
    // waits for the job, and a job that waits for one gets it even once
    // this job's rustc has exited.
    let (stored_tx, mut stored) = mpsc::unbounded_channel();
    let (exited, exit) = oneshot::channel();
    let mut receiving = JoinSet::new();
    if send_later.is_empty() {
        drop((inbox, stored_tx, exit));
    } else {
        receiving.spawn(receive_late(server.clone(), inbox, Uploads::new(send_later), stored_tx, exit));
    }
    let work = async {
        for c in coming_now {
            c.arrived(store).await?;
        }
        task::block_in_place(|| store.evict());
        let n = server.next_job.fetch_add(1, Ordering::Relaxed);
        let root = JobDir(server.jobs_dir.join(format_compact!("{n}").as_str()));
        task::block_in_place(|| root.build(store, &files, rlibs, &later, &dirs, &cwd))?;
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
        let place = |hash: Hash| {
            for r in rlibs.iter().filter(|r| r.hash == hash) {
                blobs::replace(store, &hash, &root.at(&r.path))?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let arrive = linker.map(|_| async {
            let sent = async {
                for _ in 0..sent_later {
                    let hash = stored.recv().await.context("the late rlibs stopped coming")??;
                    place(hash)?;
                }
                Ok(())
            };
            let others = async {
                for c in coming_later {
                    let hash = c.hash();
                    c.arrived(store).await?;
                    place(hash)?;
                }
                Ok(())
            };
            futures::try_join!(sent, others).map(|_| ())
        });
        run(reply, store, cmd, &root.0, slot, arrive, server.level).await
    };
    let worked = work.await;
    let _ = exited.send(());
    while let Some(r) = stored.recv().await {
        if let Err(e) = r {
            info!("a late rlib another job waited for: {e:#}");
        }
    }
    worked
}

/// Blobs a job's client sends, each stored once its last chunk is in.
struct Uploads {
    /// Each blob still to come: the guard jobs waiting for it wait on, none
    /// when another job's client sends it too, and its upload once begun.
    blobs: HashMap<Hash, (Option<Sending>, Option<Upload>)>,
    /// Blobs given up, whose chunks are dropped.
    released: HashSet<Hash>,
}

impl Uploads {
    fn new(sends: HashMap<Hash, Option<Sending>>) -> Self {
        Self { blobs: sends.into_iter().map(|(h, s)| (h, (s, None))).collect(), released: HashSet::new() }
    }

    fn done(&self) -> bool {
        self.blobs.is_empty()
    }

    /// Write a chunk the client sent; the blob's hash once it is stored.
    fn take(&mut self, store: &Store, msg: ToServer) -> Result<Option<Hash>> {
        let ToServer::Chunk { hash, data, last } = msg else {
            bail!("expected a chunk");
        };
        let Some((_, up)) = self.blobs.get_mut(&hash) else {
            if self.released.contains(&hash) {
                return Ok(None);
            }
            bail!("unrequested blob {hash}");
        };
        task::block_in_place(|| {
            let up = match up {
                Some(up) => up,
                None => up.insert(store.upload(hash)?),
            };
            up.write(&data.unpack()?).context("writing a blob")
        })?;
        if !last {
            return Ok(None);
        }
        let (sending, up) = self.blobs.remove(&hash).expect("present");
        task::block_in_place(|| up.expect("written").finish(store))?;
        // Jobs waiting for it find it stored.
        drop(sending);
        Ok(Some(hash))
    }

    /// Give up each blob no other job waits for.
    fn release_unawaited(&mut self) {
        let released = &mut self.released;
        self.blobs.retain(|hash, (sending, _)| {
            let keep = sending.as_ref().is_some_and(|s| !s.release());
            if !keep {
                released.insert(*hash);
            }
            keep
        });
    }
}

/// Receive late rlibs into the store until each is in, telling `stored`
/// of each, or of why it stopped. Once `exit` fires or drops, only those
/// another job waits for.
async fn receive_late(
    server: Arc<Server>,
    mut inbox: Inbox,
    mut uploads: Uploads,
    stored: mpsc::UnboundedSender<Result<Hash>>,
    mut exit: oneshot::Receiver<()>,
) {
    let mut exited = false;
    let received = async {
        while !uploads.done() {
            tokio::select! {
                _ = &mut exit, if !exited => {
                    exited = true;
                    uploads.release_unawaited();
                }
                msg = next(&mut inbox) => {
                    if let Some(hash) = uploads.take(&server.store, msg?)? {
                        let _ = stored.send(Ok(hash));
                    }
                    if exited {
                        uploads.release_unawaited();
                    }
                }
            }
        }
        Ok(())
    };
    if let Err(e) = received.await {
        let _ = stored.send(Err(e));
    }
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
        let dir = mem::take(&mut self.0);
        match Handle::try_current() {
            Ok(rt) => drop(rt.spawn_blocking(move || fs::remove_dir_all(dir))),
            Err(_) => drop(fs::remove_dir_all(dir)),
        }
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

/// Kills the sandbox when dropped, which does nothing once it has been
/// reaped: a pidfd can't hit a reused pid.
struct Kill(OwnedFd);

impl Kill {
    fn open(pid: libc::pid_t) -> Result<Self> {
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error()).context("opening a pidfd for the sandbox");
        }
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd as i32) }))
    }
}

impl Drop for Kill {
    fn drop(&mut self) {
        unsafe {
            libc::syscall(libc::SYS_pidfd_send_signal, self.0.as_raw_fd(), libc::SIGKILL, std::ptr::null::<()>(), 0)
        };
    }
}

/// The sandbox's pid, what kills it, and its stdout and stderr.
type Spawned = (libc::pid_t, Kill, (Option<std::process::ChildStdout>, Option<std::process::ChildStderr>));

/// Run the sandbox in `slot`, which it gives back once rustc exits. With
/// `arrive`, a future that puts late rlibs in place, a gate holds the
/// linker until it has.
async fn run(
    reply: &Reply,
    store: &Store,
    mut cmd: Command,
    root: &Path,
    slot: Slot,
    arrive: Option<impl Future<Output = Result<()>>>,
    level: i32,
) -> Result<Exited> {
    let (mut gate, theirs) = match &arrive {
        Some(_) => Gate::new().map(|(gate, theirs)| (Some(gate), Some(theirs)))?,
        None => (None, None),
    };
    let cpus = slot.grant.cpus.clone();
    let gate_fd = theirs.as_ref().map(|t| t.as_raw_fd());
    cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    unsafe {
        cmd.pre_exec(move || {
            sys::pin(&cpus).map_err(io::Error::other)?;
            gate_fd.map_or(Ok(()), Gate::inherit)
        });
    }
    let start = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (spawned_tx, spawned) = oneshot::channel::<Result<Spawned>>();
    // A thread of its own starts the sandbox and waits for it, for its
    // resource use.
    let xtx = tx.clone();
    thread::spawn(move || {
        let child = cmd.spawn();
        // The sandbox holds its end of the gate now; the gate hears it hang up.
        drop(theirs);
        let mut child = match child {
            Ok(child) => child,
            Err(e) => {
                let _ = spawned_tx.send(Err(anyhow::Error::from(e).context("starting bwrap")));
                return;
            }
        };
        let pid = child.id() as libc::pid_t;
        let kill = match Kill::open(pid) {
            Ok(kill) => kill,
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = spawned_tx.send(Err(e));
                return;
            }
        };
        let pipes = (child.stdout.take(), child.stderr.take());
        // wait4 reaps it; the Child must not try.
        mem::forget(child);
        // A job dropped meanwhile drops the Kill here, which stops the sandbox.
        let _ = spawned_tx.send(Ok((pid, kill, pipes)));
        let mut status = 0;
        let mut ru: libc::rusage = unsafe { mem::zeroed() };
        if unsafe { libc::wait4(pid, &mut status, 0, &mut ru) } == pid {
            let _ = xtx.send(Ev::Exit(ExitStatus::from_raw(status), ru, Instant::now()));
        }
    });
    let (pid, _kill, (stdout, stderr)) = spawned.await.context("the sandbox's thread died")??;
    slot.started(pid as u32);
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
                        send_artifact(reply, store, root, &line, level).await?;
                    }
                    reply.send(FromServer::Stderr(line)).await?;
                }
                Ev::Stdout(data) => reply.send(FromServer::Stdout(data)).await?,
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
    let (status, ru, exited) = exit.expect("loop ends with an exit");
    let wall_us = exited.saturating_duration_since(start).as_micros() as u64;
    let ran = Ran { wall_us, maxrss_kb: ru.ru_maxrss as u64, front };
    let code = status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    debug!("sandbox exited {code}");
    let linker_waited_us = gate.map_or(0, |g| g.waited_us());
    let measured = slot.measured();
    if code == 0 {
        slot.finish(ran, linker_waited_us);
    }
    Ok(Exited { code, ran, linker_waited_us, measured })
}

/// Send the file an artifact line names, before the line. A crate file
/// stays in the store for jobs that read it later, and its last chunk
/// tells the client its hash.
async fn send_artifact(reply: &Reply, store: &Store, root: &Path, line: &[u8], level: i32) -> Result<()> {
    let path = protocol::artifact(line)?;
    check(&path)?;
    let host = root.join(&path[1..]);
    let file = File::open(&host).with_context(|| format_compact!("opening {}", host.display()))?;
    let mode = file.metadata()?.permissions().mode() & 0o777;
    let mut hasher = remote::kept(&path).then(Hasher::default);
    let mut chunks = pin!(remote::pack_file(file, level)?);
    while let Some(chunk) = chunks.next().await {
        let (raw, data, last) = chunk.with_context(|| format_compact!("reading {}", host.display()))?;
        if let Some(h) = &mut hasher {
            h.update(&raw);
        }
        let part = match (last, &hasher) {
            (false, _) => Part::Middle,
            (true, None) => Part::End(None),
            (true, Some(h)) => {
                let hash = h.finish();
                store.adopt(&host, &hash)?;
                Part::End(Some(hash))
            }
        };
        reply.send(FromServer::Output { path: path.clone(), mode, data, part }).await?;
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
