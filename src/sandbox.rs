use crate::{
    blobs::{self, Claim, Coming, Hash, Hasher, Store, Upload},
    link::{self, Gate},
    protocol::{self, Front, Ran, Unit},
    remote::{self, ExchangeId, FileEntry, FromBox, FromServer, Late, Part, Rlib, SYSTEM, ToBox, ToServer},
    sched::{Answer, Msg, Slot},
    sys,
    toolchain::Installed,
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
    task::{AbortHandle, JoinSet},
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
    /// The zstd level it compresses outputs at, 0 for none.
    level: i32,
}

impl Server {
    pub fn new(
        store: Store,
        jobs_dir: PathBuf,
        toolchains: Vec<Installed>,
        tx: mpsc::UnboundedSender<Msg>,
        level: i32,
    ) -> Result<Self> {
        let _ = fs::remove_dir_all(&jobs_dir);
        fs::create_dir_all(&jobs_dir).with_context(|| format_compact!("creating {}", jobs_dir.display()))?;
        let stand_in = jobs_dir.join("linker");
        link::write_stand_in_linker(&stand_in)?;
        Ok(Self { store, jobs_dir, toolchains, tx, stand_in, next_job: AtomicU64::new(0), level })
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
                    // CR claude for eric: [risk/medium] Each job's inbox has no
                    // bound, and this loop never waits on it, so nothing pushes
                    // back on the client. Its client sends late rlibs right after
                    // Run, but the job doesn't read them until its sandbox is
                    // running: while it waits for another job's upload (the
                    // coming_now loop in sandbox::job, which can take a minute
                    // when a 520 MB rlib crosses wifi), builds its tree and
                    // evicts, every late chunk queues here in memory, on a box
                    // whose cache may also be in RAM. A bounded channel would
                    // stall every job on the link behind one slow job. Starting
                    // the late `receive` into the store as soon as Run arrives
                    // avoids that trade-off. (sandbox-05)
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
    mut inbox: Inbox,
    unit: Unit,
    key: ArcStr,
    target: Option<ArcStr>,
    class: Option<u32>,
) {
    let run = async {
        let Some(tc) = server.toolchains.iter().find(|t| t.toolchain.builds(&key, target.as_deref())) else {
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
        match job(&server, &reply, &mut inbox, tc, &slot).await? {
            Some((ran, linker_waited_us)) => slot.finish(ran, linker_waited_us),
            None => info!("{exact}, another box's job, failed here"),
        }
        Ok(())
    };
    if let Err(e) = run.await {
        warn!("remote job: {e:#}");
        let why: ArcStr = format_compact!("{e:#}").as_str().into();
        let _ = reply.send(FromServer::Failed(why)).await;
    }
}

/// The Ran of a successful compile, and how long its linker waited for
/// late rlibs.
async fn job(
    server: &Server,
    reply: &Reply,
    inbox: &mut Inbox,
    tc: &Installed,
    slot: &Slot,
) -> Result<Option<(Ran, u64)>> {
    let ToServer::Manifest { files, dirs, late } = next(inbox).await? else {
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
    let mut claimed = HashSet::new();
    let mut claim = |hash: Hash, send: &mut HashMap<Hash, _>, coming: &mut Vec<Coming>| {
        if claimed.insert(hash) {
            match store.claim(&hash) {
                Claim::Have => (),
                Claim::Send(s) => drop(send.insert(hash, s)),
                Claim::Coming(c) => coming.push(c),
            }
        }
    };
    for f in &files {
        claim(f.hash, &mut send_now, &mut coming_now);
    }
    for r in rlibs.iter().filter(|r| !store.has(&r.hash)) {
        claim(r.meta, &mut send_now, &mut coming_now);
        claim(r.hash, &mut send_later, &mut coming_later);
    }
    let need = FromServer::Need { now: send_now.keys().copied().collect(), later: send_later.keys().copied().collect() };
    reply.send(need).await?;
    let now = send_now.keys().copied().collect();
    receive(inbox, store, now, |hash| {
        send_now.remove(&hash);
        Ok(())
    })
    .await?;
    let ToServer::Run { args, env, cwd } = next(inbox).await? else {
        bail!("expected run");
    };
    check(&cwd)?;
    // CR claude for eric: [bug/medium] A Coming waits on an upload that moves
    // only while the sending job is alive, and late rlibs are read from the
    // inbox only inside run's loop, i.e. while the sender's rustc runs. (1)
    // When the job whose client sends a shared late rlib exits first, a compile
    // error for example, run returns, its `send_later` drops the Sending, and
    // every job waiting on that rlib fails with "never arrived", although its
    // own client could have sent it. Each then reruns locally and the daemon
    // sends Msg::Mismatch, so a compile error in one test can get the other
    // tests that share its rlibs kept local until restart. (2) If X needs now a
    // hash that is Y's late rlib, and Y needs now a hash that is X's late rlib,
    // both wait here before starting rustc, neither reads its late chunks, and
    // both hang, holding their slots, with no timeout on either side. That
    // needs two lanes whose rlibs differ in having an .rmeta beside them (see
    // remote::crate_files), so it is rare. Fix options: when a Coming fails,
    // ask this job's own client for the blob (another Need, which changes the
    // protocol); or keep reading a job's late uploads until they finish, even
    // after its rustc exits, while another job waits on them; and do not wait
    // here on a hash another job will receive only once its rustc runs.
    // (sandbox-03)
    for c in coming_now {
        c.arrived(store).await?;
    }
    // CR claude for claude: [perf/medium] sandbox::job does blocking filesystem
    // work on a tokio worker: Store::claim calls Store::has, an open plus
    // futimens, once per input file (thousands for a graphix test);
    // sandbox::receive writes each 1 MB chunk and decompresses it inline;
    // Store::evict below reads and stats every blob in the store;
    // JobDir::build makes a hard link per input; JobDir's Drop runs
    // remove_dir_all on the whole tree. Each blocks a worker for tens of ms or
    // more on a large job, and with many jobs at once they block the serve
    // loops and other jobs' sends. Move them to spawn_blocking or
    // block_in_place, as daemon::run_remote does for remote::stage.
    // (sandbox-04)
    store.evict();
    let n = server.next_job.fetch_add(1, Ordering::Relaxed);
    let root = JobDir(server.jobs_dir.join(format_compact!("{n}").as_str()));
    let later: HashSet<Hash> = send_later.keys().copied().chain(coming_later.iter().map(Coming::hash)).collect();
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
    let place = |hash: Hash| {
        for r in rlibs.iter().filter(|r| r.hash == hash) {
            blobs::replace(store, &hash, &root.at(&r.path))?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let late = match linker {
        None => None,
        Some(_) => {
            let sent = send_later.keys().copied().collect();
            let sent = receive(inbox, store, sent, |hash| {
                send_later.remove(&hash);
                place(hash)
            });
            let others = async {
                for c in coming_later {
                    let hash = c.hash();
                    c.arrived(store).await?;
                    place(hash)?;
                }
                Ok(())
            };
            Some((Gate::new()?, async { futures::try_join!(sent, others).map(|_| ()) }))
        }
    };
    run(reply, store, cmd, &root.0, slot, late, server.level).await
}

/// Receive chunks until every blob in `need` is stored, telling `stored`
/// of each as it is.
async fn receive(
    inbox: &mut Inbox,
    store: &Store,
    mut need: HashSet<Hash>,
    mut stored: impl FnMut(Hash) -> Result<()>,
) -> Result<()> {
    let mut uploads: HashMap<Hash, Upload> = HashMap::new();
    while !need.is_empty() {
        let ToServer::Chunk { hash, data, last } = next(inbox).await? else {
            bail!("expected a chunk");
        };
        if !need.contains(&hash) {
            bail!("unrequested blob {hash}");
        }
        let up = match uploads.entry(hash) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(store.upload(hash)?),
        };
        up.write(&data.unpack()?)?;
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

    // CR claude for eric: [risk/high] The job tree is hard links to store blobs,
    // bound read-write into the sandbox, and only mode 0444 stands between the
    // job and the store's inodes. The job runs as the user that owns them, so
    // it can chmod and rewrite a blob in place. The blob keeps its hash name,
    // Store::has keeps saying yes, and nothing checks the content again, so
    // every later job from every client is given the altered file. One
    // client's job can then change what other clients build against, which
    // README's Security section doesn't cover. Store::adopt has the same
    // shape: it links a job's output into the store while the job can still
    // write it. A tool that writes outputs in place over an input path would
    // corrupt the store without any intent. Options: copy or reflink instead
    // of hard linking (costs time and space), ro-bind the inputs (one mount per
    // directory of inputs), keep the store owned by another uid, or check
    // hashes when placing (costs a read). Needs a decision. (sandbox-02)
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
    reply: &Reply,
    store: &Store,
    mut cmd: Command,
    root: &Path,
    slot: &Slot,
    late: Option<(Gate, impl Future<Output = Result<()>>)>,
    level: i32,
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
    // CR claude for claude: [risk/low] There are two ways a dropped job's
    // sandbox can keep running with no one killing it, and neither is logged.
    // pidfd_open failing leaves Kill(None). And Kill is armed only once
    // `spawned` resolves, so a job aborted (Stop, channel closed) between
    // cmd.spawn on the thread and that await leaves bwrap running with its
    // slot already given back. Fail the job when pidfd_open fails, and open the
    // pidfd on the spawning thread so the Kill exists before the task can be
    // dropped. (sandbox-06)
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
    kill.0 = None;
    let (status, ru, exited) = exit.expect("loop ends with an exit");
    let wall_us = exited.saturating_duration_since(start).as_micros() as u64;
    let ran = Ran { wall_us, maxrss_kb: ru.ru_maxrss as u64, front };
    let code = status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    debug!("sandbox exited {code}");
    let linker_waited_us = gate.map_or(0, |g| g.waited_us());
    reply.send(FromServer::Exit { code, ran, linker_waited_us, measured: slot.measured() }).await?;
    Ok((code == 0).then_some((ran, linker_waited_us)))
}

/// Send the file an artifact line names, before the line. A crate file
/// stays in the store for jobs that read it later, and its last chunk
/// tells the client its hash.
// CR claude for eric: [bug/high] The server works on the job tree from the host
// while the sandboxed job, which has that tree bound read-write, can put
// symlinks in it, and every host-side operation on the tree follows them:
// File::open here and fs::copy in Store::adopt follow a final symlink, and
// hard_link, rename and create_dir_all (Store::adopt, blobs::replace for late
// rlibs, JobDir::build) follow symlinked directory components. check only
// vets the string. So a job, malicious or confused, can make the server read
// files outside the tree and send them back or adopt them into the store, and
// can redirect where blobs::replace writes, which reaches the server user's
// files outside the sandbox. That is more than README's Security section
// grants (code inside a sandbox with no home). Fix: resolve every path from
// the job tree beneath `root` without following links (openat2 with
// RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS, then fstat that the result is a
// regular file and work from the fd), or place late rlibs in a directory the
// sandbox cannot write. (sandbox-01)
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
