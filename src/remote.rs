use crate::{
    blobs::{Hash, Hashes},
    protocol::{self, Job, Measured, Ran, Reads, Unit},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use compact_str::format_compact;
use futures::{Stream, StreamExt, stream};
use log::warn;
use netidx::{path::Path as NPath, subscriber::Subscriber};
use netidx_core::atomic_id;
use netidx_derive::Pack;
use netidx_protocols::pack_channel::client::Connection;
use parking_lot::Mutex;
use poolshark::local::LPooled;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{ErrorKind, Read, Write},
    iter,
    os::unix::fs::{FileExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{OnceCell, mpsc},
    task, time,
};

/// Bytes per message when a file crosses the wire.
pub const CHUNK: usize = 1 << 20;
/// Chunks of a file read and packed at once, each on a blocking thread, so
/// compressing keeps up with the network on a busy box.
const PACKING: usize = 8;
/// A job reads at most this much it isn't told about by dep-info. A
/// package without dep-info is walked whole; past this it stays local.
const WALK_LIMIT: u64 = 256 << 20;

#[derive(Debug, Clone, Pack)]
pub struct FileEntry {
    pub path: ArcStr,
    pub hash: Hash,
}

/// An rlib a job reads only once rustc runs its linker. Until then a
/// stand-in holding just its metadata, the blob `meta`, takes its place.
#[derive(Debug, Clone, Pack)]
pub struct Rlib {
    pub path: ArcStr,
    pub hash: Hash,
    pub meta: Hash,
}

/// Rlibs that may arrive while rustc runs, and the linker, a program on
/// `PATH`, that waits for them.
#[derive(Debug, Clone, Pack)]
pub struct Late {
    pub linker: ArcStr,
    pub rlibs: Vec<Rlib>,
}

#[derive(Debug, Pack)]
pub enum ToServer {
    /// Run `unit` with the toolchain `key`, pinned to `class`, or with
    /// none on every CPU.
    Offer { unit: Unit, key: ArcStr, target: Option<ArcStr>, class: Option<u32> },
    /// `files` are in place before rustc starts; `late` rlibs may follow.
    Manifest { files: Vec<FileEntry>, dirs: Vec<ArcStr>, late: Option<Late> },
    Chunk { hash: Hash, data: Packed, last: bool },
    Run { args: Vec<ArcStr>, env: Vec<(ArcStr, ArcStr)>, cwd: ArcStr },
    /// Instead of an offer: bytes to time a send by, which the server
    /// answers and drops.
    Heartbeat(Packed),
    /// The client no longer wants the job: stop it if it still runs.
    Stop,
}

atomic_id!(ExchangeId);

/// A message on a link, for one of its exchanges: a job or a heartbeat.
#[derive(Debug, Pack)]
pub struct ToBox {
    pub id: ExchangeId,
    pub msg: ToServer,
}

#[derive(Debug, Pack)]
pub struct FromBox {
    pub id: ExchangeId,
    pub msg: FromServer,
}

/// How long connecting to a box may take.
const CONNECT: Duration = Duration::from_secs(10);

/// The channel to a box, which every job sent there and every heartbeat
/// shares, each an exchange of its own. It stays open until it fails.
pub struct Link {
    con: Connection,
    jobs: NPath,
    /// Where each open exchange's messages go; none once the channel failed.
    exchanges: Mutex<Option<HashMap<ExchangeId, mpsc::UnboundedSender<FromServer>>>>,
}

impl Link {
    async fn connect(subscriber: &Subscriber, jobs: NPath) -> Result<Arc<Self>> {
        let con = match time::timeout(CONNECT, Connection::connect(subscriber, jobs.clone())).await {
            Ok(con) => con.with_context(|| format_compact!("connecting to {jobs}"))?,
            Err(_) => bail!("connecting to {jobs} timed out"),
        };
        let link = Arc::new(Self { con, jobs, exchanges: Mutex::new(Some(HashMap::new())) });
        tokio::spawn(link.clone().read());
        Ok(link)
    }

    /// Hand each message to its exchange until the channel fails, then end
    /// every exchange.
    async fn read(self: Arc<Self>) {
        let e = loop {
            let read = self.con.recv(|m: FromBox| {
                if let Some(tx) = self.exchanges.lock().as_ref().and_then(|ex| ex.get(&m.id)) {
                    let _ = tx.send(m.msg);
                }
                true
            });
            if let Err(e) = read.await {
                break e;
            }
        };
        warn!("the channel to {} failed: {e:#}", self.jobs);
        *self.exchanges.lock() = None;
    }

    pub fn failed(&self) -> bool {
        self.exchanges.lock().is_none()
    }

    pub fn open(self: &Arc<Self>) -> Result<(Out, In)> {
        let id = ExchangeId::new();
        let (tx, rx) = mpsc::unbounded_channel();
        match &mut *self.exchanges.lock() {
            Some(ex) => ex.insert(id, tx),
            None => bail!("the channel to {} failed", self.jobs),
        };
        Ok((Out { link: self.clone(), id }, In { link: self.clone(), id, rx, over: false }))
    }
}

/// What an exchange sends.
pub struct Out {
    link: Arc<Link>,
    id: ExchangeId,
}

impl Out {
    pub fn send(&self, msg: ToServer) -> Result<()> {
        self.link.con.send_one(&ToBox { id: self.id, msg })
    }

    pub async fn flush(&self) -> Result<()> {
        self.link.con.flush().await
    }
}

/// What an exchange receives. Dropped before the box ends the exchange,
/// it tells the box to stop the job.
pub struct In {
    link: Arc<Link>,
    id: ExchangeId,
    rx: mpsc::UnboundedReceiver<FromServer>,
    over: bool,
}

impl In {
    pub async fn recv(&mut self) -> Result<FromServer> {
        let m = self.rx.recv().await.with_context(|| format_compact!("the channel to {} failed", self.link.jobs))?;
        self.over |= matches!(
            m,
            FromServer::Busy { .. } | FromServer::Failed(_) | FromServer::Exit { .. } | FromServer::Heartbeat
        );
        Ok(m)
    }
}

impl Drop for In {
    fn drop(&mut self) {
        if let Some(ex) = &mut *self.link.exchanges.lock() {
            ex.remove(&self.id);
        }
        if !self.over {
            let _ = self.link.con.send_one(&ToBox { id: self.id, msg: ToServer::Stop });
        }
    }
}

/// The link to each box jobs from here go to, opened when first used.
pub struct Links {
    subscriber: Subscriber,
    links: Mutex<HashMap<NPath, Arc<OnceCell<Arc<Link>>>>>,
}

impl Links {
    pub fn new(subscriber: Subscriber) -> Self {
        Self { subscriber, links: Mutex::default() }
    }

    /// The link to the box listening at `jobs`: the one open, else a new one.
    pub async fn get(&self, jobs: &NPath) -> Result<Arc<Link>> {
        let cell = {
            let mut links = self.links.lock();
            let cell = links.entry(jobs.clone()).or_default();
            if cell.get().is_some_and(|l| l.failed()) {
                *cell = Arc::default();
            }
            cell.clone()
        };
        cell.get_or_try_init(|| Link::connect(&self.subscriber, jobs.clone())).await.cloned()
    }
}

/// A chunk of a file as sent: as it is, or compressed with zstd when that
/// makes it smaller.
#[derive(Debug, Clone, Pack)]
pub enum Packed {
    Raw(Bytes),
    Zstd(Bytes),
}

impl Packed {
    /// `data`, compressed at `level` if that makes it smaller; as it is at
    /// level 0.
    pub fn pack(data: Bytes, level: i32) -> Self {
        if level > 0
            && let Ok(z) = zstd::bulk::compress(&data, level)
            && z.len() < data.len()
        {
            return Packed::Zstd(z.into());
        }
        Packed::Raw(data)
    }

    /// Its size as sent.
    pub fn wire_len(&self) -> usize {
        match self {
            Packed::Raw(b) | Packed::Zstd(b) => b.len(),
        }
    }

    /// The bytes it carries, at most a `CHUNK`.
    pub fn unpack(self) -> Result<Bytes> {
        match self {
            Packed::Raw(b) => Ok(b),
            Packed::Zstd(z) => Ok(zstd::bulk::decompress(&z, CHUNK).context("decompressing a chunk")?.into()),
        }
    }
}

/// `file`'s chunks in order, each the bytes it carries, those packed at
/// `level`, and whether it is the last. The last is shorter than a
/// `CHUNK`, so a file whose size is a multiple of one ends with an empty
/// chunk.
pub fn pack_file(file: File, level: i32) -> Result<impl Stream<Item = Result<(Bytes, Packed, bool)>>> {
    let len = file.metadata()?.len();
    let (file, chunks) = (Arc::new(file), len / CHUNK as u64 + 1);
    let packed = stream::iter(0..chunks).map(move |i| {
        let file = file.clone();
        async move {
            let at = i * CHUNK as u64;
            task::spawn_blocking(move || {
                let mut raw = BytesMut::zeroed((len - at).min(CHUNK as u64) as usize);
                file.read_exact_at(&mut raw, at)?;
                let raw = raw.freeze();
                Ok((raw.clone(), Packed::pack(raw, level), i + 1 == chunks))
            })
            .await?
        }
    });
    Ok(packed.buffered(PACKING))
}

/// Where a chunk of an output falls in its file: before the end, or at
/// it, with the file's hash if the server keeps it as a crate file.
#[derive(Debug, Clone, Copy, Pack)]
pub enum Part {
    Middle,
    End(Option<Hash>),
}

/// Bytes sent: of files, and as sent.
#[derive(Default)]
pub struct Sent {
    pub raw: AtomicU64,
    pub wire: AtomicU64,
}

#[derive(Debug, Pack)]
pub enum FromServer {
    /// `version` is the first version of the server's published state
    /// that reflects the answer.
    Accepted { version: u64 },
    Busy { version: u64 },
    /// The blobs the server lacks: those to send before `Run`, and late
    /// rlibs to send after it.
    Need { now: Vec<Hash>, later: Vec<Hash> },
    Stdout(Bytes),
    /// One line of rustc's stderr. An artifact line comes after the file
    /// it names.
    Stderr(Bytes),
    Output { path: ArcStr, mode: u32, data: Packed, part: Part },
    /// `ran` and `measured` are the server's measurements, whatever the
    /// code, and `linker_waited_us` how long its linker waited for late
    /// rlibs.
    Exit { code: i32, ran: Ran, linker_waited_us: u64, measured: Measured },
    Failed(ArcStr),
    /// A heartbeat arrived.
    Heartbeat,
}

/// The files a job reads and the directories it needs, at their absolute
/// client paths, and where it may write.
pub struct Staged {
    pub files: Vec<FileEntry>,
    pub dirs: Vec<ArcStr>,
    pub late: Option<Late>,
    pub outputs: Vec<PathBuf>,
    /// Where to read each blob the server may ask for, and its size.
    blobs: HashMap<Hash, (ArcStr, u64)>,
    /// The bytes staging hashed, those it already knew aside, and µs it
    /// spent on them.
    pub hashed: (u64, u64),
}

impl Staged {
    // XCR claude for claude: [bug/low] This reports every late rlib's `.rmeta` as
    // held by the server, but sandbox::job claims `r.meta` only for rlibs the
    // store lacks (`rlibs.iter().filter(|r| !store.has(&r.hash))`). When the
    // server already had the rlib, it never received the rmeta, yet
    // sched::Held::used records it as held, so a later library job that reads
    // that `.rmeta` (Reads::Metadata) is priced as sending nothing and then
    // sends it. Fix: have the server say what it holds (e.g. in Need or Exit),
    // or leave out the meta of rlibs that weren't asked for. It also stats
    // every crate file on an async worker, after the exit. (remote-03)
    // 2026-11-01 claude: Took the CR's second option, no protocol change:
    // Staged::crates(asked) counts a late rlib's .rmeta only when the server's
    // Need asked for the rlib (later) or the .rmeta (now); a server that had the
    // rlib never claimed it. That can under-report (rlib and .rmeta both coming
    // through another job's upload), which only prices a later send a little
    // high. Sizes now come from staging (Staged.blobs holds each blob's length),
    // so nothing is stat'd after the exit. Test:
    // remote::test::a_server_holds_a_late_rlibs_metadata_only_if_it_asked_for_one_of_them
    // (fails if the meta is always counted, or if sizes come from stat: the
    // files are removed first).
    /// The crate files among its inputs the server holds once the job has
    /// run, by hash with their size in kB. That leaves out the metadata of
    /// a late rlib unless the server `asked` for it or for the rlib: a
    /// server that has the rlib needs no stand-in.
    fn crates<'a>(&'a self, asked: &'a HashSet<Hash>) -> impl Iterator<Item = (Hash, u64)> + 'a {
        let late = self.late.iter().flat_map(|l| &l.rlibs).flat_map(|r| {
            let meta = (asked.contains(&r.hash) || asked.contains(&r.meta)).then_some(r.meta);
            iter::once(r.hash).chain(meta)
        });
        let files = self.files.iter().filter(|f| kept(&f.path)).map(|f| f.hash);
        files.chain(late).filter_map(|hash| Some((hash, self.blobs.get(&hash)?.1.div_ceil(1024))))
    }
}

/// `p` made absolute against `cwd`, with `.` and `..` resolved.
pub fn absolute(cwd: &Path, p: &str) -> PathBuf {
    let mut out = PathBuf::from("/");
    for c in cwd.join(p).components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(n) => out.push(n),
            Component::RootDir | Component::CurDir | Component::Prefix(_) => (),
        }
    }
    out
}

fn env<'a>(job: &'a Job, key: &str) -> Option<&'a str> {
    job.env.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// The prerequisites in a make-style dep-info file.
fn dep_info(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let Some((_, deps)) = line.split_once(": ") else { continue };
        let mut cur = String::new();
        let mut chars = deps.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => cur.extend(chars.next()),
                ' ' => {
                    if !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                    }
                }
                c => cur.push(c),
            }
        }
        if !cur.is_empty() {
            out.push(cur);
        }
    }
    out
}

/// Top-level directories the server mounts from its own system, so their
/// files are never sent.
pub const SYSTEM: &[&str] = &["usr", "etc", "proc", "dev", "sys", "run", "bin", "lib", "lib64", "sbin"];

fn system(path: &Path) -> bool {
    path.components().nth(1).is_some_and(|c| SYSTEM.iter().any(|s| c.as_os_str() == *s))
}

fn utf8(path: &Path) -> Result<ArcStr> {
    path.to_str().map(ArcStr::from).with_context(|| format_compact!("{} is not utf-8", path.display()))
}

/// Whether a server keeps the file at `path`, an output or an input, in
/// its store: crate files, which later jobs read.
pub fn kept(path: &str) -> bool {
    [".rlib", ".rmeta", ".so"].iter().any(|ext| path.ends_with(ext))
}

/// What a job sends of a crate below it.
pub enum CrateFiles {
    One(PathBuf),
    /// The rlib, which may come late, and the metadata that stands in for
    /// it until then.
    Late { rlib: PathBuf, meta: PathBuf },
}

/// What a job that reads `reads` sends of the crate named by `path`, its
/// `.rmeta`, `.rlib` or `.so`. A job that links reads the rlib, which may
/// come late if there is metadata to stand in for it.
pub fn crate_files(path: PathBuf, reads: &Reads) -> CrateFiles {
    let rlib = match path.extension() {
        Some(e) if e == "rmeta" || e == "rlib" => path.with_extension("rlib"),
        _ => return CrateFiles::One(path),
    };
    match reads {
        Reads::Metadata => CrateFiles::One(path),
        Reads::Rlibs => CrateFiles::One(rlib),
        Reads::RlibsToLink { .. } => {
            let meta = rlib.with_extension("rmeta");
            if meta.is_file() { CrateFiles::Late { rlib, meta } } else { CrateFiles::One(rlib) }
        }
    }
}

struct Collect<'a> {
    hashes: &'a Hashes,
    files: Vec<FileEntry>,
    rlibs: Vec<Rlib>,
    blobs: HashMap<Hash, (ArcStr, u64)>,
    dirs: HashSet<PathBuf>,
    seen: HashSet<PathBuf>,
    walked: u64,
    hashed: (u64, u64),
}

impl<'a> Collect<'a> {
    fn new(hashes: &'a Hashes) -> Self {
        Self {
            hashes,
            files: Vec::new(),
            rlibs: Vec::new(),
            blobs: HashMap::new(),
            dirs: HashSet::new(),
            seen: HashSet::new(),
            walked: 0,
            hashed: (0, 0),
        }
    }

    fn blob(&mut self, path: &Path) -> Result<(ArcStr, Hash)> {
        let s = utf8(path)?;
        let named = self.hashes.name(path)?;
        if let Some(us) = named.hashed_us {
            self.hashed = (self.hashed.0 + named.len, self.hashed.1 + us);
        }
        self.blobs.insert(named.hash, (s.clone(), named.len));
        Ok((s, named.hash))
    }

    fn file(&mut self, path: PathBuf) -> Result<()> {
        if system(&path) || !self.seen.insert(path.clone()) {
            return Ok(());
        }
        let (path, hash) = self.blob(&path)?;
        self.files.push(FileEntry { path, hash });
        Ok(())
    }

    /// A crate below the job: see `crate_files`.
    fn krate(&mut self, path: PathBuf, reads: &Reads) -> Result<()> {
        match crate_files(path, reads) {
            CrateFiles::One(path) => self.file(path),
            CrateFiles::Late { rlib, meta } => {
                if !self.seen.insert(rlib.clone()) {
                    return Ok(());
                }
                let (_, meta) = self.blob(&meta)?;
                let (path, hash) = self.blob(&rlib)?;
                self.rlibs.push(Rlib { path, hash, meta });
                Ok(())
            }
        }
    }

    fn dir(&mut self, path: PathBuf) {
        self.dirs.insert(path);
    }

    /// Every file under `root`, skipping hidden directories, cargo's output
    /// directories, which cargo marks with `CACHEDIR.TAG`, and other
    /// packages, which have their own `Cargo.toml`. A module directory may
    /// well be named `target`.
    fn walk(&mut self, root: &Path, recursive: bool) -> Result<()> {
        if system(root) {
            return Ok(());
        }
        let mut stack: LPooled<Vec<PathBuf>> = LPooled::take();
        stack.push(root.to_path_buf());
        while let Some(dir) = stack.pop() {
            self.dir(dir.clone());
            let Ok(entries) = fs::read_dir(&dir) else { continue };
            for e in entries.flatten() {
                let name = e.file_name();
                let Ok(ft) = e.file_type() else { continue };
                if ft.is_dir() {
                    let path = e.path();
                    let skip = name.to_string_lossy().starts_with('.')
                        || path.join("CACHEDIR.TAG").exists()
                        || path.join("Cargo.toml").exists();
                    if recursive && !skip {
                        stack.push(path);
                    }
                // XCR claude for claude: [bug/low] WALK_LIMIT counts a symlink at
                // its own size: DirEntry::metadata doesn't follow links, but the
                // check above and Hashes::name do, so a package with a symlink to
                // a large file is sent whole while `walked` grows by a few bytes.
                // Use the length from the followed fs::metadata already taken one
                // line up. Symlinked directories are never walked (file_type says
                // symlink, the followed metadata says not a file); a test should
                // say whether that is intended. (remote-02)
                // 2026-11-01 claude: The walk counts the length from the followed
                // fs::metadata it already takes. Not walking symlinked
                // directories is kept on purpose (a link may lead anywhere, even
                // into a cycle; dep-info still sends what rustc read through
                // one), and the test says so. Test:
                // remote::test::a_symlink_counts_at_its_files_size_and_a_symlinked_directory_is_not_walked
                // (a symlink to a sparse file past WALK_LIMIT must fail the walk;
                // it passed without the fix).
                } else if let Ok(m) = fs::metadata(e.path())
                    && m.is_file()
                {
                    self.walked += m.len();
                    if self.walked > WALK_LIMIT {
                        bail!("{} holds more than {} MB", root.display(), WALK_LIMIT >> 20);
                    }
                    self.file(e.path())?;
                }
            }
        }
        Ok(())
    }
}

/// What a job reads: the files its last dep-info lists, everything in its
/// package, since a new module may be among it and proc macros read what
/// they like there (its `Cargo.toml`, its `build.rs`) without dep-info
/// listing it, its build script's output, native library directories, and
/// `crates`, the crate files below it.
pub fn stage(job: &Job, crates: &[ArcStr], hashes: &Hashes) -> Result<Staged> {
    let cwd = PathBuf::from(job.cwd.as_str());
    let at = |p: &str| absolute(&cwd, p);
    let mut c = Collect::new(hashes);
    c.dir(cwd.clone());
    let manifest = env(job, "CARGO_MANIFEST_DIR").map(at);
    if let Ok(text) = fs::read_to_string(at(&job.paths.dep_info)) {
        for dep in dep_info(&text) {
            let path = at(&dep);
            // The last compile read it; this one can't if it is gone.
            if fs::metadata(&path).is_err_and(|e| e.kind() == ErrorKind::NotFound) {
                continue;
            }
            c.file(path)?;
        }
    }
    c.walk(manifest.as_ref().context("no CARGO_MANIFEST_DIR")?, true)?;
    for src in &job.paths.sources {
        c.file(at(src))?;
    }
    if let Some(out) = env(job, "OUT_DIR") {
        c.walk(&at(out), true)?;
    }
    for dir in &job.paths.search {
        c.walk(&at(dir), false)?;
    }
    for file in crates {
        c.krate(at(file), &job.reads)?;
    }
    let out_dir = at(&job.paths.out_dir);
    let mut outputs = vec![out_dir.clone()];
    c.dir(out_dir);
    for o in &job.paths.outputs {
        let o = at(o);
        if let Some(dir) = o.parent() {
            c.dir(dir.to_path_buf());
            outputs.push(dir.to_path_buf());
        }
    }
    if let Some(m) = manifest {
        c.dir(m);
    }
    let dirs = c.dirs.iter().map(|d| utf8(d)).collect::<Result<_>>()?;
    let late = match &job.reads {
        Reads::RlibsToLink { linker } if !c.rlibs.is_empty() => Some(Late { linker: linker.clone(), rlibs: c.rlibs }),
        _ => None,
    };
    Ok(Staged { files: c.files, dirs, late, outputs, blobs: c.blobs, hashed: c.hashed })
}

pub enum Event {
    /// The server took the job.
    Accepted { version: u64 },
    /// An artifact line, sent once the file it names is in place.
    Artifact(Bytes),
    Stderr(Bytes),
    Stdout(Bytes),
}

pub enum Outcome {
    Busy { version: u64 },
    Exit { code: i32, ran: Ran, measured: Measured, moved: Moved },
}

/// How rustc ended on the server, and the crate files it built there, by
/// hash with their size in kB.
struct Exited {
    code: i32,
    ran: Ran,
    linker_waited_us: u64,
    measured: Measured,
    built: Vec<(Hash, u64)>,
}

/// What moving a job's files took that compiling didn't.
pub struct Moved {
    /// Of inputs sent, and those bytes as sent.
    pub bytes: u64,
    pub wire_bytes: u64,
    /// How long the linker waited for rlibs still on their way.
    pub linker_waited_us: u64,
    /// The crate files the server holds after the job, by hash with their
    /// size in kB: its inputs, and the libraries it built.
    pub kept: Vec<(Hash, u64)>,
    pub steps: Steps,
}

/// How long each step of a sent job took, in µs: offering it, sending its
/// manifest, sending what rustc needs before it starts, and from asking
/// for rustc to its exit arriving, which takes in setting up the job,
/// rustc's run and its outputs.
#[derive(Clone, Copy)]
pub struct Steps {
    pub offer_us: u64,
    pub manifest_us: u64,
    pub send_us: u64,
    pub run_us: u64,
}

/// Send `m` and wait for the answer.
async fn ask(out: &Out, inbox: &mut In, m: ToServer) -> Result<FromServer> {
    out.send(m)?;
    out.flush().await?;
    inbox.recv().await
}

/// Run a staged job on the box `link` goes to, compressing what it sends
/// at `level`. `sink` gets rustc's output as it arrives. The hashes of the
/// outputs go in `hashes`.
pub async fn run(
    link: &Arc<Link>,
    offer: ToServer,
    job: &Job,
    staged: &Staged,
    hashes: &Hashes,
    level: i32,
    mut sink: impl FnMut(Event) -> Result<()>,
) -> Result<Outcome> {
    let (out, mut inbox) = link.open()?;
    let mut last = Instant::now();
    let mut lap = || {
        let (at, since) = (Instant::now(), last);
        last = at;
        at.duration_since(since).as_micros() as u64
    };
    match ask(&out, &mut inbox, offer).await.context("offering the job")? {
        FromServer::Accepted { version } => sink(Event::Accepted { version })?,
        FromServer::Busy { version } => return Ok(Outcome::Busy { version }),
        FromServer::Failed(why) => bail!("{why}"),
        m => bail!("unexpected {m:?}"),
    }
    let offer_us = lap();
    let manifest =
        ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone(), late: staged.late.clone() };
    let (now, later) = match ask(&out, &mut inbox, manifest).await.context("sending the manifest")? {
        FromServer::Need { now, later } => (now, later),
        FromServer::Failed(why) => bail!("{why}"),
        m => bail!("unexpected {m:?}"),
    };
    let manifest_us = lap();
    let asked: LPooled<HashSet<Hash>> = now.iter().chain(&later).copied().collect();
    let sent = Sent::default();

    for hash in now {
        send_blob(&out, staged, hash, level, &sent).await.context("sending inputs")?;
    }
    let send_us = lap();
    out.send(ToServer::Run { args: job.args.clone(), env: job.env.clone(), cwd: job.cwd.clone() })?;
    out.flush().await.context("starting rustc")?;
    // A rustc that fails before linking exits while late rlibs are still
    // on their way; its exit ends sending them, which comes once those
    // other jobs on the box wait for are in.
    let mut sending = pin!(async {
        for hash in later {
            send_blob(&out, staged, hash, level, &sent).await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let mut receiving = pin!(receive(&mut inbox, &link.jobs, staged, hashes, &mut sink));
    let mut sending_done = false;
    let Exited { code, ran, linker_waited_us, measured, built } = loop {
        tokio::select! {
            exit = &mut receiving => break exit.context("receiving outputs")?,
            r = &mut sending, if !sending_done => {
                r.context("sending late rlibs")?;
                sending_done = true;
            }
        }
    };
    let kept = staged.crates(&asked).chain(built).collect();
    let (bytes, wire_bytes) = (sent.raw.load(Ordering::Relaxed), sent.wire.load(Ordering::Relaxed));
    let steps = Steps { offer_us, manifest_us, send_us, run_us: lap() };
    let moved = Moved { bytes, wire_bytes, linker_waited_us, kept, steps };
    Ok(Outcome::Exit { code, ran, measured, moved })
}

/// Send `kb` of this program's own code, which compresses about as object
/// files do, to the box listening at `jobs`, compressed at `level`, and
/// return how many µs it took to be answered.
pub async fn heartbeat(links: &Links, jobs: &NPath, kb: u64, level: i32) -> Result<u64> {
    let exe = File::open("/proc/self/exe").context("opening this program")?;
    let mut data = Vec::with_capacity(CHUNK);
    exe.take((kb * 1024).min(CHUNK as u64)).read_to_end(&mut data)?;
    let data = Packed::pack(data.into(), level);
    let (out, mut inbox) = links.get(jobs).await?.open()?;
    let start = Instant::now();
    match ask(&out, &mut inbox, ToServer::Heartbeat(data)).await? {
        FromServer::Heartbeat => Ok(start.elapsed().as_micros() as u64),
        m => bail!("{jobs}: unexpected {m:?}"),
    }
}

/// Send the blob `hash` compressed at `level`, adding its size to `sent`.
pub async fn send_blob(out: &Out, staged: &Staged, hash: Hash, level: i32, sent: &Sent) -> Result<()> {
    let (path, _) = staged.blobs.get(&hash).with_context(|| format_compact!("server asked for unknown {hash}"))?;
    let file = File::open(path.as_str()).with_context(|| format_compact!("opening {path}"))?;
    let mut chunks = pin!(pack_file(file, level)?);
    while let Some(chunk) = chunks.next().await {
        let (raw, data, last) = chunk.with_context(|| format_compact!("reading {path}"))?;
        sent.wire.fetch_add(data.wire_len() as u64, Ordering::Relaxed);
        out.send(ToServer::Chunk { hash, data, last })?;
        out.flush().await?;
        sent.raw.fetch_add(raw.len() as u64, Ordering::Relaxed);
    }
    Ok(())
}

// XCR claude for claude: [bug/medium] A partly written output is never
// removed. remote::receive returns early on any error while a file is open
// (the link failing mid-file, Packed::unpack or write_all failing, a Failed
// or unexpected message), and Writing has no Drop, so `<stem>.rustc-dist-<pid>`
// stays in the user's --out-dir. Cargo never cleans unknown files there, the
// name changes with every daemon restart, and on the lanes target dirs are on
// a RAM-backed tmpfs, so a link that drops during a 338 MB rlib leaves 338 MB
// of RAM used until someone notices. Fix: a Drop on Writing that removes `tmp`
// unless the rename happened (e.g. take the path out on success). (remote-01)
// 2026-11-01 claude: Writing now has create/write/finish and a Drop that
// removes `tmp` unless finish renamed it (tmp is emptied then), so any early
// return from remote::receive leaves nothing in --out-dir. Test:
// remote::test::an_output_cut_short_leaves_nothing_behind (fails without the
// Drop).
/// An output arriving in chunks, written beside its destination, which it
/// replaces once whole. Dropped before then, it is removed.
struct Writing {
    dest: PathBuf,
    /// Empty once renamed to `dest`.
    tmp: PathBuf,
    file: File,
    len: u64,
}

impl Writing {
    fn create(dest: PathBuf) -> Result<Self> {
        let tmp = dest.with_extension(format_compact!("rustc-dist-{}", std::process::id()).as_str());
        let file = File::create(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?;
        Ok(Self { dest, tmp, file, len: 0 })
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data).with_context(|| format_compact!("writing {}", self.tmp.display()))?;
        self.len += data.len() as u64;
        Ok(())
    }

    /// Put the whole file at its destination with `mode`.
    fn finish(mut self, mode: u32) -> Result<()> {
        self.file.set_permissions(fs::Permissions::from_mode(mode))?;
        fs::rename(&self.tmp, &self.dest).with_context(|| format_compact!("writing {}", self.dest.display()))?;
        self.tmp = PathBuf::new();
        Ok(())
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        if !self.tmp.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.tmp);
        }
    }
}

/// rustc's output until it exits.
async fn receive(
    inbox: &mut In,
    jobs: &NPath,
    staged: &Staged,
    hashes: &Hashes,
    sink: &mut impl FnMut(Event) -> Result<()>,
) -> Result<Exited> {
    let mut writing: Option<Writing> = None;
    let mut built = Vec::new();
    loop {
        match inbox.recv().await? {
            FromServer::Output { path, mode, data, part } => {
                let data = data.unpack()?;
                let w = match &mut writing {
                    Some(w) => w,
                    None => {
                        let dest = PathBuf::from(path.as_str());
                        if !staged.outputs.iter().any(|o| dest.parent() == Some(o.as_path())) {
                            bail!("{jobs} sent {path}, outside the job's output directories");
                        }
                        writing.insert(Writing::create(dest)?)
                    }
                };
                w.write(&data)?;
                if let Part::End(kept) = part {
                    let w = writing.take().expect("open");
                    let (dest, len) = (w.dest.clone(), w.len);
                    w.finish(mode)?;
                    if let Some(hash) = kept {
                        hashes.insert(&dest, hash)?;
                        built.push((hash, len.div_ceil(1024)));
                    }
                }
            }
            FromServer::Stderr(line) => {
                let artifact = protocol::is_artifact_line(&line);
                sink(if artifact { Event::Artifact(line) } else { Event::Stderr(line) })?;
            }
            FromServer::Stdout(data) => sink(Event::Stdout(data))?,
            FromServer::Exit { code, ran, linker_waited_us, measured } => {
                return Ok(Exited { code, ran, linker_waited_us, measured, built });
            }
            FromServer::Failed(why) => bail!("{jobs}: {why}"),
            m => bail!("{jobs}: unexpected {m:?}"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::argv::{self, Invocation};

    #[test]
    fn a_chunk_is_compressed_only_when_that_makes_it_smaller() -> Result<()> {
        let text: Vec<u8> = (0..CHUNK).map(|i| b"fn main() {}\n"[i % 13]).collect();
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let noise: Vec<u8> = (0..CHUNK)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let (text, noise) = (Bytes::from(text), Bytes::from(noise));
        for (data, level, zstd) in [(&text, 1, true), (&text, 0, false), (&noise, 1, false), (&noise, 3, false)] {
            let packed = Packed::pack(data.clone(), level);
            assert_eq!(matches!(packed, Packed::Zstd(_)), zstd, "level {level}");
            assert!(packed.wire_len() <= data.len());
            assert_eq!(packed.unpack()?, data);
        }
        assert!(Packed::pack(text, 1).wire_len() < CHUNK / 100);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_is_packed_in_order_ending_with_a_short_chunk() -> Result<()> {
        let path = std::env::temp_dir().join(format!("rustc-dist-pack-{}", std::process::id()));
        for len in [0, 1, CHUNK, 3 * CHUNK + 5] {
            let data: Vec<u8> = (0..len).map(|i| (i / 7 % 251) as u8).collect();
            fs::write(&path, &data)?;
            let mut got = Vec::new();
            let mut lasts = Vec::new();
            let mut chunks = pin!(pack_file(File::open(&path)?, 1)?);
            while let Some(chunk) = chunks.next().await {
                let (raw, packed, last) = chunk?;
                assert_eq!(packed.unpack()?, raw);
                assert!(raw.len() <= CHUNK && (raw.len() < CHUNK) == last, "{len}: {} bytes", raw.len());
                got.extend_from_slice(&raw);
                lasts.push(last);
            }
            assert_eq!(got, data, "{len}");
            assert_eq!(lasts.len(), len / CHUNK + 1, "{len}");
            assert_eq!(lasts.iter().filter(|l| **l).count(), 1, "{len}");
        }
        fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn a_chunk_that_unpacks_past_a_chunk_is_refused() {
        let big = vec![0u8; CHUNK + 1];
        let z = zstd::bulk::compress(&big, 1).unwrap();
        assert!(Packed::Zstd(z.into()).unpack().is_err());
    }

    #[test]
    fn absolute_resolves_dots() {
        assert_eq!(absolute(Path::new("/w/ws"), "a/src/lib.rs"), PathBuf::from("/w/ws/a/src/lib.rs"));
        assert_eq!(absolute(Path::new("/w/ws"), "../x/./y.rs"), PathBuf::from("/w/x/y.rs"));
        assert_eq!(absolute(Path::new("/w"), "/abs/z"), PathBuf::from("/abs/z"));
    }

    #[test]
    fn walks_module_dirs_named_target_but_not_cargo_output_or_other_packages() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-walk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        for d in ["src/target", "target/release", ".git", "member/src"] {
            fs::create_dir_all(dir.join(d))?;
        }
        let files = [
            "src/lib.rs", "src/target/apple.rs", "target/CACHEDIR.TAG", "target/release/big", ".git/HEAD",
            "member/Cargo.toml", "member/src/lib.rs",
        ];
        for f in files {
            fs::write(dir.join(f), "x")?;
        }
        let hashes = Hashes::default();
        let mut c = Collect::new(&hashes);
        c.walk(&dir, true)?;
        let mut got: Vec<String> =
            c.files.iter().map(|f| f.path.trim_start_matches(dir.to_str().unwrap()).to_string()).collect();
        got.sort();
        assert_eq!(got, ["/src/lib.rs", "/src/target/apple.rs"]);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_symlink_counts_at_its_files_size_and_a_symlinked_directory_is_not_walked() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-walk-links-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        for d in ["pkg/src", "elsewhere/dir"] {
            fs::create_dir_all(dir.join(d))?;
        }
        fs::write(dir.join("pkg/src/lib.rs"), "x")?;
        fs::write(dir.join("elsewhere/dir/a.rs"), "x")?;
        fs::write(dir.join("elsewhere/small.txt"), "small")?;
        std::os::unix::fs::symlink(dir.join("elsewhere/dir"), dir.join("pkg/src/linked"))?;
        std::os::unix::fs::symlink(dir.join("elsewhere/small.txt"), dir.join("pkg/small.txt"))?;
        let hashes = Hashes::default();
        let mut c = Collect::new(&hashes);
        c.walk(&dir.join("pkg"), true)?;
        let mut got: Vec<&str> = c.files.iter().filter_map(|f| f.path.rsplit_once("/pkg/")).map(|(_, p)| p).collect();
        got.sort();
        assert_eq!(got, ["small.txt", "src/lib.rs"], "a linked directory may lead anywhere, so only dep-info sends it");
        assert_eq!(c.walked, 6);
        // Past the limit through a link, though the link itself is small.
        let big = File::create(dir.join("elsewhere/big"))?;
        big.set_len(WALK_LIMIT + 1)?;
        std::os::unix::fs::symlink(dir.join("elsewhere/big"), dir.join("pkg/big"))?;
        let mut c = Collect::new(&hashes);
        assert!(c.walk(&dir.join("pkg"), true).is_err(), "walked {}", c.walked);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn an_output_cut_short_leaves_nothing_behind() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-writing-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        let mut w = Writing::create(dir.join("liba.rlib"))?;
        w.write(b"half an rlib")?;
        drop(w);
        assert_eq!(fs::read_dir(&dir)?.count(), 0, "the partial file is gone");
        let mut w = Writing::create(dir.join("liba.rlib"))?;
        w.write(b"a whole rlib")?;
        w.finish(0o644)?;
        let left: Vec<_> = fs::read_dir(&dir)?.flatten().map(|e| e.file_name()).collect();
        assert_eq!(left, ["liba.rlib"]);
        assert_eq!(fs::read(dir.join("liba.rlib"))?, b"a whole rlib");
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_server_holds_a_late_rlibs_metadata_only_if_it_asked_for_one_of_them() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-held-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        let mut blobs = HashMap::new();
        let mut file = |name: &str, kb: usize| -> Result<(ArcStr, Hash)> {
            let path = dir.join(name);
            fs::write(&path, vec![name.as_bytes()[3]; kb * 1024])?;
            let hash = Hash::of(&fs::read(&path)?);
            let path = utf8(&path)?;
            blobs.insert(hash, (path.clone(), (kb * 1024) as u64));
            Ok((path, hash))
        };
        let (a, a_hash) = file("liba.rlib", 1)?;
        let (_, src) = file("lib.rs", 1)?;
        let [b, c] = [("libb", 4), ("libc", 8)].map(|(name, kb)| -> Result<Rlib> {
            let (path, hash) = file(&format!("{name}.rlib"), kb)?;
            let (_, meta) = file(&format!("{name}.rmeta"), 1)?;
            Ok(Rlib { path, hash, meta })
        });
        let (b, c) = (b?, c?);
        let files = vec![FileEntry { path: a, hash: a_hash }, FileEntry { path: "/x/src/lib.rs".into(), hash: src }];
        let late = Some(Late { linker: "cc".into(), rlibs: vec![b.clone(), c.clone()] });
        let staged = Staged { files, dirs: vec![], late, outputs: vec![], blobs, hashed: (0, 0) };
        // The files are gone: the sizes are those staging took.
        fs::remove_dir_all(&dir)?;
        // It lacked b, and had c.
        let asked: HashSet<Hash> = [b.hash].into_iter().collect();
        let held: Vec<_> = staged.crates(&asked).collect();
        assert_eq!(held, [(a_hash, 1), (b.hash, 4), (b.meta, 1), (c.hash, 8)]);
        Ok(())
    }

    #[test]
    fn crates_a_job_links_come_late_when_metadata_can_stand_in() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-krate-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        for f in ["libx.rlib", "libx.rmeta", "liby.rlib", "libz.so"] {
            fs::write(dir.join(f), f)?;
        }
        let hashes = Hashes::default();
        let stage = |reads: &Reads| -> Result<Collect<'_>> {
            let mut c = Collect::new(&hashes);
            for f in ["libx.rmeta", "liby.rlib", "libz.so"] {
                c.krate(dir.join(f), reads)?;
            }
            Ok(c)
        };
        let names = |c: &Collect| -> Vec<String> {
            c.files.iter().map(|f| f.path.rsplit('/').next().unwrap().to_string()).collect()
        };
        let link = stage(&Reads::RlibsToLink { linker: "cc".into() })?;
        assert_eq!(names(&link), ["liby.rlib", "libz.so"], "y has no metadata to stand in for it");
        let [x] = &link.rlibs[..] else { panic!("{:?}", link.rlibs) };
        assert!(x.path.ends_with("/libx.rlib"));
        assert_eq!(x.meta, hashes.get(&dir.join("libx.rmeta"))?.0);
        assert_eq!(link.blobs[&x.meta].0.as_str(), dir.join("libx.rmeta").to_str().unwrap(), "meta is sent from its file");
        let rlibs = stage(&Reads::Rlibs)?;
        assert_eq!(names(&rlibs), ["libx.rlib", "liby.rlib", "libz.so"]);
        assert!(rlibs.rlibs.is_empty());
        assert_eq!(names(&stage(&Reads::Metadata)?), ["libx.rmeta", "liby.rlib", "libz.so"]);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_source_gone_since_the_last_compile_is_not_sent() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-gone-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("a/src"))?;
        fs::create_dir_all(dir.join("target/deps"))?;
        fs::write(dir.join("a/src/lib.rs"), "pub fn f() {}\n")?;
        let deps = dir.join("target/deps");
        fs::write(deps.join("a-x1.d"), format!("{0}/liba-x1.rmeta: a/src/lib.rs a/src/gone.rs\n", deps.display()))?;
        let args = [
            "rustc", "--crate-name", "a", "--crate-type", "lib", "--edition", "2021", "a/src/lib.rs",
            "--emit=dep-info,metadata,link", "-C", "extra-filename=-x1", "--out-dir", deps.to_str().unwrap(),
        ];
        let Invocation::Compile(c) = argv::classify(&args, None) else { panic!("not a compile") };
        let portable = c.portable.expect("portable");
        let job = Job {
            rustc: "rustc".into(),
            args: args[1..].iter().map(|a| ArcStr::from(*a)).collect(),
            env: vec![("CARGO_MANIFEST_DIR".into(), ArcStr::from(dir.join("a").to_str().unwrap()))],
            cwd: ArcStr::from(dir.to_str().unwrap()),
            target: None,
            paths: portable.paths,
            reads: portable.reads,
        };
        let staged = stage(&job, &[], &Hashes::default())?;
        let names: Vec<&str> = staged.files.iter().filter_map(|f| f.path.rsplit_once('/')).map(|(_, n)| n).collect();
        assert_eq!(names, ["lib.rs"]);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_whole_package_is_sent_since_proc_macros_read_what_they_like_there() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-manifest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("a/src"))?;
        fs::create_dir_all(dir.join("target/deps"))?;
        fs::write(dir.join("a/src/lib.rs"), "pub fn f() {}\n")?;
        fs::write(dir.join("a/Cargo.toml"), "[package]\nname = \"a\"\n")?;
        fs::write(dir.join("a/build.rs"), "fn main() {}\n")?;
        let deps = dir.join("target/deps");
        fs::write(deps.join("a-x1.d"), format!("{0}/liba-x1.rmeta: a/src/lib.rs\n", deps.display()))?;
        let args = [
            "rustc", "--crate-name", "a", "--crate-type", "lib", "--edition", "2021", "a/src/lib.rs",
            "--emit=dep-info,metadata,link", "-C", "extra-filename=-x1", "--out-dir", deps.to_str().unwrap(),
        ];
        let Invocation::Compile(c) = argv::classify(&args, None) else { panic!("not a compile") };
        let portable = c.portable.expect("portable");
        let job = Job {
            rustc: "rustc".into(),
            args: args[1..].iter().map(|a| ArcStr::from(*a)).collect(),
            env: vec![("CARGO_MANIFEST_DIR".into(), ArcStr::from(dir.join("a").to_str().unwrap()))],
            cwd: ArcStr::from(dir.to_str().unwrap()),
            target: None,
            paths: portable.paths,
            reads: portable.reads,
        };
        let staged = stage(&job, &[], &Hashes::default())?;
        let mut names: Vec<&str> = staged.files.iter().filter_map(|f| f.path.rsplit_once('/')).map(|(_, n)| n).collect();
        names.sort();
        assert_eq!(names, ["Cargo.toml", "build.rs", "lib.rs"]);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn system_paths_are_never_sent() {
        assert!(system(Path::new("/usr/lib/libgssapi_krb5.so")));
        assert!(system(Path::new("/lib64")));
        assert!(!system(Path::new("/home/eric/.cargo/registry/src/x/lib.rs")));
        assert!(!system(Path::new("/tmp/usr/lib")));
    }

    #[test]
    fn parses_dep_info() {
        let text = "/o/libdemo-abc.rmeta: src/lib.rs src/data\\ file.txt\n\nsrc/lib.rs:\n# env-dep:X=1\n";
        assert_eq!(dep_info(text), ["src/lib.rs", "src/data file.txt"]);
    }
}
