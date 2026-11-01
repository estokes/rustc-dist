use crate::{
    blobs::{Hash, Hashes},
    protocol::{self, Job, Ran, Reads, Unit},
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use compact_str::format_compact;
use netidx::{path::Path as NPath, subscriber::Subscriber};
use netidx_derive::Pack;
use netidx_protocols::pack_channel::client::Connection;
use poolshark::local::LPooled;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{ErrorKind, Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
    pin::pin,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time;

/// Bytes per message when a file crosses the wire.
pub const CHUNK: usize = 1 << 20;
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
    Chunk { hash: Hash, data: Bytes, last: bool },
    Run { args: Vec<ArcStr>, env: Vec<(ArcStr, ArcStr)>, cwd: ArcStr },
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
    Output { path: ArcStr, mode: u32, data: Bytes, last: bool },
    /// `ran` is the server's measurement, whatever the code, and
    /// `linker_waited_us` how long its linker waited for late rlibs.
    Exit { code: i32, ran: Ran, linker_waited_us: u64 },
    Failed(ArcStr),
}

/// The files a job reads and the directories it needs, at their absolute
/// client paths, and where it may write.
pub struct Staged {
    pub files: Vec<FileEntry>,
    pub dirs: Vec<ArcStr>,
    pub late: Option<Late>,
    pub outputs: Vec<PathBuf>,
    /// Where to read each blob the server may ask for.
    blobs: HashMap<Hash, ArcStr>,
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

struct Collect<'a> {
    hashes: &'a Hashes,
    files: Vec<FileEntry>,
    rlibs: Vec<Rlib>,
    blobs: HashMap<Hash, ArcStr>,
    dirs: HashSet<PathBuf>,
    seen: HashSet<PathBuf>,
    walked: u64,
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
        }
    }

    fn blob(&mut self, path: &Path) -> Result<(ArcStr, Hash)> {
        let s = utf8(path)?;
        let (hash, _) = self.hashes.get(path)?;
        self.blobs.insert(hash, s.clone());
        Ok((s, hash))
    }

    fn file(&mut self, path: PathBuf) -> Result<()> {
        if system(&path) || !self.seen.insert(path.clone()) {
            return Ok(());
        }
        let (path, hash) = self.blob(&path)?;
        self.files.push(FileEntry { path, hash });
        Ok(())
    }

    /// A crate below the job, named by its `.rmeta`, `.rlib` or `.so`. A
    /// job that links reads the rlib, which may come late if there is
    /// metadata to stand in for it.
    fn krate(&mut self, path: PathBuf, reads: &Reads) -> Result<()> {
        let rlib = match path.extension() {
            Some(e) if e == "rmeta" || e == "rlib" => path.with_extension("rlib"),
            _ => return self.file(path),
        };
        match reads {
            Reads::Metadata => self.file(path),
            Reads::Rlibs => self.file(rlib),
            Reads::RlibsToLink { .. } => {
                let meta = rlib.with_extension("rmeta");
                if !meta.is_file() {
                    return self.file(rlib);
                }
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

    /// Every file under `root`, skipping hidden directories and cargo's
    /// output directories, which cargo marks with `CACHEDIR.TAG`. A module
    /// directory may well be named `target`.
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
                    let skip = name.to_string_lossy().starts_with('.') || path.join("CACHEDIR.TAG").exists();
                    if recursive && !skip {
                        stack.push(path);
                    }
                } else if fs::metadata(e.path()).is_ok_and(|m| m.is_file()) {
                    self.walked += e.metadata().map(|m| m.len()).unwrap_or(0);
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

/// What a job reads: its sources (the files its last dep-info lists, and
/// everything in the directories they came from, so a new module is
/// found), its build script's output, native library directories, and
/// `crates`, the crate files below it.
pub fn stage(job: &Job, crates: &[ArcStr], hashes: &Hashes) -> Result<Staged> {
    let cwd = PathBuf::from(job.cwd.as_str());
    let at = |p: &str| absolute(&cwd, p);
    let mut c = Collect::new(hashes);
    c.dir(cwd.clone());
    let manifest = env(job, "CARGO_MANIFEST_DIR").map(at);
    match fs::read_to_string(at(&job.paths.dep_info)) {
        Ok(text) => {
            let mut source_dirs: LPooled<HashSet<PathBuf>> = LPooled::take();
            for dep in dep_info(&text) {
                let path = at(&dep);
                // The last compile read it; this one can't if it is gone.
                if fs::metadata(&path).is_err_and(|e| e.kind() == ErrorKind::NotFound) {
                    continue;
                }
                if let Some(dir) = path.parent()
                    && manifest.as_ref().is_some_and(|m| dir.starts_with(m))
                {
                    source_dirs.insert(dir.to_path_buf());
                }
                c.file(path)?;
            }
            for dir in source_dirs.iter() {
                c.walk(dir, true)?;
            }
        }
        Err(_) => {
            let root = manifest.clone().context("no CARGO_MANIFEST_DIR")?;
            c.walk(&root, true)?;
        }
    }
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
    Ok(Staged { files: c.files, dirs, late, outputs, blobs: c.blobs })
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
    Exit { code: i32, ran: Ran, moved: Moved },
}

/// What moving a job's files took that compiling didn't.
pub struct Moved {
    /// Of inputs sent.
    pub bytes: u64,
    /// How long the linker waited for rlibs still on their way.
    pub linker_waited_us: u64,
}

/// Run a staged job on the box listening at `jobs`. `sink` gets rustc's
/// output as it arrives. The hashes of the outputs go in `hashes`.
pub async fn run(
    subscriber: &Subscriber,
    jobs: NPath,
    offer: ToServer,
    job: &Job,
    staged: &Staged,
    hashes: &Hashes,
    mut sink: impl FnMut(Event) -> Result<()>,
) -> Result<Outcome> {
    let con = match time::timeout(Duration::from_secs(10), Connection::connect(subscriber, jobs.clone())).await {
        Ok(con) => con.context("connecting")?,
        Err(_) => bail!("connecting timed out"),
    };
    let ask = async |m: &ToServer| {
        con.send_one(m)?;
        con.flush().await?;
        con.recv_one::<FromServer>().await
    };
    match ask(&offer).await.context("offering the job")? {
        FromServer::Accepted { version } => sink(Event::Accepted { version })?,
        FromServer::Busy { version } => return Ok(Outcome::Busy { version }),
        FromServer::Failed(why) => bail!("{why}"),
        m => bail!("unexpected {m:?}"),
    }
    let manifest =
        ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone(), late: staged.late.clone() };
    let (now, later) = match ask(&manifest).await.context("sending the manifest")? {
        FromServer::Need { now, later } => (now, later),
        FromServer::Failed(why) => bail!("{why}"),
        m => bail!("unexpected {m:?}"),
    };
    let sent = AtomicU64::new(0);
    let mut buf = vec![0u8; CHUNK];
    for hash in now {
        send_blob(&con, staged, hash, &mut buf, &sent).await.context("sending inputs")?;
    }
    con.send_one(&ToServer::Run { args: job.args.clone(), env: job.env.clone(), cwd: job.cwd.clone() })?;
    con.flush().await.context("starting rustc")?;
    let mut sending = pin!(async {
        for hash in later {
            send_blob(&con, staged, hash, &mut buf, &sent).await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let mut receiving = pin!(receive(&con, &jobs, staged, hashes, &mut sink));
    let mut sending_done = false;
    let (code, ran, linker_waited_us) = loop {
        tokio::select! {
            exit = &mut receiving => break exit.context("receiving outputs")?,
            r = &mut sending, if !sending_done => {
                // A rustc that fails before linking exits without the late
                // rlibs, and the server closes the channel after its exit,
                // which is still on its way.
                if let Err(e) = r
                    && !con.is_dead()
                {
                    return Err(e.context("sending late rlibs"));
                }
                sending_done = true;
            }
        }
    };
    Ok(Outcome::Exit { code, ran, moved: Moved { bytes: sent.load(Ordering::Relaxed), linker_waited_us } })
}

/// Send the blob `hash`, adding its size to `sent`.
pub async fn send_blob(con: &Connection, staged: &Staged, hash: Hash, buf: &mut [u8], sent: &AtomicU64) -> Result<()> {
    let path = staged.blobs.get(&hash).with_context(|| format_compact!("server asked for unknown {hash}"))?;
    let mut file = File::open(path.as_str()).with_context(|| format_compact!("opening {path}"))?;
    loop {
        let n = file.read(buf)?;
        let last = n < buf.len();
        con.send_one(&ToServer::Chunk { hash, data: Bytes::copy_from_slice(&buf[..n]), last })?;
        con.flush().await?;
        sent.fetch_add(n as u64, Ordering::Relaxed);
        if last {
            return Ok(());
        }
    }
}

/// An output arriving in chunks, written beside its destination.
struct Writing {
    dest: PathBuf,
    tmp: PathBuf,
    file: File,
    hasher: blake3::Hasher,
}

/// The server's next message. A send that finds the channel closed marks
/// it dead, and `recv_one` then fails without reading what the server sent
/// before closing it, which `try_recv_one` still returns.
async fn next(con: &Connection) -> Result<FromServer> {
    match con.recv_one().await {
        Err(e) if con.is_dead() => con.try_recv_one().await?.ok_or(e),
        r => r,
    }
}

/// rustc's output until it exits: its code, the server's measurement and
/// how long its linker waited.
async fn receive(
    con: &Connection,
    jobs: &NPath,
    staged: &Staged,
    hashes: &Hashes,
    sink: &mut impl FnMut(Event) -> Result<()>,
) -> Result<(i32, Ran, u64)> {
    let mut writing: Option<Writing> = None;
    loop {
        match next(con).await? {
            FromServer::Output { path, mode, data, last } => {
                if writing.is_none() {
                    let dest = PathBuf::from(path.as_str());
                    if !staged.outputs.iter().any(|o| dest.parent() == Some(o.as_path())) {
                        bail!("{jobs} sent {path}, outside the job's output directories");
                    }
                    let tmp = dest.with_extension(format_compact!("rustc-dist-{}", std::process::id()).as_str());
                    let file = File::create(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?;
                    writing = Some(Writing { dest, tmp, file, hasher: blake3::Hasher::new() });
                }
                let w = writing.as_mut().ok_or_else(|| anyhow!("no output open"))?;
                w.file.write_all(&data)?;
                w.hasher.update(&data);
                if last {
                    let w = writing.take().expect("open");
                    w.file.set_permissions(fs::Permissions::from_mode(mode))?;
                    fs::rename(&w.tmp, &w.dest).with_context(|| format_compact!("writing {}", w.dest.display()))?;
                    hashes.insert(&w.dest, Hash(*w.hasher.finalize().as_bytes()))?;
                }
            }
            FromServer::Stderr(line) => {
                let artifact = protocol::is_artifact_line(&line);
                sink(if artifact { Event::Artifact(line) } else { Event::Stderr(line) })?;
            }
            FromServer::Stdout(data) => sink(Event::Stdout(data))?,
            FromServer::Exit { code, ran, linker_waited_us } => return Ok((code, ran, linker_waited_us)),
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
    fn absolute_resolves_dots() {
        assert_eq!(absolute(Path::new("/w/ws"), "a/src/lib.rs"), PathBuf::from("/w/ws/a/src/lib.rs"));
        assert_eq!(absolute(Path::new("/w/ws"), "../x/./y.rs"), PathBuf::from("/w/x/y.rs"));
        assert_eq!(absolute(Path::new("/w"), "/abs/z"), PathBuf::from("/abs/z"));
    }

    #[test]
    fn walks_module_dirs_named_target_but_not_cargo_output() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-walk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        for d in ["src/target", "target/release", ".git"] {
            fs::create_dir_all(dir.join(d))?;
        }
        for f in ["src/lib.rs", "src/target/apple.rs", "target/CACHEDIR.TAG", "target/release/big", ".git/HEAD"] {
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
        assert_eq!(link.blobs[&x.meta].as_str(), dir.join("libx.rmeta").to_str().unwrap(), "meta is sent from its file");
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
