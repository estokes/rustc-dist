use crate::{
    blobs::{Hash, Hashes},
    protocol::{Job, Unit},
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
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
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

#[derive(Debug, Pack)]
pub enum ToServer {
    /// Run `unit` in `class` with the toolchain `key`.
    Offer { unit: Unit, key: ArcStr, target: Option<ArcStr>, class: u32 },
    Manifest { files: Vec<FileEntry>, dirs: Vec<ArcStr> },
    Chunk { hash: Hash, data: Bytes, last: bool },
    Run { args: Vec<ArcStr>, env: Vec<(ArcStr, ArcStr)>, cwd: ArcStr },
}

#[derive(Debug, Pack)]
pub enum FromServer {
    /// `version` is the first version of the server's published state
    /// that reflects the answer.
    Accepted { version: u64 },
    Busy { version: u64 },
    /// The blobs the server lacks.
    Need(Vec<Hash>),
    Stdout(Bytes),
    /// One line of rustc's stderr. An artifact line comes after the file
    /// it names.
    Stderr(Bytes),
    Output { path: ArcStr, mode: u32, data: Bytes, last: bool },
    Exit { code: i32, cpu_us: u64, wall_us: u64, maxrss_kb: u64 },
    Failed(ArcStr),
}

/// The files a job reads and the directories it needs, at their absolute
/// client paths, and where it may write.
pub struct Staged {
    pub files: Vec<FileEntry>,
    pub dirs: Vec<ArcStr>,
    pub outputs: Vec<PathBuf>,
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

struct Collect<'a> {
    hashes: &'a Hashes,
    files: Vec<FileEntry>,
    dirs: HashSet<PathBuf>,
    seen: HashSet<PathBuf>,
    walked: u64,
}

impl Collect<'_> {
    fn file(&mut self, path: PathBuf) -> Result<()> {
        if system(&path) || !self.seen.insert(path.clone()) {
            return Ok(());
        }
        let s = path.to_str().with_context(|| format_compact!("{} is not utf-8", path.display()))?;
        let (hash, _) = self.hashes.get(&path)?;
        self.files.push(FileEntry { path: ArcStr::from(s), hash });
        Ok(())
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
    let mut c = Collect { hashes, files: Vec::new(), dirs: HashSet::new(), seen: HashSet::new(), walked: 0 };
    c.dir(cwd.clone());
    let manifest = env(job, "CARGO_MANIFEST_DIR").map(at);
    match fs::read_to_string(at(&job.paths.dep_info)) {
        Ok(text) => {
            let mut source_dirs: LPooled<HashSet<PathBuf>> = LPooled::take();
            for dep in dep_info(&text) {
                let path = at(&dep);
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
        c.file(at(file))?;
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
    let dirs = c
        .dirs
        .into_iter()
        .map(|d| d.to_str().map(ArcStr::from).with_context(|| format_compact!("{} is not utf-8", d.display())))
        .collect::<Result<_>>()?;
    Ok(Staged { files: c.files, dirs, outputs })
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
    Exit { code: i32, cpu_us: u64, wall_us: u64, maxrss_kb: u64 },
}

/// Run a staged job on the box listening at `jobs`. `sink` gets rustc's
/// output as it arrives.
pub async fn run(
    subscriber: &Subscriber,
    jobs: NPath,
    offer: ToServer,
    job: &Job,
    staged: &Staged,
    mut sink: impl FnMut(Event) -> Result<()>,
) -> Result<Outcome> {
    let con = time::timeout(Duration::from_secs(10), Connection::connect(subscriber, jobs.clone()))
        .await
        .with_context(|| format_compact!("connecting to {jobs}"))??;
    con.send_one(&offer)?;
    con.flush().await?;
    match con.recv_one::<FromServer>().await? {
        FromServer::Accepted { version } => sink(Event::Accepted { version })?,
        FromServer::Busy { version } => return Ok(Outcome::Busy { version }),
        FromServer::Failed(why) => bail!("{jobs}: {why}"),
        m => bail!("{jobs}: unexpected {m:?}"),
    }
    con.send_one(&ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone() })?;
    con.flush().await?;
    let need = match con.recv_one::<FromServer>().await? {
        FromServer::Need(need) => need,
        FromServer::Failed(why) => bail!("{jobs}: {why}"),
        m => bail!("{jobs}: unexpected {m:?}"),
    };
    let by_hash: HashMap<Hash, &ArcStr> = staged.files.iter().map(|f| (f.hash, &f.path)).collect();
    let mut buf = vec![0u8; CHUNK];
    for hash in need {
        let path = by_hash.get(&hash).with_context(|| format_compact!("server asked for unknown {hash}"))?;
        let mut file = File::open(path.as_str()).with_context(|| format_compact!("opening {path}"))?;
        loop {
            let n = file.read(&mut buf)?;
            let last = n < buf.len();
            con.send_one(&ToServer::Chunk { hash, data: Bytes::copy_from_slice(&buf[..n]), last })?;
            con.flush().await?;
            if last {
                break;
            }
        }
    }
    con.send_one(&ToServer::Run { args: job.args.clone(), env: job.env.clone(), cwd: job.cwd.clone() })?;
    con.flush().await?;
    let mut writing: Option<(PathBuf, PathBuf, File)> = None;
    loop {
        match con.recv_one::<FromServer>().await? {
            FromServer::Output { path, mode, data, last } => {
                if writing.is_none() {
                    let dest = PathBuf::from(path.as_str());
                    if !staged.outputs.iter().any(|o| dest.parent() == Some(o.as_path())) {
                        bail!("{jobs} sent {path}, outside the job's output directories");
                    }
                    let tmp = dest.with_extension(format_compact!("rustc-dist-{}", std::process::id()).as_str());
                    let file = File::create(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?;
                    writing = Some((dest, tmp, file));
                }
                let (dest, tmp, file) = writing.as_mut().ok_or_else(|| anyhow!("no output open"))?;
                file.write_all(&data)?;
                if last {
                    file.set_permissions(fs::Permissions::from_mode(mode))?;
                    fs::rename(&*tmp, &*dest).with_context(|| format_compact!("writing {}", dest.display()))?;
                    writing = None;
                }
            }
            FromServer::Stderr(line) => {
                let artifact = line.windows(ARTIFACT.len()).any(|w| w == ARTIFACT);
                sink(if artifact { Event::Artifact(line) } else { Event::Stderr(line) })?;
            }
            FromServer::Stdout(data) => sink(Event::Stdout(data))?,
            FromServer::Exit { code, cpu_us, wall_us, maxrss_kb } => {
                return Ok(Outcome::Exit { code, cpu_us, wall_us, maxrss_kb });
            }
            FromServer::Failed(why) => bail!("{jobs}: {why}"),
            m => bail!("{jobs}: unexpected {m:?}"),
        }
    }
}

/// rustc's JSON marker for a line announcing a written file.
pub const ARTIFACT: &[u8] = br#""$message_type":"artifact""#;

#[cfg(test)]
mod test {
    use super::*;

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
        let mut c = Collect { hashes: &hashes, files: Vec::new(), dirs: HashSet::new(), seen: HashSet::new(), walked: 0 };
        c.walk(&dir, true)?;
        let mut got: Vec<String> =
            c.files.iter().map(|f| f.path.trim_start_matches(dir.to_str().unwrap()).to_string()).collect();
        got.sort();
        assert_eq!(got, ["/src/lib.rs", "/src/target/apple.rs"]);
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
