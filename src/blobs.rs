use anyhow::{Context, Result, anyhow, bail};
use bytes::{Buf, BufMut};
use compact_str::format_compact;
use log::{info, warn};
use netidx_core::pack::{Pack, PackError};
use parking_lot::Mutex;
use xxhash_rust::xxh3::Xxh3;
use poolshark::local::LPooled;
use std::{
    collections::{HashMap, hash_map::Entry},
    ffi::OsStr,
    fmt,
    fs::{self, File, Metadata},
    io::{self, ErrorKind, Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Instant, SystemTime},
};
use tokio::sync::watch;

/// A file's 128-bit xxh3 hash, the name of its content on every box. Boxes
/// trust each other, as netidx's authentication says who they are, so the
/// hash need only make chance collisions vanishingly rare, and it is as
/// fast as reading the file.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash(pub [u8; 16]);

impl Hash {
    #[cfg(test)]
    pub fn of(bytes: &[u8]) -> Self {
        let mut h = Hasher::default();
        h.update(bytes);
        h.finish()
    }

    /// The hash a blob's name in the store spells.
    fn from_hex(name: &[u8]) -> Option<Self> {
        if name.len() != 32 {
            return None;
        }
        let mut h = [0u8; 16];
        for (b, pair) in h.iter_mut().zip(name.chunks(2)) {
            *b = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
        }
        Some(Hash(h))
    }
}

/// A `Hash` of bytes as they come.
#[derive(Default)]
pub struct Hasher(Xxh3);

impl Hasher {
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub fn finish(&self) -> Hash {
        Hash(self.0.digest128().to_be_bytes())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Pack for Hash {
    fn encoded_len(&self) -> usize {
        16
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        buf.put_slice(&self.0);
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < 16 {
            return Err(PackError::BufferShort);
        }
        let mut h = [0u8; 16];
        buf.copy_to_slice(&mut h);
        Ok(Hash(h))
    }
}

/// Bytes of a file read at once to hash.
const HASH_BLOCK: usize = 1 << 20;

fn hash_file(path: &Path) -> Result<Hash> {
    let mut file = File::open(path).with_context(|| format_compact!("opening {}", path.display()))?;
    let len = file.metadata()?.len();
    let mut hasher = Hasher::default();
    let mut buf: LPooled<Vec<u8>> = LPooled::take();
    buf.resize((len as usize).clamp(1, HASH_BLOCK), 0);
    loop {
        match file.read(&mut buf) {
            Ok(0) => return Ok(hasher.finish()),
            Ok(n) => hasher.update(&buf[..n]),
            Err(e) if e.kind() == ErrorKind::Interrupted => (),
            Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Stamp {
    len: u64,
    mtime_ns: i64,
    ino: u64,
}

impl Stamp {
    fn of(meta: &Metadata) -> Self {
        Self { len: meta.len(), mtime_ns: meta.mtime() * 1_000_000_000 + meta.mtime_nsec(), ino: meta.ino() }
    }
}

/// A file's hash as of its stamp.
type Taken = (Stamp, Hash);

/// A file's hash once taken, locked while a job hashes it.
type Known = triomphe::Arc<Mutex<Option<Taken>>>;

/// What `Hashes::save` writes.
const HASHES_MAGIC: &[u8; 8] = b"rdhash01";

/// Hashes of the files jobs read, each kept until its file changes, and
/// saved, so a restart needn't read files again to name them. A job that
/// needs a file another job is hashing waits for that hash rather than
/// hash it too.
#[derive(Default)]
pub struct Hashes {
    known: Mutex<HashMap<PathBuf, Known>>,
    /// Some hash was taken since the last save.
    dirty: AtomicBool,
    /// Held while a save writes the table, so two saves never share its
    /// temporary file.
    saving: Mutex<()>,
}

impl Hashes {
    fn entry(&self, path: &Path) -> Known {
        let mut map = self.known.lock();
        match map.get(path) {
            Some(k) => k.clone(),
            None => map.entry(path.to_path_buf()).or_default().clone(),
        }
    }

    pub fn get(&self, path: &Path) -> Result<(Hash, u64)> {
        self.name(path).map(|n| (n.hash, n.len))
    }

    /// The file at `path` named, hashing it if it changed since it last was.
    pub fn name(&self, path: &Path) -> Result<Named> {
        let meta = fs::metadata(path).with_context(|| format_compact!("stat {}", path.display()))?;
        if !meta.is_file() {
            bail!("{} is not a file", path.display());
        }
        let stamp = Stamp::of(&meta);
        let known = self.entry(path);
        let mut known = known.lock();
        if let Some((s, hash)) = *known
            && s == stamp
        {
            return Ok(Named { hash, len: stamp.len, hashed_us: None });
        }
        let start = Instant::now();
        let hash = hash_file(path)?;
        *known = Some((stamp, hash));
        self.dirty.store(true, Ordering::Relaxed);
        Ok(Named { hash, len: stamp.len, hashed_us: Some(start.elapsed().as_micros() as u64) })
    }

    /// The size of the file at `path` and, if it was hashed since it last
    /// changed, its hash; none if there is no such file. It reads no file,
    /// and doesn't wait for a job hashing it.
    pub fn known(&self, path: &Path) -> Option<(Option<Hash>, u64)> {
        let meta = fs::metadata(path).ok().filter(|m| m.is_file())?;
        let stamp = Stamp::of(&meta);
        let known = self.known.lock().get(path).cloned();
        let hash = known.and_then(|k| *k.try_lock()?).filter(|(s, _)| *s == stamp).map(|(_, h)| h);
        Some((hash, stamp.len))
    }

    /// The hash of a file just written, so it needn't be read again.
    pub fn insert(&self, path: &Path, hash: Hash) -> Result<()> {
        let meta = fs::metadata(path).with_context(|| format_compact!("stat {}", path.display()))?;
        *self.entry(path).lock() = Some((Stamp::of(&meta), hash));
        self.dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// The hashes saved at `path`; none if nothing was.
    pub fn load(path: &Path) -> Result<Self> {
        let data = match fs::read(path) {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
        };
        let mut buf = &data[..];
        if buf.len() < 16 || &buf[..8] != HASHES_MAGIC {
            bail!("{} is not a hash table", path.display());
        }
        buf.advance(8);
        let n = buf.get_u64();
        let mut known = HashMap::with_capacity(n as usize);
        for _ in 0..n {
            let short = || anyhow!("{} is cut short", path.display());
            let len = buf.try_get_u32().map_err(|_| short())? as usize;
            if buf.len() < len + 8 + 8 + 8 + 16 {
                return Err(short());
            }
            let file = PathBuf::from(OsStr::from_bytes(&buf[..len]));
            buf.advance(len);
            let stamp = Stamp { len: buf.get_u64(), mtime_ns: buf.get_i64(), ino: buf.get_u64() };
            let mut h = [0u8; 16];
            buf.copy_to_slice(&mut h);
            known.insert(file, triomphe::Arc::new(Mutex::new(Some((stamp, Hash(h))))));
        }
        Ok(Self { known: Mutex::new(known), dirty: AtomicBool::new(false), saving: Mutex::new(()) })
    }

    // XCR claude for claude: [perf/medium] This stats every file the table
    // knows, writes the table, and fsyncs it, all on the caller's thread, and
    // its caller is Sched::save on the scheduler's event loop, every
    // SAVE_EVERY (60 s) whenever a hash was taken, which during a build is
    // always. On washu-chan the table covers every input of every lane (tens
    // of thousands of paths, many in swap-backed tmpfs), so placement stops
    // for as long as that takes, and an fsync on a busy disk can take seconds.
    // It also clones every PathBuf into the snapshot. Run it on a blocking
    // thread: Hashes is already shared through triomphe::Arc. (blobs-01)
    // 2026-11-01 claude: Sched::save now runs Hashes::save on spawn_blocking
    // (the one call, a few lines), and Hashes::save holds a new `saving` mutex
    // so the periodic save and the final one at shutdown never share
    // hashes.tmp. The final save at shutdown now races main's 1 s
    // shutdown_timeout; if it is cut short the old table stays (tmp plus
    // rename). No test: what changed is which thread runs it, which no test
    // here observes.
    // 2026-11-01 claude: Taking Hashes::save off the scheduler's loop holds,
    // but the save at shutdown is no longer sure to finish. Sched::run's last
    // self.save() now spawns the hashes save and returns at once. daemon::run's
    // sched.await then returns, main's rt.shutdown_timeout(1 s) stops waiting
    // for the save, and the process exits partway through writing. Before the
    // fix, Sched::run saved inline and daemon::run waited for it. So every
    // restart (each rustc-dist install) can lose up to a minute of hashes, and
    // DESIGN's Inputs says reading those files again after a restart ran at
    // about 110 MB/s from swap. Fix: have Sched::run wait for the hashes save
    // after its loop (for example, Sched::save returns the JoinHandle and run
    // awaits it at shutdown), and leave the save every minute as it is. No test
    // is needed for which thread runs it, but the shutdown path could be
    // covered by a scheduler test that drains and then checks that the hashes
    // file exists.
    // 2026-11-01 claude: Sched::save now returns the hashes save's JoinHandle;
    // the periodic save in Sched::run's loop leaves it running as before, and
    // after the loop run awaits the final one (Hashes::save's saving mutex
    // makes it wait for a periodic save still under way too), so daemon::run's
    // sched.await covers it and main's shutdown_timeout no longer cuts it off.
    // DESIGN.md (Inputs) says the table is written once more before the daemon
    // exits. Regression:
    // sched::test::the_file_hashes_are_saved_before_the_scheduler_stops hashes
    // a file, sends Msg::Drain, awaits Sched::run, and loads the hashes file at
    // once, which must hold the hash; with the final save spawned and not
    // awaited it failed 5 of 5 runs (Some((None, 5))). It is a race in
    // principle (the detached save could win), but it can't fail with the fix.
    /// Save the hashes of files still as they were hashed, if a hash was
    /// taken since the last save, and forget the rest, but those being
    /// hashed. It stats every file it knows and syncs the table, so it
    /// belongs on a blocking thread.
    pub fn save(&self, path: &Path) -> Result<()> {
        let _saving = self.saving.lock();
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return Ok(());
        }
        let mut entries: LPooled<Vec<(PathBuf, Option<Taken>)>> = LPooled::take();
        entries.extend(self.known.lock().iter().filter_map(|(p, k)| Some((p.clone(), *k.try_lock()?))));
        let current = |(p, k): &(PathBuf, Option<Taken>)| {
            k.is_some_and(|(s, _)| fs::metadata(p).is_ok_and(|m| Stamp::of(&m) == s))
        };
        let (keep, stale): (LPooled<Vec<_>>, LPooled<Vec<_>>) = entries.drain(..).partition(current);
        {
            let mut map = self.known.lock();
            for (p, _) in stale.iter() {
                map.remove(p);
            }
        }
        let tmp = path.with_extension("tmp");
        let mut w = io::BufWriter::new(File::create(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?);
        w.write_all(HASHES_MAGIC)?;
        w.write_all(&(keep.len() as u64).to_be_bytes())?;
        for (p, k) in keep.iter() {
            let (s, h) = k.expect("kept entries are known");
            let b = p.as_os_str().as_bytes();
            w.write_all(&(b.len() as u32).to_be_bytes())?;
            w.write_all(b)?;
            w.write_all(&s.len.to_be_bytes())?;
            w.write_all(&s.mtime_ns.to_be_bytes())?;
            w.write_all(&s.ino.to_be_bytes())?;
            w.write_all(&h.0)?;
        }
        w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        fs::rename(&tmp, path).with_context(|| format_compact!("writing {}", path.display()))
    }
}

/// A file as `Hashes::name` found it: its hash and size, and how long
/// hashing it took if it had to.
pub struct Named {
    pub hash: Hash,
    pub len: u64,
    pub hashed_us: Option<u64>,
}

/// A server's blobs, named by hash and read only, so job trees can be
/// hard links into the store.
pub struct Store {
    dir: PathBuf,
    cap_bytes: u64,
    used: AtomicU64,
    next_tmp: AtomicU64,
    /// Blobs running jobs read, with how many jobs read each.
    pinned: Mutex<HashMap<Hash, u32>>,
    /// `used` after the last eviction that pinned blobs kept from getting
    /// under the cap, until a pin drops; 0 if none did. Until a pin drops or
    /// more is stored, another pass could free no more.
    stuck: AtomicU64,
    arriving: Arriving,
    next_upload: AtomicU64,
}

/// When a job reads a blob: before its rustc starts, or, a late rlib, only
/// once its linker runs.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum When {
    Now,
    Late,
}

/// A blob a job's client is sending, which other jobs wait for rather than
/// ask their own clients for.
struct Arrival {
    /// Which upload it is, so only its own guard removes it.
    upload: u64,
    when: When,
    /// Its sender goes when the upload ends, stored or not.
    done: watch::Receiver<()>,
}

type Arriving = triomphe::Arc<Mutex<HashMap<Hash, Arrival>>>;

/// Where a job gets a blob it reads.
pub enum Claim {
    /// The store has it.
    Have,
    /// No job is sending it: the job's own client must, and other jobs wait
    /// for it until the guard drops.
    Send(Sending),
    /// The job needs it now, and another job's client sends it late, after
    /// that job's own inputs: the job's own client sends it too.
    SendToo,
    /// Another job's client is sending it.
    Coming(Coming),
}

/// A blob this job's client sends, which other jobs wait for.
pub struct Sending {
    arriving: Arriving,
    hash: Hash,
    upload: u64,
    done: watch::Sender<()>,
}

impl Sending {
    fn unlist(&self, arriving: &mut HashMap<Hash, Arrival>) {
        if arriving.get(&self.hash).is_some_and(|a| a.upload == self.upload) {
            arriving.remove(&self.hash);
        }
    }

    /// Stop offering the blob to jobs that claim it later, unless a job
    /// waits for it already; whether it stopped.
    pub fn release(&self) -> bool {
        let mut arriving = self.arriving.lock();
        // Claims clone the store's receiver under this lock: past it, each
        // receiver is a job waiting.
        if self.done.receiver_count() > 1 {
            return false;
        }
        self.unlist(&mut arriving);
        true
    }
}

impl Drop for Sending {
    fn drop(&mut self) {
        self.unlist(&mut self.arriving.lock());
    }
}

/// A blob another job's client is sending.
pub struct Coming {
    hash: Hash,
    done: watch::Receiver<()>,
}

impl Coming {
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Wait until the other job's upload ends; it failed if it didn't store
    /// the blob.
    pub async fn arrived(mut self, store: &Store) -> Result<()> {
        while self.done.changed().await.is_ok() {}
        if !store.has(&self.hash) {
            bail!("blob {}, which another job was sending, never arrived", self.hash);
        }
        Ok(())
    }
}

impl Store {
    pub fn open(dir: PathBuf, cap_bytes: u64) -> Result<Self> {
        let tmp = dir.join("tmp");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?;
        let used = blobs(&dir, true).iter().map(|(_, len, _)| len).sum();
        Ok(Self {
            dir,
            cap_bytes,
            used: AtomicU64::new(used),
            next_tmp: AtomicU64::new(0),
            pinned: Mutex::new(HashMap::new()),
            stuck: AtomicU64::new(0),
            arriving: Arriving::default(),
            next_upload: AtomicU64::new(0),
        })
    }

    /// Keep `hashes` from eviction until the guard drops, so a blob can't
    /// disappear between telling a client it is present and linking it
    /// into the job.
    pub fn pin<I: Iterator<Item = Hash> + Clone>(&self, hashes: I) -> Pins<'_, I> {
        let mut pinned = self.pinned.lock();
        for h in hashes.clone() {
            *pinned.entry(h).or_default() += 1;
        }
        Pins { store: self, hashes }
    }

    pub fn path(&self, h: &Hash) -> PathBuf {
        let name = format_compact!("{h}");
        self.dir.join(&name[..2]).join(name.as_str())
    }

    /// Where a job that reads the blob `h` `when` gets it: see `Claim`.
    pub fn claim(&self, h: &Hash, when: When) -> Claim {
        if self.has(h) {
            return Claim::Have;
        }
        let mut arriving = self.arriving.lock();
        if let Some(a) = arriving.get(h) {
            return match (when, a.when) {
                (When::Now, When::Late) => Claim::SendToo,
                _ => Claim::Coming(Coming { hash: *h, done: a.done.clone() }),
            };
        }
        // An upload may have ended between the two looks.
        if self.has(h) {
            return Claim::Have;
        }
        let (done, rx) = watch::channel(());
        let upload = self.next_upload.fetch_add(1, Ordering::Relaxed);
        arriving.insert(*h, Arrival { upload, when, done: rx });
        Claim::Send(Sending { arriving: self.arriving.clone(), hash: *h, upload, done })
    }

    /// Whether the blob is present, marking it used.
    pub fn has(&self, h: &Hash) -> bool {
        match File::open(self.path(h)) {
            Ok(f) => {
                let _ = f.set_modified(SystemTime::now());
                true
            }
            Err(_) => false,
        }
    }

    fn tmp(&self) -> PathBuf {
        let n = self.next_tmp.fetch_add(1, Ordering::Relaxed);
        self.dir.join("tmp").join(format_compact!("{}-{n}", std::process::id()).as_str())
    }

    pub fn upload(&self, want: Hash) -> Result<Upload> {
        let tmp = self.tmp();
        let file = File::create(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?;
        Ok(Upload { file, tmp, want, hasher: Hasher::default(), len: 0 })
    }

    /// Keep a file a job wrote, whose hash is `h`, for later jobs that
    /// read it. The file becomes read only.
    pub fn adopt(&self, path: &Path, h: &Hash) -> Result<()> {
        if self.has(h) {
            return Ok(());
        }
        let tmp = self.tmp();
        if fs::hard_link(path, &tmp).is_err() {
            fs::copy(path, &tmp).with_context(|| format_compact!("copying {}", path.display()))?;
        }
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o444))?;
        let len = fs::metadata(&tmp)?.len();
        self.insert(&tmp, h, len)
    }

    /// Move `tmp`, `len` bytes, into the store as `h`, unless another job
    /// stored it first.
    fn insert(&self, tmp: &Path, h: &Hash, len: u64) -> Result<()> {
        let dest = self.path(h);
        if let Some(dir) = dest.parent() {
            fs::create_dir_all(dir)?;
        }
        let linked = fs::hard_link(tmp, &dest);
        let _ = fs::remove_file(tmp);
        match linked {
            Ok(()) => {
                self.used.fetch_add(len, Ordering::Relaxed);
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e).with_context(|| format_compact!("storing {}", dest.display())),
        }
    }

    /// Delete the least recently used blobs no job is reading until the
    /// store is under 90% of its cap.
    pub fn evict(&self) {
        let used = self.used.load(Ordering::Relaxed);
        if used <= self.cap_bytes || used <= self.stuck.load(Ordering::Relaxed) {
            return;
        }
        let mut all = blobs(&self.dir, false);
        all.sort_by_key(|(_, _, mtime)| *mtime);
        let keep = self.cap_bytes / 10 * 9;
        let mut freed = 0;
        for (h, len, _) in all {
            if self.used.load(Ordering::Relaxed) <= keep {
                break;
            }
            let pinned = self.pinned.lock();
            if !pinned.contains_key(&h) && fs::remove_file(self.path(&h)).is_ok() {
                self.used.fetch_sub(len, Ordering::Relaxed);
                freed += len;
            }
        }
        if freed > 0 {
            info!("evicted {} MB of blobs", freed >> 20);
        }
        let left = self.used.load(Ordering::Relaxed);
        let held = left > keep;
        if self.stuck.swap(if held { left } else { 0 }, Ordering::Relaxed) == 0 && held {
            info!("blobs running jobs read keep the store at {} MB, over its cap of {} MB", left >> 20, self.cap_bytes >> 20);
        }
    }
}

/// Blobs kept from eviction. See [`Store::pin`].
pub struct Pins<'a, I: Iterator<Item = Hash> + Clone> {
    store: &'a Store,
    hashes: I,
}

impl<I: Iterator<Item = Hash> + Clone> Drop for Pins<'_, I> {
    fn drop(&mut self) {
        let mut pinned = self.store.pinned.lock();
        for h in self.hashes.clone() {
            if let Entry::Occupied(mut e) = pinned.entry(h) {
                *e.get_mut() -= 1;
                if *e.get() == 0 {
                    e.remove();
                    self.store.stuck.store(0, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Every blob with its length and last use. With `prune`, files whose
/// names aren't hashes, such as blobs named by an older hash, are removed.
fn blobs(dir: &Path, prune: bool) -> Vec<(Hash, u64, SystemTime)> {
    let mut out = Vec::new();
    let Ok(prefixes) = fs::read_dir(dir) else { return out };
    let mut pruned = 0;
    for prefix in prefixes.flatten().filter(|p| p.file_name().len() == 2) {
        let Ok(entries) = fs::read_dir(prefix.path()) else { continue };
        for e in entries.flatten() {
            let Some(h) = Hash::from_hex(e.file_name().as_encoded_bytes()) else {
                if prune && fs::remove_file(e.path()).is_ok() {
                    pruned += 1;
                }
                continue;
            };
            if let Ok(meta) = e.metadata() {
                out.push((h, meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)));
            }
        }
    }
    if pruned > 0 {
        info!("removed {pruned} files from {} that aren't blobs", dir.display());
    }
    out
}

/// A blob arriving in chunks. It enters the store only if its content
/// matches its name.
pub struct Upload {
    file: File,
    tmp: PathBuf,
    want: Hash,
    hasher: Hasher,
    len: u64,
}

impl Upload {
    pub fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.hasher.update(data);
        self.len += data.len() as u64;
        self.file.write_all(data)
    }

    pub fn finish(self, store: &Store) -> Result<()> {
        let got = self.hasher.finish();
        if got != self.want {
            let _ = fs::remove_file(&self.tmp);
            bail!("blob {} arrived with hash {got}", self.want);
        }
        self.file.set_permissions(fs::Permissions::from_mode(0o444))?;
        store.insert(&self.tmp, &self.want, self.len)
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.tmp);
    }
}

/// Hard link a blob to `dest`, or copy it if the store is on another
/// file system.
pub fn place(store: &Store, h: &Hash, dest: &Path) -> Result<()> {
    let src = store.path(h);
    if fs::hard_link(&src, dest).is_err()
        && let Err(e) = fs::copy(&src, dest)
    {
        warn!("placing {h} at {}: {e}", dest.display());
        return Err(e).with_context(|| format_compact!("placing {}", dest.display()));
    }
    Ok(())
}

/// Put a blob at `dest` in place of the file there, so a reader opens
/// either the old file or the blob.
pub fn replace(store: &Store, h: &Hash, dest: &Path) -> Result<()> {
    let tmp = dest.with_extension("rustc-dist-new");
    let _ = fs::remove_file(&tmp);
    place(store, h, &tmp)?;
    fs::rename(&tmp, dest).with_context(|| format_compact!("replacing {}", dest.display()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn a_store_removes_files_not_named_by_a_hash_when_it_opens() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-prune-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let blob = Hash::of(b"kept");
        let name = format_compact!("{blob}");
        let old = "ab".to_string() + &"0".repeat(62);
        for (prefix, file, contents) in [(&name[..2], name.as_str(), &b"kept"[..]), ("ab", old.as_str(), &[0u8; 100][..])] {
            fs::create_dir_all(dir.join(prefix))?;
            fs::write(dir.join(prefix).join(file), contents)?;
        }
        let store = Store::open(dir.clone(), 1 << 20)?;
        assert!(store.has(&blob));
        assert!(!dir.join("ab").join(&old).exists(), "a blake3 name is gone");
        assert_eq!(store.used.load(Ordering::Relaxed), 4, "and isn't counted");
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn hashes_outlive_a_restart_until_their_files_change() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-hashes-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        let (kept, changed, gone, table) = (dir.join("kept"), dir.join("changed"), dir.join("gone"), dir.join("hashes"));
        for f in [&kept, &changed, &gone] {
            fs::write(f, f.to_str().unwrap())?;
        }
        let hashes = Hashes::default();
        for f in [&kept, &changed, &gone] {
            assert!(hashes.name(f)?.hashed_us.is_some());
        }
        hashes.save(&table)?;
        let loaded = Hashes::load(&table)?;
        let named = loaded.name(&kept)?;
        assert_eq!((named.hash, named.hashed_us), (Hash::of(kept.to_str().unwrap().as_bytes()), None), "not read again");
        std::thread::sleep(std::time::Duration::from_millis(10));
        fs::write(&changed, "new contents")?;
        assert_eq!(loaded.name(&changed)?.hash, Hash::of(b"new contents"), "hashed again");
        fs::remove_file(&gone)?;
        loaded.save(&table)?;
        let again = Hashes::load(&table)?;
        assert_eq!(again.known.lock().len(), 2, "the gone file is forgotten");
        assert!(again.name(&changed)?.hashed_us.is_none());
        assert!(Hashes::load(&dir.join("none"))?.known.lock().is_empty());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_file_hashes_the_same_read_in_blocks() -> Result<()> {
        let path = std::env::temp_dir().join(format!("rustc-dist-hash-{}", std::process::id()));
        for len in [0, 1, 4096, HASH_BLOCK, 2 * HASH_BLOCK + 7] {
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            fs::write(&path, &data)?;
            assert_eq!(hash_file(&path)?, Hash::of(&data), "{len}");
        }
        fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn a_file_another_job_is_hashing_is_waited_for_not_hashed_again() -> Result<()> {
        let path = std::env::temp_dir().join(format!("rustc-dist-hashing-{}", std::process::id()));
        fs::write(&path, b"contents")?;
        let hashes = Hashes::default();
        let stamp = Stamp::of(&fs::metadata(&path)?);
        // One job holds the file while it hashes it.
        let known = hashes.entry(&path);
        let hashing = known.lock();
        assert_eq!(hashes.known(&path), Some((None, 8)), "pricing doesn't wait");
        let theirs = Hash([7; 16]);
        std::thread::scope(|s| {
            let waiter = s.spawn(|| hashes.get(&path));
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(!waiter.is_finished(), "it waits for the hash");
            let mut hashing = hashing;
            *hashing = Some((stamp, theirs));
            drop(hashing);
            assert_eq!(waiter.join().unwrap().unwrap(), (theirs, 8), "and takes it");
        });
        assert_eq!(hashes.known(&path), Some((Some(theirs), 8)));
        fs::remove_file(&path)?;
        Ok(())
    }
    use std::time::Duration;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rustc-dist-blobs-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn uploads_are_verified_and_linkable() -> Result<()> {
        let dir = scratch("store");
        let src = dir.join("src");
        fs::write(&src, b"some crate metadata")?;
        let hashes = Hashes::default();
        let (h, len) = hashes.get(&src)?;
        assert_eq!(len, 19);
        let store = Store::open(dir.join("store"), 1 << 30)?;
        assert!(!store.has(&h));
        let mut up = store.upload(h)?;
        up.write(b"some crate ")?;
        up.write(b"metadata")?;
        up.finish(&store)?;
        assert!(store.has(&h));
        let mut bad = store.upload(h)?;
        bad.write(b"something else")?;
        assert!(bad.finish(&store).is_err());
        place(&store, &h, &dir.join("linked"))?;
        assert_eq!(fs::read(dir.join("linked"))?, b"some crate metadata");
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn eviction_takes_the_least_recently_used_unpinned_blobs() -> Result<()> {
        let dir = scratch("evict");
        let store = Store::open(dir.join("store"), 35)?;
        let hashes = Hashes::default();
        let mut stored = Vec::new();
        for (i, content) in [b"blob zero", b"blob one!", b"blob two!", b"blob 3333"].iter().enumerate() {
            let src = dir.join(format!("src{i}"));
            fs::write(&src, content)?;
            let (h, _) = hashes.get(&src)?;
            store.adopt(&src, &h)?;
            store.adopt(&src, &h)?;
            let mut up = store.upload(h)?;
            up.write(*content)?;
            up.finish(&store)?;
            File::open(store.path(&h))?.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(i as u64))?;
            stored.push(h);
        }
        assert_eq!(store.used.load(Ordering::Relaxed), 36);
        let pins = store.pin([stored[0]].into_iter());
        store.evict();
        assert_eq!(stored.iter().map(|h| store.path(h).exists()).collect::<Vec<_>>(), [true, false, true, true]);
        assert_eq!(store.used.load(Ordering::Relaxed), 27);
        drop(pins);
        let mut up = store.upload(stored[1])?;
        up.write(b"blob one!")?;
        up.finish(&store)?;
        store.evict();
        assert_eq!(stored.iter().map(|h| store.path(h).exists()).collect::<Vec<_>>(), [false, true, true, true]);
        assert_eq!(store.used.load(Ordering::Relaxed), 27);
        assert!(store.pinned.lock().is_empty());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn a_blob_needed_now_is_sent_again_rather_than_waited_for_as_another_jobs_late_rlib() -> Result<()> {
        let dir = scratch("claims");
        let store = Store::open(dir.join("store"), 1 << 20)?;
        let h = Hash::of(b"an rlib");
        let Claim::Send(late) = store.claim(&h, When::Late) else { panic!("nobody sends it") };
        assert!(matches!(store.claim(&h, When::Now), Claim::SendToo), "needed now, so not waited for");
        let Claim::Coming(waiting) = store.claim(&h, When::Late) else { panic!("needed late, so waited for") };
        assert!(!late.release(), "a job waits for it");
        drop(waiting);
        assert!(late.release(), "nobody waits for it");
        let Claim::Send(again) = store.claim(&h, When::Now) else { panic!("released, so sent again") };
        drop(late);
        assert!(matches!(store.claim(&h, When::Late), Claim::Coming(_)), "the old guard leaves the new upload listed");
        drop(again);
        assert!(matches!(store.claim(&h, When::Late), Claim::Send(_)));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn an_eviction_pins_held_back_waits_for_a_pin_to_drop() -> Result<()> {
        let dir = scratch("stuck");
        let store = Store::open(dir.join("store"), 20)?;
        let mut stored = Vec::new();
        for (i, content) in [b"blob zero", b"blob one!", b"blob two!"].iter().enumerate() {
            let h = Hash::of(*content);
            let mut up = store.upload(h)?;
            up.write(*content)?;
            up.finish(&store)?;
            File::open(store.path(&h))?.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(10 + i as u64))?;
            stored.push(h);
        }
        let pins = store.pin(stored.iter().copied());
        store.evict();
        assert!(stored.iter().all(|h| store.path(h).exists()), "every blob is pinned");
        // A blob the store doesn't count, the least recently used: a pass
        // would remove it.
        let stray = Hash::of(b"stray");
        fs::create_dir_all(store.path(&stray).parent().unwrap())?;
        fs::write(store.path(&stray), b"stray")?;
        File::open(store.path(&stray))?.set_modified(SystemTime::UNIX_EPOCH)?;
        store.evict();
        assert!(store.path(&stray).exists(), "no pass while the same pins hold");
        drop(pins);
        store.evict();
        assert!(!store.path(&stray).exists(), "a pin dropped, so it tries again");
        assert_eq!(stored.iter().map(|h| store.path(h).exists()).collect::<Vec<_>>(), [false, true, true]);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn hashes_follow_file_changes() -> Result<()> {
        let dir = scratch("hashes");
        let f = dir.join("f");
        fs::write(&f, b"one")?;
        let hashes = Hashes::default();
        let (a, _) = hashes.get(&f)?;
        fs::write(&f, b"three")?;
        let (b, _) = hashes.get(&f)?;
        assert_ne!(a, b);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
