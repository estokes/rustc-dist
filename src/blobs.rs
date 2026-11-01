use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut};
use compact_str::format_compact;
use log::{info, warn};
use netidx_core::pack::{Pack, PackError};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    fs::{self, File, Metadata},
    io::{self, ErrorKind, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};
use tokio::sync::watch;

/// A file's blake3 hash, the name of its content on every box.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash(pub [u8; 32]);

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
        32
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        buf.put_slice(&self.0);
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < 32 {
            return Err(PackError::BufferShort);
        }
        let mut h = [0u8; 32];
        buf.copy_to_slice(&mut h);
        Ok(Hash(h))
    }
}

fn hash_file(path: &Path) -> Result<Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(File::open(path).with_context(|| format_compact!("opening {}", path.display()))?)
        .with_context(|| format_compact!("reading {}", path.display()))?;
    Ok(Hash(*hasher.finalize().as_bytes()))
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

/// Hashes of the files jobs read, each kept until its file changes.
#[derive(Default)]
pub struct Hashes(Mutex<HashMap<PathBuf, (Stamp, Hash)>>);

impl Hashes {
    pub fn get(&self, path: &Path) -> Result<(Hash, u64)> {
        let meta = fs::metadata(path).with_context(|| format_compact!("stat {}", path.display()))?;
        if !meta.is_file() {
            bail!("{} is not a file", path.display());
        }
        let stamp = Stamp::of(&meta);
        if let Some((s, h)) = self.0.lock().get(path)
            && *s == stamp
        {
            return Ok((*h, stamp.len));
        }
        let hash = hash_file(path)?;
        self.0.lock().insert(path.to_path_buf(), (stamp, hash));
        Ok((hash, stamp.len))
    }

    /// The size of the file at `path` and, if it was hashed since it last
    /// changed, its hash; none if there is no such file. It reads no file.
    pub fn known(&self, path: &Path) -> Option<(Option<Hash>, u64)> {
        let meta = fs::metadata(path).ok().filter(|m| m.is_file())?;
        let stamp = Stamp::of(&meta);
        let hash = self.0.lock().get(path).filter(|(s, _)| *s == stamp).map(|(_, h)| *h);
        Some((hash, stamp.len))
    }

    /// The hash of a file just written, so it needn't be read again.
    pub fn insert(&self, path: &Path, hash: Hash) -> Result<()> {
        let meta = fs::metadata(path).with_context(|| format_compact!("stat {}", path.display()))?;
        self.0.lock().insert(path.to_path_buf(), (Stamp::of(&meta), hash));
        Ok(())
    }
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
    /// Blobs a job's client is sending, which other jobs wait for rather
    /// than ask their own clients for: each one's receiver, whose sender
    /// goes when the upload ends, stored or not.
    arriving: Mutex<HashMap<Hash, watch::Receiver<()>>>,
}

/// Where a job gets a blob it reads.
pub enum Claim<'a> {
    /// The store has it.
    Have,
    /// No job is sending it: the job's own client must, and other jobs wait
    /// for it until the guard drops.
    Send(Sending<'a>),
    /// Another job's client is sending it.
    Coming(Coming),
}

/// A blob this job's client sends, which other jobs wait for.
pub struct Sending<'a> {
    store: &'a Store,
    hash: Hash,
    _done: watch::Sender<()>,
}

impl Drop for Sending<'_> {
    fn drop(&mut self) {
        self.store.arriving.lock().remove(&self.hash);
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
        let used = blobs(&dir).iter().map(|(_, len, _)| len).sum();
        Ok(Self {
            dir,
            cap_bytes,
            used: AtomicU64::new(used),
            next_tmp: AtomicU64::new(0),
            pinned: Mutex::new(HashMap::new()),
            arriving: Mutex::new(HashMap::new()),
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

    /// Whether the blob is present, marking it used.
    /// Where a job gets the blob `h`: see `Claim`.
    pub fn claim(&self, h: &Hash) -> Claim<'_> {
        if self.has(h) {
            return Claim::Have;
        }
        let mut arriving = self.arriving.lock();
        if let Some(done) = arriving.get(h) {
            return Claim::Coming(Coming { hash: *h, done: done.clone() });
        }
        // An upload may have ended between the two looks.
        if self.has(h) {
            return Claim::Have;
        }
        let (tx, rx) = watch::channel(());
        arriving.insert(*h, rx);
        Claim::Send(Sending { store: self, hash: *h, _done: tx })
    }

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
        Ok(Upload { file, tmp, want, hasher: blake3::Hasher::new(), len: 0 })
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
        if self.used.load(Ordering::Relaxed) <= self.cap_bytes {
            return;
        }
        let mut all = blobs(&self.dir);
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
        info!("evicted {} MB of blobs", freed >> 20);
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
                }
            }
        }
    }
}

/// Every blob with its length and last use.
fn blobs(dir: &Path) -> Vec<(Hash, u64, SystemTime)> {
    let mut out = Vec::new();
    let Ok(prefixes) = fs::read_dir(dir) else { return out };
    for prefix in prefixes.flatten().filter(|p| p.file_name().len() == 2) {
        let Ok(entries) = fs::read_dir(prefix.path()) else { continue };
        for e in entries.flatten() {
            let Ok(h) = blake3::Hash::from_hex(e.file_name().as_encoded_bytes()) else { continue };
            if let Ok(meta) = e.metadata() {
                out.push((Hash(*h.as_bytes()), meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)));
            }
        }
    }
    out
}

/// A blob arriving in chunks. It enters the store only if its content
/// matches its name.
pub struct Upload {
    file: File,
    tmp: PathBuf,
    want: Hash,
    hasher: blake3::Hasher,
    len: u64,
}

impl Upload {
    pub fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.hasher.update(data);
        self.len += data.len() as u64;
        self.file.write_all(data)
    }

    pub fn finish(self, store: &Store) -> Result<()> {
        let got = Hash(*self.hasher.finalize().as_bytes());
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
