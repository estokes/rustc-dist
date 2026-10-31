use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut};
use compact_str::format_compact;
use log::{info, warn};
use netidx_core::pack::{Pack, PackError};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    fmt,
    fs::{self, File, Metadata},
    io::{self, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

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
}

/// A blob used this recently is never evicted, so it can't disappear
/// between telling a client it is present and linking it into a job.
const IN_USE: Duration = Duration::from_secs(600);

impl Store {
    pub fn open(dir: PathBuf, cap_bytes: u64) -> Result<Self> {
        let tmp = dir.join("tmp");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?;
        let used = blobs(&dir).iter().map(|(_, len, _)| len).sum();
        Ok(Self { dir, cap_bytes, used: AtomicU64::new(used), next_tmp: AtomicU64::new(0) })
    }

    pub fn path(&self, h: &Hash) -> PathBuf {
        let name = format_compact!("{h}");
        self.dir.join(&name[..2]).join(name.as_str())
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
        self.insert(&tmp, h)?;
        self.used.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    fn insert(&self, tmp: &Path, h: &Hash) -> Result<()> {
        let dest = self.path(h);
        if let Some(dir) = dest.parent() {
            fs::create_dir_all(dir)?;
        }
        fs::rename(tmp, &dest).with_context(|| format_compact!("storing {}", dest.display()))
    }

    /// Delete the least recently used blobs until the store is under 90%
    /// of its cap.
    pub fn evict(&self) {
        if self.used.load(Ordering::Relaxed) <= self.cap_bytes {
            return;
        }
        let mut all = blobs(&self.dir);
        all.sort_by_key(|(_, _, mtime)| *mtime);
        let keep = self.cap_bytes / 10 * 9;
        let fresh = SystemTime::now() - IN_USE;
        let mut freed = 0;
        for (path, len, mtime) in all {
            if self.used.load(Ordering::Relaxed) <= keep || mtime > fresh {
                break;
            }
            if fs::remove_file(&path).is_ok() {
                self.used.fetch_sub(len, Ordering::Relaxed);
                freed += len;
            }
        }
        info!("evicted {} MB of blobs", freed >> 20);
    }
}

/// Every blob with its length and last use.
fn blobs(dir: &Path) -> Vec<(PathBuf, u64, SystemTime)> {
    let mut out = Vec::new();
    let Ok(prefixes) = fs::read_dir(dir) else { return out };
    for prefix in prefixes.flatten().filter(|p| p.file_name().len() == 2) {
        let Ok(entries) = fs::read_dir(prefix.path()) else { continue };
        for e in entries.flatten() {
            if let Ok(meta) = e.metadata() {
                out.push((e.path(), meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)));
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
        store.insert(&self.tmp, &self.want)?;
        store.used.fetch_add(self.len, Ordering::Relaxed);
        Ok(())
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
