use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use log::{info, warn};
use netidx_derive::Pack;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    env, fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
    slice,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::watch,
    task,
    time::{self, MissedTickBehavior},
};
use triomphe::Arc;

/// A compiler as boxes match it: rustc's commit and host. Two rustc builds
/// with the same commit and host produce the same output.
#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub struct Toolchain {
    pub key: ArcStr,
    /// Targets with a standard library installed.
    pub targets: Vec<ArcStr>,
}

impl Toolchain {
    /// `target` None is the host.
    pub fn builds(&self, key: &str, target: Option<&str>) -> bool {
        self.key == key && target.is_none_or(|t| self.targets.iter().any(|x| x == t))
    }
}

/// What a toolchain's files were like: its rustc's mtime, inode and size,
/// and when its standard libraries last changed.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Stamp {
    rustc: Option<(SystemTime, u64, u64)>,
    targets: Option<SystemTime>,
}

impl Stamp {
    fn of(sysroot: &Path) -> Self {
        let rustc = fs::metadata(sysroot.join("bin/rustc")).ok().and_then(|m| Some((m.modified().ok()?, m.ino(), m.len())));
        let targets = fs::metadata(sysroot.join("lib/rustlib")).and_then(|m| m.modified()).ok();
        Self { rustc, targets }
    }
}

/// A toolchain on this box. rustup's toolchain directory is the sysroot.
pub struct Installed {
    pub toolchain: Toolchain,
    pub sysroot: PathBuf,
    /// Taken before its rustc was asked its key.
    stamp: Stamp,
}

impl Installed {
    pub fn rustc(&self) -> PathBuf {
        self.sysroot.join("bin/rustc")
    }

    /// Whether its files are as they were when it was read, so its rustc
    /// still has its key.
    pub fn unchanged(&self) -> bool {
        Stamp::of(&self.sysroot).rustc == self.stamp.rustc
    }
}

pub fn key_of(rustc: &Path) -> Result<ArcStr> {
    let out = Command::new(rustc)
        .arg("-vV")
        .output()
        .with_context(|| format_compact!("running {} -vV", rustc.display()))?;
    if !out.status.success() {
        bail!("{} -vV failed", rustc.display());
    }
    parse_key(&String::from_utf8_lossy(&out.stdout))
}

fn parse_key(vv: &str) -> Result<ArcStr> {
    let field = |name: &str| {
        vv.lines().find_map(|l| l.strip_prefix(name)).map(str::trim).context("rustc -vV output")
    };
    Ok(format_compact!("{} {}", field("commit-hash: ")?, field("host: ")?).as_str().into())
}

// XCR claude for claude: [bug/high] The toolchains are read once, when the daemon joins the farm,
// and a toolchain is found by key but run by path (Installed::rustc). After a `rustup update` while
// the daemon runs, this box goes on advertising the old commit (farm::Member's toolchains) and runs
// the new rustc for jobs that ask for the old one (sandbox::take finds `tc` by the old key). Leaf
// crates, which load nothing but the sysroot, build cleanly there with the wrong compiler, and the
// client's next compile that reads them fails with E0514 (compiled by an incompatible version of
// rustc), locally, so nothing retries it; cargo's fingerprint names the client's rustc, so later
// builds keep the bad files until `cargo clean`. The client re-keys when its rustc's mtime changes
// (Keys::key); this side never does, and a toolchain installed later isn't served until a restart.
// Fix: before running, check tc.rustc()'s key through a Keys cache and answer "no toolchain" on a
// mismatch, and rescan and republish the toolchains when a sysroot's rustc changes. (toolchain-01)
// 2026-11-01 claude: toolchain::installed now returns Toolchains, which stamps each sysroot's rustc
// (mtime, inode, size) and lib/rustlib before asking the key. Server holds it in a Mutex and
// refreshes it before every offer (cheap stats; a full read only when something changed, warning
// when a sysroot's key changed), so an old key gets no toolchain; take also fails a job whose rustc
// changed while it ran (Installed::unchanged). That replaces the Keys cache the CR suggests, with
// the same effect and also catching a toolchain added or removed. Republishing: farm::run follows
// toolchain::watch, a task that looks every 10 s (its first look a full read, as the daemon's list
// may predate a change) and republishes on a change (a few lines in farm.rs; daemon.rs untouched,
// as Toolchains iterates like the old Vec). Tests:
// toolchain::test::toolchains_are_read_again_when_rustup_changes_them and
// toolchain::test::the_toolchains_to_publish_follow_rustup (both fail if Toolchains::current always
// says yes).
// 2026-11-01 claude: Toolchains, refresh and watch hold, and both named tests fail when
// Toolchains::current always says yes (checked). Two gaps remain. (1) The stamp is checked when the
// job is offered and again after it runs, but not just before the sandbox starts. In between come
// the manifest, uploading inputs (a minute or more over wifi), waiting for other jobs' uploads, and
// building the tree. A rustup update in that window makes the new rustc run for the old key. Its
// artifacts, including the .rmeta line cargo starts dependents on, reach the client before the
// check after the run fails the job, so a dependent built locally can still hit E0514. Fix: check
// tc.unchanged() in sandbox::job just before run(), and keep the check after the run. (2) No test
// covers the server side. With toolchains.refresh() removed from Server::toolchain and the check
// after the run turned off, all 170 tests still pass (checked). A test that runs a Server on a fake
// rustup home and swaps its rustc between two offers would cover it; Toolchains::read is private,
// so it needs a pub(crate) constructor.
// 2026-11-01 claude: sandbox::job checks tc.unchanged() just before run() starts the sandbox, after
// the inputs are in and the tree is built, and fails the job 'before the job ran' so no output
// reaches the client; take keeps the check after the run. Toolchains::read is pub(crate), and
// toolchain::test's fake rustup home now keeps each rustc as a hard link in its sysroot (so the
// sandbox can run it), printing '<commit> ran', c1 waiting until its sysroot's rustc is no longer
// c1. e2e_test::a_toolchain_rustup_changes_is_refused_at_the_offer_before_the_run_and_after_it runs
// a Server on it: rustc swapped between two offers (the second must get 'no toolchain'), between
// offer and Run (nothing may print, and the job fails), and while c1 runs (the job fails rather
// than exit 0). It fails with the refresh in Server::toolchain removed, with the check before run
// removed, and with the check after the run removed (each checked). DESIGN's toolchain paragraph
// says so.
/// rustup's toolchains on this box, as they were when read.
pub struct Toolchains {
    home: PathBuf,
    /// Every toolchain directory, by path, with its stamp, whether or not
    /// its rustc answered.
    dirs: Vec<(PathBuf, Stamp)>,
    installed: Vec<Arc<Installed>>,
}

impl Toolchains {
    /// The toolchains under rustup's `home`, each stamped before its rustc
    /// is asked its key, so a rustc that changes meanwhile is read again.
    pub(crate) fn read(home: PathBuf) -> Self {
        let mut dirs: Vec<(PathBuf, Stamp)> = match fs::read_dir(home.join("toolchains")) {
            Ok(entries) => entries
                .flatten()
                .map(|e| {
                    let path = e.path();
                    let stamp = Stamp::of(&path);
                    (path, stamp)
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        dirs.sort_by(|a, b| a.0.cmp(&b.0));
        let installed = dirs
            .iter()
            .filter_map(|(sysroot, stamp)| {
                let key = key_of(&sysroot.join("bin/rustc")).ok()?;
                let toolchain = Toolchain { key, targets: targets(sysroot) };
                Some(Arc::new(Installed { toolchain, sysroot: sysroot.clone(), stamp: *stamp }))
            })
            .collect();
        Self { home, dirs, installed }
    }

    /// Whether rustup's toolchains are as they were read: none added,
    /// removed or changed.
    pub fn current(&self) -> bool {
        let Ok(entries) = fs::read_dir(self.home.join("toolchains")) else { return self.dirs.is_empty() };
        let mut n = 0;
        for e in entries.flatten() {
            n += 1;
            let path = e.path();
            match self.dirs.binary_search_by(|(p, _)| p.as_path().cmp(&path)) {
                Ok(i) if self.dirs[i].1 == Stamp::of(&path) => (),
                _ => return false,
            }
        }
        n == self.dirs.len()
    }

    /// Read them again if they changed since they were read, and say how.
    pub fn refresh(&mut self) {
        if self.current() {
            return;
        }
        let now = Self::read(self.home.clone());
        for t in &now.installed {
            let (key, at) = (&t.toolchain.key, t.sysroot.display());
            match self.installed.iter().find(|o| o.sysroot == t.sysroot) {
                Some(o) if o.toolchain.key != *key => {
                    let old = &o.toolchain.key;
                    warn!("the rustc at {at} is now {key}, not {old}: jobs for {old} are refused")
                }
                Some(o) if o.toolchain == t.toolchain => (),
                _ => info!("serving toolchain {key} at {at} for {:?}", t.toolchain.targets),
            }
        }
        for o in self.installed.iter().filter(|o| !now.installed.iter().any(|t| t.sysroot == o.sysroot)) {
            info!("no longer serving toolchain {} at {}", o.toolchain.key, o.sysroot.display());
        }
        *self = now;
    }

    /// The toolchain `key`, with `target`'s standard library unless that
    /// is None, the host.
    pub fn find(&self, key: &str, target: Option<&str>) -> Option<&Arc<Installed>> {
        self.installed.iter().find(|t| t.toolchain.builds(key, target))
    }

    pub fn iter(&self) -> slice::Iter<'_, Arc<Installed>> {
        self.installed.iter()
    }
}

impl<'a> IntoIterator for &'a Toolchains {
    type Item = &'a Arc<Installed>;
    type IntoIter = slice::Iter<'a, Arc<Installed>>;

    fn into_iter(self) -> Self::IntoIter {
        self.installed.iter()
    }
}

fn rustup_home() -> PathBuf {
    env::var_os("RUSTUP_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".rustup"))
}

/// rustup's toolchains on this box.
pub fn installed() -> Toolchains {
    Toolchains::read(rustup_home())
}

/// How often `watch` looks at rustup's toolchains.
const WATCH_EVERY: Duration = Duration::from_secs(10);

/// The toolchains to publish, from `published` on, read again whenever
/// rustup adds, removes or changes one.
pub fn watch(published: Vec<Toolchain>) -> watch::Receiver<Vec<Toolchain>> {
    watch_in(rustup_home(), published, WATCH_EVERY)
}

fn watch_in(home: PathBuf, published: Vec<Toolchain>, every: Duration) -> watch::Receiver<Vec<Toolchain>> {
    let (tx, rx) = watch::channel(published);
    tokio::spawn(async move {
        // None at first: `published` may predate a change.
        let mut read: Option<Toolchains> = None;
        let mut tick = time::interval_at(time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => (),
                _ = tx.closed() => break,
            }
            let (home, last) = (home.clone(), read.take());
            let looked = task::spawn_blocking(move || match last {
                Some(t) if t.current() => t,
                _ => Toolchains::read(home),
            });
            let Ok(now) = looked.await else { break };
            let toolchains: Vec<Toolchain> = now.iter().map(|t| t.toolchain.clone()).collect();
            tx.send_if_modified(|p| {
                let changed = *p != toolchains;
                if changed {
                    *p = toolchains;
                }
                changed
            });
            read = Some(now);
        }
    });
    rx
}

fn targets(sysroot: &Path) -> Vec<ArcStr> {
    let Ok(dirs) = fs::read_dir(sysroot.join("lib/rustlib")) else { return Vec::new() };
    let mut targets: Vec<ArcStr> = dirs
        .flatten()
        .filter(|d| d.path().join("lib").is_dir())
        .filter_map(|d| d.file_name().to_str().map(ArcStr::from))
        .collect();
    targets.sort();
    targets
}

/// Keys of the compilers cargo runs, by path, until the file changes.
#[derive(Default)]
pub struct Keys(Mutex<HashMap<PathBuf, (SystemTime, ArcStr)>>);

impl Keys {
    pub fn key(&self, rustc: &Path) -> Result<ArcStr> {
        let mtime = fs::metadata(rustc)
            .and_then(|m| m.modified())
            .with_context(|| format_compact!("stat {}", rustc.display()))?;
        if let Some((t, key)) = self.0.lock().get(rustc)
            && *t == mtime
        {
            return Ok(key.clone());
        }
        let key = key_of(rustc)?;
        self.0.lock().insert(rustc.to_path_buf(), (mtime, key.clone()));
        Ok(key)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn keys_are_commit_and_host() {
        let vv = "rustc 1.99.1 (58cf8f9c3 2026-10-13)\nbinary: rustc\ncommit-hash: 58cf8f9c35b\n\
                  commit-date: 2026-10-13\nhost: x86_64-unknown-linux-gnu\nrelease: 1.99.1\n";
        assert_eq!(parse_key(vv).unwrap().as_str(), "58cf8f9c35b x86_64-unknown-linux-gnu");
        let t = Toolchain {
            key: "c h".into(),
            targets: vec!["h".into(), "wasm32-unknown-unknown".into()],
        };
        assert!(t.builds("c h", None));
        assert!(t.builds("c h", Some("wasm32-unknown-unknown")));
        assert!(!t.builds("c h", Some("aarch64-apple-darwin")));
        assert!(!t.builds("d h", None));
    }

    /// A rustup home whose rustcs answer `-vV` as commit `c1` or `c2` on
    /// host `h`. Run as a compiler, each prints `<commit> ran` on stderr and
    /// exits 0, c1 once its sysroot's rustc is no longer c1. A sysroot's
    /// rustc is a link to one of them, so changing it writes no file a
    /// process may be running.
    pub(crate) fn fake_rustup_home(name: &str) -> PathBuf {
        let home = std::env::temp_dir().join(format!("rustc-dist-rustup-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(home.join("rustcs")).unwrap();
        for commit in ["c1", "c2"] {
            let rustc = home.join("rustcs").join(commit);
            let wait = match commit {
                "c1" => "while [ \"$(readlink \"$0\")\" = rustc-c1 ]; do sleep 0.05; done\n",
                _ => "",
            };
            let script = format!(
                "#!/bin/sh\nif [ \"$1\" = -vV ]; then echo 'commit-hash: {commit}'; echo 'host: h'; exit 0; fi\n\
                 echo '{commit} ran' >&2\n{wait}"
            );
            fs::write(&rustc, script).unwrap();
            fs::set_permissions(&rustc, fs::Permissions::from_mode(0o755)).unwrap();
        }
        // Another thread's fork may still hold a script open for writing.
        std::thread::sleep(Duration::from_millis(100));
        home
    }

    /// Install `name` at `home`, its rustc answering as `commit`: a link to
    /// a hard link of that rustc in the sysroot, so a sandbox that has the
    /// sysroot can run it.
    pub(crate) fn install(home: &Path, name: &str, commit: &str) {
        let sysroot = home.join("toolchains").join(name);
        fs::create_dir_all(sysroot.join("bin")).unwrap();
        fs::create_dir_all(sysroot.join("lib/rustlib/h/lib")).unwrap();
        let rustc = format!("rustc-{commit}");
        let linked = sysroot.join("bin").join(&rustc);
        if !linked.exists() {
            fs::hard_link(home.join("rustcs").join(commit), &linked).unwrap();
        }
        let new = sysroot.join("bin/rustc.new");
        std::os::unix::fs::symlink(&rustc, &new).unwrap();
        fs::rename(&new, sysroot.join("bin/rustc")).unwrap();
    }

    #[test]
    fn toolchains_are_read_again_when_rustup_changes_them() {
        let home = fake_rustup_home("refresh");
        install(&home, "a", "c1");
        let mut t = Toolchains::read(home.clone());
        let a = t.find("c1 h", None).expect("a").clone();
        assert!(t.current() && a.unchanged());
        install(&home, "a", "c2");
        assert!(!t.current() && !a.unchanged(), "an update is seen");
        t.refresh();
        assert!(t.find("c1 h", None).is_none(), "and the old key is no longer served");
        assert!(t.find("c2 h", None).is_some_and(|a| a.unchanged()));
        assert!(t.current());
        fs::create_dir_all(home.join("toolchains/a/lib/rustlib/wasm32/lib")).unwrap();
        assert!(!t.current(), "a target added is seen");
        t.refresh();
        assert!(t.find("c2 h", Some("wasm32")).is_some());
        install(&home, "b", "c1");
        assert!(!t.current(), "a toolchain added is seen");
        t.refresh();
        assert!(t.find("c1 h", None).is_some());
        fs::remove_dir_all(home.join("toolchains/b")).unwrap();
        assert!(!t.current(), "a toolchain removed is seen");
        fs::remove_dir_all(&home).unwrap();
    }

    #[tokio::test]
    async fn the_toolchains_to_publish_follow_rustup() -> Result<()> {
        let home = fake_rustup_home("watch");
        install(&home, "a", "c1");
        let published = Toolchains::read(home.clone()).iter().map(|t| t.toolchain.clone()).collect();
        let mut toolchains = watch_in(home.clone(), published, Duration::from_millis(20));
        time::sleep(Duration::from_millis(200)).await;
        assert!(!toolchains.has_changed()?, "as published");
        install(&home, "a", "c2");
        time::timeout(Duration::from_secs(5), toolchains.changed()).await.context("the update wasn't seen")??;
        assert_eq!(toolchains.borrow().iter().map(|t| t.key.as_str()).collect::<Vec<_>>(), ["c2 h"]);
        fs::remove_dir_all(&home)?;
        Ok(())
    }

    #[test]
    fn finds_the_local_toolchain() {
        let found = installed();
        let rustc = Command::new("rustc").arg("--print").arg("sysroot").output().unwrap();
        let sysroot = PathBuf::from(String::from_utf8(rustc.stdout).unwrap().trim());
        let mine = found.iter().find(|i| i.sysroot == sysroot).expect("the current toolchain");
        assert_eq!(mine.toolchain.key, key_of(&mine.rustc()).unwrap());
        assert!(!mine.toolchain.targets.is_empty());
    }
}
