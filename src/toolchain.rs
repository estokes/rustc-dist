use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use netidx_derive::Pack;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::SystemTime,
};

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

/// A toolchain on this box. rustup's toolchain directory is the sysroot.
pub struct Installed {
    pub toolchain: Toolchain,
    pub sysroot: PathBuf,
}

impl Installed {
    pub fn rustc(&self) -> PathBuf {
        self.sysroot.join("bin/rustc")
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

/// rustup's toolchains on this box.
pub fn installed() -> Vec<Installed> {
    let home = env::var_os("RUSTUP_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".rustup"));
    let Ok(dirs) = fs::read_dir(home.join("toolchains")) else { return Vec::new() };
    dirs.flatten()
        .filter_map(|d| {
            let sysroot = d.path();
            let key = key_of(&sysroot.join("bin/rustc")).ok()?;
            let targets = targets(&sysroot);
            Some(Installed { toolchain: Toolchain { key, targets }, sysroot })
        })
        .collect()
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
mod test {
    use super::*;

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
