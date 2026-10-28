use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{
    fs, process::Command,
    time::SystemTime,
};

/// What `rustc -vV` prints, plus the target triples installed in the sysroot.
/// Two compilers match when release, commit, host, and LLVM version match.
/// Installed targets are a separate filter on the job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub release: ArcStr,
    pub commit: ArcStr,
    pub host: ArcStr,
    pub llvm: ArcStr,
    pub targets: Vec<ArcStr>,
}

impl Identity {
    pub fn same_compiler(&self, other: &Identity) -> bool {
        self.release == other.release
            && self.commit == other.commit
            && self.host == other.host
            && self.llvm == other.llvm
    }
}

pub fn probe(rustc: &std::path::Path) -> Result<Identity> {
    let vv = Command::new(rustc)
        .arg("-vV")
        .output()
        .with_context(|| format_compact!("running {} -vV", rustc.display()))?;
    if !vv.status.success() {
        bail!("{} -vV failed: {}", rustc.display(), String::from_utf8_lossy(&vv.stderr));
    }
    let text = String::from_utf8(vv.stdout).context("rustc -vV")?;
    let mut id = parse_vv(&text)?;
    let sysroot = Command::new(rustc)
        .args(["--print", "sysroot"])
        .output()
        .with_context(|| format_compact!("running {} --print sysroot", rustc.display()))?;
    if !sysroot.status.success() {
        bail!("{} --print sysroot failed", rustc.display());
    }
    let sysroot = String::from_utf8(sysroot.stdout).context("sysroot")?.trim().to_string();
    id.targets = installed_targets(&sysroot);
    Ok(id)
}

pub fn mtime(rustc: &std::path::Path) -> Result<SystemTime> {
    Ok(fs::metadata(rustc)
        .with_context(|| format_compact!("stat {}", rustc.display()))?
        .modified()?)
}

pub fn parse_vv(text: &str) -> Result<Identity> {
    let mut release = None;
    let mut commit = None;
    let mut host = None;
    let mut llvm = None;
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("release: ") {
            release = Some(ArcStr::from(v.trim()));
        } else if let Some(v) = line.strip_prefix("commit-hash: ") {
            commit = Some(ArcStr::from(v.trim()));
        } else if let Some(v) = line.strip_prefix("host: ") {
            host = Some(ArcStr::from(v.trim()));
        } else if let Some(v) = line.strip_prefix("LLVM version: ") {
            llvm = Some(ArcStr::from(v.trim()));
        }
    }
    Ok(Identity {
        release: release.context("rustc -vV has no release")?,
        commit: commit.context("rustc -vV has no commit-hash")?,
        host: host.context("rustc -vV has no host")?,
        llvm: llvm.context("rustc -vV has no LLVM version")?,
        targets: Vec::new(),
    })
}

fn installed_targets(sysroot: &str) -> Vec<ArcStr> {
    let rustlib = std::path::Path::new(sysroot).join("lib/rustlib");
    let Ok(rd) = fs::read_dir(&rustlib) else {
        return Vec::new();
    };
    let mut targets = Vec::new();
    for ent in rd.flatten() {
        let path = ent.path();
        if path.join("lib").is_dir() {
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                targets.push(ArcStr::from(name));
            }
        }
    }
    targets.sort();
    targets
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_vv() {
        let text = "\
rustc 1.90.0 (abc 2025-01-01)
binary: rustc
commit-hash: abcdef
commit-date: 2025-01-01
host: x86_64-unknown-linux-gnu
release: 1.90.0
LLVM version: 20.1.0
";
        let id = parse_vv(text).unwrap();
        assert_eq!(id.release.as_str(), "1.90.0");
        assert_eq!(id.commit.as_str(), "abcdef");
        assert_eq!(id.host.as_str(), "x86_64-unknown-linux-gnu");
        assert_eq!(id.llvm.as_str(), "20.1.0");
        let mut other = id.clone();
        assert!(id.same_compiler(&other));
        other.commit = ArcStr::from("nope");
        assert!(!id.same_compiler(&other));
    }
}
