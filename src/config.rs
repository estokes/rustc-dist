use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use serde::Deserialize;
use std::{
    env, fs,
    io::ErrorKind,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Share of compressed swap (zram, zswap) that jobs' predicted peak
    /// memory may fill beyond RAM.
    pub compressed_swap_share: f64,
    /// Jobserver tokens per CPU. A little over one keeps CPUs busy while
    /// jobs wait on I/O.
    pub tokens_per_cpu: f64,
    /// How long the daemon runs the calibration load for a new profile.
    pub calibration_seconds: u64,
    /// The netidx path every box of the farm publishes under. Without it
    /// this box works alone.
    pub base: Option<String>,
    /// This box's name under `base`. The host name by default.
    pub name: Option<String>,
    /// How much disk other boxes' inputs may use here.
    pub blob_cache_gb: u64,
    /// Send jobs that link (binaries, tests, proc macros) to other boxes
    /// too, which means sending every rlib below them.
    pub remote_links: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            compressed_swap_share: 0.5,
            tokens_per_cpu: 1.5,
            calibration_seconds: 60,
            base: None,
            name: None,
            blob_cache_gb: 20,
            remote_links: true,
        }
    }
}

/// `rustc-dist.toml` in a project.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Project {
    /// Crates to build on this box only.
    pub local: Vec<String>,
}

impl Project {
    pub fn keeps_local(&self, crate_name: &str) -> bool {
        let norm = |s: &str| s.replace('-', "_");
        self.local.iter().any(|l| norm(l) == norm(crate_name))
    }
}

/// The nearest `rustc-dist.toml` at or above `start`, the directory cargo
/// ran in, which is where cargo looks for its own config.
pub fn project(start: &Path) -> Result<Project> {
    for dir in start.ancestors() {
        let path = dir.join("rustc-dist.toml");
        match fs::read_to_string(&path) {
            Ok(text) => {
                return toml::from_str(&text).with_context(|| format_compact!("parsing {}", path.display()));
            }
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
        }
    }
    Ok(Project::default())
}

pub fn load() -> Result<Config> {
    let path = xdg("XDG_CONFIG_HOME", ".config").join("rustc-dist.toml");
    let cfg: Config = match fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text)
            .with_context(|| format_compact!("parsing {}", path.display()))?,
        Err(e) if e.kind() == ErrorKind::NotFound => Config::default(),
        Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
    };
    if !(0. ..=1.).contains(&cfg.compressed_swap_share) {
        bail!("{}: compressed_swap_share must be in [0, 1]", path.display());
    }
    if !(1. ..=4.).contains(&cfg.tokens_per_cpu) {
        bail!("{}: tokens_per_cpu must be in [1, 4]", path.display());
    }
    if cfg.calibration_seconds < 4 {
        bail!("{}: calibration_seconds must be at least 4", path.display());
    }
    Ok(cfg)
}

fn xdg(var: &str, home_rel: &str) -> PathBuf {
    match env::var_os(var) {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(home_rel),
    }
}

/// Other boxes' inputs and the jobs built from them.
pub fn cache_dir() -> PathBuf {
    xdg("XDG_CACHE_HOME", ".cache").join("rustc-dist")
}

pub fn systemd_unit_dir() -> PathBuf {
    xdg("XDG_CONFIG_HOME", ".config").join("systemd/user")
}

/// Calibration results and job history.
pub fn state_dir() -> PathBuf {
    xdg("XDG_STATE_HOME", ".local/state").join("rustc-dist")
}

/// Socket, lock, log, and jobserver FIFOs.
pub fn runtime_dir() -> PathBuf {
    match env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("rustc-dist"),
        _ => PathBuf::from(format_compact!("/tmp/rustc-dist-{}", unsafe { libc::getuid() }).as_str()),
    }
}

pub fn socket_path() -> PathBuf {
    runtime_dir().join("sock")
}

pub fn lock_path() -> PathBuf {
    runtime_dir().join("lock")
}

pub fn log_path() -> PathBuf {
    runtime_dir().join("log")
}

pub fn make_private_dir(dir: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format_compact!("creating {}", dir.display()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn project_files_are_found_above_the_cargo_dir() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-project-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("ws/crates/a"))?;
        assert!(project(&dir.join("ws/crates/a"))?.local.is_empty());
        fs::write(dir.join("ws/rustc-dist.toml"), "local = [\n  \"sqlx-macros\",\n  \"openssl_sys\",\n]\n")?;
        let p = project(&dir.join("ws/crates/a"))?;
        assert!(p.keeps_local("sqlx_macros"));
        assert!(p.keeps_local("openssl-sys"));
        assert!(!p.keeps_local("serde"));
        fs::write(dir.join("ws/rustc-dist.toml"), "lokal = []\n")?;
        assert!(project(&dir).is_ok() && project(&dir.join("ws")).is_err());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
