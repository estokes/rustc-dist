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
    /// Share of MemTotal that the predicted peak memory of running jobs may use.
    pub memory_fraction: f64,
    /// How long the daemon runs the calibration load for a new profile.
    pub calibration_seconds: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self { memory_fraction: 0.8, calibration_seconds: 60 }
    }
}

pub fn load() -> Result<Config> {
    let path = xdg("XDG_CONFIG_HOME", ".config").join("rustc-dist.toml");
    let cfg: Config = match fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text)
            .with_context(|| format_compact!("parsing {}", path.display()))?,
        Err(e) if e.kind() == ErrorKind::NotFound => Config::default(),
        Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
    };
    if !(cfg.memory_fraction > 0. && cfg.memory_fraction <= 1.) {
        bail!("{}: memory_fraction must be in (0, 1]", path.display());
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
