use crate::calibrate;
use anyhow::{Context, Result, bail};
use arcstr::{ArcStr, literal};
use compact_str::format_compact;
use log::warn;
use serde::{Deserialize, Serialize};
use std::{
    fmt, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

// XCR claude for claude: [doc-drift/low] Calibration loads one thread per core, on its
// first CPU, with the SMT siblings idle (calibrate::run), not every CPU; the siblings
// join the class at the core's rate, about four times what each of two threads sharing
// a core got on aieka (DESIGN.md's Calibration section). Say "with one thread on every
// core". (classes-01)
// 2026-11-01 claude: The Class doc now says the rate is from running the calibration
// load with one thread on every core. Doc only; no test.
/// CPUs of equal measured speed. `rate` is the instructions per CPU-µs
/// each retired running the calibration load with one thread on every
/// core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub rate: u64,
    pub cpus: Vec<u32>,
}

/// Every allowed CPU as one class, at the rate of a calibration run that
/// took one second. Used only when calibration fails.
pub fn fallback(allowed: &[u32]) -> Vec<Class> {
    vec![Class { rate: calibrate::RUN_INSTRUCTIONS / 1_000_000, cpus: allowed.to_vec() }]
}

/// A power profile name, also used as a file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile(ArcStr);

impl Profile {
    pub const DEFAULT: Profile = Profile(literal!("default"));

    pub fn new(name: &str) -> Result<Self> {
        let ok = !name.is_empty()
            && name.len() <= 64
            && !name.starts_with('.')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
        if !ok {
            bail!("profile names are 1 to 64 of A-Z a-z 0-9 - _ . and don't start with a dot");
        }
        Ok(Self(ArcStr::from(name)))
    }

    pub fn name(&self) -> &ArcStr {
        &self.0
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Serialize, Deserialize)]
struct StateFile {
    class: Vec<Class>,
}

fn profile_path(state_dir: &Path, profile: &Profile) -> PathBuf {
    state_dir.join("profiles").join(format_compact!("{profile}.toml").as_str())
}

fn current_path(state_dir: &Path) -> PathBuf {
    state_dir.join("profile")
}

/// The profile the laptop was last switched to.
pub fn current(state_dir: &Path) -> Profile {
    let path = current_path(state_dir);
    match fs::read_to_string(&path) {
        Ok(text) => Profile::new(text.trim()).unwrap_or_else(|e| {
            warn!("{}: {e:#}", path.display());
            Profile::DEFAULT
        }),
        Err(e) if e.kind() == ErrorKind::NotFound => Profile::DEFAULT,
        Err(e) => {
            warn!("reading {}: {e}", path.display());
            Profile::DEFAULT
        }
    }
}

pub fn set_current(state_dir: &Path, profile: &Profile) -> Result<()> {
    write_atomic(&current_path(state_dir), profile.name().as_bytes())
}

/// The calibrated classes of `profile`, fastest first, restricted to
/// `allowed`. None if the profile was never calibrated.
pub fn load(state_dir: &Path, profile: &Profile, allowed: &[u32]) -> Result<Option<Vec<Class>>> {
    let path = profile_path(state_dir, profile);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
    };
    let file: StateFile =
        toml::from_str(&text).with_context(|| format_compact!("parsing {}", path.display()))?;
    let mut classes = file.class;
    for class in &mut classes {
        if class.rate == 0 {
            bail!("{}: rate must be positive", path.display());
        }
        class.cpus.retain(|cpu| {
            let ok = allowed.contains(cpu);
            if !ok {
                warn!("cpu {cpu} from {} is not available, ignoring it", path.display());
            }
            ok
        });
    }
    classes.retain(|c| !c.cpus.is_empty());
    classes.sort_by_key(|c| std::cmp::Reverse(c.rate));
    let mut seen: Vec<u32> = classes.iter().flat_map(|c| c.cpus.iter().copied()).collect();
    let total = seen.len();
    seen.sort_unstable();
    seen.dedup();
    if seen.len() != total {
        bail!("{}: a cpu is listed in more than one class", path.display());
    }
    if classes.is_empty() {
        bail!("{}: no usable classes", path.display());
    }
    Ok(Some(classes))
}

pub fn save(state_dir: &Path, profile: &Profile, classes: &[Class]) -> Result<()> {
    let text = toml::to_string(&StateFile { class: classes.to_vec() })?;
    write_atomic(&profile_path(state_dir, profile), text.as_bytes())
}

fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).with_context(|| format_compact!("creating {}", dir.display()))?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data).with_context(|| format_compact!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format_compact!("writing {}", path.display()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn profile_names() {
        for good in ["performance", "power-saver", "AC_1.2"] {
            assert!(Profile::new(good).is_ok(), "{good}");
        }
        for bad in ["", "../x", "a/b", ".hidden", "sp ace", &"x".repeat(65)] {
            assert!(Profile::new(bad).is_err(), "{bad}");
        }
    }
}
