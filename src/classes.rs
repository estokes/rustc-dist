use crate::config;
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use log::warn;
use serde::{Deserialize, Serialize};
use std::{fs, io::ErrorKind, path::PathBuf};

/// CPUs of equal measured speed. `calib_us` is the median time one
/// calibration run took on them with every CPU loaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub calib_us: u64,
    pub cpus: Vec<u32>,
}

#[derive(Serialize, Deserialize)]
struct StateFile {
    class: Vec<Class>,
}

fn path() -> PathBuf {
    config::state_dir().join("classes.toml")
}

/// The calibrated classes, fastest first, restricted to `allowed`. If
/// the box was never calibrated, every allowed CPU is one class with a
/// calibration of one second, so costs are CPU seconds.
pub fn load(allowed: &[u32]) -> Result<Vec<Class>> {
    let path = path();
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            warn!("{} is missing; run `rustc-dist install` to calibrate", path.display());
            return Ok(vec![Class { calib_us: 1_000_000, cpus: allowed.to_vec() }]);
        }
        Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
    };
    let file: StateFile =
        toml::from_str(&text).with_context(|| format_compact!("parsing {}", path.display()))?;
    let mut classes = file.class;
    for class in &mut classes {
        if class.calib_us == 0 {
            bail!("{}: calib_us must be positive", path.display());
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
    classes.sort_by_key(|c| c.calib_us);
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
    Ok(classes)
}

pub fn save(classes: &[Class]) -> Result<()> {
    let path = path();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).with_context(|| format_compact!("creating {}", dir.display()))?;
    }
    let text = toml::to_string(&StateFile { class: classes.to_vec() })?;
    let tmp = path.with_extension("toml.tmp");
    fs::write(&tmp, text).with_context(|| format_compact!("writing {}", tmp.display()))?;
    fs::rename(&tmp, &path).with_context(|| format_compact!("writing {}", path.display()))?;
    Ok(())
}
