use crate::protocol::{Unit, UnitStatus};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use hdrhistogram::{
    Histogram,
    serialization::{Deserializer, Serializer, V2Serializer},
};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, ErrorKind, Read, Write},
    path::Path,
};

/// A unit's counts are halved once it has this many runs, so its
/// prediction follows the crate as it grows.
const HALVE_AT: u64 = 256;
/// Predicted cost of a job when nothing at all has been recorded.
const UNKNOWN_COST_MCU: u64 = 1000;
const MAGIC: &[u8; 8] = b"rdstats1";

pub struct Sample {
    pub cost_mcu: u64,
    pub wall_ms: u64,
    pub mem_kb: u64,
}

struct Hists {
    /// Thousandths of a calibration run.
    cost: Histogram<u32>,
    wall_ms: Histogram<u32>,
    mem_kb: Histogram<u32>,
}

impl Hists {
    fn new() -> Self {
        let h = |high| Histogram::new_with_bounds(1, high, 2).expect("histogram bounds");
        Self { cost: h(100_000_000), wall_ms: h(36_000_000), mem_kb: h(1 << 30) }
    }

    fn record(&mut self, s: &Sample) {
        self.cost.saturating_record(s.cost_mcu.max(1));
        self.wall_ms.saturating_record(s.wall_ms.max(1));
        self.mem_kb.saturating_record(s.mem_kb.max(1));
    }

    fn halve(&mut self) {
        for h in [&mut self.cost, &mut self.wall_ms, &mut self.mem_kb] {
            let mut half = Histogram::new_from(h);
            for v in h.iter_recorded() {
                let n = v.count_at_value() / 2;
                if n > 0 {
                    half.record_n(v.value_iterated_to(), n).expect("same bounds");
                }
            }
            *h = half;
        }
    }

    fn add(&mut self, other: &Hists) {
        self.cost.add(&other.cost).expect("same bounds");
        self.wall_ms.add(&other.wall_ms).expect("same bounds");
        self.mem_kb.add(&other.mem_kb).expect("same bounds");
    }
}

pub struct Prediction {
    pub cost_mcu: u64,
    pub mem_kb: u64,
    /// Where `cost_mcu` falls among all recorded jobs, 0 to 100.
    pub percentile: f64,
}

pub struct Stats {
    exact: HashMap<ArcStr, Hists>,
    coarse: HashMap<ArcStr, Hists>,
    /// Sum of the exact units, so a unit counts as often as it runs.
    global: Hists,
    pub dirty: bool,
}

impl Stats {
    pub fn new() -> Self {
        Self { exact: HashMap::new(), coarse: HashMap::new(), global: Hists::new(), dirty: false }
    }

    pub fn predict(&self, unit: &Unit) -> Prediction {
        let known = |m: &HashMap<ArcStr, Hists>, k: &ArcStr| {
            m.get(k).filter(|h| !h.cost.is_empty()).map(|h| {
                (h.cost.value_at_quantile(0.5), h.mem_kb.value_at_quantile(0.9))
            })
        };
        let (cost_mcu, mem_kb) = known(&self.exact, &unit.exact)
            .or_else(|| known(&self.coarse, &unit.coarse))
            .unwrap_or_else(|| {
                if self.global.cost.is_empty() {
                    (UNKNOWN_COST_MCU, 0)
                } else {
                    (self.global.cost.value_at_quantile(0.5), self.global.mem_kb.value_at_quantile(0.5))
                }
            });
        Prediction { cost_mcu, mem_kb, percentile: self.percentile(cost_mcu) }
    }

    fn percentile(&self, cost_mcu: u64) -> f64 {
        if self.global.cost.is_empty() { 50. } else { self.global.cost.percentile_below(cost_mcu) }
    }

    pub fn record(&mut self, unit: &Unit, s: Sample) {
        for (map, key) in [(&mut self.exact, &unit.exact), (&mut self.coarse, &unit.coarse)] {
            let h = map.entry(key.clone()).or_insert_with(Hists::new);
            h.record(&s);
            if h.cost.len() >= HALVE_AT {
                h.halve();
            }
        }
        self.global.record(&s);
        self.dirty = true;
    }

    /// Recompute the global histogram so halved units stop counting at
    /// their old weight.
    pub fn rebuild_global(&mut self) {
        let mut global = Hists::new();
        for h in self.exact.values() {
            global.add(h);
        }
        self.global = global;
    }

    /// The `n` most expensive units, with the class `ideal` picks for each.
    pub fn top(&self, n: usize, ideal: impl Fn(f64) -> usize) -> Vec<UnitStatus> {
        let mut units: Vec<UnitStatus> = self
            .exact
            .iter()
            .filter(|(_, h)| !h.cost.is_empty())
            .map(|(key, h)| {
                let cost_mcu = h.cost.value_at_quantile(0.5);
                UnitStatus {
                    key: key.clone(),
                    runs: h.cost.len(),
                    cost_mcu,
                    wall_ms: h.wall_ms.value_at_quantile(0.5),
                    mem_kb: h.mem_kb.value_at_quantile(0.9),
                    class: ideal(self.percentile(cost_mcu)) as u32,
                }
            })
            .collect();
        units.sort_by_key(|u| std::cmp::Reverse(u.cost_mcu));
        units.truncate(n);
        units
    }

    /// A missing file is an empty table.
    pub fn load(path: &Path) -> Result<Self> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Self::new()),
            Err(e) => return Err(e).with_context(|| format_compact!("opening {}", path.display())),
        };
        let mut r = BufReader::new(file);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            bail!("{} is not a stats file", path.display());
        }
        let mut de = Deserializer::new();
        let mut stats = Self::new();
        for map in [&mut stats.exact, &mut stats.coarse] {
            for _ in 0..read_u32(&mut r)? {
                let mut key = vec![0u8; read_u32(&mut r)? as usize];
                r.read_exact(&mut key)?;
                let key = ArcStr::from(String::from_utf8(key).context("unit key")?);
                let mut hist = || de.deserialize(&mut r).map_err(|e| anyhow!("{e:?}"));
                let h = Hists { cost: hist()?, wall_ms: hist()?, mem_kb: hist()? };
                map.insert(key, h);
            }
        }
        stats.rebuild_global();
        Ok(stats)
    }

    pub fn save(&mut self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).with_context(|| format_compact!("creating {}", dir.display()))?;
        }
        let tmp = path.with_extension("tmp");
        let mut w = BufWriter::new(
            File::create(&tmp).with_context(|| format_compact!("creating {}", tmp.display()))?,
        );
        w.write_all(MAGIC)?;
        let mut ser = V2Serializer::new();
        for map in [&self.exact, &self.coarse] {
            w.write_all(&(map.len() as u32).to_be_bytes())?;
            for (key, h) in map {
                w.write_all(&(key.len() as u32).to_be_bytes())?;
                w.write_all(key.as_bytes())?;
                for hist in [&h.cost, &h.wall_ms, &h.mem_kb] {
                    ser.serialize(hist, &mut w).map_err(|e| anyhow!("{e:?}"))?;
                }
            }
        }
        w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        fs::rename(&tmp, path).with_context(|| format_compact!("writing {}", path.display()))?;
        self.dirty = false;
        Ok(())
    }
}

fn read_u32(r: &mut impl Read) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

#[cfg(test)]
mod test {
    use super::*;

    fn unit(exact: &str, coarse: &str) -> Unit {
        Unit { exact: ArcStr::from(exact), coarse: ArcStr::from(coarse) }
    }

    fn sample(cost_mcu: u64) -> Sample {
        Sample { cost_mcu, wall_ms: cost_mcu / 10, mem_kb: 1000 }
    }

    #[test]
    fn predicts_from_exact_then_coarse_then_global() {
        let mut s = Stats::new();
        assert_eq!(s.predict(&unit("a:1", "a")).cost_mcu, UNKNOWN_COST_MCU);
        for _ in 0..10 {
            s.record(&unit("heavy:1", "heavy"), sample(50_000));
            s.record(&unit("light:1", "light"), sample(20));
        }
        let heavy = s.predict(&unit("heavy:1", "heavy"));
        let light = s.predict(&unit("light:1", "light"));
        assert!((49_000..=51_000).contains(&heavy.cost_mcu), "{}", heavy.cost_mcu);
        assert!(heavy.percentile > 99., "{}", heavy.percentile);
        assert!(light.percentile <= 50., "{}", light.percentile);
        let other_profile = s.predict(&unit("heavy:2", "heavy"));
        assert_eq!(other_profile.cost_mcu, heavy.cost_mcu);
        assert!(s.predict(&unit("new:1", "new")).cost_mcu > 0);
    }

    #[test]
    fn halving_follows_a_growing_crate() {
        let mut s = Stats::new();
        let u = unit("grows:1", "grows");
        for _ in 0..200 {
            s.record(&u, sample(1000));
        }
        for _ in 0..600 {
            s.record(&u, sample(3000));
        }
        let p = s.predict(&u).cost_mcu;
        assert!((2950..=3050).contains(&p), "{p}");
        assert!(s.exact[&u.exact].cost.len() < HALVE_AT);
    }

    #[test]
    fn save_and_load_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-stats-{}", std::process::id()));
        let path = dir.join("jobs.hdr");
        let mut s = Stats::new();
        s.record(&unit("a:1", "a"), sample(1234));
        s.record(&unit("b:1", "b"), sample(99));
        s.save(&path)?;
        assert!(!s.dirty);
        let t = Stats::load(&path)?;
        assert_eq!(t.predict(&unit("a:1", "a")).cost_mcu, s.predict(&unit("a:1", "a")).cost_mcu);
        assert_eq!(t.global.cost.len(), 2);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
