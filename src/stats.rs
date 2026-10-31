use crate::protocol::Unit;
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
/// Predicted cost, in millions of instructions, of a job when nothing at
/// all has been recorded.
const UNKNOWN_COST_MI: u64 = 1000;
const MAGIC: &[u8; 8] = b"rdstats3";

/// Whose build a run was for and where it ran.
#[derive(Clone, Copy)]
pub enum Origin {
    /// This box's build, run here.
    Here,
    /// This box's build, run on another box.
    Sent,
    /// Another box's build, run here.
    Served,
}

pub struct Sample {
    /// Millions of instructions.
    pub cost_mi: u64,
    /// Those of the front end, if rustc printed an `.rmeta` line.
    pub front_mi: Option<u64>,
    /// Hundredths of the threads busy on average after the front end.
    pub width_c: u64,
    pub wall_ms: u64,
    pub mem_kb: u64,
    pub origin: Origin,
}

struct Hists {
    cost_mi: Histogram<u32>,
    front_mi: Histogram<u32>,
    width_c: Histogram<u32>,
    wall_ms: Histogram<u32>,
    mem_kb: Histogram<u32>,
    /// Runs by `Origin`, never halved.
    runs: [u64; 3],
}

impl Hists {
    fn new() -> Self {
        let h = |high| Histogram::new_with_bounds(1, high, 2).expect("histogram bounds");
        Self {
            cost_mi: h(1_000_000_000),
            front_mi: h(1_000_000_000),
            width_c: h(100_000),
            wall_ms: h(36_000_000),
            mem_kb: h(1 << 30),
            runs: [0; 3],
        }
    }

    fn all(&mut self) -> [&mut Histogram<u32>; 5] {
        [&mut self.cost_mi, &mut self.front_mi, &mut self.width_c, &mut self.wall_ms, &mut self.mem_kb]
    }

    fn record(&mut self, s: &Sample) {
        self.cost_mi.saturating_record(s.cost_mi.max(1));
        if let Some(f) = s.front_mi {
            self.front_mi.saturating_record(f.max(1));
        }
        self.width_c.saturating_record(s.width_c.max(1));
        self.wall_ms.saturating_record(s.wall_ms.max(1));
        self.mem_kb.saturating_record(s.mem_kb.max(1));
        self.runs[s.origin as usize] += 1;
    }

    fn halve(&mut self) {
        for h in self.all() {
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
        let theirs = [&other.cost_mi, &other.front_mi, &other.width_c, &other.wall_ms, &other.mem_kb];
        for (h, o) in self.all().into_iter().zip(theirs) {
            h.add(o).expect("same bounds");
        }
        for (r, o) in self.runs.iter_mut().zip(other.runs) {
            *r += o;
        }
    }
}

pub struct Prediction {
    /// Millions of instructions.
    pub cost_mi: u64,
    pub front_mi: Option<u64>,
    /// Hundredths of a thread; none until the unit or its crate has run.
    pub width_c: Option<u64>,
    pub mem_kb: u64,
    /// It comes from the unit's or its crate's own runs, not every job's.
    pub known: bool,
}

/// What `status` shows of a unit.
pub struct UnitInfo {
    /// Runs by `Origin`.
    pub runs: [u64; 3],
    pub cost_mi: u64,
    pub front_mi: Option<u64>,
    pub width_c: u64,
    pub wall_ms: u64,
    pub mem_kb: u64,
}

fn median(h: &Histogram<u32>) -> Option<u64> {
    (!h.is_empty()).then(|| h.value_at_quantile(0.5))
}

pub struct Stats {
    exact: HashMap<ArcStr, Hists>,
    coarse: HashMap<ArcStr, Hists>,
    /// Sum of the exact units, the prior for a unit with no history.
    global: Hists,
    pub dirty: bool,
}

impl Stats {
    pub fn new() -> Self {
        Self { exact: HashMap::new(), coarse: HashMap::new(), global: Hists::new(), dirty: false }
    }

    pub fn predict(&self, exact: &ArcStr, coarse: &ArcStr) -> Prediction {
        let own = self.exact.get(exact).or_else(|| self.coarse.get(coarse)).filter(|h| !h.cost_mi.is_empty());
        match own {
            Some(h) => Prediction {
                cost_mi: h.cost_mi.value_at_quantile(0.5),
                front_mi: median(&h.front_mi),
                width_c: median(&h.width_c),
                mem_kb: h.mem_kb.value_at_quantile(0.9),
                known: true,
            },
            None => Prediction {
                cost_mi: median(&self.global.cost_mi).unwrap_or(UNKNOWN_COST_MI),
                front_mi: None,
                width_c: None,
                mem_kb: median(&self.global.mem_kb).unwrap_or(0),
                known: false,
            },
        }
    }

    /// Every unit with runs.
    pub fn units(&self) -> impl Iterator<Item = (&ArcStr, UnitInfo)> {
        self.exact.iter().filter(|(_, h)| !h.cost_mi.is_empty()).map(|(key, h)| {
            let info = UnitInfo {
                runs: h.runs,
                cost_mi: h.cost_mi.value_at_quantile(0.5),
                front_mi: median(&h.front_mi),
                width_c: h.width_c.value_at_quantile(0.5),
                wall_ms: h.wall_ms.value_at_quantile(0.5),
                mem_kb: h.mem_kb.value_at_quantile(0.9),
            };
            (key, info)
        })
    }

    pub fn record(&mut self, unit: &Unit, s: Sample) {
        for (map, key) in [(&mut self.exact, &unit.exact), (&mut self.coarse, &unit.coarse)] {
            let h = map.entry(key.clone()).or_insert_with(Hists::new);
            h.record(&s);
            if h.cost_mi.len() >= HALVE_AT {
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
                let (cost_mi, front_mi, width_c) = (hist()?, hist()?, hist()?);
                let (wall_ms, mem_kb) = (hist()?, hist()?);
                let runs = [read_u64(&mut r)?, read_u64(&mut r)?, read_u64(&mut r)?];
                map.insert(key, Hists { cost_mi, front_mi, width_c, wall_ms, mem_kb, runs });
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
                for hist in [&h.cost_mi, &h.front_mi, &h.width_c, &h.wall_ms, &h.mem_kb] {
                    ser.serialize(hist, &mut w).map_err(|e| anyhow!("{e:?}"))?;
                }
                for n in h.runs {
                    w.write_all(&n.to_be_bytes())?;
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

fn read_u64(r: &mut impl Read) -> Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

#[cfg(test)]
mod test {
    use super::*;

    fn unit(exact: &str, coarse: &str) -> Unit {
        let (exact, coarse) = (ArcStr::from(exact), ArcStr::from(coarse));
        Unit { id: exact.clone(), exact, coarse, deps: vec![], codegen_units: 16 }
    }

    fn predict(s: &Stats, u: &Unit) -> Prediction {
        s.predict(&u.exact, &u.coarse)
    }

    fn sample(cost_mi: u64) -> Sample {
        let wall_ms = cost_mi / 10;
        Sample { cost_mi, front_mi: None, width_c: 100, wall_ms, mem_kb: 1000, origin: Origin::Here }
    }

    #[test]
    fn predicts_from_exact_then_coarse_then_global() {
        let mut s = Stats::new();
        assert_eq!(predict(&s, &unit("a:1", "a")).cost_mi, UNKNOWN_COST_MI);
        assert!(!predict(&s, &unit("a:1", "a")).known);
        for _ in 0..10 {
            s.record(&unit("heavy:1", "heavy"), sample(50_000));
            s.record(&unit("light:1", "light"), sample(20));
        }
        let heavy = predict(&s, &unit("heavy:1", "heavy"));
        assert_eq!((heavy.front_mi, heavy.width_c), (None, Some(100)));
        assert!((49_000..=51_000).contains(&heavy.cost_mi), "{}", heavy.cost_mi);
        let other_profile = predict(&s, &unit("heavy:2", "heavy"));
        assert_eq!(other_profile.cost_mi, heavy.cost_mi);
        assert!(heavy.known && other_profile.known);
        let new = predict(&s, &unit("new:1", "new"));
        assert!(new.cost_mi > 0 && !new.known);
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
        let p = predict(&s, &u).cost_mi;
        assert!((2950..=3050).contains(&p), "{p}");
        assert!(s.exact[&u.exact].cost_mi.len() < HALVE_AT);
    }

    #[test]
    fn save_and_load_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-stats-{}", std::process::id()));
        let path = dir.join("jobs.hdr");
        let mut s = Stats::new();
        s.record(&unit("a:1", "a"), sample(1234));
        s.record(&unit("a:1", "a"), Sample { origin: Origin::Served, ..sample(1234) });
        s.record(&unit("b:1", "b"), Sample { front_mi: Some(30), width_c: 400, ..sample(99) });
        s.save(&path)?;
        assert!(!s.dirty);
        let t = Stats::load(&path)?;
        assert_eq!(predict(&t, &unit("a:1", "a")).cost_mi, predict(&s, &unit("a:1", "a")).cost_mi);
        assert_eq!(t.global.cost_mi.len(), 3);
        let (_, a) = t.units().find(|(k, _)| k.as_str() == "a:1").unwrap();
        assert_eq!(a.runs, [1, 0, 1]);
        let (_, b) = t.units().find(|(k, _)| k.as_str() == "b:1").unwrap();
        // Histograms keep two significant digits.
        assert_eq!(b.front_mi, Some(30));
        assert!((396..=404).contains(&b.width_c), "{}", b.width_c);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
