//! The cost table. A unit's size is the work it takes, in millions of the
//! calibration load's instructions: how long it ran, times the calibrated
//! speed of where it ran, over that place's factor at the load it saw. A
//! place's factor, learned per load bucket from units that also ran
//! elsewhere or at other loads, scales calibrated time to time there.

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
    time::{SystemTime, UNIX_EPOCH},
};

/// A place's calibration counts as this many µs of runs in each load
/// bucket, and never decays.
const PRIOR_US: f64 = 30_000_000.;
/// Load buckets, in hundredths of a runnable task per CPU.
const BUCKETS: [u32; 11] = [0, 25, 50, 75, 100, 150, 200, 300, 400, 600, 800];
/// A unit's histograms are halved once it has this many runs, so its
/// peaks follow the crate as it grows.
const HALVE_AT: u64 = 256;
/// A cell unrun for this long, as long as the build graph keeps a node, is
/// dropped when the table is saved.
const KEEP_S: f64 = 60. * 86_400.;
/// Predicted size, in millions of instructions, of a job when nothing at
/// all has been recorded.
const UNKNOWN_COST_MI: u64 = 1000;
const MAGIC: &[u8; 8] = b"rdstats6";
/// A table from before copy times decayed on their own, whose cells count
/// no sent runs.
const UNSENT_MAGIC: &[u8; 8] = b"rdstats5";
/// A table from before runs were timed, read as seed history.
const OLD_MAGIC: &[u8; 8] = b"rdstats4";
/// Where seed history ran: calibrated at one instruction per µs, so its
/// work is what was recorded, and never learning.
const SEED: &str = "seed";

/// Seconds since the epoch, which weights decay by.
pub fn now_s() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0., |d| d.as_secs_f64())
}

/// What a weight set at `at` is worth at `now`, halving every
/// `half_life_s`.
fn keep(at: f64, now: f64, half_life_s: f64) -> f64 {
    0.5f64.powf((now - at).max(0.) / half_life_s)
}

/// How fast old runs stop counting. Their weight halves every
/// `half_life_s` for how fast places run and how big units are, and every
/// `copy_half_life_s` for how long sent runs spent moving files, which
/// follows the network a laptop is on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decay {
    pub half_life_s: f64,
    pub copy_half_life_s: f64,
}

impl Default for Decay {
    fn default() -> Self {
        Self { half_life_s: 86_400., copy_half_life_s: 3_600. }
    }
}

/// The factor a place starts from: its calibrated speed up to one task per
/// CPU, then slower as CPUs time-share.
fn prior(load_c: u32) -> f64 {
    (load_c as f64 / 100.).max(1.)
}

/// The bucket below `load_c` and how far it is toward the next, or the
/// last bucket if `load_c` is past it.
fn between(load_c: u32) -> (usize, f64) {
    let last = BUCKETS.len() - 1;
    if load_c >= BUCKETS[last] {
        return (last, 0.);
    }
    let i = BUCKETS.partition_point(|&b| b <= load_c) - 1;
    (i, (load_c - BUCKETS[i]) as f64 / (BUCKETS[i + 1] - BUCKETS[i]) as f64)
}

fn nearest(load_c: u32) -> u8 {
    let (i, a) = between(load_c);
    (if a < 0.5 { i } else { i + 1 }) as u8
}

/// Whose build a run was for and where it ran.
#[derive(Clone, Copy)]
pub enum Origin {
    /// This box's build, run here.
    Here,
    /// This box's build, run on another box.
    Sent(Transfer),
    /// Another box's build, run here.
    Served,
}

impl Origin {
    fn index(self) -> usize {
        match self {
            Origin::Here => 0,
            Origin::Sent(_) => 1,
            Origin::Served => 2,
        }
    }
}

/// What moving a sent run's files cost.
#[derive(Clone, Copy)]
pub struct Transfer {
    pub sent_kb: u64,
    /// The time it spent other than compiling: staging and sending inputs,
    /// its linker waiting for rlibs, receiving the last outputs.
    pub copy_ms: u64,
}

/// How a run went.
pub struct Run {
    pub place: PlaceId,
    /// Hundredths of a runnable task per CPU, on average while it ran.
    pub load_c: u32,
    /// From start to exit, less any time its linker waited for files.
    pub wall_us: u64,
    /// Until rustc printed its `.rmeta` line, if it did.
    pub front_us: Option<u64>,
    /// The jobserver tokens it held, times µs.
    pub tokens_us: u64,
    pub mem_kb: u64,
    pub origin: Origin,
}

/// A place's index in the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceId(u32);

/// Decayed sums of a place's runs near one load: their time, and what it
/// would have been at the place's calibrated speed.
#[derive(Clone, Copy, Default)]
struct Bucket {
    at: f64,
    time_us: f64,
    calibrated_us: f64,
}

impl Bucket {
    fn factor(&self, prior: f64, now: f64, half_life_s: f64) -> f64 {
        let k = keep(self.at, now, half_life_s);
        (PRIOR_US * prior + self.time_us * k) / (PRIOR_US + self.calibrated_us * k)
    }

    fn add(&mut self, w: f64, time_us: f64, calibrated_us: f64, now: f64, half_life_s: f64) {
        let k = keep(self.at, now, half_life_s);
        self.time_us = self.time_us * k + w * time_us;
        self.calibrated_us = self.calibrated_us * k + w * calibrated_us;
        self.at = now;
    }
}

/// Decayed runs sent to a place and the µs they spent other than
/// compiling.
#[derive(Clone, Copy, Default)]
struct Copies {
    at: f64,
    runs: f64,
    copy_us: f64,
}

/// A class of a box, where light jobs run, or all of a box's CPUs, where
/// heavy jobs run.
struct Place {
    key: ArcStr,
    /// Calibrated instructions per µs: of its fastest class, where a front
    /// end runs, and per CPU.
    fast: u64,
    rate: u64,
    curve: [Bucket; BUCKETS.len()],
    copies: Copies,
    /// The factor at each bucket's load, as of the last `refresh`.
    factors: [f64; BUCKETS.len()],
}

impl Place {
    fn new(key: ArcStr, fast: u64, rate: u64) -> Self {
        let mut p = Self {
            key,
            fast: fast.max(1),
            rate: rate.max(1),
            curve: Default::default(),
            copies: Copies::default(),
            factors: [1.; BUCKETS.len()],
        };
        p.refresh(0., 1.);
        p
    }

    /// Work out the factor at each bucket's load as of `now`. A bucket
    /// starts from the starting curve at the level the place has run at
    /// over every load, so one nothing has run at follows the ones that
    /// have.
    fn refresh(&mut self, now: f64, half_life_s: f64) {
        let (mut time_us, mut calibrated_us) = (PRIOR_US, PRIOR_US);
        for (b, &load_c) in self.curve.iter().zip(&BUCKETS) {
            let k = keep(b.at, now, half_life_s);
            time_us += b.time_us * k / prior(load_c);
            calibrated_us += b.calibrated_us * k;
        }
        let level = time_us / calibrated_us;
        for ((f, b), &load_c) in self.factors.iter_mut().zip(&self.curve).zip(&BUCKETS) {
            *f = b.factor(prior(load_c) * level, now, half_life_s);
        }
    }

    /// What its calibrated time is multiplied by under `load_c`.
    fn factor(&self, load_c: u32) -> f64 {
        let last = BUCKETS.len() - 1;
        match between(load_c) {
            (i, _) if i == last => self.factors[last] * load_c as f64 / BUCKETS[last] as f64,
            (i, a) => self.factors[i] * (1. - a) + self.factors[i + 1] * a,
        }
    }

    /// A run under `load_c` took `time_us` where its calibrated time was
    /// `calibrated_us`, and weighs `w`.
    fn learn(&mut self, load_c: u32, w: f64, time_us: f64, calibrated_us: f64, now: f64, half_life_s: f64) {
        let last = BUCKETS.len() - 1;
        match between(load_c) {
            (i, _) if i == last => {
                let at_last = time_us * BUCKETS[last] as f64 / load_c as f64;
                self.curve[last].add(w, at_last, calibrated_us, now, half_life_s);
            }
            (i, a) => {
                self.curve[i].add(w * (1. - a), time_us, calibrated_us, now, half_life_s);
                self.curve[i + 1].add(w * a, time_us, calibrated_us, now, half_life_s);
            }
        }
        self.refresh(now, half_life_s);
    }
}

/// Decayed sums of a unit's runs at one place and load bucket. Work is
/// counted at the place's calibrated speed: the place's factor at the
/// cell's load divides it into the unit's size.
#[derive(Clone, Copy)]
struct Cell {
    place: PlaceId,
    bucket: u8,
    at: f64,
    runs: f64,
    load_c: f64,
    front_mi: f64,
    rest_mi: f64,
    rest_us: f64,
    tokens_us: f64,
    /// Sent runs and the µs they spent other than compiling, which decay at
    /// the copy half-life.
    sent: f64,
    copy_us: f64,
}

impl Cell {
    /// The sums that decay at the half-life for speed and size.
    fn speed(&mut self) -> [&mut f64; 6] {
        [&mut self.runs, &mut self.load_c, &mut self.front_mi, &mut self.rest_mi, &mut self.rest_us, &mut self.tokens_us]
    }

    fn copies(&mut self) -> [&mut f64; 2] {
        [&mut self.sent, &mut self.copy_us]
    }

    fn add(&mut self, mut other: Cell, now: f64, decay: Decay) {
        let (k, kc) = (keep(self.at, now, decay.half_life_s), keep(self.at, now, decay.copy_half_life_s));
        for (s, o) in self.speed().into_iter().zip(other.speed()) {
            *s = *s * k + *o;
        }
        for (s, o) in self.copies().into_iter().zip(other.copies()) {
            *s = *s * kc + *o;
        }
        self.at = now;
    }
}

/// A unit's size from some of its cells: decayed runs, their front-end and
/// rest work divided by their places' factors, and their rest's wall time
/// and tokens.
#[derive(Default)]
struct Size {
    runs: f64,
    front_mi: f64,
    rest_mi: f64,
    rest_us: f64,
    tokens_us: f64,
}

impl Size {
    fn front_mi(&self) -> f64 {
        self.front_mi / self.runs
    }

    fn rest_mi(&self) -> f64 {
        self.rest_mi / self.runs
    }

    /// Tokens held on average after the front end: 1 for a job with nothing
    /// after it.
    fn width(&self) -> f64 {
        if self.rest_us > 0. { (self.tokens_us / self.rest_us).max(1.) } else { 1. }
    }
}

struct Record {
    cells: Vec<Cell>,
    mem_kb: Histogram<u32>,
    wall_ms: Histogram<u32>,
    /// Of sent runs.
    copy_ms: Histogram<u32>,
    sent_kb: Histogram<u32>,
    /// Runs by `Origin`, never halved.
    runs: [u64; 3],
}

impl Record {
    fn new() -> Self {
        let h = |high| Histogram::new_with_bounds(1, high, 2).expect("histogram bounds");
        Self {
            cells: Vec::new(),
            mem_kb: h(1 << 30),
            wall_ms: h(36_000_000),
            copy_ms: h(36_000_000),
            sent_kb: h(1 << 30),
            runs: [0; 3],
        }
    }

    fn each(&self) -> [&Histogram<u32>; 4] {
        [&self.mem_kb, &self.wall_ms, &self.copy_ms, &self.sent_kb]
    }

    fn all(&mut self) -> [&mut Histogram<u32>; 4] {
        [&mut self.mem_kb, &mut self.wall_ms, &mut self.copy_ms, &mut self.sent_kb]
    }

    fn add(&mut self, cell: Cell, now: f64, decay: Decay) {
        match self.cells.iter_mut().find(|c| (c.place, c.bucket) == (cell.place, cell.bucket)) {
            Some(c) => c.add(cell, now, decay),
            None => self.cells.push(cell),
        }
    }

    fn count(&mut self, mem_kb: u64, wall_ms: u64, origin: Origin) {
        self.mem_kb.saturating_record(mem_kb.max(1));
        self.wall_ms.saturating_record(wall_ms.max(1));
        if let Origin::Sent(t) = origin {
            self.copy_ms.saturating_record(t.copy_ms.max(1));
            self.sent_kb.saturating_record(t.sent_kb.max(1));
        }
        self.runs[origin.index()] += 1;
        if self.mem_kb.len() >= HALVE_AT {
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
    }

    /// Its size from every cell but `skip`'s.
    fn size(&self, places: &[Place], now: f64, half_life_s: f64, skip: Option<(PlaceId, u8)>) -> Size {
        let mut s = Size::default();
        for c in self.cells.iter().filter(|c| Some((c.place, c.bucket)) != skip && c.runs > 0.) {
            let k = keep(c.at, now, half_life_s);
            let m = places[c.place.0 as usize].factor((c.load_c / c.runs) as u32);
            s.runs += c.runs * k;
            s.front_mi += c.front_mi * k / m;
            s.rest_mi += c.rest_mi * k / m;
            s.rest_us += c.rest_us * k;
            s.tokens_us += c.tokens_us * k;
        }
        s
    }
}

pub struct Prediction {
    /// Millions of the calibration load's instructions: its front end's,
    /// on one thread, and the rest's, spread over its width.
    pub front_mi: u64,
    pub rest_mi: u64,
    /// Hundredths of the threads busy after the front end; none until the
    /// unit or its crate has run.
    pub width_c: Option<u64>,
    pub mem_kb: u64,
    /// It comes from the unit's or its crate's own runs, not every job's.
    pub known: bool,
}

impl Prediction {
    pub fn cost_mi(&self) -> u64 {
        self.front_mi + self.rest_mi
    }
}

/// What `status` shows of a unit.
pub struct UnitInfo {
    /// Runs by `Origin`.
    pub runs: [u64; 3],
    pub cost_mi: u64,
    /// None if no run printed an `.rmeta` line.
    pub front_mi: Option<u64>,
    pub width_c: u64,
    pub wall_ms: u64,
    pub mem_kb: u64,
    /// Medians of sent runs.
    pub copy_ms: Option<u64>,
    pub sent_kb: Option<u64>,
}

fn median(h: &Histogram<u32>) -> Option<u64> {
    (!h.is_empty()).then(|| h.value_at_quantile(0.5))
}

fn histogram(high: u64) -> Histogram<u32> {
    Histogram::new_with_bounds(1, high, 2).expect("histogram bounds")
}

pub struct Stats {
    places: Vec<Place>,
    by_key: HashMap<ArcStr, PlaceId>,
    exact: HashMap<ArcStr, Record>,
    coarse: HashMap<ArcStr, Record>,
    /// Every unit's size and peak, the prior for a unit with no history.
    global_cost: Histogram<u32>,
    global_mem: Histogram<u32>,
    /// Seconds since the epoch, which weights decay to: see `set_now`.
    now: f64,
    decay: Decay,
    pub dirty: bool,
}

impl Stats {
    pub fn new(decay: Decay) -> Self {
        let mut stats = Self {
            places: Vec::new(),
            by_key: HashMap::new(),
            exact: HashMap::new(),
            coarse: HashMap::new(),
            global_cost: histogram(1_000_000_000),
            global_mem: histogram(1 << 30),
            now: now_s(),
            decay,
            dirty: false,
        };
        stats.place(SEED, 1, 1);
        stats
    }

    /// Decay every weight to `now`, seconds since the epoch.
    pub fn set_now(&mut self, now: f64) {
        self.now = now;
        for p in &mut self.places {
            p.refresh(now, self.decay.half_life_s);
        }
    }

    /// The place `key`, calibrated at `fast` instructions per µs for a
    /// front end and `rate` per CPU. A place whose calibration changed
    /// starts over from it.
    pub fn place(&mut self, key: &str, fast: u64, rate: u64) -> PlaceId {
        if let Some(&id) = self.by_key.get(key) {
            let p = &mut self.places[id.0 as usize];
            if (p.fast, p.rate) != (fast.max(1), rate.max(1)) {
                *p = Place::new(p.key.clone(), fast, rate);
                self.dirty = true;
            }
            return id;
        }
        let id = PlaceId(self.places.len() as u32);
        let key = ArcStr::from(key);
        self.places.push(Place::new(key.clone(), fast, rate));
        self.by_key.insert(key, id);
        self.dirty = true;
        id
    }

    /// What `place`'s calibrated time is multiplied by under `load_c`.
    pub fn factor(&self, place: PlaceId, load_c: u32) -> f64 {
        self.places[place.0 as usize].factor(load_c)
    }

    /// Predicted µs at `place` under `load_c`, the rest after its front end
    /// spread over `threads`.
    pub fn time_us(&self, pred: &Prediction, place: PlaceId, load_c: u32, threads: f64) -> u64 {
        let p = &self.places[place.0 as usize];
        let front = pred.front_mi as f64 * 1e6 / p.fast as f64;
        let rest = pred.rest_mi as f64 * 1e6 / (p.rate as f64 * threads.max(1.));
        ((front + rest) * p.factor(load_c)) as u64
    }

    /// Predicted µs a run of `unit` sent to `place` spends other than
    /// compiling: from its own sends there, else every unit's, if any.
    pub fn copy_us(&self, unit: &Unit, place: PlaceId) -> Option<u64> {
        let (now, half_life_s) = (self.now, self.decay.copy_half_life_s);
        let (mut sent, mut copy_us) = (0., 0.);
        for c in self.exact.get(&unit.exact).iter().flat_map(|r| &r.cells).filter(|c| c.place == place) {
            let k = keep(c.at, now, half_life_s);
            sent += c.sent * k;
            copy_us += c.copy_us * k;
        }
        if sent <= 0. {
            let c = &self.places[place.0 as usize].copies;
            let k = keep(c.at, now, half_life_s);
            (sent, copy_us) = (c.runs * k, c.copy_us * k);
        }
        (sent > 0.).then(|| (copy_us / sent) as u64)
    }

    pub fn predict(&self, exact: &ArcStr, coarse: &ArcStr) -> Prediction {
        let own = [(&self.exact, exact), (&self.coarse, coarse)].into_iter().find_map(|(map, key)| {
            let r = map.get(key)?;
            let s = r.size(&self.places, self.now, self.decay.half_life_s, None);
            (s.runs > 0.).then_some((r, s))
        });
        match own {
            Some((r, s)) => Prediction {
                front_mi: s.front_mi() as u64,
                rest_mi: s.rest_mi() as u64,
                width_c: Some((s.width() * 100.) as u64),
                mem_kb: r.mem_kb.value_at_quantile(0.9),
                known: true,
            },
            None => Prediction {
                front_mi: 0,
                rest_mi: median(&self.global_cost).unwrap_or(UNKNOWN_COST_MI),
                width_c: None,
                mem_kb: median(&self.global_mem).unwrap_or(0),
                known: false,
            },
        }
    }

    /// Every unit with runs.
    pub fn units(&self) -> impl Iterator<Item = (&ArcStr, UnitInfo)> {
        self.exact.iter().filter_map(|(key, r)| {
            let s = r.size(&self.places, self.now, self.decay.half_life_s, None);
            (s.runs > 0.).then(|| {
                let front_mi = s.front_mi() as u64;
                let info = UnitInfo {
                    runs: r.runs,
                    cost_mi: front_mi + s.rest_mi() as u64,
                    front_mi: (front_mi > 0).then_some(front_mi),
                    width_c: (s.width() * 100.) as u64,
                    wall_ms: median(&r.wall_ms).unwrap_or(0),
                    mem_kb: r.mem_kb.value_at_quantile(0.9),
                    copy_ms: median(&r.copy_ms),
                    sent_kb: median(&r.sent_kb),
                };
                (key, info)
            })
        })
    }

    /// Add a run of `unit`. If the unit has run elsewhere, or under other
    /// load, the run also teaches its place's factor at its load.
    pub fn record(&mut self, unit: &Unit, run: &Run) {
        let now = self.now;
        let p = &self.places[run.place.0 as usize];
        let (fast, rate) = (p.fast as f64, p.rate as f64);
        let wall_us = run.wall_us as f64;
        let front_us = run.front_us.map_or(0., |f| f as f64).min(wall_us);
        let rest_us = wall_us - front_us;
        // The front end holds the one token the job starts with.
        let tokens_us = (run.tokens_us as f64 - front_us).max(rest_us);
        let bucket = nearest(run.load_c);
        if let Some(r) = self.exact.get(&unit.exact) {
            let other = r.size(&self.places, now, self.decay.half_life_s, Some((run.place, bucket)));
            if other.runs > 0. {
                let threads = if rest_us > 0. { tokens_us / rest_us } else { 1. };
                let calibrated_us = other.front_mi() * 1e6 / fast + other.rest_mi() * 1e6 / (rate * threads);
                let w = other.runs / (other.runs + 1.);
                let half_life_s = self.decay.half_life_s;
                self.places[run.place.0 as usize].learn(run.load_c, w, wall_us, calibrated_us, now, half_life_s);
            }
        }
        let (sent, copy_us) = match run.origin {
            Origin::Sent(t) => (1., t.copy_ms as f64 * 1000.),
            Origin::Here | Origin::Served => (0., 0.),
        };
        let cell = Cell {
            place: run.place,
            bucket,
            at: now,
            runs: 1.,
            load_c: run.load_c as f64,
            front_mi: front_us * fast / 1e6,
            rest_mi: tokens_us * rate / 1e6,
            rest_us,
            tokens_us,
            sent,
            copy_us,
        };
        let decay = self.decay;
        for (map, key) in [(&mut self.exact, &unit.exact), (&mut self.coarse, &unit.coarse)] {
            let r = map.entry(key.clone()).or_insert_with(Record::new);
            r.add(cell, now, decay);
            r.count(run.mem_kb, run.wall_us / 1000, run.origin);
        }
        let place = &mut self.places[run.place.0 as usize];
        if let Origin::Sent(_) = run.origin {
            let c = &mut place.copies;
            let k = keep(c.at, now, decay.copy_half_life_s);
            (c.runs, c.copy_us, c.at) = (c.runs * k + 1., c.copy_us * k + copy_us, now);
        }
        let m = place.factor(run.load_c);
        self.global_cost.saturating_record(((cell.front_mi + cell.rest_mi) / m).max(1.) as u64);
        self.global_mem.saturating_record(run.mem_kb.max(1));
        self.dirty = true;
    }

    /// History for `unit`, as an old table's would be: `front_mi` and
    /// `rest_mi` of work, `width_c` hundredths of a thread wide.
    #[cfg(test)]
    pub fn seed(&mut self, unit: &Unit, front_mi: u64, rest_mi: u64, width_c: u64, mem_kb: u64) {
        let cell = seed_cell(front_mi, rest_mi, width_c, self.now);
        for (map, key) in [(&mut self.exact, &unit.exact), (&mut self.coarse, &unit.coarse)] {
            let r = map.entry(key.clone()).or_insert_with(Record::new);
            r.add(cell, self.now, self.decay);
            r.mem_kb.saturating_record(mem_kb.max(1));
        }
        self.global_cost.saturating_record((front_mi + rest_mi).max(1));
        self.global_mem.saturating_record(mem_kb.max(1));
        self.dirty = true;
    }

    /// Drop cells that have decayed away, and recompute the global
    /// histograms from every unit's size now.
    pub fn rebuild_global(&mut self) {
        let now = self.now;
        let (mut cost, mut mem) = (histogram(1_000_000_000), histogram(1 << 30));
        for r in self.exact.values_mut().chain(self.coarse.values_mut()) {
            r.cells.retain(|c| now - c.at <= KEEP_S);
        }
        for r in self.exact.values() {
            let s = r.size(&self.places, now, self.decay.half_life_s, None);
            if s.runs > 0. {
                cost.saturating_record(((s.front_mi + s.rest_mi) / s.runs).max(1.) as u64);
                mem.saturating_record(r.mem_kb.value_at_quantile(0.9).max(1));
            }
        }
        (self.global_cost, self.global_mem) = (cost, mem);
    }

    /// A missing file is an empty table.
    pub fn load(path: &Path, decay: Decay) -> Result<Self> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Self::new(decay)),
            Err(e) => return Err(e).with_context(|| format_compact!("opening {}", path.display())),
        };
        let mut r = BufReader::new(file);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        let mut stats = match &magic {
            MAGIC => Self::read(&mut r, decay, true)?,
            UNSENT_MAGIC => Self::read(&mut r, decay, false)?,
            OLD_MAGIC => Self::read_old(&mut r, decay)?,
            _ => bail!("{} is not a stats file", path.display()),
        };
        stats.set_now(now_s());
        stats.rebuild_global();
        Ok(stats)
    }

    /// Without `sent`, a cell's runs count as sent if they spent time
    /// moving files.
    fn read(r: &mut impl Read, decay: Decay, sent: bool) -> Result<Self> {
        let mut stats = Self::new(decay);
        stats.places.clear();
        stats.by_key.clear();
        for i in 0..read_u32(r)? {
            let key = read_key(r)?;
            let (fast, rate) = (read_u64(r)?, read_u64(r)?);
            let mut p = Place::new(key.clone(), fast, rate);
            for b in &mut p.curve {
                *b = Bucket { at: read_f64(r)?, time_us: read_f64(r)?, calibrated_us: read_f64(r)? };
            }
            p.copies = Copies { at: read_f64(r)?, runs: read_f64(r)?, copy_us: read_f64(r)? };
            stats.places.push(p);
            stats.by_key.insert(key, PlaceId(i));
        }
        if !stats.by_key.contains_key(SEED) {
            bail!("no seed place");
        }
        let mut de = Deserializer::new();
        let places = stats.places.len() as u32;
        for map in [&mut stats.exact, &mut stats.coarse] {
            for _ in 0..read_u32(r)? {
                let key = read_key(r)?;
                let mut rec = Record::new();
                for _ in 0..read_u32(r)? {
                    let place = read_u32(r)?;
                    if place >= places {
                        bail!("cell of {key} at unknown place {place}");
                    }
                    let mut bucket = [0u8];
                    r.read_exact(&mut bucket)?;
                    let mut cell = Cell {
                        place: PlaceId(place),
                        bucket: bucket[0],
                        at: read_f64(r)?,
                        runs: 0.,
                        load_c: 0.,
                        front_mi: 0.,
                        rest_mi: 0.,
                        rest_us: 0.,
                        tokens_us: 0.,
                        sent: 0.,
                        copy_us: 0.,
                    };
                    for s in cell.speed() {
                        *s = read_f64(r)?;
                    }
                    if sent {
                        cell.sent = read_f64(r)?;
                    }
                    cell.copy_us = read_f64(r)?;
                    if !sent && cell.copy_us > 0. {
                        cell.sent = cell.runs;
                    }
                    rec.cells.push(cell);
                }
                for h in rec.all() {
                    *h = de.deserialize(r).map_err(|e| anyhow!("{e:?}"))?;
                }
                for n in &mut rec.runs {
                    *n = read_u64(r)?;
                }
                map.insert(key, rec);
            }
        }
        Ok(stats)
    }

    /// A table of instruction counts: each unit's medians become a seed
    /// cell, and its other histograms carry over.
    fn read_old(r: &mut impl Read, decay: Decay) -> Result<Self> {
        let mut stats = Self::new(decay);
        let mut de = Deserializer::new();
        let now = stats.now;
        for map in [&mut stats.exact, &mut stats.coarse] {
            for _ in 0..read_u32(r)? {
                let key = read_key(r)?;
                // cost, front, width, wall, mem, copy, sent
                let mut old: [Histogram<u32>; 7] = std::array::from_fn(|_| histogram(1 << 30));
                for h in &mut old {
                    *h = de.deserialize(r).map_err(|e| anyhow!("{e:?}"))?;
                }
                let mut rec = Record::new();
                for n in &mut rec.runs {
                    *n = read_u64(r)?;
                }
                let [cost, front, width, wall, mem, copy, sent] = old;
                if let (Some(cost_mi), Some(width_c)) = (median(&cost), median(&width)) {
                    let front_mi = median(&front).unwrap_or(0).min(cost_mi);
                    rec.cells.push(seed_cell(front_mi, cost_mi - front_mi, width_c, now));
                }
                (rec.mem_kb, rec.wall_ms, rec.copy_ms, rec.sent_kb) = (mem, wall, copy, sent);
                map.insert(key, rec);
            }
        }
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
        w.write_all(&(self.places.len() as u32).to_be_bytes())?;
        for p in &self.places {
            write_key(&mut w, &p.key)?;
            w.write_all(&p.fast.to_be_bytes())?;
            w.write_all(&p.rate.to_be_bytes())?;
            for b in &p.curve {
                for x in [b.at, b.time_us, b.calibrated_us] {
                    w.write_all(&x.to_be_bytes())?;
                }
            }
            for x in [p.copies.at, p.copies.runs, p.copies.copy_us] {
                w.write_all(&x.to_be_bytes())?;
            }
        }
        let mut ser = V2Serializer::new();
        for map in [&mut self.exact, &mut self.coarse] {
            w.write_all(&(map.len() as u32).to_be_bytes())?;
            for (key, rec) in map.iter_mut() {
                write_key(&mut w, key)?;
                w.write_all(&(rec.cells.len() as u32).to_be_bytes())?;
                for cell in &mut rec.cells {
                    w.write_all(&cell.place.0.to_be_bytes())?;
                    w.write_all(&[cell.bucket])?;
                    w.write_all(&cell.at.to_be_bytes())?;
                    for s in cell.speed() {
                        w.write_all(&s.to_be_bytes())?;
                    }
                    for s in cell.copies() {
                        w.write_all(&s.to_be_bytes())?;
                    }
                }
                for h in rec.each() {
                    ser.serialize(h, &mut w).map_err(|e| anyhow!("{e:?}"))?;
                }
                for n in rec.runs {
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

fn seed_cell(front_mi: u64, rest_mi: u64, width_c: u64, now: f64) -> Cell {
    let rest_us = 1e6;
    Cell {
        place: PlaceId(0),
        bucket: 0,
        at: now,
        runs: 1.,
        load_c: 0.,
        front_mi: front_mi as f64,
        rest_mi: rest_mi as f64,
        rest_us,
        tokens_us: rest_us * width_c as f64 / 100.,
        sent: 0.,
        copy_us: 0.,
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

fn read_f64(r: &mut impl Read) -> Result<f64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_be_bytes(b))
}

fn read_key(r: &mut impl Read) -> Result<ArcStr> {
    let mut key = vec![0u8; read_u32(r)? as usize];
    r.read_exact(&mut key)?;
    Ok(ArcStr::from(String::from_utf8(key).context("key")?))
}

fn write_key(w: &mut impl Write, key: &str) -> Result<()> {
    w.write_all(&(key.len() as u32).to_be_bytes())?;
    Ok(w.write_all(key.as_bytes())?)
}

#[cfg(test)]
mod test {
    use super::*;

    fn unit(exact: &str, coarse: &str) -> Unit {
        let (exact, coarse) = (ArcStr::from(exact), ArcStr::from(coarse));
        Unit { id: exact.clone(), dir: None, exact, coarse, deps: vec![], codegen_units: 16 }
    }

    fn predict(s: &Stats, u: &Unit) -> Prediction {
        s.predict(&u.exact, &u.coarse)
    }

    /// A single-threaded run at `place` under `load_c` taking `ms`.
    fn run(place: PlaceId, load_c: u32, ms: u64) -> Run {
        let wall_us = ms * 1000;
        Run { place, load_c, wall_us, front_us: None, tokens_us: wall_us, mem_kb: 1000, origin: Origin::Here }
    }

    fn near(x: f64, want: f64) -> bool {
        (x - want).abs() <= want * 0.02
    }

    #[test]
    fn predicts_from_exact_then_coarse_then_global() {
        let mut s = Stats::new(Decay::default());
        assert_eq!(predict(&s, &unit("a:1", "a")).cost_mi(), UNKNOWN_COST_MI);
        assert!(!predict(&s, &unit("a:1", "a")).known);
        // 1000 instructions per µs: a second is 1000 M.
        let here = s.place("here/0", 1000, 1000);
        for _ in 0..10 {
            s.record(&unit("heavy:1", "heavy"), &run(here, 0, 50_000));
            s.record(&unit("light:1", "light"), &run(here, 0, 20));
        }
        let heavy = predict(&s, &unit("heavy:1", "heavy"));
        assert_eq!(heavy.width_c, Some(100));
        assert!(near(heavy.cost_mi() as f64, 50_000.), "{}", heavy.cost_mi());
        let other_profile = predict(&s, &unit("heavy:2", "heavy"));
        assert_eq!(other_profile.cost_mi(), heavy.cost_mi());
        assert!(heavy.known && other_profile.known);
        let new = predict(&s, &unit("new:1", "new"));
        assert!(new.cost_mi() > 0 && !new.known);
    }

    #[test]
    fn the_rest_is_work_spread_over_the_tokens_it_held() {
        let mut s = Stats::new(Decay::default());
        let here = s.place("here/all", 2000, 1000);
        // 1 s: the front end 0.4 s, the rest 0.6 s holding 4 tokens.
        let r = Run { front_us: Some(400_000), tokens_us: 400_000 + 2_400_000, ..run(here, 0, 1000) };
        s.record(&unit("a:1", "a"), &r);
        let p = predict(&s, &unit("a:1", "a"));
        assert_eq!((p.front_mi, p.rest_mi, p.width_c), (800, 2400, Some(400)));
        assert_eq!(s.time_us(&p, here, 0, 4.), 1_000_000);
        assert_eq!(s.time_us(&p, here, 0, 2.), 1_600_000, "half the threads, twice the rest");
    }

    #[test]
    fn a_place_slower_on_the_same_units_learns_a_larger_factor() {
        let mut s = Stats::new(Decay::default());
        let (a, b, c) = (s.place("a/0", 1000, 1000), s.place("b/0", 1000, 1000), s.place("c/0", 1000, 1000));
        // b takes twice as long as a on the same unit; c only ever runs
        // its own unit, which says nothing about it.
        for _ in 0..200 {
            s.record(&unit("u:1", "u"), &run(a, 0, 1000));
            s.record(&unit("u:1", "u"), &run(b, 0, 2000));
            s.record(&unit("v:1", "v"), &run(c, 0, 5000));
        }
        // Only the ratio is known: a's units could all be bigger, and a
        // faster.
        let ratio = s.factor(b, 0) / s.factor(a, 0);
        assert!(ratio > 1.8, "{ratio}");
        assert!(near(s.factor(c, 0), 1.), "{}", s.factor(c, 0));
        let u = predict(&s, &unit("u:1", "u"));
        let (at_a, at_b) = (s.time_us(&u, a, 0, 1.) as f64, s.time_us(&u, b, 0, 1.) as f64);
        assert!((at_a - 1e6).abs() < 5e4 && (at_b - 2e6).abs() < 1e5, "{at_a} {at_b}");
        // Load past one task per CPU starts slower, as CPUs time-share.
        assert!(near(s.factor(c, 300), 3.), "{}", s.factor(c, 300));
        assert!(near(s.factor(c, 1600), 16.), "{}", s.factor(c, 1600));
    }

    #[test]
    fn the_same_unit_under_more_load_teaches_the_curve() {
        let mut s = Stats::new(Decay::default());
        let a = s.place("a/0", 1000, 1000);
        // Three times as long at 3/4 of a task per CPU as idle.
        for _ in 0..200 {
            s.record(&unit("u:1", "u"), &run(a, 0, 1000));
            s.record(&unit("u:1", "u"), &run(a, 75, 3000));
        }
        let (idle, busy) = (s.factor(a, 0), s.factor(a, 75));
        assert!(busy / idle > 2.5, "{idle} {busy}");
        // Nothing ran at half a task per CPU: its bucket starts from the
        // level the others learned, not from the calibration.
        let half = s.factor(a, 50);
        assert!(idle < half && half < busy, "{idle} {half} {busy}");
    }

    #[test]
    fn weights_halve_every_half_life() {
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/0", 1000, 1000));
        s.set_now(0.);
        // 60 s at b against 30 at a: b's bucket holds 60 s of time for
        // about 30 of calibrated time, beside the prior's 30.
        s.record(&unit("u:1", "u"), &run(a, 0, 30_000));
        s.record(&unit("u:1", "u"), &run(b, 0, 60_000));
        let fresh = s.factor(b, 0);
        s.set_now(Decay::default().half_life_s);
        let old = s.factor(b, 0);
        assert!(fresh > old && old > 1., "{fresh} {old}");
        s.set_now(Decay::default().half_life_s * 30.);
        assert!(near(s.factor(b, 0), 1.), "back to its calibration");
    }

    #[test]
    fn copy_times_follow_their_own_shorter_half_life() {
        let mut s = Stats::new(Decay::default());
        let there = s.place("b/all", 1000, 1000);
        let sent = |copy_ms| Run { origin: Origin::Sent(Transfer { sent_kb: 1, copy_ms }), ..run(there, 0, 100) };
        // 10 s to move its files on a slow link, then 1 s two hours later:
        // the old one counts a quarter.
        s.set_now(0.);
        s.record(&unit("a:1", "a"), &sent(10_000));
        s.set_now(2. * 3600.);
        s.record(&unit("a:1", "a"), &sent(1000));
        let want = (10. * 0.25 + 1.) / 1.25 * 1e6;
        let got = s.copy_us(&unit("a:1", "a"), there).unwrap() as f64;
        assert!((got - want).abs() < want * 0.01, "{got} {want}");
        let every = s.copy_us(&unit("b:1", "b"), there).unwrap() as f64;
        assert!((every - want).abs() < want * 0.01, "every unit's too: {every}");
        // Its size counts both runs nearly alike.
        let p = predict(&s, &unit("a:1", "a"));
        assert!((99..=100).contains(&p.rest_mi), "{}", p.rest_mi);
    }

    #[test]
    fn a_new_calibration_starts_the_place_over() {
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/0", 1000, 1000));
        for _ in 0..100 {
            s.record(&unit("u:1", "u"), &run(a, 0, 1000));
            s.record(&unit("u:1", "u"), &run(b, 0, 2000));
        }
        let ratio = |s: &Stats| s.factor(b, 0) / s.factor(a, 0);
        assert!(ratio(&s) > 1.5, "{}", ratio(&s));
        assert_eq!(s.place("b/0", 1000, 1000), b);
        assert!(ratio(&s) > 1.5, "the same calibration keeps it");
        assert_eq!(s.place("b/0", 2000, 2000), b);
        assert_eq!(s.factor(b, 0), 1.);
    }

    #[test]
    fn copies_come_from_the_units_own_sends_then_every_units() {
        let mut s = Stats::new(Decay::default());
        let there = s.place("b/all", 1000, 1000);
        let sent = |copy_ms| Run { origin: Origin::Sent(Transfer { sent_kb: 1, copy_ms }), ..run(there, 0, 100) };
        assert_eq!(s.copy_us(&unit("a:1", "a"), there), None);
        s.record(&unit("a:1", "a"), &sent(300));
        s.record(&unit("b:1", "b"), &sent(100));
        assert_eq!(s.copy_us(&unit("a:1", "a"), there), Some(300_000));
        assert_eq!(s.copy_us(&unit("c:1", "c"), there), Some(200_000));
    }

    #[test]
    fn save_and_load_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-stats-{}", std::process::id()));
        let path = dir.join("jobs.hdr");
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/all", 3000, 2000));
        s.record(&unit("a:1", "a"), &run(a, 0, 1234));
        s.record(&unit("a:1", "a"), &Run { origin: Origin::Served, ..run(b, 0, 2000) });
        let sent = Origin::Sent(Transfer { sent_kb: 2048, copy_ms: 70 });
        s.record(&unit("b:1", "b"), &Run { front_us: Some(30_000), tokens_us: 230_000, origin: sent, ..run(b, 100, 99) });
        s.save(&path)?;
        assert!(!s.dirty);
        let mut t = Stats::load(&path, Decay::default())?;
        t.set_now(s.now);
        assert_eq!(t.place("b/all", 3000, 2000), b);
        assert_eq!(t.factor(a, 0), s.factor(a, 0));
        assert_eq!(t.factor(b, 0), s.factor(b, 0));
        for u in ["a:1", "b:1"] {
            let (p, q) = (predict(&s, &unit(u, u)), predict(&t, &unit(u, u)));
            assert_eq!((p.front_mi, p.rest_mi, p.width_c), (q.front_mi, q.rest_mi, q.width_c), "{u}");
        }
        let (_, a1) = t.units().find(|(k, _)| k.as_str() == "a:1").unwrap();
        assert_eq!(a1.runs, [1, 0, 1]);
        assert_eq!((a1.copy_ms, a1.front_mi), (None, None));
        let (_, b1) = t.units().find(|(k, _)| k.as_str() == "b:1").unwrap();
        assert_eq!(b1.runs, [0, 1, 0]);
        assert_eq!(b1.copy_ms, Some(70));
        assert_eq!(t.copy_us(&unit("b:1", "b"), b), Some(70_000));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn an_old_table_of_instruction_counts_loads_as_seed_history() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-stats-old-{}", std::process::id()));
        fs::create_dir_all(&dir)?;
        let path = dir.join("jobs.hdr");
        let mut w = BufWriter::new(File::create(&path)?);
        w.write_all(OLD_MAGIC)?;
        let mut ser = V2Serializer::new();
        let hist = |v: &[u64]| {
            let mut h = histogram(1 << 30);
            for &x in v {
                h.record(x).unwrap();
            }
            h
        };
        for _ in 0..2 {
            w.write_all(&1u32.to_be_bytes())?;
            write_key(&mut w, "a:1")?;
            for h in [hist(&[5000]), hist(&[1000]), hist(&[300]), hist(&[900]), hist(&[2048]), hist(&[]), hist(&[])] {
                ser.serialize(&h, &mut w).unwrap();
            }
            for n in [4u64, 0, 1] {
                w.write_all(&n.to_be_bytes())?;
            }
        }
        drop(w);
        let s = Stats::load(&path, Decay::default())?;
        let p = predict(&s, &unit("a:1", "a:1"));
        assert!(p.known);
        // Histograms keep two significant figures.
        let near = |x: u64, want: u64| x.abs_diff(want) * 100 <= want;
        assert!(near(p.front_mi, 1000) && near(p.rest_mi, 4000), "{} {}", p.front_mi, p.rest_mi);
        assert!(p.width_c.is_some_and(|w| near(w, 300)), "{:?}", p.width_c);
        assert!((2000..=2100).contains(&p.mem_kb), "{}", p.mem_kb);
        let (_, a) = s.units().next().unwrap();
        assert_eq!(a.runs, [4, 0, 1]);
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
