//! The cost table. A unit's size is the work it takes, in millions of the
//! calibration load's instructions: how long it ran, times the calibrated
//! speed of where it ran, over that place's factor at the load it saw. A
//! place's factor, learned per load bucket from units that also ran
//! elsewhere or at other loads, scales calibrated time to time there. Each
//! estimate is a weighted median of recent samples, weights halving with
//! age, so no one bad run moves it.

use crate::protocol::Unit;
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use hdrhistogram::{
    Histogram,
    serialization::{Deserializer, Serializer, V2Serializer},
};
use poolshark::local::LPooled;
use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File},
    io::{BufReader, BufWriter, ErrorKind, Read, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

/// A place's calibration counts as a sample this many µs of runs long in
/// each load bucket, and never ages: as much as a few typical jobs, so a
/// few runs that agree outweigh it, and it outweighs runs grown old.
const PRIOR_US: f64 = 3_000_000.;
/// Load buckets, in hundredths of a runnable task per CPU.
const BUCKETS: [u32; 11] = [0, 25, 50, 75, 100, 150, 200, 300, 400, 600, 800];
/// The comparisons each load bucket of a place keeps.
const COMPARISONS_KEPT: usize = 32;
/// The runs each unit keeps.
const RUNS_KEPT: usize = 16;
/// The sends each unit keeps for each place.
const UNIT_SENDS_KEPT: usize = 8;
/// The sends each place keeps of each kind.
const SENDS_KEPT: usize = 64;
/// Two sends make a slope for a place's µs per kB only if their kB differ
/// by this much.
const SLOPE_MIN_KB: f64 = 256.;
/// A unit's histograms are halved once it has this many runs, so its
/// peaks follow the crate as it grows.
const HALVE_AT: u64 = 256;
/// A sample this old, as long as the build graph keeps a node, is dropped
/// when the table is saved.
const KEEP_S: f64 = 60. * 86_400.;
/// Before sends to a box say otherwise, moving a kB to it takes this many
/// µs: 50 MB/s. It counts as one more slope, as new as can be.
const SEND_US_PER_KB: f64 = 20.;
/// What a job sent to a place nothing has been sent to spends other than
/// compiling, besides moving its bytes.
const COPY_US: f64 = 200_000.;
/// Predicted size, in millions of instructions, of a job when nothing at
/// all has been recorded.
const UNKNOWN_COST_MI: u64 = 1000;
const MAGIC: &[u8; 8] = b"rdstats9";
/// A table of decayed sums, read as one sample for each sum.
const SUMS_MAGIC: &[u8; 8] = b"rdstats8";
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

/// The weighted median of `samples`, each a value and its weight; none if
/// they weigh nothing.
fn median(samples: &mut [(f64, f64)]) -> Option<f64> {
    let total: f64 = samples.iter().map(|(_, w)| w).sum();
    if total.is_nan() || total <= 0. {
        return None;
    }
    samples.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut below = 0.;
    for &(v, w) in samples.iter() {
        below += w;
        if below >= total / 2. {
            return Some(v);
        }
    }
    samples.last().map(|(v, _)| *v)
}

/// Push `x` onto `q`, dropping the oldest past `cap`.
fn push<T>(q: &mut VecDeque<T>, x: T, cap: usize) {
    if q.len() == cap {
        q.pop_front();
    }
    q.push_back(x);
}

/// How fast old samples stop counting. Their weight halves every
/// `half_life_s` for how fast places run and how big units are, and every
/// `copy_half_life_s` for how long sends spent moving files, which follows
/// the network a laptop is on.
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

/// Pool runs of `(bucket, value, weight)` whose values fall into their
/// weighted mean until the values never fall.
fn non_decreasing(points: &mut [(usize, f64, f64)]) {
    // Pools, each its first index, weighted mean and weight.
    let mut pools: LPooled<Vec<(usize, f64, f64)>> = LPooled::take();
    for (i, &(_, v, w)) in points.iter().enumerate() {
        let mut pool = (i, v, w);
        while let Some(&(start, pv, pw)) = pools.last()
            && pv > pool.1
        {
            pools.pop();
            let total = pw + pool.2;
            pool = (start, (pv * pw + pool.1 * pool.2) / total, total);
        }
        pools.push(pool);
    }
    for (k, &(start, v, _)) in pools.iter().enumerate() {
        let end = pools.get(k + 1).map_or(points.len(), |p| p.0);
        for p in &mut points[start..end] {
            p.1 = v;
        }
    }
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
    /// It linked, so it sent every rlib below it.
    pub links: bool,
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

/// A run's time at a place over what its unit's size from runs elsewhere
/// predicted at the place's calibrated speed, weighing that prediction.
#[derive(Clone, Copy)]
struct Comparison {
    at: f64,
    ratio: f64,
    weight: f64,
}

/// What a send spent other than compiling, and the kB it sent.
#[derive(Clone, Copy)]
struct Send {
    at: f64,
    copy_us: f64,
    kb: f64,
}

/// A class of a box, where light jobs run, or all of a box's CPUs, where
/// heavy jobs run.
struct Place {
    key: ArcStr,
    /// Calibrated instructions per µs: of its fastest class, where a front
    /// end runs, and per CPU.
    fast: u64,
    rate: u64,
    curve: [VecDeque<Comparison>; BUCKETS.len()],
    /// Every unit's sends there, and heartbeats, of jobs that read only
    /// metadata and of jobs that link, which send every rlib below them.
    sends: [VecDeque<Send>; 2],
    /// The factor at each bucket's load, and the µs per kB of each kind of
    /// send, as of the last `refresh` and `fit`.
    factors: [f64; BUCKETS.len()],
    per_kb: [f64; 2],
}

impl Place {
    fn new(key: ArcStr, fast: u64, rate: u64) -> Self {
        let mut p = Self {
            key,
            fast: fast.max(1),
            rate: rate.max(1),
            curve: std::array::from_fn(|_| VecDeque::new()),
            sends: [VecDeque::new(), VecDeque::new()],
            factors: [1.; BUCKETS.len()],
            per_kb: [SEND_US_PER_KB; 2],
        };
        p.refresh(0., 1.);
        p
    }

    /// Work out the factor at each bucket's load as of `now`. A bucket
    /// runs have reached takes the weighted median of their comparisons
    /// and of the starting curve, which counts as `PRIOR_US` and never
    /// ages. Idle is where the calibration ran, so with no runs there it
    /// keeps the calibration's factor. More load never makes a run faster,
    /// so buckets that say otherwise are pooled, by the weight of what each
    /// learned. A bucket between those is interpolated; one past the last
    /// follows the starting curve's slope.
    fn refresh(&mut self, now: f64, half_life_s: f64) {
        let mut samples: LPooled<Vec<(f64, f64)>> = LPooled::take();
        // Each bucket with a factor, its factor, and the weight behind it.
        let mut learned: LPooled<Vec<(usize, f64, f64)>> = LPooled::take();
        for (i, (q, &load_c)) in self.curve.iter().zip(&BUCKETS).enumerate() {
            if q.is_empty() {
                if i == 0 {
                    learned.push((0, prior(0), PRIOR_US));
                }
                continue;
            }
            samples.clear();
            samples.push((1., PRIOR_US));
            samples.extend(q.iter().map(|c| (c.ratio / prior(load_c), c.weight * keep(c.at, now, half_life_s))));
            let weight = samples.iter().map(|(_, w)| w).sum();
            if let Some(r) = median(&mut samples) {
                learned.push((i, prior(load_c) * r, weight));
            }
        }
        non_decreasing(&mut learned);
        for (j, f) in self.factors.iter_mut().enumerate() {
            let (lo, a) = learned.iter().rev().find(|l| l.0 <= j).map_or((0, prior(0)), |l| (l.0, l.1));
            *f = match learned.iter().find(|l| l.0 > j) {
                Some(&(hi, b, _)) if lo < j => {
                    a + (b - a) * (BUCKETS[j] - BUCKETS[lo]) as f64 / (BUCKETS[hi] - BUCKETS[lo]) as f64
                }
                _ => a * prior(BUCKETS[j]) / prior(BUCKETS[lo]),
            };
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
        let ratio = time_us / calibrated_us.max(1.);
        let last = BUCKETS.len() - 1;
        let mut add = |j: usize, ratio: f64, share: f64| {
            if share > 0. {
                let c = Comparison { at: now, ratio, weight: w * share * calibrated_us };
                push(&mut self.curve[j], c, COMPARISONS_KEPT);
            }
        };
        match between(load_c) {
            (i, _) if i == last => add(last, ratio * BUCKETS[last] as f64 / load_c as f64, 1.),
            (i, a) => {
                add(i, ratio, 1. - a);
                add(i + 1, ratio, a);
            }
        }
        self.refresh(now, half_life_s);
    }

    fn send(&mut self, links: bool, send: Send, now: f64, half_life_s: f64) {
        push(&mut self.sends[usize::from(links)], send, SENDS_KEPT);
        self.fit(now, half_life_s);
    }

    /// Work out each kind's µs per kB: the weighted median of the slopes
    /// between pairs of its sends whose kB differ by at least
    /// `SLOPE_MIN_KB` (Theil-Sen), with the guess as one more.
    fn fit(&mut self, now: f64, half_life_s: f64) {
        let mut slopes: LPooled<Vec<(f64, f64)>> = LPooled::take();
        for (per_kb, sends) in self.per_kb.iter_mut().zip(&self.sends) {
            slopes.clear();
            slopes.push((SEND_US_PER_KB, 1.));
            for (i, a) in sends.iter().enumerate() {
                for b in sends.iter().skip(i + 1).filter(|b| (b.kb - a.kb).abs() >= SLOPE_MIN_KB) {
                    let w = keep(a.at, now, half_life_s) * keep(b.at, now, half_life_s);
                    slopes.push(((b.copy_us - a.copy_us) / (b.kb - a.kb), w));
                }
            }
            *per_kb = median(&mut slopes).unwrap_or(SEND_US_PER_KB).max(0.);
        }
    }
}

/// A unit's run at a place: its work counted at the place's calibrated
/// speed, which the place's factor at the run's load divides into the
/// unit's size.
#[derive(Clone, Copy)]
struct RunSample {
    place: PlaceId,
    at: f64,
    load_c: u32,
    front_mi: f64,
    rest_mi: f64,
    rest_us: f64,
    tokens_us: f64,
}

/// One of a unit's sends to a place.
#[derive(Clone, Copy)]
struct UnitSend {
    place: PlaceId,
    send: Send,
}

/// A unit's size from some of its runs: their weight, and the weighted
/// medians of their front end and rest at their places' factors and of
/// their width.
struct Size {
    runs: f64,
    front_mi: f64,
    rest_mi: f64,
    width: f64,
}

struct Record {
    runs_kept: VecDeque<RunSample>,
    sends: VecDeque<UnitSend>,
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
        Self {
            runs_kept: VecDeque::new(),
            sends: VecDeque::new(),
            mem_kb: histogram(1 << 30),
            wall_ms: histogram(36_000_000),
            copy_ms: histogram(36_000_000),
            sent_kb: histogram(1 << 30),
            runs: [0; 3],
        }
    }

    fn each(&self) -> [&Histogram<u32>; 4] {
        [&self.mem_kb, &self.wall_ms, &self.copy_ms, &self.sent_kb]
    }

    fn all(&mut self) -> [&mut Histogram<u32>; 4] {
        [&mut self.mem_kb, &mut self.wall_ms, &mut self.copy_ms, &mut self.sent_kb]
    }

    fn send(&mut self, s: UnitSend) {
        let there = self.sends.iter().filter(|o| o.place == s.place).count();
        if there >= UNIT_SENDS_KEPT
            && let Some(i) = self.sends.iter().position(|o| o.place == s.place)
        {
            self.sends.remove(i);
        }
        self.sends.push_back(s);
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

    /// Its size from its runs but those at `skip`'s place and load bucket.
    fn size(&self, places: &[Place], now: f64, half_life_s: f64, skip: Option<(PlaceId, u8)>) -> Option<Size> {
        let (mut front, mut rest, mut width): (LPooled<Vec<_>>, LPooled<Vec<_>>, LPooled<Vec<_>>) =
            (LPooled::take(), LPooled::take(), LPooled::take());
        let mut runs = 0.;
        for r in self.runs_kept.iter().filter(|r| Some((r.place, nearest(r.load_c))) != skip) {
            let (k, m) = (keep(r.at, now, half_life_s), places[r.place.0 as usize].factor(r.load_c));
            runs += k;
            front.push((r.front_mi / m, k));
            rest.push((r.rest_mi / m, k));
            let w = if r.rest_us > 0. { (r.tokens_us / r.rest_us).max(1.) } else { 1. };
            width.push((w, k));
        }
        Some(Size { runs, front_mi: median(&mut front)?, rest_mi: median(&mut rest)?, width: median(&mut width)? })
    }
}

#[derive(Debug, Clone, Copy)]
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
    pub pred: Prediction,
    pub wall_ms: u64,
    /// Medians of sent runs.
    pub copy_ms: Option<u64>,
    pub sent_kb: Option<u64>,
}

/// The prediction for a unit whose record `r` has size `s`.
fn known(r: &Record, s: &Size) -> Prediction {
    Prediction {
        front_mi: s.front_mi as u64,
        rest_mi: s.rest_mi as u64,
        width_c: Some((s.width * 100.) as u64),
        mem_kb: r.mem_kb.value_at_quantile(0.9),
        known: true,
    }
}

fn hist_median(h: &Histogram<u32>) -> Option<u64> {
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

    /// Age every weight to `now`, seconds since the epoch.
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

    /// Predicted µs a run of `unit` sent to `place`, where it must send
    /// `missing_kb`, spends other than compiling: the weighted median of
    /// its own recent sends there, else those of every job there that
    /// links as it does or doesn't, else a guess, each adjusted for
    /// sending more or fewer kB than it did at the µs per kB sends there
    /// show.
    pub fn copy_us(&self, unit: &Unit, place: PlaceId, links: bool, missing_kb: u64) -> u64 {
        let (now, half_life_s) = (self.now, self.decay.copy_half_life_s);
        let p = &self.places[place.0 as usize];
        let per_kb = p.per_kb[usize::from(links)];
        let at = |s: &Send| (s.copy_us + per_kb * (missing_kb as f64 - s.kb), keep(s.at, now, half_life_s));
        let mut samples: LPooled<Vec<(f64, f64)>> = LPooled::take();
        let own = self.exact.get(&unit.exact).into_iter().flat_map(|r| &r.sends).filter(|s| s.place == place);
        samples.extend(own.map(|s| at(&s.send)));
        if samples.is_empty() {
            samples.extend(p.sends[usize::from(links)].iter().map(at));
        }
        median(&mut samples).unwrap_or(COPY_US + per_kb * missing_kb as f64).max(0.) as u64
    }

    /// The weighted median time sends to `place` of jobs that link, or of
    /// jobs that don't, spent other than compiling, if there were any, and
    /// the MB per second they moved at.
    pub fn sends(&self, place: PlaceId, links: bool) -> (Option<u64>, f64) {
        let p = &self.places[place.0 as usize];
        let (now, half_life_s) = (self.now, self.decay.copy_half_life_s);
        let mut samples: LPooled<Vec<(f64, f64)>> = LPooled::take();
        samples.extend(p.sends[usize::from(links)].iter().map(|s| (s.copy_us, keep(s.at, now, half_life_s))));
        (median(&mut samples).map(|us| us as u64), 1000. / p.per_kb[usize::from(links)].max(1e-3))
    }

    /// A heartbeat moved `kb` to the box `places` are on in `us`: a sample
    /// of both kinds of sends to each.
    pub fn heartbeat(&mut self, places: &[PlaceId], kb: u64, us: u64) {
        let (now, half_life_s) = (self.now, self.decay.copy_half_life_s);
        let send = Send { at: now, copy_us: us as f64, kb: kb as f64 };
        for p in places {
            for links in [false, true] {
                self.places[p.0 as usize].send(links, send, now, half_life_s);
            }
        }
        self.dirty = true;
    }

    pub fn predict(&self, exact: &ArcStr, coarse: &ArcStr) -> Prediction {
        let own = [(&self.exact, exact), (&self.coarse, coarse)].into_iter().find_map(|(map, key)| {
            let r = map.get(key)?;
            let s = r.size(&self.places, self.now, self.decay.half_life_s, None)?;
            Some((r, s))
        });
        match own {
            Some((r, s)) => known(r, &s),
            None => Prediction {
                front_mi: 0,
                rest_mi: hist_median(&self.global_cost).unwrap_or(UNKNOWN_COST_MI),
                width_c: None,
                mem_kb: hist_median(&self.global_mem).unwrap_or(0),
                known: false,
            },
        }
    }

    /// Every unit with runs.
    pub fn units(&self) -> impl Iterator<Item = (&ArcStr, UnitInfo)> {
        self.exact.iter().filter_map(|(key, r)| {
            let s = r.size(&self.places, self.now, self.decay.half_life_s, None)?;
            let info = UnitInfo {
                runs: r.runs,
                pred: known(r, &s),
                wall_ms: hist_median(&r.wall_ms).unwrap_or(0),
                copy_ms: hist_median(&r.copy_ms),
                sent_kb: hist_median(&r.sent_kb),
            };
            Some((key, info))
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
        if let Some(r) = self.exact.get(&unit.exact)
            && let Some(other) = r.size(&self.places, now, self.decay.half_life_s, Some((run.place, nearest(run.load_c))))
            && other.runs > 0.
        {
            let threads = if rest_us > 0. { tokens_us / rest_us } else { 1. };
            let calibrated_us = other.front_mi * 1e6 / fast + other.rest_mi * 1e6 / (rate * threads);
            let w = other.runs / (other.runs + 1.);
            let half_life_s = self.decay.half_life_s;
            self.places[run.place.0 as usize].learn(run.load_c, w, wall_us, calibrated_us, now, half_life_s);
        }
        let sample = RunSample {
            place: run.place,
            at: now,
            load_c: run.load_c,
            front_mi: front_us * fast / 1e6,
            rest_mi: tokens_us * rate / 1e6,
            rest_us,
            tokens_us,
        };
        let sent = match run.origin {
            Origin::Sent(t) => Some((t, Send { at: now, copy_us: t.copy_ms as f64 * 1000., kb: t.sent_kb as f64 })),
            Origin::Here | Origin::Served => None,
        };
        for (map, key) in [(&mut self.exact, &unit.exact), (&mut self.coarse, &unit.coarse)] {
            let r = map.entry(key.clone()).or_insert_with(Record::new);
            push(&mut r.runs_kept, sample, RUNS_KEPT);
            if let Some((_, send)) = sent {
                r.send(UnitSend { place: run.place, send });
            }
            r.count(run.mem_kb, run.wall_us / 1000, run.origin);
        }
        let place = &mut self.places[run.place.0 as usize];
        if let Some((t, send)) = sent {
            place.send(t.links, send, now, self.decay.copy_half_life_s);
        }
        let m = place.factor(run.load_c);
        self.global_cost.saturating_record(((sample.front_mi + sample.rest_mi) / m).max(1.) as u64);
        self.global_mem.saturating_record(run.mem_kb.max(1));
        self.dirty = true;
    }

    /// History for `unit`, as an old table's would be: `front_mi` and
    /// `rest_mi` of work, `width_c` hundredths of a thread wide.
    #[cfg(test)]
    pub fn seed(&mut self, unit: &Unit, front_mi: u64, rest_mi: u64, width_c: u64, mem_kb: u64) {
        let sample = seed_run(front_mi, rest_mi, width_c, self.now);
        for (map, key) in [(&mut self.exact, &unit.exact), (&mut self.coarse, &unit.coarse)] {
            let r = map.entry(key.clone()).or_insert_with(Record::new);
            push(&mut r.runs_kept, sample, RUNS_KEPT);
            r.mem_kb.saturating_record(mem_kb.max(1));
        }
        self.global_cost.saturating_record((front_mi + rest_mi).max(1));
        self.global_mem.saturating_record(mem_kb.max(1));
        self.dirty = true;
    }

    /// Drop samples gone stale, refit every place's sends, and recompute
    /// the global histograms from every unit's size now.
    pub fn rebuild_global(&mut self) {
        let now = self.now;
        let fresh = |at: f64| now - at <= KEEP_S;
        for r in self.exact.values_mut().chain(self.coarse.values_mut()) {
            r.runs_kept.retain(|s| fresh(s.at));
            r.sends.retain(|s| fresh(s.send.at));
        }
        for p in &mut self.places {
            for q in &mut p.curve {
                q.retain(|c| fresh(c.at));
            }
            for q in &mut p.sends {
                q.retain(|s| fresh(s.at));
            }
            p.fit(now, self.decay.copy_half_life_s);
            p.refresh(now, self.decay.half_life_s);
        }
        let (mut cost, mut mem) = (histogram(1_000_000_000), histogram(1 << 30));
        for r in self.exact.values() {
            if let Some(s) = r.size(&self.places, now, self.decay.half_life_s, None) {
                cost.saturating_record((s.front_mi + s.rest_mi).max(1.) as u64);
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
            MAGIC => Self::read(&mut r, decay)?,
            SUMS_MAGIC => Self::read_sums(&mut r, decay)?,
            OLD_MAGIC => Self::read_old(&mut r, decay)?,
            _ => bail!("{} is not a stats file", path.display()),
        };
        stats.set_now(now_s());
        stats.rebuild_global();
        Ok(stats)
    }

    /// The places, each read by `read`, which pushes its samples.
    fn read_places(
        &mut self,
        r: &mut impl Read,
        mut read: impl FnMut(&mut Place, &mut dyn Read) -> Result<()>,
    ) -> Result<()> {
        self.places.clear();
        self.by_key.clear();
        for i in 0..read_u32(r)? {
            let key = read_key(r)?;
            let (fast, rate) = (read_u64(r)?, read_u64(r)?);
            let mut p = Place::new(key.clone(), fast, rate);
            read(&mut p, r)?;
            self.places.push(p);
            self.by_key.insert(key, PlaceId(i));
        }
        if !self.by_key.contains_key(SEED) {
            bail!("no seed place");
        }
        Ok(())
    }

    /// The units, each read by `read`, which pushes its samples, then their
    /// histograms and run counts.
    fn read_units(
        &mut self,
        r: &mut impl Read,
        mut read: impl FnMut(&mut Record, &mut dyn Read, u32) -> Result<()>,
    ) -> Result<()> {
        let places = self.places.len() as u32;
        let mut de = Deserializer::new();
        for map in [&mut self.exact, &mut self.coarse] {
            for _ in 0..read_u32(r)? {
                let key = read_key(r)?;
                let mut rec = Record::new();
                read(&mut rec, r, places).with_context(|| format_compact!("unit {key}"))?;
                for h in rec.all() {
                    *h = de.deserialize(r).map_err(|e| anyhow!("{e:?}"))?;
                }
                for n in &mut rec.runs {
                    *n = read_u64(r)?;
                }
                map.insert(key, rec);
            }
        }
        Ok(())
    }

    fn read(r: &mut impl Read, decay: Decay) -> Result<Self> {
        let mut stats = Self::new(decay);
        stats.read_places(r, |p, r| {
            for q in &mut p.curve {
                for _ in 0..read_u32(r)? {
                    q.push_back(Comparison { at: read_f64(r)?, ratio: read_f64(r)?, weight: read_f64(r)? });
                }
            }
            for q in &mut p.sends {
                for _ in 0..read_u32(r)? {
                    q.push_back(Send { at: read_f64(r)?, copy_us: read_f64(r)?, kb: read_f64(r)? });
                }
            }
            Ok(())
        })?;
        stats.read_units(r, |rec, r, places| {
            for _ in 0..read_u32(r)? {
                let place = read_place(r, places)?;
                rec.runs_kept.push_back(RunSample {
                    place,
                    at: read_f64(r)?,
                    load_c: read_u32(r)?,
                    front_mi: read_f64(r)?,
                    rest_mi: read_f64(r)?,
                    rest_us: read_f64(r)?,
                    tokens_us: read_f64(r)?,
                });
            }
            for _ in 0..read_u32(r)? {
                let place = read_place(r, places)?;
                let send = Send { at: read_f64(r)?, copy_us: read_f64(r)?, kb: read_f64(r)? };
                rec.sends.push_back(UnitSend { place, send });
            }
            Ok(())
        })?;
        Ok(stats)
    }

    /// A table of decayed sums: each sum's mean becomes one sample.
    fn read_sums(r: &mut impl Read, decay: Decay) -> Result<Self> {
        let mut stats = Self::new(decay);
        stats.read_places(r, |p, r| {
            for q in &mut p.curve {
                let (at, time_us, calibrated_us) = (read_f64(r)?, read_f64(r)?, read_f64(r)?);
                if calibrated_us > 0. {
                    q.push_back(Comparison { at, ratio: time_us / calibrated_us, weight: calibrated_us });
                }
            }
            for q in &mut p.sends {
                let (at, n, us, kb, _kb2, _kb_us) =
                    (read_f64(r)?, read_f64(r)?, read_f64(r)?, read_f64(r)?, read_f64(r)?, read_f64(r)?);
                if n > 0. {
                    q.push_back(Send { at, copy_us: us / n, kb: kb / n });
                }
            }
            Ok(())
        })?;
        stats.read_units(r, |rec, r, places| {
            for _ in 0..read_u32(r)? {
                let place = read_place(r, places)?;
                let mut bucket = [0u8];
                r.read_exact(&mut bucket)?;
                let at = read_f64(r)?;
                let mut sums = [0.; 9];
                for s in &mut sums {
                    *s = read_f64(r)?;
                }
                let [runs, load_c, front_mi, rest_mi, rest_us, tokens_us, sent, copy_us, sent_kb] = sums;
                if runs > 0. {
                    rec.runs_kept.push_back(RunSample {
                        place,
                        at,
                        load_c: (load_c / runs) as u32,
                        front_mi: front_mi / runs,
                        rest_mi: rest_mi / runs,
                        rest_us: rest_us / runs,
                        tokens_us: tokens_us / runs,
                    });
                }
                if sent > 0. {
                    rec.sends.push_back(UnitSend { place, send: Send { at, copy_us: copy_us / sent, kb: sent_kb / sent } });
                }
            }
            Ok(())
        })?;
        Ok(stats)
    }

    /// A table of instruction counts: each unit's medians become a seed
    /// run, and its other histograms carry over.
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
                if let (Some(cost_mi), Some(width_c)) = (hist_median(&cost), hist_median(&width)) {
                    let front_mi = hist_median(&front).unwrap_or(0).min(cost_mi);
                    rec.runs_kept.push_back(seed_run(front_mi, cost_mi - front_mi, width_c, now));
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
            for q in &p.curve {
                w.write_all(&(q.len() as u32).to_be_bytes())?;
                for c in q {
                    write_f64s(&mut w, &[c.at, c.ratio, c.weight])?;
                }
            }
            for q in &p.sends {
                w.write_all(&(q.len() as u32).to_be_bytes())?;
                for s in q {
                    write_f64s(&mut w, &[s.at, s.copy_us, s.kb])?;
                }
            }
        }
        let mut ser = V2Serializer::new();
        for map in [&self.exact, &self.coarse] {
            w.write_all(&(map.len() as u32).to_be_bytes())?;
            for (key, rec) in map.iter() {
                write_key(&mut w, key)?;
                w.write_all(&(rec.runs_kept.len() as u32).to_be_bytes())?;
                for s in &rec.runs_kept {
                    w.write_all(&s.place.0.to_be_bytes())?;
                    w.write_all(&s.at.to_be_bytes())?;
                    w.write_all(&s.load_c.to_be_bytes())?;
                    write_f64s(&mut w, &[s.front_mi, s.rest_mi, s.rest_us, s.tokens_us])?;
                }
                w.write_all(&(rec.sends.len() as u32).to_be_bytes())?;
                for s in &rec.sends {
                    w.write_all(&s.place.0.to_be_bytes())?;
                    write_f64s(&mut w, &[s.send.at, s.send.copy_us, s.send.kb])?;
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

fn seed_run(front_mi: u64, rest_mi: u64, width_c: u64, now: f64) -> RunSample {
    let rest_us = 1e6;
    RunSample {
        place: PlaceId(0),
        at: now,
        load_c: 0,
        front_mi: front_mi as f64,
        rest_mi: rest_mi as f64,
        rest_us,
        tokens_us: rest_us * width_c as f64 / 100.,
    }
}

fn read_u32(r: &mut (impl Read + ?Sized)) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_u64(r: &mut (impl Read + ?Sized)) -> Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

fn read_f64(r: &mut (impl Read + ?Sized)) -> Result<f64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_be_bytes(b))
}

fn read_place(r: &mut (impl Read + ?Sized), places: u32) -> Result<PlaceId> {
    let place = read_u32(r)?;
    if place >= places {
        bail!("a sample at unknown place {place}");
    }
    Ok(PlaceId(place))
}

fn read_key(r: &mut (impl Read + ?Sized)) -> Result<ArcStr> {
    let mut key = vec![0u8; read_u32(r)? as usize];
    r.read_exact(&mut key)?;
    Ok(ArcStr::from(String::from_utf8(key).context("key")?))
}

fn write_key(w: &mut impl Write, key: &str) -> Result<()> {
    w.write_all(&(key.len() as u32).to_be_bytes())?;
    Ok(w.write_all(key.as_bytes())?)
}

fn write_f64s(w: &mut impl Write, xs: &[f64]) -> Result<()> {
    for x in xs {
        w.write_all(&x.to_be_bytes())?;
    }
    Ok(())
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
    fn a_place_that_only_ran_busy_keeps_its_calibration_idle() {
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/0", 1000, 1000));
        // a ran the unit only at 8 tasks per CPU, a quarter as slow as the
        // starting curve has it: that says how a slows under load, not that
        // it runs four times faster idle.
        for _ in 0..200 {
            s.record(&unit("u:1", "u"), &run(b, 0, 1000));
            s.record(&unit("u:1", "u"), &run(a, 800, 2000));
        }
        assert!(near(s.factor(a, 0), 1.), "{}", s.factor(a, 0));
        assert!(near(s.factor(a, 800) / s.factor(b, 0), 2.), "{} {}", s.factor(a, 800), s.factor(b, 0));
    }

    #[test]
    fn more_load_never_predicts_a_faster_run() {
        let mut s = Stats::new(Decay::default());
        let (a, b, c) = (s.place("a/0", 1000, 1000), s.place("b/0", 1000, 1000), s.place("c/0", 1000, 1000));
        // At 4 tasks per CPU the unit took twice as long on a as at 8; at 2,
        // c took less than its calibration says idle.
        for _ in 0..200 {
            s.record(&unit("u:1", "u"), &run(b, 0, 1000));
            s.record(&unit("u:1", "u"), &run(a, 400, 8000));
            s.record(&unit("u:1", "u"), &run(a, 800, 4000));
            s.record(&unit("u:1", "u"), &run(c, 200, 400));
        }
        for place in [a, c] {
            let curve: Vec<f64> = (0..=1600).step_by(25).map(|l| s.factor(place, l)).collect();
            assert!(curve.windows(2).all(|w| w[0] <= w[1] * 1.0001), "{curve:?}");
        }
        let idle = s.factor(b, 0);
        let (four, eight) = (s.factor(a, 400) / idle, s.factor(a, 800) / idle);
        assert!(four <= eight && four > 4. && eight < 8., "{four} {eight}");
    }

    #[test]
    fn non_decreasing_pools_runs_that_fall_by_weight() {
        let mut p = [(0, 1., 1.), (1, 3., 1.), (2, 1., 3.), (3, 5., 1.), (4, 4., 1.)];
        non_decreasing(&mut p);
        let v: Vec<f64> = p.iter().map(|p| p.1).collect();
        assert_eq!(v, [1., 1.5, 1.5, 4.5, 4.5]);
    }

    #[test]
    fn old_runs_stop_counting_as_they_age() {
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/0", 1000, 1000));
        s.set_now(0.);
        // b takes twice as long as a on the same unit.
        for _ in 0..10 {
            s.record(&unit("u:1", "u"), &run(a, 0, 30_000));
            s.record(&unit("u:1", "u"), &run(b, 0, 60_000));
        }
        let ratio = |s: &Stats| s.factor(b, 0) / s.factor(a, 0);
        assert!(ratio(&s) > 1.5, "{}", ratio(&s));
        // A median doesn't drift: a day later the runs still outweigh the
        // calibration.
        s.set_now(Decay::default().half_life_s);
        assert!(ratio(&s) > 1.5, "{}", ratio(&s));
        s.set_now(Decay::default().half_life_s * 30.);
        assert!((ratio(&s) - 1.).abs() < 0.01, "back to the calibration: {}", ratio(&s));
    }

    #[test]
    fn one_bad_run_or_send_does_not_move_a_median() {
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/all", 1000, 1000));
        for _ in 0..20 {
            s.record(&unit("u:1", "u"), &run(a, 0, 1000));
            s.record(&unit("u:1", "u"), &run(b, 0, 2000));
        }
        let before = s.factor(b, 0) / s.factor(a, 0);
        // One run there starved behind other work: fifty times as long.
        s.record(&unit("u:1", "u"), &run(b, 0, 100_000));
        let after = s.factor(b, 0) / s.factor(a, 0);
        assert!((after - before).abs() < before * 0.05, "{before} {after}");
        // Seven sends of a second, then one that took 135 s.
        let sent = |copy_ms| Run { origin: Origin::Sent(Transfer { sent_kb: 1000, copy_ms, links: true }), ..run(b, 0, 100) };
        for _ in 0..7 {
            s.record(&unit("t:1", "t"), &sent(1000));
        }
        s.record(&unit("t:1", "t"), &sent(135_000));
        assert_eq!(s.copy_us(&unit("t:1", "t"), b, true, 1000), 1_000_000);
        assert_eq!(s.copy_us(&unit("other:1", "other"), b, true, 1000), 1_000_000);
    }

    #[test]
    fn copy_times_follow_their_own_shorter_half_life() {
        let mut s = Stats::new(Decay::default());
        let there = s.place("b/all", 1000, 1000);
        let sent = |copy_ms| Run { origin: Origin::Sent(Transfer { sent_kb: 1, copy_ms, links: false }), ..run(there, 0, 100) };
        // Twice 10 s to move its files on a slow link, then 1 s two hours
        // later: the old ones count a quarter each, and the new one wins
        // the median. At the day's half-life they'd still outweigh it.
        s.set_now(0.);
        s.record(&unit("a:1", "a"), &sent(10_000));
        s.record(&unit("a:1", "a"), &sent(10_000));
        s.set_now(2. * 3600.);
        s.record(&unit("a:1", "a"), &sent(1000));
        assert_eq!(s.copy_us(&unit("a:1", "a"), there, false, 1), 1_000_000);
        assert_eq!(s.copy_us(&unit("b:1", "b"), there, false, 1), 1_000_000, "every unit's too");
        // Its size counts every run.
        let p = predict(&s, &unit("a:1", "a"));
        assert!((99..=100).contains(&p.rest_mi), "{}", p.rest_mi);
    }

    #[test]
    fn a_send_is_priced_by_the_kb_the_box_is_missing() {
        let mut s = Stats::new(Decay::default());
        let there = s.place("b/all", 1000, 1000);
        let sent = |copy_ms, sent_kb| Run {
            origin: Origin::Sent(Transfer { sent_kb, copy_ms, links: true }),
            ..run(there, 0, 100)
        };
        // Sent cold, its 100 MB of rlibs took 10 s; warm, nothing to send,
        // 1 s: 90 µs a kB.
        for _ in 0..20 {
            s.record(&unit("bin:1", "bin"), &sent(10_000, 100_000));
            s.record(&unit("bin:1", "bin"), &sent(1000, 0));
        }
        let near = |got: u64, want: u64| got.abs_diff(want) < want / 50;
        let bin = unit("bin:1", "bin");
        assert!(near(s.copy_us(&bin, there, true, 0), 1_000_000), "{}", s.copy_us(&bin, there, true, 0));
        assert!(near(s.copy_us(&bin, there, true, 100_000), 10_000_000), "{}", s.copy_us(&bin, there, true, 100_000));
        // A unit never sent there is priced from every send of its kind.
        let other = unit("other:1", "other");
        assert!(near(s.copy_us(&other, there, true, 50_000), 5_500_000), "{}", s.copy_us(&other, there, true, 50_000));
        let (mean, mb_per_s) = s.sends(there, true);
        assert!(mean.is_some() && (11. ..11.2).contains(&mb_per_s), "{mb_per_s}");
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
    fn copies_come_from_the_units_own_sends_then_those_of_jobs_that_link_as_it_does() {
        let mut s = Stats::new(Decay::default());
        let there = s.place("b/all", 1000, 1000);
        let sent = |copy_ms, links| Run {
            origin: Origin::Sent(Transfer { sent_kb: 1, copy_ms, links }),
            ..run(there, 0, 100)
        };
        assert_eq!(s.copy_us(&unit("a:1", "a"), there, false, 0), COPY_US as u64, "a guess");
        s.record(&unit("a:1", "a"), &sent(300, false));
        s.record(&unit("b:1", "b"), &sent(100, false));
        s.record(&unit("d:1", "d"), &sent(200, false));
        s.record(&unit("bin:1", "bin"), &sent(9000, true));
        // Each sent 1 kB; so does the next.
        assert_eq!(s.copy_us(&unit("a:1", "a"), there, false, 1), 300_000);
        assert_eq!(s.copy_us(&unit("c:1", "c"), there, false, 1), 200_000, "libraries'");
        assert_eq!(s.copy_us(&unit("c:1", "c"), there, true, 1), 9_000_000, "links'");
    }

    #[test]
    fn save_and_load_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-stats-{}", std::process::id()));
        let path = dir.join("jobs.hdr");
        let mut s = Stats::new(Decay::default());
        let (a, b) = (s.place("a/0", 1000, 1000), s.place("b/all", 3000, 2000));
        s.record(&unit("a:1", "a"), &run(a, 0, 1234));
        s.record(&unit("a:1", "a"), &Run { origin: Origin::Served, ..run(b, 0, 2000) });
        let sent = Origin::Sent(Transfer { sent_kb: 2048, copy_ms: 70, links: true });
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
        assert_eq!((a1.copy_ms, a1.pred.front_mi), (None, 0));
        let (_, b1) = t.units().find(|(k, _)| k.as_str() == "b:1").unwrap();
        assert_eq!(b1.runs, [0, 1, 0]);
        assert_eq!(b1.copy_ms, Some(70));
        assert_eq!(t.copy_us(&unit("b:1", "b"), b, true, 2048), 70_000);
        assert_eq!(t.copy_us(&unit("c:1", "c"), b, true, 2048), 70_000, "links' sends were saved");
        assert_eq!(t.copy_us(&unit("c:1", "c"), b, false, 2048), (COPY_US + SEND_US_PER_KB * 2048.) as u64);
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
