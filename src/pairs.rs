//! Place speeds from pairs of runs of the same unit (DESIGN.md, "Speeds from
//! pairs of runs"): log time = unit + place + slope × log(max(1, load)). A
//! place's effect comes only from the differences between units' times
//! there and at other places, so a unit's size cancels and can't feed back
//! into the places' speeds. For now it only predicts beside the cost table,
//! for the placement log.

use crate::stats::{Basis, PlaceId, keep};
use arcstr::ArcStr;
use poolshark::local::LPooled;
use std::collections::HashMap;

/// Rounds of fitting places, then units, then load slopes.
const ROUNDS: usize = 4;
/// Sweeps that settle the places' effects from their pairs.
const SWEEPS: usize = 50;
/// A place's calibration counts as this many fresh runs that never age: in a
/// mean, so a place's own runs decide it once it has a few.
const PRIOR_RUNS: f64 = 2.;
/// A place's load slope before its runs say otherwise: past a task per CPU,
/// time grows with load, as when tasks share the CPUs. It counts as
/// `PRIOR_RUNS` slopes, so a place new to the fit isn't taken to be as fast
/// under any load as at its calibration.
const PRIOR_SLOPE: f64 = 1.;
/// Points of a place a load slope is fitted from.
const SLOPE_POINTS: usize = 200;
/// Two runs make a load slope only if `log(max(1, load))` differs this much
/// between them.
const SLOPE_MIN_DX: f64 = 0.2;

/// A run of a unit, by its index in `Snapshot::units`.
pub struct Sample {
    pub unit: u32,
    pub place: PlaceId,
    /// Seconds since the epoch.
    pub at: f64,
    /// Hundredths of a runnable task per CPU, on average while it ran.
    pub load_c: u32,
    pub wall_us: f64,
}

/// What a fit reads: the runs of every unit and of every coarse key, each
/// place's calibrated µs per million instructions, and the places held
/// there: the reference, which holds the scale, and the seed place, whose
/// runs are work recorded before runs were timed.
pub struct Snapshot {
    pub units: Vec<ArcStr>,
    pub samples: Vec<Sample>,
    pub coarse: Vec<(ArcStr, Vec<Sample>)>,
    pub priors: Vec<f64>,
    pub fixed: Vec<PlaceId>,
    pub now: f64,
    pub half_life_s: f64,
}

/// Each unit's effect, the log of its work in millions of instructions at
/// the reference, with the places it ran at, and each place's effect and
/// load slope.
pub struct Fit {
    units: HashMap<ArcStr, (f64, Ran)>,
    coarse: HashMap<ArcStr, f64>,
    /// For a unit with neither.
    median: Option<f64>,
    places: Vec<(f64, f64)>,
}

/// The weighted median of `samples`, each a value and its weight, or the
/// midpoint of the two values the weight splits evenly between: a unit run
/// as often at two places is between them, not at the faster one.
fn median(samples: &mut [(f64, f64)]) -> Option<f64> {
    let total: f64 = samples.iter().map(|(_, w)| w).sum();
    if total.is_nan() || total <= 0. {
        return None;
    }
    samples.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut below = 0.;
    for (i, &(v, w)) in samples.iter().enumerate() {
        below += w;
        if below >= total / 2. {
            let even = (below - total / 2.).abs() <= total * 1e-12;
            return Some(match samples.get(i + 1) {
                Some(&(next, _)) if even => (v + next) / 2.,
                _ => v,
            });
        }
    }
    samples.last().map(|(v, _)| *v)
}

/// The places a unit ran at, as bits by their index; a place past the 64th
/// counts as one it never ran at.
#[derive(Clone, Copy, Default)]
struct Ran(u64);

impl Ran {
    fn add(&mut self, place: PlaceId) {
        if let Some(bit) = 1u64.checked_shl(place.index() as u32) {
            self.0 |= bit;
        }
    }

    fn at(self, place: PlaceId) -> bool {
        1u64.checked_shl(place.index() as u32).is_some_and(|bit| self.0 & bit != 0)
    }
}

/// How loaded a run was: none up to a runnable task per CPU, the job's own
/// included, since until then nothing waits for a CPU.
fn x(load_c: u32) -> f64 {
    (load_c as f64 / 100.).max(1.).ln()
}

fn log_us(s: &Sample) -> f64 {
    s.wall_us.max(1.).ln()
}

/// Values, each with its weight.
type Weighed = LPooled<Vec<(f64, f64)>>;

/// The pairs of places, each with every unit's difference between its
/// times at the two, weighing what the less-run side weighs.
struct Pairs {
    n: usize,
    diffs: LPooled<Vec<Weighed>>,
}

impl Pairs {
    /// From every unit's median time at each place it ran at, less the
    /// load each place's slope says it ran under.
    fn new(snap: &Snapshot, by_unit: &[LPooled<Vec<usize>>], weight: &[f64], slope: &[f64]) -> Self {
        let n = snap.priors.len();
        let mut diffs: LPooled<Vec<Weighed>> = (0..n * n).map(|_| LPooled::take()).collect();
        let mut runs: LPooled<Vec<(usize, f64, f64)>> = LPooled::take();
        let mut at: LPooled<Vec<(usize, f64, f64)>> = LPooled::take();
        let mut scratch: Weighed = LPooled::take();
        for ix in by_unit {
            runs.clear();
            runs.extend(ix.iter().map(|&i| {
                let (s, p) = (&snap.samples[i], snap.samples[i].place.index());
                (p, log_us(s) - slope[p] * x(s.load_c), weight[i])
            }));
            runs.sort_by_key(|r| r.0);
            at.clear();
            for group in runs.chunk_by(|a, b| a.0 == b.0) {
                scratch.clear();
                scratch.extend(group.iter().map(|&(_, y, w)| (y, w)));
                let w = group.iter().map(|r| r.2).sum();
                if let Some(m) = median(&mut scratch) {
                    at.push((group[0].0, m, w));
                }
            }
            for (i, &(p, mp, wp)) in at.iter().enumerate() {
                for &(q, mq, wq) in &at[i + 1..] {
                    diffs[p * n + q].push((mp - mq, wp.min(wq)));
                }
            }
        }
        Self { n, diffs }
    }

    /// Each place's effect: the weighted mean of what each place it has
    /// pairs with says of it, by the weighted median of those pairs' units,
    /// weighing what the pairs weigh, and of its calibration, weighing
    /// `PRIOR_RUNS`; the fixed places stay at their calibration. The medians
    /// keep odd runs out; a median of the few places a place pairs with
    /// would stick at whichever lies between, often its calibration.
    fn places(&mut self, snap: &Snapshot) -> LPooled<Vec<f64>> {
        let n = self.n;
        let prior = |p: usize| snap.priors[p];
        let mut edges: LPooled<Vec<Option<(f64, f64)>>> = LPooled::take();
        for d in self.diffs.iter_mut() {
            let w = d.iter().map(|(_, w)| w).sum::<f64>();
            edges.push(median(d).map(|m| (m, w)));
        }
        let mut place: LPooled<Vec<f64>> = (0..n).map(prior).collect();
        for _ in 0..SWEEPS {
            for p in (0..n).filter(|&p| !snap.fixed.iter().any(|f| f.index() == p)) {
                let (mut sum, mut weight) = (prior(p) * PRIOR_RUNS, PRIOR_RUNS);
                for q in 0..n {
                    if let Some((d, w)) = edges[p * n + q] {
                        (sum, weight) = (sum + (place[q] + d) * w, weight + w);
                    }
                    if let Some((d, w)) = edges[q * n + p] {
                        (sum, weight) = (sum + (place[q] - d) * w, weight + w);
                    }
                }
                place[p] = sum / weight;
            }
        }
        place
    }
}

impl Fit {
    pub fn new(snap: &Snapshot) -> Self {
        let n = snap.priors.len();
        let known = |s: &Sample| s.place.index() < n;
        let weight: LPooled<Vec<f64>> = snap.samples.iter().map(|s| keep(s.at, snap.now, snap.half_life_s)).collect();
        let mut by_place: LPooled<Vec<LPooled<Vec<usize>>>> = (0..n).map(|_| LPooled::take()).collect();
        let mut by_unit: LPooled<Vec<LPooled<Vec<usize>>>> = (0..snap.units.len()).map(|_| LPooled::take()).collect();
        for (i, s) in snap.samples.iter().enumerate().filter(|(_, s)| known(s)) {
            by_place[s.place.index()].push(i);
            by_unit[s.unit as usize].push(i);
        }
        let mut slope: LPooled<Vec<f64>> = (0..n).map(|_| PRIOR_SLOPE).collect();
        let mut unit: LPooled<Vec<f64>> = (0..snap.units.len()).map(|_| 0.).collect();
        let mut scratch: LPooled<Vec<(f64, f64)>> = LPooled::take();
        let mut place = LPooled::take();
        for round in 0..=ROUNDS {
            place = Pairs::new(snap, &by_unit, &weight, &slope).places(snap);
            for (u, ix) in by_unit.iter().enumerate() {
                scratch.clear();
                scratch.extend(ix.iter().map(|&i| {
                    let (s, p) = (&snap.samples[i], snap.samples[i].place.index());
                    (log_us(s) - place[p] - slope[p] * x(s.load_c), weight[i])
                }));
                unit[u] = median(&mut scratch).unwrap_or(0.);
            }
            if round < ROUNDS {
                for (p, ix) in by_place.iter().enumerate().filter(|(_, ix)| !ix.is_empty()) {
                    slope[p] = load_slope(snap, ix, &weight, &unit);
                }
            }
        }
        let mut effect = |runs: &[Sample]| {
            scratch.clear();
            scratch.extend(runs.iter().filter(|s| known(s)).map(|s| {
                let p = s.place.index();
                (log_us(s) - place[p] - slope[p] * x(s.load_c), keep(s.at, snap.now, snap.half_life_s))
            }));
            median(&mut scratch)
        };
        let coarse = snap.coarse.iter().filter_map(|(k, runs)| Some((k.clone(), effect(runs)?))).collect();
        let units: HashMap<ArcStr, (f64, Ran)> = by_unit
            .iter()
            .enumerate()
            .filter(|(_, ix)| !ix.is_empty())
            .map(|(u, ix)| {
                let mut ran = Ran::default();
                for &i in ix.iter() {
                    ran.add(snap.samples[i].place);
                }
                (snap.units[u].clone(), (unit[u], ran))
            })
            .collect();
        let mut all: LPooled<Vec<(f64, f64)>> = units.values().map(|&(a, _)| (a, 1.)).collect();
        let median = median(&mut all);
        let places = (0..n).map(|p| (place[p], slope[p])).collect();
        Self { units, coarse, median, places }
    }

    /// Predicted µs of `exact`, or its `coarse` key, or the median unit, at
    /// `place` under `load_c`, and what it comes from; `loaded` false leaves
    /// out the place's load slope. None for a place the fit doesn't know.
    pub fn predict_us(&self, exact: &str, coarse: &str, place: PlaceId, load_c: u32, loaded: bool) -> Option<(u64, Basis)> {
        let &(effect, slope) = self.places.get(place.index())?;
        let (unit, basis) = match self.units.get(exact) {
            Some(&(unit, ran)) if ran.at(place) => (unit, Basis::Place),
            Some(&(unit, _)) => (unit, Basis::Pairs),
            None => (self.coarse.get(coarse).copied().or(self.median)?, Basis::Size),
        };
        let load = if loaded { slope * x(load_c) } else { 0. };
        Some(((unit + effect + load).exp() as u64, basis))
    }
}

/// The Theil-Sen slope of a place's runs' times, less their units' effects,
/// against `log(max(1, load))`, over up to `SLOPE_POINTS` of them spread across
/// the loads seen, with `PRIOR_SLOPE` as `PRIOR_RUNS` more. More load never
/// makes a run faster, so it is at least 0.
fn load_slope(snap: &Snapshot, ix: &[usize], weight: &[f64], unit: &[f64]) -> f64 {
    let mut pts: LPooled<Vec<(f64, f64, f64)>> = ix
        .iter()
        .map(|&i| {
            let s = &snap.samples[i];
            (x(s.load_c), log_us(s) - unit[s.unit as usize], weight[i])
        })
        .collect();
    pts.sort_by(|a, b| a.0.total_cmp(&b.0));
    let step = pts.len().div_ceil(SLOPE_POINTS).max(1);
    let mut slopes: LPooled<Vec<(f64, f64)>> = LPooled::take();
    slopes.push((PRIOR_SLOPE, PRIOR_RUNS));
    for (k, a) in pts.iter().step_by(step).enumerate() {
        for b in pts.iter().step_by(step).skip(k + 1).filter(|b| b.0 - a.0 > SLOPE_MIN_DX) {
            slopes.push(((b.1 - a.1) / (b.0 - a.0), a.2 * b.2));
        }
    }
    median(&mut slopes).unwrap_or(PRIOR_SLOPE).max(0.)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        protocol::Unit,
        stats::{Decay, Origin, Run, Stats},
    };

    const NOW: f64 = 1_800_000_000.;

    fn unit(name: &str) -> Unit {
        let coarse = ArcStr::from(name.split(':').next().unwrap());
        Unit { exact: ArcStr::from(name), coarse, id: ArcStr::from(name), dir: None, deps: vec![], codegen_units: 16 }
    }

    /// A stats table with a reference class `a` and places `b` and `c`, all
    /// calibrated alike, at 1000 instructions per µs.
    fn table() -> (Stats, [PlaceId; 3]) {
        let mut s = Stats::new(Decay::default());
        s.set_now(NOW);
        let places = ["h/p/0", "b/p/0", "c/p/0"].map(|k| s.place(k, 1000, 1000));
        (s, places)
    }

    fn ran(s: &mut Stats, name: &str, at: PlaceId, load_c: u32, ms: u64) {
        let wall_us = ms * 1000;
        let run = Run { place: at, load_c, wall_us, front_us: None, tokens_us: wall_us, mem_kb: 1, origin: Origin::Here };
        s.record(&unit(name), &run);
    }

    fn predict(fit: &Fit, name: &str, at: PlaceId, load_c: u32) -> u64 {
        let u = unit(name);
        fit.predict_us(&u.exact, &u.coarse, at, load_c, true).unwrap().0
    }

    fn near(got: u64, want: u64) -> bool {
        got.abs_diff(want) * 50 <= want
    }

    #[test]
    fn a_place_is_as_fast_as_pairs_say_whatever_the_units_sizes() {
        let (mut s, [a, b, _]) = table();
        // Twenty units from 1 ms to 10 s, each twice as fast on b as on a.
        for i in 0..20 {
            let ms = 10u64.pow(i % 5);
            for _ in 0..8 {
                ran(&mut s, &format!("u{i}:1"), a, 0, ms * 2);
                ran(&mut s, &format!("u{i}:1"), b, 0, ms);
            }
        }
        // A unit seen only on a takes half as long on b.
        ran(&mut s, "new:1", a, 0, 3000);
        let fit = Fit::new(&s.pair_snapshot(a));
        let got = predict(&fit, "new:1", b, 0);
        assert!(near(got, 1_500_000), "{got}");
        assert!(near(predict(&fit, "u2:1", a, 0), 200_000));
    }

    #[test]
    fn scaling_every_run_moves_the_units_and_not_the_places() {
        let fit = |k: u64| {
            let (mut s, [a, b, _]) = table();
            for i in 0..20 {
                for _ in 0..8 {
                    ran(&mut s, &format!("u{i}:1"), a, 0, k * 300 * (i + 1));
                    ran(&mut s, &format!("u{i}:1"), b, 0, k * 100 * (i + 1));
                }
            }
            (Fit::new(&s.pair_snapshot(a)), a, b)
        };
        let ((one, a, b), (ten, ..)) = (fit(1), fit(10));
        // The reference holds the scale: b stays three times as fast as a,
        // and each unit is ten times the work.
        for i in 0..20 {
            let u = format!("u{i}:1");
            assert!(near(predict(&ten, &u, a, 0), 10 * predict(&one, &u, a, 0)));
            assert!(near(predict(&ten, &u, b, 0), 10 * predict(&one, &u, b, 0)));
            assert!(near(predict(&one, &u, a, 0), 3 * predict(&one, &u, b, 0)));
        }
    }

    #[test]
    fn a_place_with_few_pairs_stays_near_its_calibration() {
        let (mut s, [a, _, c]) = table();
        for i in 0..5 {
            ran(&mut s, &format!("u{i}:1"), a, 0, 1000);
        }
        // One unit ran ten times as long on c, calibrated as fast as a: one
        // run against two of calibration moves it a third of the way.
        ran(&mut s, "u0:1", c, 0, 10_000);
        let fit = Fit::new(&s.pair_snapshot(a));
        let want = (10f64.ln() / 3.).exp() * 1e6;
        assert!(near(predict(&fit, "u1:1", c, 0), want as u64), "{}", predict(&fit, "u1:1", c, 0));
    }

    #[test]
    fn a_steady_farm_is_learned_and_stays_learned() {
        // 40 units of 0.1 to 4 s; b runs them 3 times as fast as a, c 1.5
        // times, each run off by up to 20% either way. Each hour every unit
        // runs at two of the three places.
        let truth = |i: u64| 100 * (1 + i % 40);
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut noise = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            80 + x % 41
        };
        let (mut s, [a, b, c]) = table();
        let mut fits = Vec::new();
        for hour in 0..6 {
            s.set_now(NOW + hour as f64 * 3600.);
            for i in 0..40 {
                for (k, (at, speed_c)) in [(a, 100), (b, 300), (c, 150)].into_iter().enumerate() {
                    if (i as usize + hour) % 3 != k {
                        ran(&mut s, &format!("u{i}:1"), at, 0, truth(i) * noise() / speed_c);
                    }
                }
            }
            fits.push(Fit::new(&s.pair_snapshot(a)));
        }
        // Past the first hour, when each unit has run at one pair of places.
        for fit in &fits[1..] {
            let ratio = |at| predict(fit, "u7:1", a, 0) as f64 / predict(fit, "u7:1", at, 0) as f64;
            assert!((2.85..3.15).contains(&ratio(b)) && (1.42..1.58).contains(&ratio(c)), "{} {}", ratio(b), ratio(c));
        }
        // The scale holds: the median unit stays the size it was.
        let median = |fit: &Fit| predict(fit, "new:1", a, 0);
        assert!(near(median(&fits[2]), median(&fits[5])), "{} {}", median(&fits[2]), median(&fits[5]));
    }

    #[test]
    fn a_places_load_slope_is_learned() {
        let (mut s, [a, b, _]) = table();
        // On b a run takes half again as long per task per CPU past the
        // first: a slope of 1.5 in log time against log(max(1, load)).
        for i in 0..20 {
            ran(&mut s, &format!("u{i}:1"), a, 0, 1000 + 100 * i);
            for load_c in [0, 100, 200, 400] {
                let slowed = (load_c.max(100) as f64 / 100.).powf(1.5);
                ran(&mut s, &format!("u{i}:1"), b, load_c, ((1000 + 100 * i) as f64 * slowed) as u64);
            }
        }
        let fit = Fit::new(&s.pair_snapshot(a));
        let at = |load_c| predict(&fit, "u5:1", b, load_c);
        assert!(near(at(50), 1_500_000) && near(at(300), (1.5e6 * 3f64.powf(1.5)) as u64), "{} {}", at(50), at(300));
        let u = unit("u5:1");
        assert_eq!(fit.predict_us(&u.exact, &u.coarse, b, 300, false).map(|p| p.0), Some(at(0)), "without its slope");
    }

    #[test]
    fn a_place_no_run_has_shown_loaded_is_slowed_by_its_load() {
        let (mut s, [a, _, c]) = table();
        ran(&mut s, "u:1", a, 0, 1000);
        // c is calibrated as fast as a and has never run anything: alone on
        // it a job runs at its calibration, and at 3 tasks per cpu takes 3
        // times as long.
        let fit = Fit::new(&s.pair_snapshot(a));
        assert!(near(predict(&fit, "u:1", c, 100), 1_000_000) && near(predict(&fit, "u:1", c, 300), 3_000_000));
    }

    #[test]
    fn a_unit_never_run_is_its_coarse_key_then_the_median_unit() {
        let (mut s, [a, b, _]) = table();
        for (name, ms) in [("x:1", 1000), ("y:1", 2000), ("z:1", 4000)] {
            ran(&mut s, name, a, 0, ms);
            ran(&mut s, name, b, 0, ms);
        }
        let fit = Fit::new(&s.pair_snapshot(a));
        assert!(near(predict(&fit, "x:2", a, 0), 1_000_000), "another build of x");
        assert!(near(predict(&fit, "w:1", a, 0), 2_000_000), "the median unit");
    }
}
