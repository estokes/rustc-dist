//! Predicted times from units' runs (DESIGN.md, "Speeds from pairs of
//! runs"). A unit's runs at a place predict it there. At a place it never
//! ran at, units like it do: those nearest it in time at a place it ran at
//! that ran at both, by their median difference between the two, so a cost
//! every job pays alike is carried at the size it weighs. Each run's time is
//! first taken back to no load by its place's slope in log time against
//! log(max(1, load)). Each place's effect, fitted from the differences
//! between units' times there and at other places so that a unit's size
//! cancels, translates a unit too few units are like, and with units'
//! effects predicts one never run by its crate or as the median unit.

use crate::stats::{Basis, PlaceId, keep};
use arcstr::ArcStr;
use poolshark::local::LPooled;
use std::{collections::HashMap, ops::Range};

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
/// The units like a unit whose times translate its time at a place it ran
/// at to one it never ran at: this many that ran at both, nearest it in time
/// at the first.
const SIMILAR: usize = 16;
/// The most a place a unit ran at weighs, in runs, among the places its
/// time is translated from.
const SOURCE_RUNS: f64 = 4.;

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
/// the reference, and where its times at the places it ran at are in `at`;
/// each place's effect and load slope; and for each ordered pair of places,
/// `p * places + q`, every unit that ran at both: its time at `p` and its
/// difference at `q`, by its time at `p`.
pub struct Fit {
    units: HashMap<ArcStr, (f64, Range<usize>)>,
    at: Vec<At>,
    coarse: HashMap<ArcStr, f64>,
    /// For a unit with neither.
    median: Option<f64>,
    places: Vec<(f64, f64)>,
    similar: Vec<Vec<(f64, f64)>>,
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

/// A unit's time at a place: the median of its runs' log µs there, less the
/// load each ran under by the place's slope, and what they weigh.
#[derive(Clone, Copy)]
struct At {
    place: usize,
    log_us: f64,
    weight: f64,
}

/// Every unit's time at each place it ran at, in order of place: unit `u`'s
/// are `at[spans[u]]`.
struct Times {
    at: Vec<At>,
    spans: Vec<Range<usize>>,
}

impl Times {
    fn new(snap: &Snapshot, by_unit: &[LPooled<Vec<usize>>], weight: &[f64], slope: &[f64]) -> Self {
        let mut at = Vec::new();
        let mut spans = Vec::with_capacity(by_unit.len());
        let mut runs: LPooled<Vec<(usize, f64, f64)>> = LPooled::take();
        let mut scratch: Weighed = LPooled::take();
        for ix in by_unit {
            runs.clear();
            runs.extend(ix.iter().map(|&i| {
                let (s, p) = (&snap.samples[i], snap.samples[i].place.index());
                (p, log_us(s) - slope[p] * x(s.load_c), weight[i])
            }));
            runs.sort_by_key(|r| r.0);
            let start = at.len();
            for group in runs.chunk_by(|a, b| a.0 == b.0) {
                scratch.clear();
                scratch.extend(group.iter().map(|&(_, y, w)| (y, w)));
                if let Some(log_us) = median(&mut scratch) {
                    at.push(At { place: group[0].0, log_us, weight: group.iter().map(|r| r.2).sum() });
                }
            }
            spans.push(start..at.len());
        }
        Self { at, spans }
    }

    fn units(&self) -> impl Iterator<Item = &[At]> {
        self.spans.iter().map(|span| &self.at[span.clone()])
    }

    /// For each ordered pair of places, `p * n + q`, every unit that ran at
    /// both: its time at `p` and its difference at `q`, by its time at `p`.
    fn similar(&self, n: usize) -> Vec<Vec<(f64, f64)>> {
        let mut similar: Vec<Vec<(f64, f64)>> = (0..n * n).map(|_| Vec::new()).collect();
        for at in self.units() {
            for a in at {
                for b in at.iter().filter(|b| b.place != a.place) {
                    similar[a.place * n + b.place].push((a.log_us, b.log_us - a.log_us));
                }
            }
        }
        for s in &mut similar {
            s.sort_by(|a, b| a.0.total_cmp(&b.0));
        }
        similar
    }
}

/// The pairs of places, each with every unit's difference between its
/// times at the two, weighing what the less-run side weighs.
struct Pairs {
    n: usize,
    diffs: LPooled<Vec<Weighed>>,
}

impl Pairs {
    fn new(n: usize, times: &Times) -> Self {
        let mut diffs: LPooled<Vec<Weighed>> = (0..n * n).map(|_| LPooled::take()).collect();
        for at in times.units() {
            for (i, a) in at.iter().enumerate() {
                for b in &at[i + 1..] {
                    diffs[a.place * n + b.place].push((a.log_us - b.log_us, a.weight.min(b.weight)));
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
        let mut times = Times { at: Vec::new(), spans: Vec::new() };
        for round in 0..=ROUNDS {
            times = Times::new(snap, &by_unit, &weight, &slope);
            place = Pairs::new(n, &times).places(snap);
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
        let units: HashMap<ArcStr, (f64, Range<usize>)> = by_unit
            .iter()
            .enumerate()
            .filter(|(_, ix)| !ix.is_empty())
            .map(|(u, _)| (snap.units[u].clone(), (unit[u], times.spans[u].clone())))
            .collect();
        let mut all: LPooled<Vec<(f64, f64)>> = units.values().map(|(a, _)| (*a, 1.)).collect();
        let median = median(&mut all);
        let places = (0..n).map(|p| (place[p], slope[p])).collect();
        let similar = times.similar(n);
        Self { units, at: times.at, coarse, median, places, similar }
    }

    /// Predicted µs of `exact` at `place` under `load_c`, and what it comes
    /// from: its runs there; else units like it (`translate`); else its
    /// effect and the place's; and for a unit never run, its `coarse` key's
    /// effect or the median unit's. `loaded` false leaves out the place's
    /// load slope. None for a place the fit doesn't know.
    pub fn predict_us(&self, exact: &str, coarse: &str, place: PlaceId, load_c: u32, loaded: bool) -> Option<(u64, Basis)> {
        let p = place.index();
        let &(effect, slope) = self.places.get(p)?;
        let (log_us, basis) = match self.units.get(exact) {
            Some((unit, span)) => {
                let at = &self.at[span.clone()];
                match at.iter().find(|a| a.place == p) {
                    Some(a) => (a.log_us, Basis::Place),
                    None => match self.translate(at, p) {
                        Some(log_us) => (log_us, Basis::Similar),
                        None => (unit + effect, Basis::Pairs),
                    },
                }
            }
            None => (self.coarse.get(coarse).copied().or(self.median)? + effect, Basis::Size),
        };
        let load = if loaded { slope * x(load_c) } else { 0. };
        Some(((log_us + load).exp() as u64, basis))
    }

    /// A unit's time at `p`, where it never ran, from its times `at` the
    /// places it ran at: from each, its time there plus the median
    /// difference at `p` of the `SIMILAR` units nearest it in time there
    /// that ran at both; over those places, the weighted median, each
    /// weighing its runs up to `SOURCE_RUNS`. None if no place it ran at
    /// shares that many units with `p`.
    fn translate(&self, at: &[At], p: usize) -> Option<f64> {
        let n = self.places.len();
        let mut guesses: LPooled<Vec<(f64, f64)>> = LPooled::take();
        for a in at {
            let units = &self.similar[a.place * n + p];
            if units.len() < SIMILAR {
                continue;
            }
            let mut diffs = [0f64; SIMILAR];
            let mut hi = units.partition_point(|u| u.0 < a.log_us);
            let mut lo = hi;
            for d in &mut diffs {
                let below = match (lo.checked_sub(1), units.get(hi)) {
                    (Some(i), Some(u)) => a.log_us - units[i].0 <= u.0 - a.log_us,
                    (below, _) => below.is_some(),
                };
                if below {
                    lo -= 1;
                    *d = units[lo].1;
                } else {
                    *d = units[hi].1;
                    hi += 1;
                }
            }
            diffs.sort_by(f64::total_cmp);
            let diff = (diffs[SIMILAR / 2 - 1] + diffs[SIMILAR / 2]) / 2.;
            guesses.push((a.log_us + diff, a.weight.min(SOURCE_RUNS)));
        }
        median(&mut guesses)
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

    /// How many times as fast as `from` the fit has `to`.
    fn speedup(fit: &Fit, from: PlaceId, to: PlaceId) -> f64 {
        (fit.places[from.index()].0 - fit.places[to.index()].0).exp()
    }

    fn basis(fit: &Fit, name: &str, at: PlaceId) -> Basis {
        let u = unit(name);
        fit.predict_us(&u.exact, &u.coarse, at, 0, true).unwrap().1
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
        assert!((1.96..2.04).contains(&speedup(&fit, a, b)), "{}", speedup(&fit, a, b));
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
            let (b, c) = (speedup(fit, a, b), speedup(fit, a, c));
            assert!((2.85..3.15).contains(&b) && (1.42..1.58).contains(&c), "{b} {c}");
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
    fn a_unit_is_its_own_runs_at_a_place_it_ran_at() {
        let (mut s, [a, b, _]) = table();
        for i in 0..20 {
            ran(&mut s, &format!("u{i}:1"), a, 0, 1000 + 100 * i);
            ran(&mut s, &format!("u{i}:1"), b, 0, (1000 + 100 * i) / 2);
        }
        // Twice as fast on b as the others are, it is that fast there.
        ran(&mut s, "odd:1", a, 0, 2000);
        ran(&mut s, "odd:1", b, 0, 500);
        let fit = Fit::new(&s.pair_snapshot(a));
        assert!(near(predict(&fit, "odd:1", b, 0), 500_000), "{}", predict(&fit, "odd:1", b, 0));
        assert_eq!(basis(&fit, "odd:1", b), Basis::Place);
    }

    #[test]
    fn a_unit_new_to_a_place_takes_what_units_like_it_take_there() {
        let (mut s, [a, b, _]) = table();
        // On b every job pays 400 ms, then runs twice as fast as on a: a
        // ratio near 5 for a job of 100 ms on a, near 0.5 for one of 10 s.
        let on_b = |ms: f64| 400. + ms / 2.;
        for i in 0..200 {
            let ms = 50. * (400f64.ln() * i as f64 / 199.).exp();
            ran(&mut s, &format!("u{i}:1"), a, 0, ms as u64);
            ran(&mut s, &format!("u{i}:1"), b, 0, on_b(ms) as u64);
        }
        ran(&mut s, "small:1", a, 0, 300);
        ran(&mut s, "big:1", a, 0, 5000);
        let fit = Fit::new(&s.pair_snapshot(a));
        for (name, ms) in [("small:1", 300.), ("big:1", 5000.)] {
            let got = predict(&fit, name, b, 0);
            assert!(near(got, (on_b(ms) * 1000.) as u64), "{name}: {got}");
            assert_eq!(basis(&fit, name, b), Basis::Similar);
        }
        // With fewer units like it than it takes, the places' effects.
        let (mut s, [a, b, _]) = table();
        for i in 0..SIMILAR - 1 {
            ran(&mut s, &format!("u{i}:1"), a, 0, 1000);
            ran(&mut s, &format!("u{i}:1"), b, 0, 500);
        }
        ran(&mut s, "new:1", a, 0, 1000);
        assert_eq!(basis(&Fit::new(&s.pair_snapshot(a)), "new:1", b), Basis::Pairs);
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
