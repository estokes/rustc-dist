use crate::{classes::Class, client, config, perf, sys};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use immutable_chunkmap::map::MapM;
use std::{
    env, fs,
    hint::black_box,
    io::{self, Write},
    ops::Bound,
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

/// Neighbouring CPUs, sorted by time, whose times differ by more than
/// this ratio are in different classes.
const CLASS_GAP: f64 = 1.08;
const KEYS: usize = 1 << 15;
/// Instructions one calibration run retires in a release build, for boxes
/// that can't count them.
pub const RUN_INSTRUCTIONS: u64 = 697_000_000;
const BATCH: usize = 64;
const SEED: u64 = 0x9e37_79b9_7f4a_7c15;

type Map = MapM<ArcStr, u64>;

struct Rng(u64);

impl Rng {
    /// splitmix64
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Symbol-like keys with a long shared prefix, so comparisons are
/// branchy byte compares.
fn keys() -> Vec<ArcStr> {
    let mut rng = Rng(SEED);
    (0..2 * KEYS)
        .map(|_| format_compact!("graphix::compiler::typecheck::{:016x}", rng.next()).as_str().into())
        .collect()
}

/// One calibration run: build a map in batches, keeping old versions
/// alive the way a compiler holds snapshots, with lookups, range scans,
/// bulk updates and removals, and set operations against old versions.
/// None if `stop` was given and raised part way.
fn run_once(keys: &[ArcStr], stop: Option<&AtomicBool>) -> Option<u64> {
    let mut rng = Rng(SEED);
    let pick = |rng: &mut Rng| keys[rng.below(keys.len())].clone();
    let mut map = Map::new();
    let mut snaps: [Map; 8] = std::array::from_fn(|_| Map::new());
    let mut sum = 0u64;
    for b in 0..KEYS / BATCH {
        if stop.is_some_and(|s| s.load(Ordering::Relaxed)) {
            return None;
        }
        map = map.insert_many((0..BATCH).map(|_| (pick(&mut rng), rng.next())));
        for _ in 0..128 {
            if let Some(v) = map.get(keys[rng.below(keys.len())].as_str()) {
                sum = sum.wrapping_add(*v);
            }
        }
        if b % 16 == 0 {
            snaps[(b / 16) % snaps.len()] = map.clone();
            for _ in 0..8 {
                let lo = keys[rng.below(keys.len())].as_str();
                for (_, v) in map.range::<str, _>((Bound::Included(lo), Bound::Unbounded)).take(32) {
                    sum = sum.wrapping_add(*v);
                }
            }
        }
        if b % 64 == 63 {
            map = map.update_many((0..512).map(|_| (pick(&mut rng), ())), |k, (), cur| {
                cur.map(|(_, v)| (k, v.wrapping_add(1)))
            });
            map = map.remove_many((0..256).map(|_| pick(&mut rng)));
        }
        if b % 128 == 127 {
            let old = &snaps[rng.below(snaps.len())];
            let u = map.union(old, |_, a, b| Some(a ^ b));
            let i = map.intersect(old, |_, a, b| Some(a.wrapping_add(*b)));
            let d = map.diff(old, |_, a, _| Some(*a));
            sum = sum.wrapping_add((u.len() + i.len() + d.len()) as u64);
        }
    }
    Some(sum)
}

/// A CPU's calibration: the median CPU time of a run, and the
/// instructions per CPU-µs it retired.
#[derive(Debug, Clone, Copy)]
struct Speed {
    time_us: u64,
    rate: u64,
}

/// The speed of the runs that started after `warm`, with the thread pinned
/// to `cpu`. CPU time leaves out the time other work holds the CPU, so a
/// busy box still measures each core's speed. A CPU too slow to start a run
/// after `warm` uses its earlier runs; every CPU finishes at least one.
fn worker(cpu: u32, t0: Instant, warm: Duration, stop: &AtomicBool) -> Result<Speed> {
    sys::pin(&[cpu])?;
    let keys = keys();
    let counter = perf::Counter::start();
    let count = || counter.as_ref().map_or(0, |c| c.read());
    let mut warming = Vec::new();
    let mut measured = Vec::new();
    loop {
        let start = Instant::now();
        let cpu_start = sys::thread_cpu_us();
        let instr_start = count();
        let interruptible = !(warming.is_empty() && measured.is_empty());
        let Some(sum) = run_once(&keys, interruptible.then_some(stop)) else { break };
        black_box(sum);
        let run = (sys::thread_cpu_us() - cpu_start, count() - instr_start);
        if start.duration_since(t0) >= warm { measured.push(run) } else { warming.push(run) }
        if stop.load(Ordering::Relaxed) {
            break;
        }
    }
    let runs = if measured.is_empty() { &mut warming } else { &mut measured };
    runs.sort_unstable();
    let time_us = runs[runs.len() / 2].0;
    let rate = match counter {
        Some(_) => runs.iter().map(|r| r.1).sum::<u64>() / runs.iter().map(|r| r.0).sum::<u64>().max(1),
        None => RUN_INSTRUCTIONS / time_us.max(1),
    };
    Ok(Speed { time_us, rate })
}

/// Run the load on every CPU at once and return each CPU's speed.
fn measure(cpus: &[u32], seconds: u64) -> Result<Vec<(u32, Speed)>> {
    let stop = AtomicBool::new(false);
    let t0 = Instant::now();
    let warm = Duration::from_secs(seconds) / 2;
    thread::scope(|s| {
        let workers: Vec<_> = cpus
            .iter()
            .map(|&cpu| {
                let stop = &stop;
                s.spawn(move || worker(cpu, t0, warm, stop))
            })
            .collect();
        thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        workers
            .into_iter()
            .zip(cpus)
            .map(|(w, &cpu)| {
                let speed = w.join().map_err(|_| anyhow!("calibration thread panicked"))??;
                Ok((cpu, speed))
            })
            .collect()
    })
}

/// Group CPUs by time: sort them and split where neighbours differ by
/// more than `CLASS_GAP`. A class's rate is its CPUs' mean. Fastest class
/// first.
fn group(mut speeds: Vec<(u32, Speed)>) -> Vec<Class> {
    speeds.sort_by_key(|&(cpu, s)| (s.time_us, cpu));
    let mut classes = Vec::new();
    let mut start = 0;
    for i in 1..=speeds.len() {
        if i == speeds.len() || speeds[i].1.time_us as f64 > speeds[i - 1].1.time_us as f64 * CLASS_GAP {
            let members = &speeds[start..i];
            let mut cpus: Vec<u32> = members.iter().map(|&(cpu, _)| cpu).collect();
            cpus.sort_unstable();
            let rate = members.iter().map(|(_, s)| s.rate).sum::<u64>() / members.len() as u64;
            classes.push(Class { rate, cpus });
            start = i;
        }
    }
    classes
}

/// Calibrate the classes of whatever profile the box is in now: one
/// thread on each core, on its first CPU, which takes the core's other
/// CPUs into its class.
pub fn run(cpus: &[u32], seconds: u64) -> Result<Vec<Class>> {
    let cores = sys::cores(cpus);
    let firsts: Vec<u32> = cores.iter().map(|core| core[0]).collect();
    Ok(with_siblings(group(measure(&firsts, seconds)?), &cores))
}

/// Each class with the other CPUs of its members' cores.
fn with_siblings(classes: Vec<Class>, cores: &[Vec<u32>]) -> Vec<Class> {
    classes
        .into_iter()
        .map(|c| {
            let mut cpus: Vec<u32> = cores.iter().filter(|core| c.cpus.contains(&core[0])).flatten().copied().collect();
            cpus.sort_unstable();
            Class { cpus, ..c }
        })
        .collect()
}

/// Measure and print this box's classes without saving them.
pub fn print(seconds: u64) -> Result<()> {
    let cores = sys::cores(&sys::allowed_cpus()?);
    let firsts: Vec<u32> = cores.iter().map(|core| core[0]).collect();
    eprintln!("calibrating {} cores for {seconds}s", cores.len());
    let speeds = measure(&firsts, seconds)?;
    for (core, (_, s)) in cores.iter().zip(&speeds) {
        let (ms, g) = (s.time_us as f64 / 1000., s.rate as f64 / 1000.);
        eprintln!("  cpus {core:?}: {ms:.1} ms, {g:.2} G instructions/s");
    }
    let mut out = io::stdout().lock();
    for (k, c) in with_siblings(group(speeds), &cores).iter().enumerate() {
        writeln!(out, "class {k}: {:.2} G instructions/s, cpus {:?}", c.rate as f64 / 1000., c.cpus)?;
    }
    Ok(())
}

/// Enable the systemd user service. The daemon calibrates the current
/// profile when it starts if that profile has no calibration yet.
pub fn install() -> Result<()> {
    if client::shutdown()? {
        eprintln!("stopped the running daemon so systemd can own it");
    }
    let exe = env::current_exe()?.canonicalize().context("finding rustc-dist")?;
    let dir = config::systemd_unit_dir();
    fs::create_dir_all(&dir)?;
    let unit = dir.join("rustc-dist.service");
    let text = format_compact!(
        "[Unit]\nDescription=rustc-dist build scheduler\n\n\
         [Service]\nExecStart=\"{}\" daemon\nRestart=always\nRestartSec=1\n\n\
         [Install]\nWantedBy=default.target\n",
        exe.display()
    );
    fs::write(&unit, text.as_str())
        .with_context(|| format_compact!("writing {}", unit.display()))?;
    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", "--now", "rustc-dist.service"])?;
    eprintln!("installed {}", unit.display());
    Ok(())
}

fn systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .context("running systemctl")?;
    if !status.success() {
        bail!("systemctl --user {args:?} failed");
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn siblings_join_their_cores_class() {
        let cores = [vec![0, 18], vec![1, 19], vec![2]];
        let classes = vec![Class { rate: 9, cpus: vec![0, 2] }, Class { rate: 5, cpus: vec![1] }];
        let classes = with_siblings(classes, &cores);
        assert_eq!(classes[0], Class { rate: 9, cpus: vec![0, 2, 18] });
        assert_eq!(classes[1], Class { rate: 5, cpus: vec![1, 19] });
    }

    #[test]
    fn groups_split_at_gaps() {
        let speeds = |v: &[(u32, u64)]| {
            v.iter().map(|&(cpu, time_us)| (cpu, Speed { time_us, rate: 1_000_000 / time_us })).collect()
        };
        let times = [(0, 800), (1, 810), (2, 790), (3, 1200), (4, 1230), (5, 1500), (6, 1520)];
        let classes = group(speeds(&times));
        assert_eq!(classes.len(), 3);
        assert_eq!(classes[0], Class { rate: (1250 + 1234 + 1265) / 3, cpus: vec![0, 1, 2] });
        assert_eq!(classes[1].cpus, vec![3, 4]);
        assert_eq!(classes[2].cpus, vec![5, 6]);
        assert_eq!(group(speeds(&[(0, 1000), (1, 1050)])).len(), 1);
    }

    #[test]
    fn a_run_retires_about_the_fallback_count() {
        let Some(c) = perf::Counter::start() else { return };
        let keys = keys();
        let before = c.read();
        black_box(run_once(&keys, None));
        let ratio = (c.read() - before) as f64 / RUN_INSTRUCTIONS as f64;
        // Debug builds retire many more; the fallback is for release builds.
        assert!(ratio > 0.95, "{ratio}");
        if !cfg!(debug_assertions) {
            assert!(ratio < 1.05, "{ratio}");
        }
    }

    #[test]
    fn the_load_is_deterministic() {
        let keys = keys();
        assert_eq!(run_once(&keys, None), run_once(&keys, None));
        let stop = AtomicBool::new(true);
        assert_eq!(run_once(&keys, Some(&stop)), None);
    }
}

