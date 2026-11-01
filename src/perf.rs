//! Instructions retired, counted by the CPU.

use anyhow::{Context, Result, bail};
use poolshark::local::LPooled;
use std::{
    fs, io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

/// The kernel's `perf_event_attr` at its original size, which every kernel
/// accepts.
#[repr(C)]
#[derive(Default)]
struct Attr {
    kind: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    config1: u64,
}

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
/// Where a hybrid CPU's core type goes in a hardware event's config.
const PMU_SHIFT: u32 = 32;
const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 8;
const EXCLUDE_KERNEL: u64 = 1 << 5;
const EXCLUDE_HV: u64 = 1 << 6;
const FORMAT_TOTAL_TIME_ENABLED: u64 = 1 << 0;
const FORMAT_TOTAL_TIME_RUNNING: u64 = 1 << 1;
/// Counters that together ran this many ns less than they were enabled
/// were shared with another perf user, and count low. A thread that moves
/// between kinds of core finds them a fraction of a millisecond short with
/// nothing else counting.
const SHORT_NS: u64 = 1_000_000;

/// What a counter's read gives: its count, then the ns it was enabled and
/// the ns it was running.
type Reading = [u64; 3];

/// Counts the user-space instructions the calling thread retires on every
/// kind of core.
pub struct Counter(Vec<OwnedFd>);

impl Counter {
    /// None if the kernel won't count instructions here.
    pub fn start() -> Option<Self> {
        let flags = EXCLUDE_KERNEL | EXCLUDE_HV;
        let fds = pmus()
            .into_iter()
            .map(|pmu| {
                let attr = Attr {
                    kind: PERF_TYPE_HARDWARE,
                    size: mem::size_of::<Attr>() as u32,
                    config: (pmu << PMU_SHIFT) | PERF_COUNT_HW_INSTRUCTIONS,
                    read_format: FORMAT_TOTAL_TIME_ENABLED | FORMAT_TOTAL_TIME_RUNNING,
                    flags,
                    ..Attr::default()
                };
                let fd = unsafe {
                    libc::syscall(libc::SYS_perf_event_open, &attr, 0, -1, -1, PERF_FLAG_FD_CLOEXEC)
                };
                (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd as i32) })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self(fds))
    }

    /// The instructions counted since `start`. An error if a counter can't
    /// be read or the counters count low.
    pub fn read(&self) -> Result<u64> {
        let mut readings: LPooled<Vec<Reading>> = LPooled::take();
        for fd in &self.0 {
            let mut r: Reading = [0; 3];
            let got = unsafe { libc::read(fd.as_raw_fd(), r.as_mut_ptr().cast(), mem::size_of::<Reading>()) };
            if got < 0 {
                return Err(io::Error::last_os_error()).context("reading an instruction counter");
            }
            if got as usize != mem::size_of::<Reading>() {
                bail!("reading an instruction counter gave {got} bytes");
            }
            readings.push(r);
        }
        total(&readings)
    }
}

/// The count of every kind of core's counter. Each runs only while the
/// thread is on its kind of core, so together they run whenever it does,
/// and each is enabled whenever it does.
fn total(readings: &[Reading]) -> Result<u64> {
    let count = readings.iter().map(|r| r[0]).sum();
    let enabled = readings.iter().map(|r| r[1]).max().unwrap_or(0);
    let running: u64 = readings.iter().map(|r| r[2]).sum();
    if enabled.saturating_sub(running) > SHORT_NS {
        bail!(
            "the instruction counters ran {} of the {} ms they were enabled: \
             another perf user shares them, so they count low",
            running / 1_000_000,
            enabled / 1_000_000
        );
    }
    Ok(count)
}

/// The PMU of each kind of core on a hybrid CPU, or the generic one.
fn pmus() -> Vec<u64> {
    const DIR: &str = "/sys/bus/event_source/devices";
    let Ok(devices) = fs::read_dir(DIR) else { return vec![0] };
    let mut hybrid: Vec<u64> = devices
        .flatten()
        .filter(|d| d.file_name().to_str().is_some_and(|n| n.starts_with("cpu_")))
        .filter(|d| d.path().join("events/instructions").exists())
        .filter_map(|d| fs::read_to_string(d.path().join("type")).ok()?.trim().parse().ok())
        .collect();
    if hybrid.is_empty() || fs::metadata(format!("{DIR}/cpu")).is_ok() {
        return vec![0];
    }
    hybrid.sort_unstable();
    hybrid
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{hint::black_box, thread};

    #[test]
    fn counts_the_calling_thread_only() {
        let Some(c) = Counter::start() else {
            eprintln!("instruction counters are not available here");
            return;
        };
        let spin = |n: u64| (0..n).fold(0u64, |a, i| black_box(a.wrapping_mul(31).wrapping_add(i)));
        let base = c.read().unwrap();
        black_box(spin(50_000_000));
        let spun = c.read().unwrap();
        assert!(spun - base > 50_000_000, "{}", spun - base);
        black_box(thread::spawn(move || spin(50_000_000)).join().unwrap());
        assert!(c.read().unwrap() - spun < 10_000_000, "another thread's work counted");
    }

    #[test]
    fn counters_that_ran_less_than_the_thread_count_low() {
        // On one kind of core, the other kind's counter never runs.
        assert_eq!(total(&[[5, 1000, 1000], [0, 999, 0]]).unwrap(), 5);
        // Moving between kinds, the two run in turn.
        let s = 1_000_000_000;
        assert_eq!(total(&[[5, s, s - 10_000_000], [2, s + 8, 9_990_000]]).unwrap(), 7);
        // Shared with another perf user, a counter runs part of the time.
        assert!(total(&[[5, s, s / 2], [0, s, 0]]).is_err());
        assert!(total(&[[5, 10_000_000, 5_000_000]]).is_err());
    }
}
