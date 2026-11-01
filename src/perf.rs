//! Instructions retired, counted by the CPU.

use std::{
    fs,
    mem,
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

    pub fn read(&self) -> u64 {
        self.0
            .iter()
            .map(|fd| {
                let mut n = 0u64;
                let got = unsafe { libc::read(fd.as_raw_fd(), (&raw mut n).cast(), 8) };
                if got == 8 { n } else { 0 }
            })
            .sum()
    }
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
        let base = c.read();
        black_box(spin(50_000_000));
        let spun = c.read();
        assert!(spun - base > 50_000_000, "{}", spun - base);
        black_box(thread::spawn(move || spin(50_000_000)).join().unwrap());
        assert!(c.read() - spun < 10_000_000, "another thread's work counted");
    }
}
