use anyhow::{Context, Result, bail};
use std::{fs, io, mem};

/// Pin the calling thread, and the processes it spawns, to `cpus`.
pub fn pin(cpus: &[u32]) -> Result<()> {
    let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
    for &cpu in cpus {
        unsafe { libc::CPU_SET(cpu as usize, &mut set) };
    }
    if unsafe { libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set) } != 0 {
        return Err(io::Error::last_os_error()).context("sched_setaffinity");
    }
    Ok(())
}

/// The logical CPUs this process may run on.
pub fn allowed_cpus() -> Result<Vec<u32>> {
    let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
    if unsafe { libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mut set) } != 0 {
        return Err(io::Error::last_os_error()).context("sched_getaffinity");
    }
    let cpus: Vec<u32> = (0..libc::CPU_SETSIZE as u32)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu as usize, &set) })
        .collect();
    if cpus.is_empty() {
        bail!("no cpus are available to this process");
    }
    Ok(cpus)
}

/// CPU time the calling thread has used. Unlike wall time, it doesn't
/// grow while the thread waits for a CPU that other work holds.
pub fn thread_cpu_us() -> u64 {
    let mut ts: libc::timespec = unsafe { mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1000
}

pub fn mem_total_kb() -> Result<u64> {
    let text = fs::read_to_string("/proc/meminfo").context("reading /proc/meminfo")?;
    let line = text
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .context("no MemTotal in /proc/meminfo")?;
    let kb = line.trim().trim_end_matches("kB").trim();
    kb.parse().context("parsing MemTotal")
}

pub struct Usage {
    pub cpu_us: u64,
    pub maxrss_kb: u64,
}

/// Resource use of the children this process has waited for.
pub fn children_usage() -> Usage {
    let mut ru: libc::rusage = unsafe { mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru) };
    let us = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
    Usage { cpu_us: us(ru.ru_utime) + us(ru.ru_stime), maxrss_kb: ru.ru_maxrss as u64 }
}
