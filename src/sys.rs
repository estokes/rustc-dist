use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use poolshark::local::LPooled;
use std::{
    fmt::Write as _,
    fs::{self, File},
    io::{self, Read},
    mem,
};

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

pub fn running_as_root() -> bool {
    unsafe { libc::geteuid() == 0 }
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

/// The CPUs among `cpus` grouped by the core they share, each group sorted
/// and the groups in order of their first CPU. A CPU whose siblings can't
/// be read is a core of its own.
pub fn cores(cpus: &[u32]) -> Vec<Vec<u32>> {
    let mut cores: Vec<Vec<u32>> = Vec::new();
    for &cpu in cpus {
        if cores.iter().any(|core| core.contains(&cpu)) {
            continue;
        }
        let path = format_compact!("/sys/devices/system/cpu/cpu{cpu}/topology/core_cpus_list");
        let siblings = fs::read_to_string(path.as_str()).ok().and_then(|s| cpu_list(s.trim()).ok());
        let mut core: Vec<u32> = match siblings {
            Some(s) if s.contains(&cpu) => s.into_iter().filter(|c| cpus.contains(c)).collect(),
            _ => vec![cpu],
        };
        core.sort_unstable();
        cores.push(core);
    }
    cores
}

/// A kernel CPU list such as `0-3,8,10-11`.
fn cpu_list(s: &str) -> Result<Vec<u32>> {
    let mut cpus = Vec::new();
    for part in s.split(',') {
        match part.split_once('-') {
            Some((lo, hi)) => cpus.extend(lo.parse::<u32>()?..=hi.parse::<u32>()?),
            None => cpus.push(part.parse()?),
        }
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

/// Memory in kB of uncompressed data: how much the box may hold, and how
/// much it holds now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BoxMemory {
    pub capacity_kb: u64,
    pub used_kb: u64,
}

/// zswap's compression ratio before it has stored anything.
const ZSWAP_RATIO: f64 = 3.;

/// The box's memory now. Capacity is RAM plus `swap_share` of compressed
/// swap: zram swap devices, and zswap's pool at its compression ratio.
/// Use counts swapped pages at their uncompressed size and leaves out the
/// RAM their compressed copies take.
pub fn box_memory(swap_share: f64) -> Result<BoxMemory> {
    let meminfo = fs::read_to_string("/proc/meminfo").context("reading /proc/meminfo")?;
    let swaps = fs::read_to_string("/proc/swaps").context("reading /proc/swaps")?;
    let param =
        |name: &str| fs::read_to_string(format_compact!("/sys/module/zswap/parameters/{name}").as_str());
    let zswap_pool_percent = match param("enabled") {
        Ok(on) if on.trim() == "Y" => param("max_pool_percent")?.trim().parse().ok(),
        _ => None,
    };
    memory(&meminfo, &swaps, zram_ram_kb, zswap_pool_percent, swap_share)
}

/// RAM a zram device's compressed data takes.
fn zram_ram_kb(name: &str) -> u64 {
    let stat = fs::read_to_string(format_compact!("/sys/block/{name}/mm_stat").as_str()).unwrap_or_default();
    stat.split_whitespace().nth(2).and_then(|b| b.parse::<u64>().ok()).unwrap_or(0) / 1024
}

fn memory(
    meminfo: &str,
    swaps: &str,
    zram_ram_kb: impl Fn(&str) -> u64,
    zswap_pool_percent: Option<u64>,
    swap_share: f64,
) -> Result<BoxMemory> {
    let field = |name: &str| -> Option<u64> {
        meminfo.lines().find_map(|l| l.strip_prefix(name))?.trim().trim_end_matches("kB").trim().parse().ok()
    };
    let need = |name: &str| field(name).with_context(|| format_compact!("no {name} in /proc/meminfo"));
    let (total, available) = (need("MemTotal:")?, need("MemAvailable:")?);
    let (swap_total, swap_free) = (need("SwapTotal:")?, need("SwapFree:")?);
    let (zswap, zswapped) = (field("Zswap:").unwrap_or(0), field("Zswapped:").unwrap_or(0));
    let (mut zram_kb, mut zram_ram) = (0, 0);
    for line in swaps.lines().skip(1) {
        let mut cols = line.split_whitespace();
        let (Some(dev), Some(size)) = (cols.next(), cols.nth(1)) else { continue };
        if let Some(name) = dev.strip_prefix("/dev/").filter(|n| n.starts_with("zram")) {
            zram_kb += size.parse::<u64>().unwrap_or(0);
            zram_ram += zram_ram_kb(name);
        }
    }
    let zswap_kb = zswap_pool_percent.map_or(0, |pct| {
        let ratio = if zswap > 0 { zswapped as f64 / zswap as f64 } else { ZSWAP_RATIO };
        let pool = (total * pct / 100) as f64;
        ((pool * ratio) as u64).min(swap_total.saturating_sub(zram_kb))
    });
    Ok(BoxMemory {
        capacity_kb: total + ((zram_kb + zswap_kb) as f64 * swap_share) as u64,
        used_kb: total.saturating_sub(available).saturating_sub(zram_ram + zswap)
            + swap_total.saturating_sub(swap_free),
    })
}

/// RSS plus swap, in kB, of `pid` and its descendants.
pub fn tree_footprint_kb(pid: u32) -> u64 {
    let mut path: LPooled<String> = LPooled::take();
    let mut text: LPooled<String> = LPooled::take();
    let mut kb = 0;
    descendants(pid, |p| {
        path.clear();
        let _ = write!(path, "/proc/{p}/status");
        if read_into(&path, &mut text) {
            kb += status_kb(&text);
        }
    });
    kb
}

/// VmRSS plus VmSwap from a `/proc/PID/status`.
fn status_kb(status: &str) -> u64 {
    ["VmRSS:", "VmSwap:"]
        .iter()
        .filter_map(|f| status.lines().find_map(|l| l.strip_prefix(f)))
        .filter_map(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .sum()
}

/// Call `f` with `pid` and each of its descendants, following the
/// children of every thread.
fn descendants(pid: u32, mut f: impl FnMut(u32)) {
    let mut stack: LPooled<Vec<u32>> = LPooled::take();
    let mut path: LPooled<String> = LPooled::take();
    let mut text: LPooled<String> = LPooled::take();
    stack.push(pid);
    while let Some(p) = stack.pop() {
        f(p);
        path.clear();
        let _ = write!(path, "/proc/{p}/task");
        let Ok(tasks) = fs::read_dir(path.as_str()) else { continue };
        for task in tasks.flatten() {
            path.clear();
            let _ = write!(path, "/proc/{p}/task/{}/children", task.file_name().to_string_lossy());
            if read_into(&path, &mut text) {
                stack.extend(text.split_whitespace().filter_map(|c| c.parse::<u32>().ok()));
            }
        }
    }
}

fn read_into(path: &str, text: &mut String) -> bool {
    text.clear();
    File::open(path).and_then(|mut f| f.read_to_string(text)).is_ok()
}

/// User plus system CPU time, in µs, of `pid` and its descendants so far,
/// counting every thread.
pub fn tree_cpu_us(pid: u32) -> u64 {
    let mut path: LPooled<String> = LPooled::take();
    let mut text: LPooled<String> = LPooled::take();
    let mut ticks = 0;
    descendants(pid, |p| {
        path.clear();
        let _ = write!(path, "/proc/{p}/stat");
        if read_into(&path, &mut text) {
            ticks += stat_ticks(&text);
        }
    });
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
    ticks * 1_000_000 / hz
}

/// utime plus stime from a `/proc/PID/stat`. The command name in
/// parentheses may hold spaces, so fields count from the last `)`.
fn stat_ticks(stat: &str) -> u64 {
    let Some(rest) = stat.rfind(')').map(|i| &stat[i + 1..]) else { return 0 };
    let mut fields = rest.split_whitespace().skip(11);
    let mut next = || fields.next().and_then(|f| f.parse::<u64>().ok()).unwrap_or(0);
    next() + next()
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

#[cfg(test)]
mod test {
    use super::*;
    use std::process::Command;

    #[test]
    fn cpu_lists_and_cores() -> Result<()> {
        assert_eq!(cpu_list("0-3,8,10-11")?, vec![0, 1, 2, 3, 8, 10, 11]);
        assert_eq!(cpu_list("0,18")?, vec![0, 18]);
        assert!(cpu_list("0-x").is_err());
        // Every allowed cpu is in exactly one core, the first of each core
        // first.
        let allowed = allowed_cpus()?;
        let cores = cores(&allowed);
        let mut all: Vec<u32> = cores.iter().flatten().copied().collect();
        all.sort_unstable();
        assert_eq!(all, allowed);
        assert!(cores.windows(2).all(|w| w[0][0] < w[1][0]));
        Ok(())
    }

    #[test]
    fn memory_counts_compressed_swap_uncompressed() -> Result<()> {
        // This box: 64 GB of RAM and a 64 GB zram device holding 11 GB in
        // 3.3 GB of RAM.
        let meminfo = "MemTotal:       65301996 kB\nMemFree:        25249360 kB\n\
                       MemAvailable:   50326284 kB\nSwapTotal:      65301500 kB\n\
                       SwapFree:       53757096 kB\nZswap:                 0 kB\nZswapped:              0 kB\n";
        let swaps = "Filename\tType\tSize\tUsed\tPriority\n/dev/zram0  partition\t65301500\t11544404\t100\n";
        let zram = |name: &str| if name == "zram0" { 3_449_284 } else { 0 };
        let m = memory(meminfo, swaps, zram, None, 0.5)?;
        assert_eq!(m.capacity_kb, 65_301_996 + 65_301_500 / 2);
        assert_eq!(m.used_kb, (65_301_996 - 50_326_284) - 3_449_284 + (65_301_500 - 53_757_096));
        // zswap with a 20% pool, holding 4 GB in 1 GB, in front of a disk.
        let meminfo = "MemTotal: 1000000 kB\nMemAvailable: 600000 kB\nSwapTotal: 8000000 kB\n\
                       SwapFree: 7000000 kB\nZswap: 100000 kB\nZswapped: 400000 kB\n";
        let m = memory(meminfo, "Filename Type Size Used Priority\n", |_| 0, Some(20), 0.5)?;
        assert_eq!(m.capacity_kb, 1_000_000 + 200_000 * 4 / 2);
        assert_eq!(m.used_kb, 400_000 - 100_000 + 1_000_000);
        Ok(())
    }

    #[test]
    fn stat_ticks_skip_the_command_name() {
        let stat = "4242 (rustc (x) y) R 1 2 3 4 5 6 7 8 9 10 150 25 0 0 20 0 17";
        assert_eq!(stat_ticks(stat), 175);
        assert!(tree_cpu_us(std::process::id()) > 0);
    }

    #[test]
    fn status_counts_resident_and_swapped() {
        let status =
            "Name:\trustc\nVmPeak:\t 9000 kB\nVmRSS:\t    5012 kB\nRssAnon:\t 624 kB\nVmSwap:\t     300 kB\n";
        assert_eq!(status_kb(status), 5312);
    }

    #[test]
    fn descendants_include_children_of_every_thread() -> Result<()> {
        // This runs on a test thread, not the process's main thread.
        let mut child = Command::new("sleep").arg("10").spawn()?;
        let mut seen = Vec::new();
        descendants(std::process::id(), |p| seen.push(p));
        let alone = tree_footprint_kb(child.id());
        child.kill()?;
        child.wait()?;
        assert!(seen.contains(&child.id()), "{seen:?}");
        assert!(alone > 0);
        Ok(())
    }
}
