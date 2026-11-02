use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use poolshark::local::LPooled;
use std::{
    fmt::Write as _,
    fs::{self, File},
    io::{self, Read},
    mem,
    path::Path,
};

/// The nice level jobs and calibration run at, whatever the daemon's.
pub const JOB_NICE: i32 = 0;

/// The kernel's weight for a task at each nice level from -20 to 19
/// (`sched_prio_to_weight`): it shares a CPU among the tasks runnable there
/// in these proportions.
const NICE_WEIGHTS: [u32; 40] = [
    88761, 71755, 56483, 46273, 36291, 29154, 23254, 18705, 14949, 11916, 9548, 7620, 6100, 4904, 3906, 3121, 2501,
    1991, 1586, 1277, 1024, 820, 655, 526, 423, 335, 272, 215, 172, 137, 110, 87, 70, 56, 45, 36, 29, 23, 18, 15,
];

fn nice_weight(nice: i32) -> f64 {
    NICE_WEIGHTS[(nice + 20).clamp(0, 39) as usize] as f64
}

/// The calling thread's nice level.
fn nice() -> io::Result<i32> {
    // -1 is a nice level as well as the error return.
    unsafe { *libc::__errno_location() = 0 };
    let n = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
    match io::Error::last_os_error() {
        e if n == -1 && e.raw_os_error() != Some(0) => Err(e),
        _ => Ok(n),
    }
}

/// Set the calling thread's nice level; threads and processes it starts
/// from then on inherit it.
fn set_nice(n: i32) -> io::Result<()> {
    match unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, n) } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Raise the calling thread, and what it starts from then on, to the
/// highest priority the process may take without privileges: the lowest
/// nice level its `RLIMIT_NICE` allows. Returns the level it runs at.
pub fn prioritize() -> Result<i32> {
    let mut lim: libc::rlimit = unsafe { mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NICE, &mut lim) } != 0 {
        return Err(io::Error::last_os_error()).context("getrlimit RLIMIT_NICE");
    }
    let lowest = 20 - lim.rlim_cur.min(40) as i32;
    if lowest < nice().context("getpriority")? {
        set_nice(lowest).with_context(|| format_compact!("setting nice {lowest}"))?;
    }
    nice().context("getpriority")
}

/// Run the calling thread, or a child about to exec, at no higher priority
/// than a job: nice `JOB_NICE` or above. Raising the nice level needs no
/// privilege.
pub fn job_priority() -> io::Result<()> {
    if nice()? < JOB_NICE { set_nice(JOB_NICE) } else { Ok(()) }
}

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

/// Run `systemctl --user` with `args`.
pub fn systemctl(args: &[&str]) -> Result<()> {
    let status = std::process::Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .context("running systemctl")?;
    if !status.success() {
        bail!("systemctl --user {args:?} failed");
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

/// Memory in kB: how much the box may hold, and how much it holds now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BoxMemory {
    pub capacity_kb: u64,
    pub used_kb: u64,
}

/// zswap's compression ratio before it has stored anything.
const ZSWAP_RATIO: f64 = 3.;

/// The box's memory now. Use is what RAM holds, compressed swap's data at
/// the RAM it takes, less shared pages, such as tmpfs files, that swap on
/// disk has room for. Capacity is RAM plus `swap_share` of the data
/// compressed swap can still take: free zram swap, and the rest of zswap's
/// pool at its compression ratio, up to the free slots of the swap behind
/// it that those shared pages leave, as every page zswap holds keeps one.
pub fn box_memory(swap_share: f64) -> Result<BoxMemory> {
    let meminfo = fs::read_to_string("/proc/meminfo").context("reading /proc/meminfo")?;
    let swaps = fs::read_to_string("/proc/swaps").context("reading /proc/swaps")?;
    let param =
        |name: &str| fs::read_to_string(format_compact!("/sys/module/zswap/parameters/{name}").as_str());
    let zswap_pool_percent = match param("enabled") {
        Ok(on) if on.trim() == "Y" => param("max_pool_percent")?.trim().parse().ok(),
        _ => None,
    };
    memory(&meminfo, &swaps, zswap_pool_percent, swap_share)
}

fn memory(meminfo: &str, swaps: &str, zswap_pool_percent: Option<u64>, swap_share: f64) -> Result<BoxMemory> {
    let field = |name: &str| -> Option<u64> {
        meminfo.lines().find_map(|l| l.strip_prefix(name))?.trim().trim_end_matches("kB").trim().parse().ok()
    };
    let need = |name: &str| field(name).with_context(|| format_compact!("no {name} in /proc/meminfo"));
    let (total, available) = (need("MemTotal:")?, need("MemAvailable:")?);
    let swap_free = need("SwapFree:")?;
    let (zswap, zswapped) = (field("Zswap:").unwrap_or(0), field("Zswapped:").unwrap_or(0));
    let shared = field("Shmem:").unwrap_or(0);
    let (mut zram_kb, mut zram_used) = (0, 0);
    for line in swaps.lines().skip(1) {
        let mut cols = line.split_whitespace();
        let (Some(dev), Some(size), Some(used)) = (cols.next(), cols.nth(1), cols.next()) else { continue };
        if dev.strip_prefix("/dev/").is_some_and(|n| n.starts_with("zram")) {
            zram_kb += size.parse::<u64>().unwrap_or(0);
            zram_used += used.parse::<u64>().unwrap_or(0);
        }
    }
    let zram_free = zram_kb.saturating_sub(zram_used);
    let disk_free = swap_free.saturating_sub(zram_free);
    let shared_out = shared.min(disk_free);
    let zswap_free = zswap_pool_percent.map_or(0, |pct| {
        let ratio = if zswap > 0 { zswapped as f64 / zswap as f64 } else { ZSWAP_RATIO };
        let pool = (total * pct / 100) as f64;
        ((pool * ratio) as u64).saturating_sub(zswapped).min(disk_free - shared_out)
    });
    Ok(BoxMemory {
        capacity_kb: total + ((zram_free + zswap_free) as f64 * swap_share) as u64,
        used_kb: total.saturating_sub(available).saturating_sub(shared_out),
    })
}

/// The memory `pid` and its descendants hold in RAM or swap, in kB: see
/// [`status_kb`].
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

/// Resident anonymous and shared memory plus swap from a
/// `/proc/PID/status`. File-backed pages, such as rustc's and LLVM's
/// code, which every rustc on the box shares, are left out, as
/// [`box_memory`] counts page cache as available.
fn status_kb(status: &str) -> u64 {
    ["RssAnon:", "RssShmem:", "VmSwap:"]
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

/// Peak RSS of the children this process has waited for.
pub fn children_maxrss_kb() -> u64 {
    let mut ru: libc::rusage = unsafe { mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru) };
    ru.ru_maxrss as u64
}

/// Tasks runnable on the box now other than this process's, each counted
/// by its nice level's weight against a job's, as the kernel shares a CPU
/// between them: work at nice 19 hardly slows a job, and counts 1/68 of a
/// task.
pub fn runnable() -> Result<f64> {
    runnable_in(Path::new("/proc"), std::process::id())
}

fn runnable_in(proc: &Path, skip: u32) -> Result<f64> {
    let mut path = proc.to_path_buf();
    let mut buf = [0u8; 1024];
    let mut total = 0.;
    for entry in fs::read_dir(proc).with_context(|| format_compact!("listing {}", proc.display()))?.flatten() {
        let name = entry.file_name();
        if name.to_str().and_then(|s| s.parse::<u32>().ok()).is_none_or(|pid| pid == skip) {
            continue;
        }
        path.push(&name);
        path.push("task");
        // A process or task that exits meanwhile is skipped.
        if let Ok(tasks) = fs::read_dir(&path) {
            for task in tasks.flatten() {
                path.push(task.file_name());
                path.push("stat");
                if let Ok(n) = File::open(&path).and_then(|mut f| f.read(&mut buf)) {
                    total += weight(&buf[..n]).with_context(|| format_compact!("reading {}", path.display()))?;
                }
                path.pop();
                path.pop();
            }
        }
        path.pop();
        path.pop();
    }
    Ok(total / nice_weight(JOB_NICE))
}

/// The weight a task's `stat` line adds to the box's load: its nice
/// level's if it is runnable, else none. None if the line can't be read,
/// and nothing for an empty one, a task that exited.
fn weight(stat: &[u8]) -> Option<f64> {
    if stat.is_empty() {
        return Some(0.);
    }
    // The command name, in parentheses, may hold spaces and parentheses.
    let close = stat.iter().rposition(|&b| b == b')')?;
    let mut fields = stat.get(close + 2..)?.split(|&b| b == b' ');
    if fields.next()? != b"R" {
        return Some(0.);
    }
    let nice = std::str::from_utf8(fields.nth(15)?).ok()?.parse().ok()?;
    Some(nice_weight(nice))
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
    fn memory_counts_compressed_swap_at_the_ram_it_takes() -> Result<()> {
        // 64 GB of RAM and a 64 GB zram device holding 11 GB.
        let meminfo = "MemTotal:       65301996 kB\nMemFree:        25249360 kB\n\
                       MemAvailable:   50326284 kB\nSwapTotal:      65301500 kB\n\
                       SwapFree:       53757096 kB\nZswap:                 0 kB\nZswapped:              0 kB\n";
        let swaps = "Filename\tType\tSize\tUsed\tPriority\n/dev/zram0  partition\t65301500\t11544404\t100\n";
        let m = memory(meminfo, swaps, None, 0.5)?;
        assert_eq!(m.capacity_kb, 65_301_996 + (65_301_500 - 11_544_404) / 2);
        assert_eq!(m.used_kb, 65_301_996 - 50_326_284);
        // zswap with a 20% pool, holding 400 MB in 100 MB, in front of a
        // disk: room for 400 MB more at 4:1.
        let meminfo = "MemTotal: 1000000 kB\nMemAvailable: 600000 kB\nSwapTotal: 8000000 kB\n\
                       SwapFree: 7000000 kB\nZswap: 100000 kB\nZswapped: 400000 kB\n";
        let m = memory(meminfo, "Filename Type Size Used Priority\n", Some(20), 0.5)?;
        assert_eq!(m.capacity_kb, 1_000_000 + (200_000 * 4 - 400_000) / 2);
        assert_eq!(m.used_kb, 400_000);
        // The same pool in front of an 8 GB disk holding 7 GB written back
        // from zswap and the 500 MB zswap holds: each of those keeps a slot,
        // so only the 500 MB of free slots is room.
        let meminfo = "MemTotal: 16000000 kB\nMemAvailable: 9000000 kB\nSwapTotal: 8000000 kB\n\
                       SwapFree: 500000 kB\nZswap: 125000 kB\nZswapped: 500000 kB\n";
        let swaps = "Filename Type Size Used Priority\n/dev/nvme0n1p3 partition 8000000 7500000 -2\n";
        let m = memory(meminfo, swaps, Some(20), 0.5)?;
        assert_eq!(m.capacity_kb, 16_000_000 + 500_000 / 2);
        Ok(())
    }

    #[test]
    fn tmpfs_pages_count_as_free_while_swap_on_disk_has_room() -> Result<()> {
        // A full 64 GB zram device holding 56 GB in 18 GB of RAM, a 128 GB
        // swap file with 8 GB on it, and 7 GB of tmpfs pages in RAM.
        let meminfo = "MemTotal:       65301992 kB\nMemAvailable:   20964736 kB\nShmem:           7674072 kB\n\
                       ShmemHugePages:        0 kB\nSwapTotal:      199519224 kB\nSwapFree:       128932344 kB\n";
        let swaps = "Filename\tType\tSize\tUsed\tPriority\n\
                     /var/swap/swap   file\t134217724\t7802304\t1\n\
                     /dev/zram0  partition\t65301500\t62784576\t100\n";
        let m = memory(meminfo, swaps, None, 0.5)?;
        assert_eq!(m.capacity_kb, 65_301_992 + (65_301_500 - 62_784_576) / 2);
        assert_eq!(m.used_kb, (65_301_992 - 20_964_736) - 7_674_072);
        // With the swap file full, they stay in RAM.
        let full = meminfo.replace("128932344", "2516924");
        let m = memory(&full, swaps, None, 0.5)?;
        assert_eq!(m.used_kb, 65_301_992 - 20_964_736);
        Ok(())
    }

    #[test]
    fn tmpfs_pages_claim_free_disk_swap_before_zswap() -> Result<()> {
        // zswap with a 20% pool of 16 GB, empty, in front of a disk with
        // 2 GB of free slots, and 1.5 GB of tmpfs pages in RAM: the tmpfs
        // pages take 1.5 GB of the slots, leaving zswap 500 MB.
        let meminfo = "MemTotal: 16000000 kB\nMemAvailable: 9000000 kB\nShmem: 1500000 kB\n\
                       SwapTotal: 8000000 kB\nSwapFree: 2000000 kB\nZswap: 0 kB\nZswapped: 0 kB\n";
        let swaps = "Filename Type Size Used Priority\n/dev/nvme0n1p3 partition 8000000 6000000 -2\n";
        let m = memory(meminfo, swaps, Some(20), 0.5)?;
        assert_eq!(m.used_kb, 7_000_000 - 1_500_000);
        assert_eq!(m.capacity_kb, 16_000_000 + 500_000 / 2);
        // tmpfs pages beyond the free slots take them all: no room is left
        // for zswap.
        let big = meminfo.replace("Shmem: 1500000", "Shmem: 3000000");
        let m = memory(&big, swaps, Some(20), 0.5)?;
        assert_eq!(m.used_kb, 7_000_000 - 2_000_000);
        assert_eq!(m.capacity_kb, 16_000_000);
        Ok(())
    }

    /// A task's `stat` line: `pid (comm) state`, then fields up to `nice`.
    fn stat(comm: &str, state: char, nice: i32) -> String {
        format!("42 ({comm}) {state} 1 42 42 0 -1 4194304 100 0 0 0 5 1 0 0 {} {nice} 1 0 100 1000 50\n", 20 + nice)
    }

    #[test]
    fn a_runnable_task_weighs_what_its_nice_level_does() {
        assert_eq!(weight(stat("rustc", 'R', 0).as_bytes()), Some(1024.));
        assert_eq!(weight(stat("exe", 'R', 19).as_bytes()), Some(15.));
        assert_eq!(weight(stat("a (b) c", 'R', -11).as_bytes()), Some(11916.));
        assert_eq!(weight(stat("rustc", 'S', 0).as_bytes()), Some(0.));
        assert_eq!(weight(b""), Some(0.), "it exited");
        assert_eq!(weight(b"42 (rustc"), None);
        assert_eq!(weight(b"42 (rustc) R 1 2"), None);
        // This thread is running, at the test's nice level.
        let own = fs::read(format!("/proc/self/task/{}/stat", unsafe { libc::gettid() })).unwrap();
        assert_eq!(weight(&own), Some(nice_weight(nice().unwrap())));
    }

    #[test]
    fn the_load_weighs_every_runnable_task_but_this_processs() -> Result<()> {
        let proc = std::env::temp_dir().join(format!("rustc-dist-proc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&proc);
        let task = |pid: u32, tid: u32, line: String| -> Result<()> {
            let dir = proc.join(format!("{pid}/task/{tid}"));
            fs::create_dir_all(&dir)?;
            Ok(fs::write(dir.join("stat"), line)?)
        };
        task(10, 10, stat("rustc", 'R', 0))?;
        task(10, 11, stat("rustc", 'S', 0))?;
        task(20, 20, stat("exe", 'R', 19))?;
        task(30, 30, stat("rustc-dist", 'R', -11))?;
        fs::create_dir_all(proc.join("sys/task/1"))?;
        let load = runnable_in(&proc, 30)?;
        fs::remove_dir_all(&proc)?;
        assert!((load - (1. + 15. / 1024.)).abs() < 1e-9, "{load}");
        assert!(runnable()? >= 0.);
        Ok(())
    }

    #[test]
    fn jobs_run_no_higher_than_job_nice() -> Result<()> {
        // A thread of its own: the nice level is the thread's.
        std::thread::spawn(|| -> Result<()> {
            let before = nice()?;
            let raised = prioritize()?;
            assert!(raised <= before);
            assert_eq!(nice()?, raised);
            job_priority()?;
            assert_eq!(nice()?, raised.max(JOB_NICE));
            Ok(())
        })
        .join()
        .unwrap()
    }

    #[test]
    fn status_counts_anonymous_shared_and_swapped_but_not_file_pages() {
        let status = "Name:\trustc\nVmPeak:\t 9000 kB\nVmRSS:\t    5012 kB\nRssAnon:\t 624 kB\n\
                      RssFile:\t    4300 kB\nRssShmem:\t      88 kB\nVmSwap:\t     300 kB\n";
        assert_eq!(status_kb(status), 624 + 88 + 300);
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
