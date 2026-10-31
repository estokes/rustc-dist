mod argv;
mod blobs;
mod calibrate;
mod classes;
mod client;
mod config;
mod daemon;
mod farm;
mod graph;
mod link;
mod perf;
mod protocol;
mod remote;
mod sandbox;
mod sched;
mod stats;
mod sys;
mod toolchain;
mod wrapper;

#[cfg(test)]
mod e2e_test;

use anyhow::{Result, bail};
use bytes::BytesMut;
use clap::{Args, Parser};
use compact_str::{CompactString, format_compact};
use classes::Profile;
use protocol::{Ask, Reply, Switching};
use std::{
    ffi::OsString,
    io::{self, Write},
};

const AFTER_HELP: &str = "\
Cargo runs this binary as its rustc wrapper. The first argument is the
compiler:

  ~/.cargo/config.toml
    [build]
    rustc-wrapper = \"rustc-dist\"
    jobs = 128    # the daemon does the limiting; cargo holds about 4 open
                  # files per job, and 128 fits the usual limit of 1024

Every rustc asks the daemon on its box for a slot and waits until one is
free. The daemon runs a job measured to use about one thread pinned to a
core class, and any other job on every core, with jobserver tokens lent
from the classes. It records each unit's work so the jobs the most work
waits on get fast cores, and in a farm it sends jobs to other boxes when
they finish sooner there.

Run `rustc-dist install` to enable the systemd user service. On a laptop,
have the desktop run `rustc-dist profile NAME` when the power profile
changes. The daemon calibrates each profile name the first time it sees
it, once running jobs finish; new jobs wait meanwhile. Calibrate each
profile ahead of time on an idle box with `rustc-dist profile NAME
--recalibrate`. RUSTC_DIST=off runs rustc directly.";

#[derive(Parser)]
#[command(name = "rustc-dist", version, about = "Schedules rustc on the cores of a box", after_help = AFTER_HELP)]
enum Opt {
    /// Run the daemon. The wrapper starts it when it is not running
    Daemon,
    /// Enable the systemd user service
    Install,
    /// Tell the daemon the power profile changed. A new name is calibrated first
    Profile {
        name: String,
        /// Calibrate this profile again
        #[arg(long)]
        recalibrate: bool,
    },
    /// Calibrate this box and print its core classes without saving them
    Calibrate(CalibrateArgs),
    /// Show the core classes, queue, and job history of the running daemon
    Status,
    /// Cargo's rustc invocation. The first argument is the compiler
    #[command(external_subcommand)]
    Wrapper(Vec<OsString>),
}

#[derive(Args)]
struct CalibrateArgs {
    /// How long to run the calibration load
    #[arg(long, default_value_t = 60)]
    seconds: u64,
}

fn main() {
    if let Err(e) = dispatch() {
        if e.downcast_ref::<io::Error>().is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe) {
            return;
        }
        eprintln!("rustc-dist: {e:#}");
        std::process::exit(1);
    }
}

fn dispatch() -> Result<()> {
    match Opt::parse() {
        Opt::Daemon => {
            env_logger::Builder::from_env(env_logger::Env::new().filter_or("RUSTC_DIST_LOG", "info"))
                .init();
            let rt = tokio::runtime::Runtime::new()?;
            let res = rt.block_on(daemon::run());
            // Don't wait out a calibration that is still running.
            rt.shutdown_timeout(std::time::Duration::from_secs(1));
            res
        }
        Opt::Install => calibrate::install(),
        Opt::Profile { name, recalibrate } => profile(&name, recalibrate),
        Opt::Calibrate(a) => calibrate::print(a.seconds),
        Opt::Status => status(),
        Opt::Wrapper(args) => wrapper::run(args),
    }
}

fn profile(name: &str, recalibrate: bool) -> Result<()> {
    let profile = Profile::new(name)?;
    // A daemon this starts then begins in the new profile, instead of
    // first calibrating the old one.
    classes::set_current(&config::state_dir(), &profile)?;
    let ask = Ask::Profile { name: profile.name().clone(), recalibrate };
    let mut buf = BytesMut::new();
    match client::request(ask, true, &mut buf)?.1 {
        Reply::Done => Ok(()),
        Reply::Refused(why) => bail!("{why}"),
        _ => bail!("unexpected reply from the daemon"),
    }
}

fn status() -> Result<()> {
    let mut buf = BytesMut::new();
    let Reply::Status(st) = client::request(Ask::Status, false, &mut buf)?.1 else {
        bail!("unexpected reply from the daemon");
    };
    let mut out = io::stdout().lock();
    match &st.switching {
        Switching::No => writeln!(out, "profile {}", st.profile)?,
        Switching::Draining(to) => writeln!(
            out,
            "profile {}, switching to {to}: waiting for running jobs, then calibrating",
            st.profile
        )?,
        Switching::Calibrating(to) => writeln!(out, "profile {}, calibrating {to}", st.profile)?,
    }
    writeln!(out, "\nclass  cpus  G instr/s  running  serving  lent  free")?;
    for (k, c) in st.classes.iter().enumerate() {
        writeln!(
            out,
            "{k:5}  {:4}  {:9.2}  {:7}  {:7}  {:4}  {:4}",
            c.cpus,
            c.rate as f64 / 1000.,
            c.running,
            c.serving,
            c.lent,
            c.free
        )?;
    }
    writeln!(
        out,
        "on every cpu: {} running, {} serving; lent tokens are theirs",
        st.spread.running, st.spread.serving
    )?;
    if st.finishing > 0 {
        writeln!(out, "{} jobs finishing under the previous profile", st.finishing)?;
    }
    let gib = |kb: u64| kb as f64 / (1024. * 1024.);
    writeln!(
        out,
        "\n{} waiting. Memory: {:.1} GiB in use, running jobs predicted to add {:.1}, capacity {:.1}",
        st.waiting,
        gib(st.mem_used_kb),
        gib(st.mem_growth_kb),
        gib(st.mem_capacity_kb)
    )?;
    for b in &st.boxes {
        let cpus: u32 = b.classes.iter().map(|c| c.cpus).sum();
        let free: u32 = b.classes.iter().map(|c| c.free).sum();
        let capacity: u64 = b.classes.iter().map(|c| c.rate * c.cpus as u64).sum();
        let note = if b.compatible { "" } else { ", other version" };
        writeln!(
            out,
            "box {}: profile {}, {free} of {cpus} cpus free, {:.0} G instr/s, {} from here{note}",
            b.name,
            b.profile,
            capacity as f64 / 1000.,
            b.sent
        )?;
    }
    if st.units.is_empty() {
        return Ok(());
    }
    writeln!(
        out,
        "\n{} units run, by work waiting on them ({} in the build graph). Work is in G instructions; \
         front is the single-threaded share, width the threads busy after it. Runs: here and sent \
         for this box's builds, served for other boxes'. Copy is the time a sent run spent on its \
         files rather than compiling, and MB what it sent (medians).",
        st.units.len(),
        st.graph_nodes
    )?;
    writeln!(
        out,
        "\n{:>8}  {:>8}  {:>5}  {:>5}  {:>8}  {:>8}  {:>5}  {:>5}  {:>6}  {:>8}  {:>7}  unit",
        "rank", "work", "front", "width", "wall (s)", "mem (MB)", "here", "sent", "served", "copy (s)", "MB sent"
    )?;
    for u in &st.units {
        let rank: CompactString = match u.rank_mi {
            Some(r) => format_compact!("{:8.2}", r as f64 / 1000.),
            None => format_compact!("{:>8}", "-"),
        };
        let front: CompactString = match u.front_mi {
            Some(f) => format_compact!("{:4}%", f * 100 / u.cost_mi.max(1)),
            None => format_compact!("{:>5}", "-"),
        };
        let (copy, sent): (CompactString, CompactString) = match (u.copy_ms, u.sent_kb) {
            (Some(ms), Some(kb)) => (format_compact!("{:8.2}", ms as f64 / 1000.), format_compact!("{:7.1}", kb as f64 / 1024.)),
            _ => (format_compact!("{:>8}", "-"), format_compact!("{:>7}", "-")),
        };
        writeln!(
            out,
            "{rank}  {:8.2}  {front}  {:5.1}  {:8.1}  {:8}  {:5}  {:5}  {:6}  {copy}  {sent}  {}",
            u.cost_mi as f64 / 1000.,
            u.width_c as f64 / 100.,
            u.wall_ms as f64 / 1000.,
            u.mem_kb / 1024,
            u.here,
            u.sent,
            u.served,
            u.key
        )?;
    }
    Ok(())
}
