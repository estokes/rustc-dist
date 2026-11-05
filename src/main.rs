mod argv;
mod blobs;
mod browser;
mod calibrate;
mod classes;
mod client;
mod config;
mod daemon;
mod farm;
mod graph;
mod link;
mod pairs;
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

use anyhow::{Error, Result, bail};
use bytes::BytesMut;
use clap::{Args, Parser};
use compact_str::{CompactString, format_compact};
use classes::Profile;
use protocol::{Ask, Reply, Switching, gib, hundredths};
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
    let opt = Opt::parse();
    let prints = opt.prints();
    if let Err(e) = dispatch(opt) {
        if quiet(prints, &e) {
            return;
        }
        eprintln!("rustc-dist: {e:#}");
        std::process::exit(1);
    }
}

impl Opt {
    fn prints(&self) -> bool {
        matches!(self, Opt::Status | Opt::Calibrate(_))
    }
}

/// A broken pipe ends a command that prints quietly, as when `head` stops
/// reading. For any other, the wrapper above all, it is an error.
fn quiet(prints: bool, e: &Error) -> bool {
    prints && e.downcast_ref::<io::Error>().is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe)
}

fn dispatch(opt: Opt) -> Result<()> {
    match opt {
        Opt::Daemon => {
            env_logger::Builder::from_env(env_logger::Env::new().filter_or("RUSTC_DIST_LOG", "info"))
                .init();
            // Before the runtime starts its threads, which inherit it.
            let nice = sys::prioritize()?;
            log::info!("the daemon runs at nice {nice}, jobs at {}", sys::JOB_NICE);
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
    // A daemon started later begins in this profile, instead of first
    // calibrating the old one.
    classes::set_current(&config::state_dir(), &profile)?;
    let ask = Ask::Profile { name: profile.name().clone(), recalibrate };
    let mut buf = BytesMut::new();
    match client::request_running(&ask, &mut buf)? {
        None => {
            let sock = config::socket_path();
            eprintln!("no daemon at {}; the next one will start in profile {}", sock.display(), profile.name());
            Ok(())
        }
        Some(Reply::Done) => Ok(()),
        Some(Reply::Refused(why)) => bail!("{why}"),
        Some(_) => bail!("unexpected reply from the daemon"),
    }
}

fn status() -> Result<()> {
    let mut buf = BytesMut::new();
    let Reply::Status(st) = client::request(&Ask::Status, false, &mut buf)?.1 else {
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
    writeln!(
        out,
        "load {:.2} runnable tasks per cpu, weighted by nice level. Speeds are relative to class 0 idle, learned from the jobs that \
         ran each place; work and times are seconds.",
        hundredths(st.load_c)
    )?;
    writeln!(out, "\nclass  cpus  speed idle  speed now  running  serving  lent  free")?;
    for (k, c) in st.classes.iter().enumerate() {
        writeln!(
            out,
            "{k:5}  {:4}  {:10.2}  {:9.2}  {:7}  {:7}  {:4}  {:4}",
            c.cpus,
            hundredths(c.idle_speed_c),
            hundredths(c.speed_c),
            c.running,
            c.serving,
            c.lent,
            c.free
        )?;
    }
    writeln!(
        out,
        "on every cpu: {} threads at {:.2} each now, {} running, {} serving; lent tokens are theirs",
        st.spread.threads,
        hundredths(st.spread.thread_speed_c),
        st.spread.running,
        st.spread.serving
    )?;
    if st.finishing > 0 {
        writeln!(out, "{} jobs finishing under the previous profile", st.finishing)?;
    }
    writeln!(
        out,
        "\n{} waiting. Memory: {:.1} GiB in use, running jobs predicted to add {:.1}, capacity {:.1}",
        st.waiting,
        gib(st.mem_used_kb),
        gib(st.mem_growth_kb),
        gib(st.mem_capacity_kb)
    )?;
    for b in &st.boxes {
        let mut classes = CompactString::const_new("");
        for c in &b.classes {
            let sep = if classes.is_empty() { "" } else { ", " };
            classes.push_str(&format_compact!("{sep}{} cpus at {:.2}", c.cpus, hundredths(c.speed_c)));
        }
        let seconds = |s: Option<f64>| -> CompactString {
            s.map_or_else(|| "-".into(), |s| format_compact!("{s:.2} s"))
        };
        let (library, link) = b.copy_s();
        let copy = format_compact!(
            "moving files {} for a library, {} for a link, at {} MB/s",
            seconds(library),
            seconds(link),
            b.mb_per_s
        );
        writeln!(
            out,
            "box {}: profile {}, load {:.2}; light jobs on {classes}; heavy {} threads at {:.2} each; \
             {copy}; {} tokens free, {} from here",
            b.name,
            b.profile,
            hundredths(b.load_c),
            b.threads,
            hundredths(b.thread_speed_c),
            b.free(),
            b.sent
        )?;
    }
    if st.units.is_empty() {
        return Ok(());
    }
    writeln!(
        out,
        "\n{} units run, by rank ({} in the build graph). Rank is the predicted seconds, idle, of the \
         unit and the longest chain of units after it, each where it runs fastest here. Work is \
         seconds on one thread of class 0 idle; front is the single-threaded share, width the jobserver \
         tokens held after it. Now is the predicted time here at this load, wall the measured median. Runs: \
         here and sent for this box's builds, served for other boxes'. Copy is the time a sent run \
         spent on its files rather than compiling, and MB what it sent (medians).",
        st.units.len(),
        st.graph_nodes
    )?;
    writeln!(
        out,
        "\n{:>8}  {:>8}  {:>5}  {:>5}  {:>8}  {:>8}  {:>8}  {:>5}  {:>5}  {:>6}  {:>8}  {:>7}  unit",
        "rank", "work", "front", "width", "now", "wall", "mem (MB)", "here", "sent", "served", "copy", "MB sent"
    )?;
    for u in &st.units {
        let rank: CompactString = match u.rank_s() {
            Some(r) => format_compact!("{r:8.2}"),
            None => format_compact!("{:>8}", "-"),
        };
        let front: CompactString = match u.front_pct() {
            Some(f) => format_compact!("{f:4}%"),
            None => format_compact!("{:>5}", "-"),
        };
        let (copy, sent): (CompactString, CompactString) = match u.copy() {
            Some((s, mb)) => (format_compact!("{s:8.2}"), format_compact!("{mb:7.1}")),
            _ => (format_compact!("{:>8}", "-"), format_compact!("{:>7}", "-")),
        };
        writeln!(
            out,
            "{rank}  {:8.2}  {front}  {:5.1}  {:8.2}  {:8.2}  {:8}  {:5}  {:5}  {:6}  {copy}  {sent}  {}",
            u.work_s(),
            u.width(),
            u.now_s(),
            u.wall_s(),
            u.mem_mb(),
            u.here,
            u.sent,
            u.served,
            u.key
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Context;

    #[test]
    fn only_a_command_that_prints_ends_quietly_on_a_broken_pipe() {
        let pipe = || {
            let e = Err::<(), _>(io::Error::from(io::ErrorKind::BrokenPipe));
            e.context("talking to the daemon").unwrap_err()
        };
        let wrapper = Opt::Wrapper(vec!["rustc".into()]);
        assert!(!quiet(wrapper.prints(), &pipe()), "a wrapper's broken pipe is a failed compile, not a success");
        assert!(quiet(Opt::Status.prints(), &pipe()), "seen through context");
        assert!(quiet(Opt::Calibrate(CalibrateArgs { seconds: 1 }).prints(), &pipe()));
        assert!(!quiet(Opt::Status.prints(), &anyhow::anyhow!("no daemon")));
    }
}
