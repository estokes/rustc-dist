mod argv;
mod calibrate;
mod classes;
mod client;
mod config;
mod daemon;
mod protocol;
mod sched;
mod stats;
mod sys;
mod wrapper;

use anyhow::{Result, bail};
use bytes::BytesMut;
use clap::{Args, Parser};
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
    jobs = 256    # at least the slot count; the daemon does the limiting

Every rustc asks the daemon on its box for a slot and waits until one is
free. The daemon runs it pinned to a core class, with a jobserver holding
one token per CPU of that class, and records how long each unit takes so
heavy units go to fast cores.

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
    writeln!(out, "\nclass  cpus  calibration  running")?;
    for (k, c) in st.classes.iter().enumerate() {
        writeln!(out, "{k:5}  {:4}  {:8.1} ms  {:7}", c.cpus, c.calib_us as f64 / 1000., c.running)?;
    }
    if st.finishing > 0 {
        writeln!(out, "{} jobs finishing under the previous profile", st.finishing)?;
    }
    let gib = |kb: u64| kb as f64 / (1024. * 1024.);
    writeln!(
        out,
        "\n{} waiting, predicted memory {:.1} of {:.1} GiB",
        st.waiting,
        gib(st.mem_running_kb),
        gib(st.mem_limit_kb)
    )?;
    if st.units.is_empty() {
        return Ok(());
    }
    writeln!(
        out,
        "\n{:>9}  {:>8}  {:>8}  {:>5}  {:>5}  unit",
        "cost (cu)", "wall (s)", "mem (MB)", "runs", "class"
    )?;
    for u in &st.units {
        writeln!(
            out,
            "{:9.3}  {:8.1}  {:8}  {:5}  {:5}  {}",
            u.cost_mcu as f64 / 1000.,
            u.wall_ms as f64 / 1000.,
            u.mem_kb / 1024,
            u.runs,
            u.class,
            u.key
        )?;
    }
    Ok(())
}
