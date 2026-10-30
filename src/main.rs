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
use protocol::{Ask, Reply};
use std::ffi::OsString;

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

Run `rustc-dist install` once, on an idle box, to calibrate its core
classes and enable the systemd user service. RUSTC_DIST=off runs rustc
directly.";

#[derive(Parser)]
#[command(name = "rustc-dist", version, about = "Schedules rustc on the cores of a box", after_help = AFTER_HELP)]
enum Opt {
    /// Run the daemon. The wrapper starts it when it is not running
    Daemon,
    /// Calibrate this box, save its core classes, and enable the systemd user service
    Install(CalibrateArgs),
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
    /// Calibrate even though the box is busy
    #[arg(long)]
    force: bool,
}

fn main() {
    if let Err(e) = dispatch() {
        eprintln!("rustc-dist: {e:#}");
        std::process::exit(1);
    }
}

fn dispatch() -> Result<()> {
    match Opt::parse() {
        Opt::Daemon => {
            env_logger::Builder::from_env(env_logger::Env::new().filter_or("RUSTC_DIST_LOG", "info"))
                .init();
            tokio::runtime::Runtime::new()?.block_on(daemon::run())
        }
        Opt::Install(a) => calibrate::install(a.seconds, a.force),
        Opt::Calibrate(a) => calibrate::calibrate(a.seconds, a.force).map(drop),
        Opt::Status => status(),
        Opt::Wrapper(args) => wrapper::run(args),
    }
}

fn status() -> Result<()> {
    let mut buf = BytesMut::new();
    let Reply::Status(st) = client::request(Ask::Status, false, &mut buf)?.1 else {
        bail!("unexpected reply from the daemon");
    };
    println!("class  cpus  calibration  running");
    for (k, c) in st.classes.iter().enumerate() {
        println!("{k:5}  {:4}  {:8.1} ms  {:7}", c.cpus, c.calib_us as f64 / 1000., c.running);
    }
    let gib = |kb: u64| kb as f64 / (1024. * 1024.);
    println!(
        "\n{} waiting, predicted memory {:.1} of {:.1} GiB",
        st.waiting,
        gib(st.mem_running_kb),
        gib(st.mem_limit_kb)
    );
    if st.units.is_empty() {
        return Ok(());
    }
    println!("\n{:>9}  {:>8}  {:>8}  {:>5}  {:>5}  unit", "cost (cu)", "wall (s)", "mem (MB)", "runs", "class");
    for u in &st.units {
        println!(
            "{:9.3}  {:8.1}  {:8}  {:5}  {:5}  {}",
            u.cost_mcu as f64 / 1000.,
            u.wall_ms as f64 / 1000.,
            u.mem_kb / 1024,
            u.runs,
            u.class,
            u.key
        );
    }
    Ok(())
}
