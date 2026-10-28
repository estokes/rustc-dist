mod argv;
mod config;
mod daemon;
mod farm;
mod identity;
mod protocol;
mod remote;
mod server;
mod stage;
mod wrapper;

use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use clap::{Args, Parser};
use netidx::{config::Config, path::Path, publisher::PublisherBuilder};
use server::ServeOpts;
use std::{ffi::OsString, path::PathBuf, process::Command};

const AFTER_HELP: &str = "\
Build servers publish under PATH/NAME. Clients and servers share PATH.

  ~/.config/rustc-dist.toml
    base = \"/farm/rustc\"
    name = \"box1\"
    slots = 8

Flags override the config file. The environment overrides the file too:
RUSTC_DIST_BASE, RUSTC_DIST_NAME, RUSTC_DIST_SLOTS, RUSTC_DIST_RUSTC.

Cargo runs this binary as a rustc wrapper. The first argument is the
rustc binary. `serve`, `daemon`, and `help` are the only other commands:

  [build]
  rustc-wrapper = \"rustc-dist\"

A project file keeps named crates on the local machine:

  rustc-dist.toml
    local = [\"crate_a\", \"crate-b\"]

Invocations with -C incremental stay local. The toolchain is not sent;
each server runs the rustc installed there, and only a matching
`rustc -vV` identity is used.";

#[derive(Parser)]
#[command(
    name = "rustc-dist",
    version,
    about = "Distributed rustc over netidx",
    after_help = AFTER_HELP
)]
enum Opt {
    /// Publish this machine as a build server
    Serve(ServeArgs),
    /// Client daemon. The wrapper starts this
    Daemon,
    /// Cargo's rustc invocation. The first argument is the rustc binary.
    #[command(external_subcommand)]
    Wrapper(Vec<OsString>),
}

#[derive(Args)]
struct ServeArgs {
    /// Netidx base path shared by servers and clients
    #[arg(long, value_name = "PATH")]
    base: Option<ArcStr>,
    /// Name this server publishes under
    #[arg(long, value_name = "NAME")]
    name: Option<ArcStr>,
    /// Compiler jobs accepted at once
    #[arg(long, value_name = "N")]
    slots: Option<u64>,
    /// rustc to run for remote jobs
    #[arg(long, value_name = "PATH")]
    rustc: Option<PathBuf>,
}

fn main() {
    if let Err(e) = dispatch() {
        eprintln!("rustc-dist: {e:#}");
        std::process::exit(1);
    }
}

fn dispatch() -> Result<()> {
    match Opt::parse() {
        Opt::Serve(args) => serve(args),
        Opt::Daemon => tokio::runtime::Runtime::new()?.block_on(daemon::run()),
        Opt::Wrapper(args) => wrapper::run(args),
    }
}

fn serve(parsed: ServeArgs) -> Result<()> {
    let user = config::load_user()?;
    let base = parsed
        .base
        .or(user.base)
        .context("set base with --base or in ~/.config/rustc-dist.toml")?;
    let name = parsed.name.or(user.name).unwrap_or_else(|| ArcStr::from(server::hostname()));
    let slots = parsed.slots.or(user.slots).map(|n| n as i64).unwrap_or_else(server::default_slots);
    if slots < 1 {
        bail!("slots must be at least 1");
    }
    let rustc = parsed
        .rustc
        .or_else(|| user.rustc.map(|s| PathBuf::from(s.as_str())))
        .unwrap_or_else(|| PathBuf::from("rustc"));
    let identity = identity::probe(&rustc)?;
    let vv = Command::new(&rustc).arg("-vV").output().context("rustc -vV")?;
    if !vv.status.success() {
        bail!("rustc -vV failed");
    }
    let vv = ArcStr::from(String::from_utf8(vv.stdout).context("rustc -vV")?);
    tokio::runtime::Runtime::new()?.block_on(async move {
        let cfg = Config::load_default()?;
        let publisher = PublisherBuilder::new(cfg).build().await?;
        server::serve(
            publisher,
            ServeOpts {
                base: Path::from(base),
                name,
                slots,
                rustc,
                identity,
                vv,
            },
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
        )
        .await
    })
}

#[cfg(test)]
mod test {
    use std::{fs, time::Duration};

    use anyhow::Result;
    use netidx::path::Path;
    use tokio::sync::{mpsc, oneshot};

    use crate::{
        identity,
        protocol::PROTOCOL,
        remote::{self, Outcome},
        server::{self, ServeOpts},
        stage,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_rustc_writes_the_rlib_back() -> Result<()> {
        let _ = env_logger::try_init();
        let root = std::env::temp_dir().join(format!("rustc-dist-it-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("src"))?;
        let src = root.join("src/lib.rs");
        fs::write(&src, "pub fn answer() -> i32 { 7 }\n")?;
        let out = root.join("out");
        fs::create_dir_all(&out)?;
        let argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "tiny".to_string(),
            "--edition".to_string(),
            "2021".to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--emit".to_string(),
            "link".to_string(),
            src.display().to_string(),
            "--out-dir".to_string(),
            out.display().to_string(),
        ];
        let staged = stage::stage(&argv, &[], &root)?;
        let rustc = std::path::PathBuf::from("rustc");
        let identity = identity::probe(&rustc)?;
        let vv_out = std::process::Command::new(&rustc).arg("-vV").output()?;
        let vv = arcstr::ArcStr::from(String::from_utf8(vv_out.stdout)?);
        let net = netidx::InternalOnly::new().await?;
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let publisher = net.publisher().clone();
        let subscriber = net.subscriber().clone();
        let base = Path::from("/test/rustc-dist");
        let serve = tokio::spawn(async move {
            server::serve(
                publisher,
                ServeOpts {
                    base,
                    name: arcstr::ArcStr::from("box"),
                    slots: 2,
                    rustc,
                    identity,
                    vv,
                },
                async move {
                    let _ = stop_rx.await;
                },
            )
            .await
        });
        let proto = subscriber.subscribe(Path::from("/test/rustc-dist/box/protocol"));
        tokio::time::timeout(Duration::from_secs(10), proto.wait_subscribed()).await??;
        match proto.last() {
            netidx::subscriber::Event::Update(v) => {
                assert_eq!(v.cast_to::<i64>()?, PROTOCOL);
            }
            _ => anyhow::bail!("protocol was not published"),
        }
        let (tx, mut rx) = mpsc::channel(64);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let outcome = remote::compile(
            &subscriber,
            Path::from("/test/rustc-dist/box/rpc/compile"),
            &staged,
            tx,
        )
        .await?;
        match outcome {
            Outcome::Done(0) => {}
            Outcome::Done(code) => anyhow::bail!("rustc exited {code}"),
            Outcome::Busy => anyhow::bail!("server was busy"),
        }
        let rlib = out.join("libtiny.rlib");
        assert!(rlib.is_file(), "missing {}", rlib.display());
        let _ = stop_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }
}

