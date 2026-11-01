//! A remote compile over an in-process netidx: a server box with its
//! scheduler, store and listener, and a client that stages and sends a job.

use crate::{
    argv::{self, Invocation},
    blobs::{Hashes, Store},
    classes::{self, Class, Profile},
    farm::Load,
    perf,
    protocol::{Job, Reads, Unit},
    remote::{self, CHUNK, Event, FromServer, Moved, Outcome, ToServer},
    sandbox::{self, Server},
    sched::{Msg, Sched, Setup},
    sys,
    toolchain,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use netidx::{InternalOnly, path::Path as NPath, subscriber::Value};
use netidx_protocols::pack_channel::client::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

struct ServerBox {
    _net: InternalOnly,
    subscriber: netidx::subscriber::Subscriber,
    jobs: NPath,
    tx: mpsc::UnboundedSender<Msg>,
    store_dir: PathBuf,
}

impl Drop for ServerBox {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Drain);
    }
}

async fn server(dir: &Path) -> Result<ServerBox> {
    let net = InternalOnly::new().await?;
    let state = dir.join("state");
    let allowed = sys::allowed_cpus()?;
    classes::save(&state, &Profile::DEFAULT, &[Class { rate: 1000, cpus: allowed.clone() }])?;
    let setup = Setup {
        state_dir: state,
        fifo_dir: dir.to_path_buf(),
        allowed,
        memory: Box::new(|| sys::box_memory(0.5)),
        tokens_per_cpu: 1.5,
        calibration_seconds: 4,
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let (load, _) = watch::channel(Load::default());
    tokio::spawn(Sched::new(setup, Profile::DEFAULT, load, tx.clone())?.run(rx));
    let store_dir = dir.join("blobs");
    let store = Store::open(store_dir.clone(), 1 << 30)?;
    let server = Arc::new(Server::new(store, dir.join("jobs"), toolchain::installed(), tx.clone())?);
    let jobs = NPath::from("/test/rustc-dist/b/jobs");
    tokio::spawn(sandbox::listen(net.publisher().clone(), jobs.clone(), server).await?);
    Ok(ServerBox { subscriber: net.subscriber().clone(), _net: net, jobs, tx, store_dir })
}

fn rustc() -> PathBuf {
    let out = Command::new("rustc").args(["--print", "sysroot"]).output().unwrap();
    PathBuf::from(String::from_utf8(out.stdout).unwrap().trim()).join("bin/rustc")
}

/// `dep` built here, and the job that builds `app` on top of it.
fn project(dir: &Path) -> Result<(Job, Unit)> {
    fs::create_dir_all(dir.join("dep/src"))?;
    fs::create_dir_all(dir.join("app/src"))?;
    fs::create_dir_all(dir.join("target/deps"))?;
    fs::write(dir.join("dep/src/lib.rs"), "pub fn answer() -> u32 { 21 }\n")?;
    let mut app = String::from(
        "pub fn twice() -> u32 { dep::answer() * 2 }\npub const DATA: &str = include_str!(\"data.txt\");\n",
    );
    for i in 0..300 {
        app.push_str(&format!(
            "pub fn f{i}<T: Copy + core::ops::Add<Output = T>>(a: &[T], z: T) -> T {{ a.iter().fold(z, |x, y| x + *y) }}\n\
             pub fn g{i}(v: &[u64]) -> u64 {{ f{i}(v, 0u64) + f{i}(&v.iter().map(|x| x * {i}).collect::<Vec<_>>(), 1) }}\n"
        ));
    }
    fs::write(dir.join("app/src/lib.rs"), app)?;
    fs::write(dir.join("app/src/data.txt"), "hello")?;
    let deps = dir.join("target/deps");
    let rustc = rustc();
    let status = Command::new(&rustc)
        .current_dir(dir)
        .args(["--crate-name", "dep", "--crate-type", "lib", "--edition", "2021", "dep/src/lib.rs"])
        .args(["--emit=dep-info,metadata,link", "-C", "extra-filename=-d1", "--out-dir"])
        .arg(&deps)
        .status()?;
    assert!(status.success());
    job(
        dir,
        "app",
        &[
            "--crate-name", "app", "--crate-type", "lib", "--edition", "2021", "app/src/lib.rs",
            "--error-format=json", "--json=artifacts", "--emit=dep-info,metadata,link", "-C", "extra-filename=-a1",
            "--out-dir", deps.to_str().unwrap(), "--extern", &format!("dep={}/libdep-d1.rmeta", deps.display()),
            "-L", &format!("dependency={}", deps.display()),
        ],
    )
}

/// The job cargo would give the wrapper for `args`, building `package`.
fn job(dir: &Path, package: &str, args: &[&str]) -> Result<(Job, Unit)> {
    let rustc = rustc();
    let argv: Vec<&str> = [rustc.to_str().unwrap()].into_iter().chain(args.iter().copied()).collect();
    let Invocation::Compile(c) = argv::classify(&argv, None) else { bail!("not a compile") };
    let portable = c.portable.context("portable")?;
    let job = Job {
        rustc: ArcStr::from(argv[0]),
        args: args.iter().map(|a| ArcStr::from(*a)).collect(),
        env: vec![("CARGO_MANIFEST_DIR".into(), ArcStr::from(dir.join(package).to_str().unwrap()))],
        cwd: ArcStr::from(dir.to_str().unwrap()),
        target: None,
        paths: portable.paths,
        reads: portable.reads,
    };
    Ok((job, c.unit))
}

/// A binary on top of `app`, whose rlib the server built, and `dep`, whose
/// rlib it has never seen.
fn binary(dir: &Path) -> Result<(Job, Unit)> {
    fs::create_dir_all(dir.join("main/src"))?;
    fs::write(dir.join("main/src/main.rs"), "fn main() { println!(\"{}\", app::twice()); }\n")?;
    let deps = dir.join("target/deps");
    job(
        dir,
        "main",
        &[
            "--crate-name", "main", "--crate-type", "bin", "--edition", "2021", "main/src/main.rs",
            "--error-format=json", "--json=artifacts", "--emit=dep-info,link", "-C", "extra-filename=-m1",
            "--out-dir", deps.to_str().unwrap(), "--extern", &format!("app={}/libapp-a1.rlib", deps.display()),
            "-L", &format!("dependency={}", deps.display()),
        ],
    )
}

/// Run the job, which reads `crates`, on `b`, pinned to `class` or on
/// every CPU.
async fn send(
    b: &ServerBox,
    (job, unit): &(Job, Unit),
    crates: &[ArcStr],
    hashes: &Hashes,
    class: Option<u32>,
) -> Result<(Outcome, Vec<Event>)> {
    let staged = remote::stage(job, crates, hashes)?;
    let key = toolchain::key_of(Path::new(job.rustc.as_str()))?;
    let offer = ToServer::Offer { unit: unit.clone(), key, target: None, class };
    let mut events = Vec::new();
    let outcome = remote::run(&b.subscriber, b.jobs.clone(), offer, job, &staged, hashes, |ev| {
        events.push(ev);
        Ok(())
    })
    .await?;
    Ok((outcome, events))
}

/// Run a job that links on `b` as `remote::run` does, but send the late
/// rlibs only after `delay`. Its exit code and how long its linker waited.
async fn send_late(b: &ServerBox, (job, unit): &(Job, Unit), crates: &[ArcStr], delay: Duration) -> Result<(i32, u64)> {
    let staged = remote::stage(job, crates, &Hashes::default())?;
    let con = Connection::connect(&b.subscriber, b.jobs.clone()).await?;
    let key = toolchain::key_of(Path::new(job.rustc.as_str()))?;
    con.send_one(&ToServer::Offer { unit: unit.clone(), key, target: None, class: None })?;
    con.flush().await?;
    let FromServer::Accepted { .. } = con.recv_one().await? else { bail!("not accepted") };
    let manifest =
        ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone(), late: staged.late.clone() };
    con.send_one(&manifest)?;
    con.flush().await?;
    let FromServer::Need { now, later } = con.recv_one().await? else { bail!("expected need") };
    assert!(!later.is_empty(), "nothing comes late");
    let (sent, mut buf) = (AtomicU64::new(0), vec![0u8; CHUNK]);
    for hash in now {
        remote::send_blob(&con, &staged, hash, &mut buf, &sent).await?;
    }
    con.send_one(&ToServer::Run { args: job.args.clone(), env: job.env.clone(), cwd: job.cwd.clone() })?;
    con.flush().await?;
    time::sleep(delay).await;
    for hash in later {
        remote::send_blob(&con, &staged, hash, &mut buf, &sent).await?;
    }
    loop {
        if let FromServer::Exit { code, linker_waited_us, .. } = con.recv_one().await? {
            return Ok((code, linker_waited_us));
        }
    }
}

/// A binary on `late`, built here, which the server has never seen.
fn binary_on_a_new_crate(dir: &Path) -> Result<((Job, Unit), Vec<ArcStr>)> {
    let deps = dir.join("target/deps");
    fs::create_dir_all(dir.join("late/src"))?;
    fs::create_dir_all(dir.join("main2/src"))?;
    // Not inlined, so linking a stand-in instead of the rlib fails.
    fs::write(dir.join("late/src/lib.rs"), "#[inline(never)]\npub fn three() -> u32 { 3 }\n")?;
    fs::write(dir.join("main2/src/main.rs"), "fn main() { println!(\"{}\", late::three()); }\n")?;
    let status = Command::new(rustc())
        .current_dir(dir)
        .args(["--crate-name", "late", "--crate-type", "lib", "--edition", "2021", "late/src/lib.rs"])
        .args(["--emit=dep-info,metadata,link", "-C", "extra-filename=-l1", "--out-dir"])
        .arg(&deps)
        .status()?;
    assert!(status.success());
    let rlib = format!("{}/liblate-l1.rlib", deps.display());
    let bin = job(
        dir,
        "main2",
        &[
            "--crate-name", "main2", "--crate-type", "bin", "--edition", "2021", "main2/src/main.rs",
            "--error-format=json", "--json=artifacts", "--emit=dep-info,link", "-C", "extra-filename=-n1",
            "--out-dir", deps.to_str().unwrap(), "--extern", &format!("late={rlib}"),
        ],
    )?;
    Ok((bin, vec![ArcStr::from(rlib.as_str())]))
}

/// A binary that fails to compile, on `big`, built here, whose rlib takes
/// a while to send.
fn broken_binary_on_a_big_crate(dir: &Path) -> Result<((Job, Unit), Vec<ArcStr>)> {
    let deps = dir.join("target/deps");
    fs::create_dir_all(&deps)?;
    fs::create_dir_all(dir.join("big/src"))?;
    fs::create_dir_all(dir.join("broken/src"))?;
    // The data is in the object code, not the metadata, which comes first.
    fs::write(
        dir.join("big/src/lib.rs"),
        "#[inline(never)]\npub fn data() -> &'static [u8] { static DATA: [u8; 64 << 20] = [7; 64 << 20]; &DATA }\n",
    )?;
    fs::write(dir.join("broken/src/main.rs"), "fn main() { let n: u32 = \"no\"; println!(\"{n} {}\", big::data()[0]); }\n")?;
    let status = Command::new(rustc())
        .current_dir(dir)
        .args(["--crate-name", "big", "--crate-type", "lib", "--edition", "2021", "big/src/lib.rs"])
        .args(["--emit=dep-info,metadata,link", "-C", "extra-filename=-g1", "--out-dir"])
        .arg(&deps)
        .status()?;
    assert!(status.success());
    let rlib = format!("{}/libbig-g1.rlib", deps.display());
    let bin = job(
        dir,
        "broken",
        &[
            "--crate-name", "broken", "--crate-type", "bin", "--edition", "2021", "broken/src/main.rs",
            "--error-format=json", "--json=artifacts", "--emit=dep-info,link", "-C", "extra-filename=-b1",
            "--out-dir", deps.to_str().unwrap(), "--extern", &format!("big={rlib}"),
        ],
    )?;
    Ok((bin, vec![ArcStr::from(rlib.as_str())]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compile_error_ends_a_job_whose_rlibs_are_still_coming() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-broken-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let (bin, crates) = broken_binary_on_a_big_crate(&dir.join("ws"))?;
    let rlib = fs::metadata(crates[0].as_str())?.len();
    // The server stops reading the rlib when rustc exits, and closes the
    // channel. Which side notices first varies, so try a few times.
    let mut early = 0;
    for _ in 0..6 {
        let (outcome, events) = send(&b, &bin, &crates, &Hashes::default(), None).await?;
        let Outcome::Exit { code, moved, .. } = outcome else { bail!("busy") };
        assert_eq!(code, 1, "{:?}", stderr(&events));
        early += usize::from(moved.bytes < rlib);
    }
    assert!(early > 0, "the rlib always came before rustc failed");
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_fails_to_connect_holds_up_no_other() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-connect-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let lib = project(&dir.join("ws"))?;
    let crates: Vec<ArcStr> = lib.1.deps.iter().filter_map(|d| d.file.clone()).collect();
    let jobs = b.subscriber.subscribe(b.jobs.clone());
    time::timeout(Duration::from_secs(10), async {
        jobs.wait_subscribed().await?;
        // One client asks for a channel and never takes it.
        jobs.write_with_recipt(Value::from("connect")).await?;
        // Another takes one and botches the handshake.
        let session = jobs.write_with_recipt(Value::from("connect")).await?.cast_to::<NPath>()?;
        let botched = b.subscriber.subscribe_nondurable_one(session, Some(Duration::from_secs(5))).await?;
        botched.write(Value::from("nonsense"));
        let (outcome, events) = send(&b, &lib, &crates, &Hashes::default(), None).await?;
        let Outcome::Exit { code: 0, .. } = outcome else { bail!("failed: {:?}", stderr(&events)) };
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("a job waited behind the clients that failed to connect")??;
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

fn artifacts(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Artifact(line) => Some(String::from_utf8_lossy(line).into_owned()),
            _ => None,
        })
        .collect()
}

fn stderr(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Stderr(line) => Some(String::from_utf8_lossy(line).into_owned()),
            _ => None,
        })
        .collect()
}

fn blob_count(dir: &Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|p| p.file_name().len() == 2)
        .map(|p| fs::read_dir(p.path()).unwrap().count())
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crate_builds_on_another_box() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let lib = project(&dir.join("ws"))?;
    let unit = lib.1.clone();
    let lib_crates: Vec<ArcStr> = unit.deps.iter().filter_map(|d| d.file.clone()).collect();
    let hashes = Hashes::default();
    let (outcome, events) = send(&b, &lib, &lib_crates, &hashes, None).await?;
    let Outcome::Exit { code: 0, ran, .. } = outcome else { bail!("remote rustc failed") };
    // rustc's own time; bwrap alone uses about 10 ms.
    assert!(ran.cpu_us > 200_000, "remote cpu {} us", ran.cpu_us);
    let front = ran.front.context("app prints an .rmeta line")?;
    assert!(front.wall_us < ran.wall_us && front.cpu_us <= ran.cpu_us + 10_000, "{ran:?}");
    if perf::Counter::start(false).is_some() {
        // A few hundred ms of rustc retire billions of instructions.
        assert!(ran.instructions.is_some_and(|n| n > 500_000_000), "{ran:?}");
    }
    let lines = artifacts(&events);
    assert_eq!(lines.len(), 3, "dep-info, metadata and link: {lines:?}");
    let deps = dir.join("ws/target/deps");
    for f in ["libapp-a1.rlib", "libapp-a1.rmeta", "app-a1.d"] {
        assert!(deps.join(f).is_file(), "{f} came back");
    }
    let dep_info = fs::read_to_string(deps.join("app-a1.d"))?;
    assert!(dep_info.contains("app/src/data.txt"), "{dep_info}");
    // Everything is in the store now, outputs too, so a second run sends
    // nothing and builds the same outputs.
    let blobs = blob_count(&b.store_dir);
    let (outcome, _) = send(&b, &lib, &lib_crates, &hashes, Some(0)).await?;
    assert!(matches!(outcome, Outcome::Exit { code: 0, moved: Moved { bytes: 0, .. }, .. }));
    assert_eq!(blob_count(&b.store_dir), blobs);
    // A binary links app's rlib, which the server kept, and dep's, which
    // arrives while rustc runs, behind a stand-in.
    let bin = binary(&dir.join("ws"))?;
    assert_eq!(bin.0.reads, Reads::RlibsToLink { linker: "cc".into() });
    let bin_crates: Vec<ArcStr> =
        [format!("{}/libapp-a1.rlib", deps.display()), format!("{}/libdep-d1.rmeta", deps.display())]
            .iter()
            .map(|p| ArcStr::from(p.as_str()))
            .collect();
    let (outcome, events) = send(&b, &bin, &bin_crates, &hashes, None).await?;
    let Outcome::Exit { code: 0, moved, .. } = outcome else { bail!("the binary failed: {:?}", stderr(&events)) };
    let dep_rlib = fs::metadata(deps.join("libdep-d1.rlib"))?.len();
    let app_rlib = fs::metadata(deps.join("libapp-a1.rlib"))?.len();
    assert!(moved.bytes >= dep_rlib && moved.bytes < dep_rlib + app_rlib, "{} bytes", moved.bytes);
    assert_eq!(artifacts(&events).len(), 2, "dep-info and the binary");
    let out = Command::new(deps.join("main-m1")).output()?;
    assert_eq!(out.stdout, b"42\n");
    // Now the server has every rlib, so nothing comes late.
    let (outcome, _) = send(&b, &bin, &bin_crates, &hashes, None).await?;
    let Outcome::Exit { code: 0, moved, .. } = outcome else { bail!("the binary failed again") };
    assert_eq!((moved.bytes, moved.linker_waited_us), (0, 0));
    // The linker waits for an rlib that is slow to come.
    let (late, crates) = binary_on_a_new_crate(&dir.join("ws"))?;
    let (code, linker_waited_us) = send_late(&b, &late, &crates, Duration::from_secs(2)).await?;
    assert_eq!(code, 0, "linked against the real rlib");
    assert!(linker_waited_us > 0, "the linker started before the rlib came");
    // The server recorded both runs as served, with their memory.
    let (reply, status) = oneshot::channel();
    b.tx.send(Msg::Status { reply })?;
    let status = status.await?;
    let served = status.units.iter().find(|u| u.key == unit.exact).context("no unit")?;
    assert_eq!((served.here, served.served), (0, 2));
    assert!(served.mem_kb > 10_000, "{} kB", served.mem_kb);
    // A compile error comes back as an exit code, with no outputs.
    for f in ["libapp-a1.rlib", "libapp-a1.rmeta"] {
        fs::remove_file(deps.join(f))?;
    }
    fs::write(dir.join("ws/app/src/lib.rs"), "pub fn broken() -> u32 { \"no\" }\n")?;
    let (outcome, events) = send(&b, &lib, &lib_crates, &hashes, None).await?;
    assert!(matches!(outcome, Outcome::Exit { code: 1, .. }));
    assert!(events.iter().any(|e| matches!(e, Event::Stderr(_))), "the error is reported");
    assert!(!deps.join("libapp-a1.rlib").exists());
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}
