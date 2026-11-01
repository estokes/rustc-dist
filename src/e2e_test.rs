//! A remote compile over an in-process netidx: a server box with its
//! scheduler, store and listener, and a client that stages and sends a job.

use crate::{
    argv::{self, Invocation},
    blobs::{Hashes, Store},
    classes::{self, Class, Profile},
    farm::Load,
    protocol::{Job, Reads, Unit},
    remote::{self, CHUNK, Event, FromServer, Links, Moved, Outcome, Packed, Sent, ToServer},
    sandbox::{self, Server},
    sched::{Msg, Sched, Setup},
    stats::Decay,
    sys,
    toolchain,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use netidx::{InternalOnly, path::Path as NPath, subscriber::Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

struct ServerBox {
    _net: InternalOnly,
    subscriber: netidx::subscriber::Subscriber,
    links: Links,
    jobs: NPath,
    tx: mpsc::UnboundedSender<Msg>,
    store_dir: PathBuf,
}

impl Drop for ServerBox {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Drain);
    }
}

/// The zstd level both sides compress at.
const LEVEL: i32 = 1;

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
        runnable: Box::new(sys::runnable),
        hashes: triomphe::Arc::new(Hashes::default()),
        heartbeats: mpsc::unbounded_channel().0,
        tokens_per_cpu: 1.5,
        calibration_seconds: 4,
        decay: Decay::default(),
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let (load, _) = watch::channel(Load::default());
    tokio::spawn(Sched::new(setup, Profile::DEFAULT, load, tx.clone())?.run(rx));
    let store_dir = dir.join("blobs");
    let store = Store::open(store_dir.clone(), 1 << 30)?;
    let server = Arc::new(Server::new(store, dir.join("jobs"), toolchain::installed(), tx.clone(), LEVEL)?);
    let jobs = NPath::from("/test/rustc-dist/b/jobs");
    tokio::spawn(sandbox::listen(net.publisher().clone(), jobs.clone(), server).await?);
    let (subscriber, links) = (net.subscriber().clone(), Links::new(net.subscriber().clone()));
    Ok(ServerBox { subscriber, links, _net: net, jobs, tx, store_dir })
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
    let link = b.links.get(&b.jobs).await?;
    let outcome = remote::run(&link, offer, job, &staged, hashes, LEVEL, |ev| {
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
    let (out, mut inbox) = b.links.get(&b.jobs).await?.open()?;
    let key = toolchain::key_of(Path::new(job.rustc.as_str()))?;
    out.send(ToServer::Offer { unit: unit.clone(), key, target: None, class: None })?;
    let FromServer::Accepted { .. } = inbox.recv().await? else { bail!("not accepted") };
    let manifest =
        ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone(), late: staged.late.clone() };
    out.send(manifest)?;
    let FromServer::Need { now, later } = inbox.recv().await? else { bail!("expected need") };
    assert!(!later.is_empty(), "nothing comes late");
    let sent = Sent::default();
    for hash in now {
        remote::send_blob(&out, &staged, hash, LEVEL, &sent).await?;
    }
    out.send(ToServer::Run { args: job.args.clone(), env: job.env.clone(), cwd: job.cwd.clone() })?;
    time::sleep(delay).await;
    for hash in later {
        remote::send_blob(&out, &staged, hash, LEVEL, &sent).await?;
    }
    loop {
        if let FromServer::Exit { code, linker_waited_us, .. } = inbox.recv().await? {
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
async fn a_box_answers_a_heartbeat() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-heartbeat-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let us = remote::heartbeat(&b.links, &b.jobs, 1024, LEVEL).await?;
    assert!(us > 0 && us < 10_000_000, "{us}");
    fs::remove_dir_all(&dir)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_its_client_drops_stops_on_the_box_and_frees_what_it_held() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-drop-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let lib = project(&dir.join("ws"))?;
    let big: Vec<u8> = (0..16 << 20).map(|i: u32| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
    fs::write(dir.join("ws/app/src/big.bin"), &big)?;
    let crates: Vec<ArcStr> = lib.1.deps.iter().filter_map(|d| d.file.clone()).collect();
    let link = b.links.get(&b.jobs).await?;
    // The client sends one chunk of the blob, then drops the job.
    {
        let (job, unit) = &lib;
        let staged = remote::stage(job, &crates, &Hashes::default())?;
        let (out, mut inbox) = link.open()?;
        let key = toolchain::key_of(Path::new(job.rustc.as_str()))?;
        out.send(ToServer::Offer { unit: unit.clone(), key, target: None, class: None })?;
        let FromServer::Accepted { .. } = inbox.recv().await? else { bail!("not accepted") };
        let manifest = ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone(), late: None };
        out.send(manifest)?;
        let FromServer::Need { now, .. } = inbox.recv().await? else { bail!("expected need") };
        let hash = staged.files.iter().find(|f| f.path.ends_with("big.bin")).context("big.bin")?.hash;
        assert!(now.contains(&hash));
        out.send(ToServer::Chunk { hash, data: Packed::pack(Bytes::copy_from_slice(&big[..CHUNK]), LEVEL), last: false })?;
        out.flush().await?;
    }
    // The box gives back its slot, and drops its claim on the blob, so the
    // next job sends it rather than wait for it forever.
    let serving = async || -> Result<u32> {
        let (reply, status) = oneshot::channel();
        b.tx.send(Msg::Status { reply })?;
        let st = status.await?;
        Ok(st.classes.iter().map(|c| c.serving).sum::<u32>() + st.spread.serving)
    };
    let mut tries = 0;
    while serving().await? > 0 {
        tries += 1;
        assert!(tries < 50, "the box still serves the dropped job");
        time::sleep(Duration::from_millis(100)).await;
    }
    let again = time::timeout(Duration::from_secs(60), send(&b, &lib, &crates, &Hashes::default(), None)).await;
    let (outcome, events) = again.context("the next job waited for the dropped job's blob")??;
    let Outcome::Exit { code: 0, moved, .. } = outcome else { bail!("failed: {:?}", stderr(&events)) };
    assert!(moved.bytes >= big.len() as u64, "{} bytes", moved.bytes);
    // On the same channel all along.
    let same = b.links.get(&b.jobs).await?;
    assert!(Arc::ptr_eq(&link, &same) && !link.failed());
    drop(b);
    fs::remove_dir_all(&dir)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channel_that_fails_is_replaced() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-relink-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let lib = project(&dir.join("ws"))?;
    let crates: Vec<ArcStr> = lib.1.deps.iter().filter_map(|d| d.file.clone()).collect();
    let link = b.links.get(&b.jobs).await?;
    // A client that offers one exchange twice breaks the protocol, and the
    // box closes its channel.
    let (out, mut inbox) = link.open()?;
    let key = toolchain::key_of(Path::new(lib.0.rustc.as_str()))?;
    for _ in 0..2 {
        out.send(ToServer::Offer { unit: lib.1.clone(), key: key.clone(), target: None, class: None })?;
    }
    while inbox.recv().await.is_ok() {}
    assert!(link.failed());
    let (outcome, events) = send(&b, &lib, &crates, &Hashes::default(), None).await?;
    let Outcome::Exit { code: 0, .. } = outcome else { bail!("failed: {:?}", stderr(&events)) };
    assert!(!Arc::ptr_eq(&link, &b.links.get(&b.jobs).await?));
    drop(b);
    fs::remove_dir_all(&dir)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_sent_together_send_a_blob_they_share_once() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-shared-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    // Two checkouts, as two lanes, each with the same 64 MB beside its
    // sources, which both jobs send.
    let big = 64 << 20;
    let data: Vec<u8> = (0..big).map(|i| (i % 251) as u8).collect();
    let (ws1, ws2) = (project(&dir.join("ws1"))?, project(&dir.join("ws2"))?);
    for ws in ["ws1", "ws2"] {
        fs::write(dir.join(ws).join("app/src/big.bin"), &data)?;
    }
    let crates = |lib: &(Job, Unit)| lib.1.deps.iter().filter_map(|d| d.file.clone()).collect::<Vec<ArcStr>>();
    let (c1, c2) = (crates(&ws1), crates(&ws2));
    let hashes = Hashes::default();
    let (one, two) = tokio::join!(send(&b, &ws1, &c1, &hashes, None), send(&b, &ws2, &c2, &hashes, None));
    let bytes = |o: Result<(Outcome, Vec<Event>)>| match o? {
        (Outcome::Exit { code: 0, moved, .. }, _) => Ok(moved.bytes),
        _ => bail!("failed"),
    };
    let (one, two) = (bytes(one)?, bytes(two)?);
    assert!(one.max(two) >= big, "{one} {two}");
    assert!(one + two < big * 3 / 2, "both sent it: {one} {two}");
    fs::remove_dir_all(&dir)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_that_compresses_goes_small_and_its_output_comes_back_whole() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-zstd-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let lib = project(&dir.join("ws"))?;
    let text = "a line of text that repeats\n".repeat(300_000);
    fs::write(dir.join("ws/app/src/data.txt"), &text)?;
    fs::write(dir.join("ws/app/src/lib.rs"), "pub static DATA: &str = include_str!(\"data.txt\");\n")?;
    let crates: Vec<ArcStr> = lib.1.deps.iter().filter_map(|d| d.file.clone()).collect();
    let (outcome, events) = send(&b, &lib, &crates, &Hashes::default(), None).await?;
    let Outcome::Exit { code: 0, moved, .. } = outcome else { bail!("failed: {:?}", stderr(&events)) };
    assert!(moved.bytes >= text.len() as u64, "{} bytes", moved.bytes);
    assert!(moved.wire_bytes < moved.bytes / 20, "{} bytes as {}", moved.bytes, moved.wire_bytes);
    // The rlib holds the text, and came back whole.
    let rlib = fs::read(dir.join("ws/target/deps/libapp-a1.rlib"))?;
    let at = rlib.windows(28).position(|w| w == &text.as_bytes()[..28]).context("the rlib lacks the text")?;
    assert_eq!(&rlib[at..at + text.len()], text.as_bytes());
    drop(b);
    fs::remove_dir_all(&dir)?;
    Ok(())
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
    let Outcome::Exit { code: 0, ran, measured, moved } = outcome else { bail!("remote rustc failed") };
    let steps = moved.steps;
    assert!(steps.run_us >= ran.wall_us && steps.send_us > 0 && steps.offer_us > 0, "{:?} {ran:?}", (steps.offer_us, steps.send_us, steps.run_us));
    let front = ran.front.context("app prints an .rmeta line")?;
    assert!(0 < front.wall_us && front.wall_us < ran.wall_us, "{ran:?}");
    // It held its one token at least from when rustc started to its exit,
    // and the server counted no time before.
    let tokens = measured.tokens_us as f64 / ran.wall_us as f64;
    assert!((0.8..16.).contains(&tokens), "{measured:?} over {ran:?}");
    let lines = artifacts(&events);
    assert_eq!(lines.len(), 3, "dep-info, metadata and link: {lines:?}");
    let deps = dir.join("ws/target/deps");
    for f in ["libapp-a1.rlib", "libapp-a1.rmeta", "app-a1.d"] {
        assert!(deps.join(f).is_file(), "{f} came back");
    }
    let dep_info = fs::read_to_string(deps.join("app-a1.d"))?;
    assert!(dep_info.contains("app/src/data.txt"), "{dep_info}");
    // The server named the crate files it built, so they weren't hashed
    // here, and nothing else came back named.
    for f in ["libapp-a1.rlib", "libapp-a1.rmeta"] {
        let (named, _) = hashes.known(&deps.join(f)).context("came back")?;
        assert_eq!(named.map(|h| h.0), Some(*blake3::hash(&fs::read(deps.join(f))?).as_bytes()), "{f}");
    }
    assert!(matches!(hashes.known(&deps.join("app-a1.d")), Some((None, _))), "dep-info isn't named");
    // The server holds the crate files it read and the ones it built.
    let cwd = Path::new(lib.0.cwd.as_str());
    let read = lib_crates.iter().map(|c| remote::absolute(cwd, c));
    for f in read.chain([deps.join("libapp-a1.rlib"), deps.join("libapp-a1.rmeta")]) {
        let (hash, len) = hashes.get(&f)?;
        assert!(moved.kept.contains(&(hash, len.div_ceil(1024))), "{} is held", f.display());
    }
    assert!(!moved.kept.iter().any(|(h, _)| *h == hashes.get(&deps.join("app-a1.d")).unwrap().0), "dep-info isn't");
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
    assert!(matches!(hashes.known(&deps.join("main-m1")), Some((None, _))), "a binary isn't hashed");
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
