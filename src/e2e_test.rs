//! A remote compile over an in-process netidx: a server box with its
//! scheduler, store and listener, and a client that stages and sends a job.

use crate::{
    argv::{self, Invocation},
    blobs::{Hash, Hashes, Store},
    classes::{self, Class, Profile},
    farm::Load,
    protocol::{Job, Reads, Unit},
    remote::{self, CHUNK, Event, FromServer, In, Links, Moved, Out, Outcome, Packed, Sent, Staged, ToServer},
    sandbox::{self, Server},
    sched::{Msg, Sched, Setup},
    stats::Decay,
    sys,
    toolchain::{self, Toolchains},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use netidx::{InternalOnly, path::Path as NPath, subscriber::Value};
use std::{
    fs,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

pub(crate) struct ServerBox {
    pub(crate) net: InternalOnly,
    subscriber: netidx::subscriber::Subscriber,
    links: Links,
    pub(crate) jobs: NPath,
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

pub(crate) async fn server(dir: &Path) -> Result<ServerBox> {
    server_with(dir, toolchain::installed()).await
}

/// A server box that serves `toolchains`.
async fn server_with(dir: &Path, toolchains: Toolchains) -> Result<ServerBox> {
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
    let server = Arc::new(Server::new(store, dir.join("jobs"), toolchains, tx.clone(), LEVEL)?);
    let jobs = NPath::from("/test/rustc-dist/b/jobs");
    tokio::spawn(sandbox::listen(net.publisher().clone(), jobs.clone(), server).await?);
    let (subscriber, links) = (net.subscriber().clone(), Links::new(net.subscriber().clone()));
    Ok(ServerBox { subscriber, links, net, jobs, tx, store_dir })
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
async fn send_late(b: &ServerBox, job: &(Job, Unit), crates: &[ArcStr], delay: Duration) -> Result<(i32, u64)> {
    let mut m = Manual::offer(b, job, crates).await?;
    assert!(!m.later.is_empty(), "nothing comes late");
    m.send(&m.now).await?;
    m.run().await?;
    time::sleep(delay).await;
    m.send(&m.later).await?;
    m.exit().await
}

/// A job sent a step at a time, as `remote::run` sends it.
struct Manual {
    job: Job,
    staged: Staged,
    out: Out,
    inbox: In,
    /// What the server asked for: before Run, and late rlibs.
    now: Vec<Hash>,
    later: Vec<Hash>,
}

impl Manual {
    /// Offer the job, which reads `crates`, to `b`, and send its manifest.
    async fn offer(b: &ServerBox, job: &(Job, Unit), crates: &[ArcStr]) -> Result<Self> {
        Self::offer_for(b, job, crates, toolchain::key_of(Path::new(job.0.rustc.as_str()))?).await
    }

    /// Offer the job for the toolchain `key`.
    async fn offer_for(b: &ServerBox, (job, unit): &(Job, Unit), crates: &[ArcStr], key: ArcStr) -> Result<Self> {
        let staged = remote::stage(job, crates, &Hashes::default())?;
        let (out, mut inbox) = b.links.get(&b.jobs).await?.open()?;
        out.send(ToServer::Offer { unit: unit.clone(), key, target: None, class: None })?;
        out.flush().await?;
        let FromServer::Accepted { .. } = inbox.recv().await? else { bail!("not accepted") };
        let manifest =
            ToServer::Manifest { files: staged.files.clone(), dirs: staged.dirs.clone(), late: staged.late.clone() };
        out.send(manifest)?;
        out.flush().await?;
        let FromServer::Need { now, later } = inbox.recv().await? else { bail!("expected need") };
        Ok(Self { job: job.clone(), staged, out, inbox, now, later })
    }

    /// The hash of the input at a path ending with `name`.
    fn hash(&self, name: &str) -> Result<Hash> {
        let files = self.staged.files.iter().map(|f| (&f.path, f.hash));
        let rlibs = self.staged.late.iter().flat_map(|l| &l.rlibs).map(|r| (&r.path, r.hash));
        files.chain(rlibs).find(|(p, _)| p.ends_with(name)).map(|(_, h)| h).with_context(|| format!("no {name}"))
    }

    async fn send(&self, hashes: &[Hash]) -> Result<()> {
        for hash in hashes {
            remote::send_blob(&self.out, &self.staged, *hash, LEVEL, &Sent::default()).await?;
        }
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        let job = &self.job;
        self.out.send(ToServer::Run { args: job.args.clone(), env: job.env.clone(), cwd: job.cwd.clone() })?;
        self.out.flush().await
    }

    /// Its exit code and how long its linker waited.
    async fn exit(&mut self) -> Result<(i32, u64)> {
        loop {
            match self.inbox.recv().await? {
                FromServer::Exit { code, linker_waited_us, .. } => return Ok((code, linker_waited_us)),
                FromServer::Failed(why) => bail!("{why}"),
                _ => (),
            }
        }
    }

    /// The lines rustc printed on stderr from now until the job ended, and
    /// its exit code, or why it failed.
    async fn ending(&mut self) -> Result<(Vec<Bytes>, Result<i32, ArcStr>)> {
        let mut printed = Vec::new();
        loop {
            match self.inbox.recv().await? {
                FromServer::Stderr(line) => printed.push(line),
                FromServer::Exit { code, .. } => return Ok((printed, Ok(code))),
                FromServer::Failed(why) => return Ok((printed, Err(why))),
                _ => (),
            }
        }
    }

    /// Wait for rustc to print a line holding `text`.
    async fn printed(&mut self, text: &str) -> Result<()> {
        loop {
            match self.inbox.recv().await? {
                FromServer::Stderr(line) if line.windows(text.len()).any(|w| w == text.as_bytes()) => return Ok(()),
                FromServer::Exit { .. } | FromServer::Failed(_) => bail!("ended without printing {text}"),
                _ => (),
            }
        }
    }
}

/// Where the store at `dir` keeps the blob `h`.
fn blob(dir: &Path, h: &Hash) -> PathBuf {
    let name = format!("{h}");
    dir.join(&name[..2]).join(name)
}

/// Wait up to `secs` for `done`.
pub(crate) async fn until(secs: u64, what: &str, mut done: impl FnMut() -> bool) -> Result<()> {
    for _ in 0..secs * 10 {
        if done() {
            return Ok(());
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    bail!("{what} took over {secs} s")
}

/// How many other boxes' jobs `b` serves.
pub(crate) async fn serving(b: &ServerBox) -> Result<u32> {
    let (reply, status) = oneshot::channel();
    b.tx.send(Msg::Status { reply })?;
    let st = status.await?;
    Ok(st.classes.iter().map(|c| c.serving).sum::<u32>() + st.spread.serving)
}

/// The processes whose command line names `path`.
pub(crate) fn running(path: &Path) -> Vec<u32> {
    let needle = path.as_os_str().as_bytes();
    let Ok(procs) = fs::read_dir("/proc") else { return vec![] };
    procs
        .flatten()
        .filter_map(|p| {
            let pid = p.file_name().to_str()?.parse().ok()?;
            let cmdline = fs::read(p.path().join("cmdline")).ok()?;
            cmdline.windows(needle.len()).any(|w| w == needle).then_some(pid)
        })
        .collect()
}

/// A library that never finishes compiling: its static spins in const
/// eval.
pub(crate) fn endless(dir: &Path) -> Result<(Job, Unit)> {
    fs::create_dir_all(dir.join("spin/src"))?;
    fs::create_dir_all(dir.join("target/deps"))?;
    fs::write(
        dir.join("spin/src/lib.rs"),
        "#![allow(long_running_const_eval)]\npub static SPIN: u64 = { let mut i = 0u64; while i < u64::MAX { i += 1; } i };\n",
    )?;
    let deps = dir.join("target/deps");
    job(
        dir,
        "spin",
        &[
            "--crate-name", "spin", "--crate-type", "lib", "--edition", "2021", "spin/src/lib.rs",
            "--error-format=json", "--json=artifacts", "--emit=dep-info,metadata,link", "-C", "extra-filename=-s1",
            "--out-dir", deps.to_str().unwrap(),
        ],
    )
}

/// Jobs that build binaries on the crate `late`, and the crate files below
/// them.
struct OnLate {
    bins: Vec<(Job, Unit)>,
    crates: Vec<ArcStr>,
}

/// Binaries named `names` on `late`, built here, each printing what it
/// returns, and broken if its name says so.
fn binaries_on_a_new_crate(dir: &Path, names: &[&str]) -> Result<OnLate> {
    let deps = dir.join("target/deps");
    fs::create_dir_all(dir.join("late/src"))?;
    fs::create_dir_all(&deps)?;
    fs::write(dir.join("late/src/lib.rs"), "#[inline(never)]\npub fn three() -> u32 { 3 }\n")?;
    let status = Command::new(rustc())
        .current_dir(dir)
        .args(["--crate-name", "late", "--crate-type", "lib", "--edition", "2021", "late/src/lib.rs"])
        .args(["--emit=dep-info,metadata,link", "-C", "extra-filename=-l1", "--out-dir"])
        .arg(&deps)
        .status()?;
    assert!(status.success());
    let rlib = format!("{}/liblate-l1.rlib", deps.display());
    let bins = names
        .iter()
        .map(|name| {
            fs::create_dir_all(dir.join(name).join("src"))?;
            let main = match name.starts_with("broken") {
                true => "fn main() { let n: u32 = \"no\"; println!(\"{n} {}\", late::three()); }\n",
                false => "fn main() { println!(\"{}\", late::three()); }\n",
            };
            fs::write(dir.join(name).join("src/main.rs"), main)?;
            let (src, extra) = (format!("{name}/src/main.rs"), format!("extra-filename=-{name}"));
            job(
                dir,
                name,
                &[
                    "--crate-name", name, "--crate-type", "bin", "--edition", "2021", &src, "--error-format=json",
                    "--json=artifacts", "--emit=dep-info,link", "-C", &extra, "--out-dir", deps.to_str().unwrap(),
                    "--extern", &format!("late={rlib}"),
                ],
            )
        })
        .collect::<Result<_>>()?;
    Ok(OnLate { bins, crates: vec![ArcStr::from(rlib.as_str())] })
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compile_error_ends_a_job_whose_rlibs_are_still_coming() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-broken-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let OnLate { bins, crates } = binaries_on_a_new_crate(&dir.join("ws"), &["broken_bin"])?;
    let mut m = Manual::offer(&b, &bins[0], &crates).await?;
    let rlib = m.hash("liblate-l1.rlib")?;
    assert_eq!(m.later, [rlib]);
    m.send(&m.now).await?;
    m.run().await?;
    // Part of the rlib, which never comes whole.
    let data = fs::read(crates[0].as_str())?;
    let part = Bytes::copy_from_slice(&data[..data.len() / 2]);
    m.out.send(ToServer::Chunk { hash: rlib, data: Packed::pack(part, LEVEL), last: false })?;
    m.out.flush().await?;
    let (code, _) = time::timeout(Duration::from_secs(60), m.exit()).await.context("the job waited for the rlib")??;
    assert_eq!(code, 1);
    assert!(!blob(&b.store_dir, &rlib).exists());
    // No other job waited for it, so the next asks for it again.
    let again = Manual::offer(&b, &bins[0], &crates).await?;
    assert_eq!(again.later, [rlib]);
    drop(again);
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
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_toolchain_rustup_changes_is_refused_at_the_offer_before_the_run_and_after_it() -> Result<()> {
    use toolchain::test::{fake_rustup_home, install};
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-rustup-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let home = fake_rustup_home("e2e");
    install(&home, "a", "c1");
    let b = server_with(&dir.join("box"), Toolchains::read(home.clone())).await?;
    let lib = endless(&dir.join("ws"))?;
    let c1 = ArcStr::from("c1 h");
    let within = |what| move |_| anyhow::anyhow!("{what} took over 30 s");
    let offered = Manual::offer_for(&b, &lib, &[], c1.clone()).await?;
    drop(offered);
    // Updated between two offers: the second gets no toolchain.
    install(&home, "a", "c2");
    let (out, mut inbox) = b.links.get(&b.jobs).await?.open()?;
    out.send(ToServer::Offer { unit: lib.1.clone(), key: c1.clone(), target: None, class: None })?;
    out.flush().await?;
    let reply = inbox.recv().await?;
    assert!(matches!(&reply, FromServer::Failed(why) if why.contains("no toolchain c1 h")), "{reply:?}");
    // Updated after the offer, before the run: no rustc runs.
    install(&home, "a", "c1");
    let mut m = Manual::offer_for(&b, &lib, &[], c1.clone()).await?;
    m.send(&m.now).await?;
    install(&home, "a", "c2");
    m.run().await?;
    let (printed, ended) = time::timeout(Duration::from_secs(30), m.ending()).await.map_err(within("the job"))??;
    assert!(printed.is_empty() && ended.as_ref().is_err_and(|why| why.contains("before")), "{printed:?} {ended:?}");
    // Updated while c1 runs: the job fails rather than exit 0.
    install(&home, "a", "c1");
    let mut m = Manual::offer_for(&b, &lib, &[], c1.clone()).await?;
    m.send(&m.now).await?;
    m.run().await?;
    time::timeout(Duration::from_secs(30), m.printed("c1 ran")).await.map_err(within("starting c1"))??;
    install(&home, "a", "c2");
    let (_, ended) = time::timeout(Duration::from_secs(30), m.ending()).await.map_err(within("the job"))??;
    assert!(ended.as_ref().is_err_and(|why| why.contains("while")), "{ended:?}");
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&home);
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
    let mut tries = 0;
    while serving(&b).await? > 0 {
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
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_sandbox_is_killed_when_its_client_drops_the_job_or_the_channel_fails() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-kill-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let ws = dir.join("ws");
    let lib = endless(&ws)?;
    for fail_the_channel in [false, true] {
        let mut m = Manual::offer(&b, &lib, &[]).await?;
        m.send(&m.now).await?;
        m.run().await?;
        until(30, "starting the sandbox", || !running(&ws).is_empty()).await?;
        assert_eq!(serving(&b).await?, 1);
        if fail_the_channel {
            // An exchange offered twice breaks the protocol, and the box
            // closes the channel.
            let (out, _inbox) = b.links.get(&b.jobs).await?.open()?;
            let key = toolchain::key_of(Path::new(lib.0.rustc.as_str()))?;
            for _ in 0..2 {
                out.send(ToServer::Offer { unit: lib.1.clone(), key: key.clone(), target: None, class: None })?;
            }
            out.flush().await?;
            assert!(m.exit().await.is_err(), "the channel failed");
        } else {
            drop(m);
        }
        until(10, "killing the sandbox", || running(&ws).is_empty()).await?;
        let mut tries = 0;
        while serving(&b).await? > 0 {
            tries += 1;
            assert!(tries < 100, "the box still serves the job");
            time::sleep(Duration::from_millis(100)).await;
        }
    }
    drop(b);
    let _ = fs::remove_dir_all(&dir);
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
    let _ = fs::remove_dir_all(&dir);
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
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_rlib_comes_through_another_jobs_upload_even_when_that_job_fails_first() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-shared-late-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    for (sender, ws) in [("shared_a", "ws1"), ("broken_a", "ws2")] {
        let ws = dir.join(ws);
        let OnLate { bins, crates } = binaries_on_a_new_crate(&ws, &[sender, "shared_b"])?;
        let mut a = Manual::offer(&b, &bins[0], &crates).await?;
        let rlib = a.hash("liblate-l1.rlib")?;
        assert_eq!(a.later, [rlib], "a's client sends the rlib");
        let mut waiter = Manual::offer(&b, &bins[1], &crates).await?;
        assert!(waiter.later.is_empty() && !waiter.now.contains(&rlib), "the other waits for a's upload");
        for m in [&a, &waiter] {
            m.send(&m.now).await?;
            m.run().await?;
        }
        let broken = sender.starts_with("broken");
        if broken {
            // a's rustc fails before a's client sends the rlib.
            a.printed("\"error\"").await?;
        }
        time::sleep(Duration::from_secs(1)).await;
        a.send(&a.later).await?;
        let (code, _) = a.exit().await?;
        assert_eq!(code, i32::from(broken), "{sender}");
        let (code, _) = time::timeout(Duration::from_secs(60), waiter.exit()).await??;
        assert_eq!(code, 0, "linked against a's upload, as the stand-in has no code");
    }
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_that_needs_another_jobs_late_rlib_before_rustc_starts_has_its_own_client_send_it() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-send-too-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    let (ws1, ws2) = (dir.join("ws1"), dir.join("ws2"));
    let OnLate { bins, crates } = binaries_on_a_new_crate(&ws1, &["shared_a"])?;
    // The same rlib in another lane, with no metadata beside it to stand
    // in for it, so a job there needs it before rustc starts.
    let other = binaries_on_a_new_crate(&ws2, &["shared_b"])?.bins;
    let deps2 = ws2.join("target/deps");
    fs::remove_file(deps2.join("liblate-l1.rmeta"))?;
    fs::copy(ws1.join("target/deps/liblate-l1.rlib"), deps2.join("liblate-l1.rlib"))?;
    let crates2 = vec![ArcStr::from(deps2.join("liblate-l1.rlib").to_str().unwrap())];
    let a = Manual::offer(&b, &bins[0], &crates).await?;
    let rlib = a.hash("liblate-l1.rlib")?;
    assert_eq!(a.later, [rlib]);
    a.send(&a.now).await?;
    let mut needs = Manual::offer(&b, &other[0], &crates2).await?;
    assert!(needs.now.contains(&rlib), "its own client sends it rather than wait for a's after a's rustc starts");
    needs.send(&needs.now).await?;
    needs.run().await?;
    // a's client never sends it.
    let (code, _) = time::timeout(Duration::from_secs(60), needs.exit()).await??;
    assert_eq!(code, 0);
    drop(a);
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_rlibs_are_stored_as_they_come_while_the_job_waits_for_another_jobs_upload() -> Result<()> {
    let _ = env_logger::try_init();
    let dir = std::env::temp_dir().join(format!("rustc-dist-e2e-late-store-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let b = server(&dir.join("box")).await?;
    // Two lanes with the same 4 MB beside their sources: a library, and a
    // binary with a late rlib.
    let big: Vec<u8> = (0..4 << 20).map(|i: u32| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
    let (ws1, ws2) = (dir.join("ws1"), dir.join("ws2"));
    let lib = project(&ws1)?;
    fs::write(ws1.join("app/src/big.bin"), &big)?;
    let lib_crates: Vec<ArcStr> = lib.1.deps.iter().filter_map(|d| d.file.clone()).collect();
    let OnLate { bins, crates } = binaries_on_a_new_crate(&ws2, &["shared_b"])?;
    fs::write(ws2.join("shared_b/big.bin"), &big)?;
    // The library's client sends a chunk of the shared file, then stalls.
    let sender = Manual::offer(&b, &lib, &lib_crates).await?;
    let shared = sender.hash("big.bin")?;
    assert!(sender.now.contains(&shared));
    sender.out.send(ToServer::Chunk { hash: shared, data: Packed::pack(Bytes::copy_from_slice(&big[..CHUNK]), LEVEL), last: false })?;
    sender.out.flush().await?;
    let mut bin = Manual::offer(&b, &bins[0], &crates).await?;
    assert!(!bin.now.contains(&shared), "the binary waits for the library's upload");
    let rlib = bin.hash("liblate-l1.rlib")?;
    assert_eq!(bin.later, [rlib]);
    bin.send(&bin.now).await?;
    bin.run().await?;
    bin.send(&bin.later).await?;
    until(10, "storing the late rlib", || blob(&b.store_dir, &rlib).exists()).await?;
    assert!(time::timeout(Duration::from_millis(200), bin.exit()).await.is_err(), "the binary still waits");
    // The library's client sends the rest.
    let rest = &big[CHUNK..];
    for (i, part) in rest.chunks(CHUNK).enumerate() {
        let last = i + 1 == rest.len().div_ceil(CHUNK) && part.len() < CHUNK;
        sender.out.send(ToServer::Chunk { hash: shared, data: Packed::pack(Bytes::copy_from_slice(part), LEVEL), last })?;
    }
    if rest.len().is_multiple_of(CHUNK) {
        sender.out.send(ToServer::Chunk { hash: shared, data: Packed::pack(Bytes::new(), LEVEL), last: true })?;
    }
    sender.out.flush().await?;
    let (code, _) = time::timeout(Duration::from_secs(60), bin.exit()).await??;
    assert_eq!(code, 0);
    drop(sender);
    drop(b);
    let _ = fs::remove_dir_all(&dir);
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
    let _ = fs::remove_dir_all(&dir);
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
        assert_eq!(named, Some(Hash::of(&fs::read(deps.join(f))?)), "{f}");
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
    let late = binaries_on_a_new_crate(&dir.join("ws"), &["late_bin"])?;
    let (code, linker_waited_us) = send_late(&b, &late.bins[0], &late.crates, Duration::from_secs(2)).await?;
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
