//! A remote compile over an in-process netidx: a server box with its
//! scheduler, store and listener, and a client that stages and sends a job.

use crate::{
    argv::{self, Invocation},
    blobs::{Hashes, Store},
    classes::{self, Class, Profile},
    farm::Load,
    protocol::{Job, Unit},
    remote::{self, Event, Outcome, ToServer},
    sandbox::{self, Server},
    sched::{Msg, Sched, Setup},
    sys,
    toolchain,
};
use anyhow::{Result, bail};
use arcstr::ArcStr;
use netidx::{path::Path as NPath, InternalOnly};
use netidx_protocols::pack_channel::server::Listener;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tokio::sync::{mpsc, watch};

struct Box {
    _net: InternalOnly,
    subscriber: netidx::subscriber::Subscriber,
    jobs: NPath,
    tx: mpsc::UnboundedSender<Msg>,
    store_dir: PathBuf,
}

impl Drop for Box {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Drain);
    }
}

async fn server(dir: &Path) -> Result<Box> {
    let net = InternalOnly::new().await?;
    let state = dir.join("state");
    let allowed = sys::allowed_cpus()?;
    classes::save(&state, &Profile::DEFAULT, &[Class { calib_us: 1_000_000, cpus: allowed.clone() }])?;
    let setup = Setup {
        state_dir: state,
        fifo_dir: dir.to_path_buf(),
        allowed,
        mem_limit_kb: u64::MAX,
        calibration_seconds: 4,
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let (load, _) = watch::channel(Load::default());
    tokio::spawn(Sched::new(setup, Profile::DEFAULT, load, tx.clone())?.run(rx));
    let store_dir = dir.join("blobs");
    let store = Store::open(store_dir.clone(), 1 << 30)?;
    let server = Arc::new(Server::new(store, dir.join("jobs"), toolchain::installed(), tx.clone())?);
    let jobs = NPath::from("/test/rustc-dist/b/jobs");
    let mut listener = Listener::new(net.publisher(), None, jobs.clone()).await?;
    tokio::spawn(async move {
        while let Ok(con) = listener.accept().await {
            let server = server.clone();
            tokio::spawn(async move {
                if let Err(e) = sandbox::serve(server, con).await {
                    eprintln!("serve: {e:#}");
                }
            });
        }
    });
    Ok(Box { subscriber: net.subscriber().clone(), _net: net, jobs, tx, store_dir })
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
    let argv: Vec<String> = [
        rustc.to_str().unwrap(), "--crate-name", "app", "--crate-type", "lib", "--edition", "2021",
        "app/src/lib.rs", "--error-format=json", "--json=artifacts", "--emit=dep-info,metadata,link",
        "-C", "extra-filename=-a1", "--out-dir", deps.to_str().unwrap(),
        "--extern", &format!("dep={}/libdep-d1.rmeta", deps.display()),
        "-L", &format!("dependency={}", deps.display()),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let Invocation::Compile(c) = argv::classify(&argv, None) else { bail!("not a compile") };
    let job = Job {
        rustc: ArcStr::from(argv[0].as_str()),
        args: argv[1..].iter().map(|a| ArcStr::from(a.as_str())).collect(),
        env: vec![("CARGO_MANIFEST_DIR".into(), ArcStr::from(dir.join("app").to_str().unwrap()))],
        cwd: ArcStr::from(dir.to_str().unwrap()),
        target: None,
        paths: c.portable.expect("portable"),
    };
    Ok((job, c.unit))
}

async fn send(b: &Box, job: &Job, unit: &Unit, hashes: &Hashes) -> Result<(Outcome, Vec<Event>)> {
    let crates: Vec<ArcStr> = unit.deps.iter().filter_map(|d| d.file.clone()).collect();
    let staged = remote::stage(job, &crates, hashes)?;
    let key = toolchain::key_of(Path::new(job.rustc.as_str()))?;
    let offer = ToServer::Offer { unit: unit.clone(), key, target: None, class: 0 };
    let mut events = Vec::new();
    let outcome = remote::run(&b.subscriber, b.jobs.clone(), offer, job, &staged, |ev| {
        events.push(ev);
        Ok(())
    })
    .await?;
    Ok((outcome, events))
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
    let (job, unit) = project(&dir.join("ws"))?;
    let hashes = Hashes::default();
    let (outcome, events) = send(&b, &job, &unit, &hashes).await?;
    let Outcome::Exit { code: 0, cpu_us, .. } = outcome else { bail!("remote rustc failed") };
    // rustc's own time; bwrap alone uses about 10 ms.
    assert!(cpu_us > 200_000, "remote cpu {cpu_us} us");
    let artifacts: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            Event::Artifact(line) => Some(String::from_utf8_lossy(line).into_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(artifacts.len(), 3, "dep-info, metadata and link: {artifacts:?}");
    let deps = dir.join("ws/target/deps");
    for f in ["libapp-a1.rlib", "libapp-a1.rmeta", "app-a1.d"] {
        assert!(deps.join(f).is_file(), "{f} came back");
    }
    let dep_info = fs::read_to_string(deps.join("app-a1.d"))?;
    assert!(dep_info.contains("app/src/data.txt"), "{dep_info}");
    // Everything is in the store now, so a second run sends no blobs.
    let blobs = blob_count(&b.store_dir);
    let (outcome, _) = send(&b, &job, &unit, &hashes).await?;
    assert!(matches!(outcome, Outcome::Exit { code: 0, .. }));
    assert_eq!(blob_count(&b.store_dir), blobs);
    // A compile error comes back as an exit code, with no outputs.
    for f in ["libapp-a1.rlib", "libapp-a1.rmeta"] {
        fs::remove_file(deps.join(f))?;
    }
    fs::write(dir.join("ws/app/src/lib.rs"), "pub fn broken() -> u32 { \"no\" }\n")?;
    let (outcome, events) = send(&b, &job, &unit, &hashes).await?;
    assert!(matches!(outcome, Outcome::Exit { code: 1, .. }));
    assert!(events.iter().any(|e| matches!(e, Event::Stderr(_))), "the error is reported");
    assert!(!deps.join("libapp-a1.rlib").exists());
    drop(b);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}
