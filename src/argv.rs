use crate::protocol::{Dep, JobPaths, Unit};
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{ffi::OsStr, path::Path};

pub enum Invocation {
    Probe,
    Compile(Compile),
}

pub struct Compile {
    pub name: ArcStr,
    pub unit: Unit,
    /// Present when the job builds only lib or rlib crates with plain
    /// rustc, without incremental state or host-specific codegen, so
    /// another box with the same toolchain produces the same output.
    pub portable: Option<JobPaths>,
    pub target: Option<ArcStr>,
}

/// `args[0]` is the compiler cargo asked for: rustc, or clippy-driver
/// under `cargo clippy`. `out_dir` is the `OUT_DIR` cargo set, which
/// links a crate to the build script that filled it.
pub fn classify<S: AsRef<OsStr>>(args: &[S], out_dir: Option<&OsStr>) -> Invocation {
    let Some(driver) = args.first() else { return Invocation::Probe };
    let driver = Path::new(driver.as_ref()).file_name().and_then(OsStr::to_str).unwrap_or("?");
    let mut crate_name = None;
    let mut cg = Codegen::default();
    let mut emit = "";
    let mut test = false;
    let mut out_dir_arg = None;
    let mut target = None;
    let mut linkless = true;
    let mut typed = false;
    let mut argfile = false;
    let mut deps = Vec::new();
    let mut paths = JobPaths::default();
    let mut crate_type = |t: &str, linkless: &mut bool| {
        typed = true;
        *linkless &= t.split(',').all(|t| t == "lib" || t == "rlib");
    };
    let mut args = args[1..].iter().filter_map(|a| a.as_ref().to_str());
    while let Some(arg) = args.next() {
        match arg {
            "-" | "-vV" | "-V" | "--version" => return Invocation::Probe,
            _ if arg.starts_with("--print") => return Invocation::Probe,
            "--test" => test = true,
            "--crate-name" => crate_name = args.next(),
            "--crate-type" => crate_type(args.next().unwrap_or("bin"), &mut linkless),
            "--emit" => emit = args.next().unwrap_or(""),
            "--out-dir" => out_dir_arg = args.next(),
            "--target" => target = args.next(),
            "--extern" => deps.extend(args.next().and_then(extern_dep)),
            "-C" => {
                if let Some(v) = args.next() {
                    cg.parse(v);
                }
            }
            "-L" => paths.search.extend(args.next().and_then(search_dir)),
            "-o" => paths.outputs.extend(args.next().map(ArcStr::from)),
            _ => {
                if arg.starts_with('@') {
                    argfile = true;
                } else if !arg.starts_with('-') && arg.ends_with(".rs") {
                    paths.sources.push(ArcStr::from(arg));
                } else if let Some(v) = arg.strip_prefix("-L") {
                    paths.search.extend(search_dir(v));
                } else if let Some(v) = arg.strip_prefix("--crate-name=") {
                    crate_name = Some(v);
                } else if let Some(v) = arg.strip_prefix("--crate-type=") {
                    crate_type(v, &mut linkless);
                } else if let Some(v) = arg.strip_prefix("--emit=") {
                    emit = v;
                } else if let Some(v) = arg.strip_prefix("--out-dir=") {
                    out_dir_arg = Some(v);
                } else if let Some(v) = arg.strip_prefix("--target=") {
                    target = Some(v);
                } else if let Some(v) = arg.strip_prefix("--extern=") {
                    deps.extend(extern_dep(v));
                } else if let Some(v) = arg.strip_prefix("-C") {
                    cg.parse(v);
                }
            }
        }
    }
    let Some(name) = crate_name else { return Invocation::Probe };
    let suffix = if test { ":test" } else { "" };
    let metadata = cg.metadata;
    let opt = if cg.opt.is_empty() { "0" } else { cg.opt };
    let exact: ArcStr = format_compact!("{driver}:{name}:{metadata}:{emit}{suffix}").as_str().into();
    let codegen_units = cg.units.unwrap_or(if cg.incremental { 256 } else { 16 });
    let id = if name.starts_with("build_script_") {
        out_dir_arg.and_then(|d| build_group(Path::new(d)))
    } else if !cg.extra.is_empty() {
        Some(format_compact!("{name}{}", cg.extra).as_str().into())
    } else {
        None
    };
    if let Some(dir) = out_dir.and_then(|d| Path::new(d).parent()) {
        deps.extend(build_group(dir).map(|id| Dep { id, file: None }));
    }
    let portable = driver == "rustc"
        && typed
        && linkless
        && !test
        && !argfile
        && !cg.incremental
        && !cg.native_cpu;
    let portable = match out_dir_arg {
        Some(out_dir) if portable => {
            paths.dep_info = format_compact!("{out_dir}/{name}{}.d", cg.extra).as_str().into();
            paths.out_dir = ArcStr::from(out_dir);
            paths.outputs.extend(
                emit.split(',').filter_map(|e| e.split_once('=')).map(|(_, p)| ArcStr::from(p)),
            );
            Some(paths)
        }
        _ => None,
    };
    Invocation::Compile(Compile {
        name: ArcStr::from(name),
        unit: Unit {
            coarse: format_compact!("{driver}:{name}:O{opt}:{emit}{suffix}").as_str().into(),
            id: id.unwrap_or_else(|| exact.clone()),
            exact,
            deps,
            codegen_units,
        },
        portable,
        target: target.map(ArcStr::from),
    })
}

#[derive(Default)]
struct Codegen<'a> {
    metadata: &'a str,
    extra: &'a str,
    opt: &'a str,
    units: Option<u32>,
    incremental: bool,
    native_cpu: bool,
}

impl<'a> Codegen<'a> {
    fn parse(&mut self, arg: &'a str) {
        if let Some(v) = arg.strip_prefix("metadata=") {
            self.metadata = v;
        } else if let Some(v) = arg.strip_prefix("extra-filename=") {
            self.extra = v;
        } else if let Some(v) = arg.strip_prefix("opt-level=") {
            self.opt = v;
        } else if let Some(v) = arg.strip_prefix("codegen-units=") {
            self.units = v.parse().ok();
        } else if arg == "incremental" || arg.starts_with("incremental=") {
            self.incremental = true;
        } else if arg == "target-cpu=native" {
            self.native_cpu = true;
        }
    }
}

/// A `-L` directory other rustc jobs don't write: `native=`, `all=`, or
/// a bare path.
fn search_dir(spec: &str) -> Option<ArcStr> {
    match spec.split_once('=') {
        Some(("native" | "all", dir)) => Some(ArcStr::from(dir)),
        Some(("dependency" | "crate" | "framework", _)) => None,
        _ => Some(ArcStr::from(spec)),
    }
}

/// `name=…/libfoo-1a2b.rmeta` is the output of the job with id `foo-1a2b`.
fn extern_dep(spec: &str) -> Option<Dep> {
    let (_, path) = spec.split_once('=')?;
    let file = Path::new(path).file_name()?.to_str()?;
    let stem = file.rsplit_once('.').map_or(file, |(stem, _)| stem);
    let id = ArcStr::from(stem.strip_prefix("lib").unwrap_or(stem));
    Some(Dep { id, file: Some(ArcStr::from(path)) })
}

/// A build script compiles into `…/build/pkg-HASH1` and runs into
/// `…/build/pkg-HASH2/out`. Both map to the id `out:…/build/pkg`.
fn build_group(dir: &Path) -> Option<ArcStr> {
    let parent = dir.parent()?.to_str()?;
    let name = dir.file_name()?.to_str()?;
    let (pkg, _hash) = name.rsplit_once('-')?;
    Some(format_compact!("out:{parent}/{pkg}").as_str().into())
}

#[cfg(test)]
mod test {
    use super::*;

    fn compile(args: &[&str], out_dir: Option<&str>) -> Compile {
        match classify(args, out_dir.map(OsStr::new)) {
            Invocation::Compile(c) => c,
            Invocation::Probe => panic!("{args:?} classified as a probe"),
        }
    }

    fn unit(args: &[&str], out_dir: Option<&str>) -> Unit {
        compile(args, out_dir).unit
    }

    #[test]
    fn cargo_probes() {
        for args in [
            &["/r/bin/rustc", "-vV"][..],
            &["/r/bin/rustc", "-", "--crate-name", "___", "--print=file-names", "--crate-type", "bin"],
            &["/r/bin/clippy-driver", "/r/bin/rustc", "-vV"],
            &["rustc", "--edition=2021"],
        ] {
            assert!(matches!(classify(args, None), Invocation::Probe), "{args:?}");
        }
    }

    #[test]
    fn compile_keys() {
        let u = unit(
            &[
                "/r/bin/rustc", "--crate-name", "a", "--edition=2021", "a/src/lib.rs",
                "--emit=dep-info,metadata,link", "-C", "opt-level=3", "-C", "metadata=0c5f",
            ],
            None,
        );
        assert_eq!(u.exact.as_str(), "rustc:a:0c5f:dep-info,metadata,link");
        assert_eq!(u.coarse.as_str(), "rustc:a:O3:dep-info,metadata,link");
        assert_eq!(u.id, u.exact, "no extra-filename, so the id falls back to the key");
        let t = unit(&["rustc", "--crate-name=a", "--test", "-Cmetadata=99", "--emit", "link"], None);
        assert_eq!(t.exact.as_str(), "rustc:a:99:link:test");
        assert_eq!(t.coarse.as_str(), "rustc:a:O0:link:test");
        let c = unit(&["/r/bin/clippy-driver", "/r/bin/rustc", "--crate-name", "a"], None);
        assert_eq!(c.exact.as_str(), "clippy-driver:a::");
        assert_eq!(u.codegen_units, 16, "rustc's default");
        assert_eq!(unit(&["rustc", "--crate-name", "a", "-C", "codegen-units=4"], None).codegen_units, 4);
        assert_eq!(unit(&["rustc", "--crate-name", "a", "-Cincremental=/i"], None).codegen_units, 256);
    }

    /// From a cargo build log: `a` depends on `outside`.
    #[test]
    fn graph_ids_from_cargo_argv() {
        let outside = unit(
            &[
                "rustc", "--crate-name", "outside", "--edition=2021", "/p/outside/src/lib.rs",
                "-C", "metadata=44ee2ec5bf22e136", "-C", "extra-filename=-80db8be130f292f3",
                "--out-dir", "/t/release/deps", "-L", "dependency=/t/release/deps",
            ],
            None,
        );
        assert_eq!(outside.id.as_str(), "outside-80db8be130f292f3");
        assert!(outside.deps.is_empty());
        let a = unit(
            &[
                "rustc", "--crate-name", "a", "a/src/lib.rs", "-C", "extra-filename=-8169c16474ca1297",
                "--extern", "outside=/t/release/deps/liboutside-80db8be130f292f3.rmeta",
                "--extern", "serde_derive=/t/release/deps/libserde_derive-1234abcd.so",
                "--extern", "noprelude:alloc",
            ],
            None,
        );
        assert_eq!(a.id.as_str(), "a-8169c16474ca1297");
        let ids: Vec<&str> = a.deps.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, ["outside-80db8be130f292f3", "serde_derive-1234abcd"]);
        assert_eq!(
            a.deps[0].file.as_deref(),
            Some("/t/release/deps/liboutside-80db8be130f292f3.rmeta")
        );
    }

    #[test]
    fn build_scripts_link_to_their_crate() {
        let script = unit(
            &[
                "rustc", "--crate-name", "build_script_build", "build.rs",
                "-C", "extra-filename=-c176630789daaa0c",
                "--out-dir", "/t/release/build/proc-macro2-c176630789daaa0c",
            ],
            None,
        );
        assert_eq!(script.id.as_str(), "out:/t/release/build/proc-macro2");
        let lib = unit(
            &["rustc", "--crate-name", "proc_macro2", "src/lib.rs", "-C", "extra-filename=-9f"],
            Some("/t/release/build/proc-macro2-707e471db1d82c2a/out"),
        );
        assert_eq!(lib.deps, vec![Dep { id: script.id, file: None }]);
    }

    #[test]
    fn only_portable_lib_builds_may_go_remote() {
        let lib = [
            "rustc", "--crate-name", "a", "--crate-type", "lib", "--emit=dep-info,metadata,link",
            "-C", "extra-filename=-77", "--out-dir", "/t/deps",
        ];
        assert!(compile(&lib, None).portable.is_some());
        let with = |extra: &[&str]| {
            let mut args = lib.to_vec();
            args.extend_from_slice(extra);
            compile(&args, None)
        };
        assert!(with(&["-C", "incremental=/t/inc"]).portable.is_none());
        assert!(with(&["-Ctarget-cpu=native"]).portable.is_none());
        assert!(with(&["--test"]).portable.is_none());
        assert!(with(&["--crate-type", "proc-macro"]).portable.is_none());
        assert!(with(&["@/tmp/args"]).portable.is_none());
        assert!(with(&["--crate-type=rlib"]).portable.is_some());
        assert_eq!(
            with(&["--target", "aarch64-unknown-linux-gnu"]).target.as_deref(),
            Some("aarch64-unknown-linux-gnu")
        );
        let no_type = ["rustc", "--crate-name", "b", "src/main.rs", "--out-dir", "/t"];
        assert!(compile(&no_type, None).portable.is_none(), "no crate type means bin");
        let clippy = ["clippy-driver", "rustc", "--crate-name", "a", "--crate-type", "lib", "--out-dir", "/t"];
        assert!(compile(&clippy, None).portable.is_none());
    }

    #[test]
    fn job_paths() {
        let c = compile(
            &[
                "rustc", "--crate-name", "a", "--crate-type", "lib", "a/src/lib.rs", "--cfg", "feature=\"x\"",
                "--emit=dep-info,metadata,link", "-C", "extra-filename=-77", "--out-dir", "/t/deps",
                "-L", "dependency=/t/deps", "-L", "native=/t/build/a-1/out", "-L/usr/lib/extra",
            ],
            None,
        );
        let p = c.portable.unwrap();
        assert_eq!(p.out_dir.as_str(), "/t/deps");
        assert_eq!(p.dep_info.as_str(), "/t/deps/a-77.d");
        assert_eq!(p.sources, ["a/src/lib.rs"]);
        assert_eq!(p.search, ["/t/build/a-1/out", "/usr/lib/extra"]);
        assert!(p.outputs.is_empty());
    }
}
