use crate::protocol::{Dep, JobPaths, Reads, Unit};
use arcstr::{ArcStr, literal};
use compact_str::{CompactString, format_compact};
use std::{ffi::OsStr, path::Path};

pub enum Invocation {
    Probe,
    Compile(Compile),
}

pub struct Compile {
    pub name: ArcStr,
    pub unit: Unit,
    /// Present when the job runs plain rustc without incremental state or
    /// host-specific codegen, so another box with the same toolchain
    /// produces the same output.
    pub portable: Option<Portable>,
    pub target: Option<ArcStr>,
}

pub struct Portable {
    pub paths: JobPaths,
    pub reads: Reads,
}

/// `args[0]` is the compiler cargo asked for: rustc, or clippy-driver
/// under `cargo clippy`. `out_dir` is the `OUT_DIR` cargo set, which
/// links a crate to the build script that filled it.
pub fn classify<S: AsRef<OsStr>>(args: &[S], out_dir: Option<&OsStr>) -> Invocation {
    let Some(driver) = args.first() else { return Invocation::Probe };
    let driver = Path::new(driver.as_ref()).file_name().and_then(OsStr::to_str).unwrap_or("?");
    let mut crate_name = None;
    let mut cg = Codegen::default();
    // Every `--emit` given, comma separated, as rustc takes them all.
    let mut emit = CompactString::default();
    let mut print = false;
    let mut test = false;
    let mut out_dir_arg = None;
    let mut target = None;
    let mut types = CrateTypes::default();
    let mut argfile = false;
    let mut deps = Vec::new();
    let mut paths = JobPaths::default();
    let mut args = args[1..].iter().filter_map(|a| a.as_ref().to_str());
    while let Some(arg) = args.next() {
        match arg {
            // XCR claude for claude: [bug/low] Two flags are read as if they came once, or only in
            // probes. `--print` anywhere makes a probe, but `cargo rustc --crate-type staticlib --
            // --print native-static-libs` (the documented way to get a staticlib's link line) is a
            // real compile, which then runs with no slot, outside the scheduler and memory
            // admission (the probe in argv-01 printed `probe` for it). And rustc unions repeated
            // `--emit` flags while `emit` keeps the last, so `cargo rustc -- --emit=llvm-ir` on a
            // binary keys the unit `llvm-ir` and gives Reads::Metadata though it still links (the
            // probe printed `reads=Some(Metadata)`); the remote run then fails for want of rlibs
            // and reruns here, and a `kind=path` in the earlier `--emit` is missing from
            // paths.outputs. Fix: make `--print` a probe only when no source file is given, and
            // gather every `--emit` value. (argv-02)
            // 2026-11-01 claude: A --print (either spelling, its value consumed) makes a probe
            // only when no .rs source is given; a compile that prints stays here, since --print
            // KIND=PATH writes a file no box sends back. Every --emit is gathered, comma joined,
            // into the unit key, the outputs and reads. Tests:
            // argv::test::a_print_with_a_source_file_compiles (classified a probe before) and
            // argv::test::every_emit_counts (keyed llvm-ir and read Metadata before), plus a
            // --print row in what_portable_jobs_read. README's What stays local lists --print.
            "-" | "-vV" | "-V" | "--version" => return Invocation::Probe,
            "--print" => {
                print = true;
                args.next();
            }
            "--test" => test = true,
            "--crate-name" => crate_name = args.next(),
            "--crate-type" => types.add(args.next().unwrap_or("bin")),
            "--emit" => emit_also(&mut emit, args.next()),
            "--out-dir" => out_dir_arg = args.next(),
            "--target" => target = args.next(),
            "--extern" => deps.extend(args.next().and_then(extern_dep)),
            "-C" | "--codegen" => {
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
                    types.add(v);
                } else if let Some(v) = arg.strip_prefix("--emit=") {
                    emit_also(&mut emit, Some(v));
                } else if arg.starts_with("--print=") {
                    print = true;
                } else if let Some(v) = arg.strip_prefix("--out-dir=") {
                    out_dir_arg = Some(v);
                } else if let Some(v) = arg.strip_prefix("--target=") {
                    target = Some(v);
                } else if let Some(v) = arg.strip_prefix("--extern=") {
                    deps.extend(extern_dep(v));
                } else if let Some(v) = arg.strip_prefix("--codegen=").or_else(|| arg.strip_prefix("-C")) {
                    cg.parse(v);
                }
            }
        }
    }
    let Some(name) = crate_name else { return Invocation::Probe };
    // `cargo rustc -- --print native-static-libs` compiles too.
    if print && paths.sources.is_empty() {
        return Invocation::Probe;
    }
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
    // XCR claude for claude: [doc-drift/low] README's "What stays local" list misses reasons a job
    // stays here: an `@argfile` argument (`argfile` here), a compile with no `--out-dir`, an
    // argument, compiler path or cwd that isn't UTF-8 (wrapper::job), a project `rustc-dist.toml`
    // that fails to parse (wrapper::job prints the error and keeps the job), and a package
    // directory past remote's WALK_LIMIT of 256 MB, which DESIGN mentions. List them, or say the
    // list gives the common ones. (argv-03)
    // 2026-11-01 claude: README's What stays local now lists an @file, --print, no --out-dir, a
    // non-UTF-8 argument, compiler path or cwd, a project rustc-dist.toml that doesn't parse, and
    // a package directory past 256 MB. Doc only, no test.
    let portable =
        driver == "rustc" && !argfile && !print && !cg.incremental && !cg.native_cpu && !cg.split_debuginfo;
    let portable = match out_dir_arg {
        Some(out_dir) if portable => {
            paths.dep_info = format_compact!("{out_dir}/{name}{}.d", cg.extra).as_str().into();
            paths.out_dir = ArcStr::from(out_dir);
            paths.outputs.extend(
                emit.split(',').filter_map(|e| e.split_once('=')).map(|(_, p)| ArcStr::from(p)),
            );
            Some(Portable { paths, reads: types.reads(test, &emit, &cg, target) })
        }
        _ => None,
    };
    Invocation::Compile(Compile {
        name: ArcStr::from(name),
        unit: Unit {
            coarse: format_compact!("{driver}:{name}:O{opt}:{emit}{suffix}").as_str().into(),
            id: id.unwrap_or_else(|| exact.clone()),
            dir: out_dir_arg.map(ArcStr::from),
            exact,
            deps,
            codegen_units,
        },
        portable,
        target: target.map(ArcStr::from),
    })
}

fn emit_also(emit: &mut CompactString, kinds: Option<&str>) {
    let Some(kinds) = kinds else { return };
    if !emit.is_empty() {
        emit.push(',');
    }
    emit.push_str(kinds);
}

/// The crate types a job builds. None given means a binary.
#[derive(Default)]
struct CrateTypes {
    given: bool,
    /// An executable or proc macro, which links the rlibs below it.
    links: bool,
    /// A dylib or static library, which reads the rlibs' object code
    /// before linking.
    reads_objects: bool,
}

impl CrateTypes {
    fn add(&mut self, types: &str) {
        self.given = true;
        for t in types.split(',') {
            match t {
                "lib" | "rlib" => (),
                "bin" | "proc-macro" => self.links = true,
                _ => self.reads_objects = true,
            }
        }
    }

    /// A job that emits no object code, such as `cargo check`'s, reads only
    /// metadata. With no `--emit`, rustc emits it. The default linker is
    /// found on `PATH` as `cc` for GNU targets.
    fn reads(&self, test: bool, emit: &str, cg: &Codegen, target: Option<&str>) -> Reads {
        let emits_code = emit.is_empty() || emit.split(',').any(|e| e == "link" || e.starts_with("link="));
        if !emits_code {
            return Reads::Metadata;
        }
        let links = self.links || test || !self.given;
        if self.reads_objects || links && cg.lto {
            return Reads::Rlibs;
        }
        if !links {
            return Reads::Metadata;
        }
        match cg.linker {
            Some(l) if !l.contains('/') => Reads::RlibsToLink { linker: ArcStr::from(l) },
            None if target.is_none_or(|t| t.ends_with("-linux-gnu")) => Reads::RlibsToLink { linker: literal!("cc") },
            _ => Reads::Rlibs,
        }
    }
}

#[derive(Default)]
struct Codegen<'a> {
    metadata: &'a str,
    extra: &'a str,
    opt: &'a str,
    units: Option<u32>,
    linker: Option<&'a str>,
    lto: bool,
    incremental: bool,
    native_cpu: bool,
    /// Debug info left in files rustc doesn't report as artifacts.
    split_debuginfo: bool,
}

impl<'a> Codegen<'a> {
    // XCR claude for claude: [bug/medium] rustc reads a `-C` option's name with `_` and `-` alike,
    // and takes `--codegen V` and `--codegen=V` as `-C V`, but parse matches only the dashed names
    // and classify passes only `-C V` and `-CV` here. So `-C target_cpu=native`, `--codegen
    // target-cpu=native` or `-C split_debuginfo=packed` (from RUSTFLAGS, say) leaves the job
    // portable, and another box builds it for its own CPU: code the client's CPU may not run
    // (SIGILL), with no error anywhere. The same goes for `incremental`. Probe: a temporary test
    // classifying a bin with each of `-C target_cpu=native`, `--codegen target-cpu=native`,
    // `--codegen=target-cpu=native` and `-C split_debuginfo=packed` printed `portable=true
    // reads=Some(RlibsToLink { linker: "cc" })` for all four (`cargo test --bin rustc-dist
    // probe_tmp_review`, removed since); `rustc --print cfg -C target_cpu=native` lists 38 target
    // features, 3 without it. Fix: in classify read `--codegen V` and `--codegen=V` like `-C V`,
    // and here compare the option name with `_` read as `-`. A row per spelling in
    // what_portable_jobs_read's `local` list would pin it. (argv-01)
    // 2026-11-01 claude: classify reads --codegen V and --codegen=V like -C V, and Codegen::parse
    // splits NAME=VALUE and reads _ in the name as - before matching. New rows in
    // what_portable_jobs_read's local list (-C target_cpu=native, --codegen target-cpu=native,
    // --codegen=target-cpu=native, -C split_debuginfo=packed, --codegen incremental=...) and two
    // in compile_keys (--codegen codegen_units=4, --codegen=extra_filename=-77) fail without
    // either half (checked).
    /// One `-C` option, `NAME` or `NAME=VALUE`. rustc reads `_` in a name
    /// as `-`.
    fn parse(&mut self, arg: &'a str) {
        let (name, value) = match arg.split_once('=') {
            Some((name, value)) => (name, Some(value)),
            None => (arg, None),
        };
        let name: CompactString = name.chars().map(|c| if c == '_' { '-' } else { c }).collect();
        match (name.as_str(), value) {
            ("metadata", Some(v)) => self.metadata = v,
            ("extra-filename", Some(v)) => self.extra = v,
            ("opt-level", Some(v)) => self.opt = v,
            ("codegen-units", Some(v)) => self.units = v.parse().ok(),
            ("linker", Some(v)) => self.linker = Some(v),
            ("lto", None) => self.lto = true,
            ("lto", Some(v)) => self.lto = !matches!(v, "off" | "no" | "n" | "false"),
            ("split-debuginfo", Some(v)) => self.split_debuginfo = v != "off",
            ("incremental", _) => self.incremental = true,
            ("target-cpu", Some("native")) => self.native_cpu = true,
            _ => (),
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
    fn a_print_with_a_source_file_compiles() {
        let c = compile(
            &[
                "rustc", "--crate-name", "a", "--edition=2021", "src/lib.rs", "--crate-type", "staticlib",
                "--print", "native-static-libs", "--out-dir", "/t",
            ],
            None,
        );
        assert!(c.portable.is_none(), "what it prints may go to a file");
        assert!(matches!(classify(&["rustc", "--crate-name", "a", "--print=cfg"], None), Invocation::Probe));
    }

    /// rustc takes every `--emit`, as `cargo rustc -- --emit=llvm-ir` adds
    /// one to cargo's.
    #[test]
    fn every_emit_counts() {
        let c = compile(
            &["rustc", "--crate-name", "b", "src/main.rs", "--emit=dep-info,link", "--emit", "llvm-ir=/t/b.ll", "--out-dir", "/t"],
            None,
        );
        assert_eq!(c.unit.exact.as_str(), "rustc:b::dep-info,link,llvm-ir=/t/b.ll");
        let p = c.portable.unwrap();
        assert_eq!(p.reads, Reads::RlibsToLink { linker: literal!("cc") }, "it still links");
        assert_eq!(p.paths.outputs, ["/t/b.ll"]);
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
        assert_eq!(unit(&["rustc", "--crate-name", "a", "--codegen", "codegen_units=4"], None).codegen_units, 4);
        let e = unit(&["rustc", "--crate-name", "a", "--codegen=extra_filename=-77"], None);
        assert_eq!(e.id.as_str(), "a-77", "-C read in each spelling");
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

    fn reads(args: &[&str]) -> Option<Reads> {
        compile(args, None).portable.map(|p| p.reads)
    }

    #[test]
    fn what_portable_jobs_read() {
        let lib = [
            "rustc", "--crate-name", "a", "--crate-type", "lib", "--emit=dep-info,metadata,link",
            "-C", "extra-filename=-77", "--out-dir", "/t/deps",
        ];
        let with = |extra: &[&str]| {
            let mut args = lib.to_vec();
            args.extend_from_slice(extra);
            reads(&args)
        };
        let cc = Some(Reads::RlibsToLink { linker: literal!("cc") });
        assert_eq!(reads(&lib), Some(Reads::Metadata));
        assert_eq!(with(&["--crate-type=rlib"]), Some(Reads::Metadata));
        assert_eq!(with(&["--test"]), cc);
        let check = |extra: &[&str]| {
            let mut args = lib.to_vec();
            args[5] = "--emit=dep-info,metadata";
            args.extend_from_slice(extra);
            reads(&args)
        };
        assert_eq!(check(&["--test"]), Some(Reads::Metadata), "cargo check --tests links nothing");
        assert_eq!(check(&["--crate-type", "bin"]), Some(Reads::Metadata));
        assert_eq!(with(&["--crate-type", "proc-macro"]), cc);
        assert_eq!(with(&["--crate-type", "bin", "-C", "lto=off"]), cc);
        let bin = ["rustc", "--crate-name", "b", "src/main.rs", "--out-dir", "/t"];
        assert_eq!(reads(&bin), cc, "no crate type means bin");
        let clang = Some(Reads::RlibsToLink { linker: literal!("clang") });
        assert_eq!(with(&["--crate-type", "bin", "-C", "linker=clang"]), clang);
        for rlibs in [
            &["--crate-type", "bin", "-C", "lto"][..],
            &["--crate-type", "bin", "-Clto=thin"],
            &["--crate-type", "bin", "-C", "linker=/usr/bin/clang"],
            &["--crate-type", "bin", "--target", "wasm32-unknown-unknown"],
            &["--crate-type", "staticlib"],
            &["--crate-type", "cdylib"],
        ] {
            assert_eq!(with(rlibs), Some(Reads::Rlibs), "{rlibs:?}");
        }
        assert_eq!(with(&["--crate-type", "bin", "--target", "aarch64-unknown-linux-gnu"]), cc);
        for local in [
            &["-C", "incremental=/t/inc"][..],
            &["--codegen", "incremental=/t/inc"],
            &["-Ctarget-cpu=native"],
            &["-C", "target_cpu=native"],
            &["--codegen", "target-cpu=native"],
            &["--codegen=target-cpu=native"],
            &["@/tmp/args"],
            &["-C", "split-debuginfo=unpacked"],
            &["-C", "split_debuginfo=packed"],
            &["src/lib.rs", "--print", "native-static-libs"],
        ] {
            assert_eq!(with(local), None, "{local:?}");
        }
        assert_eq!(with(&["-C", "split-debuginfo=off"]), Some(Reads::Metadata));
        assert_eq!(with(&["-C", "target-cpu=x86-64-v3"]), Some(Reads::Metadata));
        let cross = ["rustc", "--crate-name", "a", "--target", "aarch64-unknown-linux-gnu", "--out-dir", "/t"];
        assert_eq!(compile(&cross, None).target.as_deref(), Some("aarch64-unknown-linux-gnu"));
        let clippy = ["clippy-driver", "rustc", "--crate-name", "a", "--crate-type", "lib", "--out-dir", "/t"];
        assert_eq!(reads(&clippy), None);
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
        let p = c.portable.unwrap().paths;
        assert_eq!(p.out_dir.as_str(), "/t/deps");
        assert_eq!(p.dep_info.as_str(), "/t/deps/a-77.d");
        assert_eq!(p.sources, ["a/src/lib.rs"]);
        assert_eq!(p.search, ["/t/build/a-1/out", "/usr/lib/extra"]);
        assert!(p.outputs.is_empty());
    }
}
