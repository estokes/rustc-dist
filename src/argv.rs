use crate::protocol::Unit;
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{ffi::OsStr, path::Path};

pub enum Invocation {
    Probe,
    Compile(Unit),
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
    let mut deps = Vec::new();
    let mut args = args[1..].iter().filter_map(|a| a.as_ref().to_str());
    while let Some(arg) = args.next() {
        match arg {
            "-" | "-vV" | "-V" | "--version" => return Invocation::Probe,
            _ if arg.starts_with("--print") => return Invocation::Probe,
            "--test" => test = true,
            "--crate-name" => crate_name = args.next(),
            "--emit" => emit = args.next().unwrap_or(""),
            "--out-dir" => out_dir_arg = args.next(),
            "--extern" => deps.extend(args.next().and_then(extern_id)),
            "-C" => {
                if let Some(v) = args.next() {
                    cg.parse(v);
                }
            }
            _ => {
                if let Some(v) = arg.strip_prefix("--crate-name=") {
                    crate_name = Some(v);
                } else if let Some(v) = arg.strip_prefix("--emit=") {
                    emit = v;
                } else if let Some(v) = arg.strip_prefix("--out-dir=") {
                    out_dir_arg = Some(v);
                } else if let Some(v) = arg.strip_prefix("--extern=") {
                    deps.extend(extern_id(v));
                } else if let Some(v) = arg.strip_prefix("-C") {
                    cg.parse(v);
                }
            }
        }
    }
    let Some(name) = crate_name else { return Invocation::Probe };
    let test = if test { ":test" } else { "" };
    let metadata = cg.metadata;
    let opt = if cg.opt.is_empty() { "0" } else { cg.opt };
    let exact: ArcStr = format_compact!("{driver}:{name}:{metadata}:{emit}{test}").as_str().into();
    let id = if name.starts_with("build_script_") {
        out_dir_arg.and_then(|d| build_group(Path::new(d)))
    } else if !cg.extra.is_empty() {
        Some(format_compact!("{name}{}", cg.extra).as_str().into())
    } else {
        None
    };
    if let Some(dir) = out_dir.and_then(|d| Path::new(d).parent()) {
        deps.extend(build_group(dir));
    }
    Invocation::Compile(Unit {
        coarse: format_compact!("{driver}:{name}:O{opt}:{emit}{test}").as_str().into(),
        id: id.unwrap_or_else(|| exact.clone()),
        exact,
        deps,
    })
}

#[derive(Default)]
struct Codegen<'a> {
    metadata: &'a str,
    extra: &'a str,
    opt: &'a str,
}

impl<'a> Codegen<'a> {
    fn parse(&mut self, arg: &'a str) {
        if let Some(v) = arg.strip_prefix("metadata=") {
            self.metadata = v;
        } else if let Some(v) = arg.strip_prefix("extra-filename=") {
            self.extra = v;
        } else if let Some(v) = arg.strip_prefix("opt-level=") {
            self.opt = v;
        }
    }
}

/// `name=…/libfoo-1a2b.rmeta` is the output of the job with id `foo-1a2b`.
fn extern_id(spec: &str) -> Option<ArcStr> {
    let (_, path) = spec.split_once('=')?;
    let file = Path::new(path).file_name()?.to_str()?;
    let stem = file.rsplit_once('.').map_or(file, |(stem, _)| stem);
    Some(ArcStr::from(stem.strip_prefix("lib").unwrap_or(stem)))
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

    fn unit(args: &[&str], out_dir: Option<&str>) -> Unit {
        match classify(args, out_dir.map(OsStr::new)) {
            Invocation::Compile(u) => u,
            Invocation::Probe => panic!("{args:?} classified as a probe"),
        }
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
        let deps: Vec<&str> = a.deps.iter().map(|d| d.as_str()).collect();
        assert_eq!(deps, ["outside-80db8be130f292f3", "serde_derive-1234abcd"]);
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
        assert_eq!(lib.deps, vec![script.id]);
    }
}
