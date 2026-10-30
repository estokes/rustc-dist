use crate::protocol::Unit;
use compact_str::format_compact;
use std::{ffi::OsStr, path::Path};

pub enum Invocation {
    Probe,
    Compile(Unit),
}

/// `args[0]` is the compiler cargo asked for: rustc, or clippy-driver
/// under `cargo clippy`.
pub fn classify<S: AsRef<OsStr>>(args: &[S]) -> Invocation {
    let Some(driver) = args.first() else { return Invocation::Probe };
    let driver = Path::new(driver.as_ref()).file_name().and_then(OsStr::to_str).unwrap_or("?");
    let mut crate_name = None;
    let mut metadata = "";
    let mut opt = "0";
    let mut emit = "";
    let mut test = false;
    let mut args = args[1..].iter().filter_map(|a| a.as_ref().to_str());
    while let Some(arg) = args.next() {
        match arg {
            "-" | "-vV" | "-V" | "--version" => return Invocation::Probe,
            _ if arg.starts_with("--print") => return Invocation::Probe,
            "--test" => test = true,
            "--crate-name" => crate_name = args.next(),
            "--emit" => emit = args.next().unwrap_or(""),
            "-C" => {
                if let Some(v) = args.next() {
                    codegen(v, &mut metadata, &mut opt);
                }
            }
            _ => {
                if let Some(v) = arg.strip_prefix("--crate-name=") {
                    crate_name = Some(v);
                } else if let Some(v) = arg.strip_prefix("--emit=") {
                    emit = v;
                } else if let Some(v) = arg.strip_prefix("-C") {
                    codegen(v, &mut metadata, &mut opt);
                }
            }
        }
    }
    let Some(name) = crate_name else { return Invocation::Probe };
    let test = if test { ":test" } else { "" };
    Invocation::Compile(Unit {
        exact: format_compact!("{driver}:{name}:{metadata}:{emit}{test}").as_str().into(),
        coarse: format_compact!("{driver}:{name}:O{opt}:{emit}{test}").as_str().into(),
    })
}

fn codegen<'a>(arg: &'a str, metadata: &mut &'a str, opt: &mut &'a str) {
    if let Some(v) = arg.strip_prefix("metadata=") {
        *metadata = v;
    } else if let Some(v) = arg.strip_prefix("opt-level=") {
        *opt = v;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn unit(args: &[&str]) -> Unit {
        match classify(args) {
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
            assert!(matches!(classify(args), Invocation::Probe), "{args:?}");
        }
    }

    #[test]
    fn compile_keys() {
        let u = unit(&[
            "/r/bin/rustc", "--crate-name", "a", "--edition=2021", "a/src/lib.rs",
            "--emit=dep-info,metadata,link", "-C", "opt-level=3", "-C", "metadata=0c5f",
        ]);
        assert_eq!(u.exact.as_str(), "rustc:a:0c5f:dep-info,metadata,link");
        assert_eq!(u.coarse.as_str(), "rustc:a:O3:dep-info,metadata,link");
        let t = unit(&["rustc", "--crate-name=a", "--test", "-Cmetadata=99", "--emit", "link"]);
        assert_eq!(t.exact.as_str(), "rustc:a:99:link:test");
        assert_eq!(t.coarse.as_str(), "rustc:a:O0:link:test");
        let c = unit(&["/r/bin/clippy-driver", "/r/bin/rustc", "--crate-name", "a"]);
        assert_eq!(c.exact.as_str(), "clippy-driver:a::");
    }
}
