use std::ffi::OsStr;

/// Cargo passes `-C incremental=...` for the dev profile. Those invocations
/// stay on the machine that owns the cache.
pub fn has_incremental<S: AsRef<OsStr>>(args: &[S]) -> bool {
    let mut after_c = false;
    for arg in args {
        let Some(s) = arg.as_ref().to_str() else {
            after_c = false;
            continue;
        };
        if after_c {
            if s == "incremental" || s.starts_with("incremental=") {
                return true;
            }
            after_c = false;
            continue;
        }
        if s == "-C" {
            after_c = true;
            continue;
        }
        if let Some(rest) = s.strip_prefix("-C") {
            if rest == "incremental" || rest.starts_with("incremental=") {
                return true;
            }
        }
    }
    false
}

pub fn crate_name<S: AsRef<OsStr>>(args: &[S]) -> Option<String> {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let Some(s) = arg.as_ref().to_str() else { continue };
        if s == "--crate-name" {
            return args.next().and_then(|a| a.as_ref().to_str()).map(str::to_string);
        }
        if let Some(name) = s.strip_prefix("--crate-name=") {
            return Some(name.to_string());
        }
    }
    None
}

/// `--target` when the job is a cross compile, otherwise the compiler host.
pub fn job_target<S: AsRef<OsStr>>(args: &[S], host: &str) -> String {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let Some(s) = arg.as_ref().to_str() else { continue };
        if s == "--target" {
            if let Some(t) = args.next().and_then(|a| a.as_ref().to_str()) {
                return t.to_string();
            }
        }
        if let Some(t) = s.strip_prefix("--target=") {
            return t.to_string();
        }
    }
    host.to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn incremental_forms() {
        assert!(has_incremental(&["-C", "incremental=/tmp/x"]));
        assert!(has_incremental(&["-Cincremental=/tmp/x"]));
        assert!(has_incremental(&["-C", "incremental"]));
        assert!(!has_incremental(&["-C", "opt-level=2", "--crate-name", "foo"]));
        assert!(!has_incremental(&["--cfg", "incremental"]));
    }

    #[test]
    fn names_and_target() {
        assert_eq!(crate_name(&["--crate-name", "foo_bar"]).as_deref(), Some("foo_bar"));
        assert_eq!(crate_name(&["--crate-name=foo"]).as_deref(), Some("foo"));
        assert_eq!(job_target(&["--target", "aarch64-unknown-linux-gnu"], "host"), "aarch64-unknown-linux-gnu");
        assert_eq!(job_target(&["--edition", "2024"], "host"), "host");
    }
}
