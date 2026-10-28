use crate::protocol::{mirror_rel, mirror_token};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use compact_str::format_compact;
use memmap2::Mmap;
use poolshark::local::LPooled;
use std::{
    collections::HashSet,
    fs::File,
    path::{Component, Path, PathBuf},
};

pub struct Staged {
    pub argv: Vec<ArcStr>,
    pub env: Vec<(ArcStr, ArcStr)>,
    pub files: Vec<(ArcStr, Bytes)>,
    pub dirs: Vec<ArcStr>,
}

struct Collector {
    files: Vec<(ArcStr, Bytes)>,
    dirs: Vec<ArcStr>,
    seen: HashSet<PathBuf>,
    roots: Vec<PathBuf>,
}

impl Collector {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            dirs: Vec::new(),
            seen: HashSet::new(),
            roots: Vec::new(),
        }
    }

    fn add_dir(&mut self, abs: &Path) {
        let Some(s) = abs.to_str() else { return };
        let rel = mirror_rel(s);
        if !self.dirs.iter().any(|d| d == &rel) {
            self.dirs.push(rel);
        }
    }

    fn add_file(&mut self, abs: &Path) -> Result<()> {
        let abs = normalize(abs);
        if !self.seen.insert(abs.clone()) {
            return Ok(());
        }
        let Some(s) = abs.to_str() else {
            bail!("{} is not utf-8", abs.display());
        };
        let bytes = map_file(&abs)?;
        self.files.push((mirror_rel(s), bytes));
        if let Some(parent) = abs.parent() {
            self.add_dir(parent);
        }
        Ok(())
    }

    fn add_tree(&mut self, root: &Path, mode: Walk) -> Result<()> {
        let root = normalize(root);
        if !root.is_dir() {
            return Ok(());
        }
        self.roots.push(root.clone());
        self.add_dir(&root);
        let mut stack: LPooled<Vec<PathBuf>> = LPooled::take();
        stack.push(root);
        while let Some(dir) = stack.pop() {
            let rd = std::fs::read_dir(&dir)
                .with_context(|| format_compact!("reading {}", dir.display()))?;
            for ent in rd {
                let ent = ent?;
                let name = ent.file_name();
                let name = name.to_string_lossy();
                if mode == Walk::Source && (name == "target" || name == ".git") {
                    continue;
                }
                let path = ent.path();
                let ft = ent.file_type()?;
                if ft.is_dir() {
                    self.add_dir(&path);
                    if mode == Walk::Source {
                        stack.push(path);
                    }
                } else if ft.is_file() {
                    self.add_file(&path)?;
                } else if ft.is_symlink() {
                    if std::fs::metadata(&path).map(|m| m.is_file()).unwrap_or(false) {
                        self.add_file(&path)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn under_root(&self, abs: &Path) -> bool {
        let abs = normalize(abs);
        self.roots.iter().any(|r| abs.starts_with(r))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Walk {
    Source,
    Flat,
}

pub fn stage(argv: &[String], env: &[(String, String)], cwd: &Path) -> Result<Staged> {
    if argv.is_empty() {
        bail!("empty argv");
    }
    let mut col = Collector::new();
    if let Some((_, manifest)) = env.iter().find(|(k, _)| k == "CARGO_MANIFEST_DIR") {
        let dir = normalize(&absolutize(manifest, cwd));
        col.add_tree(&dir, Walk::Source)?;
    }
    if let Some((_, out)) = env.iter().find(|(k, _)| k == "OUT_DIR") {
        let dir = normalize(&absolutize(out, cwd));
        col.add_tree(&dir, Walk::Source)?;
    }

    let mut out_argv = Vec::with_capacity(argv.len().saturating_sub(1));
    let mut i = 1;
    while i < argv.len() {
        let arg = &argv[i];
        if arg == "--sysroot" {
            i += 2;
            continue;
        }
        if arg.starts_with("--sysroot=") {
            i += 1;
            continue;
        }
        if let Some(rewritten) = take_joined(arg, &["--out-dir", "--extern", "-L", "--emit", "-o", "--remap-path-prefix"]) {
            out_argv.push(rewrite_flag(&mut col, cwd, rewritten)?);
            i += 1;
            continue;
        }
        if let Some(flag) = takes_value(arg) {
            let Some(value) = argv.get(i + 1) else {
                bail!("{flag} is missing its value");
            };
            out_argv.push(ArcStr::from(flag));
            out_argv.push(rewrite_flag_value(&mut col, cwd, flag, value)?);
            i += 2;
            continue;
        }
        if let Some(rest) = arg.strip_prefix("-C") {
            if !rest.is_empty() {
                out_argv.push(rewrite_codegen(&mut col, cwd, arg, rest)?);
                i += 1;
                continue;
            }
        }
        if arg == "-C" {
            let Some(value) = argv.get(i + 1) else {
                bail!("-C is missing its value");
            };
            out_argv.push(ArcStr::from("-C"));
            out_argv.push(rewrite_codegen(&mut col, cwd, value, value)?);
            i += 2;
            continue;
        }
        if !arg.starts_with('-') {
            let abs = normalize(&absolutize(arg, cwd));
            if abs.is_file() {
                if !col.under_root(&abs) {
                    col.add_file(&abs)?;
                }
                out_argv.push(mirror_token(abs.to_str().context("path")?));
                i += 1;
                continue;
            }
        }
        out_argv.push(ArcStr::from(arg));
        i += 1;
    }

    let prefixes = col.roots.clone();
    let mut out_env = Vec::with_capacity(env.len());
    for (k, v) in env {
        if k == "RUSTC_WRAPPER" {
            continue;
        }
        let value = if is_path_list(k) {
            ArcStr::from(v)
        } else {
            rewrite_env_value(v, &prefixes)
        };
        out_env.push((ArcStr::from(k), value));
    }

    Ok(Staged { argv: out_argv, env: out_env, files: col.files, dirs: col.dirs })
}

fn is_path_list(key: &str) -> bool {
    key == "PATH" || key.ends_with("LIBRARY_PATH") || key == "PKG_CONFIG_PATH"
}

fn rewrite_env_value(value: &str, roots: &[PathBuf]) -> ArcStr {
    let mut roots: Vec<&str> = roots.iter().filter_map(|p| p.to_str()).collect();
    roots.sort_by_key(|s| std::cmp::Reverse(s.len()));
    for root in roots {
        if value == root || value.starts_with(&format!("{root}/")) {
            return mirror_token(value);
        }
    }
    ArcStr::from(value)
}

fn takes_value(arg: &str) -> Option<&str> {
    const FLAGS: &[&str] = &[
        "--out-dir",
        "--extern",
        "-L",
        "--emit",
        "-o",
        "--remap-path-prefix",
    ];
    FLAGS.iter().copied().find(|f| *f == arg)
}

struct Joined<'a> {
    flag: &'a str,
    value: &'a str,
}

fn take_joined<'a>(arg: &'a str, flags: &[&'a str]) -> Option<Joined<'a>> {
    for flag in flags {
        if let Some(value) = arg.strip_prefix(flag).and_then(|s| s.strip_prefix('=')) {
            return Some(Joined { flag, value });
        }
        if *flag == "-L" {
            if let Some(value) = arg.strip_prefix("-L") {
                if !value.is_empty() && !value.starts_with('-') {
                    return Some(Joined { flag: "-L", value });
                }
            }
        }
        if *flag == "-o" {
            if let Some(value) = arg.strip_prefix("-o") {
                if !value.is_empty() && !value.starts_with('-') {
                    return Some(Joined { flag: "-o", value });
                }
            }
        }
    }
    None
}

fn rewrite_flag(col: &mut Collector, cwd: &Path, joined: Joined<'_>) -> Result<ArcStr> {
    let value = rewrite_flag_value(col, cwd, joined.flag, joined.value)?;
    // `-L` and `-o` glue their value on (`-Ldependency=...`, `-ofilename`).
    if joined.flag == "-L" || joined.flag == "-o" {
        Ok(ArcStr::from(format!("{}{value}", joined.flag)))
    } else {
        Ok(ArcStr::from(format!("{}={value}", joined.flag)))
    }
}

fn rewrite_flag_value(col: &mut Collector, cwd: &Path, flag: &str, value: &str) -> Result<ArcStr> {
    match flag {
        "--out-dir" | "-o" | "-L" => Ok(rewrite_dir_arg(col, cwd, value)?),
        "--extern" => Ok(rewrite_extern(col, cwd, value)?),
        "--emit" => rewrite_emit(col, cwd, value),
        "--remap-path-prefix" => Ok(rewrite_remap(value)),
        _ => Ok(ArcStr::from(value)),
    }
}

fn rewrite_dir_arg(col: &mut Collector, cwd: &Path, value: &str) -> Result<ArcStr> {
    let (kind, path) = split_kind(value);
    let abs = normalize(&absolutize(path, cwd));
    if abs.is_dir() {
        col.add_tree(&abs, Walk::Flat)?;
    } else {
        col.add_dir(&abs);
        if abs.is_file() {
            col.add_file(&abs)?;
        }
    }
    let Some(s) = abs.to_str() else { bail!("{} is not utf-8", abs.display()) };
    let mirrored = mirror_token(s);
    Ok(match kind {
        Some(k) => ArcStr::from(format!("{k}={mirrored}")),
        None => mirrored,
    })
}

fn rewrite_extern(col: &mut Collector, cwd: &Path, value: &str) -> Result<ArcStr> {
    let Some((name, path)) = value.split_once('=') else {
        return Ok(ArcStr::from(value));
    };
    let abs = normalize(&absolutize(path, cwd));
    if abs.is_file() && !col.under_root(&abs) {
        col.add_file(&abs)?;
    }
    let Some(s) = abs.to_str() else { bail!("{} is not utf-8", abs.display()) };
    Ok(ArcStr::from(format!("{name}={}", mirror_token(s))))
}

fn rewrite_emit(col: &mut Collector, cwd: &Path, spec: &str) -> Result<ArcStr> {
    let mut parts = Vec::new();
    for part in spec.split(',') {
        if let Some((kind, path)) = part.split_once('=') {
            let abs = normalize(&absolutize(path, cwd));
            if let Some(parent) = abs.parent() {
                col.add_dir(parent);
            }
            let Some(s) = abs.to_str() else { bail!("{} is not utf-8", abs.display()) };
            parts.push(format!("{kind}={}", mirror_token(s)));
        } else {
            parts.push(part.to_string());
        }
    }
    Ok(ArcStr::from(parts.join(",")))
}

fn rewrite_remap(spec: &str) -> ArcStr {
    let Some((from, to)) = spec.split_once('=') else {
        return ArcStr::from(spec);
    };
    if from.starts_with('/') {
        ArcStr::from(format!("{}={to}", mirror_token(from)))
    } else {
        ArcStr::from(spec)
    }
}

fn rewrite_codegen(col: &mut Collector, cwd: &Path, original: &str, opt: &str) -> Result<ArcStr> {
    let Some(val) = opt.strip_prefix("link-arg=") else {
        return Ok(ArcStr::from(original));
    };
    if !(val.starts_with('/') || val.starts_with('.')) {
        return Ok(ArcStr::from(original));
    }
    let abs = normalize(&absolutize(val, cwd));
    if abs.is_file() {
        if !col.under_root(&abs) {
            col.add_file(&abs)?;
        }
        let Some(s) = abs.to_str() else { bail!("{} is not utf-8", abs.display()) };
        let mirrored = mirror_token(s);
        if original.starts_with("-C") && original != opt {
            return Ok(ArcStr::from(format!("-Clink-arg={mirrored}")));
        }
        return Ok(ArcStr::from(format!("link-arg={mirrored}")));
    }
    Ok(ArcStr::from(original))
}

fn split_kind(value: &str) -> (Option<&str>, &str) {
    const KINDS: &[&str] = &["dependency", "crate", "native", "framework", "all"];
    if let Some((k, p)) = value.split_once('=') {
        if KINDS.contains(&k) {
            return (Some(k), p);
        }
    }
    (None, value)
}

fn absolutize(path: &str, cwd: &Path) -> PathBuf {
    let p = Path::new(path);
    if p.is_absolute() { p.to_path_buf() } else { cwd.join(p) }
}

fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn map_file(path: &Path) -> Result<Bytes> {
    let file = File::open(path).with_context(|| format_compact!("opening {}", path.display()))?;
    let meta = file.metadata()?;
    if meta.len() == 0 {
        return Ok(Bytes::new());
    }
    // Read-only map. The file is a build input and is not written while packed.
    let map = unsafe { Mmap::map(&file)? };
    Ok(Bytes::from_owner(map))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;

    fn scratch() -> PathBuf {
        let p = std::env::temp_dir().join(format!("rustc-dist-stage-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(p.join("src")).unwrap();
        fs::create_dir_all(p.join("target/debug/deps")).unwrap();
        fs::write(p.join("src/lib.rs"), "pub fn f() -> i32 { 1 }\n").unwrap();
        fs::write(p.join("target/debug/deps/libdep.rmeta"), b"rmeta").unwrap();
        fs::create_dir_all(p.join("target/debug/build/pkg/out")).unwrap();
        fs::write(p.join("target/debug/build/pkg/out/gen.rs"), "pub const N: i32 = 1;\n").unwrap();
        p
    }

    #[test]
    fn stages_package_extern_and_out_dir() {
        let root = scratch();
        let src = root.join("src/lib.rs");
        let dep = root.join("target/debug/deps/libdep.rmeta");
        let deps = root.join("target/debug/deps");
        let out = root.join("target/debug/build/pkg/out");
        let argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "pkg".to_string(),
            src.display().to_string(),
            "--out-dir".to_string(),
            deps.display().to_string(),
            format!("--extern=dep={}", dep.display()),
            format!("-Ldependency={}", deps.display()),
            "--sysroot=/unused".to_string(),
            format!("--remap-path-prefix={}=", root.display()),
        ];
        let env = vec![
            ("CARGO_MANIFEST_DIR".to_string(), root.display().to_string()),
            ("OUT_DIR".to_string(), out.display().to_string()),
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ("RUSTC_WRAPPER".to_string(), "rustc-dist".to_string()),
        ];
        let staged = stage(&argv, &env, &root).unwrap();
        let argv_s: Vec<&str> = staged.argv.iter().map(|s| s.as_str()).collect();
        assert!(!argv_s.iter().any(|a| a.contains("sysroot")));
        assert!(!staged.env.iter().any(|(k, _)| k.as_str() == "RUSTC_WRAPPER"));
        let path_arg = argv_s.iter().find(|a| a.contains("lib.rs")).unwrap();
        assert!(path_arg.starts_with("__JOB__/a/"), "{path_arg}");
        assert!(path_arg.ends_with("/src/lib.rs"), "{path_arg}");
        assert!(staged.files.iter().any(|(r, b)| r.ends_with("/src/lib.rs") && !b.is_empty()));
        assert!(staged.files.iter().any(|(r, _)| r.ends_with("/libdep.rmeta")));
        assert!(staged.files.iter().any(|(r, _)| r.ends_with("/gen.rs")));
        assert!(!staged.files.iter().any(|(r, _)| r.contains("/.git/") || r.ends_with("/.git")));
        let path_env = staged.env.iter().find(|(k, _)| k.as_str() == "PATH").unwrap();
        assert_eq!(path_env.1.as_str(), "/usr/bin:/bin");
        let manifest = staged.env.iter().find(|(k, _)| k.as_str() == "CARGO_MANIFEST_DIR").unwrap();
        assert!(manifest.1.starts_with("__JOB__/a/"));
        let remap = argv_s.iter().find(|a| a.contains("remap")).unwrap();
        assert!(remap.contains("__JOB__/a/"), "{remap}");
        let _ = fs::remove_dir_all(&root);
    }
}
