use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::{CompactString, format_compact};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

pub const PROJECT_FILE: &str = "rustc-dist.toml";

#[derive(Debug, Clone, Default)]
pub struct UserConfig {
    pub base: Option<ArcStr>,
    pub name: Option<ArcStr>,
    pub slots: Option<u64>,
    pub rustc: Option<ArcStr>,
}

pub fn load_user() -> Result<UserConfig> {
    let mut cfg = match user_config_path() {
        Some(path) if path.is_file() => parse_user(&fs::read_to_string(&path).with_context(
            || format_compact!("reading {}", path.display()),
        )?)
        .with_context(|| format_compact!("parsing {}", path.display()))?,
        _ => UserConfig::default(),
    };
    if let Ok(base) = env::var("RUSTC_DIST_BASE") {
        if !base.is_empty() {
            cfg.base = Some(ArcStr::from(base));
        }
    }
    if let Ok(name) = env::var("RUSTC_DIST_NAME") {
        if !name.is_empty() {
            cfg.name = Some(ArcStr::from(name));
        }
    }
    if let Ok(slots) = env::var("RUSTC_DIST_SLOTS") {
        cfg.slots = Some(slots.parse::<u64>().context("RUSTC_DIST_SLOTS")?);
    }
    if let Ok(rustc) = env::var("RUSTC_DIST_RUSTC") {
        if !rustc.is_empty() {
            cfg.rustc = Some(ArcStr::from(rustc));
        }
    }
    Ok(cfg)
}

fn user_config_path() -> Option<PathBuf> {
    if let Some(p) = env::var_os("RUSTC_DIST_CONFIG") {
        return Some(PathBuf::from(p));
    }
    let base = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join(PROJECT_FILE))
}

pub fn runtime_dir() -> PathBuf {
    if let Some(dir) = env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir);
    }
    let uid = unsafe { libc::getuid() };
    PathBuf::from(format!("/tmp/rustc-dist-{uid}"))
}

pub fn socket_path() -> PathBuf {
    runtime_dir().join("rustc-dist.sock")
}

pub fn log_path() -> PathBuf {
    runtime_dir().join("rustc-dist.log")
}

/// Crate names that must be compiled on the client. The nearest
/// `rustc-dist.toml` at or above `start` wins. A missing file is an empty
/// list. A broken file is an error so the caller can fail closed.
pub fn local_crates(start: &Path) -> Result<Vec<ArcStr>> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        let file = d.join(PROJECT_FILE);
        if file.is_file() {
            let text = fs::read_to_string(&file)
                .with_context(|| format_compact!("reading {}", file.display()))?;
            return parse_local(&text)
                .with_context(|| format_compact!("parsing {}", file.display()));
        }
        dir = d.parent();
    }
    Ok(Vec::new())
}

pub fn names_match(listed: &str, crate_name: &str) -> bool {
    norm(listed) == norm(crate_name)
}

fn norm(s: &str) -> CompactString {
    let mut out = CompactString::with_capacity(s.len());
    for c in s.chars() {
        out.push(if c == '-' { '_' } else { c });
    }
    out
}

fn parse_user(text: &str) -> Result<UserConfig> {
    let mut cfg = UserConfig::default();
    for item in items(text)? {
        match item.key.as_str() {
            "base" => cfg.base = Some(item.string()?),
            "name" => cfg.name = Some(item.string()?),
            "rustc" => cfg.rustc = Some(item.string()?),
            "slots" => cfg.slots = Some(item.int()?),
            "local" => {}
            other => bail!("unknown key {other}"),
        }
    }
    Ok(cfg)
}

fn parse_local(text: &str) -> Result<Vec<ArcStr>> {
    let mut names = Vec::new();
    for item in items(text)? {
        match item.key.as_str() {
            "local" => names.extend(item.strings()?),
            "base" | "name" | "rustc" | "slots" => {}
            other => bail!("unknown key {other}"),
        }
    }
    Ok(names)
}

struct Item {
    key: String,
    value: Value,
}

enum Value {
    String(String),
    Int(u64),
    Strings(Vec<String>),
}

impl Item {
    fn string(self) -> Result<ArcStr> {
        match self.value {
            Value::String(s) => Ok(ArcStr::from(s)),
            _ => bail!("{} must be a string", self.key),
        }
    }

    fn int(self) -> Result<u64> {
        match self.value {
            Value::Int(n) => Ok(n),
            _ => bail!("{} must be an integer", self.key),
        }
    }

    fn strings(self) -> Result<Vec<ArcStr>> {
        match self.value {
            Value::Strings(v) => Ok(v.into_iter().map(ArcStr::from).collect()),
            _ => bail!("{} must be an array of strings", self.key),
        }
    }
}

fn items(text: &str) -> Result<Vec<Item>> {
    let mut out = Vec::new();
    for (n, raw) in text.lines().enumerate() {
        let line = strip_comment(raw).trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            bail!("line {}: expected key = value", n + 1);
        };
        let key = key.trim();
        if key.is_empty() || key.chars().any(|c| !(c.is_ascii_alphanumeric() || c == '_'))
        {
            bail!("line {}: bad key", n + 1);
        }
        out.push(Item { key: key.to_string(), value: parse_value(value.trim(), n + 1)? });
    }
    Ok(out)
}

fn strip_comment(line: &str) -> &str {
    let mut in_str = false;
    let mut escaped = false;
    for (i, c) in line.char_indices() {
        if in_str {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_str = false;
            }
        } else if c == '"' {
            in_str = true;
        } else if c == '#' {
            return &line[..i];
        }
    }
    line
}

fn parse_value(s: &str, line: usize) -> Result<Value> {
    if let Some(rest) = s.strip_prefix('"') {
        let (text, rest) = unescape(rest, line)?;
        if !rest.trim().is_empty() {
            bail!("line {line}: trailing junk");
        }
        return Ok(Value::String(text));
    }
    if let Some(rest) = s.strip_prefix('[') {
        return Ok(Value::Strings(parse_array(rest, line)?));
    }
    if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
        return Ok(Value::Int(s.parse().with_context(|| format_compact!("line {line}"))?));
    }
    bail!("line {line}: expected a string, integer, or array")
}

fn parse_array(s: &str, line: usize) -> Result<Vec<String>> {
    let mut rest = s.trim();
    let mut out = Vec::new();
    if let Some(r) = rest.strip_prefix(']') {
        if !r.trim().is_empty() {
            bail!("line {line}: trailing junk");
        }
        return Ok(out);
    }
    loop {
        rest = rest.trim_start();
        let Some(body) = rest.strip_prefix('"') else {
            bail!("line {line}: expected a string in the array");
        };
        let (text, after) = unescape(body, line)?;
        out.push(text);
        rest = after.trim_start();
        if let Some(r) = rest.strip_prefix(',') {
            rest = r;
            continue;
        }
        if let Some(r) = rest.strip_prefix(']') {
            if !r.trim().is_empty() {
                bail!("line {line}: trailing junk");
            }
            return Ok(out);
        }
        bail!("line {line}: expected ',' or ']'");
    }
}

fn unescape(s: &str, line: usize) -> Result<(String, &str)> {
    let mut out = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Ok((out, &s[i + c.len_utf8()..])),
            '\\' => {
                let Some((_, n)) = chars.next() else {
                    bail!("line {line}: truncated escape");
                };
                match n {
                    '\\' => out.push('\\'),
                    '"' => out.push('"'),
                    'n' => out.push('\n'),
                    other => bail!("line {line}: unknown escape \\{other}"),
                }
            }
            other => out.push(other),
        }
    }
    bail!("line {line}: unterminated string")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parses_user_and_local() {
        let user = parse_user("base = \"/farm/rustc\"\nslots = 4\n# comment\nname = \"box\"\n").unwrap();
        assert_eq!(user.base.unwrap().as_str(), "/farm/rustc");
        assert_eq!(user.slots, Some(4));
        assert_eq!(user.name.unwrap().as_str(), "box");
        let local = parse_local("local = [\"foo\", \"bar-baz\"]\nbase = \"/ignored\"\n").unwrap();
        assert_eq!(local.len(), 2);
        assert!(names_match(&local[1], "bar_baz"));
    }
}
