use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use netidx::publisher::Value;
use netidx_core::pack::Pack;
use netidx_derive::Pack;
use std::io::Write;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL: i64 = 1;
pub const BUSY: &str = "busy";
/// Replaced with the server job directory before `rustc` runs.
pub const JOB: &str = "__JOB__";
/// Staged files live at `{job}/a{absolute client path}`.
pub const MIRROR: &str = "a";

pub fn mirror_rel(abs: &str) -> ArcStr {
    ArcStr::from(format!("{MIRROR}{abs}"))
}

pub fn mirror_token(abs: &str) -> ArcStr {
    ArcStr::from(format!("{JOB}/{MIRROR}{abs}"))
}

#[derive(Debug, Clone, Pack)]
pub struct Job {
    pub argv: Vec<ArcStr>,
    pub env: Vec<(ArcStr, ArcStr)>,
    pub cwd: ArcStr,
}

#[derive(Debug, Pack)]
pub enum ToWrapper {
    RunLocal,
    Stdout(Bytes),
    Stderr(Bytes),
    Exit(i32),
    Fail(ArcStr),
}

#[derive(Debug, Pack)]
pub enum ToServer {
    Header { argv: Vec<ArcStr>, env: Vec<(ArcStr, ArcStr)>, dirs: Vec<ArcStr> },
    File { rel: ArcStr, bytes: Bytes },
    Go,
}

#[derive(Debug, Pack)]
pub enum FromServer {
    Root(ArcStr),
    Stdout(Bytes),
    Stderr(Bytes),
    Output { rel: ArcStr, bytes: Bytes },
    Exit(i32),
    Fail(ArcStr),
}

pub fn pack_value<T: Pack>(v: &T) -> Result<Value> {
    let mut buf = BytesMut::with_capacity(v.encoded_len());
    v.encode(&mut buf).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Value::from(buf.freeze()))
}

pub fn unpack_value<T: Pack>(v: Value) -> Result<T> {
    let bytes = v.cast_to::<Bytes>().context("expected bytes")?;
    let mut cur = bytes;
    let decoded = T::decode(&mut cur).map_err(|e| anyhow::anyhow!("{e}"))?;
    if cur.has_remaining() {
        bail!("trailing bytes");
    }
    Ok(decoded)
}

pub fn error_text(v: &Value) -> Option<ArcStr> {
    match v {
        Value::Error(e) => match &**e.as_arc() {
            Value::String(s) => Some(s.clone()),
            other => Some(ArcStr::from(other.to_string())),
        },
        _ => None,
    }
}

pub async fn write_msg<T: Pack, W: AsyncWrite + Unpin>(w: &mut W, v: &T) -> Result<()> {
    let len = v.encoded_len();
    if len > u32::MAX as usize {
        bail!("message too large");
    }
    let mut buf = BytesMut::with_capacity(4 + len);
    buf.put_u32(len as u32);
    v.encode(&mut buf).map_err(|e| anyhow::anyhow!("{e}"))?;
    w.write_all(&buf).await?;
    Ok(())
}

pub async fn read_msg<T: Pack>(r: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    let mut lenb = [0u8; 4];
    r.read_exact(&mut lenb).await?;
    let len = u32::from_be_bytes(lenb) as usize;
    if len > 64 * 1024 * 1024 {
        bail!("socket message is {len} bytes");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    let mut cur = &buf[..];
    let decoded = T::decode(&mut cur).map_err(|e| anyhow::anyhow!("{e}"))?;
    if cur.has_remaining() {
        bail!("trailing bytes");
    }
    Ok(decoded)
}

/// Rewrites `{job}/a/...` back to the client absolute path. Lines are
/// rewritten whole so a path split across chunks is not missed.
pub struct Rewriter {
    from: String,
    pending: Vec<u8>,
}

impl Rewriter {
    pub fn new(job_root: &str) -> Self {
        Self { from: format!("{job_root}/{MIRROR}"), pending: Vec::new() }
    }

    pub fn prefix(&self) -> &str {
        &self.from
    }

    pub fn push(&mut self, chunk: &[u8]) -> Bytes {
        self.pending.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(i) = self.pending.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=i).collect();
            rewrite_into(&self.from, &line, &mut out);
        }
        Bytes::from(out)
    }

    pub fn finish(&mut self) -> Bytes {
        if self.pending.is_empty() {
            return Bytes::new();
        }
        let line = std::mem::take(&mut self.pending);
        let mut out = Vec::new();
        rewrite_into(&self.from, &line, &mut out);
        Bytes::from(out)
    }
}

pub fn rewrite_bytes(from: &str, data: &[u8]) -> Bytes {
    let mut out = Vec::new();
    rewrite_into(from, data, &mut out);
    Bytes::from(out)
}

fn rewrite_into(from: &str, data: &[u8], out: &mut Vec<u8>) {
    match std::str::from_utf8(data) {
        Ok(s) if s.contains(from) => out.extend_from_slice(s.replace(from, "").as_bytes()),
        _ => out.extend_from_slice(data),
    }
}

pub fn safe_rel(rel: &str) -> bool {
    !rel.is_empty()
        && !rel.starts_with('/')
        && !rel.contains('\0')
        && rel.split('/').all(|p| !p.is_empty() && p != "." && p != "..")
}

pub fn client_path(rel: &str) -> Result<std::path::PathBuf> {
    if !safe_rel(rel) {
        bail!("bad output path {rel}");
    }
    let Some(rest) = rel.strip_prefix(MIRROR) else {
        bail!("output {rel} is outside the staged tree");
    };
    if !rest.starts_with('/') {
        bail!("output {rel} is outside the staged tree");
    }
    Ok(std::path::PathBuf::from(rest))
}

pub fn expand_job(s: &str, root: &str) -> String {
    if s.contains(JOB) { s.replace(JOB, root) } else { s.to_string() }
}

/// Write `bytes` to a std writer, used by the wrapper for compiler output.
pub fn write_stdio(which: bool, bytes: &[u8]) -> Result<()> {
    if which {
        std::io::stdout().write_all(bytes)?;
        std::io::stdout().flush()?;
    } else {
        std::io::stderr().write_all(bytes)?;
        std::io::stderr().flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rewrites_split_lines() {
        let mut rw = Rewriter::new("/tmp/job");
        let a = rw.push(b"error: /tmp/job/a/home/eric/f");
        assert!(a.is_empty());
        let b = rw.push(b"oo.rs:1\nnext\n");
        let text = String::from_utf8(b.to_vec()).unwrap();
        assert_eq!(text, "error: /home/eric/foo.rs:1\nnext\n");
        assert!(rw.finish().is_empty());
    }

    #[test]
    fn rejects_escaping_rels() {
        assert!(safe_rel("a/home/eric/foo.rs"));
        assert!(!safe_rel("a/../etc/passwd"));
        assert!(!safe_rel("/etc/passwd"));
        assert!(!safe_rel("a//x"));
    }
}
