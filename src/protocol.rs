use anyhow::{Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::{BufMut, BytesMut};
use netidx_core::pack::Pack;
use netidx_derive::Pack;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Sent as four raw bytes ahead of the first message, so a daemon can
/// recognize a client from another version whatever its encoding.
pub const PROTOCOL: u32 = 3;
const MAX_MSG: usize = 1 << 20;

/// What the scheduler knows a compile by. `exact` is one cargo unit;
/// `coarse` is shared by every unit of that crate at the same opt-level.
#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub struct Unit {
    pub exact: ArcStr,
    pub coarse: ArcStr,
}

#[derive(Debug, Pack)]
pub enum Ask {
    /// `rustc -vV`, `--print`, stdin input. Runs at once without a slot.
    Probe,
    Compile(Unit),
    Status,
    Shutdown,
    /// The laptop switched power profile. The daemon calibrates a profile
    /// the first time it sees it, or again if asked.
    Profile { name: ArcStr, recalibrate: bool },
}

#[derive(Debug, Pack)]
pub enum Reply {
    Trivial,
    Slot { cpus: Vec<u32>, jobserver: ArcStr },
    Status(Status),
    /// This daemon is going away. Reconnect.
    Restarting,
    Done,
    Refused(ArcStr),
}

/// Sent by the wrapper after rustc succeeds. A failed or killed rustc
/// sends nothing, so its time is not recorded.
#[derive(Debug, Pack)]
pub struct Ran {
    pub cpu_us: u64,
    pub wall_us: u64,
    pub maxrss_kb: u64,
}

#[derive(Debug, Pack)]
pub struct Status {
    pub profile: ArcStr,
    pub switching: Switching,
    pub classes: Vec<ClassStatus>,
    /// Jobs still running under an earlier profile's layout.
    pub finishing: u32,
    pub waiting: u32,
    pub mem_limit_kb: u64,
    pub mem_running_kb: u64,
    pub units: Vec<UnitStatus>,
}

/// A switch to a profile with no calibration yet. New jobs wait until it
/// is done.
#[derive(Debug, Pack)]
pub enum Switching {
    No,
    /// Waiting for running jobs to finish.
    Draining(ArcStr),
    Calibrating(ArcStr),
}

#[derive(Debug, Pack)]
pub struct ClassStatus {
    pub calib_us: u64,
    pub cpus: u32,
    pub running: u32,
}

#[derive(Debug, Pack)]
pub struct UnitStatus {
    pub key: ArcStr,
    pub runs: u64,
    pub cost_mcu: u64,
    pub wall_ms: u64,
    pub mem_kb: u64,
    pub class: u32,
}

pub fn frame<T: Pack>(buf: &mut BytesMut, v: &T) -> Result<()> {
    let len = v.encoded_len();
    if len > MAX_MSG {
        bail!("message is {len} bytes");
    }
    buf.clear();
    buf.reserve(4 + len);
    buf.put_u32(len as u32);
    v.encode(buf).map_err(|e| anyhow!("{e}"))
}

fn unframe<T: Pack>(mut body: &[u8]) -> Result<T> {
    let v = T::decode(&mut body).map_err(|e| anyhow!("{e}"))?;
    if !body.is_empty() {
        bail!("trailing bytes after message");
    }
    Ok(v)
}

fn body_len(header: [u8; 4]) -> Result<usize> {
    let len = u32::from_be_bytes(header) as usize;
    if len > MAX_MSG {
        bail!("message is {len} bytes");
    }
    Ok(len)
}

pub fn send<T: Pack>(w: &mut impl Write, buf: &mut BytesMut, v: &T) -> Result<()> {
    frame(buf, v)?;
    Ok(w.write_all(buf)?)
}

pub fn recv<T: Pack>(r: &mut impl Read, buf: &mut BytesMut) -> Result<T> {
    let mut header = [0u8; 4];
    r.read_exact(&mut header)?;
    buf.clear();
    buf.resize(body_len(header)?, 0);
    r.read_exact(buf)?;
    unframe(buf)
}

pub async fn send_async<T: Pack>(
    w: &mut (impl AsyncWrite + Unpin),
    buf: &mut BytesMut,
    v: &T,
) -> Result<()> {
    frame(buf, v)?;
    Ok(w.write_all(buf).await?)
}

pub async fn recv_async<T: Pack>(
    r: &mut (impl AsyncRead + Unpin),
    buf: &mut BytesMut,
) -> Result<T> {
    let mut header = [0u8; 4];
    r.read_exact(&mut header).await?;
    buf.clear();
    buf.resize(body_len(header)?, 0);
    r.read_exact(buf).await?;
    unframe(buf)
}
