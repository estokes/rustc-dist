use anyhow::{Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::{BufMut, Bytes, BytesMut};
use netidx_core::pack::Pack;
use netidx_derive::Pack;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Sent as four raw bytes ahead of the first message, so a daemon can
/// recognize a client from another version whatever its encoding.
pub const PROTOCOL: u32 = 11;
const MAX_MSG: usize = 1 << 24;

/// What the scheduler knows a compile by. Job history is kept under
/// `exact`, one cargo unit, and `coarse`, shared by every unit of that
/// crate at the same opt-level. `id` names the job in the build graph and
/// `deps` are the jobs whose outputs it reads. Every target directory that
/// builds the same crate the same way shares its `id`; `dir` is where this
/// one writes its crate files (`--out-dir`).
#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub struct Unit {
    pub exact: ArcStr,
    pub coarse: ArcStr,
    pub id: ArcStr,
    pub dir: Option<ArcStr>,
    pub deps: Vec<Dep>,
    /// The most threads its codegen can use: `-C codegen-units`, or
    /// rustc's default.
    pub codegen_units: u32,
}

/// A job this one reads the output of. `file` is the crate file named on
/// the command line; a build script's output directory has none.
#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub struct Dep {
    pub id: ArcStr,
    pub file: Option<ArcStr>,
}

/// What another box needs to run a compile: rustc's arguments without the
/// compiler path, the environment cargo gave it, its directory, and the
/// paths in its arguments that say what it reads and writes.
#[derive(Debug, Clone, Pack)]
pub struct Job {
    pub rustc: ArcStr,
    pub args: Vec<ArcStr>,
    pub env: Vec<(ArcStr, ArcStr)>,
    pub cwd: ArcStr,
    pub target: Option<ArcStr>,
    pub paths: JobPaths,
    pub reads: Reads,
}

/// What a job reads of the crates below it.
#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub enum Reads {
    /// Their metadata: the job builds a library.
    Metadata,
    /// Their metadata, and their rlibs once rustc runs `linker`, a program
    /// it finds on `PATH`.
    RlibsToLink { linker: ArcStr },
    /// Their rlibs from the start: LTO, static libraries, dylibs, or a
    /// linker named by its path.
    Rlibs,
}

/// Paths as they appear in the arguments, possibly relative to `cwd`.
#[derive(Debug, Clone, Default, Pack)]
pub struct JobPaths {
    pub out_dir: ArcStr,
    /// The dep-info this unit wrote last time, listing the files it read.
    pub dep_info: ArcStr,
    pub sources: Vec<ArcStr>,
    /// `-L` directories other than dependency ones: native libraries.
    pub search: Vec<ArcStr>,
    /// Outputs named explicitly by `--emit kind=path` or `-o`.
    pub outputs: Vec<ArcStr>,
}

#[derive(Debug, Pack)]
#[allow(clippy::large_enum_variant)]
pub enum Ask {
    /// `rustc -vV`, `--print`, stdin input. Runs at once without a slot.
    Probe,
    /// `build` is the pid of the cargo that ran the wrapper. `job` is
    /// present when the compile may run on another box.
    Compile { unit: Unit, build: u32, job: Option<Job> },
    Status,
    Shutdown,
    /// The laptop switched power profile. The daemon calibrates a profile
    /// the first time it sees it, or again if asked.
    Profile { name: ArcStr, recalibrate: bool },
}

#[derive(Debug, Pack)]
pub enum Reply {
    Trivial,
    /// Run rustc here. `filter_metadata` drops rustc's rmeta notice,
    /// because a remote run that failed already passed one to cargo.
    Slot { cpus: Vec<u32>, jobserver: ArcStr, filter_metadata: bool },
    /// Output of rustc run on another box, for the wrapper to print.
    Output { stderr: bool, data: Bytes },
    /// rustc ran on another box and exited with this code.
    Exit(i32),
    Status(Status),
    /// This daemon is going away. Reconnect.
    Restarting,
    Done,
    Refused(ArcStr),
}

/// Sent by the wrapper after rustc succeeds. A failed or killed rustc
/// sends nothing, so its time is not recorded.
#[derive(Debug, Clone, Copy, Pack)]
pub struct Ran {
    pub wall_us: u64,
    pub maxrss_kb: u64,
    /// When rustc printed its `.rmeta` line, if it did.
    pub front: Option<Front>,
}

/// A compile up to its `.rmeta` line: the front end, which runs on one
/// thread.
#[derive(Debug, Clone, Copy, Pack)]
pub struct Front {
    pub wall_us: u64,
}

/// What the scheduler of the box a job ran on measured while it ran.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pack)]
pub struct Measured {
    /// The jobserver tokens it held, times µs.
    pub tokens_us: u64,
    /// Runnable tasks per CPU on that box, in hundredths, on average.
    pub load_c: u32,
}

fn contains(line: &[u8], pat: &[u8]) -> bool {
    line.windows(pat.len()).any(|w| w == pat)
}

/// Whether a line of rustc's JSON stderr announces a written file.
pub fn is_artifact_line(line: &[u8]) -> bool {
    contains(line, br#""$message_type":"artifact""#)
}

/// Whether a line of rustc's JSON stderr announces its `.rmeta`.
pub fn is_metadata_line(line: &[u8]) -> bool {
    is_artifact_line(line) && contains(line, br#""emit":"metadata""#)
}

/// What `status` shows. Speeds are in hundredths of this box's fastest
/// class's, idle: what it calibrated at, over what it has learned it runs
/// at with no load. Work and times are in ms.
#[derive(Debug, Pack)]
pub struct Status {
    pub profile: ArcStr,
    pub switching: Switching,
    /// Runnable tasks per CPU, in hundredths.
    pub load_c: u32,
    /// Light jobs, pinned to one class each.
    pub classes: Vec<ClassStatus>,
    pub spread: SpreadStatus,
    /// Jobs still running under an earlier profile's layout.
    pub finishing: u32,
    pub waiting: u32,
    /// RAM plus the allowed share of free compressed swap.
    pub mem_capacity_kb: u64,
    /// What RAM holds, less shared pages swap on disk has room for.
    pub mem_used_kb: u64,
    /// What running jobs are predicted to add.
    pub mem_growth_kb: u64,
    pub graph_nodes: u32,
    pub units: Vec<UnitStatus>,
    pub boxes: Vec<BoxStatus>,
}

/// Another box as this daemon sees it.
#[derive(Debug, Pack)]
pub struct BoxStatus {
    pub name: ArcStr,
    pub profile: ArcStr,
    /// Runnable tasks per CPU there, in hundredths, as it last published.
    pub load_c: u32,
    /// Speed now of each class, for a light job.
    pub classes: Vec<BoxClass>,
    /// A heavy job's threads there, and each one's speed now.
    pub threads: u32,
    pub thread_speed_c: u32,
    /// What a job sent there spends moving files, on average.
    pub copy_ms: Option<u64>,
    /// Jobs this box has running there.
    pub sent: u32,
    /// It speaks this protocol version.
    pub compatible: bool,
}

#[derive(Debug, Pack)]
pub struct BoxClass {
    pub cpus: u32,
    pub speed_c: u32,
    /// Tokens it publishes free.
    pub free: u32,
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
    pub cpus: u32,
    /// Its speed for a light job with no load, and at the load now.
    pub idle_speed_c: u32,
    pub speed_c: u32,
    /// Jobs of this box's builds.
    pub running: u32,
    /// Other boxes' jobs.
    pub serving: u32,
    /// Tokens lent to heavy jobs.
    pub lent: u32,
    /// Tokens no job holds.
    pub free: u32,
}

/// Heavy jobs, which run on every CPU: this box's and other boxes'.
#[derive(Debug, Pack)]
pub struct SpreadStatus {
    pub running: u32,
    pub serving: u32,
    /// The threads a heavy job gets, and each one's speed now.
    pub threads: u32,
    pub thread_speed_c: u32,
}

#[derive(Debug, Pack)]
pub struct UnitStatus {
    pub key: ArcStr,
    /// Runs for this box's builds, here and on other boxes.
    pub here: u64,
    pub sent: u64,
    /// Runs here for other boxes' builds.
    pub served: u64,
    /// Its work on one thread of this box's fastest class, idle, and its
    /// front end's, if it printed an `.rmeta` line.
    pub work_ms: u64,
    pub front_ms: Option<u64>,
    /// Hundredths of the jobserver tokens held after the front end.
    pub width_c: u64,
    /// Predicted wall time here now: on the fastest class for a light job,
    /// on every CPU for a heavy one.
    pub now_ms: u64,
    /// Median measured wall time.
    pub wall_ms: u64,
    pub mem_kb: u64,
    /// Medians of sent runs: time spent moving files rather than
    /// compiling, and inputs sent.
    pub copy_ms: Option<u64>,
    pub sent_kb: Option<u64>,
    /// Its work plus the costliest chain of jobs that depend on it, if it
    /// is in this box's build graph.
    pub rank_ms: Option<u64>,
}

pub fn gib(kb: u64) -> f64 {
    kb as f64 / (1024. * 1024.)
}

/// Hundredths as a number.
pub fn hundredths(c: u32) -> f64 {
    c as f64 / 100.
}

fn secs(ms: u64) -> f64 {
    ms as f64 / 1000.
}

/// The figures `status` and the netidx browser show.
impl UnitStatus {
    pub fn rank_s(&self) -> Option<f64> {
        self.rank_ms.map(secs)
    }

    pub fn work_s(&self) -> f64 {
        secs(self.work_ms)
    }

    /// The single-threaded front end's share of its work, in percent.
    pub fn front_pct(&self) -> Option<u64> {
        self.front_ms.map(|f| f * 100 / self.work_ms.max(1))
    }

    /// Tokens held after the front end.
    pub fn width(&self) -> f64 {
        self.width_c as f64 / 100.
    }

    pub fn now_s(&self) -> f64 {
        secs(self.now_ms)
    }

    pub fn wall_s(&self) -> f64 {
        secs(self.wall_ms)
    }

    pub fn mem_mb(&self) -> u64 {
        self.mem_kb / 1024
    }

    /// Medians of its sent runs: seconds spent on files, and MB sent.
    pub fn copy(&self) -> Option<(f64, f64)> {
        Some((secs(self.copy_ms?), self.sent_kb? as f64 / 1024.))
    }
}

impl BoxStatus {
    pub fn cpus(&self) -> u32 {
        self.classes.iter().map(|c| c.cpus).sum()
    }

    pub fn free(&self) -> u32 {
        self.classes.iter().map(|c| c.free).sum()
    }

    pub fn copy_s(&self) -> Option<f64> {
        self.copy_ms.map(secs)
    }
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
