//! Starting a job that links before the rlibs below it have arrived. Until
//! it links, rustc reads only each rlib's metadata, so a stand-in archive
//! holding just that takes the rlib's place, and a stand-in linker, first
//! on `PATH`, waits at a gate until the real rlibs are in.

use anyhow::{Context, Result};
use compact_str::format_compact;
use std::{
    fs::{self, File},
    io::{self, Write},
    os::{
        fd::{OwnedFd, RawFd},
        unix::{fs::PermissionsExt, net::UnixStream as StdUnixStream},
    },
    path::Path,
    time::Instant,
};
use tokio::{io::AsyncWriteExt, net::UnixStream};

/// Where the sandbox finds the stand-in linker.
pub const BIN: &str = "/run/rustc-dist/bin";
/// The fd the sandbox inherits the gate on.
const GATE_FD: RawFd = 9;

/// Says it is waiting, waits for the gate to open, then runs the next
/// program of its own name on `PATH`.
const STAND_IN: &str = r#"#!/bin/sh
printf . >&9 && read -r _ <&9
exec 9<&-
set -f
IFS=:
path=
for d in $PATH; do
    [ "$d" = /run/rustc-dist/bin ] || path="$path${path:+:}$d"
done
unset IFS
PATH=$path
export PATH
exec "${0##*/}" "$@"
"#;

/// Write the stand-in linker to `path`.
pub fn write_stand_in_linker(path: &Path) -> Result<()> {
    fs::write(path, STAND_IN).with_context(|| format_compact!("writing {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o555))?;
    Ok(())
}

/// Write an rlib holding only `metadata`, an `.rmeta` file: what rustc
/// reads of an rlib until it links.
pub fn write_stand_in_rlib(metadata: &Path, dest: &Path) -> Result<()> {
    let mut src = File::open(metadata).with_context(|| format_compact!("opening {}", metadata.display()))?;
    let len = src.metadata()?.len();
    let mut out = File::create(dest).with_context(|| format_compact!("creating {}", dest.display()))?;
    write!(out, "!<arch>\n{:<16}{:<12}{:<6}{:<6}{:<8}{:<10}`\n", "lib.rmeta/", 0, 0, 0, 644, len)?;
    io::copy(&mut src, &mut out)?;
    if len % 2 == 1 {
        out.write_all(b"\n")?;
    }
    Ok(())
}

/// Holds the stand-in linker until every rlib is in place. The sandbox
/// inherits the other end of a socket pair as fd 9. A stand-in writes a
/// byte when it starts waiting and reads until this end shuts down.
pub struct Gate {
    ours: UnixStream,
    asked: Option<Instant>,
    hung_up: bool,
    opened: Option<Instant>,
}

impl Gate {
    /// The gate, and the sandbox's end, to pass to `inherit` in the
    /// sandbox's process and close here once it has started, so the gate
    /// sees the sandbox hang up.
    pub fn new() -> Result<(Self, OwnedFd)> {
        let (ours, theirs) = StdUnixStream::pair().context("creating the link gate")?;
        ours.set_nonblocking(true)?;
        let ours = UnixStream::from_std(ours)?;
        Ok((Self { ours, asked: None, hung_up: false, opened: None }, theirs.into()))
    }

    /// Between fork and exec: make `fd` the sandbox's fd 9.
    pub fn inherit(fd: RawFd) -> io::Result<()> {
        let ok = if fd == GATE_FD {
            unsafe { libc::fcntl(fd, libc::F_SETFD, 0) }
        } else {
            unsafe { libc::dup2(fd, GATE_FD) }
        };
        if ok < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    /// Whether no stand-in has asked yet and one still could.
    pub fn listening(&self) -> bool {
        self.asked.is_none() && !self.hung_up
    }

    /// Wait for a stand-in to start waiting, or for every process holding
    /// the sandbox's end to exit.
    pub async fn ask(&mut self) -> Result<()> {
        let mut buf = [0u8; 64];
        loop {
            self.ours.readable().await?;
            match self.ours.try_read(&mut buf) {
                Ok(0) => self.hung_up = true,
                Ok(_) => self.asked = Some(Instant::now()),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
            return Ok(());
        }
    }

    pub async fn open(&mut self) -> Result<()> {
        self.ours.shutdown().await?;
        self.opened = Some(Instant::now());
        Ok(())
    }

    /// How long a stand-in waited for the gate to open.
    pub fn waited_us(&self) -> u64 {
        let Some(asked) = self.asked else { return 0 };
        let until = self.opened.unwrap_or_else(Instant::now);
        until.saturating_duration_since(asked).as_micros() as u64
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{
        os::{fd::AsRawFd, unix::process::CommandExt},
        process::{Command, Stdio},
        time::Duration,
    };
    use tokio::time;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rustc-dist-link-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn the_stand_in_linker_waits_for_the_gate() -> Result<()> {
        let dir = scratch("gate");
        // Named for the program it stands in for, outside PATH.
        let echo = dir.join("echo");
        write_stand_in_linker(&echo)?;
        let (mut gate, theirs) = Gate::new()?;
        let fd = theirs.as_raw_fd();
        let mut cmd = Command::new(&echo);
        cmd.arg("linked").stdout(Stdio::piped());
        unsafe {
            cmd.pre_exec(move || Gate::inherit(fd));
        }
        let mut child = cmd.spawn()?;
        drop(theirs);
        time::timeout(Duration::from_secs(5), gate.ask()).await??;
        assert!(!gate.listening());
        time::sleep(Duration::from_millis(100)).await;
        assert!(child.try_wait()?.is_none(), "the linker ran before the gate opened");
        gate.open().await?;
        let out = child.wait_with_output()?;
        assert!(out.status.success());
        assert_eq!(out.stdout, b"linked\n");
        assert!(gate.waited_us() >= 100_000, "{}", gate.waited_us());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn an_open_gate_lets_the_linker_straight_through() -> Result<()> {
        let dir = scratch("open");
        let echo = dir.join("echo");
        write_stand_in_linker(&echo)?;
        let (mut gate, theirs) = Gate::new()?;
        gate.open().await?;
        let fd = theirs.as_raw_fd();
        let mut cmd = Command::new(&echo);
        cmd.arg("linked").stdout(Stdio::piped());
        unsafe {
            cmd.pre_exec(move || Gate::inherit(fd));
        }
        let out = time::timeout(Duration::from_secs(5), async { cmd.output() }).await??;
        assert_eq!(out.stdout, b"linked\n");
        fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_gate_hears_the_sandbox_hang_up_without_asking() -> Result<()> {
        let (mut gate, theirs) = Gate::new()?;
        let fd = theirs.as_raw_fd();
        let mut cmd = Command::new("true");
        unsafe {
            cmd.pre_exec(move || Gate::inherit(fd));
        }
        let mut child = cmd.spawn()?;
        drop(theirs);
        child.wait()?;
        time::timeout(Duration::from_secs(5), gate.ask()).await.context("the gate never heard the hang-up")??;
        assert!(!gate.listening(), "no stand-in is left to ask");
        assert_eq!(gate.waited_us(), 0);
        Ok(())
    }

    /// rustc reads a dependency's metadata from the stand-in, and a binary
    /// links once the real rlib replaces it.
    #[test]
    fn rustc_builds_against_a_stand_in_rlib() -> Result<()> {
        let dir = scratch("rlib");
        fs::write(dir.join("dep.rs"), "#[inline(never)]\npub fn f() -> u32 { 7 }\n")?;
        fs::write(dir.join("main.rs"), "fn main() { println!(\"{}\", dep::f()); }\n")?;
        // With rustc's diagnostics kept out of the test output.
        let rustc = |args: &[&str]| -> Result<(bool, String)> {
            let out = Command::new("rustc").current_dir(&dir).args(args).output()?;
            Ok((out.status.success(), String::from_utf8_lossy(&out.stderr).into_owned()))
        };
        assert!(rustc(&["--crate-name", "dep", "--crate-type", "lib", "--emit=metadata,link", "dep.rs"])?.0);
        fs::rename(dir.join("libdep.rlib"), dir.join("real.rlib"))?;
        write_stand_in_rlib(&dir.join("libdep.rmeta"), &dir.join("libdep.rlib"))?;
        fs::remove_file(dir.join("libdep.rmeta"))?;
        let main = ["--crate-name", "main", "--extern", "dep=libdep.rlib", "main.rs"];
        assert!(rustc(&[&main[..], &["--emit=metadata"]].concat())?.0, "metadata from the stand-in");
        let (linked, stderr) = rustc(&main)?;
        assert!(!linked && stderr.contains("undefined symbol"), "the stand-in has no code to link: {stderr}");
        fs::rename(dir.join("real.rlib"), dir.join("libdep.rlib"))?;
        assert!(rustc(&main)?.0);
        assert_eq!(Command::new(dir.join("main")).output()?.stdout, b"7\n");
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
