use crate::{
    protocol::{
        self, FromServer, Rewriter, ToServer, BUSY, client_path, error_text, pack_value,
        unpack_value,
    },
    stage::Staged,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use bytes::Bytes;
use compact_str::format_compact;
use log::debug;
use netidx::{path::Path, subscriber::Subscriber};
use netidx_protocols::rpc::client::Proc;
use std::time::Duration;
use tokio::{fs, sync::mpsc as tok_mpsc, time::timeout};

pub enum Live {
    Stdout(Bytes),
    Stderr(Bytes),
}

pub enum Outcome {
    Busy,
    Done(i32),
}

pub async fn compile(
    subscriber: &Subscriber,
    rpc: Path,
    staged: &Staged,
    live: tok_mpsc::Sender<Live>,
) -> Result<Outcome> {
    let proc = Proc::new(subscriber, rpc.clone())?;
    let reply = timeout(Duration::from_secs(30), proc.call::<[(&str, netidx::publisher::Value); 0], &str>([]))
        .await
        .with_context(|| format_compact!("calling {rpc}"))??;
    if let Some(err) = error_text(&reply) {
        if err.as_str() == BUSY {
            return Ok(Outcome::Busy);
        }
        bail!("{rpc}: {err}");
    }
    let path = reply.cast_to::<ArcStr>().with_context(|| format_compact!("{rpc} reply"))?;
    debug!("job channel {path}");
    let con = timeout(
        Duration::from_secs(30),
        netidx_protocols::channel::client::Connection::connect(subscriber, Path::from(path)),
    )
    .await
    .context("connecting to job")??;

    send_job(&con, staged).await?;

    let mut stdout_rw: Option<Rewriter> = None;
    let mut stderr_rw: Option<Rewriter> = None;
    loop {
        let value = con.recv_one().await.context("job closed")?;
        match unpack_value::<FromServer>(value)? {
            FromServer::Root(root) => {
                stdout_rw = Some(Rewriter::new(&root));
                stderr_rw = Some(Rewriter::new(&root));
            }
            FromServer::Stdout(bytes) => {
                let rw = stdout_rw.as_mut().context("stdout before job root")?;
                let out = rw.push(&bytes);
                if !out.is_empty() {
                    live.send(Live::Stdout(out)).await.context("wrapper gone")?;
                }
            }
            FromServer::Stderr(bytes) => {
                let rw = stderr_rw.as_mut().context("stderr before job root")?;
                let out = rw.push(&bytes);
                if !out.is_empty() {
                    live.send(Live::Stderr(out)).await.context("wrapper gone")?;
                }
            }
            FromServer::Output { rel, bytes } => {
                let rw = stdout_rw.as_ref().context("output before job root")?;
                write_output(rw.prefix(), &rel, bytes).await?;
            }
            FromServer::Exit(code) => {
                if let Some(rw) = stdout_rw.as_mut() {
                    let out = rw.finish();
                    if !out.is_empty() {
                        let _ = live.send(Live::Stdout(out)).await;
                    }
                }
                if let Some(rw) = stderr_rw.as_mut() {
                    let out = rw.finish();
                    if !out.is_empty() {
                        let _ = live.send(Live::Stderr(out)).await;
                    }
                }
                return Ok(Outcome::Done(code));
            }
            FromServer::Fail(msg) => bail!("{msg}"),
        }
    }
}

async fn send_job(
    con: &netidx_protocols::channel::client::Connection,
    staged: &Staged,
) -> Result<()> {
    let header = ToServer::Header {
        argv: staged.argv.clone(),
        env: staged.env.clone(),
        dirs: staged.dirs.clone(),
    };
    con.send(pack_value(&header)?)?;
    con.flush().await?;
    for (rel, bytes) in &staged.files {
        if !protocol::safe_rel(rel) {
            bail!("refusing to send {rel}");
        }
        let file = ToServer::File { rel: rel.clone(), bytes: bytes.clone() };
        con.send(pack_value(&file)?)?;
        con.flush().await?;
    }
    con.send(pack_value(&ToServer::Go)?)?;
    con.flush().await?;
    Ok(())
}

async fn write_output(prefix: &str, rel: &str, bytes: Bytes) -> Result<()> {
    let dest = client_path(rel)?;
    let data = if rel.ends_with(".d") {
        protocol::rewrite_bytes(prefix, &bytes)
    } else {
        bytes
    };
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&dest, &data).await.with_context(|| format_compact!("writing {}", dest.display()))?;
    Ok(())
}
