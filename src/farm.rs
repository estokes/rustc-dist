use crate::{protocol::PROTOCOL, sched::Msg, toolchain::Toolchain};
use anyhow::{Result, anyhow};
use arcstr::ArcStr;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, channel::mpsc as fmpsc};
use log::{info, warn};
use netidx::{
    path::Path,
    publisher::{Publisher, Val, Value},
    resolver_client::ChangeTracker,
    subscriber::{Dval, Event, SubId, Subscriber, UpdateChan, UpdatesFlags},
};
use netidx_core::pack::Pack;
use netidx_derive::Pack;
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time,
};

/// How often a changed load is republished, so a busy build doesn't send
/// an update per job.
const PUBLISH_EVERY: Duration = Duration::from_millis(100);
/// How often to look for boxes joining or leaving.
const LIST_EVERY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Pack)]
pub struct ClassState {
    /// Instructions per CPU-µs.
    pub rate: u64,
    pub cpus: u32,
    /// Jobserver tokens an offer to this class would find now: none while
    /// the box switches profile or its own jobs wait.
    pub free: u32,
}

/// What a box publishes at `BASE/NAME/state`.
#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub struct BoxState {
    pub protocol: u32,
    pub profile: ArcStr,
    /// Counts the states the box has published since it started. Its
    /// answer to an offer names the first version that reflects it.
    pub version: u64,
    pub toolchains: Vec<Toolchain>,
    pub classes: Vec<ClassState>,
}

/// The part of a box's state its scheduler changes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Load {
    pub profile: ArcStr,
    pub version: u64,
    pub classes: Vec<ClassState>,
}

/// Where a box takes remote jobs.
pub fn jobs_path(base: &Path, name: &str) -> Path {
    base.append(name).append("jobs")
}

fn pack(state: &BoxState) -> Result<Value> {
    let mut buf = BytesMut::with_capacity(state.encoded_len());
    state.encode(&mut buf).map_err(|e| anyhow!("{e}"))?;
    Ok(Value::from(buf.freeze()))
}

fn unpack(v: Value) -> Result<BoxState> {
    let mut bytes = v.cast_to::<Bytes>()?;
    BoxState::decode(&mut bytes).map_err(|e| anyhow!("{e}"))
}

pub struct Member {
    pub publisher: Publisher,
    pub subscriber: Subscriber,
    pub base: Path,
    pub name: ArcStr,
    pub toolchains: Vec<Toolchain>,
}

/// Publish this box's state and tell the scheduler about every other
/// box's.
pub async fn run(m: Member, mut load: watch::Receiver<Load>, tx: mpsc::UnboundedSender<Msg>) -> Result<()> {
    let state = |load: &Load| BoxState {
        protocol: PROTOCOL,
        profile: load.profile.clone(),
        version: load.version,
        toolchains: m.toolchains.clone(),
        classes: load.classes.clone(),
    };
    let val: Val = m.publisher.publish(m.base.append(&m.name).append("state"), pack(&state(&load.borrow_and_update()))?)?;
    info!("publishing {}", m.base.append(&m.name));
    let (updates_tx, mut updates): (UpdateChan, _) = fmpsc::channel(64);
    let mut boxes: HashMap<ArcStr, Dval> = HashMap::new();
    let mut names: HashMap<SubId, ArcStr> = HashMap::new();
    let mut tracker = ChangeTracker::new(m.base.clone());
    let mut list = time::interval(LIST_EVERY);
    let mut publish = time::interval(PUBLISH_EVERY);
    let mut changed = false;
    loop {
        tokio::select! {
            r = load.changed() => {
                r?;
                changed = true;
            }
            _ = publish.tick(), if changed => {
                changed = false;
                let mut batch = m.publisher.start_batch();
                val.update(&mut batch, pack(&state(&load.borrow_and_update()))?);
                batch.commit(None).await;
            }
            batch = updates.next() => {
                let Some(mut batch) = batch else { break };
                for (id, ev) in batch.drain(..) {
                    let Some(name) = names.get(&id) else { continue };
                    let state = match ev {
                        Event::Update(v) => match unpack(v) {
                            Ok(s) => Some((jobs_path(&m.base, name), s)),
                            Err(e) => {
                                warn!("state of {name}: {e:#}");
                                None
                            }
                        },
                        Event::Unsubscribed => None,
                    };
                    let _ = tx.send(Msg::Farm { name: name.clone(), state });
                }
            }
            _ = list.tick() => {
                if !m.subscriber.resolver().check_changed(&mut tracker).await? {
                    continue;
                }
                let listed = m.subscriber.resolver().list(m.base.clone()).await?;
                let present: Vec<ArcStr> = listed
                    .iter()
                    .filter_map(|p| Path::basename(p).map(ArcStr::from))
                    .filter(|n| *n != m.name)
                    .collect();
                for name in &present {
                    if !boxes.contains_key(name) {
                        info!("box {name} joined");
                        let dval = m.subscriber.subscribe(m.base.append(name).append("state"));
                        dval.updates(UpdatesFlags::BEGIN_WITH_LAST, updates_tx.clone());
                        names.insert(dval.id(), name.clone());
                        boxes.insert(name.clone(), dval);
                    }
                }
                boxes.retain(|name, dval| {
                    let keep = present.contains(name);
                    if !keep {
                        info!("box {name} left");
                        names.remove(&dval.id());
                        let _ = tx.send(Msg::Farm { name: name.clone(), state: None });
                    }
                    keep
                });
            }
        }
    }
    Ok(())
}
