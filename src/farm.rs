use crate::{identity::Identity, protocol::PROTOCOL};
use anyhow::Result;
use arcstr::ArcStr;
use futures::{StreamExt, channel::mpsc as fut_mpsc};
use log::{info, warn};
use netidx::{
    path::Path,
    resolver_client::ChangeTracker,
    subscriber::{Dval, Event, SubId, Subscriber, UpdateChan, UpdatesFlags, Value},
};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tokio::{sync::{mpsc, oneshot}, time};

#[derive(Clone)]
pub struct FarmHandle {
    pick: mpsc::UnboundedSender<PickReq>,
    pub subscriber: Subscriber,
}

pub struct Reservation {
    pub name: ArcStr,
    pub rpc: Path,
    done: mpsc::UnboundedSender<ArcStr>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = self.done.send(self.name.clone());
    }
}

struct PickReq {
    identity: Identity,
    target: ArcStr,
    skip: Vec<ArcStr>,
    reply: oneshot::Sender<Option<Reservation>>,
}

#[derive(Clone, Copy)]
enum Field {
    Protocol,
    Slots,
    Queue,
    Rustc,
    Targets,
}

struct Server {
    subs: Vec<Dval>,
    protocol: Option<i64>,
    slots: Option<i64>,
    queue: i64,
    release: Option<ArcStr>,
    commit: Option<ArcStr>,
    host: Option<ArcStr>,
    llvm: Option<ArcStr>,
    targets: Vec<ArcStr>,
    inflight: i64,
}

impl Server {
    fn ready(&self) -> bool {
        self.protocol == Some(PROTOCOL)
            && self.slots.is_some()
            && self.release.is_some()
            && self.commit.is_some()
            && self.host.is_some()
            && self.llvm.is_some()
    }

    fn matches(&self, id: &Identity) -> bool {
        self.release.as_ref() == Some(&id.release)
            && self.commit.as_ref() == Some(&id.commit)
            && self.host.as_ref() == Some(&id.host)
            && self.llvm.as_ref() == Some(&id.llvm)
    }

    fn has_target(&self, triple: &str) -> bool {
        self.targets.iter().any(|t| t.as_str() == triple)
    }

    fn free(&self) -> u64 {
        let slots = self.slots.unwrap_or(0).max(0) as u64;
        let used = self.queue.max(self.inflight).max(0) as u64;
        slots.saturating_sub(used)
    }
}

struct Watch {
    server: ArcStr,
    field: Field,
}

pub async fn run(subscriber: Subscriber, base: Path) -> Result<FarmHandle> {
    let (pick_tx, pick_rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = mpsc::unbounded_channel();
    let handle = FarmHandle { pick: pick_tx, subscriber: subscriber.clone() };
    tokio::spawn(async move {
        if let Err(e) = drive(subscriber, base, pick_rx, done_tx, done_rx).await {
            warn!("farm stopped: {e:#}");
        }
    });
    Ok(handle)
}

impl FarmHandle {
    pub async fn reserve(
        &self,
        identity: &Identity,
        target: &str,
        skip: &[ArcStr],
    ) -> Option<Reservation> {
        let (reply, rx) = oneshot::channel();
        self.pick
            .send(PickReq {
                identity: identity.clone(),
                target: ArcStr::from(target),
                skip: skip.to_vec(),
                reply,
            })
            .ok()?;
        rx.await.ok().flatten()
    }
}

async fn drive(
    subscriber: Subscriber,
    base: Path,
    mut picks: mpsc::UnboundedReceiver<PickReq>,
    done_tx: mpsc::UnboundedSender<ArcStr>,
    mut dones: mpsc::UnboundedReceiver<ArcStr>,
) -> Result<()> {
    let (tx, mut rx) = fut_mpsc::channel(64);
    let mut servers: HashMap<ArcStr, Server> = HashMap::new();
    let mut watches: HashMap<SubId, Watch> = HashMap::new();
    let mut tracker = ChangeTracker::new(base.clone());
    let mut tick = time::interval(Duration::from_secs(1));
    refresh(&subscriber, &base, &tx, &mut servers, &mut watches).await?;
    loop {
        tokio::select! {
            batch = rx.next() => {
                let Some(mut batch) = batch else { break };
                for (id, ev) in batch.drain(..) {
                    if let Event::Update(v) = ev {
                        apply(&mut servers, &watches, id, v);
                    }
                }
            }
            req = picks.recv() => {
                let Some(PickReq { identity, target, skip, reply }) = req else { break };
                let _ = reply.send(choose(&mut servers, &identity, &target, &skip, &done_tx));
            }
            name = dones.recv() => {
                let Some(name) = name else { break };
                if let Some(server) = servers.get_mut(&name) {
                    server.inflight = server.inflight.saturating_sub(1);
                }
            }
            _ = tick.tick() => {
                if subscriber.resolver().check_changed(&mut tracker).await? {
                    refresh(&subscriber, &base, &tx, &mut servers, &mut watches).await?;
                }
            }
        }
    }
    Ok(())
}

fn choose(
    servers: &mut HashMap<ArcStr, Server>,
    identity: &Identity,
    target: &ArcStr,
    skip: &[ArcStr],
    done: &mpsc::UnboundedSender<ArcStr>,
) -> Option<Reservation> {
    let mut best: Option<(ArcStr, u64)> = None;
    for (name, server) in servers.iter() {
        if skip.iter().any(|s| s == name) {
            continue;
        }
        if !server.ready() || !server.matches(identity) || !server.has_target(target)
        {
            continue;
        }
        let free = server.free();
        if free == 0 {
            continue;
        }
        let take = match &best {
            None => true,
            Some((best_name, best_free)) => free > *best_free || (free == *best_free && name < best_name),
        };
        if take {
            best = Some((name.clone(), free));
        }
    }
    let (name, _) = best?;
    let server = servers.get_mut(&name)?;
    server.inflight += 1;
    info!(
        "reserve {name} free {} inflight {}",
        server.free(),
        server.inflight
    );
    Some(Reservation {
        rpc: Path::from(format!("{name}/rpc/compile")),
        name,
        done: done.clone(),
    })
}

fn apply(servers: &mut HashMap<ArcStr, Server>, watches: &HashMap<SubId, Watch>, id: SubId, v: Value) {
    let Some(watch) = watches.get(&id) else { return };
    let Some(server) = servers.get_mut(&watch.server) else { return };
    match watch.field {
        Field::Protocol => server.protocol = v.clone().cast_to::<i64>().ok(),
        Field::Slots => server.slots = v.clone().cast_to::<i64>().ok(),
        Field::Queue => server.queue = v.clone().cast_to::<i64>().unwrap_or(server.queue),
        Field::Rustc => {
            if let Ok(text) = v.clone().cast_to::<ArcStr>() {
                if let Ok(id) = crate::identity::parse_vv(&text) {
                    server.release = Some(id.release);
                    server.commit = Some(id.commit);
                    server.host = Some(id.host);
                    server.llvm = Some(id.llvm);
                }
            }
        }
        Field::Targets => {
            if let Ok(text) = v.clone().cast_to::<ArcStr>() {
                server.targets = text
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(ArcStr::from)
                    .collect();
            }
        }
    }
}

async fn refresh(
    subscriber: &Subscriber,
    base: &Path,
    tx: &UpdateChan,
    servers: &mut HashMap<ArcStr, Server>,
    watches: &mut HashMap<SubId, Watch>,
) -> Result<()> {
    let listed = subscriber.resolver().list(base.clone()).await?;
    let mut live = HashSet::new();
    for path in listed.iter() {
        let name = ArcStr::from(&**path);
        live.insert(name.clone());
        if servers.contains_key(&name) {
            continue;
        }
        info!("server {path}");
        attach(subscriber, path.clone(), tx, servers, watches);
    }
    servers.retain(|name, server| {
        let keep = live.contains(name);
        if !keep {
            for sub in &server.subs {
                watches.remove(&sub.id());
            }
            info!("server {name} left");
        }
        keep
    });
    Ok(())
}

fn attach(
    subscriber: &Subscriber,
    path: Path,
    tx: &UpdateChan,
    servers: &mut HashMap<ArcStr, Server>,
    watches: &mut HashMap<SubId, Watch>,
) {
    let name = ArcStr::from(&*path);
    let mut subs = Vec::new();
    for (field, child) in [
        (Field::Protocol, "protocol"),
        (Field::Slots, "slots"),
        (Field::Queue, "queue"),
        (Field::Rustc, "rustc"),
        (Field::Targets, "targets"),
    ] {
        let dval = subscriber.subscribe(path.append(child));
        let id = dval.id();
        dval.updates(UpdatesFlags::BEGIN_WITH_LAST, tx.clone());
        watches.insert(id, Watch { server: name.clone(), field });
        subs.push(dval);
    }
    servers.insert(
        name,
        Server {
            subs,
            protocol: None,
            slots: None,
            queue: 0,
            release: None,
            commit: None,
            host: None,
            llvm: None,
            targets: Vec::new(),
            inflight: 0,
        },
    );
}

#[cfg(test)]
mod test {
    use super::*;

    fn server(slots: i64, queue: i64, inflight: i64) -> Server {
        Server {
            subs: Vec::new(),
            protocol: Some(PROTOCOL),
            slots: Some(slots),
            queue,
            release: Some(ArcStr::from("1")),
            commit: Some(ArcStr::from("abc")),
            host: Some(ArcStr::from("host")),
            llvm: Some(ArcStr::from("llvm")),
            targets: vec![ArcStr::from("host")],
            inflight,
        }
    }

    #[test]
    fn free_uses_the_higher_of_queue_and_inflight() {
        let s = server(8, 1, 4);
        assert_eq!(s.free(), 4);
        let s = server(8, 5, 1);
        assert_eq!(s.free(), 3);
        let s = server(8, 8, 0);
        assert_eq!(s.free(), 0);
    }
}
