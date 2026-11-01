//! A farm in netidx, under the config's `base` and the protocol version,
//! so boxes of different versions never see each other:
//!
//! - `BASE/vN/status/NAME/COLUMN`: a row per box, which the netidx browser
//!   draws as a table. The columns in [`STATE`] are what other boxes read;
//!   the rest are for people (see `browser`).
//! - `BASE/vN/api/NAME/jobs`: where a box takes remote jobs.
//! - `BASE/vN/units/NAME/UNIT/COLUMN`: the units a box has run.

use crate::{
    protocol::PROTOCOL,
    sched::Msg,
    toolchain::{self, Toolchain},
};
use anyhow::{Result, bail};
use compact_str::format_compact;
use arcstr::ArcStr;
use futures::{StreamExt, channel::mpsc as fmpsc};
use log::{info, warn};
use netidx::{
    path::Path,
    publisher::{FromValue, Publisher, UpdateBatch, Val, Value},
    resolver_client::ChangeTracker,
    subscriber::{Dval, Event, SubId, Subscriber, UpdateChan, UpdatesFlags},
};
use netidx_derive::Pack;
use poolshark::local::LPooled;
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

/// The status columns a box's state is published in. A change updates the
/// columns it touches in one batch, which subscribers get whole, so a box
/// never sees half of another's state.
pub const STATE: [&str; 9] =
    ["profile", "version", "toolchains", "cache GB", "load", "cpus", "class GIPS", "free", "frees in ms"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Pack)]
pub struct ClassState {
    /// Calibrated instructions per CPU-µs.
    pub rate: u64,
    pub cpus: u32,
    /// Jobserver tokens an offer to this class would find now: none while
    /// the box switches profile or its own jobs wait.
    pub free: u32,
    /// How many ms after this state was published a token is predicted to
    /// free: 0 with one free. None when the box can't say, because it takes
    /// no offers or nothing it runs holds the class.
    pub frees_in_ms: Option<u32>,
}

/// What a box publishes for other boxes, in its [`STATE`] columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoxState {
    pub profile: ArcStr,
    /// Counts the states the box has published since it started. Its
    /// answer to an offer names the first version that reflects it.
    pub version: u64,
    pub toolchains: Vec<Toolchain>,
    /// How many GB of crate files it keeps for later jobs.
    pub cache_gb: u64,
    /// Runnable tasks per CPU, in hundredths.
    pub load_c: u32,
    pub classes: Vec<ClassState>,
}

impl BoxState {
    fn columns(&self) -> [Value; STATE.len()] {
        let per_class = |f: fn(&ClassState) -> Value| Value::from(self.classes.iter().map(f).collect::<Vec<_>>());
        [
            self.profile.clone().into(),
            self.version.into(),
            self.toolchains.iter().map(|t| (t.key.clone(), t.targets.clone())).collect::<Vec<_>>().into(),
            self.cache_gb.into(),
            (self.load_c as f64 / 100.).into(),
            per_class(|c| i64::from(c.cpus).into()),
            per_class(|c| (c.rate as f64 / 1000.).into()),
            per_class(|c| i64::from(c.free).into()),
            per_class(|c| c.frees_in_ms.map(i64::from).into()),
        ]
    }

    fn from_columns(columns: &[Value]) -> Result<Self> {
        let [profile, version, toolchains, cache_gb, load, cpus, rates, free, frees_in] = columns else {
            bail!("{} state columns", columns.len());
        };
        fn cast<T: FromValue>(v: &Value) -> Result<T> {
            v.clone().cast_to()
        }
        let toolchains: Vec<(ArcStr, Vec<ArcStr>)> = cast(toolchains)?;
        let load: f64 = cast(load)?;
        let cpus: Vec<i64> = cast(cpus)?;
        let rates: Vec<f64> = cast(rates)?;
        let free: Vec<i64> = cast(free)?;
        let frees_in: Vec<Option<i64>> = cast(frees_in)?;
        let n = cpus.len();
        if rates.len() != n || free.len() != n || frees_in.len() != n {
            bail!("class columns of different lengths");
        }
        let classes = cpus
            .into_iter()
            .zip(rates)
            .zip(free)
            .zip(frees_in)
            .map(|(((cpus, rate), free), frees_in_ms)| {
                Ok(ClassState {
                    rate: (rate * 1000.).round() as u64,
                    cpus: cpus.try_into()?,
                    free: free.try_into()?,
                    frees_in_ms: frees_in_ms.map(u32::try_from).transpose()?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            profile: cast(profile)?,
            version: cast(version)?,
            toolchains: toolchains.into_iter().map(|(key, targets)| Toolchain { key, targets }).collect(),
            cache_gb: cast(cache_gb)?,
            load_c: (load * 100.).round().clamp(0., u32::MAX as f64) as u32,
            classes,
        })
    }
}

/// The part of a box's state its scheduler changes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Load {
    pub profile: ArcStr,
    pub version: u64,
    /// Runnable tasks per CPU, in hundredths.
    pub load_c: u32,
    pub classes: Vec<ClassState>,
}

/// The farm's namespace for this protocol version.
fn versioned(base: &Path) -> Path {
    base.append(format_compact!("v{PROTOCOL}").as_str())
}

/// Where a box takes remote jobs.
pub fn jobs_path(base: &Path, name: &str) -> Path {
    versioned(base).append("api").append(name).append("jobs")
}

/// The status table, a row per box.
fn status_table(base: &Path) -> Path {
    versioned(base).append("status")
}

/// A box's row in the status table.
pub fn status_path(base: &Path, name: &str) -> Path {
    status_table(base).append(name)
}

/// The table of units a box has run.
pub fn units_path(base: &Path, name: &str) -> Path {
    versioned(base).append("units").append(name)
}

/// A published value and what it last showed, so only changes are sent.
pub struct Cell(Val, Value);

impl Cell {
    pub fn publish(publisher: &Publisher, path: Path, v: Value) -> Result<Self> {
        Ok(Self(publisher.publish(path, v.clone())?, v))
    }

    pub fn set(&mut self, batch: &mut UpdateBatch, v: Value) {
        if self.1 != v {
            self.0.update(batch, v.clone());
            self.1 = v;
        }
    }
}

pub struct Member {
    pub publisher: Publisher,
    pub subscriber: Subscriber,
    pub base: Path,
    pub name: ArcStr,
    pub toolchains: Vec<Toolchain>,
    pub cache_gb: u64,
}

/// Another box being watched: its state columns as they last arrived.
struct Watched {
    _subs: Vec<Dval>,
    columns: [Option<Value>; STATE.len()],
}

impl Watched {
    /// Its state, once every column has arrived.
    fn state(&self) -> Option<Result<BoxState>> {
        let mut columns: LPooled<Vec<Value>> = LPooled::take();
        for c in &self.columns {
            columns.push(c.clone()?);
        }
        Some(BoxState::from_columns(&columns))
    }
}

/// Publish this box's state and tell the scheduler about every other
/// box's.
pub async fn run(m: Member, mut load: watch::Receiver<Load>, tx: mpsc::UnboundedSender<Msg>) -> Result<()> {
    let mut toolchains = toolchain::watch(m.toolchains.clone());
    let mut watching = true;
    let state = |load: &Load, toolchains: &[Toolchain]| BoxState {
        profile: load.profile.clone(),
        version: load.version,
        toolchains: toolchains.to_vec(),
        cache_gb: m.cache_gb,
        load_c: load.load_c,
        classes: load.classes.clone(),
    };
    let row = status_path(&m.base, &m.name);
    let mut cells = STATE
        .iter()
        .zip(state(&load.borrow_and_update(), &toolchains.borrow_and_update()).columns())
        .map(|(col, v)| Cell::publish(&m.publisher, row.append(col), v))
        .collect::<Result<Vec<_>>>()?;
    info!("publishing {row}");
    let table = status_table(&m.base);
    let (updates_tx, mut updates): (UpdateChan, _) = fmpsc::channel(64);
    let mut boxes: HashMap<ArcStr, Watched> = HashMap::new();
    let mut columns: HashMap<SubId, (ArcStr, usize)> = HashMap::new();
    let mut tracker = ChangeTracker::new(table.clone());
    let mut list = time::interval(LIST_EVERY);
    let mut publish = time::interval(PUBLISH_EVERY);
    let mut changed = false;
    loop {
        tokio::select! {
            r = load.changed() => {
                if r.is_err() {
                    // The scheduler has stopped.
                    break;
                }
                changed = true;
            }
            r = toolchains.changed(), if watching => {
                watching = r.is_ok();
                if !watching {
                    warn!("no longer watching rustup's toolchains");
                }
                changed |= watching;
            }
            _ = publish.tick(), if changed => {
                changed = false;
                let mut batch = m.publisher.start_batch();
                let state = state(&load.borrow_and_update(), &toolchains.borrow_and_update());
                for (cell, v) in cells.iter_mut().zip(state.columns()) {
                    cell.set(&mut batch, v);
                }
                batch.commit(None).await;
            }
            batch = updates.next() => {
                let Some(mut batch) = batch else { break };
                let mut touched: LPooled<Vec<ArcStr>> = LPooled::take();
                for (id, ev) in batch.drain(..) {
                    let Some((name, i)) = columns.get(&id) else { continue };
                    let Some(w) = boxes.get_mut(name) else { continue };
                    w.columns[*i] = match ev {
                        Event::Update(v) => Some(v),
                        Event::Unsubscribed => None,
                    };
                    if !touched.contains(name) {
                        touched.push(name.clone());
                    }
                }
                for name in touched.drain(..) {
                    let Some(w) = boxes.get(&name) else { continue };
                    let state = match w.state() {
                        Some(Ok(s)) => Some((jobs_path(&m.base, &name), s)),
                        Some(Err(e)) => {
                            warn!("state of {name}: {e:#}");
                            None
                        }
                        None => None,
                    };
                    let _ = tx.send(Msg::Farm { name, state });
                }
            }
            // XCR claude for claude: [bug/high] A resolver error ends the farm for good.
            // check_changed and list return Err once netidx's read client gives up on the resolver
            // (four tries with random waits of 1 to 12 s), and `?` ends run: this box's state
            // columns are unpublished as `cells` drops, it stops watching the others, and it stays
            // out until the daemon restarts, with one "left the farm" line in the log. The
            // scheduler meanwhile keeps every other box's last state, since nothing sends Msg::Farm
            // { state: None } on the way out, and goes on placing jobs by that frozen snapshot of
            // versions, free tokens and frees in ms. A resolver restart, a wifi drop or a laptop
            // suspend is enough. Fix: log the error and try again at the next tick, and whenever
            // run returns send `state: None` for each watched box. No test covers a resolver that
            // goes away and comes back. (farm-01)
            // 2026-11-01 claude: A failed check_changed or list in the list tick is logged as a
            // warning and tried again at the next tick, and after the loop run sends Msg::Farm {
            // state: None } for every box it watched.
            // farm::test::boxes_see_each_others_state_as_it_changes_until_their_farm_task_ends
            // drops b's load sender and fails if b's scheduler isn't told a is gone (checked). No
            // test covers a resolver that goes away and comes back: netidx retries for tens of
            // seconds first. Still standing: while a resolver call retries inside the tick's
            // handler, the loop neither publishes nor reads states; moving the listing into its own
            // task would fix that, left out to keep this change to error handling.
            _ = list.tick() => {
                let resolver = m.subscriber.resolver();
                let listing: Result<Option<_>> = async {
                    if !resolver.check_changed(&mut tracker).await? {
                        return Ok(None);
                    }
                    Ok(Some(resolver.list(table.clone()).await?))
                }
                .await;
                let listed = match listing {
                    Ok(Some(listed)) => listed,
                    Ok(None) => continue,
                    Err(e) => {
                        warn!("listing the farm's boxes, trying again: {e:#}");
                        continue;
                    }
                };
                let present: Vec<ArcStr> = listed
                    .iter()
                    .filter_map(|p| Path::basename(p).map(ArcStr::from))
                    .filter(|n| *n != m.name)
                    .collect();
                for name in &present {
                    if !boxes.contains_key(name) {
                        info!("box {name} joined");
                        let at = status_path(&m.base, name);
                        let subs: Vec<Dval> = STATE
                            .iter()
                            .enumerate()
                            .map(|(i, col)| {
                                let dval = m.subscriber.subscribe(at.append(col));
                                dval.updates(UpdatesFlags::BEGIN_WITH_LAST, updates_tx.clone());
                                columns.insert(dval.id(), (name.clone(), i));
                                dval
                            })
                            .collect();
                        boxes.insert(name.clone(), Watched { _subs: subs, columns: Default::default() });
                    }
                }
                boxes.retain(|name, _| {
                    let keep = present.contains(name);
                    if !keep {
                        info!("box {name} left");
                        let _ = tx.send(Msg::Farm { name: name.clone(), state: None });
                    }
                    keep
                });
                columns.retain(|_, (name, _)| boxes.contains_key(name));
            }
        }
    }
    for name in boxes.into_keys() {
        let _ = tx.send(Msg::Farm { name, state: None });
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use netidx::InternalOnly;

    fn state(version: u64, free: u32) -> BoxState {
        BoxState {
            profile: "default".into(),
            version,
            toolchains: vec![Toolchain { key: "c h".into(), targets: vec!["h".into(), "wasm32".into()] }],
            cache_gb: 64,
            load_c: 37,
            classes: vec![
                ClassState { rate: 6960, cpus: 30, free, frees_in_ms: None },
                ClassState { rate: 3970, cpus: 2, free: 3, frees_in_ms: Some(0) },
            ],
        }
    }

    async fn state_of_a(msgs: &mut mpsc::UnboundedReceiver<Msg>) -> Result<(Path, BoxState)> {
        loop {
            match time::timeout(Duration::from_secs(10), msgs.recv()).await {
                Ok(Some(Msg::Farm { name, state: Some(state) })) if name == "a" => return Ok(state),
                Ok(Some(_)) => continue,
                _ => bail!("no state from a"),
            }
        }
    }

    #[test]
    fn state_round_trips_through_its_columns() {
        let s = state(7, 0);
        assert_eq!(BoxState::from_columns(&s.columns()).unwrap(), s);
        let mut short = s.columns();
        short[7] = Value::from(vec![1i64]);
        assert!(BoxState::from_columns(&short).is_err());
    }

    #[tokio::test]
    async fn boxes_see_each_others_state_as_it_changes_until_their_farm_task_ends() -> Result<()> {
        let net = InternalOnly::new().await?;
        let base = Path::from("/farm");
        let member = |name: &str| Member {
            publisher: net.publisher().clone(),
            subscriber: net.subscriber().clone(),
            base: base.clone(),
            name: name.into(),
            toolchains: state(0, 0).toolchains,
            cache_gb: 64,
        };
        let load = |version, free| {
            let s = state(version, free);
            Load { profile: s.profile, version, load_c: s.load_c, classes: s.classes }
        };
        let (a_load, a_rx) = watch::channel(load(1, 0));
        let (a_tx, _a_msgs) = mpsc::unbounded_channel();
        tokio::spawn(run(member("a"), a_rx, a_tx));
        let (b_load, b_rx) = watch::channel(load(1, 0));
        let (b_tx, mut b_msgs) = mpsc::unbounded_channel();
        let b = tokio::spawn(run(member("b"), b_rx, b_tx));
        let (jobs, s) = state_of_a(&mut b_msgs).await?;
        assert_eq!(jobs, Path::from(format!("/farm/v{PROTOCOL}/api/a/jobs")));
        assert_eq!(s, state(1, 0));
        a_load.send(load(2, 5))?;
        let (_, s) = state_of_a(&mut b_msgs).await?;
        assert_eq!(s, state(2, 5), "the new version and its free tokens arrive together");
        drop(b_load);
        b.await??;
        let mut forgot = false;
        while let Ok(msg) = b_msgs.try_recv() {
            forgot = matches!(msg, Msg::Farm { name, state: None } if name == "a");
        }
        assert!(forgot, "a scheduler whose farm task ended doesn't place jobs by a's last state");
        Ok(())
    }
}
