//! What `status` shows, for the netidx browser: figures about the box in
//! its row of the farm's status table, beside the state columns other boxes
//! read, and every unit it has run in a table of its own. Each part is kept
//! current only while a client subscribes to it.

use crate::{
    farm::Cell,
    protocol::{ClassStatus, Status, Switching, gib},
    sched::{self, Msg},
};
use anyhow::Result;
use arcstr::ArcStr;
use compact_str::format_compact;
use futures::{StreamExt, channel::mpsc as fmpsc};
use netidx::{
    path::Path,
    publisher::{ClId, Event, Id, Publisher, UpdateBatch, Value},
    tick,
};
use std::{collections::HashMap, time::Duration};
use tokio::sync::{mpsc, oneshot};

/// Building the units' rows costs the scheduler tens of ms.
const REFRESH: Duration = sched::IDLE;

const UNIT_COLUMNS: [&str; 12] =
    ["rank s", "work s", "front %", "width", "now s", "wall s", "mem MB", "here", "sent", "served", "copy s", "sent MB"];

/// Publish this box's figures in its status row at `row` and its units
/// under `units`, both at the start for clients to find, then every
/// `REFRESH` each while a client subscribes to it and at once when the
/// first does, until the scheduler stops. Unwatched, it doesn't wake.
pub async fn run(publisher: Publisher, row: Path, units: Path, tx: mpsc::UnboundedSender<Msg>) -> Result<()> {
    let (events_tx, mut events) = fmpsc::unbounded();
    publisher.events(events_tx);
    let mut board = Board::new(row, units);
    let mut refresh = tick::interval(REFRESH);
    let mut parts = Parts { figures: true, units: true };
    loop {
        if parts.figures || parts.units {
            let (reply, status) = oneshot::channel();
            if tx.send(Msg::Status { units: parts.units, reply }).is_err() {
                return Ok(());
            }
            let Ok(st) = status.await else { return Ok(()) };
            let mut batch = publisher.start_batch();
            board.show(&publisher, &mut batch, &st, parts)?;
            batch.commit(None).await;
        }
        let watched = board.watched();
        parts = tokio::select! {
            _ = refresh.tick(), if watched.figures || watched.units => watched,
            e = events.next() => match e {
                Some(e) => board.watch(&publisher, e),
                None => return Ok(()),
            },
        };
    }
}

/// Which parts of the board to show, or which clients watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Parts {
    figures: bool,
    units: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Figures,
    Units,
}

/// A row per unit, each with a cell per column. A unit gone from the status
/// is unpublished.
struct Units {
    path: Path,
    refresh: u64,
    rows: HashMap<ArcStr, (u64, Vec<Cell>)>,
}

impl Units {
    fn show(&mut self, publisher: &Publisher, batch: &mut UpdateBatch, st: &Status) -> Result<()> {
        self.refresh += 1;
        let none = || Value::Null;
        for u in &st.units {
            let copy = u.copy();
            let values: [Value; UNIT_COLUMNS.len()] = [
                u.rank_s().map_or_else(none, |r| round(r, 2).into()),
                round(u.work_s(), 2).into(),
                u.front_pct().map_or_else(none, Value::from),
                round(u.width(), 1).into(),
                round(u.now_s(), 1).into(),
                round(u.wall_s(), 1).into(),
                u.mem_mb().into(),
                u.here.into(),
                u.sent.into(),
                u.served.into(),
                copy.map_or_else(none, |(s, _)| round(s, 2).into()),
                copy.map_or_else(none, |(_, mb)| round(mb, 1).into()),
            ];
            match self.rows.get_mut(&u.key) {
                Some((seen, cells)) => {
                    *seen = self.refresh;
                    for (cell, v) in cells.iter_mut().zip(values) {
                        cell.set(batch, v);
                    }
                }
                None => {
                    let at = self.path.append(&Path::escape(&u.key));
                    let cells = UNIT_COLUMNS
                        .iter()
                        .zip(values)
                        .map(|(col, v)| Cell::publish(publisher, at.append(col), v))
                        .collect::<Result<Vec<_>>>()?;
                    self.rows.insert(u.key.clone(), (self.refresh, cells));
                }
            }
        }
        let refresh = self.refresh;
        self.rows.retain(|_, (seen, _)| *seen == refresh);
        Ok(())
    }
}

/// The box's figures in its status row, each a cell of its own, its units,
/// and which part each client subscription is to.
struct Board {
    row: Path,
    figures: HashMap<&'static str, Cell>,
    units: Units,
    watchers: HashMap<(Id, ClId), Part>,
}

impl Board {
    fn new(row: Path, units: Path) -> Self {
        Self {
            row,
            figures: HashMap::new(),
            units: Units { path: units, refresh: 0, rows: HashMap::new() },
            watchers: HashMap::new(),
        }
    }

    fn watched(&self) -> Parts {
        let watched = |part| self.watchers.values().any(|&p| p == part);
        Parts { figures: watched(Part::Figures), units: watched(Part::Units) }
    }

    /// Follow a publisher event: the parts a client now watches that none
    /// did before.
    fn watch(&mut self, publisher: &Publisher, e: Event) -> Parts {
        let before = self.watched();
        match e {
            Event::Subscribe(id, client) => {
                if let Some(part) = self.part(publisher, id) {
                    self.watchers.insert((id, client), part);
                }
            }
            Event::Unsubscribe(id, client) => {
                self.watchers.remove(&(id, client));
            }
            Event::Destroyed(id) => self.watchers.retain(|&(i, _), _| i != id),
        }
        let after = self.watched();
        Parts { figures: after.figures && !before.figures, units: after.units && !before.units }
    }

    /// The part of the board the value `id` is in, if any.
    fn part(&self, publisher: &Publisher, id: Id) -> Option<Part> {
        if self.figures.values().any(|c| c.id() == id) {
            return Some(Part::Figures);
        }
        let path = publisher.path(id)?;
        Path::is_parent(&self.units.path, &path).then_some(Part::Units)
    }

    /// Show the figures and units of `st` that `parts` asks for.
    fn show(&mut self, publisher: &Publisher, batch: &mut UpdateBatch, st: &Status, parts: Parts) -> Result<()> {
        if parts.figures {
            self.show_figures(publisher, batch, st)?;
        }
        if parts.units {
            self.units.show(publisher, batch, st)?;
        }
        Ok(())
    }

    fn show_figures(&mut self, publisher: &Publisher, batch: &mut UpdateBatch, st: &Status) -> Result<()> {
        let switching = match &st.switching {
            Switching::No => Value::Null,
            Switching::Draining(to) => ArcStr::from(format_compact!("to {to}, once running jobs finish").as_str()).into(),
            Switching::Calibrating(to) => ArcStr::from(format_compact!("calibrating {to}").as_str()).into(),
        };
        let per_class =
            |f: fn(&ClassStatus) -> u32| Value::from(st.classes.iter().map(|c| i64::from(f(c))).collect::<Vec<_>>());
        let speeds = |f: fn(&ClassStatus) -> u32| {
            Value::from(st.classes.iter().map(|c| round(st.g_per_s(f(c)), 2)).collect::<Vec<_>>())
        };
        let sum = |f: fn(&ClassStatus) -> u32| st.classes.iter().map(f).sum::<u32>();
        let figures: [(&'static str, Value); 16] = [
            ("switching", switching),
            ("waiting", st.waiting.into()),
            ("running", (sum(|c| c.running) + st.spread.running).into()),
            ("serving", (sum(|c| c.serving) + st.spread.serving).into()),
            ("class speed", speeds(|c| c.speed_c)),
            ("class idle speed", speeds(|c| c.idle_speed_c)),
            ("heavy threads", st.spread.threads.into()),
            ("heavy thread speed", round(st.g_per_s(st.spread.thread_speed_c), 2).into()),
            ("class running", per_class(|c| c.running)),
            ("class serving", per_class(|c| c.serving)),
            ("lent", per_class(|c| c.lent)),
            ("finishing", st.finishing.into()),
            ("mem GiB", round(gib(st.mem_used_kb), 1).into()),
            ("mem growth GiB", round(gib(st.mem_growth_kb), 1).into()),
            ("mem cap GiB", round(gib(st.mem_capacity_kb), 1).into()),
            ("graph units", st.graph_nodes.into()),
        ];
        for (name, v) in figures {
            match self.figures.get_mut(name) {
                Some(cell) => cell.set(batch, v),
                None => {
                    self.figures.insert(name, Cell::publish(publisher, self.row.append(name), v)?);
                }
            }
        }
        Ok(())
    }
}

fn round(x: f64, places: i32) -> f64 {
    let scale = 10f64.powi(places);
    (x * scale).round() / scale
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::{SpreadStatus, UnitStatus};
    use anyhow::bail;
    use netidx::{InternalOnly, subscriber::Event};
    use tokio::time;

    fn unit(key: &str, work_ms: u64, copy: Option<(u64, u64)>) -> UnitStatus {
        UnitStatus {
            key: key.into(),
            here: 3,
            sent: 1,
            served: 0,
            work_ms,
            front_ms: Some(work_ms / 4),
            width_c: 250,
            now_ms: 1500,
            wall_ms: 1234,
            mem_kb: 2048,
            copy_ms: copy.map(|c| c.0),
            sent_kb: copy.map(|c| c.1),
            rank_ms: None,
        }
    }

    fn status(units: Vec<UnitStatus>) -> Status {
        Status {
            profile: "default".into(),
            switching: Switching::No,
            load_c: 75,
            base_rate: 4000,
            classes: vec![
                ClassStatus { cpus: 4, idle_speed_c: 100, speed_c: 90, running: 1, serving: 0, lent: 0, free: 5 },
                ClassStatus { cpus: 8, idle_speed_c: 40, speed_c: 35, running: 0, serving: 2, lent: 1, free: 9 },
            ],
            spread: SpreadStatus { running: 1, serving: 0, threads: 12, thread_speed_c: 55 },
            finishing: 0,
            waiting: 7,
            mem_capacity_kb: 1 << 20,
            mem_used_kb: 1 << 19,
            mem_growth_kb: 0,
            graph_nodes: 2,
            units,
            boxes: Vec::new(),
        }
    }

    async fn value(net: &InternalOnly, path: &str) -> Result<Value> {
        let at = Path::from(ArcStr::from(path));
        let val = net.subscriber().subscribe_nondurable_one(at, Some(Duration::from_secs(5))).await?;
        match val.last() {
            Event::Update(v) => Ok(v),
            Event::Unsubscribed => bail!("{path} unsubscribed"),
        }
    }

    async fn show(board: &mut Board, publisher: &Publisher, st: &Status) -> Result<()> {
        let mut batch = publisher.start_batch();
        board.show(publisher, &mut batch, st, Parts { figures: true, units: true })?;
        batch.commit(None).await;
        publisher.flushed().await;
        Ok(())
    }

    #[tokio::test]
    async fn figures_go_in_the_boxes_row_and_units_in_their_own_table() -> Result<()> {
        let net = InternalOnly::new().await?;
        let publisher = net.publisher().clone();
        // The row already holds the state columns other boxes read.
        let _state = crate::farm::STATE
            .iter()
            .map(|col| Cell::publish(&publisher, Path::from(format!("/farm/status/a/{col}")), Value::Null))
            .collect::<Result<Vec<_>>>()?;
        let mut board = Board::new(Path::from("/farm/status/a"), Path::from("/farm/units/a"));
        let serde = "/farm/units/a/rustc:serde:1:link";
        let both = vec![unit("rustc:serde:1:link", 2500, Some((1500, 3072))), unit("rustc:syn:2:link", 13760, None)];
        show(&mut board, &publisher, &status(both)).await?;
        assert_eq!(value(&net, "/farm/status/a/waiting").await?, Value::from(7u32));
        assert_eq!(value(&net, "/farm/status/a/running").await?, Value::from(2u32));
        // In G instructions of the calibration load per second, so boxes
        // compare.
        assert_eq!(value(&net, "/farm/status/a/class speed").await?, Value::from(vec![3.6, 1.4]));
        for column in net.subscriber().resolver().list(Path::from("/farm/status/a")).await?.iter() {
            let below = net.subscriber().resolver().list(column.clone()).await?;
            assert!(below.is_empty(), "{column} is a directory, not a column");
        }
        assert_eq!(value(&net, "/farm/status/a/class serving").await?, Value::from(vec![0i64, 2]));
        assert_eq!(value(&net, &format!("{serde}/work s")).await?, Value::from(2.5));
        assert_eq!(value(&net, &format!("{serde}/now s")).await?, Value::from(1.5));
        assert_eq!(value(&net, &format!("{serde}/front %")).await?, Value::from(25u64));
        assert_eq!(value(&net, &format!("{serde}/copy s")).await?, Value::from(1.5));
        assert_eq!(value(&net, &format!("{serde}/sent MB")).await?, Value::from(3.0));
        assert_eq!(value(&net, "/farm/units/a/rustc:syn:2:link/copy s").await?, Value::Null);
        // A changed unit is updated, and one gone from the status is unpublished.
        show(&mut board, &publisher, &status(vec![unit("rustc:serde:1:link", 3000, Some((1500, 3072)))])).await?;
        assert_eq!(value(&net, &format!("{serde}/work s")).await?, Value::from(3.0));
        for _ in 0..50 {
            let rows = net.subscriber().resolver().list(Path::from("/farm/units/a")).await?;
            if rows.len() == 1 {
                assert_eq!(&*rows[0], serde);
                return Ok(());
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        bail!("syn's row is still published")
    }

    /// The parts watched once the publisher's events so far are followed,
    /// and the parts the first of them newly watched.
    async fn follow(board: &mut Board, publisher: &Publisher, events: &mut fmpsc::UnboundedReceiver<netidx::publisher::Event>) -> Parts {
        publisher.flushed().await;
        let mut newly = Parts { figures: false, units: false };
        while let Ok(Some(e)) = time::timeout(Duration::from_millis(200), events.next()).await {
            let n = board.watch(publisher, e);
            newly = Parts { figures: newly.figures || n.figures, units: newly.units || n.units };
        }
        newly
    }

    #[tokio::test]
    async fn each_part_is_watched_while_a_client_subscribes_to_it() -> Result<()> {
        let net = InternalOnly::new().await?;
        let publisher = net.publisher().clone();
        let (tx, mut events) = fmpsc::unbounded();
        publisher.events(tx);
        let state = Cell::publish(&publisher, Path::from("/farm/status/a/load"), Value::Null)?;
        let mut board = Board::new(Path::from("/farm/status/a"), Path::from("/farm/units/a"));
        let both = vec![unit("rustc:serde:1:link", 2500, None), unit("rustc:syn:2:link", 13760, None)];
        show(&mut board, &publisher, &status(both)).await?;
        let none = Parts { figures: false, units: false };
        let sub = |path: &str| {
            net.subscriber().subscribe_nondurable_one(Path::from(ArcStr::from(path)), Some(Duration::from_secs(5)))
        };
        let other_boxes = sub("/farm/status/a/load").await?;
        assert_eq!(follow(&mut board, &publisher, &mut events).await, none);
        assert_eq!(board.watched(), none);
        let figure = sub("/farm/status/a/waiting").await?;
        assert_eq!(follow(&mut board, &publisher, &mut events).await, Parts { figures: true, units: false });
        let serde = sub("/farm/units/a/rustc:serde:1:link/work s").await?;
        assert_eq!(follow(&mut board, &publisher, &mut events).await, Parts { figures: false, units: true });
        let syn = sub("/farm/units/a/rustc:syn:2:link/work s").await?;
        assert_eq!(follow(&mut board, &publisher, &mut events).await, none);
        drop(serde);
        assert_eq!(follow(&mut board, &publisher, &mut events).await, none);
        assert_eq!(board.watched(), Parts { figures: true, units: true });
        // syn's row is unpublished under its subscriber.
        let mut batch = publisher.start_batch();
        board.show(&publisher, &mut batch, &status(Vec::new()), Parts { figures: false, units: true })?;
        batch.commit(None).await;
        follow(&mut board, &publisher, &mut events).await;
        assert_eq!(board.watched(), Parts { figures: true, units: false });
        drop(figure);
        follow(&mut board, &publisher, &mut events).await;
        assert_eq!(board.watched(), none);
        drop((syn, other_boxes, state));
        Ok(())
    }
}
