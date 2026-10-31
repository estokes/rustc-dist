//! What `status` shows, published under `BASE/NAME/status` as netidx tables
//! for the browser: the box's classes, the other boxes as it sees them, and
//! every unit it has run, with a few figures for the whole box beside them.

use crate::{
    protocol::{Status, Switching, gib},
    sched::Msg,
};
use anyhow::Result;
use arcstr::ArcStr;
use compact_str::format_compact;
use netidx::{
    path::Path,
    publisher::{Publisher, UpdateBatch, Val, Value},
};
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

/// Building a status costs the scheduler a few ms.
const REFRESH: Duration = Duration::from_secs(2);

const CLASS_COLUMNS: &[&str] = &["cpus", "G instr/s", "running", "serving", "lent", "free"];
const BOX_COLUMNS: &[&str] =
    &["profile", "cpus", "free", "G instr/s", "from here", "next free (s)", "compatible"];
const UNIT_COLUMNS: &[&str] = &[
    "rank", "work", "front (%)", "width", "wall (s)", "mem (MB)", "here", "sent", "served", "copy (s)", "MB sent",
];

/// Publish this box's status every couple of seconds until the scheduler
/// stops.
pub async fn run(publisher: Publisher, path: Path, tx: mpsc::UnboundedSender<Msg>) -> Result<()> {
    let mut board = Board::new(path);
    let mut refresh = time::interval(REFRESH);
    loop {
        refresh.tick().await;
        let (reply, status) = oneshot::channel();
        if tx.send(Msg::Status { reply }).is_err() {
            return Ok(());
        }
        let Ok(st) = status.await else { return Ok(()) };
        let mut batch = publisher.start_batch();
        board.show(&publisher, &mut batch, &st)?;
        batch.commit(None).await;
    }
}

/// A published value and what it last showed, so only changes are sent.
struct Cell(Val, Value);

impl Cell {
    fn publish(publisher: &Publisher, path: Path, v: Value) -> Result<Self> {
        Ok(Self(publisher.publish(path, v.clone())?, v))
    }

    fn set(&mut self, batch: &mut UpdateBatch, v: Value) {
        if self.1 != v {
            self.0.update(batch, v.clone());
            self.1 = v;
        }
    }
}

/// Rows under `path`, each with a cell per column. A row not shown in a
/// refresh is unpublished.
struct Table {
    path: Path,
    columns: &'static [&'static str],
    rows: HashMap<ArcStr, (u64, Vec<Cell>)>,
}

impl Table {
    fn new(path: Path, columns: &'static [&'static str]) -> Self {
        Self { path, columns, rows: HashMap::new() }
    }

    fn show<const N: usize>(
        &mut self,
        publisher: &Publisher,
        batch: &mut UpdateBatch,
        refresh: u64,
        row: &ArcStr,
        values: [Value; N],
    ) -> Result<()> {
        debug_assert_eq!(N, self.columns.len());
        match self.rows.get_mut(row) {
            Some((seen, cells)) => {
                *seen = refresh;
                for (cell, v) in cells.iter_mut().zip(values) {
                    cell.set(batch, v);
                }
            }
            None => {
                let at = self.path.append(&Path::escape(row));
                let cells = self
                    .columns
                    .iter()
                    .zip(values)
                    .map(|(col, v)| Cell::publish(publisher, at.append(col), v))
                    .collect::<Result<Vec<_>>>()?;
                self.rows.insert(row.clone(), (refresh, cells));
            }
        }
        Ok(())
    }

    fn drop_unseen(&mut self, refresh: u64) {
        self.rows.retain(|_, (seen, _)| *seen == refresh);
    }
}

/// Everything published: figures about the whole box, each a value of its
/// own, and the tables.
struct Board {
    path: Path,
    refresh: u64,
    figures: HashMap<&'static str, Cell>,
    classes: Table,
    boxes: Table,
    units: Table,
}

impl Board {
    fn new(path: Path) -> Self {
        Self {
            classes: Table::new(path.append("classes"), CLASS_COLUMNS),
            boxes: Table::new(path.append("boxes"), BOX_COLUMNS),
            units: Table::new(path.append("units"), UNIT_COLUMNS),
            path,
            refresh: 0,
            figures: HashMap::new(),
        }
    }

    fn figure(&mut self, publisher: &Publisher, batch: &mut UpdateBatch, name: &'static str, v: Value) -> Result<()> {
        match self.figures.get_mut(name) {
            Some(cell) => cell.set(batch, v),
            None => {
                self.figures.insert(name, Cell::publish(publisher, self.path.append(name), v)?);
            }
        }
        Ok(())
    }

    fn show(&mut self, publisher: &Publisher, batch: &mut UpdateBatch, st: &Status) -> Result<()> {
        self.refresh += 1;
        let refresh = self.refresh;
        let switching = match &st.switching {
            Switching::No => Value::Null,
            Switching::Draining(to) => ArcStr::from(format_compact!("to {to}, once running jobs finish").as_str()).into(),
            Switching::Calibrating(to) => ArcStr::from(format_compact!("calibrating {to}").as_str()).into(),
        };
        let figures = [
            ("profile", st.profile.clone().into()),
            ("switching profile", switching),
            ("waiting", st.waiting.into()),
            ("on every cpu: running", st.spread.running.into()),
            ("on every cpu: serving", st.spread.serving.into()),
            ("finishing under the last profile", st.finishing.into()),
            ("memory in use (GiB)", round(gib(st.mem_used_kb), 1).into()),
            ("memory running jobs will add (GiB)", round(gib(st.mem_growth_kb), 1).into()),
            ("memory capacity (GiB)", round(gib(st.mem_capacity_kb), 1).into()),
            ("units in the build graph", st.graph_nodes.into()),
        ];
        for (name, v) in figures {
            self.figure(publisher, batch, name, v)?;
        }
        for (k, c) in st.classes.iter().enumerate() {
            let row = ArcStr::from(format_compact!("{k}").as_str());
            let values = [
                c.cpus.into(),
                round(c.rate_g(), 2).into(),
                c.running.into(),
                c.serving.into(),
                c.lent.into(),
                c.free.into(),
            ];
            self.classes.show(publisher, batch, refresh, &row, values)?;
        }
        for b in &st.boxes {
            let values = [
                b.profile.clone().into(),
                b.cpus().into(),
                b.free().into(),
                round(b.capacity_g(), 0).into(),
                b.sent.into(),
                b.frees_in_s().map_or(Value::Null, |s| round(s, 1).into()),
                b.compatible.into(),
            ];
            self.boxes.show(publisher, batch, refresh, &b.name, values)?;
        }
        let none = || Value::Null;
        for u in &st.units {
            let copy = u.copy();
            let values = [
                u.rank_g().map_or_else(none, |r| round(r, 2).into()),
                round(u.work_g(), 2).into(),
                u.front_pct().map_or_else(none, Value::from),
                round(u.width(), 1).into(),
                round(u.wall_s(), 1).into(),
                u.mem_mb().into(),
                u.here.into(),
                u.sent.into(),
                u.served.into(),
                copy.map_or_else(none, |(s, _)| round(s, 2).into()),
                copy.map_or_else(none, |(_, mb)| round(mb, 1).into()),
            ];
            self.units.show(publisher, batch, refresh, &u.key, values)?;
        }
        self.classes.drop_unseen(refresh);
        self.boxes.drop_unseen(refresh);
        self.units.drop_unseen(refresh);
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
    use crate::{
        farm::ClassState,
        protocol::{BoxStatus, ClassStatus, SpreadStatus, UnitStatus},
    };
    use anyhow::bail;
    use netidx::{InternalOnly, subscriber::Event};

    fn unit(key: &str, cost_mi: u64, copy: Option<(u64, u64)>) -> UnitStatus {
        UnitStatus {
            key: key.into(),
            here: 3,
            sent: 1,
            served: 0,
            cost_mi,
            front_mi: Some(cost_mi / 4),
            width_c: 250,
            wall_ms: 1234,
            mem_kb: 2048,
            copy_ms: copy.map(|c| c.0),
            sent_kb: copy.map(|c| c.1),
            rank_mi: None,
        }
    }

    fn status(units: Vec<UnitStatus>) -> Status {
        let class = ClassState { rate: 6960, cpus: 30, free: 0, frees_in_ms: Some(1500) };
        Status {
            profile: "default".into(),
            switching: Switching::No,
            classes: vec![ClassStatus { rate: 5512, cpus: 4, running: 1, serving: 0, lent: 0, free: 5 }],
            spread: SpreadStatus { running: 0, serving: 0 },
            finishing: 0,
            waiting: 7,
            mem_capacity_kb: 1 << 20,
            mem_used_kb: 1 << 19,
            mem_growth_kb: 0,
            graph_nodes: 2,
            units,
            boxes: vec![BoxStatus { name: "b".into(), profile: "default".into(), classes: vec![class], sent: 2, compatible: true }],
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
        board.show(publisher, &mut batch, st)?;
        batch.commit(None).await;
        publisher.flushed().await;
        Ok(())
    }

    #[tokio::test]
    async fn status_is_published_as_tables_whose_rows_follow_it() -> Result<()> {
        let net = InternalOnly::new().await?;
        let publisher = net.publisher().clone();
        let mut board = Board::new(Path::from("/farm/a/status"));
        let serde = "/farm/a/status/units/rustc:serde:1:link";
        show(&mut board, &publisher, &status(vec![unit("rustc:serde:1:link", 2500, Some((1500, 3072))), unit("rustc:syn:2:link", 13760, None)])).await?;
        assert_eq!(value(&net, &format!("{serde}/work")).await?, Value::from(2.5));
        assert_eq!(value(&net, &format!("{serde}/front (%)")).await?, Value::from(25u64));
        assert_eq!(value(&net, &format!("{serde}/copy (s)")).await?, Value::from(1.5));
        assert_eq!(value(&net, &format!("{serde}/MB sent")).await?, Value::from(3.0));
        assert_eq!(value(&net, "/farm/a/status/units/rustc:syn:2:link/copy (s)").await?, Value::Null);
        assert_eq!(value(&net, "/farm/a/status/classes/0/G instr/s").await?, Value::from(5.51));
        assert_eq!(value(&net, "/farm/a/status/boxes/b/next free (s)").await?, Value::from(1.5));
        assert_eq!(value(&net, "/farm/a/status/boxes/b/G instr/s").await?, Value::from(209.0));
        assert_eq!(value(&net, "/farm/a/status/waiting").await?, Value::from(7u32));
        // A changed unit is updated, and one gone from the status is unpublished.
        show(&mut board, &publisher, &status(vec![unit("rustc:serde:1:link", 3000, Some((1500, 3072)))])).await?;
        assert_eq!(value(&net, &format!("{serde}/work")).await?, Value::from(3.0));
        for _ in 0..50 {
            let rows = net.subscriber().resolver().list(Path::from("/farm/a/status/units")).await?;
            if rows.len() == 1 {
                assert_eq!(&*rows[0], serde);
                return Ok(());
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        bail!("syn's row is still published")
    }
}
