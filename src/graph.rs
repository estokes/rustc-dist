use crate::{
    protocol::{Dep, Unit},
    stats::Stats,
};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use bytes::{BufMut, BytesMut};
use compact_str::format_compact;
use netidx_core::pack::{Pack, encode_varint, varint_len};
use netidx_derive::Pack;
use poolshark::local::LPooled;
use std::{
    collections::HashMap,
    fs,
    io::ErrorKind,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

/// Nodes not seen for this many days are dropped when the graph loads.
const KEEP_DAYS: u32 = 60;
const MAGIC: &[u8; 8] = b"rdgraph2";

#[derive(Pack)]
struct Node {
    id: ArcStr,
    exact: ArcStr,
    coarse: ArcStr,
    deps: Vec<Dep>,
    seen_day: u32,
}

/// Which jobs read which jobs' outputs, learned from every compile the
/// daemon admits. Builds repeat, so a job's dependents in past builds are
/// the work that will follow it in this one.
pub struct Graph {
    nodes: HashMap<ArcStr, Node>,
    /// For each id, the ids of the nodes whose deps include it.
    dependents: HashMap<ArcStr, Vec<ArcStr>>,
    /// Ranks computed since the graph or the job stats last changed.
    ranks: HashMap<ArcStr, u64>,
    pub dirty: bool,
}

fn today() -> u32 {
    (SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / 86_400) as u32
}

impl Graph {
    pub fn new() -> Self {
        Self { nodes: HashMap::new(), dependents: HashMap::new(), ranks: HashMap::new(), dirty: false }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn record(&mut self, unit: &Unit) {
        let day = today();
        let old_deps = match self.nodes.get_mut(&unit.id) {
            Some(n) if n.deps == unit.deps => {
                if n.seen_day != day || n.exact != unit.exact {
                    n.seen_day = day;
                    n.exact = unit.exact.clone();
                    n.coarse = unit.coarse.clone();
                    self.dirty = true;
                }
                return;
            }
            Some(n) => {
                n.seen_day = day;
                n.exact = unit.exact.clone();
                n.coarse = unit.coarse.clone();
                std::mem::replace(&mut n.deps, unit.deps.clone())
            }
            None => {
                let node = Node {
                    id: unit.id.clone(),
                    exact: unit.exact.clone(),
                    coarse: unit.coarse.clone(),
                    deps: unit.deps.clone(),
                    seen_day: day,
                };
                self.nodes.insert(unit.id.clone(), node);
                Vec::new()
            }
        };
        let has = |deps: &[Dep], id: &ArcStr| deps.iter().any(|d| &d.id == id);
        for dep in old_deps.iter().filter(|d| !has(&unit.deps, &d.id)) {
            if let Some(ds) = self.dependents.get_mut(&dep.id) {
                ds.retain(|d| d != &unit.id);
            }
        }
        for dep in unit.deps.iter().filter(|d| !has(&old_deps, &d.id)) {
            self.dependents.entry(dep.id.clone()).or_default().push(unit.id.clone());
        }
        self.ranks.clear();
        self.dirty = true;
    }

    /// Forget computed ranks, because predicted costs changed.
    pub fn invalidate(&mut self) {
        self.ranks.clear();
    }

    /// The predicted cost of `id` plus the costliest chain of jobs that
    /// depend on it: the work that waits on this job, in milli-cu.
    pub fn rank(&mut self, id: &ArcStr, stats: &Stats) -> u64 {
        if let Some(r) = self.ranks.get(id) {
            return *r;
        }
        // Marks `id` as in progress, so a cycle in stale data ends here.
        self.ranks.insert(id.clone(), 0);
        let own = match self.nodes.get(id) {
            Some(n) => stats.predict(&n.exact, &n.coarse).cost_mcu,
            None => 0,
        };
        let mut dependents: LPooled<Vec<ArcStr>> = LPooled::take();
        dependents.extend(self.dependents.get(id).into_iter().flatten().cloned());
        let down = dependents.iter().map(|d| self.rank(d, stats)).max().unwrap_or(0);
        let rank = own + down;
        self.ranks.insert(id.clone(), rank);
        rank
    }

    /// The crate files a job with `deps` reads: its own and, through the
    /// graph, those of every crate below it, since rustc loads the metadata
    /// of indirect dependencies too. None if a crate below it was never
    /// seen compiling, so its dependencies are unknown.
    pub fn inputs(&self, deps: &[Dep]) -> Option<Vec<ArcStr>> {
        let mut files = Vec::new();
        let mut seen: LPooled<HashMap<ArcStr, ()>> = LPooled::take();
        let mut stack: LPooled<Vec<&Dep>> = LPooled::take();
        stack.extend(deps);
        while let Some(dep) = stack.pop() {
            let Some(file) = &dep.file else { continue };
            if seen.insert(dep.id.clone(), ()).is_some() {
                continue;
            }
            files.push(file.clone());
            stack.extend(&self.nodes.get(&dep.id)?.deps);
        }
        Some(files)
    }

    /// Every node with its rank and job key, for `status`.
    pub fn ranked(&mut self, stats: &Stats) -> Vec<(ArcStr, u64)> {
        let ids: Vec<ArcStr> = self.nodes.keys().cloned().collect();
        ids.into_iter()
            .map(|id| {
                let rank = self.rank(&id, stats);
                (self.nodes[&id].exact.clone(), rank)
            })
            .collect()
    }

    /// A missing file is an empty graph.
    pub fn load(path: &Path) -> Result<Self> {
        let data = match fs::read(path) {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Self::new()),
            Err(e) => return Err(e).with_context(|| format_compact!("reading {}", path.display())),
        };
        let Some(mut body) = data.strip_prefix(MAGIC) else {
            bail!("{} is not a graph file", path.display());
        };
        let nodes = Vec::<Node>::decode(&mut body).map_err(|e| anyhow!("{}: {e}", path.display()))?;
        let oldest = today().saturating_sub(KEEP_DAYS);
        let mut graph = Self::new();
        for node in nodes.into_iter().filter(|n| n.seen_day >= oldest) {
            for dep in &node.deps {
                graph.dependents.entry(dep.id.clone()).or_default().push(node.id.clone());
            }
            graph.nodes.insert(node.id.clone(), node);
        }
        Ok(graph)
    }

    pub fn save(&mut self, path: &Path) -> Result<()> {
        // The layout of a packed Vec<Node>, which `load` decodes.
        let len = self.nodes.values().fold(varint_len(self.nodes.len() as u64), |len, n| {
            len + n.encoded_len()
        });
        let mut buf = BytesMut::with_capacity(MAGIC.len() + len);
        buf.put_slice(MAGIC);
        encode_varint(self.nodes.len() as u64, &mut buf);
        for node in self.nodes.values() {
            node.encode(&mut buf).map_err(|e| anyhow!("{e}"))?;
        }
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).with_context(|| format_compact!("creating {}", dir.display()))?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, &buf).with_context(|| format_compact!("writing {}", tmp.display()))?;
        fs::rename(&tmp, path).with_context(|| format_compact!("writing {}", path.display()))?;
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::stats::Sample;

    fn unit(id: &str, deps: &[&str]) -> Unit {
        Unit {
            exact: ArcStr::from(id),
            coarse: ArcStr::from(id),
            id: ArcStr::from(id),
            deps: deps
                .iter()
                .map(|d| Dep { id: ArcStr::from(*d), file: Some(format_compact!("/t/lib{d}.rmeta").as_str().into()) })
                .collect(),
        }
    }

    fn stats(costs: &[(&str, u64)]) -> Stats {
        let mut s = Stats::new();
        for &(id, cost_mcu) in costs {
            s.record(&unit(id, &[]), Sample { cost_mcu, wall_ms: 1, mem_kb: 1 });
        }
        s
    }

    /// libc ← regex_syntax ← regex ← app, and libc ← itoa.
    fn build() -> Graph {
        let mut g = Graph::new();
        g.record(&unit("libc", &[]));
        g.record(&unit("itoa", &["libc"]));
        g.record(&unit("regex_syntax", &["libc"]));
        g.record(&unit("regex", &["regex_syntax"]));
        g.record(&unit("app", &["regex", "itoa"]));
        g
    }

    /// Histograms keep two significant figures.
    fn about(got: u64, want: u64) {
        assert!(got.abs_diff(want) * 100 <= want, "{got} is not about {want}");
    }

    #[test]
    fn rank_is_cost_plus_the_costliest_chain_after() {
        let s = stats(&[("libc", 100), ("itoa", 50), ("regex_syntax", 9000), ("regex", 4000), ("app", 2000)]);
        let mut g = build();
        about(g.rank(&"app".into(), &s), 2000);
        about(g.rank(&"itoa".into(), &s), 50 + 2000);
        about(g.rank(&"regex_syntax".into(), &s), 9000 + 4000 + 2000);
        // Small, but the whole build waits on it.
        about(g.rank(&"libc".into(), &s), 100 + 15_000);
    }

    #[test]
    fn changed_deps_move_edges() {
        let s = stats(&[("libc", 100), ("itoa", 50), ("regex_syntax", 9000), ("regex", 4000), ("app", 2000)]);
        let mut g = build();
        g.record(&unit("app", &["itoa"]));
        about(g.rank(&"regex".into(), &s), 4000);
        about(g.rank(&"itoa".into(), &s), 2050);
    }

    #[test]
    fn inputs_include_indirect_crates() {
        let g = build();
        let app = unit("app", &["regex", "itoa"]);
        let mut files = g.inputs(&app.deps).unwrap();
        files.sort();
        assert_eq!(files, ["/t/libitoa.rmeta", "/t/liblibc.rmeta", "/t/libregex.rmeta", "/t/libregex_syntax.rmeta"]);
        let unknown = unit("x", &["never_built"]);
        assert!(g.inputs(&unknown.deps).is_none());
    }

    #[test]
    fn a_cycle_in_stale_data_terminates() {
        let s = stats(&[("a", 10), ("b", 20)]);
        let mut g = Graph::new();
        g.record(&unit("a", &["b"]));
        g.record(&unit("b", &["a"]));
        assert!(g.rank(&"a".into(), &s) >= 10);
    }

    #[test]
    fn save_and_load_round_trip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-graph-{}", std::process::id()));
        let path = dir.join("graph");
        let s = stats(&[("libc", 100), ("itoa", 50), ("regex_syntax", 9000), ("regex", 4000), ("app", 2000)]);
        let mut g = build();
        g.save(&path)?;
        let mut h = Graph::load(&path)?;
        assert_eq!(h.len(), 5);
        assert_eq!(h.rank(&"libc".into(), &s), g.rank(&"libc".into(), &s));
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
