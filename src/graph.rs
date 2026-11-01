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
    mem,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

/// Nodes not seen for this many days are dropped when the graph loads.
const KEEP_DAYS: u32 = 60;
const MAGIC: &[u8; 8] = b"rdgraph3";
/// A graph whose crate files are absolute paths. It loads as if each file
/// were beside its node.
const MAGIC_ABSOLUTE: &[u8; 8] = b"rdgraph2";

/// A dep as its node keeps it. `file` is the crate file relative to the
/// node's own directory, so one node serves every target directory its
/// crate is built in.
#[derive(Debug, PartialEq, Eq, Pack)]
struct Edge {
    id: ArcStr,
    file: Option<ArcStr>,
}

#[derive(Pack)]
struct Node {
    id: ArcStr,
    exact: ArcStr,
    coarse: ArcStr,
    deps: Vec<Edge>,
    seen_day: u32,
}

/// `dep` as an edge of a node in `dir`.
fn edge(dir: &str, dep: &Dep) -> Option<Edge> {
    let file = match &dep.file {
        Some(f) => Some(relative(dir, f)?),
        None => None,
    };
    Some(Edge { id: dep.id.clone(), file })
}

/// Whether `edges` of a node in `dir` are `deps`.
fn same_deps(dir: &str, edges: &[Edge], deps: &[Dep]) -> bool {
    let mut path: LPooled<String> = LPooled::take();
    edges.len() == deps.len()
        && edges.iter().zip(deps).all(|(e, d)| {
            e.id == d.id
                && match (&e.file, &d.file) {
                    (None, None) => true,
                    (Some(rel), Some(file)) => {
                        resolve(dir, rel, &mut path);
                        path.as_str() == file.as_str()
                    }
                    _ => false,
                }
        })
}

/// `file` as a path from `dir`. Both are absolute and normalized, as cargo
/// writes them.
fn relative(dir: &str, file: &str) -> Option<ArcStr> {
    if !dir.starts_with('/') || !file.starts_with('/') {
        return None;
    }
    let mut base = dir.trim_end_matches('/');
    let mut ups = 0;
    loop {
        if let Some(rest) = file.strip_prefix(base).and_then(|r| r.strip_prefix('/')) {
            let mut rel: LPooled<String> = LPooled::take();
            for _ in 0..ups {
                rel.push_str("../");
            }
            rel.push_str(rest);
            return Some(ArcStr::from(rel.as_str()));
        }
        base = base.rsplit_once('/')?.0;
        ups += 1;
    }
}

/// The path `rel` names from `dir`, written to `path`.
fn resolve(dir: &str, rel: &str, path: &mut String) {
    path.clear();
    path.push_str(dir.trim_end_matches('/'));
    for c in rel.split('/') {
        if c == ".." {
            path.truncate(path.rfind('/').unwrap_or(0));
        } else {
            path.push('/');
            path.push_str(c);
        }
    }
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

    /// A unit whose crate files can't be placed relative to its directory
    /// isn't recorded.
    pub fn record(&mut self, unit: &Unit) {
        let Some(dir) = unit.dir.as_deref() else { return };
        let day = today();
        if let Some(n) = self.nodes.get_mut(&unit.id)
            && same_deps(dir, &n.deps, &unit.deps)
        {
            if n.seen_day != day || n.exact != unit.exact {
                n.seen_day = day;
                n.exact = unit.exact.clone();
                n.coarse = unit.coarse.clone();
                self.dirty = true;
            }
            return;
        }
        let Some(deps) = unit.deps.iter().map(|d| edge(dir, d)).collect::<Option<Vec<_>>>() else { return };
        let old_deps = match self.nodes.get_mut(&unit.id) {
            Some(n) => {
                n.seen_day = day;
                n.exact = unit.exact.clone();
                n.coarse = unit.coarse.clone();
                mem::replace(&mut n.deps, deps)
            }
            None => {
                let node = Node {
                    id: unit.id.clone(),
                    exact: unit.exact.clone(),
                    coarse: unit.coarse.clone(),
                    deps,
                    seen_day: day,
                };
                self.nodes.insert(unit.id.clone(), node);
                Vec::new()
            }
        };
        let had = |id: &ArcStr| old_deps.iter().any(|d| &d.id == id);
        let has = |id: &ArcStr| unit.deps.iter().any(|d| &d.id == id);
        for dep in old_deps.iter().filter(|d| !has(&d.id)) {
            if let Some(ds) = self.dependents.get_mut(&dep.id) {
                ds.retain(|d| d != &unit.id);
            }
        }
        for dep in unit.deps.iter().filter(|d| !had(&d.id)) {
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
    /// depend on it: the work that waits on this job, in millions of
    /// instructions.
    pub fn rank(&mut self, id: &ArcStr, stats: &Stats) -> u64 {
        if let Some(r) = self.ranks.get(id) {
            return *r;
        }
        // Marks `id` as in progress, so a cycle in stale data ends here.
        self.ranks.insert(id.clone(), 0);
        // CR claude for claude: [bug/medium] A rank adds each job's work, front plus rest,
        // but Sched::dispatch turns rank differences into slack as time on one thread of
        // the fastest class (`time_us(horizon - rank_mi, fastest_rate)`), so a wide job
        // counts as if it ran on one thread. In `rustc-dist status` on washu-chan today,
        // graphix_package_core (5aaddb61cfdea6bd) has 98.6 s of work, 2% of it front end,
        // at width 9.6: about 12 s on that class with its threads (25 s measured wall
        // on every CPU), yet it adds 98.6 s to every chain through it; graphix_types adds
        // 243 s for about 120 s. A build whose horizon runs through such jobs gives its
        // other jobs tens of seconds of slack they don't have, and they take slow
        // classes. Fix: rank by predicted duration, the front end at the fast rate plus
        // the rest over min(width, CPUs), as Stats::time_us computes at the fastest
        // place; status's "Rank and work are seconds on one thread" changes with it.
        // (graph-01)
        // 2026-11-01 claude: Eric ruled (2026-11-01): rank by predicted duration on the
        // fastest place, front end at the fast rate plus the rest over min(width, CPUs)
        // (a).
        let own = match self.nodes.get(id) {
            Some(n) => stats.predict(&n.exact, &n.coarse).cost_mi(),
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
    /// of indirect dependencies too. Each is found from the crate above it
    /// as this job has it, so in the job's own target directory. A proc
    /// macro's dependencies are linked into its `.so`, and rustc never loads
    /// them. A crate below it that was never seen compiling, whose
    /// dependencies are unknown, is the error.
    pub fn inputs(&self, deps: &[Dep]) -> Result<Vec<ArcStr>, ArcStr> {
        let mut files = Vec::new();
        let mut seen: LPooled<HashMap<ArcStr, ()>> = LPooled::take();
        let mut stack: LPooled<Vec<(ArcStr, ArcStr)>> = LPooled::take();
        stack.extend(deps.iter().filter_map(|d| Some((d.id.clone(), d.file.clone()?))));
        let mut path: LPooled<String> = LPooled::take();
        while let Some((id, file)) = stack.pop() {
            if seen.insert(id.clone(), ()).is_some() {
                continue;
            }
            if !file.ends_with(".so") {
                let dir = file.rsplit_once('/').map_or("", |(d, _)| d);
                for e in &self.nodes.get(&id).ok_or_else(|| id.clone())?.deps {
                    let Some(rel) = &e.file else { continue };
                    if !seen.contains_key(&e.id) {
                        resolve(dir, rel, &mut path);
                        stack.push((e.id.clone(), ArcStr::from(path.as_str())));
                    }
                }
            }
            files.push(file);
        }
        Ok(files)
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
        let (absolute, mut body) = match (data.strip_prefix(MAGIC), data.strip_prefix(MAGIC_ABSOLUTE)) {
            (Some(body), _) => (false, body),
            (None, Some(body)) => (true, body),
            (None, None) => bail!("{} is not a graph file", path.display()),
        };
        let nodes = Vec::<Node>::decode(&mut body).map_err(|e| anyhow!("{}: {e}", path.display()))?;
        let oldest = today().saturating_sub(KEEP_DAYS);
        let mut graph = Self::new();
        for mut node in nodes.into_iter().filter(|n| n.seen_day >= oldest) {
            if absolute {
                for file in node.deps.iter_mut().filter_map(|e| e.file.as_mut()) {
                    if let Some((_, name)) = file.rsplit_once('/') {
                        *file = ArcStr::from(name);
                    }
                }
            }
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
    use crate::stats::Decay;

    /// A unit in the target directory `/t`.
    fn unit(id: &str, deps: &[&str]) -> Unit {
        unit_in("/t", id, deps)
    }

    /// A unit whose crate files go in `dir`, with its deps beside it. A dep
    /// named `*_macro` is a proc macro.
    fn unit_in(dir: &str, id: &str, deps: &[&str]) -> Unit {
        let file = |d: &str| {
            let ext = if d.ends_with("_macro") { "so" } else { "rmeta" };
            format_compact!("{dir}/lib{d}.{ext}").as_str().into()
        };
        Unit {
            exact: ArcStr::from(id),
            coarse: ArcStr::from(id),
            id: ArcStr::from(id),
            dir: Some(ArcStr::from(dir)),
            deps: deps.iter().map(|d| Dep { id: ArcStr::from(*d), file: Some(file(d)) }).collect(),
            codegen_units: 16,
        }
    }

    fn stats(costs: &[(&str, u64)]) -> Stats {
        let mut s = Stats::new(Decay::default());
        for &(id, cost_mi) in costs {
            s.seed(&unit(id, &[]), 0, cost_mi, 100, 1);
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
        assert_eq!(g.inputs(&unknown.deps), Err(ArcStr::from("never_built")), "the crate never seen is named");
    }

    #[test]
    fn inputs_stop_at_proc_macros() {
        let mut g = build();
        g.record(&unit("derive_macro", &["regex", "never_built"]));
        g.record(&unit("model", &["derive_macro", "itoa"]));
        let mut files = g.inputs(&unit("app", &["model"]).deps).unwrap();
        files.sort();
        assert_eq!(files, ["/t/libderive_macro.so", "/t/libitoa.rmeta", "/t/liblibc.rmeta", "/t/libmodel.rmeta"]);
    }

    #[test]
    fn inputs_come_from_the_jobs_own_target_directory() {
        // Two checkouts build the same crates the same way, so they share
        // ids. The other one recorded the crates below app last.
        let mut g = Graph::new();
        for (id, deps) in [("libc", &[][..]), ("regex_syntax", &["libc"]), ("regex", &["regex_syntax"])] {
            g.record(&unit_in("/a/target/debug/deps", id, deps));
        }
        g.record(&unit_in("/b/target/debug/deps", "app", &["regex"]));
        g.record(&unit_in("/a/target/debug/deps", "regex", &["regex_syntax"]));
        let mut files = g.inputs(&unit_in("/b/target/debug/deps", "app", &["regex"]).deps).unwrap();
        files.sort();
        assert_eq!(
            files,
            [
                "/b/target/debug/deps/liblibc.rmeta",
                "/b/target/debug/deps/libregex.rmeta",
                "/b/target/debug/deps/libregex_syntax.rmeta"
            ]
        );
    }

    #[test]
    fn a_proc_macro_below_a_target_crate_is_in_the_host_directory() {
        let host = "/a/target/debug/deps";
        let target = "/a/target/x86_64-unknown-linux-gnu/debug/deps";
        let model = Unit {
            deps: vec![Dep { id: "derive_macro".into(), file: Some(format_compact!("{host}/libderive_macro.so").as_str().into()) }],
            ..unit_in(target, "model", &[])
        };
        let mut g = Graph::new();
        g.record(&model);
        let app = unit_in("/b/target/x86_64-unknown-linux-gnu/debug/deps", "app", &["model"]);
        let mut files = g.inputs(&app.deps).unwrap();
        files.sort();
        assert_eq!(
            files,
            ["/b/target/debug/deps/libderive_macro.so", "/b/target/x86_64-unknown-linux-gnu/debug/deps/libmodel.rmeta"]
        );
    }

    #[test]
    fn moving_a_dep_to_another_directory_is_a_change() {
        let mut g = Graph::new();
        g.record(&unit_in("/a", "app", &["regex"]));
        g.dirty = false;
        g.record(&unit_in("/a", "app", &["regex"]));
        assert!(!g.dirty, "nothing changed");
        let moved = Unit {
            deps: vec![Dep { id: "regex".into(), file: Some("/elsewhere/libregex.rmeta".into()) }],
            ..unit_in("/a", "app", &[])
        };
        g.record(&moved);
        assert!(g.dirty, "regex moved");
        g.record(&unit_in("/a", "regex", &[]));
        let mut files = g.inputs(&unit_in("/c", "x", &["app"]).deps).unwrap();
        files.sort();
        assert_eq!(files, ["/c/libapp.rmeta", "/elsewhere/libregex.rmeta"]);
    }

    #[test]
    fn a_graph_of_absolute_paths_loads_with_each_dep_beside_its_node() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rustc-dist-graph-abs-{}", std::process::id()));
        fs::create_dir_all(&dir)?;
        let path = dir.join("graph");
        let node = |id: &str, deps: &[&str]| Node {
            id: ArcStr::from(id),
            exact: ArcStr::from(id),
            coarse: ArcStr::from(id),
            deps: deps
                .iter()
                .map(|d| Edge { id: ArcStr::from(*d), file: Some(format_compact!("/a/deps/lib{d}.rmeta").as_str().into()) })
                .collect(),
            seen_day: today(),
        };
        let nodes = vec![node("libc", &[]), node("regex", &["libc"])];
        let mut buf = BytesMut::new();
        buf.put_slice(MAGIC_ABSOLUTE);
        nodes.encode(&mut buf).map_err(|e| anyhow!("{e}"))?;
        fs::write(&path, &buf)?;
        let g = Graph::load(&path)?;
        let mut files = g.inputs(&unit_in("/b/deps", "app", &["regex"]).deps).unwrap();
        files.sort();
        assert_eq!(files, ["/b/deps/liblibc.rmeta", "/b/deps/libregex.rmeta"]);
        fs::remove_dir_all(&dir)?;
        Ok(())
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
