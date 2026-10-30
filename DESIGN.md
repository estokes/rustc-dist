# rustc-dist design

## Goals

- Keep every core in the farm busy without overcommitting any box, including
  when several agents run cargo on the same box.
- Use the toolchain installed on each box. Never ship compilers.
- Produce the same output a local build would: paths, `env!()`, pipelining.
- Per-project control over what stays local.

Non-goals: caching compile results, distributing link steps, boxes shared by
several users (see open questions).

## Process model

One process per box, `rustc-dist daemon` (today's `serve` merges into it). It
is at once:

- the box's admission controller: every rustc on the box, whether its job came
  from a local cargo or from another box, holds one of its slots;
- a farm member: publishes toolchain, slot classes, free counts and
  calibration under `BASE/NAME`;
- a farm client: sends its users' jobs to other boxes;
- a remote executor: runs other boxes' jobs in a sandbox.

Local and remote work draw from one slot pool, so nothing can oversubscribe the
box. It runs as a systemd user service and does not exit when idle, because it
serves remote jobs. The wrapper starts it if it is missing.

The wrapper never runs rustc without a grant. If it cannot reach or start the
daemon it fails the compile with a clear message. `RUSTC_DIST=off` is the only
bypass.

### Wrapper protocol

Wrapper to daemon: `Job { argv, env, cwd, cargo_cwd }`, where `cargo_cwd` is
`/proc/$PPID/cwd`.

Daemon to wrapper, either:

- `Run { cpus, jobserver, filter_metadata }`: the wrapper sets its CPU
  affinity, points `CARGO_MAKEFLAGS` at the box jobserver, forks and execs
  rustc, collects `wait4` rusage, sends `Ran { exit, cpu, wall, maxrss }`, and
  exits with rustc's code; or
- a remote result: `Artifact`/`Stderr`/`Stdout` lines, then `Exit(code)`. If
  the remote job fails, the daemon sends `Run` instead and the job reruns
  locally.

The wrapper spawns rustc itself so cargo's stdin, fds and environment reach
rustc untouched. Socket close ends the job: the slot is freed, and a remote
job is cancelled if its wrapper died.

Probe invocations (`-vV`, `--print`, input `-`) get `Run` at once and take no
slot.

## Slots

### Classes

Here a CPU means a logical CPU, i.e. a hardware thread. SMT siblings are
separate CPUs, each with its own slot. Compilers are memory bound, so the
second thread on a core gets real work done while the first waits on memory.

A box's CPUs are split into classes of equal speed, found by calibration (see
below) and overridable in config. On this laptop sysfs suggests three:

| class | cpus | max freq    |
|-------|------|-------------|
| P     | 4    | 4.7–4.8 GHz |
| E     | 8    | 3.7 GHz     |
| LP-E  | 4    | 3.3 GHz     |

A class has one slot per CPU. A job runs pinned to its class's CPU set (the
whole set, not one CPU).

### Jobserver

Each class has a FIFO jobserver (`--jobserver-auth=fifo:PATH`) holding one
token per CPU. Admitting a job takes a token, the job's codegen threads take
more from the same FIFO, and the token returns when the job exits. Rustc
threads per class never exceed the class's CPUs.

Cargo's own jobserver is never handed to rustc. Cargo's `jobs` setting only
caps how many wrappers wait. Set it at or above the farm's slot count.

### Memory

A job is admitted only if the predicted maxrss of running jobs plus its own
fits under a configured fraction of `MemTotal`. Predictions come from the cost
table.

## Calibration

Calibration runs once per power profile, the first time the daemon sees the
profile's name. On a laptop the desktop runs `rustc-dist profile NAME` when the
power profile changes (KDE can run a script on each switch). Names are the
user's choice. A box that never switches stays in the profile `default`. The
daemon remembers the last profile across restarts, so a new box calibrates
`default` the first time its daemon starts. `rustc-dist install` only sets up
the systemd user service. `rustc-dist profile NAME --recalibrate` measures a
profile again.

Switching to a profile that already has a calibration applies at once, to new
jobs. Jobs already running finish on the old profile's classes and
jobservers, which go away when their last job ends. Until then the box can be
oversubscribed by at most the jobs that were running at the switch. Switching
to a profile with no calibration holds new jobs. Once every running job is
done, the daemon calibrates, saves the result, and resumes. It doesn't try to
guess whether other work on the box will distort the numbers. Calibrate each
profile ahead of time on an idle box, with `rustc-dist profile NAME
--recalibrate`.

The calibration load is built into the binary: an immutable-chunkmap workload
that behaves like a compiler. Like a compiler's symbol tables and interners, it
is branchy, chases pointers and allocates heavily. It uses a fixed seed and a
fixed number of operations. It runs `insert_many`, `update_many`,
`remove_many`, `union`, `intersect`, `diff`, `get` and `range` over keys that
are expensive to compare, and keeps several versions of each map alive the way
a compiler holds snapshots. Its working set is well past L2.

Every box runs the same code from the same binary, so results compare across
boxes with no graphix checkout, no recorded job and no blob fetch. The run
takes seconds.

Calibration runs the load on every logical CPU at once, one thread pinned to each,
SMT siblings included. Both threads of a core run loaded, so they measure the
same and land in the same class. Their time already reflects sharing the core,
which is how they will run under a full farm. The
whole box is under load, so the numbers are sustained speed with shared caches,
memory bandwidth and SMT siblings all under contention, not single-core turbo.
The load repeats for about a minute so laptops drop out of their short boost
window. Each run is timed by its thread's CPU time, not wall time, so time
spent waiting for a CPU that other work holds doesn't count. Each CPU's time
is the median of its later repetitions. Per-thread
working set is a few tens of MB, so a box with many cores doesn't run out of
memory.

The classes come from those times. Sort the CPUs by time and start a new class
wherever two neighbours differ by more than about 8%. Two core types that
measure the same land in one class, which is right for scheduling. A class's
`calib_ms` is the median of its members' times.

`rustc-dist calibrate` measures and prints without saving. Each profile's
classes go in `~/.local/state/rustc-dist/profiles/NAME.toml`. In phase 2 the
daemon publishes each class at `BASE/NAME/class/N/` with `calib_ms`, `cpus` and
`free`.

Job costs are normalized by the calibration of the class each job ran on, so
one job history serves every profile.

Acceptance check, to run on an idle box: the time ratios between classes from
the chunkmap load must match, within about 10%, the ratios from a real rustc
compile run the same way (the `graphix-shell` crate under `--profile quick`,
one pinned copy per CPU). If they don't, tune the operation mix. This hasn't been run
yet: on 2026-10-30 this box was at load 119 from graphix tests and the fuzzer.

## Cost

Agentic work compiles the same units over and over: `graphix_types` hundreds
of times a day. Each unit's past runs predict its next one.

The unit of cost is one calibration run (cu). A job that used `c` CPU seconds
on class `k` costs `c / calib_s(k)` cu, the same number whichever box or class
ran it, so every run of a unit counts toward one record.

The job table is per daemon, persisted under `$XDG_STATE_HOME`. Its key is
crate name plus the `-C metadata` hash, cargo's identity for a unit (package,
version, features, profile, target). The hash doesn't change when sources are
edited. Each entry holds:

- an hdrhistogram of cost in milli-cu (2 significant figures, a few KB per
  unit);
- an hdrhistogram of wall time and of maxrss;
- the remote-pinned flag (phase 2).

Only successful compiles are recorded. Once a unit has 256 runs its counts are
halved, so the prediction follows the crate as it grows.

The predicted cost is the unit's median, which an occasional run on a
throttled box doesn't move. Admission uses the unit's p90 maxrss. A unit with
no history falls back to crate name plus opt-level plus emit kinds, then to the
median of all jobs.

A global hdrhistogram, the sum of every unit's cost histogram, gives the
median cost used for a unit with no history. It is rebuilt from the units
whenever the table is saved (every minute), so halving keeps it recent too.

## Build graph

Builds replay nearly the same trace, so the daemon learns which jobs read
which jobs' outputs and uses that to see what is coming.

Each rustc command line names its dependencies:
`--extern a=…/liba-8169c16474ca1297.rlib`. The file name is crate name plus
cargo's per-unit `-C extra-filename` hash, which the producing job was also
given. So a job's id is `crate` + `extra-filename`, and its dependencies are
the ids read off its `--extern` file names. The graph is exact, with no
timing noise.

A build script compiles as a rustc job but runs invisibly, and the crate using
its output doesn't name it in `--extern`. The build script compiles into
`…/build/pkg-HASH1` and its output lands in `…/build/pkg-HASH2/out`, which cargo
passes to the crate as `OUT_DIR`. Both map to the id `out:…/build/pkg`, which
links them.

The daemon records every admitted job's id and dependencies, persisted in
`$XDG_STATE_HOME/rustc-dist/graph`. Nodes not seen for 60 days are dropped.

A job's rank is its predicted cost plus the costliest chain of jobs that
depend on it: the work that waits on this job before the build can finish.
Ranks are cached and recomputed when the graph changes or each minute when
costs change. On a clean build of this crate the top ranks were the
`proc_macro2` build script, `unicode_ident`, `proc_macro2`, `quote`, `syn`,
and `regex_syntax`: several of them tiny, but everything waits on them.

For agentic rebuilds the graph is nearly exact about the future. Editing a
crate makes cargo rebuild it and everything depending on it, so when a rebuilt
unit arrives, its dependents are the jobs about to follow.

## Scheduling

The goal is to keep fast cores for the jobs the most work waits on, and to put
jobs with time to spare on slow cores.

1. Waiting jobs are ordered by rank, with aging: a job's priority doubles
   every 10 s it waits.
2. Each build's horizon is the largest rank among its waiting jobs and the
   remaining rank of its running ones. A build is the cargo process that ran
   the wrapper, so one agent's huge build doesn't push another's small one
   onto slow cores.
3. A job's slack is its build's horizon less its own rank: how much later it
   can finish without delaying the build.
4. For each class the daemon predicts when the job would finish there: when a
   slot frees (now, or the earliest predicted end of a job running in that
   class) plus the job's cost times the class's calibration. The job takes the
   slowest class on which it finishes within its slack of the earliest finish.
   A job with no slack takes the class where it finishes first, even if it must
   wait for it.
5. A job that waits for a class adds its predicted time, divided by the class's
   CPUs, to that class's expected wait, so later jobs don't all count on the
   same free slot.
6. A job past its predicted time is expected to run at least half again as long
   as it has, so an overdue estimate can't strand jobs waiting for its class.
7. A job is placed only after it has waited 20 ms. Cargo starts a wave of ready
   jobs within milliseconds, and the daemon should see the whole wave, including
   the job everything else waits on, before placing any of it. Younger jobs
   still count toward their build's horizon.

Remote slots (phase 2) are for jobs that may go remote and whose predicted
cost is above a remote cutoff, learned from each server's overhead (hash
exchange, upload, latency). Light jobs stay home.

A job is local-only if:

- it is incremental;
- it is named in `local`, or the cost table has learned to keep it local
  (remote-pinned);
- argv[0] is not a rustc (e.g. `clippy-driver`);
- its crate type links (bin, test, dylib, cdylib, proc-macro, staticlib);
- it uses `-C target-cpu=native`; or
- no server has a matching toolchain.

### History

The first scheduler mapped each job's cost percentile onto class bands and
never left a slot idle. On this crate's build, three of the ten heaviest units
reached P-cores: cargo's first wave of small crates took the P-cores before the
heavy crates became ready. With the graph, the gating chains (`regex_syntax`,
`regex_automata`, `aho_corasick`, `memchr`, most of `syn`) ran on P-cores, and
fast cores carried the most work. That was measured at load 160 from other
work, so build times say nothing yet; placement is all that was checked.

## Remote execution

### Farm

A daemon whose config has `base` joins the farm. It publishes one packed value
at `BASE/NAME/state`: protocol version, profile, whether it accepts jobs (not
while switching profile), its toolchains, and each class's calibration, CPUs
and running count. A changed load is republished at most every 100 ms. The
daemon watches every other box under `BASE` and takes jobs at
`BASE/NAME/jobs`, a netidx pack channel listener. Each job is one connection.

Toolchains are rustup's (`~/.rustup/toolchains/*`). A compiler is matched by
rustc's commit hash and host, and a job with `--target` also needs that
target's standard library installed. The client learns the key of the rustc
cargo runs from `rustc -vV`, cached by path and mtime.

A server admits a remote job only into a free token of the class asked for,
and only when none of its own jobs is waiting, so local work comes first. It
answers busy otherwise, and the client leaves that box alone for 2 s. An
admitted job holds a normal slot: it shares the class's jobserver and CPUs
with local jobs, and its cost goes into the server's history too.

### Inputs

Inputs are addressed by content. The client daemon caches
`(path, size, mtime, inode) -> blake3`. The input set is:

- Sources: every file the unit's previous dep-info
  (`<out-dir>/<crate><extra>.d`) lists, which includes files outside the
  package, plus everything in the package directories those files came from,
  so a newly added module is found. Without dep-info the package directory is
  walked whole, skipping `target` and hidden directories. A walk past 256 MB
  keeps the job local.
- The `OUT_DIR` tree and `-L` directories other than dependency ones. Static
  libraries get bundled into rlibs.
- Crates: the `--extern` files plus, through the build graph, those of every
  crate below them, since rustc loads the metadata of indirect dependencies.
  If a crate below was never seen compiling, its dependencies are unknown and
  the job stays local.
- Nothing under `/usr`, `/etc`, `/lib*`, `/bin`, `/sbin`, `/proc`, `/dev`,
  `/sys` or `/run`. The sandbox mounts the server's own, and a server needs its
  own system libraries anyway.

Each server keeps a read-only blob store under `~/.cache/rustc-dist`, evicting
the least recently used past `blob_cache_gb` but never anything used in the
last 10 minutes. The client sends a manifest of paths and hashes; the server
answers with the hashes it lacks; the client sends those in 1 MB chunks and
the server checks each blob's hash before storing it. Once a server is warm a
job sends a manifest and whatever sources changed.

### Sandbox

The job tree is hard links from the blob store, laid out at the client's
absolute paths. The server runs rustc under bwrap with:

- `--unshare-all` (no network, own pid namespace), `--die-with-parent`, and
  `--as-pid-1`. With rustc as pid 1, `wait4` on bwrap reports rustc's CPU
  time; behind bwrap's own init it reported 10 ms for 5 s of work.
- read-only binds of `/usr` and `/etc`, the host's `/bin`, `/lib*` and `/sbin`
  as they are (symlinks on merged-usr systems), and the toolchain sysroot;
- a tmpfs on `/tmp` unless client paths live there; nothing else of the host,
  so a checkout at the same path on the server can't be picked up;
- read-write binds of the job tree's top-level directories at `/`;
- the class FIFO bound in, and CPU affinity set before exec;
- argv and cwd unchanged; the client's env minus jobserver variables, `LD_*`,
  `PATH`, `TMPDIR` and the wrappers.

Because the paths are the client's, nothing is rewritten. Argv, env
(`env!("CARGO_MANIFEST_DIR")`), diagnostics, dep-info, debuginfo and panic
locations all name client paths.

### Outputs and pipelining

Cargo pipelines builds. It passes `--emit=dep-info,metadata,link
--json=...,artifacts` and starts dependents as soon as rustc prints
`{"artifact":"...rmeta","emit":"metadata"}`. rustc prints such a line for the
dep-info, the rmeta and the rlib.

The server reads rustc's stderr line by line. On an artifact line it sends the
file, then the line. The client writes the file (temp plus rename, keeping the
server's mode) and forwards the line to the wrapper at once. All other output
is held until rustc exits. The client accepts outputs only directly in
`--out-dir` and explicit `--emit`/`-o` directories.

### Failure

Any remote failure (non-zero exit, staging or sandbox error, dropped
connection) reruns on a local slot, and the held-back diagnostics are
discarded. If the rmeta line was already forwarded, the wrapper drops it from
the rerun's output. A unit that fails remotely and then builds locally twice is
kept local until the daemon restarts. Typical causes are proc macros that read
the network or the environment (sqlx), or an include of a file the client
never sent. Real compile errors get compiled twice. They usually fail fast.

## Config

Config is real TOML (`toml` + serde), parsed into two separate structs.

- User, `~/.config/rustc-dist.toml`: `memory_fraction`,
  `calibration_seconds`, and for a farm `base` (the netidx path every box
  publishes under), `name` (the host name by default) and `blob_cache_gb`.
  Classes live per profile in `~/.local/state/rustc-dist/profiles/`; edit
  those to override them. The netidx config is netidx's own (`NETIDX_CFG` or
  its default paths).
- Project, `rustc-dist.toml` at or above cargo's cwd (`/proc/$PPID/cwd`, which
  is where cargo looks for its own config): `local = [...]`. Starting from
  `CARGO_MANIFEST_DIR` does not work. For registry deps it points into
  `~/.cargo/registry`, and for every non-member, cargo runs rustc from that
  package's own directory.

## Security

Anyone who can open a job channel can run code as the server user, because
proc macros are client-supplied `.so` files. Three things limit that: netidx
permissions on `BASE`, a bwrap sandbox with no network and no server home, and
the client-side check on output paths.

## Incremental builds

Incremental is being turned off. Workspace crates can then go remote, and each
compile of a unit does the same work, so its histogram stays tight. Jobs that
still use `-C incremental` stay local, because the incremental cache lives on
the client.

## Earlier remote code

Phase 1 removed the first sketch's remote modules (`farm.rs`, `remote.rs`,
`server.rs`, `stage.rs`, `identity.rs`). They are in git at 418df61. Phase 2
can reuse `farm.rs` discovery and `identity.rs` toolchain matching from there.
The argv and env rewriting, `Rewriter`, and the per-job RPC/session are not
coming back.

## Phases

1. Built 2026-10-30. Single box, no network: merged daemon, mandatory grants,
   classes, FIFO jobservers and pinning, calibration per power profile, job
   table and histograms, build graph and rank placement, TOML config,
   `status`. Several agents on one box share cores without thrashing, and the
   jobs the most work waits on get the fast cores.
2. Built 2026-10-30. Remote: farm membership over netidx, toolchain
   matching, blob store and manifest protocol, inputs from dep-info and the
   build graph, bwrap sandbox, pipelined outputs, local fallback and remote
   pinning. See "Farm test" below.
3. Cost tables and graphs shared across the farm. A measured comparison of
   build times with and without rank placement, on an idle box.

## Farm test (2026-10-30)

Two daemons on this laptop, each with its own state, talking through a private
netidx resolver: A with 2 slow CPUs, B with 4 fast ones. A built this crate
twice. The first build only taught A costs and the graph; unknown jobs are
predicted below the 1 s remote cutoff, so nothing left. On the clean second
build A sent 18 jobs to B: 16 compiled there, 1 failed and was rebuilt locally,
1 got a busy answer. The build succeeded and the binary ran. Wall times on a
box at load 100+ from other work mean nothing.

What the test found:

- Remote CPU time read 10 ms for every job, because bwrap's init hid rustc
  from `wait4`. Fixed with `--as-pid-1`; the end-to-end test now checks it.
- The failure was `cc`, whose modules live in `src/target/`: the input walk
  skipped every directory named `target`. It now skips directories holding
  cargo's `CACHEDIR.TAG`. Rebuilt, `cc` compiled on B.
- A `-sys` crate's `-L /usr/lib` made staging walk all of `/usr/lib`. System
  directories are now never sent; the sandbox mounts the server's.
- Too little went remote. Slack sent heavy jobs to A's slow local class, and
  the small jobs that can't go remote queued behind them while B idled. Slack
  now chooses only among local classes, and a job goes remote whenever that
  finishes sooner than its local choice.
- B answered busy with free slots published, because each remote rustc's
  codegen threads took the rest of the class's tokens. That is correct; the
  client backs off for 2 s. Publishing free tokens instead of admitted jobs
  would avoid the round trip, but tokens can't be counted without taking them.

Known gaps:

- Not yet run across two machines.
- One connection per job. A persistent connection per pair of boxes would
  save the setup if it shows up in measurements.
- Upload and output file I/O run on async worker threads.
- The first build of a project stays local, since its costs are unknown.
- A server's `status` lists units from its own graph, so jobs it served for
  others don't appear there, though they are in its job history.

## Guesses to test on the quiet cluster

Every number below is a guess. None has been tested on an idle box.

| guess | value | where |
|---|---|---|
| wait before placing a job, to see cargo's whole wave | 20 ms | `src/sched.rs:44` |
| priority doubles while waiting every | 10 s | `src/sched.rs:37` |
| re-place waiting jobs every | 250 ms | `src/sched.rs:41` |
| an overdue job runs at least half again its elapsed time | 1/2 | `src/sched.rs:245` |
| neighbouring CPUs split into classes above | 8% | `src/calibrate.rs:19` |
| calibration length | 60 s | `src/config.rs:22` |
| calibration keys per run (about 0.25 s on a P-core) | 2^15 | `src/calibrate.rs:20` |
| halve a unit's history at | 256 runs | `src/stats.rs:18` |
| cost of a job with no history at all | 1 cu | `src/stats.rs:20` |
| share of RAM predicted peaks may use | 80% | `src/config.rs:22` |
| keep graph nodes unseen for | 60 days | `src/graph.rs:18` |
| only send jobs predicted to take at least | 1 s | `src/sched.rs` `REMOTE_MIN_US` |
| predicted cost of going remote | 200 ms | `src/sched.rs` `REMOTE_OVERHEAD_US` |
| leave a busy box alone for | 2 s | `src/sched.rs` `BUSY_BACKOFF` |
| keep a unit local after remote-fail, local-pass | 2 times | `src/sched.rs` `PIN_AFTER` |
| blob cache | 20 GB | `src/config.rs` |

Experiments, roughly in order:

1. The calibration acceptance check: chunkmap class ratios against a real
   rustc compile run the same way.
2. The same clean build and the same edit-and-rebuild, with rank placement and
   with plain heaviest-first placement. This needs a switch that turns ranks
   off; add it then.
3. Vary the gather window (0, 20, 100 ms) on clean builds, where the first wave
   matters most.
4. Several concurrent builds, to see whether per-build slack keeps a small
   build fast next to a big one.

## Open questions

1. One user per box is assumed. Several users would need a system daemon, and
   the wrapper would have to read input files, since it runs as the user.
2. Should a box prefer its own users' jobs over remote ones?
3. Link jobs: mold uses every CPU. Should a link job take more than one slot,
   or should mold's threads be capped?
4. Does cargo tolerate a second metadata artifact line? If so,
   `filter_metadata` can go.
5. The daemon only governs rustc. Test binaries and fuzzers, which caused the
   load of 119 above, bypass it. Should the class jobservers be offered to
   them too?

## Checked on this box (2026-10-30)

- Cargo runs the wrapper for `rustc -vV` and for the stdin `--print` probe.
- Rustc's cwd is the workspace root for members and the package directory for
  non-members.
- Pipelined jobs get `--emit=dep-info,metadata,link` and
  `--json=diagnostic-rendered-ansi,artifacts,future-incompat`.
- `CARGO_MAKEFLAGS` carries `--jobserver-auth=3,5`. Rustc warns "failed to
  connect to jobserver" when those fds are invalid, which is what the server
  sees today.
- Rustc accepts `--jobserver-auth=fifo:PATH` without a warning and returns
  every token.
- Under clippy, workspace members run as `clippy-driver rustc ...`.
- bwrap 0.13.1 works, and so do unprivileged user namespaces.
