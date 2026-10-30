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

A box's CPUs are split into classes of equal speed, detected from sysfs (core
type from `/sys/devices/cpu_core` and `cpu_atom`, `cpuinfo_max_freq`,
`cpu_capacity`), overridable in config. This laptop:

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

Calibration runs once per core class, when the server is installed
(`rustc-dist install`), and never again. Install also detects the classes and
sets up the systemd user service.

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

Install runs the load on one thread pinned to one CPU of each class, with all
classes running at the same time so the box is under load rather than in
single-core turbo. The wall time per class goes into the box's state file. The
daemon publishes it as `BASE/NAME/class/N/calib_ms`, next to `cpus` and `free`.

Acceptance check, to run on an idle box: the time ratios between classes from
the chunkmap load must match, within about 10%, the ratios from a real rustc
compile pinned the same way (the `graphix-shell` crate under
`--profile quick`). If they don't, tune the operation mix. This hasn't been run
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

- an hdrhistogram of cost in milli-cu (1 to 3.6M, 2 significant figures, a few
  KB per unit);
- an hdrhistogram of wall time and of maxrss;
- the remote-pinned flag.

The predicted cost is the unit's median, which an occasional run on a
throttled box doesn't move. Admission uses the unit's p90 maxrss. A unit with
no history falls back to crate name plus opt-level plus emit kinds, then to the
median of all jobs.

A second hdrhistogram, over all finished jobs (two rotating windows), sets the
percentile bands used in scheduling. It reflects the workload as it actually
arrives, so a unit that runs a hundred times counts a hundred times.

## Scheduling

Heavy jobs go to strong cores and light jobs to weak cores, and a slot never
sits idle while a job waits.

1. Capacity curve: take the classes the job may use (local only, or the whole
   farm), sort them slow to fast and weight them by slot count. Each class gets
   a percentile band. On this laptop alone: LP-E 0–25, E 25–75, P 75–100.
2. A job's percentile is the histogram rank of its predicted cost. Its ideal
   class is the one whose band contains that percentile.
3. When a slot frees or a job arrives, walk the waiting jobs heaviest first
   (with aging, so light jobs are not starved by a stream of heavy ones). Give
   each job the free slot nearest its ideal class, preferring the faster one on
   ties. If no slot is free, the job waits.
4. Remote slots are only for jobs that may go remote and whose predicted cost
   is above the remote cutoff. The cutoff is learned from each server's
   overhead (hash exchange, upload, latency). Light jobs stay home.

A job is local-only if:

- it is incremental;
- it is named in `local`, or the cost table has learned to keep it local
  (remote-pinned);
- argv[0] is not a rustc (e.g. `clippy-driver`);
- its crate type links (bin, test, dylib, cdylib, proc-macro, staticlib);
- it uses `-C target-cpu=native`; or
- no server has a matching toolchain.

Later: let a heavy job wait for a strong slot when waiting and then running on
it beats running now on a weak one. That needs estimates of when running jobs
will finish. The daemon has these for its own jobs.

## Remote execution

### Inputs

Inputs are addressed by content. The client daemon caches
`(path, size, mtime, inode) -> blake3`. Outputs it received from a server are
hashed from the bytes already in hand.

The input set is:

- Sources: the unit's previous dep-info (`<out-dir>/<crate><extra>.d`) if
  present. It is exact and includes files outside the package. Otherwise, walk
  `CARGO_MANIFEST_DIR`, skipping `target` and `.git`.
- The `OUT_DIR` tree and `-L native=` directories. Static libraries get bundled
  into rlibs.
- Crates: the `--extern` files plus everything they depend on, directly or
  indirectly. The daemon sees every compile on its box, so it records each
  artifact's externs and can expand those dependencies itself. For artifacts
  it never saw, every rmeta and rlib in the `-L dependency=` directories goes
  in. Only hashes cross the wire, and each file is uploaded once per server.

Each server keeps a read-only blob store (LRU, size cap). At job start the
client sends a manifest `[(path, hash, mode)]`. The server answers with the
hashes it lacks, the client sends those in chunks, then sends go. Once a server
is warm, a job costs a manifest plus whatever sources changed.

Transport: one persistent netidx channel per pair of boxes, carrying many
jobs. This replaces the per-job RPC and per-job published session.

### Sandbox

The job tree is hardlinks from the blob store, laid out at the client's
absolute paths. The server runs rustc under bwrap with:

- `--unshare-all` (no network), `--die-with-parent`;
- read-only binds of `/usr /lib /lib64 /bin /etc` and the toolchain sysroot;
- tmpfs on `/tmp` and `/home`, so nothing in the server's home leaks in (a
  checkout at the same path on the server cannot be picked up by accident);
- read-write binds of the job tree at the client's paths;
- the class FIFO bound in, and CPU affinity set before exec;
- argv and cwd unchanged; the client's env minus jobserver variables, `LD_*`
  and `RUSTC_WRAPPER`; `PATH=/usr/bin:/bin`.

Because the paths are the client's, nothing is rewritten. Argv, env
(`env!("CARGO_MANIFEST_DIR")`), diagnostics, dep-info, debuginfo and panic
locations all name client paths.

### Outputs and pipelining

Cargo pipelines builds. It passes `--emit=dep-info,metadata,link
--json=...,artifacts` and starts dependents as soon as rustc prints
`{"artifact":"...rmeta","emit":"metadata"}`. The current code forwards that
line live but writes outputs only after rustc exits. A dependent can therefore
start before the rmeta exists on the client.

The server reads rustc's stderr line by line. On an artifact line it sends the
file, then the line. The client writes the file (temp plus rename, keeping the
server's mode), then forwards the line. All other output is held until exit.
The outputs are the files named by artifact lines, with a diff of the out-dir
as a fallback. The client accepts outputs only under `--out-dir` and `--emit`
paths.

### Failure

Any remote failure (non-zero exit, sandbox error, dropped connection) reruns
on a local slot, and the held-back diagnostics are discarded. A unit that fails
remotely but passes locally is marked remote-pinned in the cost table. Typical
causes are proc macros that read the network or the environment (sqlx), or an
include of a file the client never sent. If the metadata line was already
forwarded, the wrapper drops it from the rerun's output (`filter_metadata`).
Real compile errors get compiled twice. They usually fail fast.

## Config

Config is real TOML (`toml` + serde), parsed into two separate structs.

- User, `~/.config/rustc-dist.toml`: `base`, `name`, a `rustc` override, class
  overrides, the memory fraction.
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

## Keep and replace

Keep: `farm.rs` discovery (extended with classes), the daemon accept loop,
`identity.rs`, `argv.rs`, the socket framing in `protocol.rs`.

Replace: the argv and env rewriting in `stage.rs`, `protocol::Rewriter`, the
snapshot diff and per-job RPC/session in `server.rs`, the parser in
`config.rs`, and the wrapper's paths that exec rustc directly.

## Phases

1. Single box, no network: merged daemon, mandatory grants, classes, FIFO
   jobservers and pinning, `install` with calibration, job table and
   histograms, scheduler, TOML config. This is useful on its own. Several
   agents on one box share cores without thrashing, and heavy crates land on
   P cores.
2. Remote: blob store and manifest protocol, dependency tracking, bwrap
   sandbox, pipelined outputs, fallback and remote pinning.
3. Waiting for a better slot, cost tables shared across the farm, fairness
   between builds.

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
