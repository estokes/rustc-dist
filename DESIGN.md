# rustc-dist design

## Goals

- Keep every core in the farm busy without overcommitting any box, including
  when several agents run cargo on the same box.
- Use the toolchain installed on each box. Never ship compilers.
- Produce the same output a local build would: paths, `env!()`, pipelining.
- Per-project control over what stays local.

Non-goals: caching compile results (a server keeps the crate files it builds
only so later jobs needn't send them), boxes shared by several users (see open
questions).

## Process model

One process per box, `rustc-dist daemon` (today's `serve` merges into it). It
is at once:

- the box's admission controller: every rustc on the box, whether its job came
  from a local cargo or from another box, holds one of its slots;
- a farm member: publishes toolchains, slot classes, free counts and
  calibration in its row of `BASE/status`;
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

A job is light if it has been measured to keep fewer than 1.5 threads busy
after its front end; every other job, including one never measured, is
heavy. A light job runs pinned to one class's CPU set (the whole set, not one
CPU). A heavy job runs on every CPU of the box.

### Jobserver

Each class has a FIFO jobserver (`--jobserver-auth=fifo:PATH`) holding 1.5
tokens per CPU (`tokens_per_cpu`). Mild overcommit keeps CPUs busy while jobs
wait on I/O; around 4x would be lost to context switching. Admitting a light
job takes a token from its class, its codegen threads take more from the same
FIFO, and the token returns when the job exits.

A heavy job gets a FIFO of its own, which the daemon lends tokens into from
the class FIFOs, fastest class first. Admitting it takes one token, which it
holds without reading, and lends one spare. Rustc reads a token when it wants
another thread, which the daemon can't see, so the spare is how a job asks.
Every 100 ms the daemon checks each heavy job's FIFO: if the spare is gone it
lends twice what the job holds, up to its codegen units; spares left unused for
a whole tick go back, all but one. When the job ends, every token it borrowed
goes back to the class it came from, so a killed rustc can't leak them. Light
and heavy jobs together never hold more than the classes' tokens.

A class whose last light job ends is refilled, to recover tokens a killed
rustc leaked, but only with the tokens heavy jobs don't hold.

Cargo's own jobserver is never handed to rustc. Cargo's `jobs` setting only
caps how many wrappers wait. Set it at or above the farm's slot count.

### Memory

Boxes overcommit RAM into compressed swap. Capacity is RAM plus half of
compressed swap (`compressed_swap_share`). For zram, compressed swap is the
size of its swap devices; for zswap, it is the pool limit times the compression
ratio zswap is getting, or 3:1 until it has stored something, and never more
than the swap behind it. Everything is counted as uncompressed data.

The daemon measures the box rather than trusting its own bookkeeping, so other
processes count. Use is what RAM holds (`MemTotal - MemAvailable`), less the
RAM that compressed swap takes, plus all swapped data at its uncompressed size.
Each job's footprint is the RSS plus swap of its processes: the wrapper and
everything under it, or bwrap and everything under it for another box's job.
The daemon walks every thread's children, since rustc starts the linker from
its compiler thread.

A job fits if the box's use, plus what running jobs are predicted to add
(predicted peak less current footprint), plus its own predicted peak, is within
capacity. Memory and footprints are sampled every 250 ms while jobs run, and
again before placing a job if the last sample is older. A finished job's
footprint leaves the box's use at once. With nothing running here, one job
runs whatever its size, so the box can't sit idle behind a job bigger than it.

A job that doesn't fit can still go to another box. If it can't, jobs after it
don't run here either, so smaller jobs can't starve it, and the queue is looked
at again every 250 ms, since other processes may free memory. A server refuses
an offer that doesn't fit.

Predictions come from the cost table: the 90th percentile of the unit's (or its
crate's) recorded peaks, or the median of all jobs for a new crate. A recorded
peak is the larger of rustc's `maxrss` and the most footprint seen while it ran
here. `maxrss` counts only resident pages, so a job swapped out at its peak
would otherwise be recorded small, and overcommit would grow on itself.

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
measure the same land in one class, which is right for scheduling. Each run
also counts its instructions, and a class's rate is its members' mean
instructions per CPU-µs. Without counters a run is taken to retire 697 M
instructions, what a release build's run retires.

`rustc-dist calibrate` measures and prints without saving. Each profile's
classes go in `~/.local/state/rustc-dist/profiles/NAME.toml`, and boxes publish
each class's rate, CPUs and free tokens.

The calibrated rate is only a starting point. Every job that runs pinned to a
class adds its instructions and CPU time to the class's rate, which weighs the
calibration as 10 CPU-seconds of jobs and halves the weight of older jobs every
60 CPU-seconds. Class speeds so come to reflect rustc rather than chunkmap,
which replaces the acceptance check once planned here (comparing chunkmap's
class ratios against a real compile's). What each class has learned is saved
per profile in `~/.local/state/rustc-dist/rates/NAME.toml`, with the job stats
every minute and on shutdown, and when the profile changes, so a restarted
daemon carries on from it. Each entry names its class's CPUs and calibrated
rate; a new calibration, or a class edited by hand, starts from calibration
again.

## Cost

Agentic work compiles the same units over and over: `graphix_types` hundreds
of times a day. Each unit's past runs predict its next one.

The unit of cost is an instruction: the user-space instructions retired by
rustc and every thread and process under it, counted by the CPU (both core
types on a hybrid CPU). The same job retires about the same count whichever
box, class, profile or load ran it, so every run of a unit counts toward one
record. A box that can't count instructions estimates them from CPU time at
its class's rate. A predicted time is cost over rate.

The job table is per daemon, persisted under `$XDG_STATE_HOME`. Its key is
crate name plus the `-C metadata` hash, cargo's identity for a unit (package,
version, features, profile, target). The hash doesn't change when sources are
edited. Each entry holds:

- hdrhistograms (2 significant figures) of cost in millions of instructions,
  of the front end's instructions, of width, of wall time and of peak memory;
- runs counted by where they ran and for whom.

The front end is the compile up to rustc's `.rmeta` line, which runs on one
thread. Its instructions are the job's instructions times its share of CPU
time, read from `/proc` when the line appears, since a thread's instruction
count only reaches the job's total when the thread exits. Width is the CPU
time after the front end over the wall time after it: how many threads codegen
kept busy. A job with no `.rmeta` line (a binary) records its width over the
whole run.

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
   class) plus the job's cost over the class's rate. The job takes the
   slowest class on which it finishes within its slack of the earliest
   finish, choosing among classes with a free slot if any of them will do.
   It never waits for a slow class while a faster one sits idle: a short job
   on a fast core delays a later critical job by at most its own run time,
   while an idle core is lost. A job with no slack takes the class where it
   finishes first, even if it must wait for it.
5. A job that waits for a class adds its predicted time, divided by the class's
   CPUs, to that class's expected wait, so later jobs don't all count on the
   same free slot. A class whose last token a pass hands out frees no sooner
   than the quickest job that pass placed there.
6. A job past its predicted time is expected to run at least half again as long
   as it has, so an overdue estimate can't strand jobs waiting for its class.
7. A job is placed only after it has waited 20 ms. Cargo starts a wave of ready
   jobs within milliseconds, and the daemon should see the whole wave, including
   the job everything else waits on, before placing any of it. Younger jobs
   still count toward their build's horizon.

A job that may go remote considers other boxes if it has no history (neither
the unit nor its crate has run before), if it is predicted to take at least
1 s on the fastest local class, or if no local class has a free slot left, so
it would wait here. A job with history chooses among local classes
as above, and goes to another box instead if it would finish there sooner,
counting 200 ms for the trip. A job with no history has nothing to predict
with, so it takes free capacity: the fastest local class with a token, else the
fastest other box with one free, else it waits for the local class expected to
free first. Light jobs with history stay home.

A job is local-only if:

- it is incremental;
- it is named in `local`, or the cost table has learned to keep it local
  (remote-pinned);
- argv[0] is not a rustc (e.g. `clippy-driver`);
- it links and `remote_links` is off;
- it uses `-C target-cpu=native`;
- it splits debug info (`-C split-debuginfo` other than `off`) into files
  rustc doesn't report as artifacts, so they wouldn't come back; or
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

A daemon whose config has `base` (say `/build`) joins the farm. The farm's
netidx namespace:

- `BASE/status/NAME/COLUMN`: a row per box, which the netidx browser draws as
  a table.
- `BASE/api/NAME/jobs`: where the box takes jobs, a netidx pack channel
  listener. Each job is one connection.
- `BASE/units/NAME/UNIT/COLUMN`: the units the box has run (see below).

A box's state is eight columns of its status row, each its own value:
protocol version, profile, a version number, its toolchains, and per class
(as arrays) CPUs, G instructions/s, free tokens, and how many ms until its
next token is predicted to free. A change updates the columns it touches in
one batch, which subscribers get whole and in order, so no box sees half a
state, and unchanged columns aren't sent. Free tokens
are counted in the class's jobserver FIFO with `FIONREAD`, which doesn't take
them, so they leave out the tokens rustc's codegen threads hold. The
prediction is the soonest predicted end of a job holding one of the class's
tokens, this box's or another's: 0 with a token free, unknown while the box
takes no offers. It changing alone doesn't republish; it goes out with the
next change, and clients count it down from when the state arrived. A box
publishes no free tokens while it switches profile or its own jobs wait. The
scheduler counts after every event, and every 100 ms while jobs run, since
codegen threads take and return tokens without telling it. A changed load is
republished at most every 100 ms. The daemon lists `BASE/status` for other
boxes and subscribes to each one's state columns.

Toolchains are rustup's (`~/.rustup/toolchains/*`). A compiler is matched by
rustc's commit hash and host, and a job with `--target` also needs that
target's standard library installed. The client learns the key of the rustc
cargo runs from `rustc -vV`, cached by path and mtime.

A server admits a remote job only into a free token of the class asked for,
or for a heavy job the fastest class with one, and only when none of its own
jobs is waiting, so local work comes first. It
answers busy otherwise. An admitted job holds a normal slot: it shares the
class's jobserver and CPUs with local jobs, and its cost goes into the
server's history too.

Each answer, accepted or busy, names the version of the server's state that
first reflects it, and the server publishes that version right after. A
client offers a class only while the jobs it sent there that the box's state
doesn't count yet are fewer than the free tokens published. A sent job stops
counting once the box has accepted it and published the version its answer
named. After a busy answer the client skips the box until it publishes the
version named. So a client neither counts its own jobs twice nor offers into
tokens it has already taken, and every refusal makes the server publish a
fresh state. Versions start from zero when a box restarts, which clients see
as a version going down. Memory isn't published, so a box can still refuse
for memory.

`status` lists every unit the box has run, with runs split three ways: this
box's builds run here, this box's builds run elsewhere, and other boxes'
builds run here. Units outside this box's build graph have no rank. Units sent
elsewhere also show the median time their sent runs spent on files and the MB
they sent. Each class shows this box's jobs, other boxes' jobs and free tokens.

In a farm each box also publishes what `status` shows, for the netidx browser.
Beside its state columns, its status row has figures for people: jobs
waiting, running and serving, CPUs, total G instructions/s, per class jobs
running, serving and tokens lent, and memory. So `BASE/status` shows the
whole farm at a glance. Its units go in a table of their own at
`BASE/units/NAME`, with `status`'s columns. Values are typed numbers, so the
browser sorts them. These refresh every 2 s and send only cells that
changed. Building a status takes the scheduler a few ms, which is why it
isn't faster.

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
  The walk stops at proc macros: their dependencies are linked into the
  `.so`, and rustc never loads them (checked on this box, for a library using
  a proc macro and a binary linking that library). If a crate below was never
  seen compiling, its dependencies are unknown and the job stays local. A job
  that links reads every crate's rlib rather than its `.rmeta`; see "Linking"
  below.
- Nothing under `/usr`, `/etc`, `/lib*`, `/bin`, `/sbin`, `/proc`, `/dev`,
  `/sys` or `/run`. The sandbox mounts the server's own, and a server needs its
  own system libraries anyway.

Each server keeps a read-only blob store in `cache_dir` (`~/.cache/rustc-dist`
by default; a tmpfs keeps it in memory), evicting the least recently used past
`cache_gb` down to 90% of it, except blobs a running job reads, which are
pinned from its manifest until it ends. The client sends a manifest of paths and hashes; the server
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

### Linking

Binaries, tests, proc macros and build scripts link the rlibs below them, so
another box needs every one of them. Until it links, though, rustc reads only
each rlib's metadata (its `lib.rmeta` member), and rustc and the linker `mmap`
their inputs, so they can't read from pipes. So the copy overlaps the compile:

- For each rlib whose `.rmeta` is beside it on the client, the manifest lists
  the rlib with the hash of that `.rmeta`. A server that lacks the rlib asks
  for the `.rmeta` before rustc starts and the rlib after, and until it comes
  puts a stand-in at the rlib's path: an archive holding just the `.rmeta`
  as its `lib.rmeta` member. Rustc accepts raw metadata there.
- The sandbox puts a stand-in linker, a short shell script, first on `PATH`
  (`/run/rustc-dist/bin`), named for the linker rustc will run: `-C linker`,
  or `cc`. It inherits one end of a socket pair as fd 9, writes a byte to say
  it is waiting, and reads until the server shuts its end, which the server
  does once every late rlib has replaced its stand-in (by rename, so a reader
  sees one file or the other). Then it runs the next program of that name on
  `PATH`. Rustc puts its sysroot directories first on the linker's `PATH`,
  which hold no `cc`.
- The client sends the late rlibs while it receives rustc's output.

Without LTO, rustc passes rlibs to the linker by path and reads none of
their object code itself. LTO, static libraries and dylibs do read it, as
does a linker named by its path (it can't be put behind the stand-in) or a
non-GNU target, whose default linker isn't `cc`: those jobs receive every
rlib before rustc starts. So does an rlib with no `.rmeta` beside it. An rlib
the server already has is placed at once.

Each sent run records the bytes it sent and the time it spent other than
compiling: staging, sending inputs before rustc starts, the linker waiting at
the gate, and receiving the last outputs. `status` shows their medians per
unit next to its wall time, so it shows whether sending a link job pays off.
Linking on other boxes can be turned off with `remote_links = false`.

### Outputs and pipelining

Cargo pipelines builds. It passes `--emit=dep-info,metadata,link
--json=...,artifacts` and starts dependents as soon as rustc prints
`{"artifact":"...rmeta","emit":"metadata"}`. rustc prints such a line for the
dep-info, the rmeta and the rlib.

The server reads rustc's stderr line by line. On an artifact line it sends the
file, then the line. The client writes the file (temp plus rename, keeping the
server's mode), records its hash so staging a later job needn't read it, and
forwards the line to the wrapper at once. All other output is held until rustc
exits. The client accepts outputs only directly in `--out-dir` and explicit
`--emit`/`-o` directories.

The server keeps every `.rlib`, `.rmeta` and `.so` it builds in its blob
store, hard linked from the job's tree. A job whose dependencies built on the
same box has little to send: on a warm box, a binary sends its own sources.

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

- User, `~/.config/rustc-dist.toml`: `compressed_swap_share`, `tokens_per_cpu`,
  `calibration_seconds`, and for a farm `base` (the netidx path every box
  publishes under), `name` (the host name without its domain by default), `bind` (where other
  boxes reach this one, in netidx's syntax, since netidx's default is often
  loopback only), `cache_dir`, `cache_gb` and `remote_links` (on by default).
  Classes live per profile in `~/.local/state/rustc-dist/profiles/`; edit
  those to override them. Their learned rates live in `rates/` beside it. The netidx config is netidx's own (`NETIDX_CFG` or
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
the client-side check on output paths. The client names the program the
stand-in linker stands in for; the server accepts only a plain file name.

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
3. Capacity scheduling, planned 2026-10-30; see below.
4. Cost tables and graphs shared across the farm. A measured comparison of
   build times with and without rank placement, on an idle box.

## Capacity scheduling (phase 3)

The goal: work on the laptop and have every box in the house share the
compiling, with the biggest jobs on the 9950X3D2. This is a small cluster
built for the purpose. Every box has the same toolchains and the same system
libraries and linker, kept so by the sysadmin.

### Stage 1: measure work in instructions (built 2026-10-30)

- A job's work is the user-space instructions retired by rustc and every
  thread and process under it, counted by the CPU's performance counters
  (summed over both core types on a hybrid CPU). The same job retires about
  the same count on any core, box, power profile or load, unlike CPU time.
  Where counters aren't available, work is estimated from CPU time.
- Each class has a rate: instructions per CPU-µs. Calibration counts the
  chunkmap run's instructions to seed it; jobs pinned to the class then
  refine it, so class speeds come to reflect rustc rather than chunkmap.
  A class's capacity is its CPUs times its rate; a box's is the sum.
- A job's shape: its front end, the single-threaded part before rustc
  prints its `.rmeta` line, as a share of its CPU time; and its width, the
  average threads busy after that (CPU over wall time). A job that prints
  no `.rmeta` line (a binary) has only a width.
- Costs, ranks and predictions move to instructions; boxes publish rates.

### Stage 2: heavy jobs span classes (built 2026-10-30)

- Every job is heavy until measured light. Light jobs stay pinned to their
  class; heavy jobs run on every CPU with tokens lent from the classes (see
  Jobserver above). Class pools hold 1.5 tokens per CPU.
- A heavy job's predicted time is its front end at the fastest class's rate
  plus the rest at the mean rate per CPU, over the threads it can get:
  min(its width, the box's CPUs). Its front end runs first, on one thread,
  so the short jobs holding tokens when it starts are gone by the time its
  codegen wants them. Its width is as measured, or its codegen
  units if never measured. It goes wherever it finishes first, this box or
  another, and on another box it runs on every CPU too; a server starts it
  holding a token from its fastest class and a spare. With no history to
  predict by, it takes this box if a token is free, else the other box with
  the most free capacity, else waits here.
- Light jobs choose among classes by slack, as before.

### Stage 3: binaries on other boxes, artifacts cached (built 2026-10-31)

- Binaries, tests, proc macros and build scripts go to other boxes like
  libraries. They need every rlib below them, which the build graph lists,
  and the server's linker, which is the same as the client's by assumption.
- A server keeps the crate files it builds in its blob store.
- The copy overlaps the compile, behind stand-in rlibs and a stand-in linker
  (see "Linking" above). Checked by the end-to-end test: a binary built
  against a stand-in for a dependency's rlib that the client sends two
  seconds late links once it arrives, and runs. A stand-in has no code, so
  linking it fails; that is how the test knows the linker waited.
- Each sent run records the bytes it sent and the time it spent other than
  compiling; `status` shows them. Whether sending link jobs pays off is to be
  measured on the real network. `remote_links` turns them off.

### Stage 4: placement by capacity

- A heavy job with history also considers a box with no token free, starting
  when that box predicts its next token frees, and waits for it if it
  finishes there soonest (built 2026-10-31). It is offered once the box shows
  a free token; until then no other job from here takes that box's tokens,
  since none show free.
- Still to do: count each job's predicted transfer time rather than a flat
  200 ms.

## Farm test (2026-10-30)

Two daemons on this laptop, each with its own state, talking through a private
netidx resolver: A with 2 slow CPUs, B with 4 fast ones. A built this crate
twice. The first build only taught A costs and the graph; unknown jobs were
then predicted below the 1 s remote cutoff, so nothing left. On the clean second
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
  codegen threads took the rest of the class's tokens. Boxes now publish free
  tokens, counted with `FIONREAD`, and the version in each answer replaced the
  2 s backoff. The same work found that a client counted each of its jobs on
  a box twice (in the box's running count and its own sent count), so one
  client could fill at most half a box.

Known gaps:

- One connection per job. A persistent connection per pair of boxes would
  save the setup if it shows up in measurements.
- Upload and output file I/O run on async worker threads.
- A job stays local if the build graph doesn't know every crate below it,
  which happens when a project's target directory was built before the daemon
  first saw it.
- Memory isn't published, so a box can still refuse an offer for memory. A
  job run on another box is recorded with that box's `maxrss`, which misses
  swapped pages.
- The input walk doesn't descend below a `.so`, which is right for proc
  macros. A Rust dylib's dependencies aren't sent either, so a job using one
  fails remotely and builds here.
- An rlib's `lib.rmeta-link` member, which real rlibs have, is missing from
  stand-ins. Rustc hasn't needed it in tests so far.

## Cluster test (2026-10-31)

Three boxes in one netidx domain, publishing under `/build/rustc-dist` (now
`/build`, laid out as in "Farm" above), each
listening on the LAN (`bind`), with the cache on tmpfs
(`/tmp/rustc-dist-cache`, 16 GB). The laptop is on wifi; the others are wired.

| box | CPUs | RAM | calibrated classes (G instructions/s per CPU) |
|---|---|---|---|
| washu-chan (laptop) | Ultra X7 358H, 16 | 62 GB | 4 at 3.95, 8 at 2.20, 4 at 1.49 |
| ryouko | 9950X3D2, 32 | 123 GB | 30 at 4.51, 2 at 3.97 |
| aieka | i9-10980XE, 36 | 62 GB | 36 at 0.59 |

Stable rustc was the same commit on all three. Nightly differed, so nightly
builds stay local. The test is a clean graphix dev build (its own profile,
opt-level "s"), launched from washu-chan with `RUSTC_WRAPPER=rustc-dist` and
256 cargo jobs into a fresh target directory.

| graphix dev build | wall |
|---|---|
| washu-chan alone, plain cargo | 2m 27s |
| farm from washu-chan, first build | 2m 11s |
| farm from washu-chan, second build | 1m 28s |
| ryouko alone, plain cargo | 1m 19s |

The first build sent 147 jobs (141 to ryouko, 6 to aieka), and the laptop
spent 1119 s of CPU. The second sent 141 (140 to ryouko, 1 to aieka),
including both `graphix_types` units, and the laptop spent 377 s. No remote
job failed in either, nor in 623 more from builds stopped part way. File moves
averaged 167 ms per job to ryouko in the first build and 103 ms in the
second; aieka's took 0.4 to 1.2 s, for reasons not yet known.

What the test found:

- The build is bound by its longest chain, not by capacity. Over the units
  both builds ran there are 6944 G instructions of work, 2188 G of it on the
  chain `graphix_types` → `graphix_compiler` → `graphix_package_core` →
  `graphix_shell` and what lies below it. The whole farm could do the work in
  about 21 s and ryouko alone in about 28 s, but the chain needs roughly 50 s
  on ryouko even with every codegen thread busy. Boxes past ryouko barely
  matter for this build.
- `graphix_types` (962 G instructions, 17% front end) took 61 s on an idle
  ryouko, averaging 2.8 threads after its front end. Heavy units the laptop
  ran itself, under load, show 1.0. Whether rustc can't spread that crate's
  codegen or its tokens ran short is not yet known.
- Calibration underrates aieka. Per thread, its load runs at 3.64 G/s on one
  CPU, 2.29 with one thread on each of 18 cores, and 0.56 with all 36
  threads, so SMT halves the box's throughput under it. The load's working
  set fits ryouko's large L3 and not aieka's. Instruction counting works
  there, in bwrap too, with `perf_event_paranoid` at 2.
- aieka can't learn its way out. A server spreads an offer with no class over
  every CPU, and spread jobs don't teach a class (`finished` in
  `src/sched.rs`). Light jobs go to aieka only if its class looks fastest,
  which at 0.59 it never does, so it only gets heavy jobs and keeps its
  calibration. ryouko learned from light jobs: class 0 went from 4.51 to
  8.00. The laptop's classes went to 5.51, 4.14 and 3.33.
- With no history, the first build sent jobs that compiled in 20 to 80 ms.
  Unknown units count as 1 G instructions, which aieka's rate made look
  longer than the 1 s cutoff.
- A restarted daemon was listed twice in the resolver for about seven
  minutes, because dropping the publisher at exit didn't finish clearing it.
  The daemon now shuts its publisher down before exiting, but that still
  leaves its paths behind on these boxes: netidx sends the clear only to the
  default resolver (the workstation's own, serving `/local`), not to the
  domain resolver `/build` is referred to, so a stopped box stays listed
  until that resolver's writer ttl, about five minutes. Recorded in netidx
  as CR `resolver-clear-01` on `ResolverWrite::clear`.
- Publishers on these workstations listen on loopback by default. `bind` in
  the config sets where other boxes reach a box.
- Slack held short jobs for the LPE class while P and E sat idle. In a build
  sampled every 100 ms or so, 100 to 120 jobs waited for the first 18 s while
  LPE was 100% busy and E had 10 to 12 of its 12 slots free, 3 to 50% busy.
  Over the build, jobs waited about 31 s while P or E had a free slot. Each
  was a short job off the long chain with hundreds of seconds of slack, and
  short jobs never left the box, being under the 1 s cutoff. Now a job takes
  a free class within its slack before waiting for a slower one, and a job
  that would wait here may go to another box whatever its size. Afterwards
  the build took 1m 20s, against 1m 28s before and 1m 19s on ryouko alone.
  It sent 384 jobs (292 to ryouko, 92 to aieka) instead of 141, and the
  laptop spent 277 s of CPU instead of 377. All three classes were close to
  100% busy for the first 11 s, with at most 12 jobs waiting, while ryouko
  ran up to 35 of its jobs at once and aieka up to 47. Jobs waited with a
  P or E slot free for about 2 s of the build, not 31.
- `graphix_types` sometimes went to aieka, taking 101 s instead of 60 s on
  ryouko, and the build 1m 58s. With short jobs now going away, ryouko was
  often full when it became ready. A box with no free token wasn't a
  candidate, and a heavy job was predicted with as many threads as a box had
  tokens free, so aieka, with dozens free, looked able to run all its
  codegen threads at once while the laptop, nearly full, looked limited to
  one. Now boxes publish when their next token frees, a heavy job may wait
  for a busy box, and it is predicted with as many threads as a box has
  CPUs.
- Asking for `status` every 100 ms added about 13 s to a build (93 s
  sampled against 80 s not), so the status reply slows the scheduler.
- Cargo holds about 4 open files per running job, so 256 jobs fail with "Too
  many open files" under the usual soft limit of 1024 (this test's shell had
  a higher one). 128 fits, still covers the farm's 126 slots, and builds as
  fast; the help text recommends it.

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
| cost of a job with no history at all | 1 G instructions | `src/stats.rs` `UNKNOWN_COST_MI` |
| a class's calibration counts as | 10 CPU-s of jobs | `src/sched.rs` `RATE_PRIOR_US` |
| older jobs count half toward a class's rate after | 60 CPU-s | `src/sched.rs` `RATE_HALF_LIFE_US` |
| share of compressed swap predicted peaks may use beyond RAM | 50% | `src/config.rs` `compressed_swap_share` |
| zswap compression ratio before it has stored anything | 3:1 | `src/sys.rs` `ZSWAP_RATIO` |
| sample memory and job footprints every | 250 ms | `src/sched.rs` `MEASURE` |
| keep graph nodes unseen for | 60 days | `src/graph.rs:18` |
| only send jobs predicted to take at least | 1 s | `src/sched.rs` `REMOTE_MIN_US` |
| predicted cost of going remote | 200 ms | `src/sched.rs` `REMOTE_OVERHEAD_US` |
| count free tokens and lend to heavy jobs every | 100 ms | `src/sched.rs` `SAMPLE` |
| jobserver tokens per CPU | 1.5 | `src/config.rs` `tokens_per_cpu` |
| a job is light below this many threads busy | 1.5 | `src/sched.rs` `LIGHT_WIDTH_C` |
| a heavy job's spare is taken: lend it | twice what it holds | `src/sched.rs` `Layout::lend` |
| keep a unit local after remote-fail, local-pass | 2 times | `src/sched.rs` `PIN_AFTER` |
| cache size | 20 GB | `src/config.rs` `cache_gb` |

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
