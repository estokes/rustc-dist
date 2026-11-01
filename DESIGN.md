# rustc-dist design

## Goals

- Keep every core in the farm busy without overcommitting any box, including
  when several agents run cargo on the same box.
- Use the toolchain installed on each box. Never ship compilers.
- Produce the same output a local build would: paths, `env!()`, pipelining.
- Get the most out of the build configuration the user chose. Never change
  it: flags, linker and linker options stay as cargo passes them.
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
  calibration in its row of `BASE/vN/status`;
- a farm client: sends its users' jobs to other boxes;
- a remote executor: runs other boxes' jobs in a sandbox.

Local and remote work draw from one slot pool, so nothing can oversubscribe the
box. It runs as a systemd user service and does not exit when idle, because it
serves remote jobs. A wrapper that finds no daemon starts one: through
systemd if its user service is installed, so a wrapper can't start a second
daemon while systemd restarts its own and leave the service failing to take
the lock; on its own otherwise.

The wrapper never runs rustc without a grant. If it cannot reach or start the
daemon it fails the compile with a clear message. `RUSTC_DIST=off` bypasses
it, and so does running as root: the daemon won't run as root, as it runs
other boxes' proc macros.

### Wrapper protocol

The wrapper sends `PROTOCOL` as four raw bytes, then one message:

- `Ask::Compile { unit, build, job }`: the unit (its keys, its id in the build
  graph, the jobs whose outputs it reads, its codegen units), cargo's pid,
  and, when the compile may run on another box, a `Job`: rustc's path,
  arguments and environment, the working directory, the target, the paths it
  reads and writes, and what it reads of the crates below it. The wrapper
  reads the project's `rustc-dist.toml` itself, at or above cargo's cwd
  (`/proc/$PPID/cwd`), and sends no `Job` for a crate the project keeps local.
- `Ask::Probe` for `-vV`, `--print` without a source file, or input `-`. The
  daemon answers `Trivial` and the wrapper runs rustc without a slot.
- `Ask::Status` and `Ask::Profile`, from the command line.

The daemon answers a compile with either:

- `Slot { cpus, jobserver, filter_metadata }`: the wrapper sets its CPU
  affinity, points `CARGO_MAKEFLAGS` at the job's jobserver, spawns rustc,
  and exits with rustc's code. If rustc succeeds it first sends `Finished`:
  its wall time, maxrss, when it printed its `.rmeta` line, and the crate
  files it wrote, which the daemon hashes for later remote jobs.
  `filter_metadata` drops rustc's `.rmeta` line, which cargo already has from
  a remote run that failed; or
- a remote run's output as `Output` messages, then `Exit(code)`. Artifact
  lines come as rustc prints them; the rest waits for rustc to exit. If the
  remote run fails, the daemon sends `Slot` instead and the job reruns here.

The wrapper spawns rustc itself so cargo's stdin, fds and environment reach
rustc untouched. Socket close ends the job: the slot is freed, and a remote
job is stopped on the other box, whether its wrapper hangs up while the job
waits for a place, while its files are staged, or while it runs there.

A wrapper follows a daemon that restarts. A daemon that drains answers the
jobs it holds with `Restarting`; one that dies just hangs up. Either way the
wrapper connects and asks again, starting the daemon if it must, and gives
up only once failures that each came within a second of asking have gone on
for 30 s, so a job that waited long in the queue still follows the restart.
If a remote run had already passed cargo its `.rmeta` line, the wrapper drops
that line from the rerun's output, here or on another box.

A daemon refuses a client of another `PROTOCOL`. In place of a reply's length
it sends `0xFFFFFFFF`, longer than any message, then its own `PROTOCOL`, and
the client fails, saying which protocol each speaks. The daemon never
restarts for a client: a deploy restarts it, and `rustc-dist install` stops a
running daemon of any version by sending `0xFFFFFFFF` in place of `PROTOCOL`.

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

A job is light if it has been measured to hold fewer than 1.5 jobserver
tokens on average after its front end; every other job, including one never
measured, is heavy. A light job runs pinned to one class's CPU set (the whole set, not one
CPU). A heavy job runs on every CPU of the box.

### Jobserver

Each class has 1.5 tokens per CPU (`tokens_per_cpu`), a count the daemon
keeps. Mild overcommit keeps CPUs busy while jobs wait on I/O; around 4x would
be lost to context switching.

Every job gets a FIFO jobserver of its own (`--jobserver-auth=fifo:PATH`),
which the daemon lends tokens into: from its class for a light job, from every
class, fastest first, for a heavy one. Admitting a job takes one token, which
it holds without reading. Rustc reads a token when it wants another thread,
which the daemon can't see, so a spare in its jobserver is how it asks. The
spare is lent on credit: no class's token backs it until rustc takes it, so a
job that never asks, as most light jobs don't, ties up only the token it
holds. Every 100 ms the daemon checks each job's FIFO. A spare taken is
backed by a class token, or, if every class is out, owed until one frees or the
job gives a token back. A job that took every token offered is lent twice what
it holds, up to its codegen units. Spares left unused for a whole tick go
back, all but one, which stays on credit. A job with no spare is offered one
on credit when its classes have a free token. When the job ends, every token
it borrowed goes back to the class it came from, so a killed rustc can't leak
them. Jobs never hold more than the classes' tokens, but for spares taken
before a class had a token to back them.

The daemon counts the tokens each job holds, the one it started with
included, at every tick, from when its rustc starts. That is how wide a job
is (see Cost).

Cargo's own jobserver is never handed to rustc. Cargo's `jobs` setting only
caps how many wrappers wait. Set it at or above the farm's slot count.

### Memory

Boxes overcommit RAM into compressed swap. Capacity is RAM plus half of the
data compressed swap can still take (`compressed_swap_share`). For zram, that
is its swap devices' free space; for zswap, the pool limit times the
compression ratio zswap is getting, or 3:1 until it has stored something,
less what the pool holds, and never more than the free space of the swap
behind it, since every page zswap holds keeps a slot there.

The daemon measures the box rather than trusting its own bookkeeping, so other
processes count. Use is what RAM holds (`MemTotal - MemAvailable`), which
counts compressed swap's data at the RAM it takes. Swap on disk takes no RAM.
Shared pages in RAM (`Shmem`), mostly tmpfs files, count as free while swap on
disk has room for them, since the kernel can move them there. A tmpfs that
holds target directories would otherwise look like jobs' memory. Its pages
already in zram count at their compressed size: without a writeback device,
zram never moves them to disk.
Each job's footprint is the anonymous and shared memory its processes hold
in RAM, plus their swap (`RssAnon + RssShmem + VmSwap`): the wrapper and
everything under it, or bwrap and everything under it for another box's job.
File-backed pages are left out, as the box's use leaves out page cache: most
are rustc's and LLVM's code, which every rustc on the box shares (65 to 93 MB
of a small rustc's 107 to 124 MB of RSS on washu-chan).
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

Calibration runs the load on every core at once, one thread pinned to each
core's first CPU, with its SMT siblings idle. A core's other CPUs join its
class at its rate. Two threads sharing a core run slower than one alone, and
the more cache a load wants the slower: on aieka (i9-10980XE) the load ran at
2.29 G instructions/s per core with one thread each and 0.56 per thread with
two, a box's total halved. That loss depends on the load, so calibration
leaves it, like rustc's IPC differing from the load's, to the factors places
learn from real jobs (see Cost). The whole box is under load, so the numbers are
sustained speed with shared caches and memory bandwidth under contention,
not single-core turbo.
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

The calibrated rate is only a starting point: each class's place in the cost
table learns a factor on its calibrated time from the jobs that run there (see
Cost). That replaces the acceptance check once planned here (comparing
chunkmap's class ratios against a real compile's).

## Cost

Agentic work compiles the same units over and over: `graphix_types` hundreds
of times a day. Each unit's past runs predict its next one. See Stage 5 below
for why the table works in wall time.

A place is a class of a box, where light jobs run, or all of a box's CPUs,
where heavy jobs run: this box's (`here/PROFILE/K`, `here/PROFILE/all`) or
another's (`NAME/PROFILE/...`). Each has its calibrated rates: its class's,
or for all CPUs the fastest class's, where a front end runs, and the mean per
CPU. A place's factor scales calibrated time to time there. It is a curve over
load: one value per bucket at 0, ¼, ½, ¾, 1, 1½, 2, 3, 4, 6 and 8 runnable
tasks per CPU, interpolated between them and growing with load past the last.

A box's load is its runnable tasks per CPU (`procs_running` in `/proc/stat`),
so test binaries and other work the daemon doesn't run count. The count swings
from one moment to the next, so it is sampled every 250 ms into a moving
average in which a sample counts `1/e` as much after 10 s, and placement,
publishing and the samples a run takes all use the average. A run's load is
the mean of the average while it ran. Boxes publish their load.

A unit's size is the work it takes, in millions of the calibration load's
instructions: its front end, the compile up to rustc's `.rmeta` line, which
runs on one thread, and the rest, in thread-work. A run that took `t` at a
place under load `l` says its front end was its front-end time times the
place's fast rate, and the rest its token-µs after the front end times the
place's rate per CPU, both over the place's factor at `l`. Width is the
tokens it held on average after its front end. Predicted time at a place is
its front end at the fast rate plus the rest at the rate per CPU over the
threads it can get, min(width, the place's CPUs for a heavy job), times the
place's factor at the load there now plus the threads the job adds.

The size and width a place's prediction uses come from the unit's own runs
at that place when it has any, else from its runs at other places of that
box (another class, the same profile), and only for a box it never ran on
from all its runs. Each run counts at its place's factor for the load it
saw, so the prediction is the time it took there scaled by the place's curve
to the load now: the curve's shape matters, not its level. On 2026-11-01
graphix_types (`836c9a8b3f8d481b`) had nine runs on ryouko of 55 s and two
here of 108 and 146 s. Its size came mostly from ryouko, and washu-chan's
factor for heavy jobs, learned from every unit, turned it into 54 s here:
it stayed here and took 146 s. It also held 2.8 tokens on ryouko and 4
here, which one width per unit hid in the factor.

Every estimate is a weighted median of recent samples, weights halving with
age, so old samples fade but no one sample, however long, moves the middle.
What a run teaches:

- The unit: its last 16 runs, each its work at its place's calibrated speed
  and its load. Its size is the weighted median of the size each implies at
  its place's current factor, and its width the median of their widths.
- Its place's factor at its load, if the unit also ran elsewhere or under
  another load: a comparison, its time over what its size from those other
  runs predicts at the place's calibrated speed. It weighs that prediction
  times `n/(n+1)` for `n` other runs, so long jobs count most. A unit seen at
  one place and load only says nothing about that place against the others.
  Each load bucket keeps its last 32 comparisons.

Weights halve every `half_life_s` of wall-clock time, a day by default: a box's
speed doesn't change fast, and the curves model load, so what a place learned
stays good that long. A bucket's factor is the weighted median of its
comparisons and of the starting curve: the calibrated speed, flat up to one
task per CPU and then slower in proportion as CPUs time-share. The curve counts
as a sample of 3 s of runs that never ages, as much as a few typical jobs, so
a few runs that agree outweigh it, and it outweighs runs grown old: a place
with no recent comparisons goes back to its calibration. Idle is where the
calibration ran, so with no runs there the idle bucket keeps the
calibration's factor, with the curve's weight. More load never makes a run
faster, so buckets whose factors fall as load grows are pooled into their
mean, weighted by what each learned (pool adjacent violators). A bucket
nothing has reached lies between the nearest buckets that have; one past the
last that has follows the starting curve's slope. A place whose calibration
changes starts over.

On 2026-11-01 washu-chan ran builds at 2 to 14 runnable tasks per CPU and
none idle. Its fastest class learned a factor of 0.42 at 2 tasks per CPU,
4.00 at 4 and 1.34 at 8: runnable tasks are a poor measure of how much a
rustc slows there. Without pooling, predictions here swung threefold as the
load moved between buckets. Without the idle anchor, the idle bucket copied
the 0.21 learned at 2 tasks per CPU relative to the curve, and status, which
shows speeds against this box's fastest class idle, put ryouko at 0.74 of it.
Calibrated, ryouko's cores are 3.3 times as fast. Only ratios between factors are
fixed by data, since every unit could be bigger and every place faster; the
calibration's weight anchors the scale.

A sent run's time is everything the client waits for, from offering the job to
its last output. Its part other than compiling (staging and sending inputs,
the linker waiting for late rlibs, receiving outputs) is its own term, added
in full. It depends most on the bytes the box lacks, so a job whose inputs the
box already holds, such as crates it just built, is cheap to send there:

- At submit, the client finds the crate files the job would send (its
  `.rmeta`s, or the rlibs a job that links reads) with their sizes, and their
  hashes where it took them since the files last changed. It reads no file.
- For each box it keeps a view of the crate files the box holds: what jobs
  sent there read and the libraries they built there, which the server keeps,
  trimmed least recently used first to the cache size the box publishes, as
  the server trims its store. A file whose hash isn't known counts as
  missing everywhere.
- The term is the weighted median of the unit's own last 8 sends to that
  place, else of the last 64 sends there of jobs that link as it does or
  doesn't (a job that links sends every rlib below it), else a guess of
  200 ms, each adjusted for the kB the box lacks now against the kB that send
  moved, at the place's µs per kB: the weighted median of the slopes between
  pairs of those 64 whose kB differ by at least 256 kB (Theil-Sen), with
  50 MB/s as one more slope. kB are counted before compression, so a place's
  µs per kB takes in how well what crosses compresses.
- While jobs wait here, a box with a token free that has run nothing from here
  and had no heartbeat for a minute gets one: half a MB or a MB, in turn, of
  the start of the rustc-dist binary, which compresses about as object code
  does, and which the box answers and drops. Its time is a send of each
  kind there, so a box whose recent sends judged
  it badly gets fresh samples though no job goes. A heartbeat takes no token
  and runs nothing.

Its weights halve every `copy_half_life_s`, an hour by default, since the
network a laptop is on changes as it moves. The server reports the token-µs and
load it measured, counted from when rustc started, so time waiting for inputs
isn't counted as the job's.

The table is per daemon, persisted under `$XDG_STATE_HOME` (`jobs.hdr`). Its
key is crate name plus the `-C metadata` hash, cargo's identity for a unit
(package, version, features, profile, target). The hash doesn't change when
sources are edited. A unit with no history falls back to crate name plus
opt-level plus emit kinds, then to the median size of all units. Each entry
also holds hdrhistograms (2 significant figures) of wall time, peak memory,
and sent runs' copy time and bytes, halved at 256 runs, and runs counted by
where they ran and for whom. Admission uses the unit's p90 peak. Only
successful compiles are recorded. A cell unrun for about 60 days, as long as
the build graph keeps a node, is dropped. A table from before runs were timed
loads as seed history: each unit's median instruction count, front end and
width become one run at a place with factor 1.

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

Cargo's hash doesn't depend on where the workspace is, so every checkout of a
workspace builds its crates under the same file names, even checkouts of
different branches. Agents working in several checkouts at once give the same
ids to crates in different target directories. A node therefore keeps each
dependency's crate file relative to its own directory, and a job finds each
crate below it from the crate above as the job has it, so in its own target
directory. Checkouts share nodes and ranks, and each job reads only its own
files. A proc macro under a `--target` build sits in the host directory, which
the relative path reaches.

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
   class) plus its predicted time there (see Cost). The job takes the
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
1 s on one thread of the fastest local class, or if it would wait here: no
local class has a free slot left, or its predicted memory doesn't fit. A job
with history chooses among local classes as above, and goes to another box
instead if it would finish there sooner, counting the time its sends there
spend moving files (see Cost). That includes a box with no token free, from
when it predicts one frees: with the laptop busy running tests, a job waits for
another box rather than start here and take longer. A job waiting for a box
adds its predicted time over the class's CPUs to that class's wait for jobs
placed after it, as on this box. When jobs from here took the tokens a box
shows free, before its state counts them, the next frees when the first of
those jobs is predicted to end. A job that waits for a token is predicted at
the load there now, its own threads left out, since it takes over from a job
that ends; one that starts now adds them.

A job with no history has no size to say waiting pays: it goes where it
finishes soonest of the places with a token free, here or another box, else
waits for this box's. Its size is the median of every unit's, so what decides
is how fast each place runs now, its load included.

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

A daemon whose config has `base` (say `/build`) joins the farm. It answers
wrappers while it joins, which takes 15 s or more when the resolver is out of
reach, and keeps their jobs here until it has joined. The farm's netidx
namespace is under the protocol version, `BASE/vN` (`/build/v18`), so boxes
of different versions never see each other:

- `BASE/vN/status/NAME/COLUMN`: a row per box, which the netidx browser draws as
  a table.
- `BASE/vN/api/NAME/jobs`: where the box takes jobs, a netidx pack channel
  listener. A daemon opens one channel to each box when it first sends it
  something and keeps it until it fails: the handshake goes through the
  resolver and isn't cheap. Every job and heartbeat sent there is an
  exchange on it, each message tagged with the exchange's id. The box runs
  each job as a task of its own, so one that waits holds up no other, and
  a job whose client drops it gets `Stop`, which ends it and kills its
  rustc; so does the channel failing. Each client's handshake runs on its
  own with 30 s to finish, so a client that dies or stalls while connecting
  holds up no other.
- `BASE/vN/units/NAME/UNIT/COLUMN`: the units the box has run (see below).

`PROTOCOL` counts versions of every message between boxes and between a
daemon and its clients: changing any of them bumps it. A daemon refuses a
client of another version (see "Wrapper protocol"), so after an upgrade the
daemon must restart before the new version's wrappers can use it.

A box's state is nine columns of its status row, each its own value:
`profile`, `version` (a counter), `toolchains`, `cache GB` (what its store
keeps), `load` (runnable
tasks per CPU), and per class (as arrays) `cpus`, `class GIPS` (calibrated G
instructions/s), `free` tokens, and `frees in ms`, until its next token is
predicted to free. A change updates the columns it touches in
one batch, which subscribers get whole and in order, so no box sees half a
state, and unchanged columns aren't sent. Free tokens are the class's tokens
no job holds, so they leave out the tokens lent to rustc's codegen threads.
The prediction is the soonest predicted end of a job holding one of the
class's tokens, this box's or another's: 0 with a token free, unknown while
the box takes no offers. It changing alone doesn't republish; it goes out with
the next change, and clients count it down from when the state arrived. The
load changing republishes only once it has moved a quarter of a task per CPU.
A box publishes no free tokens while it switches profile or its own jobs wait.
The scheduler publishes after every event, which includes the 100 ms ticks
that lend tokens while jobs run. A changed state is republished at most every
100 ms. The daemon lists `BASE/vN/status` for other
boxes and subscribes to each one's state columns. A resolver error is logged
and the listing tried again at the next tick. Netidx retries the resolver
for up to tens of seconds first, and meanwhile the box neither publishes its
state nor reads the others'. Whenever the farm task ends, the scheduler is
told every other box is gone, so it never places jobs by a state that no
longer changes.

Toolchains are rustup's (`~/.rustup/toolchains/*`). A compiler is matched by
rustc's commit hash and host, and a job with `--target` also needs that
target's standard library installed. The client learns the key of the rustc
cargo runs from `rustc -vV`, cached by path and mtime.

A server admits a remote job only into a free token of the class asked for,
or for a heavy job the fastest class with one, and only when none of its own
jobs is waiting, so local work comes first. It
answers busy otherwise. An admitted job holds a normal slot: it shares the
class's tokens and CPUs with local jobs, and its runs go into the server's
history too.

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

`status` shows the cost model. Speeds are relative to this box's fastest class
idle: its calibrated rate over the factor it has learned with no load. Each
class shows its speed idle and at the load now, this box's jobs, other boxes'
jobs and free tokens; heavy jobs, their threads and each one's speed now;
each other box, its load, its classes' and threads' speeds as this box has
learned them, the time jobs sent there spend moving files, for libraries and
for jobs that link, the MB per second its sends moved at, and its free tokens. Every unit the box has run is listed with its rank and work in
seconds on one thread of the fastest class idle, its front end's share, its
width, its predicted time here now and its measured wall time, and runs split
three ways: this box's builds run here, this box's builds run elsewhere, and
other boxes' builds run here. Units outside this box's build graph have no
rank. Units sent elsewhere also show the median time their sent runs spent on
files and the MB they sent.

In a farm each box also publishes what `status` shows, for the netidx browser.
Beside its state columns, its status row has figures for people: jobs
`waiting`, `running` and `serving`, per class `class speed` now and
`class idle speed`, `heavy threads` and `heavy thread speed`, speeds in G
instructions of the calibration load per second (calibrated rate over learned
factor), which compare across boxes where `status`'s relative ones don't, per
class
`class running`, `class serving` and tokens `lent`, `finishing` under the last
profile, memory
(`mem GiB`, `mem growth GiB` predicted from running jobs, `mem cap GiB`) and
`graph units`. So `BASE/vN/status` shows the
whole farm at a glance. Its units go in a table of their own at
`BASE/vN/units/NAME`, with `status`'s columns. Values are typed numbers, so the
browser sorts them. These refresh every 2 s and send only cells that
changed. Building a status takes the scheduler some tens of ms with
thousands of units, which is why it isn't faster.

### Inputs

Inputs are addressed by content: a 128-bit xxh3 hash. Boxes trust each other,
as netidx's authentication says who they are, so the hash need only make
chance collisions vanishingly rare. The client daemon caches
`(path, size, mtime, inode) -> hash`. The input set is:

- Sources: every file the unit's previous dep-info
  (`<out-dir>/<crate><extra>.d`) lists, which includes files outside the
  package, plus the whole package directory, so a newly added module is found
  and proc macros find what they read there without dep-info listing it. The
  walk skips cargo's output directories (`CACHEDIR.TAG`), hidden directories,
  and directories with a `Cargo.toml` of their own, which are other packages.
  A walk past 256 MB keeps the job local.
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
pinned from its manifest until it ends. The client hashes the files a job
reads when it sends the job, keeping each hash until the file's size, mtime
or inode changes, and saving them with the cost table (`hashes` in the state
directory) so a restart needn't read files again to name them. On washu-chan
the lanes' target directories are on a tmpfs larger than RAM, so a file
built a while ago is often in swap (zram or a swap file on disk), and the
first read of it after a restart ran at about 110 MB/s. The remote job's log
line says how many MB staging hashed and how long that took. Most are known by then: the wrapper tells the daemon the
crate files a compile here wrote, which the daemon hashes in the background
once it ends, and crate files built on another box come back with their
hash. Placement prices a send only by hashes already known. A job that needs
a file another job is hashing waits for that hash. On 2026-11-01, with
washu-chan at 8 to 14 runnable tasks per CPU, staging took 8.4 s for a
graphix test that links, 31 s for a rustc-dist test, and 1.3 s for each of
ten build scripts sent at once, which hashed the same 355 MB each, then with
blake3 on one thread. On one thread there, with the box nearly idle,
blake3 hashed graphix_types' 338 MB rlib at 2.9 to 3.8 GB/s and 128-bit xxh3
at 18 to 21 GB/s, as fast as reading it; zstd at level 1 compresses about
0.3 to 0.5 GB/s. A server's store removes, when it opens, files whose names
aren't hashes, such as blobs named by an older hash. The client sends a manifest of paths and hashes; the server
answers with the hashes it lacks; the client sends those in 1 MB chunks and
the server checks each blob's hash before storing it. Each chunk goes
compressed with zstd at the sender's `compression_level` (1 by default, 0 for
none) when that makes it smaller, and is stored as it was; outputs come back
the same way at the server's level. On graphix's rlibs, rmetas and sources on
2026-11-01, level 1 took rlibs to a fifth and the rest to a third, level 3
did 8% better on rlibs, and a dictionary trained on rmetas gained nothing. A blob another job's
client is already sending isn't asked for again: the job waits for it, before
rustc starts if it needs it then, or behind a stand-in if it is a late rlib.
If that upload fails, so does the job, and its client builds it. On
2026-11-01, fifteen graphix tests sent to ryouko at once each sent the same
520 MB of rlibs, 7.8 GB over wifi, and each linker waited over two minutes. Once a server is warm a
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
file, then the line. A crate file's last chunk carries its hash, which the
server takes anyway to keep the file in its store. The client writes the file
(temp plus rename, keeping the server's mode), records a crate file's hash so
staging a later job needn't read it, hashes nothing itself, and forwards the
line to the wrapper at once. All other output is held until rustc
exits. The client accepts outputs only directly in `--out-dir` and explicit
`--emit`/`-o` directories.

The server keeps every `.rlib`, `.rmeta` and `.so` it builds in its blob
store, hard linked from the job's tree. A job whose dependencies built on the
same box has little to send: on a warm box, a binary sends its own sources.

### Failure

Any remote failure (non-zero exit, staging or sandbox error, dropped
connection) reruns on a local slot, and the held-back diagnostics are
discarded. If the rmeta line was already forwarded, the wrapper drops it from
the rerun's output. A remote failure is logged as a warning with the box and
rustc's first error, with its first note, where a proc macro's panic message
goes. A unit that fails remotely and then builds locally twice is kept local
until the daemon restarts, which is logged too, and the placement log gives
that as the reason it can't go. Typical causes are proc macros that read the
network or the environment (sqlx), or an include of a file the client never
sent. Real compile errors get compiled twice. They usually fail fast.

Staging sends the whole package directory, since proc macros read what they
like there and dep-info lists none of it. On 2026-11-01 every graphix_package
crate failed on ryouko: graphix-derive's `defpackage!` reads the package's
`Cargo.toml` and checks that it has a `build.rs` and a `src/graphix`
directory, and panicked without them. Several lanes built each crate, so each
was kept local within minutes of every restart. The failure was logged only
at debug, so nothing showed why; sending `Cargo.toml` alone got past the
first check and the new warning named the second.

## Config

Config is real TOML (`toml` + serde), parsed into two separate structs.

- User, `~/.config/rustc-dist.toml`: `compressed_swap_share`, `tokens_per_cpu`,
  `calibration_seconds`, and for a farm `base` (the netidx path every box
  publishes under), `name` (the host name without its domain by default), `bind` (where other
  boxes reach this one, in netidx's syntax, since netidx's default is often
  loopback only), `cache_dir`, `cache_gb`, `remote_links` (on by default),
  `half_life_s` and `copy_half_life_s` (see Cost), and `compression_level`
  (see Farm).
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
  finishes there soonest (built 2026-10-31; light jobs too, 2026-11-01). It is offered once the box shows
  a free token; until then no other job from here takes that box's tokens,
  since none show free.
- Each job's predicted transfer time replaced the flat 200 ms in Stage 5.

### Stage 5: predict wall time (built 2026-11-01)

The scheduler predicts when a job would finish at each place it could run, but
every number it predicts from comes from CPU time: a class's rate is
instructions per CPU-µs, and a job's width is its CPU time over its wall time.
CPU time isn't time.

- On 2026-11-01 at 06:59 the laptop was at load 246 while ryouko and aieka sat
  idle. ryouko published 1.69 G instructions per CPU-s (calibrated 5.96; it had
  learned 7.89 by the evening before), aieka 1.40 (calibrated 2.32), so the
  laptop sent them almost nothing.
- Their rates came from the narrow jobs they served, mostly build scripts
  linking about 355 MB of rlibs with mold. A narrow job there is pinned to all
  32 CPUs, and mold starts a thread per CPU, which spins while it waits. The
  same small link on idle ryouko, allowed 1 CPU: 100 ms of CPU in 102 ms, 9.3 G
  instructions per CPU-s. Allowed 32: 534 ms of CPU in 78 ms, 2.3. lld allowed
  32: 162 ms of CPU, 5.8. A plain rustc there ran at 9.3, in bwrap or not, and
  the wide `graphix_tests` runs it served at about 8.8.
- Wide jobs, most of the time spent, never teach a rate, since they borrow
  CPUs from several classes.
- Each box learns from the jobs it happens to run, and clients choose those,
  so rates learned on different boxes don't compare.

So measure what is predicted: how long each job took where it ran, as the
client saw it.

Cost, Jobserver and the farm's state above describe what was built. The
plan's main points, and where the build went past it:

- Each place has a factor, a curve over the load there, that scales its
  calibrated time to time there, learned only from units that also ran
  elsewhere or under other load. Load is measured from the box, since test
  binaries and fuzzers bypass the daemon: at 06:59 the laptop ran five rustc
  jobs at load 246. Because the curves model load, old runs keep counting for
  a day rather than minutes.
- Size is in the calibration load's instructions rather than seconds at a
  reference speed, which comes to the same thing at one instruction per µs and
  keeps ranks and `status` in the units they had.
- A sent run's transfer counts in full, as its own term per unit and place, so
  a fast box behind a slow link or a cold cache isn't taken for a slow box.
  `build_script_build:980e7c5bc3a8806d` took 10.2 s on ryouko with nothing to
  send, and 38.3 s when it sent 345 MB, 28.2 s of that the linker waiting for
  rlibs.
- Width is the tokens a job held after its front end: rustc takes a token for
  each codegen thread, and a spinning linker takes none. That needs every job
  to have a jobserver of its own. Lent like a heavy job's, every light job
  would also hold a spare it never reads, halving light jobs' share of the
  tokens; so spares are lent on credit, and the heavy jobs' unread spare went
  too.
- A server's slot exists while the job's inputs are still arriving, so its
  meter counts from when rustc starts.
- A bucket starts from the calibrated curve at the level the place has learned
  over all its buckets, so a load nothing has run at doesn't read faster than
  the loads around it.
- The table from before loads as seed history, so the first builds after the
  change still know every unit's size.
- What went: instructions counted per job, CPU time, and class rates learned
  from it (`rates/NAME.toml`, which a new daemon ignores). Calibration still
  counts instructions to compare CPUs, and boxes publish calibrated rates as
  starting factors.

A unit's recent sends mixed runs that sent 345 MB with runs that sent
nothing, so a send is now priced by the bytes the box lacks (see Cost).

Open: placing a job also slows the jobs already on the box. The curves could
predict that too, but at first a prediction covers only the job being placed.

### Stage 6: robust estimates and heartbeats (built 2026-11-01)

The table's estimates are decayed means and sums, which one bad run moves:

- Fifteen graphix tests sent to ryouko at once each sent the same 520 MB (since
  fixed: a box no longer asks for a blob already on its way). Their 135 s sends
  set ryouko's average for jobs that link and each test's own average, so link
  jobs went to aieka instead. aieka's cache stayed warm and ryouko never got
  the chance to look better.
- A place's factor is total time over total calibrated time, so one run that
  swapped, or starved behind other work, pulls it. aieka's heavy factor of 0.29,
  a third of its calibrated time on a box ryouko should beat, is likely one.
- A box that looks bad gets no jobs, so nothing corrects it but time.

Each estimate becomes a weighted median of its recent samples. Weights halve
with age at the half-life, as now, so old samples still fade, but no single
sample, however long, moves the middle:

- A unit's sends to a place: its last 8, each the time it spent moving files
  and the kB it sent. The prediction is their weighted median of each send's
  time adjusted to the kB missing now, at the place's µs per kB.
- Sends to a place of each kind (link or not): the last 64, the same way. The
  µs per kB is the weighted median of the slopes between pairs of them whose
  kB differ by at least 1 MB (Theil-Sen), the 50 MB/s guess until there are
  such pairs. Stats stay box to box: networks are complex, and one box's
  link says little about another's.
- A place's factor at each load bucket: the last 32 comparisons, each a run's
  time over what its unit's size from runs elsewhere predicts, weighted by that
  prediction, so long jobs still count most, and by age. Calibration counts as
  a sample of weight `P` that never ages, as now, so a bucket with no recent
  runs goes back to it.
- A unit's size: the weighted median over its last 16 runs of the size each
  implies at its place's current factor.

That holds about a kB per unit, some 7 MB for 7,000 units, and the table's
format changes.

Recent samples are only as fresh as the jobs sent, and a box the medians
judge badly gets none. So while jobs are waiting here and another box takes
none from here and has a token free, this box sends it a heartbeat: half a MB
or a MB of bytes no box has, alternately, so they are never already held, at
most once a minute per box. A heartbeat's time and size are a sample for the
box's sends of each kind, so throughput and the fixed cost of a send stay
current, and enough fresh heartbeats outweigh old bad sends as those age.
A heartbeat takes no token and runs nothing.

Heartbeats keep a box's transfer stats fresh, not its speed. A box's speed
doesn't change the way a network does, and the calibration a factor goes back
to as its runs age is a speed measured on that box, so no job is sent only to
learn. If a wrong factor holds too long in practice, `half_life_s` is the
knob.

As built: the calibration counts as 3 s of runs rather than 30, since a median
moves only once samples outweigh it, and 30 s held a box twenty times slower
than its calibration at its calibration for over thirty one-second jobs; and
a bucket nothing has reached lies between its neighbours rather than at a
level for the whole place, which a median snaps to one side of.

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

- Upload and output file I/O run on async worker threads, except reading
  and compressing what is sent, which runs on blocking threads, eight
  chunks at once: on 2026-11-01, with washu-chan's load at 100 to 190
  runnable tasks, a release build packed 200 MB of rlib at level 1 at
  249 MB/s one chunk at a time and 604 MB/s eight at once.
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
  there, in bwrap too, with `perf_event_paranoid` at 2. Calibration now runs one
  thread per core (see "Calibration"): aieka calibrates at 2.32 and ryouko at
  5.96, close to the 1.89 and about 6.9 their classes had learned from rustc.
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
  The daemon now shuts its publisher down before exiting.
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
| halve a unit's histograms at | 256 runs | `src/stats.rs` `HALVE_AT` |
| size of a job with no history at all | 1 G instructions | `src/stats.rs` `UNKNOWN_COST_MI` |
| a place's calibration counts as, in each load bucket | 3 s of runs | `src/stats.rs` `PRIOR_US` |
| samples kept: a unit's runs, its sends to a place, a place's sends of a kind, a bucket's comparisons | 16, 8, 64, 32 | `src/stats.rs` `*_KEPT` |
| two sends make a slope if their kB differ by | 256 kB | `src/stats.rs` `SLOPE_MIN_KB` |
| heartbeat to an idle box while jobs wait, at most every | 60 s | `src/sched.rs` `HEARTBEAT_EVERY` |
| runs count half toward speed and size after | 1 day | `half_life_s` in the config |
| sent runs count half toward time moving files after | 1 hour | `copy_half_life_s` in the config |
| zstd level for what a box sends | 1 | `compression_level` in the config |
| load buckets, runnable tasks per CPU | 0, ¼, ½, ¾, 1, 1½, 2, 3, 4, 6, 8 | `src/stats.rs` `BUCKETS` |
| republish load alone once it moves | ¼ task per CPU | `src/sched.rs` `LOAD_STEP_C` |
| a load sample counts `1/e` as much after | 10 s | `src/sched.rs` `LOAD_TAU` |
| share of compressed swap predicted peaks may use beyond RAM | 50% | `src/config.rs` `compressed_swap_share` |
| zswap compression ratio before it has stored anything | 3:1 | `src/sys.rs` `ZSWAP_RATIO` |
| sample memory and job footprints every | 250 ms | `src/sched.rs` `MEASURE` |
| keep graph nodes unseen for | 60 days | `src/graph.rs` `KEEP_DAYS` |
| only send jobs predicted to take at least | 1 s | `src/sched.rs` `REMOTE_MIN_US` |
| time moving files, before anything was sent to a place | 200 ms | `src/sched.rs` `REMOTE_OVERHEAD_US` |
| count and lend each job's tokens every | 100 ms | `src/sched.rs` `SAMPLE` |
| jobserver tokens per CPU | 1.5 | `src/config.rs` `tokens_per_cpu` |
| a job is light below this many tokens held | 1.5 | `src/sched.rs` `LIGHT_WIDTH_C` |
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
3. Link jobs: mold uses every CPU it may. Should a link job take more than one
   slot? Capping mold's threads would change the user's configuration (see
   Goals).
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
