# rustc-dist

Build Rust on every machine you own.

rustc-dist is a cargo `rustc-wrapper` plus a small daemon on each box. The
daemon decides where each rustc cargo starts should run: which cores on this
box, or, when another box would finish it sooner, a different machine
entirely. Boxes find each other and trade work over
[netidx](https://github.com/netidx/netidx). Work on a laptop and let the
desktops in the house do the compiling, with the biggest jobs going to the
fastest machine.

It is not a cache. Every job is compiled, just on the core or machine where
it finishes soonest.

## Results

A clean dev build of [graphix](https://github.com/graphix-lang/graphix) (about 650
crates), launched from a laptop. The farm was the laptop plus two desktops on
the same LAN:

| | wall time |
|---|---|
| laptop alone, plain cargo | 2m 27s |
| laptop + farm | 1m 20s |
| laptop on battery at a 12 W SoC limit + farm | 1m 23s |
| fastest desktop alone, plain cargo | 1m 19s |

The laptop's own speed hardly matters any more. A build from the laptop
finishes about as fast as logging into the fastest box and building there.

## What it does

On one box:

- **Core classes.** It calibrates the box's cores (performance, efficiency
  and low-power cores on hybrid CPUs) separately for each power profile, and
  shares each class's jobserver tokens out to the jobs on it. SMT siblings
  join their core's class.
- **It learns from wall time.** Each unit's size comes from how long it took
  where it ran, and each class and box learns how much slower than its
  calibration it runs, at each load, from units that ran in more than one
  place.
- **The critical path goes first.** It records the build graph, so it knows
  how much work waits on each job. The jobs most work waits on get the fast cores. Jobs with time
  to spare go to slow cores, but never wait for one while a faster core sits
  idle.
- **Heavy jobs spread.** A job that holds several jobserver tokens runs on
  every core, borrowing them from the classes.
- **Memory.** Jobs are admitted by predicted peak memory, counting a share of
  compressed swap (zram or zswap).
- **It learns.** Each class's speed is corrected by the jobs that actually run
  there, and what it learns survives restarts.

Across boxes (a farm):

- Each daemon publishes its state to netidx. A job goes wherever it is
  predicted to finish first, including waiting for a busy box that is much
  faster.
- Libraries, binaries, tests, proc macros and build scripts can all go
  remote. The server runs rustc in a [bubblewrap](https://github.com/containers/bubblewrap)
  sandbox with no network and no home directory.
- Servers keep the files they receive and the crates they build in a
  content-addressed cache with an LRU size cap, so repeat builds send little.
  The cache can live on a tmpfs.
- A job that links starts compiling before its dependencies' rlibs arrive. It
  builds against metadata-only stand-ins, and a stand-in linker waits for the
  real rlibs.
- A remote failure reruns locally, and cargo never sees diagnostics twice.
- `rustc-dist status` shows a box's classes, queue and job history. Every box
  also publishes a status table to netidx, which the
  [netidx browser](https://netidx.github.io/netidx-book/) shows as one row
  per box.

## Requirements

- Linux. It uses `/proc`, CPU affinity, FIFO jobservers and `perf_event_open`.
- Toolchains installed with [rustup](https://rustup.rs). In a farm, a job only
  goes to a box with the same toolchain, matched by rustc's commit hash and
  host, and with the target's standard library for `--target` builds. A
  box serves toolchains rustup installs or updates while it runs, and stops
  serving the ones they replace.
- A systemd user session, for `rustc-dist install`. Without one, the wrapper
  starts the daemon itself.
- On boxes that take other boxes' jobs: `bwrap`, the linker your cargo config
  names (`cc` by default, or for example `clang` with `mold`), and any system
  libraries your `-sys` crates link against.
- Optional: hardware instruction counters (`perf_event_paranoid` of 2 or
  lower), which calibration uses to compare cores. Without them, each
  calibration run is taken to retire a fixed count.

## Install

```sh
cargo install rustc-dist
```

After an upgrade, run `rustc-dist install` again, or restart the service: a
daemon refuses wrappers of another version. They wait up to 10 s for it to
restart, then fail and say so.

## Set up one box

1. Install the systemd user service. Do this while the box is idle: the first
   start runs a 60 second calibration load on every core, and builds wait
   until it finishes.

   ```sh
   rustc-dist install
   ```

2. Tell cargo to use it, in `~/.cargo/config.toml`:

   ```toml
   [build]
   rustc-wrapper = "rustc-dist"
   jobs = 128
   incremental = false
   ```

   - `jobs` only caps how many rustc processes cargo starts; the daemon
     decides how many actually run. Cargo holds about four open files per
     job, so 128 fits the usual limit of 1024. Go higher only if you've
     raised that limit.
   - Incremental jobs always stay on the box that has the incremental cache.
     With incremental off, workspace crates can be sent to other boxes too.

3. Profiles are names you choose, switched with `rustc-dist profile NAME`:
   for example from a script when a laptop's power profile changes, or when a
   long-running load starts on the box. The daemon calibrates each new
   profile name the first time it sees it, once running jobs finish, and
   switches to a known one at once. If no daemon is running, the command
   records the profile for the next start. To calibrate a profile ahead of
   time, on an idle box:

   ```sh
   rustc-dist profile power-saver --recalibrate
   ```

   The command must run as the user the daemon runs as, with that user's
   `HOME`. From a root script, for example:

   ```sh
   sudo -u alice /home/alice/.cargo/bin/rustc-dist profile power-saver
   ```

4. Build as usual. `rustc-dist status` shows what it's doing, and
   `RUSTC_DIST=off cargo build` runs rustc directly.

## Set up a farm

1. **A netidx domain.** Every box needs a netidx resolver it can reach, and
   working authentication. The [netidx book](https://netidx.github.io/netidx-book/)
   covers setting up a resolver and its security
   ([crate](https://crates.io/crates/netidx), [docs](https://docs.rs/netidx)).
   rustc-dist uses each box's default netidx client config (`NETIDX_CFG`, or
   netidx's default paths).

2. **A base path.** Pick a path for the farm, such as `/build`, and give every
   box's identity permission to publish, subscribe, list and write under it.
   rustc-dist uses, under its protocol version `vN` so boxes of different
   versions never meet:

   - `BASE/vN/status/NAME/...`: one row per box. The browser shows it as a
     table of the whole farm.
   - `BASE/vN/api/NAME/jobs`: where the box takes jobs.
   - `BASE/vN/units/NAME/...`: the units the box has run.

3. **Configure each box** in `~/.config/rustc-dist.toml`:

   ```toml
   base = "/build"
   # Where other boxes reach this one, in netidx bind syntax. netidx's
   # default is often loopback only.
   bind = "192.168.1.0/24"
   # Where other boxes' inputs and builds are kept. A tmpfs keeps them in
   # memory.
   cache_dir = "/tmp/rustc-dist-cache"
   cache_gb = 16
   ```

4. **Start it on each box** with `rustc-dist install`, on an idle box. On boxes
   that serve builds without anyone logged in, also run
   `loginctl enable-linger $USER`, so the user service keeps running.

5. **Check** that `netidx resolver list /build/status` shows every box, and
   that `rustc-dist status` lists the others.

Then build from any box. The first build of a project teaches rustc-dist its
build graph and each unit's cost, and later builds use them. A job whose
dependencies were built before rustc-dist first saw the project stays local,
so start with a clean target directory.

## Configuration

`~/.config/rustc-dist.toml`. Every key is optional.

| key | default | meaning |
|---|---|---|
| `base` | none | the farm's netidx path; without it the box works alone |
| `name` | host name without the domain | this box's name in the farm |
| `bind` | netidx's default | where other boxes reach this one, e.g. `192.168.1.0/24` |
| `cache_dir` | `~/.cache/rustc-dist` | other boxes' inputs and builds; keep it on one file system |
| `cache_gb` | 20 | cache size; the least recently used go first |
| `remote_links` | `true` | send jobs that link (binaries, tests, proc macros) to other boxes |
| `compression_level` | 1 | zstd level for files this box sends, 0 for none |
| `tokens_per_cpu` | 1.5 | jobserver tokens per CPU |
| `compressed_swap_share` | 0.5 | share of compressed swap that predicted job memory may fill beyond RAM |
| `calibration_seconds` | 60 | how long calibration runs |
| `half_life_s` | 86400 | seconds after which old runs count half toward how fast each place runs and how big each unit is |
| `copy_half_life_s` | 3600 | seconds after which old sent runs count half toward how long sending a job takes |

A project can keep crates on the box that builds it, for example proc macros
that read the network or the local environment. Put a `rustc-dist.toml` at
or above the directory you run cargo in:

```toml
local = ["sqlx-macros"]
```

Calibrations and history live in `~/.local/state/rustc-dist`. A history file
the daemon can't read, for example after going back to an older version, is
moved aside as `NAME.unreadable`, and the daemon starts without it. The daemon logs
to the journal (`journalctl --user -u rustc-dist`), at the level set by
`RUSTC_DIST_LOG` (`info` by default). `RUSTC_DIST_LOG=info,placement=debug`
adds a line per job saying where it went: whether it ran before and its time
on one thread of this box's fastest class; each place it could have gone,
with the seconds until a slot, moving its files and running there, and what
that run time comes from (`[own]` its runs there, `[box]` its runs on that
box, `[est]` an estimate from its size); the one it took starred; and why the
boxes it couldn't go to were out.

## What stays local

A job runs only on the box that started it if it:

- is incremental;
- uses `-C target-cpu=native`;
- splits debug info into separate files (`-C split-debuginfo` other than `off`);
- is run by something other than rustc, such as `clippy-driver`;
- reads arguments from an `@file`, prints something (`--print`), or has no
  `--out-dir`;
- has an argument, compiler path or working directory that isn't UTF-8;
- links, and `remote_links` is off;
- is named in a project's `local` list, or failed remotely and then built
  locally twice;
- is in a project whose `rustc-dist.toml` doesn't parse (the wrapper prints
  why);
- depends on a crate the daemon never saw compile, so it can't tell what to
  send;
- is in a package directory holding more than 256 MB, not counting build
  output, other packages and hidden directories;
- needs a toolchain no other box has.

The placement log (see above) says which, and a job that fails on another box
is logged as a warning with the box and its first error.

## Security

Anyone who can open a job channel can run code as the server's user, because
proc macros are client-supplied shared libraries. The daemon won't run as
root, and the wrapper runs rustc directly when cargo runs as root. Three things limit that:
netidx permissions on the base path, a bubblewrap sandbox with no network
and no home directory, and checks on every path a client sends. Only run a
farm within a netidx domain whose members you trust.

The farm is trusted, not adversarial: clients already run the code servers
compile for them. So the sandbox keeps honest jobs apart; it does not stop a
hostile one. A job can plant symlinks in its own tree that the server then
follows when it reads outputs, and it can rewrite the store blobs its inputs
are hard links to, which later jobs from every client would use.

## How it works

[DESIGN.md](DESIGN.md) covers the scheduler, calibration, the cost model,
remote execution, the farm's netidx layout, and the measurements behind them.

## License

MIT. See [LICENSE](LICENSE).
