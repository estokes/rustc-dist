# rustc-dist

Build Rust on every machine you own.

rustc-dist is a cargo `rustc-wrapper` that distributes rust compile jobs across
a cluster of machines. Discovery, communication, and security are handled by
[netidx](https://github.com/netidx/netidx); if you already have a netidx domain,
setup is simple. Jobs are placed where they are predicted to finish the fastest,
and the daemon learns from run history and accounts for network latency. Work on
battery and watch the heavy compile jobs migrate to a desktop or a cloud server.

Unlike sccache it is not a cache, every compile job runs, and toolchains are
not transferred.

## Results

The point isn't one faster build: how much that gains depends on the crate
graph, and a chain of crates that each wait on the last runs no faster on
more machines. The point is capacity, used well, when many builds want it at
once, such as eight coding agents in their own checkouts all compiling
together.

Cargo alone doesn't handle that. Each cargo sizes its jobserver to the whole
box and knows nothing of the others. Builds that share a target directory wait
on its lock; builds in separate ones run side by side, with many agents rustc
threads overcommit the box and thrash. With rustc-dist, every rustc on the box,
from any cargo, holds one of the daemon's tokens (default 1.5 per CPU). Work
always goes where it's predicted to finish fastest, but when total work exceeds
capacity overcommit is limited and thrashing is eliminated.

In daily use, with several agents building their own checkouts of
[graphix](https://github.com/graphix-lang/graphix), the fastest desktop does
most of the compiling, the laptop runs the tests, and the other desktop takes
the surges. Our estimate is that agentic work got at least twice as fast.

One clean dev build of graphix (about 650 crates), from a laptop with two
desktops on the same LAN:

| | wall time |
|---|---|
| laptop alone, plain cargo | 2m 27s |
| laptop + farm | 1m 20s |
| laptop on battery at a 12 W SoC limit + farm | 1m 23s |
| fastest desktop alone, plain cargo | 1m 19s |

The laptop's own speed hardly matters: a build from it finishes about as fast
as one on the fastest box, and a surge of many agents compiling at once will
utilize all three boxes efficiently.

## What it does

On one box:

- **Core classes.** It calibrates each class of cores (performance,
  efficiency, low-power) for each power profile, and schedules jobs on the
  slowest core that will finish fast enough not to bottleneck the build.
- **Critical path first.** It records the build graph, so the jobs most work
  waits on get the fast cores. Jobs with time to spare take slow cores, but
  never wait for one while a faster core sits idle. Jobs that use several
  threads spread over every core.
- **Memory.** Jobs are admitted by predicted peak memory, counting a share of
  compressed swap.
- **It learns.** Each unit's size, and how much each class and box slows
  under load, come from the wall times of real jobs, and survive restarts.

Across boxes (a farm):

- A job goes wherever it is predicted to finish first, even to wait for a
  busy box that is much faster.
- Libraries, binaries, tests, proc macros and build scripts can all go
  remote, into a [bubblewrap](https://github.com/containers/bubblewrap)
  sandbox with no network and no home directory.
- Servers keep the files they receive and the crates they build in a
  size-capped cache, so repeat builds send little.
- A job that fails remotely reruns locally, or remotely again if it only
  lacked a file it wasn't sent. Cargo never sees diagnostics twice.
- `rustc-dist status` shows a box's classes, queue and job history, and the
  [netidx browser](https://netidx.github.io/netidx-book/) shows the whole
  farm as a table.

## Requirements

- Linux.
- Toolchains installed with [rustup](https://rustup.rs). A job only goes to a
  box with the same toolchain (rustc's commit hash and host), and with the
  target's standard library for `--target` builds.
- A systemd user session, for `rustc-dist install`. Without one, the wrapper
  starts the daemon itself.
- On boxes that take other boxes' jobs: `bwrap`, the linker your cargo config
  names, and any system libraries your `-sys` crates link against.
- Optional: hardware instruction counters (`perf_event_paranoid` of 2 or
  lower), which calibration uses to compare cores.

## Install

```sh
cargo install rustc-dist
```

After an upgrade, run `rustc-dist install` again or restart the service: a
daemon refuses wrappers of another version.

## Set up one box

1. Install the systemd user service while the box is idle: the first start
   runs a 60 second calibration on every core, and builds wait for it.

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

   The daemon, not `jobs`, decides how many compiles run; 128 keeps cargo's open
   files (about four per job) under the usual limit of 1024. With incremental
   off, workspace crates can go to other boxes too.

3. Optionally, switch profiles with `rustc-dist profile NAME`, for example
   when a laptop's power profile changes. A new name is calibrated the first
   time; `--recalibrate` does it again. Run it as the daemon's user, with that
   user's `HOME`; from a root script, for example,
   `sudo -u alice /home/alice/.cargo/bin/rustc-dist profile power-saver`.

4. Build as usual. `RUSTC_DIST=off cargo build` runs rustc directly.

## Set up a farm

1. **A netidx domain.** Every box needs a netidx resolver it can reach and
   working authentication; the
   [netidx book](https://netidx.github.io/netidx-book/) covers both.
   rustc-dist uses each box's default netidx client config.

2. **A base path**, such as `/build`, under which every box's identity may
   publish, subscribe, list and write. rustc-dist works under `BASE/vN`, where
   `N` is its protocol version, so boxes of different versions never meet.

3. **Configure each box** in `~/.config/rustc-dist.toml`:

   ```toml
   base = "/build"
   # Where other boxes reach this one; netidx's default is often loopback.
   bind = "192.168.1.0/24"
   ```

4. **Start it on each box** with `rustc-dist install`. On boxes that serve
   builds with no one logged in, also run `loginctl enable-linger $USER`.

5. **Check** that `rustc-dist status` lists the other boxes.

The first build of a project teaches rustc-dist its build graph and costs. A
job whose dependencies built before rustc-dist first saw the project stays
local, so start with a clean target directory.

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
| `half_life_s` | 86400 | half-life of past runs, for speeds and unit sizes |
| `copy_half_life_s` | 3600 | half-life of past sends, for transfer times |

A project can keep crates on the box that builds it, such as proc macros that
read the network or the local environment, with a `rustc-dist.toml` at or
above the directory you run cargo in:

```toml
local = ["sqlx-macros"]
```

Calibrations and history live in `~/.local/state/rustc-dist`. The daemon logs
to the journal (`journalctl --user -u rustc-dist`) at the level
`RUSTC_DIST_LOG` sets; `RUSTC_DIST_LOG=info,placement=debug` adds where each
job went, the predicted times everywhere it could have gone, and why other
boxes were out.

## What stays local

A job runs only on the box that started it if it:

- is incremental, uses `-C target-cpu=native`, or splits debug info
  (`-C split-debuginfo` other than `off`);
- is run by something other than rustc, such as `clippy-driver`, reads
  arguments from an `@file`, prints something (`--print`), or has no
  `--out-dir`;
- has an argument, compiler path or working directory that isn't UTF-8;
- links, and `remote_links` is off;
- is in a project's `local` list, or one whose `rustc-dist.toml` doesn't
  parse, or failed remotely and then built locally twice;
- depends on a crate the daemon never saw compile;
- is in a package directory of over 256 MB, build output aside;
- needs a toolchain no other box has.

The placement log says which.

## Security

Anyone who can open a job channel can run code as the server's user, since
proc macros are shared libraries the client sends. The daemon won't run as
root. netidx permissions, the sandbox and checks on every path a client sends
keep honest jobs apart, but don't stop a hostile one, which could for example
rewrite cached files that later jobs read. Clients already run the code
servers compile for them, so run a farm only within a netidx domain whose
members you trust.

## How it works

[DESIGN.md](DESIGN.md) covers the scheduler, calibration, the cost model,
remote execution, the farm's netidx layout, and the measurements behind them.

## License

MIT. See [LICENSE](LICENSE).
