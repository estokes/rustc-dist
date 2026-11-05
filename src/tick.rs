//! One clock grid for every periodic wakeup. A timer ticks on the multiples
//! of its period from one epoch, and the periods divide each other, so
//! timers due together fire in one wakeup: a quiet daemon wakes once every
//! `IDLE`, which costs a battery-powered box far less than as many wakeups
//! spread out.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::time::{self, Interval, MissedTickBehavior};

/// How often a quiet daemon wakes. Every idle period is a multiple of it.
pub const IDLE: Duration = Duration::from_secs(2);

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// The first tick of the grid of `every` after `after`.
pub fn next(every: Duration, after: Instant) -> Instant {
    let epoch = *EPOCH;
    let every_ns = every.as_nanos().max(1);
    let ticks = after.saturating_duration_since(epoch).as_nanos() / every_ns + 1;
    epoch + Duration::from_nanos((ticks * every_ns) as u64)
}

/// An interval on the grid of `every`, first ticking at the next tick
/// after now. Ticks missed, while it wasn't polled or was slow, are skipped,
/// so it stays on the grid.
pub fn interval(every: Duration) -> Interval {
    let mut i = time::interval_at(next(every, Instant::now()).into(), every);
    i.set_missed_tick_behavior(MissedTickBehavior::Skip);
    i
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn grids_share_their_ticks() {
        let now = Instant::now();
        let idle = next(IDLE, now);
        assert!(idle > now && idle <= now + IDLE);
        assert_eq!(next(IDLE, idle), idle + IDLE, "a tick is not after itself");
        // A grid whose period divides another's ticks with it.
        let quarter = Duration::from_millis(250);
        let mut t = next(quarter, now);
        while t < idle {
            t = next(quarter, t);
        }
        assert_eq!(t, idle);
        let minute = next(Duration::from_secs(60), now);
        assert_eq!(next(IDLE, minute - Duration::from_millis(1)), minute);
    }
}
