//! One clock grid for every periodic wakeup. A timer on the grid of `every`
//! ticks when the wall clock reads a whole multiple of `every` since the Unix
//! epoch, and the periods divide each other, so timers due together fire in
//! one wakeup: a quiet daemon wakes once every `IDLE`, which costs a
//! battery-powered box far less than as many wakeups spread out. Boxes whose
//! clocks agree tick together, so what one publishes on a tick reaches the
//! others as they wake for theirs.
//!
//! The wall clock is read once, when the grid is first used; it moves with
//! the monotonic clock after that, so a step of the wall clock doesn't move
//! it.

use std::{
    sync::LazyLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::{self, Interval, MissedTickBehavior};

/// How often a quiet daemon wakes. Every idle period is a multiple of it.
pub const IDLE: Duration = Duration::from_secs(2);

/// An instant, and the wall clock at it, in ns since the Unix epoch.
static REFERENCE: LazyLock<(Instant, u128)> = LazyLock::new(|| {
    let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    (Instant::now(), wall.as_nanos())
});

/// The wall clock at `t`, in ns since the Unix epoch.
fn wall_ns(t: Instant) -> u128 {
    let (at, wall) = *REFERENCE;
    match t.checked_duration_since(at) {
        Some(d) => wall + d.as_nanos(),
        None => wall.saturating_sub(at.duration_since(t).as_nanos()),
    }
}

/// The first tick of the grid of `every` after `after`.
pub fn next(every: Duration, after: Instant) -> Instant {
    let every_ns = every.as_nanos().max(1);
    after + Duration::from_nanos((every_ns - wall_ns(after) % every_ns) as u64)
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
    fn ticks_fall_on_whole_multiples_of_the_period_in_wall_time() {
        let now = Instant::now();
        let idle = next(IDLE, now);
        assert!(idle > now && idle <= now + IDLE);
        assert_eq!(wall_ns(idle) % IDLE.as_nanos(), 0);
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
        // An instant before the grid was first used.
        let before = REFERENCE.0 - Duration::from_millis(300);
        assert_eq!(wall_ns(next(IDLE, before)) % IDLE.as_nanos(), 0);
    }
}
