//! Exponential backoff with jitter, so every symbol on every connection does not retry in
//! lockstep after a shared outage.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Backoff {
    current: Duration,
    max: Duration,
}

impl Backoff {
    pub(crate) const BASE: Duration = Duration::from_millis(250);

    pub const fn new(max: Duration) -> Self {
        Self {
            current: Self::BASE,
            max,
        }
    }

    pub const fn reset(&mut self) {
        self.current = Self::BASE;
    }

    pub fn next(&mut self) -> Duration {
        let wait = self.current;
        self.current = (self.current * 2).min(self.max);
        wait + jitter(wait)
    }
}

/// Up to +50% of `base`, derived from the wall clock rather than a `rand` dependency.
pub fn jitter(base: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    let span = u64::try_from(base.as_nanos() / 2)
        .unwrap_or(u64::MAX)
        .max(1);
    Duration::from_nanos(nanos % span)
}

#[cfg(test)]
mod test {
    use super::{Backoff, Duration};

    #[test]
    fn backoff_grows_and_caps_so_a_dead_endpoint_cannot_hot_spin() {
        let max = Duration::from_secs(30);
        let mut backoff = Backoff::new(max);

        let first = backoff.next();
        assert!(first >= Backoff::BASE, "{first:?}");

        let mut last = first;
        for _ in 0..20 {
            last = backoff.next();
            // Jitter adds at most +50%, so nothing may exceed the cap by more than that.
            assert!(last <= max + max / 2, "{last:?} exceeded the cap");
        }
        assert!(last > first, "backoff must grow: {first:?} -> {last:?}");

        backoff.reset();
        assert!(backoff.next() < Duration::from_secs(1));
    }
}
