use std::time::{Duration, Instant};

pub struct EveryOnceInAWhile {
    interval: Duration,
    last: Instant,
    ever_called: bool,
}

impl EveryOnceInAWhile {
    /// Creates a new `EveryOnceInAWhile` instance.
    ///
    /// # Arguments
    ///
    /// * `interval` - The interval in seconds.
    /// * `do_first_now` - Whether the first check is done immediately.
    pub fn new(interval: f32, do_first_now: bool) -> Self {
        let interval_duration = Duration::from_secs_f32(interval);
        let last_instant = if do_first_now {
            Instant::now() - interval_duration
        } else {
            Instant::now()
        };

        EveryOnceInAWhile {
            interval: interval_duration,
            last: last_instant,
            ever_called: false,
        }
    }

    /// Determines if the action should now be performed based on the interval.
    pub fn now(&mut self) -> bool {
        if self.last.elapsed() >= self.interval {
            self.last = Instant::now();
            self.ever_called = true;
            true
        } else {
            false
        }
    }

    /// Checks if the action has ever been called.
    pub fn was_ever_time(&self) -> bool {
        self.ever_called
    }
}
