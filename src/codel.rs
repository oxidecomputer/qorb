//! An implementation of the Controlled Delay algorithm.
//!
//! Refer to https://queue.acm.org/appendices/codel.html for
//! additional context.

use std::time::{Duration, Instant};

/// Parameters to customize the Controlled Delay algorithm.
///
/// Refer to the documentation on [ControlledDelay] for
/// additional context.
#[derive(Clone, Debug)]
pub struct ControlledDelayParameters {
    /// The duration of travel through a healthy queue.
    pub target: Duration,

    /// A window in which a healthy "target" duration should be reached.
    pub interval: Duration,

    /// A multiplier of the interval beyond which we must "miss" the target
    /// to initially to start dropping from the queue.
    ///
    /// A value of "N" means we'll drop connections as soon as we've passed
    /// (1 + N) intervals without beating the target duration.
    pub interval_hysteresis_factor: u32,
}

impl Default for ControlledDelayParameters {
    fn default() -> Self {
        Self {
            target: Duration::from_millis(5),
            interval: Duration::from_millis(100),
            interval_hysteresis_factor: 1,
        }
    }
}

/// Controlled Delay (or "CoDel") is an algorithm to manage queues.
///
/// This algorithm monitors latency through a queue, and gives feedback
/// about whether or not the latency through that queue is bufferbloat
/// (undue end-to-end latency due to stalling in a queue).
///
/// This algorithm works with the following parameters:
/// - Interval: A window of time in which we are monitoring the queue.
/// - Target Delay: The "maximum acceptable queue time" that we expect
/// a packet to take when moving through the queue.
///
/// While observing a queue for an Interval duration:
/// - If we observe any packets moving through the queue faster than our "target
/// delay", the queue is working as expected!
/// - If we do not observe any packets beating the "target time" - in other
/// words, servicing requests in the queue is taking too long - then the queue
/// is overloaded, and requests should be dropped.
#[derive(Debug)]
pub struct ControlledDelay {
    params: ControlledDelayParameters,
    must_hit_target_by: Option<Instant>,

    last_emptied: Option<Instant>,

    // The time to drop the next packet.
    drop_next: Option<Instant>,
    // Packets dropped since entering drop state.
    drop_count: usize,
    // True if we're dropping packets form the queue.
    dropping: bool,
}

impl ControlledDelay {
    pub fn new(params: ControlledDelayParameters) -> Self {
        Self {
            params,
            must_hit_target_by: None,

            last_emptied: None,

            drop_next: None,
            drop_count: 0,
            dropping: false,
        }
    }

    fn can_drop(&mut self, now: Instant, start: Instant) -> bool {
        let sojourn_time = now - start;

        // If the queue is moving quickly enough, we shouldn't drop anything.
        //
        // This also clears the tracking that we need to hit the target.
        if sojourn_time < self.params.target {
            self.must_hit_target_by = None;
            return false;
        }

        match self.must_hit_target_by {
            Some(must_hit_target_by) => {
                // Return if we should have already hit the target, but haven't.
                return must_hit_target_by <= now;
            }
            None => {
                // If we haven't started an interval yet, do so now.
                self.must_hit_target_by = Some(now + self.params.interval);
                return false;
            }
        }
    }

    /// Given the starting time of a claim, returns if the request
    /// should time out or not.
    pub fn should_drop(&mut self, start: Instant) -> bool {
        self.should_drop_inner(Instant::now(), start)
    }

    // This is a pattern used throughout this module:
    //
    // Any functions depending on "now" take it as an input parameter,
    // so we can more easily create deterministic tests.
    fn should_drop_inner(&mut self, now: Instant, start: Instant) -> bool {
        let ok_to_drop = self.can_drop(now, start);

        if self.dropping {
            if !ok_to_drop {
                self.dropping = false;
                return false;
            }

            if let Some(drop_next) = self.drop_next {
                if now >= drop_next {
                    self.drop_count += 1;
                    self.set_drop_next(now);
                    return true;
                }
            }
            return false;
        }

        let failed_target_for_a_while = self
            .must_hit_target_by
            .map(|must_hit_target_by| {
                // Note that when "must_hit_target_by" is set, it gets
                // set to "now + interval". Admittedly, that's a different
                // value of "now" than the one being evaluated here.
                now - must_hit_target_by >=
                    self.params.interval * self.params.interval_hysteresis_factor
            })
            .unwrap_or(false);

        let dropped_recently = self
            .drop_next
            .map(|drop_next| now - drop_next < self.params.interval)
            .unwrap_or(false);

        if ok_to_drop && (failed_target_for_a_while || dropped_recently) {
            self.dropping = true;

            if dropped_recently {
                self.drop_count = if self.drop_count > 2 {
                    self.drop_count - 2
                } else {
                    1
                };
            } else {
                self.drop_count = 1;
            }

            self.set_drop_next(now);

            return true;
        }

        return false;
    }

    fn set_drop_next(&mut self, now: Instant) {
        if self.drop_count > 0 {
            self.drop_next = Some(
                now + (self
                    .params
                    .interval
                    .div_f64((self.drop_count as f64).sqrt())),
            );
        }
    }

    pub fn queue_cleared(&mut self) {
        self.queue_cleared_inner(Instant::now());
    }

    fn queue_cleared_inner(&mut self, now: Instant,) {
        self.last_emptied = Some(now);
        self.drop_next = None;
        self.dropping = false;
        self.drop_count = 0;
        self.must_hit_target_by = None;
    }

    /// Get the maximum duration for a request to be idle in the queue
    /// before timing out.
    pub fn get_max_idle(&self) -> Duration {
        self.get_max_idle_inner(Instant::now())
    }

    fn get_max_idle_inner(&self, now: Instant) -> Duration {
        let bound = self.params.target * 10;
        if let Some(last_emptied) = self.last_emptied {
            if last_emptied < now - bound {
                return self.params.target * 3;
            }
        }
        return bound;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::VecDeque;

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    struct TestHarness {
        cd: ControlledDelay,
        test_start: Instant,
        now: Instant,
        count: usize,
        entries: VecDeque<(usize, Instant)>,
    }

    impl TestHarness {
        fn new(cd: ControlledDelay) -> Self {
            let now = Instant::now();
            Self {
                cd,
                test_start: now,
                now,
                count: 0,
                entries: VecDeque::new(),
            }
        }

        // Add an entry to the queue at "now"
        fn push(&mut self) -> &mut Self {
            self.entries.push_back((self.count, self.now));
            self.count += 1;
            self
        }

        // Wait for a certain length of time
        fn wait(&mut self, length: Duration) -> &mut Self {
            self.now = self.now.checked_add(length).unwrap();
            self
        }

        // Pop an entry from the queue, and assert that we should
        // not drop it.
        fn expect_dequeue(&mut self) -> &mut Self {
            let (count, start) = self.entries.pop_front().unwrap();
            let now = self.now;

            assert!(
                !self.cd.should_drop_inner(now, start),
                "Expected that we would not drop this entry:\n\
                 \tEntry #{count} (zero-indexed) \n\
                 \tEntered Queue at {entry} ms \n\
                 \tExited Queue at {exit} ms \n\
                 {cd}\n",
                entry = (start - self.test_start).as_millis(),
                exit = (now - self.test_start).as_millis(),
                cd = self.debug_cd(),
            );
            self
        }

        // Pop an entry from the queue, and assert that we should
        // drop it.
        fn expect_drop(&mut self) -> &mut Self {
            let (count, start) = self.entries.pop_front().unwrap();
            let now = self.now;

            assert!(
                self.cd.should_drop_inner(now, start),
                "Expected that we would drop this entry:\n\
                 \tEntry #{count} (zero-indexed) \n\
                 \tEntered Queue at {entry} ms \n\
                 \tExited Queue at {exit} ms\n\
                 {cd}\n",
                entry = (start - self.test_start).as_millis(),
                exit = (now - self.test_start).as_millis(),
                cd = self.debug_cd(),
            );
            self
        }

        fn debug_cd(&self) -> String {
            let must_hit_target_by = match self.cd.must_hit_target_by {
                Some(must_hit_target_by) => {
                    format!(
                        "{} ms",
                        must_hit_target_by.duration_since(self.test_start).as_millis()
                    )
                },
                None => "None".to_string(),
            };

            let drop_next = match self.cd.drop_next {
                Some(drop_next) => {
                    format!(
                        "{} ms",
                        drop_next.duration_since(self.test_start).as_millis()
                    )
                },
                None => "None".to_string(),
            };

            format!("ControlledDelay:\n\
                \tdropping: {dropping}\n\
                \tmust_hit_target_by: {must_hit_target_by},\n\
                \tdrop_next: {drop_next}\n\
                \tdrop_count: {drop_count}",
                dropping = self.cd.dropping,
                drop_count = self.cd.drop_count,
            )
        }


    }

    #[test]
    fn quick_requests_do_not_overload() {
        let params = ControlledDelayParameters {
            target: ms(5),
            interval: ms(100),
            interval_hysteresis_factor: 0,
        };
        let cd = ControlledDelay::new(params.clone());
        let mut harness = TestHarness::new(cd);

        harness
            .push()             // >-> 1 ms to complete
            .wait(ms(1))        //   |
            .push()             // >-|-> completes immediately
            .expect_dequeue()   // <-< |
            .expect_dequeue();  // <---<
    }

    #[test]
    fn slow_requests_cause_drops() {
        let params = ControlledDelayParameters {
            target: ms(5),
            interval: ms(100),
            interval_hysteresis_factor: 0,
        };
        let cd = ControlledDelay::new(params.clone());
        let mut harness = TestHarness::new(cd);

        harness
            .push()           // >-> 100 ms to complete
            .push()           // >-|-> 200 ms to complete
            .push()           // >-|-|-> 300 ms to complete
            .wait(ms(100))    //   | | |
            .expect_dequeue() // <-< | |
            .wait(ms(100))    //     | |
            .expect_drop()    // <---< |
            .wait(ms(100))    //       |
            .expect_drop();   // <-----<
    }

    #[test]
    fn keep_dropping_at_interval() {
        let params = ControlledDelayParameters {
            target: ms(5),
            interval: ms(100),
            interval_hysteresis_factor: 0,
        };
        let cd = ControlledDelay::new(params.clone());
        let mut harness = TestHarness::new(cd);

        // Request #0
        harness
            .push()
            .wait(ms(100))
            // Sets deadline to "100 ms from now"
            .expect_dequeue();

        // Request #1
        harness
            .push()
            .wait(ms(100))
            // Barely Misses deadline, gets dropped. Also sets
            // "drop_next" to:
            //   now + interval / sqrt(drop_count)
            // = now + 100
            .expect_drop();

        // Request #2
        harness
            .push()
            .wait(ms(100))
            // Sets "drop_next" to:
            //   now + interval / sqrt(drop_count)
            // = now + 70.7
            .expect_drop();

        // Request #3
        harness
            .push()
            .wait(ms(71))
            // Sets "drop_next" to:
            //   now + interval / sqrt(drop_count)
            // = now + 57.7
            .expect_drop();

        // Request #4
        harness
            .push()
            .wait(ms(58))
            .expect_drop();
    }

    #[test]
    fn recover_from_dropping_then_hit_drop_next_timeout() {
        let params = ControlledDelayParameters {
            target: ms(5),
            interval: ms(100),
            interval_hysteresis_factor: 0,
        };
        let cd = ControlledDelay::new(params.clone());
        let mut harness = TestHarness::new(cd);

        harness
            .push() // #0
            .wait(ms(100))
            .expect_dequeue()
            .push() // #1
            .wait(ms(100))
            .expect_drop()
            .push() // #2
            .wait(ms(100))
            .expect_drop()
            .push() // #3
            // See "keep_dropping_at_interval" above, if this was 71 ms,
            // it would be dropped.
            .wait(ms(70))
            .expect_dequeue()
            .push() // #4
            // As long as we're slower than the "target time",
            // we'll pass the "drop_next" interval on this request.
            .wait(ms(6))
            .expect_drop();
    }
}
