use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Mutex,
};
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct WindowedCounter {
    /// The instant the WindowedCounter was created
    anchor: Instant,

    /// "number of milliseconds since the anchor", indicating the start of our
    /// current time slice
    epoch: AtomicU64,

    /// The indes of our current epoch, within `self.buckets`.
    ///
    /// Note that values are not stored to that bucket until the current epoch
    /// is finished; they are stored within `self.current`.
    epoch_index: Mutex<usize>,

    /// Buckets tracking the number of events counted over `1/NUM_BUCKETS` of
    /// the time window.
    buckets: [AtomicUsize; NUM_BUCKETS],

    /// The fraction of the time window represented by one bucket, in
    /// milliseconds.
    bucket_window_ms: u64,

    /// Writes to the current time slice, committed when expiring past buckets.
    ///
    /// This is a value, not an index.
    current: AtomicUsize,
}

const NUM_BUCKETS: usize = 10;

impl WindowedCounter {
    pub fn new(window: Duration) -> Self {
        // clippy doesn't like interior mutable items in `const`s, because
        // mutating an instance of the `const` value will not mutate the const.
        // that is the *correct* behavior here, as the const is used just as an
        // array initializer; every time it's referenced, it *should* produce a
        // new value. therefore, this warning is incorrect in this case.
        //
        // see https://github.com/rust-lang/rust-clippy/issues/7665
        #[allow(clippy::declare_interior_mutable_const)]
        const ATOMIC_USIZE_ZERO: AtomicUsize = AtomicUsize::new(0);

        WindowedCounter {
            anchor: Instant::now(),
            epoch: AtomicU64::new(0),
            epoch_index: Mutex::new(0),
            buckets: [ATOMIC_USIZE_ZERO; NUM_BUCKETS],
            bucket_window_ms: (window / (NUM_BUCKETS) as u32).as_millis() as u64,
            current: AtomicUsize::new(0),
        }
    }

    pub fn add(&self, amount: usize) {
        self.expire();
        // TODO: probably doesn't need to be seqcst...
        self.current.fetch_add(amount, Ordering::SeqCst);
    }

    pub fn sum(&self) -> usize {
        self.expire();
        let current = self.current.load(Ordering::SeqCst);
        let prev: usize = self.buckets.iter().fold(0, |acc, bucket| {
            acc.saturating_add(bucket.load(Ordering::SeqCst))
        });
        current.saturating_add(prev)
    }

    /// Expire all counts outside the time window.
    fn expire(&self) {
        // the unchecked cast to `u64` is almost certainly fine here, since the
        // duration is relative to the epoch anchor, which is the time this
        // counter was created. if the time since the counter was created is
        // `u64::MAX` milliseconds, that would be 1.8E16 seconds, or around 584
        // million years.... if you're still running this code in 500 million
        // years, you probably have worse problems...
        let mut my_epoch = self.anchor.elapsed().as_millis() as u64;
        let cur_epoch = self.epoch.load(Ordering::Acquire);
        let mut delta = my_epoch - cur_epoch;

        if delta < self.bucket_window_ms {
            // still in the current epoch's bucket, we don't need to do
            // anything requiring a lock.
            return;
        }

        let mut epoch_index = self.epoch_index.lock().unwrap();
        my_epoch = self.anchor.elapsed().as_millis() as u64;
        match self
            .epoch
            .compare_exchange(cur_epoch, my_epoch, Ordering::AcqRel, Ordering::Acquire)
        {
            // advanced the epoch, time to actually expire old counts.
            Ok(_) => {}
            // someone else advanced the epoch while we were doing math, nothing
            // else for us to do here...
            Err(actual) => {
                dbg!(actual);
                return;
            }
        }

        // commit all the writes in the current bucket
        let to_commit = self.current.swap(0, Ordering::SeqCst);
        self.buckets[*epoch_index].store(to_commit, Ordering::SeqCst);

        // clear all the buckets that we've passed over based on the elapsed
        // time delta.
        let mut i = (*epoch_index + 1) % NUM_BUCKETS;
        while delta > self.bucket_window_ms {
            self.buckets[i].store(0, Ordering::SeqCst);
            delta -= self.bucket_window_ms;
            i = (i + 1) % NUM_BUCKETS;
        }

        // always zero the current bucket, since its count will be stored in
        // `self.current`, rather than in the bucket itself.
        self.buckets[i].store(0, Ordering::SeqCst);
        *epoch_index = i;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time;

    fn counter() -> WindowedCounter {
        WindowedCounter::new(time::Duration::from_secs(3))
    }

    #[tokio::test]
    async fn sum_no_sliding() {
        time::pause();
        let ctr = counter();

        ctr.add(1);
        assert_eq!(1, ctr.sum());
        ctr.add(1);
        assert_eq!(2, ctr.sum());
        ctr.add(3);
        assert_eq!(5, ctr.sum());
    }

    #[tokio::test]
    async fn sliding_short_window() {
        time::pause();
        let ctr = counter();

        dbg!(ctr.add(1));
        assert_eq!(1, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(1, dbg!(ctr.sum()));

        dbg!(ctr.add(2));
        assert_eq!(3, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(3, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(2, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(0, dbg!(ctr.sum()));
    }

    #[tokio::test]
    async fn sliding_over_large_window() {
        time::pause();
        let ctr = WindowedCounter::new(Duration::from_secs(20));

        for i in 0..21 {
            ctr.add(dbg!(i % 3));
            dbg!(time::advance(Duration::from_secs(1)).await);
        }

        assert_eq!(20, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(18, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(18, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(5)).await);
        assert_eq!(12, dbg!(ctr.sum()));
        dbg!(ctr.add(1));

        dbg!(time::advance(Duration::from_secs(10)).await);
        assert_eq!(3, dbg!(ctr.sum()));
    }

    #[tokio::test]
    async fn sliding_past_empty_buckets() {
        time::pause();
        let ctr = counter();

        dbg!(ctr.add(1));
        assert_eq!(1, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        dbg!(ctr.add(2));
        assert_eq!(3, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(1)).await);
        dbg!(ctr.add(1));
        assert_eq!(4, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(2)).await);
        assert_eq!(1, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(100)).await);
        assert_eq!(0, dbg!(ctr.sum()));

        dbg!(ctr.add(100));
        dbg!(time::advance(Duration::from_secs(1)).await);
        assert_eq!(100, dbg!(ctr.sum()));

        dbg!(ctr.add(100));
        dbg!(time::advance(Duration::from_secs(1)).await);

        dbg!(ctr.add(100));
        assert_eq!(300, dbg!(ctr.sum()));

        dbg!(time::advance(Duration::from_secs(100)).await);
        assert_eq!(0, dbg!(ctr.sum()));
    }
}
