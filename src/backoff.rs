use rand::{rng, Rng};
use tokio::time::Duration;

pub trait ExponentialBackoff: Sized {
    fn add_spread(&self, spread: Duration) -> Self;
    fn exponential_backoff(&self, max: Duration) -> Self;
}

impl ExponentialBackoff for Duration {
    fn add_spread(&self, spread: Duration) -> Self {
        let mut rng = rng();
        let nanos = spread.as_nanos();
        let spread = if nanos == 0 {
            0
        } else {
            rng.random_range(0..spread.as_nanos())
        };
        self.saturating_add(Duration::from_nanos(spread.try_into().unwrap()))
    }

    fn exponential_backoff(&self, max: Duration) -> Self {
        std::cmp::min(self.saturating_mul(2), max)
    }
}
