use rand::{thread_rng, Rng};
use tokio::time::Duration;

pub trait ExponentialBackoff: Sized {
    fn add_spread(&self, spread: Duration) -> Self;
    fn exponential_backoff(&self) -> Self;
}

impl ExponentialBackoff for Duration {
    fn add_spread(&self, spread: Duration) -> Self {
        let mut rng = thread_rng();
        let spread = rng.gen_range(0..spread.as_nanos());
        self.saturating_add(Duration::from_nanos(spread.try_into().unwrap()))
    }

    fn exponential_backoff(&self) -> Self {
        self.saturating_mul(2)
    }
}
