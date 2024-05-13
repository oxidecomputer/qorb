//! Configuration options which can alter the behavior of the pool.

use crate::slot::SetConfig;
use tokio::time::Duration;

/// Policy which is applicable to a connection pool.
#[derive(Clone, Debug)]
pub struct Policy {
    /// The interval at which rebalancing backends should occur.
    pub rebalance_interval: Duration,

    /// Configuration for a slot set attempting to connect to a backend
    pub set_config: SetConfig,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            rebalance_interval: Duration::from_secs(60),
            set_config: SetConfig::default(),
        }
    }
}
