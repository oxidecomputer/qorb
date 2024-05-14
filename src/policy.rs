//! Configuration options which can alter the behavior of the pool.

use crate::slot::SetConfig;
use tokio::time::Duration;

/// Policy which is applicable to a connection pool.
#[derive(Clone, Debug)]
pub struct Policy {
    /// The total number of spares wanted within the pool.
    ///
    /// This is the count of spares, across all backends, that the pool attempts
    /// to keep available, as "unclaimed connections".
    ///
    /// As an example, if N claims are checked out of the pool, the pool will
    /// attempt to create "N + spares_wanted" total connections.
    pub spares_wanted: usize,

    /// The maximum number of connections within the pool.
    ///
    /// This is a hard capacity, which prevents "spares_wanted" from always
    /// growing, even when connections are claimed from the pool.
    pub max_slots: usize,

    /// The interval at which rebalancing backends should occur.
    pub rebalance_interval: Duration,

    /// Configuration for a slot set attempting to connect to a backend
    pub set_config: SetConfig,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            spares_wanted: 8,
            max_slots: 16,
            rebalance_interval: Duration::from_secs(60),
            set_config: SetConfig::default(),
        }
    }
}
