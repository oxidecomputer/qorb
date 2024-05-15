//! Configuration options which can alter the behavior of the pool.

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
    /// This is a hard capacity across all backends combined.
    ///
    /// To adjust the capacity for each backend individually, see:
    /// [SetConfig::max_count].
    pub max_slots: usize,

    /// The interval at which rebalancing backends should occur.
    pub rebalance_interval: Duration,

    /// Configuration for a slot set attempting to connect to a backend.
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

/// Configuration options for "Slot Sets".
///
/// Slot sets are groups of slots which all are connected to the same backend.
#[derive(Clone, Debug)]
pub struct SetConfig {
    /// The max number of slots for the connection set.
    ///
    /// The total number of slots across all backends is capped
    /// by [Policy::max_slots].
    pub max_count: usize,

    /// The minimum time before retrying connection requests
    pub min_connection_backoff: Duration,

    /// The maximum time to backoff between connection requests
    pub max_connection_backoff: Duration,

    /// When retrying a connection, add a random amount of delay between [0, spread).
    ///
    /// If "Duration::ZERO" is used, no spread is added.
    pub spread: Duration,

    /// How long to wait before checking on the health of a connection.
    ///
    /// This has no backoff - on success, we wait this same period, and on
    /// failure, we reconnect, which uses the normal "connection_backoff"
    /// configs.
    ///
    /// To prevent periodic checks, supply "Duration::MAX".
    pub health_interval: Duration,

    /// The maximum amount of time a health check has before failing.
    pub health_check_timeout: Duration,
}

impl Default for SetConfig {
    fn default() -> Self {
        Self {
            max_count: 16,
            min_connection_backoff: Duration::from_millis(20),
            max_connection_backoff: Duration::from_secs(30),
            spread: Duration::from_millis(20),
            health_interval: Duration::from_secs(30),
            health_check_timeout: Duration::from_secs(3),
        }
    }
}
