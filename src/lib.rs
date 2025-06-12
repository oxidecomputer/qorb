//! qorb is a connection pooling crate.
//!
//! qorb offers a flexible interface for managing connections.
//!
//! It uses the following terminology:
//! * Services are named entities providing the same interface.
//! * Backends are specific instantiations of a program, providing
//!   a service. In the case of, e.g., a distributed database, a single
//!   service would be provided by multiple backends.
//!
//! # Usage
//!
//! * The main interface for this crate is [pool::Pool].
//! * To construct a pool, you must supply a [resolver::Resolver] and
//!   a [backend::Connector]. These are interfaces which specify "how to find
//!   backends" and "how to create connections to a backend", respectively.
//!
//! # DTrace probes
//!
//! qorb contains a number of DTrace USDT probes, which fire as the qorb pool
//! manages its connections. These probes provide visibility into when qorb
//! initiates connections; checks health; and vends out claims from a pool. The
//! full list of probes is:
//!
//! - `claim-start`: Fires before attempting to make take a claim from the pool.
//! - `claim-done`: Fires before returning a successful claim to the client.
//! - `claim-failed`: Fires on failure to take a claim from the pool.
//! - `connect-start`: Fires before attempting a connection to a backend.
//! - `connect-done`: Fires after successfully connecting to a backend.
//! - `connect-failed`: Fires after failing to connect to a backend.
//! - `pool-claim-start`: Fires when a pool tries to make a claim, on behalf of
//!   a user-requested claim-start.
//! - `pool-claim-done`: Fires when a pool successfully makes a claim.
//! - `pool-claim-failed`: Fires when a pool cannot make a claim.
//! - `slot-set-claim-start`: Fires when a claim has been made to a backend.
//! - `slot-set-claim-done`: Fires when a backend returns a claim.
//! - `slot-set-claim-failed`: Fires when a backend cannot return a claim.
//! - `handle-claimed`: Fires after claiming a handle from the pool, before
//!   returning it to the client.
//! - `handle-returned`: Fires when a handle is returned to the pool, after it
//!   is dropped.
//! - `health-check-start`: Fires when a health check for a backend starts.
//! - `health-check-done`: Fires when a health check for a backend completes
//!   successfully.
//! - `health-check-failed`: Fires when a health check for a backend fails.
//! - `recycle-start`: Fires before attempting to recycle a connection.
//! - `recycle-done`: Fires after successsfully recycling a connection.
//! - `recycle-failed`: Fires when failing to recycle a connection.
//! - `rebalance-start`: Fires when rebalancing the connection pool.
//! - `rebalance-done`: Fires when the connection pool is done rebalancing.
//!
//! The existence of the probes is behind the `"probes"` feature, which is
//! enabled by default. Probes are zero-cost unless they are explicitly enabled,
//! by tracing the program with the `dtrace(1)` command-line tool.
//!
//! On most systems, the USDT probes must be registered with the DTrace kernel
//! module. This process is technically fallible, although extremely unlikely to
//! fail in practice. To account for this, the `pool::Pool::new` constructor is
//! fallible, returning an `Err` if registration fails. However, it's very
//! context-dependent what one wants to do with this failure -- some
//! applications may choose to panic, while others would rather have a pool that
//! can't be instrumented rather than no pool at all.
//!
//! To support both of these cases, the `Result` returned from `pool::Pool::new`
//! gives access to the pool itself in both the `Ok` or `Err` variant. Some
//! applications may just `unwrap()` or propagate an error, while others may
//! choose to extract the pool in either case. (This is similar to the
//! `std::sync::PoisonError`.)

// Public API
pub mod backend;
pub mod claim;
pub mod policy;
pub mod pool;
#[cfg(feature = "qtop")]
pub mod qtop;
pub mod resolver;
pub mod service;
pub mod slot;

// Necessary for implementation
mod backoff;
// mod codel;
mod join;
mod priority_list;
mod rebalancer;
#[cfg(test)]
mod test_utils;
mod window_counter;

// Default implementations of generic interfaces
pub mod connectors;
pub mod resolvers;

/// Uniquely identifies a claim
#[derive(Copy, Clone, Debug)]
pub(crate) struct ClaimId(pub u64);

impl ClaimId {
    fn new() -> Self {
        let id = usdt::UniqueId::new().as_u64();
        Self(id)
    }
}

impl Default for ClaimId {
    fn default() -> Self {
        Self::new()
    }
}

/// USDT probes for tracing how qorb makes pools and hands out claims.
#[cfg(feature = "probes")]
#[usdt::provider(provider = "qorb")]
mod probes {
    /// Fires right before attempting to acquire a claim from the pool.
    fn claim__start(pool: &str, claim_id: u64) {}

    /// Fires when a claim is successfully acquired from the pool.
    ///
    /// Also identifies the underlying slot which is being used.
    fn claim__done(pool: &str, claim_id: u64, slot_id: u64) {}

    /// Fires when we _fail_ to acquire a claim from the pool, with a string
    /// identifying the reason.
    fn claim__failed(pool: &str, claim_id: u64, reason: &str) {}

    /// Fires right before attempting to make a connection, with the address
    /// we're connecting to.
    fn connect__start(pool: &str, slot_id: u64, addr: &str) {}

    /// Fires just after successfully making a connection.
    fn connect__done(pool: &str, slot_id: u64, addr: &str) {}

    /// Fires just after failing to make a connection, with a string
    /// identifying the reason.
    fn connect__failed(pool: &str, slot_id: u64, addr: &str, reason: &str) {}

    /// Fires when the pool is attempting to make a claim.
    ///
    /// This is similar to "claim__start", but it follows the internal
    /// pool-specific task, which may happen multiple times if e.g.
    /// waiting for a backend to be ready.
    ///
    /// For example: a user may try to claim a connection before any backends
    /// are ready. This will call "pool__claim__start" and later fail, but the
    /// request will remain enqueued. Once a new backend is available, and
    /// has claims that can be used, this probe will fire again.
    fn pool__claim__start(pool: &str, claim_id: u64) {}

    /// Fires when the pool has made a claim successfully.
    fn pool__claim__done(pool: &str, claim_id: u64, slot_id: u64) {}

    /// Fires when the pool has failed to make any claim.
    fn pool__claim__failed(pool: &str, claim_id: u64) {}

    /// Fires when a claim request has been made to a specific backend
    fn slot__set__claim__start(pool: &str, claim_id: u64, addr: &str) {}

    /// Fires when a slot claim to a specific backend completes successfully
    fn slot__set__claim__done(pool: &str, claim_id: u64, slot_id: u64) {}

    /// Fires when a slot claim to a specific backend fails
    fn slot__set__claim__failed(pool: &str, claim_id: u64, reason: &str) {}

    /// Fires when qorb creates a new handle, before returning it in a call to
    /// `qorb::Pool::claim()`. This includes an ID for the handle, and the
    /// address of the backend the handle is connected to.
    ///
    /// This fires within the "slot set", the moment the slot has been
    /// grabbed successfully.
    fn handle__claimed(pool: &str, claim_id: u64, slot_id: u64, addr: &str) {}

    /// Fires when a handle is returned to qorb, usually when it is dropped.
    fn handle__returned(pool: &str, slot_id: u64) {}

    /// Fires just before we start a health-check to a backend.
    fn health__check__start(pool: &str, slot_id: u64, addr: &str) {}

    /// Fires after a successful health-check to a backend.
    fn health__check__done(pool: &str, slot_id: u64) {}

    /// Fires after a failed health-check to a backend.
    fn health__check__failed(pool: &str, slot_id: u64, reason: &str) {}

    /// Fires just before we recycle a connection to a backend.
    fn recycle__start(pool: &str, slot_id: u64, addr: &str) {}

    /// Fires after successfully recycling a connection to a backend.
    fn recycle__done(pool: &str, slot_id: u64) {}

    /// Fires after failing to recycle a connection to a backend.
    fn recycle__failed(pool: &str, slot_id: u64, reason: &str) {}

    /// Fires when the connection pool is rebalancing
    fn rebalance__start(pool: &str) {}

    /// Fires when the connection pool has finished rebalancing
    fn rebalance__done(pool: &str) {}
}
