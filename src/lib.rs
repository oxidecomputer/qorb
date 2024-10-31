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
//! - `handle-claimed`: Fires after claiming a handle from the pool, before
//!   returning it to the client.
//! - `handle-recycled`: Fires when a handle is returned to the pool, after it
//!   is dropped.
//! - `health-check-start`: Fires when a health check for a backend starts.
//! - `health-check-done`: Fires when a health check for a backend completes
//!   successfully.
//! - `health-check-failed`: Fires when a health check for a backend fails.
//! - `recycle-start`: Fires before attempting to recycle a connection.
//! - `recycle-done`: Fires after successsfully recycling a connection.
//! - `recycle-failed`: Fires when failing to recycle a connection.
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

/// USDT probes for tracing how qorb makes pools and hands out claims.
#[cfg(feature = "probes")]
#[usdt::provider(provider = "qorb")]
mod probes {
    /// Fires right before attempting to acquire a claim from the pool.
    fn claim__start(id: &usdt::UniqueId) {}

    /// Fires when a claim is successfully acquired from the pool.
    fn claim__done(id: &usdt::UniqueId) {}

    /// Fires when we _fail_ to acquire a claim from the pool, with a string
    /// identifying the reason.
    fn claim__failed(id: &usdt::UniqueId, reason: &str) {}

    /// Fires right before attempting to make a connection, with the address
    /// we're connecting to.
    fn connect__start(id: &usdt::UniqueId, addr: &str) {}

    /// Fires just after successfully making a connection.
    fn connect__done(id: &usdt::UniqueId) {}

    /// Fires just after failing to make a connectiona, with a string
    /// identifying the reason.
    fn connect__failed(id: &usdt::UniqueId, reason: &str) {}

    /// Fires when qorb creates a new handle, before returning it in a call to
    /// `qorb::Pool::claim()`. This includes an ID for the handle, and the
    /// address of the backend the handle is connected to.
    fn handle__claimed(id: usize, addr: &str) {}

    /// Fires when a handle is returned to qorb, usually when it is dropped.
    fn handle__returned(id: usize) {}

    /// Fires just before we start a health-check to a backend.
    fn health__check__start(id: &usdt::UniqueId, addr: &str) {}

    /// Fires after a successful health-check to a backend.
    fn health__check__done(id: &usdt::UniqueId) {}

    /// Fires after a failed health-check to a backend.
    fn health__check__failed(id: &usdt::UniqueId, reason: &str) {}

    /// Fires just before we recycle a connection to a backend.
    fn recycle__start(id: &usdt::UniqueId, addr: &str) {}

    /// Fires after successfully recycling a connection to a backend.
    fn recycle__done(id: &usdt::UniqueId) {}

    /// Fires after failing to recycle a connection to a backend.
    fn recycle__failed(id: &usdt::UniqueId, reason: &str) {}
}
