//! qorb is a connection pooling crate.
//!
//! qorb offers a flexible interface for managing connections.
//!
//! It uses the following terminology:
//! * Services are named entities providing the same interface.
//! * Backends are specific instantiations of a program, providing
//! a service. In the case of, e.g., a distributed database, a single
//! service would be provided by multiple backends.

// Public API
pub mod backend;
pub mod claim;
pub mod connection;
pub mod policy;
pub mod pool;
pub mod resolver;
pub mod service;

// Necessary for implementation
// mod codel;
mod slot;

// Default implementations of generic interfaces
pub mod resolvers;
