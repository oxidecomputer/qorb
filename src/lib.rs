//! qorb is a connection pooling crate.
//!
//! qorb offers a flexible interface for managing connections.
//!
//! It uses the following terminology:
//! * Services are named entities providing the same interface.
//! * Backends are specific instantiations of a program, providing
//! a service. In the case of, e.g., a distributed database, a single
//! service would be provided by multiple backends.

pub mod backend;
pub mod codel;
pub mod connection;
pub mod policy;
pub mod pool;
pub mod resolver;
pub mod service;

pub mod resolvers;
