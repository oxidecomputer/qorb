//! The interface for the resolver, which finds backends.

use crate::backend::{self, Backend};

use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Clone, Debug)]
pub enum Error {}

/// Events sent to the connection pool from the resolver
#[derive(Clone)]
pub enum Event {
    /// One or more backends have been added
    Added(Vec<(backend::Name, Backend)>),

    /// One or more backends have been removed
    Removed(Vec<backend::Name>)
}

/// Translates a service name into a set of backends.
///
/// The resolver is responsible for knowing which [crate::service::Name]
/// it is resolving. It is responsible for reporting the set of
/// all possible backends, but not reporting nor tracking their health.
#[async_trait]
pub trait Resolver: Send + Sync {
    /// Monitors for new backends, and returns information about the health
    /// of the resolver.
    async fn step(&mut self) -> Vec<Event>;
}

/// Helper type for anything that implements the Resolver interface.
pub type BoxedResolver = Box<dyn Resolver>;
