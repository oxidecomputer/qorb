use crate::backend::{self, Backend};

use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Clone, Debug)]
pub enum Error {}

#[derive(Clone)]
pub enum Event {
    Added(Vec<(backend::Name, Backend)>),

    Removed(Vec<backend::Name>),
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

pub type BoxedResolver = Box<dyn Resolver>;
