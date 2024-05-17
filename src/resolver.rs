//! The interface for the resolver, which finds backends.

use crate::backend::{self, Backend};

use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Error, Clone, Debug)]
pub enum Error {}

/// Events sent to the connection pool from the resolver
#[derive(Clone, Debug)]
pub enum Event {
    /// One or more backends have been added
    Added(Vec<(backend::Name, Backend)>),

    /// One or more backends have been removed
    Removed(Vec<backend::Name>),
}

pub type AllBackends = Arc<BTreeMap<backend::Name, Backend>>;

/// Translates a service name into a set of backends.
///
/// The resolver is responsible for knowing which [crate::service::Name]
/// it is resolving. It is responsible for reporting the set of
/// all possible backends, but not reporting nor tracking their health.
pub trait Resolver: Send + Sync {
    /// Start running a resolver.
    ///
    /// Returns a receiver to track ongoing activity.
    fn monitor(&mut self) -> watch::Receiver<AllBackends>;
}

/// Helper type for anything that implements the Resolver interface.
pub type BoxedResolver = Box<dyn Resolver>;
