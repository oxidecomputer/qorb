//! The interface for the resolver, which finds backends.

use crate::backend::{self, Backend};

use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::watch;

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
