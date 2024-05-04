use crate::backend::{self, Backend};
use thiserror::Error;
use tokio::sync::broadcast;

#[derive(Clone)]
pub enum ResolverState {
    Stopped,
    Stopping,
    Starting,
    Running,
    Failed { err: ResolveError },
}

#[derive(Error, Clone, Debug)]
pub enum ResolveError {}

#[derive(Clone)]
pub enum ResolveEvent {
    BackendAdded { backend: Backend },
    BackendRemoved { name: backend::Name },
    StateChange { state: ResolverState },
}

/// Translates a service name into a set of backends.
///
/// The resolver is responsible for knowing which [crate::service::Name]
/// it is resolving. It is responsible for reporting the set of
/// all possible backends, but not reporting nor tracking their health.
pub trait Resolver: Send + Sync {
    /// Allows a caller to monitor for updates from the resolver.
    fn monitor(&self) -> broadcast::Receiver<ResolveEvent>;

    /// Access the last-known set of backends.
    ///
    /// Note that it is preferable to call "monitor", as this
    /// value can become out-of-date.
    fn backends(&self) -> Vec<Backend>;

    /// Access the last-known state.
    ///
    /// Note that it is preferable to call "monitor", as this
    /// value can become out-of-date.
    fn state(&self) -> ResolverState;
}

pub type BoxedResolver = Box<dyn Resolver>;
