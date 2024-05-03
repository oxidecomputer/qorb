use std::collections::HashMap;
use thiserror::Error;

use crate::backend;
use crate::claim;
use crate::connection::Connection;
use crate::policy::Policy;
use crate::resolver;
use crate::slot;

#[derive(Error, Debug)]
pub enum Error {
    #[error("No backends found for this service")]
    NoBackends,

    #[error(transparent)]
    Slot(#[from] slot::Error),

    #[error("Cannot resolve backend name for service")]
    Resolve(#[from] resolver::ResolveError),
}

/// Manages a set of connections to a service
pub struct Pool<Conn: Connection> {
    backend_connector: backend::BoxedBackendConnector<Conn>,
    resolver: resolver::BoxedResolver,
    slots: HashMap<backend::Backend, slot::Set<Conn>>,
    policy: Policy,
}

impl<Conn: Connection + Send + 'static> Pool<Conn> {
    /// Creates a new connection pool.
    ///
    /// - resolver: Describes how backends should be found for the service.
    /// - backend_connector: Describes how the connections to a specific
    /// backend should be made.
    pub fn new(
        resolver: resolver::BoxedResolver,
        backend_connector: backend::BoxedBackendConnector<Conn>,
        policy: Policy,
    ) -> Self {
        Self {
            backend_connector,
            resolver,
            slots: HashMap::new(),
            policy,
        }
    }

    pub async fn claim(&self) -> Result<claim::Handle<Conn>, Error> {
        // TODO: We need a smarter policy to pick the backend.
        //
        // This is where the priority list could come into play.
        //
        // NOTE: For now, we're trying the first one?
        let Some(set) = self.slots.values().next() else {
            return Err(Error::NoBackends);
        };

        let claim = set.claim().await?;
        Ok(claim)
    }
}
