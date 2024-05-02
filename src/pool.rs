use thiserror::Error;

use crate::backend;
use crate::connection::Connection;
use crate::policy::Policy;
use crate::resolver;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot resolve backend name for service")]
    Resolve(#[from] resolver::ResolveError),
}

/// Manages a set of connections to a service
pub struct Pool<Conn: Connection> {
    backend_connector: backend::BoxedBackendConnector<Conn>,
    resolver: resolver::BoxedResolver,
    policy: Policy,
}

impl<Conn: Connection> Pool<Conn> {
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
            policy,
        }
    }

    // TODO: Don't do the work here!
    pub fn claim_connection(&self) -> Result<Conn, Error> {
        todo!();
    }
}
