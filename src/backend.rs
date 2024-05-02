use crate::connection;

use std::net::SocketAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {}

/// Describes the name of a backend.
pub struct Name(pub String);

/// A single instance of a service.
pub struct Backend {
    name: Name,
    address: SocketAddr,
}

/// Describes how a connection to a Backend should be constructed.
pub trait BackendConnector {
    type Connection: connection::Connection;

    fn connect(&self, backend: &Backend) -> Result<Self::Connection, Error>;
}

pub type BoxedBackendConnector<Conn> = Box<dyn BackendConnector<Connection = Conn>>;
