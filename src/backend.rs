use crate::connection;

use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {}

/// Describes the name of a backend.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct Name(pub String);

/// A single instance of a service.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct Backend {
    pub address: SocketAddr,
}

/// Describes how a connection to a Backend should be constructed.
#[async_trait]
pub trait Connector: Send + Sync {
    type Connection: connection::Connection;

    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, Error>;
}

pub type SharedConnector<Conn> = Arc<dyn Connector<Connection = Conn>>;
