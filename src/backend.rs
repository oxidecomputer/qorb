//! The interface for identifying and connecting to backend services.

use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O Error")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(anyhow::Error),
}

/// Describes the name of a backend.
#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug, Hash)]
pub struct Name(pub Arc<str>);

impl Name {
    pub fn new(name: impl ToString) -> Self {
        Self(name.to_string().into())
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl From<String> for Name {
    fn from(s: String) -> Self {
        Self(s.into())
    }
}

impl From<&'_ str> for Name {
    fn from(s: &'_ str) -> Self {
        Self(s.into())
    }
}

impl std::borrow::Borrow<str> for Name {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// A single instance of a service.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Ord, PartialOrd)]
pub struct Backend {
    pub address: SocketAddr,
}

/// Interface for raw connections.
pub trait Connection: Send + 'static {}

impl<T> Connection for T where T: Send + 'static {}

/// Describes how a connection to a Backend should be constructed.
#[async_trait]
pub trait Connector: Send + Sync {
    type Connection: Connection;

    /// Creates a connection to a backend.
    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, Error>;

    /// Determines if the connection to a backend is still valid.
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Error>;
}

pub type SharedConnector<Conn> = Arc<dyn Connector<Connection = Conn>>;
