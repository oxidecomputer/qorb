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

impl Backend {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }
}

/// Interface for raw connections.
pub trait Connection: Send + 'static {}

impl<T> Connection for T where T: Send + 'static {}

/// Describes how a connection to a Backend should be constructed.
#[async_trait]
pub trait Connector: Send + Sync {
    type Connection: Connection;

    /// Creates a connection to a backend.
    ///
    /// If this function returns "Ok(Self::Connection)", qorb assumes the
    /// connection is valid. The implementation of this method should take
    /// care not to be "lazy" - if this connection method succeeds, but
    /// "Self::is_valid" would fail, the connections in the pool will
    /// churn between "connecting" and "failing health checks".
    ///
    /// ## Cancel Safety
    ///
    /// The future returned by `connect` may be cancelled by the `qorb` pool if
    /// the slot is dropped. If this occurs and a connection has already been
    /// established, the connection should be closed. If the connection is not
    /// yet established, the attempt to establish it should also be cancelled.
    /// In both cases, any cleanup logic required by the type of connection
    /// should be performed when the `connect` future is dropped.
    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, Error>;

    /// Determines if the connection to a backend is still valid.
    ///
    /// This method is periodically called on connections within the pool
    /// every [crate::policy::SetConfig::health_interval], and can run
    /// for [crate::policy::SetConfig::health_check_timeout] before
    /// timing out.
    ///
    /// By default this method does nothing.
    ///
    /// ## Cancel Safety
    ///
    /// The future returned by `is_valid` may be cancelled by the `qorb` pool if
    /// the slot is dropped, or the health check timeout has passed.
    /// In these cases, the connection will be dropped.
    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Error> {
        Ok(())
    }

    /// Performs validation or setup on the connection as it is being claimed.
    ///
    /// This method is called on connections within the pool as they are being
    /// claimed, and can run for
    /// [crate::policy::SetConfig::health_check_timeout] before timing out.
    ///
    /// Regardless of whether this method fails or not, "on_recycle" is still
    /// invoked for the connection when it is later returned to the pool.
    ///
    /// By default this method does nothing.
    ///
    /// ## Cancel Safety
    ///
    /// The future returned by `on_acquire` may be cancelled by the `qorb` pool
    /// if the health check timeout has passed. If this occurs, the connection
    /// itself will be passed to `on_recycle`, which can decide whether or
    /// not the connection should remain valid.
    async fn on_acquire(&self, _conn: &mut Self::Connection) -> Result<(), Error> {
        Ok(())
    }

    /// Instructs the pool how to clean a connection before it is returned
    /// to the connection pool.
    ///
    /// This method is called on connections within the pool before
    /// they are can be used for new claims. This function can run for
    /// [crate::policy::SetConfig::health_check_timeout] before timing out.
    ///
    /// By default this method calls [Self::is_valid].
    ///
    /// ## Cancel Safety
    ///
    /// The future returned by `on_recycle` may be cancelled by the `qorb` pool
    /// if the health check timeout has passed or the slot has been dropped. If
    /// this occurs, the connection will be dropped.
    async fn on_recycle(&self, conn: &mut Self::Connection) -> Result<(), Error> {
        self.is_valid(conn).await
    }
}

pub type SharedConnector<Conn> = Arc<dyn Connector<Connection = Conn>>;
