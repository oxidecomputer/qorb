//! Implementation of [Connector] for TCP.

use crate::backend::{self, Backend, Error};

use async_trait::async_trait;
use tokio::net::TcpStream;

pub struct TcpConnector {}

#[async_trait]
impl backend::Connector for TcpConnector {
    type Connection = TcpStream;

    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, Error> {
        TcpStream::connect(backend.address)
            .await
            .map_err(|e| e.into())
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Error> {
        // NOTE: Although it would be possible to send requests on a TcpStream,
        // doing so requires some cooperation from the server.
        //
        // Zero-length reads don't really distinguish between "bad client
        // arguments" and "server closed connection", and any "real" read
        // interferes with the connections traffic.
        //
        // Although we might recommend a keepalive on the connection flow, this
        // method could be implemented by a simple "ping/pong" message with
        // cooperating servers.
        Ok(())
    }
}
