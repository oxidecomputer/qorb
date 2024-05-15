//! Implementation of [Connector] for TCP.

use crate::backend::{self, Backend, Error};

use anyhow::anyhow;
use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::R2D2Connection;
use diesel::Connection;

pub struct DieselPgConnector {
    prefix: String,
    suffix: String,
}

impl DieselPgConnector {
    pub fn new(user: &str, db: &str, args: Option<&str>) -> Self {
        Self {
            prefix: format!("postgresql://{user}"),
            suffix: format!(
                "/{db}{}",
                args.map(|args| format!("?{args}"))
                    .unwrap_or("".to_string())
            ),
        }
    }

    fn to_url(&self, address: std::net::SocketAddr) -> String {
        format!(
            "{prefix}{address}{suffix}",
            prefix = self.prefix,
            suffix = self.suffix,
        )
    }
}

#[async_trait]
impl backend::Connector for DieselPgConnector {
    type Connection = PgConnection;

    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, Error> {
        let url = self.to_url(backend.address);
        PgConnection::establish(&url).map_err(|e| Error::Other(anyhow!(e)))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Error> {
        conn.ping().map_err(|e| Error::Other(anyhow!(e)))
    }
}
