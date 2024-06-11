use anyhow::anyhow;
use async_trait::async_trait;
use progenitor::generate_api;
use qorb::backend::{self, Backend};

generate_api!("examples/dropshot_echo_server/openapi.json");

#[allow(dead_code)]
struct EchoClientConnector {}

#[async_trait]
impl backend::Connector for EchoClientConnector {
    type Connection = Client;

    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, backend::Error> {
        Ok(Client::new(&format!(
            "http://{address}",
            address = backend.address
        )))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), backend::Error> {
        let response = conn
            .echo("hi")
            .await
            .map_err(|e| backend::Error::Other(anyhow!(e)))?;
        if response.into_inner() != "hi" {
            backend::Error::Other(anyhow!("Unexpected response"));
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // TODO: Create pool, use EchoClientConnector
}
