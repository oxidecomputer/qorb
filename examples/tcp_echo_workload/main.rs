use qorb::connectors::tcp::TcpConnector;
use qorb::policy::Policy;
use qorb::pool::Pool;
use qorb::resolvers::dns::{DnsResolver, DnsResolverConfig};
use qorb::service;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    use tracing_subscriber::fmt::format::FmtSpan;
    tracing_subscriber::fmt()
        .with_thread_names(true)
        .with_span_events(FmtSpan::ENTER)
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .init();

    let bootstrap_dns = vec!["[::1]:1234".parse().unwrap()];

    let resolver = Box::new(DnsResolver::new(
        service::Name("_echo._tcp.test.com.".to_string()),
        bootstrap_dns,
        DnsResolverConfig::default(),
    ));
    let backend_connector = Arc::new(TcpConnector {});
    let policy = Policy::default();

    let pool = Pool::new(resolver, backend_connector, policy);

    loop {
        sleep(Duration::from_secs(1)).await;

        println!("making claim");
        match pool.claim().await {
            Ok(mut stream) => {
                if let Err(err) = stream.write_all(b"hello").await {
                    eprintln!("Failed to write to server: {err:?}");
                    continue;
                }

                let mut buf = [0; 5];
                if let Err(err) = stream.read_exact(&mut buf[..]).await {
                    eprintln!("Failed to read from server: {err:?}");
                    continue;
                }
                assert_eq!(&buf, b"hello");
                println!("Contacted server!");
            }
            Err(err) => {
                eprintln!("Failed to grab claim: {err:?}");
            }
        }
    }
}
