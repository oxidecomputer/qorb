use async_trait::async_trait;
use qorb::backend;
use qorb::policy::{Policy, SetConfig};
use qorb::pool::Pool;
use qorb::resolvers::dns::{DnsResolver, DnsResolverConfig};
use qorb::service;
use std::env;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio::time::Duration;

// This is almost identical to [qorb::connectors::tcp::TcpConnector], but it
// actually has an "is_valid" implementation.
struct TcpConnector {}

#[async_trait]
impl backend::Connector for TcpConnector {
    type Connection = TcpStream;

    async fn connect(
        &self,
        backend: &backend::Backend,
    ) -> Result<Self::Connection, backend::Error> {
        TcpStream::connect(backend.address)
            .await
            .map_err(|e| e.into())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), backend::Error> {
        // It would normally be disruptive to write to an arbitrary server, but
        // since this is an echo server, it's cool for the pool to send and
        // recieve messages.
        let mut buf = [0; 1];
        conn.write_all(b"a").await?;
        conn.read_exact(&mut buf).await?;
        Ok(())
    }
}

enum Tracing {
    Off,
    On,
    ReallyOn,
}

fn tracing(t: Tracing) {
    let (events, max_level) = match t {
        Tracing::Off => return,
        Tracing::On => (FmtSpan::NONE, tracing::Level::INFO),
        Tracing::ReallyOn => (FmtSpan::ENTER, tracing::Level::DEBUG),
    };

    use tracing_subscriber::fmt::format::{format, FmtSpan};
    tracing_subscriber::fmt()
        .event_format(format().compact())
        .with_thread_names(false)
        .with_span_events(events)
        .with_max_level(max_level)
        .with_test_writer()
        .init();
}

fn usage(args: &[String]) {
    eprintln!("Usage: {} <options>", args[0]);
    eprintln!("Options may include: ");
    eprintln!("  --help: See this help message");
    eprintln!("  --tracing: Enable tracing");
    eprintln!("  --super-tracing: Enable more tracing");
    eprintln!("  --bootstrap=<DNS address>: Provide a bootstrap DNS address");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut trace_level = Tracing::Off;
    let mut bootstrap_address = "[::1]:1234".parse().unwrap();
    for arg in &args[1..] {
        match arg.as_str() {
            "--tracing" => trace_level = Tracing::On,
            "--super-tracing" => trace_level = Tracing::ReallyOn,
            "--help" => {
                usage(&args);
                return;
            }
            other => match other.split_once('=') {
                Some(("--bootstrap", address)) => {
                    bootstrap_address = address.parse().unwrap();
                }
                _ => {
                    usage(&args);
                    return;
                }
            },
        }
    }

    tracing(trace_level);

    // We need to tell the pool about at least one DNS server to get the party
    // started.
    let bootstrap_dns = vec![bootstrap_address];

    // This pool will try to lookup the echo server, using configuration
    // defined in "example/dns_server/test.com.zone".
    let resolver = Box::new(DnsResolver::new(
        service::Name("_echo._tcp.test.com.".to_string()),
        bootstrap_dns,
        DnsResolverConfig {
            query_interval: Duration::from_secs(5),
            query_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    ));
    // We're using a custom connector that lets the pool perform health
    // checks on its own.
    let backend_connector = Arc::new(TcpConnector {});

    // Tweak some of the default intervals to a smaller duration, so
    // it's easier to see what's going on.
    let policy = Policy {
        set_config: SetConfig {
            max_connection_backoff: Duration::from_secs(5),
            health_interval: Duration::from_secs(3),
            ..Default::default()
        },
        rebalance_interval: Duration::from_secs(10),
        ..Default::default()
    };

    // Actually make the pool!
    let pool = Pool::new(resolver, backend_connector, policy);

    #[cfg(feature = "qtop")]
    tokio::spawn(qtop(pool.stats().clone()));

    // In a loop:
    //
    // - Grab a connection from the pool
    // - Try to use it
    loop {
        sleep(Duration::from_millis(250)).await;

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
                println!("Contacted server - OK");
            }
            Err(err) => {
                eprintln!("Failed to grab claim: {err:?}");
            }
        }
    }
}

#[cfg(feature = "qtop")]
async fn qtop(stats: qorb::pool::Stats) {
    // Build a description of the API.
    let mut api = dropshot::ApiDescription::new();
    api.register(qorb::qtop::serve_stats).unwrap();
    let log = dropshot::ConfigLogging::StderrTerminal {
        level: dropshot::ConfigLoggingLevel::Info,
    };
    // Set up the server.
    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot {
            bind_address: "127.0.0.1:42069".parse().unwrap(),
            ..Default::default()
        },
        api,
        stats,
        &log.to_logger("qtop").unwrap(),
    )
    .map_err(|error| format!("failed to create server: {}", error))
    .unwrap()
    .start()
    .await
    .unwrap();
}
