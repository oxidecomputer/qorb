//! Implementation of [Resolver] for DNS

use crate::backend;
use crate::resolver::{AllBackends, Resolver};
use crate::service;
use crate::window_counter::WindowedCounter;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::Protocol;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::TokioAsyncResolver;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{event, instrument, Level};

#[derive(Clone)]
struct BackendRecord {
    /// What backend have we found?
    backend: backend::Backend,
    /// When does this record expire?
    expires_at: Option<Instant>,
}

struct Client {
    resolver: TokioAsyncResolver,
    hardcoded_ttl: Option<Duration>,
    failed_requests: WindowedCounter,
}

impl Client {
    fn new(address: SocketAddr, hardcoded_ttl: Option<Duration>, failure_window: Duration) -> Self {
        let mut rc = ResolverConfig::new();
        rc.add_name_server(NameServerConfig::new(address, Protocol::Udp));
        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false;
        let resolver = TokioAsyncResolver::tokio(rc, opts);
        Self {
            resolver,
            hardcoded_ttl,
            failed_requests: WindowedCounter::new(failure_window),
        }
    }

    fn mark_error(&self) {
        self.failed_requests.add(1);
    }

    #[instrument(skip(self), name = "Client::lookup_socket_v6")]
    async fn lookup_socket_v6(
        &self,
        name: &service::Name,
    ) -> Result<HashMap<backend::Name, BackendRecord>, anyhow::Error> {
        // Look up all the SRV records for this particular name.
        let srv = self.resolver.srv_lookup(&name.0).await?;
        event!(Level::DEBUG, ?srv, "Successfully looked up SRV record");

        let futures = std::iter::repeat(self.resolver.clone())
            .zip(srv.into_iter())
            .map(|(resolver, srv)| async move {
                let target = srv.target();
                let port = srv.port();
                resolver
                    .ipv6_lookup(target.clone())
                    .await
                    .map(|aaaa| (target.to_utf8(), aaaa, port))
                    .map_err(|err| (target.clone(), err))
            });

        // Look up the AAAA records for each of the SRV records.
        let socket_addrs = futures::future::join_all(futures)
            .await
            .into_iter()
            .flat_map(move |target| match target {
                Ok((target, aaaa, port)) => {
                    event!(Level::DEBUG, ?aaaa, "Successfully looked up AAAA record");
                    let expires_at = match self.hardcoded_ttl {
                        Some(duration) => Instant::now().checked_add(duration),
                        None => Some(aaaa.valid_until()),
                    };
                    let name = backend::Name::from(target);
                    Some(aaaa.into_iter().map(move |ip| {
                        (
                            name.clone(),
                            BackendRecord {
                                backend: backend::Backend {
                                    address: SocketAddr::V6(SocketAddrV6::new(*ip, port, 0, 0)),
                                },
                                expires_at,
                            },
                        )
                    }))
                }
                Err(_) => None,
            })
            .flatten()
            .collect();
        Ok(socket_addrs)
    }
}

/// Resolves a service name to a backend by contacting several DNS servers.
struct DnsResolverWorker {
    // Message-passing channel to notify the pool of updates
    watch_tx: watch::Sender<AllBackends>,

    // What service are we trying to find backends for?
    service: service::Name,

    // What DNS servers are we actively contacting?
    dns_servers: HashMap<SocketAddr, Client>,

    // What backends do we think we've found so far?
    backends: HashMap<backend::Name, BackendRecord>,

    config: DnsResolverConfig,
}

impl DnsResolverWorker {
    fn new(
        watch_tx: watch::Sender<AllBackends>,
        service: service::Name,
        bootstrap_servers: Vec<SocketAddr>,
        config: DnsResolverConfig,
    ) -> Self {
        let mut result = Self {
            watch_tx,
            service,
            dns_servers: HashMap::new(),
            backends: HashMap::new(),
            config,
        };
        for address in bootstrap_servers {
            result.ensure_dns_server(address);
        }
        result
    }

    // Begins tracking a DNS server, if it does not already exist.
    fn ensure_dns_server(&mut self, address: SocketAddr) {
        let failure_window = self.config.query_interval * 10;
        self.dns_servers
            .entry(address)
            .or_insert_with(|| Client::new(address, self.config.hardcoded_ttl, failure_window));
    }

    async fn run(mut self) {
        let mut query_interval = tokio::time::interval(self.config.query_interval);
        loop {
            let next_tick = query_interval.tick();
            let next_backend_expiration = self.sleep_until_next_backend_expiration();

            tokio::select! {
                _ = next_tick => {
                    self.query_dns().await;
                },
                backend_name = next_backend_expiration => {
                    if self.backends.remove(&backend_name).is_some() {
                        self.watch_tx.send_modify(|backends| {
                            let backends = Arc::make_mut(backends);
                            backends.remove(&backend_name);
                        });
                    }
                },
            }
        }
    }

    // Looks up a particular service across all known DNS servers.
    //
    // This is currently used to lookup both backends and DNS servers
    // themselves, if dynamic resolution is enabled.
    async fn dns_lookup(
        &self,
        service: &service::Name,
    ) -> Option<HashMap<backend::Name, BackendRecord>> {
        let mut dns_lookup = FuturesUnordered::new();
        dns_lookup.extend(self.dns_servers.values().map(|client| {
            let duration = self.config.query_timeout;
            let service = &service;
            async move {
                let result = timeout(duration, client.lookup_socket_v6(service)).await;
                (client, result)
            }
        }));

        // For all the DNS requests we sent out: Collect results and
        // also update the health of our servers, depending on
        // whether they responded in time or not.
        let first_result = Arc::new(Mutex::new(None));

        dns_lookup
            .for_each_concurrent(Some(self.config.max_dns_concurrency), |(client, result)| {
                let first_result = first_result.clone();
                async move {
                    match result {
                        Ok(Ok(backends)) => {
                            first_result.lock().unwrap().get_or_insert(backends);
                        }
                        Ok(Err(err)) => {
                            event!(Level::ERROR, ?err, "DNS request failed");
                            client.mark_error();
                        }
                        Err(err) => {
                            event!(Level::ERROR, ?err, "DNS request timed out");
                            client.mark_error();
                        }
                    }
                }
            })
            .await;

        // TODO: As a policy choice, we could combine the results of
        // all DNS servers. At the moment, however, we're taking "whoever
        // returned results the fastest".
        let result = first_result.lock().unwrap().take();
        result
    }

    async fn query_for_dns_servers(&mut self) {
        let Some(resolver_service) = &self.config.resolver_service else {
            return;
        };
        let Some(records) = self.dns_lookup(resolver_service).await else {
            return;
        };

        for record in records.values() {
            let address = record.backend.address;
            self.ensure_dns_server(address);
        }
    }

    // Queries DNS servers and updates our set of backends
    async fn query_dns(&mut self) {
        // If requested, update the set of DNS servers we're accessing.
        //
        // This is currently queried on the same interval as the backends
        // we're trying to access, by virtue of just "happening before the
        // backend lookup" within this function.
        self.query_for_dns_servers().await;

        // Query the set of backends for the service we're actually trying to
        // contact.
        let Some(backends) = self.dns_lookup(&self.service).await else {
            return;
        };

        let mut added = vec![];
        let mut removed = vec![];

        let our_backends = &mut self.backends;
        for (name, record) in &backends {
            if !our_backends.contains_key(name) {
                added.push((name.clone(), record.backend.clone()));
            }
        }
        for name in our_backends.keys() {
            if !backends.contains_key(name) {
                removed.push(name.clone());
            }
        }
        *our_backends = backends;

        if added.is_empty() && removed.is_empty() {
            return;
        }

        // Update the client-visible set of backends
        self.watch_tx.send_modify(|backends| {
            let backends = Arc::make_mut(backends);
            for (name, backend) in added {
                backends.insert(name, backend);
            }
            for name in removed {
                backends.remove(&name);
            }
        });
    }

    async fn sleep_until_next_backend_expiration(&self) -> backend::Name {
        let next_expiration = self.backends.iter().reduce(|soonest, backend| {
            let Some(backend_expiration) = backend.1.expires_at else {
                return soonest;
            };
            let (
                _,
                BackendRecord {
                    expires_at: Some(soonest_expiration),
                    ..
                },
            ) = &soonest
            else {
                return backend;
            };
            if backend_expiration < *soonest_expiration {
                backend
            } else {
                soonest
            }
        });

        let Some((name, record)) = next_expiration else {
            let () = futures::future::pending().await;
            unreachable!();
        };

        tokio::time::sleep_until(record.expires_at.unwrap().into()).await;
        name.clone()
    }
}

/// Implements [crate::resolver::Resolver] via UDP DNS lookup.
///
/// Currently only supports Ipv6 addresses.
pub struct DnsResolver {
    handle: tokio::task::JoinHandle<()>,
    watch_rx: watch::Receiver<AllBackends>,
}

impl DnsResolver {
    /// Creates a new DNS resolver which queries for backends.
    ///
    /// - `service`: The name of the SRV records to observe from DNS.
    /// These are associated with AAAA records, and the SocketAddrs represented
    /// by those records are returned through the [crate::resolver::Resolver] interface.
    /// - `bootstrap_servers`: The initial list of DNS servers to query.
    /// - `config`: Additional tweakable configuration options.
    pub fn new(
        service: service::Name,
        bootstrap_servers: Vec<SocketAddr>,
        config: DnsResolverConfig,
    ) -> Self {
        let (watch_tx, watch_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let worker = DnsResolverWorker::new(watch_tx, service, bootstrap_servers, config);
        let handle = tokio::task::spawn(async move {
            worker.run().await;
        });

        Self { handle, watch_rx }
    }
}

impl Drop for DnsResolver {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Resolver for DnsResolver {
    fn monitor(&mut self) -> watch::Receiver<AllBackends> {
        self.watch_rx.clone()
    }
}

// How often do we want to query the DNS servers for updates on the set of
// available backends?
pub const DEFAULT_QUERY_INTERVAL: Duration = Duration::from_secs(60);

// How long do we expect a healthy DNS server to take to respond?
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration options to tweak resolution behavior.
pub struct DnsResolverConfig {
    /// What SRV name should be used to find additional DNS servers?
    ///
    /// Default: None
    pub resolver_service: Option<service::Name>,

    /// How many DNS servers should we query concurrently?
    ///
    /// Default: 5
    pub max_dns_concurrency: usize,

    /// How long should we wait before re-querying DNS servers?
    ///
    /// Default: 60 seconds
    pub query_interval: Duration,

    /// After starting to query a DNS server, how long until we timeout?
    ///
    /// Default: 10 seconds
    pub query_timeout: Duration,

    /// Provides an option to ignore TTL from DNS and use an override
    ///
    /// Default: None, TTL is respected
    pub hardcoded_ttl: Option<Duration>,
}

impl Default for DnsResolverConfig {
    fn default() -> Self {
        Self {
            resolver_service: None,
            max_dns_concurrency: 5,
            query_interval: DEFAULT_QUERY_INTERVAL,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
            hardcoded_ttl: None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use hickory_server::authority::{AuthorityObject, Catalog, ZoneType};
    use hickory_server::proto::rr::{
        rdata, LowerName, Name, RData, Record, RecordSet, RecordType, RrKey,
    };
    use hickory_server::server::ServerFuture;
    use hickory_server::store::in_memory::InMemoryAuthority;
    use std::collections::BTreeMap;
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::str::FromStr;
    use std::sync::Arc;

    fn setup_tracing_subscriber() {
        use tracing_subscriber::fmt::format::FmtSpan;
        tracing_subscriber::fmt()
            .with_thread_names(true)
            .with_span_events(FmtSpan::ENTER)
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .init();
    }

    fn soa_record(name: &str) -> (RrKey, RecordSet) {
        (
            RrKey::new(LowerName::from_str(name).unwrap(), RecordType::SOA),
            Record::from_rdata(
                Name::from_utf8(name).unwrap(),
                0,
                RData::SOA(rdata::SOA::new(
                    Name::from_utf8(name).unwrap(),
                    Name::from_utf8(name).unwrap(),
                    0,
                    0,
                    0,
                    0,
                    0,
                )),
            )
            .into(),
        )
    }

    fn aaaa_record(aaaa: &AAAA, addr: Ipv6Addr) -> (RrKey, RecordSet) {
        (
            RrKey::new(LowerName::from_str(&aaaa.name).unwrap(), RecordType::AAAA),
            Record::from_rdata(
                Name::from_utf8(&aaaa.name).unwrap(),
                aaaa.ttl,
                RData::AAAA(rdata::AAAA::from(addr)),
            )
            .into(),
        )
    }
    fn srv_record(name: &str, aaaa_records: &[AAAA]) -> (RrKey, RecordSet) {
        let mut record_set = RecordSet::new(&Name::from_utf8(name).unwrap(), RecordType::SRV, 0);

        for aaaa in aaaa_records {
            let port = aaaa.port;
            let aaaa_name = &aaaa.name;
            record_set.insert(
                Record::from_rdata(
                    Name::from_utf8(name).unwrap(),
                    0,
                    RData::SRV(rdata::SRV::new(
                        0,
                        0,
                        port,
                        Name::from_utf8(aaaa_name).unwrap(),
                    )),
                ),
                0,
            );
        }

        (
            RrKey::new(LowerName::from_str(name).unwrap(), RecordType::SRV),
            record_set,
        )
    }

    struct AAAA {
        port: u16,
        name: String,
        ttl: u32,
    }

    // Configuring a DNS server with hickory is a mess of configuration options.
    //
    // This builder attempts to make that config slightly easier for tests.
    struct DnsServerBuilder {
        domain: String,
        srv: String,
        aaaa_records: Vec<AAAA>,
    }

    impl DnsServerBuilder {
        fn new(domain: impl ToString, srv: impl ToString) -> Self {
            Self {
                domain: domain.to_string(),
                srv: srv.to_string(),
                aaaa_records: vec![],
            }
        }

        fn add_backend(mut self, port: u16, name: impl ToString, ttl: u32) -> Self {
            self.aaaa_records.push(AAAA {
                port,
                name: name.to_string(),
                ttl,
            });
            self
        }

        async fn run(self) -> SocketAddr {
            let aaaa_records = self
                .aaaa_records
                .iter()
                .map(|aaaa| aaaa_record(&aaaa, Ipv6Addr::LOCALHOST));

            let mut records = BTreeMap::from([
                soa_record(&self.domain),
                srv_record(&self.srv, self.aaaa_records.as_slice()),
            ]);
            records.extend(aaaa_records);

            let authority = Box::new(Arc::new(
                InMemoryAuthority::new(
                    Name::from_utf8(&self.domain).unwrap(),
                    records,
                    ZoneType::Primary,
                    true,
                )
                .unwrap(),
            )) as Box<dyn AuthorityObject>;
            let mut catalog = Catalog::new();
            catalog.upsert(LowerName::from_str(&self.domain).unwrap(), authority);

            let listener = tokio::net::UdpSocket::bind("[::1]:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            event!(Level::DEBUG, ?addr, "New DNS server on address");

            let mut server = ServerFuture::new(catalog);
            server.register_socket(listener);

            tokio::task::spawn(async move {
                server.block_until_done().await.unwrap();
            });

            addr
        }
    }

    #[tokio::test]
    async fn test_resolve_from_one_dns_server() {
        setup_tracing_subscriber();

        // Start the DNS server, which runs independently of the resolver
        let dns_server_address = DnsServerBuilder::new("example.com", "test.example.com")
            .add_backend(1234, "test001.example.com.", 100)
            .add_backend(5678, "test002.example.com.", 100)
            .run()
            .await;

        // Start the resolver, which queries the DNS server
        let service = service::Name("test.example.com".into());
        let bootstrap_servers = vec![dns_server_address];
        let config = DnsResolverConfig::default();
        let mut resolver = DnsResolver::new(service, bootstrap_servers, config);

        // Wait until any number of backends appear
        let mut monitor = resolver.monitor();
        let backends = monitor
            .wait_for(|all_backends| {
                let some_backends = !all_backends.is_empty();
                event!(
                    Level::DEBUG,
                    some_backends,
                    "Waiting for some backends to appear"
                );
                some_backends
            })
            .await
            .unwrap()
            .clone();

        assert_eq!(backends.len(), 2);
        let (name, backend) = backends.iter().next().unwrap();
        assert_eq!(name, &backend::Name::new("test001.example.com."));
        assert_eq!(
            backend.address,
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1234, 0, 0))
        );
    }

    // Tests that we can access our backend services at "test.example.com",
    // when we use DNS resolution to find additional DNS servers at "dns.example.com".
    #[tokio::test]
    async fn dynamic_resolution() {
        setup_tracing_subscriber();

        // Start a DNS server which knows about the backend we're trying to contact.
        //
        // This server contains backend information that's actually useful to the resolver!
        let dns_server_address = DnsServerBuilder::new("example.com", "test.example.com")
            .add_backend(1234, "test001.example.com.", 100)
            .add_backend(5678, "test002.example.com.", 100)
            .run()
            .await;

        // Start another DNS server which knows about the first DNS server only.
        //
        // This server contains no backend information about the "test" service we're trying to
        // reach.
        let bootstrap_dns_server_address = DnsServerBuilder::new("example.com", "dns.example.com")
            .add_backend(dns_server_address.port(), "dns001.example.com.", 100)
            .run()
            .await;

        // Start the resolver, but only with knowledge of the bootstrap server.
        let service = service::Name("test.example.com".into());
        let bootstrap_servers = vec![bootstrap_dns_server_address];
        let config = DnsResolverConfig {
            resolver_service: Some(service::Name("dns.example.com".into())),
            ..Default::default()
        };
        let mut resolver = DnsResolver::new(service, bootstrap_servers, config);

        // Wait until any number of backends appear. For this to happen, we must have looked up
        // the additional DNS server.
        let mut monitor = resolver.monitor();
        let backends = monitor
            .wait_for(|all_backends| {
                let some_backends = !all_backends.is_empty();
                event!(
                    Level::DEBUG,
                    some_backends,
                    "Waiting for some backends to appear"
                );
                some_backends
            })
            .await
            .unwrap()
            .clone();

        assert_eq!(backends.len(), 2);
        let (name, backend) = backends.iter().next().unwrap();
        assert_eq!(name, &backend::Name::new("test001.example.com."));
        assert_eq!(
            backend.address,
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1234, 0, 0))
        );
    }

    async fn wait_for_backends(
        monitor: &mut watch::Receiver<AllBackends>,
        count: usize,
    ) -> AllBackends {
        monitor
            .wait_for(|all_backends| {
                let backend_count = all_backends.len() == count;
                event!(
                    Level::DEBUG,
                    all_backends = ?all_backends,
                    wanted = count,
                    "Waiting for backends to appear"
                );
                backend_count
            })
            .await
            .unwrap()
            .clone()
    }

    // Test the expected, if quirky, behavior of a zero TTL DNS record.
    #[tokio::test]
    async fn test_zero_ttl() {
        setup_tracing_subscriber();

        // Start the DNS server, which runs independently of the resolver
        let dns_server_address = DnsServerBuilder::new("example.com", "test.example.com")
            .add_backend(1234, "test001.example.com.", 0)
            .run()
            .await;

        // Start the resolver, which queries the DNS server
        let service = service::Name("test.example.com".into());
        let bootstrap_servers = vec![dns_server_address];
        let config = DnsResolverConfig {
            query_interval: Duration::from_millis(10),
            hardcoded_ttl: None,
            ..Default::default()
        };
        let mut resolver = DnsResolver::new(service, bootstrap_servers, config);

        let mut monitor = resolver.monitor();

        // We have a query interval of 10ms, but a TTL of zero.
        // That means we're going to see these backends, but only very briefly,
        // and they'll rapidly be discarded.
        //
        // As a result, we expect to oscillate between zero and one backends.
        wait_for_backends(&mut monitor, 1).await;
        wait_for_backends(&mut monitor, 0).await;
        wait_for_backends(&mut monitor, 1).await;
        wait_for_backends(&mut monitor, 0).await;
        wait_for_backends(&mut monitor, 1).await;
        wait_for_backends(&mut monitor, 0).await;
    }

    // Tests that TTLs expire at an expected rate.
    #[tokio::test]
    async fn test_ttl_expiration() {
        setup_tracing_subscriber();

        // Note the TTL on these records -- we'll wait increasing amounts of
        // time to force each record to expire.
        let dns_server_address = DnsServerBuilder::new("example.com", "test.example.com")
            .add_backend(1234, "test001.example.com.", 10)
            .add_backend(5678, "test002.example.com.", 100)
            .add_backend(9012, "test003.example.com.", 1000)
            .run()
            .await;

        let service = service::Name("test.example.com".into());
        let bootstrap_servers = vec![dns_server_address];
        let config = DnsResolverConfig {
            ..Default::default()
        };
        let mut resolver = DnsResolver::new(service, bootstrap_servers, config);

        // Observe all records
        let mut monitor = resolver.monitor();
        let backends = wait_for_backends(&mut monitor, 3).await;
        assert_eq!(backends.len(), 3);
        let mut iter = backends.keys();
        assert_eq!(
            iter.next().unwrap(),
            &backend::Name::new("test001.example.com.")
        );
        assert_eq!(
            iter.next().unwrap(),
            &backend::Name::new("test002.example.com.")
        );
        assert_eq!(
            iter.next().unwrap(),
            &backend::Name::new("test003.example.com.")
        );

        // Force expiration of test001
        tokio::time::pause();
        tokio::time::advance(tokio::time::Duration::from_secs(50)).await;
        tokio::time::resume();
        let backends = wait_for_backends(&mut monitor, 2).await;
        assert_eq!(backends.len(), 2);
        let mut iter = backends.keys();
        assert_eq!(
            iter.next().unwrap(),
            &backend::Name::new("test002.example.com.")
        );
        assert_eq!(
            iter.next().unwrap(),
            &backend::Name::new("test003.example.com.")
        );

        // Force expiration of test002
        tokio::time::pause();
        tokio::time::advance(tokio::time::Duration::from_secs(500)).await;
        tokio::time::resume();
        let backends = wait_for_backends(&mut monitor, 1).await;
        assert_eq!(backends.len(), 1);
        let mut iter = backends.keys();
        assert_eq!(
            iter.next().unwrap(),
            &backend::Name::new("test003.example.com.")
        );

        // Force expiration of test003
        tokio::time::pause();
        tokio::time::advance(tokio::time::Duration::from_secs(5000)).await;
        tokio::time::resume();
        let backends = wait_for_backends(&mut monitor, 0).await;
        assert_eq!(backends.len(), 0);
    }

    // TODO: Test timeouts?
    // TODO: Test health of failing DNS server?
}
