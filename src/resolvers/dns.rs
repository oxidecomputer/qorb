//! Implementation of [Resolver] for DNS

use crate::backend;
use crate::resolver::{AllBackends, Resolver};
use crate::service;

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

    // TODO: We definitely need a more elaborate representation of health here,
    // beyond "the number of missed requests in a row we've had".
    //
    // This number will go up when the DNS client cannot access the server, but
    // this is but one of many signals.
    //
    // TODO: Maybe use the failure window here too?
    missed_requests_count: usize,
}

impl Client {
    fn new(address: SocketAddr, hardcoded_ttl: Option<Duration>) -> Self {
        let mut rc = ResolverConfig::new();
        rc.add_name_server(NameServerConfig::new(address, Protocol::Udp));
        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false;
        let resolver = TokioAsyncResolver::tokio(rc, opts);
        Self {
            resolver,
            hardcoded_ttl,
            missed_requests_count: 0,
        }
    }

    fn mark_ok(&mut self) {
        self.missed_requests_count = 0;
    }

    fn mark_error(&mut self) {
        self.missed_requests_count += 1;
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
    dns_servers: Vec<Client>,

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
        Self {
            watch_tx,
            service,
            dns_servers: bootstrap_servers
                .into_iter()
                .map(|address| Client::new(address, config.hardcoded_ttl))
                .collect(),
            backends: HashMap::new(),
            config,
        }
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
                        self.watch_tx.send_modify(|mut backends| {
                            let backends = Arc::make_mut(&mut backends);
                            backends.remove(&backend_name);
                        });
                    }
                },

                // TODO: There's more work we need to do here, under the realm of
                // "Dynamic DNS":
                //
                // - Query DNS for the set over servers we should be using
                // - Monitor the TTLs of our own DNS Servers
            }
        }
    }

    // Queries DNS servers and updates our set of backends
    async fn query_dns(&mut self) {
        // Periodically query the backends from all our DNS servers
        let mut dns_lookup = FuturesUnordered::new();
        dns_lookup.extend(self.dns_servers.iter_mut().map(|client| {
            let service = &self.service;
            let duration = self.config.query_timeout;
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
                            client.mark_ok();
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
        let Some(backends) = first_result.lock().unwrap().take() else {
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
        self.watch_tx.send_modify(|mut backends| {
            let backends = Arc::make_mut(&mut backends);
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

pub struct DnsResolver {
    handle: tokio::task::JoinHandle<()>,
    watch_rx: watch::Receiver<AllBackends>,
}

impl DnsResolver {
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
            max_dns_concurrency: 5,
            query_interval: DEFAULT_QUERY_INTERVAL,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
            hardcoded_ttl: None,
        }
    }
}
