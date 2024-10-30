//! A pool which uses a [resolver] to find a [backend], and vend out a [claim]

use crate::backend;
use crate::backend::Connection;
use crate::claim;
use crate::policy::Policy;
use crate::priority_list::PriorityList;
#[cfg(feature = "probes")]
use crate::probes;
use crate::rebalancer;
use crate::resolver;
use crate::slot;

use futures::StreamExt;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::interval;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamMap;
use tracing::{event, instrument, Level};

#[derive(Error, Debug)]
pub enum Error {
    #[error("No backends found for this service")]
    NoBackends,

    #[error("Backends exist, but none are online")]
    NoBackendsOnline,

    #[error("Backends exist, and appear online, but all claims are used")]
    AllClaimsUsed,

    #[error("Pool terminated")]
    Terminated,
}

impl Error {
    #[cfg(feature = "probes")]
    // Convert to a static string for USDT probes.
    const fn as_str(&self) -> &'static str {
        match self {
            Error::NoBackends => "NoBackends",
            Error::NoBackendsOnline => "NoBackendsOnline",
            Error::AllClaimsUsed => "AllClaimsUsed",
            Error::Terminated => "Terminated",
        }
    }
}

enum Request<Conn: Connection> {
    Claim {
        tx: oneshot::Sender<Result<claim::Handle<Conn>, Error>>,
    },
    Terminate,
}

/// A shared reference to backend stats
#[derive(Clone)]
pub struct BackendStats(Arc<Mutex<slot::Stats>>);

impl BackendStats {
    /// Samples stats from a backend at a single point-in-time
    pub fn get(&self) -> slot::Stats {
        self.0.lock().unwrap().clone()
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for BackendStats {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let inner = self.0.lock().unwrap();
        inner.serialize(serializer)
    }
}

// A claim request that could not complete immediately
struct ClaimRequest<Conn: Connection> {
    tx: oneshot::Sender<Result<claim::Handle<Conn>, Error>>,
    deadline: tokio::time::Instant,
}

struct PoolInner<Conn: Connection> {
    backend_connector: backend::SharedConnector<Conn>,

    resolver: resolver::BoxedResolver,
    slots: HashMap<backend::Name, slot::Set<Conn>>,
    priority_list: PriorityList<backend::Name>,

    request_queue: VecDeque<ClaimRequest<Conn>>,
    policy: Policy,

    // Tracks stats for each backend.
    //
    // Should be kept in lockstep with "Self::slots".
    stats_tx: watch::Sender<HashMap<backend::Name, BackendStats>>,

    rx: mpsc::Receiver<Request<Conn>>,
}

impl<Conn: Connection> PoolInner<Conn> {
    fn new(
        resolver: resolver::BoxedResolver,
        backend_connector: backend::SharedConnector<Conn>,
        policy: Policy,
        rx: mpsc::Receiver<Request<Conn>>,
        stats_tx: watch::Sender<HashMap<backend::Name, BackendStats>>,
    ) -> Self {
        Self {
            backend_connector,
            resolver,
            slots: HashMap::new(),
            priority_list: PriorityList::new(),
            request_queue: VecDeque::new(),
            policy,
            stats_tx,
            rx,
        }
    }

    // Sum up the total number of spares across all slot sets.
    fn stats_summary(&self) -> slot::Stats {
        let mut stats = slot::Stats::default();
        for slot_set in self.slots.values() {
            stats = stats + slot_set.get_stats();
        }
        stats
    }

    // Creates or destroys slots sets, depending on the event from
    // the resolver.
    //
    // Returns the newly added backends, if any.
    #[instrument(skip(self), name = "PoolInner::handle_resolve_event")]
    fn handle_resolve_event(
        &mut self,
        all_backends: resolver::AllBackends,
    ) -> Vec<(backend::Name, watch::Receiver<slot::SetState>)> {
        let mut new_backends = vec![];

        // Gather information from all backends to make sure we don't provision
        // more slots than the maximum indicated by our policy.
        let stats = self.stats_summary();
        let mut slots_left = self.policy.max_slots.saturating_sub(stats.all_slots());

        // Add all new backends first
        for (name, backend) in all_backends.iter() {
            let std::collections::hash_map::Entry::Vacant(entry) = self.slots.entry(name.clone())
            else {
                continue;
            };
            self.priority_list
                .push(rebalancer::new_backend(name.clone()));

            // If we provision zero slots: We'll provision one later during
            // rebalancing, if we can.
            //
            // If we provision one slot: Once it connects, and the backend looks
            // viable, we'll provision more slots, if we can.
            let initial_slot_count = if slots_left > 0 {
                slots_left -= 1;
                1
            } else {
                0
            };

            let set = slot::Set::new(
                self.policy.set_config.clone(),
                initial_slot_count,
                name.clone(),
                backend.clone(),
                self.backend_connector.clone(),
            );
            self.stats_tx.send_modify(|map| {
                map.insert(name.clone(), BackendStats(set.stats.clone()));
            });
            new_backends.push((name.clone(), set.monitor()));
            entry.insert(set);
        }

        let mut to_remove = vec![];
        for name in self.slots.keys() {
            if !all_backends.contains_key(name) {
                to_remove.push(name.clone());
            }
        }

        for name in &to_remove {
            self.slots.remove(name);
            self.stats_tx
                .send_if_modified(|stats| stats.remove(name).is_some());
        }
        new_backends
    }

    // Forcefully fail the next client claim request.
    fn fail_claim(&mut self) {
        let Some(request) = self.request_queue.pop_front() else {
            return;
        };
        // We would have claimed this request if we could have done
        // so earlier. Don't bother trying to access it now.
        //
        // Instead, identify "why haven't we succeeded" to help the
        // client diagnose what's happening.
        let err = match self.slots.len() {
            // We don't know of any valid backends from the resolver
            0 => Error::NoBackends,
            _ => {
                if self
                    .slots
                    .values()
                    .any(|set| matches!(set.get_state(), slot::SetState::Online { .. }))
                {
                    // Backends exist, and appear online, but we couldn't get a claim.
                    // Presumably, this means all existing claims are in-use.
                    Error::AllClaimsUsed
                } else {
                    // Backends exist, but we don't see any connections that appear alive.
                    Error::NoBackendsOnline
                }
            }
        };

        let _ = request.tx.send(Err(err));
    }

    // Waits until the next claim request timeout.
    //
    // If one does not exist, wait forever.
    async fn run(mut self) {
        let mut rebalance_interval = interval(self.policy.rebalance_interval);
        rebalance_interval.reset();

        let mut new_backends = vec![];
        let mut backend_status_stream = StreamMap::new();
        let mut resolver_stream = WatchStream::new(self.resolver.monitor());
        loop {
            // Either get the next request timeout, or wait forever.
            let next_request_timeout = async {
                match self.request_queue.front() {
                    Some(oldest_request) => tokio::time::sleep_until(oldest_request.deadline).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                // Handle requests from clients
                request = self.rx.recv() => {
                    match request {
                        Some(Request::Claim { tx }) => {
                            self.claim_or_enqueue(tx).await
                        }
                        // The caller has explicitly asked us to terminate, and
                        // we should respond to them once we've stopped doing
                        // work.
                        Some(Request::Terminate) => {
                            self.terminate().await;
                            return;
                        },
                        // The caller has abandoned their connecion to the pool.
                        //
                        // We stop handling new requests, but have no one to
                        // notify. Given that the caller no longer needs the
                        // pool, we choose to terminate to avoid leaks.
                        None => {
                            self.terminate().await;
                            return;
                        }
                    }
                }
                // Timeout old requests from clients
                _ = next_request_timeout => self.fail_claim(),
                // Handle updates from the resolver
                Some(all_backends) = resolver_stream.next() => {
                    event!(Level::INFO, "Resolver updated known backends");
                    // Update the set of backends we know about,
                    // and gather the list of all "new" backends.
                    new_backends.extend(self.handle_resolve_event(all_backends));

                    // Monitor all the new backends for changes
                    for (name, receiver) in new_backends.drain(..) {
                        backend_status_stream.insert(
                            name,
                            WatchStream::new(receiver),
                        );
                    }
                }
                // Periodically rebalance the allocation of slots to backends
                _ = rebalance_interval.tick() => {
                    event!(Level::INFO, "Rebalancing: timer tick");
                    self.rebalance().await;
                }
                // If any of the slots change state, update their allocations.
                Some((name, status)) = &mut backend_status_stream.next(), if !backend_status_stream.is_empty() => {
                    event!(Level::INFO, name = ?name, status = ?status, "Rebalancing: Backend has new status");
                    rebalance_interval.reset();
                    self.rebalance().await;

                    if matches!(status, slot::SetState::Online { has_unclaimed_slots: true }) {
                        self.try_claim_from_queue().await;
                    }
                },
            }
        }
    }

    async fn claim_or_enqueue(&mut self, tx: oneshot::Sender<Result<claim::Handle<Conn>, Error>>) {
        let result = self.claim().await;
        if result.is_ok() {
            let _ = tx.send(result);
            return;
        }
        // If we fail to claim a request...
        //
        // - Keep this request in a queue. A backend may gain slots or come
        // online later.
        // - Start the clock on a timeout.
        self.request_queue.push_back(ClaimRequest {
            tx,
            deadline: tokio::time::Instant::now() + self.policy.claim_timeout,
        });
    }

    async fn try_claim_from_queue(&mut self) {
        loop {
            let Some(request) = self.request_queue.pop_front() else {
                return;
            };

            let result = self.claim().await;
            if result.is_ok() {
                let _ = request.tx.send(result);
            } else {
                self.request_queue.push_front(request);
                return;
            }
        }
    }

    // Terminate all background tasks, including:
    // - The resolver (may or may not have background
    // tasks, this is dependent on the implementation)
    // - Each of the slot sets
    #[instrument(skip(self), name = "PoolInner::terminate")]
    async fn terminate(&mut self) {
        self.resolver.terminate().await;

        for (_backend, mut slot_set) in self.slots.drain() {
            slot_set.terminate().await;
        }
    }

    #[instrument(skip(self), name = "PoolInner::rebalance")]
    async fn rebalance(&mut self) {
        let mut questionable_backend_count = 0;
        let mut usable_backends = vec![];

        // Pass 1: Limit spares from backends that might not be functioning
        let iter = self.slots.iter_mut();
        for (name, slot_set) in iter {
            match slot_set.get_state() {
                slot::SetState::Offline => {
                    let _ = slot_set.set_wanted_count(1).await;
                    questionable_backend_count += 1;
                }
                slot::SetState::Online { .. } => {
                    usable_backends.push(name.clone());
                }
            }
        }

        if usable_backends.is_empty() {
            event!(Level::DEBUG, "No observed usable backends");
            return;
        }

        event!(Level::DEBUG, backends = ?usable_backends, "Observed usable backends");

        // Each "questionable" backend uses one slot. Among the remaining
        // backends, attempt to evenly distribute all wanted slots.
        let total_slots_wanted = std::cmp::min(
            self.stats_summary().claimed_slots + self.policy.spares_wanted,
            self.policy.max_slots,
        )
        .saturating_sub(questionable_backend_count);
        let slots_wanted_per_backend = total_slots_wanted.div_ceil(usable_backends.len());

        // Pass 2: Provision spares equitably among the functioning backends
        for name in usable_backends {
            let Some(slot_set) = self.slots.get_mut(&name) else {
                continue;
            };
            let _ = slot_set.set_wanted_count(slots_wanted_per_backend).await;
        }

        let mut new_priority_list = PriorityList::new();
        let iter = std::mem::take(&mut self.priority_list).into_iter();
        for std::cmp::Reverse(mut weighted_backend) in iter {
            // If the backend no longer exists, drop it from the priority list.
            let Some(slot) = self.slots.get(&weighted_backend.value) else {
                event!(Level::DEBUG, backend = ?weighted_backend.value, "Dropping backend");
                continue;
            };

            // Otherwise, the backend priority is set to the number of failures
            // seen. More failures => less preferable backend.
            weighted_backend.score = slot.failure_count();

            // TODO: Is this randomness actually necessary?
            rebalancer::add_random_jitter(&mut weighted_backend);

            event!(
                Level::DEBUG,
                backend = ?weighted_backend.value,
                score = ?weighted_backend.score,
                "Rebalancing backend with score (lower preferred)"
            );
            new_priority_list.push(weighted_backend);
        }
        self.priority_list = new_priority_list;
    }

    async fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        let mut attempted_backend = vec![];
        let mut result = Err(Error::NoBackends);

        loop {
            // Whenever we consider a new backend, add it to the
            // "attempted_backend" list. We want to put it back in the
            // priority list before returning, but we don't want to
            // re-consider the same backend twice for this request.
            let Some(mut weighted_backend) = self.priority_list.pop() else {
                event!(Level::DEBUG, "No backends left to consider");
                break;
            };

            // The priority list lags behind the known set of backends, so it's
            // possible we have stale entries referencing backends that have
            // been removed. If that's the case, remove them here.
            //
            // This will also happen when we periodically rebalance
            // the priority list.
            let Some(set) = self.slots.get_mut(&weighted_backend.value) else {
                event!(Level::DEBUG, "Saw backend in priority list without set");
                continue;
            };

            // Use this claim if we can, or continue looking if we can't use it.
            //
            // Either way, put this backend back in the priority list after
            // we're done with it.
            let Ok(claim) = set.claim().await else {
                event!(Level::DEBUG, "Failed to actually get claim for backend");
                rebalancer::claimed_err(&mut weighted_backend);
                attempted_backend.push(weighted_backend);
                continue;
            };
            rebalancer::claimed_ok(&mut weighted_backend);
            attempted_backend.push(weighted_backend);

            result = Ok(claim);
            break;
        }

        self.priority_list.extend(attempted_backend.into_iter());
        result
    }
}

/// Manages a set of connections to a service
pub struct Pool<Conn: Connection> {
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    tx: mpsc::Sender<Request<Conn>>,
    stats: Stats,
}

/// Pool-side stats, including statistics for each backend.
#[derive(Clone)]
pub struct Stats {
    /// Per-backend statistics
    pub rx: watch::Receiver<HashMap<backend::Name, BackendStats>>,

    /// The total number of claims made (successfully or unsuccessfully)
    /// within the pool so far.
    pub claims: Arc<AtomicUsize>,
}

impl<Conn: Connection + Send + 'static> Pool<Conn> {
    /// Creates a new connection pool.
    ///
    /// - resolver: Describes how backends should be found for the service.
    /// - backend_connector: Describes how the connections to a specific
    /// backend should be made.
    ///
    /// ```no_run
    /// use qorb::connectors::tcp::TcpConnector;
    /// use qorb::pool::Pool;
    /// use qorb::policy::Policy;
    /// use qorb::resolvers::dns::{DnsResolver, DnsResolverConfig};
    /// use qorb::service;
    /// use std::sync::Arc;
    ///
    /// # async {
    /// // Create the resolver -- here, we're using DNS
    /// let bootstrap_dns = vec![ "[::1]:53".parse().unwrap() ];
    /// let resolver = Box::new(DnsResolver::new(
    ///     service::Name("_my_service._tcp.domain.com.".to_string()),
    ///     bootstrap_dns,
    ///     DnsResolverConfig::default(),
    /// ));
    ///
    /// // Create the connector -- we're using a simple TCP connection
    /// // with no health checks.
    /// let connector = Arc::new(
    ///     TcpConnector {}
    /// );
    ///
    /// // Create the connection pool itself.
    /// let policy = Policy::default();
    /// let pool = Pool::new(resolver, connector, policy);
    ///
    /// // Grab a connection from the pool.
    /// // Note that it may take a moment for the pool to create connections
    /// // to backends, and those backends may also be offline.
    /// let connection = pool.claim().await.unwrap();
    ///
    /// # };
    /// ```
    #[instrument(skip(resolver, backend_connector), name = "Pool::new")]
    pub fn new(
        resolver: resolver::BoxedResolver,
        backend_connector: backend::SharedConnector<Conn>,
        policy: Policy,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let (stats_tx, stats_rx) = watch::channel(HashMap::default());
        let handle = tokio::task::spawn(async move {
            let worker = PoolInner::new(resolver, backend_connector, policy, rx, stats_tx);
            worker.run().await;
        });

        Self {
            handle: Mutex::new(Some(handle)),
            tx,
            stats: Stats {
                rx: stats_rx,
                claims: Arc::new(AtomicUsize::new(0)),
            },
        }
    }

    /// Terminates the connection pool
    pub async fn terminate(&self) -> Result<(), Error> {
        self.tx
            .send(Request::Terminate)
            .await
            .map_err(|_| Error::Terminated)?;
        let Some(handle) = self.handle.lock().unwrap().take() else {
            return Ok(());
        };
        handle.await.map_err(|_| Error::Terminated)
    }

    /// Returns a reference to pool-wide stats
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Acquires a handle to a connection within the connection pool.
    #[instrument(level = "debug", skip(self), err, name = "Pool::claim")]
    pub async fn claim(&self) -> Result<claim::Handle<Conn>, Error> {
        #[cfg(feature = "probes")]
        let id = usdt::UniqueId::new();
        #[cfg(feature = "probes")]
        probes::claim__start!(|| &id);
        let res = self.do_claim().await;
        #[cfg(feature = "probes")]
        match &res {
            Ok(_) => probes::claim__done!(|| &id),
            Err(e) => probes::claim__failed!(|| (&id, e.as_str())),
        }
        res
    }

    async fn do_claim(&self) -> Result<claim::Handle<Conn>, Error> {
        let (tx, rx) = oneshot::channel();

        // TODO: The work of "on_acquire" could be done here? Would prevent the
        // slot set from blocking, and we'd also be able to loop. Picking a new
        // backend might also be the right call if "on_acquire" is failing?
        //
        // There is admittedly a question of "fairness" within a queue of
        // claims; looping at this point would send us to the back of the queue
        // if there were multiple callers.

        self.tx
            .send(Request::Claim { tx })
            .await
            .map_err(|_| Error::Terminated)?;
        let claim = rx.await.map_err(|_| Error::Terminated)?;
        self.stats.claims.fetch_add(1, Ordering::Relaxed);
        claim
    }
}

impl<Conn: Connection> Drop for Pool<Conn> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::backend::{self, Backend, Connector};
    use crate::policy::{Policy, SetConfig};
    use crate::resolver::{AllBackends, Resolver};
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Duration;

    #[derive(Clone)]
    struct TestResolver {
        tx: watch::Sender<AllBackends>,
    }

    impl TestResolver {
        fn new() -> Self {
            let backends = Arc::new(BTreeMap::new());
            let (tx, _) = watch::channel(backends);
            Self { tx }
        }

        fn replace(&self, backends: BTreeMap<backend::Name, Backend>) {
            self.tx.send_replace(Arc::new(backends));
        }
    }

    impl Resolver for TestResolver {
        fn monitor(&mut self) -> watch::Receiver<AllBackends> {
            self.tx.subscribe()
        }
    }

    struct TestConnection {
        id: usize,
        backend: Backend,
    }

    impl TestConnection {
        fn new(id: usize, backend: Backend) -> Self {
            Self { id, backend }
        }
    }

    struct TestConnector {
        next_id: AtomicUsize,
    }

    impl TestConnector {
        fn new() -> Self {
            Self {
                next_id: AtomicUsize::new(1),
            }
        }
    }

    #[async_trait]
    impl Connector for TestConnector {
        type Connection = TestConnection;

        async fn connect(&self, backend: &Backend) -> Result<Self::Connection, backend::Error> {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            Ok(TestConnection::new(id, backend.clone()))
        }
    }

    // Tests that a claim can be made to a single backend.
    #[tokio::test]
    async fn test_get_claim_from_one_backend() {
        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(TestConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        let pool = Pool::new(resolver, connector, Policy::default());
        let handle = pool.claim().await.expect("Failed to get claim");

        assert_eq!(handle.id, 1);
        assert_eq!(handle.backend.address, address);
    }

    // Tests that a claim can be made before backends actually appear,
    // and they'll be enqueued / complete later.
    #[tokio::test]
    async fn test_get_claim_before_backend_appears() {
        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(TestConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let pool = Pool::new(resolver.clone(), connector, Policy::default());

        let join_handle = tokio::task::spawn(async move {
            let handle = pool.claim().await.expect("Failed to get claim");
            assert_eq!(handle.id, 1);
            assert_eq!(handle.backend.address, address);
        });

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        join_handle.await.expect("Background task failed");
    }

    // Tests that claims are enqueued when there are more claims being made
    // than slots available.
    #[tokio::test]
    async fn test_get_more_claims_than_slots() {
        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(TestConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        let pool = Pool::new(
            resolver,
            connector,
            Policy {
                spares_wanted: 5,
                max_slots: 5,
                set_config: SetConfig {
                    max_count: 5,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        // Fill all the spares with claims
        let mut handles = vec![];
        for i in 1..=5 {
            let handle = pool.claim().await.expect("Failed to get claim");
            assert_eq!(handle.id, i);
            assert_eq!(handle.backend.address, address);
            handles.push(handle);
        }

        // When we try another claim, it should not be able to complete
        let result = tokio::time::timeout(Duration::from_millis(50), pool.claim()).await;
        assert!(
            result.is_err(),
            "Unexpected non-error result (expected timeout)"
        );

        // If we make space (drop a previously-used handle, which should recycle a slot),
        // then the next claim we make should succeed, and re-use that old connection.
        handles.remove(0);

        let handle = pool
            .claim()
            .await
            .expect("Failed to get claim after space became available!");
        assert_eq!(handle.id, 1);
    }

    // Get a claim through one backend, update the resolver, and observe
    // that we connect to the new backend.
    #[tokio::test]
    async fn test_claim_after_backend_swap() {
        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(TestConnector::new());

        // This address will appear in DNS first
        let address1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // This address will appear in DNS later
        let address2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090);

        // Start with address1
        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address1),
        )]));
        let pool = Pool::new(
            resolver.clone(),
            connector,
            Policy {
                claim_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        );

        // We can access that first address
        let handle = pool.claim().await.expect("Failed to get claim");
        assert_eq!(handle.id, 1);
        assert_eq!(handle.backend.address, address1);
        drop(handle);

        resolver.replace(BTreeMap::from([(
            backend::Name::new("bbb"),
            Backend::new(address2),
        )]));

        // NOTE: We don't really have a great interface for "the moment the
        // DNS resolution update propagates to the slot sets", but it should
        // happen eventually.
        loop {
            let handle = pool.claim().await.expect("Failed to get claim");

            if handle.backend.address == address1 {
                eprintln!("Still accessing old address; waiting to shift to new backend...");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            assert_eq!(handle.backend.address, address2);
            break;
        }

        // The moment we've processed the resolver update, we should no longer
        // see any connections to the old backend.
        //
        // Confirm that we can keep pulling claims from the pool until it's all
        // used up, and they'll only point to the new backend.
        let mut handles = vec![];
        loop {
            match pool.claim().await {
                Ok(handle) => {
                    assert_eq!(handle.backend.address, address2);
                    handles.push(handle);
                }
                Err(err) => {
                    assert!(
                        matches!(err, Error::AllClaimsUsed,),
                        "Unexpected error: {err:?}"
                    );
                    break;
                }
            }
        }

        // Since we connect pretty quickly, we should have used up all our
        // slots.
        assert_eq!(handles.len(), Policy::default().max_slots);
    }

    #[tokio::test]
    async fn test_terminate() {
        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(TestConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        let pool = Pool::new(resolver, connector, Policy::default());
        let handle = pool.claim().await.expect("Failed to get claim");

        assert_eq!(handle.id, 1);
        assert_eq!(handle.backend.address, address);

        pool.terminate().await.unwrap();
        assert!(matches!(
            pool.terminate().await.unwrap_err(),
            Error::Terminated,
        ));
        assert!(matches!(
            pool.claim().await.map(|_| ()).unwrap_err(),
            Error::Terminated,
        ));
    }

    fn setup_tracing_subscriber() {
        use tracing_subscriber::fmt::format::FmtSpan;
        tracing_subscriber::fmt()
            .with_thread_names(true)
            .with_span_events(FmtSpan::ENTER)
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .init();
    }

    #[tokio::test]
    async fn test_terminate_with_slow_active_claim() {
        setup_tracing_subscriber();

        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(crate::test_utils::SlowConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        let pool = Pool::new(resolver, connector.clone(), Policy::default());
        let _handle = pool.claim().await.expect("Failed to get claim");

        // Create a large delay, which terminate() should skip.
        connector.stall();
        pool.terminate().await.unwrap();
        connector.panic_on_access();

        assert!(matches!(
            pool.terminate().await.unwrap_err(),
            Error::Terminated,
        ));
        assert!(matches!(
            pool.claim().await.map(|_| ()).unwrap_err(),
            Error::Terminated,
        ));
    }

    #[tokio::test]
    async fn test_terminate_with_slow_setup() {
        setup_tracing_subscriber();

        let resolver = Box::new(TestResolver::new());
        let connector = Arc::new(crate::test_utils::SlowConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        // Create a large delay, which terminate() should skip.
        connector.stall();

        let pool = Pool::new(resolver, connector.clone(), Policy::default());

        pool.terminate().await.unwrap();
        connector.panic_on_access();

        assert!(matches!(
            pool.terminate().await.unwrap_err(),
            Error::Terminated,
        ));
        assert!(matches!(
            pool.claim().await.map(|_| ()).unwrap_err(),
            Error::Terminated,
        ));
    }

    #[tokio::test]
    async fn test_better_errors() {
        setup_tracing_subscriber();

        let resolver = TestResolver::new();
        let connector = Arc::new(crate::test_utils::FaultyConnector::new());
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let pool = Pool::new(
            Box::new(resolver.clone()),
            connector.clone(),
            Policy {
                // NOTE: We need to set this so we can test the
                // 'all claims in-use' error case.
                max_slots: 1,
                // We will hit this timeout, so keep it relatively short.
                claim_timeout: Duration::from_millis(5),
                ..Default::default()
            },
        );

        // Failure case: No backends appear in the resolver
        let claim_err = pool.claim().await.map(|_| ()).unwrap_err();
        assert!(
            matches!(claim_err, Error::NoBackends),
            "Unexpected error: {claim_err}"
        );

        resolver.replace(BTreeMap::from([(
            backend::Name::new("aaa"),
            Backend::new(address),
        )]));

        let claim = pool.claim().await.expect("Failed to make claim");

        // Failure case: We've hit the claim capacity with a backend
        // that is online.
        let claim_err = pool.claim().await.map(|_| ()).unwrap_err();
        assert!(
            matches!(claim_err, Error::AllClaimsUsed),
            "Unexpected error: {claim_err}"
        );
        connector.start_failing();
        drop(claim);

        // Failure case: Although the backend appears in the resolver,
        // it's not online. Give a more informative error than "no slots
        // ready".
        let claim_err = pool.claim().await.map(|_| ()).unwrap_err();
        assert!(
            matches!(claim_err, Error::NoBackendsOnline),
            "Unexpected error: {claim_err}"
        );
    }
}
