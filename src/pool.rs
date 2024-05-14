//! A pool which uses a [resolver] to find a [backend], and vend out a [claim]

use crate::backend;
use crate::claim;
use crate::connection::Connection;
use crate::policy::Policy;
use crate::priority_list::PriorityList;
use crate::rebalancer;
use crate::resolver;
use crate::slot;

use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use tracing::{instrument, span, Level};

#[derive(Error, Debug)]
pub enum Error {
    #[error("No backends found for this service")]
    NoBackends,

    #[error(transparent)]
    Slot(#[from] slot::Error),

    #[error("Cannot resolve backend name for service")]
    Resolve(#[from] resolver::Error),

    #[error("Pool terminated")]
    Terminated,
}

enum Request<Conn: Connection> {
    Claim {
        tx: oneshot::Sender<Result<claim::Handle<Conn>, Error>>,
    },
}

struct PoolInner<Conn: Connection> {
    backend_connector: backend::SharedConnector<Conn>,

    resolver: resolver::BoxedResolver,
    slots: HashMap<backend::Name, slot::Set<Conn>>,
    priority_list: PriorityList<backend::Name>,

    policy: Policy,

    rx: mpsc::Receiver<Request<Conn>>,
}

impl<Conn: Connection> PoolInner<Conn> {
    fn new(
        resolver: resolver::BoxedResolver,
        backend_connector: backend::SharedConnector<Conn>,
        policy: Policy,
        rx: mpsc::Receiver<Request<Conn>>,
    ) -> Self {
        Self {
            backend_connector,
            resolver,
            slots: HashMap::new(),
            priority_list: PriorityList::new(),
            policy,
            rx,
        }
    }

    // Sum up the total number of spares across all slot sets.
    fn stats_summary(&self) -> crate::slot::Stats {
        let mut stats = crate::slot::Stats::default();
        for slot_set in self.slots.values() {
            stats = stats + slot_set.get_stats();
        }
        stats
    }

    fn handle_resolve_event(&mut self, event: resolver::Event) {
        use resolver::Event::*;
        match event {
            Added(backends) => {
                let _span = span!(Level::TRACE, "Adding slots for backends").entered();
                if backends.is_empty() {
                    return;
                }

                // Gather information from all backends
                let stats = self.stats_summary();
                let total_spares = stats.spares();
                let total_slots = stats.all_slots();

                // We attempt to spread all remaining spares across the backends
                // we see. This assumes that it's desirable to provision spares
                // to backends as soon as they're seen from the resolver.
                //
                // This has the side effect that late-arriving backends won't
                // have space for spares. This is okay -- we'll wait to use them
                // until the next time rebalancing occurs.
                let mut new_spares = if self.policy.spares_wanted > total_spares {
                    self.policy.spares_wanted - total_spares
                } else {
                    0
                };

                // Enforce slot maximums
                if total_slots + new_spares > self.policy.max_slots {
                    new_spares = self.policy.max_slots - total_slots;
                }

                let new_spares_per_backend = new_spares.div_ceil(backends.len());
                for (name, backend) in backends {
                    let _slot_set = self.slots.entry(name.clone()).or_insert_with(|| {
                        self.priority_list.push(rebalancer::new_backend(name));

                        let spare_count = std::cmp::min(new_spares_per_backend, new_spares);
                        new_spares -= spare_count;

                        slot::Set::new(
                            self.policy.set_config.clone(),
                            spare_count,
                            backend.clone(),
                            self.backend_connector.clone(),
                        )
                    });
                }
            }
            Removed(backend_names) => {
                let _span = span!(Level::TRACE, "Removing slots for backends").entered();
                for name in backend_names {
                    self.slots.remove(&name);
                }
            }
        }
    }

    async fn run(mut self) {
        let mut rebalance_interval = interval(self.policy.rebalance_interval);
        rebalance_interval.reset();
        loop {
            tokio::select! {
                // Handle requests from clients
                request = self.rx.recv() => {
                    match request {
                        Some(Request::Claim { tx }) => {
                            let result = self.claim().await;
                            let _ = tx.send(result);
                        },
                        None => return,
                    }
                }
                // Handle updates from the resolver
                //
                // TODO: Do we want this to just happen in a bg task?
                events = self.resolver.step() => {
                    for event in events {
                        self.handle_resolve_event(event);
                    }
                }
                _ = rebalance_interval.tick() => {
                    self.rebalance();
                },
            }
        }
    }

    fn rebalance(&mut self) {
        let stats = self.stats_summary();

        let mut iter = std::mem::take(&mut self.priority_list).into_iter();
        let mut new_priority_list = PriorityList::new();
        while let Some(std::cmp::Reverse(mut weighted_backend)) = iter.next() {
            // If the backend no longer exists, drop it from the priority list.
            let Some(slot) = self.slots.get(&weighted_backend.value) else {
                continue;
            };

            // Otherwise, the backend priority is set to the number of failures
            // seen. More failures => less preferable backend.
            weighted_backend.score = slot.failure_count();

            // TODO: Is this randomness actually necessary?
            rebalancer::add_random_jitter(&mut weighted_backend);
            new_priority_list.push(weighted_backend);
        }
        self.priority_list = new_priority_list;
    }

    #[instrument(skip(self), err, name = "PoolInner::claim")]
    async fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        let mut attempted_backend = vec![];
        let mut result = Err(Error::NoBackends);

        loop {
            // Whenever we consider a new backend, add it to the
            // "attempted_backend" list. We want to put it back in the
            // priority list before returning, but we don't want to
            // re-consider the same backend twice for this request.
            let Some(mut weighted_backend) = self.priority_list.pop() else {
                break;
            };

            // The priority list lags behind the known set of backends, so it's
            // possible we have stale entries referencing backends that have
            // been removed. If that's the case, remove them here.
            //
            // This will also happen when we periodically rebalance
            // the priority list.
            let Some(set) = self.slots.get_mut(&weighted_backend.value) else {
                continue;
            };

            // Use this claim if we can, or continue looking if we can't use it.
            //
            // Either way, put this backend back in the priority list after
            // we're done with it.
            let Ok(claim) = set.claim().await else {
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
    handle: tokio::task::JoinHandle<()>,
    tx: mpsc::Sender<Request<Conn>>,
}

impl<Conn: Connection + Send + 'static> Pool<Conn> {
    /// Creates a new connection pool.
    ///
    /// - resolver: Describes how backends should be found for the service.
    /// - backend_connector: Describes how the connections to a specific
    /// backend should be made.
    #[instrument(skip(resolver, backend_connector), name = "Pool::new")]
    pub fn new(
        resolver: resolver::BoxedResolver,
        backend_connector: backend::SharedConnector<Conn>,
        policy: Policy,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);

        let handle = tokio::task::spawn(async move {
            let worker = PoolInner::new(resolver, backend_connector, policy, rx);
            worker.run().await;
        });

        Self { handle, tx }
    }

    /// Acquires a handle to a connection within the connection pool.
    #[instrument(level = "debug", skip(self), err, name = "Pool::claim")]
    pub async fn claim(&self) -> Result<claim::Handle<Conn>, Error> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Request::Claim { tx })
            .await
            .map_err(|_| Error::Terminated)?;
        rx.await.map_err(|_| Error::Terminated)?
    }
}

impl<Conn: Connection> Drop for Pool<Conn> {
    fn drop(&mut self) {
        self.handle.abort()
    }
}
