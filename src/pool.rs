//! A pool which uses a [resolver] to find a [backend], and vend out a [claim]

use crate::backend;
use crate::backend::Connection;
use crate::claim;
use crate::policy::Policy;
use crate::priority_list::PriorityList;
use crate::rebalancer;
use crate::resolver;
use crate::slot;

use futures::StreamExt;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::interval;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamMap;
use tracing::{event, instrument, span, Level};

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
    #[instrument(skip(self))]
    fn handle_resolve_event(
        &mut self,
        event: resolver::Event,
    ) -> Vec<(backend::Name, watch::Receiver<slot::SetState>)> {
        use resolver::Event::*;

        let mut new_backends = vec![];
        match event {
            Added(backends) => {
                let _span = span!(Level::TRACE, "Adding slots for backends").entered();
                if backends.is_empty() {
                    return vec![];
                }

                // Gather information from all backends to make sure we don't provision
                // more slots than the maximum indicated by our policy.
                let stats = self.stats_summary();
                let mut slots_left = self.policy.max_slots.saturating_sub(stats.all_slots());

                for (name, backend) in backends {
                    let _slot_set = self.slots.entry(name.clone()).or_insert_with(|| {
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
                            backend.clone(),
                            self.backend_connector.clone(),
                        );
                        new_backends.push((name, set.monitor()));
                        set
                    });
                }
            }
            Removed(backend_names) => {
                let _span = span!(Level::TRACE, "Removing slots for backends").entered();
                for name in backend_names {
                    self.slots.remove(&name);
                }
            }
        };

        new_backends
    }

    async fn run(mut self) {
        let mut rebalance_interval = interval(self.policy.rebalance_interval);
        rebalance_interval.reset();

        let mut new_backends = vec![];
        let mut backend_status_stream = StreamMap::new();
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
                // TODO: Do we want this to just happen in a bg task?
                events = self.resolver.step() => {
                    // Update the set of backends we know about,
                    // and gather the list of all "new" backends.
                    for event in events {
                        new_backends.extend(self.handle_resolve_event(event));
                    }

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
                    self.rebalance().await;
                }
                // If any of the slots change state, update their allocations.
                Some((name, _status)) = &mut backend_status_stream.next(), if !backend_status_stream.is_empty() => {
                    event!(Level::DEBUG, name = ?name, "Backend just came online");
                    rebalance_interval.reset();
                    self.rebalance().await;
                },
            }
        }
    }

    #[instrument(skip(self))]
    async fn rebalance(&mut self) {
        let mut questionable_backend_count = 0;
        let mut usable_backends = vec![];

        // Pass 1: Limit spares from backends that might not be functioning
        let mut iter = self.slots.iter_mut();
        while let Some((name, slot_set)) = iter.next() {
            match slot_set.get_state() {
                slot::SetState::Offline => {
                    let _ = slot_set.set_wanted_count(1).await;
                    questionable_backend_count += 1;
                }
                slot::SetState::Online => {
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
