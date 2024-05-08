use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::backend;
use crate::claim;
use crate::connection::Connection;
use crate::policy::Policy;
use crate::resolver;
use crate::slot;

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
            resolver,
            backend_connector,
            slots: HashMap::new(),
            policy,
            rx,
        }
    }

    fn handle_resolve_event(&mut self, event: resolver::Event) {
        use resolver::Event::*;
        match event {
            Added(backends) => {
                // Make sure that we have a slot set for each of the backends.
                for (name, backend) in backends {
                    let _slot_set = self.slots.entry(name.clone()).or_insert_with(|| {
                        // TODO: Respect TTL?
                        slot::Set::new(
                            slot::SetParameters {
                                backend: backend.clone(),
                                desired_count: 16,
                                max_count: 16,
                            },
                            self.backend_connector.clone(),
                        )
                    });
                }
            }
            Removed(backend_names) => {
                for name in backend_names {
                    self.slots.remove(&name);
                }
            }
        }
    }

    async fn run(mut self) {
        loop {
            // TODO: Wrap all the work in "Box::pin"?
            //
            // This didn't work last time you tried because they each held
            // "&mut" references to "self", which conflicted with you also
            // mutating "self" below (e.g., in "claim()",
            // "handle_resolve_event()").

            let slot_work =
                futures::future::join_all(self.slots.values_mut().map(|set| set.step()));

            tokio::select! {
                // Handle requests from clients
                request = self.rx.recv() => {
                    match request {
                        Some(Request::Claim { tx }) => {
                            let result = self.claim();
                            let _ = tx.send(result);
                        },
                        None => return,
                    }
                }
                // Handle updates from the resolver
                events = self.resolver.step() => {
                    for event in events {
                        self.handle_resolve_event(event);
                    }
                }
                // Handle updates from the slot sets
                _ = slot_work => {}
            }
        }
    }

    pub fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        // TODO: We need a smarter policy to pick the backend.
        //
        // This is where the priority list could come into play.
        //
        // NOTE: For now, we're trying the first one?
        let Some(set) = self.slots.values_mut().next() else {
            return Err(Error::NoBackends);
        };

        let claim = set.claim()?;
        Ok(claim)
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

    pub async fn claim(&self) -> Result<claim::Handle<Conn>, Error> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Request::Claim { tx })
            .await
            .map_err(|_| Error::Terminated)?;
        rx.await.map_err(|_| Error::Terminated)?
    }
}
