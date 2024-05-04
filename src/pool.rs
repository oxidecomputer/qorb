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
    Resolve(#[from] resolver::ResolveError),

    #[error("Pool terminated")]
    Terminated,
}

enum Request<Conn: Connection> {
    Claim {
        tx: oneshot::Sender<Result<claim::Handle<Conn>, Error>>,
    },
}

struct PoolInner<Conn: Connection> {
    resolver: resolver::BoxedResolver,
    backend_connector: backend::SharedConnector<Conn>,
    slots: HashMap<backend::Backend, slot::Set<Conn>>,
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

    fn handle_resolve_event(
        &mut self,
        event: resolver::ResolveEvent,
    ) {
        use resolver::ResolveEvent::*;
        match event {
            BackendAdded { backend } => {
                let slot_set = slot::Set::new(
                    slot::SetParameters {
                        backend: backend.clone(),
                        desired_count: 16,
                        max_count: 16,
                    },
                    self.backend_connector.clone(),
                );
                self.slots.insert(backend, slot_set);
            },
            BackendRemoved { name } => {
                todo!();
            },
            StateChange { state } => {
                todo!();
            },
        }
    }

    async fn run(mut self) {
        let mut resolver_monitor = self.resolver.monitor();
        loop {
            tokio::select! {
                request = self.rx.recv() => {
                    match request {
                        Some(Request::Claim { tx }) => {
                            let result = self.claim().await;
                            let _ = tx.send(result);
                        },
                        None => return,
                    }
                }
                resolve_event = resolver_monitor.recv() => {
                    match resolve_event {
                        Ok(event) => self.handle_resolve_event(event),
                        Err(_) => {
                            todo!("Handle this case of resolver funk");
                        }
                    }
                }
            }
        }
    }

    pub async fn claim(&self) -> Result<claim::Handle<Conn>, Error> {
        // TODO: We need a smarter policy to pick the backend.
        //
        // This is where the priority list could come into play.
        //
        // NOTE: For now, we're trying the first one?
        let Some(set) = self.slots.values().next() else {
            return Err(Error::NoBackends);
        };

        // TODO: Do we actually want the *pool* to await a response from
        // the slot set? We could forward the request on behalf of the client,
        // or we could try sending requests to many backends at once.
        let claim = set.claim().await?;
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

        Self {
            handle,
            tx,
        }
    }

    pub async fn claim(&self) -> Result<claim::Handle<Conn>, Error> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(Request::Claim { tx }).await.map_err(|_| Error::Terminated)?;
        rx.await.map_err(|_| Error::Terminated)?
    }
}
