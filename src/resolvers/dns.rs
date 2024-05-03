use crate::backend::Backend;
use crate::resolver::{ResolveEvent, Resolver, ResolverState};
use crate::service;
use std::net::SocketAddr;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task;

enum State {
    Running,
}

enum Request {
    GetBackends { tx: oneshot::Sender<Vec<Backend>> },
    GetState { tx: oneshot::Sender<State> },
}

struct Worker {
    service: service::Name,

    rx: mpsc::Receiver<Request>,

    // TODO: We probably want to track e.g. health of these servers.
    //
    //
    // TODO: We also want to track the servers that have been uncovered
    // dynamically.
    bootstrap_servers: Vec<SocketAddr>,

    max_dns_concurrency: usize,
}

impl Worker {
    fn new(
        service: service::Name,
        bootstrap_servers: Vec<SocketAddr>,
        max_dns_concurrency: usize,
    ) -> (Self, mpsc::Sender<Request>) {
        let (tx, rx) = mpsc::channel(32);
        (
            Self {
                service,
                rx,
                bootstrap_servers,
                max_dns_concurrency,
            },
            tx,
        )
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {

                // TODO: In addition to handling requests, we also need to
                // periodically send queries to our DNS servers to find
                // backends!

                request = self.rx.recv() => {
                    use Request::*;
                    match request {
                        Some(GetBackends { tx }) => {
                            let _ = tx.send(vec![]);
                        }
                        Some(GetState { tx } ) => {
                            let _ = tx.send(State::Running);
                        }
                        None => {
                            return;
                        },
                    }
                }
            }
        }
    }
}

pub struct DnsResolver {
    handle: tokio::task::JoinHandle<()>,
    tx: mpsc::Sender<Request>,
}

impl DnsResolver {
    pub fn new(
        service: service::Name,
        bootstrap_servers: Vec<SocketAddr>,
        max_dns_concurrency: usize,
    ) -> Self {
        let (mut worker, tx) = Worker::new(service, bootstrap_servers, max_dns_concurrency);
        let handle = task::spawn(async move {
            worker.run().await;
        });
        Self { handle, tx }
    }
}

// TODO: Implement these by asking the background task for the latest info?
// TODO: Monitor should be an emission of events from the bg task
// TODO: this impl needs to be async?

impl Resolver for DnsResolver {
    fn monitor(&self) -> broadcast::Receiver<ResolveEvent> {
        todo!();
    }

    fn backends(&self) -> Vec<Backend> {
        todo!();
    }

    fn state(&self) -> ResolverState {
        todo!();
    }
}
