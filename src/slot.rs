use crate::backend::{Backend, SharedConnector};
use crate::backoff::ExponentialBackoff;
use crate::claim;
use crate::connection::Connection;

use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{interval, Duration};
use tracing::{event, instrument, span, Level};

#[derive(Error, Debug)]
pub enum Error {
    #[error("No slots available for backend")]
    NoSlotsReady,

    #[error("Worker Terminated Unexpectedly")]
    SlotWorkerTerminated,
}

#[derive_where(Debug)]
enum State<Conn: Connection> {
    // The slot is attempting to connect.
    //
    // - (On success) State becomes ConnectedUnclaimed
    Connecting,

    // The slot has an active connection to a backend, and is ready for use.
    //
    // - (On client claim request) State becomes ConnectedClaimed
    // - (On health monitor task) State becomes ConnectedChecking
    ConnectedUnclaimed(DebugIgnore<Conn>),

    // The slot has an active connection to a backend, and is being validated.
    //
    // - (On success) State becomes ConnectedUnclaimed
    // - (On failure) State becomes Connecting
    ConnectedChecking,

    // The slot has an active connection to a backend, and is claimed.
    //
    // - (When claim dropped) State becomes ConnectedClaimed
    ConnectedClaimed,
}

impl<Conn: Connection> State<Conn> {
    fn removable(&self) -> bool {
        match self {
            State::ConnectedClaimed => false,
            _ => true,
        }
    }
}

struct SlotInner<Conn: Connection> {
    state: State<Conn>,

    // A task which may may be monitoring the slot.
    //
    // In the "Connecting" state, this task attempts to connect to the backend.
    // In the "ConnectedUnclaimed", this task monitors the connection's health.
    handle: Option<AbortHandle>,
}

impl<Conn: Connection> Drop for SlotInner<Conn> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

// A slot represents a connection to a single backend,
// which may or may not actually exist.
//
// Slots scale up and down in quantity at the request of the rebalancer.
struct Slot<Conn: Connection> {
    inner: Arc<Mutex<SlotInner<Conn>>>,
}

impl<Conn: Connection> Clone for Slot<Conn> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Conn: Connection> Slot<Conn> {
    async fn loop_until_connected(
        &self,
        config: &SetConfig,
        connector: &SharedConnector<Conn>,
        backend: &Backend,
    ) {
        let mut retry_duration = config.min_connection_backoff.add_spread(config.spread);

        loop {
            match connector.connect(&backend).await {
                Ok(conn) => {
                    let mut slot = self.inner.lock().unwrap();
                    slot.state = State::ConnectedUnclaimed(DebugIgnore(conn));
                    return;
                }
                Err(_err) => {
                    retry_duration = retry_duration.exponential_backoff();
                    tokio::time::sleep(retry_duration).await;
                }
            }
        }
    }

    async fn validate_health_if_connected(&self, connector: &SharedConnector<Conn>) {
        // Grab the connection, if and only if it's idle in
        // the pool.
        let mut conn = {
            let mut slot = self.inner.lock().unwrap();
            if !matches!(slot.state, State::ConnectedUnclaimed(_)) {
                return;
            }
            let State::ConnectedUnclaimed(DebugIgnore(conn)) =
                std::mem::replace(&mut slot.state, State::ConnectedChecking)
            else {
                return;
            };
            conn
        };

        // It's important that we don't hold the slot lock across an
        // await point. To avoid this issue, we actually take the
        // slot out of the connection pool, replacing the state of
        // "ConnectedUnclaimed" with "ConnectedChecking".
        //
        // This actually makes the connection unavailable to clients
        // while we're performing the health checks. It's important
        // that we put the state back after the check succeed!
        let result = connector.is_valid(&mut conn).await;

        let mut slot = self.inner.lock().unwrap();
        match result {
            Ok(()) => {
                slot.state = State::ConnectedUnclaimed(DebugIgnore(conn));
            }
            Err(_) => {
                slot.state = State::Connecting;
            }
        }
    }
}

// An arbitrary opaque identifier for a slot, to distinguish it from
// other slots which already exist.
type SlotId = usize;

/// A wrapper around a connection that gives the slot set enough context
/// to put it back in the correct spot when the client gives it back to us.
pub(crate) struct BorrowedConnection<Conn: Connection> {
    pub(crate) conn: Conn,
    id: SlotId,
}

impl<Conn: Connection> BorrowedConnection<Conn> {
    pub(crate) fn new(conn: Conn, id: SlotId) -> Self {
        Self { conn, id }
    }
}

#[derive(Clone)]
pub struct SetConfig {
    /// The currently-desired number of slots to use
    pub desired_count: usize,

    /// The max number of slots for the connection set
    pub max_count: usize,

    /// The minimum time before retrying connection requests
    pub min_connection_backoff: Duration,

    /// The maximum time to backoff between connection requests
    pub max_connection_backoff: Duration,

    /// When retrying a connection, add a random amount of delay between [0, spread)
    pub spread: Duration,

    /// How long to wait before checking on the health of a connection.
    ///
    /// This has no backoff - on success, we wait this same period, and
    /// on failure, we use the "connection_backoff" configs.
    ///
    /// If "None", no periodic checks are performed.
    pub health_interval: Option<Duration>,
}

impl Default for SetConfig {
    fn default() -> Self {
        Self {
            desired_count: 8,
            max_count: 16,
            min_connection_backoff: Duration::from_millis(20),
            max_connection_backoff: Duration::from_secs(30),
            spread: Duration::from_millis(20),
            health_interval: Some(Duration::from_secs(30)),
        }
    }
}

enum SetRequest<Conn: Connection> {
    Claim {
        tx: oneshot::Sender<Result<claim::Handle<Conn>, Error>>,
    },
    SetWantedCount {
        count: usize,
    },
}

struct SetWorker<Conn: Connection> {
    backend: Backend,
    config: SetConfig,

    // Interface for actually connecting to backends
    backend_connector: SharedConnector<Conn>,

    rx: mpsc::Receiver<SetRequest<Conn>>,

    // Sender and receiver for returning old handles.
    //
    // This is to guarantee a size, and to vend out permits to claim::Handles so they can be sure
    // that their connections can return to the set without error.
    slot_tx: mpsc::Sender<BorrowedConnection<Conn>>,
    slot_rx: mpsc::Receiver<BorrowedConnection<Conn>>,

    // The actual slots themselves.
    slots: BTreeMap<SlotId, Slot<Conn>>,

    next_slot_id: SlotId,
}

impl<Conn: Connection> SetWorker<Conn> {
    fn new(
        rx: mpsc::Receiver<SetRequest<Conn>>,
        config: SetConfig,
        backend: Backend,
        backend_connector: SharedConnector<Conn>,
    ) -> Self {
        let (slot_tx, slot_rx) = mpsc::channel(config.max_count);
        let mut set = Self {
            backend,
            config,
            backend_connector,
            rx,
            slot_tx,
            slot_rx,
            slots: BTreeMap::new(),
            next_slot_id: 0,
        };
        set.set_wanted_count(set.config.desired_count);
        set
    }

    // Creates a new Slot, which always starts as "Connecting", and spawn a task
    // to actually connect to the backend and monitor slot health.
    fn create_slot(&mut self, slot_id: SlotId) {
        let slot = Slot {
            inner: Arc::new(Mutex::new(SlotInner {
                state: State::Connecting,
                handle: None,
            })),
        };
        let slot = self.slots.entry(slot_id).or_insert(slot).clone();

        slot.inner.lock().unwrap().handle = Some(
            tokio::task::spawn({
                let slot = slot.clone();
                let config = self.config.clone();
                let connector = self.backend_connector.clone();
                let backend = self.backend.clone();
                async move {
                    let mut monitor_interval =
                        interval(config.health_interval.unwrap_or(Duration::MAX));

                    loop {
                        event!(Level::TRACE, slot_id = slot_id, "Starting Slot work loop");
                        enum Work {
                            DoConnect,
                            DoMonitor,
                        }

                        // We're deciding what work to do, based on the state,
                        // within an isolated scope. This is due to:
                        // https://github.com/rust-lang/rust/issues/69663
                        //
                        // Even if we drop the MutexGuard before `.await` points,
                        // rustc still sees something "non-Send" held across an
                        // `.await`.
                        let work = {
                            let slot_inner = slot.inner.lock().unwrap();
                            match &slot_inner.state {
                                State::Connecting => Work::DoConnect,
                                State::ConnectedUnclaimed(_)
                                | State::ConnectedChecking
                                | State::ConnectedClaimed => Work::DoMonitor,
                            }
                        };

                        match work {
                            Work::DoConnect => {
                                span!(Level::TRACE, "Slot worker connecting", slot_id);
                                slot.loop_until_connected(&config, &connector, &backend)
                                    .await;
                                monitor_interval.reset();
                            }
                            Work::DoMonitor => {
                                // TODO: What if this task is super long?
                                // TODO: Do we need a way to "kick back to
                                // connecting" if a client connects, and sees an
                                // issue on setup/teardown?
                                span!(Level::TRACE, "Slot worker monitoring", slot_id);
                                monitor_interval.tick().await;
                                slot.validate_health_if_connected(&connector).await;
                                monitor_interval.reset();
                            }
                        }
                    }
                }
            })
            .abort_handle(),
        );
    }

    // Borrows a connection out of the first unclaimed slot.
    //
    // Returns a Handle which has enough context to put the claim back,
    // once it's dropped by the client.
    fn take_connected_unclaimed_slot(
        &mut self,
        permit: mpsc::OwnedPermit<BorrowedConnection<Conn>>,
    ) -> Option<claim::Handle<Conn>> {
        for (id, slot) in &mut self.slots {
            let mut slot = slot.inner.lock().unwrap();
            if matches!(slot.state, State::ConnectedUnclaimed(_)) {
                // We intentionally "take the connection out" of the slot and
                // "place it into a claim::Handle" in the same method.
                //
                // This makes it difficult to leak a connection, unless the drop
                // method of the claim::Handle is skipped.
                let State::ConnectedUnclaimed(DebugIgnore(conn)) =
                    std::mem::replace(&mut slot.state, State::ConnectedClaimed)
                else {
                    panic!(
                        "We just matched this type before replacing it; this should be impossible"
                    );
                };

                let borrowed_conn = BorrowedConnection::new(conn, *id);

                // The "drop" method of the claim::Handle will return it to
                // the slot set, through the permit (which is connected to
                // slot_rx).
                return Some(claim::Handle::new(borrowed_conn, permit));
            }
        }
        None
    }

    // Takes back borrowed slots from clients who dropped their claim handles.
    #[instrument(level = "trace", skip(self, borrowed_conn))]
    fn recycle_connection(&mut self, borrowed_conn: BorrowedConnection<Conn>) {
        let slot_id = borrowed_conn.id;
        let slot = self
            .slots
            .get_mut(&slot_id)
            .expect(
                "A borrowed connection was returned to this\
                pool, and it should reference a slot that \
                cannot be removed while borrowed",
            )
            .inner
            .clone();
        {
            let mut slot = slot.lock().unwrap();
            assert!(
                matches!(slot.state, State::ConnectedClaimed),
                "Unexpected slot state {:?}",
                slot.state
            );
            slot.state = State::ConnectedUnclaimed(DebugIgnore(borrowed_conn.conn));
        }

        // If we tried to shrink the slot count while too many connections were
        // in-use, it's possible there's more work to do. Try to conform the
        // slot count after recycling each connection.
        self.conform_slot_count();
    }

    fn set_wanted_count(&mut self, count: usize) {
        self.config.desired_count = std::cmp::min(count, self.config.max_count);
        self.conform_slot_count();
    }

    // Makes the number of slots as close to "desired_count" as we can get.
    fn conform_slot_count(&mut self) {
        let desired = self.config.desired_count;

        if desired < self.slots.len() {
            // Fewer slots wanted. Remove as many as we can.
            let count_to_remove = self.slots.len() - desired;
            let mut to_remove = Vec::with_capacity(count_to_remove);

            // Gather all the keys we are trying to remove.
            //
            // If there are many non-removable slots, it's possible
            // we don't immediately quiesce to this smaller requested count.
            for (key, slot) in &self.slots {
                if to_remove.len() >= count_to_remove {
                    break;
                }
                let slot = slot.inner.lock().unwrap();
                if slot.state.removable() {
                    to_remove.push(*key);
                }
            }

            for key in to_remove {
                self.slots.remove(&key);
            }
        } else if desired > self.slots.len() {
            // More slots wanted. This case is easy, we can always fill
            // in "connecting" slots immediately.
            let new_slots = desired - self.slots.len();
            for slot_id in self.next_slot_id..self.next_slot_id + new_slots {
                self.create_slot(slot_id);
            }
            self.next_slot_id += new_slots;
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        // Before we vend out the slot's connection to a client, make sure that
        // we have space to take it back once they're done with it.
        let Ok(permit) = self.slot_tx.clone().try_reserve_owned() else {
            // This is more of an "all slots in-use" error,
            // but it should look the same to clients.
            return Err(Error::NoSlotsReady);
        };

        let Some(handle) = self.take_connected_unclaimed_slot(permit) else {
            return Err(Error::NoSlotsReady);
        };

        Ok(handle)
    }

    #[instrument(level = "trace", skip(self), name = "SetWorker::run")]
    async fn run(&mut self) {
        loop {
            tokio::select! {
                request = self.slot_rx.recv() => {
                    match request {
                        Some(borrowed_conn) => self.recycle_connection(borrowed_conn),
                        None => {
                            panic!("This should never happen, we hold onto a copy of the sender");
                        },
                    }
                },
                request = self.rx.recv() => {
                    match request {
                        Some(SetRequest::Claim { tx }) => {
                            let result = self.claim();
                            let _ = tx.send(result);
                        },
                        Some(SetRequest::SetWantedCount { count }) => {
                            self.set_wanted_count(count);
                        },
                        None => {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// A set of slots for a particular backend.
pub(crate) struct Set<Conn: Connection> {
    tx: mpsc::Sender<SetRequest<Conn>>,

    handle: JoinHandle<()>,
}

impl<Conn: Connection> Set<Conn> {
    pub(crate) fn new(
        config: SetConfig,
        backend: Backend,
        backend_connector: SharedConnector<Conn>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::task::spawn(async move {
            let mut worker = SetWorker::new(rx, config, backend, backend_connector);
            worker.run().await;
        });

        Self { tx, handle }
    }

    #[instrument(skip(self), name = "Set::claim")]
    pub(crate) async fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(SetRequest::Claim { tx })
            .await
            .map_err(|_| Error::SlotWorkerTerminated)?;

        rx.await.map_err(|_| Error::SlotWorkerTerminated)?
    }

    #[instrument(skip(self), name = "Set::set_wanted_count")]
    pub(crate) async fn set_wanted_count(&mut self, count: usize) -> Result<(), Error> {
        self.tx
            .send(SetRequest::SetWantedCount { count })
            .await
            .map_err(|_| Error::SlotWorkerTerminated)?;
        Ok(())
    }
}

impl<Conn: Connection> Drop for Set<Conn> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend;
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    struct TestConnection {}

    impl crate::connection::Connection for TestConnection {}

    struct TestConnector {}

    #[async_trait::async_trait]
    impl backend::Connector for TestConnector {
        type Connection = TestConnection;

        /// Creates a connection to a backend.
        async fn connect(&self, _backend: &Backend) -> Result<Self::Connection, backend::Error> {
            Ok(TestConnection {})
        }

        /// Determines if the connection to a backend is still valid.
        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), backend::Error> {
            Ok(())
        }
    }

    const BACKEND: SocketAddr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0));

    fn setup_tracing() {
        tracing_subscriber::fmt()
            .with_thread_names(true)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ENTER)
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .init();
    }

    #[tokio::test]
    async fn test_one_claim() {
        setup_tracing();
        let mut set = Set::new(
            SetConfig::default(),
            backend::Backend { address: BACKEND },
            Arc::new(TestConnector {}),
        );

        // TODO: Would be need to have a better way to inspect the slot set
        tokio::time::sleep(Duration::from_secs(1)).await;

        let _conn = set.claim().await.unwrap();
    }

    #[tokio::test]
    async fn test_all_claims() {
        setup_tracing();
        let mut set = Set::new(
            SetConfig {
                desired_count: 3,
                max_count: 10,
                ..Default::default()
            },
            backend::Backend { address: BACKEND },
            Arc::new(TestConnector {}),
        );

        // TODO: Would be need to have a better way to inspect the slot set
        tokio::time::sleep(Duration::from_secs(1)).await;

        let _conn1 = set.claim().await.unwrap();
        let _conn2 = set.claim().await.unwrap();
        let conn3 = set.claim().await.unwrap();

        set.claim().await.map(|_| ()).unwrap_err();

        drop(conn3);

        // TODO: Would be need to have a better way to inspect the slot set
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _conn4 = set.claim().await.unwrap();
    }
}
