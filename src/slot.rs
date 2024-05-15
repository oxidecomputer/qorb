use crate::backend::Connection;
use crate::backend::{Backend, SharedConnector};
use crate::backoff::ExponentialBackoff;
use crate::claim;
use crate::policy::SetConfig;
use crate::window_counter::WindowedCounter;

use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{interval, Duration};
use tracing::{event, instrument, span, Instrument, Level};

#[derive(Error, Debug)]
pub enum Error {
    #[error("No slots available for backend")]
    NoSlotsReady,

    #[error("Worker Terminated Unexpectedly")]
    SlotWorkerTerminated,
}

// The state of an individual slot within a slot set.
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

    // Final state, when the slot is no longer in-use.
    Terminated,
}

impl<Conn: Connection> State<Conn> {
    fn removable(&self) -> bool {
        !matches!(self, State::ConnectedClaimed)
    }

    fn connected(&self) -> bool {
        match self {
            State::Connecting | State::Terminated => false,
            State::ConnectedUnclaimed(_) | State::ConnectedChecking | State::ConnectedClaimed => {
                true
            }
        }
    }
}

struct SlotInner<Conn: Connection> {
    // The state of this individual connection.
    state: State<Conn>,

    // This is wrapped in an "Arc" because it's shared with all slots in the slot set.
    stats: Arc<Mutex<Stats>>,

    status_tx: watch::Sender<SetState>,

    // A task which may may be monitoring the slot.
    //
    // In the "Connecting" state, this task attempts to connect to the backend.
    // In the "ConnectedUnclaimed", this task monitors the connection's health.
    handle: Option<AbortHandle>,
}

impl<Conn: Connection> SlotInner<Conn> {
    // Transitions to a new state, returning the old state.
    //
    // Additionally, if this updates the state of the whole backend,
    // emits a SetState on [Self::status_tx].
    fn state_transition(&mut self, new: State<Conn>) -> State<Conn> {
        if matches!(self.state, State::Terminated) {
            return State::Terminated;
        }

        let old = std::mem::replace(&mut self.state, new);

        let mut stats = self.stats.lock().unwrap();
        let before_no_connected = stats.has_no_connected_slots();
        stats.exit_state(&old);
        stats.enter_state(&self.state);
        let after_no_connected = stats.has_no_connected_slots();

        // "Not connected" may mean:
        // - We're still initializing the backend, or
        // - All connections to this backend have failed
        //
        // Either way, transitioning into or out of this state is
        // an important signal to the pool, which may want to tune
        // the number of slots provisioned to this set.
        match (before_no_connected, after_no_connected) {
            (true, false) => {
                event!(Level::INFO, "state_transition: Set Online");
                self.status_tx.send_replace(SetState::Online);
            }
            (false, true) => {
                event!(Level::INFO, "state_transition: Set Offline");
                self.status_tx.send_replace(SetState::Offline);
            }
            _ => (),
        };

        old
    }
}

impl<Conn: Connection> Drop for SlotInner<Conn> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// A slot represents a connection to a single backend,
// which may or may not actually exist.
//
// Slots scale up and down in quantity at the request of the rebalancer.
struct Slot<Conn: Connection> {
    // This "failure_window" is shared with all slots in the slot set.
    failure_window: Arc<WindowedCounter>,
    inner: Arc<Mutex<SlotInner<Conn>>>,
}

impl<Conn: Connection> Clone for Slot<Conn> {
    fn clone(&self) -> Self {
        Self {
            failure_window: self.failure_window.clone(),
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
            match connector.connect(backend).await {
                Ok(conn) => {
                    let mut slot = self.inner.lock().unwrap();
                    slot.state_transition(State::ConnectedUnclaimed(DebugIgnore(conn)));
                    return;
                }
                Err(err) => {
                    event!(Level::WARN, err = ?err, backend = ?backend, "Failed to connect");
                    self.failure_window.add(1);
                    retry_duration =
                        retry_duration.exponential_backoff(config.max_connection_backoff);
                    tokio::time::sleep(retry_duration).await;
                }
            }
        }
    }

    #[instrument(level = "trace", skip(self, connector))]
    async fn validate_health_if_connected(
        &self,
        connector: &SharedConnector<Conn>,
        timeout: Duration,
    ) {
        // Grab the connection, if and only if it's idle in the pool.
        let mut conn = {
            let mut slot = self.inner.lock().unwrap();
            if !matches!(slot.state, State::ConnectedUnclaimed(_)) {
                return;
            }
            let State::ConnectedUnclaimed(DebugIgnore(conn)) =
                slot.state_transition(State::ConnectedChecking)
            else {
                panic!("We just verified that the state was 'ConnectedUnclaimed'");
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
        // that we put the state back after the check succeeds!
        let result = tokio::time::timeout(timeout, connector.is_valid(&mut conn)).await;

        let mut slot = self.inner.lock().unwrap();
        match result {
            Ok(Ok(())) => {
                event!(Level::TRACE, "Connection remains healthy");
                slot.state_transition(State::ConnectedUnclaimed(DebugIgnore(conn)));
            }
            Ok(Err(err)) => {
                event!(Level::WARN, err = ?err, "Connection failed during health check");
                self.failure_window.add(1);
                slot.state_transition(State::Connecting);
            }
            Err(_) => {
                event!(Level::WARN, "Connection timed out during health check");
                self.failure_window.add(1);
                slot.state_transition(State::Connecting);
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

/// Describes the state of connections to this backend.
#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    pub(crate) connecting_slots: usize,
    pub(crate) unclaimed_slots: usize,
    pub(crate) checking_slots: usize,
    pub(crate) claimed_slots: usize,
}

impl std::ops::Add for Stats {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            connecting_slots: self.connecting_slots + other.connecting_slots,
            unclaimed_slots: self.unclaimed_slots + other.unclaimed_slots,
            checking_slots: self.checking_slots + other.checking_slots,
            claimed_slots: self.claimed_slots + other.claimed_slots,
        }
    }
}

impl Stats {
    /// Returns the total number of slots in use, regardless of their state.
    pub(crate) fn all_slots(&self) -> usize {
        self.connecting_slots + self.unclaimed_slots + self.checking_slots + self.claimed_slots
    }

    // Returns true if there are no known connected slots.
    fn has_no_connected_slots(&self) -> bool {
        self.unclaimed_slots == 0 && self.checking_slots == 0 && self.claimed_slots == 0
    }

    fn enter_state<Conn: Connection>(&mut self, state: &State<Conn>) {
        match state {
            State::Connecting => self.connecting_slots += 1,
            State::ConnectedUnclaimed(_) => self.unclaimed_slots += 1,
            State::ConnectedChecking => self.checking_slots += 1,
            State::ConnectedClaimed => self.claimed_slots += 1,
            State::Terminated => (),
        };
    }

    fn exit_state<Conn: Connection>(&mut self, state: &State<Conn>) {
        match state {
            State::Connecting => self.connecting_slots -= 1,
            State::ConnectedUnclaimed(_) => self.unclaimed_slots -= 1,
            State::ConnectedChecking => self.checking_slots -= 1,
            State::ConnectedClaimed => self.claimed_slots -= 1,
            State::Terminated => panic!("Should not leave terminated state"),
        };
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

// Owns and runs work on behalf of a [Set].
struct SetWorker<Conn: Connection> {
    backend: Backend,
    config: SetConfig,

    wanted_count: usize,

    // Interface for actually connecting to backends
    backend_connector: SharedConnector<Conn>,

    // Interface for receiving client requests
    rx: mpsc::Receiver<SetRequest<Conn>>,

    // Interface for communicating backend status
    status_tx: watch::Sender<SetState>,

    // Sender and receiver for returning old handles.
    //
    // This is to guarantee a size, and to vend out permits to claim::Handles so they can be sure
    // that their connections can return to the set without error.
    slot_tx: mpsc::Sender<BorrowedConnection<Conn>>,
    slot_rx: mpsc::Receiver<BorrowedConnection<Conn>>,

    // The actual slots themselves.
    slots: BTreeMap<SlotId, Slot<Conn>>,

    // Summary information about the health of all slots.
    //
    // Must be kept in lockstep with "Self::slots"
    stats: Arc<Mutex<Stats>>,

    failure_window: Arc<WindowedCounter>,

    next_slot_id: SlotId,
}

impl<Conn: Connection> SetWorker<Conn> {
    fn new(
        rx: mpsc::Receiver<SetRequest<Conn>>,
        status_tx: watch::Sender<SetState>,
        config: SetConfig,
        wanted_count: usize,
        backend: Backend,
        backend_connector: SharedConnector<Conn>,
        stats: Arc<Mutex<Stats>>,
        failure_window: Arc<WindowedCounter>,
    ) -> Self {
        let (slot_tx, slot_rx) = mpsc::channel(config.max_count);
        let mut set = Self {
            backend,
            config,
            wanted_count,
            backend_connector,
            stats,
            failure_window,
            rx,
            status_tx,
            slot_tx,
            slot_rx,
            slots: BTreeMap::new(),
            next_slot_id: 0,
        };
        set.set_wanted_count(wanted_count);
        set
    }

    // Creates a new Slot, which always starts as "Connecting", and spawn a task
    // to actually connect to the backend and monitor slot health.
    fn create_slot(&mut self, slot_id: SlotId) {
        let slot = Slot {
            failure_window: self.failure_window.clone(),
            inner: Arc::new(Mutex::new(SlotInner {
                state: State::Connecting,
                stats: self.stats.clone(),
                status_tx: self.status_tx.clone(),
                handle: None,
            })),
        };
        let slot = self.slots.entry(slot_id).or_insert(slot).clone();
        self.stats
            .lock()
            .unwrap()
            .enter_state(&State::<Conn>::Connecting);

        slot.inner.lock().unwrap().handle = Some(
            tokio::task::spawn({
                let slot = slot.clone();
                let config = self.config.clone();
                let connector = self.backend_connector.clone();
                let backend = self.backend.clone();
                async move {
                    let mut interval = interval(config.health_interval);

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
                                State::Terminated => return,
                            }
                        };

                        match work {
                            Work::DoConnect => {
                                let span = span!(Level::TRACE, "Slot worker connecting", slot_id);
                                async {
                                    slot.loop_until_connected(&config, &connector, &backend)
                                        .await;
                                    interval
                                        .reset_after(interval.period().add_spread(config.spread));
                                }
                                .instrument(span)
                                .await;
                            }
                            Work::DoMonitor => {
                                let span = span!(Level::TRACE, "Slot worker monitoring", slot_id);
                                async {
                                    interval.tick().await;
                                    slot.validate_health_if_connected(
                                        &connector,
                                        config.health_check_timeout,
                                    )
                                    .await;
                                    interval
                                        .reset_after(interval.period().add_spread(config.spread));
                                }
                                .instrument(span)
                                .await;
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
            event!(Level::TRACE, id = id, state = ?slot.state, "Considering slot");
            if matches!(slot.state, State::ConnectedUnclaimed(_)) {
                event!(Level::TRACE, id = id, "Found unclaimed slot");
                // We intentionally "take the connection out" of the slot and
                // "place it into a claim::Handle" in the same method.
                //
                // This makes it difficult to leak a connection, unless the drop
                // method of the claim::Handle is skipped.
                let State::ConnectedUnclaimed(DebugIgnore(conn)) =
                    slot.state_transition(State::ConnectedClaimed)
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
    #[instrument(level = "trace", skip(self, borrowed_conn), fields(slot_id = borrowed_conn.id))]
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
            slot.state_transition(State::ConnectedUnclaimed(DebugIgnore(borrowed_conn.conn)));
        }

        // If we tried to shrink the slot count while too many connections were
        // in-use, it's possible there's more work to do. Try to conform the
        // slot count after recycling each connection.
        self.conform_slot_count();
    }

    fn set_wanted_count(&mut self, count: usize) {
        self.wanted_count = std::cmp::min(count, self.config.max_count);
        self.conform_slot_count();
    }

    // Makes the number of slots as close to "desired_count" as we can get.
    #[instrument(level = "trace", skip(self), fields(wanted_count = self.wanted_count))]
    fn conform_slot_count(&mut self) {
        let desired = self.wanted_count;

        use std::cmp::Ordering::*;
        match desired.cmp(&self.slots.len()) {
            Less => {
                // Fewer slots wanted. Remove as many as we can.
                event!(
                    Level::TRACE,
                    current = self.slots.len(),
                    "Reducing slot count"
                );

                // Gather all the keys we are trying to remove.
                //
                // If there are many non-removable slots, it's possible
                // we don't immediately quiesce to this smaller requested count.
                let count_to_remove = self.slots.len() - desired;
                let mut to_remove = Vec::with_capacity(count_to_remove);

                // We iterate through all slots twice:
                // - First, we try to remove unconnected slots
                // - Then we remove any removable slots remaining
                //
                // This is a minor optimization that avoids tearing down a
                // connected spare while also trying to create a new one.
                let filters = [
                    |slot: &SlotInner<Conn>| !slot.state.connected() && slot.state.removable(),
                    |slot: &SlotInner<Conn>| slot.state.removable(),
                ];
                for filter in filters {
                    for (key, slot) in &self.slots {
                        if to_remove.len() >= count_to_remove {
                            break;
                        }
                        let mut slot = slot.inner.lock().unwrap();
                        if filter(&*slot) {
                            to_remove.push(*key);

                            // It's important that we terminate the slot
                            // immediately, so the task which manages the slot
                            // will not continue modifying the state.
                            slot.state_transition(State::Terminated);
                        }
                    }
                }

                for key in to_remove {
                    event!(Level::TRACE, slot_id = key, "Removing slot");
                    let Some(slot) = self.slots.remove(&key) else {
                        continue;
                    };
                    let Some(handle) = slot.inner.lock().unwrap().handle.take() else {
                        continue;
                    };
                    event!(Level::TRACE, slot_id = key, "Aborting task");
                    handle.abort();
                }
            }
            Greater => {
                // More slots wanted. This case is easy, we can always fill
                // in "connecting" slots immediately.
                event!(
                    Level::TRACE,
                    current = self.slots.len(),
                    "Increasing slot count"
                );
                let new_slots = desired - self.slots.len();
                for slot_id in self.next_slot_id..self.next_slot_id + new_slots {
                    self.create_slot(slot_id);
                }
                self.next_slot_id += new_slots;
            }
            Equal => {}
        }
    }

    #[instrument(level = "trace", skip(self), err)]
    fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        // Before we vend out the slot's connection to a client, make sure that
        // we have space to take it back once they're done with it.
        let Ok(permit) = self.slot_tx.clone().try_reserve_owned() else {
            event!(Level::TRACE, "Could not reserve slot_tx permit");
            // This is more of an "all slots in-use" error,
            // but it should look the same to clients.
            return Err(Error::NoSlotsReady);
        };

        let Some(handle) = self.take_connected_unclaimed_slot(permit) else {
            event!(Level::TRACE, "Failed to take unclaimed slot");
            return Err(Error::NoSlotsReady);
        };

        Ok(handle)
    }

    #[instrument(level = "trace", skip(self), name = "SetWorker::run")]
    async fn run(&mut self) {
        loop {
            tokio::select! {
                // Recycle old requests
                request = self.slot_rx.recv() => {
                    match request {
                        Some(borrowed_conn) => self.recycle_connection(borrowed_conn),
                        None => {
                            panic!("This should never happen, we hold onto a copy of the sender");
                        },
                    }
                },
                // Handle requests from clients
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

/// Describes the overall status of the backend.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum SetState {
    /// At least one slot is connected to the backend.
    Online,

    /// No slots are known to be connected to the backend.
    Offline,
}

/// A set of slots for a particular backend.
pub(crate) struct Set<Conn: Connection> {
    tx: mpsc::Sender<SetRequest<Conn>>,
    status_rx: watch::Receiver<SetState>,

    stats: Arc<Mutex<Stats>>,
    failure_window: Arc<WindowedCounter>,

    handle: JoinHandle<()>,
}

impl<Conn: Connection> Set<Conn> {
    /// Creates a new slot set.
    ///
    /// Creates several slots which attempt to connect to the backend service
    /// through background tasks.
    ///
    /// These tasks are stopped when [Set] is dropped.
    pub(crate) fn new(
        config: SetConfig,
        wanted_count: usize,
        backend: Backend,
        backend_connector: SharedConnector<Conn>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let (status_tx, status_rx) = watch::channel(SetState::Offline);
        let failure_duration = config.max_connection_backoff * 2;
        let stats = Arc::new(Mutex::new(Stats::default()));
        let failure_window = Arc::new(WindowedCounter::new(failure_duration));
        let handle = tokio::task::spawn({
            let stats = stats.clone();
            let failure_window = failure_window.clone();
            async move {
                let mut worker = SetWorker::new(
                    rx,
                    status_tx,
                    config,
                    wanted_count,
                    backend,
                    backend_connector,
                    stats,
                    failure_window,
                );
                worker.run().await;
            }
        });

        Self {
            tx,
            status_rx,
            stats,
            failure_window,
            handle,
        }
    }

    /// Returns a [tokio::mpsc::watch::Receiver] emitting the last-known [SetState].
    ///
    /// This provides an interface for the pool to use to monitor slot health.
    #[instrument(skip(self), name = "Set::monitor")]
    pub(crate) fn monitor(&self) -> watch::Receiver<SetState> {
        self.status_rx.clone()
    }

    /// Returns the last-known [SetState].
    #[instrument(skip(self), ret, name = "Set::get_state")]
    pub(crate) fn get_state(&self) -> SetState {
        *self.status_rx.borrow()
    }

    /// Returns a claim from the slot set, if one is connected.
    ///
    /// If no unclaimed slots are connected, an error is returned.
    #[instrument(skip(self), name = "Set::claim")]
    pub(crate) async fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(SetRequest::Claim { tx })
            .await
            .map_err(|_| Error::SlotWorkerTerminated)?;

        rx.await.map_err(|_| Error::SlotWorkerTerminated)?
    }

    /// Updates the number of "wanted" slots within the slot set.
    ///
    /// This will eventually update the number of slots being used by the slot
    /// set, though it may take a moment to propagate if many slots are
    /// currently claimed by clients.
    #[instrument(skip(self), name = "Set::set_wanted_count")]
    pub(crate) async fn set_wanted_count(&mut self, count: usize) -> Result<(), Error> {
        self.tx
            .send(SetRequest::SetWantedCount { count })
            .await
            .map_err(|_| Error::SlotWorkerTerminated)?;
        Ok(())
    }

    /// Returns the number of failures encountered over a window of time.
    ///
    /// This acts as a proxy for backend health.
    #[instrument(skip(self), ret, name = "Set::failure_count")]
    pub(crate) fn failure_count(&self) -> usize {
        self.failure_window.sum()
    }

    /// Collect statistics about the number of slots within each respective
    /// state.
    ///
    /// Calling this function is racy, so its usage is recommended only for
    /// test environments and approximate heuristics.
    #[instrument(skip(self), ret, name = "Set::get_stats")]
    pub(crate) fn get_stats(&self) -> Stats {
        self.stats.lock().unwrap().clone()
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
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestConnection {}

    struct TestConnector {
        can_connect: AtomicBool,
    }

    impl TestConnector {
        fn new() -> TestConnector {
            Self {
                can_connect: AtomicBool::new(true),
            }
        }

        #[instrument(level = "trace", skip(self))]
        fn set_connectable(&self, can_connect: bool) {
            self.can_connect.store(can_connect, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl backend::Connector for TestConnector {
        type Connection = TestConnection;

        /// Creates a connection to a backend.
        async fn connect(&self, backend: &Backend) -> Result<Self::Connection, backend::Error> {
            assert_eq!(backend, &backend::Backend { address: BACKEND });

            if self.can_connect.load(Ordering::SeqCst) {
                event!(Level::INFO, "TestConnector::Connect - OK");
                Ok(TestConnection {})
            } else {
                event!(Level::WARN, "TestConnector::Connect - FAIL");
                Err(backend::Error::Other(anyhow::anyhow!("Failed")))
            }
        }

        /// Determines if the connection to a backend is still valid.
        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), backend::Error> {
            if self.can_connect.load(Ordering::SeqCst) {
                event!(Level::INFO, "TestConnector::is_valid - OK");
                Ok(())
            } else {
                event!(Level::WARN, "TestConnector::is_valid - FAIL");
                Err(backend::Error::Other(anyhow::anyhow!("Failed")))
            }
        }
    }

    const BACKEND: SocketAddr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0));

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
    async fn test_one_claim() {
        setup_tracing_subscriber();
        let mut set = Set::new(
            SetConfig::default(),
            5,
            backend::Backend { address: BACKEND },
            Arc::new(TestConnector::new()),
        );

        // Let the connections fill up
        loop {
            let stats = set.get_stats();
            if stats.unclaimed_slots == 0 {
                event!(Level::WARN, "No unclaimed slots");
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                break;
            }
        }

        let _conn = set.claim().await.unwrap();
    }

    #[tokio::test]
    async fn test_drain_slots() {
        setup_tracing_subscriber();
        let mut set = Set::new(
            SetConfig::default(),
            3,
            backend::Backend { address: BACKEND },
            Arc::new(TestConnector::new()),
        );

        // Let the connections fill up
        set.monitor()
            .wait_for(|state| matches!(state, SetState::Online))
            .await
            .unwrap();

        // Grab a connection, then set the "Wanted" count to zero.
        let conn = set.claim().await.unwrap();
        set.set_wanted_count(0).await.unwrap();

        // Let the connections drain
        loop {
            let stats = set.get_stats();
            if stats.unclaimed_slots > 0 {
                event!(Level::WARN, "Should be no unclaimed slots");
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                break;
            }
        }

        assert_eq!(set.get_state(), SetState::Online);
        assert_eq!(set.get_stats().claimed_slots, 1);
        drop(conn);

        set.monitor()
            .wait_for(|state| matches!(state, SetState::Offline))
            .await
            .unwrap();
        assert_eq!(set.get_stats().all_slots(), 0);
    }

    #[tokio::test]
    async fn test_no_slots_add_some_later() {
        setup_tracing_subscriber();
        let mut set = Set::new(
            SetConfig::default(),
            0,
            backend::Backend { address: BACKEND },
            Arc::new(TestConnector::new()),
        );

        // We start with nothing available
        set.claim()
            .await
            .map(|_| ())
            .expect_err("Should not be able to get claims yet");
        assert_eq!(set.get_state(), SetState::Offline);

        // We can later adjust the count of desired slots
        set.set_wanted_count(3).await.unwrap();

        // Let the connections fill up
        set.monitor()
            .wait_for(|state| matches!(state, SetState::Online))
            .await
            .unwrap();

        // When this completes, the connections may be claimed
        let _conn = set.claim().await.unwrap();
    }

    #[tokio::test]
    async fn test_all_claims() {
        setup_tracing_subscriber();
        let mut set = Set::new(
            SetConfig::default(),
            3,
            backend::Backend { address: BACKEND },
            Arc::new(TestConnector::new()),
        );

        // Let the connections fill up
        loop {
            let stats = set.get_stats();
            if stats.unclaimed_slots < 3 {
                event!(Level::WARN, "Not enough unclaimed slots");
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                break;
            }
        }

        let _conn1 = set.claim().await.unwrap();
        let _conn2 = set.claim().await.unwrap();
        let conn3 = set.claim().await.unwrap();

        set.claim()
            .await
            .map(|_| ())
            .expect_err("We should fail to acquire a 4th claim from 3 slot set");

        drop(conn3);

        // Let the connection be recycled
        loop {
            let stats = set.get_stats();
            if stats.unclaimed_slots < 1 {
                event!(Level::WARN, "Not enough unclaimed slots");
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                event!(Level::INFO, "Observed Slots: {:?}", stats);
                break;
            }
        }

        let _conn4 = set.claim().await.unwrap();
    }

    #[tokio::test]
    async fn test_monitor_state() {
        setup_tracing_subscriber();

        // Make sure that when we start, no connections can actually succeed.
        let connector = Arc::new(TestConnector::new());
        connector.set_connectable(false);

        let set = Set::new(
            SetConfig {
                max_count: 3,
                min_connection_backoff: Duration::from_millis(1),
                max_connection_backoff: Duration::from_millis(10),
                spread: Duration::ZERO,
                health_interval: Duration::from_millis(1),
                ..Default::default()
            },
            3,
            backend::Backend { address: BACKEND },
            connector.clone(),
        );

        // The set should initialize as "Offline", since nothing can connect.
        assert_eq!(set.get_state(), SetState::Offline);
        let mut monitor = set.monitor();
        assert_eq!(*monitor.borrow(), SetState::Offline);

        // Enable connections, and let the pool fill up
        connector.set_connectable(true);

        // Let the connections fill up
        loop {
            let stats = set.get_stats();
            if stats.unclaimed_slots < 3 {
                event!(Level::WARN, "Not enough unclaimed slots");
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                break;
            }
        }

        assert_eq!(*monitor.borrow_and_update(), SetState::Online);
        assert_eq!(set.get_state(), SetState::Online);

        // Disable connections, rely on health monitoring to disable them once
        // more.
        connector.set_connectable(false);

        // Let the connections die as their health checks fail
        monitor.changed().await.unwrap();
        assert_eq!(*monitor.borrow_and_update(), SetState::Offline);
        let stats = set.get_stats();
        assert_eq!(stats.all_slots(), 3);
        assert_eq!(stats.connecting_slots, 3);
    }
}
