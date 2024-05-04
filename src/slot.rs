use crate::backend::{Backend, SharedConnector};
use crate::claim;
use crate::connection::Connection;

use debug_ignore::DebugIgnore;
use derive_where::derive_where;
use std::collections::BTreeMap;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Error, Debug)]
pub enum Error {
    #[error("No slots available for backend")]
    NoSlotsReady,

    #[error("Worker Terminated Unexpectedly")]
    SlotWorkerTerminated,
}

#[derive_where(Debug)]
enum State<Conn: Connection> {
    /// The slot is attempting to connect.
    Connecting,

    /// The slot has an active connection to a backend, and is ready for use.
    ConnectedUnclaimed(DebugIgnore<Conn>),

    /// The slot has an active connection to a backend, and is claimed.
    ConnectedClaimed,

    /// The slot is not connected, and is not attempting to connect.
    Stopped,
}

impl<Conn: Connection> State<Conn> {
    fn removable(&self) -> bool {
        use State::*;
        match self {
            ConnectedClaimed => false,
            _ => true,
        }
    }
}

/// A slot represents a connection to a single backend,
/// which may or may not actually exist.
///
/// Slots scale up and down in quantity at the request of the rebalancer.
pub struct Slot<Conn: Connection> {
    state: State<Conn>,
}

impl<Conn: Connection> Slot<Conn> {
    pub fn new() -> Self {
        Self {
            state: State::Connecting,
        }
    }
}

/// An arbitrary opaque identifier for a slot, to distinguish it from
/// other slots which already exist.
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

pub(crate) struct SetParameters {
    /// The backend to which all the slots are connected, or trying to connect
    pub(crate) backend: Backend,

    /// The currently-desired number of slots to use.
    pub(crate) desired_count: usize,

    /// The max number of slots for the connection set.
    pub(crate) max_count: usize,
}

/// A set of slots for a particular backend.
pub(crate) struct Set<Conn: Connection> {
    params: SetParameters,

    // Interface for actually connecting to backends
    backend_connector: SharedConnector<Conn>,

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

impl<Conn: Connection> Set<Conn> {
    pub(crate) fn new(
        mut params: SetParameters,
        backend_connector: SharedConnector<Conn>,
    ) -> Self {
        let (slot_tx, slot_rx) = mpsc::channel(params.max_count);

        // Cap the "goal" slot count to always be within the maximum size
        params.desired_count = std::cmp::min(params.desired_count, params.max_count);

        // Set up the initial set of slots
        let init_count = params.desired_count;
        let slots = BTreeMap::from_iter((0..init_count).into_iter().map(|id| (id, Slot::new())));

        Self {
            params,
            backend_connector,
            slot_tx,
            slot_rx,
            slots,
            next_slot_id: init_count,
        }
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
    fn recycle_connection(&mut self, borrowed_conn: BorrowedConnection<Conn>) {
        let slot = self.slots.get_mut(&borrowed_conn.id).expect(
            "A borrowed connection was returned to this\
                pool, and it should reference a slot that \
                cannot be removed while borrowed",
        );
        assert!(
            matches!(slot.state, State::ConnectedClaimed),
            "Unexpected slot state {:?}",
            slot.state
        );
        slot.state = State::ConnectedUnclaimed(DebugIgnore(borrowed_conn.conn));

        // If we tried to shrink the slot count while too many connections were
        // in-use, it's possible there's more work to do. Try to conform the
        // slot count after recycling each connection.
        self.conform_slot_count();
    }

    pub fn set_wanted_count(&mut self, count: usize) {
        self.params.desired_count = std::cmp::min(count, self.params.max_count);
        self.conform_slot_count();
    }

    // Makes the number of slots as close to "desired_count" as we can get.
    fn conform_slot_count(&mut self) {
        let desired = self.params.desired_count;

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
            self.slots.extend(
                (self.next_slot_id..self.next_slot_id + new_slots)
                    .into_iter()
                    .map(|id| (id, Slot::new())),
            );
            self.next_slot_id += new_slots;
        }
    }

    pub fn claim(&mut self) -> Result<claim::Handle<Conn>, Error> {
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

    /// Must be called periodically to handle background tasks for the slot set.
    ///
    /// This includes:
    /// - Recycling previously-claimed connections returned by clients
    /// - TODO
    pub async fn step(&mut self) {
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
                // TODO: also, monitor our own slots for progress??
            }
        }
    }
}
