//! Utilities to help with testing qorb

use crate::backend::{self, Backend, Connector};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// A test-only connector which can slow down connection access
/// to mimic high-latency connection issues.
pub struct SlowConnector {
    delay_ms: AtomicU64,
    panic_on_access: AtomicBool,
}

impl SlowConnector {
    /// Creates a new connector, which only takes 1ms per operation
    pub fn new() -> Self {
        Self {
            delay_ms: AtomicU64::new(1),
            panic_on_access: AtomicBool::new(false),
        }
    }

    /// Stalls all new operations through the connector, forcing them to
    /// take an unrealistically long time.
    pub fn stall(&self) {
        self.delay_ms.store(9999999, Ordering::SeqCst);
    }

    /// Mark that any future access through the connector should panic.
    ///
    /// Although "stall" prevents connections from getting through the
    /// Connector APIs, this ensures that new connection operations
    /// aren't even starting.
    pub fn panic_on_access(&self) {
        self.panic_on_access.store(true, Ordering::SeqCst);
    }

    // Internal shared logic for each of the connetor APIs
    async fn react_to_connection_operation(&self) {
        if self.panic_on_access.load(Ordering::SeqCst) {
            panic!("Should not be making new requests through this connector!");
        }

        let delay_ms = self.delay_ms.load(Ordering::SeqCst);
        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
    }
}

#[async_trait]
impl Connector for SlowConnector {
    type Connection = ();

    async fn connect(&self, _backend: &Backend) -> Result<Self::Connection, backend::Error> {
        self.react_to_connection_operation().await;
        Ok(())
    }

    async fn is_valid(&self, _: &mut Self::Connection) -> Result<(), backend::Error> {
        self.react_to_connection_operation().await;
        Ok(())
    }

    async fn on_acquire(&self, _: &mut Self::Connection) -> Result<(), backend::Error> {
        self.react_to_connection_operation().await;
        Ok(())
    }

    async fn on_recycle(&self, _: &mut Self::Connection) -> Result<(), backend::Error> {
        self.react_to_connection_operation().await;
        Ok(())
    }
}
