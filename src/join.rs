//! Helpers for joining terminating tasks

use tokio::task::JoinError;

pub(crate) fn propagate_panics(result: Result<(), JoinError>) {
    match result {
        // Success or cancellation: Quietly return
        Ok(()) => return,
        Err(err) if err.is_cancelled() => return,
        // Propagate panics
        Err(err) if err.is_panic() => {
            std::panic::panic_any(err.into_panic());
        }
        Err(err) => {
            panic!("Unexpected join error (other than panic or cancellation): {err}");
        }
    }
}
