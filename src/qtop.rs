//! Optional module for exporting stats via dropshot

use crate::pool;
use dropshot::channel;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::WebsocketConnection;
use futures::SinkExt;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::time::{self, Duration};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;

impl pool::Stats {
    pub async fn serve_ws(
        mut self,
        ws: WebsocketConnection,
        update_interval: Duration,
    ) -> dropshot::WebsocketChannelResult {
        let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            ws.into_inner(),
            Role::Server,
            None,
        )
        .await;
        let mut interval = time::interval(update_interval);
        let mut cache = HashMap::new();
        let mut claims_last = self.claims.load(Ordering::Relaxed);
        loop {
            if self.rx.has_changed()? {
                cache = (*self.rx.borrow_and_update()).clone();
            }
            let claims = self.claims.load(Ordering::Relaxed);
            let claims_per_interval = claims - claims_last;
            claims_last = claims;
            ws.send(Message::Binary(serde_json::to_vec(&serde_json::json!({
                "claims": claims_per_interval,
                "sets": &cache,
            }))?))
            .await?;

            tokio::select! {
                _ = interval.tick() => {},
                _ = self.rx.changed() => {},
            }
        }
    }
}

// HTTP API interface

#[derive(Deserialize, JsonSchema)]
struct QueryParams {
    update_secs: Option<u8>,
}

/// Exports statistics from a connection pool at a regular interval.
#[channel {
    protocol = WEBSOCKETS,
    path = "/qtop/stats",
}]
pub async fn serve_stats(
    rqctx: RequestContext<pool::Stats>,
    qp: Query<QueryParams>,
    upgraded: WebsocketConnection,
) -> dropshot::WebsocketChannelResult {
    let update_interval = Duration::from_secs(u64::from(qp.into_inner().update_secs.unwrap_or(1)));
    rqctx
        .context()
        .clone()
        .serve_ws(upgraded, update_interval)
        .await
}
