use crate::{
    backend,
    pool::{self, Pool},
    service,
};
use dropshot::RequestContext;
use dropshot::WebsocketConnection;
use dropshot::{channel, endpoint};
use dropshot::{HttpError, HttpResponseOk};
use dropshot::{Path, Query};
use futures::{stream::FuturesUnordered, SinkExt, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    sync::watch,
    time::{self, Duration},
};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
pub struct Qtop {
    pools_tx: watch::Sender<BTreeMap<service::Name, pool::Stats>>,
    pools: watch::Receiver<BTreeMap<service::Name, pool::Stats>>,
}

impl Qtop {
    pub fn api() -> dropshot::ApiDescription<Self> {
        let mut api = dropshot::ApiDescription::new();
        api.register(get_pools)
            .expect("registering `get_pools` should succeed");
        api.register(get_pool_stats)
            .expect("registering `get_pool_stats` should succeed");
        api.register(get_all_stats)
            .expect("registering `get_all_stats` should succeed");
        api
    }

    #[must_use]
    pub fn new() -> Self {
        let (pools_tx, pools) = watch::channel(BTreeMap::new());
        Self { pools_tx, pools }
    }

    pub fn add_pool<C: backend::Connection>(&self, pool: &Pool<C>) {
        let name = pool.service_name().clone();
        let stats = pool.stats();
        self.pools_tx.send_if_modified(|pools| {
            match pools.entry(name) {
                // Pool is already present, do nothing.
                Entry::Occupied(entry) if stats.same_pool(entry.get()) => false,
                Entry::Occupied(mut entry) => {
                    entry.insert(stats.clone());
                    true
                }
                Entry::Vacant(entry) => {
                    entry.insert(stats.clone());
                    true
                }
            }
        });
    }

    pub async fn serve_ws(
        mut self,
        mut ws: tokio_tungstenite::WebSocketStream<dropshot::WebsocketConnectionRaw>,
        update_interval: Duration,
    ) -> dropshot::WebsocketChannelResult {
        let mut interval = time::interval(update_interval);
        let mut cache = Cache::default();
        let mut changed_pools = FuturesUnordered::new();
        loop {
            if self.pools.has_changed()? {
                let new_pools = self.pools.borrow_and_update();
                cache.pools.retain(|name, _| new_pools.contains_key(name));
                for (name, stats) in new_pools.iter() {
                    cache.pools.entry(name.clone()).or_insert_with(|| {
                        let mut stats = stats.clone();
                        let sets = stats.rx.borrow_and_update().clone();
                        let claims = stats.claims.load(Ordering::Relaxed);
                        changed_pools.push({
                            let name = name.clone();
                            let changed = todo!();
                            async move {
                                changed.await;
                                name
                            }
                        });
                        PoolCache {
                            stats,
                            sets,
                            claims,
                        }
                    });
                }
            }

            tokio::select! {
                next_changed_pool = changed_pools.next() => {
                    if let Some((name, stats)) = next_changed_pool {
                        cache.pools.entry(name).and_modify(|cache| {
                            cache.stats = stats.clone();
                            cache.sets = stats.rx.borrow_and_update().clone();
                            cache.claims = stats.claims.load(Ordering::Relaxed);
                        });
                    }
                }
                _ = interval.tick() => {},
                _ = self.pools.changed() => {},
            }
        }
    }
}
impl Default for Qtop {
    fn default() -> Self {
        Self::new()
    }
}

impl serde::Serialize for pool::SerializeStats {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let inner = self.0.lock().unwrap();
        inner.serialize(serializer)
    }
}

#[derive(Default)]
struct Cache {
    pools: BTreeMap<service::Name, PoolCache>,
}

#[derive(serde::Serialize)]
struct PoolCache {
    #[serde(skip)]
    stats: pool::Stats,
    sets: BTreeMap<backend::Name, pool::SerializeStats>,
    claims: usize,
}

fn serialize_atomic_usize<S: serde::Serializer>(
    atomic: &Arc<AtomicUsize>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    atomic.load(Ordering::Relaxed).serialize(serializer)
}

impl pool::Stats {
    pub async fn serve_ws(
        mut self,
        mut ws: tokio_tungstenite::WebSocketStream<dropshot::WebsocketConnectionRaw>,
        update_interval: Duration,
    ) -> dropshot::WebsocketChannelResult {
        let mut interval = time::interval(update_interval);
        let mut cache = BTreeMap::new();
        let mut claims_last = self.claims.load(Ordering::Relaxed);
        loop {
            if self.rx.has_changed()? {
                cache.clone_from(&*self.rx.borrow_and_update());
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

#[derive(Serialize, JsonSchema)]
#[schemars(example = "PoolList::example")]
struct PoolList {
    pools: Vec<service::Name>,
}

impl PoolList {
    fn example() -> Self {
        PoolList {
            pools: vec!["foo".into(), "bar".into()],
        }
    }
}

#[derive(Deserialize, JsonSchema)]
struct QueryParams {
    update_secs: Option<u8>,
}

#[endpoint{
    method = GET,
    path = "/qtop/pools",
}]
async fn get_pools(rqctx: RequestContext<Qtop>) -> Result<HttpResponseOk<PoolList>, HttpError> {
    let pools = rqctx.context().pools.borrow().keys().cloned().collect();
    Ok(HttpResponseOk(PoolList { pools }))
}

#[endpoint {
    method = GET,
    path = "/qtop/stats/{pool}",
}]
async fn get_pool_stats(
    rqctx: RequestContext<Qtop>,
    path: Path<String>,
    qp: Query<QueryParams>,
    websock: dropshot::WebsocketUpgrade,
) -> dropshot::WebsocketEndpointResult {
    let update_interval = Duration::from_secs(u64::from(qp.into_inner().update_secs.unwrap_or(1)));
    let pool = path.into_inner();
    match rqctx.context().pools.borrow().get(pool.as_str()) {
        Some(stats) => {
            let stats = stats.clone();
            websock.handle(move |upgraded| async move {
                let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                    upgraded.into_inner(),
                    Role::Server,
                    None,
                )
                .await;
                stats.serve_ws(ws, update_interval).await
            })
        }
        None => Err(HttpError::for_not_found(
            None,
            format!("no pool named {pool}"),
        )),
    }
}

#[channel {
    protocol = WEBSOCKETS,
    path = "/qtop/stats",
}]
async fn get_all_stats(
    rqctx: RequestContext<Qtop>,
    qp: Query<QueryParams>,
    upgraded: WebsocketConnection,
) -> dropshot::WebsocketChannelResult {
    // let update_interval = Duration::from_secs(u64::from(qp.into_inner().update_secs.unwrap_or(1)));
    // rqctx
    //     .context()
    //     .clone()
    //     .serve_ws(upgraded, update_interval)
    //     .await
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dropshot_api_is_reasonable() {
        // just ensure that the API description doesn't panic...
        let _api = Qtop::api();
    }
}
