use criterion::{black_box, criterion_group, criterion_main, Criterion};

use async_trait::async_trait;
use qorb::backend::{self, Backend, Connector};
use qorb::policy::Policy;
use qorb::pool::Pool;
use qorb::resolver::{AllBackends, Resolver};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::watch;

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("concurrently claim 10", |b| {
        b.to_async(&rt).iter(|| concurrent_claims(black_box(10)))
    });

    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("concurrently claim 100", |b| {
        b.to_async(&rt).iter(|| concurrent_claims(black_box(100)))
    });

    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("concurrently claim 1000", |b| {
        b.to_async(&rt).iter(|| concurrent_claims(black_box(1000)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

#[derive(Clone)]
struct TestResolver {
    tx: watch::Sender<AllBackends>,
}

impl TestResolver {
    fn new() -> Self {
        let backends = Arc::new(BTreeMap::new());
        let (tx, _) = watch::channel(backends);
        Self { tx }
    }

    fn replace(&self, backends: BTreeMap<backend::Name, Backend>) {
        self.tx.send_replace(Arc::new(backends));
    }
}

impl Resolver for TestResolver {
    fn monitor(&mut self) -> watch::Receiver<AllBackends> {
        self.tx.subscribe()
    }
}

struct TestConnection {}

impl TestConnection {
    fn new() -> Self {
        Self {}
    }
}

struct TestConnector {}

impl TestConnector {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Connector for TestConnector {
    type Connection = TestConnection;

    async fn connect(&self, _backend: &Backend) -> Result<Self::Connection, backend::Error> {
        Ok(TestConnection::new())
    }
}

async fn concurrent_claims(count: usize) {
    let resolver = Box::new(TestResolver::new());
    let connector = Arc::new(TestConnector::new());
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    resolver.replace(BTreeMap::from([(
        backend::Name::new("aaa"),
        Backend::new(address),
    )]));

    let pool = Arc::new(
        Pool::new(resolver, connector, Policy::default()).expect("Failed to register probes"),
    );

    let futs: Vec<_> = (0..count)
        .map(|_| {
            tokio::task::spawn({
                let pool = pool.clone();
                async move {
                    let handle = pool.claim().await.expect("Failed to get claim");
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;
                    drop(handle);
                }
            })
        })
        .collect();
    futures::future::try_join_all(futs)
        .await
        .expect("Failed to get claims");
}
