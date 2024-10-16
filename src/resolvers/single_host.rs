//! Implementation of [Resolver] that always returns an explicit address.

use tokio::sync::watch;

use crate::backend;
use crate::resolver::{AllBackends, Resolver};

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// A [`Resolver`] that always returns a single address.
#[derive(Clone, Debug)]
pub struct SingleHostResolver {
    tx: watch::Sender<AllBackends>,
}

const SINGLE_HOST_BACKEND_NAME: &str = "singleton";

impl SingleHostResolver {
    /// Construct a resolver to always return the provided address.
    pub fn new(address: SocketAddr) -> Self {
        let backends = Arc::new(BTreeMap::from([(
            backend::Name::new(SINGLE_HOST_BACKEND_NAME),
            backend::Backend { address },
        )]));
        let (tx, _rx) = watch::channel(backends.clone());
        Self { tx }
    }
}

impl Resolver for SingleHostResolver {
    fn monitor(&mut self) -> watch::Receiver<AllBackends> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::{
        backend::Backend, resolver::Resolver as _, resolvers::single_host::SINGLE_HOST_BACKEND_NAME,
    };

    use super::SingleHostResolver;

    #[test]
    fn single_host_resolver_returns_address() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4444);
        let mut res = SingleHostResolver::new(addr);
        let rx = res.monitor();
        let backends = rx.borrow();
        assert_eq!(backends.len(), 1);
        let Backend { address } = backends
            .get(SINGLE_HOST_BACKEND_NAME)
            .expect("Expected the single host backend name");
        assert_eq!(
            address, &addr,
            "Single host resolver returned wrong address"
        );
    }
}
