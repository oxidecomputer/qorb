//! Implementation of [Resolver] that always returns a fixed set of addresses.

use tokio::sync::watch;

use crate::backend;
use crate::resolver::{AllBackends, Resolver};

use std::net::SocketAddr;
use std::sync::Arc;

/// A [`Resolver`] that always returns a fixed set of addresses.
#[derive(Clone, Debug)]
pub struct FixedResolver {
    tx: watch::Sender<AllBackends>,
}

impl FixedResolver {
    pub fn new(addrs: impl IntoIterator<Item = SocketAddr>) -> FixedResolver {
        let all_backends = Arc::new(
            addrs
                .into_iter()
                .map(|address| (backend::Name::new(address), backend::Backend { address }))
                .collect(),
        );
        let (tx, _rx) = watch::channel(all_backends);
        FixedResolver { tx }
    }
}

impl Resolver for FixedResolver {
    fn monitor(&mut self) -> watch::Receiver<AllBackends> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::{backend::Backend, backend::Name, resolver::Resolver as _};

    use super::FixedResolver;

    #[test]
    fn fixed_resolver_returns_addresses() {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4444);
        let addr2 = SocketAddr::new("ff:dd:ee::3".parse().unwrap(), 4445);
        let mut res = FixedResolver::new([addr1, addr2]);
        let rx = res.monitor();
        let backends = rx.borrow();
        println!("{:?}", backends);
        assert_eq!(backends.len(), 2);
        let Backend { address } = backends.get(&Name::new("127.0.0.1:4444")).unwrap();
        assert_eq!(*address, addr1);
        let Backend { address } = backends.get(&Name::new("[ff:dd:ee::3]:4445")).unwrap();
        assert_eq!(*address, addr2);
    }
}
