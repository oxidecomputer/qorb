use crate::backend::Backend;
use crate::resolver::{ResolveEvent, Resolver, ResolverState};
use crate::service;
use tokio::sync::broadcast;

pub struct DnsResolver {
    service: service::Name,
}

impl DnsResolver {
    pub fn new(service: service::Name) -> Self {
        Self { service }
    }
}

impl Resolver for DnsResolver {
    fn monitor(&self) -> broadcast::Receiver<ResolveEvent> {
        todo!();
    }

    fn backends(&self) -> Vec<Backend> {
        todo!();
    }

    fn state(&self) -> ResolverState {
        todo!();
    }
}
