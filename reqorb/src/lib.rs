// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `reqorb` glues [`reqwest`] onto [`qorb`], in a somewhat unpleasant manner.

use core::marker::PhantomData;

use async_trait::async_trait;
use qorb::backend::{self, Backend};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

/// Unfortunate but reasonable.
#[must_use]
pub struct ReqorbConnector<C = reqwest::Client> {
    _client: PhantomData<C>,
    builder_fn: Option<Arc<dyn Fn(&Backend) -> reqwest::ClientBuilder + Send + Sync + 'static>>,
    known_domain: Option<Arc<str>>,
}

impl Default for ReqorbConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> ReqorbConnector<C> {
    pub fn new() -> Self {
        Self {
            _client: PhantomData,
            builder_fn: None,
            known_domain: None,
        }
    }

    /// Indicates that a single DNS domain is known for all backends that will
    /// be resolved by the `qorb` pool.
    ///
    /// Setting this reduces overhead from no-op DNS lookups.
    pub fn known_domain(self, domain: impl Into<Arc<str>>) -> Self {
        Self {
            known_domain: Some(domain.into()),
            ..self
        }
    }

    /// Sets a function that will be used to create a `reqwest::ClientBuilder`
    /// for each backend.
    ///
    /// This method can be used to customize the client's behavior, such as
    /// setting custom headers or timeouts.
    ///
    /// Note that the following builder settings will be overridden by the
    /// `ReqorbConnector`, and therefore should not be set by the provided
    /// builder function:
    ///
    /// - [`reqwest::ClientBuilder::pool_max_idle_per_host`]
    /// - [`reqwest::ClientBuilder::pool_idle_timeout`]
    /// - [`reqwest::ClientBuilder::dns_resolver`]
    pub fn client_builder_fn(
        self,
        mk_builder: impl Fn(&Backend) -> reqwest::ClientBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            builder_fn: Some(Arc::new(mk_builder)),
            ..self
        }
    }

    fn reqwest_builder(&self, backend: &Backend) -> reqwest::ClientBuilder {
        struct FixedResolver(SocketAddr);
        impl reqwest::dns::Resolve for FixedResolver {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                // ha ha ha this is terrible
                Box::pin(std::future::ready(Ok(
                    Box::new(std::iter::once(self.0)) as reqwest::dns::Addrs
                )))
            }
        }

        // If we were configured with other `reqwest` builder settings, use those.
        let mut builder = self
            .builder_fn
            .as_ref()
            .map(|f| f(backend))
            .unwrap_or_else(reqwest::ClientBuilder::new);

        if let Some(ref domain) = self.known_domain {
            // if we know the domain that this client will be used for, override
            // DNS resolution for that domain to the backend's address. this
            // lets us bypass the overhead of the `FixedResolver` nonsense
            // around boxing the future and address iterator.
            builder = builder.resolve_to_addrs(&*domain, &[backend.address])
        }

        builder
            // allow the reqwest client to pool *precisely one connection* per host,
            // forcing it to act as a persistent connection.
            .pool_max_idle_per_host(1)
            // never time out idle connections
            .pool_idle_timeout(None)
            .dns_resolver(Arc::new(FixedResolver(backend.address)))
    }
}

impl<C> Clone for ReqorbConnector<C> {
    fn clone(&self) -> Self {
        ReqorbConnector {
            _client: PhantomData,
            known_domain: self.known_domain.clone(),
            builder_fn: self.builder_fn.clone(),
        }
    }
}

impl<C> std::fmt::Debug for ReqorbConnector<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            _client,
            known_domain,
            builder_fn,
        } = self;
        f.debug_struct("ReqorbConnector")
            .field("known_domain", &known_domain)
            .field(
                "builder_fn",
                &format_args!(
                    "{}",
                    if builder_fn.is_some() {
                        "Some(...)"
                    } else {
                        "None"
                    }
                ),
            )
            .finish()
    }
}

#[derive(Debug)]
pub struct ReqorbClient<C = reqwest::Client>
where
    C: backend::Connection,
{
    client: C,
    /// An error recorded whilst the client was in use. Unfortunately, our only
    /// way to easily communicate errors back to `qorb` is to make the caller
    /// explicitly tell us that the client is error-y.
    error: Option<backend::Error>,
}

impl<C> Deref for ReqorbClient<C>
where
    C: backend::Connection,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<C> ReqorbClient<C>
where
    C: backend::Connection,
{
    /// Indicate that an error was observed whilst this client was in use, and
    /// should be reported to the `qorb` pool from which this client was
    /// acquired.
    ///
    /// Code using a `ReqorbClient` MUST call this method if it wishes to inform
    /// the `qorb` pool of errors that occur when using the client.
    pub fn record_error(&mut self, error: impl Into<backend::Error>) {
        self.error = Some(error.into());
    }
}

#[async_trait]
impl<C> backend::Connector for ReqorbConnector<C>
where
    // The connection type is "something that can be constructed from a
    // reqwest::Client". This way, we can construct pools of e.g.
    // `progenitor`-generated clients of various types.
    C: From<reqwest::Client> + backend::Connection + Sync,
{
    type Connection = ReqorbClient<C>;

    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, backend::Error> {
        let client = self
            .reqwest_builder(backend)
            .build()
            .map_err(|e| backend::Error::Other(e.into()))?
            .into();
        Ok(ReqorbClient {
            client,
            error: None,
        })
    }

    async fn is_valid(&self, client: &mut Self::Connection) -> Result<(), backend::Error> {
        match client.error.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
