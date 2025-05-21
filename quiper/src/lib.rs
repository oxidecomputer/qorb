// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`qorb`] as a [`hyper-util`] connection pool

use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connected, Connection as HyperUtilConnection};
use pin_project::pin_project;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub mod connector;

pub struct HyperPool<Conn: Send + 'static = connector::TcpConnection> {
    pool: Arc<qorb::pool::Pool<Conn>>,
}

impl<Conn: Send + 'static> Clone for HyperPool<Conn> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

#[pin_project]
pub struct QorbConn<Conn = connector::TcpConnection>
where
    Conn: Send + 'static,
{
    #[pin]
    handle: Pin<qorb::claim::Handle<Conn>>,
}

// === impl HyperPool ===

impl<Conn: Unpin + Send + 'static> HyperPool<Conn> {
    pub fn new(pool: Arc<qorb::pool::Pool<Conn>>) -> Self {
        Self { pool }
    }

    pub fn build_client<B>(
        self,
        mut builder: hyper_util::client::legacy::Builder,
    ) -> Client<Self, B>
    where
        Conn: Read + Write + HyperUtilConnection,
        B: hyper::body::Body + Send,
        B::Data: Send,
    {
        builder
            .pool_idle_timeout(None)
            .pool_max_idle_per_host(0)
            .build(self)
    }
}

impl<Conn> tower_service::Service<http::Uri> for HyperPool<Conn>
where
    Conn: Unpin + Send + 'static,
{
    type Response = QorbConn<Conn>;
    type Error = qorb::pool::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // We might, at first glance, think that it would be nicer to be able to
        // ask the pool if it has an immediately-available connection here, and
        // register interest to be woken if it does not. But...it turns out this
        // just doesn't matter at all, because `hyper-util` will always oneshot
        // the service anyway:
        // https://docs.rs/hyper-util/0.1.12/src/hyper_util/client/legacy/connect/mod.rs.html#348-350
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let pool = self.pool.clone();
        Box::pin(async move {
            let handle = pool.claim().await?;
            Ok(QorbConn {
                handle: Pin::new(handle),
            })
        })
    }
}

// === impl QorbConn ===

impl<Conn> Read for QorbConn<Conn>
where
    Conn: Read + Send + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().handle.as_mut().poll_read(cx, buf)
    }
}

impl<Conn> Write for QorbConn<Conn>
where
    Conn: Write + Send + 'static,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().handle.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().handle.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().handle.as_mut().poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.project().handle.as_mut().poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.handle.is_write_vectored()
    }
}

impl<Conn> HyperUtilConnection for QorbConn<Conn>
where
    Conn: HyperUtilConnection + Send + 'static,
{
    fn connected(&self) -> Connected {
        self.handle.as_ref().connected()
    }
}
