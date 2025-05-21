// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use async_trait::async_trait;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::client::legacy::connect::{Connected, Connection as HyperUtilConnection};
use hyper_util::rt::TokioIo;
use pin_project::pin_project;
use qorb::backend::{self, Backend};
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;

#[derive(Debug, Default)]
pub struct HttpConnector {
    _p: (),
}

#[pin_project]
pub struct TcpConnection {
    // TODO(eliza): TLS!
    #[pin]
    sock: TokioIo<TcpStream>,
    last_err: Option<io::Error>,
}

#[async_trait]
impl backend::Connector for HttpConnector {
    type Connection = TcpConnection;

    async fn connect(&self, backend: &Backend) -> Result<Self::Connection, backend::Error> {
        let sock = TcpStream::connect(backend.address)
            .await
            .map_err(backend::Error::from)?;
        // TODO(eliza): we are definitely gonna want to set a bunch of socket
        // options in production...
        Ok(TcpConnection {
            sock: TokioIo::new(sock),
            last_err: None,
        })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), backend::Error> {
        match conn.last_err.take() {
            Some(err) => Err(backend::Error::Io(err)),
            None => Ok(()),
        }
    }
}

impl Read for TcpConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();
        this.sock
            .as_mut()
            .poll_read(cx, buf)
            .map_err(capture_err(this.last_err))
    }
}

impl Write for TcpConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        this.sock
            .as_mut()
            .poll_write(cx, buf)
            .map_err(capture_err(this.last_err))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        this.sock
            .as_mut()
            .poll_flush(cx)
            .map_err(capture_err(this.last_err))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        this.sock
            .as_mut()
            .poll_shutdown(cx)
            .map_err(capture_err(this.last_err))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        this.sock
            .as_mut()
            .poll_write_vectored(cx, bufs)
            .map_err(capture_err(this.last_err))
    }

    fn is_write_vectored(&self) -> bool {
        self.sock.is_write_vectored()
    }
}

impl HyperUtilConnection for TcpConnection {
    fn connected(&self) -> Connected {
        self.sock.connected()
    }
}

fn capture_err(last: &mut Option<io::Error>) -> impl FnMut(io::Error) -> io::Error {
    use io::ErrorKind;
    move |e| {
        let kind = e.kind();
        match kind {
            // These indicate conditions in which we would probably want to tell
            // `qorb` that the connection is no longer usable.
            ErrorKind::ConnectionReset
            | ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionAborted
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::AddrNotAvailable => {
                *last = Some(io::Error::from(ErrorKind::Interrupted));
            }
            _ => {}
        }
        e
    }
}
