//! Connections which are borrowed from the connection pool.

use crate::backend::Connection;
use crate::slot::BorrowedConnection;
use tokio::sync::mpsc::OwnedPermit;

pub struct Handle<Conn: Connection> {
    inner: Option<BorrowedConnection<Conn>>,
    permit: Option<OwnedPermit<BorrowedConnection<Conn>>>,
}

impl<Conn: Connection> Handle<Conn> {
    pub(crate) fn new(
        conn: BorrowedConnection<Conn>,
        permit: OwnedPermit<BorrowedConnection<Conn>>,
    ) -> Self {
        Self {
            inner: Some(conn),
            permit: Some(permit),
        }
    }
}

impl<Conn> std::ops::Deref for Handle<Conn>
where
    Conn: Send + 'static,
{
    type Target = Conn;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().map(|inner| &inner.conn).unwrap()
    }
}

impl<Conn> std::ops::DerefMut for Handle<Conn>
where
    Conn: Send + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().map(|inner| &mut inner.conn).unwrap()
    }
}

impl<Conn: Connection> Drop for Handle<Conn> {
    fn drop(&mut self) {
        let conn = self.inner.take().unwrap();
        let permit = self.permit.take().unwrap();

        permit.send(conn);
    }
}
