use crate::connection::Connection;
use crate::slot::BorrowedConnection;
use tokio::sync::mpsc::OwnedPermit;

pub struct Handle<Conn: Connection> {
    inner: Option<BorrowedConnection<Conn>>,
    permit: Option<OwnedPermit<BorrowedConnection<Conn>>>,
}

// TODO: Should this impl deref here?
//
// The handle basically wants to be treated like a Connection.
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

    pub fn connection(&self) -> &Conn {
        self.inner.as_ref().map(|inner| &inner.conn).unwrap()
    }
}

impl<Conn: Connection> Drop for Handle<Conn> {
    fn drop(&mut self) {
        self.permit.take().unwrap().send(self.inner.take().unwrap());
    }
}
