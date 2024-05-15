//! Default implementations of [crate::backend::Connector]

pub mod tcp;

#[cfg(feature = "diesel_pg")]
pub mod diesel_pg;
