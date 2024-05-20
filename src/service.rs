//! Interface for services
use std::sync::Arc;

/// Describes the name of a service
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Name(pub Arc<str>);

impl Name {
    pub fn new(s: impl ToString) -> Self {
        Self(Arc::from(s.to_string()))
    }
}

impl From<String> for Name {
    fn from(s: String) -> Self {
        Self(Arc::from(s))
    }
}

impl From<&'_ str> for Name {
    fn from(s: &'_ str) -> Self {
        Self(Arc::from(s))
    }
}

impl std::borrow::Borrow<str> for Name {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}
