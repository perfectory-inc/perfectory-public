//! Permission value object.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Permission parsing error.
#[derive(Debug, Error)]
pub enum PermissionError {
    /// Permission did not contain exactly one colon separator.
    #[error("permission must use 'resource:action' format, got {0:?}")]
    InvalidFormat(String),
}

/// Permission string in `<resource>:<action>` format.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Permission(String);

impl Permission {
    /// Parses a permission string in `<resource>:<action>` format.
    ///
    /// # Errors
    /// Returns [`PermissionError::InvalidFormat`] when the string does not contain exactly one
    /// colon separator.
    pub fn parse(input: impl Into<String>) -> Result<Self, PermissionError> {
        let raw = input.into();
        if raw.split(':').count() != 2 {
            return Err(PermissionError::InvalidFormat(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the validated permission string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Permission {
    type Error = PermissionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<Permission> for String {
    fn from(permission: Permission) -> Self {
        permission.0
    }
}
