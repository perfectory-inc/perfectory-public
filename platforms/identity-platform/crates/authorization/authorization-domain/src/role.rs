//! Role code and staff role grant model.

use chrono::{DateTime, Utc};
use identity_shared_kernel::StaffId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Validation error returned while parsing a role code.
#[derive(Debug, Error)]
pub enum RoleCodeError {
    /// Role code was empty.
    #[error("role code must not be empty")]
    Empty,
    /// Role code contained unsupported characters.
    #[error("role code allows only uppercase ASCII letters, digits, and underscores, got {0:?}")]
    InvalidCharacters(String),
}

/// Validated role code in `SCREAMING_SNAKE_CASE` style.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RoleCode(String);

impl RoleCode {
    /// Parses a role code.
    ///
    /// # Errors
    /// Returns [`RoleCodeError::Empty`] when input is empty, or
    /// [`RoleCodeError::InvalidCharacters`] when input contains unsupported characters.
    pub fn parse(input: impl Into<String>) -> Result<Self, RoleCodeError> {
        let raw = input.into();
        if raw.is_empty() {
            return Err(RoleCodeError::Empty);
        }
        if !raw.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        }) {
            return Err(RoleCodeError::InvalidCharacters(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the validated role code string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RoleCode {
    type Error = RoleCodeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<RoleCode> for String {
    fn from(code: RoleCode) -> Self {
        code.0
    }
}

/// Role grant assigned to a staff account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoleGrant {
    /// Staff account that received the role.
    pub staff_id: StaffId,
    /// Granted role code.
    pub role_code: RoleCode,
    /// UTC timestamp when the role was granted.
    pub granted_at: DateTime<Utc>,
    /// Staff account that granted the role.
    pub granted_by: StaffId,
}
