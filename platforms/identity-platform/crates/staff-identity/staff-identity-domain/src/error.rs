//! Staff identity error types.

use thiserror::Error;

/// Error returned by staff identity operations.
#[derive(Debug, Error)]
pub enum StaffIdentityError {
    /// Staff account was not found.
    #[error("staff not found (id={0})")]
    StaffNotFound(String),

    /// Identity-provider subject is already assigned to another staff account.
    #[error("duplicate identity provider subject")]
    DuplicateZitadelSubject,

    /// Staff session has expired.
    #[error("session expired")]
    SessionExpired,

    /// JWT ID has already been revoked.
    #[error("revoked JTI ({0})")]
    JtiRevoked(String),

    /// The requested verified session does not exist.
    #[error("staff session not found")]
    SessionNotFound,

    /// Identity-provider claims failed validation.
    #[error("invalid identity claims: {0}")]
    InvalidClaims(String),

    /// Infrastructure failure surfaced through the staff identity boundary.
    #[error("infrastructure error: {0}")]
    Infrastructure(String),
}
