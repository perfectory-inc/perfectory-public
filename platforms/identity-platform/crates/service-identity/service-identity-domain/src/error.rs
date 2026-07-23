//! Service identity error types.

use thiserror::Error;

/// Error returned by service identity operations.
#[derive(Debug, Error)]
pub enum ServiceIdentityError {
    /// The supplied service credential failed verification.
    #[error("invalid service credential")]
    InvalidCredential,
    /// Infrastructure or audit persistence failed.
    #[error("service identity infrastructure error: {0}")]
    Infrastructure(String),
}
