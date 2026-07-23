//! Shared kernel validation errors.

use thiserror::Error;

use crate::pnu::PnuError;

/// Error type for shared-kernel value object validation.
#[derive(Debug, Error)]
pub enum KernelError {
    /// PNU validation failed.
    #[error(transparent)]
    Pnu(#[from] PnuError),
}
