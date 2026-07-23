//! Normalization capability errors.

use thiserror::Error;

/// Error returned by Normalization domain, application, and adapter operations.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum NormalizationError {
    /// Caller input or a flexible proposal payload failed validation.
    #[error("{0}")]
    InvalidInput(String),

    /// The requested proposal does not exist.
    #[error("normalization proposal not found")]
    ProposalNotFound,

    /// The requested application ledger row does not exist.
    #[error("normalization application not found")]
    ApplicationNotFound,

    /// The requested proposal lifecycle transition is not allowed.
    #[error("{0}")]
    InvalidState(String),

    /// The canonical target does not exist.
    #[error("normalization target not found: {0}")]
    TargetNotFound(String),

    /// The canonical target version differs from the operator's expected version.
    #[error("normalization target version mismatch (expected={expected}, current={current})")]
    TargetVersionConflict {
        /// Version supplied by the operator.
        expected: i64,
        /// Version currently stored on the canonical target.
        current: i64,
    },

    /// Canonical fields changed after the application selected for rollback.
    #[error("normalization target state changed after application: {0}")]
    TargetStateConflict(String),

    /// The canonical target is archived and cannot be changed.
    #[error("normalization target is archived: {0}")]
    TargetArchived(String),

    /// Persistence or adapter work failed. The detail is for internal diagnostics only.
    #[error("normalization persistence failed: {0}")]
    Persistence(String),
}
