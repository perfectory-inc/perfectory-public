//! Lakehouse domain errors.

use thiserror::Error;

/// Error returned by Lakehouse domain and application operations.
#[derive(Debug, Error)]
pub enum LakehouseError {
    /// A Lakehouse contract failed validation.
    #[error("invalid lakehouse contract: {0}")]
    InvalidContract(String),

    /// A Lakehouse batch run failed validation.
    #[error("invalid lakehouse batch run summary: {0}")]
    InvalidLakehouseBatchRun(String),

    /// A Registry command failed validation.
    #[error("invalid lakehouse registry input: {0}")]
    InvalidLakehouseRegistryInput(String),

    /// The active Gold version differed from the caller expectation.
    #[error(
        "industrial-complex gold pointer version mismatch (expected_current_version={expected:?}, current={current:?})"
    )]
    IndustrialComplexGoldPointerVersionConflict {
        /// Active version expected by the caller.
        expected: Option<String>,
        /// Active version currently stored.
        current: Option<String>,
    },

    /// The canonical industrial complex required by a Lakehouse operation was not found.
    #[error("industrial complex not found (id={0})")]
    IndustrialComplexNotFound(String),

    /// A Lakehouse publication attempted to reuse an existing object key.
    #[error("file asset object key already exists ({0})")]
    ObjectKeyConflict(String),

    /// Persistence failed behind the Lakehouse boundary.
    #[error("lakehouse persistence error: {0}")]
    Persistence(String),

    /// An upstream adapter failed.
    #[error("lakehouse upstream error: {0}")]
    Upstream(String),
}
