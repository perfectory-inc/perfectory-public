//! Catalog domain errors.

use foundation_shared_kernel::pnu::PnuError;
use thiserror::Error;

/// Error type returned by Catalog domain and application operations.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Industrial complex was not found.
    #[error("industrial complex not found (id={0})")]
    ComplexNotFound(String),

    /// Industrial complex is already archived.
    #[error("industrial complex already archived (id={0})")]
    ComplexAlreadyArchived(String),

    /// Parcel was not found.
    #[error("parcel not found (id={0})")]
    ParcelNotFound(String),

    /// Vector tile manifest was not found.
    #[error("vector tile manifest not found ({0})")]
    VectorTileManifestNotFound(String),

    /// Rollback command failed domain validation.
    #[error("invalid vector tile manifest rollback: {0}")]
    InvalidVectorTileManifestRollback(String),

    /// Promote command failed domain validation.
    #[error("invalid vector tile manifest promotion: {0}")]
    InvalidVectorTileManifestPromotion(String),

    /// Industrial complex command failed domain validation.
    #[error("invalid industrial complex input: {0}")]
    InvalidIndustrialComplexInput(String),

    /// Parcel marker anchor rebuild command failed validation.
    #[error("invalid parcel marker anchor rebuild: {0}")]
    InvalidParcelMarkerAnchorRebuild(String),

    /// Vector tile manifest version already exists.
    #[error("vector tile manifest already exists ({0})")]
    VectorTileManifestAlreadyExists(String),

    /// File asset object key already exists.
    #[error("file asset object key already exists ({0})")]
    FileAssetObjectKeyConflict(String),

    /// Active vector tile manifest version differed from caller expectation.
    #[error(
        "vector tile manifest version mismatch (expected_current_version={expected}, current={current})"
    )]
    VectorTileManifestVersionConflict {
        /// Version expected by the caller.
        expected: String,
        /// Version that is currently active.
        current: String,
    },

    /// Industrial complex official source code already exists.
    #[error("industrial complex official source code already exists ({0})")]
    ComplexOfficialCodeConflict(String),

    /// Parcel PNU already exists.
    #[error("parcel PNU already exists (pnu={0})")]
    ParcelPnuConflict(String),

    /// Optimistic concurrency check failed.
    #[error("catalog version mismatch (expected_version={expected}, current={current})")]
    ComplexVersionConflict {
        /// Version expected by the caller.
        expected: i64,
        /// Version currently stored in Catalog.
        current: i64,
    },

    /// Canonical fields changed after the mutation selected for compensation.
    #[error("catalog state changed after normalization application (id={0})")]
    ComplexStateConflict(String),

    /// PNU validation failed.
    #[error("PNU validation failed")]
    InvalidPnu(#[from] PnuError),

    /// Infrastructure failure surfaced through the Catalog boundary.
    #[error("infrastructure error: {0}")]
    Infrastructure(String),
}
