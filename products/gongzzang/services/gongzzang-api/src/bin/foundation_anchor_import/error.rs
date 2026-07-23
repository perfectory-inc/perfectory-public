use thiserror::Error;

use super::foundation_anchor_import::AnchorImportError;

#[derive(Debug, Error)]
pub enum AnchorImporterError {
    #[error("{name} must be set")]
    MissingEnv { name: &'static str },
    #[error("failed to read {path}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("artifact file is not UTF-8 JSONL: {path}")]
    InvalidUtf8 {
        path: String,
        #[source]
        source: std::string::FromUtf8Error,
    },
    #[error("manifest object count mismatch: expected {expected}, got {actual}")]
    ObjectCountMismatch { expected: usize, actual: usize },
    #[error("foundation platform event inbox row was not found: event_id={event_id}")]
    InboxEventNotFound { event_id: String },
    #[error("foundation platform event inbox row is not pending_import: event_id={event_id}")]
    InboxEventNotPending { event_id: String },
    #[error(
        "foundation platform event inbox row is already locked for import: event_id={event_id}"
    )]
    InboxEventAlreadyLocked { event_id: String },
    #[error(
        "failed to release foundation platform event import advisory lock: event_id={event_id}"
    )]
    EventImportLockReleaseFailed { event_id: String },
    #[error("invalid foundation platform event payload field: {field}")]
    InvalidEventPayload { field: &'static str },
    #[error("invalid foundation platform anchor artifact object key: {object_key}")]
    InvalidArtifactObjectKey { object_key: String },
    #[error("FOUNDATION_PLATFORM_ANCHOR_IMPORT_BATCH_LIMIT must be between 1 and 100")]
    InvalidBatchLimit,
    #[error("pending inbox batch source cannot be loaded by a single import run")]
    BatchSourceInSingleRun,
    #[error("foundation platform anchor import batch failed for {failed_count} event(s)")]
    BatchImportFailed { failed_count: u64 },
    #[error("failed to build artifact HTTP client")]
    HttpClient {
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to fetch foundation platform artifact: {url}")]
    FetchArtifact {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("foundation platform artifact fetch failed: {url} returned {status}")]
    FetchArtifactStatus {
        url: String,
        status: reqwest::StatusCode,
    },
    #[error("foundation platform artifact fetch circuit failed: {url}: {error}")]
    FetchArtifactCircuit { url: String, error: String },
    #[error("manifest artifact row count is too large for this process")]
    ArtifactRowCountOverflow,
    #[error("manifest object byte size is too large for this process")]
    ArtifactObjectSizeOverflow,
    #[error("{label} artifact size mismatch for {object_key}: expected {expected}, got {actual}")]
    SizeMismatch {
        label: &'static str,
        object_key: String,
        expected: u64,
        actual: usize,
    },
    #[error("{label} artifact checksum mismatch")]
    ChecksumMismatch {
        label: &'static str,
        expected: String,
        actual: String,
    },
    #[error("manifest artifact row count mismatch: expected {expected}, got {actual}")]
    ArtifactRowCountMismatch { expected: u64, actual: usize },
    #[error("invalid RFC3339 timestamp")]
    Timestamp(#[from] chrono::ParseError),
    #[error(transparent)]
    Anchor(#[from] AnchorImportError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Repository(#[from] listing_domain::repository::RepoError),
}
