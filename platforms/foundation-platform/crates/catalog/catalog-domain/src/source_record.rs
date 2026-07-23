//! Source lineage for imported Catalog facts.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::SourceRecordId;
use foundation_shared_kernel::ObjectKey;
use serde::{Deserialize, Serialize};

/// Source record describing where canonical Catalog data came from.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceRecord {
    /// Stable foundation-platform source record identifier.
    pub id: SourceRecordId,
    /// Source system or import pipeline name.
    pub source: String,
    /// Optional source URL.
    pub source_url: Option<String>,
    /// Optional identifier from the source system.
    pub external_id: Option<String>,
    /// UTC timestamp when the source data was captured.
    pub captured_at: DateTime<Utc>,
    /// Optional SHA-256 checksum of the source artifact.
    pub checksum_sha256: Option<String>,
    /// Optional provider-neutral object key for the raw source artifact.
    pub raw_object_key: Option<ObjectKey>,
    /// UTC timestamp when the source record was created.
    pub created_at: DateTime<Utc>,
}
