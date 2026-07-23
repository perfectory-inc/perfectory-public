//! Current Gold data pointers for industrial-complex heavy detail artifacts.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, FileAssetId, SourceRecordId};
use foundation_shared_kernel::ObjectKey;
use serde::{Deserialize, Serialize};

/// Thin Lakehouse read model that points to R2/Iceberg Gold artifacts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplexGoldPointer {
    /// Industrial complex whose heavy detail is represented by this pointer.
    pub complex_id: ComplexId,
    /// Active Gold artifact version.
    pub current_version: String,
    /// Previously active Gold artifact version, when one existed.
    pub previous_version: Option<String>,
    /// File asset row for the Gold profile artifact.
    pub profile_file_asset_id: FileAssetId,
    /// Provider-neutral object key for the Gold profile artifact.
    pub profile_object_key: ObjectKey,
    /// File asset row for the optional spatial locator artifact.
    pub spatial_locator_file_asset_id: Option<FileAssetId>,
    /// Provider-neutral object key for the optional spatial locator artifact.
    pub spatial_locator_object_key: Option<ObjectKey>,
    /// Source record row that describes the publish input.
    pub source_record_id: SourceRecordId,
    /// Source snapshot represented by this Gold artifact.
    pub source_snapshot_id: String,
    /// Iceberg snapshot id represented by this Gold artifact.
    pub iceberg_snapshot_id: String,
    /// Number of profile rows represented by the artifact.
    pub profile_row_count: u64,
    /// SHA-256 checksum for the profile artifact.
    pub profile_checksum_sha256: String,
    /// UTC timestamp when the pointer was published.
    pub published_at: DateTime<Utc>,
    /// UTC timestamp when the pointer row was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

/// Provider-neutral domain event emitted when a Gold pointer is published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexGoldPointerPublished {
    /// Industrial complex whose pointer changed.
    pub complex_id: ComplexId,
    /// Newly active artifact version.
    pub current_version: String,
    /// Previously active artifact version.
    pub previous_version: Option<String>,
    /// Gold profile object key.
    pub profile_object_key: String,
    /// Optional spatial locator object key.
    pub spatial_locator_object_key: Option<String>,
    /// Source record that describes the publication input.
    pub source_record_id: SourceRecordId,
    /// Source snapshot represented by the artifact.
    pub source_snapshot_id: String,
    /// Iceberg snapshot represented by the artifact.
    pub iceberg_snapshot_id: String,
    /// Number of profile rows represented by the artifact.
    pub profile_row_count: u64,
    /// SHA-256 checksum of the profile artifact.
    pub profile_checksum_sha256: String,
    /// UTC publish time.
    pub published_at: DateTime<Utc>,
}

impl IndustrialComplexGoldPointer {
    /// Builds the outbox payload for publishing this pointer.
    #[must_use]
    pub fn published_event(&self) -> IndustrialComplexGoldPointerPublished {
        IndustrialComplexGoldPointerPublished {
            complex_id: self.complex_id,
            current_version: self.current_version.clone(),
            previous_version: self.previous_version.clone(),
            profile_object_key: self.profile_object_key.as_str().to_owned(),
            spatial_locator_object_key: self
                .spatial_locator_object_key
                .as_ref()
                .map(|key| key.as_str().to_owned()),
            source_record_id: self.source_record_id,
            source_snapshot_id: self.source_snapshot_id.clone(),
            iceberg_snapshot_id: self.iceberg_snapshot_id.clone(),
            profile_row_count: self.profile_row_count,
            profile_checksum_sha256: self.profile_checksum_sha256.clone(),
            published_at: self.published_at,
        }
    }
}
