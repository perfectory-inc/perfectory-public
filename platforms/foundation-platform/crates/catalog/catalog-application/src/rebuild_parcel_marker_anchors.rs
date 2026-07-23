//! Use case for rebuilding PNU-backed parcel marker anchors.

use std::sync::Arc;

use catalog_domain::{CatalogError, MarkerAnchorAlgorithm};
use foundation_shared_kernel::ids::StaffId;

use crate::ports::{ParcelMarkerAnchorRebuildCommand, ParcelMarkerAnchorRebuildPort};

const PARCEL_BOUNDARY_SOURCE_TABLE: &str = "silver.parcel_boundaries";

/// Input required to rebuild parcel marker anchors from an approved mirror snapshot.
pub struct RebuildParcelMarkerAnchorsInput {
    /// Approved Iceberg snapshot represented by the `PostGIS` mirror rows.
    pub source_snapshot_id: String,
    /// Stable algorithm implementation version.
    pub algorithm_version: String,
    /// Staff operator that requested the rebuild, when invoked interactively.
    pub requested_by_staff_id: Option<StaffId>,
    /// Optional caller-supplied request id used for trace correlation.
    pub request_id: Option<String>,
}

/// Rebuilds parcel marker anchors through the Catalog application boundary.
pub struct RebuildParcelMarkerAnchors {
    rebuilder: Arc<dyn ParcelMarkerAnchorRebuildPort>,
}

impl RebuildParcelMarkerAnchors {
    /// Creates a use case instance backed by a rebuild port.
    #[must_use]
    pub fn new(rebuilder: Arc<dyn ParcelMarkerAnchorRebuildPort>) -> Self {
        Self { rebuilder }
    }

    /// Rebuilds active parcel marker anchors for one approved Iceberg snapshot.
    ///
    /// # Errors
    /// Returns `CatalogError::InvalidParcelMarkerAnchorRebuild` when input is not canonical,
    /// or a repository error when the rebuild cannot be persisted.
    pub async fn execute(
        &self,
        input: RebuildParcelMarkerAnchorsInput,
    ) -> Result<crate::ports::ParcelMarkerAnchorRebuildReport, CatalogError> {
        self.rebuilder
            .rebuild_parcel_marker_anchors(ParcelMarkerAnchorRebuildCommand {
                source_snapshot_id: normalize_snapshot_id(&input.source_snapshot_id)?,
                source_table: PARCEL_BOUNDARY_SOURCE_TABLE.to_owned(),
                algorithm: MarkerAnchorAlgorithm::Polylabel,
                algorithm_version: normalize_algorithm_version(&input.algorithm_version)?,
                requested_by_staff_id: input.requested_by_staff_id,
                request_id: normalize_optional_request_id(input.request_id)?,
            })
            .await
    }
}

fn normalize_snapshot_id(raw: &str) -> Result<String, CatalogError> {
    let value = raw.trim();
    if value.len() >= "iceberg:abc".len()
        && value.len() <= "iceberg:".len() + 128
        && value.starts_with("iceberg:")
        && value["iceberg:".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Ok(value.to_owned());
    }

    Err(CatalogError::InvalidParcelMarkerAnchorRebuild(
        "source_snapshot_id must use iceberg:<snapshot-id> format".to_owned(),
    ))
}

fn normalize_algorithm_version(raw: &str) -> Result<String, CatalogError> {
    let value = raw.trim();
    if value.len() >= 2
        && value.len() <= 128
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b':' | b'-')
        })
    {
        return Ok(value.to_owned());
    }

    Err(CatalogError::InvalidParcelMarkerAnchorRebuild(
        "algorithm_version must be a stable lowercase identifier".to_owned(),
    ))
}

fn normalize_optional_request_id(raw: Option<String>) -> Result<Option<String>, CatalogError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value = raw.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Ok(Some(value.to_owned()));
    }

    Err(CatalogError::InvalidParcelMarkerAnchorRebuild(
        "request_id must be a printable value no longer than 128 bytes".to_owned(),
    ))
}
