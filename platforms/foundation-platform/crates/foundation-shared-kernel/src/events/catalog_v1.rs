//! Catalog context domain events, version 1.
//!
//! Event payloads are append-only wire contracts. Structural changes should create a new event
//! version instead of mutating existing payload semantics.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{ComplexId, ParcelId, StaffId, VectorTileManifestId};
use crate::pnu::Pnu;

/// Union of Catalog events published through the foundation-platform outbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CatalogEvent {
    /// An industrial complex was registered in Catalog.
    #[serde(rename = "catalog.industrial_complex.created.v1")]
    IndustrialComplexCreated(IndustrialComplexCreatedV1),
    /// An industrial complex was registered with source-side official identity.
    #[serde(rename = "catalog.industrial_complex.created.v2")]
    IndustrialComplexCreatedV2(IndustrialComplexCreatedV2),
    /// Canonical industrial complex metadata changed.
    #[serde(rename = "catalog.industrial_complex.updated.v1")]
    IndustrialComplexUpdated(IndustrialComplexUpdatedV1),
    /// An industrial complex was archived without hard-deleting its lineage.
    #[serde(rename = "catalog.industrial_complex.archived.v1")]
    IndustrialComplexArchived(IndustrialComplexArchivedV1),
    /// The active industrial-complex Gold profile pointer changed.
    #[serde(rename = "catalog.industrial_complex.gold_pointer.published.v1")]
    IndustrialComplexGoldPointerPublished(IndustrialComplexGoldPointerPublishedV1),
    /// A parcel kind changed.
    #[serde(rename = "catalog.parcel.kind_changed.v1")]
    ParcelKindChanged(ParcelKindChangedV1),
    /// A parcel marker anchor artifact snapshot was published.
    #[serde(rename = "catalog.parcel_marker_anchor.snapshot.published.v1")]
    ParcelMarkerAnchorSnapshotPublished(ParcelMarkerAnchorSnapshotPublishedV1),
    /// The active vector tile manifest was rolled back to an existing version.
    #[serde(rename = "catalog.vector_tile_manifest.rolled_back.v1")]
    VectorTileManifestRolledBack(VectorTileManifestRolledBackV1),
    /// A vector tile manifest build was promoted to the active pointer.
    #[serde(rename = "catalog.vector_tile_manifest.promoted.v1")]
    VectorTileManifestPromoted(VectorTileManifestPromotedV1),
    /// Raw provider bytes for a collection job were durably written to Bronze (R2).
    ///
    /// Claim-Check notification (gongzzang ADR-0047): the raw payload stays in R2 Bronze;
    /// this event carries only the object pointer, content checksum, counts, and lineage.
    /// Emitted only on a successful (or reused) Bronze write — lifecycle status such as
    /// running/failed/empty belongs to the separate `collection.job_status` stream, not here.
    #[serde(rename = "catalog.collection.raw_written.v1")]
    CollectionRawWritten(CollectionRawWrittenV1),
}

/// Event emitted when an industrial complex is registered.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplexCreatedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Industrial complex that was created.
    pub complex_id: ComplexId,
    /// Human-readable industrial complex name.
    pub name: String,
    /// primary legal-dong code that identifies the complex scope.
    pub primary_bjdong_code: String,
    /// UTC timestamp when the complex was created.
    pub created_at: DateTime<Utc>,
}

/// Event emitted when an industrial complex is registered with source identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplexCreatedV2 {
    /// Payload schema version. Always `2` for this event type.
    pub schema_version: u32,
    /// Industrial complex that was created.
    pub complex_id: ComplexId,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// primary legal-dong code that identifies the complex scope.
    pub primary_bjdong_code: String,
    /// UTC timestamp when the complex was created.
    pub created_at: DateTime<Utc>,
}

/// Event emitted when canonical industrial complex metadata changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplexUpdatedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Industrial complex that was updated.
    pub complex_id: ComplexId,
    /// Changed field names for fine-grained consumer invalidation.
    pub changed_fields: Vec<String>,
    /// UTC timestamp when the complex was updated.
    pub updated_at: DateTime<Utc>,
}

/// Event emitted when an industrial complex is archived.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplexArchivedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Industrial complex that was archived.
    pub complex_id: ComplexId,
    /// Staff operator that requested the archive.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
    /// Human-readable archive reason.
    pub reason: Option<String>,
    /// UTC timestamp when the archive completed.
    pub archived_at: DateTime<Utc>,
}

/// Event emitted when an industrial-complex Gold profile pointer is published.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplexGoldPointerPublishedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Industrial complex whose active Gold pointer changed.
    pub complex_id: ComplexId,
    /// Newly active Gold artifact version.
    pub current_version: String,
    /// Previously active Gold artifact version, when one existed.
    pub previous_version: Option<String>,
    /// Provider-neutral object key for the Gold profile artifact.
    pub profile_object_key: String,
    /// Provider-neutral object key for the optional spatial locator artifact.
    pub spatial_locator_object_key: Option<String>,
    /// Source record row that describes the publish input.
    pub source_record_id: uuid::Uuid,
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
}

/// Event emitted when a parcel kind changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParcelKindChangedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Parcel that changed.
    pub parcel_id: ParcelId,
    /// Canonical PNU for the parcel.
    pub pnu: Pnu,
    /// Previous parcel kind wire value.
    pub previous_kind: String,
    /// New parcel kind wire value.
    pub new_kind: String,
    /// UTC timestamp when the kind changed.
    pub changed_at: DateTime<Utc>,
}

/// Event emitted when parcel marker anchor artifacts are published.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParcelMarkerAnchorSnapshotPublishedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Immutable anchor snapshot id for idempotent consumer imports.
    pub anchor_snapshot_id: String,
    /// Source geometry version represented by this artifact snapshot.
    pub source_geometry_version: String,
    /// Absolute URL for the anchor artifact manifest.
    pub artifact_manifest_url: String,
    /// SHA-256 checksum for the manifest-defined artifact snapshot.
    pub artifact_checksum_sha256: String,
    /// Accepted anchor row count represented by the artifact snapshot.
    pub row_count: u64,
    /// UTC timestamp when the snapshot was published.
    pub published_at: DateTime<Utc>,
}

/// Event emitted when the active vector tile manifest is rolled back.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorTileManifestRolledBackV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Manifest that became active after rollback.
    pub manifest_id: VectorTileManifestId,
    /// Manifest that had been active before rollback.
    pub previous_manifest_id: VectorTileManifestId,
    /// Active version after rollback.
    pub current_version: String,
    /// Version that was active before rollback.
    pub previous_version: String,
    /// Active version observed by the caller before rollback.
    pub expected_current_version: String,
    /// Staff operator that requested the rollback.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
    /// Human-readable rollback reason.
    pub rollback_reason: String,
    /// UTC timestamp when the rollback completed.
    pub rolled_back_at: DateTime<Utc>,
}

/// Event emitted when a vector tile manifest build is promoted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorTileManifestPromotedV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Manifest that became active after promote.
    pub manifest_id: VectorTileManifestId,
    /// Manifest that had been active before promote.
    pub previous_manifest_id: VectorTileManifestId,
    /// Active version after promote.
    pub current_version: String,
    /// Version that was active before promote.
    pub previous_version: String,
    /// Active version observed by the caller before promote.
    pub expected_current_version: String,
    /// Staff operator that requested the promote.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
    /// UTC timestamp when the promote completed.
    pub promoted_at: DateTime<Utc>,
}

/// Event emitted when a collection job's raw provider bytes are written to Bronze (R2).
///
/// This is the Claim-Check `raw_written` notification from gongzzang ADR-0047. The raw
/// payload is never carried here — it stays in R2 Bronze. Consumers receive a pointer
/// (`bronze_object_key`), an integrity digest (`bronze_checksum_sha256`), counts, and the
/// raw lineage required by ADR-0047 (source, endpoint slug, fetch time, license, SRID,
/// request count) to correlate and trace the write. `event_id` on the outbox envelope is
/// the consumer idempotency key (at-least-once).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CollectionRawWrittenV1 {
    /// Payload schema version. Always `1` for this event type.
    pub schema_version: u32,
    /// Immutable collection snapshot/run id this write belongs to.
    pub collection_snapshot_id: String,
    /// Collection job identity that produced the write.
    pub job_id: String,
    /// Logical collection-scope identity (e.g. `scope:legal-dong:1111010100`). Partition key.
    pub scope_unit_id: String,
    /// Data provider / source, for example `data.go.kr`.
    pub provider: String,
    /// Provider endpoint that was collected (operation name).
    pub endpoint: String,
    /// Endpoint slug (stable routing identity, e.g. `data-go-kr-building-register-getBrTitleInfo`).
    pub endpoint_slug: String,
    /// R2 Bronze object key — the Claim-Check pointer to the raw bytes. When
    /// `bronze_object_count > 1` this is the LAST object key of the write (the compact form;
    /// per-page keys are reconstructed from it), matching the ledger `last_object_key` semantics.
    pub bronze_object_key: String,
    /// Number of Bronze objects (pages/parts) this write produced (`1` for a single-object write).
    pub bronze_object_count: u64,
    /// Lowercase hex SHA-256 of the raw Bronze bytes (producer-computed; never trust `ETag`).
    pub bronze_checksum_sha256: String,
    /// Size in bytes of the raw Bronze object.
    pub bronze_size_bytes: u64,
    /// Logical source record count contained in the written object(s).
    pub source_record_count: u64,
    /// Number of provider requests consumed to produce this write (quota lineage).
    pub request_count: u64,
    /// Request fingerprint (lowercase hex SHA-256) used as the collection idempotency key.
    pub request_fingerprint_sha256: String,
    /// Schema version of the request fingerprint algorithm (e.g.
    /// `foundation-platform.bronze_request_fingerprint.v1`), so the hash is self-describing.
    pub request_fingerprint_schema_version: String,
    /// Data license/usage terms for the collected source, or `None` until a license is sourced.
    /// (The collection pipeline currently records no license for any provider — see `bronze_object`
    /// `license_name`/`license_url`, both `None` today — so this is honestly `None` rather than a
    /// fabricated value.)
    pub license: Option<String>,
    /// Spatial reference identifier (EPSG code, e.g. `EPSG:4326`) for spatial sources, or `None`
    /// for attribute-only sources. Required by the SRID-must-be-explicit rule for spatial data.
    pub srid: Option<String>,
    /// Whether the object was satisfied by reuse of an already-collected Bronze object
    /// (no fresh provider request) rather than a new write.
    pub reused_bronze_object: bool,
    /// UTC timestamp when the upstream provider data was fetched.
    pub fetched_at_utc: DateTime<Utc>,
    /// UTC timestamp when this event was emitted.
    pub occurred_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::{
        CatalogEvent, CollectionRawWrittenV1, IndustrialComplexArchivedV1,
        IndustrialComplexCreatedV1, IndustrialComplexCreatedV2,
        IndustrialComplexGoldPointerPublishedV1, ParcelMarkerAnchorSnapshotPublishedV1,
        VectorTileManifestPromotedV1, VectorTileManifestRolledBackV1,
    };
    use crate::ids::{ComplexId, StaffId, VectorTileManifestId};
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn round_trip_serializes_tag() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::IndustrialComplexCreated(IndustrialComplexCreatedV1 {
            schema_version: 1,
            complex_id: ComplexId::new(Uuid::nil()),
            name: "Synthetic Industrial Complex Alpha".into(),
            primary_bjdong_code: "2820000000".into(),
            created_at: Utc::now(),
        });
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("catalog.industrial_complex.created.v1"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn round_trip_serializes_industrial_complex_created_v2_identity(
    ) -> Result<(), serde_json::Error> {
        let event = CatalogEvent::IndustrialComplexCreatedV2(IndustrialComplexCreatedV2 {
            schema_version: 2,
            complex_id: ComplexId::new(Uuid::nil()),
            official_complex_code: "SYNTHETIC-COMPLEX-001".into(),
            name: "Synthetic Industrial Complex Alpha".into(),
            primary_bjdong_code: "2820000000".into(),
            created_at: Utc::now(),
        });
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("catalog.industrial_complex.created.v2"));
        assert!(json.contains("official_complex_code"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn vector_tile_manifest_rollback_serializes_tag() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::VectorTileManifestRolledBack(VectorTileManifestRolledBackV1 {
            schema_version: 1,
            manifest_id: VectorTileManifestId::new(Uuid::nil()),
            previous_manifest_id: VectorTileManifestId::new(Uuid::nil()),
            current_version: "0196e7e0-3c20-7000-8000-000000000041".into(),
            previous_version: "0196e7e0-3c20-7000-8000-000000000042".into(),
            expected_current_version: "0196e7e0-3c20-7000-8000-000000000042".into(),
            operator_staff_id: StaffId::new(Uuid::nil()),
            request_id: Some("req-1".into()),
            rollback_reason: "bad tile build".into(),
            rolled_back_at: Utc::now(),
        });
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("catalog.vector_tile_manifest.rolled_back.v1"));
        assert!(json.contains("operator_staff_id"));
        assert!(json.contains("request_id"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn vector_tile_manifest_promote_serializes_tag() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::VectorTileManifestPromoted(VectorTileManifestPromotedV1 {
            schema_version: 1,
            manifest_id: VectorTileManifestId::new(Uuid::nil()),
            previous_manifest_id: VectorTileManifestId::new(Uuid::nil()),
            current_version: "0196e7e0-3c20-7000-8000-000000000043".into(),
            previous_version: "0196e7e0-3c20-7000-8000-000000000042".into(),
            expected_current_version: "0196e7e0-3c20-7000-8000-000000000042".into(),
            operator_staff_id: StaffId::new(Uuid::nil()),
            request_id: Some("req-2".into()),
            promoted_at: Utc::now(),
        });
        let json = serde_json::to_string(&event)?;
        assert!(json.contains("catalog.vector_tile_manifest.promoted.v1"));
        assert!(json.contains("operator_staff_id"));
        assert!(json.contains("request_id"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn industrial_complex_gold_pointer_published_serializes_tag() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::IndustrialComplexGoldPointerPublished(
            IndustrialComplexGoldPointerPublishedV1 {
                schema_version: 1,
                complex_id: ComplexId::new(Uuid::nil()),
                current_version: "0196e7e0-3c20-7000-8000-100000000001".into(),
                previous_version: Some("gold-industrial-complex-profile-v0".into()),
                profile_object_key: "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json".into(),
                spatial_locator_object_key: Some(
                    "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet".into(),
                ),
                source_record_id: Uuid::nil(),
                source_snapshot_id: "bronze-industrial-complex-20260518".into(),
                iceberg_snapshot_id: "iceberg-snapshot-42".into(),
                profile_row_count: 1,
                profile_checksum_sha256:
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
                published_at: Utc::now(),
            },
        );

        let json = serde_json::to_string(&event)?;

        assert!(json.contains("catalog.industrial_complex.gold_pointer.published.v1"));
        assert!(json.contains("profile_object_key"));
        assert!(json.contains("iceberg_snapshot_id"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn industrial_complex_archived_serializes_audit_context() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::IndustrialComplexArchived(IndustrialComplexArchivedV1 {
            schema_version: 1,
            complex_id: ComplexId::new(Uuid::nil()),
            operator_staff_id: StaffId::new(Uuid::nil()),
            request_id: Some("archive-req-1".into()),
            reason: Some("duplicate source record".into()),
            archived_at: Utc::now(),
        });

        let json = serde_json::to_string(&event)?;

        assert!(json.contains("catalog.industrial_complex.archived.v1"));
        assert!(json.contains("operator_staff_id"));
        assert!(json.contains("archive-req-1"));
        assert!(json.contains("duplicate source record"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn parcel_marker_anchor_snapshot_published_serializes_tag() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::ParcelMarkerAnchorSnapshotPublished(
            ParcelMarkerAnchorSnapshotPublishedV1 {
                schema_version: 1,
                anchor_snapshot_id: "anchor-snapshot-018f0000-0000-7000-8000-000000000001"
                    .into(),
                source_geometry_version: "iceberg:parcel-boundaries-snapshot-001".into(),
                artifact_manifest_url:
                    "https://foundation-platform.example.com/gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json"
                        .into(),
                artifact_checksum_sha256:
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
                row_count: 39_862_470,
                published_at: Utc::now(),
            },
        );

        let json = serde_json::to_string(&event)?;

        assert!(json.contains("catalog.parcel_marker_anchor.snapshot.published.v1"));
        assert!(json.contains("artifact_manifest_url"));
        assert!(json.contains("artifact_checksum_sha256"));
        let _back: CatalogEvent = serde_json::from_str(&json)?;
        Ok(())
    }

    #[test]
    fn collection_raw_written_serializes_pointer_and_checksum() -> Result<(), serde_json::Error> {
        let event = CatalogEvent::CollectionRawWritten(CollectionRawWrittenV1 {
            schema_version: 1,
            collection_snapshot_id: "registry:2026-06-22".into(),
            job_id: "job-data-go-kr-building-register-0001".into(),
            scope_unit_id: "scope:legal-dong:1111010100".into(),
            provider: "data.go.kr".into(),
            endpoint: "getBrTitleInfo".into(),
            endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".into(),
            bronze_object_key:
                "bronze/source=datagokr__building_register_main/operation=getBrTitleInfo/sigungu=11680/bjdong=10100/page-000001.json"
                    .into(),
            bronze_object_count: 1,
            bronze_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            bronze_size_bytes: 4_096,
            source_record_count: 42,
            request_count: 1,
            request_fingerprint_sha256:
                "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".into(),
            request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
                .into(),
            license: None,
            srid: None,
            reused_bronze_object: false,
            fetched_at_utc: Utc::now(),
            occurred_at: Utc::now(),
        });

        let json = serde_json::to_string(&event)?;

        assert!(json.contains("catalog.collection.raw_written.v1"));
        assert!(json.contains("bronze_object_key"));
        assert!(json.contains("bronze_checksum_sha256"));
        assert!(json.contains("request_fingerprint_sha256"));
        assert!(json.contains("request_fingerprint_schema_version"));
        assert!(json.contains("fetched_at_utc"));
        assert!(json.contains("endpoint_slug"));
        let back: CatalogEvent = serde_json::from_str(&json)?;
        assert!(matches!(
            back,
            CatalogEvent::CollectionRawWritten(payload)
                if payload.source_record_count == 42
                    && payload.bronze_object_count == 1
                    && payload.srid.is_none()
                    && !payload.reused_bronze_object
        ));
        Ok(())
    }
}
