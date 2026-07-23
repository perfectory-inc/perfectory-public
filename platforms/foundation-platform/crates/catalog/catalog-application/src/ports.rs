//! Catalog application outbound ports.
//!
//! Read-only queries are separated from mutation unit-of-work operations. Mutations that change
//! canonical Catalog state and publish outbox events must happen in a single transaction.

use std::collections::BTreeMap;

use async_trait::async_trait;
use catalog_domain::{
    Blueprint, Building, CatalogError, ComplexAnchorSummary, ComplexMutation, ComplexNotice,
    DigitalTwinAsset, FileAsset, IndustrialComplex, IndustrialComplexKind, IndustryGroup,
    IndustryGroupMember, Manufacturer, MarkerAnchorAlgorithm, MarkerTileRequest, Parcel,
    ParcelIndustryAssignment, ParcelKind, SpatialLayer, VectorTileManifest,
};
use foundation_shared_kernel::ids::{ComplexId, NoticeId, ParcelId, StaffId};
use foundation_shared_kernel::pnu::Pnu;
use uuid::Uuid;

/// Command for switching the active vector tile manifest to a previous immutable version.
#[derive(Clone, Debug)]
pub struct VectorTileManifestRollbackCommand {
    /// Version that should become active after rollback.
    pub to_version: String,
    /// Active version observed by the caller before rollback.
    pub expected_current_version: String,
    /// Human-readable rollback reason persisted for audit.
    pub reason: String,
    /// Staff operator that requested the rollback.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
}

/// Command for promoting a validated vector tile build to the active manifest pointer.
#[derive(Clone, Debug)]
pub struct VectorTileManifestPromotionCommand {
    /// Version that should become active after promote.
    pub current_version: String,
    /// Active version observed by the caller before promote.
    pub expected_current_version: String,
    /// URL template clients use to request vector tiles.
    pub tiles_url_template: String,
    /// Source record describing the build input.
    pub source_record: VectorTileSourceRecordCommand,
    /// File asset metadata for the manifest JSON artifact.
    pub manifest_file_asset: VectorTileFileAssetCommand,
    /// Layer artifacts keyed by logical layer name.
    pub artifacts: BTreeMap<String, VectorTileArtifactPromotionCommand>,
    /// Staff operator that requested the promote.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
}

/// Source record command embedded in vector tile promote operations.
#[derive(Clone, Debug)]
pub struct VectorTileSourceRecordCommand {
    /// Source system or pipeline name.
    pub source: String,
    /// Optional source URL for traceability.
    pub source_url: Option<String>,
    /// Optional source-side identifier.
    pub external_id: Option<String>,
    /// Optional SHA-256 checksum in lowercase hexadecimal form.
    pub checksum_sha256: Option<String>,
    /// Optional provider-neutral object key for the raw source artifact.
    pub raw_object_key: Option<String>,
}

/// File asset command embedded in vector tile promote operations.
#[derive(Clone, Debug)]
pub struct VectorTileFileAssetCommand {
    /// Provider-neutral object storage key.
    pub object_key: String,
    /// MIME type recorded for the object.
    pub mime_type: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional SHA-256 checksum in lowercase hexadecimal form.
    pub checksum_sha256: Option<String>,
    /// Optional display title for operators.
    pub title: Option<String>,
    /// Visibility wire value: `public`, `internal`, or `private`.
    pub visibility: String,
}

/// Per-layer artifact command embedded in vector tile promote operations.
#[derive(Clone, Debug)]
pub struct VectorTileArtifactPromotionCommand {
    /// Source layer name embedded in the vector tile payload.
    pub source_layer: String,
    /// Minimum zoom level available in stored tiles.
    pub tile_min_zoom: u8,
    /// Maximum zoom level available in stored tiles.
    pub tile_max_zoom: u8,
    /// Minimum zoom level where clients should render the layer.
    pub render_min_zoom: u8,
    /// Maximum zoom level where clients should render the layer.
    pub render_max_zoom: u8,
    /// File asset metadata for the layer `TileJSON` document.
    pub tilejson_file_asset: VectorTileFileAssetCommand,
    /// Provider-neutral prefix that contains the layer's tile objects.
    pub object_key_prefix: String,
    /// Number of flat tile objects generated for this layer.
    pub flat_tile_count: u64,
    /// Total bytes across flat tile objects for this layer.
    pub flat_tile_total_bytes: u64,
    /// Source file assets used to build this layer.
    pub source_file_assets: Vec<VectorTileFileAssetCommand>,
}

/// Command for rebuilding PNU-backed parcel marker anchors from the approved `PostGIS` mirror.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParcelMarkerAnchorRebuildCommand {
    /// Approved Iceberg snapshot represented by the `PostGIS` mirror rows.
    pub source_snapshot_id: String,
    /// Canonical source table represented by the mirror rows.
    pub source_table: String,
    /// Anchor derivation algorithm.
    pub algorithm: MarkerAnchorAlgorithm,
    /// Stable algorithm implementation version.
    pub algorithm_version: String,
    /// Staff operator that requested the rebuild, when invoked interactively.
    pub requested_by_staff_id: Option<StaffId>,
    /// Optional caller-supplied request id used for trace correlation.
    pub request_id: Option<String>,
}

/// Result of a parcel marker anchor rebuild.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParcelMarkerAnchorRebuildReport {
    /// Generation run persisted for traceability.
    pub generation_run_id: Uuid,
    /// Approved Iceberg snapshot represented by the rebuild.
    pub source_snapshot_id: String,
    /// Canonical source table represented by the rebuild.
    pub source_table: String,
    /// Anchor derivation algorithm.
    pub algorithm: MarkerAnchorAlgorithm,
    /// Stable algorithm implementation version.
    pub algorithm_version: String,
    /// Mirror rows inspected for this snapshot.
    pub scanned_row_count: u64,
    /// Anchor rows inserted or updated.
    pub loaded_row_count: u64,
    /// Mirror rows rejected before writing anchors.
    pub rejected_row_count: u64,
    /// Previously active anchors superseded for the same PNUs.
    pub superseded_row_count: u64,
}

/// Mutation port for rebuilding derived parcel marker anchors.
#[async_trait]
pub trait ParcelMarkerAnchorRebuildPort: Send + Sync {
    /// Rebuilds active parcel marker anchors from serving `PostGIS` mirror rows.
    ///
    /// # Errors
    /// Returns `CatalogError` when source mirror validation or persistence fails.
    async fn rebuild_parcel_marker_anchors(
        &self,
        command: ParcelMarkerAnchorRebuildCommand,
    ) -> Result<ParcelMarkerAnchorRebuildReport, CatalogError>;
}

/// Command for creating or updating a Catalog industrial complex by official source code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpsertIndustrialComplexCommand {
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Domain-level industrial complex kind.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code shared by parcels that belong to the complex.
    pub primary_bjdong_code: String,
    /// Official complex area in square meters.
    pub area_m2: u64,
}

/// Read-only Catalog queries.
#[async_trait]
pub trait CatalogRepository: Send + Sync {
    /// Lists canonical industrial complexes in stable Catalog order.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_complexes(&self) -> Result<Vec<IndustrialComplex>, CatalogError>;

    /// Finds an industrial complex by id.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn find_complex(&self, id: ComplexId) -> Result<Option<IndustrialComplex>, CatalogError>;

    /// Summarizes active PNU marker anchors for one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails or stored anchor data is invalid.
    async fn find_complex_anchor_summary(
        &self,
        complex_id: ComplexId,
    ) -> Result<Option<ComplexAnchorSummary>, CatalogError>;

    /// Finds a parcel by id.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn find_parcel_by_id(&self, id: ParcelId) -> Result<Option<Parcel>, CatalogError>;

    /// Finds a parcel by PNU.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn find_parcel_by_pnu(&self, pnu: &Pnu) -> Result<Option<Parcel>, CatalogError>;

    /// Lists parcels that belong to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_parcels_by_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Parcel>, CatalogError>;

    /// Lists buildings on parcels that belong to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_buildings_by_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Building>, CatalogError>;

    /// Lists buildings on one parcel identified by PNU.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_buildings_by_pnu(&self, pnu: &Pnu) -> Result<Vec<Building>, CatalogError>;

    /// Lists manufacturers on parcels that belong to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_manufacturers_by_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Manufacturer>, CatalogError>;

    /// Lists notices attached to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_complex_notices(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<ComplexNotice>, CatalogError>;

    /// Lists file assets attached to one notice.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_notice_file_assets(
        &self,
        notice_id: NoticeId,
    ) -> Result<Vec<FileAsset>, CatalogError>;

    /// Lists file assets attached directly to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_complex_attachments(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<FileAsset>, CatalogError>;

    /// Lists blueprints assigned to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_complex_blueprints(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Blueprint>, CatalogError>;

    /// Lists spatial layers assigned to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_complex_spatial_layers(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<SpatialLayer>, CatalogError>;

    /// Lists digital twin assets assigned to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_complex_digital_twin_assets(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<DigitalTwinAsset>, CatalogError>;

    /// Lists industry groups, optionally restricted to one industrial complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_industry_groups(
        &self,
        complex_id: Option<ComplexId>,
    ) -> Result<Vec<IndustryGroup>, CatalogError>;

    /// Lists industry code members for all groups owned by one complex.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_industry_group_members_for_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<IndustryGroupMember>, CatalogError>;

    /// Lists industry assignments for one parcel.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_parcel_industry_assignments(
        &self,
        parcel_id: ParcelId,
    ) -> Result<Vec<ParcelIndustryAssignment>, CatalogError>;

    /// Loads the currently active vector tile manifest, if one exists.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn get_active_vector_tile_manifest(
        &self,
    ) -> Result<Option<VectorTileManifest>, CatalogError>;

    /// Renders a validated marker tile request as MVT/PBF bytes.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access or `PostGIS` tile encoding fails.
    async fn get_marker_tile(&self, request: MarkerTileRequest) -> Result<Vec<u8>, CatalogError>;
}

/// Mutation boundary that writes Catalog state and matching outbox events atomically.
#[async_trait]
pub trait CatalogUnitOfWork: Send + Sync {
    /// Creates a new industrial complex and emits its creation event in the same transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when uniqueness checks, persistence, or outbox writes fail.
    async fn create_complex(&self, complex: &IndustrialComplex) -> Result<(), CatalogError>;

    /// Creates or updates industrial complexes by source-side official code in one transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when uniqueness checks, persistence, or outbox writes fail.
    async fn upsert_complexes_by_official_code(
        &self,
        commands: &[UpsertIndustrialComplexCommand],
    ) -> Result<Vec<IndustrialComplex>, CatalogError>;

    /// Updates an industrial complex and emits its update event in the same transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when the expected version is stale or persistence fails.
    async fn update_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        mutate: ComplexMutation,
    ) -> Result<IndustrialComplex, CatalogError>;

    /// Archives an industrial complex and emits its archive event in the same transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when the complex is missing, already archived, the expected version
    /// is stale, or persistence fails.
    async fn archive_complex(
        &self,
        id: ComplexId,
        expected_version: i64,
        operator_staff_id: StaffId,
        reason: Option<String>,
        request_id: Option<String>,
    ) -> Result<IndustrialComplex, CatalogError>;

    /// Updates a parcel kind and emits a race-free parcel kind changed event.
    ///
    /// # Errors
    /// Returns `CatalogError` when the parcel is missing, the expected version is stale,
    /// the new kind is invalid for the transition, or persistence fails.
    async fn update_parcel_kind(
        &self,
        id: ParcelId,
        expected_version: i64,
        new_kind: ParcelKind,
    ) -> Result<Parcel, CatalogError>;

    /// Switches the active vector tile manifest pointer to an existing immutable version.
    ///
    /// # Errors
    /// Returns `CatalogError` when the active or target manifest is missing, the expected
    /// version is stale, or persistence/outbox writes fail.
    async fn rollback_vector_tile_manifest(
        &self,
        command: VectorTileManifestRollbackCommand,
    ) -> Result<VectorTileManifest, CatalogError>;

    /// Registers vector tile lineage and promotes the new manifest in one transaction.
    ///
    /// # Errors
    /// Returns `CatalogError` when source/file/artifact rows cannot be written, the expected
    /// active version is stale, or the outbox event cannot be recorded.
    async fn promote_vector_tile_manifest(
        &self,
        command: VectorTileManifestPromotionCommand,
    ) -> Result<VectorTileManifest, CatalogError>;
}
