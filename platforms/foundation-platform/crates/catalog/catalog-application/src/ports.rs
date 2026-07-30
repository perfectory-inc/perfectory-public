//! Catalog application outbound ports.
//!
//! Read-only queries are separated from mutation unit-of-work operations. Mutations that change
//! canonical Catalog state and publish outbox events must happen in a single transaction.

use std::collections::BTreeMap;

use async_trait::async_trait;
use catalog_domain::{
    Blueprint, Building, CanonicalIcebergSnapshotId, CatalogError, ComplexAnchorSummary,
    ComplexMutation, ComplexNotice, DigitalTwinAsset, FileAsset, IndustrialComplex,
    IndustrialComplexKind, IndustryGroup, IndustryGroupMember, Manufacturer, MarkerAnchorAlgorithm,
    MarkerTileRequest, Parcel, ParcelIndustryAssignment, ParcelKind, RuntimeTileLayer,
    RuntimeTileLineage, RuntimeTilesUrlTemplate, ServingGeneration, SpatialLayer,
    VectorTileBuildOutcome, VectorTileManifest, VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::{
    ComplexId, NoticeId, ParcelId, PostgisProjectionRevisionId, StaffId, VectorTileBuildJobId,
    VectorTileDataRevisionId, VectorTileReleaseId,
};
use foundation_shared_kernel::pnu::Pnu;
use uuid::Uuid;

/// Whether this deployment may publish the v2 runtime manifest.
///
/// The switch used to be read from the process environment inside the request handler, which made
/// it untestable without mutating global state and put a deployment decision in the middle of a
/// per-request code path. As an injected type it is read once at startup and passed down, so the
/// same decision cannot be answered differently by two call sites.
///
/// Absent or unrecognised means disabled. A publication capability that defaulted to on would
/// publish from any environment that forgot to say no.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeManifestPublicationCapability(bool);

impl RuntimeManifestPublicationCapability {
    /// Publication permitted.
    #[must_use]
    pub const fn enabled() -> Self {
        Self(true)
    }

    /// Publication refused.
    #[must_use]
    pub const fn disabled() -> Self {
        Self(false)
    }

    /// Whether publication is permitted.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        self.0
    }

    /// Interprets a deployment switch value.
    ///
    /// Accepts the truthy spellings the compose files and workflows already use. Anything else —
    /// including an unset variable, an empty string, or a typo — is disabled.
    #[must_use]
    pub fn from_env_value(value: Option<&str>) -> Self {
        match value.map(|raw| raw.trim().to_ascii_lowercase()) {
            Some(normalized) if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") => {
                Self::enabled()
            }
            _ => Self::disabled(),
        }
    }
}

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
    /// Immutable Gold/Iceberg snapshot that produced this tile build.
    pub source_snapshot_id: String,
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

/// Command for activating a complete dynamic `PostGIS` source for one publication unit.
///
/// This is the v2 publication entry point: every unit starts dynamic, and a static release may
/// only ever replace a dynamic one built from the same data revision. The expectations are carried
/// on the command rather than read inside the transaction so that two concurrent edits starting
/// from the same observed state cannot both win — the loser sees a version conflict instead of
/// silently overwriting.
///
/// The expectations are deliberately per-unit and there is no expected global manifest version.
/// A global check would be stricter than the invariant requires: two edits to *different* units
/// are not in conflict, and the guide requires both to commit with the global generation advancing
/// twice in order. The global manifest is therefore rebuilt from the locked pointer at commit time
/// rather than compared against a value the caller read earlier.
#[derive(Clone, Debug)]
pub struct MarkTileLayerDynamicCommand {
    /// Publication unit being switched, for example `parcels`.
    pub unit_key: String,
    /// Release the caller observed as active, or `None` for a first publication.
    ///
    /// `None` is a claim that the unit has never published, not an instruction to skip the check.
    /// The implementation refuses it when a release already exists.
    pub expected_active_release_id: Option<VectorTileReleaseId>,
    /// Serving generation the caller observed, or `None` for a first publication.
    ///
    /// Paired with `expected_active_release_id`: one present without the other describes a state
    /// that cannot exist, so the use case refuses the mixed form rather than guessing.
    ///
    /// It is not redundant with the release id. A same-data rollback re-activates a *preserved*
    /// dynamic release, so one release id can be the active one at two different generations; the
    /// release alone would not tell those two states apart.
    pub expected_serving_generation: Option<ServingGeneration>,
    /// Logical content revision this activation publishes.
    pub data_revision: VectorTileDataRevisionId,
    /// Immutable Iceberg snapshot the projection was built from.
    pub canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId,
    /// Complete `PostGIS` projection revision backing the dynamic source.
    pub postgis_projection_revision: PostgisProjectionRevisionId,
    /// Configured Martin source name.
    pub martin_source_id: String,
    /// Query-free tile URL template the Catalog pointer publishes.
    pub tiles_url_template: RuntimeTilesUrlTemplate,
    /// Complete layer set for this unit, keyed exactly as the published manifest keys it.
    ///
    /// An empty set is refused: a unit that serves no layer is not a publication. `cache_policy`
    /// is absent by design — a dynamic source is never browser-cacheable, so the use case writes
    /// the single permitted value instead of accepting one.
    pub layers: BTreeMap<String, RuntimeTileLayer>,
    /// Audit lineage for the projection input.
    pub lineage: RuntimeTileLineage,
    /// Caller-chosen key that makes a retried activation return the first outcome.
    pub idempotency_key: String,
    /// Staff operator that requested the activation.
    pub operator_staff_id: StaffId,
}

/// Command for recording a static build attempt against one publication unit.
///
/// Starting a build is a ledger write, not a side effect of running one: the row fixes which
/// release and which frozen snapshot the artifacts must correspond to, so a build that later
/// reports different inputs can be refused instead of trusted.
#[derive(Clone, Debug)]
pub struct StartVectorTileBuildCommand {
    /// Publication unit the build targets.
    pub unit_key: String,
    /// Release the build takes as its input.
    pub input_release_id: VectorTileReleaseId,
    /// Logical content revision that release represents.
    pub input_data_revision: VectorTileDataRevisionId,
    /// Immutable Iceberg snapshot the build freezes.
    ///
    /// The transaction checks it against the snapshot pinned on `input_release_id`. A build that
    /// claims a different snapshot from the release it names is either pointed at the wrong release
    /// or reporting the wrong snapshot, and afterwards the two cannot be told apart.
    pub frozen_source_snapshot_id: CanonicalIcebergSnapshotId,
    /// Caller-chosen key that makes a retried start return the first build rather than a second.
    pub idempotency_key: String,
    /// Staff operator that requested the build.
    pub operator_staff_id: StaffId,
}

/// Command for recording what one static build attempt produced.
#[derive(Clone, Debug)]
pub struct RecordVectorTileBuildResultCommand {
    /// Build being reported.
    pub build_job_id: VectorTileBuildJobId,
    /// What the attempt produced. Only `validated` and `failed` are expressible; see
    /// [`VectorTileBuildOutcome`].
    pub outcome: VectorTileBuildOutcome,
    /// Staff operator that reported the result.
    pub operator_staff_id: StaffId,
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

    /// Loads the active strict v2 single-source runtime manifest, if enabled and published.
    ///
    /// # Errors
    /// Returns `CatalogError` when the normalized publication ledger is inconsistent or unreadable.
    async fn get_active_vector_tile_runtime_manifest(
        &self,
    ) -> Result<Option<VectorTileRuntimeManifest>, CatalogError>;

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

    /// Compare-and-swap the normalized v2 runtime manifest pointer.
    ///
    /// The SQL function enforces completeness and monotonic generation in the same transaction
    /// that changes the singleton pointer. Implementations that do not own the v2 ledger keep the
    /// default error so existing test doubles cannot accidentally pretend to publish it.
    async fn promote_vector_tile_runtime_manifest(
        &self,
        expected_manifest_id: Option<Uuid>,
        next_manifest_id: Uuid,
    ) -> Result<u64, CatalogError> {
        let _ = (expected_manifest_id, next_manifest_id);
        Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "runtime manifest promotion is not implemented by this Catalog unit of work".to_owned(),
        ))
    }

    /// Activates a complete dynamic `PostGIS` source for one publication unit.
    ///
    /// One transaction writes the projection rows, the immutable dynamic release, the unit's active
    /// pointer and serving generation, the new global manifest and its generation, and the additive
    /// v2 outbox event. A partial activation is never visible: if any write fails the pointer does
    /// not move.
    ///
    /// The default is an error for the same reason the runtime-manifest promotion above is: a test
    /// double that inherited a silent success would report a publication that never happened.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when the observed release or generation is stale, when the unit is
    /// absent, when the layer set is empty, or when persistence and outbox writes fail.
    async fn mark_tile_layer_dynamic(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        let _ = command;
        Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "dynamic tile layer activation is not implemented by this Catalog unit of work"
                .to_owned(),
        ))
    }

    /// Records a static build attempt and returns its ledger identity.
    ///
    /// The transaction refuses a build whose claimed frozen snapshot is not the one pinned on its
    /// input release, and returns the existing build for a repeated idempotency key rather than
    /// opening a second attempt against the same inputs.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when the unit or release is absent, when the claimed snapshot does not
    /// match the release, or when the write fails.
    async fn start_vector_tile_build(
        &self,
        command: StartVectorTileBuildCommand,
    ) -> Result<VectorTileBuildJobId, CatalogError> {
        let _ = command;
        Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "static build start is not implemented by this Catalog unit of work".to_owned(),
        ))
    }

    /// Records what a static build attempt produced.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when the build is absent, when it has already reached a terminal
    /// status, or when the write fails.
    async fn record_vector_tile_build_result(
        &self,
        command: RecordVectorTileBuildResultCommand,
    ) -> Result<(), CatalogError> {
        let _ = command;
        Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "static build result recording is not implemented by this Catalog unit of work"
                .to_owned(),
        ))
    }
}
