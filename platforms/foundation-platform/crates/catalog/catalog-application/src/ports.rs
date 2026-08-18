//! Catalog application outbound ports.
//!
//! Read-only queries are separated from mutation unit-of-work operations. Mutations that change
//! canonical Catalog state and publish outbox events must happen in a single transaction.

use std::collections::BTreeMap;

use async_trait::async_trait;
use catalog_domain::{
    Blueprint, BuildEvidenceDigest, Building, CanonicalIcebergSnapshotId, CatalogError,
    CatalogMutationKind, ComplexAnchorSummary, ComplexMutation, ComplexNotice, DigitalTwinAsset,
    FileAsset, IndustrialComplex, IndustrialComplexKind, IndustrialComplexStatus, IndustryGroup,
    IndustryGroupMember, MarkerAnchorAlgorithm, MarkerTileRequest, Parcel,
    ParcelIndustryAssignment, ParcelKind, PmtilesChecksum, RequestFingerprint,
    RequestFingerprintBuilder, RuntimeTileLayer, RuntimeTileLineage, RuntimeTilesUrlTemplate,
    ServingGeneration, SpatialLayer, VectorTileBuildOutcome, VectorTileManifest,
    VectorTileRuntimeManifest,
};
use chrono::NaiveDate;
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, LakehouseComplexId, NoticeId, ParcelId, PostgisProjectionRevisionId,
    StaffId, VectorTileBuildJobId, VectorTileDataRevisionId, VectorTileReleaseId,
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

impl MarkTileLayerDynamicCommand {
    /// Digests everything that makes this request the request it is.
    ///
    /// The field list is written out rather than derived, so adding a field to the command above does
    /// not compile until someone decides whether it belongs to request identity. A derived encoding
    /// would answer that question silently, and answer it wrong the moment a field is reordered.
    ///
    /// Two membership decisions are worth stating because the intuitive answer is the wrong one.
    ///
    /// `idempotency_key` is **excluded**: it is the ledger's key, so hashing it would make every
    /// fingerprint trivially unique and the mismatch check dead code.
    ///
    /// `operator_staff_id` is **included**, even though it means a colleague re-sending the same body
    /// is refused. The alternative is worse: the activation is recorded against the first operator,
    /// so replaying that outcome to a second one reports a success they did not cause while the
    /// ledger names someone else as the actor. A refusal is annoying and visible; a falsified audit
    /// trail is neither.
    ///
    /// The `expected_*` pair is included for the same reason Stripe tells clients to mint a fresh key
    /// when they change the request: a retry made on *new* observations is a new decision, not the
    /// old one repeated.
    #[must_use]
    pub fn request_fingerprint(&self) -> RequestFingerprint {
        let mut builder = RequestFingerprintBuilder::new(CatalogMutationKind::MarkTileLayerDynamic)
            .text(&self.unit_key)
            .optional_displayed(self.expected_active_release_id.as_ref())
            .optional_integer(
                self.expected_serving_generation
                    .map(ServingGeneration::value),
            )
            .displayed(&self.data_revision)
            .text(self.canonical_iceberg_snapshot_id.as_str())
            .displayed(&self.postgis_projection_revision)
            .text(&self.martin_source_id)
            .text(self.tiles_url_template.as_str())
            .count(self.layers.len());
        // `BTreeMap` iteration is already sorted, and the count above means a shorter layer set can
        // never encode as the prefix of a longer one.
        for (layer_id, layer) in &self.layers {
            builder = builder
                .text(layer_id)
                .text(&layer.source_layer)
                .text(layer.feature_id_property.as_str())
                .integer(u64::from(layer.tile_min_zoom))
                .integer(u64::from(layer.tile_max_zoom))
                .integer(u64::from(layer.render_min_zoom))
                .integer(u64::from(layer.render_max_zoom))
                .count(layer.feature_filter_properties.len());
            for (property, value) in &layer.feature_filter_properties {
                builder = builder.text(property).text(value);
            }
        }
        builder = builder
            .displayed(&self.lineage.source_record_id)
            .count(self.lineage.source_file_asset_ids.len());
        // In order, deliberately not sorted: the order lands in a Postgres array column, so a
        // reordered lineage really is a different request.
        for file_asset_id in &self.lineage.source_file_asset_ids {
            builder = builder.displayed(file_asset_id);
        }
        builder.displayed(&self.operator_staff_id).finish()
    }
}

/// A publication command's answer, and whether this call produced it.
///
/// An enum rather than a `(manifest, replayed: bool)` pair because a caller has to handle the
/// difference: a replay returns the manifest the *first* call published, whose `manifest_generation`
/// may be older than the pointer's, and clients compare generations to decide what to refetch. This
/// repository already has the failure this prevents — `PgNormalizationUnitOfWork` returns a `created`
/// flag that the route discards, so a deduplicated submission is reported as a fresh one.
#[derive(Clone, Debug)]
pub enum PublishedRuntimeManifest {
    /// This call assembled and promoted the manifest.
    Published(VectorTileRuntimeManifest),
    /// A prior call with the same idempotency key published it; nothing was written now.
    Replayed(VectorTileRuntimeManifest),
}

impl PublishedRuntimeManifest {
    /// The manifest, for callers that genuinely do not distinguish the two cases.
    #[must_use]
    pub const fn manifest(&self) -> &VectorTileRuntimeManifest {
        match self {
            Self::Published(manifest) | Self::Replayed(manifest) => manifest,
        }
    }

    /// Consumes this outcome and returns the manifest.
    #[must_use]
    pub fn into_manifest(self) -> VectorTileRuntimeManifest {
        match self {
            Self::Published(manifest) | Self::Replayed(manifest) => manifest,
        }
    }

    /// Whether the ledger answered instead of the transaction.
    #[must_use]
    pub const fn was_replayed(&self) -> bool {
        matches!(self, Self::Replayed(_))
    }
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

/// Command for promoting a validated static build to the active serving source for one unit.
///
/// The layer set is absent on purpose. A static release replaces a dynamic one built from the same
/// data revision, so it must serve the same layers; the transaction copies them from the input
/// release, which makes "static serves different layers than dynamic" unrepresentable rather than
/// merely checked.
///
/// The Martin source name and the `PMTiles` object key are absent for the same kind of reason. Both
/// are derived from `unit_key` and `release_id` by
/// `catalog_domain::static_release_pmtiles_object_key`, so accepting them would only allow a caller
/// to disagree with the one definition.
#[derive(Clone, Debug)]
pub struct PromoteTileLayerStaticCommand {
    /// Publication unit being switched to its static release.
    pub unit_key: String,
    /// Validated build whose artifacts are being promoted.
    pub build_job_id: VectorTileBuildJobId,
    /// Release identity the artifact was uploaded under.
    ///
    /// Preallocated by the caller, as the immutable manifest's `file_asset` identity is: the object
    /// key embeds the release, so the id has to exist before the upload.
    pub release_id: VectorTileReleaseId,
    /// Release the caller observed as active.
    ///
    /// Not optional. The first publication of a unit is always dynamic, so a static promotion
    /// against a unit that has never published describes a state that cannot exist.
    pub expected_active_release_id: VectorTileReleaseId,
    /// Serving generation the caller observed. Not optional, for the same reason.
    pub expected_serving_generation: ServingGeneration,
    /// Release the build took as its input.
    pub input_release_id: VectorTileReleaseId,
    /// Immutable Iceberg snapshot the build froze.
    pub frozen_source_snapshot_id: CanonicalIcebergSnapshotId,
    /// Evidence the candidate artifacts were validated against.
    pub validation_evidence: BuildEvidenceDigest,
    /// Query-free tile URL template the Catalog pointer publishes.
    pub tiles_url_template: RuntimeTilesUrlTemplate,
    /// File asset row for the uploaded `PMTiles` object.
    pub pmtiles_file_asset_id: FileAssetId,
    /// Checksum of the uploaded `PMTiles` object bytes.
    pub pmtiles_sha256: PmtilesChecksum,
    /// Byte size of the uploaded `PMTiles` object. Zero is refused by the database.
    pub pmtiles_bytes: u64,
    /// Caller-chosen key that makes a retried promotion return the first outcome.
    pub idempotency_key: String,
    /// Staff operator that requested the promotion.
    pub operator_staff_id: StaffId,
}

/// Command for returning one publication unit to its preserved same-revision dynamic release.
///
/// There is no target release field. The unit records exactly one `fallback_release_id`, and the
/// database constrains its data revision to equal the active one, so "roll back to the recorded
/// fallback" cannot name a release from a different revision. A caller-supplied target would
/// reintroduce that possibility and turn a structural guarantee back into a check.
#[derive(Clone, Debug)]
pub struct RollbackTileLayerSourceCommand {
    /// Publication unit being returned to its fallback.
    pub unit_key: String,
    /// Release the caller observed as active.
    pub expected_active_release_id: VectorTileReleaseId,
    /// Serving generation the caller observed.
    pub expected_serving_generation: ServingGeneration,
    /// Operator-facing reason persisted for audit.
    pub reason: String,
    /// Caller-chosen key that makes a retried rollback return the first outcome.
    pub idempotency_key: String,
    /// Staff operator that requested the rollback.
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
    /// Lakehouse identifier of the same complex, or `None` when the writer has no snapshot.
    ///
    /// The value to store, like every other field on this command: `None` clears it. Only a loader
    /// reading a Gold snapshot can supply one.
    pub lakehouse_complex_id: Option<LakehouseComplexId>,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Domain-level industrial complex kind.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code shared by parcels that belong to the complex.
    ///
    /// This is the value to write, so `None` clears an existing code rather than leaving it in
    /// place. A caller whose source does not carry the column is saying the canonical row must not
    /// claim one either (root ADR-0040 decision 5).
    pub primary_bjdong_code: Option<String>,
    /// Official complex area in square meters.
    pub area_m2: u64,
    /// Lifecycle status to write, or `None` to record that the source stated none.
    ///
    /// The same rule as `primary_bjdong_code` governs every sourced field below: the command
    /// carries the value to store, so `None` clears whatever the row held. A loader whose snapshot
    /// dropped a value is saying the canonical row must stop claiming it.
    pub status: Option<IndustrialComplexStatus>,
    /// Province-level administrative code to write.
    pub sido_code: Option<String>,
    /// City/county/district administrative code to write.
    pub sigungu_code: Option<String>,
    /// Address text to write.
    pub address_text: Option<String>,
    /// Managing organization to write.
    pub management_agency_name: Option<String>,
    /// Developing organization to write.
    pub developer_name: Option<String>,
    /// Official designation date to write.
    pub designated_date: Option<NaiveDate>,
    /// Site-formation completion date to write.
    pub completion_date: Option<NaiveDate>,
}

/// What one upsert command did to the canonical table.
///
/// Reported by the write path rather than inferred afterwards from `version` or timestamps: a
/// loader that counts inserts and updates by guessing would keep reporting whatever its guess was.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpsertIndustrialComplexEffect {
    /// No row carried this official code, so one was created.
    Inserted,
    /// A row carried this official code and at least one canonical field changed.
    Updated,
    /// A row carried this official code and every canonical field already matched.
    Unchanged,
}

/// One command's result: the stored complex and what the write actually did.
#[derive(Clone, Debug)]
pub struct UpsertIndustrialComplexOutcome {
    /// Canonical complex as stored after the command.
    pub complex: IndustrialComplex,
    /// Effect the command had on the canonical table.
    pub effect: UpsertIndustrialComplexEffect,
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

    /// Lists buildings on one parcel identified by PNU.
    ///
    /// # Errors
    /// Returns `CatalogError` when repository access fails.
    async fn list_buildings_by_pnu(&self, pnu: &Pnu) -> Result<Vec<Building>, CatalogError>;

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
    ) -> Result<Vec<UpsertIndustrialComplexOutcome>, CatalogError>;

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

    /// Updates a parcel kind, writes the edit ledger row, and emits a race-free parcel kind
    /// changed event — all in one transaction.
    ///
    /// The ledger row is not optional bookkeeping. ADR-0006 rebuilds the serving projection from a
    /// snapshot plus the edit ledger, so an edit that lands in the row without landing in the
    /// ledger is an edit a rebuild loses (ADR-0023).
    ///
    /// # Errors
    /// Returns `CatalogError` when the parcel is missing, the expected version is stale,
    /// the new kind is invalid for the transition, or persistence fails.
    async fn update_parcel_kind(
        &self,
        id: ParcelId,
        expected_version: i64,
        new_kind: ParcelKind,
        applied_by: StaffId,
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
    /// A repeated `idempotency_key` carrying the same request returns the first outcome marked
    /// [`PublishedRuntimeManifest::Replayed`] and writes nothing — no release, no manifest, no second
    /// outbox event. The same key carrying a *different* request is refused: it is a key reuse, not a
    /// retry, and the earlier outcome does not answer it.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when the observed release or generation is stale, when the unit is
    /// absent, when the layer set is empty, when the idempotency key was already used for a different
    /// request, or when persistence and outbox writes fail.
    async fn mark_tile_layer_dynamic(
        &self,
        command: MarkTileLayerDynamicCommand,
    ) -> Result<PublishedRuntimeManifest, CatalogError> {
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

    /// Promotes a validated static build to the active serving source for one publication unit.
    ///
    /// One transaction writes the immutable static release and its layers copied from the input
    /// release, moves the unit's active pointer while preserving the previous dynamic release as the
    /// same-revision fallback, advances the serving and manifest generations, and records the
    /// additive v2 outbox event.
    ///
    /// A build whose input release stopped being active is recorded `superseded` rather than
    /// promoted — see `catalog_domain::validate_build_promotion`. The distinction matters because a
    /// conflict invites a retry and a supersession does not.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when the observed release or generation is stale, when the build is not
    /// validated, when its frozen snapshot does not match its input release, or when the writes fail.
    async fn promote_tile_layer_static(
        &self,
        command: PromoteTileLayerStaticCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        let _ = command;
        Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "static tile layer promotion is not implemented by this Catalog unit of work"
                .to_owned(),
        ))
    }

    /// Returns one publication unit to its preserved same-revision dynamic release.
    ///
    /// The data revision does not change: a rollback undoes a source switch, not a publication.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError` when the observed release or generation is stale, when the unit has no
    /// recorded fallback, or when the writes fail.
    async fn rollback_tile_layer_source(
        &self,
        command: RollbackTileLayerSourceCommand,
    ) -> Result<VectorTileRuntimeManifest, CatalogError> {
        let _ = command;
        Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "tile layer source rollback is not implemented by this Catalog unit of work".to_owned(),
        ))
    }
}
