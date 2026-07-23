//! Catalog context HTTP DTOs.
//!
//! 이 모듈의 타입은 산업단지 Catalog의 공개 wire contract입니다. 값 검증과 도메인 규칙은
//! application/domain crate에서 수행하고, 이 모듈은 JSON 필드명과 외부 계약을 안정적으로
//! 표현하는 데 집중합니다.

use std::collections::BTreeMap;

use catalog_domain::{
    ALL_ACTIVE_MARKER_FILTER_HASH, PARCEL_ANCHOR_MARKER_TILE_LAYER,
    PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

/// Request body for registering an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterComplexRequest {
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Industrial complex kind. Wire values: `national`, `general`, `agricultural`, `urban_high_tech`.
    pub kind: String,
    /// primary legal-dong code shared by parcels that belong to the complex.
    pub primary_bjdong_code: String,
    /// Official complex area in square meters.
    pub area_m2: u64,
}

/// Request body for updating canonical industrial-complex metadata with optimistic concurrency.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateComplexRequest {
    /// Optional replacement human-readable industrial complex name.
    pub name: Option<String>,
    /// Optional replacement official complex area in square meters.
    pub area_m2: Option<u64>,
    /// Version observed by the caller; stale values return conflict.
    pub if_match_version: i64,
}

/// Request body for archiving an industrial complex with optimistic concurrency.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ArchiveComplexRequest {
    /// Version observed by the caller; stale values return conflict.
    pub if_match_version: i64,
    /// Optional human-readable archive reason for audit.
    pub reason: Option<String>,
}

/// Canonical industrial complex response owned by foundation-platform Catalog.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct IndustrialComplexResponse {
    /// Stable foundation-platform identifier for the complex.
    pub id: Uuid,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Industrial complex kind as a public wire value.
    pub kind: String,
    /// primary legal-dong code shared by parcels that belong to the complex.
    pub primary_bjdong_code: String,
    /// Official complex area in square meters.
    pub area_m2: u64,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
    /// UTC timestamp when the complex was archived, if no longer active.
    pub archived_at: Option<DateTime<Utc>>,
    /// Current R2/Iceberg Gold pointer for heavy industrial-complex detail.
    pub gold_pointer: Option<IndustrialComplexGoldPointerResponse>,
}

/// Thin pointer to R2/Iceberg Gold industrial-complex data.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct IndustrialComplexGoldPointerResponse {
    /// Active Gold artifact version.
    pub current_version: String,
    /// Previously active Gold artifact version, when one existed.
    pub previous_version: Option<String>,
    /// Provider-neutral object key for the Gold profile artifact.
    pub profile_object_key: String,
    /// Provider-neutral object key for the optional spatial locator artifact.
    pub spatial_locator_object_key: Option<String>,
    /// Source record that describes the publish input.
    pub source_record_id: Uuid,
    /// Source snapshot represented by the artifact.
    pub source_snapshot_id: String,
    /// Iceberg snapshot id represented by the artifact.
    pub iceberg_snapshot_id: String,
    /// Number of profile rows represented by the artifact.
    pub profile_row_count: u64,
    /// SHA-256 checksum for the profile artifact.
    pub profile_checksum_sha256: String,
    /// UTC timestamp when the pointer was published.
    pub published_at: DateTime<Utc>,
}

/// Canonical parcel response for a parcel inside an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ParcelResponse {
    /// Stable foundation-platform identifier for the parcel.
    pub id: Uuid,
    /// Industrial complex that owns this parcel.
    pub complex_id: Uuid,
    /// Parcel Number Unit identifier.
    #[schema(
        min_length = 19,
        max_length = 19,
        pattern = "^[0-9]{10}[1289][0-9]{8}$"
    )]
    pub pnu: String,
    /// Parcel kind. Wire values: `factory`, `support`, `public`, `river`, `other`.
    pub kind: String,
    /// Official parcel area in square meters.
    pub area_m2: u64,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Canonical building response for buildings assigned to industrial-complex parcels.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct BuildingResponse {
    /// Stable foundation-platform identifier for the building.
    pub id: Uuid,
    /// Parcel that owns this building.
    pub parcel_id: Uuid,
    /// Source building purpose code.
    pub purpose_code: String,
    /// Source building structure code.
    pub structure_code: String,
    /// Official floor area in square meters.
    pub floor_area_m2: f64,
    /// Number of above-ground stories.
    pub stories: i16,
    /// Number of below-ground (basement) floors. `0` when none or unknown.
    pub below_ground_floors: i16,
    /// Whether the building has a rooftop (옥탑) structure counted as a floor.
    pub has_rooftop: bool,
    /// 옥탑 공용부 allocated area (㎡) reconciled from 전유공용면적. `null` when the
    /// building has no rooftop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rooftop_area_m2: Option<f64>,
    /// 옥탑 용도 (주용도 · 기타용도) reconciled from 전유공용면적. Empty when the
    /// building has no rooftop.
    pub rooftop_usage: String,
    /// Construction year from the official source.
    pub built_year: i32,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Canonical 전유부 호 (building unit) response for a parcel.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UnitResponse {
    /// Stable foundation-platform identifier for the unit.
    pub id: Uuid,
    /// Parcel that owns this unit.
    pub parcel_id: Uuid,
    /// 건물명 (normalized building name, may be empty).
    pub building_name: String,
    /// 동명칭 — only real 동 numbers; empty otherwise.
    pub dong_name: String,
    /// 호명칭.
    pub ho_name: String,
    /// Floor label (지상/지하 + number), free text from source.
    pub floor_label: String,
    /// 전유면적 (exclusive area, m²), reconciled from 전유공용면적. `null` when unmatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_area_m2: Option<f64>,
    /// 주용도명, reconciled from 전유공용면적. Empty when unmatched.
    pub usage_name: String,
    /// 구조명, reconciled from 전유공용면적. Empty when unmatched.
    pub structure_name: String,
}

/// Manufacturer read response that deliberately omits sensitive business identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ManufacturerResponse {
    /// Stable foundation-platform identifier for the manufacturer.
    pub id: Uuid,
    /// Primary parcel occupied by the manufacturer.
    pub primary_parcel_id: Uuid,
    /// Manufacturer display name.
    pub name: String,
    /// Korean Standard Industrial Classification code.
    pub ksic_code: String,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Provider-neutral file metadata owned by foundation-platform Catalog.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct FileAssetResponse {
    /// Stable foundation-platform identifier for the file asset.
    pub id: Uuid,
    /// Provider-neutral object storage key. This intentionally avoids S3/R2-specific naming.
    pub object_key: String,
    /// MIME type recorded for the object.
    pub mime_type: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional SHA-256 checksum in lowercase hexadecimal form.
    pub checksum_sha256: Option<String>,
    /// Optional display title for UI surfaces.
    pub title: Option<String>,
    /// File visibility. Wire values: `public`, `internal`, `private`.
    pub visibility: String,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Official notice or announcement attached to an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ComplexNoticeResponse {
    /// Stable foundation-platform identifier for the notice.
    pub id: Uuid,
    /// Industrial complex that owns this notice.
    pub complex_id: Uuid,
    /// Notice type. Wire values: `notice`, `announcement`, `sale`, `regulation`, `maintenance`, `other`.
    pub notice_type: String,
    /// Notice title.
    pub title: String,
    /// Optional short summary prepared for list views.
    pub summary: Option<String>,
    /// Publication timestamp when the source provides one.
    pub published_at: Option<DateTime<Utc>>,
    /// File attachments linked to the notice.
    pub attachments: Vec<FileAssetResponse>,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Blueprint or drawing metadata assigned to an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct BlueprintResponse {
    /// Stable foundation-platform identifier for the blueprint.
    pub id: Uuid,
    /// Industrial complex that owns this blueprint.
    pub complex_id: Uuid,
    /// File asset that stores the blueprint object.
    pub file_asset_id: Uuid,
    /// Blueprint kind. Wire values: `master_plan`, `parcel_map`, `utility_plan`, `floor_plan`, `other`.
    pub blueprint_kind: String,
    /// Coordinate reference system used by the drawing.
    pub coordinate_system: String,
    /// Optional scale label from the source drawing.
    pub scale: Option<String>,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Geospatial layer metadata assigned below an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SpatialLayerResponse {
    /// Stable foundation-platform identifier for the spatial layer.
    pub id: Uuid,
    /// Industrial complex that owns this layer.
    pub complex_id: Uuid,
    /// Optional parcel that narrows the layer scope.
    pub parcel_id: Option<Uuid>,
    /// Optional blueprint that the layer overlays.
    pub blueprint_id: Option<Uuid>,
    /// Layer kind. Wire values: `complex_boundary`, `parcel_boundary`, `zone`, `road`, `utility`, `blueprint_overlay`, `other`.
    pub layer_kind: String,
    /// Optional object key for the geometry artifact.
    pub geometry_object_key: Option<String>,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Digital twin asset metadata assigned below an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DigitalTwinAssetResponse {
    /// Stable foundation-platform identifier for the digital twin asset.
    pub id: Uuid,
    /// Industrial complex that owns this asset.
    pub complex_id: Uuid,
    /// Optional parcel that narrows the asset scope.
    pub parcel_id: Option<Uuid>,
    /// Optional building identifier when the asset represents a building.
    pub building_id: Option<Uuid>,
    /// File asset that stores the 3D or visualization artifact.
    pub file_asset_id: Uuid,
    /// Asset kind. Wire values: `model_3d`, `tileset_3d`, `point_cloud`, `panorama`, `other`.
    pub asset_kind: String,
    /// Optional coordinate transform payload for renderer alignment.
    pub coordinate_transform: Option<Value>,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Industry grouping allowed or recommended inside an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct IndustryGroupResponse {
    /// Stable foundation-platform identifier for the industry group.
    pub id: Uuid,
    /// Industrial complex that owns this industry group.
    pub complex_id: Uuid,
    /// Display name of the industry group.
    pub name: String,
    /// Optional source-provided description.
    pub description: Option<String>,
    /// Industry code members that make up the group.
    pub members: Vec<IndustryGroupMemberResponse>,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Single industry code inside an industry group.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct IndustryGroupMemberResponse {
    /// Industry code value, for example a KSIC code.
    pub industry_code: String,
    /// Industry code system. Currently `ksic`.
    pub industry_code_system: String,
}

/// Assignment between a parcel and an industry group.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ParcelIndustryAssignmentResponse {
    /// Stable foundation-platform identifier for the assignment.
    pub id: Uuid,
    /// Parcel receiving this assignment.
    pub parcel_id: Uuid,
    /// Industry group assigned to the parcel.
    pub industry_group_id: Uuid,
    /// Assignment kind. Wire values: `allowed`, `recommended`, `restricted`.
    pub assignment_kind: String,
    /// Monotonic record version used for optimistic concurrency.
    pub version: i64,
    /// UTC timestamp of the last canonical Catalog update.
    pub updated_at: DateTime<Utc>,
}

/// Runtime vector tile manifest consumed by Gongzzang map surfaces.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct VectorTileManifestResponse {
    /// Manifest schema version.
    pub schema_version: u32,
    /// Active tile artifact version.
    pub current_version: String,
    /// Previously active tile artifact version retained for rollback awareness.
    pub previous_version: String,
    /// URL template for vector tile requests.
    pub tiles_url_template: String,
    /// UTC timestamp when this manifest became active.
    pub published_at: DateTime<Utc>,
    /// Layer artifacts keyed by logical layer name, for example `parcels`.
    pub artifacts: BTreeMap<String, VectorTileArtifactResponse>,
}

/// Per-layer vector tile artifact metadata inside the runtime manifest.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct VectorTileArtifactResponse {
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
    /// Provider-neutral object key for this layer's `TileJSON` document.
    pub tilejson_object_key: String,
    /// Provider-neutral prefix that contains this layer's tile objects.
    pub object_key_prefix: String,
    /// Number of flat tile objects generated for this layer.
    pub flat_tile_count: u64,
    /// Total bytes across flat tile objects for this layer.
    pub flat_tile_total_bytes: u64,
    /// Logical filter properties mapped to concrete feature property names in the vector tile.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub feature_filter_properties: BTreeMap<String, String>,
    /// Source and file lineage that produced this artifact.
    pub lineage: VectorTileLineageResponse,
}

/// Lineage links required to audit a vector tile artifact.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct VectorTileLineageResponse {
    /// Source record that describes the tile build input.
    pub source_record_id: Uuid,
    /// File asset row for the manifest JSON file.
    pub manifest_file_asset_id: Uuid,
    /// File asset row for the layer `TileJSON` file.
    pub tilejson_file_asset_id: Uuid,
    /// File asset rows for source files used to build the layer.
    pub source_file_asset_ids: Vec<Uuid>,
}

/// Public contract for PNU-anchor backed marker tile endpoints.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct MarkerTileContractResponse {
    /// Wire response format for map-wide marker surfaces. Current value: `mvt_pbf`.
    pub response_format: String,
    /// Canonical source for marker positions. Current value: `pnu_anchor`.
    pub position_source: String,
    /// Public launch marker requests must not be based on arbitrary bbox inputs.
    pub bbox_marker_runtime_forbidden: bool,
    /// Successful marker responses must not silently drop eligible records.
    pub dropped_marker_success_forbidden: bool,
    /// Canonical launch runtime for static marker tiles.
    pub launch_runtime_source: String,
    /// Runtime manifest endpoint consumed before clients materialize tile URLs.
    pub runtime_manifest_endpoint: String,
    /// Database-backed reference endpoint must not be treated as the launch hot path.
    pub db_reference_endpoint_launch_forbidden: bool,
    /// Intended scope for the database-backed marker endpoint.
    pub db_reference_endpoint_scope: String,
    /// Highest zoom level served by aggregate anchor artifacts.
    pub aggregate_anchor_max_zoom: u8,
    /// Lowest zoom level where exact parcel anchors may be requested.
    pub exact_anchor_min_zoom: u8,
    /// Canonical marker tile endpoint shape.
    pub endpoint_template: String,
    /// Marker layers currently supported by the public tile endpoint.
    pub supported_layers: Vec<String>,
    /// Default filter identity for the supported anchor layer.
    pub default_filter_hash: String,
}

impl MarkerTileContractResponse {
    /// Returns the canonical PNU-anchor backed MVT/PBF marker tile contract.
    #[must_use]
    pub fn pnu_anchor_pbf() -> Self {
        let contract = PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT;
        Self {
            response_format: contract.response_format.to_owned(),
            position_source: contract.position_source.to_owned(),
            bbox_marker_runtime_forbidden: contract.bbox_marker_runtime_forbidden,
            dropped_marker_success_forbidden: contract.dropped_marker_success_forbidden,
            launch_runtime_source: contract.launch_runtime_source.to_owned(),
            runtime_manifest_endpoint: contract.runtime_manifest_endpoint.to_owned(),
            db_reference_endpoint_launch_forbidden: contract.db_reference_endpoint_launch_forbidden,
            db_reference_endpoint_scope: contract.db_reference_endpoint_scope.to_owned(),
            aggregate_anchor_max_zoom: contract.aggregate_anchor_max_zoom,
            exact_anchor_min_zoom: contract.exact_anchor_min_zoom,
            endpoint_template: contract.endpoint_template.to_owned(),
            supported_layers: vec![PARCEL_ANCHOR_MARKER_TILE_LAYER.to_owned()],
            default_filter_hash: ALL_ACTIVE_MARKER_FILTER_HASH.to_owned(),
        }
    }
}

/// Minimal marker feature metadata mirrored by PBF marker tile properties.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct MarkerTileFeatureResponse {
    /// Product-owned object id or aggregate id.
    pub id: String,
    /// Parcel identity used to resolve the anchor.
    pub pnu: String,
    /// Stable marker kind for style selection.
    pub kind: String,
    /// Number of records represented by this feature.
    pub count: u64,
    /// Optional deterministic display priority.
    pub rank: Option<u32>,
    /// Opaque lookup reference for details.
    pub detail_ref: String,
}

/// Anchor-derived map summary for one industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ComplexAnchorSummaryResponse {
    /// Industrial complex whose active parcel anchors were summarized.
    pub complex_id: Uuid,
    /// Canonical source for the summary coordinates. Current value: `pnu_anchor`.
    pub position_source: String,
    /// Average longitude of active parcel anchors in EPSG:4326.
    pub center_lng: f64,
    /// Average latitude of active parcel anchors in EPSG:4326.
    pub center_lat: f64,
    /// Minimum active anchor longitude in EPSG:4326.
    pub min_lng: f64,
    /// Minimum active anchor latitude in EPSG:4326.
    pub min_lat: f64,
    /// Maximum active anchor longitude in EPSG:4326.
    pub max_lng: f64,
    /// Maximum active anchor latitude in EPSG:4326.
    pub max_lat: f64,
    /// Number of active parcel anchors represented by this summary.
    pub anchor_count: u64,
}

/// Request body for rebuilding parcel marker anchors from an approved mirror snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ParcelMarkerAnchorRebuildRequest {
    /// Approved Iceberg snapshot represented by the `PostGIS` mirror rows.
    pub source_snapshot_id: String,
    /// Stable algorithm implementation version.
    pub algorithm_version: String,
}

/// Response body returned after a parcel marker anchor rebuild finishes.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ParcelMarkerAnchorRebuildResponse {
    /// Generation run persisted for traceability.
    pub generation_run_id: Uuid,
    /// Approved Iceberg snapshot represented by the rebuild.
    pub source_snapshot_id: String,
    /// Canonical source table represented by the rebuild.
    pub source_table: String,
    /// Anchor derivation algorithm.
    pub algorithm: String,
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

/// Request body for updating a parcel kind with optimistic concurrency.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateParcelKindRequest {
    /// New parcel kind wire value.
    pub new_kind: String,
    /// Version observed by the caller; stale values return conflict.
    pub if_match_version: i64,
}

/// Request body for manually rolling back the active vector tile manifest.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct RollbackVectorTileManifestRequest {
    /// Version that should become active after rollback.
    pub to_version: String,
    /// Active version the caller expects before rollback.
    pub expected_current_version: String,
    /// Human-readable rollback reason for audit logs.
    pub reason: String,
}

/// Request body for promoting a vector tile build into the active manifest slot.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct PromoteVectorTileManifestRequest {
    /// Version that should become active after promote.
    pub current_version: String,
    /// Active version the caller expects before promote.
    pub expected_current_version: String,
    /// URL template clients use to request vector tiles.
    pub tiles_url_template: String,
    /// Source record describing the build input.
    pub source_record: PromoteSourceRecordRequest,
    /// File asset metadata for the manifest JSON artifact.
    pub manifest_file_asset: PromoteFileAssetRequest,
    /// Layer artifacts keyed by logical layer name.
    pub artifacts: BTreeMap<String, PromoteVectorTileArtifactRequest>,
}

/// Source record payload supplied during vector tile manifest promote.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct PromoteSourceRecordRequest {
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

/// File asset payload supplied during vector tile manifest promote.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct PromoteFileAssetRequest {
    /// Provider-neutral object storage key. This intentionally avoids S3/R2-specific naming.
    pub object_key: String,
    /// MIME type recorded for the object.
    pub mime_type: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional SHA-256 checksum in lowercase hexadecimal form.
    pub checksum_sha256: Option<String>,
    /// Optional display title for UI surfaces.
    pub title: Option<String>,
    /// File visibility. Wire values: `public`, `internal`, `private`.
    pub visibility: String,
}

/// Per-layer artifact payload supplied during vector tile manifest promote.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct PromoteVectorTileArtifactRequest {
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
    /// File asset metadata for this layer's `TileJSON` document.
    pub tilejson_file_asset: PromoteFileAssetRequest,
    /// Provider-neutral prefix that contains this layer's tile objects.
    pub object_key_prefix: String,
    /// Number of flat tile objects generated for this layer.
    pub flat_tile_count: u64,
    /// Total bytes across flat tile objects for this layer.
    pub flat_tile_total_bytes: u64,
    /// Source file assets used to build this layer.
    pub source_file_assets: Vec<PromoteFileAssetRequest>,
}
