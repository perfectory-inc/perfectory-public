//! Catalog HTTP handler.

/// `GET /catalog/v1/complexes` — the paged, filtered industrial-complex collection.
pub mod complex_search;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use axum::extract::{Extension, OriginalUri, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use catalog_application::ports::{
    CatalogRepository, VectorTileArtifactPromotionCommand, VectorTileFileAssetCommand,
    VectorTileSourceRecordCommand,
};
use catalog_application::{
    archive_complex::ArchiveIndustrialComplexInput,
    promote_vector_tile_manifest::PromoteVectorTileManifestInput,
    rebuild_parcel_marker_anchors::RebuildParcelMarkerAnchorsInput,
    register_complex::RegisterIndustrialComplexInput,
    rollback_vector_tile_manifest::RollbackVectorTileManifestInput,
    update_complex::UpdateIndustrialComplexInput, update_parcel_kind::UpdateParcelKindInput,
};
use catalog_domain::{
    ActiveTileSource, Blueprint, Building, CatalogError, ComplexAnchorSummary, ComplexNotice,
    DigitalTwinAsset, FileAsset, IndustrialComplex, IndustrialComplexKind, IndustryGroup,
    IndustryGroupMember, MarkerTileRequest, Parcel, ParcelIndustryAssignment, ParcelKind,
    SpatialLayer, VectorTileArtifact, VectorTileManifest, VectorTileRuntimeManifest,
};
use catalog_infrastructure::{BuildingUnitRow, UnitPageKey};
use foundation_contracts::catalog::{
    ArchiveComplexRequest, BlueprintResponse, BuildingResponse, ComplexAnchorSummaryResponse,
    ComplexNoticeResponse, DigitalTwinAssetResponse, FileAssetResponse,
    IndustrialComplexGoldPointerResponse, IndustrialComplexResponse, IndustryGroupMemberResponse,
    IndustryGroupResponse, MarkerTileContractResponse, ParcelIndustryAssignmentResponse,
    ParcelMarkerAnchorRebuildRequest, ParcelMarkerAnchorRebuildResponse, ParcelResponse,
    PromoteFileAssetRequest, PromoteSourceRecordRequest, PromoteVectorTileArtifactRequest,
    PromoteVectorTileManifestRequest, RegisterComplexRequest, RollbackVectorTileManifestRequest,
    SpatialLayerResponse, UnitPageResponse, UnitResponse, UpdateComplexRequest,
    UpdateParcelKindRequest, VectorTileArtifactResponse, VectorTileDynamicPostgisResponse,
    VectorTileLineageResponse, VectorTileManifestResponse, VectorTilePublicationUnitResponse,
    VectorTileRuntimeLayerResponse, VectorTileRuntimeLineageResponse,
    VectorTileRuntimeManifestResponse, VectorTileRuntimeSourceResponse,
    VectorTileStaticPmtilesResponse,
};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId, ParcelId, StaffId};
use foundation_shared_kernel::pnu::Pnu;
use lakehouse_application::RecordLakehouseBatchRunInput;
use lakehouse_domain::IndustrialComplexGoldPointer;
use lakehouse_domain::LakehouseError;
use serde::Deserialize;
use uuid::Uuid;

use super::ApiError;

use crate::identity_authorization::AuthorizedPrincipal;
use crate::state::AppState;

const VECTOR_TILE_MANIFEST_ROLLBACK_PATH: &str = "/catalog/v1/vector-tiles/manifest:rollback";
const VECTOR_TILE_MANIFEST_PROMOTE_PATH: &str = "/catalog/v1/vector-tiles/manifest:promote";
const PARCEL_MARKER_ANCHOR_REBUILD_PATH: &str = "/catalog/v1/parcel-marker-anchors:rebuild";
const MARKER_TILE_CONTENT_TYPE: &str = "application/x-protobuf";
const MARKER_TILE_CACHE_CONTROL: &str = "public, max-age=30";
const FOUNDATION_PLATFORM_RUNTIME_ENV: &str = "FOUNDATION_PLATFORM_RUNTIME_ENV";
const DB_MARKER_TILE_REFERENCE_ENABLED_ENV: &str =
    "FOUNDATION_PLATFORM_DB_MARKER_TILE_REFERENCE_ENABLED";

#[utoipa::path(
    post,
    path = "/catalog/v1/complexes",
    operation_id = "registerComplex",
    request_body = RegisterComplexRequest,
    responses((status = 201, description = "Industrial complex registered", body = IndustrialComplexResponse)),
    security(("bearerAuth" = []))
)]
pub async fn register_complex(
    State(state): State<Arc<AppState>>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<RegisterComplexRequest>,
) -> Result<(StatusCode, Json<IndustrialComplexResponse>), ApiError> {
    let kind = IndustrialComplexKind::from_wire(&body.kind)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let result = state
        .register_complex
        .execute(RegisterIndustrialComplexInput {
            official_complex_code: body.official_complex_code,
            name: body.name,
            kind,
            primary_bjdong_code: body.primary_bjdong_code,
            area_m2: body.area_m2,
        })
        .await?;

    let resp = IndustrialComplexResponse {
        id: result.id.as_uuid(),
        lakehouse_complex_id: result.lakehouse_complex_id.map(|id| id.as_uuid()),
        official_complex_code: result.official_complex_code,
        name: result.name,
        kind: result.kind.wire_name().to_owned(),
        primary_bjdong_code: result.primary_bjdong_code,
        area_m2: result.area_m2,
        status: result.status.map(|status| status.wire_name().to_owned()),
        sido_code: result.sido_code,
        sigungu_code: result.sigungu_code,
        address_text: result.address_text,
        management_agency_name: result.management_agency_name,
        developer_name: result.developer_name,
        designated_date: result.designated_date,
        construction_start_date: result.construction_start_date,
        completion_date: result.completion_date,
        development_progress_percent: result.development_progress_percent,
        lot_sales_status: result
            .lot_sales_status
            .map(|status| status.wire_name().to_owned()),
        business_period_raw: result.business_period_raw,
        business_period_start_month: result.business_period_start_month,
        business_period_end_month: result.business_period_end_month,
        designation_basis_law_raw: result.designation_basis_law_raw,
        development_method_raw: result.development_method_raw,
        development_purpose_raw: result.development_purpose_raw,
        invited_industries_raw: result.invited_industries_raw,
        version: result.version,
        updated_at: result.updated_at,
        archived_at: result.archived_at,
        gold_pointer: None,
    };
    Ok((StatusCode::CREATED, Json(resp)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}",
    operation_id = "getComplex",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = IndustrialComplexResponse))
)]
pub async fn get_complex(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<IndustrialComplexResponse>, ApiError> {
    let complex = state
        .catalog_repo
        .find_complex(ComplexId::new(id))
        .await?
        .ok_or_else(|| ApiError::NotFound(id.to_string()))?;
    let gold_pointer = state
        .industrial_complex_gold_pointer_reader
        .find_industrial_complex_gold_pointer(complex.id)
        .await?;

    Ok(Json(industrial_complex_response(complex, gold_pointer)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/by-lakehouse-id/{lakehouse_complex_id}",
    operation_id = "getComplexByLakehouseId",
    params((
        "lakehouse_complex_id" = Uuid,
        Path,
        description = "Lakehouse industrial-complex id (UUIDv5), as published in Gold artifact keys and `complex` vector tile feature ids",
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )),
    responses(
        (status = 200, body = IndustrialComplexResponse),
        (status = 404, description = "No canonical complex carries that lakehouse id")
    )
)]
pub async fn get_complex_by_lakehouse_id(
    State(state): State<Arc<AppState>>,
    Path(lakehouse_complex_id): Path<Uuid>,
) -> Result<Json<IndustrialComplexResponse>, ApiError> {
    let complex = state
        .catalog_repo
        .find_complex_by_lakehouse_id(LakehouseComplexId::new(lakehouse_complex_id))
        .await?
        .ok_or_else(|| ApiError::NotFound(lakehouse_complex_id.to_string()))?;
    let gold_pointer = state
        .industrial_complex_gold_pointer_reader
        .find_industrial_complex_gold_pointer(complex.id)
        .await?;

    Ok(Json(industrial_complex_response(complex, gold_pointer)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}/anchor-summary",
    operation_id = "getComplexAnchorSummary",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = ComplexAnchorSummaryResponse))
)]
pub async fn get_complex_anchor_summary(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<ComplexAnchorSummaryResponse>, ApiError> {
    let summary = state
        .catalog_repo
        .find_complex_anchor_summary(ComplexId::new(id))
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("complex anchor summary {id}")))?;

    Ok(Json(complex_anchor_summary_response(&summary)))
}

#[utoipa::path(
    patch,
    path = "/catalog/v1/complexes/{id}",
    operation_id = "updateComplex",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    request_body = UpdateComplexRequest,
    responses((status = 200, body = IndustrialComplexResponse)),
    security(("bearerAuth" = []))
)]
pub async fn update_complex(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<UpdateComplexRequest>,
) -> Result<Json<IndustrialComplexResponse>, ApiError> {
    let complex_id = ComplexId::new(id);
    let updated = state
        .update_complex
        .execute(UpdateIndustrialComplexInput {
            complex_id,
            expected_version: body.if_match_version,
            name: body.name,
            area_m2: body.area_m2,
        })
        .await?;
    let gold_pointer = state
        .industrial_complex_gold_pointer_reader
        .find_industrial_complex_gold_pointer(updated.id)
        .await?;

    Ok(Json(industrial_complex_response(updated, gold_pointer)))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/complexes/{id}/archive",
    operation_id = "archiveComplex",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    request_body = ArchiveComplexRequest,
    responses((status = 200, body = IndustrialComplexResponse)),
    security(("bearerAuth" = []))
)]
pub async fn archive_complex(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<ArchiveComplexRequest>,
) -> Result<Json<IndustrialComplexResponse>, ApiError> {
    let archived = state
        .archive_complex
        .execute(ArchiveIndustrialComplexInput {
            complex_id: ComplexId::new(id),
            expected_version: body.if_match_version,
            operator_staff_id: StaffId::new(principal.principal_id),
            reason: body.reason,
            request_id: request_id_from_headers(&headers),
        })
        .await?;
    let gold_pointer = state
        .industrial_complex_gold_pointer_reader
        .find_industrial_complex_gold_pointer(archived.id)
        .await?;

    Ok(Json(industrial_complex_response(archived, gold_pointer)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/parcels/by-pnu/{pnu}/buildings",
    operation_id = "listParcelBuildingsByPnu",
    params((
        "pnu" = String,
        Path,
        description = "19-digit Parcel Number Unit",
        min_length = 19,
        max_length = 19,
        pattern = "^[0-9]{10}[1289][0-9]{8}$"
    )),
    responses((status = 200, body = [BuildingResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_parcel_buildings_by_pnu(
    State(state): State<Arc<AppState>>,
    Path(pnu): Path<String>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<Vec<BuildingResponse>>, ApiError> {
    let pnu = Pnu::parse(pnu).map_err(CatalogError::InvalidPnu)?;
    let buildings = state.catalog_repo.list_buildings_by_pnu(&pnu).await?;

    Ok(Json(buildings.iter().map(building_response).collect()))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/parcels/by-pnu/{pnu}/units",
    operation_id = "listParcelUnitsByPnu",
    params((
        "pnu" = String,
        Path,
        description = "19-digit Parcel Number Unit",
        min_length = 19,
        max_length = 19,
        pattern = "^[0-9]{10}[1289][0-9]{8}$"
    )),
    responses((status = 200, body = [UnitResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_parcel_units_by_pnu(
    State(state): State<Arc<AppState>>,
    Path(pnu): Path<String>,
) -> Result<Json<Vec<UnitResponse>>, ApiError> {
    let pnu = Pnu::parse(pnu).map_err(CatalogError::InvalidPnu)?;
    let units = state.catalog_repo.list_units_by_pnu(&pnu).await?;

    Ok(Json(units.iter().map(unit_response).collect()))
}

/// Default page size for a building's unit list (ADR-0076 §3).
const UNIT_PAGE_DEFAULT_LIMIT: i64 = 200;
/// Documented maximum page size; a request past it is refused, not clamped.
const UNIT_PAGE_MAX_LIMIT: i64 = 1000;
/// What every malformed cursor answers: the caller is holding something this API never issued,
/// and guessing a position inside 19M rows would serve someone else's page.
const CURSOR_REFUSAL: &str = "cursor is not one this API issued";

/// Query half of `GET /catalog/v1/buildings/{id}/units`.
#[derive(Debug, Deserialize)]
pub struct UnitPageQuery {
    /// Page size; defaults to [`UNIT_PAGE_DEFAULT_LIMIT`], bounded by [`UNIT_PAGE_MAX_LIMIT`].
    limit: Option<i64>,
    /// Opaque continuation cursor from the previous page's `next_cursor`.
    cursor: Option<String>,
}

/// Encodes the keyset of a page's last row as an opaque cursor.
///
/// Unit separator (`\u{1f}`) as the delimiter because it cannot appear in 동·호 names — both
/// come out of the normalizer's printable text — and base64url so the value travels in a query
/// string without escaping.
fn encode_unit_cursor(unit: &BuildingUnitRow) -> String {
    use base64::Engine as _;
    let raw = format!("{}\u{1f}{}\u{1f}{}", unit.dong_name, unit.ho_name, unit.id);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

/// Decodes a caller-supplied cursor, refusing anything this API could not have issued.
fn decode_unit_cursor(cursor: &str) -> Result<UnitPageKey, ApiError> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ApiError::BadRequest(CURSOR_REFUSAL.to_owned()))?;
    let raw =
        String::from_utf8(bytes).map_err(|_| ApiError::BadRequest(CURSOR_REFUSAL.to_owned()))?;
    let mut parts = raw.splitn(3, '\u{1f}');
    let (Some(dong_name), Some(ho_name), Some(id)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(ApiError::BadRequest(CURSOR_REFUSAL.to_owned()));
    };
    let id = Uuid::parse_str(id).map_err(|_| ApiError::BadRequest(CURSOR_REFUSAL.to_owned()))?;
    Ok(UnitPageKey {
        dong_name: dong_name.to_owned(),
        ho_name: ho_name.to_owned(),
        id,
    })
}

#[utoipa::path(
    get,
    path = "/catalog/v1/buildings/{id}",
    operation_id = "getBuilding",
    params(("id" = Uuid, Path, description = "Building id")),
    responses((status = 200, body = BuildingResponse), (status = 404, description = "Building not found")),
    security(("bearerAuth" = []))
)]
pub async fn get_building(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<BuildingResponse>, ApiError> {
    let building = state
        .catalog_repo
        .find_building(id)
        .await?
        .ok_or_else(|| ApiError::NotFound(id.to_string()))?;

    Ok(Json(building_response(&building)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/buildings/by-register-pk/{register_pk}",
    operation_id = "getBuildingByRegisterPk",
    params((
        "register_pk" = String,
        Path,
        description = "Title-register PK (관리대장 PK), the building's natural key"
    )),
    responses((status = 200, body = BuildingResponse), (status = 404, description = "Building not found")),
    security(("bearerAuth" = []))
)]
pub async fn get_building_by_register_pk(
    State(state): State<Arc<AppState>>,
    Path(register_pk): Path<String>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<BuildingResponse>, ApiError> {
    let register_pk = register_pk.trim();
    if register_pk.is_empty() {
        return Err(ApiError::BadRequest(
            "register_pk must not be empty".to_owned(),
        ));
    }
    let building = state
        .catalog_repo
        .find_building_by_register_pk(register_pk)
        .await?
        .ok_or_else(|| ApiError::NotFound(register_pk.to_owned()))?;

    Ok(Json(building_response(&building)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/buildings/{id}/units",
    operation_id = "listBuildingUnits",
    params(
        ("id" = Uuid, Path, description = "Building id"),
        ("limit" = Option<i64>, Query, description = "Page size, 1..=1000 (default 200)"),
        ("cursor" = Option<String>, Query, description = "Opaque cursor from the previous page's next_cursor")
    ),
    responses((status = 200, body = UnitPageResponse), (status = 404, description = "Building not found")),
    security(("bearerAuth" = []))
)]
pub async fn list_building_units(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Query(query): Query<UnitPageQuery>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<UnitPageResponse>, ApiError> {
    let limit = query.limit.unwrap_or(UNIT_PAGE_DEFAULT_LIMIT);
    if !(1..=UNIT_PAGE_MAX_LIMIT).contains(&limit) {
        return Err(ApiError::BadRequest(format!(
            "limit must be between 1 and {UNIT_PAGE_MAX_LIMIT}, got {limit}"
        )));
    }
    // An absent building answers 404; an empty page on a real building answers 200 with no
    // items. Collapsing the two would make "no such building" indistinguishable from "a
    // building with no units", and both states exist in this dataset.
    state
        .catalog_repo
        .find_building(id)
        .await?
        .ok_or_else(|| ApiError::NotFound(id.to_string()))?;
    let after = query
        .cursor
        .as_deref()
        .map(decode_unit_cursor)
        .transpose()?;

    let rows = state
        .catalog_repo
        .list_units_by_building(id, after.as_ref(), limit + 1)
        .await?;

    // Bounds-checked above: 1..=1000 always fits usize.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let page_len = limit as usize;
    let has_more = rows.len() > page_len;
    let items: Vec<UnitResponse> = rows.iter().take(page_len).map(unit_response).collect();
    let next_cursor = if has_more {
        rows.get(page_len - 1).map(encode_unit_cursor)
    } else {
        None
    };

    Ok(Json(UnitPageResponse { items, next_cursor }))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/parcels/by-pnu/{pnu}",
    operation_id = "getParcelByPnu",
    params((
        "pnu" = String,
        Path,
        description = "19-digit Parcel Number Unit",
        min_length = 19,
        max_length = 19,
        pattern = "^[0-9]{10}[1289][0-9]{8}$"
    )),
    responses((status = 200, body = ParcelResponse), (status = 404, description = "Parcel not found")),
    security(("bearerAuth" = []))
)]
pub async fn get_parcel_by_pnu(
    State(state): State<Arc<AppState>>,
    Path(pnu): Path<String>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<ParcelResponse>, ApiError> {
    let pnu = Pnu::parse(pnu).map_err(CatalogError::InvalidPnu)?;
    let parcel = state
        .catalog_repo
        .find_parcel_by_pnu(&pnu)
        .await?
        .ok_or_else(|| ApiError::NotFound(pnu.as_str().to_owned()))?;

    Ok(Json(parcel_response(&parcel)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/parcels/{id}",
    operation_id = "getParcel",
    params(("id" = Uuid, Path, description = "Parcel id")),
    responses((status = 200, body = ParcelResponse), (status = 404, description = "Parcel not found")),
    security(("bearerAuth" = []))
)]
pub async fn get_parcel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Extension(_principal): Extension<AuthorizedPrincipal>,
) -> Result<Json<ParcelResponse>, ApiError> {
    let parcel = state
        .catalog_repo
        .find_parcel_by_id(ParcelId::new(id))
        .await?
        .ok_or_else(|| ApiError::NotFound(id.to_string()))?;

    Ok(Json(parcel_response(&parcel)))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}/notices",
    operation_id = "listComplexNotices",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = [ComplexNoticeResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_complex_notices(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<ComplexNoticeResponse>>, ApiError> {
    let notices = state
        .catalog_repo
        .list_complex_notices(ComplexId::new(id))
        .await?;
    let mut responses = Vec::with_capacity(notices.len());

    for notice in notices {
        let attachments = state
            .catalog_repo
            .list_notice_file_assets(notice.id)
            .await?;
        responses.push(complex_notice_response(notice, attachments));
    }

    Ok(Json(responses))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}/attachments",
    operation_id = "listComplexAttachments",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = [FileAssetResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_complex_attachments(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<FileAssetResponse>>, ApiError> {
    let attachments = state
        .catalog_repo
        .list_complex_attachments(ComplexId::new(id))
        .await?;

    Ok(Json(
        attachments.into_iter().map(file_asset_response).collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}/blueprints",
    operation_id = "listComplexBlueprints",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = [BlueprintResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_complex_blueprints(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<BlueprintResponse>>, ApiError> {
    let blueprints = state
        .catalog_repo
        .list_complex_blueprints(ComplexId::new(id))
        .await?;

    Ok(Json(
        blueprints.into_iter().map(blueprint_response).collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}/spatial-layers",
    operation_id = "listComplexSpatialLayers",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = [SpatialLayerResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_complex_spatial_layers(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<SpatialLayerResponse>>, ApiError> {
    let layers = state
        .catalog_repo
        .list_complex_spatial_layers(ComplexId::new(id))
        .await?;

    Ok(Json(
        layers.into_iter().map(spatial_layer_response).collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/complexes/{id}/digital-twin-assets",
    operation_id = "listComplexDigitalTwinAssets",
    params(("id" = Uuid, Path, description = "Industrial complex id")),
    responses((status = 200, body = [DigitalTwinAssetResponse])),
    security(("bearerAuth" = []))
)]
pub async fn list_complex_digital_twin_assets(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<DigitalTwinAssetResponse>>, ApiError> {
    let assets = state
        .catalog_repo
        .list_complex_digital_twin_assets(ComplexId::new(id))
        .await?;

    Ok(Json(
        assets
            .into_iter()
            .map(digital_twin_asset_response)
            .collect(),
    ))
}

#[derive(Debug, Deserialize)]
pub struct ListIndustryGroupsQuery {
    pub complex_id: Option<Uuid>,
}

#[utoipa::path(
    get,
    path = "/catalog/v1/industry-groups",
    operation_id = "listIndustryGroups",
    params(("complex_id" = Option<Uuid>, Query, description = "Optional industrial complex filter")),
    responses((status = 200, body = [IndustryGroupResponse]))
)]
pub async fn list_industry_groups(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListIndustryGroupsQuery>,
) -> Result<Json<Vec<IndustryGroupResponse>>, ApiError> {
    let complex_id = query.complex_id.map(ComplexId::new);
    let groups = state.catalog_repo.list_industry_groups(complex_id).await?;
    let members = if let Some(complex_id) = complex_id {
        state
            .catalog_repo
            .list_industry_group_members_for_complex(complex_id)
            .await?
    } else {
        Vec::new()
    };

    Ok(Json(
        groups
            .into_iter()
            .map(|group| industry_group_response(group, &members))
            .collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/parcels/{id}/industry-assignments",
    operation_id = "listParcelIndustryAssignments",
    params(("id" = Uuid, Path, description = "Parcel id")),
    responses((status = 200, body = [ParcelIndustryAssignmentResponse]))
)]
pub async fn list_parcel_industry_assignments(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<ParcelIndustryAssignmentResponse>>, ApiError> {
    let assignments = state
        .catalog_repo
        .list_parcel_industry_assignments(ParcelId::new(id))
        .await?;

    Ok(Json(
        assignments
            .iter()
            .map(parcel_industry_assignment_response)
            .collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/catalog/v1/vector-tiles/manifest",
    operation_id = "getVectorTileManifest",
    responses((status = 200, body = VectorTileManifestResponse), (status = 404, description = "Active manifest not found"))
)]
pub async fn get_vector_tile_manifest(
    State(state): State<Arc<AppState>>,
) -> Result<Json<VectorTileManifestResponse>, ApiError> {
    let manifest = state
        .catalog_repo
        .get_active_vector_tile_manifest()
        .await?
        .ok_or_else(|| ApiError::NotFound("active vector tile manifest".to_owned()))?;

    Ok(Json(vector_tile_manifest_response(manifest)))
}

/// Returns the atomic v2 runtime manifest with a quoted `ETag` and conditional GET support.
#[utoipa::path(
    get,
    path = "/catalog/v1/vector-tiles/runtime-manifest",
    operation_id = "getVectorTileRuntimeManifest",
    responses((status = 200, body = VectorTileRuntimeManifestResponse), (status = 304), (status = 404, description = "v2 runtime manifest is disabled or not published"))
)]
pub async fn get_vector_tile_runtime_manifest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if !state.runtime_manifest_publication().is_enabled() {
        return Err(ApiError::NotFound(
            "v2 vector tile runtime manifest is disabled".to_owned(),
        ));
    }
    let manifest = state
        .catalog_repo
        .get_active_vector_tile_runtime_manifest()
        .await?
        .ok_or_else(|| ApiError::NotFound("active v2 vector tile runtime manifest".to_owned()))?;
    let etag = format!("\"{}\"", manifest.current_version);
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|candidate| candidate.trim() == etag))
    {
        return Ok((
            StatusCode::NOT_MODIFIED,
            [
                (header::ETAG, etag),
                (
                    header::CACHE_CONTROL,
                    "no-cache, must-revalidate".to_owned(),
                ),
            ],
        )
            .into_response());
    }
    let body =
        serde_json::to_vec(&vector_tile_runtime_manifest_response(manifest)).map_err(|error| {
            ApiError::Internal(format!("failed to serialize runtime manifest: {error}"))
        })?;
    let etag_header = HeaderValue::from_str(&etag)
        .map_err(|error| ApiError::Internal(format!("invalid runtime manifest ETag: {error}")))?;
    Ok((
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-cache, must-revalidate"),
            ),
            (header::ETAG, etag_header),
        ],
        body,
    )
        .into_response())
}

#[utoipa::path(
    get,
    path = "/map/v1/marker-tiles/contract",
    operation_id = "getMarkerTileContract",
    responses((status = 200, body = MarkerTileContractResponse))
)]
pub async fn get_marker_tile_contract() -> Json<MarkerTileContractResponse> {
    Json(MarkerTileContractResponse::pnu_anchor_pbf())
}

#[derive(Debug, Deserialize)]
pub struct MarkerTilePath {
    pub layer: String,
    pub z: u8,
    pub x: u32,
    pub y_pbf: String,
}

#[derive(Debug, Deserialize)]
pub struct MarkerTileQuery {
    pub filter_hash: String,
}

#[utoipa::path(
    get,
    path = "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf",
    operation_id = "getMarkerTile",
    params(
        ("layer" = String, Path),
        ("z" = u8, Path),
        ("x" = u32, Path),
        ("y" = u32, Path),
        ("filter_hash" = String, Query)
    ),
    responses((status = 200, content_type = "application/x-protobuf", body = Vec<u8>))
)]
pub async fn get_marker_tile(
    State(state): State<Arc<AppState>>,
    Path(path): Path<MarkerTilePath>,
    Query(query): Query<MarkerTileQuery>,
) -> Result<Response, ApiError> {
    if !db_reference_marker_tile_enabled() {
        return Err(ApiError::Forbidden(
            "database-backed marker tile reference endpoint is disabled in production runtime"
                .to_owned(),
        ));
    }

    let y = parse_marker_tile_y_pbf(&path.y_pbf)?;
    let request = MarkerTileRequest::new(&path.layer, path.z, path.x, y, &query.filter_hash)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let tile = state.catalog_repo.get_marker_tile(request).await?;
    let mut response = tile.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(MARKER_TILE_CONTENT_TYPE),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(MARKER_TILE_CACHE_CONTROL),
    );
    Ok(response)
}

#[utoipa::path(
    post,
    path = "/catalog/v1/parcel-marker-anchors:rebuild",
    operation_id = "rebuildParcelMarkerAnchors",
    request_body = ParcelMarkerAnchorRebuildRequest,
    responses((status = 200, body = ParcelMarkerAnchorRebuildResponse)),
    security(("bearerAuth" = []))
)]
pub async fn rebuild_parcel_marker_anchors(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<ParcelMarkerAnchorRebuildRequest>,
) -> Result<Json<ParcelMarkerAnchorRebuildResponse>, ApiError> {
    require_exact_manifest_action_path(uri.path(), PARCEL_MARKER_ANCHOR_REBUILD_PATH)?;
    let report = state
        .rebuild_parcel_marker_anchors
        .execute(RebuildParcelMarkerAnchorsInput {
            source_snapshot_id: body.source_snapshot_id,
            algorithm_version: body.algorithm_version,
            requested_by_staff_id: Some(StaffId::new(principal.principal_id)),
            request_id: request_id_from_headers(&headers),
        })
        .await?;

    Ok(Json(parcel_marker_anchor_rebuild_response(report)))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/lakehouse/batch-runs",
    operation_id = "recordLakehouseBatchRun",
    request_body = serde_json::Value,
    responses((status = 201, body = serde_json::Value)),
    security(("bearerAuth" = []))
)]
pub async fn record_lakehouse_batch_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let raw_summary = serde_json::to_string(&body)
        .map_err(|error| ApiError::BadRequest(format!("invalid summary JSON: {error}")))?;
    let summary = state
        .record_lakehouse_batch_run
        .execute(RecordLakehouseBatchRunInput {
            summary_json: raw_summary,
            recorded_by_staff_id: StaffId::new(principal.principal_id),
            request_id: request_id_from_headers(&headers),
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "schema_version": summary.schema_version,
            "job_name": summary.job_name,
            "contract": summary.contract,
            "row_count": summary.row_count,
            "persisted_row_count": summary.persisted_row_count,
            "source_snapshot_count": summary.source_snapshot_count,
        })),
    ))
}

#[utoipa::path(
    post,
    path = "/catalog/v1/vector-tiles/manifest:rollback",
    operation_id = "rollbackVectorTileManifest",
    request_body = RollbackVectorTileManifestRequest,
    responses((status = 200, body = VectorTileManifestResponse)),
    security(("bearerAuth" = []))
)]
pub async fn rollback_vector_tile_manifest(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<RollbackVectorTileManifestRequest>,
) -> Result<Json<VectorTileManifestResponse>, ApiError> {
    require_exact_manifest_action_path(uri.path(), VECTOR_TILE_MANIFEST_ROLLBACK_PATH)?;
    let manifest = state
        .rollback_vector_tile_manifest
        .execute(RollbackVectorTileManifestInput {
            to_version: body.to_version,
            expected_current_version: body.expected_current_version,
            reason: body.reason,
            operator_staff_id: StaffId::new(principal.principal_id),
            request_id: request_id_from_headers(&headers),
        })
        .await?;

    Ok(Json(vector_tile_manifest_response(manifest)))
}

#[utoipa::path(
    put,
    path = "/catalog/v1/vector-tiles/manifest:promote",
    operation_id = "promoteVectorTileManifest",
    request_body = PromoteVectorTileManifestRequest,
    responses((status = 200, body = VectorTileManifestResponse)),
    security(("bearerAuth" = []))
)]
pub async fn promote_vector_tile_manifest(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<PromoteVectorTileManifestRequest>,
) -> Result<Json<VectorTileManifestResponse>, ApiError> {
    require_exact_manifest_action_path(uri.path(), VECTOR_TILE_MANIFEST_PROMOTE_PATH)?;
    let manifest = state
        .promote_vector_tile_manifest
        .execute(PromoteVectorTileManifestInput {
            current_version: body.current_version,
            expected_current_version: body.expected_current_version,
            tiles_url_template: body.tiles_url_template,
            source_snapshot_id: body.source_snapshot_id,
            source_record: source_record_command(body.source_record),
            manifest_file_asset: file_asset_command(body.manifest_file_asset),
            artifacts: body
                .artifacts
                .into_iter()
                .map(|(layer, artifact)| (layer, artifact_command(artifact)))
                .collect(),
            operator_staff_id: StaffId::new(principal.principal_id),
            request_id: request_id_from_headers(&headers),
        })
        .await?;

    Ok(Json(vector_tile_manifest_response(manifest)))
}

#[utoipa::path(
    patch,
    path = "/catalog/v1/parcels/{id}/kind",
    operation_id = "updateParcelKind",
    params(("id" = Uuid, Path, description = "Parcel id")),
    request_body = UpdateParcelKindRequest,
    responses((status = 200, body = ParcelResponse)),
    security(("bearerAuth" = []))
)]
pub async fn update_parcel_kind(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Json(body): Json<UpdateParcelKindRequest>,
) -> Result<Json<ParcelResponse>, ApiError> {
    let new_kind =
        ParcelKind::from_wire(&body.new_kind).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let updated = state
        .update_parcel_kind
        .execute(UpdateParcelKindInput {
            parcel_id: ParcelId::new(id),
            expected_version: body.if_match_version,
            new_kind,
            applied_by: StaffId::new(principal.principal_id),
        })
        .await?;

    Ok(Json(parcel_response(&updated)))
}

fn industrial_complex_response(
    complex: IndustrialComplex,
    gold_pointer: Option<IndustrialComplexGoldPointer>,
) -> IndustrialComplexResponse {
    IndustrialComplexResponse {
        id: complex.id.as_uuid(),
        lakehouse_complex_id: complex.lakehouse_complex_id.map(|id| id.as_uuid()),
        official_complex_code: complex.official_complex_code,
        name: complex.name,
        kind: complex.kind.wire_name().to_owned(),
        primary_bjdong_code: complex.primary_bjdong_code,
        area_m2: complex.area_m2,
        status: complex.status.map(|status| status.wire_name().to_owned()),
        sido_code: complex.sido_code,
        sigungu_code: complex.sigungu_code,
        address_text: complex.address_text,
        management_agency_name: complex.management_agency_name,
        developer_name: complex.developer_name,
        designated_date: complex.designated_date,
        construction_start_date: complex.construction_start_date,
        completion_date: complex.completion_date,
        development_progress_percent: complex.development_progress_percent,
        lot_sales_status: complex
            .lot_sales_status
            .map(|status| status.wire_name().to_owned()),
        business_period_raw: complex.business_period_raw,
        business_period_start_month: complex.business_period_start_month,
        business_period_end_month: complex.business_period_end_month,
        designation_basis_law_raw: complex.designation_basis_law_raw,
        development_method_raw: complex.development_method_raw,
        development_purpose_raw: complex.development_purpose_raw,
        invited_industries_raw: complex.invited_industries_raw,
        version: complex.version,
        updated_at: complex.updated_at,
        archived_at: complex.archived_at,
        gold_pointer: gold_pointer.map(industrial_complex_gold_pointer_response),
    }
}

fn industrial_complex_list_response(
    complexes: Vec<IndustrialComplex>,
    mut gold_pointers: HashMap<ComplexId, IndustrialComplexGoldPointer>,
) -> Vec<IndustrialComplexResponse> {
    complexes
        .into_iter()
        .map(|complex| {
            let gold_pointer = gold_pointers.remove(&complex.id);
            industrial_complex_response(complex, gold_pointer)
        })
        .collect()
}

fn industrial_complex_gold_pointer_response(
    pointer: IndustrialComplexGoldPointer,
) -> IndustrialComplexGoldPointerResponse {
    IndustrialComplexGoldPointerResponse {
        current_version: pointer.current_version,
        previous_version: pointer.previous_version,
        profile_object_key: pointer.profile_object_key.as_str().to_owned(),
        profile_url_template: pointer.profile_url_template.as_str().to_owned(),
        spatial_locator_object_key: pointer
            .spatial_locator_object_key
            .as_ref()
            .map(|key| key.as_str().to_owned()),
        source_record_id: pointer.source_record_id.as_uuid(),
        source_snapshot_id: pointer.source_snapshot_id,
        iceberg_snapshot_id: pointer.iceberg_snapshot_id,
        profile_row_count: pointer.profile_row_count,
        profile_checksum_sha256: pointer.profile_checksum_sha256,
        published_at: pointer.published_at,
    }
}

fn complex_anchor_summary_response(summary: &ComplexAnchorSummary) -> ComplexAnchorSummaryResponse {
    ComplexAnchorSummaryResponse {
        complex_id: summary.complex_id.as_uuid(),
        position_source: summary.position_source.to_owned(),
        center_lng: summary.center_lng,
        center_lat: summary.center_lat,
        min_lng: summary.min_lng,
        min_lat: summary.min_lat,
        max_lng: summary.max_lng,
        max_lat: summary.max_lat,
        anchor_count: summary.anchor_count,
    }
}

fn parcel_response(parcel: &Parcel) -> ParcelResponse {
    ParcelResponse {
        id: parcel.id.as_uuid(),
        pnu: parcel.pnu.as_str().to_owned(),
        kind: parcel.kind.map(|kind| kind.wire_name().to_owned()),
        area_m2: parcel.area_m2,
        version: parcel.version,
        updated_at: parcel.updated_at,
    }
}

fn unit_response(unit: &BuildingUnitRow) -> UnitResponse {
    UnitResponse {
        id: unit.id,
        parcel_id: unit.parcel_id,
        building_id: unit.building_id,
        building_name: unit.building_name.clone(),
        dong_name: unit.dong_name.clone(),
        ho_name: unit.ho_name.clone(),
        floor_label: unit.floor_label.clone(),
        exclusive_area_m2: unit.exclusive_area_m2,
        usage_name: unit.usage_name.clone(),
        structure_name: unit.structure_name.clone(),
    }
}

fn building_response(building: &Building) -> BuildingResponse {
    BuildingResponse {
        id: building.id.as_uuid(),
        parcel_id: building.parcel_id.as_uuid(),
        register_pk: building.register_pk.clone(),
        purpose_code: building.purpose_code.clone(),
        structure_code: building.structure_code.clone(),
        floor_area_m2: building.floor_area_m2,
        stories: building.stories,
        below_ground_floors: building.below_ground_floors,
        has_rooftop: building.has_rooftop,
        rooftop_area_m2: building.rooftop_area_m2,
        rooftop_usage: building.rooftop_usage.clone(),
        built_year: building.built_year,
        updated_at: building.updated_at,
    }
}

fn file_asset_response(asset: FileAsset) -> FileAssetResponse {
    FileAssetResponse {
        id: asset.id.as_uuid(),
        object_key: asset.object_key.as_str().to_owned(),
        mime_type: asset.mime_type,
        size_bytes: asset.size_bytes,
        checksum_sha256: asset.checksum_sha256,
        title: asset.title,
        visibility: asset.visibility.wire_name().to_owned(),
        version: asset.version,
        updated_at: asset.updated_at,
    }
}

fn complex_notice_response(
    notice: ComplexNotice,
    attachments: Vec<FileAsset>,
) -> ComplexNoticeResponse {
    ComplexNoticeResponse {
        id: notice.id.as_uuid(),
        complex_id: notice.complex_id.as_uuid(),
        notice_type: notice.notice_type.wire_name().to_owned(),
        title: notice.title,
        summary: notice.summary,
        published_at: notice.published_at,
        attachments: attachments.into_iter().map(file_asset_response).collect(),
        version: notice.version,
        updated_at: notice.updated_at,
    }
}

fn blueprint_response(blueprint: Blueprint) -> BlueprintResponse {
    BlueprintResponse {
        id: blueprint.id.as_uuid(),
        complex_id: blueprint.complex_id.as_uuid(),
        file_asset_id: blueprint.file_asset_id.as_uuid(),
        blueprint_kind: blueprint.blueprint_kind.wire_name().to_owned(),
        coordinate_system: blueprint.coordinate_system,
        scale: blueprint.scale,
        version: blueprint.version,
        updated_at: blueprint.updated_at,
    }
}

fn spatial_layer_response(layer: SpatialLayer) -> SpatialLayerResponse {
    SpatialLayerResponse {
        id: layer.id.as_uuid(),
        complex_id: layer.complex_id.as_uuid(),
        parcel_id: layer.parcel_id.map(|id| id.as_uuid()),
        blueprint_id: layer.blueprint_id.map(|id| id.as_uuid()),
        layer_kind: layer.layer_kind.wire_name().to_owned(),
        geometry_object_key: layer.geometry_object_key.map(|key| key.as_str().to_owned()),
        version: layer.version,
        updated_at: layer.updated_at,
    }
}

fn digital_twin_asset_response(asset: DigitalTwinAsset) -> DigitalTwinAssetResponse {
    DigitalTwinAssetResponse {
        id: asset.id.as_uuid(),
        complex_id: asset.complex_id.as_uuid(),
        parcel_id: asset.parcel_id.map(|id| id.as_uuid()),
        building_id: asset.building_id.map(|id| id.as_uuid()),
        file_asset_id: asset.file_asset_id.as_uuid(),
        asset_kind: asset.asset_kind.wire_name().to_owned(),
        coordinate_transform: asset.coordinate_transform,
        version: asset.version,
        updated_at: asset.updated_at,
    }
}

fn industry_group_response(
    group: IndustryGroup,
    members: &[IndustryGroupMember],
) -> IndustryGroupResponse {
    IndustryGroupResponse {
        id: group.id.as_uuid(),
        complex_id: group.complex_id.as_uuid(),
        name: group.name,
        description: group.description,
        members: members
            .iter()
            .filter(|member| member.industry_group_id == group.id)
            .map(|member| IndustryGroupMemberResponse {
                industry_code: member.industry_code.clone(),
                industry_code_system: member.industry_code_system.wire_name().to_owned(),
            })
            .collect(),
        version: group.version,
        updated_at: group.updated_at,
    }
}

fn parcel_industry_assignment_response(
    assignment: &ParcelIndustryAssignment,
) -> ParcelIndustryAssignmentResponse {
    ParcelIndustryAssignmentResponse {
        id: assignment.id.as_uuid(),
        parcel_id: assignment.parcel_id.as_uuid(),
        industry_group_id: assignment.industry_group_id.as_uuid(),
        assignment_kind: assignment.assignment_kind.wire_name().to_owned(),
        version: assignment.version,
        updated_at: assignment.updated_at,
    }
}

fn vector_tile_manifest_response(manifest: VectorTileManifest) -> VectorTileManifestResponse {
    let artifacts = manifest
        .artifacts
        .into_iter()
        .map(|artifact| {
            let layer = artifact.layer.clone();
            (layer, vector_tile_artifact_response(artifact))
        })
        .collect::<BTreeMap<_, _>>();

    VectorTileManifestResponse {
        schema_version: 1,
        current_version: manifest.current_version,
        previous_version: manifest.previous_version,
        tiles_url_template: manifest.tiles_url_template.as_str().to_owned(),
        source_snapshot_id: manifest.source_snapshot_id,
        published_at: manifest.published_at,
        artifacts,
    }
}

fn vector_tile_runtime_manifest_response(
    manifest: VectorTileRuntimeManifest,
) -> VectorTileRuntimeManifestResponse {
    let publication_units = manifest
        .publication_units
        .into_iter()
        .map(|(unit_key, unit)| {
            let source = match unit.source {
                ActiveTileSource::DynamicPostgis(source) => {
                    VectorTileRuntimeSourceResponse::DynamicPostgis(
                        VectorTileDynamicPostgisResponse {
                            martin_source_id: source.martin_source_id,
                            tiles_url_template: source.tiles_url_template.as_str().to_owned(),
                            cache_policy: source.cache_policy,
                        },
                    )
                }
                ActiveTileSource::StaticPmtiles(source) => {
                    VectorTileRuntimeSourceResponse::StaticPmtiles(
                        VectorTileStaticPmtilesResponse {
                            martin_source_id: source.martin_source_id,
                            tiles_url_template: source.tiles_url_template.as_str().to_owned(),
                            pmtiles_object_key: source.pmtiles_object_key,
                            pmtiles_file_asset_id: source.pmtiles_file_asset_id.as_uuid(),
                            pmtiles_sha256: source.pmtiles_sha256,
                            pmtiles_bytes: source.pmtiles_bytes,
                        },
                    )
                }
            };
            let layers = unit
                .layers
                .into_iter()
                .map(|(layer_id, layer)| {
                    (
                        layer_id,
                        VectorTileRuntimeLayerResponse {
                            source_layer: layer.source_layer,
                            feature_id_property: layer.feature_id_property.as_str().to_owned(),
                            tile_min_zoom: layer.tile_min_zoom,
                            tile_max_zoom: layer.tile_max_zoom,
                            render_min_zoom: layer.render_min_zoom,
                            render_max_zoom: layer.render_max_zoom,
                            feature_filter_properties: layer.feature_filter_properties,
                        },
                    )
                })
                .collect();
            (
                unit_key,
                VectorTilePublicationUnitResponse {
                    data_revision: unit.data_revision.as_uuid(),
                    serving_generation: unit.serving_generation.value(),
                    active_release_id: unit.active_release_id.as_uuid(),
                    canonical_iceberg_snapshot_id: unit
                        .canonical_iceberg_snapshot_id
                        .as_str()
                        .to_owned(),
                    source,
                    layers,
                    lineage: VectorTileRuntimeLineageResponse {
                        source_record_id: unit.lineage.source_record_id.as_uuid(),
                        source_file_asset_ids: unit
                            .lineage
                            .source_file_asset_ids
                            .into_iter()
                            .map(|id| id.as_uuid())
                            .collect(),
                    },
                },
            )
        })
        .collect();
    VectorTileRuntimeManifestResponse {
        schema_version: 2,
        current_version: manifest.current_version.as_uuid(),
        manifest_generation: manifest.manifest_generation.value(),
        refresh_after_seconds: manifest.refresh_after_seconds,
        published_at: manifest.published_at,
        publication_units,
    }
}

fn parcel_marker_anchor_rebuild_response(
    report: catalog_application::ports::ParcelMarkerAnchorRebuildReport,
) -> ParcelMarkerAnchorRebuildResponse {
    ParcelMarkerAnchorRebuildResponse {
        generation_run_id: report.generation_run_id,
        source_snapshot_id: report.source_snapshot_id,
        source_table: report.source_table,
        algorithm: report.algorithm.wire_name().to_owned(),
        algorithm_version: report.algorithm_version,
        scanned_row_count: report.scanned_row_count,
        loaded_row_count: report.loaded_row_count,
        rejected_row_count: report.rejected_row_count,
        superseded_row_count: report.superseded_row_count,
    }
}

fn vector_tile_artifact_response(artifact: VectorTileArtifact) -> VectorTileArtifactResponse {
    let feature_filter_properties = artifact.feature_filter_properties();

    VectorTileArtifactResponse {
        source_layer: artifact.source_layer,
        tile_min_zoom: artifact.tile_zoom.min(),
        tile_max_zoom: artifact.tile_zoom.max(),
        render_min_zoom: artifact.render_zoom.min(),
        render_max_zoom: artifact.render_zoom.max(),
        tilejson_object_key: artifact.tilejson_object_key.as_str().to_owned(),
        object_key_prefix: artifact.object_key_prefix.as_str().to_owned(),
        flat_tile_count: artifact.flat_tile_count,
        flat_tile_total_bytes: artifact.flat_tile_total_bytes,
        feature_filter_properties,
        lineage: VectorTileLineageResponse {
            source_record_id: artifact.lineage.source_record_id.as_uuid(),
            manifest_file_asset_id: artifact.lineage.manifest_file_asset_id.as_uuid(),
            tilejson_file_asset_id: artifact.lineage.tilejson_file_asset_id.as_uuid(),
            source_file_asset_ids: artifact
                .lineage
                .source_file_asset_ids
                .into_iter()
                .map(|id| id.as_uuid())
                .collect(),
        },
    }
}

fn source_record_command(request: PromoteSourceRecordRequest) -> VectorTileSourceRecordCommand {
    VectorTileSourceRecordCommand {
        source: request.source,
        source_url: request.source_url,
        external_id: request.external_id,
        checksum_sha256: request.checksum_sha256,
        raw_object_key: request.raw_object_key,
    }
}

fn file_asset_command(request: PromoteFileAssetRequest) -> VectorTileFileAssetCommand {
    VectorTileFileAssetCommand {
        object_key: request.object_key,
        mime_type: request.mime_type,
        size_bytes: request.size_bytes,
        checksum_sha256: request.checksum_sha256,
        title: request.title,
        visibility: request.visibility,
    }
}

fn artifact_command(
    request: PromoteVectorTileArtifactRequest,
) -> VectorTileArtifactPromotionCommand {
    VectorTileArtifactPromotionCommand {
        source_layer: request.source_layer,
        tile_min_zoom: request.tile_min_zoom,
        tile_max_zoom: request.tile_max_zoom,
        render_min_zoom: request.render_min_zoom,
        render_max_zoom: request.render_max_zoom,
        tilejson_file_asset: file_asset_command(request.tilejson_file_asset),
        object_key_prefix: request.object_key_prefix,
        flat_tile_count: request.flat_tile_count,
        flat_tile_total_bytes: request.flat_tile_total_bytes,
        source_file_assets: request
            .source_file_assets
            .into_iter()
            .map(file_asset_command)
            .collect(),
    }
}

fn require_exact_manifest_action_path(path: &str, expected: &str) -> Result<(), ApiError> {
    if path == expected {
        Ok(())
    } else {
        Err(ApiError::NotFound(path.to_owned()))
    }
}

fn parse_marker_tile_y_pbf(raw: &str) -> Result<u32, ApiError> {
    let y = raw
        .strip_suffix(".pbf")
        .ok_or_else(|| ApiError::BadRequest("marker tile path must end with .pbf".to_owned()))?;
    y.parse::<u32>()
        .map_err(|error| ApiError::BadRequest(format!("invalid marker tile y coordinate: {error}")))
}

/// HTTP 응답으로 매핑되는 공용 에러.
fn db_reference_marker_tile_enabled() -> bool {
    db_reference_marker_tile_enabled_from_vars(|key| std::env::var(key).ok())
}

fn db_reference_marker_tile_enabled_from_vars(lookup: impl Fn(&str) -> Option<String>) -> bool {
    if let Some(raw) = lookup(DB_MARKER_TILE_REFERENCE_ENABLED_ENV) {
        return matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        );
    }

    let runtime_env = lookup(FOUNDATION_PLATFORM_RUNTIME_ENV);
    !is_production_runtime_name(runtime_env.as_deref())
}

fn is_production_runtime_name(runtime_env: Option<&str>) -> bool {
    runtime_env.map(str::trim).is_some_and(|value| {
        value.eq_ignore_ascii_case("production") || value.eq_ignore_ascii_case("prod")
    })
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

impl From<CatalogError> for ApiError {
    fn from(err: CatalogError) -> Self {
        match err {
            CatalogError::ComplexNotFound(id)
            | CatalogError::ParcelNotFound(id)
            | CatalogError::VectorTileManifestNotFound(id) => Self::NotFound(id),
            CatalogError::ComplexOfficialCodeConflict(p) | CatalogError::ParcelPnuConflict(p) => {
                Self::Conflict(p)
            }
            CatalogError::ComplexAlreadyArchived(id) | CatalogError::ComplexStateConflict(id) => {
                Self::Conflict(id)
            }
            CatalogError::VectorTileManifestAlreadyExists(version) => Self::Conflict(version),
            CatalogError::FileAssetObjectKeyConflict(object_key) => Self::Conflict(object_key),
            CatalogError::ComplexVersionConflict { .. } => {
                Self::Conflict("version mismatch".into())
            }
            CatalogError::VectorTileManifestVersionConflict { expected, current } => {
                Self::Conflict(format!(
                    "vector tile manifest version mismatch: expected_current_version={expected}, current={current}"
                ))
            }
            CatalogError::VectorTileServingStateConflict {
                unit_key,
                expected,
                current,
            } => Self::Conflict(format!(
                "vector tile serving state mismatch for {unit_key}: expected {expected}, current {current}"
            )),
            // 409 for both, following the status Stripe serves `idempotency_key_in_use` and the IETF
            // Idempotency-Key draft serves a concurrent retry on. 422 would arguably fit the reuse
            // case, but `ApiError` has no 422 and inventing one for a route that does not exist yet
            // would be shaping transport for an unwritten caller.
            CatalogError::MutationIdempotencyKeyReused {
                idempotency_key,
                command_kind,
            } => Self::Conflict(format!(
                "idempotency key {idempotency_key} was already used by {command_kind} for a different request; use a new key"
            )),
            CatalogError::MutationContended { idempotency_key } => Self::Conflict(format!(
                "idempotency key {idempotency_key} is in use by another request; retry with the same key"
            )),
            CatalogError::MutationFingerprintVersionChanged {
                idempotency_key,
                recorded,
                current,
            } => Self::Conflict(format!(
                "idempotency key {idempotency_key} was recorded under request fingerprint {recorded}; this deployment computes {current}, so the two cannot be compared. Use a new key."
            )),
            CatalogError::InvalidPnu(e) => Self::BadRequest(e.to_string()),
            CatalogError::InvalidVectorTileManifestRollback(msg)
            | CatalogError::InvalidVectorTileManifestPromotion(msg)
            | CatalogError::InvalidVectorTileRuntimeManifest(msg)
            | CatalogError::InvalidIndustrialComplexInput(msg)
            | CatalogError::InvalidParcelMarkerAnchorRebuild(msg) => Self::BadRequest(msg),
            CatalogError::Infrastructure(msg) => Self::Internal(msg),
        }
    }
}

impl From<LakehouseError> for ApiError {
    fn from(error: LakehouseError) -> Self {
        match error {
            LakehouseError::InvalidContract(message)
            | LakehouseError::InvalidLakehouseBatchRun(message)
            | LakehouseError::InvalidLakehouseRegistryInput(message) => Self::BadRequest(message),
            LakehouseError::IndustrialComplexGoldPointerVersionConflict { expected, current } => {
                Self::Conflict(format!(
                    "industrial complex gold pointer version mismatch: expected_current_version={expected:?}, current={current:?}"
                ))
            }
            LakehouseError::IndustrialComplexNotFound(id) => Self::NotFound(id),
            LakehouseError::ObjectKeyConflict(object_key) => Self::Conflict(object_key),
            LakehouseError::Persistence(message) | LakehouseError::Upstream(message) => {
                Self::Internal(message)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
#[path = "catalog_tests.rs"]
mod tests;
