//! Building and unit reads, including annual official assessments.

use super::{ApiError, AppState, AuthorizedPrincipal};
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use catalog_application::ports::CatalogRepository;
use catalog_domain::{Building, CatalogError, UnitOfficialPriceRow};
use catalog_infrastructure::{BuildingUnitRow, UnitPageKey};
use foundation_contracts::catalog::{
    BuildingResponse, UnitOfficialPriceResponse, UnitPageResponse, UnitResponse,
};
use foundation_shared_kernel::pnu::Pnu;
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

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
    let prices = state
        .catalog_repo
        .list_unit_official_prices_by_pnu(&pnu)
        .await?;
    Ok(Json(unit_responses(&units, &prices)))
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
pub(super) fn encode_unit_cursor(unit: &BuildingUnitRow) -> String {
    use base64::Engine as _;
    let raw = format!("{}\u{1f}{}\u{1f}{}", unit.dong_name, unit.ho_name, unit.id);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

/// Decodes a caller-supplied cursor, refusing anything this API could not have issued.
pub(super) fn decode_unit_cursor(cursor: &str) -> Result<UnitPageKey, ApiError> {
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
    let building = state
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
    let parcel = state
        .catalog_repo
        .find_parcel_by_id(building.parcel_id)
        .await?
        .ok_or_else(|| ApiError::Internal("building parcel is absent".to_owned()))?;
    let prices = state
        .catalog_repo
        .list_unit_official_prices_by_pnu(&parcel.pnu)
        .await?;
    let items = unit_responses(&rows[..rows.len().min(page_len)], &prices);
    let next_cursor = if has_more {
        rows.get(page_len - 1).map(encode_unit_cursor)
    } else {
        None
    };

    Ok(Json(UnitPageResponse { items, next_cursor }))
}

pub(super) fn unit_responses(
    units: &[BuildingUnitRow],
    prices: &[UnitOfficialPriceRow],
) -> Vec<UnitResponse> {
    let mut histories: BTreeMap<(&str, &str), Vec<UnitOfficialPriceResponse>> = BTreeMap::new();
    for row in prices {
        histories
            .entry((&row.dong_name, &row.ho_name))
            .or_default()
            .push(UnitOfficialPriceResponse::from(&row.price));
    }
    for history in histories.values_mut() {
        history.sort_by_key(|price| std::cmp::Reverse(price.base_year));
    }
    units
        .iter()
        .map(|unit| {
            let mut response = unit_response(unit);
            response.official_price_history = histories
                .get(&(unit.dong_name.as_str(), unit.ho_name.as_str()))
                .cloned()
                .unwrap_or_default();
            response
        })
        .collect()
}

pub(super) fn unit_response(unit: &BuildingUnitRow) -> UnitResponse {
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
        official_price_history: Vec::new(),
    }
}

pub(super) fn building_response(building: &Building) -> BuildingResponse {
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
