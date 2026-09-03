//! `GET /api/buildings/:building_id/units` paginated unit list route (root ADR-0078 §2).
//!
//! Gongzzang owns the B2C route contract and user-facing response shape. Canonical
//! 전유부 호 data is read through Foundation Platform, which owns the page order and
//! the continuation cursor — this route passes the cursor through verbatim and never
//! parses, stores, or fabricates one.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use product_identity_infrastructure::middleware::AuthenticatedUser;
use serde::{Deserialize, Serialize};

use crate::http::problem::{problem, ProblemResponse};

/// Reader communication or parsing error surfaced to the route layer.
#[derive(Debug)]
pub enum BuildingUnitsError {
    /// The upstream refused the request as malformed (bad cursor or limit).
    UpstreamRejected {
        /// Upstream status code.
        status: u16,
    },
    /// The building does not exist upstream.
    BuildingNotFound,
    /// Transport, credential, or translation failure.
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// Building-units reader port returning one route-facing page.
///
/// Implementations:
/// - production: `services/gongzzang-api/src/building_reader.rs::FoundationPlatformBuildingUnitsReader`
/// - dev fallback: `startup.rs::NoOpBuildingUnitsReader`
pub trait BuildingUnitsReader: Send + Sync {
    /// Lists one page of a building's units.
    ///
    /// # Errors
    ///
    /// Returns a reader error when the backing Foundation Platform call or response
    /// translation fails.
    fn list_page<'a>(
        &'a self,
        building_id: &'a str,
        limit: Option<u32>,
        cursor: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<BuildingUnitsPage, BuildingUnitsError>>
                + Send
                + 'a,
        >,
    >;
}

/// Shared state for `/api/buildings/:building_id/units`.
#[derive(Clone)]
pub struct BuildingUnitsState {
    /// Units reader port.
    pub reader: Arc<dyn BuildingUnitsReader>,
}

/// Route-facing unit record — the columns the unit panel draws.
#[derive(Debug, Clone, PartialEq)]
pub struct BuildingUnitRecord {
    /// Unit identifier from Foundation Platform.
    pub id: String,
    /// 동명칭 — only real 동 numbers; empty otherwise.
    pub dong_name: String,
    /// 호명칭.
    pub ho_name: String,
    /// Floor label (지상/지하 + number).
    pub floor_label: String,
    /// 전유면적 (m²). `None` when the register left it unmatched.
    pub exclusive_area_m2: Option<f64>,
    /// 주용도명. Empty when unmatched.
    pub usage_name: String,
}

/// One route-facing page of units.
#[derive(Debug, Clone, PartialEq)]
pub struct BuildingUnitsPage {
    /// Units in the upstream's stable order.
    pub units: Vec<BuildingUnitRecord>,
    /// Opaque continuation cursor from upstream; `None` on the last page.
    pub next_cursor: Option<String>,
}

/// Query parameters for `GET /api/buildings/:building_id/units`.
#[derive(Debug, Deserialize)]
pub struct BuildingUnitsQuery {
    /// Page size; upstream owns the default and the bound.
    pub limit: Option<u32>,
    /// Opaque continuation cursor from a previous page's `next_cursor`.
    pub cursor: Option<String>,
}

/// HTTP response shape for one unit.
#[derive(Debug, Serialize)]
pub struct BuildingUnitResponse {
    /// Unit identifier from Foundation Platform.
    pub id: String,
    /// 동명칭 — empty when the register named no 동.
    pub dong_name: String,
    /// 호명칭.
    pub ho_name: String,
    /// Floor label.
    pub floor_label: String,
    /// 전유면적 (m²). Absent when the register left it unmatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_area_m2: Option<f64>,
    /// 주용도명. Empty when unmatched.
    pub usage_name: String,
}

/// Unit page response.
#[derive(Debug, Serialize)]
pub struct BuildingUnitsResponse {
    /// The page of units. Empty when the building has none.
    pub units: Vec<BuildingUnitResponse>,
    /// Cursor for the next page; absent when this page is the last.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl From<BuildingUnitRecord> for BuildingUnitResponse {
    fn from(u: BuildingUnitRecord) -> Self {
        Self {
            id: u.id,
            dong_name: u.dong_name,
            ho_name: u.ho_name,
            floor_label: u.floor_label,
            exclusive_area_m2: u.exclusive_area_m2,
            usage_name: u.usage_name,
        }
    }
}

/// Handles `GET /api/buildings/:building_id/units`.
///
/// # Errors
///
/// - `400 invalid-units-request` when upstream refuses the cursor or limit.
/// - `404 building-not-found` when the building does not exist.
/// - `502 units-lookup-failed` when the Foundation Platform lookup fails.
pub async fn list_building_units(
    State(state): State<BuildingUnitsState>,
    _auth: AuthenticatedUser,
    Path(building_id): Path<String>,
    Query(q): Query<BuildingUnitsQuery>,
) -> Result<Json<BuildingUnitsResponse>, ProblemResponse> {
    let page = state
        .reader
        .list_page(&building_id, q.limit, q.cursor.as_deref())
        .await
        .map_err(|e| match e {
            BuildingUnitsError::UpstreamRejected { status } => problem(
                "invalid-units-request",
                "호실 목록 요청이 올바르지 않아요",
                StatusCode::BAD_REQUEST,
                Some(format!("upstream status {status}")),
            ),
            BuildingUnitsError::BuildingNotFound => problem(
                "building-not-found",
                "해당 건물을 찾을 수 없어요",
                StatusCode::NOT_FOUND,
                None,
            ),
            BuildingUnitsError::Other(source) => {
                tracing::warn!(error = %source, building_id = %building_id, "unit page read failed");
                problem(
                    "units-lookup-failed",
                    "호실 목록을 불러오지 못했어요",
                    StatusCode::BAD_GATEWAY,
                    None,
                )
            }
        })?;

    Ok(Json(BuildingUnitsResponse {
        units: page.units.into_iter().map(Into::into).collect(),
        next_cursor: page.next_cursor,
    }))
}
