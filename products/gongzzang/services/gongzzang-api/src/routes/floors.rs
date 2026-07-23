//! `GET /api/floors?parcel_pnu=:pnu` per-building floor summary route.
//!
//! Gongzzang owns the B2C route contract and user-facing response shape.
//! Canonical catalog building/floor data is read through Foundation Platform via the
//! same `BuildingRegisterReader` port used by `/api/buildings`; this route just
//! reshapes it into the 지상/지하/옥탑 summary the parcel 층 구성 card renders.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use product_identity_infrastructure::middleware::AuthenticatedUser;
use serde::{Deserialize, Serialize};
use shared_kernel::pnu::Pnu;

use crate::http::problem::{problem, ProblemResponse};
use crate::routes::buildings::{BuildingRegisterRecord, BuildingsState};

/// Query parameters for `GET /api/floors`.
#[derive(Debug, Deserialize)]
pub struct FloorsQuery {
    /// Parcel PNU, 19 digits.
    pub parcel_pnu: String,
}

/// Floor summary for one building.
#[derive(Debug, Serialize)]
pub struct FloorBuildingResponse {
    /// Building identifier from Foundation Platform.
    pub id: String,
    /// Building name, empty when Foundation Platform has no route-facing name.
    pub name: String,
    /// Number of above-ground floors.
    pub above_ground: u8,
    /// Number of below-ground (basement) floors.
    pub below_ground: u8,
    /// Whether the building has a rooftop (옥탑) structure counted as a floor.
    pub has_rooftop: bool,
    /// 옥탑 공용부 allocated area (㎡) reconciled from 전유공용면적. Omitted when the
    /// building has no rooftop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rooftop_area_m2: Option<f64>,
    /// 옥탑 용도 (주용도 · 기타용도). Empty when the building has no rooftop.
    pub rooftop_usage: String,
}

/// Floor summary list response.
#[derive(Debug, Serialize)]
pub struct FloorsResponse {
    /// One entry per building on the parcel. Empty when none are available.
    pub buildings: Vec<FloorBuildingResponse>,
}

/// Handles `GET /api/floors?parcel_pnu=...`.
///
/// # Errors
///
/// - `400 invalid-pnu` when the PNU is malformed.
/// - `502 floors-lookup-failed` when the Foundation Platform building lookup fails.
pub async fn list_floors(
    State(state): State<BuildingsState>,
    _auth: AuthenticatedUser,
    Query(q): Query<FloorsQuery>,
) -> Result<Json<FloorsResponse>, ProblemResponse> {
    let pnu = Pnu::try_new(&q.parcel_pnu).map_err(|e| {
        problem(
            "invalid-pnu",
            "잘못된 필지 PNU 에요",
            StatusCode::BAD_REQUEST,
            Some(format!("{e}")),
        )
    })?;

    let buildings = state.reader.list_by_pnu(&pnu).await.map_err(|e| {
        tracing::warn!(error = %e, pnu = %q.parcel_pnu, "floor summary read failed");
        problem(
            "floors-lookup-failed",
            "층 정보를 불러오지 못했어요",
            StatusCode::BAD_GATEWAY,
            None,
        )
    })?;

    Ok(Json(to_floor_summary(buildings)))
}

/// Reshapes route-facing building records into the parcel floor summary. Pure so
/// it can be unit-tested without the auth extractor or a live reader.
fn to_floor_summary(buildings: Vec<BuildingRegisterRecord>) -> FloorsResponse {
    FloorsResponse {
        buildings: buildings
            .into_iter()
            .map(|b| FloorBuildingResponse {
                id: b.id,
                name: b.name,
                above_ground: b.above_ground_floors,
                below_ground: b.below_ground_floors,
                has_rooftop: b.has_rooftop,
                rooftop_area_m2: b.rooftop_area_m2,
                rooftop_usage: b.rooftop_usage,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, above: u8, below: u8, rooftop: bool) -> BuildingRegisterRecord {
        BuildingRegisterRecord {
            id: id.to_owned(),
            name: String::new(),
            address: None,
            purpose: "02000".to_owned(),
            structure: "11".to_owned(),
            plot_area_m2: None,
            building_area_m2: None,
            building_coverage_ratio: None,
            total_area_m2: 1234.5,
            floor_area_ratio: None,
            above_ground_floors: above,
            below_ground_floors: below,
            has_rooftop: rooftop,
            rooftop_area_m2: rooftop.then_some(13.87),
            rooftop_usage: if rooftop {
                "기타제2종근린생활시설 · 주차장".to_owned()
            } else {
                String::new()
            },
            height_m: None,
            passenger_elevators: None,
            emergency_elevators: None,
            indoor_self_parking: None,
            outdoor_self_parking: None,
            annex_building_count: None,
            annex_building_area_m2: None,
            permitted_at: None,
            started_at: None,
            approved_at: None,
        }
    }

    #[test]
    fn maps_building_records_into_floor_summary() {
        let response = to_floor_summary(vec![
            record("building-01", 11, 2, true),
            record("building-02", 3, 0, false),
        ]);

        assert_eq!(response.buildings.len(), 2);
        let first = &response.buildings[0];
        assert_eq!(first.id, "building-01");
        assert_eq!(first.above_ground, 11);
        assert_eq!(first.below_ground, 2);
        assert!(first.has_rooftop);
        assert_eq!(first.rooftop_area_m2, Some(13.87));
        assert_eq!(first.rooftop_usage, "기타제2종근린생활시설 · 주차장");
        let second = &response.buildings[1];
        assert_eq!(second.above_ground, 3);
        assert_eq!(second.below_ground, 0);
        assert!(!second.has_rooftop);
        assert_eq!(second.rooftop_area_m2, None);
        assert_eq!(second.rooftop_usage, "");
    }

    #[test]
    fn empty_building_list_yields_empty_summary() {
        let response = to_floor_summary(Vec::new());
        assert!(response.buildings.is_empty());
    }
}
