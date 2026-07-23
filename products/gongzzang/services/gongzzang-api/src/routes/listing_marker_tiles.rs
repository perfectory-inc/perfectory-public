//! Gongzzang-owned listing marker PBF tile route.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use listing_domain::repository::{
    ListingMarkerFilter, ListingMarkerTileQuery, LISTING_MARKER_TILE_CONTENT_TYPE,
};
use serde::Deserialize;

use crate::http::problem::{problem, ProblemResponse};
use crate::listing_marker_serving::ListingMarkerServingGateway;
use crate::routes::listing_marker_common::{is_stable_filter_hash, resolve_listing_marker_filter};

/// Shared state for listing marker tile routes.
#[derive(Clone)]
pub struct ListingMarkerTilesState {
    /// Gongzzang marker serving gateway.
    pub serving: Arc<ListingMarkerServingGateway>,
}

/// `GET /map/v1/marker-tiles/listing/:z/:x/:y.pbf` query parameters.
#[derive(Debug, Deserialize)]
pub struct ListingMarkerTileHttpQuery {
    /// Registered listing marker filter hash.
    pub filter_hash: Option<String>,
}

/// `GET /map/v1/marker-tiles/listing/:z/:x/:y.pbf`.
#[tracing::instrument(skip(state), fields(
    z = %z_raw,
    x = %x_raw,
    y = %y_pbf,
    filter_hash = ?q.filter_hash,
))]
pub async fn get_listing_marker_tile(
    State(state): State<ListingMarkerTilesState>,
    Path((z_raw, x_raw, y_pbf)): Path<(String, String, String)>,
    Query(q): Query<ListingMarkerTileHttpQuery>,
) -> Result<Response, ProblemResponse> {
    let filter_hash = q
        .filter_hash
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            problem(
                "map/listing-marker-filter-missing",
                "filter_hash is required",
                StatusCode::BAD_REQUEST,
                None,
            )
        })?;

    if !is_stable_filter_hash(filter_hash) {
        return Err(problem(
            "map/listing-marker-filter-malformed",
            "filter_hash is malformed",
            StatusCode::BAD_REQUEST,
            None,
        ));
    }

    let filter = resolve_listing_marker_filter(&state.serving, filter_hash).await?;
    let query = parse_listing_marker_tile_query(&z_raw, &x_raw, &y_pbf, filter)?;

    let tile = state
        .serving
        .find_listing_marker_tile(filter_hash, query)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "listing marker tile query failed");
            if e.to_string().contains("completeness violation")
                || e.to_string().contains("budget violation")
            {
                problem(
                    "map/listing-marker-tile-unrepresentable",
                    "listing marker tile cannot be represented truthfully",
                    StatusCode::UNPROCESSABLE_ENTITY,
                    None,
                )
            } else {
                problem(
                    "map/listing-marker-tile-unavailable",
                    "listing marker tile is unavailable",
                    StatusCode::SERVICE_UNAVAILABLE,
                    None,
                )
            }
        })?;

    tracing::info!(
        layer = tile.layer_name,
        filter_hash,
        anchor_snapshot_id = ?tile.anchor_snapshot_id,
        eligible_count = tile.eligible_count,
        represented_count = tile.represented_count,
        feature_count = tile.feature_count,
        aggregate_count = tile.aggregate_count,
        tile_byte_size = tile.bytes.len(),
        "listing marker tile encoded"
    );

    let mut response = (StatusCode::OK, tile.bytes).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(LISTING_MARKER_TILE_CONTENT_TYPE),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30, stale-while-revalidate=30"),
    );
    Ok(response)
}

fn parse_listing_marker_tile_query(
    z_raw: &str,
    x_raw: &str,
    y_pbf: &str,
    filter: ListingMarkerFilter,
) -> Result<ListingMarkerTileQuery, ProblemResponse> {
    let y_raw = y_pbf.strip_suffix(".pbf").ok_or_else(|| {
        problem(
            "map/listing-marker-tile-path",
            "tile path must end with .pbf",
            StatusCode::BAD_REQUEST,
            None,
        )
    })?;
    let z = z_raw.parse::<u8>().map_err(|e| {
        problem(
            "map/listing-marker-tile-coordinate",
            "tile z coordinate is invalid",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;
    let x = x_raw.parse::<u32>().map_err(|e| {
        problem(
            "map/listing-marker-tile-coordinate",
            "tile x coordinate is invalid",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;
    let y = y_raw.parse::<u32>().map_err(|e| {
        problem(
            "map/listing-marker-tile-coordinate",
            "tile y coordinate is invalid",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })?;
    ListingMarkerTileQuery::try_new(z, x, y, filter).map_err(|e| {
        problem(
            "map/listing-marker-tile-coordinate",
            "tile coordinate is outside the supported range",
            StatusCode::BAD_REQUEST,
            Some(e.to_string()),
        )
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn parses_listing_marker_tile_path_with_pbf_suffix() {
        let query = parse_listing_marker_tile_query(
            "14",
            "8780",
            "6345.pbf",
            ListingMarkerFilter::AllActive,
        )
        .expect("query");

        assert_eq!(query.z, 14);
        assert_eq!(query.x, 8780);
        assert_eq!(query.y, 6345);
        assert_eq!(query.filter, ListingMarkerFilter::AllActive);
    }

    #[test]
    fn parses_listing_marker_low_zoom_tile_path() {
        let query = parse_listing_marker_tile_query(
            "13",
            "4390",
            "3172.pbf",
            ListingMarkerFilter::AllActive,
        )
        .expect("low zoom listing marker tile must be accepted");

        assert_eq!(query.z, 13);
        assert_eq!(query.x, 4390);
        assert_eq!(query.y, 3172);
    }

    #[test]
    fn rejects_listing_marker_tile_path_without_pbf_suffix() {
        let err = parse_listing_marker_tile_query("8", "10", "11", ListingMarkerFilter::AllActive)
            .expect_err("z below listing marker tile minimum must be rejected");

        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validates_stable_filter_hash_charset() {
        assert!(is_stable_filter_hash("all-active-v1"));
        assert!(!is_stable_filter_hash("all active"));
        assert!(!is_stable_filter_hash("all/active"));
    }

    /// axum 0.8 (matchit 0.8) 라우팅 회귀 가드: `app.rs` 의 타일 등록 패턴
    /// `/map/v1/marker-tiles/listing/{z}/{x}/{y_pbf}` 가 실제 `.pbf` URL 과
    /// 매치되고, `.pbf` suffix 를 포함한 full-segment 가 `Path` 로 전달되는지 검증.
    #[tokio::test]
    async fn tile_route_pattern_matches_concrete_pbf_url() {
        use axum::body::{to_bytes, Body};
        use axum::http::Request;
        use axum::routing::get;
        use axum::Router;
        use tower::ServiceExt;

        #[allow(clippy::unused_async)] // axum `Handler` 는 async fn 시그니처 요구.
        async fn probe(Path((z, x, y_pbf)): Path<(String, String, String)>) -> String {
            format!("{z}|{x}|{y_pbf}")
        }

        let app: Router<()> =
            Router::new().route("/map/v1/marker-tiles/listing/{z}/{x}/{y_pbf}", get(probe));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/map/v1/marker-tiles/listing/14/8780/6345.pbf")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        assert_eq!(&body[..], b"14|8780|6345.pbf");
    }
}
