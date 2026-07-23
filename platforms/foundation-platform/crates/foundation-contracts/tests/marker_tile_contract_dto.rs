//! Contract tests for marker tile DTOs.

use foundation_contracts::catalog::{
    ComplexAnchorSummaryResponse, MarkerTileContractResponse, MarkerTileFeatureResponse,
    ParcelMarkerAnchorRebuildRequest, ParcelMarkerAnchorRebuildResponse,
};
use uuid::Uuid;

#[test]
fn marker_tile_contract_response_exposes_pbf_and_pnu_anchor_contract(
) -> Result<(), serde_json::Error> {
    let response = MarkerTileContractResponse::pnu_anchor_pbf();

    let json = serde_json::to_value(response)?;

    assert_eq!(json["response_format"].as_str(), Some("mvt_pbf"));
    assert_eq!(json["position_source"].as_str(), Some("pnu_anchor"));
    assert_eq!(json["bbox_marker_runtime_forbidden"].as_bool(), Some(true));
    assert_eq!(
        json["dropped_marker_success_forbidden"].as_bool(),
        Some(true)
    );
    assert_eq!(
        json["launch_runtime_source"].as_str(),
        Some("r2_cdn_vector_tile_manifest")
    );
    assert_eq!(
        json["runtime_manifest_endpoint"].as_str(),
        Some("/catalog/v1/vector-tiles/manifest")
    );
    assert_eq!(
        json["db_reference_endpoint_launch_forbidden"].as_bool(),
        Some(true)
    );
    assert_eq!(
        json["db_reference_endpoint_scope"].as_str(),
        Some("diagnostics_bounded_proof_admin")
    );
    assert_eq!(json["aggregate_anchor_max_zoom"].as_u64(), Some(11));
    assert_eq!(json["exact_anchor_min_zoom"].as_u64(), Some(12));
    assert_eq!(
        json["endpoint_template"].as_str(),
        Some("/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}")
    );
    assert_eq!(json["supported_layers"][0].as_str(), Some("parcel_anchor"));
    assert_eq!(json["default_filter_hash"].as_str(), Some("all-active-v1"));
    assert!(json.get("bbox").is_none());
    assert!(json.get("bounds").is_none());
    Ok(())
}

#[test]
fn marker_tile_feature_response_uses_pnu_and_completeness_metadata() -> Result<(), serde_json::Error>
{
    let response = MarkerTileFeatureResponse {
        id: "listing:01HXZ".to_owned(),
        pnu: "9999900101100010001".to_owned(),
        kind: "listing".to_owned(),
        count: 7,
        rank: Some(10),
        detail_ref: "listing-group:9999900101100010001".to_owned(),
    };

    let json = serde_json::to_value(response)?;

    assert_eq!(json["pnu"].as_str(), Some("9999900101100010001"));
    assert_eq!(json["count"].as_u64(), Some(7));
    assert_eq!(
        json["detail_ref"].as_str(),
        Some("listing-group:9999900101100010001")
    );
    assert!(json.get("latitude").is_none());
    assert!(json.get("longitude").is_none());
    assert!(json.get("geom_point").is_none());
    Ok(())
}

#[test]
fn complex_anchor_summary_response_exposes_anchor_derived_center_without_bbox_input(
) -> Result<(), serde_json::Error> {
    let complex_id = Uuid::now_v7();
    let response = ComplexAnchorSummaryResponse {
        complex_id,
        position_source: "pnu_anchor".to_owned(),
        center_lng: 127.123_470_234_75,
        center_lat: 36.123_425,
        min_lng: 127.123_470,
        min_lat: 36.123_420,
        max_lng: 127.123_470_234_80,
        max_lat: 36.123_430,
        anchor_count: 2,
    };

    let json = serde_json::to_value(response)?;
    let complex_id_string = complex_id.to_string();

    assert_eq!(
        json["complex_id"].as_str(),
        Some(complex_id_string.as_str())
    );
    assert_eq!(json["position_source"].as_str(), Some("pnu_anchor"));
    assert_eq!(json["center_lng"].as_f64(), Some(127.123_470_234_75));
    assert_eq!(json["center_lat"].as_f64(), Some(36.123_425));
    assert_eq!(json["anchor_count"].as_u64(), Some(2));
    assert!(json.get("request_bbox").is_none());
    assert!(json.get("latitude").is_none());
    assert!(json.get("longitude").is_none());
    Ok(())
}

#[test]
fn parcel_marker_anchor_rebuild_contract_is_snapshot_and_algorithm_only(
) -> Result<(), serde_json::Error> {
    let request = ParcelMarkerAnchorRebuildRequest {
        source_snapshot_id: "iceberg:parcel-boundary-snapshot-20260522".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
    };

    let request_json = serde_json::to_value(request)?;

    assert_eq!(
        request_json["source_snapshot_id"].as_str(),
        Some("iceberg:parcel-boundary-snapshot-20260522")
    );
    assert_eq!(
        request_json["algorithm_version"].as_str(),
        Some("postgis-st_maximuminscribedcircle-v1")
    );
    assert!(request_json.get("bbox").is_none());
    assert!(request_json.get("bounds").is_none());
    assert!(request_json.get("latitude").is_none());
    assert!(request_json.get("longitude").is_none());

    let generation_run_id = Uuid::now_v7();
    let response = ParcelMarkerAnchorRebuildResponse {
        generation_run_id,
        source_snapshot_id: "iceberg:parcel-boundary-snapshot-20260522".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        algorithm: "polylabel".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        scanned_row_count: 100,
        loaded_row_count: 100,
        rejected_row_count: 0,
        superseded_row_count: 12,
    };

    let response_json = serde_json::to_value(response)?;
    let generation_run_id_string = generation_run_id.to_string();

    assert_eq!(
        response_json["generation_run_id"].as_str(),
        Some(generation_run_id_string.as_str())
    );
    assert_eq!(response_json["algorithm"].as_str(), Some("polylabel"));
    assert_eq!(response_json["scanned_row_count"].as_u64(), Some(100));
    assert_eq!(response_json["loaded_row_count"].as_u64(), Some(100));
    assert_eq!(response_json["rejected_row_count"].as_u64(), Some(0));
    assert_eq!(response_json["superseded_row_count"].as_u64(), Some(12));
    Ok(())
}
