//! Contract tests for marker tile DTOs.

use foundation_contracts::catalog::{MarkerTileContractResponse, MarkerTileFeatureResponse};

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
