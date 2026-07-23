//! Contract tests for PNU-anchor backed marker tile domain value objects.

use catalog_domain::{
    ComplexAnchorSummary, MarkerAnchorAlgorithm, MarkerTileFeature, MarkerTileLayer,
    MarkerTileRequest, ParcelMarkerAnchor, PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT,
};
use chrono::Utc;
use foundation_shared_kernel::ids::ComplexId;
use foundation_shared_kernel::Pnu;

fn assert_f64_near(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < f64::EPSILON,
        "expected {actual} to equal {expected}"
    );
}

#[test]
fn marker_tile_contract_is_mvt_pbf_with_pnu_anchor_positions() {
    let contract = PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT;

    assert_eq!(contract.response_format, "mvt_pbf");
    assert_eq!(contract.position_source, "pnu_anchor");
    assert!(contract.bbox_marker_runtime_forbidden);
    assert!(contract.dropped_marker_success_forbidden);
    assert_eq!(
        contract.launch_runtime_source,
        "r2_cdn_vector_tile_manifest"
    );
    assert_eq!(
        contract.runtime_manifest_endpoint,
        "/catalog/v1/vector-tiles/manifest"
    );
    assert!(contract.db_reference_endpoint_launch_forbidden);
    assert_eq!(
        contract.db_reference_endpoint_scope,
        "diagnostics_bounded_proof_admin"
    );
    assert_eq!(contract.aggregate_anchor_max_zoom, 11);
    assert_eq!(contract.exact_anchor_min_zoom, 12);
    assert_eq!(
        contract.endpoint_template,
        "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}"
    );
}

#[test]
fn parcel_marker_anchor_requires_valid_coordinates_and_lineage(
) -> Result<(), Box<dyn std::error::Error>> {
    let pnu = Pnu::parse("9999900101100010001")?;
    let checksum = "a".repeat(64);
    let anchor = ParcelMarkerAnchor::new(
        pnu.clone(),
        127.123_470_234_50,
        36.123_456_5,
        MarkerAnchorAlgorithm::Polylabel,
        "polylabel-v1",
        "vworld-cadastral-2026-05",
        &checksum,
        Utc::now(),
    )?;

    assert_eq!(anchor.pnu, pnu);
    assert_eq!(anchor.algorithm.wire_name(), "polylabel");
    assert_eq!(anchor.source_geometry_checksum_sha256, checksum);

    assert!(ParcelMarkerAnchor::new(
        Pnu::parse("9999900101100010001")?,
        181.0,
        36.123_456_5,
        MarkerAnchorAlgorithm::Polylabel,
        "polylabel-v1",
        "vworld-cadastral-2026-05",
        "b".repeat(64),
        Utc::now(),
    )
    .is_err());
    assert!(ParcelMarkerAnchor::new(
        Pnu::parse("9999900101100010001")?,
        127.123_470_234_50,
        91.0,
        MarkerAnchorAlgorithm::Polylabel,
        "polylabel-v1",
        "vworld-cadastral-2026-05",
        "b".repeat(64),
        Utc::now(),
    )
    .is_err());
    assert!(ParcelMarkerAnchor::new(
        Pnu::parse("9999900101100010001")?,
        127.123_470_234_50,
        36.123_456_5,
        MarkerAnchorAlgorithm::Polylabel,
        "",
        "vworld-cadastral-2026-05",
        "b".repeat(64),
        Utc::now(),
    )
    .is_err());
    assert!(ParcelMarkerAnchor::new(
        Pnu::parse("9999900101100010001")?,
        127.123_470_234_50,
        36.123_456_5,
        MarkerAnchorAlgorithm::Polylabel,
        "polylabel-v1",
        "vworld-cadastral-2026-05",
        "not-a-sha256",
        Utc::now(),
    )
    .is_err());

    Ok(())
}

#[test]
fn complex_anchor_summary_uses_active_pnu_anchor_extent() -> Result<(), Box<dyn std::error::Error>>
{
    let complex_id = ComplexId::new(uuid::Uuid::now_v7());
    let summary = ComplexAnchorSummary::new(
        complex_id,
        127.123_470_234_75,
        36.123_425,
        127.123_470,
        36.123_420,
        127.123_470_234_80,
        36.123_430,
        2,
    )?;

    assert_eq!(summary.complex_id, complex_id);
    assert_eq!(summary.position_source, "pnu_anchor");
    assert_f64_near(summary.center_lng, 127.123_470_234_75);
    assert_f64_near(summary.center_lat, 36.123_425);
    assert_f64_near(summary.min_lng, 127.123_470);
    assert_f64_near(summary.min_lat, 36.123_420);
    assert_f64_near(summary.max_lng, 127.123_470_234_80);
    assert_f64_near(summary.max_lat, 36.123_430);
    assert_eq!(summary.anchor_count, 2);

    assert!(ComplexAnchorSummary::new(
        complex_id,
        127.123_470_234_75,
        36.123_425,
        127.123_470_234_80,
        36.123_420,
        127.123_470,
        36.123_430,
        2,
    )
    .is_err());
    assert!(ComplexAnchorSummary::new(
        complex_id,
        127.123_470_234_75,
        36.123_425,
        127.123_470,
        36.123_420,
        127.123_470_234_80,
        36.123_430,
        0
    )
    .is_err());
    Ok(())
}

#[test]
fn marker_tile_feature_preserves_completeness_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let feature = MarkerTileFeature::new(
        "listing:01HXZ",
        Pnu::parse("9999900101100010001")?,
        "listing",
        7,
        Some(10),
        "listing-group:9999900101100010001",
    )?;

    assert_eq!(feature.count, 7);
    assert_eq!(feature.detail_ref, "listing-group:9999900101100010001");

    assert!(MarkerTileFeature::new(
        "listing:01HXZ",
        Pnu::parse("9999900101100010001")?,
        "listing",
        0,
        None,
        "listing-group:9999900101100010001",
    )
    .is_err());
    assert!(MarkerTileFeature::new(
        "listing:01HXZ",
        Pnu::parse("9999900101100010001")?,
        "listing",
        1,
        None,
        "",
    )
    .is_err());

    Ok(())
}

#[test]
fn marker_tile_request_accepts_only_supported_tile_address_and_filter(
) -> Result<(), Box<dyn std::error::Error>> {
    let request = MarkerTileRequest::new("parcel_anchor", 14, 13_523, 6_159, "all-active-v1")?;

    assert_eq!(request.layer, MarkerTileLayer::ParcelAnchor);
    assert_eq!(request.layer.wire_name(), "parcel_anchor");
    assert_eq!(request.z, 14);
    assert_eq!(request.x, 13_523);
    assert_eq!(request.y, 6_159);
    assert_eq!(request.filter_hash, "all-active-v1");

    assert!(MarkerTileRequest::new("listing", 14, 13_523, 6_159, "all-active-v1").is_err());
    assert!(MarkerTileRequest::new("parcel_anchor", 11, 0, 0, "all-active-v1").is_err());
    assert!(MarkerTileRequest::new("parcel_anchor", 12, 3_494, 1_591, "all-active-v1").is_ok());
    assert!(MarkerTileRequest::new("parcel_anchor", 25, 13_523, 6_159, "all-active-v1").is_err());
    assert!(MarkerTileRequest::new("parcel_anchor", 1, 2, 0, "all-active-v1").is_err());
    assert!(MarkerTileRequest::new("parcel_anchor", 14, 13_523, 6_159, "bbox=bad").is_err());

    Ok(())
}
