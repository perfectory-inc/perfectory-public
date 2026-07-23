use super::*;

#[test]
fn cors_production_without_explicit_origins_allows_no_origins() {
    let origins = super::cors_allowed_origins_from(None, Some("production"));

    assert!(origins.is_empty());
}

#[test]
fn cors_default_local_origins_include_gongzzang_preview() -> Result<(), Box<dyn Error>> {
    let origins = super::cors_allowed_origins_from(None, Some("development"));
    let origin_strings = origins
        .iter()
        .map(HeaderValue::to_str)
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        origin_strings,
        vec![
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:3900",
            "http://127.0.0.1:3900",
        ]
    );
    Ok(())
}

#[test]
fn cors_invalid_local_origins_fall_back_to_default_local_origins() -> Result<(), Box<dyn Error>> {
    let origins = super::cors_allowed_origins_from(Some("\u{7f}"), Some("development"));
    let origin_strings = origins
        .iter()
        .map(HeaderValue::to_str)
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        origin_strings,
        vec![
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:3900",
            "http://127.0.0.1:3900",
        ]
    );
    Ok(())
}

#[test]
fn canonical_route_label_bounds_dynamic_metric_cardinality() {
    assert_eq!(
        super::canonical_route_label("/catalog/v1/complexes/abc/parcels"),
        "/catalog/v1/complexes/{id}/parcels"
    );
    assert_eq!(
        super::canonical_route_label("/catalog/v1/complexes/abc/archive"),
        "/catalog/v1/complexes/{id}/archive"
    );
    assert_eq!(
        super::canonical_route_label("/catalog/v1/complexes/abc/anchor-summary"),
        "/catalog/v1/complexes/{id}/anchor-summary"
    );
    assert_eq!(
        super::canonical_route_label("/catalog/v1/vector-tiles/manifest:rollback"),
        "/catalog/v1/vector-tiles/manifest:action"
    );
    assert_eq!(
        super::canonical_route_label("/catalog/v1/parcel-marker-anchors:rebuild"),
        "/catalog/v1/parcel-marker-anchors:rebuild"
    );
    assert_eq!(
        super::canonical_route_label("/map/v1/marker-tiles/contract"),
        "/map/v1/marker-tiles/contract"
    );
    assert_eq!(
        super::canonical_route_label("/map/v1/marker-tiles/parcel_anchor/12/3494/1591.pbf"),
        "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf"
    );
    assert_eq!(
        super::canonical_route_label("/catalog/v1/parcels/by-pnu/9999900101100090000/buildings"),
        "/catalog/v1/parcels/by-pnu/{pnu}/buildings"
    );
    assert_eq!(
        super::canonical_route_label("/catalog/v1/parcels/by-pnu/9999900101100090000"),
        "/catalog/v1/parcels/by-pnu/{pnu}"
    );
    assert_eq!(
        super::canonical_route_label("/internal/lakehouse/artifacts"),
        "/internal/lakehouse/artifacts"
    );
    assert_eq!(
        super::canonical_route_label("/internal/normalization/proposals"),
        "/internal/normalization/proposals"
    );
    assert_eq!(
        super::canonical_route_label(
            "/catalog/v1/normalization/proposals/018f7c6a-0000-7000-8000-000000000001/approve"
        ),
        "/catalog/v1/normalization/proposals/{id}/{action}"
    );
    assert_eq!(
        super::canonical_route_label(
            "/catalog/v1/normalization/proposals/018f7c6a-0000-7000-8000-000000000001/reject"
        ),
        "/catalog/v1/normalization/proposals/{id}/{action}"
    );
    assert_eq!(
        super::canonical_route_label(
            "/catalog/v1/normalization/proposals/018f7c6a-0000-7000-8000-000000000001/apply"
        ),
        "/catalog/v1/normalization/proposals/{id}/{action}"
    );
    assert_eq!(
        super::canonical_route_label(
            "/catalog/v1/normalization/applications/018f7c6a-0000-7000-8000-000000000001/rollback"
        ),
        "/catalog/v1/normalization/applications/{id}/{action}"
    );
}
