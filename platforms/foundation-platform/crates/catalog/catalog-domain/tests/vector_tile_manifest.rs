//! Contract tests for vector tile manifest domain value objects.

use catalog_domain::{vector_tile_feature_filter_properties, TilesUrlTemplate, ZoomRange};
use foundation_shared_kernel::ObjectKeyPrefix;

#[test]
fn tiles_url_template_requires_all_runtime_placeholders() {
    assert!(TilesUrlTemplate::parse(
        "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf"
    )
    .is_ok());

    assert!(
        TilesUrlTemplate::parse("https://static.example.com/gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/{z}/{x}/{y}.pbf").is_err()
    );
    assert!(
        TilesUrlTemplate::parse("https://static.example.com/{object_key_prefix}/{z}/{x}.pbf")
            .is_err()
    );
    assert!(
        TilesUrlTemplate::parse("https://static.example.com/{object_key_prefix}/{x}/{y}.pbf")
            .is_err()
    );
}

#[test]
fn zoom_range_rejects_inverted_or_out_of_bounds_values(
) -> Result<(), catalog_domain::ZoomRangeError> {
    let range = ZoomRange::new(8, 16)?;
    assert_eq!(range.min(), 8);
    assert_eq!(range.max(), 16);

    assert!(ZoomRange::new(16, 8).is_err());
    assert!(ZoomRange::new(0, 25).is_err());
    Ok(())
}

#[test]
fn object_key_prefix_allows_directory_prefix_but_rejects_ambiguous_paths() {
    assert!(ObjectKeyPrefix::parse(
        "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels/"
    )
    .is_ok());
    assert!(ObjectKeyPrefix::parse(
        "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels"
    )
    .is_ok());
    assert!(ObjectKeyPrefix::parse("").is_err());
    assert!(ObjectKeyPrefix::parse(
        "/gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels/"
    )
    .is_err());
    assert!(ObjectKeyPrefix::parse("gold/../parcels/").is_err());
    assert!(ObjectKeyPrefix::parse("gold\\v42\\parcels\\").is_err());
}

#[test]
fn foundation_platform_reference_layers_advertise_safe_feature_filter_properties() {
    let complex = vector_tile_feature_filter_properties("complex");
    assert_eq!(
        complex.get("official_complex_code").map(String::as_str),
        Some("official_complex_code")
    );

    let parcels = vector_tile_feature_filter_properties("parcels");
    assert_eq!(parcels.get("pnu").map(String::as_str), Some("pnu"));

    assert!(vector_tile_feature_filter_properties("listing").is_empty());
}
