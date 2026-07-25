//! Contract tests for the single-source v2 spatial publication manifest.

use catalog_domain::{
    validate_serving_transition, ServingGeneration, ServingSelection, ServingSourceKind,
    VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::VectorTileDataRevisionId;
use serde_json::json;
use uuid::Uuid;

fn valid_manifest() -> serde_json::Value {
    json!({
        "schema_version": 2,
        "current_version": "0196e7e0-3c20-7000-8000-000000000052",
        "manifest_generation": 108,
        "refresh_after_seconds": 4,
        "published_at": "2026-07-24T00:00:00Z",
        "publication_units": {
            "parcels": {
                "data_revision": "0196e7e0-3c20-7000-8000-000000000061",
                "serving_generation": 42,
                "active_release_id": "0196e7e0-3c20-7000-8000-000000000062",
                "canonical_iceberg_snapshot_id": "70000000000000001",
                "source": {
                    "kind": "static_pmtiles",
                    "martin_source_id": "parcels-0196e7e0-3c20-7000-8000-000000000062",
                    "tiles_url_template": "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
                    "pmtiles_object_key": "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels-0196e7e0-3c20-7000-8000-000000000062.pmtiles",
                    "pmtiles_file_asset_id": "0196e7e0-3c20-7000-8000-000000000063",
                    "pmtiles_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "pmtiles_bytes": 987_654_321
                },
                "layers": {
                    "parcels": {
                        "source_layer": "parcels",
                        "feature_id_property": "pnu",
                        "tile_min_zoom": 8,
                        "tile_max_zoom": 16,
                        "render_min_zoom": 10,
                        "render_max_zoom": 22,
                        "feature_filter_properties": {"pnu": "pnu"}
                    }
                },
                "lineage": {
                    "source_record_id": "0196e7e0-3c20-7000-8000-000000000064",
                    "source_file_asset_ids": ["0196e7e0-3c20-7000-8000-000000000065"]
                }
            }
        }
    })
}

#[test]
fn v2_manifest_accepts_one_complete_static_source() -> Result<(), String> {
    let manifest: VectorTileRuntimeManifest =
        serde_json::from_value(valid_manifest()).map_err(|error| error.to_string())?;
    assert_eq!(manifest.schema_version, 2);
    manifest.validate()?;
    Ok(())
}

#[test]
fn v2_manifest_rejects_unknown_source_and_dynamic_cache_busting_query() {
    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["kind"] = json!("overlay");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["kind"] = json!("dynamic_postgis");
    value["publication_units"]["parcels"]["source"] = json!({
        "kind": "dynamic_postgis",
        "martin_source_id": "parcels",
        "tiles_url_template": "http://127.0.0.1:3000/parcels/{z}/{x}/{y}",
        "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
        "cache_policy": "no_store"
    });
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_ok());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"] = json!({
        "kind": "dynamic_postgis",
        "martin_source_id": "parcels",
        "tiles_url_template": "http://127.0.0.1:3000/parcels/{z}/{x}/{y}?generation=42",
        "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
        "cache_policy": "no_store"
    });
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["tiles_url_template"] =
        serde_json::json!("http://127.0.0.1:3000/not-parcels/{z}/{x}/{y}");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());
}

#[test]
fn v2_manifest_rejects_javascript_unsafe_generation_and_bad_snapshot() {
    let mut value = valid_manifest();
    value["manifest_generation"] = json!(9_007_199_254_740_992_u64);
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["canonical_iceberg_snapshot_id"] = json!("0");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());
}

#[test]
fn v2_manifest_rejects_duplicate_source_ids_and_unsafe_unit_names() {
    let mut duplicate = valid_manifest();
    duplicate["publication_units"]["parcels"]["source"]["martin_source_id"] =
        serde_json::json!("shared");
    duplicate["publication_units"]["anchors"] = duplicate["publication_units"]["parcels"].clone();
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(duplicate).is_err());

    let mut unsafe_name = valid_manifest();
    let parcels_unit = unsafe_name["publication_units"]["parcels"].clone();
    unsafe_name["publication_units"] = serde_json::json!({"bad/name": parcels_unit});
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(unsafe_name).is_err());
}

#[test]
fn v2_manifest_rejects_static_source_identity_that_does_not_match_release_filename() {
    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["martin_source_id"] =
        serde_json::json!("parcels");
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());

    let mut value = valid_manifest();
    value["publication_units"]["parcels"]["source"]["pmtiles_object_key"] = serde_json::json!(
        "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels.pmtiles"
    );
    assert!(serde_json::from_value::<VectorTileRuntimeManifest>(value).is_err());
}

fn selection(
    source_kind: ServingSourceKind,
    revision: u128,
    generation: u64,
) -> Result<ServingSelection, String> {
    Ok(ServingSelection {
        source_kind,
        data_revision: VectorTileDataRevisionId::new(Uuid::from_u128(revision)),
        serving_generation: ServingGeneration::new(generation)?,
    })
}

#[test]
fn serving_state_machine_allows_only_complete_source_transitions() -> Result<(), String> {
    let revision_a = 1;
    let revision_b = 2;

    assert!(validate_serving_transition(
        None,
        selection(ServingSourceKind::DynamicPostgis, revision_a, 1)?
    )
    .is_ok());

    let dynamic_a = selection(ServingSourceKind::DynamicPostgis, revision_a, 1)?;
    assert!(validate_serving_transition(
        Some(dynamic_a),
        selection(ServingSourceKind::StaticPmtiles, revision_a, 2)?
    )
    .is_ok());

    let static_a = selection(ServingSourceKind::StaticPmtiles, revision_a, 2)?;
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::DynamicPostgis, revision_b, 3)?
    )
    .is_ok());
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::DynamicPostgis, revision_a, 3)?
    )
    .is_ok());
    Ok(())
}

#[test]
fn serving_state_machine_rejects_stale_static_and_generation_gaps() -> Result<(), String> {
    let static_a = selection(ServingSourceKind::StaticPmtiles, 1, 7)?;
    let stale_static = selection(ServingSourceKind::StaticPmtiles, 2, 8)?;
    assert!(validate_serving_transition(Some(static_a), stale_static).is_err());

    assert!(
        validate_serving_transition(None, selection(ServingSourceKind::DynamicPostgis, 1, 2)?)
            .is_err()
    );
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::DynamicPostgis, 1, 9)?
    )
    .is_err());
    assert!(validate_serving_transition(
        Some(static_a),
        selection(ServingSourceKind::StaticPmtiles, 1, 8)?
    )
    .is_ok());
    Ok(())
}
