//! Contract tests for the strict v2 runtime manifest DTO and dispatcher.

use foundation_contracts::catalog::{
    VectorTileManifestDocument, VectorTileRuntimeManifestResponse,
};
use serde_json::json;

fn valid_v2() -> serde_json::Value {
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
                    "kind": "dynamic_postgis",
                    "martin_source_id": "parcels",
                    "tiles_url_template": "http://127.0.0.1:3000/parcels/{z}/{x}/{y}",
                    "postgis_projection_revision": "0196e7e0-3c20-7000-8000-000000000063",
                    "cache_policy": "no_store"
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
fn exact_dispatcher_parses_v2_and_round_trips() {
    let document: VectorTileManifestDocument = serde_json::from_value(valid_v2()).unwrap();
    let VectorTileManifestDocument::V2(manifest) = &document else {
        panic!("expected v2 manifest")
    };
    assert_eq!(manifest.schema_version, 2);
    manifest.validate().unwrap();
    assert_eq!(serde_json::to_value(document).unwrap()["schema_version"], 2);
}

#[test]
fn exact_dispatcher_fails_closed_for_unknown_version_and_invalid_v2() {
    let mut unknown = valid_v2();
    unknown["schema_version"] = json!(3);
    assert!(serde_json::from_value::<VectorTileManifestDocument>(unknown).is_err());

    let mut invalid = valid_v2();
    invalid["refresh_after_seconds"] = json!(5);
    assert!(serde_json::from_value::<VectorTileManifestDocument>(invalid).is_err());
}

#[test]
fn dto_validation_is_available_before_endpoint_publish() {
    let value: VectorTileRuntimeManifestResponse = serde_json::from_value(valid_v2()).unwrap();
    value.validate().unwrap();
}
