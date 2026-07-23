//! Contract tests for vector tile manifest DTOs.

use std::collections::BTreeMap;

use chrono::Utc;
use foundation_contracts::catalog::{
    PromoteFileAssetRequest, PromoteSourceRecordRequest, PromoteVectorTileArtifactRequest,
    PromoteVectorTileManifestRequest, RollbackVectorTileManifestRequest,
    VectorTileArtifactResponse, VectorTileLineageResponse, VectorTileManifestResponse,
};
use uuid::Uuid;

#[test]
fn vector_tile_manifest_response_matches_runtime_contract_shape() -> Result<(), serde_json::Error> {
    let manifest_file_asset_id = Uuid::now_v7();
    let manifest_file_asset_id_string = manifest_file_asset_id.to_string();
    let tilejson_file_asset_id = Uuid::now_v7();
    let source_file_asset_id = Uuid::now_v7();
    let mut artifacts = BTreeMap::new();
    artifacts.insert(
        "parcels".to_owned(),
        VectorTileArtifactResponse {
            source_layer: "parcels".to_owned(),
            tile_min_zoom: 8,
            tile_max_zoom: 16,
            render_min_zoom: 10,
            render_max_zoom: 22,
            tilejson_object_key:
                "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels.json"
                    .to_owned(),
            object_key_prefix:
                "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/parcels/"
                    .to_owned(),
            flat_tile_count: 10,
            flat_tile_total_bytes: 2048,
            feature_filter_properties: BTreeMap::from([("pnu".to_owned(), "pnu".to_owned())]),
            lineage: VectorTileLineageResponse {
                source_record_id: Uuid::now_v7(),
                manifest_file_asset_id,
                tilejson_file_asset_id,
                source_file_asset_ids: vec![source_file_asset_id],
            },
        },
    );
    let response = VectorTileManifestResponse {
        schema_version: 1,
        current_version: "0196e7e0-3c20-7000-8000-000000000042".to_owned(),
        previous_version: "0196e7e0-3c20-7000-8000-000000000041".to_owned(),
        tiles_url_template: "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf"
            .to_owned(),
        published_at: Utc::now(),
        artifacts,
    };

    let json = serde_json::to_value(response)?;

    assert_eq!(
        json["current_version"].as_str(),
        Some("0196e7e0-3c20-7000-8000-000000000042")
    );
    assert_eq!(
        json["previous_version"].as_str(),
        Some("0196e7e0-3c20-7000-8000-000000000041")
    );
    assert_eq!(
        json["artifacts"]["parcels"]["source_layer"].as_str(),
        Some("parcels")
    );
    assert_eq!(
        json["artifacts"]["parcels"]["lineage"]["manifest_file_asset_id"].as_str(),
        Some(manifest_file_asset_id_string.as_str())
    );
    assert_eq!(
        json["artifacts"]["parcels"]["feature_filter_properties"]["pnu"].as_str(),
        Some("pnu")
    );
    let legacy_object_key_name = ["s3", "_key"].concat();
    assert!(json.get(legacy_object_key_name.as_str()).is_none());
    Ok(())
}

#[test]
fn rollback_vector_tile_manifest_request_uses_version_and_reason() -> Result<(), serde_json::Error>
{
    let request = RollbackVectorTileManifestRequest {
        to_version: "0196e7e0-3c20-7000-8000-000000000041".to_owned(),
        expected_current_version: "0196e7e0-3c20-7000-8000-000000000042".to_owned(),
        reason: "bad tile build".to_owned(),
    };

    let json = serde_json::to_value(request)?;

    assert_eq!(
        json["to_version"].as_str(),
        Some("0196e7e0-3c20-7000-8000-000000000041")
    );
    assert_eq!(
        json["expected_current_version"].as_str(),
        Some("0196e7e0-3c20-7000-8000-000000000042")
    );
    assert_eq!(json["reason"].as_str(), Some("bad tile build"));
    assert!(json.get("operator_staff_id").is_none());
    assert!(json.get("s3_key").is_none());
    Ok(())
}

#[test]
fn promote_vector_tile_manifest_request_uses_object_key_contract() -> Result<(), serde_json::Error>
{
    let mut artifacts = BTreeMap::new();
    artifacts.insert(
        "parcels".to_owned(),
        PromoteVectorTileArtifactRequest {
            source_layer: "parcels".to_owned(),
            tile_min_zoom: 8,
            tile_max_zoom: 16,
            render_min_zoom: 10,
            render_max_zoom: 22,
            tilejson_file_asset: PromoteFileAssetRequest {
                object_key:
                    "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000043/parcels.json"
                        .to_owned(),
                mime_type: "application/json".to_owned(),
                size_bytes: 2048,
                checksum_sha256: Some("a".repeat(64)),
                title: Some("parcel vector tile TileJSON".to_owned()),
                visibility: "public".to_owned(),
            },
            object_key_prefix:
                "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000043/parcels/"
                    .to_owned(),
            flat_tile_count: 42,
            flat_tile_total_bytes: 4096,
            source_file_assets: vec![PromoteFileAssetRequest {
                object_key: "gold/vector-tiles/sources/0196e7e0-3c20-7000-8000-000000000043.zip"
                    .to_owned(),
                mime_type: "application/zip".to_owned(),
                size_bytes: 8192,
                checksum_sha256: Some("b".repeat(64)),
                title: Some("parcel vector tile source archive".to_owned()),
                visibility: "internal".to_owned(),
            }],
        },
    );
    let request = PromoteVectorTileManifestRequest {
        current_version: "0196e7e0-3c20-7000-8000-000000000043".to_owned(),
        expected_current_version: "0196e7e0-3c20-7000-8000-000000000042".to_owned(),
        tiles_url_template: "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf"
            .to_owned(),
        source_record: PromoteSourceRecordRequest {
            source: "vector-tile-build".to_owned(),
            source_url: Some(
                "https://builds.example.test/0196e7e0-3c20-7000-8000-000000000043".to_owned(),
            ),
            external_id: Some("0196e7e0-3c20-7000-8000-000000000043".to_owned()),
            checksum_sha256: Some("c".repeat(64)),
            raw_object_key: Some(
                "gold/vector-tiles/sources/0196e7e0-3c20-7000-8000-000000000043.zip".to_owned(),
            ),
        },
        manifest_file_asset: PromoteFileAssetRequest {
            object_key: "gold/vector-tiles/manifests/0196e7e0-3c20-7000-8000-000000000043.json"
                .to_owned(),
            mime_type: "application/json".to_owned(),
            size_bytes: 1024,
            checksum_sha256: Some("d".repeat(64)),
            title: Some("parcel vector tile manifest".to_owned()),
            visibility: "public".to_owned(),
        },
        artifacts,
    };

    let json = serde_json::to_value(request)?;

    assert_eq!(
        json["current_version"].as_str(),
        Some("0196e7e0-3c20-7000-8000-000000000043")
    );
    assert_eq!(
        json["expected_current_version"].as_str(),
        Some("0196e7e0-3c20-7000-8000-000000000042")
    );
    assert_eq!(
        json["manifest_file_asset"]["object_key"].as_str(),
        Some("gold/vector-tiles/manifests/0196e7e0-3c20-7000-8000-000000000043.json")
    );
    assert_eq!(
        json["artifacts"]["parcels"]["tilejson_file_asset"]["object_key"].as_str(),
        Some("gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000043/parcels.json")
    );
    assert!(json.get("operator_staff_id").is_none());
    assert!(json.get("s3_key").is_none());
    Ok(())
}
