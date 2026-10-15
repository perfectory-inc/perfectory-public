use sha2::{Digest, Sha256};

use super::{
    build_tilejson, parse_anchor_entry, render_tile_sql, tile_address_sql,
    validate_anchor_manifest, validate_build_scope, validate_object_key,
    validate_object_key_prefix, AnchorArtifactManifest, AnchorArtifactObject,
    AnchorArtifactRejectObject, PbfArtifactBuildConfig, PbfArtifactOutput, PbfArtifactOutputConfig,
};

#[test]
fn object_keys_use_immutable_artifact_ids() -> anyhow::Result<()> {
    let artifact_id = "018f0000-0000-7000-8000-000000000001";
    validate_object_key(&format!(
        "gold/vector-tiles/artifacts/{artifact_id}/manifest.json"
    ))?;
    validate_object_key_prefix(&format!("gold/vector-tiles/artifacts/{artifact_id}"))?;
    assert!(crate::r2_layout::vector_tile_artifact_prefix("2026-05-25").is_err());
    assert!(validate_object_key("/gold/vector-tiles/artifacts").is_err());
    assert!(validate_object_key("gold/../manifest.json").is_err());
    assert!(validate_object_key("gold/manifest.json").is_err());
    assert!(validate_object_key_prefix("gold").is_err());
    assert!(validate_object_key_prefix("gold/vector-tiles/artifacts/").is_err());
    Ok(())
}

#[tokio::test]
async fn output_prefix_is_derived_from_immutable_artifact_version() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-pbf-output-prefix-{}",
        uuid::Uuid::now_v7()
    ));
    let output = PbfArtifactOutput::from_config(
        &PbfArtifactOutputConfig::Local {
            root: root.clone(),
            prefix: "gold/vector-tiles/artifacts".to_owned(),
        },
        "018f0000-0000-7000-8000-000000000001",
    )
    .await?;

    assert_eq!(
        output.prefix(),
        "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001"
    );
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn immutable_artifact_objects_refuse_overwrite() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-pbf-create-only-{}",
        uuid::Uuid::now_v7()
    ));
    let output = PbfArtifactOutput::from_config(
        &PbfArtifactOutputConfig::Local {
            root: root.clone(),
            prefix: "gold/vector-tiles/artifacts".to_owned(),
        },
        "018f0000-0000-7000-8000-000000000001",
    )
    .await?;
    let key = format!("{}/tilejson.json", output.prefix());

    output
        .put_object(
            key.clone(),
            br#"{"tilejson":"3.0.0"}"#.to_vec(),
            "application/json",
            "public, max-age=31536000, immutable",
        )
        .await?;
    let duplicate = output
        .put_object(
            key,
            br#"{"tilejson":"3.0.0"}"#.to_vec(),
            "application/json",
            "public, max-age=31536000, immutable",
        )
        .await;

    std::fs::remove_dir_all(root)?;
    assert!(duplicate.is_err(), "immutable artifact key was overwritten");
    Ok(())
}

#[test]
fn tile_sql_uses_temp_stage_and_postgis_mvt_without_catalog_anchor_table() {
    let tile_sql = render_tile_sql();
    assert!(tile_sql.contains("parcel_marker_anchor_pbf_stage"));
    assert!(tile_sql.contains("ST_AsMVTGeom"));
    assert!(tile_sql.contains("ST_AsMVT"));
    assert!(!tile_sql.contains("catalog.parcel_marker_anchor"));

    let address_sql = tile_address_sql();
    assert!(address_sql.contains("generate_series"));
    assert!(address_sql.contains("parcel_marker_anchor_pbf_stage"));
}

#[test]
fn tilejson_exposes_pnu_anchor_pbf_contract() -> anyhow::Result<()> {
    let body = build_tilejson(
        "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001",
        "018f0000-0000-7000-8000-000000000001",
        0,
        12,
    )?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(json["format"].as_str(), Some("pbf"));
    assert_eq!(
        json["vector_layers"][0]["id"].as_str(),
        Some("parcel_anchor")
    );
    assert_eq!(
        json["tiles"][0].as_str(),
        Some("gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001/parcel_anchor/{z}/{x}/{y}.pbf")
    );
    Ok(())
}

#[test]
fn anchor_manifest_requires_count_and_checksum_lineage() {
    let object = AnchorArtifactObject {
        shard_id: "shard-000001".to_owned(),
        source_object_key: "silver/parcel-boundaries/part-000001.jsonl".to_owned(),
        artifact_object_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/part-000001.jsonl".to_owned(),
        row_count: 2,
        size_bytes: 128,
        checksum_sha256: "a".repeat(64),
    };
    let rejected_object = AnchorArtifactRejectObject {
        shard_id: "shard-000001".to_owned(),
        source_object_key: "silver/parcel-boundaries/part-000001.jsonl".to_owned(),
        rejected_object_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/rejected/part-000001.jsonl".to_owned(),
        row_count: 1,
        size_bytes: 64,
        checksum_sha256: "b".repeat(64),
    };
    let mut digest = Sha256::new();
    digest.update(object.checksum_sha256.as_bytes());
    digest.update(rejected_object.checksum_sha256.as_bytes());
    let manifest = AnchorArtifactManifest {
        schema_version: "foundation-platform.parcel_marker_anchor_artifact_manifest.v1".to_owned(),
        artifact_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
        source_snapshot_id: "iceberg:parcel-boundaries-snapshot-001".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        anchor_srid: "EPSG:4326".to_owned(),
        algorithm: "polylabel".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        source_row_count: 3,
        artifact_object_count: 1,
        artifact_row_count: 2,
        rejected_object_count: 1,
        rejected_row_count: 1,
        checksum_sha256: hex_lower_test(&digest.finalize()),
        objects: vec![object],
        rejected_objects: vec![rejected_object],
    };
    assert!(validate_anchor_manifest(&manifest).is_ok());

    let mut broken = manifest;
    broken.artifact_row_count = 3;
    assert!(validate_anchor_manifest(&broken).is_err());
}

#[test]
fn build_scope_defaults_are_bounded_not_national() -> anyhow::Result<()> {
    let manifest = AnchorArtifactManifest {
        schema_version: "foundation-platform.parcel_marker_anchor_artifact_manifest.v1".to_owned(),
        artifact_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
        source_snapshot_id: "iceberg:parcel-boundaries-snapshot-001".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        anchor_srid: "EPSG:4326".to_owned(),
        algorithm: "polylabel".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        source_row_count: 1_000_001,
        artifact_object_count: 2,
        artifact_row_count: 1_000_001,
        rejected_object_count: 0,
        rejected_row_count: 0,
        checksum_sha256: "a".repeat(64),
        objects: Vec::new(),
        rejected_objects: Vec::new(),
    };
    let config = PbfArtifactBuildConfig {
        database_url: "postgres://example".to_owned(),
        input: super::AnchorArtifactInputConfig::R2 {
            manifest_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json".to_owned(),
        },
        output: PbfArtifactOutputConfig::R2 {
            prefix: "gold/vector-tiles/artifacts".to_owned(),
        },
        min_zoom: 0,
        max_zoom: 12,
        expected_anchor_row_count: None,
        max_input_object_count: 1,
        max_input_row_count: 1_000_000,
        summary_path: None,
    };

    let error = validate_build_scope(&config, &manifest)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected bounded scope rejection"))?;
    assert!(error.to_string().contains("object count"));
    Ok(())
}

#[test]
fn national_exact_point_tiles_below_zoom_12_are_blocked_until_aggregate_path_exists(
) -> anyhow::Result<()> {
    let manifest = AnchorArtifactManifest {
        schema_version: "foundation-platform.parcel_marker_anchor_artifact_manifest.v1".to_owned(),
        artifact_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
        source_snapshot_id: "national-promotion:silver-parcel-boundaries-vworld".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        anchor_srid: "EPSG:4326".to_owned(),
        algorithm: "polylabel".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        source_row_count: 39_862_472,
        artifact_object_count: 85,
        artifact_row_count: 39_862_470,
        rejected_object_count: 2,
        rejected_row_count: 2,
        checksum_sha256: "a".repeat(64),
        objects: Vec::new(),
        rejected_objects: Vec::new(),
    };
    let config = PbfArtifactBuildConfig {
        database_url: "postgres://example".to_owned(),
        input: super::AnchorArtifactInputConfig::R2 {
            manifest_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json".to_owned(),
        },
        output: PbfArtifactOutputConfig::R2 {
            prefix: "gold/vector-tiles/artifacts".to_owned(),
        },
        min_zoom: 0,
        max_zoom: 12,
        expected_anchor_row_count: Some(39_862_470),
        max_input_object_count: 100,
        max_input_row_count: 40_000_000,
        summary_path: None,
    };

    let error = validate_build_scope(&config, &manifest)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected low-zoom national exact-point rejection"))?;
    assert!(error.to_string().contains("aggregate marker tile path"));
    Ok(())
}

#[test]
fn national_exact_point_tiles_at_zoom_12_are_allowed_for_high_zoom_proof() -> anyhow::Result<()> {
    let manifest = AnchorArtifactManifest {
        schema_version: "foundation-platform.parcel_marker_anchor_artifact_manifest.v1".to_owned(),
        artifact_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
        source_snapshot_id: "national-promotion:silver-parcel-boundaries-vworld".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        anchor_srid: "EPSG:4326".to_owned(),
        algorithm: "polylabel".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        source_row_count: 39_862_472,
        artifact_object_count: 85,
        artifact_row_count: 39_862_470,
        rejected_object_count: 2,
        rejected_row_count: 2,
        checksum_sha256: "a".repeat(64),
        objects: Vec::new(),
        rejected_objects: Vec::new(),
    };
    let config = PbfArtifactBuildConfig {
        database_url: "postgres://example".to_owned(),
        input: super::AnchorArtifactInputConfig::R2 {
            manifest_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json".to_owned(),
        },
        output: PbfArtifactOutputConfig::R2 {
            prefix: "gold/vector-tiles/artifacts".to_owned(),
        },
        min_zoom: 12,
        max_zoom: 12,
        expected_anchor_row_count: Some(39_862_470),
        max_input_object_count: 100,
        max_input_row_count: 40_000_000,
        summary_path: None,
    };

    validate_build_scope(&config, &manifest)?;
    Ok(())
}

#[test]
fn anchor_entry_validation_rejects_cross_snapshot_lines() -> anyhow::Result<()> {
    let manifest = AnchorArtifactManifest {
        schema_version: "foundation-platform.parcel_marker_anchor_artifact_manifest.v1".to_owned(),
        artifact_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
        source_snapshot_id: "iceberg:parcel-boundaries-snapshot-001".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        anchor_srid: "EPSG:4326".to_owned(),
        algorithm: "polylabel".to_owned(),
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        source_row_count: 1,
        artifact_object_count: 1,
        artifact_row_count: 1,
        rejected_object_count: 0,
        rejected_row_count: 0,
        checksum_sha256: "a".repeat(64),
        objects: Vec::new(),
        rejected_objects: Vec::new(),
    };
    let object = AnchorArtifactObject {
        shard_id: "shard-000001".to_owned(),
        source_object_key: "silver/parcel-boundaries/part-000001.jsonl".to_owned(),
        artifact_object_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/part-000001.jsonl".to_owned(),
        row_count: 1,
        size_bytes: 128,
        checksum_sha256: "a".repeat(64),
    };
    let line = serde_json::json!({
        "schema_version": "foundation-platform.parcel_marker_anchor_artifact_entry.v1",
        "pnu": "9999900101100010001",
        "anchor_lng": 127.123_470_234_50,
        "anchor_lat": 36.123_456,
        "anchor_srid": "EPSG:4326",
        "algorithm": "polylabel",
        "algorithm_version": "postgis-st_maximuminscribedcircle-v1",
        "source_snapshot_id": "iceberg:other-snapshot",
        "source_table": "silver.parcel_boundaries",
        "source_row_id": "row-1",
        "source_object_key": "silver/parcel-boundaries/part-000001.jsonl",
        "source_geometry_checksum_sha256": "b".repeat(64)
    })
    .to_string();

    let error = parse_anchor_entry(line.as_bytes(), &object, &manifest, 1)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected snapshot mismatch"))?;
    assert!(error.to_string().contains("source lineage"));
    Ok(())
}

fn hex_lower_test(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}
