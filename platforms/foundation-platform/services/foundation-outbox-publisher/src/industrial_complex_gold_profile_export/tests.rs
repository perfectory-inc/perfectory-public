//! Tests for the industrial-complex Gold profile export.

use std::path::PathBuf;

use serde_json::{json, Map as JsonMap, Value as JsonValue};

use super::{
    export_entry, parse_max_concurrency, profile_document, ProfileExportConfig, ProfileOutput,
    ProfileOutputConfig,
};
use foundation_shared_kernel::ObjectUrlTemplate;
use profile_document::GoldSnapshotProvenance;

fn temporary_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-gold-profile-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}

// Snapshot ids here sit in the repository-reserved synthetic namespace
// (`scripts/guard/public-fixture-safety.py`), not the production values: a real Iceberg
// snapshot id is a 19-digit integer and is indistinguishable by shape from a parcel number.
// The production snapshot this command was proven against is recorded in root ADR-0036.
fn provenance() -> GoldSnapshotProvenance {
    GoldSnapshotProvenance {
        table: "gold.complex_catalog".to_owned(),
        iceberg_snapshot_id: "999990000000000001".to_owned(),
        metadata_location:
            "s3://foundation-platform-lakehouse-prod/__r2_data_catalog/t/metadata/00001.metadata.json"
                .to_owned(),
        manifest_list_location:
            "s3://foundation-platform-lakehouse-prod/__r2_data_catalog/t/metadata/snap-1.avro"
                .to_owned(),
    }
}

fn row() -> JsonMap<String, JsonValue> {
    let JsonValue::Object(map) = json!({
        "complex_id": "0196e7e0-3c20-7000-8000-100000000002",
        "official_complex_code": "446400",
        "name": "fixture complex",
        "kind": "general",
        "status": "active",
        "sido_code": "41",
        "sigungu_code": JsonValue::Null,
        "address_text": "fixture address",
        "official_area_sqm": "1234.56",
        "calculated_area_sqm": JsonValue::Null,
        "parcel_count": 0,
        "boundary_object_key": JsonValue::Null,
        "source_snapshot_id": "vworldkr__sandan_profile-202506",
        "iceberg_snapshot_id": "999990000000000003",
        "published_at_utc": "2026-08-18T00:00:00Z"
    }) else {
        unreachable!("fixture literal is an object")
    };
    map
}

fn config(root: PathBuf) -> anyhow::Result<ProfileExportConfig> {
    Ok(ProfileExportConfig {
        output: ProfileOutputConfig::Local { root },
        profile_url_template: Some(ObjectUrlTemplate::parse(
            "https://lakehouse.example.com/{object_key}",
        )?),
        expected_row_count: None,
        max_concurrency: 1,
        summary_path: None,
    })
}

#[tokio::test]
async fn re_running_the_same_snapshot_reuses_the_identical_object() -> anyhow::Result<()> {
    let root = temporary_root("idempotent");
    let output = ProfileOutput::from_config(&ProfileOutputConfig::Local { root: root.clone() })?;
    let artifact = profile_document::build(&provenance(), &row())?;

    let created = output.write_create_only(&artifact).await?;
    let recreated = output.write_create_only(&artifact).await?;

    std::fs::remove_dir_all(&root)?;
    assert!(created, "first create-only write did not create the object");
    assert!(
        !recreated,
        "second create-only write reported a fresh create instead of reusing stored bytes"
    );
    Ok(())
}

#[tokio::test]
async fn a_colliding_key_with_different_bytes_fails_instead_of_overwriting() -> anyhow::Result<()> {
    let root = temporary_root("collision");
    let output = ProfileOutput::from_config(&ProfileOutputConfig::Local { root: root.clone() })?;
    let artifact = profile_document::build(&provenance(), &row())?;
    output.write_create_only(&artifact).await?;

    let mut different = artifact.clone();
    different.body = b"{\"schema_version\":\"other\"}\n".to_vec();
    let result = output.write_create_only(&different).await;

    let stored = std::fs::read(
        root.join("gold/industrial-complex/profiles")
            .join(format!("{}.json", artifact.artifact_id)),
    )?;
    std::fs::remove_dir_all(&root)?;
    assert!(
        result.is_err(),
        "a differing object was silently overwritten"
    );
    assert_eq!(stored, artifact.body, "stored bytes were replaced");
    Ok(())
}

#[test]
fn export_entries_carry_the_pointer_publish_inputs() -> anyhow::Result<()> {
    let provenance = provenance();
    let row = row();
    let artifact = profile_document::build(&provenance, &row)?;

    let entry = export_entry(
        &config(temporary_root("entry"))?,
        &provenance,
        &row,
        &artifact,
        true,
    )?;

    assert_eq!(entry.current_version, artifact.artifact_id.to_string());
    assert_eq!(entry.profile_object_key, artifact.object_key);
    assert_eq!(
        entry.profile_url,
        Some(format!(
            "https://lakehouse.example.com/{}",
            artifact.object_key
        ))
    );
    assert_eq!(entry.profile_row_count, 1);
    assert_eq!(entry.profile_checksum_sha256, artifact.checksum_sha256);
    assert_eq!(entry.source_snapshot_id, "vworldkr__sandan_profile-202506");
    // The scanned Gold snapshot, not the `iceberg_snapshot_id` column the projection job wrote.
    assert_eq!(entry.iceberg_snapshot_id, "999990000000000001");
    assert_eq!(entry.published_at_utc, "2026-08-18T00:00:00Z");
    Ok(())
}

/// Producing the artifact must not wait on a serving hostname; pointing at it must not proceed
/// without one (root ADR-0037). Without a template the entry simply carries no URL.
#[test]
fn an_export_without_an_address_template_still_produces_the_artifact() -> anyhow::Result<()> {
    let provenance = provenance();
    let row = row();
    let artifact = profile_document::build(&provenance, &row)?;
    let mut config = config(temporary_root("no-template"))?;
    config.profile_url_template = None;

    let entry = export_entry(&config, &provenance, &row, &artifact, true)?;

    assert_eq!(entry.profile_url, None);
    assert_eq!(entry.profile_object_key, artifact.object_key);
    Ok(())
}

#[test]
fn export_concurrency_is_explicitly_bounded() -> anyhow::Result<()> {
    assert_eq!(parse_max_concurrency("1")?, 1);
    assert_eq!(parse_max_concurrency("32")?, 32);
    assert!(parse_max_concurrency("0").is_err());
    assert!(parse_max_concurrency("33").is_err());
    Ok(())
}
