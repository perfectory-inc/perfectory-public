//! Tests for reading an export summary into pointer publications.

use super::{
    read_export_summary, resolve_profile_url_template, ExportSummary, EXPORT_SUMMARY_SCHEMA_VERSION,
};
use serde_json::json;
use std::path::PathBuf;

const TEMPLATE: &str = "https://lakehouse.example.com/{object_key}";
const LAKEHOUSE_COMPLEX_ID: &str = "001533c1-8504-5651-bd49-d9df4e87bc37";

fn temporary_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-gold-pointer-summary-{label}-{}.json",
        uuid::Uuid::now_v7()
    ))
}

/// A summary shaped exactly like the one `export-industrial-complex-gold-profiles` writes.
fn summary_json(profile_url_template: Option<&str>) -> serde_json::Value {
    json!({
        "schema_version": EXPORT_SUMMARY_SCHEMA_VERSION,
        "profile_schema_version": "foundation-platform.industrial_complex_gold_profile.v1",
        "generated_at_utc": "2026-08-18T00:00:00Z",
        "export_run_id": "0196e7e0-3c20-7000-8000-100000000000",
        "gold_table": "gold.complex_catalog",
        // Repository-reserved synthetic snapshot namespace, not a production value.
        "gold_iceberg_snapshot_id": "999990000000000001",
        "gold_metadata_location": "s3://lakehouse/metadata/00001.metadata.json",
        "gold_manifest_list_location": "s3://lakehouse/metadata/snap-1.avro",
        "data_file_count": 1,
        "scanned_row_count": 1,
        "output_storage_driver": "local",
        "output_bucket": serde_json::Value::Null,
        "profile_url_template": profile_url_template,
        "artifact_count": 1,
        "created_object_count": 1,
        "reused_object_count": 0,
        "placeholder_parcel_count_row_count": 1,
        "null_calculated_area_row_count": 1,
        "artifacts": [{
            "complex_id": LAKEHOUSE_COMPLEX_ID,
            "official_complex_code": "446400",
            "current_version": "018f0000-0000-7000-8000-000000000001",
            "profile_object_key":
                "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json",
            "profile_url": serde_json::Value::Null,
            "profile_size_bytes": 1401,
            "profile_row_count": 1,
            "profile_checksum_sha256": "a".repeat(64),
            "source_snapshot_id": "vworldkr__sandan_profile-202506",
            "iceberg_snapshot_id": "999990000000000001",
            "published_at_utc": "2026-08-18T00:00:00Z",
            "created": true
        }]
    })
}

fn write_summary(label: &str, value: &serde_json::Value) -> anyhow::Result<PathBuf> {
    let path = temporary_path(label);
    std::fs::write(&path, serde_json::to_vec_pretty(value)?)?;
    Ok(path)
}

/// The export names the publish inputs "그 이름 그대로" (root ADR-0036 decision 7). If these field
/// names ever stop lining up, this is where it is caught rather than at 1,442 hand copies.
#[test]
fn reads_the_publish_inputs_the_export_wrote() -> anyhow::Result<()> {
    let path = write_summary("fields", &summary_json(Some(TEMPLATE)))?;

    let summary: ExportSummary = read_export_summary(&path)?;

    std::fs::remove_file(&path)?;
    assert_eq!(summary.schema_version, EXPORT_SUMMARY_SCHEMA_VERSION);
    assert_eq!(summary.gold_table, "gold.complex_catalog");
    assert_eq!(summary.output_storage_driver, "local");
    assert_eq!(summary.output_bucket, None);
    assert_eq!(summary.artifacts.len(), 1);

    let artifact = &summary.artifacts[0];
    assert_eq!(
        artifact.lakehouse_complex_id.to_string(),
        LAKEHOUSE_COMPLEX_ID
    );
    assert_eq!(
        artifact.current_version,
        "018f0000-0000-7000-8000-000000000001"
    );
    assert_eq!(
        artifact.profile_object_key,
        "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json"
    );
    assert_eq!(artifact.profile_size_bytes, 1401);
    assert_eq!(artifact.profile_row_count, 1);
    assert_eq!(artifact.profile_checksum_sha256, "a".repeat(64));
    assert_eq!(
        artifact.source_snapshot_id,
        "vworldkr__sandan_profile-202506"
    );
    assert_eq!(artifact.iceberg_snapshot_id, "999990000000000001");
    assert_eq!(artifact.published_at_utc, "2026-08-18T00:00:00Z");
    Ok(())
}

/// The production export ran before any serving host existed, so its summary carries no template.
/// Publishing it must be possible once a host exists, without rewriting immutable objects.
#[test]
fn an_address_stated_at_publish_time_is_used_when_the_export_stated_none() {
    assert_eq!(
        resolve_profile_url_template(Some(TEMPLATE), None),
        Some(TEMPLATE.to_owned())
    );
}

#[test]
fn the_export_address_is_used_when_publish_time_states_none() {
    assert_eq!(
        resolve_profile_url_template(None, Some(TEMPLATE)),
        Some(TEMPLATE.to_owned())
    );
}

#[test]
fn a_publish_time_address_overrides_the_export_address() {
    assert_eq!(
        resolve_profile_url_template(Some(TEMPLATE), Some("https://old.example.com/{object_key}")),
        Some(TEMPLATE.to_owned())
    );
}

/// Root ADR-0037: consumers must not be sent to an object with no way to fetch it.
#[test]
fn no_address_anywhere_stops_the_run() {
    assert_eq!(resolve_profile_url_template(None, None), None);
}

#[test]
fn a_summary_of_another_schema_is_not_read_as_this_one() -> anyhow::Result<()> {
    let mut value = summary_json(Some(TEMPLATE));
    value["schema_version"] = json!("foundation-platform.some_other_summary.v1");
    let path = write_summary("schema", &value)?;

    let summary: ExportSummary = read_export_summary(&path)?;

    std::fs::remove_file(&path)?;
    assert_ne!(
        summary.schema_version, EXPORT_SUMMARY_SCHEMA_VERSION,
        "the run guards on this value; it must survive parsing to be checked"
    );
    Ok(())
}
