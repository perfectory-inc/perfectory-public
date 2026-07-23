use chrono::{TimeZone as _, Utc};
use serde_json::{json, Value as JsonValue};

use super::compile_building_hub_bronze_catalog_recovery_manifest;
use crate::bronze_catalog_recovery_manifest::BronzeCatalogRecoveryManifestStatus;

#[test]
fn exact_hub_inventory_and_r2_match_compiles_authoritative_candidate() {
    let manifest = compile(
        provider_inventory("ready"),
        r2_audit("OPN209912310000000012"),
    )
    .expect("exact Hub evidence should compile");

    assert_eq!(manifest.status, BronzeCatalogRecoveryManifestStatus::Ready);
    assert_eq!(manifest.sources.len(), 1);
    assert!(manifest.unresolved.is_empty());
    let source = &manifest.sources[0];
    assert_eq!(source.source.slug, "hubgokr__building_register_main");
    assert_eq!(source.source.provider, "hub.go.kr");
    assert_eq!(source.candidates.len(), 1);
    let candidate = &source.candidates[0];
    assert_eq!(
        candidate.object_key,
        "bronze/source=hubgokr__building_register_main/OPN209912310000000012.zip"
    );
    assert_eq!(candidate.expected_size_bytes, 87_378);
    assert_eq!(candidate.snapshot_date, "2026-06-01");
    assert_eq!(candidate.snapshot_granularity, "month");
    assert_eq!(candidate.snapshot_basis, "provider_file_period");
    assert_eq!(candidate.request_params["taskGroupCode"], "03");
    assert_eq!(candidate.request_params["taskCode"], "0303");
    assert_eq!(candidate.request_params["categoryName"], "Building");
    assert_eq!(candidate.evidence_kind, "provider_inventory");
}

#[test]
fn blocked_inventory_with_missing_file_produces_blocked_manifest_not_guessed_metadata() {
    let mut inventory = provider_inventory("blocked");
    inventory["jobs"][0]["files"] = json!([]);
    inventory["blockers"] = json!([{
        "endpoint_slug": "hub-building-building_register_main",
        "source_slug": "hubgokr__building_register_main",
        "reason": "missing_provider_inventory_match"
    }]);

    let manifest = compile(inventory, r2_audit("OPN209912310000000012"))
        .expect("missing current provider evidence must remain inspectable");

    assert_eq!(
        manifest.status,
        BronzeCatalogRecoveryManifestStatus::Blocked
    );
    assert!(manifest.sources.is_empty());
    assert_eq!(manifest.unresolved.len(), 1);
    assert_eq!(
        manifest.unresolved[0].reason,
        "missing_provider_inventory_match"
    );
}

#[test]
fn r2_objects_outside_requested_hub_sources_are_ignored() {
    let mut audit = r2_audit("OPN209912310000000012");
    audit["objects"]
        .as_array_mut()
        .expect("R2 objects fixture")
        .push(json!({
            "key": "bronze/source=hubgokr__building_register_master/OPN-OTHER.zip",
            "size_bytes": 10,
            "last_modified": "2026-07-02T12:00:55Z",
            "e_tag": "other-etag",
            "classification": "bronze_catalog_metadata_missing"
        }));

    let manifest = compile(provider_inventory("ready"), audit)
        .expect("unselected Hub source must remain outside recovery scope");

    assert_eq!(manifest.status, BronzeCatalogRecoveryManifestStatus::Ready);
    assert!(manifest.unresolved.is_empty());
    assert_eq!(manifest.sources[0].candidates.len(), 1);
}

fn compile(
    provider_inventory: JsonValue,
    r2_audit: JsonValue,
) -> anyhow::Result<crate::bronze_catalog_recovery_manifest::BronzeCatalogRecoveryManifest> {
    compile_building_hub_bronze_catalog_recovery_manifest(
        &endpoint_catalog().to_string(),
        "docs/catalog/public-source-endpoint-catalog.v1.json",
        &provider_inventory.to_string(),
        "target/audit/building-hub-bronze-catalog-recovery-inventory.json",
        &r2_audit.to_string(),
        "target/r2-inventory-audit/r2-inventory-audit.json",
        Utc.with_ymd_and_hms(2026, 7, 14, 1, 2, 3).unwrap(),
    )
}

fn endpoint_catalog() -> JsonValue {
    json!({
        "schema_version": "foundation-platform.public_source_endpoint_catalog.v1",
        "status": "ready",
        "endpoints": [{
            "endpoint_slug": "hub-building-building_register_main",
            "provider": "hub.go.kr",
            "group": "building_hub_bulk",
            "display_name_ko": "Building register main",
            "dataset_slug": "building_register_main",
            "operation": "building_register_main",
            "source_acquisition_lane": "bulk_file",
            "national_collection_allowed": true,
            "provider_inventory_selector": {
                "task_group_code": "03",
                "task_code": "0303"
            },
            "auth_kind": "provider_managed_credential",
            "bronze": {"source_slug": "hubgokr__building_register_main"}
        }]
    })
}

fn provider_inventory(status: &str) -> JsonValue {
    json!({
        "schema_version": "foundation-platform.building_hub_bronze_catalog_recovery_inventory.v1",
        "generated_at_utc": "2026-07-14T00:00:00Z",
        "status": status,
        "endpoint_catalog": {
            "uri": "docs/catalog/public-source-endpoint-catalog.v1.json",
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        },
        "requested_source_slugs": ["hubgokr__building_register_main"],
        "blockers": [],
        "jobs": [{
            "endpoint_slug": "hub-building-building_register_main",
            "source_slug": "hubgokr__building_register_main",
            "source_name": "Building register main",
            "dataset_name": "building_register_main",
            "base_uri": "https://www.hub.go.kr",
            "terms_url": "https://www.hub.go.kr/terms",
            "operation": "building_register_main",
            "provider_module": "building_hub_bulk",
            "task_group_code": "03",
            "task_code": "0303",
            "files": [{
                "category_name": "Building",
                "service_name": "Building register main",
                "service_period_label": "2026-06",
                "provider_file_period": "2026-06",
                "provider_file_id": "OPN209912310000000012"
            }]
        }]
    })
}

fn r2_audit(provider_file_id: &str) -> JsonValue {
    json!({
        "schema_version": "foundation-platform.r2_inventory_audit.v1",
        "generated_at_utc": "2026-07-14T00:00:00Z",
        "objects": [{
            "key": format!(
                "bronze/source=hubgokr__building_register_main/{provider_file_id}.zip"
            ),
            "size_bytes": 87378,
            "last_modified": "2026-07-02T12:00:55Z",
            "e_tag": "hub-etag",
            "classification": "bronze_catalog_metadata_missing"
        }]
    })
}
