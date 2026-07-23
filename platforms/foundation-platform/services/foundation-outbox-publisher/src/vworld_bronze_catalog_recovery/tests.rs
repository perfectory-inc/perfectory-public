use chrono::{TimeZone as _, Utc};
use serde_json::{json, Value as JsonValue};

use super::compile_vworld_bronze_catalog_recovery_manifest;
use crate::bronze_catalog_recovery_manifest::BronzeCatalogRecoveryManifestStatus;

#[test]
fn exact_provider_inventory_and_r2_match_compiles_authoritative_candidate() {
    let manifest =
        compile(base_provider_inventory(), base_r2_audit()).expect("exact evidence should compile");

    assert_eq!(manifest.status, BronzeCatalogRecoveryManifestStatus::Ready);
    assert_eq!(manifest.sources.len(), 1);
    assert!(manifest.unresolved.is_empty());
    let source = &manifest.sources[0];
    assert_eq!(source.source.slug, "vworldkr__land_characteristic");
    assert_eq!(source.source.provider, "VWorld");
    assert_eq!(source.candidates.len(), 1);
    let candidate = &source.candidates[0];
    assert_eq!(
        candidate.object_key,
        "bronze/source=vworldkr__land_characteristic/20991231DS99992-9003.zip"
    );
    assert_eq!(candidate.expected_size_bytes, 4_096);
    assert_eq!(
        candidate.source_identity_key,
        "provider_file_id=20991231DS99992-9003"
    );
    assert_eq!(candidate.snapshot_date, "2026-06-30");
    assert_eq!(candidate.snapshot_granularity, "day");
    assert_eq!(candidate.snapshot_basis, "provider_snapshot_date");
    assert_eq!(candidate.provider_updated_at.as_deref(), Some("2026-07-01"));
    assert_eq!(candidate.content_type, "application/zip");
    assert_eq!(
        candidate.request_params["endpointSlug"],
        "vworld-dataset-land_characteristic"
    );
    assert_eq!(
        candidate.request_params["physicalObjectFileNameBasis"],
        "r2_inventory_key_leaf"
    );
    assert_eq!(
        candidate.request_params["provider_file_name_label"],
        "Land characteristic"
    );
    assert_eq!(candidate.evidence_kind, "provider_inventory");
}

#[test]
fn missing_provider_dates_use_r2_last_modified_as_explicit_collection_fallback() {
    let mut inventory = base_provider_inventory();
    inventory["jobs"][0]["files"][0]["base_ym"] = json!("-");
    inventory["jobs"][0]["files"][0]["updated_at"] = json!("-");

    let manifest = compile(inventory, base_r2_audit()).expect("fallback should compile");
    let candidate = &manifest.sources[0].candidates[0];

    assert_eq!(candidate.snapshot_date, "2026-07-02");
    assert_eq!(candidate.snapshot_granularity, "day");
    assert_eq!(candidate.snapshot_basis, "collected_at_fallback");
}

#[test]
fn malformed_non_empty_provider_date_is_rejected_instead_of_silently_dropped() {
    let mut inventory = base_provider_inventory();
    inventory["jobs"][0]["files"][0]["updated_at"] = json!("2026/07/01");

    let error = compile(inventory, base_r2_audit())
        .expect_err("malformed authoritative provider date must fail loud");

    assert!(error.to_string().contains("updated_at"));
}

#[test]
fn duplicate_endpoint_catalog_slug_is_rejected_instead_of_last_write_wins() {
    let mut endpoints = endpoint_catalog();
    let duplicate = endpoints["endpoints"][0].clone();
    endpoints["endpoints"]
        .as_array_mut()
        .expect("endpoint fixture array")
        .push(duplicate);

    let error =
        compile_with_endpoint_catalog(endpoints, base_provider_inventory(), base_r2_audit())
            .expect_err("duplicate endpoint SSOT keys must fail loud");

    assert!(error.to_string().contains("duplicate endpoint_slug"));
}

#[test]
fn recovery_scope_ignores_r2_sources_not_selected_by_provider_inventory() {
    let mut r2_audit = base_r2_audit();
    r2_audit["objects"]
        .as_array_mut()
        .expect("R2 fixture array")
        .push(json!({
            "key": "bronze/source=vworldkr__parcel/20991231DS99995-9008.zip",
            "size_bytes": 2048,
            "last_modified": "2026-07-02T12:00:56Z",
            "e_tag": "parcel-etag",
            "classification": "bronze_catalog_metadata_missing",
            "action": "review",
            "reason": "Catalog metadata is missing"
        }));

    let manifest = compile(base_provider_inventory(), r2_audit)
        .expect("unselected source must remain outside this evidence scope");

    assert_eq!(manifest.status, BronzeCatalogRecoveryManifestStatus::Ready);
    assert_eq!(manifest.sources.len(), 1);
    assert!(manifest.unresolved.is_empty());
}

#[test]
fn duplicate_r2_inventory_key_is_rejected_instead_of_creating_duplicate_candidates() {
    let mut r2_audit = base_r2_audit();
    let duplicate = r2_audit["objects"][0].clone();
    r2_audit["objects"]
        .as_array_mut()
        .expect("R2 fixture array")
        .push(duplicate);

    let error = compile(base_provider_inventory(), r2_audit)
        .expect_err("duplicate physical object evidence must fail loud");

    assert!(error.to_string().contains("duplicate R2 inventory key"));
}

fn compile(
    provider_inventory: JsonValue,
    r2_audit: JsonValue,
) -> anyhow::Result<crate::bronze_catalog_recovery_manifest::BronzeCatalogRecoveryManifest> {
    compile_with_endpoint_catalog(endpoint_catalog(), provider_inventory, r2_audit)
}

fn compile_with_endpoint_catalog(
    endpoint_catalog: JsonValue,
    provider_inventory: JsonValue,
    r2_audit: JsonValue,
) -> anyhow::Result<crate::bronze_catalog_recovery_manifest::BronzeCatalogRecoveryManifest> {
    compile_vworld_bronze_catalog_recovery_manifest(
        &endpoint_catalog.to_string(),
        "docs/catalog/public-source-endpoint-catalog.v1.json",
        &provider_inventory.to_string(),
        "target/audit/vworld-dataset-file-inventory.json",
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
            "endpoint_slug": "vworld-dataset-land_characteristic",
            "provider": "VWorld",
            "dataset_slug": "land_characteristic",
            "operation": "land_characteristic",
            "source_acquisition_lane": "provider_dataset_file",
            "provider_dataset_selector": {"svc_cde": "NA", "ds_id": "4"},
            "auth_kind": "provider_managed_credential",
            "bronze": {"source_slug": "vworldkr__land_characteristic"}
        }]
    })
}

fn base_provider_inventory() -> JsonValue {
    json!({
        "schema_version": "foundation-platform.vworld_dataset_file_inventory.v1",
        "generated_at_utc": "2026-07-02T11:00:00Z",
        "status": "ready",
        "jobs": [{
            "endpoint_slug": "vworld-dataset-land_characteristic",
            "source_slug": "vworldkr__land_characteristic",
            "source_name": "VWorld land characteristic",
            "dataset_name": "Land characteristic",
            "base_uri": "https://www.vworld.kr",
            "terms_url": "https://www.vworld.kr/terms.do",
            "operation": "land_characteristic",
            "provider_module": "vworld_dataset_file",
            "svc_cde": "NA",
            "ds_id": "4",
            "files": [{
                "svc_cde": "NA",
                "ds_id": "4",
                "download_ds_id": "20991231DS99992",
                "file_no": "9003",
                "provider_file_name": "Land characteristic",
                "file_format": "CSV",
                "size_mb_label": "1.0",
                "size_kib": 1024,
                "provider_file_kind": "SHP",
                "base_ym": "2026-06-30",
                "updated_at": "2026-07-01",
                "download_kind": "single_resource_file"
            }]
        }]
    })
}

fn base_r2_audit() -> JsonValue {
    json!({
        "schema_version": "foundation-platform.r2_inventory_audit.v1",
        "generated_at_utc": "2026-07-14T00:00:00Z",
        "objects": [{
            "key": "bronze/source=vworldkr__land_characteristic/20991231DS99992-9003.zip",
            "size_bytes": 4096,
            "last_modified": "2026-07-02T12:00:55Z",
            "e_tag": "land-characteristic-etag",
            "classification": "bronze_catalog_metadata_missing",
            "action": "review",
            "reason": "Catalog metadata is missing"
        }]
    })
}
