use chrono::{NaiveDate, TimeZone as _, Utc};
use serde_json::{json, Map as JsonMap};

use super::{compile_provider_file_recovery, ProviderFileEvidence, ProviderFileR2Object};
use crate::bronze_catalog_recovery_manifest::RecoverySourceSnapshot;

#[test]
fn exact_provider_identity_compiles_one_canonical_candidate() {
    let compilation = compile_provider_file_recovery(
        &["hubgokr__building_register_main".to_owned()],
        vec![provider_evidence()],
        vec![r2_object("OPN209912310000000012")],
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
    )
    .expect("exact provider evidence must compile");

    assert!(compilation.unresolved.is_empty());
    assert_eq!(compilation.sources.len(), 1);
    let candidate = &compilation.sources[0].candidates[0];
    assert_eq!(
        candidate.object_key,
        "bronze/source=hubgokr__building_register_main/OPN209912310000000012.zip"
    );
    assert_eq!(candidate.expected_size_bytes, 87_378);
    assert_eq!(
        candidate.observed_r2_etag.as_deref(),
        Some("inventory-etag")
    );
    assert_eq!(
        candidate.source_identity_key,
        "provider_file_id=OPN209912310000000012"
    );
    assert_eq!(candidate.snapshot_date, "2026-06-01");
    assert_eq!(candidate.snapshot_granularity, "month");
    assert_eq!(candidate.snapshot_basis, "provider_file_period");
    assert_eq!(candidate.request_params["taskGroupCode"], "03");
    assert_eq!(
        candidate.request_params["physicalObjectFileNameBasis"],
        "r2_inventory_key_leaf"
    );
}

#[test]
fn missing_and_ambiguous_provider_evidence_are_quarantined() {
    let missing = compile_provider_file_recovery(
        &["hubgokr__building_register_main".to_owned()],
        vec![provider_evidence()],
        vec![r2_object("OPN-OLD")],
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
    )
    .expect("missing evidence is a manifest result, not a compiler crash");
    assert_eq!(missing.sources.len(), 0);
    assert_eq!(
        missing.unresolved[0].reason,
        "missing_provider_inventory_match"
    );

    let evidence = provider_evidence();
    let ambiguous = compile_provider_file_recovery(
        &["hubgokr__building_register_main".to_owned()],
        vec![evidence.clone(), evidence],
        vec![r2_object("OPN209912310000000012")],
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
    )
    .expect("ambiguous evidence is quarantined");
    assert_eq!(ambiguous.sources.len(), 0);
    assert_eq!(
        ambiguous.unresolved[0].reason,
        "ambiguous_provider_inventory_match"
    );
    assert_eq!(ambiguous.unresolved[0].matching_evidence_count, 2);
}

#[test]
fn duplicate_r2_key_and_reserved_extra_field_fail_loud() {
    let object = r2_object("OPN209912310000000012");
    let duplicate_error = compile_provider_file_recovery(
        &["hubgokr__building_register_main".to_owned()],
        vec![provider_evidence()],
        vec![object.clone(), object],
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
    )
    .expect_err("duplicate physical evidence must not create duplicate candidates");
    assert!(duplicate_error
        .to_string()
        .contains("duplicate R2 inventory key"));

    let mut evidence = provider_evidence();
    evidence
        .request_params_extra
        .insert("provider_file_id".to_owned(), json!("override"));
    let reserved_error = compile_provider_file_recovery(
        &["hubgokr__building_register_main".to_owned()],
        vec![evidence],
        vec![r2_object("OPN209912310000000012")],
        NaiveDate::from_ymd_opt(2026, 7, 14).unwrap(),
    )
    .expect_err("provider adapters must not overwrite common lineage fields");
    assert!(reserved_error
        .to_string()
        .contains("reserved request_params"));
}

fn provider_evidence() -> ProviderFileEvidence {
    let mut request_params_extra = JsonMap::new();
    request_params_extra.insert("taskGroupCode".to_owned(), json!("03"));
    request_params_extra.insert("taskCode".to_owned(), json!("0303"));
    ProviderFileEvidence {
        source: RecoverySourceSnapshot {
            endpoint_slug: "hub-building-building_register_main".to_owned(),
            slug: "hubgokr__building_register_main".to_owned(),
            name: "Building register main".to_owned(),
            provider: "hub.go.kr".to_owned(),
            dataset_name: "building_register_main".to_owned(),
            base_url: Some("https://www.hub.go.kr".to_owned()),
            auth_kind: "manual".to_owned(),
            payload_format: "unknown".to_owned(),
            terms_url: Some("https://www.hub.go.kr/terms".to_owned()),
        },
        operation: "building_register_main".to_owned(),
        provider_file_period: Some("2026-06".to_owned()),
        provider_snapshot_date: None,
        provider_file_id: "OPN209912310000000012".to_owned(),
        provider_file_name_label: "Building register main (2026-06)".to_owned(),
        provider_updated_at: None,
        request_params_extra,
    }
}

fn r2_object(provider_file_id: &str) -> ProviderFileR2Object {
    ProviderFileR2Object {
        key: format!("bronze/source=hubgokr__building_register_main/{provider_file_id}.zip"),
        size_bytes: 87_378,
        last_modified: Some(
            Utc.with_ymd_and_hms(2026, 7, 2, 12, 0, 55)
                .unwrap()
                .to_rfc3339(),
        ),
        e_tag: Some("inventory-etag".to_owned()),
        classification: "bronze_catalog_metadata_missing".to_owned(),
    }
}
