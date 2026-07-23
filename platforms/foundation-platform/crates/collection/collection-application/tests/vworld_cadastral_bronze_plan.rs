//! Contract tests for `VWorld` cadastral 2D Data API Bronze page planning.
//!
//! These assert the redesigned cadastral object key (Task 3 / T1.1, ADR 0016): the object key
//! drops `operation=GetFeature` (per-lane constant kept only in lineage), `filter_kind=attr`
//! (a zero-information `const`), and `size=` (a request knob), and replaces the opaque
//! `filter_sha256=<64hex>` with a HUMAN-READABLE scope key parsed from `attr_filter`
//! (`pnu=<value>` / `emd=<value>`), or a short `filter_fingerprint=<12hex>` for filters that
//! cannot be cleanly reduced to a single `field:=:value`. The provider operation + raw
//! `attr_filter` stay in `source_partition_key` / `request_params` for traceability.

use chrono::NaiveDate;
use collection_application::{
    plan_vworld_cadastral_bronze_page, VWorldCadastralBronzePagePlanInput,
    VWorldCadastralPageRequest,
};
use collection_domain::SchemaObservedType;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn single_feature_payload(pnu: &str) -> JsonValue {
    json!({
        "response": {
            "status": "OK",
            "record": { "total": "1", "current": "1" },
            "page": { "total": "1", "current": "1", "size": "10" },
            "result": {
                "featureCollection": {
                    "type": "FeatureCollection",
                    "bbox": [
                        127.123_470_234_326_75,
                        36.123_442_520_939_82,
                        127.123_470_234_349_38,
                        36.123_443_568_861_19
                    ],
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {
                                "pnu": pnu,
                                "jibun": "580-1",
                                "bonbun": "0580",
                                "bubun": "0001"
                            },
                            "geometry": { "type": "MultiPolygon", "coordinates": [] }
                        }
                    ]
                }
            }
        }
    })
}

fn request_with_attr_filter(attr_filter: &str) -> VWorldCadastralPageRequest {
    VWorldCadastralPageRequest {
        dataset: "LP_PA_CBND_BUBUN".to_owned(),
        attr_filter: Some(attr_filter.to_owned()),
        columns: vec![
            "pnu".to_owned(),
            "jibun".to_owned(),
            "bonbun".to_owned(),
            "bubun".to_owned(),
            "ag_geom".to_owned(),
        ],
        geometry: true,
        attribute: true,
        crs: Some("EPSG:4326".to_owned()),
        page: 1,
        size: 1000,
    }
}

/// Single clean `pnu:=:<value>` filter → `pnu=<value>` scope key in the object key; the operation,
/// `filter_kind`, and `size` segments are gone, while `source_partition_key` and `request_params`
/// still carry `GetFeature` + the raw `attrFilter` for lineage.
#[test]
fn vworld_cadastral_bronze_plan_emits_human_readable_pnu_scope_key() -> TestResult {
    let pnu = "9999900801105800001";
    let payload = single_feature_payload(pnu);
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000016")?);

    let plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: request_with_attr_filter(&format!("pnu:=:{pnu}")),
        raw_payload,
        payload,
    })?;

    // Object key: source + dataset + human-readable pnu scope + page leaf. No operation=, no
    // filter_kind=, no filter_sha256=, no size=.
    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__cadastral/dataset=LP_PA_CBND_BUBUN/pnu=9999900801105800001/page-000001.json"
    );
    // Lineage (source_partition_key) keeps the provider operation + the FULL hashed attr scope is
    // gone; the human scope replaces it, but operation + dataset + page are still present.
    assert_eq!(
        plan.source_partition_key,
        "operation=GetFeature/dataset=LP_PA_CBND_BUBUN/pnu=9999900801105800001/page=000001"
    );
    assert_eq!(plan.logical_record_count, 1);
    // request_params keep the provider operation + the raw attr_filter for traceability.
    assert_eq!(plan.request_params["operation"], "GetFeature");
    assert_eq!(plan.request_params["service"], "data");
    assert_eq!(plan.request_params["request"], "GetFeature");
    assert_eq!(plan.request_params["data"], "LP_PA_CBND_BUBUN");
    assert_eq!(plan.request_params["format"], "json");
    assert_eq!(plan.request_params["attrFilter"], format!("pnu:=:{pnu}"));
    assert_eq!(plan.request_params["geometry"], "true");
    assert_eq!(plan.request_params["attribute"], "true");
    assert_eq!(plan.request_params["crs"], "EPSG:4326");
    assert_eq!(plan.request_params["page"], 1);
    assert_eq!(plan.request_params["size"], 1000);
    assert!(plan.request_params.get("pageNo").is_none());
    assert!(plan.request_params.get("numOfRows").is_none());

    let pnu_profile = plan
        .schema_observations
        .iter()
        .find(|field| {
            field.field_path == "response.result.featureCollection.features[].properties.pnu"
        })
        .ok_or("expected pnu schema observation")?;
    assert_eq!(pnu_profile.observed_type, SchemaObservedType::String);
    assert!(pnu_profile.candidate_key_score > 0.99);
    Ok(())
}

/// Single clean `emdCd:=:<code>` filter → `emd=<code>` (the field is renamed `emdCd`→`emd`); the
/// raw `emdCd:=:...` is still carried in `request_params` for lineage.
#[test]
fn vworld_cadastral_bronze_plan_emits_region_scope_key_for_emd_filter() -> TestResult {
    let payload = single_feature_payload("9999900601105800001");
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000017")?);

    let plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        request: request_with_attr_filter("emdCd:=:11680103"),
        raw_payload,
        payload,
    })?;

    assert_eq!(
        plan.object_key.as_str(),
        "bronze/source=vworldkr__cadastral/dataset=LP_PA_CBND_BUBUN/emd=11680103/page-000001.json"
    );
    assert_eq!(
        plan.source_partition_key,
        "operation=GetFeature/dataset=LP_PA_CBND_BUBUN/emd=11680103/page=000001"
    );
    // Lineage keeps the provider's own field name (`emdCd`), only the object key renames it to `emd`.
    assert_eq!(plan.request_params["attrFilter"], "emdCd:=:11680103");
    Ok(())
}

/// A compound / multi-clause filter cannot be reduced to a single clean `field:=:value`, so the
/// object key falls back to `filter_fingerprint=<12 hex>` (NOT a bare 64-hex sha, and NEVER
/// mislabeled `pnu=`/`emd=`). The fingerprint is the first 12 hex chars of the canonical
/// `attr_filter` sha256.
#[test]
fn vworld_cadastral_bronze_plan_falls_back_to_fingerprint_for_compound_filter() -> TestResult {
    let payload = single_feature_payload("9999900801105800001");
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000018")?);

    let plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        // Two AND-ed clauses: not a single field=value, so it must fingerprint, not guess pnu=.
        request: request_with_attr_filter("emdCd:=:11680103 AND jibun:LIKE:580"),
        raw_payload,
        payload,
    })?;

    let key = plan.object_key.as_str();
    let prefix = "bronze/source=vworldkr__cadastral/dataset=LP_PA_CBND_BUBUN/filter_fingerprint=";
    assert!(
        key.starts_with(prefix),
        "compound filter must fall back to filter_fingerprint, got: {key}"
    );
    let suffix = "/page-000001.json";
    assert!(key.ends_with(suffix), "unexpected leaf: {key}");
    let fingerprint = &key[prefix.len()..key.len() - suffix.len()];
    // Exactly 12 lowercase-hex chars — short, intentional, NOT a bare 64-hex sha.
    assert_eq!(
        fingerprint.len(),
        12,
        "fingerprint must be 12 hex chars: {fingerprint}"
    );
    assert!(
        fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "fingerprint must be lowercase hex: {fingerprint}"
    );
    // Never mislabeled as a clean single-field scope.
    assert!(
        !key.contains("/pnu="),
        "compound filter must not be labeled pnu=: {key}"
    );
    assert!(
        !key.contains("/emd="),
        "compound filter must not be labeled emd=: {key}"
    );
    // The raw compound filter is still preserved for lineage.
    assert_eq!(
        plan.request_params["attrFilter"],
        "emdCd:=:11680103 AND jibun:LIKE:580"
    );
    Ok(())
}

/// A `field:op:value` whose VALUE itself contains `:` cannot be parsed unambiguously into a single
/// clean scope, so the safe direction is to fingerprint (never guess the value boundary). This
/// proves the parser refuses to mislabel a colon-bearing value as a clean `pnu=`/`emd=`.
#[test]
fn vworld_cadastral_bronze_plan_fingerprints_value_containing_colon() -> TestResult {
    let payload = single_feature_payload("9999900801105800001");
    let raw_payload = serde_json::to_vec(&payload)?;
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000019")?);

    let plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: run_id,
        // The value `a:b` makes `field:op:value` ambiguous → must fingerprint, never label pnu=.
        request: request_with_attr_filter("pnu:=:a:b"),
        raw_payload,
        payload,
    })?;

    let key = plan.object_key.as_str();
    assert!(
        key.contains("/filter_fingerprint="),
        "a value containing ':' must fingerprint, got: {key}"
    );
    assert!(
        !key.contains("/pnu="),
        "colon value must not be labeled pnu=: {key}"
    );
    Ok(())
}

#[test]
fn vworld_cadastral_bronze_plan_rejects_unbounded_requests() -> TestResult {
    let payload = json!({"response": {"result": {"featureCollection": {"features": []}}}});
    let error = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date: NaiveDate::from_ymd_opt(2026, 5, 18).ok_or("valid date")?,
        ingestion_run_id: IngestionRunId::new(Uuid::nil()),
        request: VWorldCadastralPageRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: None,
            columns: Vec::new(),
            geometry: true,
            attribute: true,
            crs: Some("EPSG:4326".to_owned()),
            page: 1,
            size: 1000,
        },
        raw_payload: b"{}".to_vec(),
        payload,
    })
    .err()
    .ok_or("unbounded VWorld cadastral request must be rejected")?;

    assert!(
        error.to_string().contains("attr_filter is required"),
        "unexpected error: {error}"
    );
    Ok(())
}
