use super::{
    assert_records_present, detail_complex_codes, live_write_enabled, planned_request_count,
    request_spacing, IlisDataset, ILIS_DATASETS, ILIS_DETAIL_DATASET, MIN_REQUEST_SPACING,
    PROVIDER,
};

use collection_domain::{provider_id, source_slug};
use serde_json::json;

/// The join key is the code, so every dataset this lane collects must be addressable by it. A
/// dataset whose slug drifts off the canonical grammar cannot produce a Bronze key at all, and the
/// generator — not a literal in this file — is what decides that.
/// Every dataset this lane can collect, bulk and detail alike.
fn all_datasets() -> Vec<IlisDataset> {
    let mut datasets = ILIS_DATASETS.to_vec();
    datasets.push(ILIS_DETAIL_DATASET);
    datasets
}

#[test]
fn every_declared_dataset_produces_a_canonical_source_slug() -> anyhow::Result<()> {
    assert_eq!(provider_id(PROVIDER).as_deref(), Some("industrylandorkr"));
    for dataset in all_datasets() {
        let slug = source_slug(PROVIDER, dataset.dataset_slug)?;
        assert!(
            slug.starts_with("industrylandorkr__"),
            "unexpected slug {slug}"
        );
    }
    Ok(())
}

/// The bulk page size is the reason this lane sends two requests instead of a hundred and fifty.
/// A page size that dropped below the known row counts would silently truncate the collection into
/// a first page that looks complete.
#[test]
fn bulk_page_sizes_cover_the_known_row_counts() {
    for dataset in ILIS_DATASETS {
        let known_rows = match dataset.dataset_slug {
            // 1,473 complexes and 18,583 notices were counted in the source investigation.
            "industrial_complex_list" => 1_473,
            "industrial_complex_notice" => 18_583,
            other => panic!("undeclared dataset {other}"),
        };
        assert!(
            dataset.page_size >= known_rows,
            "{} page size {} does not cover {known_rows} rows",
            dataset.dataset_slug,
            dataset.page_size
        );
    }
}

#[test]
fn declared_operations_are_distinct_and_key_safe() {
    let datasets = all_datasets();
    let mut operations: Vec<&str> = datasets.iter().map(|d| d.operation).collect();
    operations.sort_unstable();
    let count = operations.len();
    operations.dedup();
    assert_eq!(operations.len(), count, "two datasets share one operation");
    for dataset in &datasets {
        assert!(
            dataset.operation.bytes().all(|b| b.is_ascii_alphanumeric()),
            "operation {} is not object-key safe",
            dataset.operation
        );
    }
}

/// The budget is the operator's. It is counted before the first request, so a run that cannot
/// finish inside the ceiling never spends half of it first.
#[test]
fn the_request_count_is_known_before_the_first_request() {
    let codes = vec!["244460".to_owned(), "247110".to_owned()];
    assert_eq!(planned_request_count(true, &codes), ILIS_DATASETS.len() + 2);
    assert_eq!(planned_request_count(false, &codes), 2);
    assert_eq!(planned_request_count(true, &[]), ILIS_DATASETS.len());
    assert_eq!(planned_request_count(false, &[]), 0);
}

/// Each detail code is one request, so a repeat or a malformed entry must fail here rather than at
/// the provider.
#[test]
fn detail_codes_are_validated_and_deduplicated_before_any_request() -> anyhow::Result<()> {
    assert_eq!(detail_complex_codes(None)?, Vec::<String>::new());
    assert_eq!(detail_complex_codes(Some(" , "))?, Vec::<String>::new());
    assert_eq!(
        detail_complex_codes(Some(" 244460 , 247110 "))?,
        vec!["244460".to_owned(), "247110".to_owned()]
    );
    assert!(detail_complex_codes(Some("244460,244460")).is_err());
    assert!(detail_complex_codes(Some("../../etc")).is_err());
    assert!(detail_complex_codes(Some("244 460")).is_err());
    Ok(())
}

/// The detail endpoint answers with one record, not a list, and it still has to be recognised as
/// records — otherwise the fallback for the complexes the bulk list omits fails on every code.
#[test]
fn a_single_record_detail_response_counts_as_records() -> anyhow::Result<()> {
    assert_records_present(
        &ILIS_DETAIL_DATASET,
        &json!({ "result": { "data": { "danji_cd": "99999", "addr_cd": "1153000000" } } }),
    )?;
    assert!(
        assert_records_present(&ILIS_DETAIL_DATASET, &json!({ "result": { "data": {} } })).is_err()
    );
    assert!(assert_records_present(&ILIS_DETAIL_DATASET, &json!({ "result": null })).is_err());
    // The bulk envelope must not satisfy the detail dataset: they really are different shapes.
    assert!(assert_records_present(
        &ILIS_DETAIL_DATASET,
        &json!({ "result": { "dataList": [] } })
    )
    .is_err());
    Ok(())
}

/// A provider that renames its envelope must fail loudly and hand the operator the new shape. A
/// zero-length records array would otherwise be committed as a Bronze object that looks collected.
#[test]
fn a_missing_records_array_names_the_arrays_the_response_does_carry() {
    let dataset = ILIS_DATASETS[0];
    let payload = json!({ "result": { "rows": [ { "danji_cd": "99999" } ] }, "count": 1 });

    let error = assert_records_present(&dataset, &payload)
        .expect_err("a response without the declared array must fail");

    let message = format!("{error:#}");
    assert!(message.contains(dataset.logical_items_pointer), "{message}");
    assert!(message.contains("/result/rows (len 1)"), "{message}");
}

#[test]
fn a_present_records_array_is_accepted() -> anyhow::Result<()> {
    let dataset = ILIS_DATASETS[0];
    assert_records_present(&dataset, &json!({ "result": { "dataList": [] } }))?;
    Ok(())
}

/// The politeness floor belongs to the provider. A configured value may only slow the run down.
#[test]
fn configured_spacing_can_only_widen_the_provider_floor() -> anyhow::Result<()> {
    let floor = MIN_REQUEST_SPACING.as_millis();
    assert_eq!(request_spacing(None)?.interval_millis(), floor);
    assert_eq!(request_spacing(Some(1))?.interval_millis(), floor);
    assert_eq!(request_spacing(Some(0))?.interval_millis(), floor);
    assert_eq!(request_spacing(Some(5_000))?.interval_millis(), 5_000);
    Ok(())
}

#[test]
fn live_write_is_off_unless_explicitly_enabled() {
    for enabled in ["1", "true", "TRUE", "yes"] {
        assert!(live_write_enabled(Some(enabled)), "{enabled}");
    }
    for disabled in [None, Some(""), Some("0"), Some("false"), Some("dry-run")] {
        assert!(!live_write_enabled(disabled), "{disabled:?}");
    }
}

/// The endpoint catalog is the human-facing SSOT for what we collect and from where. A lane whose
/// datasets are not in it is a collector nobody can find by reading the catalog, and the two lists
/// would be free to drift; this reads the catalog rather than restating it.
#[test]
fn every_declared_dataset_is_registered_in_the_endpoint_catalog() -> anyhow::Result<()> {
    let catalog_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/catalog/public-source-endpoint-catalog.v1.json");
    let catalog: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&catalog_path)?)?;
    let endpoints = catalog["endpoints"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("endpoints must be an array"))?;

    for dataset in all_datasets() {
        let slug = source_slug(PROVIDER, dataset.dataset_slug)?;
        let entry = endpoints
            .iter()
            .find(|entry| entry["bronze"]["source_slug"] == slug.as_str())
            .ok_or_else(|| anyhow::anyhow!("{slug} is absent from the endpoint catalog"))?;
        assert_eq!(entry["provider"], PROVIDER, "{slug}");
        assert_eq!(entry["operation"], dataset.operation, "{slug}");
        assert_eq!(entry["dataset_slug"], dataset.dataset_slug, "{slug}");
    }
    Ok(())
}

#[test]
fn the_dataset_table_is_not_empty() {
    // The table is the collection scope. An empty one would make every assertion above vacuous.
    assert!(!ILIS_DATASETS.is_empty());
    let _: &IlisDataset = &ILIS_DATASETS[0];
}
