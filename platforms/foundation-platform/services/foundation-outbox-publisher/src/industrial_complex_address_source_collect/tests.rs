use super::{
    assert_records_present, compile_plan, detail_complex_codes, fetched_dataset,
    live_write_enabled, persist_with_adapters, planned_request_count, request_spacing,
    unlisted_profile_codes, AddressSourceCollectConfig, AddressSourceCollectReport, FetchedDataset,
    IlisDataset, ILIS_DATASETS, ILIS_DETAIL_DATASET, MIN_REQUEST_SPACING,
    NATIONAL_SCOPE_PARTITION_NAME, NATIONAL_SCOPE_PARTITION_VALUE, PROVIDER,
};

use std::{path::Path, sync::Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_domain::{
    provider_id, source_slug, BronzeObject, CollectionError, IngestionRun, SchemaProfile,
    SourceCatalogEntry,
};
use foundation_outbox::FileObjectStorage;
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use serde_json::json;
use uuid::Uuid;

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

/// The detail requests a live run makes are the profile's complexes the bulk list lacks: each once,
/// in profile order, and none for a code the list carries (codes are synthetic).
#[test]
fn detail_codes_are_the_profile_codes_the_bulk_list_lacks() {
    let profile = ["99999A", "99999B", " ", "99999C", "99999B"].map(str::to_owned);
    let list = json!({ "result": { "dataList": [
        { "danji_cd": "99999A" }, { "danji_cd": " 99999C " }, { "danji_cd": null }
    ] } });
    assert_eq!(
        unlisted_profile_codes(&profile, &list),
        vec!["99999B".to_owned()]
    );
    // A list without its records array lacks every profile code.
    assert_eq!(
        unlisted_profile_codes(&profile[..1], &json!({})),
        vec!["99999A".to_owned()]
    );
}

#[derive(Default)]
struct RecordingUow {
    runs: Mutex<Vec<IngestionRun>>,
    objects: Mutex<Vec<BronzeObject>>,
}

fn poisoned(_: impl std::fmt::Debug) -> CollectionError {
    CollectionError::Infrastructure("lock poisoned".to_owned())
}

#[async_trait]
impl BronzeIngestUnitOfWork for RecordingUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        // One id per slug, as the ledger's upsert on `slug` returns.
        let mut source = entry.clone();
        source.id = SourceCatalogId::new(Uuid::from_u128(u128::from(
            entry.slug.bytes().map(u32::from).sum::<u32>(),
        )));
        Ok(source)
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        self.runs.lock().map_err(poisoned)?.push(run.clone());
        Ok(run.clone())
    }

    async fn complete_ingestion_run(
        &self,
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        self.runs
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|run| run.id == command.id)
            .cloned()
            .ok_or_else(|| CollectionError::IngestionRunNotFound(command.id.to_string()))
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(self
            .objects
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|o| {
                o.source_catalog_id == source_catalog_id && o.object_key.as_str() == object_key
            })
            .cloned())
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        self.objects.lock().map_err(poisoned)?.push(object.clone());
        Ok(object.clone())
    }

    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        Ok(profile.clone())
    }
}

fn at() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2099-01-02T04:40:00Z")
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_default()
}

fn live_config() -> anyhow::Result<AddressSourceCollectConfig> {
    Ok(AddressSourceCollectConfig {
        base_uri: "http://127.0.0.1:9".to_owned(),
        user_agent: "test".to_owned(),
        request_spacing: request_spacing(None)?,
        max_requests: 8,
        live_write: true,
        summary_path: None,
        detail_complex_codes: Vec::new(),
        collect_bulk_datasets: true,
    })
}

/// The complex list as ILIS answers it (synthetic codes), its rows in the order given.
fn list_answer(codes: &[&str]) -> anyhow::Result<FetchedDataset> {
    let rows: Vec<_> = codes
        .iter()
        .enumerate()
        .map(|(index, code)| json!({"rn": index + 1, "danji_cd": code, "addr_cd": "9999900000"}))
        .collect();
    let payload =
        json!({"code": 200, "msg": "ok", "result": {"cnt": rows.len(), "dataList": rows}});
    fetched_dataset(
        ILIS_DATASETS[0],
        NATIONAL_SCOPE_PARTITION_NAME,
        NATIONAL_SCOPE_PARTITION_VALUE,
        serde_json::to_vec(&payload)?,
        payload,
    )
}

async fn collect(
    uow: &RecordingUow,
    storage: &FileObjectStorage,
    answer: FetchedDataset,
    at: DateTime<Utc>,
) -> anyhow::Result<AddressSourceCollectReport> {
    persist_with_adapters(
        &live_config()?,
        IngestionRunId::new(Uuid::new_v4()),
        at,
        &[answer],
        uow,
        storage,
    )
    .await
}

fn write_object(root: &Path, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let path = root.join(key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

fn recorded(uow: &RecordingUow) -> anyhow::Result<usize> {
    Ok(uow
        .objects
        .lock()
        .map_err(|_| anyhow::anyhow!("lock poisoned"))?
        .len())
}

const HAND_LOAD_KEY: &str = "bronze/source=industrylandorkr__industrial_complex_list/operation=danjiList/scope=national/page-000001.json";
const HAND_LOAD_BYTES: &[u8] = br#"{"hand":"load"}"#;

/// 2026-11-22: the first two scheduled runs stopped on a Bronze checksum conflict. A hand load of
/// 2026-08-18 held the request-only key `.../scope=national/page-000001.json` with another answer
/// and no ledger row, and the write-once bucket refused the new one. The answer now lands under its
/// content-qualified key beside that object, which stays as it is; the same answer again records
/// nothing and reports unchanged; a reordered answer is a new object.
#[tokio::test]
async fn a_new_answer_lands_beside_the_hand_load_and_the_same_answer_is_unchanged(
) -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("ilis-address-source-{}", Uuid::new_v4()));
    let storage = FileObjectStorage::new(&root)?;
    let uow = RecordingUow::default();
    write_object(&root, HAND_LOAD_KEY, HAND_LOAD_BYTES)?;

    let first = collect(&uow, &storage, list_answer(&["99999A", "99999B"])?, at()).await?;
    let landed = &first.datasets[0];
    assert!(landed.written);
    assert_eq!(
        landed.object_key,
        format!(
            "bronze/source=industrylandorkr__industrial_complex_list/operation=danjiList/scope=national/page-000001--sha256-{}.json",
            landed.checksum_sha256
        )
    );
    assert!(root.join(&landed.object_key).is_file());
    assert_eq!(std::fs::read(root.join(HAND_LOAD_KEY))?, HAND_LOAD_BYTES);

    // The next day, the same answer: the same object, no new row, `unchanged`.
    let again = collect(
        &uow,
        &storage,
        list_answer(&["99999A", "99999B"])?,
        at() + Duration::days(1),
    )
    .await?;
    assert!(!again.datasets[0].written);
    assert_eq!(again.datasets[0].object_key, landed.object_key);
    assert_eq!(recorded(&uow)?, 1);

    // The same rows in another order, as the notice list comes back: other bytes, a new object.
    let reordered = collect(&uow, &storage, list_answer(&["99999B", "99999A"])?, at()).await?;
    assert!(reordered.datasets[0].written);
    assert_ne!(reordered.datasets[0].object_key, landed.object_key);
    assert_eq!(recorded(&uow)?, 2);
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

/// The content-qualified objects of 2026-09-26..30 have no row in the production ledger. An answer
/// equal to one of them is that object: the commit records its missing row (the committer's
/// recovery) rather than refusing it, and the run reports it as new to the ledger.
#[tokio::test]
async fn an_answer_bronze_holds_without_a_row_is_recorded_not_refused() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!("ilis-address-source-{}", Uuid::new_v4()));
    let storage = FileObjectStorage::new(&root)?;
    let uow = RecordingUow::default();
    let answer = list_answer(&["99999A"])?;
    let planned = compile_plan(&answer, IngestionRunId::new(Uuid::new_v4()), at())?;
    write_object(&root, planned.object_key.as_str(), &answer.raw_payload)?;

    let report = collect(&uow, &storage, answer, at()).await?;
    assert_eq!(report.datasets[0].object_key, planned.object_key.as_str());
    assert!(report.datasets[0].written);
    assert_eq!(recorded(&uow)?, 1);
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
