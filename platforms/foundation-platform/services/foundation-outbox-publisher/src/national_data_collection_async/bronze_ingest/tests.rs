//! Unit tests for the async Bronze-commit context (`super`).
//!
//! These mirror the sync committer tests but exercise the async lane's seam: a page committed
//! through [`BronzeIngestContext::commit_page`] now (a) writes its raw payload `CreateOnly` and
//! (b) records a `bronze_object` row against the per-`source_slug` `(source_catalog_id,
//! ingestion_run_id)` the pre-pass resolved — and a 412 + no-row case self-heals by recording the
//! missing row (the recoverable commit protocol, ADR 0016).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use chrono::Utc;
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_building_register_bronze_page, BuildingRegisterBronzePagePlanInput,
    BuildingRegisterPageRequest,
};
use collection_domain::CollectionError;
use collection_domain::{BronzeObject, IngestionRun, SchemaProfile, SourceCatalogEntry};
use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};
use foundation_outbox::{ObjectStorageService, PublishError};
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use uuid::Uuid;

use super::{BronzeIngestContext, LedgerEntry};

type TestResult = anyhow::Result<()>;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

const SOURCE_SLUG: &str = "datagokr__building_register_main";

fn building_register_entry() -> LedgerEntry {
    LedgerEntry {
        job_id: "job-async-1".to_owned(),
        provider: "data.go.kr".to_owned(),
        endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".to_owned(),
        endpoint: "getBrTitleInfo".to_owned(),
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11680".to_owned(),
        bjdong_cd: "10300".to_owned(),
        lawd_cd: String::new(),
        deal_ymd: String::new(),
        scope_unit_id: "scope:legal-dong:1168010300".to_owned(),
        shard_id: "national-shard-0001".to_owned(),
        idempotency_key: "test/job-async-1".to_owned(),
        source_slug: SOURCE_SLUG.to_owned(),
        request_fingerprint_sha256: "a".repeat(64),
        request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
            .to_owned(),
        collection_snapshot_id: "registry:test".to_owned(),
        status: "planned".to_owned(),
        page_start: Some(1),
        page_end: Some(1),
        max_pages: 1,
        num_of_rows: 100,
        request_count_estimate: 1,
    }
}

fn building_register_request() -> (BuildingRegisterPageRequest, Vec<u8>, serde_json::Value) {
    let payload = serde_json::json!({
        "response": { "body": { "items": { "item": [
            { "mgmBldrgstPk": "11680-10300-1", "totArea": "100.25" }
        ] } } }
    });
    let raw_payload = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    let request = BuildingRegisterPageRequest {
        operation: "getBrTitleInfo".to_owned(),
        sigungu_cd: "11680".to_owned(),
        bjdong_cd: "10300".to_owned(),
        page_no: 1,
        num_of_rows: 100,
    };
    (request, raw_payload, payload)
}

/// Fake object storage: records writes, can be pre-seeded with already-present keys (whose stored
/// `x-amz-meta-sha256` `read_object_sha256` returns) to drive the `CreateOnly`-collision recovery.
#[derive(Default)]
struct FakeStorage {
    writes: Mutex<Vec<PutObjectRequest>>,
    existing: Mutex<BTreeMap<String, Option<String>>>,
}

impl FakeStorage {
    fn with_existing(key: &str, sha256: Option<&str>) -> Self {
        let storage = Self::default();
        lock(&storage.existing).insert(key.to_owned(), sha256.map(ToOwned::to_owned));
        storage
    }

    fn writes(&self) -> Vec<PutObjectRequest> {
        lock(&self.writes).clone()
    }
}

#[async_trait]
impl ObjectStorageService for FakeStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        if matches!(request.write_mode, ObjectWriteMode::CreateOnly)
            && lock(&self.existing).contains_key(&request.key)
        {
            return Err(PublishError::ObjectAlreadyExists {
                key: request.key.clone(),
            });
        }
        lock(&self.writes).push(request);
        Ok(())
    }

    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError> {
        Ok(lock(&self.existing).get(key).cloned().flatten())
    }
}

/// Fake unit-of-work: records source upserts, run creations, and bronze_object rows; can be
/// pre-seeded with an existing row to exercise the "row exists" recovery branches.
#[derive(Default)]
struct FakeUow {
    sources: Mutex<Vec<SourceCatalogEntry>>,
    runs: Mutex<Vec<IngestionRun>>,
    recorded: Mutex<Vec<BronzeObject>>,
    existing_rows: Mutex<BTreeMap<String, BronzeObject>>,
}

impl FakeUow {
    fn recorded(&self) -> Vec<BronzeObject> {
        lock(&self.recorded).clone()
    }

    fn created_runs(&self) -> Vec<IngestionRun> {
        lock(&self.runs).clone()
    }

    fn upserted_sources(&self) -> Vec<SourceCatalogEntry> {
        lock(&self.sources).clone()
    }
}

#[async_trait]
impl BronzeIngestUnitOfWork for FakeUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        lock(&self.sources).push(entry.clone());
        Ok(entry.clone())
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        lock(&self.runs).push(run.clone());
        Ok(run.clone())
    }

    async fn complete_ingestion_run(
        &self,
        _command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        Err(CollectionError::Infrastructure(
            "complete_ingestion_run not used by the async lane".to_owned(),
        ))
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(lock(&self.existing_rows)
            .get(object_key)
            .filter(|row| row.source_catalog_id == source_catalog_id)
            .cloned())
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        lock(&self.recorded).push(object.clone());
        Ok(object.clone())
    }

    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        Ok(profile.clone())
    }
}

/// `prepare` upserts one source + creates one run per distinct slug, and a subsequent
/// `commit_page` writes the raw payload `CreateOnly` AND records exactly one `bronze_object` row
/// bound to the prepared `(source_catalog_id, ingestion_run_id)`. This is the ADR-0016 option-a
/// proof: the async lane now WRITES a `bronze_object` row (it previously wrote none).
#[tokio::test]
async fn commit_page_writes_object_create_only_and_records_bronze_object_row() -> TestResult {
    let now = Utc::now();
    let selected = vec![building_register_entry()];
    let storage = Arc::new(FakeStorage::default());
    let uow = Arc::new(FakeUow::default());

    let context = BronzeIngestContext::prepare(
        Arc::clone(&storage) as Arc<dyn ObjectStorageService>,
        Arc::clone(&uow) as Arc<dyn BronzeIngestUnitOfWork>,
        &selected,
        now,
    )
    .await?;

    // Exactly one source upsert + one run created for the single slug.
    assert_eq!(uow.upserted_sources().len(), 1);
    assert_eq!(uow.upserted_sources()[0].slug, SOURCE_SLUG);
    assert_eq!(uow.created_runs().len(), 1);
    let prepared = context.source_run(SOURCE_SLUG)?;

    let (request, raw_payload, payload) = building_register_request();
    // Reference object identity the lane's own plan produces (the committer compiles the same plan).
    let expected_plan = plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
        source_slug: SOURCE_SLUG,
        ingest_date: now.date_naive(),
        ingestion_run_id: prepared.ingestion_run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    let outcome = context
        .commit_page(
            SOURCE_SLUG,
            now.date_naive(),
            now,
            request,
            raw_payload.clone(),
            payload,
        )
        .await?;

    // Same object key + checksum the direct put would have used (byte-identical key).
    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);

    // The storage write happened once, write-once (CreateOnly) + sha256 stamped, at the compiled key.
    let writes = storage.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].key, expected_plan.object_key.as_str());
    assert_eq!(writes[0].body, raw_payload);
    assert_eq!(writes[0].write_mode, ObjectWriteMode::CreateOnly);
    assert_eq!(
        writes[0].sha256.as_deref(),
        Some(expected_plan.checksum_sha256.as_str())
    );

    // The async lane now records a bronze_object row (the option-a change), bound to the prepared
    // source + run identity.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    let row = &recorded[0];
    assert_eq!(row.object_key.as_str(), expected_plan.object_key.as_str());
    assert_eq!(row.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(row.dedupe_key, expected_plan.dedupe_key);
    assert_eq!(row.source_catalog_id, prepared.source_catalog_id);
    assert_eq!(row.ingestion_run_id, prepared.ingestion_run_id);
    assert_eq!(row.id, outcome.bronze_object_id);
    Ok(())
}

/// Recovery: a prior run wrote the object to R2 (matching `x-amz-meta-sha256`) but its DB record
/// failed (no row). The `CreateOnly` write now hits 412 / already-exists; the shared recovery core
/// records the missing row and succeeds — no fresh write, exactly one recovered row.
#[tokio::test]
async fn commit_page_recovers_when_object_exists_but_row_missing() -> TestResult {
    let now = Utc::now();
    let selected = vec![building_register_entry()];

    // First, plan against a placeholder run to learn the deterministic key+checksum to pre-seed.
    let (request, raw_payload, payload) = building_register_request();
    let reference_plan = plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
        source_slug: SOURCE_SLUG,
        ingest_date: now.date_naive(),
        ingestion_run_id: IngestionRunId::new(Uuid::new_v4()),
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    // Object already present with matching checksum, but NO bronze_object row recorded yet.
    let storage = Arc::new(FakeStorage::with_existing(
        reference_plan.object_key.as_str(),
        Some(&reference_plan.checksum_sha256),
    ));
    let uow = Arc::new(FakeUow::default());

    let context = BronzeIngestContext::prepare(
        Arc::clone(&storage) as Arc<dyn ObjectStorageService>,
        Arc::clone(&uow) as Arc<dyn BronzeIngestUnitOfWork>,
        &selected,
        now,
    )
    .await?;

    let outcome = context
        .commit_page(
            SOURCE_SLUG,
            now.date_naive(),
            now,
            request,
            raw_payload,
            payload,
        )
        .await?;

    assert_eq!(outcome.object_key, reference_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, reference_plan.checksum_sha256);

    // No fresh write (object already existed), but the missing row was recovered (recorded once).
    assert!(storage.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        reference_plan.object_key.as_str()
    );
    assert_eq!(recorded[0].checksum_sha256, reference_plan.checksum_sha256);
    assert_eq!(recorded[0].id, outcome.bronze_object_id);
    Ok(())
}
