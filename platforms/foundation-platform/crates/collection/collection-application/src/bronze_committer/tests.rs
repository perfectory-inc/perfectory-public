//! Unit tests for the Bronze commit boundary (`super::bronze_committer`).
//!
//! Extracted into this `#[path]` submodule (the parent `bronze_committer.rs` carries the
//! production code) so each file stays focused and comfortably under the AGENTS.md 500-review /
//! 1500-block size guidance after the per-lane page-commit dedup (Task 3). The tests are unchanged:
//! they remain the behaviour proof that the generic `commit_public_data_page` (and its thin
//! building-register / real-transaction wrappers) produce the same object key, bytes, recorded row,
//! and recovery semantics as before.

use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{NaiveDate, Utc};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, SchemaProfile, SnapshotBasis, SnapshotGranularity,
    SourceCatalogEntry,
};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKey;
use uuid::Uuid;

use std::collections::BTreeMap;

use super::{
    BronzeCommitError, BronzeCommitter, BronzePayload, BronzeRawObjectWriter, BronzeStorageError,
    BronzeWriteMode, BronzeWriteOutcome, BronzeWriteRequest, BuildingRegisterCommitInput,
    PlannedBronzeObject, RealTransactionCommitInput, VWorldCadastralCommitInput,
    VWorldLandRegisterCommitInput, VWorldNedCommitInput,
};
use crate::building_register_bronze_plan::{
    plan_building_register_bronze_page, BuildingRegisterBronzePagePlanInput,
    BuildingRegisterPageRequest,
};
use crate::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use crate::real_transaction_bronze_plan::{
    plan_real_transaction_bronze_page, RealTransactionBronzePagePlanInput,
    RealTransactionPageRequest,
};
use crate::vworld_cadastral_bronze_plan::{
    plan_vworld_cadastral_bronze_page, VWorldCadastralBronzePagePlanInput,
    VWorldCadastralPageRequest,
};
use crate::vworld_land_register_bronze_plan::{
    plan_vworld_land_register_bronze_page, VWorldLandRegisterBronzePagePlanInput,
    VWorldLandRegisterPageRequest,
};
use crate::vworld_ned_bronze_plan::{
    plan_vworld_ned_bronze_page, VWorldNedBronzePagePlanInput, VWorldNedPageRequest,
};

type TestResult = anyhow::Result<()>;

/// Locks a test mutex, recovering from poison so a panicking test cannot cascade into spurious
/// failures elsewhere (and so the denied `expect_used`/`unwrap_used` lints stay satisfied).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn sample_object_key() -> anyhow::Result<ObjectKey> {
    Ok(collection_domain::build_bronze_object_key(
        collection_domain::BronzeObjectKeyParts {
            source_slug: "datagokr__building_register_main",
            partition_path: "sigungu=11680/bjdong=10300",
            leaf_name: "page-000001",
            extension: "json",
        },
    )?)
}

fn sample_planned(payload: Vec<u8>, checksum: &str) -> anyhow::Result<PlannedBronzeObject> {
    let object_key = sample_object_key()?;
    let now = Utc::now();
    let record = BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id: SourceCatalogId::new(Uuid::new_v4()),
        ingestion_run_id: IngestionRunId::new(Uuid::new_v4()),
        source_record_id: None,
        source_partition_key: Some(
            "operation=getBrTitleInfo/sigungu=11680/bjdong=10300/page=000001".to_owned(),
        ),
        source_identity_key: "sigungu=11680/bjdong=10300/page=000001/page_size=100".to_owned(),
        dedupe_key: format!("datagokr__building_register_main:p:sha256={checksum}"),
        request_params: serde_json::json!({ "operation": "getBrTitleInfo" }),
        object_key: object_key.clone(),
        checksum_sha256: checksum.to_owned(),
        content_type: "application/json".to_owned(),
        size_bytes: payload.len() as u64,
        logical_record_count: Some(1),
        collected_at: now,
        snapshot_period: None,
        snapshot_date: NaiveDate::from_ymd_opt(2026, 5, 14)
            .ok_or_else(|| anyhow::anyhow!("invalid snapshot date"))?,
        snapshot_granularity: SnapshotGranularity::Day,
        snapshot_basis: SnapshotBasis::CollectedAtFallback,
        provider_file_id: None,
        provider_file_name: None,
        provider_updated_at: None,
        effective_date: None,
        created_at: now,
    };
    Ok(PlannedBronzeObject {
        object_key: object_key.as_str().to_owned(),
        payload: BronzePayload::InMemory(payload),
        content_type: "application/json".to_owned(),
        cache_control: "no-store, max-age=0".to_owned(),
        checksum_sha256: checksum.to_owned(),
        record,
    })
}

/// Pre-seeded state for a key the fake storage already holds, used to simulate the
/// `CreateOnly`-collision branches of the recovery protocol.
#[derive(Clone, Debug)]
struct ExistingObject {
    /// Stored `x-amz-meta-sha256`, or `None` for an object written without checksum metadata.
    sha256: Option<String>,
}

/// Fake storage that records writes and can be pre-seeded with already-present keys.
///
/// Simulates the three storage states the recovery tree branches on:
/// - key ABSENT: `write_object` records the write and returns [`BronzeWriteOutcome::Written`];
/// - key EXISTS (same/diff sha, or no sha): `write_object` returns
///   [`BronzeWriteOutcome::AlreadyExists`] without writing, and `read_object_sha256` returns the
///   seeded checksum.
#[derive(Default)]
struct RecordingWriter {
    writes: Mutex<Vec<BronzeWriteRequest>>,
    existing: Mutex<BTreeMap<String, ExistingObject>>,
    fail_message: Option<String>,
}

impl RecordingWriter {
    fn failing(message: &str) -> Self {
        Self {
            writes: Mutex::new(Vec::new()),
            existing: Mutex::new(BTreeMap::new()),
            fail_message: Some(message.to_owned()),
        }
    }

    /// Pre-seeds an existing object at `key` whose stored checksum is `sha256`
    /// (`None` => the object has no `x-amz-meta-sha256` metadata).
    fn with_existing(key: &str, sha256: Option<&str>) -> Self {
        let writer = Self::default();
        lock(&writer.existing).insert(
            key.to_owned(),
            ExistingObject {
                sha256: sha256.map(ToOwned::to_owned),
            },
        );
        writer
    }

    fn writes(&self) -> Vec<BronzeWriteRequest> {
        lock(&self.writes).clone()
    }
}

#[async_trait::async_trait]
impl BronzeRawObjectWriter for RecordingWriter {
    async fn write_object(
        &self,
        request: BronzeWriteRequest,
    ) -> Result<BronzeWriteOutcome, BronzeStorageError> {
        if let Some(message) = &self.fail_message {
            return Err(BronzeStorageError(message.clone()));
        }
        // CreateOnly collision: a pre-seeded key reports already-exists without writing.
        if matches!(request.write_mode, BronzeWriteMode::CreateOnly)
            && lock(&self.existing).contains_key(&request.key)
        {
            return Ok(BronzeWriteOutcome::AlreadyExists);
        }
        lock(&self.writes).push(request);
        Ok(BronzeWriteOutcome::Written)
    }

    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, BronzeStorageError> {
        Ok(lock(&self.existing)
            .get(key)
            .and_then(|object| object.sha256.clone()))
    }
}

#[derive(Default)]
struct RecordingUow {
    recorded: Mutex<Vec<BronzeObject>>,
    /// Rows already present in the store, keyed by `object_key`, used to simulate the
    /// "row exists" branches of the recovery tree (`find_bronze_object_by_object_key`).
    existing_rows: Mutex<BTreeMap<String, BronzeObject>>,
    fail_record: bool,
}

impl RecordingUow {
    const fn failing_record() -> Self {
        Self {
            recorded: Mutex::new(Vec::new()),
            existing_rows: Mutex::new(BTreeMap::new()),
            fail_record: true,
        }
    }

    /// Pre-seeds an already-recorded `bronze_object` row at its `object_key`.
    fn with_existing_row(row: BronzeObject) -> Self {
        let uow = Self::default();
        lock(&uow.existing_rows).insert(row.object_key.as_str().to_owned(), row);
        uow
    }

    fn recorded(&self) -> Vec<BronzeObject> {
        lock(&self.recorded).clone()
    }
}

#[async_trait::async_trait]
impl BronzeIngestUnitOfWork for RecordingUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        Ok(entry.clone())
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        Ok(run.clone())
    }

    async fn complete_ingestion_run(
        &self,
        _command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        Err(CollectionError::Infrastructure(
            "complete_ingestion_run not used in committer test".to_owned(),
        ))
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(lock(&self.existing_rows)
            .get(object_key)
            .and_then(|row| (row.source_catalog_id == source_catalog_id).then(|| row.clone())))
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        if self.fail_record {
            return Err(CollectionError::Infrastructure(
                "simulated bronze metadata failure".to_owned(),
            ));
        }
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

#[tokio::test]
async fn commit_writes_object_then_records_metadata() -> TestResult {
    let payload = br#"{"response":{"body":{"items":{"item":[{"mgmBldrgstPk":"x"}]}}}}"#.to_vec();
    let checksum = "a".repeat(64);
    let planned = sample_planned(payload.clone(), &checksum)?;
    let expected_key = planned.object_key.clone();
    let expected_record = planned.record.clone();

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit(&writer, &uow, planned)
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    // Storage received the object at the expected key with the expected bytes.
    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].key, expected_key);
    assert_eq!(recorded_writes[0].body, payload);
    assert_eq!(recorded_writes[0].content_type, "application/json");

    // record_bronze_object was called with the expected row.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        expected_record.object_key.as_str()
    );
    assert_eq!(recorded[0].checksum_sha256, expected_record.checksum_sha256);
    assert_eq!(recorded[0].dedupe_key, expected_record.dedupe_key);

    assert_eq!(outcome.object_key, expected_key);
    assert_eq!(outcome.checksum_sha256, checksum);
    Ok(())
}

#[tokio::test]
async fn commit_does_not_record_metadata_when_storage_write_fails() -> TestResult {
    let planned = sample_planned(b"{}".to_vec(), &"b".repeat(64))?;
    let writer = RecordingWriter::failing("simulated R2 outage");
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let error = committer
        .commit(&writer, &uow, planned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected storage failure"))?;

    assert!(matches!(error, BronzeCommitError::Storage { .. }));
    assert!(uow.recorded().is_empty());
    Ok(())
}

#[tokio::test]
async fn commit_reports_record_failure_after_object_is_written() -> TestResult {
    let planned = sample_planned(b"{}".to_vec(), &"c".repeat(64))?;
    let writer = RecordingWriter::default();
    let uow = RecordingUow::failing_record();
    let committer = BronzeCommitter::new();

    let error = committer
        .commit(&writer, &uow, planned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected record failure"))?;

    assert!(matches!(error, BronzeCommitError::Record { .. }));
    // The object was written even though metadata recording failed.
    assert_eq!(writer.writes().len(), 1);
    Ok(())
}

/// Success path: a `CreateOnly` write into an absent key writes the bytes (with the `sha256`
/// metadata stamped) and records exactly one row.
#[tokio::test]
async fn commit_create_only_write_stamps_sha256_and_records_once() -> TestResult {
    let checksum = "d".repeat(64);
    let planned = sample_planned(b"{}".to_vec(), &checksum)?;
    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    committer
        .commit(&writer, &uow, planned)
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].write_mode, BronzeWriteMode::CreateOnly);
    assert_eq!(
        recorded_writes[0].sha256.as_deref(),
        Some(checksum.as_str())
    );
    assert_eq!(uow.recorded().len(), 1);
    Ok(())
}

/// 412 + a recorded row with the SAME checksum => idempotent success: nothing is written and no
/// second row is recorded (we already collected + recorded this exact object on a prior run).
#[tokio::test]
async fn commit_already_exists_with_matching_row_is_idempotent_skip() -> TestResult {
    let checksum = "e".repeat(64);
    let planned = sample_planned(b"{}".to_vec(), &checksum)?;
    let key = planned.object_key.clone();
    let outcome_checksum = planned.checksum_sha256.clone();
    // The store already holds the object AND its bronze_object row, same checksum.
    let writer = RecordingWriter::with_existing(&key, Some(&checksum));
    let uow = RecordingUow::with_existing_row(planned.record.clone());
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit(&writer, &uow, planned)
        .await
        .map_err(|error| anyhow::anyhow!("idempotent commit failed: {error}"))?;

    assert_eq!(outcome.object_key, key);
    assert_eq!(outcome.checksum_sha256, outcome_checksum);
    // Nothing written, nothing newly recorded: pure idempotent skip.
    assert!(writer.writes().is_empty());
    assert!(uow.recorded().is_empty());
    Ok(())
}

/// 412 + a recorded row with a DIFFERENT checksum => fail loud with `ChecksumConflict`
/// (quarantine terminal): the key is occupied by different content; never silently overwrite.
#[tokio::test]
async fn commit_already_exists_with_conflicting_row_fails_loud() -> TestResult {
    let our_checksum = "1".repeat(64);
    let planned = sample_planned(b"{}".to_vec(), &our_checksum)?;
    let key = planned.object_key.clone();
    // The recorded row carries a DIFFERENT checksum than the payload this run computed.
    let mut conflicting_row = planned.record.clone();
    conflicting_row.checksum_sha256 = "2".repeat(64);
    let writer = RecordingWriter::with_existing(&key, Some(&"2".repeat(64)));
    let uow = RecordingUow::with_existing_row(conflicting_row);
    let committer = BronzeCommitter::new();

    let error = committer
        .commit(&writer, &uow, planned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected checksum conflict"))?;

    match error {
        BronzeCommitError::ChecksumConflict { key: conflicted } => {
            assert_eq!(conflicted, key);
        }
        other => return Err(anyhow::anyhow!("unexpected error: {other}")),
    }
    assert!(uow.recorded().is_empty());
    Ok(())
}

/// 412 + NO recorded row + the existing object's stored sha == ours => RECOVER: the object is
/// ours (R2 written previously, DB record failed), so record the missing row now and succeed.
#[tokio::test]
async fn commit_already_exists_without_row_recovers_when_object_sha_matches() -> TestResult {
    let checksum = "3".repeat(64);
    let planned = sample_planned(b"{}".to_vec(), &checksum)?;
    let key = planned.object_key.clone();
    let expected_record = planned.record.clone();
    // Object present with matching x-amz-meta-sha256, but NO bronze_object row recorded yet.
    let writer = RecordingWriter::with_existing(&key, Some(&checksum));
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit(&writer, &uow, planned)
        .await
        .map_err(|error| anyhow::anyhow!("recovery commit failed: {error}"))?;

    assert_eq!(outcome.object_key, key);
    assert_eq!(outcome.checksum_sha256, checksum);
    // No write (the object already existed), but the missing row is now recorded.
    assert!(writer.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].object_key.as_str(), key);
    assert_eq!(recorded[0].checksum_sha256, expected_record.checksum_sha256);
    assert_eq!(recorded[0].dedupe_key, expected_record.dedupe_key);
    Ok(())
}

/// 412 + NO recorded row + the existing object's stored sha != ours => fail loud
/// `ChecksumConflict` (the colliding object is not the one this run produced).
#[tokio::test]
async fn commit_already_exists_without_row_fails_loud_when_object_sha_differs() -> TestResult {
    let our_checksum = "4".repeat(64);
    let planned = sample_planned(b"{}".to_vec(), &our_checksum)?;
    let key = planned.object_key.clone();
    // Object present with a DIFFERENT stored checksum, and no row.
    let writer = RecordingWriter::with_existing(&key, Some(&"5".repeat(64)));
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let error = committer
        .commit(&writer, &uow, planned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected checksum conflict"))?;

    match error {
        BronzeCommitError::ChecksumConflict { key: conflicted } => {
            assert_eq!(conflicted, key);
        }
        other => return Err(anyhow::anyhow!("unexpected error: {other}")),
    }
    assert!(uow.recorded().is_empty());
    Ok(())
}

/// 412 + NO recorded row + the existing object has NO stored sha256 metadata => fail loud
/// `ChecksumConflict` (can't prove the object is ours; do not silently adopt it).
#[tokio::test]
async fn commit_already_exists_without_row_fails_loud_when_object_sha_missing() -> TestResult {
    let our_checksum = "6".repeat(64);
    let planned = sample_planned(b"{}".to_vec(), &our_checksum)?;
    let key = planned.object_key.clone();
    // Object present but with NO x-amz-meta-sha256, and no row.
    let writer = RecordingWriter::with_existing(&key, None);
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let error = committer
        .commit(&writer, &uow, planned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected checksum conflict"))?;

    assert!(matches!(error, BronzeCommitError::ChecksumConflict { .. }));
    assert!(uow.recorded().is_empty());
    Ok(())
}

fn sample_building_register_input() -> (BuildingRegisterPageRequest, Vec<u8>, serde_json::Value) {
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

/// The committer's owned key-compile produces the SAME object key + records a row whose
/// identity (key/checksum/dedupe/partition/size/logical count) matches the standalone
/// building-register Bronze plan — i.e. the relocation is behaviour-preserving.
#[tokio::test]
async fn commit_building_register_page_compiles_same_key_and_records_matching_row() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_building_register_input();

    // Reference: the object identity the standalone plan would have produced.
    let expected_plan = plan_building_register_bronze_page(BuildingRegisterBronzePagePlanInput {
        source_slug: "datagokr__building_register_main",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_building_register_page(
            &writer,
            &uow,
            BuildingRegisterCommitInput {
                source_slug: "datagokr__building_register_main",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload: raw_payload.clone(),
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    // Same object key + checksum as the standalone plan.
    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(outcome.plan.object_key, expected_plan.object_key);
    assert_eq!(outcome.plan.dedupe_key, expected_plan.dedupe_key);

    // Storage received the raw bytes at the compiled key.
    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].key, expected_plan.object_key.as_str());
    assert_eq!(recorded_writes[0].body, raw_payload);
    assert_eq!(recorded_writes[0].content_type, "application/json");

    // The recorded bronze_object row carries the plan-derived identity + the record context.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    let row = &recorded[0];
    assert_eq!(row.object_key.as_str(), expected_plan.object_key.as_str());
    assert_eq!(row.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(row.dedupe_key, expected_plan.dedupe_key);
    assert_eq!(
        row.source_partition_key.as_deref(),
        Some(expected_plan.source_partition_key.as_str())
    );
    assert_eq!(row.size_bytes, expected_plan.size_bytes);
    assert_eq!(
        row.logical_record_count,
        Some(expected_plan.logical_record_count)
    );
    assert_eq!(row.source_catalog_id, source_catalog_id);
    assert_eq!(row.ingestion_run_id, run_id);
    assert_eq!(row.id, outcome.bronze_object_id);
    assert_eq!(row.content_type, "application/json");
    assert_eq!(row.collected_at, collected_at);
    Ok(())
}

/// An invalid raw identity fails at the compile step before any storage write or record.
#[tokio::test]
async fn commit_building_register_page_rejects_invalid_identity_before_write() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let payload = serde_json::json!({ "response": { "body": { "items": { "item": [] } } } });
    let raw_payload = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let error = committer
        .commit_building_register_page(
            &writer,
            &uow,
            BuildingRegisterCommitInput {
                source_slug: "datagokr__building_register_main",
                ingest_date: collected_at.date_naive(),
                ingestion_run_id: run_id,
                request: BuildingRegisterPageRequest {
                    operation: "getBrTitleInfo".to_owned(),
                    // Non-5-digit region code: invalid per the plan's validation.
                    sigungu_cd: "ABC".to_owned(),
                    bjdong_cd: "10300".to_owned(),
                    page_no: 1,
                    num_of_rows: 100,
                },
                raw_payload,
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected compile failure"))?;

    assert!(matches!(error, BronzeCommitError::Plan { .. }));
    assert!(writer.writes().is_empty());
    assert!(uow.recorded().is_empty());
    Ok(())
}

fn sample_real_transaction_input() -> (RealTransactionPageRequest, Vec<u8>, serde_json::Value) {
    let payload = serde_json::json!({
        "response": { "body": { "items": { "item": [
            { "거래금액": "12,000", "건물면적": "84.5" }
        ] } } }
    });
    let raw_payload = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    // Real-transaction identity is LAWD_CD + DEAL_YMD (not sigungu/bjdong).
    let request = RealTransactionPageRequest {
        operation: "getRTMSDataSvcInduTrade".to_owned(),
        lawd_cd: "11680".to_owned(),
        deal_ymd: "202605".to_owned(),
        page_no: 1,
        num_of_rows: 1000,
    };
    (request, raw_payload, payload)
}

/// Success path: the committer's owned key-compile for real-transaction produces the SAME object
/// key + records a row whose identity (key/checksum/dedupe/partition/size/logical count) matches
/// the standalone real-transaction Bronze plan, and writes the raw bytes once with `CreateOnly` +
/// the `sha256` metadata stamped. Proves the `building_register` write+record core applies
/// unchanged to the real-transaction lane.
#[tokio::test]
async fn commit_real_transaction_page_compiles_same_key_and_records_matching_row() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_real_transaction_input();

    // Reference: the object identity the standalone plan would have produced.
    let expected_plan = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
        source_slug: "datagokr__real_transaction_industrial_trade",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_real_transaction_page(
            &writer,
            &uow,
            RealTransactionCommitInput {
                source_slug: "datagokr__real_transaction_industrial_trade",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload: raw_payload.clone(),
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    // Same object key + checksum as the standalone plan.
    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(outcome.plan.object_key, expected_plan.object_key);
    assert_eq!(outcome.plan.dedupe_key, expected_plan.dedupe_key);

    // Storage received the raw bytes at the compiled key, CreateOnly + sha256 stamped.
    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].key, expected_plan.object_key.as_str());
    assert_eq!(recorded_writes[0].body, raw_payload);
    assert_eq!(recorded_writes[0].write_mode, BronzeWriteMode::CreateOnly);
    assert_eq!(
        recorded_writes[0].sha256.as_deref(),
        Some(expected_plan.checksum_sha256.as_str())
    );

    // The recorded bronze_object row carries the plan-derived identity + the record context.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    let row = &recorded[0];
    assert_eq!(row.object_key.as_str(), expected_plan.object_key.as_str());
    assert_eq!(row.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(row.dedupe_key, expected_plan.dedupe_key);
    assert_eq!(
        row.source_partition_key.as_deref(),
        Some(expected_plan.source_partition_key.as_str())
    );
    assert_eq!(row.size_bytes, expected_plan.size_bytes);
    assert_eq!(
        row.logical_record_count,
        Some(expected_plan.logical_record_count)
    );
    assert_eq!(row.source_catalog_id, source_catalog_id);
    assert_eq!(row.ingestion_run_id, run_id);
    assert_eq!(row.id, outcome.bronze_object_id);
    Ok(())
}

/// Recovery path for real-transaction: the object is already in storage with a MATCHING
/// `x-amz-meta-sha256` (a prior run wrote it) but NO `bronze_object` row exists (that run's DB
/// record failed). The `CreateOnly` write hits already-exists; the SHARED recovery core records
/// the missing row and succeeds — proving the `building_register` recovery tree applies to the
/// real-transaction lane with no duplication.
#[tokio::test]
async fn commit_real_transaction_page_recovers_when_object_exists_but_row_missing() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_real_transaction_input();

    // Compile the same plan first so the fake storage can be pre-seeded with the EXACT object key
    // and checksum, simulating a prior run's R2 write whose DB record then failed.
    let expected_plan = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
        source_slug: "datagokr__real_transaction_industrial_trade",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    // Object already present with matching checksum, but NO bronze_object row recorded yet.
    let writer = RecordingWriter::with_existing(
        expected_plan.object_key.as_str(),
        Some(&expected_plan.checksum_sha256),
    );
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_real_transaction_page(
            &writer,
            &uow,
            RealTransactionCommitInput {
                source_slug: "datagokr__real_transaction_industrial_trade",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload,
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("recovery commit failed: {error}"))?;

    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);

    // No fresh write (the object already existed), but the missing row was recovered.
    assert!(writer.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        expected_plan.object_key.as_str()
    );
    assert_eq!(recorded[0].checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(recorded[0].id, outcome.bronze_object_id);
    Ok(())
}

fn sample_vworld_ned_input() -> (VWorldNedPageRequest, Vec<u8>, serde_json::Value) {
    let payload = serde_json::json!({
        "landCharVOList": { "landCharVOList": [
            { "pnu": "9999900101100010000", "lndcgrCodeNm": "대" }
        ] }
    });
    let raw_payload = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    // The generic NED lane CARRIES its logical pointer + candidate-key suffixes on the request, so
    // the committer's `&self` key-compile has everything it needs (no separate planner input).
    let request = VWorldNedPageRequest {
        operation: "getLandCharacteristic".to_owned(),
        partition_name: "pnu".to_owned(),
        partition_value: "9999900101100010000".to_owned(),
        query_params: BTreeMap::from([("pnu".to_owned(), "9999900101100010000".to_owned())]),
        page_no: 1,
        num_of_rows: 1000,
        logical_items_pointer: "/landCharVOList/landCharVOList".to_owned(),
        candidate_key_field_suffixes: vec!["pnu".to_owned()],
    };
    (request, raw_payload, payload)
}

/// Success path: the committer's owned key-compile for the generic V-World NED lane produces the
/// SAME object key + records a row whose identity (key/checksum/dedupe/partition/size/logical count)
/// matches the standalone NED Bronze plan, and writes the raw bytes once with `CreateOnly` + the
/// `sha256` metadata stamped. Proves the shared write+record core applies unchanged to the NED lane
/// (whose per-operation logical pointer + candidate suffixes are carried on the request).
#[tokio::test]
async fn commit_vworld_ned_page_compiles_same_key_and_records_matching_row() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_vworld_ned_input();

    // Reference: the object identity the standalone plan would have produced.
    let expected_plan = plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
        source_slug: "vworldkr__land_characteristic",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_vworld_ned_page(
            &writer,
            &uow,
            VWorldNedCommitInput {
                source_slug: "vworldkr__land_characteristic",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload: raw_payload.clone(),
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    // Same object key + checksum as the standalone plan.
    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(outcome.plan.object_key, expected_plan.object_key);
    assert_eq!(outcome.plan.dedupe_key, expected_plan.dedupe_key);

    // Storage received the raw bytes at the compiled key, CreateOnly + sha256 stamped.
    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].key, expected_plan.object_key.as_str());
    assert_eq!(recorded_writes[0].body, raw_payload);
    assert_eq!(recorded_writes[0].write_mode, BronzeWriteMode::CreateOnly);
    assert_eq!(
        recorded_writes[0].sha256.as_deref(),
        Some(expected_plan.checksum_sha256.as_str())
    );

    // The recorded bronze_object row carries the plan-derived identity + the record context.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    let row = &recorded[0];
    assert_eq!(row.object_key.as_str(), expected_plan.object_key.as_str());
    assert_eq!(row.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(row.dedupe_key, expected_plan.dedupe_key);
    assert_eq!(
        row.source_partition_key.as_deref(),
        Some(expected_plan.source_partition_key.as_str())
    );
    assert_eq!(row.size_bytes, expected_plan.size_bytes);
    assert_eq!(
        row.logical_record_count,
        Some(expected_plan.logical_record_count)
    );
    assert_eq!(row.source_catalog_id, source_catalog_id);
    assert_eq!(row.ingestion_run_id, run_id);
    assert_eq!(row.id, outcome.bronze_object_id);
    Ok(())
}

/// Recovery path for the generic V-World NED lane: the object is already in storage with a MATCHING
/// `x-amz-meta-sha256` (a prior run wrote it) but NO `bronze_object` row exists (that run's DB record
/// failed). The `CreateOnly` write hits already-exists; the SHARED recovery core records the missing
/// row and succeeds — proving the `building_register` / `real_transaction` recovery tree applies to
/// the NED lane with no duplication.
#[tokio::test]
async fn commit_vworld_ned_page_recovers_when_object_exists_but_row_missing() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_vworld_ned_input();

    let expected_plan = plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
        source_slug: "vworldkr__land_characteristic",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    // Object already present with matching checksum, but NO bronze_object row recorded yet.
    let writer = RecordingWriter::with_existing(
        expected_plan.object_key.as_str(),
        Some(&expected_plan.checksum_sha256),
    );
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_vworld_ned_page(
            &writer,
            &uow,
            VWorldNedCommitInput {
                source_slug: "vworldkr__land_characteristic",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload,
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("recovery commit failed: {error}"))?;

    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);

    // No fresh write (the object already existed), but the missing row was recovered.
    assert!(writer.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        expected_plan.object_key.as_str()
    );
    assert_eq!(recorded[0].checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(recorded[0].id, outcome.bronze_object_id);
    Ok(())
}

fn sample_vworld_land_register_input() -> (VWorldLandRegisterPageRequest, Vec<u8>, serde_json::Value)
{
    let payload = serde_json::json!({
        "ladfrlVOList": { "ladfrlVOList": [
            { "pnu": "9999900601100010000", "ldCodeNm": "SYNTHETIC-DISTRICT" }
        ] }
    });
    let raw_payload = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    // Land-register identity is the PNU (10-digit prefix or 19-digit parcel).
    let request = VWorldLandRegisterPageRequest {
        operation: "ladfrlList".to_owned(),
        pnu: "9999900601100010000".to_owned(),
        page_no: 1,
        num_of_rows: 1000,
    };
    (request, raw_payload, payload)
}

/// Success path: the committer's owned key-compile for the V-World land-register lane produces the
/// SAME object key + records a row whose identity matches the standalone land-register Bronze plan,
/// and writes the raw bytes once with `CreateOnly` + the `sha256` metadata stamped.
#[tokio::test]
async fn commit_vworld_land_register_page_compiles_same_key_and_records_matching_row() -> TestResult
{
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_vworld_land_register_input();

    // Reference: the object identity the standalone plan would have produced.
    let expected_plan =
        plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
            source_slug: "vworldkr__land_register",
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        })
        .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_vworld_land_register_page(
            &writer,
            &uow,
            VWorldLandRegisterCommitInput {
                source_slug: "vworldkr__land_register",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload: raw_payload.clone(),
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    // Same object key + checksum as the standalone plan.
    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(outcome.plan.object_key, expected_plan.object_key);
    assert_eq!(outcome.plan.dedupe_key, expected_plan.dedupe_key);

    // Storage received the raw bytes at the compiled key, CreateOnly + sha256 stamped.
    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].key, expected_plan.object_key.as_str());
    assert_eq!(recorded_writes[0].body, raw_payload);
    assert_eq!(recorded_writes[0].write_mode, BronzeWriteMode::CreateOnly);
    assert_eq!(
        recorded_writes[0].sha256.as_deref(),
        Some(expected_plan.checksum_sha256.as_str())
    );

    // The recorded bronze_object row carries the plan-derived identity + the record context.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    let row = &recorded[0];
    assert_eq!(row.object_key.as_str(), expected_plan.object_key.as_str());
    assert_eq!(row.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(row.dedupe_key, expected_plan.dedupe_key);
    assert_eq!(
        row.source_partition_key.as_deref(),
        Some(expected_plan.source_partition_key.as_str())
    );
    assert_eq!(row.size_bytes, expected_plan.size_bytes);
    assert_eq!(
        row.logical_record_count,
        Some(expected_plan.logical_record_count)
    );
    assert_eq!(row.source_catalog_id, source_catalog_id);
    assert_eq!(row.ingestion_run_id, run_id);
    assert_eq!(row.id, outcome.bronze_object_id);
    Ok(())
}

/// Recovery path for the V-World land-register lane: the object is already in storage with a MATCHING
/// `x-amz-meta-sha256` but NO `bronze_object` row exists. The `CreateOnly` write hits already-exists;
/// the SHARED recovery core records the missing row and succeeds — same recovery tree, zero
/// duplication.
#[tokio::test]
async fn commit_vworld_land_register_page_recovers_when_object_exists_but_row_missing() -> TestResult
{
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_vworld_land_register_input();

    let expected_plan =
        plan_vworld_land_register_bronze_page(VWorldLandRegisterBronzePagePlanInput {
            source_slug: "vworldkr__land_register",
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        })
        .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    // Object already present with matching checksum, but NO bronze_object row recorded yet.
    let writer = RecordingWriter::with_existing(
        expected_plan.object_key.as_str(),
        Some(&expected_plan.checksum_sha256),
    );
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_vworld_land_register_page(
            &writer,
            &uow,
            VWorldLandRegisterCommitInput {
                source_slug: "vworldkr__land_register",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload,
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("recovery commit failed: {error}"))?;

    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);

    // No fresh write (the object already existed), but the missing row was recovered.
    assert!(writer.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        expected_plan.object_key.as_str()
    );
    assert_eq!(recorded[0].checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(recorded[0].id, outcome.bronze_object_id);
    Ok(())
}

fn sample_vworld_cadastral_input() -> (VWorldCadastralPageRequest, Vec<u8>, serde_json::Value) {
    let pnu = "9999900801105800001";
    let payload = serde_json::json!({
        "response": {
            "status": "OK",
            "record": { "total": "1", "current": "1" },
            "result": { "featureCollection": { "features": [
                {
                    "type": "Feature",
                    "properties": { "pnu": pnu, "jibun": "580-1" },
                    "geometry": { "type": "MultiPolygon", "coordinates": [] }
                }
            ] } }
        }
    });
    let raw_payload = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    // Single clean `pnu:=:<value>` filter → the redesigned object key uses a human-readable
    // `pnu=<value>` scope (no operation=/filter_kind=/filter_sha256=/size= segments).
    let request = VWorldCadastralPageRequest {
        dataset: "LP_PA_CBND_BUBUN".to_owned(),
        attr_filter: Some(format!("pnu:=:{pnu}")),
        columns: vec!["pnu".to_owned(), "ag_geom".to_owned()],
        geometry: true,
        attribute: true,
        crs: Some("EPSG:4326".to_owned()),
        page: 1,
        size: 1000,
    };
    (request, raw_payload, payload)
}

/// Success path: the committer's owned key-compile for the V-World cadastral lane produces the SAME
/// (redesigned) object key + records a row whose identity matches the standalone cadastral Bronze
/// plan, and writes the raw bytes once with `CreateOnly` + the `sha256` metadata stamped. The
/// committed key is the NEW human-readable scope key (no `operation`/`filter_kind`/`filter_sha256`/
/// `size` segments) — proving routing cadastral through the committer gives it `CreateOnly` +
/// recovery while preserving the redesigned key.
#[tokio::test]
async fn commit_vworld_cadastral_page_compiles_same_key_and_records_matching_row() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_vworld_cadastral_input();

    // Reference: the object identity the standalone plan would have produced.
    let expected_plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    // The redesigned key: human-readable pnu scope, no operation/filter_kind/filter_sha256/size.
    assert_eq!(
        expected_plan.object_key.as_str(),
        "bronze/source=vworldkr__cadastral/dataset=LP_PA_CBND_BUBUN/pnu=9999900801105800001/page-000001.json"
    );

    let writer = RecordingWriter::default();
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_vworld_cadastral_page(
            &writer,
            &uow,
            VWorldCadastralCommitInput {
                source_slug: "vworldkr__cadastral",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload: raw_payload.clone(),
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("commit failed: {error}"))?;

    // Same (redesigned) object key + checksum as the standalone plan.
    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(outcome.plan.object_key, expected_plan.object_key);
    assert_eq!(outcome.plan.dedupe_key, expected_plan.dedupe_key);

    // Storage received the raw bytes at the compiled key, CreateOnly + sha256 stamped.
    let recorded_writes = writer.writes();
    assert_eq!(recorded_writes.len(), 1);
    assert_eq!(recorded_writes[0].key, expected_plan.object_key.as_str());
    assert_eq!(recorded_writes[0].body, raw_payload);
    assert_eq!(recorded_writes[0].write_mode, BronzeWriteMode::CreateOnly);
    assert_eq!(
        recorded_writes[0].sha256.as_deref(),
        Some(expected_plan.checksum_sha256.as_str())
    );

    // The recorded bronze_object row carries the plan-derived identity + the record context. The
    // source_partition_key (lineage) still carries the provider operation `GetFeature`.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    let row = &recorded[0];
    assert_eq!(row.object_key.as_str(), expected_plan.object_key.as_str());
    assert_eq!(row.checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(row.dedupe_key, expected_plan.dedupe_key);
    assert_eq!(
        row.source_partition_key.as_deref(),
        Some(expected_plan.source_partition_key.as_str())
    );
    assert!(
        expected_plan
            .source_partition_key
            .starts_with("operation=GetFeature/"),
        "lineage must keep the provider operation: {}",
        expected_plan.source_partition_key
    );
    assert_eq!(row.size_bytes, expected_plan.size_bytes);
    assert_eq!(
        row.logical_record_count,
        Some(expected_plan.logical_record_count)
    );
    assert_eq!(row.source_catalog_id, source_catalog_id);
    assert_eq!(row.ingestion_run_id, run_id);
    assert_eq!(row.id, outcome.bronze_object_id);
    Ok(())
}

/// Recovery path for the V-World cadastral lane: the object is already in storage with a MATCHING
/// `x-amz-meta-sha256` (a prior run wrote it) but NO `bronze_object` row exists. The `CreateOnly`
/// write hits already-exists; the SHARED recovery core records the missing row and succeeds — same
/// recovery tree, zero duplication, now applied to the redesigned cadastral key.
#[tokio::test]
async fn commit_vworld_cadastral_page_recovers_when_object_exists_but_row_missing() -> TestResult {
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let collected_at = Utc::now();
    let ingest_date = collected_at.date_naive();
    let (request, raw_payload, payload) = sample_vworld_cadastral_input();

    let expected_plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
        source_slug: "vworldkr__cadastral",
        ingest_date,
        ingestion_run_id: run_id,
        request: request.clone(),
        raw_payload: raw_payload.clone(),
        payload: payload.clone(),
    })
    .map_err(|error| anyhow::anyhow!("reference plan failed: {error}"))?;

    // Object already present with matching checksum, but NO bronze_object row recorded yet.
    let writer = RecordingWriter::with_existing(
        expected_plan.object_key.as_str(),
        Some(&expected_plan.checksum_sha256),
    );
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_vworld_cadastral_page(
            &writer,
            &uow,
            VWorldCadastralCommitInput {
                source_slug: "vworldkr__cadastral",
                ingest_date,
                ingestion_run_id: run_id,
                request,
                raw_payload,
                payload,
                source_catalog_id,
                collected_at,
                content_type: "application/json".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("recovery commit failed: {error}"))?;

    assert_eq!(outcome.object_key, expected_plan.object_key.as_str());
    assert_eq!(outcome.checksum_sha256, expected_plan.checksum_sha256);

    // No fresh write (the object already existed), but the missing row was recovered.
    assert!(writer.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        expected_plan.object_key.as_str()
    );
    assert_eq!(recorded[0].checksum_sha256, expected_plan.checksum_sha256);
    assert_eq!(recorded[0].id, outcome.bronze_object_id);
    Ok(())
}
