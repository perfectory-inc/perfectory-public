//! Unit tests for the STREAMING Bronze commit path (`super::bronze_committer`).
//!
//! Kept in a separate `#[path]` submodule from the in-memory page tests so each file stays under
//! the AGENTS.md 1500-line block limit. These tests are the behaviour proof for
//! [`BronzeCommitter::commit_streaming_bulk`](super::BronzeCommitter::commit_streaming_bulk) and its
//! 412 GET-rehash recovery tree (success + idempotent skip + recover-via-rehash + fail-loud), which
//! differs from the in-memory page recovery: the checksum is computed in-flight (never stamped as
//! `x-amz-meta-sha256`), so a 412 with no row recovers by reading the existing object back and
//! rehashing its bytes rather than HEAD-reading stored metadata.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{NaiveDate, Utc};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, SchemaProfile, SnapshotBasis, SnapshotGranularity,
    SourceCatalogEntry,
};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKey;
use uuid::Uuid;

use super::{
    BronzeCommitError, BronzeCommitter, BronzeStorageError, BronzeStreamingRawObjectWriter,
    BronzeStreamingWriteOutcome, BronzeStreamingWriteRequest, PlannedStreamingBronzeObject,
    StreamedObjectRehash, StreamingBronzeRecord,
};
use crate::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};

type TestResult = anyhow::Result<()>;

/// Locks a test mutex, recovering from poison so a panicking test cannot cascade into spurious
/// failures elsewhere (and so the denied `expect_used`/`unwrap_used` lints stay satisfied).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Minimal Bronze unit-of-work for the streaming tests: it only needs to record rows and look one
/// up by `(source_catalog_id, object_key)` for the recovery tree; the other trait methods are
/// unused here and return a clear infrastructure error if ever called.
#[derive(Default)]
struct RecordingUow {
    recorded: Mutex<Vec<BronzeObject>>,
    existing_rows: Mutex<BTreeMap<String, BronzeObject>>,
}

impl RecordingUow {
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
            "complete_ingestion_run not used in streaming committer test".to_owned(),
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

/// A streamed object the fake storage already holds, used to simulate the streaming
/// `CreateOnly`-collision branches. `rehash` is what a GET-rehash read-back would return (the
/// streaming recovery cannot use `x-amz-meta-sha256` — a streaming write never stamps it).
#[derive(Clone, Debug)]
struct ExistingStreamedObject {
    /// Checksum + size a read-back-and-rehash would compute, or `None` to simulate an object that
    /// cannot be read/rehashed at recovery time (absent at read / read failure).
    rehash: Option<StreamedObjectRehash>,
}

/// What a streaming write records when it actually streams (key absent).
#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamingWriteRecord {
    request: BronzeStreamingWriteRequest,
    checksum_sha256: String,
    size_bytes: u64,
}

/// Fake streaming writer for the streaming seam.
///
/// - key ABSENT: `write_streaming_object` records the write and returns `Written` with the
///   configured in-flight checksum + size (the committer never passes bytes, so the fake supplies
///   what the real port would compute while streaming);
/// - key EXISTS: `write_streaming_object` returns `AlreadyExists` without streaming, and
///   `read_object_sha256_by_rehash` returns the seeded GET-rehash (or `None`).
struct RecordingStreamingWriter {
    writes: Mutex<Vec<StreamingWriteRecord>>,
    existing: Mutex<BTreeMap<String, ExistingStreamedObject>>,
    /// Checksum + size the streaming write reports for a fresh (key-absent) write.
    streamed_checksum_sha256: String,
    streamed_size_bytes: u64,
}

impl RecordingStreamingWriter {
    /// A writer whose fresh streaming write reports `checksum`/`size` (key absent everywhere).
    fn fresh(checksum: &str, size_bytes: u64) -> Self {
        Self {
            writes: Mutex::new(Vec::new()),
            existing: Mutex::new(BTreeMap::new()),
            streamed_checksum_sha256: checksum.to_owned(),
            streamed_size_bytes: size_bytes,
        }
    }

    /// Pre-seeds an existing streamed object at `key` whose GET-rehash returns `rehash`
    /// (`None` => the object cannot be read back / rehashed at recovery time).
    fn with_existing(key: &str, rehash: Option<StreamedObjectRehash>) -> Self {
        let writer = Self::fresh(&"0".repeat(64), 0);
        lock(&writer.existing).insert(key.to_owned(), ExistingStreamedObject { rehash });
        writer
    }

    fn writes(&self) -> Vec<StreamingWriteRecord> {
        lock(&self.writes).clone()
    }
}

#[async_trait::async_trait]
impl BronzeStreamingRawObjectWriter for RecordingStreamingWriter {
    async fn write_streaming_object(
        &self,
        request: BronzeStreamingWriteRequest,
    ) -> Result<BronzeStreamingWriteOutcome, BronzeStorageError> {
        // CreateOnly collision: a pre-seeded key reports already-exists without streaming.
        if lock(&self.existing).contains_key(&request.key) {
            return Ok(BronzeStreamingWriteOutcome::AlreadyExists);
        }
        let checksum_sha256 = self.streamed_checksum_sha256.clone();
        let size_bytes = self.streamed_size_bytes;
        lock(&self.writes).push(StreamingWriteRecord {
            request,
            checksum_sha256: checksum_sha256.clone(),
            size_bytes,
        });
        Ok(BronzeStreamingWriteOutcome::Written {
            checksum_sha256,
            size_bytes,
        })
    }

    async fn read_object_sha256_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamedObjectRehash>, BronzeStorageError> {
        Ok(lock(&self.existing)
            .get(key)
            .and_then(|object| object.rehash.clone()))
    }
}

fn sample_streaming_record(
    object_key: &ObjectKey,
    source_catalog_id: SourceCatalogId,
) -> StreamingBronzeRecord {
    StreamingBronzeRecord {
        object_key: object_key.clone(),
        content_type: "application/zip".to_owned(),
        source_catalog_id,
        ingestion_run_id: IngestionRunId::new(Uuid::new_v4()),
        source_partition_key: "operation=building_register_main/provider_file_id=OPN1".to_owned(),
        source_identity_key: "provider_file_id=OPN1".to_owned(),
        dedupe_key_prefix: "hubgokr__building_register_main:provider_file_id=OPN1".to_owned(),
        request_params: serde_json::json!({ "provider_file_id": "OPN1", "raw_preserved": true }),
        snapshot_period: Some("2026-05".to_owned()),
        snapshot_date: NaiveDate::from_ymd_opt(2026, 5, 1)
            .unwrap_or_else(|| unreachable!("valid test date")),
        snapshot_granularity: SnapshotGranularity::Month,
        snapshot_basis: SnapshotBasis::ProviderFilePeriod,
        provider_file_id: Some("OPN1".to_owned()),
        provider_file_name: Some("OPN1.zip".to_owned()),
        provider_updated_at: None,
        collected_at: Utc::now(),
    }
}

fn streaming_object_key() -> anyhow::Result<ObjectKey> {
    Ok(ObjectKey::parse(
        "bronze/source=hubgokr__building_register_main/OPN1.zip",
    )?)
}

/// Streaming success path: a `CreateOnly` stream into an absent key streams the bytes (the port
/// computes the checksum + size in-flight) and records exactly one row carrying that streamed
/// checksum + size and the sha-completed dedupe key.
#[tokio::test]
async fn commit_streaming_bulk_streams_then_records_with_inflight_checksum() -> TestResult {
    let object_key = streaming_object_key()?;
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let record = sample_streaming_record(&object_key, source_catalog_id);
    let checksum = "a".repeat(64);
    let size_bytes = 4096_u64;

    let writer = RecordingStreamingWriter::fresh(&checksum, size_bytes);
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_streaming_bulk(
            &writer,
            &uow,
            PlannedStreamingBronzeObject {
                cache_control: "no-store, max-age=0".to_owned(),
                expected_size_bytes: size_bytes,
                record: record.clone(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("streaming commit failed: {error}"))?;

    // The stream targeted the planned key with the declared length.
    let streamed = writer.writes();
    assert_eq!(streamed.len(), 1);
    assert_eq!(streamed[0].request.key, object_key.as_str());
    assert_eq!(streamed[0].request.content_type, "application/zip");
    assert_eq!(streamed[0].request.expected_size_bytes, size_bytes);

    // Exactly one row recorded, carrying the in-flight checksum + size and the completed dedupe key.
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].object_key.as_str(), object_key.as_str());
    assert_eq!(recorded[0].checksum_sha256, checksum);
    assert_eq!(recorded[0].size_bytes, size_bytes);
    assert_eq!(recorded[0].content_type, "application/zip");
    assert_eq!(
        recorded[0].dedupe_key,
        record.dedupe_key_for_checksum(&checksum)
    );
    assert_eq!(
        recorded[0].source_partition_key.as_deref(),
        Some(record.source_partition_key.as_str())
    );
    assert_eq!(recorded[0].source_catalog_id, source_catalog_id);

    assert_eq!(outcome.object_key, object_key.as_str());
    assert_eq!(outcome.checksum_sha256, checksum);
    Ok(())
}

/// Streaming 412 + a `bronze_object` row already exists => idempotent skip: nothing streamed,
/// nothing newly recorded (the content-stable `object_key` proves the existing object is ours), and
/// the outcome echoes the existing row's recorded checksum.
#[tokio::test]
async fn commit_streaming_bulk_already_exists_with_row_is_idempotent_skip() -> TestResult {
    let object_key = streaming_object_key()?;
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let record = sample_streaming_record(&object_key, source_catalog_id);
    let recorded_checksum = "b".repeat(64);

    // Storage already holds the object; the DB already has its row (recorded on a prior run).
    let writer = RecordingStreamingWriter::with_existing(object_key.as_str(), None);
    let existing_row =
        build_existing_streaming_row(&object_key, source_catalog_id, &recorded_checksum, 4096);
    let uow = RecordingUow::with_existing_row(existing_row);
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_streaming_bulk(
            &writer,
            &uow,
            PlannedStreamingBronzeObject {
                cache_control: "no-store, max-age=0".to_owned(),
                expected_size_bytes: 4096,
                record,
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("idempotent streaming commit failed: {error}"))?;

    assert_eq!(outcome.object_key, object_key.as_str());
    // Echoes the EXISTING row's checksum (no fresh stream computed one).
    assert_eq!(outcome.checksum_sha256, recorded_checksum);
    // Pure idempotent skip: nothing streamed, nothing newly recorded.
    assert!(writer.writes().is_empty());
    assert!(uow.recorded().is_empty());
    Ok(())
}

/// Streaming 412 + NO row + the existing object IS readable (GET-rehash succeeds) => RECOVER: the
/// missing row is recorded from the rehashed checksum + size, without re-streaming the provider.
#[tokio::test]
async fn commit_streaming_bulk_already_exists_without_row_recovers_via_rehash() -> TestResult {
    let object_key = streaming_object_key()?;
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let record = sample_streaming_record(&object_key, source_catalog_id);
    let rehashed_checksum = "c".repeat(64);
    let rehashed_size = 8192_u64;

    // Object present (a prior run streamed it) with a readable GET-rehash, but NO bronze_object row.
    let writer = RecordingStreamingWriter::with_existing(
        object_key.as_str(),
        Some(StreamedObjectRehash {
            checksum_sha256: rehashed_checksum.clone(),
            size_bytes: rehashed_size,
        }),
    );
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let outcome = committer
        .commit_streaming_bulk(
            &writer,
            &uow,
            PlannedStreamingBronzeObject {
                cache_control: "no-store, max-age=0".to_owned(),
                expected_size_bytes: rehashed_size,
                record: record.clone(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("streaming recovery commit failed: {error}"))?;

    assert_eq!(outcome.object_key, object_key.as_str());
    // The recovered checksum is the GET-rehash of the existing object (NOT x-amz-meta-sha256).
    assert_eq!(outcome.checksum_sha256, rehashed_checksum);
    // No re-stream (the provider is not re-downloaded), but the missing row is recovered from the
    // rehash with the rehashed checksum + size + completed dedupe key.
    assert!(writer.writes().is_empty());
    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].object_key.as_str(), object_key.as_str());
    assert_eq!(recorded[0].checksum_sha256, rehashed_checksum);
    assert_eq!(recorded[0].size_bytes, rehashed_size);
    assert_eq!(
        recorded[0].dedupe_key,
        record.dedupe_key_for_checksum(&rehashed_checksum)
    );
    Ok(())
}

/// Streaming 412 + NO row + the existing object CANNOT be read back / rehashed => fail loud
/// `ChecksumConflict` (never silently treat 412 as a no-op when we cannot prove the object is ours).
#[tokio::test]
async fn commit_streaming_bulk_already_exists_without_row_fails_loud_when_unreadable() -> TestResult
{
    let object_key = streaming_object_key()?;
    let source_catalog_id = SourceCatalogId::new(Uuid::new_v4());
    let record = sample_streaming_record(&object_key, source_catalog_id);

    // Object present but its GET-rehash read-back fails / the object is gone => None.
    let writer = RecordingStreamingWriter::with_existing(object_key.as_str(), None);
    let uow = RecordingUow::default();
    let committer = BronzeCommitter::new();

    let error = committer
        .commit_streaming_bulk(
            &writer,
            &uow,
            PlannedStreamingBronzeObject {
                cache_control: "no-store, max-age=0".to_owned(),
                expected_size_bytes: 4096,
                record,
            },
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected checksum conflict"))?;

    match error {
        BronzeCommitError::ChecksumConflict { key } => assert_eq!(key, object_key.as_str()),
        other => return Err(anyhow::anyhow!("unexpected error: {other}")),
    }
    // 412 was NOT treated as a no-op: no row was recorded and we failed loud.
    assert!(uow.recorded().is_empty());
    Ok(())
}

/// Builds an already-recorded `bronze_object` row at `object_key` for the streaming idempotent-skip
/// test (the row a prior run would have recorded).
fn build_existing_streaming_row(
    object_key: &ObjectKey,
    source_catalog_id: SourceCatalogId,
    checksum_sha256: &str,
    size_bytes: u64,
) -> BronzeObject {
    let now = Utc::now();
    BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id,
        ingestion_run_id: IngestionRunId::new(Uuid::new_v4()),
        source_record_id: None,
        source_partition_key: Some(
            "operation=building_register_main/provider_file_id=OPN1".to_owned(),
        ),
        source_identity_key: "provider_file_id=OPN1".to_owned(),
        dedupe_key: format!(
            "hubgokr__building_register_main:provider_file_id=OPN1:sha256={checksum_sha256}"
        ),
        request_params: serde_json::json!({ "provider_file_id": "OPN1" }),
        object_key: object_key.clone(),
        checksum_sha256: checksum_sha256.to_owned(),
        content_type: "application/zip".to_owned(),
        size_bytes,
        logical_record_count: None,
        collected_at: now,
        snapshot_period: Some("2026-05".to_owned()),
        snapshot_date: NaiveDate::from_ymd_opt(2026, 5, 1)
            .unwrap_or_else(|| unreachable!("valid test date")),
        snapshot_granularity: SnapshotGranularity::Month,
        snapshot_basis: SnapshotBasis::ProviderFilePeriod,
        provider_file_id: Some("OPN1".to_owned()),
        provider_file_name: Some("OPN1.zip".to_owned()),
        provider_updated_at: None,
        effective_date: None,
        created_at: now,
    }
}
