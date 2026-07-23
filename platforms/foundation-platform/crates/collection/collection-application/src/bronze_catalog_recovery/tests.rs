use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard, PoisonError},
};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use collection_domain::{
    BronzeObject, CollectionError, SnapshotBasis, SnapshotGranularity, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_shared_kernel::ids::SourceCatalogId;
use foundation_shared_kernel::ObjectKey;
use serde_json::json;
use uuid::Uuid;

use super::{
    ApplyBronzeCatalogRecoveryCommand, BronzeCatalogRecoveryCandidate,
    BronzeCatalogRecoveryCatalogWriter, BronzeCatalogRecoveryError, BronzeCatalogRecoveryInput,
    BronzeCatalogRecoveryMode, BronzeCatalogRecoveryObjectReader, BronzeCatalogRecoveryService,
    ExistingBronzeObject, RecoveryEvidenceKind,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;
type TestValueResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn dry_run_verifies_bytes_without_mutating_catalog() -> TestResult {
    let candidate = candidate()?;
    let reader = FakeReader::with_object(
        candidate.object_key.as_str(),
        ExistingBronzeObject {
            checksum_sha256: checksum('a'),
            size_bytes: 12,
            observed_r2_etag: candidate.observed_r2_etag.clone(),
            observed_r2_last_modified: candidate.observed_r2_last_modified,
        },
    );
    let writer = FakeCatalogWriter::default();

    let report = BronzeCatalogRecoveryService::new()
        .execute(
            &reader,
            None,
            input(BronzeCatalogRecoveryMode::DryRun, vec![candidate])?,
        )
        .await?;

    assert_eq!(report.validated_object_count, 1);
    assert_eq!(report.applied_object_count, 0);
    assert_eq!(report.total_size_bytes, 12);
    assert_eq!(report.excluded_unresolved_object_count, 0);
    assert_eq!(report.ingestion_run_id, None);
    assert_eq!(writer.mutation_count(), 0);
    Ok(())
}

#[tokio::test]
async fn recovery_reads_the_source_through_one_batch_contract() -> TestResult {
    let candidate = candidate()?;
    let reader = BatchOnlyReader::new(ExistingBronzeObject {
        checksum_sha256: checksum('a'),
        size_bytes: 12,
        observed_r2_etag: candidate.observed_r2_etag.clone(),
        observed_r2_last_modified: candidate.observed_r2_last_modified,
    });
    let writer = FakeCatalogWriter::default();

    let report = BronzeCatalogRecoveryService::new()
        .execute(
            &reader,
            Some(&writer),
            input(BronzeCatalogRecoveryMode::Apply, vec![candidate])?,
        )
        .await?;

    assert_eq!(reader.batch_read_count(), 1);
    assert_eq!(report.applied_object_count, 1);
    assert_eq!(writer.mutation_count(), 1);
    Ok(())
}

#[tokio::test]
async fn apply_records_recovery_provenance_and_no_false_r2_write_count() -> TestResult {
    let candidate = candidate()?;
    let reader = FakeReader::with_object(
        candidate.object_key.as_str(),
        ExistingBronzeObject {
            checksum_sha256: checksum('a'),
            size_bytes: 12,
            observed_r2_etag: candidate.observed_r2_etag.clone(),
            observed_r2_last_modified: candidate.observed_r2_last_modified,
        },
    );
    let writer = FakeCatalogWriter::default();

    let mut recovery_input = input(BronzeCatalogRecoveryMode::Apply, vec![candidate])?;
    recovery_input.excluded_unresolved_object_count = 7;
    let report = BronzeCatalogRecoveryService::new()
        .execute(&reader, Some(&writer), recovery_input)
        .await?;

    assert_eq!(report.validated_object_count, 1);
    assert_eq!(report.applied_object_count, 1);
    assert_eq!(report.excluded_unresolved_object_count, 7);
    assert!(report.ingestion_run_id.is_some());

    let runs = writer.created_runs();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger.wire_name(), "replay");
    assert_eq!(
        runs[0].request_params["catalog_recovery"]["evidence_manifest_sha256"],
        checksum('f')
    );
    assert_eq!(
        runs[0].request_params["catalog_recovery"]["excluded_unresolved_object_count"],
        7
    );

    let objects = writer.recorded_objects();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].checksum_sha256, checksum('a'));
    assert_eq!(objects[0].size_bytes, 12);
    assert_eq!(
        objects[0].request_params["catalog_recovery"]["kind"],
        "evidence_rehydration"
    );
    assert_eq!(
        objects[0].request_params["catalog_recovery"]["collected_at_basis"],
        "r2_last_modified"
    );
    assert_eq!(
        objects[0].request_params["catalog_recovery"]["observed_r2_etag"],
        "inventory-etag"
    );
    assert_eq!(
        objects[0].request_params["catalog_recovery"]["observed_r2_last_modified"],
        "2026-06-30T12:00:00Z"
    );

    let completions = writer.completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status.wire_name(), "succeeded");
    assert_eq!(completions[0].logical_records_seen, 1);
    assert_eq!(completions[0].objects_written, 0);
    Ok(())
}

#[tokio::test]
async fn mismatch_or_missing_object_fails_before_any_catalog_mutation() -> TestResult {
    let missing_candidate = candidate()?;
    let checksum_mismatch_candidate = candidate()?;
    let size_mismatch_candidate = candidate()?;
    let cases = [
        FakeReader::empty(),
        FakeReader::with_object(
            checksum_mismatch_candidate.object_key.as_str(),
            ExistingBronzeObject {
                checksum_sha256: checksum('b'),
                size_bytes: 12,
                observed_r2_etag: checksum_mismatch_candidate.observed_r2_etag.clone(),
                observed_r2_last_modified: checksum_mismatch_candidate.observed_r2_last_modified,
            },
        ),
        FakeReader::with_object(
            size_mismatch_candidate.object_key.as_str(),
            ExistingBronzeObject {
                checksum_sha256: checksum('a'),
                size_bytes: 13,
                observed_r2_etag: size_mismatch_candidate.observed_r2_etag.clone(),
                observed_r2_last_modified: size_mismatch_candidate.observed_r2_last_modified,
            },
        ),
    ];

    for (reader, candidate) in cases.into_iter().zip([
        missing_candidate,
        checksum_mismatch_candidate,
        size_mismatch_candidate,
    ]) {
        let writer = FakeCatalogWriter::default();
        let result = BronzeCatalogRecoveryService::new()
            .execute(
                &reader,
                Some(&writer),
                input(BronzeCatalogRecoveryMode::Apply, vec![candidate])?,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(writer.mutation_count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn object_modified_after_inventory_is_rejected_before_catalog_mutation() -> TestResult {
    let candidate = candidate()?;
    let reader = FakeReader::with_object(
        candidate.object_key.as_str(),
        ExistingBronzeObject {
            checksum_sha256: checksum('a'),
            size_bytes: 12,
            observed_r2_etag: candidate.observed_r2_etag.clone(),
            observed_r2_last_modified: fixture_timestamp("2026-07-01T12:00:00Z")?,
        },
    );
    let writer = FakeCatalogWriter::default();

    let result = BronzeCatalogRecoveryService::new()
        .execute(
            &reader,
            Some(&writer),
            input(BronzeCatalogRecoveryMode::Apply, vec![candidate])?,
        )
        .await;

    assert!(matches!(
        result,
        Err(BronzeCatalogRecoveryError::ObjectVersionMismatch { .. })
    ));
    assert_eq!(writer.mutation_count(), 0);
    Ok(())
}

#[tokio::test]
async fn matching_etag_accepts_list_and_head_subsecond_precision_difference() -> TestResult {
    let mut candidate = candidate()?;
    candidate.observed_r2_last_modified = fixture_timestamp("2026-06-30T12:00:00.309Z")?;
    let reader = FakeReader::with_object(
        candidate.object_key.as_str(),
        ExistingBronzeObject {
            checksum_sha256: checksum('a'),
            size_bytes: 12,
            observed_r2_etag: candidate.observed_r2_etag.clone(),
            observed_r2_last_modified: fixture_timestamp("2026-06-30T12:00:00Z")?,
        },
    );

    let report = BronzeCatalogRecoveryService::new()
        .execute(
            &reader,
            None,
            input(BronzeCatalogRecoveryMode::DryRun, vec![candidate])?,
        )
        .await?;

    assert_eq!(report.validated_object_count, 1);
    Ok(())
}

#[tokio::test]
async fn changed_etag_is_rejected_before_catalog_mutation() -> TestResult {
    let candidate = candidate()?;
    let reader = FakeReader::with_object(
        candidate.object_key.as_str(),
        ExistingBronzeObject {
            checksum_sha256: checksum('a'),
            size_bytes: 12,
            observed_r2_etag: "different-etag".to_owned(),
            observed_r2_last_modified: candidate.observed_r2_last_modified,
        },
    );
    let writer = FakeCatalogWriter::default();

    let result = BronzeCatalogRecoveryService::new()
        .execute(
            &reader,
            Some(&writer),
            input(BronzeCatalogRecoveryMode::Apply, vec![candidate])?,
        )
        .await;

    assert!(matches!(
        result,
        Err(BronzeCatalogRecoveryError::ObjectVersionMismatch { .. })
    ));
    assert_eq!(writer.mutation_count(), 0);
    Ok(())
}

#[tokio::test]
async fn invalid_or_path_derived_evidence_is_rejected_before_r2_read() -> TestResult {
    let mut wrong_source = candidate()?;
    wrong_source.object_key =
        ObjectKey::parse("bronze/source=vworldkr__parcel/20991231DS99991-9002.zip")?;

    let mut path_derived = candidate()?;
    path_derived.evidence_kind = RecoveryEvidenceKind::ObjectPathInference;

    for invalid_candidate in [wrong_source, path_derived] {
        let reader = FakeReader::empty();
        let writer = FakeCatalogWriter::default();
        let result = BronzeCatalogRecoveryService::new()
            .execute(
                &reader,
                Some(&writer),
                input(BronzeCatalogRecoveryMode::Apply, vec![invalid_candidate])?,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(reader.read_count(), 0);
        assert_eq!(writer.mutation_count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn catalog_apply_failure_does_not_leave_partial_metadata() -> TestResult {
    let first = candidate()?;
    let mut second = candidate()?;
    second.object_key =
        ObjectKey::parse("bronze/source=vworldkr__land_characteristic/20991231DS99992-9004.zip")?;
    second.source_identity_key = "provider_file_id=20991231DS99992-9004".to_owned();
    second.provider_file_id = Some("20991231DS99992-9004".to_owned());
    second.provider_file_name = Some("20991231DS99992-9004.zip".to_owned());

    let reader = FakeReader::with_objects([
        (
            first.object_key.as_str(),
            ExistingBronzeObject {
                checksum_sha256: checksum('a'),
                size_bytes: 12,
                observed_r2_etag: first.observed_r2_etag.clone(),
                observed_r2_last_modified: first.observed_r2_last_modified,
            },
        ),
        (
            second.object_key.as_str(),
            ExistingBronzeObject {
                checksum_sha256: checksum('a'),
                size_bytes: 12,
                observed_r2_etag: second.observed_r2_etag.clone(),
                observed_r2_last_modified: second.observed_r2_last_modified,
            },
        ),
    ]);
    let writer = FakeCatalogWriter::failing();

    let result = BronzeCatalogRecoveryService::new()
        .execute(
            &reader,
            Some(&writer),
            input(BronzeCatalogRecoveryMode::Apply, vec![first, second])?,
        )
        .await;

    assert!(result.is_err());
    assert_eq!(writer.apply_call_count(), 1);
    assert_eq!(writer.mutation_count(), 0, "Catalog apply must be atomic");
    Ok(())
}

fn input(
    mode: BronzeCatalogRecoveryMode,
    candidates: Vec<BronzeCatalogRecoveryCandidate>,
) -> TestValueResult<BronzeCatalogRecoveryInput> {
    Ok(BronzeCatalogRecoveryInput {
        mode,
        source: source()?,
        evidence_manifest_uri: "file://target/audit/vworld-provider-inventory.json".to_owned(),
        evidence_manifest_sha256: checksum('f'),
        excluded_unresolved_object_count: 0,
        started_at: fixture_timestamp("2026-07-14T09:00:00Z")?,
        candidates,
    })
}

fn source() -> TestValueResult<SourceCatalogEntry> {
    let now = fixture_timestamp("2026-07-14T09:00:00Z")?;
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::now_v7()),
        slug: "vworldkr__land_characteristic".to_owned(),
        name: "VWorld land characteristic".to_owned(),
        provider: "VWorld".to_owned(),
        dataset_name: "land_characteristic".to_owned(),
        base_url: Some("https://www.vworld.kr".to_owned()),
        auth_kind: SourceAuthKind::Manual,
        payload_format: SourcePayloadFormat::Zip,
        license_name: None,
        license_url: None,
        terms_url: Some("https://www.vworld.kr/dtmk/dtmk_ntads_s001.do".to_owned()),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn candidate() -> TestValueResult<BronzeCatalogRecoveryCandidate> {
    let observed_at = fixture_timestamp("2026-06-30T12:00:00Z")?;
    Ok(BronzeCatalogRecoveryCandidate {
        object_key: ObjectKey::parse(
            "bronze/source=vworldkr__land_characteristic/20991231DS99992-9003.zip",
        )?,
        expected_size_bytes: 12,
        expected_checksum_sha256: Some(checksum('a')),
        source_partition_key: Some(
            "operation=land_characteristic/provider_file_id=20991231DS99992-9003".to_owned(),
        ),
        source_identity_key: "provider_file_id=20991231DS99992-9003".to_owned(),
        request_params: json!({
            "provider_file_id": "20991231DS99992-9003",
            "provider_inventory_selector": {"svc_cde": "LT", "ds_id": "00178"}
        }),
        content_type: "application/zip".to_owned(),
        logical_record_count: None,
        observed_r2_etag: "inventory-etag".to_owned(),
        observed_r2_last_modified: observed_at,
        snapshot_period: Some("2017-11".to_owned()),
        snapshot_date: NaiveDate::parse_from_str("2017-11-28", "%Y-%m-%d")?,
        snapshot_granularity: SnapshotGranularity::Day,
        snapshot_basis: SnapshotBasis::ProviderSnapshotDate,
        provider_file_id: Some("20991231DS99992-9003".to_owned()),
        provider_file_name: Some("20991231DS99992-9003.zip".to_owned()),
        provider_updated_at: None,
        effective_date: None,
        evidence_kind: RecoveryEvidenceKind::ProviderInventory,
    })
}

fn fixture_timestamp(value: &str) -> TestValueResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn checksum(ch: char) -> String {
    std::iter::repeat_n(ch, 64).collect()
}

#[derive(Default)]
struct FakeReader {
    objects: BTreeMap<String, ExistingBronzeObject>,
    read_count: Mutex<usize>,
}

impl FakeReader {
    fn empty() -> Self {
        Self::default()
    }

    fn with_object(key: &str, object: ExistingBronzeObject) -> Self {
        Self {
            objects: BTreeMap::from([(key.to_owned(), object)]),
            read_count: Mutex::new(0),
        }
    }

    fn with_objects<'a>(
        objects: impl IntoIterator<Item = (&'a str, ExistingBronzeObject)>,
    ) -> Self {
        Self {
            objects: objects
                .into_iter()
                .map(|(key, object)| (key.to_owned(), object))
                .collect(),
            read_count: Mutex::new(0),
        }
    }

    fn read_count(&self) -> usize {
        *lock(&self.read_count)
    }
}

#[async_trait]
impl BronzeCatalogRecoveryObjectReader for FakeReader {
    async fn read_existing_object(
        &self,
        key: &str,
    ) -> Result<Option<ExistingBronzeObject>, super::BronzeCatalogRecoveryStorageError> {
        *lock(&self.read_count) += 1;
        Ok(self.objects.get(key).cloned())
    }
}

struct BatchOnlyReader {
    object: ExistingBronzeObject,
    batch_read_count: Mutex<usize>,
}

impl BatchOnlyReader {
    fn new(object: ExistingBronzeObject) -> Self {
        Self {
            object,
            batch_read_count: Mutex::new(0),
        }
    }

    fn batch_read_count(&self) -> usize {
        *lock(&self.batch_read_count)
    }
}

#[async_trait]
impl BronzeCatalogRecoveryObjectReader for BatchOnlyReader {
    async fn read_existing_object(
        &self,
        _key: &str,
    ) -> Result<Option<ExistingBronzeObject>, super::BronzeCatalogRecoveryStorageError> {
        Err(super::BronzeCatalogRecoveryStorageError(
            "single-object recovery read must not be used".to_owned(),
        ))
    }

    async fn read_existing_objects(
        &self,
        keys: &[String],
    ) -> Vec<Result<Option<ExistingBronzeObject>, super::BronzeCatalogRecoveryStorageError>> {
        *lock(&self.batch_read_count) += 1;
        keys.iter().map(|_| Ok(Some(self.object.clone()))).collect()
    }
}

#[derive(Default)]
struct FakeCatalogWriter {
    committed: Mutex<Option<ApplyBronzeCatalogRecoveryCommand>>,
    apply_calls: Mutex<usize>,
    fail: bool,
}

impl FakeCatalogWriter {
    fn failing() -> Self {
        Self {
            fail: true,
            ..Self::default()
        }
    }

    fn mutation_count(&self) -> usize {
        usize::from(lock(&self.committed).is_some())
    }

    fn apply_call_count(&self) -> usize {
        *lock(&self.apply_calls)
    }

    fn created_runs(&self) -> Vec<collection_domain::IngestionRun> {
        lock(&self.committed)
            .iter()
            .map(|batch| batch.run.clone())
            .collect()
    }

    fn recorded_objects(&self) -> Vec<BronzeObject> {
        lock(&self.committed)
            .as_ref()
            .map_or_else(Vec::new, |batch| batch.objects.clone())
    }

    fn completions(&self) -> Vec<crate::ports::CompleteIngestionRunCommand> {
        lock(&self.committed)
            .iter()
            .map(|batch| batch.completion.clone())
            .collect()
    }
}

#[async_trait]
impl BronzeCatalogRecoveryCatalogWriter for FakeCatalogWriter {
    async fn apply_recovery(
        &self,
        command: ApplyBronzeCatalogRecoveryCommand,
    ) -> Result<collection_domain::IngestionRun, CollectionError> {
        *lock(&self.apply_calls) += 1;
        if self.fail {
            return Err(CollectionError::Infrastructure(
                "injected Catalog recovery write failure".to_owned(),
            ));
        }
        let mut run = command.run.clone();
        run.status = command.completion.status;
        run.finished_at = Some(command.completion.finished_at);
        run.logical_records_seen = command.completion.logical_records_seen;
        run.objects_written = command.completion.objects_written;
        *lock(&self.committed) = Some(command);
        Ok(run)
    }
}
