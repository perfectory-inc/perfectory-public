use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::ports::{
    BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand,
};
use collection_domain::CollectionError;
use collection_domain::{
    BronzeObject, IngestionRun, IngestionRunStatus, SchemaProfile, SnapshotBasis,
    SnapshotGranularity, SourceCatalogEntry,
};
use collection_infrastructure::{
    VWorldDatasetFileInventoryItem, VWorldDatasetFileKind, VWorldDatasetFileStream,
};
use foundation_outbox::{
    object_storage::{
        ObjectWriteMode, PutObjectRequest, StreamingObjectRehash, StreamingPutObjectRequest,
    },
    ObjectStorageService, ObjectStorageStreamingService, PublishError,
};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKey;
use futures_util::{stream, StreamExt as _};
use serde_json::json;
use uuid::Uuid;

use super::{
    download_request_from_inventory_file, eligible_inventory_file_count, existing_file_report,
    failed_file_report, parse_dataset_file_max_in_flight, persist_file_stream_with_adapters,
    plan_streamed_file_location, select_inventory_files, validate_inventory_file_identity,
    vworld_dataset_file_ingest_status, vworld_dataset_login_config, VWorldDatasetFileIngestConfig,
    VWorldDatasetFileJob,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn download_request_from_inventory_file_uses_provider_download_dataset_id() {
    let file = test_inventory_file("4", "20991231DS99992", "9006", "2026-05");

    let request = download_request_from_inventory_file(&file);

    assert_eq!(request.download_ds_id, "20991231DS99992");
    assert_eq!(request.file_no, "9006");
    assert_eq!(
        request.download_kind,
        VWorldDatasetFileKind::SingleResourceFile
    );
}

#[test]
fn plan_streamed_file_location_uses_updated_at_when_base_year_month_is_missing() -> TestResult {
    let job = test_job();
    let mut file = test_inventory_file("30017", "20991231DS99994", "9007", "-");
    file.updated_at = "2026-04-10".to_owned();
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000301")?);
    let ingest_date = chrono::NaiveDate::from_ymd_opt(2026, 6, 4).ok_or("invalid ingest date")?;

    let plan = plan_streamed_file_location(
        &job,
        &file,
        run_id,
        ingest_date,
        "SYNTHETIC_BOUNDARY_ARCHIVE.zip".to_owned(),
    )?;

    assert!(!plan.source_partition_key.contains("period="));
    assert!(plan
        .source_partition_key
        .contains("provider_file_id=20991231DS99994-9007"));
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-04"));
    assert_eq!(
        plan.snapshot_date,
        chrono::NaiveDate::from_ymd_opt(2026, 4, 10).ok_or("invalid snapshot date")?
    );
    assert_eq!(plan.snapshot_basis.as_str(), "provider_updated_at");
    Ok(())
}

#[test]
fn plan_streamed_file_location_uses_base_year_month_day_as_snapshot_date() -> TestResult {
    let job = test_job();
    let mut file = test_inventory_file("20991231DS99992", "20991231DS99992", "9003", "2026-05-20");
    file.updated_at = "2026-05-21".to_owned();
    let run_id = IngestionRunId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000302")?);
    let ingest_date = chrono::NaiveDate::from_ymd_opt(2026, 6, 4).ok_or("invalid ingest date")?;

    let plan = plan_streamed_file_location(
        &job,
        &file,
        run_id,
        ingest_date,
        "SYNTHETIC_REGION.zip".to_owned(),
    )?;

    assert!(!plan.source_partition_key.contains("period="));
    assert!(plan
        .source_partition_key
        .contains("provider_file_id=20991231DS99992-9003"));
    assert_eq!(plan.snapshot_period.as_deref(), Some("2026-05"));
    assert_eq!(
        plan.snapshot_date,
        chrono::NaiveDate::from_ymd_opt(2026, 5, 20).ok_or("invalid snapshot date")?
    );
    assert_eq!(plan.snapshot_basis.as_str(), "provider_snapshot_date");
    Ok(())
}

#[test]
fn login_config_is_not_built_when_cookie_header_is_explicit() {
    let mut config = test_config();
    config.cookie_header = Some("PJSESSIONID=already-authenticated".to_owned());
    config.username = Some("unit-test-vworld-user".to_owned());
    config.password = Some("unit-test-vworld-password".to_owned());

    let login_config = vworld_dataset_login_config(&config, "https://www.vworld.kr");

    assert!(login_config.is_none());
}

#[test]
fn login_config_uses_provider_credentials_when_cookie_header_is_absent() -> TestResult {
    let mut config = test_config();
    config.username = Some("unit-test-vworld-user".to_owned());
    config.password = Some("unit-test-vworld-password".to_owned());

    let login_config = vworld_dataset_login_config(&config, "https://www.vworld.kr")
        .ok_or("expected login config")?;

    assert_eq!(login_config.base_uri, "https://www.vworld.kr");
    assert_eq!(login_config.user_agent, config.user_agent);
    assert_eq!(login_config.username, "unit-test-vworld-user");
    assert_eq!(login_config.password, "unit-test-vworld-password");
    Ok(())
}

#[test]
fn dataset_file_parallelism_defaults_to_four_and_rejects_zero() -> TestResult {
    assert_eq!(parse_dataset_file_max_in_flight(None)?, 4);
    assert_eq!(parse_dataset_file_max_in_flight(Some("6".to_owned()))?, 6);

    let error = parse_dataset_file_max_in_flight(Some("0".to_owned()))
        .err()
        .ok_or("expected zero max-in-flight to fail")?;
    assert!(
        format!("{error:#}").contains(
            "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT must be greater than zero"
        ),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn persist_file_stream_records_file_metadata_after_storage_write() -> TestResult {
    let job = test_job();
    let inventory_file = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    let raw_payload = b"PK\x03\x04vworld bytes".to_vec();
    let downloaded = test_file_stream("SYNTHETIC_BOUNDARY_ARCHIVE.zip", raw_payload.clone());
    let run_id = test_run_id("018f0000-0000-7000-8000-000000000302")?;
    let started_at = test_started_at()?;
    let expected_object_key =
        "bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip".to_owned();
    let expected_source_partition_key =
        "operation=boundary_census_emd/provider_file_id=20991231DS99994-9007".to_owned();
    let expected_size_bytes = raw_payload.len() as u64;
    let uow = RecordingUow::default();
    let storage = RecordingObjectStorage::default();

    persist_file_stream_with_adapters(
        &job,
        &inventory_file,
        run_id,
        started_at,
        downloaded,
        &uow,
        &storage,
    )
    .await?;

    assert_eq!(storage.writes()?.len(), 0);
    let streaming_writes = storage.streaming_writes()?;
    assert_eq!(streaming_writes.len(), 1);
    assert_eq!(streaming_writes[0].key, expected_object_key);
    assert_eq!(streaming_writes[0].body, raw_payload);
    assert_eq!(streaming_writes[0].content_type, "application/zip");
    assert_eq!(streaming_writes[0].cache_control, "no-store, max-age=0");
    assert_eq!(streaming_writes[0].size_bytes, expected_size_bytes);
    // Routed through the committer: streaming bulk writes are now write-once `CreateOnly` (was
    // `OverwriteAllowed` on the legacy direct path).
    assert_eq!(streaming_writes[0].write_mode, ObjectWriteMode::CreateOnly);

    let objects = uow.objects()?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].object_key.as_str(), expected_object_key);
    assert_eq!(
        objects[0].source_partition_key.as_deref(),
        Some(expected_source_partition_key.as_str())
    );
    // The dedupe key is byte-identical to the legacy path's
    // `<slug>:<source_partition_key>:sha256=<checksum>` shape (committer appends the streamed sha
    // to the same prefix the SSOT helper produces).
    assert!(objects[0].dedupe_key.starts_with(
        "vworldkr__boundary_census_emd:provider_file_id=20991231DS99994-9007:sha256="
    ));
    assert_eq!(objects[0].content_type, "application/zip");
    assert_eq!(objects[0].logical_record_count, None);
    assert_eq!(
        objects[0].request_params["provider_file_name"],
        serde_json::json!("SYNTHETIC_BOUNDARY_ARCHIVE.zip")
    );

    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
    assert_eq!(completions[0].objects_written, 1);
    assert_eq!(completions[0].logical_records_seen, 0);
    assert_eq!(completions[0].error_message, None);
    Ok(())
}

/// Streaming recovery (ADR 0016): a `CreateOnly` `412` collision with NO recorded `bronze_object`
/// row (R2 streamed previously, the DB record failed) GET-rehashes the existing object and RECOVERS
/// by recording the missing row from that checksum + size — instead of failing the run.
#[tokio::test]
async fn persist_file_stream_recovers_missing_row_on_create_only_collision() -> TestResult {
    let job = test_job();
    let inventory_file = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    let raw_payload = b"PK\x03\x04vworld bytes".to_vec();
    let downloaded = test_file_stream("SYNTHETIC_BOUNDARY_ARCHIVE.zip", raw_payload.clone());
    let run_id = test_run_id("018f0000-0000-7000-8000-000000000305")?;
    let started_at = test_started_at()?;
    let expected_object_key =
        "bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip".to_owned();
    let rehashed_checksum = "b".repeat(64);
    let rehashed_size = 4242;
    let uow = RecordingUow::default();
    // The existing object collides on the CreateOnly stream; the read-back rehash makes it OURS so
    // the committer recovers the missing row rather than failing.
    let storage = RecordingObjectStorage::already_exists(StreamingObjectRehash {
        checksum_sha256: rehashed_checksum.clone(),
        size_bytes: rehashed_size,
        observed_e_tag: None,
        observed_last_modified: None,
    });

    let report = persist_file_stream_with_adapters(
        &job,
        &inventory_file,
        run_id,
        started_at,
        downloaded,
        &uow,
        &storage,
    )
    .await?;

    // The body was NOT re-streamed (the conditional write was rejected before the body was read).
    assert_eq!(storage.streaming_writes()?.len(), 0);

    // The missing row was recovered from the GET-rehash's checksum + size.
    let objects = uow.objects()?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].object_key.as_str(), expected_object_key);
    assert_eq!(objects[0].checksum_sha256, rehashed_checksum);
    assert_eq!(objects[0].size_bytes, rehashed_size);
    assert_eq!(report.size_bytes, rehashed_size);

    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
    assert_eq!(completions[0].objects_written, 1);
    Ok(())
}

/// A streaming write failure (an R2 outage, not a `412` collision) marks the run failed, records no
/// `bronze_object`, and surfaces the underlying storage cause — the unhappy-path contract is
/// preserved after routing through the committer.
#[tokio::test]
async fn persist_file_stream_marks_run_failed_when_storage_write_fails() -> TestResult {
    let job = test_job();
    let inventory_file = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    let downloaded = test_file_stream(
        "SYNTHETIC_BOUNDARY_ARCHIVE.zip",
        b"PK\x03\x04vworld bytes".to_vec(),
    );
    let run_id = test_run_id("018f0000-0000-7000-8000-000000000306")?;
    let started_at = test_started_at()?;
    let uow = RecordingUow::default();
    let storage = RecordingObjectStorage::failing("simulated R2 failure");

    let error = persist_file_stream_with_adapters(
        &job,
        &inventory_file,
        run_id,
        started_at,
        downloaded,
        &uow,
        &storage,
    )
    .await
    .err()
    .ok_or("expected storage write failure")?;

    assert!(
        format!("{error:#}").contains("simulated R2 failure"),
        "unexpected error: {error}"
    );
    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, IngestionRunStatus::Failed);
    assert_eq!(completions[0].objects_written, 0);
    assert!(completions[0].error_message.is_some());
    assert_eq!(uow.objects()?.len(), 0);
    Ok(())
}

#[tokio::test]
async fn existing_file_report_marks_provider_file_as_skipped() -> TestResult {
    let job = test_job();
    let inventory_file = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    let started_at = test_started_at()?;
    let existing_object_key =
        "bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip";
    let repo = RecordingRepo::with_existing(existing_bronze_object(
        "operation=boundary_census_emd/provider_file_id=20991231DS99994-9007",
        existing_object_key,
        5678,
    )?);
    let uow = RecordingUow::default();

    let report = existing_file_report(&job, &inventory_file, started_at, &repo, &uow)
        .await?
        .ok_or("expected existing Bronze object to be detected")?;

    assert_eq!(report.status, "skipped_existing");
    assert_eq!(report.object_key.as_deref(), Some(existing_object_key));
    assert_eq!(report.size_bytes, Some(5678));
    assert_eq!(uow.objects()?.len(), 0);
    assert_eq!(uow.completions()?.len(), 0);
    Ok(())
}

#[test]
fn inventory_file_identity_rejects_ambiguous_components() {
    // "20991231DS99994-9" + "007" and "20991231DS99994" + "9-007" would flatten to
    // the same provider_file_id,
    // so non-alphanumeric components must be rejected before the resume lookup runs.
    let ambiguous_ds_id = test_inventory_file("30017", "20991231DS99994-9", "007", "2026-05");
    assert!(validate_inventory_file_identity(&ambiguous_ds_id).is_err());

    let ambiguous_file_no = test_inventory_file("30017", "20991231DS99994", "9-007", "2026-05");
    assert!(validate_inventory_file_identity(&ambiguous_file_no).is_err());

    let clean = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    assert!(validate_inventory_file_identity(&clean).is_ok());
}

#[test]
fn select_inventory_files_rejects_duplicate_partition_identities() -> TestResult {
    let mut job = test_job();
    job.files = vec![
        test_inventory_file("30017", "20991231DS99994", "9007", "2026-05"),
        test_inventory_file("30017", "20991231DS99994", "9007", "2026-05"),
    ];

    let error = select_inventory_files(&[job], None, None, false)
        .err()
        .ok_or("duplicate partition identities must be rejected")?;
    assert!(
        error.to_string().contains("duplicate partition identity"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn select_inventory_files_can_exclude_selection_archives() -> TestResult {
    let mut job = test_job();
    let single = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    let mut selection_archive =
        test_inventory_file("20991231DS99991", "20991231DS99991", "9002", "2026-06");
    selection_archive.download_kind = VWorldDatasetFileKind::SelectionArchive;
    job.files = vec![single.clone(), selection_archive];

    let selected = select_inventory_files(&[job], None, None, true)?;

    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected[0].file.download_kind,
        VWorldDatasetFileKind::SingleResourceFile
    );
    assert_eq!(selected[0].file.download_ds_id, single.download_ds_id);
    assert_eq!(selected[0].file.file_no, single.file_no);
    Ok(())
}

#[test]
fn eligible_inventory_file_count_excludes_selection_archives_when_requested() {
    let mut job = test_job();
    let single = test_inventory_file("30017", "20991231DS99994", "9007", "2026-05");
    let mut selection_archive =
        test_inventory_file("20991231DS99991", "20991231DS99991", "9002", "2026-06");
    selection_archive.download_kind = VWorldDatasetFileKind::SelectionArchive;
    job.files = vec![single, selection_archive];

    assert_eq!(eligible_inventory_file_count(&[job.clone()], false), 2);
    assert_eq!(eligible_inventory_file_count(&[job], true), 1);
}

#[test]
fn ingest_status_can_defer_provider_acquisition_blockers() {
    let status = vworld_dataset_file_ingest_status(0, 1, true);

    assert_eq!(
        status.evidence_status,
        "ready_with_provider_acquisition_deferred"
    );
    assert!(!status.should_bail);
}

#[test]
fn ingest_status_blocks_provider_acquisition_without_defer_flag() {
    let status = vworld_dataset_file_ingest_status(0, 1, false);

    assert_eq!(status.evidence_status, "blocked");
    assert!(status.should_bail);
}

#[test]
fn ingest_status_always_blocks_real_failures() {
    let status = vworld_dataset_file_ingest_status(1, 1, true);

    assert_eq!(status.evidence_status, "blocked");
    assert!(status.should_bail);
}

#[test]
fn failed_file_report_classifies_raon_selection_archive_as_provider_acquisition_blocked(
) -> TestResult {
    let job = test_job();
    let mut inventory_file =
        test_inventory_file("20991231DS99993", "20991231DS99993", "9005", "2026-05");
    inventory_file.download_kind = VWorldDatasetFileKind::SelectionArchive;
    let started_at = test_started_at()?;

    let report = failed_file_report(
        &job,
        &inventory_file,
        started_at,
        CollectionError::ProviderAcquisitionBlocked(
            "VWorld dataset selection archive requires RAON/KUpload desktop agent".to_owned(),
        )
        .into(),
    );

    assert_eq!(report.status, "provider_acquisition_blocked");
    assert!(report
        .error_message
        .as_deref()
        .is_some_and(|message| message.contains("RAON/KUpload desktop agent")));
    Ok(())
}

fn test_job() -> VWorldDatasetFileJob {
    VWorldDatasetFileJob {
        endpoint_slug: "vworld-dataset-boundary_census_emd".to_owned(),
        source_slug: "vworldkr__boundary_census_emd".to_owned(),
        source_name: "VWorld boundary census emd".to_owned(),
        dataset_name: "boundary_census_emd".to_owned(),
        base_uri: "https://www.vworld.kr".to_owned(),
        terms_url: Some("https://www.vworld.kr/dtmk/dtmk_ntads_s001.do".to_owned()),
        operation: "boundary_census_emd".to_owned(),
        provider_module: "boundary_census_emd".to_owned(),
        svc_cde: "MK".to_owned(),
        ds_id: "30017".to_owned(),
        files: Vec::new(),
    }
}

fn test_config() -> VWorldDatasetFileIngestConfig {
    VWorldDatasetFileIngestConfig {
        file_inventory_path: "target/audit/test-vworld-inventory.json".into(),
        evidence_path: "target/audit/test-vworld-evidence.json".into(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
        username: None,
        password: None,
        live_write: None,
        max_jobs: None,
        max_files: None,
        max_in_flight: 4,
        full_download_confirmed: false,
        exclude_selection_archives: false,
        defer_provider_acquisition_blocked: false,
    }
}

fn test_run_id(raw: &str) -> anyhow::Result<IngestionRunId> {
    Ok(IngestionRunId::new(Uuid::parse_str(raw)?))
}

fn test_started_at() -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339("2026-06-04T00:00:00Z")?.to_utc())
}

fn test_file_stream(provider_file_name: &str, raw_payload: Vec<u8>) -> VWorldDatasetFileStream {
    VWorldDatasetFileStream::from_body_stream(
        "application/zip".to_owned(),
        provider_file_name.to_owned(),
        Some(raw_payload.len() as u64),
        stream::iter([Ok(Bytes::from(raw_payload))]).boxed(),
    )
}

fn test_inventory_file(
    ds_id: &str,
    download_ds_id: &str,
    file_no: &str,
    base_ym: &str,
) -> VWorldDatasetFileInventoryItem {
    VWorldDatasetFileInventoryItem {
        svc_cde: "NA".to_owned(),
        ds_id: ds_id.to_owned(),
        download_ds_id: download_ds_id.to_owned(),
        file_no: file_no.to_owned(),
        provider_file_name: "provider.zip".to_owned(),
        file_format: "SHP".to_owned(),
        size_mb_label: "1".to_owned(),
        size_kib: 1_024,
        provider_file_kind: "data".to_owned(),
        base_ym: base_ym.to_owned(),
        updated_at: "2026-05-13".to_owned(),
        download_kind: VWorldDatasetFileKind::SingleResourceFile,
    }
}

fn existing_bronze_object(
    source_partition_key: &str,
    object_key: &str,
    size_bytes: u64,
) -> anyhow::Result<BronzeObject> {
    Ok(BronzeObject {
        id: BronzeObjectId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000501")?),
        source_catalog_id: SourceCatalogId::new(Uuid::parse_str(
            "018f0000-0000-7000-8000-000000000502",
        )?),
        ingestion_run_id: test_run_id("018f0000-0000-7000-8000-000000000503")?,
        source_record_id: None,
        source_partition_key: Some(source_partition_key.to_owned()),
        source_identity_key: "provider_file_id=20991231DS99994-9007".to_owned(),
        dedupe_key: format!("{source_partition_key}:sha256=test"),
        request_params: json!({}),
        object_key: ObjectKey::parse(object_key)?,
        checksum_sha256: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
            .to_owned(),
        content_type: "application/zip".to_owned(),
        size_bytes,
        logical_record_count: None,
        collected_at: test_started_at()?,
        snapshot_period: Some("2026-05".to_owned()),
        snapshot_date: NaiveDate::from_ymd_opt(2026, 5, 1).expect("valid snapshot date"),
        snapshot_granularity: SnapshotGranularity::Month,
        snapshot_basis: SnapshotBasis::ProviderFilePeriod,
        provider_file_id: Some("20991231DS99994-9007".to_owned()),
        provider_file_name: Some("provider.zip".to_owned()),
        provider_updated_at: Some(
            NaiveDate::from_ymd_opt(2026, 5, 13).expect("valid provider update date"),
        ),
        effective_date: None,
        created_at: test_started_at()?,
    })
}

#[derive(Debug, Default)]
struct RecordingUow {
    source: Mutex<Option<SourceCatalogEntry>>,
    run: Mutex<Option<IngestionRun>>,
    objects: Mutex<Vec<BronzeObject>>,
    completions: Mutex<Vec<CompleteIngestionRunCommand>>,
}

#[derive(Debug, Default)]
struct RecordingRepo {
    existing: Mutex<Option<BronzeObject>>,
}

impl RecordingRepo {
    fn with_existing(object: BronzeObject) -> Self {
        Self {
            existing: Mutex::new(Some(object)),
        }
    }
}

#[async_trait]
impl BronzeIngestRepository for RecordingRepo {
    async fn find_source_catalog_by_slug(
        &self,
        _slug: &str,
    ) -> Result<Option<SourceCatalogEntry>, CollectionError> {
        Ok(None)
    }

    async fn find_ingestion_run(
        &self,
        _id: IngestionRunId,
    ) -> Result<Option<IngestionRun>, CollectionError> {
        Ok(None)
    }

    async fn list_bronze_objects_by_run(
        &self,
        _run_id: IngestionRunId,
    ) -> Result<Vec<BronzeObject>, CollectionError> {
        Ok(Vec::new())
    }

    async fn find_bronze_object_by_source_partition_key(
        &self,
        _source_catalog_id: SourceCatalogId,
        source_partition_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        let existing = lock(&self.existing, "existing")?.clone();
        Ok(existing
            .filter(|object| object.source_partition_key.as_deref() == Some(source_partition_key)))
    }

    async fn list_schema_profiles_by_run(
        &self,
        _run_id: IngestionRunId,
    ) -> Result<Vec<SchemaProfile>, CollectionError> {
        Ok(Vec::new())
    }
}

impl RecordingUow {
    fn objects(&self) -> Result<Vec<BronzeObject>, CollectionError> {
        Ok(lock(&self.objects, "objects")?.clone())
    }

    fn completions(&self) -> Result<Vec<CompleteIngestionRunCommand>, CollectionError> {
        Ok(lock(&self.completions, "completions")?.clone())
    }
}

#[async_trait]
impl BronzeIngestUnitOfWork for RecordingUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        *lock(&self.source, "source")? = Some(entry.clone());
        Ok(entry.clone())
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        *lock(&self.run, "run")? = Some(run.clone());
        Ok(run.clone())
    }

    async fn complete_ingestion_run(
        &self,
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        lock(&self.completions, "completions")?.push(command.clone());
        let mut run = lock(&self.run, "run")?
            .clone()
            .ok_or_else(|| CollectionError::Infrastructure("run was not created".to_owned()))?;
        run.status = command.status;
        run.finished_at = Some(command.finished_at);
        run.logical_records_seen = command.logical_records_seen;
        run.objects_written = command.objects_written;
        run.error_message = command.error_message;
        *lock(&self.run, "run")? = Some(run.clone());
        Ok(run)
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(lock(&self.objects, "objects")?
            .iter()
            .rev()
            .find(|object| {
                object.source_catalog_id == source_catalog_id
                    && object.object_key.as_str() == object_key
            })
            .cloned())
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        lock(&self.objects, "objects")?.push(object.clone());
        Ok(object.clone())
    }

    async fn upsert_schema_profile(
        &self,
        _profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        Err(CollectionError::Infrastructure(
            "VWorld dataset file Bronze ingest must not upsert schema profiles".to_owned(),
        ))
    }
}

fn lock<'a, T>(
    mutex: &'a Mutex<T>,
    name: &'static str,
) -> Result<MutexGuard<'a, T>, CollectionError> {
    mutex
        .lock()
        .map_err(|_| CollectionError::Infrastructure(format!("{name} mutex poisoned")))
}

#[derive(Debug, Default)]
struct RecordingObjectStorage {
    writes: Mutex<Vec<PutObjectRequest>>,
    streaming_writes: Mutex<Vec<StreamingWriteRecord>>,
    already_exists: bool,
    rehash: Option<StreamingObjectRehash>,
    failure_message: Option<String>,
}

impl RecordingObjectStorage {
    /// A storage fake whose `CreateOnly` streaming put always collides (R2 `412`), and whose
    /// GET-rehash read-back returns `rehash` — mirroring the prior R2-success / DB-fail crash the
    /// committer's streaming recovery protocol heals.
    fn already_exists(rehash: StreamingObjectRehash) -> Self {
        Self {
            already_exists: true,
            rehash: Some(rehash),
            ..Self::default()
        }
    }

    /// A storage fake whose streaming put fails with an infrastructure error (an R2 outage, not a
    /// `412` collision) so the committer surfaces a `Storage` error and the run is marked failed.
    fn failing(message: &str) -> Self {
        Self {
            failure_message: Some(message.to_owned()),
            ..Self::default()
        }
    }

    fn writes(&self) -> Result<Vec<PutObjectRequest>, PublishError> {
        self.writes
            .lock()
            .map(|writes| writes.clone())
            .map_err(|_| {
                PublishError::Infrastructure("object storage writes mutex poisoned".to_owned())
            })
    }

    fn streaming_writes(&self) -> Result<Vec<StreamingWriteRecord>, PublishError> {
        self.streaming_writes
            .lock()
            .map(|writes| writes.clone())
            .map_err(|_| {
                PublishError::Infrastructure(
                    "object storage streaming writes mutex poisoned".to_owned(),
                )
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamingWriteRecord {
    key: String,
    body: Vec<u8>,
    content_type: String,
    cache_control: String,
    size_bytes: u64,
    write_mode: ObjectWriteMode,
}

#[async_trait]
impl ObjectStorageService for RecordingObjectStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        self.writes
            .lock()
            .map_err(|_| {
                PublishError::Infrastructure("object storage writes mutex poisoned".to_owned())
            })?
            .push(request);
        Ok(())
    }

    async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
        Ok(None)
    }
}

#[async_trait]
impl ObjectStorageStreamingService for RecordingObjectStorage {
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError> {
        if let Some(message) = &self.failure_message {
            return Err(PublishError::Infrastructure(message.clone()));
        }
        // A `CreateOnly` collision surfaces as `ObjectAlreadyExists` (R2 `412`) so the committer
        // runs its streaming recovery protocol instead of failing.
        if self.already_exists {
            return Err(PublishError::ObjectAlreadyExists { key: request.key });
        }
        let body = request
            .body
            .collect()
            .await
            .map_err(|error| {
                PublishError::Infrastructure(format!("streaming test body read failed: {error}"))
            })?
            .into_bytes()
            .to_vec();
        self.streaming_writes
            .lock()
            .map_err(|_| {
                PublishError::Infrastructure(
                    "object storage streaming writes mutex poisoned".to_owned(),
                )
            })?
            .push(StreamingWriteRecord {
                key: request.key,
                body,
                content_type: request.content_type,
                cache_control: request.cache_control,
                size_bytes: request.size_bytes,
                write_mode: request.write_mode,
            });
        Ok(())
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        _key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        // The committer's streaming recovery GET-rehashes the existing object when a `412` collides
        // with no recorded row; the fake returns the configured rehash (or `None`).
        Ok(self.rehash.clone())
    }
}
