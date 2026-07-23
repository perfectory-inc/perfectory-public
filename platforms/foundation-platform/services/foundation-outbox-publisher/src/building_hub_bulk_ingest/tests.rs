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
    SnapshotGranularity, SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::BuildingHubBulkFileStream;
use foundation_outbox::{
    object_storage::{PutObjectRequest, StreamingObjectRehash, StreamingPutObjectRequest},
    ObjectStorageService, ObjectStorageStreamingService, PublishError,
};
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKey;
use futures_util::{stream, StreamExt as _};
use serde_json::json;
use uuid::Uuid;

use super::{
    collection_job_to_ingest_config, existing_collection_job_report,
    parse_collection_max_in_flight, persist_bulk_file_stream_with_adapters,
    pre_download_skip_report, select_collection_jobs, BuildingHubBulkCollectionPlanJob,
    BuildingHubBulkIngestConfig,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn persist_bulk_file_stream_records_file_metadata_after_storage_write() -> TestResult {
    let config = test_config();
    let run_id = test_run_id("018f0000-0000-7000-8000-000000000201")?;
    let started_at = test_started_at()?;
    let raw_payload = b"PK\x03\x04provider zip bytes".to_vec();
    let file = test_file_stream("building_register_main_202605.zip", raw_payload.clone());
    let expected_object_key =
        "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip".to_owned();
    let expected_source_partition_key =
        "operation=building_register_main/provider_file_id=OPN209912310000000008".to_owned();
    let expected_size_bytes = raw_payload.len() as u64;
    let uow = RecordingUow::default();
    let storage = RecordingObjectStorage::default();

    let report =
        persist_bulk_file_stream_with_adapters(&config, run_id, started_at, file, &uow, &storage)
            .await?;

    assert_eq!(report.run_id, run_id);
    assert_eq!(report.objects_written, 1);
    assert_eq!(report.logical_records_seen, 0);

    assert_eq!(storage.writes()?.len(), 0);
    let streaming_writes = storage.streaming_writes()?;
    assert_eq!(streaming_writes.len(), 1);
    assert_eq!(streaming_writes[0].key, expected_object_key);
    assert_eq!(streaming_writes[0].body, raw_payload);
    assert_eq!(streaming_writes[0].content_type, "application/zip");
    assert_eq!(streaming_writes[0].cache_control, "no-store, max-age=0");
    assert_eq!(streaming_writes[0].size_bytes, expected_size_bytes);

    let source = uow.source()?;
    assert_eq!(source.slug, config.source_slug);
    assert_eq!(source.provider, "hub.go.kr");
    assert_eq!(source.payload_format, SourcePayloadFormat::Zip);

    let objects = uow.objects()?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].object_key.as_str(), expected_object_key);
    assert_eq!(
        objects[0].source_partition_key.as_deref(),
        Some(expected_source_partition_key.as_str())
    );
    assert!(objects[0].dedupe_key.starts_with(
        "hubgokr__building_register_main:provider_file_id=OPN209912310000000008:sha256="
    ));
    assert_eq!(objects[0].content_type, "application/zip");
    assert_eq!(objects[0].logical_record_count, None);
    assert_eq!(
        objects[0].request_params["provider_file_id"],
        "OPN209912310000000008"
    );

    let completions = uow.completions()?;
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
    assert_eq!(completions[0].objects_written, 1);
    assert_eq!(completions[0].logical_records_seen, 0);
    assert_eq!(completions[0].error_message, None);
    Ok(())
}

#[tokio::test]
async fn persist_bulk_file_stream_marks_run_failed_when_storage_write_fails() -> TestResult {
    let config = test_config();
    let run_id = test_run_id("018f0000-0000-7000-8000-000000000202")?;
    let started_at = test_started_at()?;
    let file = test_file_stream(
        "building_register_main_202605.zip",
        b"PK\x03\x04provider zip bytes".to_vec(),
    );
    let uow = RecordingUow::default();
    let storage = RecordingObjectStorage::failing("simulated R2 failure");

    let error =
        persist_bulk_file_stream_with_adapters(&config, run_id, started_at, file, &uow, &storage)
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

#[test]
fn collection_job_to_ingest_config_preserves_provider_file_identity() {
    let job = BuildingHubBulkCollectionPlanJob {
        catalog_binding_status: "provider_inventory_only".to_owned(),
        endpoint_slug: "hub-go-kr-public-bulk-task-01-0101".to_owned(),
        source_slug: "hub-go-kr-public-bulk-task-01-0101".to_owned(),
        source_name: "building permit / basis".to_owned(),
        dataset_name: "basis".to_owned(),
        base_uri: "https://www.hub.go.kr".to_owned(),
        terms_url: Some("https://www.hub.go.kr/list".to_owned()),
        operation: "hub_task_01_0101".to_owned(),
        provider_file_period: "2026-05".to_owned(),
        provider_file_id: "OPN209912310000000005".to_owned(),
        category_name: "building permit".to_owned(),
        service_name: "basis".to_owned(),
        service_period_label: "2026-04".to_owned(),
        task_group_code: "01".to_owned(),
        task_code: "0101".to_owned(),
    };

    let config =
        collection_job_to_ingest_config(&job, "foundation-platform-test/1.0", Some("1".to_owned()));

    assert_eq!(config.source_slug, "hub-go-kr-public-bulk-task-01-0101");
    assert_eq!(config.source_name, "building permit / basis");
    assert_eq!(config.dataset_name, "basis");
    assert_eq!(config.operation, "hub_task_01_0101");
    assert_eq!(config.provider_file_period, "2026-05");
    assert_eq!(config.provider_file_id, "OPN209912310000000005");
    assert_eq!(config.live_write.as_deref(), Some("1"));
}

#[test]
fn collection_parallelism_defaults_to_four_and_rejects_zero() -> TestResult {
    assert_eq!(parse_collection_max_in_flight(None)?, 4);
    assert_eq!(parse_collection_max_in_flight(Some("8".to_owned()))?, 8);

    let error = parse_collection_max_in_flight(Some("0".to_owned()))
        .err()
        .ok_or("expected zero max-in-flight to fail")?;
    assert!(
        format!("{error:#}").contains(
            "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_IN_FLIGHT must be greater than zero"
        ),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn existing_collection_job_report_marks_provider_file_as_skipped() -> TestResult {
    let job = test_collection_job();
    let config =
        collection_job_to_ingest_config(&job, "foundation-platform-test/1.0", Some("1".to_owned()));
    let started_at = test_started_at()?;
    let existing_object_key =
        "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip";
    let repo = RecordingRepo::with_existing(existing_bronze_object(
        "operation=building_register_main/provider_file_id=OPN209912310000000008",
        existing_object_key,
        1234,
    )?);
    let uow = RecordingUow::default();

    let report = existing_collection_job_report(&job, &config, started_at, &repo, &uow)
        .await?
        .ok_or("expected existing Bronze object to be detected")?;

    assert_eq!(report.status, "skipped_existing");
    assert_eq!(report.object_key.as_deref(), Some(existing_object_key));
    assert_eq!(report.size_bytes, Some(1234));
    assert_eq!(uow.objects()?.len(), 0);
    assert_eq!(uow.completions()?.len(), 0);
    Ok(())
}

/// The pre-download skip is taken (returns the `skipped_existing` evidence) when force-refetch is
/// OFF and a matching Bronze object already exists — the default request-fingerprint optimization.
#[tokio::test]
async fn pre_download_skip_taken_when_force_refetch_off_and_object_exists() -> TestResult {
    let job = test_collection_job();
    let config =
        collection_job_to_ingest_config(&job, "foundation-platform-test/1.0", Some("1".to_owned()));
    let started_at = test_started_at()?;
    let repo = RecordingRepo::with_existing(existing_bronze_object(
        "operation=building_register_main/provider_file_id=OPN209912310000000008",
        "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip",
        1234,
    )?);
    let uow = RecordingUow::default();

    let report = pre_download_skip_report(false, &job, &config, started_at, &repo, &uow)
        .await?
        .ok_or("force-refetch OFF with an existing object must take the skip")?;

    assert_eq!(report.status, "skipped_existing");
    Ok(())
}

/// The pre-download skip is BYPASSED (returns `None`, so the caller re-downloads and re-runs the
/// post-download content check) when `FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH` is set — even though a
/// matching Bronze object exists. The existence check is not even consulted.
#[tokio::test]
async fn pre_download_skip_bypassed_when_force_refetch_on() -> TestResult {
    let job = test_collection_job();
    let config =
        collection_job_to_ingest_config(&job, "foundation-platform-test/1.0", Some("1".to_owned()));
    let started_at = test_started_at()?;
    let repo = RecordingRepo::with_existing(existing_bronze_object(
        "operation=building_register_main/provider_file_id=OPN209912310000000008",
        "bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip",
        1234,
    )?);
    let uow = RecordingUow::default();

    let report = pre_download_skip_report(true, &job, &config, started_at, &repo, &uow).await?;

    assert!(
        report.is_none(),
        "force-refetch ON must bypass the skip even when an object exists, so the caller re-downloads"
    );
    Ok(())
}

fn test_config() -> BuildingHubBulkIngestConfig {
    BuildingHubBulkIngestConfig {
        source_slug: "hubgokr__building_register_main".to_owned(),
        source_name: "hub.go.kr building-register main bulk".to_owned(),
        provider: "hub.go.kr".to_owned(),
        dataset_name: "building-register-main".to_owned(),
        base_uri: "https://www.hub.go.kr".to_owned(),
        terms_url: Some(
            "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do".to_owned(),
        ),
        operation: "building_register_main".to_owned(),
        provider_file_period: "2026-05".to_owned(),
        provider_file_id: "OPN209912310000000008".to_owned(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        live_write: Some("1".to_owned()),
    }
}

#[test]
fn select_collection_jobs_rejects_duplicate_partition_identities() -> TestResult {
    let jobs = vec![test_collection_job(), test_collection_job()];

    let error = select_collection_jobs(&jobs, None)
        .err()
        .ok_or("duplicate partition identities must be rejected")?;
    assert!(
        error.to_string().contains("duplicate partition identity"),
        "unexpected error: {error}"
    );
    Ok(())
}

fn test_collection_job() -> BuildingHubBulkCollectionPlanJob {
    BuildingHubBulkCollectionPlanJob {
        catalog_binding_status: "cataloged_endpoint".to_owned(),
        endpoint_slug: "hub-building-building_register_main".to_owned(),
        source_slug: "hubgokr__building_register_main".to_owned(),
        source_name: "hub.go.kr building-register main bulk".to_owned(),
        dataset_name: "building-register-main".to_owned(),
        base_uri: "https://www.hub.go.kr".to_owned(),
        terms_url: Some(
            "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do".to_owned(),
        ),
        operation: "building_register_main".to_owned(),
        provider_file_period: "2026-05".to_owned(),
        provider_file_id: "OPN209912310000000008".to_owned(),
        category_name: "building-register".to_owned(),
        service_name: "main".to_owned(),
        service_period_label: "2026-04".to_owned(),
        task_group_code: "03".to_owned(),
        task_code: "0303".to_owned(),
    }
}

fn test_run_id(raw: &str) -> anyhow::Result<IngestionRunId> {
    Ok(IngestionRunId::new(Uuid::parse_str(raw)?))
}

fn test_started_at() -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339("2026-06-04T00:00:00Z")?.to_utc())
}

fn test_file_stream(provider_file_name: &str, raw_payload: Vec<u8>) -> BuildingHubBulkFileStream {
    BuildingHubBulkFileStream::from_body_stream(
        "application/zip".to_owned(),
        provider_file_name.to_owned(),
        Some(raw_payload.len() as u64),
        stream::iter([Ok(Bytes::from(raw_payload))]).boxed(),
    )
}

fn existing_bronze_object(
    source_partition_key: &str,
    object_key: &str,
    size_bytes: u64,
) -> anyhow::Result<BronzeObject> {
    Ok(BronzeObject {
        id: BronzeObjectId::new(Uuid::parse_str("018f0000-0000-7000-8000-000000000401")?),
        source_catalog_id: SourceCatalogId::new(Uuid::parse_str(
            "018f0000-0000-7000-8000-000000000402",
        )?),
        ingestion_run_id: test_run_id("018f0000-0000-7000-8000-000000000403")?,
        source_record_id: None,
        source_partition_key: Some(source_partition_key.to_owned()),
        source_identity_key: "provider_file_id=OPN209912310000000008".to_owned(),
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
        provider_file_id: Some("OPN209912310000000008".to_owned()),
        provider_file_name: Some("building-register-main.zip".to_owned()),
        provider_updated_at: None,
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
    fn source(&self) -> Result<SourceCatalogEntry, CollectionError> {
        lock(&self.source, "source")?
            .clone()
            .ok_or_else(|| CollectionError::Infrastructure("source was not recorded".to_owned()))
    }

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
            "bulk-file Bronze ingest must not upsert schema profiles".to_owned(),
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
    failure_message: Option<String>,
}

impl RecordingObjectStorage {
    fn failing(message: &str) -> Self {
        Self {
            writes: Mutex::new(Vec::new()),
            streaming_writes: Mutex::new(Vec::new()),
            failure_message: Some(message.to_owned()),
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
}

#[async_trait]
impl ObjectStorageService for RecordingObjectStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        if let Some(message) = &self.failure_message {
            return Err(PublishError::Infrastructure(message.clone()));
        }
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
            });
        Ok(())
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        _key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        // The hub bulk lane tests exercise only the normal (key-absent) committer path; the 412
        // GET-rehash recovery is covered by the committer's streaming_tests, so this fake never
        // needs to surface an existing object.
        Ok(None)
    }
}
