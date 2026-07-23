use std::{
    collections::BTreeMap,
    io::{Cursor, Write},
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use chrono::Utc;
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_domain::CollectionError;
use collection_domain::{
    BronzeObject, IngestionRun, IngestionTrigger, SchemaProfile, SourceCatalogEntry,
    SourcePayloadFormat,
};
use foundation_outbox::{
    object_storage::{
        ObjectWriteMode, PutObjectRequest, StreamingObjectRehash, StreamingPutObjectRequest,
    },
    ObjectStorageService, ObjectStorageStreamingService, PublishError,
};
use foundation_shared_kernel::ids::SourceCatalogId;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};
use zip::{write::SimpleFileOptions, ZipWriter};

use super::{
    commit_staged_replay_to_bronze, stream_replay_request_directly_to_bronze,
    stream_replay_request_to_landing, validate_landing_payload, LandingPayload,
    ProviderAcquisitionBronzeCommitConfig, ProviderAcquisitionReplayRequest, StagedReplayObject,
};

#[test]
fn rejects_empty_landing_payload() {
    let payload = LandingPayload {
        object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        bytes: Vec::new(),
    };

    let error = validate_landing_payload(&payload).expect_err("empty payload");

    assert!(error.to_string().contains("empty"));
}

#[test]
fn rejects_provider_html_landing_payload() {
    let payload = LandingPayload {
        object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        bytes: b"<html><body>error</body></html>".to_vec(),
    };

    let error = validate_landing_payload(&payload).expect_err("html payload");

    assert!(error.to_string().contains("provider HTML"));
}

#[test]
fn accepts_zip_like_payload() {
    let payload = LandingPayload {
        object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        bytes: vec![0x50, 0x4b, 0x03, 0x04, 1, 2, 3],
    };

    validate_landing_payload(&payload).expect("valid zip-like payload");
}

#[tokio::test]
async fn streams_private_replay_request_to_landing_without_leaking_secrets() {
    let server = MockServer::start().await;
    let zip_bytes = zip_with_entry("data.csv", b"provider,data\n1,ok\n");
    Mock::given(method("POST"))
        .and(path("/vwDnMng/raonkupload/handler/raonkhandler.jsp"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .set_body_bytes(zip_bytes.clone()),
        )
        .mount(&server)
        .await;
    let storage = RecordingObjectStorage::default();
    let request = ProviderAcquisitionReplayRequest {
        schema_version: "foundation-platform.provider_acquisition_replay_request.v1".to_owned(),
        landing_object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        replay_url: format!(
            "{}/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            server.uri()
        ),
        method: "POST".to_owned(),
        request_content_type: "application/x-www-form-urlencoded".to_owned(),
        post_data: "k00=secret-payload".to_owned(),
        provider_declared_size_bytes: Some(zip_bytes.len() as u64),
        expected_size_bytes: None,
        cookie_header: Some("JSESSIONID=secret-cookie".to_owned()),
        user_agent: Some("foundation-platform-test".to_owned()),
        referer_url: Some(format!("{}/vwDnMng/test-page", server.uri())),
    };

    let report = stream_replay_request_to_landing(&storage, &request, &reqwest::Client::new())
        .await
        .expect("replay should stream to landing storage");

    let writes = storage.streaming_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].body, zip_bytes);
    assert_eq!(writes[0].key, request.landing_object_key);
    assert_eq!(writes[0].write_mode, ObjectWriteMode::CreateOnly);
    assert_eq!(writes[0].size_bytes, zip_bytes.len() as u64);
    assert_eq!(report.checksum_sha256, sha256_hex(&writes[0].body));
    assert_eq!(
        report.provider_declared_size_bytes,
        Some(zip_bytes.len() as u64)
    );
    assert_eq!(report.validation_status, "landed");
    let public_report = serde_json::to_string(&report).expect("serialize report");
    assert!(!public_report.contains("secret-payload"));
    assert!(!public_report.contains("secret-cookie"));
}

#[tokio::test]
async fn streams_provider_filedown_get_replay_request_to_landing() {
    let server = MockServer::start().await;
    let zip_bytes = zip_with_entry("data.csv", b"provider,data\n1,ok\n");
    Mock::given(method("GET"))
        .and(path("/vwDnMng/raonkupload/handler/raonkhandler.jsp"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .set_body_bytes(zip_bytes.clone()),
        )
        .mount(&server)
        .await;
    let storage = RecordingObjectStorage::default();
    let request = ProviderAcquisitionReplayRequest {
        schema_version: "foundation-platform.provider_acquisition_replay_request.v1".to_owned(),
        landing_object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        replay_url: format!(
            "{}/vwDnMng/raonkupload/handler/raonkhandler.jsp?k00=secret-payload",
            server.uri()
        ),
        method: "GET".to_owned(),
        request_content_type: String::new(),
        post_data: String::new(),
        provider_declared_size_bytes: Some(zip_bytes.len() as u64),
        expected_size_bytes: None,
        cookie_header: Some("JSESSIONID=secret-cookie".to_owned()),
        user_agent: Some("foundation-platform-test".to_owned()),
        referer_url: Some(format!("{}/dtmk/downloadDtnaResourceFile.do", server.uri())),
    };

    let report = stream_replay_request_to_landing(&storage, &request, &reqwest::Client::new())
        .await
        .expect("GET replay should stream to landing storage");

    let writes = storage.streaming_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].body, zip_bytes);
    assert_eq!(writes[0].key, request.landing_object_key);
    assert_eq!(report.size_bytes, zip_bytes.len() as u64);
}

#[tokio::test]
async fn rejects_replay_body_that_is_much_smaller_than_provider_declared_size() {
    let server = MockServer::start().await;
    let tiny_zip = zip_with_entry("data.csv", b"tiny");
    Mock::given(method("POST"))
        .and(path("/vwDnMng/raonkupload/handler/raonkhandler.jsp"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .set_body_bytes(tiny_zip),
        )
        .mount(&server)
        .await;
    let storage = RecordingObjectStorage::default();
    let request = ProviderAcquisitionReplayRequest {
        schema_version: "foundation-platform.provider_acquisition_replay_request.v1".to_owned(),
        landing_object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        replay_url: format!(
            "{}/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            server.uri()
        ),
        method: "POST".to_owned(),
        request_content_type: "application/x-www-form-urlencoded".to_owned(),
        post_data: "k00=secret-payload".to_owned(),
        provider_declared_size_bytes: Some(525_053_918),
        expected_size_bytes: None,
        cookie_header: None,
        user_agent: None,
        referer_url: None,
    };

    let error = stream_replay_request_to_landing(&storage, &request, &reqwest::Client::new())
        .await
        .expect_err("tiny RAON wrapper must not be landed as the provider file");

    assert!(
        error.to_string().contains("provider-declared size"),
        "error should name provider-declared size mismatch: {error}"
    );
    assert!(storage.streaming_writes().is_empty());
}

#[tokio::test]
async fn rejects_html_replay_response_before_landing_write() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/vwDnMng/raonkupload/handler/raonkhandler.jsp"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/html")
                .set_body_string("<html>expired</html>"),
        )
        .mount(&server)
        .await;
    let storage = RecordingObjectStorage::default();
    let request = ProviderAcquisitionReplayRequest {
        schema_version: "foundation-platform.provider_acquisition_replay_request.v1".to_owned(),
        landing_object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        replay_url: format!(
            "{}/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            server.uri()
        ),
        method: "POST".to_owned(),
        request_content_type: "application/x-www-form-urlencoded".to_owned(),
        post_data: "k00=secret-payload".to_owned(),
        provider_declared_size_bytes: Some(20),
        expected_size_bytes: None,
        cookie_header: None,
        user_agent: None,
        referer_url: None,
    };

    let error = stream_replay_request_to_landing(&storage, &request, &reqwest::Client::new())
        .await
        .expect_err("provider HTML must not be landed");

    assert!(
        error.to_string().contains("provider HTML"),
        "error should name provider HTML: {error}"
    );
    assert!(storage.streaming_writes().is_empty());
}

#[tokio::test]
async fn rejects_zip_replay_that_contains_provider_html_entry_without_size_hint() {
    let server = MockServer::start().await;
    let wrapper_zip = zip_with_entry(
        "SYNTHETIC_SINGLE_20991231.zip",
        b"<!DOCTYPE html><html></html>",
    );
    Mock::given(method("POST"))
        .and(path("/vwDnMng/raonkupload/handler/raonkhandler.jsp"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .set_body_bytes(wrapper_zip),
        )
        .mount(&server)
        .await;
    let storage = RecordingObjectStorage::default();
    let request = ProviderAcquisitionReplayRequest {
        schema_version: "foundation-platform.provider_acquisition_replay_request.v1".to_owned(),
        landing_object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        replay_url: format!(
            "{}/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            server.uri()
        ),
        method: "POST".to_owned(),
        request_content_type: "application/x-www-form-urlencoded".to_owned(),
        post_data: "k00=secret-payload".to_owned(),
        provider_declared_size_bytes: None,
        expected_size_bytes: None,
        cookie_header: None,
        user_agent: None,
        referer_url: None,
    };

    let error = stream_replay_request_to_landing(&storage, &request, &reqwest::Client::new())
        .await
        .expect_err("ZIP wrapper with provider HTML must not be landed");

    assert!(
        error.to_string().contains("provider HTML inside ZIP"),
        "error should name nested provider HTML: {error}"
    );
    assert!(storage.streaming_writes().is_empty());
}

#[test]
fn bronze_commit_config_requires_explicit_source_slug_when_enabled() {
    let lookup = lookup_from([]);

    let error = ProviderAcquisitionBronzeCommitConfig::from_lookup(true, &lookup)
        .expect_err("Bronze commit must not infer source_slug from a landing key");

    assert!(
        error
            .to_string()
            .contains("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_SLUG"),
        "error should name the missing explicit source slug: {error}"
    );
}

#[tokio::test]
async fn commits_staged_replay_to_bronze_from_explicit_metadata() {
    let temp_dir = std::env::temp_dir().join(format!("provider-acquisition-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .expect("create temp dir");
    let staged_path = temp_dir.join("replay.zip");
    let body = b"PK\x03\x04provider zip bytes".to_vec();
    tokio::fs::write(&staged_path, &body)
        .await
        .expect("write staged body");
    let staged = StagedReplayObject {
        path: staged_path.clone(),
        size_bytes: body.len() as u64,
        checksum_sha256: sha256_hex(&body),
    };
    let config = sample_bronze_commit_config();
    let storage = RecordingObjectStorage::default();
    let uow = RecordingBronzeUow::default();

    let outcome = commit_staged_replay_to_bronze(
        &storage,
        &uow,
        &config,
        &staged,
        "application/octet-stream",
        Utc::now(),
    )
    .await
    .expect("staged replay should commit through BronzeCommitter");

    assert_eq!(
        outcome.object_key,
        "bronze/source=vworldkr__parcel/20991231DS99991-9002.zip"
    );
    assert_eq!(outcome.size_bytes, body.len() as u64);
    assert_eq!(outcome.checksum_sha256, sha256_hex(&body));

    let writes = storage.streaming_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(
        writes[0].key,
        "bronze/source=vworldkr__parcel/20991231DS99991-9002.zip"
    );
    assert_eq!(writes[0].body, body);
    assert_eq!(writes[0].write_mode, ObjectWriteMode::CreateOnly);

    let recorded = uow.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].object_key.as_str(),
        "bronze/source=vworldkr__parcel/20991231DS99991-9002.zip"
    );
    assert_eq!(
        recorded[0].provider_file_id.as_deref(),
        Some("20991231DS99991-9002")
    );
    assert_eq!(recorded[0].provider_file_name.as_deref(), Some("9002.zip"));
    assert_eq!(recorded[0].checksum_sha256, sha256_hex(&writes[0].body));
    assert_eq!(recorded[0].size_bytes, body.len() as u64);
    assert_eq!(
        recorded[0].source_identity_key,
        "provider_file_id=20991231DS99991-9002"
    );
    assert_eq!(
        recorded[0].source_partition_key.as_deref(),
        Some("operation=parcel/provider_file_id=20991231DS99991-9002")
    );
    let sources = uow.sources();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].payload_format, SourcePayloadFormat::Zip);

    tokio::fs::remove_file(staged_path)
        .await
        .expect("remove staged body");
    tokio::fs::remove_dir(temp_dir)
        .await
        .expect("remove temp dir");
}

#[tokio::test]
async fn direct_bronze_replay_skips_landing_write_and_records_bronze() {
    let server = MockServer::start().await;
    let zip_bytes = zip_with_entry("data.csv", b"provider,data\n1,ok\n");
    Mock::given(method("POST"))
        .and(path("/vwDnMng/raonkupload/handler/raonkhandler.jsp"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .set_body_bytes(zip_bytes.clone()),
        )
        .mount(&server)
        .await;
    let storage = RecordingObjectStorage::default();
    let uow = RecordingBronzeUow::default();
    let config = sample_bronze_commit_config();
    let request = ProviderAcquisitionReplayRequest {
        schema_version: "foundation-platform.provider_acquisition_replay_request.v1".to_owned(),
        landing_object_key:
            "landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=job-001/file.zip"
                .to_owned(),
        replay_url: format!(
            "{}/vwDnMng/raonkupload/handler/raonkhandler.jsp",
            server.uri()
        ),
        method: "POST".to_owned(),
        request_content_type: "application/x-www-form-urlencoded".to_owned(),
        post_data: "k00=secret-payload".to_owned(),
        provider_declared_size_bytes: Some(zip_bytes.len() as u64),
        expected_size_bytes: None,
        cookie_header: Some("JSESSIONID=secret-cookie".to_owned()),
        user_agent: Some("foundation-platform-test".to_owned()),
        referer_url: Some(format!("{}/dtmk/downloadDtnaResourceFile.do", server.uri())),
    };

    let report = stream_replay_request_directly_to_bronze(
        &storage,
        &uow,
        &config,
        &request,
        &reqwest::Client::new(),
    )
    .await
    .expect("direct replay should commit to Bronze");

    let writes = storage.streaming_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(
        writes[0].key,
        "bronze/source=vworldkr__parcel/20991231DS99991-9002.zip"
    );
    assert_eq!(writes[0].write_mode, ObjectWriteMode::CreateOnly);
    assert_eq!(writes[0].body, zip_bytes);
    assert_eq!(report.validation_status, "committed_without_landing");
    assert_eq!(
        report.object_key,
        "bronze/source=vworldkr__parcel/20991231DS99991-9002.zip"
    );
    assert_eq!(
        report.bronze_object_key.as_deref(),
        Some("bronze/source=vworldkr__parcel/20991231DS99991-9002.zip")
    );
    assert_eq!(report.bronze_size_bytes, Some(zip_bytes.len() as u64));
    assert_eq!(
        report.bronze_checksum_sha256.as_deref(),
        Some(sha256_hex(&zip_bytes).as_str())
    );
}

#[derive(Default)]
struct RecordingObjectStorage {
    streaming_writes: Mutex<Vec<StreamingWriteRecord>>,
}

impl RecordingObjectStorage {
    fn streaming_writes(&self) -> Vec<StreamingWriteRecord> {
        lock(&self.streaming_writes, "streaming_writes")
            .expect("streaming writes lock")
            .clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamingWriteRecord {
    key: String,
    content_type: String,
    write_mode: ObjectWriteMode,
    body: Vec<u8>,
    size_bytes: u64,
}

#[async_trait]
impl ObjectStorageService for RecordingObjectStorage {
    async fn put_object(&self, _request: PutObjectRequest) -> Result<(), PublishError> {
        unreachable!("provider acquisition replay import must use streaming storage")
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
        let key = request.key;
        let content_type = request.content_type;
        let write_mode = request.write_mode;
        let size_bytes = request.size_bytes;
        let body = request
            .body
            .collect()
            .await
            .map_err(|error| {
                PublishError::Infrastructure(format!("streaming test body read failed: {error}"))
            })?
            .into_bytes()
            .to_vec();
        lock(&self.streaming_writes, "streaming_writes")?.push(StreamingWriteRecord {
            key,
            content_type,
            write_mode,
            body,
            size_bytes,
        });
        Ok(())
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        _key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        Ok(None)
    }
}

fn lock<'a, T>(mutex: &'a Mutex<T>, name: &'static str) -> Result<MutexGuard<'a, T>, PublishError> {
    mutex
        .lock()
        .map_err(|_| PublishError::Infrastructure(format!("{name} mutex poisoned")))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            use std::fmt::Write as _;

            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}

fn lookup_from<const N: usize>(
    entries: [(&'static str, &'static str); N],
) -> impl Fn(&str) -> anyhow::Result<Option<String>> {
    let values: BTreeMap<&'static str, &'static str> = entries.into_iter().collect();
    move |name| Ok(values.get(name).map(|value| (*value).to_owned()))
}

fn zip_with_entry(name: &str, bytes: &[u8]) -> Vec<u8> {
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = ZipWriter::new(&mut buffer);
        writer
            .start_file(name, SimpleFileOptions::default())
            .expect("start zip file");
        writer.write_all(bytes).expect("write zip entry");
        writer.finish().expect("finish zip");
    }
    buffer.into_inner()
}

fn sample_bronze_commit_config() -> ProviderAcquisitionBronzeCommitConfig {
    ProviderAcquisitionBronzeCommitConfig {
        source_slug: "vworldkr__parcel".to_owned(),
        source_name: "V-World parcel dataset file".to_owned(),
        provider: "VWorld".to_owned(),
        dataset_name: "parcel".to_owned(),
        base_uri: Some("https://www.vworld.kr".to_owned()),
        terms_url: Some("https://www.vworld.kr/dev/v4dv_2ddataguide_s002.do".to_owned()),
        operation: "parcel".to_owned(),
        provider_file_id: "20991231DS99991-9002".to_owned(),
        provider_file_name: "9002.zip".to_owned(),
        provider_file_period: None,
        provider_snapshot_date: None,
        provider_updated_at: None,
    }
}

#[derive(Default)]
struct RecordingBronzeUow {
    sources: Mutex<Vec<SourceCatalogEntry>>,
    recorded: Mutex<Vec<BronzeObject>>,
}

impl RecordingBronzeUow {
    fn sources(&self) -> Vec<SourceCatalogEntry> {
        self.sources.lock().expect("sources lock").clone()
    }

    fn recorded(&self) -> Vec<BronzeObject> {
        self.recorded.lock().expect("recorded lock").clone()
    }
}

#[async_trait]
impl BronzeIngestUnitOfWork for RecordingBronzeUow {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        self.sources
            .lock()
            .expect("sources lock")
            .push(entry.clone());
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
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        Ok(IngestionRun {
            id: command.id,
            source_catalog_id: SourceCatalogId::new(Uuid::new_v4()),
            trigger: IngestionTrigger::Manual,
            status: command.status,
            request_params: serde_json::json!({}),
            started_at: Utc::now(),
            finished_at: Some(command.finished_at),
            logical_records_seen: command.logical_records_seen,
            objects_written: command.objects_written,
            error_message: command.error_message,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
        })
    }

    async fn find_bronze_object_by_object_key(
        &self,
        _source_catalog_id: SourceCatalogId,
        _object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        Ok(None)
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        self.recorded
            .lock()
            .expect("recorded lock")
            .push(object.clone());
        Ok(object.clone())
    }

    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        Ok(profile.clone())
    }
}
