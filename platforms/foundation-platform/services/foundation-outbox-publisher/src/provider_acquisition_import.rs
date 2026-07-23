#[cfg(test)]
mod tests;

use std::{fs, io::Read as _, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::{
    plan_public_data_bulk_file_storage_location,
    ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand},
    public_data_bulk_file_request_params, BronzeCommitter, PlannedStreamingBronzeObject,
    PublicDataBulkFileIdentity, PublicDataBulkFileStorageLocationInput,
    StreamingBronzeCommitOutcome, StreamingBronzeRecord,
};
use collection_domain::{
    CollectionError, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_outbox::{
    object_storage::{ByteStream, ObjectWriteMode, StreamingPutObjectRequest},
    ObjectStorageStreamingService,
};
use futures_util::{stream, stream::BoxStream, StreamExt as _};
use reqwest::header::{CONTENT_TYPE, COOKIE, REFERER, USER_AGENT};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use uuid::Uuid;

use crate::bronze_object_storage::{
    bronze_streaming_object_storage_from_env, live_write_target_preflight,
};
use crate::bulk_streaming_bronze::BronzeStreamingObjectStorageWriter;
use crate::public_data_control_support::{
    optional_bool_env, optional_env_value, required_env_value,
};

const REPORT_SCHEMA_VERSION: &str = "foundation-platform.provider_acquisition_import_validation.v1";
const REPLAY_REPORT_SCHEMA_VERSION: &str =
    "foundation-platform.provider_acquisition_landing_replay.v1";
const REPLAY_REQUEST_SCHEMA_VERSION: &str =
    "foundation-platform.provider_acquisition_replay_request.v1";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/provider-acquisition-import-validation.json";
const LANDING_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const MIN_DECLARED_SIZE_FOR_RATIO_CHECK_BYTES: u64 = 1_048_576;
const MIN_REPLAY_TO_DECLARED_SIZE_RATIO_DENOMINATOR: u64 = 2;
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const COMMIT_BRONZE_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE";
const SOURCE_SLUG_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_SLUG";
const SOURCE_NAME_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_SOURCE_NAME";
const PROVIDER_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER";
const DATASET_NAME_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DATASET_NAME";
const BASE_URI_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BASE_URI";
const TERMS_URL_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_TERMS_URL";
const OPERATION_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_OPERATION";
const PROVIDER_FILE_ID_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_ID";
const PROVIDER_FILE_NAME_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_NAME";
const PROVIDER_FILE_PERIOD_ENV: &str =
    "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_FILE_PERIOD";
const PROVIDER_SNAPSHOT_DATE_ENV: &str =
    "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_SNAPSHOT_DATE";
const PROVIDER_UPDATED_AT_ENV: &str =
    "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PROVIDER_UPDATED_AT";
const RETIRED_LOCAL_ARTIFACT_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_LOCAL_ARTIFACT_PATH";
const DIRECT_TO_BRONZE_ENV: &str = "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_DIRECT_TO_BRONZE";

pub(crate) struct LandingPayload {
    pub(crate) object_key: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Serialize)]
struct LandingValidationReport {
    schema_version: &'static str,
    object_key: String,
    size_bytes: usize,
    checksum_sha256: String,
    validation_status: &'static str,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct ProviderAcquisitionReplayRequest {
    pub(crate) schema_version: String,
    pub(crate) landing_object_key: String,
    pub(crate) replay_url: String,
    pub(crate) method: String,
    pub(crate) request_content_type: String,
    pub(crate) post_data: String,
    #[serde(default)]
    pub(crate) provider_declared_size_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) expected_size_bytes: Option<u64>,
    pub(crate) cookie_header: Option<String>,
    pub(crate) user_agent: Option<String>,
    pub(crate) referer_url: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderAcquisitionBronzeCommitConfig {
    pub(crate) source_slug: String,
    pub(crate) source_name: String,
    pub(crate) provider: String,
    pub(crate) dataset_name: String,
    pub(crate) base_uri: Option<String>,
    pub(crate) terms_url: Option<String>,
    pub(crate) operation: String,
    pub(crate) provider_file_id: String,
    pub(crate) provider_file_name: String,
    pub(crate) provider_file_period: Option<String>,
    pub(crate) provider_snapshot_date: Option<NaiveDate>,
    pub(crate) provider_updated_at: Option<NaiveDate>,
}

impl ProviderAcquisitionBronzeCommitConfig {
    fn from_env() -> Result<Option<Self>> {
        let enabled = optional_bool_env(COMMIT_BRONZE_ENV)?.unwrap_or(false);
        Self::from_lookup(enabled, &|name| optional_env_value(name))
    }

    pub(crate) fn from_lookup<F>(enabled: bool, lookup: &F) -> Result<Option<Self>>
    where
        F: Fn(&str) -> Result<Option<String>>,
    {
        if !enabled {
            return Ok(None);
        }

        Ok(Some(Self {
            source_slug: required_lookup(lookup, SOURCE_SLUG_ENV)?,
            source_name: required_lookup(lookup, SOURCE_NAME_ENV)?,
            provider: required_lookup(lookup, PROVIDER_ENV)?,
            dataset_name: required_lookup(lookup, DATASET_NAME_ENV)?,
            base_uri: optional_lookup(lookup, BASE_URI_ENV)?,
            terms_url: optional_lookup(lookup, TERMS_URL_ENV)?,
            operation: required_lookup(lookup, OPERATION_ENV)?,
            provider_file_id: required_lookup(lookup, PROVIDER_FILE_ID_ENV)?,
            provider_file_name: required_lookup(lookup, PROVIDER_FILE_NAME_ENV)?,
            provider_file_period: optional_lookup(lookup, PROVIDER_FILE_PERIOD_ENV)?,
            provider_snapshot_date: optional_date_lookup(lookup, PROVIDER_SNAPSHOT_DATE_ENV)?,
            provider_updated_at: optional_date_lookup(lookup, PROVIDER_UPDATED_AT_ENV)?,
        }))
    }

    fn identity(&self) -> PublicDataBulkFileIdentity {
        PublicDataBulkFileIdentity {
            operation: self.operation.clone(),
            provider_file_period: self.provider_file_period.clone(),
            provider_snapshot_date: self.provider_snapshot_date,
            provider_file_id: self.provider_file_id.clone(),
            provider_file_name: self.provider_file_name.clone(),
            provider_updated_at: self.provider_updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct LandingReplayReport {
    schema_version: &'static str,
    pub(crate) object_key: String,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_sha256: String,
    pub(crate) provider_declared_size_bytes: Option<u64>,
    replay_status: u16,
    replay_content_type: String,
    pub(crate) validation_status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bronze_object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bronze_checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bronze_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bronze_object_id: Option<String>,
}

enum ProviderAcquisitionImportConfig {
    PayloadValidation {
        object_key: String,
        payload_path: PathBuf,
        output_path: PathBuf,
    },
    ReplayToLanding {
        replay_request_path: PathBuf,
        output_path: PathBuf,
    },
}

impl ProviderAcquisitionImportConfig {
    fn from_env() -> Result<Self> {
        let output_path =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_IMPORT_OUTPUT_PATH")?
                .map_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH), PathBuf::from);
        if optional_env_value(RETIRED_LOCAL_ARTIFACT_PATH_ENV)?.is_some() {
            return Err(anyhow!(
                "{RETIRED_LOCAL_ARTIFACT_PATH_ENV} is retired; use FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH"
            ));
        }
        if let Some(replay_request_path) =
            optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_REPLAY_REQUEST_PATH")?
        {
            return Ok(Self::ReplayToLanding {
                replay_request_path: PathBuf::from(replay_request_path),
                output_path,
            });
        }

        let object_key =
            required_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_LANDING_OBJECT_KEY")?;
        let payload_path = PathBuf::from(required_env_value(
            "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_LANDING_PAYLOAD_PATH",
        )?);

        Ok(Self::PayloadValidation {
            object_key,
            payload_path,
            output_path,
        })
    }
}

pub(crate) fn validate_landing_payload(payload: &LandingPayload) -> Result<()> {
    if !payload.object_key.starts_with("landing/") {
        return Err(anyhow!("landing object key must start with landing/"));
    }
    if payload.bytes.is_empty() {
        return Err(anyhow!("landing payload is empty"));
    }

    let leading = String::from_utf8_lossy(&payload.bytes[..payload.bytes.len().min(64)]);
    let leading = leading.trim_start().to_ascii_lowercase();
    if leading.starts_with("<html") || leading.starts_with("<!doctype") {
        return Err(anyhow!(
            "landing payload looks like provider HTML, not raw data"
        ));
    }

    if payload.object_key.to_ascii_lowercase().ends_with(".zip")
        && !payload.bytes.starts_with(&[0x50, 0x4b])
    {
        return Err(anyhow!("zip landing payload must start with PK signature"));
    }

    Ok(())
}

pub(crate) async fn run() -> Result<()> {
    let config = ProviderAcquisitionImportConfig::from_env()?;
    match config {
        ProviderAcquisitionImportConfig::PayloadValidation {
            object_key,
            payload_path,
            output_path,
        } => run_payload_validation(object_key, payload_path, output_path),
        ProviderAcquisitionImportConfig::ReplayToLanding {
            replay_request_path,
            output_path,
        } => run_replay_to_landing(replay_request_path, output_path).await,
    }
}

fn run_payload_validation(
    object_key: String,
    payload_path: PathBuf,
    output_path: PathBuf,
) -> Result<()> {
    let bytes = fs::read(&payload_path).with_context(|| {
        format!(
            "failed to read provider acquisition landing payload {}",
            payload_path.display()
        )
    })?;
    let payload = LandingPayload { object_key, bytes };
    validate_landing_payload(&payload)?;

    let report = LandingValidationReport {
        schema_version: REPORT_SCHEMA_VERSION,
        object_key: payload.object_key,
        size_bytes: payload.bytes.len(),
        checksum_sha256: sha256_hex(&payload.bytes),
        validation_status: "validated",
    };
    write_report(&output_path, &report)?;

    tracing::info!(
        output_path = %output_path.display(),
        size_bytes = report.size_bytes,
        checksum_sha256 = %report.checksum_sha256,
        "provider acquisition landing payload validated"
    );

    Ok(())
}

async fn run_replay_to_landing(replay_request_path: PathBuf, output_path: PathBuf) -> Result<()> {
    let request: ProviderAcquisitionReplayRequest =
        serde_json::from_slice(&fs::read(&replay_request_path).with_context(|| {
            format!(
                "failed to read provider acquisition replay request {}",
                replay_request_path.display()
            )
        })?)
        .with_context(|| {
            format!(
                "failed to parse provider acquisition replay request {}",
                replay_request_path.display()
            )
        })?;
    live_write_target_preflight()?;
    let bronze_config = ProviderAcquisitionBronzeCommitConfig::from_env()?;
    let bronze_uow = if bronze_config.is_some() {
        let database_url = required_env_value("DATABASE_URL").context(
            "DATABASE_URL is required when FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE=1",
        )?;
        let pool = PgPool::connect(&database_url)
            .await
            .context("failed to connect to DATABASE_URL for provider acquisition Bronze commit")?;
        Some(collection_infrastructure::PgBronzeIngestUnitOfWork::new(
            pool,
        ))
    } else {
        None
    };
    let storage = bronze_streaming_object_storage_from_env()
        .await
        .context("failed to create provider acquisition landing object storage")?;
    if optional_bool_env(DIRECT_TO_BRONZE_ENV)?.unwrap_or(false) {
        let config = bronze_config.as_ref().context(
            "FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_COMMIT_BRONZE=1 is required for direct-to-Bronze replay import",
        )?;
        let uow = bronze_uow
            .as_ref()
            .context("DATABASE_URL is required for direct-to-Bronze replay import")?;
        let report = stream_replay_request_directly_to_bronze(
            storage.as_ref(),
            uow,
            config,
            &request,
            &reqwest::Client::new(),
        )
        .await?;
        write_report(&output_path, &report)?;
        tracing::info!(
            output_path = %output_path.display(),
            size_bytes = report.size_bytes,
            checksum_sha256 = %report.checksum_sha256,
            "provider acquisition replay committed directly to Bronze"
        );
        return Ok(());
    }
    let mut staged_report =
        fetch_stage_and_put_replay_to_landing(storage.as_ref(), &request, &reqwest::Client::new())
            .await?;
    let bronze_result =
        if let (Some(config), Some(uow)) = (bronze_config.as_ref(), bronze_uow.as_ref()) {
            let staged = staged_report
                .staged
                .as_ref()
                .context("provider acquisition Bronze commit requires the staged replay file")?;
            Some(
                commit_staged_replay_to_bronze(
                    storage.as_ref(),
                    uow,
                    config,
                    staged,
                    &staged_report.replay_content_type,
                    Utc::now(),
                )
                .await,
            )
        } else {
            None
        };
    cleanup_staged_replay(staged_report.staged.as_ref()).await;
    if let Some(result) = bronze_result {
        let outcome = result?;
        staged_report.public.bronze_object_key = Some(outcome.object_key);
        staged_report.public.bronze_checksum_sha256 = Some(outcome.checksum_sha256);
        staged_report.public.bronze_size_bytes = Some(outcome.size_bytes);
        staged_report.public.bronze_object_id = Some(outcome.bronze_object_id.to_string());
        staged_report.public.validation_status = "landed_and_committed";
    }
    let report = staged_report.public;
    write_report(&output_path, &report)?;

    tracing::info!(
        output_path = %output_path.display(),
        size_bytes = report.size_bytes,
        checksum_sha256 = %report.checksum_sha256,
        "provider acquisition replay streamed to landing storage"
    );

    Ok(())
}

#[cfg(test)]
pub(crate) async fn stream_replay_request_to_landing<Storage>(
    storage: &Storage,
    request: &ProviderAcquisitionReplayRequest,
    client: &reqwest::Client,
) -> Result<LandingReplayReport>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let staged_report = fetch_stage_and_put_replay_to_landing(storage, request, client).await?;
    cleanup_staged_replay(staged_report.staged.as_ref()).await;
    Ok(staged_report.public)
}

pub(crate) async fn stream_replay_request_directly_to_bronze<Storage, Uow>(
    storage: &Storage,
    uow: &Uow,
    config: &ProviderAcquisitionBronzeCommitConfig,
    request: &ProviderAcquisitionReplayRequest,
    client: &reqwest::Client,
) -> Result<LandingReplayReport>
where
    Storage: ObjectStorageStreamingService + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let mut staged_report = fetch_and_stage_replay_response(request, client).await?;
    let staged = staged_report
        .staged
        .as_ref()
        .context("provider acquisition direct Bronze commit requires the staged replay file")?;
    let bronze_result = commit_staged_replay_to_bronze(
        storage,
        uow,
        config,
        staged,
        &staged_report.replay_content_type,
        Utc::now(),
    )
    .await;
    cleanup_staged_replay(staged_report.staged.as_ref()).await;
    let outcome = bronze_result?;
    staged_report.public.object_key = outcome.object_key.clone();
    staged_report.public.bronze_object_key = Some(outcome.object_key);
    staged_report.public.bronze_checksum_sha256 = Some(outcome.checksum_sha256);
    staged_report.public.bronze_size_bytes = Some(outcome.size_bytes);
    staged_report.public.bronze_object_id = Some(outcome.bronze_object_id.to_string());
    staged_report.public.validation_status = "committed_without_landing";
    Ok(staged_report.public)
}

async fn fetch_stage_and_put_replay_to_landing<Storage>(
    storage: &Storage,
    request: &ProviderAcquisitionReplayRequest,
    client: &reqwest::Client,
) -> Result<StagedLandingReplayReport>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let staged_report = fetch_and_stage_replay_response(request, client).await?;
    let staged = staged_report
        .staged
        .as_ref()
        .context("provider acquisition landing write requires the staged replay file")?;
    let byte_stream = ByteStream::read_from()
        .path(staged.path.clone())
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to open provider acquisition staging file {}",
                staged.path.display()
            )
        })?;

    storage
        .put_streaming_object(StreamingPutObjectRequest {
            key: request.landing_object_key.clone(),
            content_type: staged_report.replay_content_type.clone(),
            cache_control: LANDING_CACHE_CONTROL.to_owned(),
            size_bytes: staged.size_bytes,
            body: byte_stream,
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await
        .with_context(|| {
            format!(
                "failed to stream provider acquisition replay to {}",
                request.landing_object_key
            )
        })?;

    Ok(staged_report)
}

async fn fetch_and_stage_replay_response(
    request: &ProviderAcquisitionReplayRequest,
    client: &reqwest::Client,
) -> Result<StagedLandingReplayReport> {
    validate_replay_request(request)?;
    let mut replay = if request.method.eq_ignore_ascii_case("GET") {
        client.get(&request.replay_url)
    } else {
        client
            .post(&request.replay_url)
            .header(CONTENT_TYPE, request.request_content_type.clone())
            .body(request.post_data.clone())
    };
    if let Some(cookie_header) = request.cookie_header.as_deref() {
        replay = replay.header(COOKIE, cookie_header);
    }
    if let Some(user_agent) = request.user_agent.as_deref() {
        replay = replay.header(USER_AGENT, user_agent);
    }
    if let Some(referer_url) = request.referer_url.as_deref() {
        replay = replay.header(REFERER, referer_url);
    }

    let response = replay
        .send()
        .await
        .context("provider acquisition replay request failed")?;
    let status = response.status();
    let replay_status = status.as_u16();
    if !status.is_success() {
        return Err(anyhow!(
            "provider acquisition replay returned non-success status {replay_status}"
        ));
    }
    let replay_content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let source_body = response
        .bytes_stream()
        .map(|chunk| chunk.map_err(anyhow::Error::from))
        .boxed();
    let staged = stage_replay_response(request, &replay_content_type, source_body).await?;
    if let Err(error) = validate_staged_replay_payload(request, &replay_content_type, &staged).await
    {
        cleanup_staged_replay(Some(&staged)).await;
        return Err(error);
    }
    if let Err(error) = validate_staged_replay_size(request, &staged) {
        cleanup_staged_replay(Some(&staged)).await;
        return Err(error);
    }

    Ok(StagedLandingReplayReport {
        public: LandingReplayReport {
            schema_version: REPLAY_REPORT_SCHEMA_VERSION,
            object_key: request.landing_object_key.clone(),
            size_bytes: staged.size_bytes,
            checksum_sha256: staged.checksum_sha256.clone(),
            provider_declared_size_bytes: request.provider_size_hint(),
            replay_status,
            replay_content_type: replay_content_type.clone(),
            validation_status: "landed",
            bronze_object_key: None,
            bronze_checksum_sha256: None,
            bronze_size_bytes: None,
            bronze_object_id: None,
        },
        replay_content_type,
        staged: Some(staged),
    })
}

async fn cleanup_staged_replay(staged: Option<&StagedReplayObject>) {
    if let Some(staged) = staged {
        if let Err(cleanup_error) = tokio::fs::remove_file(&staged.path).await {
            tracing::warn!(
                path = %staged.path.display(),
                error = %cleanup_error,
                "failed to remove provider acquisition staging file"
            );
        }
    }
}

struct StagedLandingReplayReport {
    public: LandingReplayReport,
    replay_content_type: String,
    staged: Option<StagedReplayObject>,
}

pub(crate) async fn commit_staged_replay_to_bronze<Storage, Uow>(
    storage: &Storage,
    uow: &Uow,
    config: &ProviderAcquisitionBronzeCommitConfig,
    staged: &StagedReplayObject,
    content_type: &str,
    started_at: DateTime<Utc>,
) -> Result<StreamingBronzeCommitOutcome>
where
    Storage: ObjectStorageStreamingService + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(config, content_type, started_at))
        .await
        .context("failed to upsert provider acquisition source catalog entry")?;
    let run_id = foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4());
    let identity = config.identity();
    let location =
        plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
            source_slug: &config.source_slug,
            ingest_date: started_at.date_naive(),
            ingestion_run_id: run_id,
            identity: identity.clone(),
        })
        .context("failed to plan provider acquisition Bronze object location")?;
    let run = uow
        .create_ingestion_run(&ingestion_run(
            source.id,
            run_id,
            started_at,
            initial_bronze_request_params(config, &identity, location.object_key.as_str()),
        ))
        .await
        .context("failed to create provider acquisition ingestion run")?;

    let body = staged_file_body_stream(staged.path.clone()).await?;
    let writer = BronzeStreamingObjectStorageWriter::new(storage, content_type.to_owned(), body);
    let planned = PlannedStreamingBronzeObject {
        cache_control: BRONZE_CACHE_CONTROL.to_owned(),
        expected_size_bytes: staged.size_bytes,
        record: StreamingBronzeRecord {
            object_key: location.object_key.clone(),
            content_type: content_type.to_owned(),
            source_catalog_id: source.id,
            ingestion_run_id: run.id,
            source_partition_key: location.source_partition_key.clone(),
            source_identity_key: location.source_identity_key.clone(),
            dedupe_key_prefix: format!("{}:{}", config.source_slug, location.source_identity_key),
            request_params: public_data_bulk_file_request_params(&identity),
            snapshot_period: location.snapshot_period,
            snapshot_date: location.snapshot_date,
            snapshot_granularity: location.snapshot_granularity,
            snapshot_basis: location.snapshot_basis,
            provider_file_id: Some(identity.provider_file_id),
            provider_file_name: Some(identity.provider_file_name),
            provider_updated_at: identity.provider_updated_at,
            collected_at: started_at,
        },
    };

    let outcome = match BRONZE_COMMITTER
        .commit_streaming_bulk(&writer, uow, planned)
        .await
        .context("failed to commit provider acquisition replay to Bronze")
    {
        Ok(outcome) => outcome,
        Err(error) => return Err(mark_run_failed_after_error(uow, run.id, 0, error).await),
    };

    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: 0,
        objects_written: 1,
        error_message: None,
    })
    .await
    .context("failed to complete provider acquisition ingestion run")?;

    Ok(outcome)
}

async fn staged_file_body_stream(
    path: PathBuf,
) -> Result<BoxStream<'static, Result<Bytes, CollectionError>>> {
    let file = tokio::fs::File::open(&path).await.with_context(|| {
        format!(
            "failed to open provider acquisition staged file {}",
            path.display()
        )
    })?;
    Ok(stream::try_unfold(file, |mut file| async move {
        let mut buffer = vec![0_u8; 64 * 1024];
        let read = file.read(&mut buffer).await.map_err(|error| {
            CollectionError::Infrastructure(format!(
                "failed to read provider acquisition staged file: {error}"
            ))
        })?;
        if read == 0 {
            return Ok(None);
        }
        buffer.truncate(read);
        Ok(Some((Bytes::from(buffer), file)))
    })
    .boxed())
}

fn source_catalog_entry(
    config: &ProviderAcquisitionBronzeCommitConfig,
    content_type: &str,
    now: DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: foundation_shared_kernel::ids::SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: config.source_name.clone(),
        provider: config.provider.clone(),
        dataset_name: config.dataset_name.clone(),
        base_url: config.base_uri.clone(),
        auth_kind: SourceAuthKind::Manual,
        payload_format: payload_format_from_replay(content_type, &config.provider_file_name),
        license_name: None,
        license_url: None,
        terms_url: config.terms_url.clone(),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

const fn ingestion_run(
    source_catalog_id: foundation_shared_kernel::ids::SourceCatalogId,
    run_id: foundation_shared_kernel::ids::IngestionRunId,
    now: DateTime<Utc>,
    request_params: JsonValue,
) -> IngestionRun {
    IngestionRun {
        id: run_id,
        source_catalog_id,
        trigger: IngestionTrigger::Manual,
        status: IngestionRunStatus::Running,
        request_params,
        started_at: now,
        finished_at: None,
        logical_records_seen: 0,
        objects_written: 0,
        error_message: None,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

fn initial_bronze_request_params(
    config: &ProviderAcquisitionBronzeCommitConfig,
    identity: &PublicDataBulkFileIdentity,
    object_key: &str,
) -> JsonValue {
    json!({
        "sourceAcquisitionLane": "provider_acquisition_replay",
        "operation": identity.operation,
        "providerFileId": identity.provider_file_id,
        "providerFileName": identity.provider_file_name,
        "sourceSlug": config.source_slug,
        "objectKey": object_key,
    })
}

async fn mark_run_failed_after_error<Uow>(
    uow: &Uow,
    run_id: foundation_shared_kernel::ids::IngestionRunId,
    objects_written: u64,
    error: anyhow::Error,
) -> anyhow::Error
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let failure_message = truncate_failure_message(&format!("{error:#}"));
    let failure_result = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run_id,
            status: IngestionRunStatus::Failed,
            finished_at: Utc::now(),
            logical_records_seen: 0,
            objects_written,
            error_message: Some(failure_message),
        })
        .await;
    match failure_result {
        Ok(_) => error,
        Err(failure_error) => error.context(format!(
            "also failed to mark provider acquisition ingestion run {run_id} as failed: {failure_error}"
        )),
    }
}

fn truncate_failure_message(message: &str) -> String {
    const LIMIT: usize = 1_000;
    if message.len() <= LIMIT {
        return message.to_owned();
    }
    format!("{}...", &message[..LIMIT])
}

fn payload_format_from_replay(content_type: &str, provider_file_name: &str) -> SourcePayloadFormat {
    let normalized = content_type.to_ascii_lowercase();
    let normalized_name = provider_file_name.to_ascii_lowercase();
    if normalized.contains("zip") || normalized_name.ends_with(".zip") {
        SourcePayloadFormat::Zip
    } else if normalized.contains("json") {
        SourcePayloadFormat::Json
    } else if normalized.contains("xml") {
        SourcePayloadFormat::Xml
    } else if normalized.contains("csv") {
        SourcePayloadFormat::Csv
    } else {
        SourcePayloadFormat::Binary
    }
}

fn required_lookup<F>(lookup: &F, name: &'static str) -> Result<String>
where
    F: Fn(&str) -> Result<Option<String>>,
{
    optional_lookup(lookup, name)?.with_context(|| format!("{name} is required"))
}

fn optional_lookup<F>(lookup: &F, name: &'static str) -> Result<Option<String>>
where
    F: Fn(&str) -> Result<Option<String>>,
{
    lookup(name).with_context(|| format!("failed to read {name}"))
}

fn optional_date_lookup<F>(lookup: &F, name: &'static str) -> Result<Option<NaiveDate>>
where
    F: Fn(&str) -> Result<Option<String>>,
{
    optional_lookup(lookup, name)?
        .map(|value| {
            NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                .with_context(|| format!("{name} must use YYYY-MM-DD"))
        })
        .transpose()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            use std::fmt::Write as _;

            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}

fn validate_replay_request(request: &ProviderAcquisitionReplayRequest) -> Result<()> {
    if request.schema_version != REPLAY_REQUEST_SCHEMA_VERSION {
        return Err(anyhow!(
            "provider acquisition replay request schema_version must be {REPLAY_REQUEST_SCHEMA_VERSION}"
        ));
    }
    if !request.landing_object_key.starts_with("landing/") {
        return Err(anyhow!("landing object key must start with landing/"));
    }
    let method = request.method.to_ascii_uppercase();
    if method != "POST" && method != "GET" {
        return Err(anyhow!(
            "provider acquisition replay method must be GET or POST"
        ));
    }
    if request.replay_url.trim().is_empty() {
        return Err(anyhow!("provider acquisition replay_url must not be empty"));
    }
    if method == "POST" {
        if !request
            .request_content_type
            .to_ascii_lowercase()
            .contains("application/x-www-form-urlencoded")
        {
            return Err(anyhow!(
                "provider acquisition replay request_content_type must be application/x-www-form-urlencoded for POST"
            ));
        }
        if request.post_data.trim().is_empty() {
            return Err(anyhow!(
                "provider acquisition replay post_data must not be empty for POST"
            ));
        }
    }
    if request
        .provider_size_hint()
        .is_some_and(|size_bytes| size_bytes == 0)
    {
        return Err(anyhow!(
            "provider acquisition replay provider_declared_size_bytes must be positive when present"
        ));
    }
    Ok(())
}

fn validate_first_replay_chunk(
    request: &ProviderAcquisitionReplayRequest,
    content_type: &str,
    first_chunk: &[u8],
) -> Result<()> {
    if is_html_payload(content_type, first_chunk) {
        return Err(anyhow!(
            "provider acquisition replay for {} returned provider HTML, not raw data",
            request.landing_object_key
        ));
    }
    if request
        .landing_object_key
        .to_ascii_lowercase()
        .ends_with(".zip")
        && !first_chunk.starts_with(&[0x50, 0x4b])
    {
        return Err(anyhow!(
            "zip landing replay for {} must start with PK signature",
            request.landing_object_key
        ));
    }
    Ok(())
}

fn validate_staged_replay_size(
    request: &ProviderAcquisitionReplayRequest,
    staged: &StagedReplayObject,
) -> Result<()> {
    let Some(provider_declared_size) = request.provider_size_hint() else {
        return Ok(());
    };
    if provider_declared_size < MIN_DECLARED_SIZE_FOR_RATIO_CHECK_BYTES {
        return Ok(());
    }
    if staged
        .size_bytes
        .saturating_mul(MIN_REPLAY_TO_DECLARED_SIZE_RATIO_DENOMINATOR)
        < provider_declared_size
    {
        return Err(anyhow!(
            "provider acquisition replay for {} is much smaller than provider-declared size: \
             actual_size_bytes={} provider_declared_size_bytes={}",
            request.landing_object_key,
            staged.size_bytes,
            provider_declared_size
        ));
    }
    Ok(())
}

async fn validate_staged_replay_payload(
    request: &ProviderAcquisitionReplayRequest,
    content_type: &str,
    staged: &StagedReplayObject,
) -> Result<()> {
    if !is_zip_replay(&request.landing_object_key, content_type) {
        return Ok(());
    }

    let object_key = request.landing_object_key.clone();
    let path = staged.path.clone();
    tokio::task::spawn_blocking(move || {
        validate_zip_entries_do_not_start_with_html(&object_key, path)
    })
    .await
    .context("failed to join provider acquisition ZIP validation task")?
}

fn is_zip_replay(object_key: &str, content_type: &str) -> bool {
    object_key.to_ascii_lowercase().ends_with(".zip")
        || content_type.to_ascii_lowercase().contains("zip")
}

fn validate_zip_entries_do_not_start_with_html(object_key: &str, path: PathBuf) -> Result<()> {
    let file = fs::File::open(&path).with_context(|| {
        format!(
            "failed to open provider acquisition ZIP staging file {}",
            path.display()
        )
    })?;
    let mut archive = zip::ZipArchive::new(file).with_context(|| {
        format!("provider acquisition replay for {object_key} was not a readable ZIP")
    })?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).with_context(|| {
            format!("failed to inspect provider acquisition ZIP entry {index} for {object_key}")
        })?;
        if entry.is_dir() {
            continue;
        }
        let entry_name = entry.name().to_owned();
        let mut prefix = [0_u8; 512];
        let read = entry.read(&mut prefix).with_context(|| {
            format!("failed to read provider acquisition ZIP entry {entry_name} for {object_key}")
        })?;
        if starts_with_html_marker(&prefix[..read]) {
            return Err(anyhow!(
                "provider acquisition replay for {object_key} contains provider HTML inside ZIP entry {entry_name}"
            ));
        }
    }
    Ok(())
}

fn starts_with_html_marker(bytes: &[u8]) -> bool {
    let trimmed = bytes
        .iter()
        .copied()
        .skip_while(|byte| byte.is_ascii_whitespace())
        .take(16)
        .collect::<Vec<_>>();
    let prefix = String::from_utf8_lossy(&trimmed).to_ascii_lowercase();
    prefix.starts_with("<html") || prefix.starts_with("<!doctype")
}

impl ProviderAcquisitionReplayRequest {
    fn provider_size_hint(&self) -> Option<u64> {
        self.provider_declared_size_bytes
            .or(self.expected_size_bytes)
    }
}

pub(crate) struct StagedReplayObject {
    pub(crate) path: PathBuf,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_sha256: String,
}

async fn stage_replay_response(
    request: &ProviderAcquisitionReplayRequest,
    content_type: &str,
    mut source_body: BoxStream<'static, Result<Bytes>>,
) -> Result<StagedReplayObject> {
    let first_chunk = source_body
        .next()
        .await
        .transpose()
        .context("failed to read first provider acquisition replay chunk")?
        .with_context(|| {
            format!(
                "provider acquisition replay for {} returned an empty body",
                request.landing_object_key
            )
        })?;
    validate_first_replay_chunk(request, content_type, &first_chunk)?;

    let staging_dir = provider_acquisition_staging_dir();
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create provider acquisition staging directory {}",
                staging_dir.display()
            )
        })?;
    let staging_path = staging_dir.join(format!("{}.bin", Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&staging_path)
        .await
        .with_context(|| {
            format!(
                "failed to create provider acquisition staging file {}",
                staging_path.display()
            )
        })?;
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;

    write_replay_chunk(&mut file, &mut hasher, &mut size_bytes, &first_chunk).await?;
    while let Some(chunk) = source_body.next().await {
        let chunk = chunk.context("failed to read provider acquisition replay chunk")?;
        write_replay_chunk(&mut file, &mut hasher, &mut size_bytes, &chunk).await?;
    }
    file.flush()
        .await
        .context("failed to flush provider acquisition staging file")?;
    drop(file);
    if size_bytes == 0 {
        return Err(anyhow!(
            "provider acquisition replay for {} returned an empty body",
            request.landing_object_key
        ));
    }

    Ok(StagedReplayObject {
        path: staging_path,
        size_bytes,
        checksum_sha256: bytes_to_hex(&hasher.finalize()),
    })
}

async fn write_replay_chunk(
    file: &mut tokio::fs::File,
    hasher: &mut Sha256,
    size_bytes: &mut u64,
    chunk: &[u8],
) -> Result<()> {
    file.write_all(chunk)
        .await
        .context("failed to write provider acquisition replay staging chunk")?;
    hasher.update(chunk);
    *size_bytes = size_bytes
        .checked_add(u64::try_from(chunk.len()).context("provider chunk length overflowed u64")?)
        .ok_or_else(|| anyhow!("provider acquisition replay stream length overflowed u64"))?;
    Ok(())
}

fn provider_acquisition_staging_dir() -> PathBuf {
    optional_env_value("FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_STAGING_DIR")
        .ok()
        .flatten()
        .map_or_else(
            || PathBuf::from("target/provider-acquisition-staging"),
            PathBuf::from,
        )
}

fn is_html_payload(content_type: &str, first_chunk: &[u8]) -> bool {
    let normalized_content_type = content_type.to_ascii_lowercase();
    normalized_content_type.contains("text/html")
        || normalized_content_type.contains("application/xhtml")
        || first_chunk
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'<')
}

fn write_report<T>(path: &PathBuf, report: &T) -> Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create provider acquisition import report directory {}",
                parent.display()
            )
        })?;
    }
    fs::write(path, serde_json::to_vec_pretty(report)?).with_context(|| {
        format!(
            "failed to write provider acquisition import report {}",
            path.display()
        )
    })
}
