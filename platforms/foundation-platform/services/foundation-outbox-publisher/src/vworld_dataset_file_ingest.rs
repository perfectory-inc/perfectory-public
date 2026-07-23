use std::{env, fs, path::PathBuf};

use anyhow::{bail, Context};
use chrono::{NaiveDate, Utc};
use collection_application::ports::{
    BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand,
};
use collection_application::{
    plan_public_data_bulk_file_storage_location, public_data_bulk_file_request_params,
    public_data_bulk_file_source_partition_key, BronzeCommitter, PlannedStreamingBronzeObject,
    PublicDataBulkFileIdentity, PublicDataBulkFileSourcePartitionKeyInput,
    PublicDataBulkFileStorageLocationInput, PublicDataBulkFileStorageLocationPlan,
    StreamingBronzeRecord,
};
use collection_domain::{
    CollectionError, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::{
    PgBronzeIngestRepository, PgBronzeIngestUnitOfWork, VWorldDatasetFileClient,
    VWorldDatasetFileConfig, VWorldDatasetFileDownloadRequest, VWorldDatasetFileInventoryItem,
    VWorldDatasetFileKind, VWorldDatasetFileStream, VWorldDatasetLoginClient,
    VWorldDatasetLoginConfig,
};
use foundation_outbox::ObjectStorageStreamingService;
use foundation_shared_kernel::ids::IngestionRunId;
use foundation_shared_kernel::ids::SourceCatalogId;
use futures_util::{stream, StreamExt};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::bronze_streaming_object_storage_from_env;
use crate::bulk_streaming_bronze::BronzeStreamingObjectStorageWriter;
use crate::public_data_control_support::{optional_env_value, required_env_value};

const DEFAULT_FILE_INVENTORY_PATH: &str = "target/audit/vworld-dataset-file-inventory.json";
const DEFAULT_EVIDENCE_PATH: &str = "target/audit/vworld-dataset-file-ingest-evidence.json";
const DEFAULT_USER_AGENT: &str = "foundation-platform-vworld-dataset-file-ingestor/1.0";
const DEFAULT_PAGE_SIZE: u64 = 100;
const EVIDENCE_SCHEMA_VERSION: &str = "foundation-platform.vworld_dataset_file_ingest_evidence.v1";

pub async fn run() -> anyhow::Result<()> {
    let mut config = VWorldDatasetFileIngestConfig::from_env()?;
    let inventory = read_file_inventory(&config.file_inventory_path)?;
    let selected_files = select_inventory_files(
        &inventory.jobs,
        config.max_jobs,
        config.max_files,
        config.exclude_selection_archives,
    )?;
    if selected_files.len()
        == eligible_inventory_file_count(&inventory.jobs, config.exclude_selection_archives)
        && !config.full_download_confirmed
    {
        bail!(
            "full VWorld dataset file ingest requires FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1"
        );
    }
    config.cookie_header = resolve_vworld_dataset_cookie_header(&config, &selected_files).await?;

    let live_write = live_write_enabled(config.live_write.as_deref());
    let mut indexed_reports = if live_write {
        // Fail fast (and log the resolved target) before any provider file body streams to the
        // first put, instead of discovering a misconfigured R2 target mid-download.
        crate::bronze_object_storage::live_write_target_preflight()
            .context("VWorld dataset file ingest live-write target preflight failed")?;
        let pool = PgPool::connect(&env::var("DATABASE_URL").context("DATABASE_URL is required")?)
            .await
            .context("failed to connect to database for VWorld dataset file ingest")?;
        let repo = PgBronzeIngestRepository::new(pool.clone());
        let uow = PgBronzeIngestUnitOfWork::new(pool);
        let storage = bronze_streaming_object_storage_from_env()
            .await
            .context("failed to configure object storage for VWorld dataset file ingest")?;
        stream::iter(selected_files.into_iter().enumerate())
            .map(|(index, selected)| {
                let config = config.clone();
                let repo = &repo;
                let uow = &uow;
                let storage = storage.as_ref();
                async move {
                    let SelectedVWorldDatasetFile { job, file } = selected;
                    let started_at = Utc::now();
                    let report =
                        match ingest_file_with_adapters(&job, &file, &config, repo, uow, storage)
                            .await
                        {
                            Ok(report) => report,
                            Err(error) => failed_file_report(&job, &file, started_at, error),
                        };
                    (index, report)
                }
            })
            .buffer_unordered(config.max_in_flight)
            .collect::<Vec<_>>()
            .await
    } else {
        stream::iter(selected_files.into_iter().enumerate())
            .map(|(index, selected)| {
                let config = config.clone();
                async move {
                    let SelectedVWorldDatasetFile { job, file } = selected;
                    let started_at = Utc::now();
                    let report = match ingest_file(&job, &file, &config).await {
                        Ok(report) => report,
                        Err(error) => failed_file_report(&job, &file, started_at, error),
                    };
                    (index, report)
                }
            })
            .buffer_unordered(config.max_in_flight)
            .collect::<Vec<_>>()
            .await
    };
    indexed_reports.sort_by_key(|(index, _)| *index);
    let reports = indexed_reports
        .into_iter()
        .map(|(_, report)| report)
        .collect::<Vec<_>>();

    let provider_acquisition_blocked_file_count = reports
        .iter()
        .filter(|report| report.status == "provider_acquisition_blocked")
        .count() as u64;
    let failed_file_count = reports
        .iter()
        .filter(|report| report.status == "failed")
        .count() as u64;
    let ingest_status = vworld_dataset_file_ingest_status(
        failed_file_count,
        provider_acquisition_blocked_file_count,
        config.defer_provider_acquisition_blocked,
    );
    let evidence = VWorldDatasetFileIngestEvidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        status: ingest_status.evidence_status,
        file_inventory_path: config
            .file_inventory_path
            .to_string_lossy()
            .replace('\\', "/"),
        selected_file_count: reports.len() as u64,
        max_in_flight: config.max_in_flight,
        succeeded_file_count: reports
            .iter()
            .filter(|report| report.status == "succeeded")
            .count() as u64,
        skipped_file_count: reports
            .iter()
            .filter(|report| report.status == "skipped_existing")
            .count() as u64,
        provider_acquisition_blocked_file_count,
        failed_file_count,
        live_write_enabled: live_write,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        files: reports,
    };
    write_evidence(&config.evidence_path, &evidence)?;
    if ingest_status.should_bail {
        bail!(
            "VWorld dataset file ingest blocked selected_files={} failed={} provider_acquisition_blocked={} report={}",
            evidence.selected_file_count,
            evidence.failed_file_count,
            evidence.provider_acquisition_blocked_file_count,
            config.evidence_path.display()
        );
    }
    Ok(())
}

#[derive(Clone, Eq, PartialEq)]
struct VWorldDatasetFileIngestConfig {
    file_inventory_path: PathBuf,
    evidence_path: PathBuf,
    user_agent: String,
    page_size: u64,
    cookie_header: Option<String>,
    username: Option<String>,
    password: Option<String>,
    live_write: Option<String>,
    max_jobs: Option<usize>,
    max_files: Option<usize>,
    max_in_flight: usize,
    full_download_confirmed: bool,
    exclude_selection_archives: bool,
    defer_provider_acquisition_blocked: bool,
}

impl VWorldDatasetFileIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            file_inventory_path: optional_env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH",
            )?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_FILE_INVENTORY_PATH)),
            evidence_path: optional_env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH",
            )?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_EVIDENCE_PATH)),
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            page_size: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE")?
                .map(|value| {
                    parse_positive_u64("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_PAGE_SIZE", &value)
                })
                .transpose()?
                .unwrap_or(DEFAULT_PAGE_SIZE),
            cookie_header: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER")?,
            username: optional_env_value_any(&[
                "FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME",
                "VWORLD_USERNAME",
            ])?,
            password: optional_env_value_any(&[
                "FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD",
                "VWORLD_PASSWORD",
            ])?,
            live_write: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE")?,
            max_jobs: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS")?
                .map(|value| {
                    parse_positive_usize("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_JOBS", &value)
                })
                .transpose()?,
            max_files: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES")?
                .map(|value| {
                    parse_positive_usize(
                        "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_FILES",
                        &value,
                    )
                })
                .transpose()?,
            max_in_flight: parse_dataset_file_max_in_flight(optional_env_value(
                "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT",
            )?)?,
            full_download_confirmed: live_write_enabled(
                optional_env_value(
                    "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD",
                )?
                .as_deref(),
            ),
            exclude_selection_archives: live_write_enabled(
                optional_env_value(
                    "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES",
                )?
                .as_deref(),
            ),
            defer_provider_acquisition_blocked: live_write_enabled(
                optional_env_value(
                    "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_DEFER_PROVIDER_ACQUISITION_BLOCKED",
                )?
                .as_deref(),
            ),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VWorldDatasetFileIngestStatus {
    evidence_status: &'static str,
    should_bail: bool,
}

fn vworld_dataset_file_ingest_status(
    failed_file_count: u64,
    provider_acquisition_blocked_file_count: u64,
    defer_provider_acquisition_blocked: bool,
) -> VWorldDatasetFileIngestStatus {
    if failed_file_count > 0 {
        return VWorldDatasetFileIngestStatus {
            evidence_status: "blocked",
            should_bail: true,
        };
    }
    if provider_acquisition_blocked_file_count == 0 {
        return VWorldDatasetFileIngestStatus {
            evidence_status: "ready",
            should_bail: false,
        };
    }
    if defer_provider_acquisition_blocked {
        return VWorldDatasetFileIngestStatus {
            evidence_status: "ready_with_provider_acquisition_deferred",
            should_bail: false,
        };
    }
    VWorldDatasetFileIngestStatus {
        evidence_status: "blocked",
        should_bail: true,
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct VWorldDatasetFileInventoryReport {
    jobs: Vec<VWorldDatasetFileJob>,
}

#[derive(Clone, Debug)]
struct SelectedVWorldDatasetFile {
    job: VWorldDatasetFileJob,
    file: VWorldDatasetFileInventoryItem,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
struct VWorldDatasetFileIngestEvidence {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    file_inventory_path: String,
    selected_file_count: u64,
    max_in_flight: usize,
    succeeded_file_count: u64,
    skipped_file_count: u64,
    provider_acquisition_blocked_file_count: u64,
    failed_file_count: u64,
    live_write_enabled: bool,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    files: Vec<VWorldDatasetFileIngestItemEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
struct VWorldDatasetFileIngestItemEvidence {
    endpoint_slug: String,
    source_slug: String,
    download_ds_id: String,
    file_no: String,
    provider_file_name: String,
    status: String,
    object_key: Option<String>,
    size_bytes: Option<u64>,
    error_message: Option<String>,
    duration_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
struct VWorldDatasetFileJob {
    endpoint_slug: String,
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    terms_url: Option<String>,
    operation: String,
    provider_module: String,
    svc_cde: String,
    ds_id: String,
    files: Vec<VWorldDatasetFileInventoryItem>,
}

async fn ingest_file(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
    config: &VWorldDatasetFileIngestConfig,
) -> anyhow::Result<VWorldDatasetFileIngestItemEvidence> {
    let started_at = Utc::now();
    validate_inventory_file_identity(file)?;
    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: job.base_uri.clone(),
        user_agent: config.user_agent.clone(),
        page_size: config.page_size,
        cookie_header: config.cookie_header.clone(),
    })?;
    let downloaded = client
        .open_file_stream_with_provider_file_name_fallback(
            &download_request_from_inventory_file(file),
            &file.provider_file_name,
        )
        .await
        .with_context(|| {
            format!(
                "failed to open VWorld dataset file stream endpoint={} download_ds_id={} file_no={}",
                job.endpoint_slug, file.download_ds_id, file.file_no
            )
        })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let location = plan_streamed_file_location(
        job,
        file,
        run_id,
        started_at.date_naive(),
        downloaded.provider_file_name.clone(),
    )
    .context("failed to plan VWorld dataset Bronze file location")?;
    let object_key = location.object_key.as_str().to_owned();
    let expected_size_bytes = downloaded.expected_size_bytes;

    let size_bytes = if live_write_enabled(config.live_write.as_deref()) {
        Some(
            persist_file_stream(run_id, started_at, job, file, downloaded)
                .await?
                .size_bytes,
        )
    } else {
        expected_size_bytes
    };

    Ok(VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: "succeeded".to_owned(),
        object_key: Some(object_key),
        size_bytes,
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    })
}

async fn ingest_file_with_adapters<Repo, Uow, Storage>(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
    config: &VWorldDatasetFileIngestConfig,
    repo: &Repo,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<VWorldDatasetFileIngestItemEvidence>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let started_at = Utc::now();
    validate_inventory_file_identity(file)?;
    // Pre-download skip: if a Bronze object already exists for this file's `source_partition_key`
    // (which includes `provider_file_id` = `{download_ds_id}-{file_no}`), skip the download. This is
    // a request-fingerprint optimization (per docs/catalog/source-change-detection-policy.md) and is
    // correct ONLY because `provider_file_id` is content-stable for this provider (a new published
    // file gets a fresh id, so new content ⇒ new id). If a provider ever reused a file id with
    // changed bytes, this skip would miss the change — set FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1 to
    // bypass it and force the post-download SHA256 content check (the policy's correctness baseline).
    // First re-collect on an empty DB never hits this skip.
    let force_refetch = crate::public_data_control_support::bronze_force_refetch_enabled()?;
    if !force_refetch {
        if let Some(existing) = existing_file_report(job, file, started_at, repo, uow)
            .await
            .context("failed to check existing VWorld dataset Bronze object")?
        {
            return Ok(existing);
        }
    }

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: job.base_uri.clone(),
        user_agent: config.user_agent.clone(),
        page_size: config.page_size,
        cookie_header: config.cookie_header.clone(),
    })?;
    let downloaded = client
        .open_file_stream_with_provider_file_name_fallback(
            &download_request_from_inventory_file(file),
            &file.provider_file_name,
        )
        .await
        .with_context(|| {
            format!(
                "failed to open VWorld dataset file stream endpoint={} download_ds_id={} file_no={}",
                job.endpoint_slug, file.download_ds_id, file.file_no
            )
        })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let location = plan_streamed_file_location(
        job,
        file,
        run_id,
        started_at.date_naive(),
        downloaded.provider_file_name.clone(),
    )
    .context("failed to plan VWorld dataset Bronze file location")?;
    let object_key = location.object_key.as_str().to_owned();
    let size_bytes =
        persist_file_stream_with_adapters(job, file, run_id, started_at, downloaded, uow, storage)
            .await?
            .size_bytes;

    Ok(VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: "succeeded".to_owned(),
        object_key: Some(object_key),
        size_bytes: Some(size_bytes),
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    })
}

async fn existing_file_report<Repo, Uow>(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
    started_at: chrono::DateTime<Utc>,
    repo: &Repo,
    uow: &Uow,
) -> anyhow::Result<Option<VWorldDatasetFileIngestItemEvidence>>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(job, started_at))
        .await
        .context("failed to upsert VWorld dataset source catalog entry before resume check")?;
    let source_partition_key =
        public_data_bulk_file_source_partition_key(PublicDataBulkFileSourcePartitionKeyInput {
            operation: &job.operation,
            provider_file_id: &provider_file_id(file),
        })
        .context("failed to plan VWorld dataset source partition key")?;
    let existing = repo
        .find_bronze_object_by_source_partition_key(source.id, &source_partition_key)
        .await
        .with_context(|| {
            format!(
                "failed to query existing VWorld Bronze object for {}",
                source_partition_key
            )
        })?;

    Ok(existing.map(|object| VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: "skipped_existing".to_owned(),
        object_key: Some(object.object_key.as_str().to_owned()),
        size_bytes: Some(object.size_bytes),
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    }))
}

fn failed_file_report(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
    started_at: chrono::DateTime<Utc>,
    error: anyhow::Error,
) -> VWorldDatasetFileIngestItemEvidence {
    let provider_acquisition_blocked = is_provider_acquisition_blocked(&error);
    let error_message = truncate_failure_message(&format!("{error:#}"));
    VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: if provider_acquisition_blocked {
            "provider_acquisition_blocked".to_owned()
        } else {
            "failed".to_owned()
        },
        object_key: None,
        size_bytes: None,
        error_message: Some(error_message),
        duration_ms: elapsed_millis(started_at),
    }
}

fn is_provider_acquisition_blocked(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<CollectionError>()
            .is_some_and(|error| matches!(error, CollectionError::ProviderAcquisitionBlocked(_)))
    })
}

fn download_request_from_inventory_file(
    file: &VWorldDatasetFileInventoryItem,
) -> VWorldDatasetFileDownloadRequest {
    VWorldDatasetFileDownloadRequest {
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        download_kind: file.download_kind.clone(),
    }
}

fn plan_streamed_file_location(
    job: &VWorldDatasetFileJob,
    inventory_file: &VWorldDatasetFileInventoryItem,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
    provider_file_name: String,
) -> anyhow::Result<PublicDataBulkFileStorageLocationPlan> {
    plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
        source_slug: &job.source_slug,
        ingest_date,
        ingestion_run_id: run_id,
        identity: streamed_file_identity(job, inventory_file, provider_file_name),
    })
    .map_err(anyhow::Error::from)
}

fn streamed_file_identity(
    job: &VWorldDatasetFileJob,
    inventory_file: &VWorldDatasetFileInventoryItem,
    provider_file_name: String,
) -> PublicDataBulkFileIdentity {
    PublicDataBulkFileIdentity {
        operation: job.operation.clone(),
        provider_file_period: provider_file_period(inventory_file),
        provider_snapshot_date: provider_snapshot_date(inventory_file),
        provider_file_id: provider_file_id(inventory_file),
        provider_file_name,
        provider_updated_at: provider_updated_at(inventory_file),
    }
}

fn provider_file_period(file: &VWorldDatasetFileInventoryItem) -> Option<String> {
    let value = file.base_ym.trim();
    if value.is_empty() || value == "-" || parse_provider_date(value).is_some() {
        return None;
    }
    Some(value.to_owned())
}

fn provider_snapshot_date(file: &VWorldDatasetFileInventoryItem) -> Option<NaiveDate> {
    parse_provider_date(file.base_ym.trim())
}

fn provider_updated_at(file: &VWorldDatasetFileInventoryItem) -> Option<NaiveDate> {
    let value = file.updated_at.trim();
    if value.is_empty() || value == "-" {
        return None;
    }
    parse_provider_date(value)
}

fn parse_provider_date(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn provider_file_id(file: &VWorldDatasetFileInventoryItem) -> String {
    format!("{}-{}", file.download_ds_id, file.file_no)
}

/// Rejects inventory identity components that could make the flattened
/// `provider_file_id` (`{download_ds_id}-{file_no}`) ambiguous: both parts must be ASCII
/// alphanumeric (the download client enforces the same rule later), so the joining hyphen
/// can never collide and the resume lookup can never skip the wrong file.
fn validate_inventory_file_identity(file: &VWorldDatasetFileInventoryItem) -> anyhow::Result<()> {
    for (name, value) in [
        ("download_ds_id", file.download_ds_id.as_str()),
        ("file_no", file.file_no.as_str()),
    ] {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            anyhow::bail!(
                "VWorld inventory file {name} must be non-empty ASCII alphanumeric, got {value:?}"
            );
        }
    }
    Ok(())
}

async fn resolve_vworld_dataset_cookie_header(
    config: &VWorldDatasetFileIngestConfig,
    selected_files: &[SelectedVWorldDatasetFile],
) -> anyhow::Result<Option<String>> {
    if config.cookie_header.is_some() {
        return Ok(config.cookie_header.clone());
    }
    let Some(first_selected_file) = selected_files.first() else {
        bail!("at least one VWorld dataset file must be selected before resolving provider auth");
    };
    let Some(login_config) = vworld_dataset_login_config(config, &first_selected_file.job.base_uri)
    else {
        bail!(
            "VWorld dataset file ingest requires FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER or provider credentials via FOUNDATION_PLATFORM_VWORLD_DATASET_USERNAME/VWORLD_USERNAME and FOUNDATION_PLATFORM_VWORLD_DATASET_PASSWORD/VWORLD_PASSWORD"
        );
    };
    let client = VWorldDatasetLoginClient::new(&login_config)
        .context("failed to configure VWorld dataset login client")?;
    client
        .fetch_cookie_header()
        .await
        .map(Some)
        .context("failed to acquire VWorld dataset login session")
}

fn vworld_dataset_login_config(
    config: &VWorldDatasetFileIngestConfig,
    base_uri: &str,
) -> Option<VWorldDatasetLoginConfig> {
    if config.cookie_header.is_some() {
        return None;
    }
    Some(VWorldDatasetLoginConfig {
        base_uri: base_uri.to_owned(),
        user_agent: config.user_agent.clone(),
        username: config.username.clone()?,
        password: config.password.clone()?,
    })
}

async fn persist_file_stream(
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    job: &VWorldDatasetFileJob,
    inventory_file: &VWorldDatasetFileInventoryItem,
    file: VWorldDatasetFileStream,
) -> anyhow::Result<VWorldDatasetFilePersistReport> {
    // Single-file live-write path (the non-orchestrated `ingest_file` branch): validate + log the
    // resolved R2 target before the first put. Reached only when live write is enabled.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("VWorld dataset file ingest live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for VWorld dataset file ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_streaming_object_storage_from_env()
        .await
        .context("failed to configure object storage for VWorld dataset file ingest")?;
    persist_file_stream_with_adapters(
        job,
        inventory_file,
        run_id,
        started_at,
        file,
        &uow,
        storage.as_ref(),
    )
    .await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldDatasetFilePersistReport {
    size_bytes: u64,
}

async fn persist_file_stream_with_adapters<Uow, Storage>(
    job: &VWorldDatasetFileJob,
    inventory_file: &VWorldDatasetFileInventoryItem,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    file: VWorldDatasetFileStream,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<VWorldDatasetFilePersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(job, started_at))
        .await
        .context("failed to upsert VWorld dataset source catalog entry")?;
    let provider_file_name = file.provider_file_name.clone();
    let location = plan_streamed_file_location(
        job,
        inventory_file,
        run_id,
        started_at.date_naive(),
        provider_file_name.clone(),
    )
    .context("failed to plan VWorld dataset Bronze object location")?;
    let run = uow
        .create_ingestion_run(&ingestion_run(
            source.id,
            run_id,
            started_at,
            initial_batch_request_params(
                job,
                inventory_file,
                &provider_file_name,
                location.object_key.as_str(),
            ),
        ))
        .await
        .context("failed to create VWorld dataset file ingestion run")?;

    // Route the streaming write + record through the SINGLE Bronze committer (ADR 0016): it streams
    // the body write-once (`CreateOnly` / `If-None-Match: *`, computing sha256 in-flight), then
    // records the `bronze_object` row from that streamed checksum + size — and on a 412 collision
    // runs the streaming recovery (idempotent skip if the row exists, else GET-rehash + recover).
    // Behaviour for the normal (key-absent) path is byte-identical to the previous inline
    // stream-then-record: same object key, same partition key, same `request_params`, same dedupe
    // key (`<slug>:<source_partition_key>:sha256=<checksum>`), same streamed bytes, same size — only
    // the write mode changes from `OverwriteAllowed` to `CreateOnly` and a 412 now self-heals.
    let identity = streamed_file_identity(job, inventory_file, provider_file_name);
    let content_type = file.content_type.clone();
    let expected_size_bytes = file.expected_size_bytes.with_context(|| {
        format!(
            "provider file {} omitted Content-Length; streaming single-pass Bronze upload requires an exact length",
            identity.provider_file_id
        )
    })?;
    let writer = BronzeStreamingObjectStorageWriter::new(
        storage,
        content_type.clone(),
        file.into_body_stream(),
    );
    let planned = PlannedStreamingBronzeObject {
        cache_control: BRONZE_CACHE_CONTROL.to_owned(),
        expected_size_bytes,
        record: StreamingBronzeRecord {
            object_key: location.object_key.clone(),
            content_type,
            source_catalog_id: source.id,
            ingestion_run_id: run.id,
            source_partition_key: location.source_partition_key.clone(),
            source_identity_key: location.source_identity_key.clone(),
            dedupe_key_prefix: format!("{}:{}", job.source_slug, location.source_identity_key),
            request_params: public_data_bulk_file_request_params(&identity),
            collected_at: started_at,
            snapshot_period: location.snapshot_period,
            snapshot_date: location.snapshot_date,
            snapshot_granularity: location.snapshot_granularity,
            snapshot_basis: location.snapshot_basis,
            provider_file_id: Some(identity.provider_file_id),
            provider_file_name: Some(identity.provider_file_name),
            provider_updated_at: identity.provider_updated_at,
        },
    };

    let mut objects_written = 0;
    let outcome = match BRONZE_COMMITTER
        .commit_streaming_bulk(&writer, uow, planned)
        .await
        .context("failed to commit VWorld dataset Bronze object")
    {
        Ok(outcome) => {
            objects_written += 1;
            outcome
        }
        Err(error) => {
            return Err(mark_run_failed_after_error(uow, run.id, objects_written, error).await);
        }
    };

    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: 0,
        objects_written,
        error_message: None,
    })
    .await
    .context("failed to complete VWorld dataset file ingestion run")?;
    Ok(VWorldDatasetFilePersistReport {
        size_bytes: outcome.size_bytes,
    })
}

/// The single Bronze committer instance (ADR 0016). Stateless, so a const is enough.
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();

/// Cache-Control header attached to every streamed Bronze bulk object.
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";

async fn mark_run_failed_after_error<Uow>(
    uow: &Uow,
    run_id: IngestionRunId,
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
            "also failed to mark VWorld dataset file ingestion run {run_id} as failed: {failure_error}"
        )),
    }
}

fn source_catalog_entry(
    job: &VWorldDatasetFileJob,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: job.source_slug.clone(),
        name: job.source_name.clone(),
        provider: "VWorld".to_owned(),
        dataset_name: job.dataset_name.clone(),
        base_url: Some(job.base_uri.clone()),
        auth_kind: SourceAuthKind::Manual,
        payload_format: SourcePayloadFormat::Unknown,
        license_name: None,
        license_url: None,
        terms_url: job.terms_url.clone(),
        collection_frequency: None,
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    }
}

const fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    run_id: IngestionRunId,
    now: chrono::DateTime<Utc>,
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

fn initial_batch_request_params(
    job: &VWorldDatasetFileJob,
    inventory_file: &VWorldDatasetFileInventoryItem,
    provider_file_name: &str,
    object_key: &str,
) -> JsonValue {
    json!({
        "sourceAcquisitionLane": "provider_dataset_file",
        "endpointSlug": job.endpoint_slug,
        "operation": job.operation,
        "svcCde": job.svc_cde,
        "dsId": job.ds_id,
        "downloadDsId": inventory_file.download_ds_id,
        "fileNo": inventory_file.file_no,
        "providerFileName": provider_file_name,
        "downloadKind": inventory_file.download_kind,
        "objectKey": object_key
    })
}

fn read_file_inventory(path: &PathBuf) -> anyhow::Result<VWorldDatasetFileInventoryReport> {
    let content = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read VWorld dataset file inventory: {}",
            path.display()
        )
    })?;
    serde_json::from_str(&content).context("failed to parse VWorld dataset file inventory")
}

fn select_inventory_files(
    jobs: &[VWorldDatasetFileJob],
    max_jobs: Option<usize>,
    max_files: Option<usize>,
    exclude_selection_archives: bool,
) -> anyhow::Result<Vec<SelectedVWorldDatasetFile>> {
    if jobs.is_empty() {
        bail!("VWorld dataset file inventory contains no jobs");
    }
    let job_limit = max_jobs.unwrap_or(jobs.len()).min(jobs.len());
    let file_limit = max_files.unwrap_or(usize::MAX);
    let mut selected = Vec::new();
    // Duplicate partition identities would race the skip-resume check once files run through
    // buffer_unordered (check-then-write without a DB claim), so refuse them up front.
    let mut seen = std::collections::HashSet::new();
    for job in jobs.iter().take(job_limit) {
        for file in &job.files {
            if exclude_selection_archives
                && file.download_kind == VWorldDatasetFileKind::SelectionArchive
            {
                continue;
            }
            if selected.len() >= file_limit {
                return Ok(selected);
            }
            let identity = (
                job.source_slug.clone(),
                job.operation.clone(),
                provider_file_id(file),
            );
            if !seen.insert(identity) {
                bail!(
                    "VWorld dataset file inventory contains a duplicate partition identity                      source_slug={} operation={} provider_file_id={}",
                    job.source_slug,
                    job.operation,
                    provider_file_id(file)
                );
            }
            selected.push(SelectedVWorldDatasetFile {
                job: job.clone(),
                file: file.clone(),
            });
        }
    }
    Ok(selected)
}

fn parse_dataset_file_max_in_flight(raw: Option<String>) -> anyhow::Result<usize> {
    raw.map(|value| {
        parse_positive_usize(
            "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_MAX_IN_FLIGHT",
            &value,
        )
    })
    .transpose()
    .map(|value| value.unwrap_or(4))
}

fn inventory_file_count(jobs: &[VWorldDatasetFileJob]) -> usize {
    jobs.iter().map(|job| job.files.len()).sum()
}

fn eligible_inventory_file_count(
    jobs: &[VWorldDatasetFileJob],
    exclude_selection_archives: bool,
) -> usize {
    if !exclude_selection_archives {
        return inventory_file_count(jobs);
    }
    jobs.iter()
        .flat_map(|job| job.files.iter())
        .filter(|file| file.download_kind != VWorldDatasetFileKind::SelectionArchive)
        .count()
}

fn write_evidence(
    path: &PathBuf,
    evidence: &VWorldDatasetFileIngestEvidence,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create VWorld dataset file ingest evidence directory: {}",
                parent.display()
            )
        })?;
    }
    fs::write(path, serde_json::to_vec_pretty(evidence)?).with_context(|| {
        format!(
            "failed to write VWorld dataset file ingest evidence: {}",
            path.display()
        )
    })
}

fn optional_env_value_any(names: &[&str]) -> anyhow::Result<Option<String>> {
    for name in names {
        if let Some(value) = optional_env_value(name)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn parse_positive_u64(name: &str, value: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

fn parse_positive_usize(name: &str, value: &str) -> anyhow::Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

fn live_write_enabled(value: Option<&str>) -> bool {
    matches!(value, Some("1"))
}

fn elapsed_millis(started_at: chrono::DateTime<Utc>) -> u64 {
    u64::try_from((Utc::now() - started_at).num_milliseconds().max(0)).unwrap_or(u64::MAX)
}

fn truncate_failure_message(message: &str) -> String {
    const MAX_FAILURE_MESSAGE_BYTES: usize = 1_000;
    if message.len() <= MAX_FAILURE_MESSAGE_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_FAILURE_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &message[..end])
}

#[cfg(test)]
mod tests;
