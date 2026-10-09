use std::{env, fs, path::PathBuf, sync::Arc};

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
    build_bronze_content_object_key, BronzeObject, CollectionError, IngestionRun,
    IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat,
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

use crate::bronze_object_storage::live_write_bronze_streaming_object_storage_from_env;
use crate::bulk_streaming_bronze::BronzeStreamingObjectStorageWriter;
use crate::content_spool::{spool_payload, SpoolDir};
use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::vworld_credentials::{
    optional_vworld_password, optional_vworld_username, vworld_password_name, vworld_username_name,
};

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
    let mut budget_check = None;
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
        let mut to_fetch = selected_files.into_iter().enumerate().collect::<Vec<_>>();
        let mut held_reports = Vec::new();
        if let Some(budget) = config.new_bytes_budget {
            // Root ADR-0168: before any body is opened, learn which listed files Bronze already
            // holds and refuse the run whole when the rest is more than the budget. A backlog is
            // fetched by an operator who raised the budget on purpose, never by a daily run that
            // happened to find it.
            let force_refetch = crate::public_data_control_support::bronze_force_refetch_enabled()?;
            let (held, pending) =
                partition_held_files(to_fetch, force_refetch, &repo, &uow).await?;
            let check = NewBytesBudgetCheck {
                budget,
                pending_listed_bytes: listed_bytes(
                    pending.iter().map(|(_, selected)| &selected.file),
                ),
                pending_file_count: pending.len() as u64,
            };
            budget_check = Some(check);
            if check.is_exceeded() {
                let mut reports = held;
                reports.extend(pending.into_iter().map(|(index, selected)| {
                    (
                        index,
                        deferred_by_budget_report(&selected.job, &selected.file),
                    )
                }));
                reports.sort_by_key(|(index, _)| *index);
                let evidence = ingest_evidence(
                    &config,
                    reports.into_iter().map(|(_, report)| report).collect(),
                    live_write,
                    NEW_BYTES_BUDGET_EXCEEDED_STATUS,
                    budget_check,
                );
                write_evidence(&config.evidence_path, &evidence)?;
                bail!(
                    "VWorld dataset file ingest refused before any download: {} files not held list {} bytes, above the new-bytes budget of {} (root ADR-0168) report={}",
                    check.pending_file_count,
                    check.pending_listed_bytes,
                    check.budget,
                    config.evidence_path.display()
                );
            }
            held_reports = held;
            to_fetch = pending;
        }
        let storage = live_write_bronze_streaming_object_storage_from_env()
            .await
            .context("failed to configure object storage for VWorld dataset file ingest")?;
        let spool = open_spool(&config)?;
        let payload_key = PayloadKey::for_run(config.bronze_key, spool.as_ref())?;
        let mut fetched = stream::iter(to_fetch)
            .map(|(index, selected)| {
                let config = config.clone();
                let repo = &repo;
                let uow = &uow;
                let storage = storage.as_ref();
                async move {
                    let SelectedVWorldDatasetFile { job, file } = selected;
                    let started_at = Utc::now();
                    let report = match ingest_file_with_adapters(
                        &job,
                        &file,
                        &config,
                        payload_key,
                        repo,
                        uow,
                        storage,
                    )
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
            .await;
        fetched.extend(held_reports);
        fetched
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
    let evidence = ingest_evidence(
        &config,
        reports,
        live_write,
        ingest_status.evidence_status,
        budget_check,
    );
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

/// Evidence status of a run refused by its new-bytes budget (root ADR-0168).
const NEW_BYTES_BUDGET_EXCEEDED_STATUS: &str = "blocked_new_bytes_budget";
/// File status of a listed file the budget kept from being downloaded.
const DEFERRED_BY_BUDGET_STATUS: &str = "deferred_new_bytes_budget";

/// What one run would download that Bronze does not hold, against what it may (root ADR-0168).
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
struct NewBytesBudgetCheck {
    budget: u64,
    pending_listed_bytes: u64,
    pending_file_count: u64,
}

impl NewBytesBudgetCheck {
    const fn is_exceeded(self) -> bool {
        self.pending_listed_bytes > self.budget
    }
}

/// The provider's listed size of `files`, in bytes. The listing gives KiB (`size_kib`); the body is
/// not opened to learn more.
fn listed_bytes<'a>(files: impl Iterator<Item = &'a VWorldDatasetFileInventoryItem>) -> u64 {
    files
        .map(|file| file.size_kib.saturating_mul(1024))
        .fold(0_u64, u64::saturating_add)
}

type IndexedFile = (usize, SelectedVWorldDatasetFile);
type IndexedReport = (usize, VWorldDatasetFileIngestItemEvidence);

/// Splits the selected files into those Bronze already holds (with their skip reports) and those a
/// run would download. With `force_refetch` nothing counts as held, as on the download path.
async fn partition_held_files<Repo, Uow>(
    files: Vec<IndexedFile>,
    force_refetch: bool,
    repo: &Repo,
    uow: &Uow,
) -> anyhow::Result<(Vec<IndexedReport>, Vec<IndexedFile>)>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    if force_refetch {
        return Ok((Vec::new(), files));
    }
    let mut held = Vec::new();
    let mut pending = Vec::new();
    for (index, selected) in files {
        // An identity the download path refuses is left to it, so it fails as that one file.
        if validate_inventory_file_identity(&selected.file).is_err() {
            pending.push((index, selected));
            continue;
        }
        match existing_file_report(&selected.job, &selected.file, Utc::now(), repo, uow)
            .await
            .context("failed to check existing VWorld dataset Bronze object")?
        {
            Some(report) => held.push((index, report)),
            None => pending.push((index, selected)),
        }
    }
    Ok((held, pending))
}

fn deferred_by_budget_report(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
) -> VWorldDatasetFileIngestItemEvidence {
    VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: DEFERRED_BY_BUDGET_STATUS.to_owned(),
        object_key: None,
        size_bytes: Some(file.size_kib.saturating_mul(1024)),
        error_message: None,
        duration_ms: 0,
    }
}

fn ingest_evidence(
    config: &VWorldDatasetFileIngestConfig,
    reports: Vec<VWorldDatasetFileIngestItemEvidence>,
    live_write: bool,
    status: &'static str,
    new_bytes_budget: Option<NewBytesBudgetCheck>,
) -> VWorldDatasetFileIngestEvidence {
    let count = |status: &str| {
        reports
            .iter()
            .filter(|report| report.status == status)
            .count() as u64
    };
    VWorldDatasetFileIngestEvidence {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        status,
        file_inventory_path: config
            .file_inventory_path
            .to_string_lossy()
            .replace('\\', "/"),
        selected_file_count: reports.len() as u64,
        max_in_flight: config.max_in_flight,
        succeeded_file_count: count("succeeded"),
        skipped_file_count: count("skipped_existing"),
        provider_acquisition_blocked_file_count: count("provider_acquisition_blocked"),
        failed_file_count: count("failed"),
        deferred_by_budget_file_count: count(DEFERRED_BY_BUDGET_STATUS),
        new_bytes_budget,
        live_write_enabled: live_write,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        files: reports,
    }
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
    bronze_key: BronzeKeyForm,
    /// `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_NEW_BYTES_BUDGET`: the most listed bytes a live run
    /// may download that Bronze does not hold (root ADR-0168). Unset, there is no budget.
    new_bytes_budget: Option<u64>,
    /// `FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR`: where a content-addressed run spools each
    /// body while hashing it (root ADR-0168). Required for `content_addressed`.
    spool_dir: Option<PathBuf>,
}

/// How a landed file's Bronze key is chosen (`FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BronzeKeyForm {
    /// `<provider file id>.<ext>` (`provider_file_id`, the default): one key per provider file
    /// number. A new upload under the same number cannot land in a bucket that refuses overwrites.
    ProviderFileId,
    /// `<provider file id>--sha256-<checksum>.<ext>` (`content_addressed`, root ADR-0152): one key
    /// per payload. A new upload lands beside the old one, and the same bytes again find their own
    /// object, so a rerun succeeds without writing.
    ContentAddressed,
}

const BRONZE_KEY_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY";
const NEW_BYTES_BUDGET_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_NEW_BYTES_BUDGET";
const SPOOL_DIR_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR";

/// A budget is a whole number of bytes; zero is allowed and means "download nothing new".
fn parse_new_bytes_budget(raw: &str) -> anyhow::Result<u64> {
    raw.trim()
        .parse::<u64>()
        .with_context(|| format!("{NEW_BYTES_BUDGET_ENV} must be a whole number of bytes"))
}

fn parse_bronze_key_form(raw: Option<&str>) -> anyhow::Result<BronzeKeyForm> {
    match raw.map(str::trim) {
        None | Some("" | "provider_file_id") => Ok(BronzeKeyForm::ProviderFileId),
        Some("content_addressed") => Ok(BronzeKeyForm::ContentAddressed),
        Some(other) => {
            bail!("{BRONZE_KEY_ENV} must be provider_file_id or content_addressed, not {other:?}")
        }
    }
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
            username: optional_vworld_username()?,
            password: optional_vworld_password()?,
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
            bronze_key: parse_bronze_key_form(optional_env_value(BRONZE_KEY_ENV)?.as_deref())?,
            new_bytes_budget: optional_env_value(NEW_BYTES_BUDGET_ENV)?
                .map(|value| parse_new_bytes_budget(&value))
                .transpose()?,
            spool_dir: optional_env_value(SPOOL_DIR_ENV)?.map(PathBuf::from),
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
    deferred_by_budget_file_count: u64,
    /// The new-bytes budget check, when the run had one (root ADR-0168).
    new_bytes_budget: Option<NewBytesBudgetCheck>,
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
    let expected_size_bytes = downloaded.expected_size_bytes;

    // Without a live write the bytes are never read, so a content-addressed key is unknown: the
    // evidence then names no key rather than one that does not exist.
    let (object_key, size_bytes) = if live_write_enabled(config.live_write.as_deref()) {
        let persisted =
            persist_file_stream(run_id, started_at, job, file, downloaded, config).await?;
        (Some(persisted.object_key), Some(persisted.size_bytes))
    } else if config.bronze_key == BronzeKeyForm::ContentAddressed {
        (None, expected_size_bytes)
    } else {
        (
            Some(location.object_key.as_str().to_owned()),
            expected_size_bytes,
        )
    };

    Ok(VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: "succeeded".to_owned(),
        object_key,
        size_bytes,
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    })
}

async fn ingest_file_with_adapters<Repo, Uow, Storage>(
    job: &VWorldDatasetFileJob,
    file: &VWorldDatasetFileInventoryItem,
    config: &VWorldDatasetFileIngestConfig,
    payload_key: PayloadKey<'_>,
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
    // a request-fingerprint optimization (per docs/catalog/source-change-detection-policy.md). The
    // provider DOES reuse a file id with changed bytes — 연속지적도 (ds 30563) republishes every
    // edition under the same file numbers (root ADR-0148) — so the skip also requires the held
    // object to carry the provider update date the inventory lists (`holds_listed_release`).
    // FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1 still bypasses it and forces the post-download
    // SHA256 content check (the policy's correctness baseline). First re-collect on an empty DB
    // never hits this skip.
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
    let persisted = persist_file_stream_with_adapters(
        job,
        file,
        run_id,
        started_at,
        downloaded,
        payload_key,
        uow,
        storage,
    )
    .await?;

    Ok(VWorldDatasetFileIngestItemEvidence {
        endpoint_slug: job.endpoint_slug.clone(),
        source_slug: job.source_slug.clone(),
        download_ds_id: file.download_ds_id.clone(),
        file_no: file.file_no.clone(),
        provider_file_name: file.provider_file_name.clone(),
        status: "succeeded".to_owned(),
        object_key: Some(persisted.object_key),
        size_bytes: Some(persisted.size_bytes),
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

    Ok(existing
        .filter(|object| holds_listed_release(object, file))
        .filter(holds_known_checksum)
        .map(|object| VWorldDatasetFileIngestItemEvidence {
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

/// Whether the Bronze object held under this provider file id is the release the inventory lists.
///
/// The id alone does not say so: VWorld reuses a file's number across releases. 연속지적도 (ds
/// 30563) file 100 held the 2026-06 edition and then the 2026-09 one (root ADR-0148), so a skip on
/// the id would have kept June forever. The provider's update date tells releases apart; where the
/// inventory gives none, the id is all there is and it decides, as it did before.
fn holds_listed_release(object: &BronzeObject, file: &VWorldDatasetFileInventoryItem) -> bool {
    match provider_updated_at(file) {
        None => true,
        listed => object.provider_updated_at == listed,
    }
}

/// Whether the ledger knows the held object's bytes: a SHA-256 the committer recorded.
///
/// The key form is not part of "held" (root ADR-0168, amending ADR-0152). A release committed under
/// a plain provider-file key before content-addressed keys existed is the same release: same
/// provider file id, same provider update date, its checksum in the ledger. Refusing to skip it made
/// a content-addressed run download every such file again; a reader that needs a key naming its
/// bytes checks the key itself (the 30527 handoff refuses any other, ADR-0152 §3) and fetches with
/// `FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1`.
fn holds_known_checksum(object: &BronzeObject) -> bool {
    object.checksum_sha256.len() == 64
        && object
            .checksum_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
        let username_name = vworld_username_name()?;
        let password_name = vworld_password_name()?;
        bail!(
            "VWorld dataset file ingest requires FOUNDATION_PLATFORM_VWORLD_DATASET_COOKIE_HEADER or provider credentials via {username_name} and {password_name}"
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
    config: &VWorldDatasetFileIngestConfig,
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
    let storage = live_write_bronze_streaming_object_storage_from_env()
        .await
        .context("failed to configure object storage for VWorld dataset file ingest")?;
    let spool = open_spool(config)?;
    persist_file_stream_with_adapters(
        job,
        inventory_file,
        run_id,
        started_at,
        file,
        PayloadKey::for_run(config.bronze_key, spool.as_ref())?,
        &uow,
        storage.as_ref(),
    )
    .await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldDatasetFilePersistReport {
    object_key: String,
    size_bytes: u64,
}

/// The provider bytes and the key they land under, decided before the ingestion run is opened.
struct PlannedPayload<'a, Storage: ?Sized> {
    object_key: foundation_shared_kernel::ObjectKey,
    expected_size_bytes: u64,
    writer: BronzeStreamingObjectStorageWriter<'a, Storage>,
}

/// Chooses the key and the write port for one provider file.
///
/// `ProviderFileId` streams the body to the provider-file key, as before. `ContentAddressed` writes
/// the body to the spool while hashing it (root ADR-0168: no in-memory size cap), names the key after
/// its SHA-256, and reads that key back: absent, the spooled bytes are uploaded (multipart above the
/// single-put size); present with the same checksum and size, nothing is written and the committer
/// reconciles the ledger row (a rerun of the same upload); present with other bytes, the file is
/// refused, since a key that names one checksum cannot hold another. The spooled file belongs to the
/// write port's body and is removed when that body is dropped, read or not.
async fn plan_payload<'a, Storage>(
    location: &PublicDataBulkFileStorageLocationPlan,
    provider_file_id: &str,
    file: VWorldDatasetFileStream,
    payload_key: PayloadKey<'_>,
    storage: &'a Storage,
) -> anyhow::Result<PlannedPayload<'a, Storage>>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let content_type = file.content_type.clone();
    let expected_size_bytes = file.expected_size_bytes.with_context(|| {
        format!(
            "provider file {provider_file_id} omitted Content-Length; streaming single-pass Bronze upload requires an exact length"
        )
    })?;
    let spool = match payload_key {
        PayloadKey::ProviderFileId => {
            return Ok(PlannedPayload {
                object_key: location.object_key.clone(),
                expected_size_bytes,
                writer: BronzeStreamingObjectStorageWriter::new(
                    storage,
                    content_type,
                    file.into_body_stream(),
                ),
            });
        }
        PayloadKey::ContentAddressed(spool) => spool,
    };
    let spooled = Arc::new(
        spool_payload(
            spool,
            file.into_body_stream(),
            &content_type,
            expected_size_bytes,
            provider_file_id,
        )
        .await?,
    );
    let object_key =
        build_bronze_content_object_key(&location.object_key, &spooled.checksum_sha256)
            .context("failed to name the content-addressed Bronze key")?;
    let held = storage
        .read_object_sha256_and_size_by_rehash(object_key.as_str())
        .await
        .with_context(|| format!("failed to read back {}", object_key.as_str()))?;
    let writer = match held {
        None => BronzeStreamingObjectStorageWriter::new(
            storage,
            content_type,
            Arc::clone(&spooled).body_stream(),
        ),
        Some(held)
            if held.checksum_sha256 == spooled.checksum_sha256
                && held.size_bytes == spooled.size_bytes =>
        {
            BronzeStreamingObjectStorageWriter::already_present(storage, content_type)
        }
        Some(held) => bail!(
            "{} holds sha256 {} ({} bytes), not the {expected_size_bytes} bytes it names",
            object_key.as_str(),
            held.checksum_sha256,
            held.size_bytes
        ),
    };
    Ok(PlannedPayload {
        object_key,
        expected_size_bytes,
        writer,
    })
}

/// How one run keys what it lands: by provider file id, or by content through a spool.
#[derive(Clone, Copy)]
enum PayloadKey<'s> {
    ProviderFileId,
    ContentAddressed(&'s SpoolDir),
}

impl<'s> PayloadKey<'s> {
    fn for_run(bronze_key: BronzeKeyForm, spool: Option<&'s SpoolDir>) -> anyhow::Result<Self> {
        match (bronze_key, spool) {
            (BronzeKeyForm::ProviderFileId, _) => Ok(Self::ProviderFileId),
            (BronzeKeyForm::ContentAddressed, Some(spool)) => Ok(Self::ContentAddressed(spool)),
            (BronzeKeyForm::ContentAddressed, None) => {
                bail!("{BRONZE_KEY_ENV}=content_addressed requires {SPOOL_DIR_ENV}")
            }
        }
    }
}

/// The spool a content-addressed run writes through, opened once per run so its reservations are
/// shared by every file in flight.
fn open_spool(config: &VWorldDatasetFileIngestConfig) -> anyhow::Result<Option<SpoolDir>> {
    match (config.bronze_key, config.spool_dir.as_deref()) {
        (BronzeKeyForm::ContentAddressed, Some(path)) => SpoolDir::open(path).map(Some),
        (BronzeKeyForm::ContentAddressed, None) => {
            bail!("{BRONZE_KEY_ENV}=content_addressed requires {SPOOL_DIR_ENV}")
        }
        (BronzeKeyForm::ProviderFileId, _) => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
async fn persist_file_stream_with_adapters<Uow, Storage>(
    job: &VWorldDatasetFileJob,
    inventory_file: &VWorldDatasetFileInventoryItem,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    file: VWorldDatasetFileStream,
    payload_key: PayloadKey<'_>,
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
    let identity = streamed_file_identity(job, inventory_file, provider_file_name.clone());
    let content_type = file.content_type.clone();
    let PlannedPayload {
        object_key,
        expected_size_bytes,
        writer,
    } = plan_payload(
        &location,
        &identity.provider_file_id,
        file,
        payload_key,
        storage,
    )
    .await?;
    let run = uow
        .create_ingestion_run(&ingestion_run(
            source.id,
            run_id,
            started_at,
            initial_batch_request_params(
                job,
                inventory_file,
                &provider_file_name,
                object_key.as_str(),
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
    let planned = PlannedStreamingBronzeObject {
        cache_control: BRONZE_CACHE_CONTROL.to_owned(),
        expected_size_bytes,
        record: StreamingBronzeRecord {
            object_key,
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
        object_key: outcome.object_key,
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
        provider: "vworld.kr".to_owned(),
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
