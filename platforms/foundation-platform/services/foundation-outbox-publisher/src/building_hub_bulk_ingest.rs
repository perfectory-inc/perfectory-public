//! `hub.go.kr` bulk-file Bronze ingestion commands.

use std::{fs, path::PathBuf};

use anyhow::{bail, Context};
use chrono::Utc;
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
    IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use collection_infrastructure::{
    BuildingHubBulkClient, BuildingHubBulkConfig, BuildingHubBulkDownloadRequest,
    BuildingHubBulkFileStream, PgBronzeIngestRepository, PgBronzeIngestUnitOfWork,
};
use foundation_outbox::ObjectStorageStreamingService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::bronze_streaming_object_storage_from_env;
use crate::bulk_streaming_bronze::BronzeStreamingObjectStorageWriter;
use crate::public_data_control_support::{optional_env_value, required_env_value};

const DEFAULT_BASE_URI: &str = "https://www.hub.go.kr";
const DEFAULT_TERMS_URL: &str = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do";
const DEFAULT_USER_AGENT: &str = "foundation-platform-building-hub-bulk-ingestor/1.0";
const DEFAULT_COLLECTION_PLAN_PATH: &str = "target/audit/building-hub-bulk-collection-plan.json";
const DEFAULT_COLLECTION_EVIDENCE_PATH: &str =
    "target/audit/building-hub-bulk-collection-ingest-evidence.json";
const COLLECTION_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.building_hub_bulk_collection_ingest_evidence.v1";

/// Runs one `hub.go.kr` bulk-file Bronze ingestion.
pub async fn run() -> anyhow::Result<()> {
    let config = BuildingHubBulkIngestConfig::from_env()?;
    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent.clone(),
    })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let now = Utc::now();
    let file = client
        .open_file_stream(&BuildingHubBulkDownloadRequest {
            file_id: config.provider_file_id.clone(),
        })
        .await
        .context("failed to open hub.go.kr bulk file stream")?;

    if !live_write_enabled(config.live_write.as_deref()) {
        let location = plan_streamed_file_location(
            &config,
            run_id,
            now.date_naive(),
            file.provider_file_name.clone(),
        )
        .context("failed to plan hub.go.kr bulk Bronze file location")?;
        tracing::info!(
            source_slug = %config.source_slug,
            operation = %config.operation,
            provider_file_period = %config.provider_file_period,
            provider_file_id = %config.provider_file_id,
            object_key = %location.object_key.as_str(),
            size_bytes = ?file.expected_size_bytes,
            "hub.go.kr bulk Bronze ingest dry run succeeded"
        );
        return Ok(());
    }

    // Fail fast (and log the resolved target) before the file body streams to the first put.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("hub.go.kr bulk live-write target preflight failed")?;
    persist_bulk_file_stream(run_id, now, &config, file)
        .await
        .map(|_| ())
}

/// Runs a collection plan produced by `plan-building-hub-bulk-collection`.
pub async fn run_collection() -> anyhow::Result<()> {
    let config = BuildingHubBulkCollectionIngestConfig::from_env()?;
    let plan = read_collection_plan(&config.plan_path)?;
    let selected_jobs = select_collection_jobs(&plan.jobs, config.max_jobs)?;
    if selected_jobs.len() == plan.jobs.len() && !config.full_download_confirmed {
        bail!(
            "full hub.go.kr bulk collection requires FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_CONFIRM_FULL_DOWNLOAD=1"
        );
    }

    let live_write = live_write_enabled(config.live_write.as_deref());
    let mut indexed_job_reports = if live_write {
        // Fail fast (and log the resolved target) before any large provider download starts,
        // instead of discovering a misconfigured R2 target mid-run on the first put.
        crate::bronze_object_storage::live_write_target_preflight()
            .context("hub.go.kr bulk collection live-write target preflight failed")?;
        let pool =
            PgPool::connect(&std::env::var("DATABASE_URL").context("DATABASE_URL is required")?)
                .await
                .context("failed to connect to database for hub.go.kr bulk collection ingest")?;
        let repo = PgBronzeIngestRepository::new(pool.clone());
        let uow = PgBronzeIngestUnitOfWork::new(pool);
        let storage = bronze_streaming_object_storage_from_env()
            .await
            .context("failed to configure object storage for hub.go.kr bulk collection ingest")?;
        stream::iter(selected_jobs.iter().cloned().enumerate())
            .map(|(index, job)| {
                let config = config.clone();
                let repo = &repo;
                let uow = &uow;
                let storage = storage.as_ref();
                async move {
                    let started_at = Utc::now();
                    let report = match ingest_collection_job_with_adapters(
                        &job, &config, repo, uow, storage,
                    )
                    .await
                    {
                        Ok(report) => report,
                        Err(error) => failed_collection_job_report(&job, started_at, error),
                    };
                    (index, report)
                }
            })
            .buffer_unordered(config.max_in_flight)
            .collect::<Vec<_>>()
            .await
    } else {
        stream::iter(selected_jobs.iter().cloned().enumerate())
            .map(|(index, job)| {
                let config = config.clone();
                async move {
                    let started_at = Utc::now();
                    let report = match ingest_collection_job(&job, &config).await {
                        Ok(report) => report,
                        Err(error) => failed_collection_job_report(&job, started_at, error),
                    };
                    (index, report)
                }
            })
            .buffer_unordered(config.max_in_flight)
            .collect::<Vec<_>>()
            .await
    };
    indexed_job_reports.sort_by_key(|(index, _)| *index);
    let job_reports = indexed_job_reports
        .into_iter()
        .map(|(_, report)| report)
        .collect::<Vec<_>>();

    let failed_job_count = job_reports
        .iter()
        .filter(|report| report.status == "failed")
        .count() as u64;
    let evidence = BuildingHubBulkCollectionIngestEvidence {
        schema_version: COLLECTION_EVIDENCE_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        status: if failed_job_count == 0 {
            "ready"
        } else {
            "blocked"
        },
        plan_path: config.plan_path.to_string_lossy().replace('\\', "/"),
        selected_job_count: job_reports.len() as u64,
        max_in_flight: config.max_in_flight,
        succeeded_job_count: job_reports
            .iter()
            .filter(|report| report.status == "succeeded")
            .count() as u64,
        skipped_job_count: job_reports
            .iter()
            .filter(|report| report.status == "skipped_existing")
            .count() as u64,
        failed_job_count,
        live_write_enabled: live_write,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        jobs: job_reports,
    };
    write_collection_evidence(&config.evidence_path, &evidence)?;

    if evidence.failed_job_count > 0 {
        bail!(
            "hub.go.kr bulk collection blocked selected_jobs={} failed={} report={}",
            evidence.selected_job_count,
            evidence.failed_job_count,
            config.evidence_path.display()
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingHubBulkIngestConfig {
    source_slug: String,
    source_name: String,
    provider: String,
    dataset_name: String,
    base_uri: String,
    terms_url: Option<String>,
    operation: String,
    provider_file_period: String,
    provider_file_id: String,
    user_agent: String,
    live_write: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingHubBulkCollectionIngestConfig {
    plan_path: PathBuf,
    evidence_path: PathBuf,
    max_jobs: Option<usize>,
    max_in_flight: usize,
    user_agent: String,
    live_write: Option<String>,
    full_download_confirmed: bool,
}

impl BuildingHubBulkCollectionIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            plan_path: optional_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH",
            )?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_COLLECTION_PLAN_PATH)),
            evidence_path: optional_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_EVIDENCE_PATH",
            )?
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_COLLECTION_EVIDENCE_PATH)),
            max_jobs: optional_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_JOBS",
            )?
            .map(|value| {
                parse_positive_usize(
                    "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_JOBS",
                    &value,
                )
            })
            .transpose()?,
            max_in_flight: parse_collection_max_in_flight(optional_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_IN_FLIGHT",
            )?)?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            live_write: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_LIVE_WRITE")?,
            full_download_confirmed: live_write_enabled(
                optional_env_value(
                    "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_CONFIRM_FULL_DOWNLOAD",
                )?
                .as_deref(),
            ),
        })
    }
}

impl BuildingHubBulkIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        let provider = optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER")?
            .unwrap_or_else(|| "hub.go.kr".to_owned());
        let operation = required_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_OPERATION")?;
        // hub.go.kr / VWorld / mois / factoryon / juso operations are already canonical snake_case
        // (`dataset_slug == operation`, ADR 0014 D3), so the supplied source_slug must equal the
        // generated canonical `{providerid}__{operation}`. These three envs are otherwise wired
        // independently with no consistency check; validate them together so a stale source_slug
        // can never reach the write path.
        let source_slug = crate::public_data_control_support::resolve_canonical_source_slug(
            "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_SOURCE_SLUG",
            Some(required_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_SOURCE_SLUG",
            )?),
            &provider,
            &operation,
        )?;
        Ok(Self {
            source_slug,
            source_name: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_SOURCE_NAME")?
                .unwrap_or_else(|| "hub.go.kr bulk file".to_owned()),
            provider,
            dataset_name: required_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_DATASET_NAME")?,
            base_uri: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            terms_url: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_TERMS_URL")?
                .or_else(|| Some(DEFAULT_TERMS_URL.to_owned())),
            operation,
            provider_file_period: required_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER_FILE_PERIOD",
            )?,
            provider_file_id: required_env_value(
                "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_PROVIDER_FILE_ID",
            )?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            live_write: optional_env_value("FOUNDATION_PLATFORM_BUILDING_HUB_BULK_LIVE_WRITE")?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct BuildingHubBulkCollectionPlanFile {
    jobs: Vec<BuildingHubBulkCollectionPlanJob>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct BuildingHubBulkCollectionPlanJob {
    catalog_binding_status: String,
    endpoint_slug: String,
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    terms_url: Option<String>,
    operation: String,
    provider_file_period: String,
    provider_file_id: String,
    category_name: String,
    service_name: String,
    service_period_label: String,
    task_group_code: String,
    task_code: String,
}

#[derive(Debug, Serialize)]
struct BuildingHubBulkCollectionIngestEvidence {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    plan_path: String,
    selected_job_count: u64,
    max_in_flight: usize,
    succeeded_job_count: u64,
    skipped_job_count: u64,
    failed_job_count: u64,
    live_write_enabled: bool,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    jobs: Vec<BuildingHubBulkCollectionJobEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BuildingHubBulkCollectionJobEvidence {
    source_slug: String,
    provider_file_id: String,
    status: String,
    object_key: Option<String>,
    size_bytes: Option<u64>,
    error_message: Option<String>,
    duration_ms: u64,
}

async fn ingest_collection_job(
    job: &BuildingHubBulkCollectionPlanJob,
    collection_config: &BuildingHubBulkCollectionIngestConfig,
) -> anyhow::Result<BuildingHubBulkCollectionJobEvidence> {
    let started_at = Utc::now();
    let config = collection_job_to_ingest_config(
        job,
        &collection_config.user_agent,
        collection_config.live_write.clone(),
    );
    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent.clone(),
    })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let file = client
        .open_file_stream(&BuildingHubBulkDownloadRequest {
            file_id: config.provider_file_id.clone(),
        })
        .await
        .context("failed to open hub.go.kr bulk collection file stream")?;
    let location = plan_streamed_file_location(
        &config,
        run_id,
        started_at.date_naive(),
        file.provider_file_name.clone(),
    )
    .context("failed to plan hub.go.kr bulk collection Bronze file location")?;
    let object_key = location.object_key.as_str().to_owned();
    let expected_size_bytes = file.expected_size_bytes;

    let size_bytes = if live_write_enabled(config.live_write.as_deref()) {
        // Single-job live-write path: validate + log the resolved R2 target before the first put.
        // (`run_collection` routes live writes through the adapter branch, which preflights once up
        // front; this guards the path so no live write can ever reach storage without a preflight.)
        crate::bronze_object_storage::live_write_target_preflight()
            .context("hub.go.kr bulk collection job live-write target preflight failed")?;
        Some(
            persist_bulk_file_stream(run_id, started_at, &config, file)
                .await?
                .size_bytes,
        )
    } else {
        expected_size_bytes
    };

    Ok(BuildingHubBulkCollectionJobEvidence {
        source_slug: job.source_slug.clone(),
        provider_file_id: job.provider_file_id.clone(),
        status: "succeeded".to_owned(),
        object_key: Some(object_key),
        size_bytes,
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    })
}

async fn ingest_collection_job_with_adapters<Repo, Uow, Storage>(
    job: &BuildingHubBulkCollectionPlanJob,
    collection_config: &BuildingHubBulkCollectionIngestConfig,
    repo: &Repo,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<BuildingHubBulkCollectionJobEvidence>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let started_at = Utc::now();
    let config = collection_job_to_ingest_config(
        job,
        &collection_config.user_agent,
        collection_config.live_write.clone(),
    );
    // Pre-download skip: if a Bronze object already exists for this job's `source_partition_key`
    // (which includes `provider_file_id`), skip the download. This is a request-fingerprint
    // optimization (per docs/catalog/source-change-detection-policy.md) and is correct ONLY because
    // `provider_file_id` is content-stable for hub.go.kr (each published file gets a fresh OPN id, so
    // new content ⇒ new id). If a provider ever reused a file id with changed bytes, this skip would
    // miss the change — set FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH=1 to bypass it and force the
    // post-download SHA256 content check (the policy's correctness baseline). First re-collect on an
    // empty DB never hits this skip.
    let force_refetch = crate::public_data_control_support::bronze_force_refetch_enabled()?;
    if let Some(existing) =
        pre_download_skip_report(force_refetch, job, &config, started_at, repo, uow).await?
    {
        return Ok(existing);
    }

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent.clone(),
    })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let file = client
        .open_file_stream(&BuildingHubBulkDownloadRequest {
            file_id: config.provider_file_id.clone(),
        })
        .await
        .context("failed to open hub.go.kr bulk collection file stream")?;
    let location = plan_streamed_file_location(
        &config,
        run_id,
        started_at.date_naive(),
        file.provider_file_name.clone(),
    )
    .context("failed to plan hub.go.kr bulk collection Bronze file location")?;
    let object_key = location.object_key.as_str().to_owned();
    let size_bytes =
        persist_bulk_file_stream_with_adapters(&config, run_id, started_at, file, uow, storage)
            .await?
            .size_bytes;

    Ok(BuildingHubBulkCollectionJobEvidence {
        source_slug: job.source_slug.clone(),
        provider_file_id: job.provider_file_id.clone(),
        status: "succeeded".to_owned(),
        object_key: Some(object_key),
        size_bytes: Some(size_bytes),
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    })
}

/// Resolves the pre-download skip decision: when `force_refetch` is set the existence check is
/// bypassed entirely (no DB query) so the caller re-downloads and re-runs the post-download content
/// check; otherwise it consults [`existing_collection_job_report`] and skips on a hit. Factored out
/// so the bypass-vs-skip decision is unit-testable without a download client.
async fn pre_download_skip_report<Repo, Uow>(
    force_refetch: bool,
    job: &BuildingHubBulkCollectionPlanJob,
    config: &BuildingHubBulkIngestConfig,
    started_at: chrono::DateTime<Utc>,
    repo: &Repo,
    uow: &Uow,
) -> anyhow::Result<Option<BuildingHubBulkCollectionJobEvidence>>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    if force_refetch {
        return Ok(None);
    }
    existing_collection_job_report(job, config, started_at, repo, uow)
        .await
        .context("failed to check existing hub.go.kr bulk Bronze object")
}

async fn existing_collection_job_report<Repo, Uow>(
    job: &BuildingHubBulkCollectionPlanJob,
    config: &BuildingHubBulkIngestConfig,
    started_at: chrono::DateTime<Utc>,
    repo: &Repo,
    uow: &Uow,
) -> anyhow::Result<Option<BuildingHubBulkCollectionJobEvidence>>
where
    Repo: BronzeIngestRepository + ?Sized,
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(config, started_at))
        .await
        .context("failed to upsert hub.go.kr bulk source catalog entry before resume check")?;
    let source_partition_key =
        public_data_bulk_file_source_partition_key(PublicDataBulkFileSourcePartitionKeyInput {
            operation: &config.operation,
            provider_file_id: &config.provider_file_id,
        })
        .context("failed to plan hub.go.kr bulk source partition key")?;
    let existing = repo
        .find_bronze_object_by_source_partition_key(source.id, &source_partition_key)
        .await
        .with_context(|| {
            format!(
                "failed to query existing hub.go.kr Bronze object for {}",
                source_partition_key
            )
        })?;

    Ok(existing.map(|object| BuildingHubBulkCollectionJobEvidence {
        source_slug: job.source_slug.clone(),
        provider_file_id: job.provider_file_id.clone(),
        status: "skipped_existing".to_owned(),
        object_key: Some(object.object_key.as_str().to_owned()),
        size_bytes: Some(object.size_bytes),
        error_message: None,
        duration_ms: elapsed_millis(started_at),
    }))
}

fn failed_collection_job_report(
    job: &BuildingHubBulkCollectionPlanJob,
    started_at: chrono::DateTime<Utc>,
    error: anyhow::Error,
) -> BuildingHubBulkCollectionJobEvidence {
    BuildingHubBulkCollectionJobEvidence {
        source_slug: job.source_slug.clone(),
        provider_file_id: job.provider_file_id.clone(),
        status: "failed".to_owned(),
        object_key: None,
        size_bytes: None,
        error_message: Some(truncate_failure_message(&format!("{error:#}"))),
        duration_ms: elapsed_millis(started_at),
    }
}

fn collection_job_to_ingest_config(
    job: &BuildingHubBulkCollectionPlanJob,
    user_agent: &str,
    live_write: Option<String>,
) -> BuildingHubBulkIngestConfig {
    BuildingHubBulkIngestConfig {
        source_slug: job.source_slug.clone(),
        source_name: job.source_name.clone(),
        provider: "hub.go.kr".to_owned(),
        dataset_name: job.dataset_name.clone(),
        base_uri: job.base_uri.clone(),
        terms_url: job.terms_url.clone(),
        operation: job.operation.clone(),
        provider_file_period: job.provider_file_period.clone(),
        provider_file_id: job.provider_file_id.clone(),
        user_agent: user_agent.to_owned(),
        live_write,
    }
}

fn read_collection_plan(path: &PathBuf) -> anyhow::Result<BuildingHubBulkCollectionPlanFile> {
    let content = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read hub.go.kr bulk collection plan: {}",
            path.display()
        )
    })?;
    serde_json::from_str(&content).context("failed to parse hub.go.kr bulk collection plan")
}

fn select_collection_jobs(
    jobs: &[BuildingHubBulkCollectionPlanJob],
    max_jobs: Option<usize>,
) -> anyhow::Result<&[BuildingHubBulkCollectionPlanJob]> {
    if jobs.is_empty() {
        bail!("hub.go.kr bulk collection plan contains no jobs");
    }
    let end = max_jobs.unwrap_or(jobs.len()).min(jobs.len());
    let selected = &jobs[..end];
    // Duplicate partition identities would race the skip-resume check once jobs run through
    // buffer_unordered (check-then-write without a DB claim), so refuse them up front.
    let mut seen = std::collections::HashSet::new();
    for job in selected {
        if !seen.insert((
            job.source_slug.as_str(),
            job.operation.as_str(),
            job.provider_file_period.as_str(),
            job.provider_file_id.as_str(),
        )) {
            bail!(
                "hub.go.kr bulk collection plan contains a duplicate partition identity                  source_slug={} operation={} period={} provider_file_id={}",
                job.source_slug,
                job.operation,
                job.provider_file_period,
                job.provider_file_id
            );
        }
    }
    Ok(selected)
}

fn parse_collection_max_in_flight(raw: Option<String>) -> anyhow::Result<usize> {
    raw.map(|value| {
        parse_positive_usize(
            "FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_MAX_IN_FLIGHT",
            &value,
        )
    })
    .transpose()
    .map(|value| value.unwrap_or(4))
}

fn write_collection_evidence(
    path: &PathBuf,
    evidence: &BuildingHubBulkCollectionIngestEvidence,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create hub.go.kr bulk collection evidence directory: {}",
                parent.display()
            )
        })?;
    }
    fs::write(path, serde_json::to_vec_pretty(evidence)?).with_context(|| {
        format!(
            "failed to write hub.go.kr bulk collection evidence: {}",
            path.display()
        )
    })
}

fn plan_streamed_file_location(
    config: &BuildingHubBulkIngestConfig,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
    provider_file_name: String,
) -> anyhow::Result<PublicDataBulkFileStorageLocationPlan> {
    plan_public_data_bulk_file_storage_location(&PublicDataBulkFileStorageLocationInput {
        source_slug: &config.source_slug,
        ingest_date,
        ingestion_run_id: run_id,
        identity: streamed_file_identity(config, provider_file_name),
    })
    .map_err(anyhow::Error::from)
}

fn streamed_file_identity(
    config: &BuildingHubBulkIngestConfig,
    provider_file_name: String,
) -> PublicDataBulkFileIdentity {
    PublicDataBulkFileIdentity {
        operation: config.operation.clone(),
        provider_file_period: Some(config.provider_file_period.clone()),
        provider_snapshot_date: None,
        provider_file_id: config.provider_file_id.clone(),
        provider_file_name,
        provider_updated_at: None,
    }
}

async fn persist_bulk_file_stream(
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    config: &BuildingHubBulkIngestConfig,
    file: BuildingHubBulkFileStream,
) -> anyhow::Result<BuildingHubBulkPersistReport> {
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for hub.go.kr bulk ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_streaming_object_storage_from_env()
        .await
        .context("failed to configure object storage for hub.go.kr bulk ingest")?;
    let report = persist_bulk_file_stream_with_adapters(
        config,
        run_id,
        started_at,
        file,
        &uow,
        storage.as_ref(),
    )
    .await?;

    tracing::info!(
        run_id = %report.run_id,
        last_object_key = ?report.last_object_key,
        last_bronze_object_id = ?report.last_bronze_object_id,
        logical_record_count = report.logical_records_seen,
        objects_written = report.objects_written,
        "hub.go.kr bulk Bronze ingest live write succeeded"
    );
    Ok(report)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildingHubBulkPersistReport {
    run_id: IngestionRunId,
    last_object_key: Option<String>,
    last_bronze_object_id: Option<BronzeObjectId>,
    size_bytes: u64,
    logical_records_seen: u64,
    objects_written: u64,
}

async fn persist_bulk_file_stream_with_adapters<Uow, Storage>(
    config: &BuildingHubBulkIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    file: BuildingHubBulkFileStream,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<BuildingHubBulkPersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(config, started_at))
        .await
        .context("failed to upsert hub.go.kr bulk source catalog entry")?;
    let provider_file_name = file.provider_file_name.clone();
    let location = plan_streamed_file_location(
        config,
        run_id,
        started_at.date_naive(),
        provider_file_name.clone(),
    )
    .context("failed to plan hub.go.kr bulk Bronze object location")?;
    let run = uow
        .create_ingestion_run(&ingestion_run(
            source.id,
            run_id,
            started_at,
            initial_batch_request_params(config, &provider_file_name, location.object_key.as_str()),
        ))
        .await
        .context("failed to create hub.go.kr bulk ingestion run")?;

    // Route the streaming write + record through the SINGLE Bronze committer (ADR 0016): it streams
    // the body write-once (`CreateOnly` / `If-None-Match: *`, computing sha256 in-flight), then
    // records the `bronze_object` row from that streamed checksum + size — and on a 412 collision
    // runs the streaming recovery (idempotent skip if the row exists, else GET-rehash + recover).
    // Behaviour for the normal (key-absent) path is identical to the previous inline
    // stream-then-record: same object key, same streamed bytes, same recorded row, same size.
    let identity = streamed_file_identity(config, provider_file_name);
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
            dedupe_key_prefix: format!("{}:{}", config.source_slug, location.source_identity_key),
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
        .context("failed to commit hub.go.kr bulk Bronze object")
    {
        Ok(outcome) => {
            objects_written += 1;
            outcome
        }
        Err(error) => {
            return Err(mark_run_failed_after_error(uow, run.id, objects_written, error).await);
        }
    };

    let completed = uow
        .complete_ingestion_run(CompleteIngestionRunCommand {
            id: run.id,
            status: IngestionRunStatus::Succeeded,
            finished_at: Utc::now(),
            logical_records_seen: 0,
            objects_written,
            error_message: None,
        })
        .await
        .context("failed to complete hub.go.kr bulk ingestion run")?;

    Ok(BuildingHubBulkPersistReport {
        run_id: completed.id,
        last_object_key: Some(outcome.object_key),
        last_bronze_object_id: Some(outcome.bronze_object_id),
        size_bytes: outcome.size_bytes,
        logical_records_seen: completed.logical_records_seen,
        objects_written: completed.objects_written,
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
            "also failed to mark hub.go.kr bulk ingestion run {run_id} as failed: {failure_error}"
        )),
    }
}

fn source_catalog_entry(
    config: &BuildingHubBulkIngestConfig,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: config.source_name.clone(),
        provider: config.provider.clone(),
        dataset_name: config.dataset_name.clone(),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::Manual,
        payload_format: SourcePayloadFormat::Zip,
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
    config: &BuildingHubBulkIngestConfig,
    provider_file_name: &str,
    object_key: &str,
) -> JsonValue {
    json!({
        "sourceAcquisitionLane": "bulk_file",
        "operation": config.operation,
        "providerFilePeriod": config.provider_file_period,
        "providerFileId": config.provider_file_id,
        "providerFileName": provider_file_name,
        "objectKey": object_key
    })
}

fn live_write_enabled(value: Option<&str>) -> bool {
    matches!(value, Some("1"))
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
