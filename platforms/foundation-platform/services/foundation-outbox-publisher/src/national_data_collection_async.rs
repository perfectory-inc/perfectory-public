//! Async national data collection ledger executor.

use std::{process::Command, sync::Arc, time::Instant};

use anyhow::{bail, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_infrastructure::{
    DataGoKrBuildingRegisterClient, DataGoKrBuildingRegisterConfig, PgBronzeIngestUnitOfWork,
};
use foundation_outbox::{
    CollectionJob, CollectionSuccess, ObjectStorageService, OutboxRawWrittenSink, RawWrittenSink,
};
use futures_util::{stream, StreamExt};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use foundation_shared_kernel::events::catalog_v1::CollectionRawWrittenV1;
use foundation_shared_kernel::ids::IngestionRunId;

use crate::bronze_object_storage::bronze_object_storage_from_env;
use crate::provider_rate_limiter::{OutcomeSignal, ProviderRateLimiter, ReserveOutcome};

mod bronze_ingest;
mod building_register;
mod config;
mod env;
mod events;
mod evidence;
mod ledger;
mod ledger_job_bus;
mod page_queue;
mod plan;
mod real_transaction;

use bronze_ingest::BronzeIngestContext;
use config::AsyncExecutorConfig;
use env::required_env_value;
use events::{failed_event, started_event, succeeded_event, EventWriter};
use evidence::{
    write_json_file, AdaptiveExecutionEvidence, EvidenceCircuitBreaker, EvidenceEventLog,
    EvidencePlan, ExecutionEvidence, EXECUTION_SCHEMA_VERSION,
};
use ledger::{read_ledger_entries, read_succeeded_job_ids, select_pending_jobs, LedgerEntry};
use page_queue::{aggregate_page_queue_results, PageTask, PageTaskOutcome};
use plan::read_plan;
use real_transaction::RealTransactionServiceApiClients;

const DEFAULT_BASE_URI: &str = "https://apis.data.go.kr/1613000/BldRgstHubService";
const BRONZE_JSON_CONTENT_TYPE: &str = "application/json";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";

pub async fn run() -> anyhow::Result<()> {
    let config = AsyncExecutorConfig::from_env()?;
    config.validate_execution_confirmation()?;
    if config.evidence_path.exists() || config.event_log_path.exists() {
        bail!("national async ledger execution output already exists");
    }

    let plan = read_plan(&config.plan_path)?;
    let entries = read_ledger_entries(&plan.ledger_path)?;
    let succeeded = read_succeeded_job_ids(&config.evidence_scan_dir, &plan.compiler_input_hash)?;
    let selected = select_pending_jobs(&entries, &succeeded, config.max_jobs, config.request_cap)?;

    let rate_limiter = ProviderRateLimiter::from_env()?.map(Arc::new);
    let (selected, deferred_due_to_lane_budget) =
        partition_by_lane_budget(rate_limiter.as_deref(), selected)?;
    if selected.is_empty() {
        bail!(
            "national async: all {deferred_due_to_lane_budget} pending jobs deferred by lane budget; nothing to run"
        );
    }

    let data_go_kr_service_key = required_env_value("DATA_GO_KR_SERVICE_KEY")?;
    let data_go_kr_request_policy = config.data_go_kr_request_policy()?;
    let real_transaction_clients = Arc::new(RealTransactionServiceApiClients::build(
        &selected,
        &data_go_kr_service_key,
        data_go_kr_request_policy,
    )?);
    let client = Arc::new(DataGoKrBuildingRegisterClient::new_with_policy(
        &DataGoKrBuildingRegisterConfig {
            base_uri: config.base_uri.clone(),
            service_key: data_go_kr_service_key.clone(),
        },
        data_go_kr_request_policy,
    )?);
    // This async executor always writes Bronze objects (no dry-run branch), so the whole run is a
    // live-write path: validate + log the resolved R2 target before the first put, instead of
    // discovering a misconfigured target mid-run after pages have already been fetched.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("national async collection live-write target preflight failed")?;
    let storage: Arc<dyn ObjectStorageService> = Arc::from(bronze_object_storage_from_env().await?);
    let event_writer = EventWriter::open(&config.event_log_path)?;
    let git_head = git_head();
    let started_at = Utc::now();

    // DATABASE_URL is now ALWAYS required: routing every async page write through the single
    // BronzeCommitter (ADR 0016 option-a) means the lane records a `bronze_object` row, and that row
    // carries NOT-NULL FKs to `catalog.source_catalog` + `catalog.ingestion_run` — so the executor
    // needs a Postgres pool to upsert those and to record the rows. (Previously the DB was optional,
    // only used by the raw_written outbox sink.) One pool is shared: the `BronzeIngestContext`'s
    // unit-of-work and the optional raw_written sink both clone it (sqlx `PgPool` is a `Clone`
    // connection pool, `Send`+`Sync`), and the committer is stateless — so every `buffer_unordered`
    // page task shares one pool + one committer with no per-task connection setup.
    let database_url = required_env_value("DATABASE_URL").context(
        "national async data collection now writes `bronze_object` rows (ADR 0016 option-a) and so requires DATABASE_URL",
    )?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to DATABASE_URL for national async Bronze commit")?;
    let uow: Arc<dyn BronzeIngestUnitOfWork> =
        Arc::new(PgBronzeIngestUnitOfWork::new(pool.clone()));
    // Sequential setup pre-pass (before the concurrent fan-out): upsert one source + create one open
    // ingestion run per distinct source_slug, so the committer's `bronze_object` writes have real FK
    // targets. The resulting per-slug identity is shared read-only across the page tasks.
    let bronze = Arc::new(
        BronzeIngestContext::prepare(
            Arc::clone(&storage),
            Arc::clone(&uow),
            &selected,
            started_at,
        )
        .await
        .context("failed to prepare national async Bronze ingest context")?,
    );

    // Optional producer sink: emit collection.raw_written into catalog.outbox_event on success (the
    // existing OutboxWorker fans it out). Still an EXPLICIT opt-in via
    // FOUNDATION_PLATFORM_NATIONAL_RAW_WRITTEN_OUTBOX=1; it now reuses the same pool the committer uses
    // (no second connection). The checksum it carries still comes from the per-job report, which is
    // sourced from the committed Bronze object's plan checksum (the same value the committer recorded
    // on the `bronze_object` row), so the event-fabric claim-check stays intact.
    let raw_written_outbox_enabled =
        std::env::var("FOUNDATION_PLATFORM_NATIONAL_RAW_WRITTEN_OUTBOX")
            .ok()
            .as_deref()
            == Some("1");
    let raw_written_sink: Option<Arc<dyn RawWrittenSink>> = if raw_written_outbox_enabled {
        Some(Arc::new(OutboxRawWrittenSink::new(pool.clone())))
    } else {
        None
    };
    let execution = if config.page_queue_enabled {
        execute_selected_jobs_page_queue(
            selected.clone(),
            &config,
            &plan.compiler_input_hash,
            Arc::clone(&client),
            Arc::clone(&bronze),
            event_writer.clone(),
            Arc::clone(&real_transaction_clients),
            raw_written_sink,
            rate_limiter.clone(),
        )
        .await?
    } else {
        execute_selected_jobs(
            selected.clone(),
            &config,
            &plan.compiler_input_hash,
            Arc::clone(&client),
            Arc::clone(&bronze),
            event_writer.clone(),
            Arc::clone(&real_transaction_clients),
            raw_written_sink,
            rate_limiter.clone(),
        )
        .await?
    };

    event_writer.flush()?;

    let summary = summarize_job_results(&execution.results);

    let status = if summary.failed_job_count == 0 {
        "ready"
    } else {
        "blocked"
    };
    let evidence = ExecutionEvidence {
        schema_version: EXECUTION_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head,
        status,
        executed: true,
        execution_strategy: execution.execution_strategy,
        selected_job_count: selected.len(),
        skipped_job_count: succeeded.len(),
        deferred_due_to_lane_budget,
        ledger_read_mode: "rust_async_full_scan",
        ledger_scanned_row_count: entries.len(),
        ledger_loaded_row_count: selected.len(),
        max_in_flight: config.max_in_flight,
        circuit_breaker: EvidenceCircuitBreaker {
            failure_threshold: config.circuit_breaker_failure_threshold,
            open_seconds: config.circuit_breaker_open_seconds,
        },
        adaptive_in_flight: execution.adaptive_in_flight,
        empty_job_count: 0,
        reused_job_count: 0,
        succeeded_job_count: summary.succeeded_job_count,
        failed_job_count: summary.failed_job_count,
        request_count_total: summary.request_count_total,
        provider_request_count_total: summary.provider_request_count_total,
        raw_response_preserved: summary.provider_request_count_total > 0,
        source_record_count: summary.source_record_count,
        bronze_total_size_bytes: summary.bronze_total_size_bytes,
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        plan: EvidencePlan {
            path: config.plan_path.to_string_lossy().replace('\\', "/"),
            compiler_input_hash_sha256: plan.compiler_input_hash,
        },
        event_log: EvidenceEventLog {
            path: config.event_log_path.to_string_lossy().replace('\\', "/"),
            entry_count: event_writer.entry_count()?,
        },
        evidence_limitations: vec![
            "ledger_execution_slice_only",
            "does_not_promote_silver_gold_national_tables",
            "does_not_approve_production_cutover",
            "does_not_mark_national_rollout_complete",
        ],
        next_gates: vec!["silver-gold-national-promotion"],
    };
    write_json_file(&config.evidence_path, &evidence)?;

    if summary.failed_job_count > 0 {
        bail!(
            "national async data collection blocked jobs={} succeeded={} failed={} report={}",
            selected.len(),
            summary.succeeded_job_count,
            summary.failed_job_count,
            config.evidence_path.display()
        );
    }

    tracing::info!(
        jobs = selected.len(),
        succeeded = summary.succeeded_job_count,
        failed = summary.failed_job_count,
        provider_requests = summary.provider_request_count_total,
        source_records = summary.source_record_count,
        elapsed_seconds = (Utc::now() - started_at).num_seconds(),
        report = %config.evidence_path.display(),
        "national async data collection completed"
    );
    println!(
        "national-data-collection-async-ledger-execution-ok status=ready jobs={} succeeded={} failed={} requests={} report={}",
        selected.len(),
        summary.succeeded_job_count,
        summary.failed_job_count,
        summary.provider_request_count_total,
        config.evidence_path.display()
    );
    Ok(())
}

async fn execute_selected_jobs(
    selected: Vec<LedgerEntry>,
    config: &AsyncExecutorConfig,
    compiler_input_hash: &str,
    client: Arc<DataGoKrBuildingRegisterClient>,
    bronze: Arc<BronzeIngestContext>,
    event_writer: EventWriter,
    real_transaction_clients: Arc<RealTransactionServiceApiClients>,
    raw_written_sink: Option<Arc<dyn RawWrittenSink>>,
    rate_limiter: Option<Arc<ProviderRateLimiter>>,
) -> anyhow::Result<JobExecutionRun> {
    if !config.adaptive_in_flight.enabled {
        let results = execute_job_window(
            selected,
            config.max_in_flight,
            compiler_input_hash,
            client,
            bronze,
            event_writer,
            real_transaction_clients,
            raw_written_sink,
            rate_limiter.clone(),
        )
        .await;
        return Ok(JobExecutionRun {
            results,
            execution_strategy: "job_window_fixed",
            adaptive_in_flight: AdaptiveExecutionEvidence {
                enabled: false,
                start_in_flight: config.max_in_flight,
                final_in_flight: config.max_in_flight,
                max_in_flight: config.max_in_flight,
                window_count: 1,
            },
        });
    }

    let mut cursor = 0_usize;
    let mut current_in_flight = config.adaptive_in_flight.start_in_flight;
    let mut window_count = 0_u64;
    let mut results = Vec::new();
    while cursor < selected.len() {
        let end = cursor.saturating_add(current_in_flight).min(selected.len());
        let window_entries = selected[cursor..end].to_vec();
        let mut window_results = execute_job_window(
            window_entries,
            current_in_flight,
            compiler_input_hash,
            Arc::clone(&client),
            Arc::clone(&bronze),
            event_writer.clone(),
            Arc::clone(&real_transaction_clients),
            raw_written_sink.clone(),
            rate_limiter.clone(),
        )
        .await;
        let window_summary = summarize_job_results(&window_results);
        current_in_flight = config
            .adaptive_in_flight
            .next_in_flight(current_in_flight, &window_summary);
        results.append(&mut window_results);
        window_count += 1;
        cursor = end;
    }

    Ok(JobExecutionRun {
        results,
        execution_strategy: "job_window_adaptive",
        adaptive_in_flight: AdaptiveExecutionEvidence {
            enabled: true,
            start_in_flight: config.adaptive_in_flight.start_in_flight,
            final_in_flight: current_in_flight,
            max_in_flight: config.adaptive_in_flight.max_in_flight,
            window_count,
        },
    })
}

async fn execute_job_window(
    entries: Vec<LedgerEntry>,
    max_in_flight: usize,
    compiler_input_hash: &str,
    client: Arc<DataGoKrBuildingRegisterClient>,
    bronze: Arc<BronzeIngestContext>,
    event_writer: EventWriter,
    real_transaction_clients: Arc<RealTransactionServiceApiClients>,
    raw_written_sink: Option<Arc<dyn RawWrittenSink>>,
    rate_limiter: Option<Arc<ProviderRateLimiter>>,
) -> Vec<anyhow::Result<JobSuccessReport>> {
    stream::iter(entries)
        .map(|entry| {
            let state = JobExecutionState {
                client: Arc::clone(&client),
                bronze: Arc::clone(&bronze),
                event_writer: event_writer.clone(),
                compiler_input_hash: compiler_input_hash.to_owned(),
                real_transaction_clients: Arc::clone(&real_transaction_clients),
                raw_written_sink: raw_written_sink.clone(),
                rate_limiter: rate_limiter.clone(),
            };
            async move { execute_job(entry, state).await }
        })
        .buffer_unordered(max_in_flight)
        .collect::<Vec<_>>()
        .await
}

async fn execute_selected_jobs_page_queue(
    selected: Vec<LedgerEntry>,
    config: &AsyncExecutorConfig,
    compiler_input_hash: &str,
    client: Arc<DataGoKrBuildingRegisterClient>,
    bronze: Arc<BronzeIngestContext>,
    event_writer: EventWriter,
    real_transaction_clients: Arc<RealTransactionServiceApiClients>,
    raw_written_sink: Option<Arc<dyn RawWrittenSink>>,
    rate_limiter: Option<Arc<ProviderRateLimiter>>,
) -> anyhow::Result<JobExecutionRun> {
    let mut page_tasks = Vec::new();
    for entry in &selected {
        event_writer.write_event(&started_event(entry, compiler_input_hash))?;
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let ingest_date = Utc::now().date_naive();
        for page_no in page_numbers_for_entry(entry)? {
            page_tasks.push(PageTask {
                entry: entry.clone(),
                run_id,
                ingest_date,
                page_no,
            });
        }
    }

    let state = JobExecutionState {
        client,
        bronze,
        event_writer: event_writer.clone(),
        compiler_input_hash: compiler_input_hash.to_owned(),
        real_transaction_clients,
        // Per-page execution does not emit; raw_written is emitted per-job AFTER aggregation below
        // (the aggregate yields one report per job, in `selected` order), so this stays None.
        raw_written_sink: None,
        // Limiter is per-page, independent of the raw_written sink.
        rate_limiter: rate_limiter.clone(),
    };
    // Fetch-window start for raw_written lineage (page tasks below perform the provider fetches).
    let fetched_at = Utc::now();
    let page_outcomes = stream::iter(page_tasks)
        .map(|task| {
            let state = state.clone();
            async move {
                match execute_single_page(
                    &task.entry,
                    task.page_no,
                    task.run_id,
                    task.ingest_date,
                    &state,
                )
                .await
                {
                    Ok(report) => PageTaskOutcome::Succeeded(report),
                    Err(error) => PageTaskOutcome::Failed {
                        job_id: task.entry.job_id.clone(),
                        error_message: format_error_chain(error.as_ref()),
                    },
                }
            }
        })
        .buffer_unordered(config.max_in_flight)
        .collect::<Vec<_>>()
        .await;

    let results =
        aggregate_page_queue_results(&selected, page_outcomes, compiler_input_hash, &event_writer)?;

    // Emit raw_written per successful job. aggregate_page_queue_results yields exactly one result per
    // `selected` entry, in order, so the zip pairs each entry with its report. A sink failure only
    // warns — the job_succeeded ledger event written during aggregation is authoritative
    // (at-least-once / re-derivable; true transactional-outbox atomicity is Option B).
    if let Some(sink) = &raw_written_sink {
        debug_assert_eq!(results.len(), selected.len());
        for (entry, result) in selected.iter().zip(results.iter()) {
            if let Ok(report) = result {
                let event = raw_written_event(entry, report, fetched_at, Utc::now());
                if let Err(error) = sink.emit(&event).await {
                    tracing::warn!(
                        job_id = %entry.job_id,
                        %error,
                        "raw_written emit failed after successful page-queue collection; ledger event is durable, notification is re-derivable"
                    );
                }
            }
        }
    }

    Ok(JobExecutionRun {
        results,
        execution_strategy: "page_queue_fixed",
        adaptive_in_flight: AdaptiveExecutionEvidence {
            enabled: false,
            start_in_flight: config.max_in_flight,
            final_in_flight: config.max_in_flight,
            max_in_flight: config.max_in_flight,
            window_count: 1,
        },
    })
}

#[derive(Clone)]
struct JobExecutionState {
    client: Arc<DataGoKrBuildingRegisterClient>,
    /// Shared Bronze-commit context (storage + `bronze_object` UoW + committer + per-source-slug run
    /// identity). Every page task routes its raw write through this single ADR-0016 commit seam, so
    /// the async lane writes a `bronze_object` row + gets `CreateOnly` + recovery. Cloned (`Arc`)
    /// into each `buffer_unordered` task; the underlying sqlx `PgPool` is itself a shared pool.
    bronze: Arc<BronzeIngestContext>,
    event_writer: EventWriter,
    compiler_input_hash: String,
    real_transaction_clients: Arc<RealTransactionServiceApiClients>,
    /// Optional producer sink: when present (DB configured), a `collection.raw_written` event is
    /// emitted after each job's success. `None` keeps the JSONL-only path (proofs / no DB).
    raw_written_sink: Option<Arc<dyn RawWrittenSink>>,
    /// Optional in-memory adaptive rate limiter. `None` keeps the unthrottled path (Slice 4-A opt-in).
    rate_limiter: Option<Arc<ProviderRateLimiter>>,
}

#[derive(Clone, Debug)]
struct JobSuccessReport {
    provider_request_count: u64,
    source_record_count: u64,
    bronze_size_bytes: u64,
    last_object_key: String,
    /// Lowercase hex SHA-256 of the last Bronze object written (the one named by `last_object_key`).
    last_checksum_sha256: String,
}

#[derive(Clone, Debug)]
struct PageSuccessReport {
    job_id: String,
    page_no: u32,
    effective_page_size: u32,
    logical_record_count: u64,
    provider_total_count: Option<u64>,
    source_record_count: u64,
    bronze_size_bytes: u64,
    object_key: String,
    /// Lowercase hex SHA-256 of this page's Bronze object (from the Bronze page plan).
    checksum_sha256: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct JobExecutionSummary {
    succeeded_job_count: u64,
    failed_job_count: u64,
    request_count_total: u64,
    provider_request_count_total: u64,
    source_record_count: u64,
    bronze_total_size_bytes: u64,
}

/// Project a planned `LedgerEntry` into the transport-neutral [`CollectionJob`] identity + spec.
fn to_collection_job(entry: &LedgerEntry) -> CollectionJob {
    CollectionJob {
        job_id: entry.job_id.clone(),
        scope_unit_id: entry.scope_unit_id.clone(),
        shard_id: entry.shard_id.clone(),
        provider: entry.provider.clone(),
        endpoint: entry.endpoint.clone(),
        endpoint_slug: entry.endpoint_slug.clone(),
        idempotency_key: entry.idempotency_key.clone(),
        request_fingerprint_sha256: entry.request_fingerprint_sha256.clone(),
        request_fingerprint_schema_version: entry.request_fingerprint_schema_version.clone(),
        collection_snapshot_id: entry.collection_snapshot_id.clone(),
        spec: json!({
            "operation": entry.operation,
            "sigungu_cd": entry.sigungu_cd,
            "bjdong_cd": entry.bjdong_cd,
            "lawd_cd": entry.lawd_cd,
            "deal_ymd": entry.deal_ymd,
            "page_start": entry.page_start,
            "page_end": entry.page_end,
            "max_pages": entry.max_pages,
            "num_of_rows": entry.num_of_rows,
        }),
    }
}

/// Build the `collection.raw_written` claim-check payload from a completed job's entry + report.
///
/// `srid` is `None` (this async executor collects only data.go.kr attribute-only sources) and
/// `license` is `None` (no provider records a license today; never fabricated). `fetched_at` is the
/// job's fetch time (≈ job start), kept distinct from `occurred_at` (event emit time).
fn raw_written_event(
    entry: &LedgerEntry,
    report: &JobSuccessReport,
    fetched_at: DateTime<Utc>,
    occurred_at: DateTime<Utc>,
) -> CollectionRawWrittenV1 {
    let success = CollectionSuccess {
        bronze_object_key: report.last_object_key.clone(),
        // This async executor writes exactly one Bronze object per provider request and never
        // reuses, so object_count == request_count == page count. If a non-1:1 endpoint or the
        // reuse path is ever wired here, thread a true object count through JobSuccessReport instead.
        bronze_object_count: report.provider_request_count,
        bronze_checksum_sha256: report.last_checksum_sha256.clone(),
        bronze_size_bytes: report.bronze_size_bytes,
        source_record_count: report.source_record_count,
        request_count: report.provider_request_count,
        reused_bronze_object: false,
        license: None,
        srid: None,
        fetched_at_utc: fetched_at,
    };
    success.into_raw_written(&to_collection_job(entry), occurred_at)
}

fn summarize_job_results(results: &[anyhow::Result<JobSuccessReport>]) -> JobExecutionSummary {
    let mut summary = JobExecutionSummary::default();
    for result in results {
        match result {
            Ok(report) => {
                summary.succeeded_job_count += 1;
                summary.request_count_total += report.provider_request_count;
                summary.provider_request_count_total += report.provider_request_count;
                summary.source_record_count += report.source_record_count;
                summary.bronze_total_size_bytes += report.bronze_size_bytes;
            }
            Err(_) => {
                summary.failed_job_count += 1;
            }
        }
    }
    summary
}

/// Sequential pre-pass: reserve each job's estimated requests against its lane budget. Jobs that do
/// not fit are deferred (kept out of the run list; they stay `planned` and re-select next run). When
/// `limiter` is `None`, everything runs and `deferred = 0` (behavior-preserving).
fn partition_by_lane_budget(
    limiter: Option<&ProviderRateLimiter>,
    selected: Vec<LedgerEntry>,
) -> anyhow::Result<(Vec<LedgerEntry>, u64)> {
    let Some(limiter) = limiter else {
        return Ok((selected, 0));
    };
    let mut to_run = Vec::new();
    let mut deferred = 0_u64;
    for entry in selected {
        let lane_id = limiter.resolve_lane(&entry.provider, &entry.operation)?;
        match limiter.reserve(&lane_id, u64::from(entry.request_count_estimate))? {
            ReserveOutcome::Granted => to_run.push(entry),
            ReserveOutcome::DeferredLaneBudget => deferred += 1,
        }
    }
    Ok((to_run, deferred))
}

/// Handle returned by `acquire_lane`; carries what `record_lane` needs. `started` is captured AFTER
/// the pacing wait so latency measures provider time, not our own throttle delay.
struct RateLaneHandle {
    limiter: Arc<ProviderRateLimiter>,
    lane_id: String,
    started: Instant,
}

/// Pace one upcoming fetch on `entry`'s lane (no-op when the limiter is disabled).
async fn acquire_lane(
    state: &JobExecutionState,
    entry: &LedgerEntry,
) -> anyhow::Result<Option<RateLaneHandle>> {
    match &state.rate_limiter {
        Some(limiter) => {
            let lane_id = limiter.resolve_lane(&entry.provider, &entry.operation)?;
            limiter.acquire(&lane_id).await?;
            Ok(Some(RateLaneHandle {
                limiter: Arc::clone(limiter),
                lane_id,
                started: Instant::now(),
            }))
        }
        None => Ok(None),
    }
}

/// Feed one fetch outcome back into the lane (AIMD). No-op when `handle` is `None`. Generic over the
/// page and error types so all provider clients reuse it. Pacing/budget stay split: this never
/// touches the budget counter.
fn record_lane<T, E>(handle: Option<RateLaneHandle>, result: &Result<T, E>) -> anyhow::Result<()>
where
    E: std::error::Error,
{
    if let Some(handle) = handle {
        let latency_ms = u32::try_from(handle.started.elapsed().as_millis()).unwrap_or(u32::MAX);
        let signal = match result {
            Ok(_) => OutcomeSignal::success(),
            Err(error) => OutcomeSignal::failure(format_error_chain(error)),
        };
        handle
            .limiter
            .record(&handle.lane_id, &signal, latency_ms)?;
    }
    Ok(())
}

fn page_numbers_for_entry(entry: &LedgerEntry) -> anyhow::Result<Vec<u32>> {
    let page_start = entry.page_start.unwrap_or(1);
    let page_end = entry.page_end.unwrap_or(entry.max_pages);
    if page_start == 0 || page_end < page_start {
        bail!("invalid page window for job {}", entry.job_id);
    }
    Ok((page_start..=page_end).collect())
}

struct JobExecutionRun {
    results: Vec<anyhow::Result<JobSuccessReport>>,
    adaptive_in_flight: AdaptiveExecutionEvidence,
    execution_strategy: &'static str,
}

async fn execute_job(
    entry: LedgerEntry,
    state: JobExecutionState,
) -> anyhow::Result<JobSuccessReport> {
    state
        .event_writer
        .write_event(&started_event(&entry, &state.compiler_input_hash))?;

    // Fetch time for raw_written lineage (≈ when the provider pages are fetched, below).
    let fetched_at = Utc::now();
    let result = execute_job_pages(&entry, &state).await;
    match result {
        Ok(report) => {
            // Ledger is SSOT: the success event is written first and is authoritative.
            state.event_writer.write_event(&succeeded_event(
                &entry,
                &state.compiler_input_hash,
                &report,
            ))?;
            // Then emit the raw_written claim-check (if a sink is wired). A sink failure must NOT
            // fail an already-succeeded job: the Bronze bytes + ledger event are durable and the
            // notification is at-least-once / re-derivable (ADR-0047). True atomicity is Option B.
            if let Some(sink) = &state.raw_written_sink {
                let event = raw_written_event(&entry, &report, fetched_at, Utc::now());
                if let Err(error) = sink.emit(&event).await {
                    tracing::warn!(
                        job_id = %entry.job_id,
                        %error,
                        "raw_written emit failed after successful collection; ledger event is durable, notification is re-derivable"
                    );
                }
            }
            Ok(report)
        }
        Err(error) => {
            let error_message = format_error_chain(error.as_ref());
            state.event_writer.write_event(&failed_event(
                &entry,
                &state.compiler_input_hash,
                error_message,
            ))?;
            Err(error)
        }
    }
}

async fn execute_job_pages(
    entry: &LedgerEntry,
    state: &JobExecutionState,
) -> anyhow::Result<JobSuccessReport> {
    if real_transaction::is_entry(entry) {
        return real_transaction::execute_job_pages(entry, state).await;
    }
    building_register::execute_job_pages(entry, state).await
}

async fn execute_single_page(
    entry: &LedgerEntry,
    page_no: u32,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
    state: &JobExecutionState,
) -> anyhow::Result<PageSuccessReport> {
    if real_transaction::is_entry(entry) {
        return real_transaction::execute_single_page(entry, page_no, run_id, ingest_date, state)
            .await;
    }
    building_register::execute_single_page(entry, page_no, run_id, ingest_date, state).await
}

fn json_u64_pointer(value: &JsonValue, pointer: &str) -> anyhow::Result<Option<u64>> {
    match value.pointer(pointer) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(number)) => Ok(number.as_u64()),
        Some(JsonValue::String(raw)) => raw
            .trim()
            .parse::<u64>()
            .map(Some)
            .with_context(|| format!("JSON field {pointer} must be an unsigned integer")),
        _ => bail!("JSON field {pointer} must be an unsigned integer"),
    }
}

fn effective_page_size_from_response_metadata(
    payload: &JsonValue,
    requested_page_size: u32,
) -> anyhow::Result<u32> {
    let Some(raw_page_size) = json_u64_pointer(payload, "/response/body/numOfRows")? else {
        return Ok(requested_page_size);
    };
    if raw_page_size == 0 {
        bail!("building-register response body numOfRows must be greater than zero");
    }
    u32::try_from(raw_page_size)
        .with_context(|| "building-register response body numOfRows must fit in u32")
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn git_head() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn redact_sensitive_error(error: &str) -> String {
    let mut redacted = error.to_owned();
    for token in [
        "DATA_GO_KR_SERVICE_KEY",
        "VWORLD_API_KEY",
        "R2_SECRET_ACCESS_KEY",
        "R2_ACCESS_KEY_ID",
        "serviceKey",
    ] {
        redacted = redacted.replace(token, "[redacted]");
    }
    redacted
}

fn format_error_chain(error: &dyn std::error::Error) -> String {
    let mut messages = vec![error.to_string()];
    let mut current = error.source();
    while let Some(source) = current {
        messages.push(source.to_string());
        current = source.source();
    }
    redact_sensitive_error(&messages.join(" | caused_by: "))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use anyhow::anyhow;

    use super::{select_pending_jobs, JobSuccessReport, LedgerEntry};

    fn ledger_entry(job_id: &str, request_count_estimate: u32) -> LedgerEntry {
        LedgerEntry {
            job_id: job_id.to_owned(),
            provider: "data.go.kr".to_owned(),
            endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".to_owned(),
            endpoint: "getBrTitleInfo".to_owned(),
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11110".to_owned(),
            bjdong_cd: "10100".to_owned(),
            lawd_cd: String::new(),
            deal_ymd: String::new(),
            scope_unit_id: "scope:legal-dong:1111010100".to_owned(),
            shard_id: "national-shard-0001".to_owned(),
            idempotency_key: format!("test/{job_id}"),
            source_slug: format!("source-{job_id}"),
            request_fingerprint_sha256: "a".repeat(64),
            request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
                .to_owned(),
            collection_snapshot_id: "registry:test".to_owned(),
            status: "planned".to_owned(),
            page_start: Some(1),
            page_end: Some(request_count_estimate),
            max_pages: request_count_estimate,
            num_of_rows: 100,
            request_count_estimate,
        }
    }

    #[test]
    fn succeeded_event_carries_real_bronze_checksum() {
        let entry = ledger_entry("job-x", 1);
        let checksum = "b".repeat(64);
        let report = JobSuccessReport {
            provider_request_count: 1,
            source_record_count: 5,
            bronze_size_bytes: 100,
            last_object_key: "bronze/source=x/page-000001.json".to_owned(),
            last_checksum_sha256: checksum.clone(),
        };

        let event = super::events::succeeded_event(&entry, "compiler-input-hash", &report);
        let json = serde_json::to_string(&event).unwrap();

        // The integrity digest is the real per-object sha256, not the old empty placeholder.
        assert!(json.contains(&checksum));
        assert!(!json.contains("\"bronze_checksum_sha256\":\"\""));
    }

    #[test]
    fn raw_written_event_maps_entry_and_report() {
        let entry = ledger_entry("job-y", 1);
        let report = JobSuccessReport {
            provider_request_count: 3,
            source_record_count: 50,
            bronze_size_bytes: 2_048,
            last_object_key: "bronze/source=y/page-000003.json".to_owned(),
            last_checksum_sha256: "c".repeat(64),
        };
        let fetched_at =
            chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap_or_default();
        let occurred_at =
            chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_050, 0).unwrap_or_default();

        let event = super::raw_written_event(&entry, &report, fetched_at, occurred_at);

        // Identity from the entry; artifact/counts from the report.
        assert_eq!(event.job_id, "job-y");
        assert_eq!(
            event.endpoint_slug,
            "data-go-kr-building-register-getBrTitleInfo"
        );
        assert_eq!(event.bronze_object_key, "bronze/source=y/page-000003.json");
        assert_eq!(event.bronze_checksum_sha256, "c".repeat(64));
        assert_eq!(event.bronze_object_count, 3); // = pages (provider_request_count)
        assert_eq!(event.request_count, 3);
        assert_eq!(event.source_record_count, 50);
        // Honestly absent — never fabricated (no provider records a license; data.go.kr is attribute-only).
        assert!(event.license.is_none());
        assert!(event.srid.is_none());
        assert!(!event.reused_bronze_object);
        assert_eq!(event.occurred_at, occurred_at);
        assert_eq!(event.fetched_at_utc, fetched_at);
        assert_ne!(event.fetched_at_utc, event.occurred_at);
    }

    #[test]
    fn select_pending_jobs_skips_succeeded_and_stops_before_request_cap() {
        let entries = vec![
            ledger_entry("job-a", 2),
            ledger_entry("job-b", 3),
            ledger_entry("job-c", 5),
        ];
        let succeeded = BTreeSet::from(["job-a".to_owned()]);

        let selected = select_pending_jobs(&entries, &succeeded, 10, 7).unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-b"]
        );
    }

    #[test]
    fn select_pending_jobs_rejects_first_job_over_request_cap() {
        let entries = vec![ledger_entry("job-a", 8)];
        let succeeded = BTreeSet::new();

        let error = select_pending_jobs(&entries, &succeeded, 10, 7).unwrap_err();

        assert!(error
            .to_string()
            .contains("first pending job request estimate exceeds request cap"));
    }

    #[test]
    fn format_error_chain_includes_nested_causes() {
        let error = anyhow::anyhow!("outer").context("inner");

        assert_eq!(
            super::format_error_chain(error.as_ref()),
            "inner | caused_by: outer"
        );
    }

    #[test]
    fn summarize_job_results_counts_only_succeeded_requests() {
        let results = vec![
            Ok(JobSuccessReport {
                provider_request_count: 7,
                source_record_count: 700,
                bronze_size_bytes: 1_024,
                last_object_key: "bronze/succeeded.json".to_owned(),
                last_checksum_sha256:
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            }),
            Err(anyhow!("request failed")),
        ];

        let summary = super::summarize_job_results(&results);

        assert_eq!(summary.succeeded_job_count, 1);
        assert_eq!(summary.failed_job_count, 1);
        assert_eq!(summary.request_count_total, 7);
        assert_eq!(summary.provider_request_count_total, 7);
        assert_eq!(summary.source_record_count, 700);
        assert_eq!(summary.bronze_total_size_bytes, 1_024);
    }

    #[test]
    fn adaptive_in_flight_increases_after_clean_window() {
        let config = super::config::AdaptiveInFlightConfig {
            enabled: true,
            start_in_flight: 64,
            min_in_flight: 1,
            max_in_flight: 256,
            increase_step: 16,
            decrease_percent: 50,
        };
        let summary = super::JobExecutionSummary {
            succeeded_job_count: 64,
            failed_job_count: 0,
            request_count_total: 64,
            provider_request_count_total: 64,
            source_record_count: 6_400,
            bronze_total_size_bytes: 1_024,
        };

        assert_eq!(config.next_in_flight(64, &summary), 80);
    }

    #[test]
    fn adaptive_in_flight_decreases_after_failed_window() {
        let config = super::config::AdaptiveInFlightConfig {
            enabled: true,
            start_in_flight: 128,
            min_in_flight: 8,
            max_in_flight: 256,
            increase_step: 16,
            decrease_percent: 50,
        };
        let summary = super::JobExecutionSummary {
            succeeded_job_count: 127,
            failed_job_count: 1,
            request_count_total: 127,
            provider_request_count_total: 127,
            source_record_count: 12_700,
            bronze_total_size_bytes: 1_024,
        };

        assert_eq!(config.next_in_flight(128, &summary), 64);
    }

    #[test]
    fn page_numbers_for_entry_expands_page_window() {
        let mut entry = ledger_entry("job-a", 3);
        entry.page_start = Some(11);
        entry.page_end = Some(13);
        entry.max_pages = 20;

        assert_eq!(
            super::page_numbers_for_entry(&entry).unwrap(),
            vec![11, 12, 13]
        );
    }

    #[test]
    fn page_numbers_for_entry_rejects_invalid_window() {
        let mut entry = ledger_entry("job-a", 3);
        entry.page_start = Some(0);
        entry.page_end = Some(3);

        let error = super::page_numbers_for_entry(&entry).unwrap_err();

        assert!(error.to_string().contains("invalid page window"));
    }

    #[test]
    fn real_transaction_page_request_uses_ledger_lawd_month_scope() -> anyhow::Result<()> {
        let mut entry = ledger_entry("real-transaction-job", 1);
        entry.endpoint_slug = "data-go-kr-real-transaction-getRTMSDataSvcInduTrade".to_owned();
        entry.endpoint = "getRTMSDataSvcInduTrade".to_owned();
        entry.operation = "getRTMSDataSvcInduTrade".to_owned();
        entry.lawd_cd = "11680".to_owned();
        entry.deal_ymd = "202605".to_owned();
        entry.num_of_rows = 500;

        let request = super::real_transaction::request_for_entry(&entry, 7)?;

        assert_eq!(request.operation, "getRTMSDataSvcInduTrade");
        assert_eq!(request.lawd_cd, "11680");
        assert_eq!(request.deal_ymd, "202605");
        assert_eq!(request.page_no, 7);
        assert_eq!(request.num_of_rows, 500);
        Ok(())
    }

    #[test]
    fn real_transaction_async_uses_shared_service_api_client_registry() {
        let source = include_str!("national_data_collection_async/real_transaction.rs");
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("test module marker must exist");

        assert_eq!(
            production_source
                .matches("DataGoKrServiceApiClient::new_with_policy")
                .count(),
            1,
            "real-transaction async execution must build service API clients once in a shared registry, not per job/page"
        );
    }
}

#[cfg(test)]
mod rate_limiter_wiring_tests {
    use super::*;
    use crate::provider_rate_limiter::fixtures::building_register_test_policy;
    use crate::provider_rate_limiter::{MessageThrottleClassifier, ProviderRateLimiter};
    use std::collections::BTreeMap;

    fn building_register_entry(job_id: &str, estimate: u32) -> LedgerEntry {
        LedgerEntry {
            job_id: job_id.to_owned(),
            provider: "data.go.kr".to_owned(),
            endpoint_slug: String::new(),
            endpoint: "getBrTitleInfo".to_owned(),
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11110".to_owned(),
            bjdong_cd: "10100".to_owned(),
            lawd_cd: String::new(),
            deal_ymd: String::new(),
            scope_unit_id: "s".to_owned(),
            shard_id: "0".to_owned(),
            idempotency_key: job_id.to_owned(),
            source_slug: "src".to_owned(),
            request_fingerprint_sha256: "f".to_owned(),
            request_fingerprint_schema_version: "v1".to_owned(),
            collection_snapshot_id: "snap".to_owned(),
            status: "planned".to_owned(),
            page_start: Some(1),
            page_end: Some(1),
            max_pages: 1,
            num_of_rows: 100,
            request_count_estimate: estimate,
        }
    }

    fn limiter(daily_budget: u64) -> anyhow::Result<ProviderRateLimiter> {
        let mut budgets = BTreeMap::new();
        budgets.insert(
            "data-go-kr:building-register-open-api".to_owned(),
            Some(daily_budget),
        );
        ProviderRateLimiter::new(
            building_register_test_policy(),
            budgets,
            Box::new(MessageThrottleClassifier),
        )
    }

    fn real_transaction_entry_token_less_endpoint(job_id: &str, estimate: u32) -> LedgerEntry {
        let mut entry = building_register_entry(job_id, estimate);
        entry.operation = "getRTMSDataSvcAptTradeDev".to_owned(); // routes as real-transaction
        entry.endpoint = "rt-endpoint".to_owned(); // deliberately token-less
        entry.lawd_cd = "11110".to_owned();
        entry.deal_ymd = "202601".to_owned();
        entry
    }

    #[test]
    fn partition_resolves_real_transaction_by_operation_not_endpoint() -> anyhow::Result<()> {
        // two-lane limiter so the real-transaction lane exists.
        let mut budgets = std::collections::BTreeMap::new();
        budgets.insert(
            "data-go-kr:building-register-open-api".to_owned(),
            Some(1_000_u64),
        );
        budgets.insert(
            "data-go-kr:real-transaction-open-api".to_owned(),
            Some(1_000_u64),
        );
        let limiter = crate::provider_rate_limiter::ProviderRateLimiter::new(
            crate::provider_rate_limiter::fixtures::data_go_kr_two_lane_test_policy(),
            budgets,
            Box::new(crate::provider_rate_limiter::MessageThrottleClassifier),
        )?;
        let selected = vec![real_transaction_entry_token_less_endpoint("rt", 5)];
        // With endpoint-keying this would bail; with operation-keying it resolves + runs.
        let (to_run, deferred) = partition_by_lane_budget(Some(&limiter), selected)?;
        assert_eq!(to_run.len(), 1);
        assert_eq!(deferred, 0);
        Ok(())
    }

    #[test]
    fn partition_without_limiter_runs_everything() -> anyhow::Result<()> {
        let selected = vec![
            building_register_entry("a", 5),
            building_register_entry("b", 5),
        ];
        let (to_run, deferred) = partition_by_lane_budget(None, selected)?;
        assert_eq!(to_run.len(), 2);
        assert_eq!(deferred, 0);
        Ok(())
    }

    #[test]
    fn partition_defers_jobs_over_lane_budget() -> anyhow::Result<()> {
        let limiter = limiter(8)?;
        let selected = vec![
            building_register_entry("a", 5),
            building_register_entry("b", 5), // 5+5 > 8 -> deferred
            building_register_entry("c", 3), // 5+3 = 8 -> runs
        ];
        let (to_run, deferred) = partition_by_lane_budget(Some(&limiter), selected)?;
        let run_ids: Vec<&str> = to_run.iter().map(|entry| entry.job_id.as_str()).collect();
        assert_eq!(run_ids, vec!["a", "c"]);
        assert_eq!(deferred, 1);
        Ok(())
    }
}
