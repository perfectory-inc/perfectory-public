//! data.go.kr real-transaction Bronze ingestion command.

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_application::{
    plan_real_transaction_bronze_page, RealTransactionBronzePagePlan,
    RealTransactionBronzePagePlanInput, RealTransactionPageRequest,
};
use collection_domain::{
    IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use collection_infrastructure::{
    DataGoKrRequestPolicy, DataGoKrServiceApiClient, DataGoKrServiceApiConfig,
    PgBronzeIngestUnitOfWork,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::bronze_object_storage_from_env;
use crate::bronze_schema_profile::CandidateKeyOverride;
use crate::page_collector::{collect_planned_pages, CollectablePage, PageCollectorLane};
use crate::pagination_guard::assert_page_window_complete;
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{
    optional_duration_millis_env, optional_duration_seconds_env, optional_env_value,
    optional_positive_u32_env, optional_u32_env, optional_u64_env, required_env_value,
};

const PROVIDER: &str = "data.go.kr";
const DEFAULT_BASE_ROOT: &str = "https://apis.data.go.kr/1613000";
const DEFAULT_OPERATION: &str = "getRTMSDataSvcInduTrade";
const DEFAULT_MAX_PAGES: u32 = 1;
const DEFAULT_USER_AGENT: &str = "foundation-platform-real-transaction-ingestor/1.0";
// The Bronze content-type / cache-control constants now live with the shared PageCollector loop
// (ADR 0017), which is the single place that builds the per-page commit input.

/// One fetched real-transaction page: the compiled Bronze plan plus the RAW page identity + parsed
/// payload the [`BronzeCommitter`](collection_application::BronzeCommitter) needs to OWN the key-compile.
///
/// The compiled `plan` drives the page-window completeness assertion, the dry-run summary, and the
/// run-level logical-record / size accounting; the persist stage hands the raw `request` +
/// `raw_payload` + `payload` to the committer, which re-runs the real-transaction Bronze plan as its
/// owned compile step (ADR 0016) — the exact mirror of the building-register lane.
#[derive(Clone, Debug)]
struct RealTransactionPlannedPage {
    plan: RealTransactionBronzePagePlan,
    request: RealTransactionPageRequest,
    raw_payload: Vec<u8>,
    payload: JsonValue,
}

/// Runs one data.go.kr real-transaction Bronze ingestion batch.
pub async fn run() -> anyhow::Result<()> {
    let config = RealTransactionIngestConfig::from_env()?;
    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: config.base_uri.clone(),
            service_key: config.service_key.clone(),
            user_agent: config.user_agent.clone(),
        },
        config.request_policy,
    )?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let now = Utc::now();
    let pages = plan_pages(&config, &client, run_id, now.date_naive()).await?;

    if !live_write_enabled(config.live_write.as_deref()) {
        tracing::info!(
            operation = %config.request.operation,
            objects_planned = pages.len(),
            logical_records_seen = total_logical_record_count_of_pages(&pages),
            total_size_bytes = total_size_bytes_of_pages(&pages),
            first_object_key = pages.first().map(|page| page.plan.object_key.as_str()),
            last_object_key = pages.last().map(|page| page.plan.object_key.as_str()),
            "real-transaction Bronze ingest dry run succeeded"
        );
        return Ok(());
    }

    persist_plans(&config, run_id, now, &pages).await
}

async fn plan_pages(
    config: &RealTransactionIngestConfig,
    client: &DataGoKrServiceApiClient,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
) -> anyhow::Result<Vec<RealTransactionPlannedPage>> {
    let mut pages = Vec::new();
    let mut last_page_observation = None;
    for (request_index, request) in page_requests_for_batch(&config.request, config.max_pages)?
        .into_iter()
        .enumerate()
    {
        if let Some(spacing) = config.request_spacing {
            spacing.wait_before_request(request_index).await;
        }
        let public_request = request.to_public_data_request().with_context(|| {
            format!(
                "failed to build real-transaction request for {} page {}",
                request.operation, request.page_no
            )
        })?;
        let fetched_page = client.fetch_page(&public_request).await.with_context(|| {
            format!(
                "failed to fetch data.go.kr real-transaction operation {} page {}",
                request.operation, request.page_no
            )
        })?;
        let provider_total_count =
            json_u64_pointer(&fetched_page.payload, "/response/body/totalCount")?;
        let effective_page_size =
            effective_page_size_from_response_metadata(&fetched_page.payload, request.num_of_rows)?;
        let plan = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload.clone(),
            payload: fetched_page.payload.clone(),
        })
        .with_context(|| {
            format!(
                "failed to plan real-transaction Bronze operation {} page {}",
                request.operation, request.page_no
            )
        })?;
        let logical_record_count = plan.logical_record_count;
        pages.push(RealTransactionPlannedPage {
            plan,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload,
            payload: fetched_page.payload,
        });
        last_page_observation = Some((
            request.page_no,
            effective_page_size,
            logical_record_count,
            provider_total_count,
        ));
        if should_stop_after_page(
            request.page_no,
            effective_page_size,
            logical_record_count,
            provider_total_count,
        ) {
            break;
        }
    }

    if let Some((last_page, page_size, logical_record_count, provider_total_count)) =
        last_page_observation
    {
        assert_page_window_complete(
            "real-transaction",
            last_page,
            page_size,
            logical_record_count,
            total_logical_record_count_of_pages(&pages),
            provider_total_count,
            config.max_pages,
        )?;
    }

    Ok(pages)
}

fn effective_page_size_from_response_metadata(
    payload: &JsonValue,
    requested_page_size: u32,
) -> anyhow::Result<u32> {
    let Some(raw_page_size) = json_u64_pointer(payload, "/response/body/numOfRows")? else {
        return Ok(requested_page_size);
    };
    if raw_page_size == 0 {
        bail!("real-transaction response body numOfRows must be greater than zero");
    }
    u32::try_from(raw_page_size)
        .with_context(|| "real-transaction response body numOfRows must fit in u32")
}

fn should_stop_after_page(
    page: u32,
    page_size: u32,
    logical_record_count: u64,
    provider_total_count: Option<u64>,
) -> bool {
    if let Some(total_count) = provider_total_count {
        return u64::from(page).saturating_mul(u64::from(page_size)) >= total_count;
    }
    logical_record_count < u64::from(page_size)
}

fn page_requests_for_batch(
    base_request: &RealTransactionPageRequest,
    max_pages: u32,
) -> anyhow::Result<Vec<RealTransactionPageRequest>> {
    (0..max_pages)
        .map(|offset| {
            let page_no = base_request
                .page_no
                .checked_add(offset)
                .context("real-transaction pageNo window exceeds u32")?;
            Ok(RealTransactionPageRequest {
                operation: base_request.operation.clone(),
                lawd_cd: base_request.lawd_cd.clone(),
                deal_ymd: base_request.deal_ymd.clone(),
                page_no,
                num_of_rows: base_request.num_of_rows,
            })
        })
        .collect()
}

async fn persist_plans(
    config: &RealTransactionIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[RealTransactionPlannedPage],
) -> anyhow::Result<()> {
    if pages.is_empty() {
        bail!("real-transaction ingest produced no Bronze page plans");
    }

    // Live-write path (`run` already gated on live_write before calling this): validate + log the
    // resolved R2 target before the first put, instead of failing mid-run on a misconfigured target.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("real-transaction live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for real-transaction ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for real-transaction Bronze ingest")?;

    let report =
        persist_plans_with_adapters(config, run_id, started_at, pages, &uow, storage.as_ref())
            .await?;

    tracing::info!(
        run_id = %report.run_id,
        last_object_key = ?report.last_object_key,
        last_bronze_object_id = ?report.last_bronze_object_id,
        logical_record_count = report.logical_records_seen,
        objects_written = report.objects_written,
        "real-transaction Bronze ingest live write succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RealTransactionPersistReport {
    run_id: IngestionRunId,
    last_object_key: Option<String>,
    last_bronze_object_id: Option<BronzeObjectId>,
    logical_records_seen: u64,
    objects_written: u64,
}

async fn persist_plans_with_adapters<Uow, Storage>(
    config: &RealTransactionIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[RealTransactionPlannedPage],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<RealTransactionPersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    if pages.is_empty() {
        bail!("real-transaction ingest produced no Bronze page plans");
    }

    // The compiled plans the planning stage already produced drive the run-level request snapshot;
    // the per-page commit + schema-profile + accounting now lives in the shared PageCollector loop
    // (ADR 0017), which hands each page's RAW identity to the BronzeCommitter (ADR 0016). The loop
    // body, ordering, `objects_written` accounting, commit-error -> terminal-failure mapping, run
    // lifecycle, and schema-profile gathering are identical across page lanes, so they live ONCE in
    // `collect_planned_pages`; this lane supplies only its catalog identity + per-source declaration.
    let plans: Vec<RealTransactionBronzePagePlan> =
        pages.iter().map(|page| page.plan.clone()).collect();

    let collectable_pages: Vec<CollectablePage<RealTransactionPageRequest>> = pages
        .iter()
        .map(|page| CollectablePage {
            plan: page.plan.clone(),
            request: page.request.clone(),
            raw_payload: page.raw_payload.clone(),
            payload: page.payload.clone(),
        })
        .collect();

    let report = collect_planned_pages(
        &RealTransactionLane,
        source_catalog_entry(config, started_at),
        ingestion_run(
            // The collector overrides `source_catalog_id` from the upserted source, so any placeholder
            // is fine here; pass the real-transaction run identity + request snapshot as before.
            SourceCatalogId::new(Uuid::nil()),
            run_id,
            started_at,
            batch_request_params(config, &plans),
        ),
        &config.source_slug,
        started_at,
        &collectable_pages,
        uow,
        storage,
    )
    .await?;

    Ok(RealTransactionPersistReport {
        run_id: report.run_id,
        last_object_key: report.last_object_key,
        last_bronze_object_id: report.last_bronze_object_id,
        logical_records_seen: report.logical_records_seen,
        objects_written: report.objects_written,
    })
}

/// Real-transaction page lane declaration (ADR 0017): the only per-source bits the shared
/// [`collect_planned_pages`] loop needs — the lane label used in commit-error context, and the
/// candidate-key override (real-transaction applies none). The loop, accounting, commit handoff, run
/// lifecycle, and schema-profile gathering all live in the collector.
struct RealTransactionLane;

impl PageCollectorLane for RealTransactionLane {
    type Request = RealTransactionPageRequest;

    fn lane_label(&self) -> &str {
        "real-transaction"
    }

    fn candidate_key_override(&self) -> CandidateKeyOverride {
        CandidateKeyOverride::None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RealTransactionOperationSpec {
    operation: &'static str,
    service_root: &'static str,
    /// Canonical semantic dataset identity (ADR 0014 D3); the Bronze `source_slug` is derived from
    /// this through `collection_domain::source_slug("data.go.kr", dataset_slug)`, never hand-written.
    dataset_slug: &'static str,
    source_name: &'static str,
    dataset_name: &'static str,
}

const OPERATION_SPECS: &[RealTransactionOperationSpec] = &[
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcRHTrade",
        service_root: "RTMSDataSvcRHTrade",
        dataset_slug: "real_transaction_row_house_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcRHTrade)",
        dataset_name: "real-transaction-row-house-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcRHRent",
        service_root: "RTMSDataSvcRHRent",
        dataset_slug: "real_transaction_row_house_rent",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcRHRent)",
        dataset_name: "real-transaction-row-house-rent",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcInduTrade",
        service_root: "RTMSDataSvcInduTrade",
        dataset_slug: "real_transaction_industrial_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcInduTrade)",
        dataset_name: "real-transaction-industrial-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcLandTrade",
        service_root: "RTMSDataSvcLandTrade",
        dataset_slug: "real_transaction_land_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcLandTrade)",
        dataset_name: "real-transaction-land-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcNrgTrade",
        service_root: "RTMSDataSvcNrgTrade",
        dataset_slug: "real_transaction_commercial_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcNrgTrade)",
        dataset_name: "real-transaction-commercial-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcOffiTrade",
        service_root: "RTMSDataSvcOffiTrade",
        dataset_slug: "real_transaction_officetel_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcOffiTrade)",
        dataset_name: "real-transaction-officetel-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcOffiRent",
        service_root: "RTMSDataSvcOffiRent",
        dataset_slug: "real_transaction_officetel_rent",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcOffiRent)",
        dataset_name: "real-transaction-officetel-rent",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcAptTradeDev",
        service_root: "RTMSDataSvcAptTradeDev",
        dataset_slug: "real_transaction_apartment_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcAptTradeDev)",
        dataset_name: "real-transaction-apartment-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcAptRent",
        service_root: "RTMSDataSvcAptRent",
        dataset_slug: "real_transaction_apartment_rent",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcAptRent)",
        dataset_name: "real-transaction-apartment-rent",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcSilvTrade",
        service_root: "RTMSDataSvcSilvTrade",
        dataset_slug: "real_transaction_apartment_presale",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcSilvTrade)",
        dataset_name: "real-transaction-apartment-presale",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcSHTrade",
        service_root: "RTMSDataSvcSHTrade",
        dataset_slug: "real_transaction_detached_house_trade",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcSHTrade)",
        dataset_name: "real-transaction-detached-house-trade",
    },
    RealTransactionOperationSpec {
        operation: "getRTMSDataSvcSHRent",
        service_root: "RTMSDataSvcSHRent",
        dataset_slug: "real_transaction_detached_house_rent",
        source_name: "data.go.kr Real Transaction (RTMSDataSvcSHRent)",
        dataset_name: "real-transaction-detached-house-rent",
    },
];

fn operation_specs() -> &'static [RealTransactionOperationSpec] {
    OPERATION_SPECS
}

fn operation_spec(operation: &str) -> anyhow::Result<RealTransactionOperationSpec> {
    operation_specs()
        .iter()
        .copied()
        .find(|spec| spec.operation == operation)
        .with_context(|| format!("real-transaction operation is not supported: {operation}"))
}

pub(crate) fn default_base_uri_for_operation(operation: &str) -> anyhow::Result<String> {
    operation_spec(operation).map(default_base_uri_for_spec)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RealTransactionIngestConfig {
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    service_key: String,
    user_agent: String,
    request: RealTransactionPageRequest,
    max_pages: u32,
    request_spacing: Option<ProviderRequestSpacing>,
    request_policy: DataGoKrRequestPolicy,
    live_write: Option<String>,
}

impl RealTransactionIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        let operation = optional_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_OPERATION")?
            .unwrap_or_else(|| DEFAULT_OPERATION.to_owned());
        let spec = operation_spec(&operation)?;
        let default_policy = DataGoKrRequestPolicy::default();
        let max_attempts =
            optional_positive_u32_env("FOUNDATION_PLATFORM_DATA_GO_KR_MAX_ATTEMPTS")?
                .unwrap_or_else(|| default_policy.max_attempts());
        let request_timeout = optional_duration_seconds_env(
            "FOUNDATION_PLATFORM_DATA_GO_KR_REQUEST_TIMEOUT_SECONDS",
        )?
        .unwrap_or_else(|| default_policy.request_timeout());
        let initial_backoff = optional_duration_millis_env(
            "FOUNDATION_PLATFORM_DATA_GO_KR_RETRY_INITIAL_BACKOFF_MS",
        )?
        .unwrap_or_else(|| default_policy.initial_backoff());
        let max_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_DATA_GO_KR_RETRY_MAX_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.max_backoff());

        Ok(Self {
            source_slug: crate::public_data_control_support::resolve_canonical_source_slug(
                "FOUNDATION_PLATFORM_REAL_TRANSACTION_SOURCE_SLUG",
                optional_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_SOURCE_SLUG")?,
                PROVIDER,
                spec.dataset_slug,
            )?,
            source_name: spec.source_name.to_owned(),
            dataset_name: spec.dataset_name.to_owned(),
            base_uri: optional_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_BASE_URI")?
                .unwrap_or_else(|| default_base_uri_for_spec(spec)),
            service_key: required_env_value("DATA_GO_KR_SERVICE_KEY")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request: RealTransactionPageRequest {
                operation,
                lawd_cd: required_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_LAWD_CD")?,
                deal_ymd: required_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_DEAL_YMD")?,
                page_no: optional_u32_env("FOUNDATION_PLATFORM_REAL_TRANSACTION_PAGE_NO")?
                    .unwrap_or(1),
                num_of_rows: optional_u32_env("FOUNDATION_PLATFORM_REAL_TRANSACTION_NUM_OF_ROWS")?
                    .unwrap_or(1000),
            },
            max_pages: optional_positive_u32_env("FOUNDATION_PLATFORM_REAL_TRANSACTION_MAX_PAGES")?
                .unwrap_or(DEFAULT_MAX_PAGES),
            request_spacing: ProviderRequestSpacing::optional_from_millis(
                optional_u64_env("FOUNDATION_PLATFORM_REAL_TRANSACTION_MIN_PAGE_INTERVAL_MS")?.or(
                    optional_u64_env("FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS")?,
                ),
            )?,
            request_policy: DataGoKrRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
            live_write: optional_env_value("FOUNDATION_PLATFORM_REAL_TRANSACTION_LIVE_WRITE")?,
        })
    }
}

fn default_base_uri_for_spec(spec: RealTransactionOperationSpec) -> String {
    format!("{DEFAULT_BASE_ROOT}/{}", spec.service_root)
}

fn source_catalog_entry(
    config: &RealTransactionIngestConfig,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: config.source_name.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: config.dataset_name.clone(),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: Some("https://www.data.go.kr/".to_owned()),
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

fn batch_request_params(
    config: &RealTransactionIngestConfig,
    plans: &[RealTransactionBronzePagePlan],
) -> JsonValue {
    json!({
        "operation": config.request.operation,
        "lawdCd": config.request.lawd_cd,
        "dealYmd": config.request.deal_ymd,
        "startPageNo": config.request.page_no,
        "numOfRows": config.request.num_of_rows,
        "maxPages": config.max_pages,
        "pagesPlanned": plans.len(),
        "_type": "json"
    })
}

fn json_u64_pointer(payload: &JsonValue, pointer: &str) -> anyhow::Result<Option<u64>> {
    let Some(value) = payload.pointer(pointer) else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(number) => number
            .as_u64()
            .with_context(|| {
                format!("real-transaction JSON field {pointer} must be an unsigned integer")
            })
            .map(Some),
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<u64>()
                .with_context(|| {
                    format!("real-transaction JSON field {pointer} must be an unsigned integer")
                })
                .map(Some)
        }
        _ => bail!("real-transaction JSON field {pointer} must be an unsigned integer"),
    }
}

// `total_logical_record_count` over plans (the failure-path / completion accounting helper) now lives
// in the shared PageCollector loop, which owns the run lifecycle (ADR 0017). The dry-run summary still
// needs the planned-page variant below.
fn total_logical_record_count_of_pages(pages: &[RealTransactionPlannedPage]) -> u64 {
    pages
        .iter()
        .map(|page| page.plan.logical_record_count)
        .sum()
}

fn total_size_bytes_of_pages(pages: &[RealTransactionPlannedPage]) -> u64 {
    pages.iter().map(|page| page.plan.size_bytes).sum()
}
// `total_size_bytes` over plans was inlined into `total_size_bytes_of_pages`; the only caller was the
// dry-run summary, which now iterates the planned-page carrier.

fn live_write_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use collection_domain::source_slug;

    #[test]
    fn operation_registry_covers_required_real_transaction_operations() {
        let operations = operation_specs()
            .iter()
            .map(|spec| spec.operation)
            .collect::<Vec<_>>();

        assert_eq!(
            operations,
            vec![
                "getRTMSDataSvcRHTrade",
                "getRTMSDataSvcRHRent",
                "getRTMSDataSvcInduTrade",
                "getRTMSDataSvcLandTrade",
                "getRTMSDataSvcNrgTrade",
                "getRTMSDataSvcOffiTrade",
                "getRTMSDataSvcOffiRent",
                "getRTMSDataSvcAptTradeDev",
                "getRTMSDataSvcAptRent",
                "getRTMSDataSvcSilvTrade",
                "getRTMSDataSvcSHTrade",
                "getRTMSDataSvcSHRent",
            ]
        );
    }

    #[test]
    fn operation_registry_derives_canonical_source_slugs() -> anyhow::Result<()> {
        for spec in operation_specs() {
            // The Bronze slug is generator-produced from the spec's dataset_slug (ADR 0014 D3),
            // not hand-written; it must be canonical `datagokr__<dataset_slug>`.
            let slug = source_slug(PROVIDER, spec.dataset_slug)?;
            assert_eq!(slug, format!("datagokr__{}", spec.dataset_slug));
            assert_eq!(
                default_base_uri_for_spec(*spec),
                format!("{DEFAULT_BASE_ROOT}/{}", spec.service_root)
            );
        }
        Ok(())
    }

    #[test]
    fn page_requests_cover_configured_batch_window() -> anyhow::Result<()> {
        let base_request = RealTransactionPageRequest {
            operation: "getRTMSDataSvcInduTrade".to_owned(),
            lawd_cd: "11680".to_owned(),
            deal_ymd: "202605".to_owned(),
            page_no: 7,
            num_of_rows: 100,
        };

        let pages = page_requests_for_batch(&base_request, 3)?;

        assert_eq!(
            pages
                .iter()
                .map(|request| request.page_no)
                .collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert!(pages.iter().all(|request| {
            request.operation == "getRTMSDataSvcInduTrade"
                && request.lawd_cd == "11680"
                && request.deal_ymd == "202605"
                && request.num_of_rows == 100
        }));
        Ok(())
    }

    // ---- Live persist path through the BronzeCommitter (Task 3) ----
    //
    // These mirror the building-register lane's persist tests. The put/record path now flows through
    // `committer.commit_real_transaction_page` (CreateOnly + recoverable commit protocol), so the
    // R2-already-exists case is no longer an unconditional hard failure: a matching-checksum object
    // with a missing DB row RECOVERS, while a conflicting-checksum object fails loud.

    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    // `CompleteIngestionRunCommand` is only needed by the test `FakeBronzeUow`'s
    // `BronzeIngestUnitOfWork` impl now that the production loop (run lifecycle) lives in the shared
    // PageCollector (ADR 0017); the production code no longer references it directly.
    use collection_application::ports::CompleteIngestionRunCommand;
    use collection_domain::CollectionError;
    use collection_domain::{BronzeObject, SchemaProfile};
    use foundation_outbox::{object_storage::PutObjectRequest, PublishError};
    use uuid::Uuid;

    fn test_config() -> RealTransactionIngestConfig {
        RealTransactionIngestConfig {
            source_slug: "datagokr__real_transaction_industrial_trade".to_owned(),
            source_name: "data.go.kr Real Transaction (RTMSDataSvcInduTrade)".to_owned(),
            dataset_name: "real-transaction-industrial-trade".to_owned(),
            base_uri: format!("{DEFAULT_BASE_ROOT}/RTMSDataSvcInduTrade"),
            service_key: "redacted-test-key".to_owned(),
            user_agent: DEFAULT_USER_AGENT.to_owned(),
            request: RealTransactionPageRequest {
                operation: "getRTMSDataSvcInduTrade".to_owned(),
                lawd_cd: "11680".to_owned(),
                deal_ymd: "202605".to_owned(),
                page_no: 1,
                num_of_rows: 1000,
            },
            max_pages: 1,
            request_spacing: None,
            request_policy: DataGoKrRequestPolicy::default(),
            live_write: Some("1".to_owned()),
        }
    }

    fn test_planned_page(
        config: &RealTransactionIngestConfig,
        run_id: IngestionRunId,
        page_no: u32,
    ) -> anyhow::Result<RealTransactionPlannedPage> {
        let payload = json!({
            "response": { "body": { "items": { "item": [
                { "거래금액": format!("12,{page_no:03}"), "건물면적": "84.5" }
            ] } } }
        });
        let raw_payload = serde_json::to_vec(&payload)?;
        let request = RealTransactionPageRequest {
            operation: config.request.operation.clone(),
            lawd_cd: config.request.lawd_cd.clone(),
            deal_ymd: config.request.deal_ymd.clone(),
            page_no,
            num_of_rows: config.request.num_of_rows,
        };
        let plan = plan_real_transaction_bronze_page(RealTransactionBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date: chrono::NaiveDate::from_ymd_opt(2026, 5, 14)
                .ok_or_else(|| anyhow::anyhow!("valid date"))?,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        })?;
        Ok(RealTransactionPlannedPage {
            plan,
            request,
            raw_payload,
            payload,
        })
    }

    #[tokio::test]
    async fn persist_marks_run_failed_when_bronze_metadata_recording_fails() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let pages = vec![test_planned_page(&config, run_id, 1)?];
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::RecordBronzeObject);
        let storage = FakeObjectStorage::default();

        let error =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected persistence failure"))?;

        assert!(
            error
                .to_string()
                .contains("failed to record real-transaction Bronze object metadata"),
            "unexpected error: {error}"
        );
        // The object was written (CreateOnly) before the record attempt failed.
        assert_eq!(storage.written_keys()?.len(), 1);

        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Failed);
        assert_eq!(completions[0].objects_written, 1);
        assert_eq!(completions[0].logical_records_seen, 1);
        Ok(())
    }

    #[tokio::test]
    async fn persist_marks_run_failed_when_r2_write_fails() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let pages = vec![test_planned_page(&config, run_id, 1)?];
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
        let storage = FakeObjectStorage::failing("simulated R2 outage");

        let error =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected R2 write failure"))?;

        // The outermost context names the write step; the inner R2 outage is preserved in the chain.
        assert!(
            error
                .to_string()
                .contains("failed to write real-transaction Bronze object"),
            "unexpected error: {error}"
        );
        assert!(
            format!("{error:#}").contains("simulated R2 outage"),
            "the inner R2 outage must survive in the error chain: {error:#}"
        );
        assert_eq!(storage.written_keys()?.len(), 0);

        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Failed);
        assert_eq!(completions[0].objects_written, 0);
        // The terminal failure message (built with `{error:#}`) carries the inner outage too.
        assert!(completions[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("simulated R2 outage"));
        Ok(())
    }

    /// Recoverable commit protocol end-to-end through the real-transaction live persist path: the
    /// object is already in R2 with a matching checksum (a prior run's write) but no `bronze_object`
    /// row exists (that prior run's DB record failed). The CreateOnly write hits already-exists; the
    /// committer recovers by recording the missing row, so the run completes Succeeded — an
    /// R2-already-exists is no longer a hard failure.
    #[tokio::test]
    async fn persist_recovers_when_r2_object_exists_but_db_row_is_missing() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let page = test_planned_page(&config, run_id, 1)?;
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
        // Object already present with this page's exact checksum, but no DB row recorded yet.
        let storage = FakeObjectStorage::with_existing_object(
            page.plan.object_key.as_str(),
            &page.plan.checksum_sha256,
        );
        let pages = vec![page.clone()];

        let report =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await?;

        assert_eq!(report.objects_written, 1);
        assert_eq!(report.logical_records_seen, 1);
        // No fresh write (the object already existed), but the missing row was recovered.
        assert!(storage.written_keys()?.is_empty());
        let recorded = uow.recorded_bronze_objects()?;
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].object_key.as_str(),
            page.plan.object_key.as_str()
        );
        assert_eq!(recorded[0].checksum_sha256, page.plan.checksum_sha256);

        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Succeeded);
        assert!(completions[0].error_message.is_none());
        Ok(())
    }

    /// Quarantine terminal through the real-transaction live persist path: the object is already in
    /// R2 but with a DIFFERENT checksum and no DB row. The committer cannot prove the object is ours,
    /// so it fails loud and the run is marked Failed (never silently overwritten).
    #[tokio::test]
    async fn persist_fails_loud_when_r2_object_checksum_conflicts() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T00:00:00Z")?.to_utc();
        let config = test_config();
        let source_id = SourceCatalogId::new(Uuid::new_v4());
        let page = test_planned_page(&config, run_id, 1)?;
        let uow = FakeBronzeUow::new(source_id, FakeFailureMode::None);
        let conflicting_sha = "0".repeat(64);
        let storage = FakeObjectStorage::with_existing_object(
            page.plan.object_key.as_str(),
            &conflicting_sha,
        );
        let pages = vec![page];

        let error =
            persist_plans_with_adapters(&config, run_id, started_at, &pages, &uow, &storage)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected checksum conflict failure"))?;

        assert!(
            error.to_string().contains("Bronze checksum conflict"),
            "unexpected error: {error}"
        );
        assert!(uow.recorded_bronze_objects()?.is_empty());
        let completions = uow.completions()?;
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, IngestionRunStatus::Failed);
        // Nothing newly written: the CreateOnly collision did not add an object.
        assert_eq!(completions[0].objects_written, 0);
        Ok(())
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FakeFailureMode {
        None,
        RecordBronzeObject,
    }

    struct FakeBronzeUow {
        source_id: SourceCatalogId,
        failure_mode: FakeFailureMode,
        runs: Mutex<Vec<IngestionRun>>,
        completions: Mutex<Vec<CompleteIngestionRunCommand>>,
        bronze_objects: Mutex<Vec<BronzeObject>>,
        schema_profiles: Mutex<Vec<SchemaProfile>>,
    }

    impl FakeBronzeUow {
        const fn new(source_id: SourceCatalogId, failure_mode: FakeFailureMode) -> Self {
            Self {
                source_id,
                failure_mode,
                runs: Mutex::new(Vec::new()),
                completions: Mutex::new(Vec::new()),
                bronze_objects: Mutex::new(Vec::new()),
                schema_profiles: Mutex::new(Vec::new()),
            }
        }

        fn completions(&self) -> anyhow::Result<Vec<CompleteIngestionRunCommand>> {
            Ok(self
                .completions
                .lock()
                .map_err(|_| anyhow::anyhow!("completion lock poisoned"))?
                .clone())
        }

        fn recorded_bronze_objects(&self) -> anyhow::Result<Vec<BronzeObject>> {
            Ok(self
                .bronze_objects
                .lock()
                .map_err(|_| anyhow::anyhow!("bronze object lock poisoned"))?
                .clone())
        }
    }

    #[async_trait]
    impl BronzeIngestUnitOfWork for FakeBronzeUow {
        async fn upsert_source_catalog_entry(
            &self,
            entry: &SourceCatalogEntry,
        ) -> Result<SourceCatalogEntry, CollectionError> {
            let mut source = entry.clone();
            source.id = self.source_id;
            Ok(source)
        }

        async fn create_ingestion_run(
            &self,
            run: &IngestionRun,
        ) -> Result<IngestionRun, CollectionError> {
            self.runs
                .lock()
                .map_err(|_| CollectionError::Infrastructure("run lock poisoned".to_owned()))?
                .push(run.clone());
            Ok(run.clone())
        }

        async fn complete_ingestion_run(
            &self,
            command: CompleteIngestionRunCommand,
        ) -> Result<IngestionRun, CollectionError> {
            self.completions
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("completion lock poisoned".to_owned())
                })?
                .push(command.clone());
            let run = self
                .runs
                .lock()
                .map_err(|_| CollectionError::Infrastructure("run lock poisoned".to_owned()))?
                .iter()
                .find(|run| run.id == command.id)
                .cloned()
                .ok_or_else(|| CollectionError::IngestionRunNotFound(command.id.to_string()))?;

            Ok(IngestionRun {
                status: command.status,
                finished_at: Some(command.finished_at),
                logical_records_seen: command.logical_records_seen,
                objects_written: command.objects_written,
                error_message: command.error_message,
                ..run
            })
        }

        async fn find_bronze_object_by_object_key(
            &self,
            source_catalog_id: SourceCatalogId,
            object_key: &str,
        ) -> Result<Option<BronzeObject>, CollectionError> {
            Ok(self
                .bronze_objects
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("bronze object lock poisoned".to_owned())
                })?
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
            if self.failure_mode == FakeFailureMode::RecordBronzeObject {
                return Err(CollectionError::Infrastructure(
                    "simulated bronze metadata failure".to_owned(),
                ));
            }
            self.bronze_objects
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("bronze object lock poisoned".to_owned())
                })?
                .push(object.clone());
            Ok(object.clone())
        }

        async fn upsert_schema_profile(
            &self,
            profile: &SchemaProfile,
        ) -> Result<SchemaProfile, CollectionError> {
            self.schema_profiles
                .lock()
                .map_err(|_| {
                    CollectionError::Infrastructure("schema profile lock poisoned".to_owned())
                })?
                .push(profile.clone());
            Ok(profile.clone())
        }
    }

    #[derive(Default)]
    struct FakeObjectStorage {
        written_keys: Mutex<Vec<String>>,
        /// Pre-seeded objects already in storage, keyed by object key -> stored `x-amz-meta-sha256`.
        /// A `CreateOnly` write to a seeded key returns `ObjectAlreadyExists` (the R2 412), driving
        /// the committer's recoverable commit protocol; `read_object_sha256` reports the seeded sum.
        existing: Mutex<BTreeMap<String, String>>,
        fail_message: Option<String>,
    }

    impl FakeObjectStorage {
        fn failing(message: &str) -> Self {
            Self {
                written_keys: Mutex::new(Vec::new()),
                existing: Mutex::new(BTreeMap::new()),
                fail_message: Some(message.to_owned()),
            }
        }

        fn with_existing_object(key: &str, sha256: &str) -> Self {
            let storage = Self::default();
            storage
                .existing
                .lock()
                .expect("existing lock")
                .insert(key.to_owned(), sha256.to_owned());
            storage
        }

        fn written_keys(&self) -> anyhow::Result<Vec<String>> {
            Ok(self
                .written_keys
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
                .clone())
        }
    }

    #[async_trait]
    impl ObjectStorageService for FakeObjectStorage {
        async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
            if let Some(message) = &self.fail_message {
                return Err(PublishError::Infrastructure(message.clone()));
            }
            // CreateOnly collision with a pre-seeded key surfaces as ObjectAlreadyExists (R2 412).
            if matches!(
                request.write_mode,
                foundation_outbox::object_storage::ObjectWriteMode::CreateOnly
            ) && self
                .existing
                .lock()
                .map_err(|_| PublishError::Infrastructure("existing lock poisoned".to_owned()))?
                .contains_key(&request.key)
            {
                return Err(PublishError::ObjectAlreadyExists { key: request.key });
            }
            self.written_keys
                .lock()
                .map_err(|_| PublishError::Infrastructure("storage lock poisoned".to_owned()))?
                .push(request.key);
            Ok(())
        }

        async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError> {
            Ok(self
                .existing
                .lock()
                .map_err(|_| PublishError::Infrastructure("existing lock poisoned".to_owned()))?
                .get(key)
                .cloned())
        }
    }
}
