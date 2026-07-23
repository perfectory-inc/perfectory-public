//! `VWorld` cadastral 2D Data API Bronze ingestion command.

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_application::{
    plan_vworld_cadastral_bronze_page, VWorldCadastralBronzePagePlan,
    VWorldCadastralBronzePagePlanInput, VWorldCadastralPageRequest,
};
use collection_domain::{
    IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use collection_infrastructure::{
    PgBronzeIngestUnitOfWork, VWorldDataApiClient, VWorldDataApiConfig, VWorldDataFeatureRequest,
    VWorldRequestPolicy,
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
    optional_bool_env, optional_csv_env, optional_duration_millis_env,
    optional_duration_seconds_env, optional_env_value, optional_positive_u32_env, optional_u32_env,
    optional_u64_env, required_env_value,
};

const SOURCE_NAME: &str = "VWorld Cadastral Parcel Boundaries";
const PROVIDER: &str = "vworld";
/// Catalog-native provider label used by the canonical `source_slug` generator (ADR 0014 D2).
const GENERATOR_PROVIDER: &str = "VWorld";
/// Canonical semantic dataset identity for VWorld cadastral parcel boundaries (a real dataset,
/// distinct from `parcel`; ADR 0014 §6). The Bronze slug is generator-derived from this.
const DEFAULT_DATASET_SLUG: &str = "cadastral";
const DEFAULT_BASE_URI: &str = "https://api.vworld.kr";
const DEFAULT_DATASET: &str = "LP_PA_CBND_BUBUN";
const DEFAULT_MAX_PAGES: u32 = 1;
const DEFAULT_PAGE: u32 = 1;
const DEFAULT_SIZE: u32 = 1000;
const DEFAULT_USER_AGENT: &str = "foundation-outbox-publisher/0.1";
// The Bronze content-type / cache-control constants now live with the shared PageCollector loop
// (ADR 0017), which is the single place that builds the per-page commit input.

/// One fetched `VWorld` cadastral page: the compiled Bronze plan plus the RAW page identity +
/// parsed payload the [`BronzeCommitter`] needs to OWN the key-compile (ADR 0016).
///
/// The compiled `plan` drives the page-window completeness assertion, the dry-run summary, and the
/// run-level logical-record / size accounting; the persist stage hands the raw `request` +
/// `raw_payload` + `payload` to the committer, which re-runs the cadastral Bronze plan as its owned
/// compile step — the exact mirror of the land-register / NED / real-transaction lanes.
#[derive(Clone, Debug)]
struct VWorldCadastralPlannedPage {
    plan: VWorldCadastralBronzePagePlan,
    request: VWorldCadastralPageRequest,
    raw_payload: Vec<u8>,
    payload: JsonValue,
}

/// Runs one `VWorld` cadastral Bronze ingestion batch.
pub async fn run() -> anyhow::Result<()> {
    let config = VWorldCadastralIngestConfig::from_env()?;
    let client = VWorldDataApiClient::new_with_policy(
        &VWorldDataApiConfig {
            base_uri: config.base_uri.clone(),
            api_key: config.api_key.clone(),
            domain: config.domain.clone(),
            user_agent: config.user_agent.clone(),
        },
        config.request_policy,
    )?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let now = Utc::now();
    let pages = plan_pages(&config, &client, run_id, now.date_naive()).await?;

    if !live_write_enabled(config.live_write.as_deref()) {
        tracing::info!(
            objects_planned = pages.len(),
            logical_records_seen = total_logical_record_count_of_pages(&pages),
            total_size_bytes = total_size_bytes_of_pages(&pages),
            first_object_key = pages.first().map(|page| page.plan.object_key.as_str()),
            last_object_key = pages.last().map(|page| page.plan.object_key.as_str()),
            "VWorld cadastral Bronze ingest dry run succeeded"
        );
        return Ok(());
    }

    persist_plans(&config, run_id, now, &pages).await
}

async fn plan_pages(
    config: &VWorldCadastralIngestConfig,
    client: &VWorldDataApiClient,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
) -> anyhow::Result<Vec<VWorldCadastralPlannedPage>> {
    let mut pages = Vec::new();
    for request in &config.requests {
        pages.extend(fetch_window_pages(config, client, run_id, ingest_date, request).await?);
    }

    Ok(pages)
}

async fn fetch_window_pages(
    config: &VWorldCadastralIngestConfig,
    client: &VWorldDataApiClient,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
    base_request: &VWorldCadastralPageRequest,
) -> anyhow::Result<Vec<VWorldCadastralPlannedPage>> {
    let mut pages = Vec::new();
    let mut last_page_observation = None;
    for (request_index, request) in page_requests_for_batch(base_request, config.max_pages)?
        .into_iter()
        .enumerate()
    {
        if let Some(spacing) = config.request_spacing {
            spacing.wait_before_request(request_index).await;
        }
        let fetched_page = client
            .fetch_feature_page(&data_feature_request(&request))
            .await
            .with_context(|| {
                format!(
                    "failed to fetch VWorld cadastral page {} for dataset {}",
                    request.page, request.dataset
                )
            })?;
        let provider_total_count =
            json_u64_pointer(&fetched_page.payload, "/response/record/total")?;
        let plan = plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload.clone(),
            payload: fetched_page.payload.clone(),
        })
        .with_context(|| {
            format!(
                "failed to plan VWorld cadastral Bronze page {} for dataset {}",
                request.page, request.dataset
            )
        })?;
        let logical_record_count = plan.logical_record_count;
        pages.push(VWorldCadastralPlannedPage {
            plan,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload,
            payload: fetched_page.payload,
        });
        last_page_observation = Some((
            request.page,
            request.size,
            logical_record_count,
            provider_total_count,
        ));
        if should_stop_after_page(
            request.page,
            request.size,
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
            "VWorld cadastral",
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

fn json_u64_pointer(payload: &JsonValue, pointer: &str) -> anyhow::Result<Option<u64>> {
    let Some(value) = payload.pointer(pointer) else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Number(number) => number
            .as_u64()
            .with_context(|| format!("VWorld JSON field {pointer} must be an unsigned integer"))
            .map(Some),
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<u64>()
                .with_context(|| format!("VWorld JSON field {pointer} must be an unsigned integer"))
                .map(Some)
        }
        _ => bail!("VWorld JSON field {pointer} must be an unsigned integer"),
    }
}

fn page_requests_for_batch(
    base_request: &VWorldCadastralPageRequest,
    max_pages: u32,
) -> anyhow::Result<Vec<VWorldCadastralPageRequest>> {
    (0..max_pages)
        .map(|offset| {
            let page = base_request
                .page
                .checked_add(offset)
                .context("VWorld cadastral page window exceeds u32")?;
            Ok(VWorldCadastralPageRequest {
                dataset: base_request.dataset.clone(),
                attr_filter: base_request.attr_filter.clone(),
                columns: base_request.columns.clone(),
                geometry: base_request.geometry,
                attribute: base_request.attribute,
                crs: base_request.crs.clone(),
                page,
                size: base_request.size,
            })
        })
        .collect()
}

fn data_feature_request(request: &VWorldCadastralPageRequest) -> VWorldDataFeatureRequest {
    VWorldDataFeatureRequest {
        dataset: request.dataset.clone(),
        attr_filter: request.attr_filter.clone(),
        columns: request.columns.clone(),
        geometry: request.geometry,
        attribute: request.attribute,
        crs: request.crs.clone(),
        page: request.page,
        size: request.size,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldCadastralRequestSeed {
    dataset: String,
    attr_filter: Option<String>,
    pnu: Option<String>,
    columns: Vec<String>,
    geometry: bool,
    attribute: bool,
    crs: Option<String>,
    page: u32,
    size: u32,
}

fn cadastral_requests_from_env(
    seed: VWorldCadastralRequestSeed,
) -> anyhow::Result<Vec<VWorldCadastralPageRequest>> {
    let attr_filter = seed
        .attr_filter
        .or_else(|| seed.pnu.map(|value| format!("pnu:=:{value}")))
        .context(
            "VWorld cadastral ingest requires FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER or PNU",
        )?;

    Ok(vec![VWorldCadastralPageRequest {
        dataset: seed.dataset,
        attr_filter: Some(attr_filter),
        columns: seed.columns,
        geometry: seed.geometry,
        attribute: seed.attribute,
        crs: seed.crs,
        page: seed.page,
        size: seed.size,
    }])
}

async fn persist_plans(
    config: &VWorldCadastralIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[VWorldCadastralPlannedPage],
) -> anyhow::Result<()> {
    if pages.is_empty() {
        bail!("VWorld cadastral ingest produced no Bronze page plans");
    }

    // Live-write path (`run` already gated on live_write before calling this): validate + log the
    // resolved R2 target before the first put, instead of failing mid-run on a misconfigured target.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("VWorld cadastral live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for VWorld cadastral ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for VWorld cadastral Bronze ingest")?;

    let report =
        persist_plans_with_adapters(config, run_id, started_at, pages, &uow, storage.as_ref())
            .await?;

    tracing::info!(
        run_id = %report.run_id,
        last_object_key = ?report.last_object_key,
        last_bronze_object_id = ?report.last_bronze_object_id,
        logical_record_count = report.logical_records_seen,
        objects_written = report.objects_written,
        "VWorld cadastral Bronze ingest live write succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldCadastralPersistReport {
    run_id: IngestionRunId,
    last_object_key: Option<String>,
    last_bronze_object_id: Option<BronzeObjectId>,
    logical_records_seen: u64,
    objects_written: u64,
}

async fn persist_plans_with_adapters<Uow, Storage>(
    config: &VWorldCadastralIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[VWorldCadastralPlannedPage],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<VWorldCadastralPersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    if pages.is_empty() {
        bail!("VWorld cadastral ingest produced no Bronze page plans");
    }

    // The compiled plans the planning stage already produced drive the run-level request snapshot;
    // the per-page commit + schema-profile + accounting now lives in the shared PageCollector loop
    // (ADR 0017), which hands each page's RAW identity to the BronzeCommitter (ADR 0016). The loop
    // body, ordering, `objects_written` accounting, commit-error -> terminal-failure mapping, run
    // lifecycle, and schema-profile gathering are identical across page lanes, so they live ONCE in
    // `collect_planned_pages`; this lane supplies only its catalog identity + per-source declaration.
    let plans: Vec<VWorldCadastralBronzePagePlan> =
        pages.iter().map(|page| page.plan.clone()).collect();

    let collectable_pages: Vec<CollectablePage<VWorldCadastralPageRequest>> = pages
        .iter()
        .map(|page| CollectablePage {
            plan: page.plan.clone(),
            request: page.request.clone(),
            raw_payload: page.raw_payload.clone(),
            payload: page.payload.clone(),
        })
        .collect();

    let report = collect_planned_pages(
        &VWorldCadastralLane,
        source_catalog_entry(config, started_at),
        ingestion_run(
            // The collector overrides `source_catalog_id` from the upserted source, so any placeholder
            // is fine here; pass the cadastral run identity + request snapshot as before.
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

    Ok(VWorldCadastralPersistReport {
        run_id: report.run_id,
        last_object_key: report.last_object_key,
        last_bronze_object_id: report.last_bronze_object_id,
        logical_records_seen: report.logical_records_seen,
        objects_written: report.objects_written,
    })
}

/// VWorld cadastral page lane declaration (ADR 0017): the only per-source bits the shared
/// [`collect_planned_pages`] loop needs — the lane label used in commit-error context, and the
/// candidate-key override (cadastral re-scores the trailing `pnu` segment). The loop, accounting,
/// commit handoff, run lifecycle, and schema-profile gathering all live in the collector.
struct VWorldCadastralLane;

impl PageCollectorLane for VWorldCadastralLane {
    type Request = VWorldCadastralPageRequest;

    fn lane_label(&self) -> &str {
        "VWorld cadastral"
    }

    fn candidate_key_override(&self) -> CandidateKeyOverride {
        CandidateKeyOverride::LastDotSegmentEquals("pnu")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldCadastralIngestConfig {
    source_slug: String,
    base_uri: String,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    requests: Vec<VWorldCadastralPageRequest>,
    max_pages: u32,
    request_spacing: Option<ProviderRequestSpacing>,
    request_policy: VWorldRequestPolicy,
    live_write: Option<String>,
}

impl VWorldCadastralIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        let default_policy = VWorldRequestPolicy::default();
        let max_attempts = optional_positive_u32_env("FOUNDATION_PLATFORM_VWORLD_MAX_ATTEMPTS")?
            .unwrap_or_else(|| default_policy.max_attempts());
        let request_timeout =
            optional_duration_seconds_env("FOUNDATION_PLATFORM_VWORLD_REQUEST_TIMEOUT_SECONDS")?
                .unwrap_or_else(|| default_policy.request_timeout());
        let initial_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_INITIAL_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.initial_backoff());
        let max_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_VWORLD_RETRY_MAX_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.max_backoff());
        let pnu = optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PNU")?.or(
            optional_env_value("FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU")?,
        );
        for forbidden in [
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOM_FILTER",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_BBOX",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_ROWS",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_COLUMNS",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ADAPTIVE_SUBDIVISION",
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_SUBDIVISION_DEPTH",
        ] {
            if optional_env_value(forbidden)?.is_some() {
                bail!(
                    "{forbidden} is not supported for VWorld cadastral ingest; use FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER"
                );
            }
        }
        let attr_filter = optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER")?;
        let dataset = optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_DATASET")?
            .unwrap_or_else(|| DEFAULT_DATASET.to_owned());
        let columns = optional_csv_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_COLUMNS")?
            .unwrap_or_else(default_columns);
        let geometry =
            optional_bool_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOMETRY")?.unwrap_or(true);
        let attribute =
            optional_bool_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTRIBUTE")?.unwrap_or(true);
        let crs = optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_CRS")?
            .or_else(|| Some("EPSG:4326".to_owned()));
        let page =
            optional_u32_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PAGE")?.unwrap_or(DEFAULT_PAGE);
        let size =
            optional_u32_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SIZE")?.unwrap_or(DEFAULT_SIZE);

        Ok(Self {
            source_slug: crate::public_data_control_support::resolve_canonical_source_slug(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SOURCE_SLUG",
                optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SOURCE_SLUG")?,
                GENERATOR_PROVIDER,
                DEFAULT_DATASET_SLUG,
            )?,
            base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_DATA_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            api_key: required_env_value("VWORLD_API_KEY")?,
            domain: optional_env_value("VWORLD_DOMAIN")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            requests: cadastral_requests_from_env(VWorldCadastralRequestSeed {
                dataset,
                attr_filter,
                pnu,
                columns,
                geometry,
                attribute,
                crs,
                page,
                size,
            })?,
            max_pages: optional_positive_u32_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_PAGES")?
                .unwrap_or(DEFAULT_MAX_PAGES),
            request_spacing: ProviderRequestSpacing::optional_from_millis(
                optional_u64_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MIN_PAGE_INTERVAL_MS")?.or(
                    optional_u64_env("FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS")?,
                ),
            )?,
            request_policy: VWorldRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
            live_write: optional_env_value("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_LIVE_WRITE")?,
        })
    }
}

fn source_catalog_entry(
    config: &VWorldCadastralIngestConfig,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: config.source_slug.clone(),
        name: SOURCE_NAME.to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: config.requests.first().map_or_else(
            || DEFAULT_DATASET.to_owned(),
            |request| request.dataset.clone(),
        ),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: Some(
            "https://www.vworld.kr/dev/v4dv_2ddataguide2_s002.do?svcIde=cadastral".to_owned(),
        ),
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
    config: &VWorldCadastralIngestConfig,
    plans: &[VWorldCadastralBronzePagePlan],
) -> JsonValue {
    let first_request = config.requests.first();
    json!({
        "dataset": first_request.map(|request| &request.dataset),
        "requestWindows": config.requests.len(),
        "firstAttrFilter": first_request.and_then(|request| request.attr_filter.as_ref()),
        "columns": first_request.map(|request| &request.columns),
        "geometry": first_request.map(|request| request.geometry),
        "attribute": first_request.map(|request| request.attribute),
        "crs": first_request.and_then(|request| request.crs.as_ref()),
        "startPage": first_request.map(|request| request.page),
        "size": first_request.map(|request| request.size),
        "maxPages": config.max_pages,
        "pagesPlanned": plans.len(),
        "format": "json"
    })
}

// `total_logical_record_count` over plans (the failure-path / completion accounting helper) now lives
// in the shared PageCollector loop, which owns the run lifecycle (ADR 0017). The dry-run summary still
// needs the planned-page variant below.
fn total_logical_record_count_of_pages(pages: &[VWorldCadastralPlannedPage]) -> u64 {
    pages
        .iter()
        .map(|page| page.plan.logical_record_count)
        .sum()
}

fn total_size_bytes_of_pages(pages: &[VWorldCadastralPlannedPage]) -> u64 {
    pages.iter().map(|page| page.plan.size_bytes).sum()
}

fn live_write_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

fn default_columns() -> Vec<String> {
    ["pnu", "jibun", "bonbun", "bubun", "addr", "ag_geom"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        json_u64_pointer, page_requests_for_batch, should_stop_after_page,
        VWorldCadastralPageRequest,
    };
    use crate::bronze_object_storage::{
        bronze_object_storage_driver_from_options, BronzeObjectStorageDriver,
    };
    use crate::pagination_guard::assert_page_window_complete;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn page_requests_cover_configured_batch_window() -> anyhow::Result<()> {
        let base_request = VWorldCadastralPageRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: Some("emdCd:=:11680103".to_owned()),
            columns: vec!["pnu".to_owned(), "ag_geom".to_owned()],
            geometry: true,
            attribute: true,
            crs: Some("EPSG:4326".to_owned()),
            page: 7,
            size: 1000,
        };

        let pages = page_requests_for_batch(&base_request, 3)?;

        assert_eq!(
            pages.iter().map(|request| request.page).collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert!(pages.iter().all(|request| {
            request.dataset == "LP_PA_CBND_BUBUN"
                && request.attr_filter.as_deref() == Some("emdCd:=:11680103")
                && request.columns == ["pnu".to_owned(), "ag_geom".to_owned()]
                && request.size == 1000
        }));
        Ok(())
    }

    #[test]
    fn stop_condition_uses_provider_total_count_before_short_page_fallback() {
        assert!(should_stop_after_page(22, 1000, 1000, Some(22_000)));
        assert!(!should_stop_after_page(21, 1000, 1000, Some(22_000)));
        assert!(should_stop_after_page(1, 1000, 0, Some(0)));
        assert!(should_stop_after_page(28, 1000, 582, None));
        assert!(!should_stop_after_page(27, 1000, 1000, None));
    }

    #[test]
    fn page_window_rejects_exhausted_cap_before_provider_total_is_complete() -> anyhow::Result<()> {
        let error = match assert_page_window_complete(
            "VWorld cadastral",
            1,
            1000,
            1000,
            1000,
            Some(2_500),
            1,
        ) {
            Ok(()) => anyhow::bail!("expected exhausted page cap to fail"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("page cap exhausted"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn parses_vworld_total_count_from_response_metadata() -> anyhow::Result<()> {
        let payload = json!({
            "response": {
                "record": {
                    "total": "22000"
                }
            }
        });
        assert_eq!(
            json_u64_pointer(&payload, "/response/record/total")?,
            Some(22_000)
        );
        assert_eq!(json_u64_pointer(&payload, "/response/missing")?, None);
        Ok(())
    }

    #[test]
    fn bronze_object_storage_driver_allows_local_root() -> anyhow::Result<()> {
        assert_eq!(
            bronze_object_storage_driver_from_options(Some("local"), Some("target/bronze"))?,
            BronzeObjectStorageDriver::Local(PathBuf::from("target/bronze"))
        );
        assert_eq!(
            bronze_object_storage_driver_from_options(None, None)?,
            BronzeObjectStorageDriver::R2
        );
        assert!(bronze_object_storage_driver_from_options(Some("local"), None).is_err());
        Ok(())
    }
}
