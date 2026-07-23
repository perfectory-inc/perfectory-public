//! Generic `VWorld` NED attribute Bronze ingestion command.

use std::collections::BTreeMap;

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_application::{
    plan_vworld_ned_bronze_page, VWorldNedBronzePagePlan, VWorldNedBronzePagePlanInput,
    VWorldNedPageRequest,
};
use collection_domain::{
    IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind, SourceCatalogEntry,
    SourcePayloadFormat,
};
use collection_infrastructure::{
    PgBronzeIngestUnitOfWork, VWorldNedAttributeClient, VWorldNedAttributeConfig,
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
    optional_csv_env, optional_duration_millis_env, optional_duration_seconds_env,
    optional_env_value, optional_positive_u32_env, optional_u32_env, optional_u64_env,
    required_env_value,
};

const PROVIDER: &str = "vworld";
/// Catalog-native provider label used by the canonical `source_slug` generator (ADR 0014 D2).
/// Distinct from `PROVIDER` (the source-catalog row's stored provider value).
const GENERATOR_PROVIDER: &str = "VWorld";
const DEFAULT_BASE_URI: &str = "https://api.vworld.kr/ned/data";
const DEFAULT_OPERATION: &str = "ladfrlList";
const DEFAULT_PARTITION_NAME: &str = "pnu";
const DEFAULT_MAX_PAGES: u32 = 1;
const DEFAULT_USER_AGENT: &str = "foundation-platform-vworld-ned-ingestor/1.0";
// The Bronze content-type / cache-control constants now live with the shared PageCollector loop
// (ADR 0017), which is the single place that builds the per-page commit input.

/// One fetched generic `VWorld` NED page: the compiled Bronze plan plus the RAW page identity +
/// parsed payload the [`BronzeCommitter`] needs to OWN the key-compile (ADR 0016).
///
/// The compiled `plan` drives the page-window completeness assertion, the dry-run summary, and the
/// run-level logical-record / size accounting; the persist stage hands the raw `request` +
/// `raw_payload` + `payload` to the committer, which re-runs the NED Bronze plan as its owned compile
/// step — the exact mirror of the building-register / real-transaction lanes.
#[derive(Clone, Debug)]
struct VWorldNedPlannedPage {
    plan: VWorldNedBronzePagePlan,
    request: VWorldNedPageRequest,
    raw_payload: Vec<u8>,
    payload: JsonValue,
}

/// Runs one generic `VWorld` NED attribute Bronze ingestion batch.
pub async fn run() -> anyhow::Result<()> {
    let config = VWorldNedAttributeIngestConfig::from_env()?;
    let client = VWorldNedAttributeClient::new_with_policy(
        &VWorldNedAttributeConfig {
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
            operation = %config.request.operation,
            objects_planned = pages.len(),
            logical_records_seen = total_logical_record_count_of_pages(&pages),
            total_size_bytes = total_size_bytes_of_pages(&pages),
            first_object_key = pages.first().map(|page| page.plan.object_key.as_str()),
            last_object_key = pages.last().map(|page| page.plan.object_key.as_str()),
            "VWorld NED attribute Bronze ingest dry run succeeded"
        );
        return Ok(());
    }

    persist_plans(&config, run_id, now, &pages).await
}

async fn plan_pages(
    config: &VWorldNedAttributeIngestConfig,
    client: &VWorldNedAttributeClient,
    run_id: IngestionRunId,
    ingest_date: chrono::NaiveDate,
) -> anyhow::Result<Vec<VWorldNedPlannedPage>> {
    let mut pages = Vec::new();
    let mut last_page_observation = None;
    for (request_index, request) in page_requests_for_batch(&config.request, config.max_pages)?
        .into_iter()
        .enumerate()
    {
        if let Some(spacing) = config.request_spacing {
            spacing.wait_before_request(request_index).await;
        }
        let fetched_page = client
            .fetch_json_page(
                &request.operation,
                &request.query_params,
                request.page_no,
                request.num_of_rows,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to fetch VWorld NED operation {} page {}",
                    request.operation, request.page_no
                )
            })?;
        let provider_total_count = match &config.total_count_pointer {
            Some(pointer) => json_u64_pointer(&fetched_page.payload, pointer)?,
            None => None,
        };
        let plan = plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload.clone(),
            payload: fetched_page.payload.clone(),
        })
        .with_context(|| {
            format!(
                "failed to plan VWorld NED Bronze operation {} page {}",
                request.operation, request.page_no
            )
        })?;
        let logical_record_count = plan.logical_record_count;
        pages.push(VWorldNedPlannedPage {
            plan,
            request: request.clone(),
            raw_payload: fetched_page.raw_payload,
            payload: fetched_page.payload,
        });
        last_page_observation = Some((
            request.page_no,
            request.num_of_rows,
            logical_record_count,
            provider_total_count,
        ));
        if should_stop_after_page(
            request.page_no,
            request.num_of_rows,
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
            "VWorld NED attribute",
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

fn page_requests_for_batch(
    base_request: &VWorldNedPageRequest,
    max_pages: u32,
) -> anyhow::Result<Vec<VWorldNedPageRequest>> {
    (0..max_pages)
        .map(|offset| {
            let page_no = base_request
                .page_no
                .checked_add(offset)
                .context("VWorld NED pageNo window exceeds u32")?;
            Ok(VWorldNedPageRequest {
                operation: base_request.operation.clone(),
                partition_name: base_request.partition_name.clone(),
                partition_value: base_request.partition_value.clone(),
                query_params: base_request.query_params.clone(),
                page_no,
                num_of_rows: base_request.num_of_rows,
                logical_items_pointer: base_request.logical_items_pointer.clone(),
                candidate_key_field_suffixes: base_request.candidate_key_field_suffixes.clone(),
            })
        })
        .collect()
}

async fn persist_plans(
    config: &VWorldNedAttributeIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[VWorldNedPlannedPage],
) -> anyhow::Result<()> {
    if pages.is_empty() {
        bail!("VWorld NED attribute ingest produced no Bronze page plans");
    }

    // Live-write path (`run` already gated on live_write before calling this): validate + log the
    // resolved R2 target before the first put, instead of failing mid-run on a misconfigured target.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("VWorld NED attribute live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for VWorld NED attribute ingest")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for VWorld NED attribute Bronze ingest")?;

    let report =
        persist_plans_with_adapters(config, run_id, started_at, pages, &uow, storage.as_ref())
            .await?;

    tracing::info!(
        run_id = %report.run_id,
        last_object_key = ?report.last_object_key,
        last_bronze_object_id = ?report.last_bronze_object_id,
        logical_record_count = report.logical_records_seen,
        objects_written = report.objects_written,
        "VWorld NED attribute Bronze ingest live write succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldNedAttributePersistReport {
    run_id: IngestionRunId,
    last_object_key: Option<String>,
    last_bronze_object_id: Option<BronzeObjectId>,
    logical_records_seen: u64,
    objects_written: u64,
}

async fn persist_plans_with_adapters<Uow, Storage>(
    config: &VWorldNedAttributeIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[VWorldNedPlannedPage],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<VWorldNedAttributePersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    if pages.is_empty() {
        bail!("VWorld NED attribute ingest produced no Bronze page plans");
    }

    // The compiled plans the planning stage already produced drive the run-level request snapshot;
    // the per-page commit + schema-profile + accounting now lives in the shared PageCollector loop
    // (ADR 0017), which hands each page's RAW identity to the BronzeCommitter (ADR 0016). The loop
    // body, ordering, `objects_written` accounting, commit-error -> terminal-failure mapping, run
    // lifecycle, and schema-profile gathering are identical across page lanes, so they live ONCE in
    // `collect_planned_pages`; this lane supplies only its catalog identity + per-source declaration.
    let plans: Vec<VWorldNedBronzePagePlan> = pages.iter().map(|page| page.plan.clone()).collect();

    let collectable_pages: Vec<CollectablePage<VWorldNedPageRequest>> = pages
        .iter()
        .map(|page| CollectablePage {
            plan: page.plan.clone(),
            request: page.request.clone(),
            raw_payload: page.raw_payload.clone(),
            payload: page.payload.clone(),
        })
        .collect();

    let report = collect_planned_pages(
        &VWorldNedLane,
        source_catalog_entry(config, started_at),
        ingestion_run(
            // The collector overrides `source_catalog_id` from the upserted source, so any placeholder
            // is fine here; pass the NED run identity + request snapshot as before.
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

    Ok(VWorldNedAttributePersistReport {
        run_id: report.run_id,
        last_object_key: report.last_object_key,
        last_bronze_object_id: report.last_bronze_object_id,
        logical_records_seen: report.logical_records_seen,
        objects_written: report.objects_written,
    })
}

/// VWorld NED attribute page lane declaration (ADR 0017): the only per-source bits the shared
/// [`collect_planned_pages`] loop needs — the lane label used in commit-error context, and the
/// candidate-key override (the generic NED lane applies none; per-operation candidate-key suffixes
/// already ride on the request). The loop, accounting, commit handoff, run lifecycle, and
/// schema-profile gathering all live in the collector.
struct VWorldNedLane;

impl PageCollectorLane for VWorldNedLane {
    type Request = VWorldNedPageRequest;

    fn lane_label(&self) -> &str {
        "VWorld NED attribute"
    }

    fn candidate_key_override(&self) -> CandidateKeyOverride {
        CandidateKeyOverride::None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VWorldNedOperationSpec {
    operation: &'static str,
    /// Canonical semantic dataset identity (ADR 0014 D3). The Bronze `source_slug` is derived from
    /// this through `collection_domain::source_slug("VWorld", dataset_slug)`, never hand-written. The
    /// provider-native `operation` (e.g. `ladfrlList`) is the API call id and is NOT the slug.
    dataset_slug: &'static str,
    source_name: &'static str,
    dataset_name: &'static str,
    logical_items_pointer: &'static str,
    total_count_pointer: Option<&'static str>,
    candidate_key_field_suffixes: &'static [&'static str],
}

const OPERATION_SPECS: &[VWorldNedOperationSpec] = &[
    VWorldNedOperationSpec {
        operation: "ladfrlList",
        dataset_slug: "land_register",
        source_name: "VWorld Land Register",
        dataset_name: "vworld-ned-ladfrl-list",
        logical_items_pointer: "/ladfrlVOList/ladfrlVOList",
        total_count_pointer: Some("/ladfrlVOList/totalCount"),
        candidate_key_field_suffixes: &["pnu"],
    },
    VWorldNedOperationSpec {
        operation: "getLandCharacteristic",
        dataset_slug: "land_characteristic",
        source_name: "VWorld Land Characteristic",
        dataset_name: "vworld-ned-land-characteristic",
        logical_items_pointer: "/landCharVOList/landCharVOList",
        total_count_pointer: Some("/landCharVOList/totalCount"),
        candidate_key_field_suffixes: &["pnu"],
    },
    VWorldNedOperationSpec {
        operation: "getIndvdLandPriceAttr",
        dataset_slug: "land_individual_price",
        source_name: "VWorld Individual Land Price",
        dataset_name: "vworld-ned-individual-land-price",
        logical_items_pointer: "/indvdLandPrices/field",
        total_count_pointer: Some("/indvdLandPrices/totalCount"),
        candidate_key_field_suffixes: &["pnu", "stdrYear"],
    },
    VWorldNedOperationSpec {
        operation: "getPossessionAttr",
        dataset_slug: "land_ownership",
        source_name: "VWorld Land Ownership",
        dataset_name: "vworld-ned-land-ownership",
        logical_items_pointer: "/possessions/field",
        total_count_pointer: Some("/possessions/totalCount"),
        candidate_key_field_suffixes: &["pnu"],
    },
    VWorldNedOperationSpec {
        operation: "getLandUseAttr",
        dataset_slug: "land_use_plan",
        source_name: "VWorld Land Use Plan",
        dataset_name: "vworld-ned-land-use-plan",
        logical_items_pointer: "/landUses/field",
        total_count_pointer: Some("/landUses/totalCount"),
        candidate_key_field_suffixes: &["pnu"],
    },
    VWorldNedOperationSpec {
        operation: "getLandMoveAttr",
        dataset_slug: "land_transfer_history",
        source_name: "VWorld Land Transfer History",
        dataset_name: "vworld-ned-land-transfer-history",
        logical_items_pointer: "/landMoves/field",
        total_count_pointer: Some("/landMoves/totalCount"),
        candidate_key_field_suffixes: &["pnu"],
    },
    VWorldNedOperationSpec {
        operation: "ldaregList",
        dataset_slug: "land_right_registration",
        source_name: "VWorld Land Right Registration",
        dataset_name: "vworld-ned-land-right-registration",
        logical_items_pointer: "/ldaregVOList/ldaregVOList",
        total_count_pointer: Some("/ldaregVOList/totalCount"),
        candidate_key_field_suffixes: &["pnu"],
    },
];

fn operation_specs() -> &'static [VWorldNedOperationSpec] {
    OPERATION_SPECS
}

fn operation_spec(operation: &str) -> Option<VWorldNedOperationSpec> {
    operation_specs()
        .iter()
        .copied()
        .find(|spec| spec.operation == operation)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VWorldNedAttributeIngestConfig {
    source_slug: String,
    source_name: String,
    dataset_name: String,
    base_uri: String,
    api_key: String,
    domain: Option<String>,
    user_agent: String,
    request: VWorldNedPageRequest,
    total_count_pointer: Option<String>,
    max_pages: u32,
    request_spacing: Option<ProviderRequestSpacing>,
    request_policy: VWorldRequestPolicy,
    live_write: Option<String>,
}

impl VWorldNedAttributeIngestConfig {
    fn from_env() -> anyhow::Result<Self> {
        let operation = optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_OPERATION")?
            .unwrap_or_else(|| DEFAULT_OPERATION.to_owned());
        let spec = operation_spec(&operation);
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
        let partition_name = optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_PARTITION_NAME")?
            .unwrap_or_else(|| DEFAULT_PARTITION_NAME.to_owned());
        let partition_value = required_env_value("FOUNDATION_PLATFORM_VWORLD_NED_PARTITION_VALUE")?;
        let query_params = query_params_from_parts(
            &partition_name,
            &partition_value,
            optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_EXTRA_QUERY_PARAMS")?.as_deref(),
        )?;
        let logical_items_pointer =
            optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_LOGICAL_ITEMS_POINTER")?
                .or_else(|| spec.map(|value| value.logical_items_pointer.to_owned()))
                .context(
                    "FOUNDATION_PLATFORM_VWORLD_NED_LOGICAL_ITEMS_POINTER is required for unsupported VWorld NED operation",
                )?;
        let total_count_pointer = optional_env_value(
            "FOUNDATION_PLATFORM_VWORLD_NED_TOTAL_COUNT_POINTER",
        )?
        .or_else(|| spec.and_then(|value| value.total_count_pointer.map(ToOwned::to_owned)));
        let candidate_key_field_suffixes =
            optional_csv_env("FOUNDATION_PLATFORM_VWORLD_NED_CANDIDATE_KEY_SUFFIXES")?
                .unwrap_or_else(|| {
                    spec.map_or_else(Vec::new, |value| {
                        value
                            .candidate_key_field_suffixes
                            .iter()
                            .map(|suffix| (*suffix).to_owned())
                            .collect()
                    })
                });

        Ok(Self {
            source_slug: match spec.map(|value| value.dataset_slug) {
                // Known operation: the override (if any) is validated against the canonical slug.
                Some(dataset_slug) => crate::public_data_control_support::resolve_canonical_source_slug(
                    "FOUNDATION_PLATFORM_VWORLD_NED_SOURCE_SLUG",
                    optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_SOURCE_SLUG")?,
                    GENERATOR_PROVIDER,
                    dataset_slug,
                )?,
                // Unsupported operation: there is no canonical dataset_slug to validate against, so
                // an explicit override is required (and trusted, as before).
                None => optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_SOURCE_SLUG")?.context(
                    "FOUNDATION_PLATFORM_VWORLD_NED_SOURCE_SLUG is required for unsupported VWorld NED operation",
                )?,
            },
            source_name: optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_SOURCE_NAME")?
                .or_else(|| spec.map(|value| value.source_name.to_owned()))
                .context(
                    "FOUNDATION_PLATFORM_VWORLD_NED_SOURCE_NAME is required for unsupported VWorld NED operation",
                )?,
            dataset_name: optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_DATASET_NAME")?
                .or_else(|| spec.map(|value| value.dataset_name.to_owned()))
                .context(
                    "FOUNDATION_PLATFORM_VWORLD_NED_DATASET_NAME is required for unsupported VWorld NED operation",
                )?,
            base_uri: optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_BASE_URI")?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            api_key: required_env_value("VWORLD_API_KEY")?,
            domain: optional_env_value("VWORLD_DOMAIN")?,
            user_agent: optional_env_value("FOUNDATION_PLATFORM_VWORLD_USER_AGENT")?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request: VWorldNedPageRequest {
                operation,
                partition_name,
                partition_value,
                query_params,
                page_no: optional_u32_env("FOUNDATION_PLATFORM_VWORLD_NED_PAGE_NO")?.unwrap_or(1),
                num_of_rows: optional_u32_env("FOUNDATION_PLATFORM_VWORLD_NED_NUM_OF_ROWS")?
                    .unwrap_or(1000),
                // The generic NED lane carries its per-operation logical pointer + candidate-key
                // suffixes ON the request, so the BronzeCommitter's `&self` key-compile (ADR 0016)
                // has the full lane input.
                logical_items_pointer,
                candidate_key_field_suffixes,
            },
            total_count_pointer,
            max_pages: optional_positive_u32_env("FOUNDATION_PLATFORM_VWORLD_NED_MAX_PAGES")?
                .unwrap_or(DEFAULT_MAX_PAGES),
            request_spacing: ProviderRequestSpacing::optional_from_millis(
                optional_u64_env("FOUNDATION_PLATFORM_VWORLD_NED_MIN_PAGE_INTERVAL_MS")?.or(
                    optional_u64_env("FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS")?,
                ),
            )?,
            request_policy: VWorldRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
            live_write: optional_env_value("FOUNDATION_PLATFORM_VWORLD_NED_LIVE_WRITE")?,
        })
    }
}

fn query_params_from_parts(
    partition_name: &str,
    partition_value: &str,
    raw_extra_params: Option<&str>,
) -> anyhow::Result<BTreeMap<String, String>> {
    let mut params = BTreeMap::from([(partition_name.to_owned(), partition_value.to_owned())]);
    for (name, value) in parse_extra_query_params(raw_extra_params)? {
        if let Some(existing) = params.get(&name) {
            if existing != &value {
                bail!("duplicate VWorld NED query parameter with different value: {name}");
            }
        }
        params.insert(name, value);
    }
    Ok(params)
}

fn parse_extra_query_params(raw: Option<&str>) -> anyhow::Result<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    let Some(raw) = raw else {
        return Ok(params);
    };
    for part in raw.split(';') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (name, value) = trimmed.split_once('=').context(
            "FOUNDATION_PLATFORM_VWORLD_NED_EXTRA_QUERY_PARAMS must use name=value pairs",
        )?;
        let name = name.trim();
        let value = value.trim();
        if name.is_empty() || value.is_empty() {
            bail!("VWorld NED extra query parameter name and value must not be empty");
        }
        if params.insert(name.to_owned(), value.to_owned()).is_some() {
            bail!("duplicate VWorld NED extra query parameter: {name}");
        }
    }
    Ok(params)
}

fn source_catalog_entry(
    config: &VWorldNedAttributeIngestConfig,
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
        terms_url: Some("https://www.vworld.kr/dev/v4apiRefer.do".to_owned()),
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
    config: &VWorldNedAttributeIngestConfig,
    plans: &[VWorldNedBronzePagePlan],
) -> JsonValue {
    json!({
        "operation": config.request.operation,
        "partitionName": config.request.partition_name,
        "partitionValue": config.request.partition_value,
        "queryParams": config.request.query_params,
        "startPageNo": config.request.page_no,
        "numOfRows": config.request.num_of_rows,
        "maxPages": config.max_pages,
        "pagesPlanned": plans.len(),
        "totalCountPointer": config.total_count_pointer,
        "format": "json"
    })
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
            .with_context(|| format!("VWorld NED JSON field {pointer} must be an unsigned integer"))
            .map(Some),
        JsonValue::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed
                .parse::<u64>()
                .with_context(|| {
                    format!("VWorld NED JSON field {pointer} must be an unsigned integer")
                })
                .map(Some)
        }
        _ => bail!("VWorld NED JSON field {pointer} must be an unsigned integer"),
    }
}

// `total_logical_record_count` over plans (the failure-path / completion accounting helper) now lives
// in the shared PageCollector loop, which owns the run lifecycle (ADR 0017). The dry-run summary still
// needs the planned-page variant below.
fn total_logical_record_count_of_pages(pages: &[VWorldNedPlannedPage]) -> u64 {
    pages
        .iter()
        .map(|page| page.plan.logical_record_count)
        .sum()
}

fn total_size_bytes_of_pages(pages: &[VWorldNedPlannedPage]) -> u64 {
    pages.iter().map(|page| page.plan.size_bytes).sum()
}

fn live_write_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use collection_domain::source_slug;

    #[test]
    fn operation_registry_derives_canonical_source_slugs() -> anyhow::Result<()> {
        for spec in operation_specs() {
            // The Bronze slug is generator-produced from the spec's dataset_slug (ADR 0014 D3).
            let slug = source_slug(GENERATOR_PROVIDER, spec.dataset_slug)?;
            assert_eq!(slug, format!("vworldkr__{}", spec.dataset_slug));
            assert!(
                spec.logical_items_pointer.starts_with('/'),
                "logical pointer must be JSON pointer: {}",
                spec.operation
            );
        }
        Ok(())
    }

    #[test]
    fn operation_registry_matches_observed_bronze_payload_shapes() {
        let pointers = operation_specs()
            .iter()
            .map(|spec| (spec.operation, spec.logical_items_pointer))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(pointers["ladfrlList"], "/ladfrlVOList/ladfrlVOList");
        assert_eq!(pointers["getIndvdLandPriceAttr"], "/indvdLandPrices/field");
        assert_eq!(pointers["getPossessionAttr"], "/possessions/field");
        assert_eq!(pointers["getLandUseAttr"], "/landUses/field");
        assert_eq!(pointers["getLandMoveAttr"], "/landMoves/field");
        assert_eq!(pointers["ldaregList"], "/ldaregVOList/ldaregVOList");
    }

    #[test]
    fn page_requests_cover_configured_batch_window() -> anyhow::Result<()> {
        let base_request = VWorldNedPageRequest {
            operation: "getLandCharacteristic".to_owned(),
            partition_name: "pnu".to_owned(),
            partition_value: "9999900101100010000".to_owned(),
            query_params: BTreeMap::from([("pnu".to_owned(), "9999900101100010000".to_owned())]),
            page_no: 7,
            num_of_rows: 100,
            logical_items_pointer: "/landCharVOList/landCharVOList".to_owned(),
            candidate_key_field_suffixes: vec!["pnu".to_owned()],
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
            request.operation == "getLandCharacteristic"
                && request.partition_name == "pnu"
                && request.partition_value == "9999900101100010000"
                && request.query_params["pnu"] == "9999900101100010000"
                && request.num_of_rows == 100
                && request.logical_items_pointer == "/landCharVOList/landCharVOList"
                && request.candidate_key_field_suffixes == vec!["pnu".to_owned()]
        }));
        Ok(())
    }

    #[test]
    fn query_params_include_partition_and_extra_values() -> anyhow::Result<()> {
        let params = query_params_from_parts("pnu", "9999900101100010000", Some("stdrYear=2024"))?;

        assert_eq!(params["pnu"], "9999900101100010000");
        assert_eq!(params["stdrYear"], "2024");
        Ok(())
    }

    #[test]
    fn parses_vworld_ned_total_count_from_operation_container() -> anyhow::Result<()> {
        let payload = json!({
            "landUses": {
                "field": [{"pnu": "9999900801105800001"}],
                "totalCount": "20"
            }
        });

        assert_eq!(
            json_u64_pointer(&payload, "/landUses/totalCount")?,
            Some(20)
        );
        assert_eq!(json_u64_pointer(&payload, "/missing/totalCount")?, None);
        Ok(())
    }

    #[test]
    fn stop_condition_uses_provider_total_count() {
        assert!(should_stop_after_page(2, 10, 10, Some(20)));
        assert!(!should_stop_after_page(1, 10, 10, Some(20)));
        assert!(should_stop_after_page(1, 10, 2, None));
        assert!(!should_stop_after_page(1, 10, 10, None));
    }

    // ---- Live persist path through the BronzeCommitter (Task 3) ----
    //
    // These mirror the real-transaction lane's persist tests. The put/record path now flows through
    // `committer.commit_vworld_ned_page` (CreateOnly + recoverable commit protocol), so the
    // R2-already-exists case is no longer an unconditional hard failure: a matching-checksum object
    // with a missing DB row RECOVERS, while a conflicting-checksum object fails loud.

    use std::sync::Mutex;

    use async_trait::async_trait;
    // `CompleteIngestionRunCommand` is only needed by the test `FakeBronzeUow`'s
    // `BronzeIngestUnitOfWork` impl now that the production loop (run lifecycle) lives in the shared
    // PageCollector (ADR 0017); the production code no longer references it directly.
    use collection_application::ports::CompleteIngestionRunCommand;
    use collection_domain::CollectionError;
    use collection_domain::{BronzeObject, IngestionRun, SchemaProfile};
    use foundation_outbox::{object_storage::PutObjectRequest, PublishError};

    fn test_config() -> VWorldNedAttributeIngestConfig {
        VWorldNedAttributeIngestConfig {
            source_slug: "vworldkr__land_characteristic".to_owned(),
            source_name: "VWorld Land Characteristic".to_owned(),
            dataset_name: "vworld-ned-land-characteristic".to_owned(),
            base_uri: DEFAULT_BASE_URI.to_owned(),
            api_key: "redacted-test-key".to_owned(),
            domain: None,
            user_agent: DEFAULT_USER_AGENT.to_owned(),
            request: VWorldNedPageRequest {
                operation: "getLandCharacteristic".to_owned(),
                partition_name: "pnu".to_owned(),
                partition_value: "9999900101100010000".to_owned(),
                query_params: BTreeMap::from([(
                    "pnu".to_owned(),
                    "9999900101100010000".to_owned(),
                )]),
                page_no: 1,
                num_of_rows: 1000,
                logical_items_pointer: "/landCharVOList/landCharVOList".to_owned(),
                candidate_key_field_suffixes: vec!["pnu".to_owned()],
            },
            total_count_pointer: Some("/landCharVOList/totalCount".to_owned()),
            max_pages: 1,
            request_spacing: None,
            request_policy: VWorldRequestPolicy::default(),
            live_write: Some("1".to_owned()),
        }
    }

    fn test_planned_page(
        config: &VWorldNedAttributeIngestConfig,
        run_id: IngestionRunId,
        page_no: u32,
    ) -> anyhow::Result<VWorldNedPlannedPage> {
        let payload = json!({
            "landCharVOList": { "landCharVOList": [
                { "pnu": "9999900101100010000", "page": format!("{page_no:03}") }
            ] }
        });
        let raw_payload = serde_json::to_vec(&payload)?;
        let request = VWorldNedPageRequest {
            page_no,
            ..config.request.clone()
        };
        let plan = plan_vworld_ned_bronze_page(VWorldNedBronzePagePlanInput {
            source_slug: &config.source_slug,
            ingest_date: chrono::NaiveDate::from_ymd_opt(2026, 6, 2)
                .ok_or_else(|| anyhow::anyhow!("valid date"))?,
            ingestion_run_id: run_id,
            request: request.clone(),
            raw_payload: raw_payload.clone(),
            payload: payload.clone(),
        })?;
        Ok(VWorldNedPlannedPage {
            plan,
            request,
            raw_payload,
            payload,
        })
    }

    /// Recoverable commit protocol end-to-end through the VWorld NED live persist path: the object is
    /// already in R2 with a matching checksum (a prior run's write) but no `bronze_object` row exists
    /// (that prior run's DB record failed). The CreateOnly write hits already-exists; the committer
    /// recovers by recording the missing row, so the run completes Succeeded — an R2-already-exists is
    /// no longer a hard failure.
    #[tokio::test]
    async fn persist_recovers_when_r2_object_exists_but_db_row_is_missing() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")?.to_utc();
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

    /// Quarantine terminal through the VWorld NED live persist path: the object is already in R2 but
    /// with a DIFFERENT checksum and no DB row. The committer cannot prove the object is ours, so it
    /// fails loud and the run is marked Failed (never silently overwritten).
    #[tokio::test]
    async fn persist_fails_loud_when_r2_object_checksum_conflicts() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")?.to_utc();
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

    /// A Bronze metadata record failure still fails the run, with the same context message the inline
    /// put+record path produced, and the object is counted as written (the CreateOnly write happened
    /// before the record).
    #[tokio::test]
    async fn persist_marks_run_failed_when_bronze_metadata_recording_fails() -> anyhow::Result<()> {
        let run_id = IngestionRunId::new(Uuid::new_v4());
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")?.to_utc();
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
                .contains("failed to record VWorld NED attribute Bronze object metadata"),
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
    }

    impl FakeObjectStorage {
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
