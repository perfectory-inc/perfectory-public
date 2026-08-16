//! ILIS (industryland.or.kr) industrial-complex address-source Bronze collection.
//!
//! The industrial-complex profile Bronze object carries no administrative location, so
//! `export-industrial-complex-bronze-raw-jsonl` takes the location as an injected address
//! resolution (root ADR-0033). Nothing produced that resolution: the only run that ever existed
//! used synthetic addresses. This command collects the source the resolution is derived from.
//!
//! `industryland.or.kr` is an approved provider (`APPROVED_PROVIDER_DOMAINS`, root ADR-0032), so
//! this is not a scratch download into a temporary directory: the raw responses land as Bronze
//! objects with a canonical `source_slug`, a `sha256`, a `catalog.bronze_object` ledger row, and
//! the write-once `CreateOnly` policy the [`BronzeCommitter`](collection_application::BronzeCommitter)
//! enforces.
//!
//! Two datasets, one bulk request each:
//!
//! | dataset_slug | endpoint | why |
//! |---|---|---|
//! | `industrial_complex_list` | `il/danji/list.do` | `danji_cd` + `danji_loc` + `addr_cd` per complex |
//! | `industrial_complex_notice` | `il/danref/list.do` | the notices whose addresses resolve a complex whose own `addr_cd` is stale |
//!
//! The request budget is the operator's, not this command's to spend: `..._MAX_REQUESTS` is a hard
//! ceiling checked before the first request, and the spacing between requests is required rather
//! than optional.

use std::{path::PathBuf, time::Duration};

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::{
    ports::BronzeIngestUnitOfWork, IlisIndustrialComplexPageRequest, PublicDataBronzePagePlan,
};
use collection_domain::{
    source_slug, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::{
    IlisIndustrialComplexApiClient, IlisIndustrialComplexApiConfig, PgBronzeIngestUnitOfWork,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::live_write_bronze_object_storage_from_env;
use crate::bronze_schema_profile::CandidateKeyOverride;
use crate::page_collector::{collect_planned_pages, CollectablePage, PageCollectorLane};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};

/// Catalog-native provider label. `provider_id` derives `industrylandorkr` from it (root ADR-0032).
const PROVIDER: &str = "industryland.or.kr";
const DEFAULT_BASE_URI: &str = "https://www.industryland.or.kr";
const DEFAULT_USER_AGENT: &str =
    "foundation-platform-industrial-complex-address-source/1.0 (+https://github.com/perfectory)";
const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_ADDRESS_SOURCE";

/// Minimum spacing between two requests at this provider.
///
/// Not a default that a smaller value can override: [`ProviderRequestSpacing`] below is clamped up
/// to this floor, because the politeness budget belongs to the provider and not to whoever is
/// impatient today.
const MIN_REQUEST_SPACING: Duration = Duration::from_secs(2);

/// Hard ceiling on requests per run when the operator sets none.
const DEFAULT_MAX_REQUESTS: usize = 8;

/// One collectable ILIS dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IlisDataset {
    /// Canonical dataset slug, the second half of the Bronze `source_slug`.
    pub(crate) dataset_slug: &'static str,
    /// Provider-native operation recorded in lineage and in the Bronze object key.
    pub(crate) operation: &'static str,
    /// Provider-native endpoint path.
    pub(crate) endpoint_path: &'static str,
    /// Bulk page size. Larger than the known row count so one request returns the whole set.
    pub(crate) page_size: u32,
    /// JSON pointer to the logical records in the response.
    pub(crate) logical_items_pointer: &'static str,
    /// Human-readable dataset name recorded on the source catalog entry.
    pub(crate) dataset_name: &'static str,
}

/// The two bulk datasets the address resolution is derived from.
///
/// A page walk over these endpoints would be 148 requests for the same bytes. One bulk request per
/// dataset is both cheaper for us and politer to the provider, which is why `page_size` is a
/// property of the dataset rather than a knob.
pub(crate) const ILIS_DATASETS: &[IlisDataset] = &[
    IlisDataset {
        dataset_slug: "industrial_complex_list",
        operation: "danjiList",
        endpoint_path: "il/danji/list.do",
        page_size: 2_000,
        logical_items_pointer: "/result/dataList",
        dataset_name: "ILIS 산업단지 목록",
    },
    IlisDataset {
        dataset_slug: "industrial_complex_notice",
        operation: "danrefList",
        endpoint_path: "il/danref/list.do",
        page_size: 20_000,
        logical_items_pointer: "/result/dataList",
        dataset_name: "ILIS 산업단지 고시 목록",
    },
];

/// The per-complex fallback for the complexes the bulk list does not carry.
///
/// It costs one request per complex, so it is never run over the whole set: the resolution build
/// names the exact codes it could not resolve, and the operator passes those back here. That loop
/// is why `..._DETAIL_COMPLEX_CODES` is an explicit input rather than something this command infers.
pub(crate) const ILIS_DETAIL_DATASET: IlisDataset = IlisDataset {
    dataset_slug: "industrial_complex_detail",
    operation: "danjiDetInfo",
    endpoint_path: "il/danji/det/info.do",
    // The detail endpoint takes no paging; the shared planner still requires a positive page size,
    // and one record per object is exactly what this returns.
    page_size: 1,
    // The detail endpoint nests one level deeper than the bulk endpoints.
    logical_items_pointer: "/result/data",
    dataset_name: "ILIS 산업단지 상세",
};

/// Partition name for a national bulk object.
const NATIONAL_SCOPE_PARTITION_NAME: &str = "scope";
const NATIONAL_SCOPE_PARTITION_VALUE: &str = "national";

/// Partition name for a per-complex detail object.
const COMPLEX_PARTITION_NAME: &str = "complex";

struct AddressSourceCollectConfig {
    base_uri: String,
    user_agent: String,
    request_spacing: ProviderRequestSpacing,
    max_requests: usize,
    live_write: bool,
    summary_path: Option<PathBuf>,
    /// Complexes to fetch individually, because the bulk list does not carry them.
    detail_complex_codes: Vec<String>,
    /// Whether to collect the two bulk datasets. Off when only the detail fallback is wanted, so a
    /// second run does not re-request 15MB the Bronze root already holds.
    collect_bulk_datasets: bool,
}

/// Evidence returned by one collection run.
#[derive(Clone, Debug, Eq, PartialEq)]
struct AddressSourceCollectReport {
    requests_sent: usize,
    datasets: Vec<AddressSourceDatasetReport>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AddressSourceDatasetReport {
    source_slug: String,
    object_key: String,
    checksum_sha256: String,
    size_bytes: u64,
    logical_record_count: u64,
    written: bool,
}

/// Collects the ILIS address source into Bronze.
///
/// # Errors
/// Returns an error when the request budget cannot cover the datasets, a provider request fails,
/// the response does not carry the expected records array, or the Bronze commit fails.
pub async fn run() -> anyhow::Result<()> {
    let config = AddressSourceCollectConfig::from_env()?;
    let client = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent.clone(),
    })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let started_at = Utc::now();
    let fetched = fetch_datasets(&config, &client).await?;

    if !config.live_write {
        tracing::info!(
            requests_sent = fetched.len(),
            datasets = fetched.len(),
            "ILIS industrial-complex address-source dry run succeeded; set {}_LIVE_WRITE=1 to \
             commit the Bronze objects",
            PREFIX
        );
        return Ok(());
    }

    let report = persist(&config, run_id, started_at, &fetched).await?;
    if let Some(summary_path) = config.summary_path.as_ref() {
        write_json_file(summary_path, &summary_json(&config, &report))?;
    }
    tracing::info!(
        requests_sent = report.requests_sent,
        objects = report.datasets.len(),
        "ILIS industrial-complex address-source Bronze collection succeeded"
    );
    Ok(())
}

/// One fetched dataset, planned but not yet committed.
struct FetchedDataset {
    dataset: IlisDataset,
    source_slug: String,
    request: IlisIndustrialComplexPageRequest,
    raw_payload: Vec<u8>,
    payload: JsonValue,
}

/// Number of provider requests one run will send, before any of them is sent.
pub(crate) fn planned_request_count(
    collect_bulk_datasets: bool,
    detail_complex_codes: &[String],
) -> usize {
    let bulk = if collect_bulk_datasets {
        ILIS_DATASETS.len()
    } else {
        0
    };
    bulk + detail_complex_codes.len()
}

async fn fetch_datasets(
    config: &AddressSourceCollectConfig,
    client: &IlisIndustrialComplexApiClient,
) -> anyhow::Result<Vec<FetchedDataset>> {
    // The budget is checked BEFORE the first request, not counted down during the run: a run that
    // cannot finish inside the operator's ceiling must not spend half of it first.
    let planned = planned_request_count(
        config.collect_bulk_datasets,
        config.detail_complex_codes.as_slice(),
    );
    if planned == 0 {
        bail!(
            "this run would send no provider requests; set {PREFIX}_DETAIL_COMPLEX_CODES or leave \
             {PREFIX}_COLLECT_BULK_DATASETS on"
        );
    }
    if planned > config.max_requests {
        bail!(
            "{PREFIX}_MAX_REQUESTS is {} but this run needs {planned} provider requests",
            config.max_requests
        );
    }

    let mut fetched = Vec::with_capacity(planned);
    let mut request_index = 0_usize;
    if config.collect_bulk_datasets {
        for dataset in ILIS_DATASETS {
            config
                .request_spacing
                .wait_before_request(request_index)
                .await;
            request_index += 1;
            let page = client
                .fetch_page(dataset.endpoint_path, 1, dataset.page_size)
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch the ILIS {} bulk page from {}",
                        dataset.dataset_slug, dataset.endpoint_path
                    )
                })?;
            assert_records_present(dataset, &page.payload)?;
            fetched.push(fetched_dataset(
                *dataset,
                NATIONAL_SCOPE_PARTITION_NAME,
                NATIONAL_SCOPE_PARTITION_VALUE,
                page.raw_payload,
                page.payload,
            )?);
        }
    }
    for code in &config.detail_complex_codes {
        config
            .request_spacing
            .wait_before_request(request_index)
            .await;
        request_index += 1;
        let page = client
            .fetch_detail(ILIS_DETAIL_DATASET.endpoint_path, code)
            .await
            .with_context(|| format!("failed to fetch the ILIS detail for complex {code}"))?;
        assert_records_present(&ILIS_DETAIL_DATASET, &page.payload)?;
        fetched.push(fetched_dataset(
            ILIS_DETAIL_DATASET,
            COMPLEX_PARTITION_NAME,
            code.as_str(),
            page.raw_payload,
            page.payload,
        )?);
    }
    Ok(fetched)
}

fn fetched_dataset(
    dataset: IlisDataset,
    partition_name: &str,
    partition_value: &str,
    raw_payload: Vec<u8>,
    payload: JsonValue,
) -> anyhow::Result<FetchedDataset> {
    Ok(FetchedDataset {
        dataset,
        source_slug: source_slug(PROVIDER, dataset.dataset_slug)?,
        request: IlisIndustrialComplexPageRequest {
            operation: dataset.operation.to_owned(),
            partition_name: partition_name.to_owned(),
            partition_value: partition_value.to_owned(),
            page_no: 1,
            page_size: dataset.page_size,
            logical_items_pointer: dataset.logical_items_pointer.to_owned(),
            candidate_key_field_suffixes: vec!["danji_cd".to_owned()],
        },
        raw_payload,
        payload,
    })
}

/// Fails when the provider's records are not where the dataset says they are.
///
/// The bulk endpoints answer with an array and the detail endpoint with a single object; both are
/// records. The message names every array the response DOES carry, with its length. A provider that
/// renames its envelope is a real event, and the operator needs the new shape in the failure rather
/// than a silently zero-row Bronze object that looks collected.
fn assert_records_present(dataset: &IlisDataset, payload: &JsonValue) -> anyhow::Result<()> {
    match payload.pointer(dataset.logical_items_pointer) {
        Some(JsonValue::Array(_)) => return Ok(()),
        Some(JsonValue::Object(fields)) if !fields.is_empty() => return Ok(()),
        _ => {}
    }
    let mut observed = Vec::new();
    collect_array_pointers("", payload, &mut observed);
    let observed = if observed.is_empty() {
        "none".to_owned()
    } else {
        observed.join(", ")
    };
    bail!(
        "the ILIS {} response carries no records at {}; arrays observed: {observed}",
        dataset.dataset_slug,
        dataset.logical_items_pointer
    )
}

/// Collects every JSON pointer whose value is an array, with its length, down to depth three.
fn collect_array_pointers(prefix: &str, value: &JsonValue, found: &mut Vec<String>) {
    if prefix.matches('/').count() > 3 {
        return;
    }
    match value {
        JsonValue::Array(items) => found.push(format!("{prefix} (len {})", items.len())),
        JsonValue::Object(entries) => {
            for (name, child) in entries {
                collect_array_pointers(&format!("{prefix}/{name}"), child, found);
            }
        }
        _ => {}
    }
}

async fn persist(
    config: &AddressSourceCollectConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    fetched: &[FetchedDataset],
) -> anyhow::Result<AddressSourceCollectReport> {
    crate::bronze_object_storage::live_write_target_preflight()
        .context("ILIS industrial-complex address-source live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for ILIS address-source Bronze collection")?;
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = live_write_bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for ILIS address-source Bronze collection")?;

    persist_with_adapters(config, run_id, started_at, fetched, &uow, storage.as_ref()).await
}

async fn persist_with_adapters<Uow, Storage>(
    config: &AddressSourceCollectConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    fetched: &[FetchedDataset],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<AddressSourceCollectReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    if fetched.is_empty() {
        bail!("ILIS address-source collection produced no Bronze page plans");
    }
    let mut datasets = Vec::with_capacity(fetched.len());
    for (dataset_index, item) in fetched.iter().enumerate() {
        // Each dataset is its own source catalog entry and its own ingestion run: they are two
        // datasets, and folding them into one run would make the ledger claim a single collection
        // that never happened.
        let dataset_run_id = if dataset_index == 0 {
            run_id
        } else {
            IngestionRunId::new(Uuid::new_v4())
        };
        let plan = compile_plan(item, dataset_run_id, started_at)?;
        let report = collect_planned_pages(
            &IlisLane,
            source_catalog_entry(config, item, started_at),
            ingestion_run(
                SourceCatalogId::new(Uuid::nil()),
                dataset_run_id,
                started_at,
                request_params(item),
            ),
            item.source_slug.as_str(),
            started_at,
            &[CollectablePage {
                plan: plan.clone(),
                request: item.request.clone(),
                raw_payload: item.raw_payload.clone(),
                payload: item.payload.clone(),
            }],
            uow,
            storage,
        )
        .await?;
        datasets.push(AddressSourceDatasetReport {
            source_slug: item.source_slug.clone(),
            object_key: plan.object_key.as_str().to_owned(),
            checksum_sha256: plan.checksum_sha256.clone(),
            size_bytes: plan.size_bytes,
            logical_record_count: plan.logical_record_count,
            written: report.objects_written > 0,
        });
    }
    Ok(AddressSourceCollectReport {
        requests_sent: fetched.len(),
        datasets,
    })
}

fn compile_plan(
    item: &FetchedDataset,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
) -> anyhow::Result<PublicDataBronzePagePlan> {
    collection_application::plan_ilis_industrial_complex_bronze_page(
        collection_application::IlisIndustrialComplexBronzePagePlanInput {
            source_slug: item.source_slug.as_str(),
            ingest_date: started_at.date_naive(),
            ingestion_run_id: run_id,
            request: item.request.clone(),
            raw_payload: item.raw_payload.clone(),
            payload: item.payload.clone(),
        },
    )
    .with_context(|| {
        format!(
            "failed to plan the ILIS {} Bronze object",
            item.dataset.dataset_slug
        )
    })
}

/// ILIS page lane declaration (ADR 0017).
struct IlisLane;

impl PageCollectorLane for IlisLane {
    type Request = IlisIndustrialComplexPageRequest;

    fn lane_label(&self) -> &str {
        "ILIS industrial-complex address source"
    }

    fn candidate_key_override(&self) -> CandidateKeyOverride {
        // `danji_cd` is the join key the whole address resolution is built on (root ADR-0020: the
        // join is by code, never by name and never by geometry), so it is the candidate key.
        CandidateKeyOverride::LastDotSegmentEquals("danji_cd")
    }
}

impl AddressSourceCollectConfig {
    fn from_env() -> anyhow::Result<Self> {
        let spacing_millis = optional_env_value(&format!("{PREFIX}_REQUEST_SPACING_MS"))?
            .map(|value| value.trim().parse::<u64>())
            .transpose()
            .with_context(|| format!("{PREFIX}_REQUEST_SPACING_MS must be a whole number"))?;
        let max_requests = optional_env_value(&format!("{PREFIX}_MAX_REQUESTS"))?
            .map(|value| value.trim().parse::<usize>())
            .transpose()
            .with_context(|| format!("{PREFIX}_MAX_REQUESTS must be a whole number"))?
            .unwrap_or(DEFAULT_MAX_REQUESTS);
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| DEFAULT_BASE_URI.to_owned()),
            user_agent: optional_env_value(&format!("{PREFIX}_USER_AGENT"))?
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_owned()),
            request_spacing: request_spacing(spacing_millis)?,
            max_requests,
            live_write: live_write_enabled(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?.as_deref(),
            ),
            summary_path: optional_env_value(&format!("{PREFIX}_SUMMARY_PATH"))?.map(PathBuf::from),
            detail_complex_codes: detail_complex_codes(
                optional_env_value(&format!("{PREFIX}_DETAIL_COMPLEX_CODES"))?.as_deref(),
            )?,
            collect_bulk_datasets: !matches!(
                optional_env_value(&format!("{PREFIX}_COLLECT_BULK_DATASETS"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("0" | "false" | "no")
            ),
        })
    }
}

/// Parses the explicit list of complexes to fetch individually.
///
/// Every entry is one provider request, so a malformed entry fails here rather than at the
/// provider, and a repeated one is rejected instead of being requested twice.
fn detail_complex_codes(value: Option<&str>) -> anyhow::Result<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let mut codes = Vec::new();
    for raw in value.split(',') {
        let code = raw.trim();
        if code.is_empty() {
            continue;
        }
        if !code.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            bail!("{PREFIX}_DETAIL_COMPLEX_CODES must be ASCII alphanumeric codes: {code:?}");
        }
        if codes.iter().any(|existing| existing == code) {
            bail!("{PREFIX}_DETAIL_COMPLEX_CODES repeats {code:?}; each code is one request");
        }
        codes.push(code.to_owned());
    }
    Ok(codes)
}

/// Builds the request spacing, clamped up to [`MIN_REQUEST_SPACING`].
///
/// A configured value may only make the run slower. The floor is the provider's, and an env var is
/// not authority to go below it.
fn request_spacing(configured_millis: Option<u64>) -> anyhow::Result<ProviderRequestSpacing> {
    let configured = configured_millis.map_or(MIN_REQUEST_SPACING, Duration::from_millis);
    ProviderRequestSpacing::try_new(configured.max(MIN_REQUEST_SPACING))
}

fn live_write_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes")
    )
}

fn source_catalog_entry(
    config: &AddressSourceCollectConfig,
    item: &FetchedDataset,
    now: chrono::DateTime<Utc>,
) -> SourceCatalogEntry {
    SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: item.source_slug.clone(),
        name: item.dataset.dataset_name.to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: item.dataset.dataset_slug.to_owned(),
        base_url: Some(config.base_uri.clone()),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
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

fn request_params(item: &FetchedDataset) -> JsonValue {
    let is_detail = item.request.partition_name == COMPLEX_PARTITION_NAME;
    json!({
        "operation": item.dataset.operation,
        "endpointPath": item.dataset.endpoint_path,
        "partitionName": item.request.partition_name,
        "partitionValue": item.request.partition_value,
        "pageNo": 1,
        "pageSize": item.dataset.page_size,
        "logicalItemsPointer": item.dataset.logical_items_pointer,
        "method": if is_detail { "GET" } else { "POST" },
    })
}

fn summary_json(
    config: &AddressSourceCollectConfig,
    report: &AddressSourceCollectReport,
) -> JsonValue {
    json!({
        "schema_version": "foundation-platform.industrial_complex_address_source_collect.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "provider": PROVIDER,
        "base_uri": config.base_uri,
        "user_agent": config.user_agent,
        "request_spacing_ms": config.request_spacing.interval_millis(),
        "max_requests": config.max_requests,
        "collect_bulk_datasets": config.collect_bulk_datasets,
        "detail_complex_codes": config.detail_complex_codes,
        "requests_sent": report.requests_sent,
        "objects": report.datasets.iter().map(|dataset| json!({
            "source_slug": dataset.source_slug,
            "object_key": dataset.object_key,
            "checksum_sha256": dataset.checksum_sha256,
            "size_bytes": dataset.size_bytes,
            "logical_record_count": dataset.logical_record_count,
            "written": dataset.written,
        })).collect::<Vec<_>>(),
        "evidence_limitations": [
            "bronze_collection_only",
            "does_not_build_the_address_resolution",
            "does_not_approve_production_cutover",
        ],
    })
}

#[cfg(test)]
mod tests;
