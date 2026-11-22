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
//! enforces. Every object is content-qualified (`page-000001--sha256-<hex>.json`, root ADR-0152): the
//! provider answers the same request with new bytes from one day to the next, so a new answer lands
//! beside the earlier ones and the same answer again is the same object. A run reports `changed`
//! only when it recorded an answer the ledger did not hold.
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
/// It costs one request per complex, so it is never run over the whole set. A live run asks for
/// the complexes of the newest profile workbook in Bronze that the bulk list it just fetched does
/// not carry ([`unlisted_profile_codes`]); `..._DETAIL_COMPLEX_CODES` replaces that set when an
/// operator names the codes.
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
    let mut config = AddressSourceCollectConfig::from_env()?;
    let client = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
        base_uri: config.base_uri.clone(),
        user_agent: config.user_agent.clone(),
    })?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let started_at = Utc::now();
    // A live run opens its ledger and store before the first request: the detail codes are read
    // off the newest profile in Bronze, and a run that could not write must not ask first.
    let target = if config.live_write {
        Some(LiveTarget::open().await?)
    } else {
        None
    };
    let mut fetched = fetch_bulk_datasets(&config, &client).await?;
    if let (Some(target), true) = (target.as_ref(), config.detail_complex_codes.is_empty()) {
        if let Some(list) = fetched
            .iter()
            .find(|item| item.dataset.dataset_slug == COMPLEX_LIST_DATASET_SLUG)
        {
            let profile_codes = newest_profile_codes(&target.pool, target.storage.as_ref()).await?;
            config.detail_complex_codes = unlisted_profile_codes(&profile_codes, &list.payload);
        }
    }
    let sent = fetched.len();
    fetched.extend(fetch_detail_datasets(&config, &client, sent).await?);

    let Some(target) = target else {
        tracing::info!(
            requests_sent = fetched.len(),
            datasets = fetched.len(),
            "ILIS industrial-complex address-source dry run succeeded; set {}_LIVE_WRITE=1 to \
             commit the Bronze objects",
            PREFIX
        );
        return Ok(());
    };

    let uow = PgBronzeIngestUnitOfWork::new(target.pool.clone());
    let report = persist_with_adapters(
        &config,
        run_id,
        started_at,
        &fetched,
        &uow,
        target.storage.as_ref(),
    )
    .await?;
    if let Some(summary_path) = config.summary_path.as_ref() {
        write_json_file(summary_path, &summary_json(&config, &report))?;
    }
    tracing::info!(
        requests_sent = report.requests_sent,
        objects = report.datasets.len(),
        "ILIS industrial-complex address-source Bronze collection succeeded"
    );
    // A new object is what starts the industrial-complex Silver load (root ADR-0565 §5); an
    // answer identical to the last one commits nothing and starts nothing.
    let changed = report.datasets.iter().any(|dataset| dataset.written);
    println!(
        "foundation-job-outcome {}",
        if changed { "changed" } else { "unchanged" }
    );
    Ok(())
}

/// The bulk list's dataset slug: the complexes it carries need no detail request.
const COMPLEX_LIST_DATASET_SLUG: &str = "industrial_complex_list";

/// The profile source whose complexes the address resolution covers (the producer's default).
const PROFILE_SOURCE_SLUG: &str = "vworldkr__sandan_profile";

/// The ledger and the Bronze store a live run writes to.
struct LiveTarget {
    pool: PgPool,
    storage: Box<dyn ObjectStorageService>,
}

impl LiveTarget {
    async fn open() -> anyhow::Result<Self> {
        crate::bronze_object_storage::live_write_target_preflight()
            .context("ILIS industrial-complex address-source live-write target preflight failed")?;
        let database_url = required_env_value("DATABASE_URL")?;
        let pool = PgPool::connect(&database_url)
            .await
            .context("failed to connect to database for ILIS address-source Bronze collection")?;
        let storage = live_write_bronze_object_storage_from_env().await.context(
            "failed to configure object storage for ILIS address-source Bronze collection",
        )?;
        Ok(Self { pool, storage })
    }
}

/// The complex codes of the newest profile workbook in Bronze (the ledger's newest `.zip` by the
/// provider's date, then by collection).
///
/// The workbook is a few hundred kilobytes; it is read whole and decoded by the producer's own
/// decoder, so the codes asked for are the codes the export will resolve.
async fn newest_profile_codes(
    pool: &PgPool,
    storage: &dyn ObjectStorageService,
) -> anyhow::Result<Vec<String>> {
    let key = sqlx::query_scalar::<_, String>(
        "SELECT b.object_key FROM catalog.bronze_object b \
         JOIN catalog.source_catalog s ON s.id = b.source_catalog_id \
         WHERE s.slug = $1 AND b.object_key LIKE '%.zip' \
         ORDER BY b.provider_updated_at DESC NULLS LAST, b.collected_at DESC, b.id DESC LIMIT 1",
    )
    .bind(PROFILE_SOURCE_SLUG)
    .fetch_optional(pool)
    .await
    .context("failed to read the newest industrial-complex profile from the Bronze ledger")?
    .with_context(|| format!("the Bronze ledger holds no {PROFILE_SOURCE_SLUG} workbook"))?;
    let bytes = storage
        .read_object_bytes(&key)
        .await
        .with_context(|| format!("failed to read the Bronze object {key}"))?;
    let mut file = tempfile::NamedTempFile::new()?;
    std::io::Write::write_all(&mut file, &bytes)?;
    let sheet =
        crate::industrial_complex_bronze_raw_jsonl_export::decode_profile_rows_from_bronze_zip(
            file.path(),
            None,
            None,
        )?;
    Ok(sheet
        .records
        .iter()
        .map(|record| record.official_complex_code.trim().to_owned())
        .collect())
}

/// The profile's complex codes the bulk list does not carry, in profile order, each once: the
/// complexes only the detail endpoint answers (measured 2026-08-16: four of 1,442).
pub(crate) fn unlisted_profile_codes(profile_codes: &[String], list: &JsonValue) -> Vec<String> {
    let listed: std::collections::BTreeSet<&str> = list
        .pointer(ILIS_DATASETS[0].logical_items_pointer)
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|record| record.get("danji_cd").and_then(JsonValue::as_str))
        .map(str::trim)
        .collect();
    let mut seen = std::collections::BTreeSet::new();
    profile_codes
        .iter()
        .map(|code| code.trim())
        .filter(|code| !code.is_empty() && !listed.contains(code))
        .filter(|code| seen.insert(*code))
        .map(str::to_owned)
        .collect()
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

/// Refuses a run whose requests the operator's ceiling cannot cover, before the requests it counts.
fn within_budget(config: &AddressSourceCollectConfig) -> anyhow::Result<usize> {
    // The budget is checked BEFORE the requests, not counted down during the run: a run that
    // cannot finish inside the operator's ceiling must not spend half of it first. A live run
    // that derives its detail codes is checked again, before the first detail request, once the
    // bulk list has said which codes it lacks.
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
    Ok(planned)
}

async fn fetch_bulk_datasets(
    config: &AddressSourceCollectConfig,
    client: &IlisIndustrialComplexApiClient,
) -> anyhow::Result<Vec<FetchedDataset>> {
    let planned = within_budget(config)?;
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
    Ok(fetched)
}

async fn fetch_detail_datasets(
    config: &AddressSourceCollectConfig,
    client: &IlisIndustrialComplexApiClient,
    already_sent: usize,
) -> anyhow::Result<Vec<FetchedDataset>> {
    if config.detail_complex_codes.is_empty() {
        return Ok(Vec::new());
    }
    within_budget(config)?;
    // Derived codes come from a workbook, not the operator: held to the operator's rule all the
    // same, since each is a path segment of a provider request.
    let codes = detail_complex_codes(Some(&config.detail_complex_codes.join(",")))?;
    let mut fetched = Vec::with_capacity(codes.len());
    for (request_index, code) in (already_sent..).zip(codes.iter()) {
        config
            .request_spacing
            .wait_before_request(request_index)
            .await;
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
        let entry = source_catalog_entry(config, item, started_at);
        let report = collect_planned_pages(
            &IlisLane,
            entry.clone(),
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
        let written = recorded_by_run(uow, &entry, plan.object_key.as_str(), report.run_id).await?;
        datasets.push(AddressSourceDatasetReport {
            source_slug: item.source_slug.clone(),
            object_key: plan.object_key.as_str().to_owned(),
            checksum_sha256: plan.checksum_sha256.clone(),
            size_bytes: plan.size_bytes,
            logical_record_count: plan.logical_record_count,
            written,
        });
    }
    Ok(AddressSourceCollectReport {
        requests_sent: fetched.len(),
        datasets,
    })
}

/// Whether this run recorded the ledger row of `object_key`: the answer is new to Bronze.
///
/// The committer succeeds without writing when Bronze already holds the answer (the same bytes have
/// the same content-qualified key), and the collector's commit count counts that success all the
/// same. Only the row says which run first recorded the answer, so an answer equal to an earlier
/// one reports `unchanged` and starts no Silver load.
async fn recorded_by_run<Uow>(
    uow: &Uow,
    entry: &SourceCatalogEntry,
    object_key: &str,
    run_id: IngestionRunId,
) -> anyhow::Result<bool>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
{
    let source = uow
        .upsert_source_catalog_entry(entry)
        .await
        .with_context(|| format!("failed to read the {} source catalog entry", entry.slug))?;
    let row = uow
        .find_bronze_object_by_object_key(source.id, object_key)
        .await
        .with_context(|| format!("failed to read the ledger row of {object_key}"))?
        .with_context(|| format!("the ledger holds no row for the committed {object_key}"))?;
    Ok(row.ingestion_run_id == run_id)
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
