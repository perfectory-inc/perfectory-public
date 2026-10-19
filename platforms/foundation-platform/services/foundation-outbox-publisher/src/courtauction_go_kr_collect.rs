//! 법원경매정보 (courtauction.go.kr) list searches, Bronze collection (root ADR-0280, ADR-0281).
//!
//! The collection manners are the contract's (`courtauction-go-kr.contract.json`), and this module
//! only carries them out:
//!
//! - one plain HTTP client, no proxy (environment proxies are switched off), one request at a time,
//!   at least the contract's spacing apart, identified by a fixed User-Agent naming our product (a
//!   contact only when one is set, root ADR-0283);
//! - only inside the quiet hours (KST), and never more than the per-run request bound;
//! - robots.txt first, every run: a disallowed search path is not asked;
//! - any refusal signal (403, 429, 503, a Retry-After header, an HTML page where JSON belongs, the
//!   site's `ipcheck=false`) ends the run at once, lands what was collected, and writes `refused.json`
//!   in the state directory; while that file exists no run starts. A person removes it.
//!
//! Each search's responses of one run land as one gzip JSON Lines Bronze object, one line per page
//! request: the request body, the HTTP status and the response body as received. The Silver load
//! (`infra/lakehouse/spark/jobs/courtauction_go_kr.py`) parses them.

use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_public_data_bulk_file, BronzeCommitter, BronzePayload, PlannedBronzeObject,
    PublicDataBulkFileIdentity, PublicDataBulkFilePlan, PublicDataBulkFilePlanInput,
};
use collection_domain::{
    source_slug, BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use reqwest::header::{ACCEPT, CONTENT_TYPE, RETRY_AFTER};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
    BronzeObjectStorageWriter,
};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};

/// `infra/lakehouse/contracts/courtauction-go-kr.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/courtauction-go-kr.contract.json");
const PROVIDER: &str = "courtauction.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_COURT_AUCTION";
const MANIFEST_SCHEMA: &str = "foundation-platform.courtauction_go_kr_collect.v1";
const REFUSED_FILE: &str = "refused.json";
/// The one-run waiver of the quiet hours an operator writes (`by-pnu-pack-operator.sh job
/// start-outside-quiet-hours`), read and removed before the first request.
pub(crate) const QUIET_HOURS_WAIVER_FILE: &str = "attended-outside-quiet-hours.json";
/// A waiver older than this is refused: it must come from the start it was written for.
const QUIET_HOURS_WAIVER_MAX_AGE_SECONDS: i64 = 900;
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const KST_HOURS: i64 = 9;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct QuietHours {
    pub(crate) start: String,
    pub(crate) end: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Manners {
    pub(crate) request_spacing_seconds: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) max_requests_per_run: u32,
    pub(crate) network_retries: u32,
    pub(crate) quiet_hours_kst: QuietHours,
    pub(crate) robots_path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Refusal {
    pub(crate) http_statuses: Vec<u16>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Search {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    pub(crate) page_size: u32,
    pub(crate) conditions: Vec<BTreeMap<String, String>>,
}

/// Read by the test that holds the collector's own source to ADR-0281 §4.
#[cfg(test)]
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Forbidden {
    pub(crate) names: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent_product: String,
    pub(crate) contact_env: String,
    pub(crate) manners: Manners,
    pub(crate) refusal: Refusal,
    #[cfg(test)]
    pub(crate) forbidden_in_collector: Forbidden,
    pub(crate) item_search: Search,
    pub(crate) sale_result_search: Search,
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    serde_json::from_str(CONTRACT_JSON).context("courtauction-go-kr.contract.json is not valid")
}

/// The two searches, by the role the manifest names them with.
pub(crate) fn searches(contract: &SourceContract) -> [(&'static str, &Search); 2] {
    [
        ("item_search", &contract.item_search),
        ("sale_result_search", &contract.sale_result_search),
    ]
}

/// The one User-Agent every request carries: our product, and a contact only when one is set
/// (root ADR-0283). An empty or missing contact does not block the run.
pub(crate) fn user_agent(
    contract: &SourceContract,
    contact: Option<&str>,
) -> anyhow::Result<String> {
    let contact = contact.map(str::trim).filter(|contact| !contact.is_empty());
    let Some(contact) = contact else {
        return Ok(contract.user_agent_product.clone());
    };
    ensure!(
        contact.is_ascii() && !contact.contains(['(', ')', '\r', '\n']),
        "{} must be a plain e-mail address or URL",
        contract.contact_env
    );
    Ok(format!("{} (+{contact})", contract.user_agent_product))
}

fn clock(text: &str) -> anyhow::Result<NaiveTime> {
    NaiveTime::parse_from_str(text, "%H:%M").with_context(|| format!("{text:?} is not HH:MM"))
}

/// Whether `at` falls inside the quiet hours (KST, start inclusive, end exclusive).
pub(crate) fn in_quiet_hours(manners: &Manners, at: DateTime<Utc>) -> anyhow::Result<bool> {
    let local = (at + chrono::Duration::hours(KST_HOURS)).time();
    let (start, end) = (
        clock(&manners.quiet_hours_kst.start)?,
        clock(&manners.quiet_hours_kst.end)?,
    );
    Ok(if start <= end {
        start <= local && local < end
    } else {
        local >= start || local < end
    })
}

/// What robots.txt asks of us: the disallowed path prefixes and a crawl delay, from the groups
/// naming our product token or `*`. No file (404) asks nothing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Robots {
    pub(crate) disallow: Vec<String>,
    pub(crate) crawl_delay_seconds: Option<u64>,
}

pub(crate) fn read_robots(text: &str, product: &str) -> Robots {
    let token = product
        .split('/')
        .next()
        .unwrap_or(product)
        .to_ascii_lowercase();
    let mut robots = Robots::default();
    let mut applies = false;
    let mut in_agents = false;
    for raw in text.lines() {
        let line = raw.split('#').next().unwrap_or("").trim();
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        let (field, value) = (field.trim().to_ascii_lowercase(), value.trim());
        if field == "user-agent" {
            let agent = value.to_ascii_lowercase();
            let names_us = agent == "*" || token.starts_with(&agent) || agent.starts_with(&token);
            applies = if in_agents {
                applies || names_us
            } else {
                names_us
            };
            in_agents = true;
            continue;
        }
        in_agents = false;
        if !applies {
            continue;
        }
        if field == "disallow" && !value.is_empty() {
            robots.disallow.push(value.to_owned());
        } else if field == "crawl-delay" {
            robots.crawl_delay_seconds = value.parse::<f64>().ok().map(|seconds| {
                // shortcut: whole seconds; a fractional delay rounds up, never down.
                seconds.max(0.0).ceil() as u64
            });
        }
    }
    robots
}

impl Robots {
    pub(crate) fn allows(&self, path: &str) -> bool {
        !self
            .disallow
            .iter()
            .any(|prefix| path.starts_with(prefix.as_str()))
    }
}

/// Why a run stopped early, if it did.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum Stop {
    /// The site refused us: the run ends and no later run starts until a person clears it.
    Refused(String),
    /// The night's request bound was reached.
    RequestBound,
    /// The quiet hours ended.
    QuietHoursEnded,
    /// robots.txt disallows the path.
    RobotsDisallow(String),
}

/// One search's lines and whether every condition was read to its last page.
#[derive(Clone, Debug, Default)]
pub(crate) struct SearchResult {
    pub(crate) lines: Vec<String>,
    pub(crate) complete: bool,
    pub(crate) rows: u64,
}

/// Sends the run's requests, honoring every manner of the contract.
pub(crate) struct Collector<'a> {
    contract: &'a SourceContract,
    http: reqwest::Client,
    base_uri: String,
    spacing: ProviderRequestSpacing,
    pub(crate) sent: u32,
    /// Set only by an attended waiver for this one run: the quiet hours are not asked.
    pub(crate) quiet_hours_waived: Option<String>,
}

/// What a response came to.
pub(crate) enum Page {
    Rows { rows: Vec<JsonValue>, total: u64 },
    Refused(String),
}

impl<'a> Collector<'a> {
    pub(crate) fn new(
        contract: &'a SourceContract,
        base_uri: &str,
        user_agent: &str,
        spacing: ProviderRequestSpacing,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            contract,
            http: reqwest::Client::builder()
                .user_agent(user_agent)
                .no_proxy()
                .timeout(Duration::from_secs(
                    contract.manners.request_timeout_seconds,
                ))
                .pool_max_idle_per_host(1)
                .build()
                .context("failed to build the courtauction.go.kr HTTP client")?,
            base_uri: base_uri.trim_end_matches('/').to_owned(),
            spacing,
            sent: 0,
            quiet_hours_waived: None,
        })
    }

    /// GETs robots.txt. A refusal here is a refusal.
    pub(crate) async fn robots(&mut self) -> anyhow::Result<Result<Robots, String>> {
        let path = self.contract.manners.robots_path.clone();
        let response = self.send(|http, url| http.get(url), &path).await?;
        let status = response.status().as_u16();
        if self.contract.refusal.http_statuses.contains(&status) {
            return Ok(Err(format!("robots.txt answered HTTP {status}")));
        }
        if status == 404 || !(200..300).contains(&status) {
            return Ok(Ok(Robots::default()));
        }
        let text = response.text().await.unwrap_or_default();
        Ok(Ok(read_robots(&text, &self.contract.user_agent_product)))
    }

    async fn send(
        &mut self,
        build: impl Fn(&reqwest::Client, String) -> reqwest::RequestBuilder,
        path: &str,
    ) -> anyhow::Result<reqwest::Response> {
        let url = format!("{}{path}", self.base_uri);
        let mut attempt = 0;
        loop {
            self.spacing.wait_before_request(self.sent as usize).await;
            self.sent += 1;
            match build(&self.http, url.clone()).send().await {
                Ok(response) => return Ok(response),
                // Network errors only; never a refusal.
                Err(error) if attempt < self.contract.manners.network_retries => {
                    attempt += 1;
                    tracing::warn!(attempt, error = %error, "courtauction.go.kr request failed; retrying");
                    tokio::time::sleep(Duration::from_secs(5 * 2_u64.pow(attempt))).await;
                }
                Err(error) => return Err(error).with_context(|| format!("POST {path} failed")),
            }
        }
    }

    /// POSTs one page of one condition and reads it.
    pub(crate) async fn page(
        &mut self,
        search: &Search,
        condition: &BTreeMap<String, String>,
        page_no: u64,
        at: DateTime<Utc>,
        lines: &mut Vec<String>,
    ) -> anyhow::Result<Page> {
        let body = json!({
            "dma_pageInfo": {"pageNo": page_no, "pageSize": search.page_size, "totalYn": "Y"},
            "dma_srchGdsDtlSrchInfo": condition,
        });
        let response = self
            .send(
                |http, url| {
                    http.post(url)
                        .header(CONTENT_TYPE, "application/json;charset=UTF-8")
                        .header(ACCEPT, "application/json")
                        .json(&body)
                },
                &search.path,
            )
            .await?;
        let status = response.status().as_u16();
        let retry_after = response.headers().contains_key(RETRY_AFTER);
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        let text = response.text().await.unwrap_or_default();
        lines.push(serde_json::to_string(&json!({
            "operation": search.operation,
            "request": body,
            "requested_at_utc": at.to_rfc3339(),
            "http_status": status,
            "body": text,
        }))?);
        Ok(read_page(
            self.contract,
            status,
            retry_after,
            &content_type,
            &text,
        ))
    }
}

/// Reads one search response: rows and the total, or the refusal it is.
pub(crate) fn read_page(
    contract: &SourceContract,
    status: u16,
    retry_after: bool,
    content_type: &str,
    text: &str,
) -> Page {
    if contract.refusal.http_statuses.contains(&status) {
        return Page::Refused(format!("HTTP {status}"));
    }
    if retry_after {
        return Page::Refused("Retry-After".to_owned());
    }
    if !(200..300).contains(&status) {
        return Page::Refused(format!("HTTP {status}"));
    }
    if !content_type.contains("json") {
        return Page::Refused(format!("a {content_type:?} page where JSON belongs"));
    }
    let Ok(value) = serde_json::from_str::<JsonValue>(text) else {
        return Page::Refused("a body that is not JSON".to_owned());
    };
    let data = &value["data"];
    if data["ipcheck"] == JsonValue::Bool(false) {
        return Page::Refused("ipcheck=false".to_owned());
    }
    let total = match &data["dma_pageInfo"]["totalCnt"] {
        JsonValue::String(text) => text.trim().parse().ok(),
        JsonValue::Number(number) => number.as_u64(),
        _ => None,
    };
    let Some(total) = total else {
        return Page::Refused("a JSON body without data.dma_pageInfo.totalCnt".to_owned());
    };
    let rows = data["dlt_srchResult"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    Page::Rows { rows, total }
}

/// Runs the night: robots, then both searches, stopping at the first refusal, the request bound
/// or the end of the quiet hours. `now` is asked before every request.
pub(crate) async fn collect(
    contract: &SourceContract,
    collector: &mut Collector<'_>,
    now: impl Fn() -> DateTime<Utc>,
) -> anyhow::Result<(BTreeMap<&'static str, SearchResult>, Option<Stop>)> {
    let mut results: BTreeMap<&'static str, SearchResult> = BTreeMap::new();
    let robots = match collector.robots().await? {
        Ok(robots) => robots,
        Err(reason) => return Ok((results, Some(Stop::Refused(reason)))),
    };
    if let Some(delay) = robots.crawl_delay_seconds {
        if u128::from(delay) * 1000 > collector.spacing.interval_millis() {
            collector.spacing = ProviderRequestSpacing::try_new(Duration::from_secs(delay))?;
        }
    }
    for (role, search) in searches(contract) {
        let result = results.entry(role).or_default();
        if !robots.allows(&search.path) {
            return Ok((results, Some(Stop::RobotsDisallow(search.path.clone()))));
        }
        for condition in &search.conditions {
            let mut read = 0_u64;
            for page_no in 1_u64.. {
                if collector.sent >= contract.manners.max_requests_per_run {
                    return Ok((results, Some(Stop::RequestBound)));
                }
                let at = now();
                if collector.quiet_hours_waived.is_none() && !in_quiet_hours(&contract.manners, at)?
                {
                    return Ok((results, Some(Stop::QuietHoursEnded)));
                }
                match collector
                    .page(search, condition, page_no, at, &mut result.lines)
                    .await?
                {
                    Page::Refused(reason) => return Ok((results, Some(Stop::Refused(reason)))),
                    Page::Rows { rows, total } => {
                        read += rows.len() as u64;
                        if rows.is_empty() || read >= total {
                            break;
                        }
                    }
                }
            }
            result.rows += read;
        }
        result.complete = true;
    }
    Ok((results, None))
}

pub(crate) fn gzip_lines(lines: &[String]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        encoder.write_all(line.as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    Ok(encoder.finish()?)
}

/// The run's id and every object's `source_snapshot_id`: when the run started.
pub(crate) fn run_id(at: DateTime<Utc>) -> String {
    at.format("%Y%m%dT%H%M%SZ").to_string()
}

pub(crate) fn plan(
    search: &Search,
    bytes: Vec<u8>,
    run: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let file_id = format!(
        "{}-{}",
        search.dataset_slug.replace('_', "-"),
        run_id(at).to_ascii_lowercase()
    );
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, &search.dataset_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run,
        identity: PublicDataBulkFileIdentity {
            operation: search.operation.clone(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: file_id.clone(),
            provider_file_name: format!("{file_id}.gz"),
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: "application/gzip".to_owned(),
    })
    .with_context(|| {
        format!(
            "failed to plan the Bronze object for {}",
            search.dataset_slug
        )
    })
}

fn write_local_copy(output_dir: &Path, plan: &PublicDataBulkFilePlan) -> anyhow::Result<String> {
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?;
    std::fs::create_dir_all(output_dir.join("objects"))?;
    let relative = format!("objects/{name}");
    std::fs::write(output_dir.join(&relative), &plan.raw_payload)?;
    Ok(relative)
}

/// Commits one search's object and returns its manifest entry.
pub(crate) async fn persist<Uow, Storage>(
    search: &Search,
    output_dir: &Path,
    lines: &[String],
    at: DateTime<Utc>,
    live: Option<(&Uow, &Storage)>,
) -> anyhow::Result<JsonValue>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let bytes = gzip_lines(lines)?;
    let Some((uow, storage)) = live else {
        let plan = plan(search, bytes, IngestionRunId::new(Uuid::new_v4()), at)?;
        let local = write_local_copy(output_dir, &plan)?;
        return Ok(
            json!({"object_key": plan.object_key.as_str(), "checksum_sha256": plan.checksum_sha256, "local_path": local, "written": false}),
        );
    };
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(search, at)?)
        .await
        .with_context(|| format!("failed to upsert the {} source", search.dataset_slug))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, search, at))
        .await
        .with_context(|| format!("failed to create the {} run", search.dataset_slug))?;
    let plan = plan(search, bytes, run.id, at)?;
    let outcome = BRONZE_COMMITTER
        .commit(
            &writer,
            uow,
            PlannedBronzeObject {
                object_key: plan.object_key.as_str().to_owned(),
                payload: BronzePayload::InMemory(plan.raw_payload.clone()),
                content_type: plan.content_type.clone(),
                cache_control: BRONZE_CACHE_CONTROL.to_owned(),
                checksum_sha256: plan.checksum_sha256.clone(),
                record: BronzeObject {
                    id: BronzeObjectId::new(Uuid::new_v4()),
                    source_catalog_id: source.id,
                    ingestion_run_id: run.id,
                    source_record_id: None,
                    source_partition_key: Some(plan.source_partition_key.clone()),
                    source_identity_key: plan.source_identity_key.clone(),
                    dedupe_key: plan.dedupe_key.clone(),
                    request_params: plan.request_params.clone(),
                    object_key: plan.object_key.clone(),
                    checksum_sha256: plan.checksum_sha256.clone(),
                    content_type: plan.content_type.clone(),
                    size_bytes: plan.size_bytes,
                    logical_record_count: None,
                    collected_at: at,
                    snapshot_period: plan.snapshot_period.clone(),
                    snapshot_date: plan.snapshot_date,
                    snapshot_granularity: plan.snapshot_granularity,
                    snapshot_basis: plan.snapshot_basis,
                    provider_file_id: None,
                    provider_file_name: None,
                    provider_updated_at: None,
                    effective_date: None,
                    created_at: at,
                },
            },
        )
        .await
        .with_context(|| format!("failed to commit {}", plan.object_key.as_str()))?;
    ensure!(
        outcome.checksum_sha256 == plan.checksum_sha256,
        "Bronze holds different bytes at {}",
        plan.object_key.as_str()
    );
    let local = write_local_copy(output_dir, &plan)?;
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: u64::try_from(lines.len()).unwrap_or(u64::MAX),
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {} run", search.dataset_slug))?;
    Ok(
        json!({"object_key": plan.object_key.as_str(), "checksum_sha256": plan.checksum_sha256, "local_path": local, "written": true}),
    )
}

fn source_catalog_entry(search: &Search, now: DateTime<Utc>) -> anyhow::Result<SourceCatalogEntry> {
    let contract = source_contract()?;
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &search.dataset_slug)?,
        name: search.dataset_slug.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: search.dataset_slug.clone(),
        base_url: Some(contract.base_url),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some("nightly, quiet hours only".to_owned()),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    search: &Search,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": search.operation}),
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

struct Config {
    base_uri: String,
    user_agent: String,
    spacing: ProviderRequestSpacing,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        let floor = Duration::from_secs(contract.manners.request_spacing_seconds);
        let configured = optional_env_value(&format!("{PREFIX}_REQUEST_SPACING_MS"))?
            .map(|value| value.trim().parse::<u64>())
            .transpose()
            .with_context(|| format!("{PREFIX}_REQUEST_SPACING_MS must be a whole number"))?
            .map_or(floor, Duration::from_millis);
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone()),
            user_agent: user_agent(
                contract,
                optional_env_value(&contract.contact_env)?.as_deref(),
            )?,
            // A setting may make the run slower, never faster than the contract.
            spacing: ProviderRequestSpacing::try_new(configured.max(floor))?,
            live_write: matches!(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("1" | "true" | "yes")
            ),
            output_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?),
            state_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_STATE_DIR"))?),
        })
    }
}

/// Takes the attended waiver of the quiet hours, if an operator wrote one for this start.
///
/// The file is removed before anything else, so it serves one run at most. A waiver without a
/// reason or written more than 15 minutes ago is refused: the run stops without a request.
pub(crate) fn take_quiet_hours_waiver(
    state_dir: &Path,
    now: DateTime<Utc>,
) -> anyhow::Result<Option<String>> {
    let path = state_dir.join(QUIET_HOURS_WAIVER_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()))
        }
    };
    std::fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))?;
    let value: JsonValue = serde_json::from_slice(&bytes)
        .with_context(|| format!("{} is not JSON", path.display()))?;
    let reason = value["reason"].as_str().map(str::trim).unwrap_or_default();
    ensure!(!reason.is_empty(), "the quiet-hours waiver names no reason");
    let requested_at = value["requested_at"]
        .as_i64()
        .context("the quiet-hours waiver has no requested_at epoch second")?;
    let age = now.timestamp() - requested_at;
    ensure!(
        (-60..=QUIET_HOURS_WAIVER_MAX_AGE_SECONDS).contains(&age),
        "the quiet-hours waiver was written {age} seconds ago; it serves only the start it was written for"
    );
    Ok(Some(format!(
        "{reason} (who={}, requested_at={requested_at})",
        value["who"].as_str().unwrap_or("unknown")
    )))
}

/// Refuses to start while a refusal is on record.
pub(crate) fn refusal_on_record(state_dir: &Path) -> anyhow::Result<Option<String>> {
    let path = state_dir.join(REFUSED_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let value: JsonValue =
        serde_json::from_slice(&std::fs::read(&path)?).unwrap_or(JsonValue::Null);
    Ok(Some(
        value["reason"].as_str().unwrap_or("unreadable").to_owned(),
    ))
}

/// Collects one night into Bronze.
///
/// # Errors
/// Returns an error when the configuration is invalid, a refusal is on record or happens now, a
/// Bronze commit fails, or the manifest cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    std::fs::create_dir_all(&config.output_dir)?;
    std::fs::create_dir_all(&config.state_dir)?;
    if let Some(reason) = refusal_on_record(&config.state_dir)? {
        bail!(
            "courtauction.go.kr refused an earlier run ({reason}); no run starts until a person checks it and removes {}",
            config.state_dir.join(REFUSED_FILE).display()
        );
    }
    let started = Utc::now();
    let waiver = take_quiet_hours_waiver(&config.state_dir, started)?;
    if let Some(reason) = &waiver {
        tracing::warn!(%reason, "courtauction.go.kr: quiet hours waived for this one attended run");
    }
    if waiver.is_none() && !in_quiet_hours(&contract.manners, started)? {
        println!("courtauction.go.kr: outside the quiet hours; nothing asked");
        println!("foundation-job-outcome unchanged");
        return Ok(());
    }
    let mut collector = Collector::new(
        &contract,
        &config.base_uri,
        &config.user_agent,
        config.spacing,
    )?;
    collector.quiet_hours_waived.clone_from(&waiver);
    let (results, stop) = collect(&contract, &mut collector, Utc::now).await?;

    let mut objects = Vec::new();
    let uow;
    let storage;
    let live = if config.live_write {
        live_write_target_preflight().context("court auction live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?).await?;
        uow = PgBronzeIngestUnitOfWork::new(pool);
        storage = live_write_bronze_object_storage_from_env().await?;
        Some((&uow, storage.as_ref()))
    } else {
        None
    };
    for (role, search) in searches(&contract) {
        let Some(result) = results.get(role).filter(|result| !result.lines.is_empty()) else {
            continue;
        };
        let mut entry = persist(search, &config.output_dir, &result.lines, started, live).await?;
        entry["role"] = json!(role);
        entry["complete"] = json!(result.complete);
        entry["rows"] = json!(result.rows);
        objects.push(entry);
    }
    write_json_file(
        &config.output_dir.join("manifest.json"),
        &json!({
            "schema_version": MANIFEST_SCHEMA,
            "source_snapshot_id": run_id(started),
            "collected_at_utc": started.to_rfc3339(),
            "requests": collector.sent,
            "quiet_hours_waived": waiver,
            "stopped": stop,
            "objects": objects,
        }),
    )?;
    if let Some(Stop::Refused(reason)) = &stop {
        write_json_file(
            &config.state_dir.join(REFUSED_FILE),
            &json!({"reason": reason, "at_utc": Utc::now().to_rfc3339(), "requests": collector.sent}),
        )?;
        bail!("courtauction.go.kr refused the run ({reason}); stopped after {} requests and recorded it", collector.sent);
    }
    tracing::info!(
        requests = collector.sent,
        ?stop,
        "courtauction.go.kr collection ended"
    );
    println!(
        "foundation-job-outcome {}",
        if objects
            .iter()
            .any(|o| o["written"] == JsonValue::Bool(true))
        {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
