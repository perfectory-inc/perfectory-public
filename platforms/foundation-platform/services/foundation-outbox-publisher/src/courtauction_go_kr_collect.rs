//! 법원경매정보 (courtauction.go.kr) list searches, Bronze collection (root ADR-0280, ADR-0289).
//!
//! The collector asks as a browser and does not give up (root ADR-0289); every value it uses is
//! the contract's (`courtauction-go-kr.contract.json`), and this module only carries them out:
//!
//! - a pool of browser sessions (`collection_infrastructure::BrowserSessionPool`, no proxy) and
//!   each endpoint group's adaptive width (`collection.pace`);
//! - a block (`collection.block_signals`: the site's `ipcheck=false`, a redirect, HTML where JSON
//!   belongs) is not a refusal: `collection_application::ask` waits the contract's next step,
//!   renews the blocked session and asks again, with no bound on how many times. A failure that is
//!   not a block is asked again the contract's number of times, then its page is recorded as
//!   failed in the manifest and the pass is incomplete; nothing is dropped silently;
//! - no hours of its own: an operator may narrow the hours for a bounded time (root ADR-0282,
//!   `switches/<job>.window`, read before every page);
//! - the run's time budget (`collection.session.run_time_budget_seconds`) is the only thing that
//!   ends the asking early; the pages it left are asked again by the next run. A run that got no
//!   answer at all fails, which is what the readiness report and the unit's failure alert read.
//!
//! Each search's answers of one run land as one gzip JSON Lines Bronze object, one line per page:
//! the request body, the HTTP status and the response body as received. The Silver load
//! (`infra/lakehouse/spark/jobs/courtauction_go_kr.py`) parses them.

use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    ask, plan_public_data_bulk_file, AskCounts, AskRules, AskTally, Asked, BronzeCommitter,
    BronzePayload, CourtAuctionSessions, PaceGate, PlannedBronzeObject, PublicDataBulkFileIdentity,
    PublicDataBulkFilePlan, PublicDataBulkFilePlanInput, SessionRecord, SiteMethod, SiteRequest,
};
use collection_domain::court_auction::{BlockSignals, Pace, PaceRule};
use collection_domain::{
    source_slug, BronzeObject, IngestionRun, IngestionRunStatus, IngestionTrigger, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::{
    BrowserSessionConfig, BrowserSessionPool, PgBronzeIngestUnitOfWork,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tokio::time::Instant;
use uuid::Uuid;

use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
    BronzeObjectStorageWriter,
};
use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};

/// `infra/lakehouse/contracts/courtauction-go-kr.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/courtauction-go-kr.contract.json");
const PROVIDER: &str = "courtauction.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_COURT_AUCTION";
const MANIFEST_SCHEMA: &str = "foundation-platform.courtauction_go_kr_collect.v2";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const KST_HOURS: i64 = 9;
/// This collection's job in orchestration/jobs.v1.json: its window override is `<switches>/<id>.window`.
const JOB_ID: &str = "court_auction_collect";
/// Where foundation-api writes the operators' switches (root ADR-0282 §3).
const SWITCHES_DIR_ENV: &str = "FOUNDATION_JOB_SWITCHES_DIR";
const SWITCHES_DIR: &str = "/var/lib/foundation-jobs/switches";

/// Hours an operator narrowed the asking to (KST, root ADR-0282).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Window {
    pub(crate) start: String,
    pub(crate) end: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Search {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    pub(crate) page_size: u32,
    /// The `collection.pace` group this search's requests are paced in.
    pub(crate) pace_group: String,
    /// The headers the site's own screen sends to this endpoint.
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) conditions: Vec<BTreeMap<String, String>>,
    /// The bid-date range every request of the search carries (root ADR-0290 §1-§2).
    #[serde(default)]
    pub(crate) date_window: Option<DateWindow>,
}

/// A bid-date range fixed for a whole run: `days_before` to `days_after` around its KST start date.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DateWindow {
    pub(crate) start_field: String,
    pub(crate) end_field: String,
    pub(crate) days_before: i64,
    pub(crate) days_after: i64,
}

/// The condition a request asks: the contract's condition with the search's date range, if it has
/// one, around `day` (the run's KST start date, so every page of the run asks the same range).
pub(crate) fn asked_condition(
    search: &Search,
    condition: &BTreeMap<String, String>,
    day: NaiveDate,
) -> BTreeMap<String, String> {
    let mut asked = condition.clone();
    if let Some(window) = &search.date_window {
        let ymd = |days: i64| {
            (day + chrono::Duration::days(days))
                .format("%Y%m%d")
                .to_string()
        };
        asked.insert(window.start_field.clone(), ymd(-window.days_before));
        asked.insert(window.end_field.clone(), ymd(window.days_after));
    }
    asked
}

/// How sessions are opened, renewed and given time (`collection.session`, root ADR-0289 §2-§3).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SessionContract {
    pub(crate) browser_profile: String,
    pub(crate) pool_size: usize,
    pub(crate) opening_path: String,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) block_waits_seconds: Vec<u64>,
    pub(crate) failure_retries: u32,
    pub(crate) failure_wait_seconds: u64,
    pub(crate) run_time_budget_seconds: u64,
    pub(crate) headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Collection {
    pub(crate) block_signals: BlockSignals,
    pub(crate) pace: BTreeMap<String, PaceRule>,
    pub(crate) session: SessionContract,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) item_search: Search,
    pub(crate) sale_result_search: Search,
    pub(crate) collection: Collection,
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

/// The contract's waits and retries, as the asking reads them.
pub(crate) fn ask_rules(contract: &SourceContract) -> AskRules {
    let session = &contract.collection.session;
    AskRules {
        signals: contract.collection.block_signals.clone(),
        block_waits: session
            .block_waits_seconds
            .iter()
            .copied()
            .map(Duration::from_secs)
            .collect(),
        failure_retries: session.failure_retries,
        failure_wait: Duration::from_secs(session.failure_wait_seconds),
    }
}

/// The pool the contract describes, against `base_url` (the contract's, or a test server).
pub(crate) fn session_pool(
    contract: &SourceContract,
    base_url: &str,
) -> anyhow::Result<BrowserSessionPool> {
    let session = &contract.collection.session;
    BrowserSessionPool::new(BrowserSessionConfig {
        base_url: base_url.to_owned(),
        browser_profile: session.browser_profile.clone(),
        pool_size: session.pool_size,
        opening_path: session.opening_path.clone(),
        request_timeout: Duration::from_secs(session.request_timeout_seconds),
    })
    .map_err(|error| anyhow::anyhow!("courtauction.go.kr session pool: {error}"))
}

fn clock(text: &str) -> anyhow::Result<NaiveTime> {
    NaiveTime::parse_from_str(text, "%H:%M").with_context(|| format!("{text:?} is not HH:MM"))
}

/// Whether `at` falls inside `window` (KST, start inclusive, end exclusive; across midnight when the
/// end comes first; the whole day when the two are equal).
pub(crate) fn in_window(window: &Window, at: DateTime<Utc>) -> anyhow::Result<bool> {
    let local = (at + chrono::Duration::hours(KST_HOURS)).time();
    let (start, end) = (clock(&window.start)?, clock(&window.end)?);
    Ok(if start == end {
        true
    } else if start < end {
        start <= local && local < end
    } else {
        local >= start || local < end
    })
}

/// The hours an operator narrowed this job to at `at` (`<switches>/<job>.window`, root ADR-0282),
/// while its `until` lies ahead, with who set it and why. A switch that cannot be read is not one:
/// the job asks at any hour.
pub(crate) fn operator_window(switches: &Path, at: DateTime<Utc>) -> Option<(Window, String)> {
    let text = std::fs::read_to_string(switches.join(format!("{JOB_ID}.window"))).ok()?;
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix('='))
            .map(str::to_owned)
    };
    let (start, end, until) = (field("start")?, field("end")?, field("until")?);
    let ahead = DateTime::parse_from_rfc3339(&until).is_ok_and(|until| until > at);
    // HH:MM exactly, as the ledger keeps it: chrono's %H alone also reads `9:00`.
    let strict = |text: &str| text.len() == 5 && clock(text).is_ok();
    (ahead && strict(&start) && strict(&end)).then(|| {
        let who = field("actor").unwrap_or_default();
        let why = field("reason").unwrap_or_default();
        (
            Window { start, end },
            format!("until {until}, set by {who}: {why}"),
        )
    })
}

/// Whether the job may ask at `at`: always, unless an operator's window is set and `at` is outside it.
pub(crate) fn may_ask(switches: &Path, at: DateTime<Utc>) -> anyhow::Result<bool> {
    operator_window(switches, at).map_or(Ok(true), |(window, _)| in_window(&window, at))
}

fn switches_dir() -> PathBuf {
    std::env::var_os(SWITCHES_DIR_ENV).map_or_else(|| PathBuf::from(SWITCHES_DIR), PathBuf::from)
}

/// Why a run ended before every page was read, if it did.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) enum Stop {
    /// The run's time budget ran out; the next run asks again.
    TimeBudget,
    /// An operator's window ended (root ADR-0282).
    WindowEnded,
}

/// A page that failed past its retries, kept so nothing is dropped silently (root ADR-0289 §3).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct FailedPage {
    pub(crate) condition: BTreeMap<String, String>,
    pub(crate) page_no: u64,
    pub(crate) reason: String,
}

/// One search's lines and whether every condition was read to its last page.
#[derive(Clone, Debug, Default)]
pub(crate) struct SearchResult {
    pub(crate) lines: Vec<String>,
    pub(crate) complete: bool,
    pub(crate) rows: u64,
    pub(crate) failed: Vec<FailedPage>,
}

/// What one run's asking needs: the pool, one gate per pace group, the rules and the deadline.
pub(crate) struct Asking<'a> {
    pub(crate) sessions: &'a dyn CourtAuctionSessions,
    pub(crate) gates: BTreeMap<String, PaceGate>,
    pub(crate) rules: AskRules,
    pub(crate) tally: AskTally,
    pub(crate) deadline: Instant,
    pub(crate) switches: PathBuf,
}

impl<'a> Asking<'a> {
    pub(crate) fn new(
        contract: &SourceContract,
        sessions: &'a dyn CourtAuctionSessions,
        deadline: Instant,
        switches: PathBuf,
    ) -> Self {
        Self {
            sessions,
            gates: contract
                .collection
                .pace
                .iter()
                .map(|(group, rule)| (group.clone(), PaceGate::new(Pace::new(rule.clone()))))
                .collect(),
            rules: ask_rules(contract),
            tally: AskTally::default(),
            deadline,
            switches,
        }
    }
}

/// What one page came to.
enum Paged {
    Rows {
        line: String,
        rows: u64,
        total: u64,
    },
    Failed {
        line: Option<String>,
        reason: String,
    },
    Stopped(Stop),
}

/// Asks one page of one condition. The page's line holds the request and the answer as received.
async fn page(
    asking: &Asking<'_>,
    contract: &SourceContract,
    search: &Search,
    condition: &BTreeMap<String, String>,
    page_no: u64,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<Paged> {
    let at = now();
    if !may_ask(&asking.switches, at)? {
        return Ok(Paged::Stopped(Stop::WindowEnded));
    }
    let gate = asking
        .gates
        .get(&search.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", search.pace_group))?;
    let body = json!({
        "dma_pageInfo": {"pageNo": page_no, "pageSize": search.page_size, "totalYn": "Y"},
        "dma_srchGdsDtlSrchInfo": condition,
    });
    let request = SiteRequest {
        method: SiteMethod::Post,
        path: search.path.clone(),
        headers: contract
            .collection
            .session
            .headers
            .iter()
            .chain(&search.headers)
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        body: Some(body.clone()),
    };
    let line = |status: u16, text: &str| {
        serde_json::to_string(&json!({
            "operation": search.operation,
            "request": body,
            "requested_at_utc": at.to_rfc3339(),
            "http_status": status,
            "body": text,
        }))
    };
    Ok(
        match ask(
            asking.sessions,
            gate,
            &asking.rules,
            &asking.tally,
            &request,
            asking.deadline,
        )
        .await
        {
            Asked::Data { data, reply } => {
                let total = match &data["dma_pageInfo"]["totalCnt"] {
                    JsonValue::String(text) => text.trim().parse().ok(),
                    JsonValue::Number(number) => number.as_u64(),
                    _ => None,
                };
                let line = line(reply.status, &reply.body)?;
                match total {
                    Some(total) => Paged::Rows {
                        line,
                        rows: data["dlt_srchResult"].as_array().map_or(0, Vec::len) as u64,
                        total,
                    },
                    None => Paged::Failed {
                        line: Some(line),
                        reason: "an answer without data.dma_pageInfo.totalCnt".to_owned(),
                    },
                }
            }
            Asked::Failed { reason, reply } => Paged::Failed {
                line: reply
                    .map(|reply| line(reply.status, &reply.body))
                    .transpose()?,
                reason,
            },
            Asked::Unfinished => Paged::Stopped(Stop::TimeBudget),
        },
    )
}

/// Runs both searches: for each condition its first page, which tells how many there are, then
/// every other page at once, as many in flight as the search's pace group allows. `now` is asked
/// before every page.
pub(crate) async fn collect(
    contract: &SourceContract,
    asking: &Asking<'_>,
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(BTreeMap<&'static str, SearchResult>, Option<Stop>)> {
    let mut results: BTreeMap<&'static str, SearchResult> = BTreeMap::new();
    let mut stop = None;
    // The run's KST date, taken from its start, fixes the date range of every page.
    let day = (now() + chrono::Duration::hours(KST_HOURS)).date_naive();
    for (role, search) in searches(contract) {
        let result = results.entry(role).or_default();
        let mut complete = true;
        for condition in &search.conditions {
            let asked = asked_condition(search, condition, day);
            let failed = |page_no: u64, reason: String| FailedPage {
                condition: asked.clone(),
                page_no,
                reason,
            };
            let (first_rows, total) = match page(asking, contract, search, &asked, 1, &now).await? {
                Paged::Rows { line, rows, total } => {
                    result.lines.push(line);
                    (rows, total)
                }
                Paged::Failed { line, reason } => {
                    result.lines.extend(line);
                    result.failed.push(failed(1, reason));
                    complete = false;
                    continue;
                }
                Paged::Stopped(why) => {
                    stop.get_or_insert(why);
                    complete = false;
                    break;
                }
            };
            result.rows += first_rows;
            if first_rows == 0 || first_rows >= total {
                continue;
            }
            let pages = total.div_ceil(u64::from(search.page_size.max(1)));
            let rest = join_all(
                (2..=pages).map(|page_no| page(asking, contract, search, &asked, page_no, &now)),
            )
            .await;
            for (page_no, paged) in (2..=pages).zip(rest) {
                match paged? {
                    Paged::Rows { line, rows, .. } => {
                        result.lines.push(line);
                        result.rows += rows;
                    }
                    Paged::Failed { line, reason } => {
                        result.lines.extend(line);
                        result.failed.push(failed(page_no, reason));
                        complete = false;
                    }
                    Paged::Stopped(why) => {
                        stop.get_or_insert(why);
                        complete = false;
                    }
                }
            }
            if stop.is_some() {
                break;
            }
        }
        result.complete = complete;
        if stop.is_some() {
            break;
        }
    }
    Ok((results, stop))
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

/// What the whole run did, recorded with every object it landed: requests, blocks and renewals
/// (root ADR-0289 §2-§3) and every session the pool opened, so nobody estimates them from a clock.
pub(crate) fn night_summary(
    counts: AskCounts,
    sessions: &[SessionRecord],
    stop: Option<&Stop>,
) -> JsonValue {
    json!({
        "requests_sent": counts.requests,
        "asking": counts,
        "sessions": sessions,
        "stopped": stop,
    })
}

/// Commits one search's object, with the night's summary in its ledger row and its run, and
/// returns its manifest entry.
pub(crate) async fn persist<Uow, Storage>(
    search: &Search,
    output_dir: &Path,
    lines: &[String],
    at: DateTime<Utc>,
    night: &JsonValue,
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
        .create_ingestion_run(&ingestion_run(source.id, search, night, at))
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
                    request_params: with_night(&plan.request_params, lines.len(), night),
                    object_key: plan.object_key.clone(),
                    checksum_sha256: plan.checksum_sha256.clone(),
                    content_type: plan.content_type.clone(),
                    size_bytes: plan.size_bytes,
                    logical_record_count: u64::try_from(lines.len()).ok(),
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
        collection_frequency: Some(
            "on the court_auction_collect schedule (orchestration/jobs.v1.json)".to_owned(),
        ),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

/// The object's request parameters with the requests it holds and the night's summary.
pub(crate) fn with_night(
    params: &JsonValue,
    requests_in_object: usize,
    night: &JsonValue,
) -> JsonValue {
    let mut params = params.clone();
    if let Some(map) = params.as_object_mut() {
        map.insert("requests_in_object".to_owned(), json!(requests_in_object));
        map.insert("night".to_owned(), night.clone());
    }
    params
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    search: &Search,
    night: &JsonValue,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": search.operation, "night": night}),
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
    live_write: bool,
    output_dir: PathBuf,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone()),
            live_write: matches!(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("1" | "true" | "yes")
            ),
            output_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?),
        })
    }
}

/// Collects one run into Bronze.
///
/// # Errors
/// Returns an error when the configuration is invalid, a Bronze commit fails, the manifest cannot
/// be written, or the run got no answer at all (no progress: the unit's failure is the alert).
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    std::fs::create_dir_all(&config.output_dir)?;
    let started = Utc::now();
    let switches = switches_dir();
    match operator_window(&switches, started) {
        Some((window, by)) => println!(
            "courtauction.go.kr: the operator's window {}-{} KST ({by})",
            window.start, window.end
        ),
        None => println!("courtauction.go.kr: no operator window; asking at any hour"),
    }
    if !may_ask(&switches, started)? {
        println!("courtauction.go.kr: outside the operator's window; nothing asked");
        println!("foundation-job-outcome unchanged");
        return Ok(());
    }
    let pool = session_pool(&contract, &config.base_uri)?;
    let deadline =
        Instant::now() + Duration::from_secs(contract.collection.session.run_time_budget_seconds);
    let asking = Asking::new(&contract, &pool, deadline, switches);
    let (results, stop) = collect(&contract, &asking, Utc::now).await?;
    let counts = asking.tally.counts();
    let sessions = pool.sessions();
    let night = night_summary(counts, &sessions, stop.as_ref());

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
        let mut entry = persist(
            search,
            &config.output_dir,
            &result.lines,
            started,
            &night,
            live,
        )
        .await?;
        entry["role"] = json!(role);
        entry["complete"] = json!(result.complete);
        entry["rows"] = json!(result.rows);
        entry["failed_pages"] = json!(result.failed);
        objects.push(entry);
    }
    write_json_file(
        &config.output_dir.join("manifest.json"),
        &json!({
            "schema_version": MANIFEST_SCHEMA,
            "source_snapshot_id": run_id(started),
            "collected_at_utc": started.to_rfc3339(),
            "night": night,
            "objects": objects,
        }),
    )?;
    tracing::info!(
        ?counts,
        sessions = sessions.len(),
        ?stop,
        "courtauction.go.kr collection ended"
    );
    println!("courtauction.go.kr: {night}");
    if counts.answered == 0 {
        bail!(
            "courtauction.go.kr answered nothing in this run ({} requests, {} blocks, {} renewals): no progress",
            counts.requests,
            counts.blocks,
            counts.renewals
        );
    }
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
