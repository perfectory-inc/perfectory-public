//! 국토교통부 실거래가 (data.go.kr RTMS OpenAPI), Bronze collection (root ADR-0348).
//!
//! The unit of collection is one (dataset, 시군구 `LAWD_CD`, 계약 달 `DEAL_YMD`) bucket, read to
//! `totalCount` page by page; a bucket lands only whole. Each run plans every bucket it owes from
//! its state and the contract (`plan_work`), in the order root ADR-0348 §3–§4 sets: the recent
//! months (age 0–2) first, then the final ask of a code that left the 시군구 list, then the older
//! refresh tiers, then the history never asked (newest month first, the industrial datasets first),
//! and asks them one request at a time until the run's call bound (ADR-0300 H). Every
//! `commit_every_calls` calls, at a bucket boundary, the buckets read so far land as one gzip JSON
//! Lines Bronze object per dataset (one line per request: its parameters without the key, its HTTP
//! status and the body as received) and the state is saved: a stopped run asks only its last bucket
//! again.
//!
//! The 시군구 list is `lawd-set.json` in the state directory, written by the Silver load from
//! `reference.legal_dong_code_snapshot` (`real_transaction_lawd_set.py`). A code new to the list is
//! owed every month already completed; a code that left it is asked once more for every month (the
//! answer is empty, and the Silver load sets the bucket's rows down; root ADR-0349).
//!
//! HTTP 429 (the gateway's per-second or daily limit) ends the run where it is: the next run goes
//! on. Any other answer that is not rows fails the run: a key or parameter problem must never read
//! as "no transactions".

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
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

/// `infra/lakehouse/contracts/data-go-kr-rtms.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/data-go-kr-rtms.contract.json");
const PROVIDER: &str = "data.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_RTMS";
const SERVICE_KEY_ENV: &str = "DATA_GO_KR_SERVICE_KEY";
const MANIFEST_SCHEMA: &str = "foundation-platform.data_go_kr_rtms_collect.v1";
const STATE_SCHEMA: &str = "foundation-platform.data_go_kr_rtms_state.v1";
const LAWD_SET_SCHEMA: &str = "foundation-platform.rtms_lawd_set.v1";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const CONTENT_TYPE: &str = "application/gzip";
const KST_HOURS: i64 = 9;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Dataset {
    pub(crate) operation: String,
    pub(crate) path: String,
    /// The first 계약 달 the provider holds (`YYYY-MM`, measured; root ADR-0348 Context).
    pub(crate) first_month: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Tier {
    /// The oldest month age (0 = this KST month) the tier covers.
    pub(crate) max_age_months: u32,
    /// How many days a completed month waits before it is asked again.
    pub(crate) every_days: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ResultCodes {
    pub(crate) ok: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) request_spacing_ms: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) format_param: BTreeMap<String, String>,
    pub(crate) result_codes: ResultCodes,
    pub(crate) page_size: u32,
    pub(crate) per_run_call_limit: u32,
    pub(crate) run_time_budget_seconds: u64,
    pub(crate) commit_every_calls: u32,
    pub(crate) daily_call_limit_per_dataset: u32,
    pub(crate) lawd_set_max_age_days: i64,
    pub(crate) refresh_tiers: Vec<Tier>,
    /// The datasets asked first within each stage (root ADR-0348 §4), in this order.
    pub(crate) priority: Vec<String>,
    pub(crate) datasets: BTreeMap<String, Dataset>,
}

impl SourceContract {
    fn dataset(&self, name: &str) -> anyhow::Result<&Dataset> {
        self.datasets
            .get(name)
            .with_context(|| format!("the contract names no dataset {name}"))
    }

    fn priority_of(&self, name: &str) -> usize {
        self.priority
            .iter()
            .position(|p| p == name)
            .unwrap_or(self.priority.len())
    }
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    let contract: SourceContract = serde_json::from_str(CONTRACT_JSON)
        .context("data-go-kr-rtms.contract.json is not valid")?;
    for (name, dataset) in &contract.datasets {
        month_index(&dataset.first_month)
            .with_context(|| format!("{name}: first_month {:?}", dataset.first_month))?;
    }
    ensure!(
        contract
            .refresh_tiers
            .windows(2)
            .all(|w| w[0].max_age_months < w[1].max_age_months),
        "refresh_tiers must grow in max_age_months"
    );
    Ok(contract)
}

/// A calendar month as `year * 12 + (month - 1)`.
pub(crate) type Month = i32;

pub(crate) fn month_index(text: &str) -> anyhow::Result<Month> {
    let digits: String = text.chars().filter(char::is_ascii_digit).collect();
    ensure!(digits.len() == 6, "not a YYYY-MM or YYYYMM month: {text:?}");
    let year: i32 = digits[..4].parse()?;
    let month: i32 = digits[4..].parse()?;
    ensure!((1..=12).contains(&month), "not a month: {text:?}");
    Ok(year * 12 + month - 1)
}

pub(crate) fn yyyymm(month: Month) -> String {
    format!("{:04}{:02}", month.div_euclid(12), month.rem_euclid(12) + 1)
}

/// The KST calendar date of an instant (Korea has no daylight saving time).
pub(crate) fn kst_date(at: DateTime<Utc>) -> NaiveDate {
    (at + chrono::Duration::hours(KST_HOURS)).date_naive()
}

fn kst_month(at: DateTime<Utc>) -> Month {
    let date = kst_date(at);
    date.year() * 12 + i32::try_from(date.month0()).unwrap_or(0)
}

/// Calls spent per dataset on one KST day.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Budget {
    pub(crate) kst_date: Option<NaiveDate>,
    pub(crate) calls: BTreeMap<String, u32>,
}

/// One (dataset, month): when every 시군구 was last read whole, and the 시군구 the pass under way
/// has read.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct MonthMark {
    pub(crate) completed_utc: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) done: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct State {
    #[serde(default)]
    pub(crate) budget: Budget,
    /// `"<dataset>/<YYYYMM>"` → its mark.
    #[serde(default)]
    pub(crate) months: BTreeMap<String, MonthMark>,
    /// Every 시군구 code the state has planned for (the list, plus codes still retiring).
    #[serde(default)]
    pub(crate) known_lawds: BTreeSet<String>,
    /// A code new to the list → the `"<dataset>/<YYYYMM>"` completed before it came, still owed.
    #[serde(default)]
    pub(crate) entering: BTreeMap<String, BTreeSet<String>>,
    /// A code that left the list → the `"<dataset>/<YYYYMM>"` still to be asked once more.
    #[serde(default)]
    pub(crate) retiring: BTreeMap<String, BTreeSet<String>>,
}

pub(crate) fn month_key(dataset: &str, month: Month) -> String {
    format!("{dataset}/{}", yyyymm(month))
}

fn split_month_key(key: &str) -> anyhow::Result<(&str, Month)> {
    let (dataset, month) = key
        .split_once('/')
        .with_context(|| format!("not a dataset/month key: {key:?}"))?;
    Ok((dataset, month_index(month)?))
}

impl State {
    fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw: JsonValue = serde_json::from_slice(
            &std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
        )
        .with_context(|| format!("{} is not JSON", path.display()))?;
        ensure!(
            raw["schema_version"] == STATE_SCHEMA,
            "{} is not a {STATE_SCHEMA} file",
            path.display()
        );
        serde_json::from_value(raw["state"].clone())
            .with_context(|| format!("{} does not hold a state", path.display()))
    }

    fn save(&self, path: &Path) -> anyhow::Result<()> {
        let tmp = path.with_extension("json.tmp");
        write_json_file(
            &tmp,
            &json!({"schema_version": STATE_SCHEMA, "state": self}),
        )?;
        std::fs::rename(&tmp, path).with_context(|| format!("failed to install {}", path.display()))
    }

    pub(crate) fn roll_budget(&mut self, now: DateTime<Utc>) {
        let today = kst_date(now);
        if self.budget.kst_date != Some(today) {
            self.budget = Budget {
                kst_date: Some(today),
                ..Budget::default()
            };
        }
    }

    /// Takes in a new 시군구 list: a new code is owed every completed month, a code that left is
    /// owed one more ask of every month the state completed or began.
    pub(crate) fn absorb_lawd_set(&mut self, lawds: &BTreeSet<String>) {
        let completed: BTreeSet<String> = self
            .months
            .iter()
            .filter(|(_, mark)| mark.completed_utc.is_some())
            .map(|(key, _)| key.clone())
            .collect();
        for lawd in lawds.difference(&self.known_lawds.clone()) {
            self.retiring.remove(lawd);
            if !completed.is_empty() {
                self.entering.insert(lawd.clone(), completed.clone());
            }
        }
        for lawd in self.known_lawds.clone().difference(lawds) {
            if self.retiring.contains_key(lawd) {
                continue;
            }
            self.entering.remove(lawd);
            let asked: BTreeSet<String> = self
                .months
                .iter()
                .filter(|(_, mark)| mark.completed_utc.is_some() || mark.done.contains(lawd))
                .map(|(key, _)| key.clone())
                .collect();
            if !asked.is_empty() {
                self.retiring.insert(lawd.clone(), asked);
            }
            for mark in self.months.values_mut() {
                mark.done.remove(lawd);
            }
        }
        self.known_lawds = lawds.iter().chain(self.retiring.keys()).cloned().collect();
    }

    /// Records one bucket read whole.
    pub(crate) fn settle(&mut self, item: &WorkItem, lawds: &BTreeSet<String>, now: DateTime<Utc>) {
        let key = month_key(&item.dataset, item.month);
        match item.stage {
            Stage::Retiring => {
                if let Some(owed) = self.retiring.get_mut(&item.lawd) {
                    owed.remove(&key);
                    if owed.is_empty() {
                        self.retiring.remove(&item.lawd);
                        self.known_lawds.remove(&item.lawd);
                    }
                }
            }
            Stage::Entering => {
                if let Some(owed) = self.entering.get_mut(&item.lawd) {
                    owed.remove(&key);
                    if owed.is_empty() {
                        self.entering.remove(&item.lawd);
                    }
                }
            }
            _ => {
                let mark = self.months.entry(key).or_default();
                mark.done.insert(item.lawd.clone());
                if lawds.is_subset(&mark.done) {
                    mark.completed_utc = Some(now);
                    mark.done.clear();
                }
            }
        }
    }
}

/// Why a bucket is owed, in the order a run asks them (root ADR-0348 §4).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub(crate) enum Stage {
    /// A month of the first refresh tier (age 0–2).
    Recent,
    /// The final ask of a code that left the list.
    Retiring,
    /// A later refresh tier (`tier` is its index in the contract).
    Refresh { tier: usize },
    /// A completed month a code new to the list was not asked for.
    Entering,
    /// A month never completed: the history.
    Backfill,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct WorkItem {
    pub(crate) stage: Stage,
    pub(crate) priority: usize,
    /// Newest month first: the negated month index.
    pub(crate) newest_first: Month,
    pub(crate) dataset: String,
    pub(crate) month: Month,
    pub(crate) lawd: String,
}

fn item(
    contract: &SourceContract,
    stage: Stage,
    dataset: &str,
    month: Month,
    lawd: &str,
) -> WorkItem {
    WorkItem {
        stage,
        priority: contract.priority_of(dataset),
        newest_first: -month,
        dataset: dataset.to_owned(),
        month,
        lawd: lawd.to_owned(),
    }
}

/// The tier a month of `age` falls in, or `None` once it is older than every tier.
fn tier_of(contract: &SourceContract, age: i32) -> Option<usize> {
    let age = u32::try_from(age).unwrap_or(0);
    contract
        .refresh_tiers
        .iter()
        .position(|tier| age <= tier.max_age_months)
}

/// Every bucket owed now, in asking order.
pub(crate) fn plan_work(
    contract: &SourceContract,
    state: &State,
    lawds: &BTreeSet<String>,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<WorkItem>> {
    let current = kst_month(now);
    let mut work = Vec::new();
    for (name, dataset) in &contract.datasets {
        let first = month_index(&dataset.first_month)?;
        for month in first..=current {
            let mark = state.months.get(&month_key(name, month));
            let completed = mark.and_then(|mark| mark.completed_utc);
            let tier = tier_of(contract, current - month);
            let stage = match (completed, tier) {
                (None, _) => Stage::Backfill,
                (Some(at), Some(tier)) => {
                    if now - at < chrono::Duration::days(contract.refresh_tiers[tier].every_days) {
                        continue;
                    }
                    if tier == 0 {
                        Stage::Recent
                    } else {
                        Stage::Refresh { tier }
                    }
                }
                (Some(_), None) => continue,
            };
            // A month of the newest tier is asked again whole even before its first completion.
            let stage = if stage == Stage::Backfill && tier == Some(0) {
                Stage::Recent
            } else {
                stage
            };
            for lawd in lawds {
                if mark.is_some_and(|mark| mark.done.contains(lawd)) {
                    continue;
                }
                work.push(item(contract, stage, name, month, lawd));
            }
        }
    }
    for (stage, owed) in [
        (Stage::Retiring, &state.retiring),
        (Stage::Entering, &state.entering),
    ] {
        for (lawd, keys) in owed {
            for key in keys {
                let (dataset, month) = split_month_key(key)?;
                if contract.datasets.contains_key(dataset) {
                    work.push(item(contract, stage, dataset, month, lawd));
                }
            }
        }
    }
    work.sort();
    Ok(work)
}

/// The 시군구 list the Silver load wrote (`real_transaction_lawd_set.py`).
pub(crate) fn read_lawd_set(
    path: &Path,
    max_age_days: i64,
    now: DateTime<Utc>,
) -> anyhow::Result<Option<BTreeSet<String>>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw: JsonValue = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("{} is not JSON", path.display()))?;
    ensure!(
        raw["schema_version"] == LAWD_SET_SCHEMA,
        "{} is not a {LAWD_SET_SCHEMA} file",
        path.display()
    );
    let written: DateTime<Utc> = raw["written_at_utc"]
        .as_str()
        .context("the 시군구 list carries no written_at_utc")?
        .parse()
        .context("written_at_utc is not a timestamp")?;
    ensure!(
        now - written <= chrono::Duration::days(max_age_days),
        "the 시군구 list was written {written}, more than {max_age_days} days ago: the Silver load \
         that refreshes it has not run (a stale list misses a new 시군구 without a sign)"
    );
    let codes = raw["lawd_codes"]
        .as_array()
        .context("the 시군구 list carries no lawd_codes")?
        .iter()
        .map(|code| {
            code.as_str()
                .filter(|code| code.len() == 5 && code.bytes().all(|b| b.is_ascii_digit()))
                .map(str::to_owned)
                .with_context(|| format!("not a 5-digit 시군구 code: {code}"))
        })
        .collect::<anyhow::Result<BTreeSet<_>>>()?;
    ensure!(!codes.is_empty(), "the 시군구 list is empty");
    Ok(Some(codes))
}

/// How one response reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Answer {
    Rows {
        items: usize,
        total: u64,
    },
    /// HTTP 429: the gateway's per-second or daily limit.
    Refused,
}

pub(crate) fn read_answer(
    contract: &SourceContract,
    status: u16,
    body: &str,
) -> anyhow::Result<Answer> {
    if status == 429 {
        return Ok(Answer::Refused);
    }
    ensure!(
        (200..300).contains(&status),
        "the provider answered HTTP {status}"
    );
    let value: JsonValue = serde_json::from_str(body).map_err(|_| {
        // An XML body here is the gateway's own error (an unregistered key, say); its text never
        // carries the key.
        anyhow::anyhow!(
            "the provider answered a body that is not JSON: {}",
            body.chars().take(200).collect::<String>()
        )
    })?;
    let header = &value["response"]["header"];
    let code = header["resultCode"].as_str().unwrap_or_default();
    if !contract.result_codes.ok.iter().any(|ok| ok == code) {
        bail!(
            "the provider refused the request: resultCode {code:?} {:?}",
            header["resultMsg"].as_str().unwrap_or_default()
        );
    }
    let body = &value["response"]["body"];
    let total = match &body["totalCount"] {
        JsonValue::Number(number) => number.as_u64(),
        JsonValue::String(text) => text.trim().parse().ok(),
        _ => None,
    }
    .context("an answer carries no totalCount")?;
    // One item comes back as an object, several as an array, none as an empty string.
    let items = match &body["items"]["item"] {
        JsonValue::Array(items) => items.len(),
        JsonValue::Object(_) => 1,
        _ => 0,
    };
    Ok(Answer::Rows { items, total })
}

struct Config {
    base_uri: String,
    service_key: String,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
    lawd_set_path: PathBuf,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        let service_key = required_env_value(SERVICE_KEY_ENV)?;
        ensure!(
            !service_key.trim().is_empty(),
            "{SERVICE_KEY_ENV} must not be empty"
        );
        let state_dir = PathBuf::from(required_env_value(&format!("{PREFIX}_STATE_DIR"))?);
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone())
                .trim_end_matches('/')
                .to_owned(),
            service_key: service_key.trim().to_owned(),
            live_write: matches!(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("1" | "true" | "yes")
            ),
            output_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?),
            lawd_set_path: optional_env_value(&format!("{PREFIX}_LAWD_SET"))?
                .filter(|value| !value.trim().is_empty())
                .map_or_else(|| state_dir.join("lawd-set.json"), PathBuf::from),
            state_dir,
        })
    }
}

/// Sends the requests of one bucket and keeps every response as a Bronze line.
pub(crate) struct Fetcher<'a> {
    contract: &'a SourceContract,
    http: reqwest::Client,
    base_uri: String,
    service_key: String,
    spacing: ProviderRequestSpacing,
    pub(crate) sent: usize,
}

/// What reading one bucket came to.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Bucket {
    /// Every page, as Bronze lines.
    Whole(Vec<String>),
    /// The gateway refused; nothing of the bucket is kept.
    Refused,
}

impl<'a> Fetcher<'a> {
    pub(crate) fn new(
        contract: &'a SourceContract,
        base_uri: &str,
        service_key: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            contract,
            http: reqwest::Client::builder()
                .user_agent(contract.user_agent.clone())
                .timeout(Duration::from_secs(contract.request_timeout_seconds))
                .build()
                .context("failed to build the data.go.kr HTTP client")?,
            base_uri: base_uri.trim_end_matches('/').to_owned(),
            service_key: service_key.to_owned(),
            spacing: ProviderRequestSpacing::try_new(Duration::from_millis(
                contract.request_spacing_ms,
            ))?,
            sent: 0,
        })
    }

    /// Reads one bucket to its last page.
    pub(crate) async fn bucket(
        &mut self,
        state: &mut State,
        work: &WorkItem,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Bucket> {
        let dataset = self.contract.dataset(&work.dataset)?;
        let mut lines = Vec::new();
        let mut seen = 0_u64;
        for page in 1_u32.. {
            let params = [
                ("LAWD_CD", work.lawd.clone()),
                ("DEAL_YMD", yyyymm(work.month)),
                ("pageNo", page.to_string()),
                ("numOfRows", self.contract.page_size.to_string()),
            ];
            self.spacing.wait_before_request(self.sent).await;
            self.sent += 1;
            *state.budget.calls.entry(work.dataset.clone()).or_insert(0) += 1;
            let mut query = vec![("serviceKey", self.service_key.clone())];
            query.extend(
                self.contract
                    .format_param
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.clone())),
            );
            query.extend(params.iter().map(|(name, value)| (*name, value.clone())));
            let response = self
                .http
                .get(format!("{}{}", self.base_uri, dataset.path))
                .query(&query)
                .send()
                .await
                // The URL holds the key; an error must never print it.
                .map_err(|error| {
                    anyhow::anyhow!("{} request failed: {}", work.dataset, error.without_url())
                })?;
            let status = response.status().as_u16();
            let body = response.text().await.map_err(|error| {
                anyhow::anyhow!("{} body failed: {}", work.dataset, error.without_url())
            })?;
            let answer = read_answer(self.contract, status, &body)
                .with_context(|| format!("{} {:?}", work.dataset, params))?;
            let Answer::Rows { items, total } = answer else {
                return Ok(Bucket::Refused);
            };
            lines.push(serde_json::to_string(&json!({
                "operation": dataset.operation,
                "request": params.iter().map(|(name, value)| ((*name).to_owned(), JsonValue::from(value.clone()))).collect::<serde_json::Map<_, _>>(),
                "requested_at_utc": now.to_rfc3339(),
                "http_status": status,
                "body": body,
            }))?);
            seen += u64::try_from(items).unwrap_or(u64::MAX);
            if items == 0 || seen >= total {
                return Ok(Bucket::Whole(lines));
            }
        }
        unreachable!("pages are unbounded")
    }
}

/// What one run did, for the manifest and the job outcome.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct RunReport {
    pub(crate) owed_at_start: usize,
    pub(crate) owed_by_stage: BTreeMap<String, usize>,
    pub(crate) buckets_read: usize,
    pub(crate) calls: usize,
    /// Why the run stopped before the work it owed was done: `call_limit`, `time_budget`,
    /// `daily_limit`, `refused` (HTTP 429).
    pub(crate) stopped_by: Option<String>,
}

fn stage_name(stage: Stage) -> String {
    match stage {
        Stage::Recent => "recent".to_owned(),
        Stage::Retiring => "retiring".to_owned(),
        Stage::Refresh { tier } => format!("refresh_tier_{tier}"),
        Stage::Entering => "entering".to_owned(),
        Stage::Backfill => "backfill".to_owned(),
    }
}

pub(crate) fn run_id(at: DateTime<Utc>) -> String {
    at.format("%Y%m%dT%H%M%SZ").to_string()
}

pub(crate) fn gzip_lines(lines: &[String]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        encoder.write_all(line.as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    Ok(encoder.finish()?)
}

/// The provider file id of one dataset's `sequence`-th object of a run: the run id last, so the
/// Silver load reads it from the key.
pub(crate) fn file_id(dataset: &str, sequence: usize, at: DateTime<Utc>) -> String {
    format!(
        "{}-{sequence:04}-{}",
        dataset.replace('_', "-"),
        run_id(at).to_ascii_lowercase()
    )
}

pub(crate) fn plan(
    dataset: &str,
    operation: &str,
    file_id: &str,
    bytes: Vec<u8>,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, dataset)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: operation.to_owned(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: file_id.to_owned(),
            provider_file_name: format!("{file_id}.gz"),
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: CONTENT_TYPE.to_owned(),
    })
    .with_context(|| format!("failed to plan the Bronze object for {dataset}"))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestObject {
    pub(crate) dataset_slug: String,
    pub(crate) object_key: String,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: u64,
    pub(crate) lines: usize,
    pub(crate) written: bool,
}

/// Commits one dataset's object: source row, run row, write-once object, ledger row. Without a
/// live target it only plans (a dry run).
async fn persist<Uow, Storage>(
    contract: &SourceContract,
    dataset: &str,
    file_id: &str,
    lines: &[String],
    at: DateTime<Utc>,
    live: Option<(&Uow, &Storage)>,
) -> anyhow::Result<ManifestObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let operation = &contract.dataset(dataset)?.operation;
    let bytes = gzip_lines(lines)?;
    let Some((uow, storage)) = live else {
        let plan = plan(
            dataset,
            operation,
            file_id,
            bytes,
            IngestionRunId::new(Uuid::new_v4()),
            at,
        )?;
        return Ok(manifest_object(dataset, &plan, lines.len(), false));
    };
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, dataset, at)?)
        .await
        .with_context(|| format!("failed to upsert the {dataset} source"))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, at, operation, lines.len()))
        .await
        .with_context(|| format!("failed to create the {dataset} run"))?;
    let plan = plan(dataset, operation, file_id, bytes, run.id, at)?;
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
                record: bronze_object_record(&source, &run, at, &plan, file_id),
            },
        )
        .await
        .with_context(|| format!("failed to commit {}", plan.object_key.as_str()))?;
    ensure!(
        outcome.checksum_sha256 == plan.checksum_sha256,
        "Bronze holds different bytes at {}",
        plan.object_key.as_str()
    );
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: u64::try_from(lines.len()).unwrap_or(u64::MAX),
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {dataset} run"))?;
    Ok(manifest_object(dataset, &plan, lines.len(), true))
}

fn manifest_object(
    dataset: &str,
    plan: &PublicDataBulkFilePlan,
    lines: usize,
    written: bool,
) -> ManifestObject {
    ManifestObject {
        dataset_slug: dataset.to_owned(),
        object_key: plan.object_key.as_str().to_owned(),
        checksum_sha256: plan.checksum_sha256.clone(),
        size_bytes: plan.size_bytes,
        lines,
        written,
    }
}

fn bronze_object_record(
    source: &SourceCatalogEntry,
    run: &IngestionRun,
    at: DateTime<Utc>,
    plan: &PublicDataBulkFilePlan,
    provider_file_id: &str,
) -> BronzeObject {
    BronzeObject {
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
        provider_file_id: Some(provider_file_id.to_owned()),
        provider_file_name: Some(format!("{provider_file_id}.gz")),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    dataset: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, dataset)?,
        name: dataset.to_owned(),
        provider: PROVIDER.to_owned(),
        dataset_name: dataset.to_owned(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some("hourly, recent months daily (root ADR-0348)".to_owned()),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    now: DateTime<Utc>,
    operation: &str,
    lines: usize,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": operation, "requests": lines}),
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

/// Lands what the run read so far, one object per dataset, then saves the state that records it.
struct Lander<'a> {
    contract: &'a SourceContract,
    now: DateTime<Utc>,
    pending: BTreeMap<String, Vec<String>>,
    sequence: BTreeMap<String, usize>,
    objects: Vec<ManifestObject>,
    live: Option<(PgBronzeIngestUnitOfWork, Box<dyn ObjectStorageService>)>,
}

impl Lander<'_> {
    async fn flush(&mut self, state: &State, state_path: &Path) -> anyhow::Result<()> {
        for (dataset, lines) in std::mem::take(&mut self.pending) {
            if lines.is_empty() {
                continue;
            }
            let sequence = self.sequence.entry(dataset.clone()).or_insert(0);
            *sequence += 1;
            let id = file_id(&dataset, *sequence, self.now);
            let object = match &self.live {
                Some((uow, storage)) => {
                    persist(
                        self.contract,
                        &dataset,
                        &id,
                        &lines,
                        self.now,
                        Some((uow, storage.as_ref())),
                    )
                    .await?
                }
                None => {
                    persist::<PgBronzeIngestUnitOfWork, dyn ObjectStorageService>(
                        self.contract,
                        &dataset,
                        &id,
                        &lines,
                        self.now,
                        None,
                    )
                    .await?
                }
            };
            self.objects.push(object);
        }
        // Saved only after the objects landed: a failed commit leaves the state as it was, so the
        // next run asks those buckets again. A dry run never moves the state.
        if self.live.is_some() {
            state.save(state_path)?;
        }
        Ok(())
    }
}

/// Collects the buckets owed into Bronze and writes the run's manifest.
///
/// # Errors
/// Returns an error when the configuration is invalid, the provider answers anything but rows or
/// HTTP 429, a Bronze commit fails, or the manifest or state cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let now = Utc::now();
    let started = Instant::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let Some(lawds) = read_lawd_set(&config.lawd_set_path, contract.lawd_set_max_age_days, now)?
    else {
        // The first runs before the Silver load wrote the list: nothing to ask yet.
        println!(
            "data-go-kr-rtms waiting for the 시군구 list at {} (written by the Silver load)",
            config.lawd_set_path.display()
        );
        println!("foundation-job-outcome unchanged");
        return Ok(());
    };
    let state_path = config.state_dir.join("state.json");
    let mut state = State::load(&state_path)?;
    state.roll_budget(now);
    state.absorb_lawd_set(&lawds);
    let work = plan_work(&contract, &state, &lawds, now)?;
    let mut report = RunReport {
        owed_at_start: work.len(),
        ..RunReport::default()
    };
    for item in &work {
        *report
            .owed_by_stage
            .entry(stage_name(item.stage))
            .or_insert(0) += 1;
    }

    let live = if config.live_write {
        live_write_target_preflight().context("RTMS collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for RTMS collection")?;
        let storage = live_write_bronze_object_storage_from_env()
            .await
            .context("failed to configure object storage for RTMS collection")?;
        Some((PgBronzeIngestUnitOfWork::new(pool), storage))
    } else {
        None
    };
    let mut lander = Lander {
        contract: &contract,
        now,
        pending: BTreeMap::new(),
        sequence: BTreeMap::new(),
        objects: Vec::new(),
        live,
    };
    let mut fetcher = Fetcher::new(&contract, &config.base_uri, &config.service_key)?;
    let budget = Duration::from_secs(contract.run_time_budget_seconds);
    let per_run = usize::try_from(contract.per_run_call_limit).unwrap_or(usize::MAX);
    let every = usize::try_from(contract.commit_every_calls).unwrap_or(usize::MAX);
    let mut since_flush = 0_usize;
    for item in &work {
        if fetcher.sent >= per_run {
            report.stopped_by = Some("call_limit".to_owned());
            break;
        }
        if started.elapsed() >= budget {
            report.stopped_by = Some("time_budget".to_owned());
            break;
        }
        if state.budget.calls.get(&item.dataset).copied().unwrap_or(0)
            >= contract.daily_call_limit_per_dataset
        {
            report.stopped_by = Some("daily_limit".to_owned());
            continue;
        }
        let before = fetcher.sent;
        match fetcher.bucket(&mut state, item, now).await {
            Ok(Bucket::Whole(lines)) => {
                lander
                    .pending
                    .entry(item.dataset.clone())
                    .or_default()
                    .extend(lines);
                state.settle(item, &lawds, Utc::now());
                report.buckets_read += 1;
            }
            Ok(Bucket::Refused) => {
                report.stopped_by = Some("refused".to_owned());
                break;
            }
            Err(error) => {
                // What landed stays landed; the failing bucket is asked again next run.
                lander.flush(&state, &state_path).await?;
                return Err(error);
            }
        }
        since_flush += fetcher.sent - before;
        if since_flush >= every {
            lander.flush(&state, &state_path).await?;
            since_flush = 0;
        }
    }
    lander.flush(&state, &state_path).await?;
    report.calls = fetcher.sent;

    let manifest = json!({
        "schema_version": MANIFEST_SCHEMA,
        "source_snapshot_id": run_id(now),
        "collected_at_utc": now.to_rfc3339(),
        "live_write": config.live_write,
        "lawd_count": lawds.len(),
        "report": report,
        "calls_today": state.budget.calls,
        "objects": lander.objects,
    });
    write_json_file(&config.output_dir.join("manifest.json"), &manifest)?;
    println!(
        "data-go-kr-rtms-summary-json {}",
        serde_json::to_string(&manifest["report"]).unwrap_or_default()
    );
    println!(
        "foundation-job-outcome {}",
        if lander.objects.iter().any(|object| object.written) {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
