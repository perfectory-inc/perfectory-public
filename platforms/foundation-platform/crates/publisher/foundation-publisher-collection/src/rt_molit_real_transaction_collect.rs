//! 국토교통부 실거래가 from the `rt.molit.go.kr` 조건별 자료제공 bulk export, scheduled (root ADR-0348).
//!
//! The unit is one (dataset, 계약 달) nationwide CSV: the provider allows one month per nationwide
//! export and answers every row of it (95,786 for 2025-09 아파트 전월세, measured). A run plans the
//! months it owes from its state and the contract (`plan_work`): the recent months first (age 0-2,
//! asked again daily), then the older refresh tiers (3-12 weekly, 13-24 monthly: cancellations
//! arrive up to 442 days after a contract), then the history never asked, newest month first, the
//! industrial datasets first. It asks them politely (the manners of root ADR-0281): one session,
//! one request at a time, `request_spacing_seconds` apart, only inside the KST quiet hours, at most
//! `max_downloads_per_run` exports a run and `daily_download_limit` a KST day (the provider's own
//! limit is 100 downloads a day).
//!
//! Every export lands content-qualified (`export--sha256-<hex>.csv`): a month whose bytes did not
//! change since its last pull is not written again, and one whose bytes changed lands beside the
//! earlier object (append-only). The state is saved after every export, so a stopped run loses at
//! most the export in flight.
//!
//! A refusal (HTTP 403/429/503, or a page where JSON or CSV belongs) ends the run at once and writes
//! `refused.json`; while that file exists every run refuses before any request, until a person
//! removes it (root ADR-0281 §3). The provider's daily-limit answer ends the run and waits for the
//! next KST day. Any other provider error fails the run.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Utc};
use collection_application::{
    plan_rt_molit_real_transaction_export_content_addressed, RtMolitExportScope,
    RtMolitRealTransactionExportPlanInput, RtMolitRealTransactionExportRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};
use crate::rt_molit_real_transaction_export_ingest::{
    persist_plan, RtMolitExportClient, RtMolitExportIngestConfig, RtMolitExportIngestInput,
    RtMolitRefusal,
};

/// `infra/lakehouse/contracts/rt-molit-real-transaction.contract.json`, as compiled in.
pub const CONTRACT_JSON: &str =
    include_str!("../../../../infra/lakehouse/contracts/rt-molit-real-transaction.contract.json");
const PREFIX: &str = "FOUNDATION_PLATFORM_RT_MOLIT_COLLECT";
const STATE_SCHEMA: &str = "foundation-platform.rt_molit_real_transaction_state.v1";
const MANIFEST_SCHEMA: &str = "foundation-platform.rt_molit_real_transaction_collect.v1";
const KST_HOURS: i64 = 9;

#[derive(Clone, Debug, Deserialize)]
pub struct Dataset {
    pub thing_code: String,
    pub deal_type_code: String,
    /// The first 계약 달 the provider holds (`YYYY-MM`, measured; root ADR-0348).
    pub first_month: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Tier {
    pub max_age_months: u32,
    pub every_days: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct QuietHours {
    pub start: String,
    pub end: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Manners {
    pub request_spacing_seconds: u64,
    pub quiet_hours_kst: QuietHours,
    pub max_downloads_per_run: u32,
    pub daily_download_limit: u32,
    pub run_time_budget_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SourceContract {
    pub base_url: String,
    pub user_agent: String,
    pub manners: Manners,
    /// Words of the provider's count-check error that mean its daily download limit is spent.
    pub daily_limit_messages: Vec<String>,
    pub refresh_tiers: Vec<Tier>,
    pub priority: Vec<String>,
    pub datasets: BTreeMap<String, Dataset>,
}

impl SourceContract {
    fn priority_of(&self, name: &str) -> usize {
        self.priority
            .iter()
            .position(|p| p == name)
            .unwrap_or(self.priority.len())
    }
}

/// The compiled-in contract, checked.
///
/// # Errors
/// When the contract does not parse or names a bad month, tier order or quiet hours.
pub fn source_contract() -> anyhow::Result<SourceContract> {
    let contract: SourceContract = serde_json::from_str(CONTRACT_JSON)
        .context("rt-molit-real-transaction.contract.json is not valid")?;
    for (name, dataset) in &contract.datasets {
        month_index(&dataset.first_month)
            .with_context(|| format!("{name}: first_month {:?}", dataset.first_month))?;
        crate::rt_molit_real_transaction_export_ingest::rt_molit_dataset_slug(
            &dataset.thing_code,
            &dataset.deal_type_code,
        )
        .ok()
        .filter(|slug| slug == name)
        .with_context(|| format!("{name}: thing/deal codes name another dataset"))?;
    }
    ensure!(
        contract
            .refresh_tiers
            .windows(2)
            .all(|w| w[0].max_age_months < w[1].max_age_months),
        "refresh_tiers must grow in max_age_months"
    );
    quiet_hours(&contract.manners.quiet_hours_kst)?;
    Ok(contract)
}

/// A calendar month as `year * 12 + (month - 1)`.
pub type Month = i32;

/// `YYYY-MM` or `YYYYMM` as a [`Month`].
///
/// # Errors
/// When the text is not a month.
pub fn month_index(text: &str) -> anyhow::Result<Month> {
    let digits: String = text.chars().filter(char::is_ascii_digit).collect();
    ensure!(digits.len() == 6, "not a YYYY-MM month: {text:?}");
    let year: i32 = digits[..4].parse()?;
    let month: i32 = digits[4..].parse()?;
    ensure!((1..=12).contains(&month), "not a month: {text:?}");
    Ok(year * 12 + month - 1)
}

#[must_use]
pub fn yyyymm(month: Month) -> String {
    format!("{:04}{:02}", month.div_euclid(12), month.rem_euclid(12) + 1)
}

fn month_dates(month: Month) -> anyhow::Result<(NaiveDate, NaiveDate)> {
    let year = month.div_euclid(12);
    let number = u32::try_from(month.rem_euclid(12) + 1)?;
    let first = NaiveDate::from_ymd_opt(year, number, 1).context("a month's first day")?;
    let next = if number == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, number + 1, 1)
    }
    .context("the next month's first day")?;
    Ok((first, next.pred_opt().context("a month's last day")?))
}

fn kst(at: DateTime<Utc>) -> chrono::NaiveDateTime {
    (at + chrono::Duration::hours(KST_HOURS)).naive_utc()
}

fn kst_month(at: DateTime<Utc>) -> Month {
    let date = kst(at).date();
    date.year() * 12 + i32::try_from(date.month0()).unwrap_or(0)
}

fn quiet_hours(hours: &QuietHours) -> anyhow::Result<(NaiveTime, NaiveTime)> {
    let parse = |text: &str| {
        NaiveTime::parse_from_str(text, "%H:%M").with_context(|| format!("not a time: {text:?}"))
    };
    Ok((parse(&hours.start)?, parse(&hours.end)?))
}

/// Whether `at` lies inside the contract's KST quiet hours.
///
/// # Errors
/// When the contract's hours do not parse.
pub fn inside_quiet_hours(contract: &SourceContract, at: DateTime<Utc>) -> anyhow::Result<bool> {
    let (start, end) = quiet_hours(&contract.manners.quiet_hours_kst)?;
    let now = kst(at).time();
    Ok(if start <= end {
        start <= now && now < end
    } else {
        now >= start || now < end
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    pub kst_date: Option<NaiveDate>,
    pub downloads: u32,
    /// The provider answered its daily-limit message on this day.
    #[serde(default)]
    pub provider_limit_reached: bool,
}

/// One (dataset, month): when it was last read whole, and the checksum of that export.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MonthMark {
    pub completed_utc: Option<DateTime<Utc>>,
    pub checksum_sha256: Option<String>,
    pub provider_count: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub budget: Budget,
    /// `"<dataset>/<YYYYMM>"` → its mark.
    #[serde(default)]
    pub months: BTreeMap<String, MonthMark>,
}

#[must_use]
pub fn month_key(dataset: &str, month: Month) -> String {
    format!("{dataset}/{}", yyyymm(month))
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

    pub fn roll_budget(&mut self, now: DateTime<Utc>) {
        let today = kst(now).date();
        if self.budget.kst_date != Some(today) {
            self.budget = Budget {
                kst_date: Some(today),
                ..Budget::default()
            };
        }
    }
}

/// Why a month is owed, in the order a run asks them (root ADR-0348 §4).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub enum Stage {
    /// A month of the first refresh tier (age 0-2), asked again daily.
    Recent,
    /// A later refresh tier (`tier` is its index in the contract).
    Refresh { tier: usize },
    /// A month never read whole: the history.
    Backfill,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkItem {
    pub stage: Stage,
    pub priority: usize,
    /// Newest month first: the negated month index.
    pub newest_first: Month,
    pub dataset: String,
    pub month: Month,
}

fn tier_of(contract: &SourceContract, age: i32) -> Option<usize> {
    let age = u32::try_from(age).unwrap_or(0);
    contract
        .refresh_tiers
        .iter()
        .position(|tier| age <= tier.max_age_months)
}

/// Every month owed now, in asking order.
///
/// # Errors
/// When a dataset's first month does not parse.
pub fn plan_work(
    contract: &SourceContract,
    state: &State,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<WorkItem>> {
    let current = kst_month(now);
    let mut work = Vec::new();
    for (name, dataset) in &contract.datasets {
        for month in month_index(&dataset.first_month)?..=current {
            let completed = state
                .months
                .get(&month_key(name, month))
                .and_then(|mark| mark.completed_utc);
            let tier = tier_of(contract, current - month);
            let stage = match (completed, tier) {
                (None, Some(0)) => Stage::Recent,
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
            work.push(WorkItem {
                stage,
                priority: contract.priority_of(name),
                newest_first: -month,
                dataset: name.clone(),
                month,
            });
        }
    }
    work.sort();
    Ok(work)
}

/// How one export ended.
#[derive(Debug)]
pub enum Asked {
    /// The export's bytes and the provider's count.
    Export { count: u64, bytes: Vec<u8> },
    /// The provider's daily download limit is spent.
    DailyLimit(String),
}

/// Reads an error of the client: a refusal stays a refusal, the provider's daily-limit message is
/// [`Asked::DailyLimit`], anything else is the run's failure.
fn classify(contract: &SourceContract, error: anyhow::Error) -> anyhow::Result<Asked> {
    if error.downcast_ref::<RtMolitRefusal>().is_some() {
        return Err(error);
    }
    let text = format!("{error:#}");
    if contract
        .daily_limit_messages
        .iter()
        .any(|words| text.contains(words.as_str()))
    {
        return Ok(Asked::DailyLimit(text));
    }
    Err(error)
}

/// What one run did.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct RunReport {
    pub owed_at_start: usize,
    pub owed_by_stage: BTreeMap<String, usize>,
    pub exports: usize,
    pub written: usize,
    pub unchanged: usize,
    pub rows: u64,
    /// Exports whose CSV held a different number of rows than the count check said.
    pub count_mismatches: Vec<String>,
    /// `outside_quiet_hours`, `run_limit`, `daily_limit`, `provider_daily_limit`, `time_budget`.
    pub stopped_by: Option<String>,
}

fn stage_name(stage: Stage) -> String {
    match stage {
        Stage::Recent => "recent".to_owned(),
        Stage::Refresh { tier } => format!("refresh_tier_{tier}"),
        Stage::Backfill => "backfill".to_owned(),
    }
}

/// The data rows of an export: the lines after the header line that starts with `"NO"`.
#[must_use]
pub fn csv_data_rows(bytes: &[u8]) -> Option<u64> {
    let mut lines = bytes.split(|byte| *byte == b'\n');
    lines.position(|line| line.starts_with(b"\"NO\""))?;
    Some(
        lines
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            .count() as u64,
    )
}

struct Config {
    base_uri: String,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
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
            state_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_STATE_DIR"))?),
        })
    }
}

fn request_for(
    contract: &SourceContract,
    item: &WorkItem,
) -> anyhow::Result<RtMolitRealTransactionExportRequest> {
    let dataset = &contract.datasets[&item.dataset];
    let (contract_from, contract_to) = month_dates(item.month)?;
    Ok(RtMolitRealTransactionExportRequest {
        thing_code: dataset.thing_code.clone(),
        deal_type_code: dataset.deal_type_code.clone(),
        contract_from,
        contract_to,
        scope: RtMolitExportScope::Nationwide,
        response_format: "csv".to_owned(),
    })
}

/// Collects the months owed and writes the run's manifest.
///
/// # Errors
/// When a refusal file blocks the run, the provider refuses (the file is then written), answers an
/// unknown error, or a Bronze commit, the state or the manifest cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let refused_path = config.state_dir.join("refused.json");
    if refused_path.exists() {
        bail!(
            "rt.molit.go.kr refused an earlier run ({}); no request is sent until a person reads it \
             and removes the file (root ADR-0348 §6)",
            refused_path.display()
        );
    }
    let now = Utc::now();
    let started = Instant::now();
    let state_path = config.state_dir.join("state.json");
    let mut state = State::load(&state_path)?;
    state.roll_budget(now);
    let work = plan_work(&contract, &state, now)?;
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
    let manners = &contract.manners;
    let spacing = Duration::from_secs(manners.request_spacing_seconds);
    let mut objects = Vec::new();
    if !inside_quiet_hours(&contract, now)? {
        report.stopped_by = Some("outside_quiet_hours".to_owned());
    } else if !work.is_empty() {
        if config.live_write {
            crate::bronze_object_storage::live_write_target_preflight()
                .context("rt.molit.go.kr collection live-write preflight failed")?;
        }
        let client =
            RtMolitExportClient::new(config.base_uri.clone(), contract.user_agent.clone())?;
        let session = match client.open_session().await {
            Ok(session) => session,
            Err(error) => return refuse(&refused_path, error, &report),
        };
        for item in &work {
            if state.budget.provider_limit_reached {
                report.stopped_by = Some("provider_daily_limit".to_owned());
                break;
            }
            if state.budget.downloads >= manners.daily_download_limit {
                report.stopped_by = Some("daily_limit".to_owned());
                break;
            }
            if report.exports >= usize::try_from(manners.max_downloads_per_run).unwrap_or(0) {
                report.stopped_by = Some("run_limit".to_owned());
                break;
            }
            if started.elapsed() >= Duration::from_secs(manners.run_time_budget_seconds)
                || !inside_quiet_hours(&contract, Utc::now())?
            {
                report.stopped_by = Some("time_budget".to_owned());
                break;
            }
            let request = request_for(&contract, item)?;
            state.budget.downloads += 1;
            let asked = match client.fetch_in_session(&session, &request, spacing).await {
                Ok(fetched) => Asked::Export {
                    count: fetched.provider_count,
                    bytes: fetched.raw_payload,
                },
                Err(error) => match classify(&contract, error) {
                    Ok(asked) => asked,
                    Err(error) if error.downcast_ref::<RtMolitRefusal>().is_some() => {
                        state.save(&state_path)?;
                        return refuse(&refused_path, error, &report);
                    }
                    Err(error) => {
                        state.save(&state_path)?;
                        return Err(error)
                            .with_context(|| format!("{} {}", item.dataset, yyyymm(item.month)));
                    }
                },
            };
            let (count, bytes) = match asked {
                Asked::DailyLimit(message) => {
                    tracing::info!(message, "rt.molit.go.kr daily download limit reached");
                    state.budget.provider_limit_reached = true;
                    state.save(&state_path)?;
                    report.stopped_by = Some("provider_daily_limit".to_owned());
                    break;
                }
                Asked::Export { count, bytes } => (count, bytes),
            };
            report.exports += 1;
            report.rows += count;
            if csv_data_rows(&bytes) != Some(count) {
                report
                    .count_mismatches
                    .push(format!("{} {}", item.dataset, yyyymm(item.month)));
            }
            let key = month_key(&item.dataset, item.month);
            let source_slug = collection_domain::source_slug("rt.molit.go.kr", &item.dataset)?;
            let plan = plan_rt_molit_real_transaction_export_content_addressed(
                RtMolitRealTransactionExportPlanInput {
                    source_slug: &source_slug,
                    request: request.clone(),
                    raw_payload: bytes,
                },
            )?;
            let unchanged = state
                .months
                .get(&key)
                .and_then(|mark| mark.checksum_sha256.as_deref())
                == Some(plan.checksum_sha256.as_str());
            if unchanged {
                report.unchanged += 1;
            } else if config.live_write {
                let ingest = RtMolitExportIngestConfig::from_input(RtMolitExportIngestInput {
                    request,
                    base_uri: Some(config.base_uri.clone()),
                    user_agent: Some(contract.user_agent.clone()),
                    live_write: Some("1".to_owned()),
                })?;
                let object_key = plan.object_key.as_str().to_owned();
                persist_plan(&ingest, count, plan.clone()).await?;
                objects.push(json!({"dataset": item.dataset, "month": yyyymm(item.month),
                                    "object_key": object_key, "rows": count}));
                report.written += 1;
            } else {
                objects.push(json!({"dataset": item.dataset, "month": yyyymm(item.month),
                                    "object_key": plan.object_key.as_str(), "rows": count,
                                    "planned_only": true}));
            }
            let mark = state.months.entry(key).or_default();
            mark.completed_utc = Some(Utc::now());
            mark.checksum_sha256 = Some(plan.checksum_sha256.clone());
            mark.provider_count = Some(count);
            if config.live_write {
                state.save(&state_path)?;
            }
        }
    }
    if config.live_write {
        state.save(&state_path)?;
    }
    let manifest = json!({
        "schema_version": MANIFEST_SCHEMA,
        "collected_at_utc": now.to_rfc3339(),
        "live_write": config.live_write,
        "report": report,
        "downloads_today": state.budget.downloads,
        "objects": objects,
    });
    write_json_file(&config.output_dir.join("manifest.json"), &manifest)?;
    println!(
        "rt-molit-collect-summary-json {}",
        serde_json::to_string(&manifest["report"]).unwrap_or_default()
    );
    println!(
        "foundation-job-outcome {}",
        if report.written > 0 {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(())
}

/// Writes the refusal file and fails the run (root ADR-0281 §3).
fn refuse(path: &Path, error: anyhow::Error, report: &RunReport) -> anyhow::Result<()> {
    write_json_file(
        path,
        &json!({
            "refused_at_utc": Utc::now().to_rfc3339(),
            "reason": format!("{error:#}"),
            "report": report,
            "clear": "read the reason, check the site by hand, then remove this file",
        }),
    )?;
    Err(error.context(
        "rt.molit.go.kr refused; later runs are blocked until a person removes refused.json",
    ))
}

#[cfg(test)]
mod tests;
