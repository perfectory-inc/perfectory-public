//! 국토교통부 실거래가, 12 kinds, from the data.go.kr RTMS `OpenAPI`: the one channel (owner 2026-11-28, root
//! ADR-0348 as amended), run as an AWS lane.
//!
//! The unit is one (kind, 계약 달) nationwide, as the export it replaces: every 시군구 (`LAWD_CD`) and page of
//! that month is asked, and the month lands as one Bronze object only when every call answered and every
//! 시군구 answered all the rows it counted. Silver keeps its whole-month rule (root ADR-0349 §3): a month is
//! never half there. The months owed and their order are the export's plan (`MonthPlan`, one for both
//! channels): the recent months daily, the older refresh tiers weekly and monthly, then the history from
//! `history_from_month` (the owner's history decision is that one value).
//!
//! Each API has a daily call budget per KST day (`daily_call_limit_per_api`). A month is started only when
//! its API's budget left covers what the month took last time (one call per 시군구 at first); a month
//! the budget or the provider's quota stops is dropped whole and asked again on a later day. The 시군구 are
//! the active 5-digit codes of the newest code.go.kr 법정동 table in Bronze, read by the parser the
//! reference table's load uses (`legal_dong_code_snapshot_stage::FullTableContract`).
//!
//! Every response is kept raw as a Bronze line (operation, request, time, HTTP status, body); the
//! object's key carries its checksum, so a month whose answers did not change is the same object.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_domain::{source_slug, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat};
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_outbox::object_storage::{
    R2InventoryRequest, R2ObjectStorage, R2ObjectStorageConfig,
};
use foundation_outbox::ObjectStorageService;
use foundation_publisher_collection::rt_molit_real_transaction_collect::{
    month_index, month_key, plan_months, yyyymm, Month, MonthMark, MonthPlan,
};
use foundation_publisher_silver::legal_dong_code_snapshot_stage::{
    CodeRow, FullTableContract, EXISTS,
};
use foundation_shared_kernel::env_source::ProcessEnv;
use foundation_shared_kernel::ids::SourceCatalogId;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_line_objects::{JsonLinesWriter, LineObjects, Target};
use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
};
use crate::data_go_kr_onbid_collect::{kst_date, read_answer, Answer, ResultCodes};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};

/// `infra/lakehouse/contracts/data-go-kr-rtms.contract.json`, as compiled in.
const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/data-go-kr-rtms.contract.json");
const PROVIDER: &str = "data.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_RTMS";
const SERVICE_KEY_ENV: &str = "DATA_GO_KR_SERVICE_KEY";
const MANIFEST_SCHEMA: &str = "foundation-platform.data_go_kr_rtms_collect.v1";
const STATE_SCHEMA: &str = "foundation-platform.data_go_kr_rtms_state.v1";

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Kind {
    pub(crate) service: String,
    pub(crate) operation: String,
    /// The first 계약 달 the provider holds (`YYYY-MM`, measured on the export; root ADR-0348).
    pub(crate) first_month: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Districts {
    pub(crate) source_prefix: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) result_type: String,
    pub(crate) result_codes: ResultCodes,
    pub(crate) page_size: u64,
    pub(crate) request_spacing_ms: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) daily_call_limit_per_api: u32,
    pub(crate) history_from_month: String,
    pub(crate) districts: Districts,
    #[serde(flatten)]
    pub(crate) plan: MonthPlan,
    pub(crate) kinds: BTreeMap<String, Kind>,
}

impl SourceContract {
    /// Each kind's first month asked: the later of its first month and `history_from_month`.
    pub(crate) fn first_months(&self) -> anyhow::Result<BTreeMap<String, String>> {
        let from = month_index(&self.history_from_month)?;
        self.kinds
            .iter()
            .map(|(name, kind)| {
                let first = month_index(&kind.first_month)?;
                Ok((name.clone(), yyyymm(first.max(from))))
            })
            .collect()
    }
}

/// The compiled-in contract, checked.
///
/// # Errors
/// When the contract does not parse, names a bad month, tier order or page size, or its plan names
/// a kind it does not have.
pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    let contract: SourceContract = serde_json::from_str(CONTRACT_JSON)
        .context("data-go-kr-rtms.contract.json is not valid")?;
    contract.plan.check()?;
    month_index(&contract.history_from_month)
        .with_context(|| format!("history_from_month {:?}", contract.history_from_month))?;
    for (name, kind) in &contract.kinds {
        month_index(&kind.first_month)
            .with_context(|| format!("{name}: first_month {:?}", kind.first_month))?;
        ensure!(
            kind.operation.starts_with("get") && !kind.service.is_empty(),
            "{name}: an operation is get<Service>"
        );
    }
    ensure!(
        contract
            .plan
            .priority
            .iter()
            .all(|name| contract.kinds.contains_key(name)),
        "priority names a kind the contract does not have"
    );
    ensure!(
        (1..=1000).contains(&contract.page_size),
        "page_size must be 1 to 1000"
    );
    ensure!(
        contract.daily_call_limit_per_api > 0,
        "daily_call_limit_per_api must be above 0"
    );
    Ok(contract)
}

// --- the 시군구 ---------------------------------------------------------------------------------------

/// The `LAWD_CD` a month is asked for: the active 시군구-level codes of the 법정동 table (5 digits, its
/// last five zero and the 시군구 part not), except a 시 whose 구 are listed (`41110` 수원시 when `41111`
/// 장안구 is), which is asked by its 구.
pub(crate) fn districts(rows: &[CodeRow]) -> Vec<String> {
    let sigungu: BTreeSet<&str> = rows
        .iter()
        .filter(|row| {
            row.status == EXISTS
                && row.region_cd.len() == 10
                && &row.region_cd[5..] == "00000"
                && &row.region_cd[2..5] != "000"
        })
        .map(|row| &row.region_cd[..5])
        .collect();
    sigungu
        .iter()
        .filter(|code| {
            !(code.ends_with('0')
                && sigungu
                    .iter()
                    .any(|other| other != *code && other[..4] == code[..4]))
        })
        .map(|code| (*code).to_owned())
        .collect()
}

/// The newest code.go.kr 법정동 table in Bronze, through the lane's read-only key.
async fn district_table(contract: &SourceContract) -> anyhow::Result<(String, String)> {
    let reader =
        R2ObjectStorage::from_config(R2ObjectStorageConfig::lakehouse_reader_from(&ProcessEnv)?);
    let listed = reader
        .inventory_audit(R2InventoryRequest::new(
            Some(&contract.districts.source_prefix),
            None,
        )?)
        .await?;
    let newest = listed
        .objects()
        .iter()
        .max_by(|left, right| {
            (left.last_modified.as_deref(), &left.key)
                .cmp(&(right.last_modified.as_deref(), &right.key))
        })
        .with_context(|| format!("no 법정동 table under {}", contract.districts.source_prefix))?;
    let bytes = reader.get_object_bytes(&newest.key).await?;
    let text =
        String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{} is not UTF-8", newest.key))?;
    Ok((newest.key.clone(), text))
}

// --- state ------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Budget {
    pub(crate) kst_date: Option<NaiveDate>,
    /// Calls sent this KST day, per kind (one API each).
    pub(crate) calls: BTreeMap<String, u32>,
    /// Kinds whose API answered its quota today.
    pub(crate) quota_refused: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct State {
    #[serde(default)]
    pub(crate) budget: Budget,
    /// `"<kind>/<YYYYMM>"` → its mark.
    #[serde(default)]
    pub(crate) months: BTreeMap<String, MonthMark>,
    /// `"<kind>/<YYYYMM>"` → the calls it took when last read whole.
    #[serde(default)]
    pub(crate) calls_per_month: BTreeMap<String, u32>,
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

    /// Starts a new KST day's counts when the day changed.
    pub(crate) fn roll_budget(&mut self, now: DateTime<Utc>) {
        let today = kst_date(now);
        if self.budget.kst_date != Some(today) {
            self.budget = Budget {
                kst_date: Some(today),
                ..Budget::default()
            };
        }
    }

    /// Calls the API of `kind` may still send today.
    pub(crate) fn left(&self, contract: &SourceContract, kind: &str) -> u32 {
        if self.budget.quota_refused.contains(kind) {
            return 0;
        }
        contract
            .daily_call_limit_per_api
            .saturating_sub(self.budget.calls.get(kind).copied().unwrap_or(0))
    }

    /// Whether a month of `kind` fits today: what it took last time, or one call per 시군구.
    pub(crate) fn fits(
        &self,
        contract: &SourceContract,
        kind: &str,
        month: Month,
        districts: usize,
    ) -> bool {
        let needed = self
            .calls_per_month
            .get(&month_key(kind, month))
            .copied()
            .unwrap_or_else(|| u32::try_from(districts).unwrap_or(u32::MAX));
        self.left(contract, kind) >= needed
    }
}

// --- asking ------------------------------------------------------------------------------------------

struct Fetcher {
    http: reqwest::Client,
    base_uri: String,
    service_key: String,
    spacing: ProviderRequestSpacing,
    sent: usize,
}

/// One whole month of one kind: its response lines, the rows the 시군구 counted, and the calls it took.
#[derive(Debug, Default)]
pub(crate) struct AskedMonth {
    pub(crate) lines: Vec<String>,
    pub(crate) rows: u64,
    pub(crate) calls: u32,
}

/// What asking one month came to.
#[derive(Debug)]
pub(crate) enum MonthAnswer {
    Whole(AskedMonth),
    /// The budget or the provider's quota stopped it; nothing of it is kept.
    Stopped,
}

impl Fetcher {
    async fn send(
        &mut self,
        contract: &SourceContract,
        kind: &Kind,
        params: &[(&str, String)],
        now: DateTime<Utc>,
    ) -> anyhow::Result<(String, Answer)> {
        self.spacing.wait_before_request(self.sent).await;
        self.sent += 1;
        let mut query = vec![
            ("serviceKey", self.service_key.clone()),
            ("_type", contract.result_type.clone()),
        ];
        query.extend(params.iter().map(|(name, value)| (*name, value.clone())));
        let response = self
            .http
            .get(format!(
                "{}/{}/{}",
                self.base_uri, kind.service, kind.operation
            ))
            .query(&query)
            .send()
            .await
            // The URL holds the key; an error must never print it.
            .map_err(|error| {
                anyhow::anyhow!("{} request failed: {}", kind.operation, error.without_url())
            })?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(|error| {
            anyhow::anyhow!("{} body failed: {}", kind.operation, error.without_url())
        })?;
        let line = serde_json::to_string(&json!({
            "operation": kind.operation,
            "request": params.iter().map(|(name, value)| ((*name).to_owned(), JsonValue::from(value.clone()))).collect::<serde_json::Map<_, _>>(),
            "requested_at_utc": now.to_rfc3339(),
            "http_status": status,
            "body": body,
        }))?;
        let answer = read_answer(&contract.result_codes, status, &body)
            .with_context(|| format!("{} {params:?}", kind.operation))?;
        Ok((line, answer))
    }
}

/// Asks every 시군구 and page of one month of `name`.
async fn ask_month(
    fetcher: &mut Fetcher,
    contract: &SourceContract,
    state: &mut State,
    name: &str,
    month: Month,
    districts: &[String],
    now: DateTime<Utc>,
) -> anyhow::Result<MonthAnswer> {
    let kind = &contract.kinds[name];
    let deal_ymd = yyyymm(month);
    let mut asked = AskedMonth::default();
    for lawd in districts {
        let mut seen = 0_u64;
        for page in 1_u64.. {
            if state.left(contract, name) == 0 {
                return Ok(MonthAnswer::Stopped);
            }
            *state.budget.calls.entry(name.to_owned()).or_insert(0) += 1;
            asked.calls += 1;
            let params = [
                ("LAWD_CD", lawd.clone()),
                ("DEAL_YMD", deal_ymd.clone()),
                ("pageNo", page.to_string()),
                ("numOfRows", contract.page_size.to_string()),
            ];
            let (line, answer) = fetcher.send(contract, kind, &params, now).await?;
            asked.lines.push(line);
            match answer {
                Answer::NoData => break,
                Answer::QuotaRefused => {
                    state.budget.quota_refused.insert(name.to_owned());
                    return Ok(MonthAnswer::Stopped);
                }
                Answer::Rows { items, total } => {
                    seen += u64::try_from(items.len())?;
                    if items.is_empty() || seen >= total {
                        ensure!(
                            seen == total,
                            "{name} {deal_ymd} {lawd}: {seen} rows on the pages, the provider counted {total}"
                        );
                        asked.rows += total;
                        break;
                    }
                }
            }
        }
    }
    Ok(MonthAnswer::Whole(asked))
}

// --- landing -----------------------------------------------------------------------------------------

/// One landed (or, in a dry run, planned) month, as the manifest names it.
#[derive(Clone, Debug, Serialize)]
struct ManifestObject {
    dataset_slug: String,
    deal_ymd: String,
    object_key: String,
    checksum_sha256: String,
    size_bytes: u64,
    lines: usize,
    rows: u64,
    calls: u32,
    written: bool,
}

/// The provider file id of one month: the kind and the month. The key adds the object's checksum.
pub(crate) fn file_id(name: &str, month: Month) -> String {
    format!("{}-{}", name.replace('_', "-"), yyyymm(month))
}

async fn persist<Uow, Storage>(
    contract: &SourceContract,
    output_dir: &Path,
    name: &str,
    month: Month,
    asked: &AskedMonth,
    at: DateTime<Utc>,
    live: Option<(&Uow, &Storage)>,
) -> anyhow::Result<ManifestObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let kind = &contract.kinds[name];
    let id = file_id(name, month);
    let deal_ymd = yyyymm(month);
    let params_month = deal_ymd.clone();
    let mut objects = JsonLinesWriter::new(
        LineObjects {
            source: SourceCatalogEntry {
                id: SourceCatalogId::new(Uuid::new_v4()),
                slug: source_slug(PROVIDER, name)?,
                name: name.to_owned(),
                provider: PROVIDER.to_owned(),
                dataset_name: name.to_owned(),
                base_url: Some(contract.base_url.clone()),
                auth_kind: SourceAuthKind::ServiceKey,
                payload_format: SourcePayloadFormat::Json,
                license_name: None,
                license_url: None,
                terms_url: None,
                collection_frequency: Some("daily (root ADR-0348 §4 tiers)".to_owned()),
                is_active: true,
                created_at: at,
                updated_at: at,
                version: 1,
            },
            operation: &kind.operation,
            collected_at: at,
            file_id: Box::new(move |_| id.clone()),
            content_qualified: true,
            params: Box::new(move || json!({"deal_ymd": params_month})),
        },
        Target::of(live, output_dir),
    )?;
    objects.push_group(&asked.lines).await?;
    let landed = objects.finish().await?;
    let [object] = landed.as_slice() else {
        bail!(
            "{name} {deal_ymd} landed as {} objects: one month is one object",
            landed.len()
        );
    };
    Ok(ManifestObject {
        dataset_slug: name.to_owned(),
        deal_ymd,
        object_key: object.object_key.clone(),
        checksum_sha256: object.checksum_sha256.clone(),
        size_bytes: object.size_bytes,
        lines: object.lines,
        rows: asked.rows,
        calls: asked.calls,
        written: object.written,
    })
}

// --- the run -----------------------------------------------------------------------------------------

struct Config {
    base_uri: String,
    service_key: String,
    request_spacing: ProviderRequestSpacing,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
    /// A local copy of the 법정동 table (a dry run's); none, the newest in Bronze.
    districts_file: Option<PathBuf>,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        let service_key = required_env_value(SERVICE_KEY_ENV)?;
        ensure!(
            !service_key.trim().is_empty(),
            "{SERVICE_KEY_ENV} must not be empty"
        );
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone())
                .trim_end_matches('/')
                .to_owned(),
            service_key: service_key.trim().to_owned(),
            request_spacing: ProviderRequestSpacing::try_new(Duration::from_millis(
                contract.request_spacing_ms,
            ))?,
            live_write: matches!(
                optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?
                    .map(|value| value.trim().to_ascii_lowercase())
                    .as_deref(),
                Some("1" | "true" | "yes")
            ),
            output_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?),
            state_dir: PathBuf::from(required_env_value(&format!("{PREFIX}_STATE_DIR"))?),
            districts_file: optional_env_value(&format!("{PREFIX}_DISTRICTS_FILE"))?
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from),
        })
    }
}

/// `collect-data-go-kr-rtms`: one run of the lane.
///
/// # Errors
/// A bad contract or setting, an unreadable 법정동 table, a refused or inconsistent answer, or a failed
/// landing. Months landed before the failure stay landed and marked.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let now = Utc::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let state_path = config.state_dir.join("state.json");
    let mut state = State::load(&state_path)?;
    state.roll_budget(now);

    let (table_key, table) = match &config.districts_file {
        Some(path) => (
            path.display().to_string(),
            std::fs::read_to_string(path)
                .with_context(|| format!("cannot read {}", path.display()))?,
        ),
        None => district_table(&contract).await?,
    };
    let districts = districts(&FullTableContract::compiled()?.parse(&table)?);
    drop(table);
    ensure!(
        !districts.is_empty(),
        "the 법정동 table {table_key} names no 시군구"
    );

    let first_months = contract.first_months()?;
    let work = plan_months(
        &contract.plan,
        first_months
            .iter()
            .map(|(name, first)| (name.as_str(), first.as_str())),
        &state.months,
        now,
    )?;
    let mut fetcher = Fetcher {
        http: reqwest::Client::builder()
            .user_agent(contract.user_agent.clone())
            .timeout(Duration::from_secs(contract.request_timeout_seconds))
            .build()
            .context("failed to build the data.go.kr HTTP client")?,
        base_uri: config.base_uri.clone(),
        service_key: config.service_key.clone(),
        spacing: config.request_spacing,
        sent: 0,
    };
    let live = if config.live_write {
        live_write_target_preflight().context("RTMS collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for RTMS collection")?;
        Some((
            PgBronzeIngestUnitOfWork::new(pool),
            live_write_bronze_object_storage_from_env()
                .await
                .context("failed to configure object storage for RTMS collection")?,
        ))
    } else {
        None
    };

    let mut objects = Vec::new();
    let mut deferred = BTreeMap::<String, u32>::new();
    let mut stopped = BTreeSet::<String>::new();
    for item in &work {
        if stopped.contains(&item.dataset)
            || !state.fits(&contract, &item.dataset, item.month, districts.len())
        {
            *deferred.entry(item.dataset.clone()).or_insert(0) += 1;
            continue;
        }
        let asked = match ask_month(
            &mut fetcher,
            &contract,
            &mut state,
            &item.dataset,
            item.month,
            &districts,
            now,
        )
        .await?
        {
            MonthAnswer::Whole(asked) => asked,
            MonthAnswer::Stopped => {
                stopped.insert(item.dataset.clone());
                *deferred.entry(item.dataset.clone()).or_insert(0) += 1;
                continue;
            }
        };
        let object = match &live {
            Some((uow, storage)) => {
                persist(
                    &contract,
                    &config.output_dir,
                    &item.dataset,
                    item.month,
                    &asked,
                    now,
                    Some((uow, storage.as_ref())),
                )
                .await?
            }
            None => {
                persist::<PgBronzeIngestUnitOfWork, dyn ObjectStorageService>(
                    &contract,
                    &config.output_dir,
                    &item.dataset,
                    item.month,
                    &asked,
                    now,
                    None,
                )
                .await?
            }
        };
        let key = month_key(&item.dataset, item.month);
        state.months.insert(
            key.clone(),
            MonthMark {
                completed_utc: Some(now),
                checksum_sha256: Some(object.checksum_sha256.clone()),
                provider_count: Some(object.rows),
                verified: true,
            },
        );
        state.calls_per_month.insert(key, asked.calls);
        if config.live_write {
            // Saved after every month: a later failure keeps what landed.
            state.save(&state_path)?;
        }
        objects.push(object);
    }
    if config.live_write {
        state.save(&state_path)?;
    }
    let summary = json!({
        "districts": districts.len(),
        "district_table": table_key,
        "owed": work.len(),
        "landed": objects.len(),
        "rows": objects.iter().map(|object| object.rows).sum::<u64>(),
        "calls": state.budget.calls,
        "deferred": deferred,
        "quota_refused": state.budget.quota_refused,
    });
    write_json_file(
        &config.output_dir.join("manifest.json"),
        &json!({
            "schema_version": MANIFEST_SCHEMA,
            "collected_at_utc": now.to_rfc3339(),
            "live_write": config.live_write,
            "summary": summary,
            "objects": objects,
        }),
    )?;
    println!("data-go-kr-rtms-summary-json {summary}");
    println!(
        "foundation-job-outcome {}",
        if objects.iter().any(|object| object.written) {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(())
}

#[cfg(test)]
#[path = "data_go_kr_rtms_collect/tests.rs"]
mod tests;
