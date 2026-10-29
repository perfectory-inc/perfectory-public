//! 차세대 온비드 public sale from data.go.kr, Bronze collection (root ADR-0280).
//!
//! One run, every hour: the announcement list and the announcement bid-result list for their
//! opening windows; the full open-item list when the contract's interval has passed since the last
//! complete pass; and the bid result of each item condition that left the list, oldest first. Each
//! API's responses of one run land as one gzip JSON Lines Bronze object, one line per request:
//! the request's parameters (never the service key), its HTTP status and the response body as
//! received. Nothing here interprets a row beyond what the run itself needs (paging, the item keys
//! that decide which results to ask): the Silver load parses the lines
//! (`infra/lakehouse/spark/jobs/data_go_kr_onbid.py`).
//!
//! data.go.kr counts each API's calls separately per day (the gateway's X-RateLimit-Limit). A run
//! stops an API before it would pass the contract's own bound for the KST day, and treats HTTP 429
//! (the gateway's quota answer) the same way: the API waits for the next day; the run does not
//! fail. Any other answer that is neither rows (`00`) nor an empty answer (`03`) fails the run: a
//! key or parameter problem must never read as "no items". Calls are counted in the state file even when a run
//! fails, so a failing hour cannot spend tomorrow's quota today.
//!
//! The endpoints, parameters, limits and intervals come from `data-go-kr-onbid.contract.json`, the
//! same file the Silver load reads.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveDate, Utc};
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

/// `infra/lakehouse/contracts/data-go-kr-onbid.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/data-go-kr-onbid.contract.json");
const PROVIDER: &str = "data.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_ONBID";
const SERVICE_KEY_ENV: &str = "DATA_GO_KR_SERVICE_KEY";
const MANIFEST_SCHEMA: &str = "foundation-platform.data_go_kr_onbid_collect.v1";
const STATE_SCHEMA: &str = "foundation-platform.data_go_kr_onbid_state.v1";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const CONTENT_TYPE: &str = "application/gzip";
const KST_HOURS: i64 = 9;

/// The four APIs, by the contract's names for them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum Api {
    AnnouncementList,
    AnnouncementBidResultList,
    ItemList,
    ItemBidResult,
    /// 물건상세 (getRlstDtlInf2): the open item conditions' full detail, incl. photo/attachment URLs.
    ItemDetail,
}

impl Api {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 5] = [
        Self::AnnouncementList,
        Self::AnnouncementBidResultList,
        Self::ItemList,
        Self::ItemBidResult,
        Self::ItemDetail,
    ];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::AnnouncementList => "announcement_list",
            Self::AnnouncementBidResultList => "announcement_bid_result_list",
            Self::ItemList => "item_list",
            Self::ItemBidResult => "item_bid_result",
            Self::ItemDetail => "item_detail",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Endpoint {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) page_size: Option<u32>,
    #[serde(default)]
    pub(crate) item_type_code: Option<String>,
    #[serde(default)]
    pub(crate) opening_window_days_before: Option<i64>,
    #[serde(default)]
    pub(crate) opening_window_days_after: Option<i64>,
    #[serde(default)]
    pub(crate) private_contract_flags: Vec<String>,
    /// At most this many of this endpoint's item-keyed calls per run, so one run stays within the
    /// job's time limit (item_detail backfills many open items; without this the run times out).
    #[serde(default)]
    pub(crate) per_run_limit: Option<u32>,
    #[serde(default)]
    pub(crate) retry_after_hours: Option<i64>,
    #[serde(default)]
    pub(crate) max_asks: Option<u32>,
    /// item_detail: the list-row fields whose digest says the condition changed and its detail is
    /// owed again (root ADR-0330 §2).
    #[serde(default)]
    pub(crate) refresh_on_list_fields: Vec<String>,
    /// item_detail: the list-row field holding the round's bid end (KST `yyyyMMddHHmm[ss]`), the
    /// order details are owed in (root ADR-0330 §2).
    #[serde(default)]
    pub(crate) bid_end_field: Option<String>,
    pub(crate) every_hours: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ResultCodes {
    pub(crate) ok: String,
    pub(crate) no_data: String,
}

/// How 물건상세 photos and appraisal attachments are mirrored to the shared content-addressed store
/// (ADR-0296 §2 / ADR-0297). Consumed by `onbid_media`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Media {
    /// Object-key prefix the content-addressed media lands under (`<prefix>/sha256/<hash>.<ext>`).
    pub(crate) object_prefix: String,
    /// The only host whose URLs are fetched (the provider serves all media from `www.onbid.co.kr`).
    pub(crate) url_host: String,
    /// Downloads attempted per KST day; the day's total across runs.
    pub(crate) daily_download_limit: u32,
    /// Downloads attempted in one collect run — bounds one run's wall time (attempts ×
    /// `download_timeout_seconds`) so a high daily budget cannot make a run exceed its time limit
    /// (ADR-0300 H: every collection job declares a per-run bound).
    pub(crate) per_run_limit: u32,
    /// Per-download timeout (seconds); short because photos are small — a hung link must not let the
    /// media step eat the job's time limit.
    pub(crate) download_timeout_seconds: u64,
    /// A single file larger than this is not stored (safety valve; root: the 5.89 GB single-PUT stall).
    pub(crate) max_file_bytes: u64,
    /// Downloads of one condition's media in flight at once (root ADR-0330 §3).
    pub(crate) concurrency: usize,
    /// No new condition's media is started once this many seconds of the media step have passed,
    /// so the step's wall time is bounded whatever the counts (root ADR-0330 §3).
    pub(crate) run_time_budget_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) request_spacing_ms: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) result_type: String,
    pub(crate) result_codes: ResultCodes,
    pub(crate) daily_call_limit_per_api: BTreeMap<String, JsonValue>,
    pub(crate) property_division_codes: BTreeMap<String, JsonValue>,
    pub(crate) item_list: Endpoint,
    pub(crate) announcement_list: Endpoint,
    pub(crate) announcement_bid_result_list: Endpoint,
    pub(crate) item_bid_result: Endpoint,
    pub(crate) item_detail: Endpoint,
    pub(crate) media: Media,
}

impl SourceContract {
    pub(crate) const fn endpoint(&self, api: Api) -> &Endpoint {
        match api {
            Api::AnnouncementList => &self.announcement_list,
            Api::AnnouncementBidResultList => &self.announcement_bid_result_list,
            Api::ItemList => &self.item_list,
            Api::ItemBidResult => &self.item_bid_result,
            Api::ItemDetail => &self.item_detail,
        }
    }

    fn daily_limit(&self, api: Api) -> anyhow::Result<u32> {
        self.daily_call_limit_per_api
            .get(api.name())
            .and_then(JsonValue::as_u64)
            .and_then(|limit| u32::try_from(limit).ok())
            .with_context(|| format!("the contract names no daily limit for {}", api.name()))
    }

    fn property_codes(&self) -> Vec<String> {
        self.property_division_codes
            .keys()
            .filter(|key| !key.starts_with('$'))
            .cloned()
            .collect()
    }
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    serde_json::from_str(CONTRACT_JSON).context("data-go-kr-onbid.contract.json is not valid")
}

/// One item condition, the key of the item list and of a bid result.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub(crate) struct ItemKey {
    pub(crate) cltr_mng_no: String,
    pub(crate) pbct_cdtn_no: i64,
}

/// A closed item condition whose result is still owed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingResult {
    pub(crate) key: ItemKey,
    pub(crate) closed_at_utc: DateTime<Utc>,
    pub(crate) asks: u32,
    pub(crate) next_ask_at_utc: DateTime<Utc>,
}

/// Calls spent per API on one KST day.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Budget {
    pub(crate) kst_date: Option<NaiveDate>,
    pub(crate) calls: BTreeMap<String, u32>,
    /// APIs the gateway answered 429 for on this day.
    pub(crate) quota_refused: BTreeSet<String>,
    /// 물건상세 media downloads attempted on this KST day (ADR-0296 §2), bounded by the contract's
    /// `media.daily_download_limit`.
    #[serde(default)]
    pub(crate) media_downloads: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct State {
    #[serde(default)]
    pub(crate) budget: Budget,
    /// The start of the last item pass that read every combination to its last page.
    #[serde(default)]
    pub(crate) last_complete_item_pass_utc: Option<DateTime<Utc>>,
    /// The item conditions that pass held; `None` before the first complete pass.
    #[serde(default)]
    pub(crate) open_items: Option<BTreeSet<ItemKey>>,
    #[serde(default)]
    pub(crate) pending_results: Vec<PendingResult>,
    /// Closed conditions the provider still had no result for after `max_asks`.
    #[serde(default)]
    pub(crate) results_given_up: u64,
    /// What the last complete item pass said about each open condition (root ADR-0330 §2).
    #[serde(default, with = "keyed")]
    pub(crate) listed: BTreeMap<ItemKey, Listed>,
    /// Open conditions whose 물건상세 was fetched AND whose media all landed (root ADR-0330 §1), with
    /// the list digest they were fetched at. Pruned each run to the still-open set. (The earlier
    /// `detailed` set marked a condition at fetch even when the media cap skipped its photos; it is
    /// no longer read, so those conditions are fetched again.)
    #[serde(default, with = "keyed")]
    pub(crate) details: BTreeMap<ItemKey, DetailMark>,
    /// Media URL → its stored blob, for the open items only: a URL already stored is not
    /// downloaded again (root ADR-0330 §3).
    #[serde(default)]
    pub(crate) media: BTreeMap<String, MediaRef>,
}

/// One open condition as the item list showed it.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Listed {
    /// The bid end as 14 KST digits (`yyyyMMddHHmmss`); `None` when the row has none.
    pub(crate) bid_ends_kst: Option<String>,
    /// Digest of the contract's `refresh_on_list_fields` values.
    pub(crate) digest: String,
}

/// A condition whose detail and media are complete.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DetailMark {
    /// The list digest it was fetched at; empty when fetched before the item list was read.
    pub(crate) digest: String,
    /// Photos stored for it.
    pub(crate) photos: u32,
}

/// A stored media file, as the detail Bronze line is rewritten to point at it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct MediaRef {
    pub(crate) sha256: String,
    /// The extension (`jpg`, `pdf`, …), `MediaFormat::extension`.
    pub(crate) media_format: String,
    pub(crate) size_bytes: u64,
    /// The item it was seen for, so the memo is pruned with the open items.
    pub(crate) cltr_mng_no: String,
}

/// A map keyed by `ItemKey` as a JSON list of `[key, value]` pairs (JSON keys are strings only).
mod keyed {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::ItemKey;

    pub(crate) fn serialize<S: Serializer, V: Serialize>(
        map: &BTreeMap<ItemKey, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(map.iter())
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>, V: Deserialize<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<ItemKey, V>, D::Error> {
        Ok(Vec::<(ItemKey, V)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
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
}

/// The KST calendar date of an instant (Korea has no daylight saving time).
pub(crate) fn kst_date(at: DateTime<Utc>) -> NaiveDate {
    (at + chrono::Duration::hours(KST_HOURS)).date_naive()
}

/// How one response reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Answer {
    /// `00`: the page's items and the total the provider reports.
    Rows { items: Vec<JsonValue>, total: u64 },
    /// `03`: nothing for this request.
    NoData,
    /// HTTP 429: the gateway's daily quota for this API is spent.
    QuotaRefused,
}

/// Reads a response, refusing anything that is neither rows, an empty answer nor the quota answer.
pub(crate) fn read_answer(
    contract: &SourceContract,
    status: u16,
    body: &str,
) -> anyhow::Result<Answer> {
    if status == 429 {
        return Ok(Answer::QuotaRefused);
    }
    ensure!(
        (200..300).contains(&status),
        "the provider answered HTTP {status}"
    );
    let value: JsonValue = serde_json::from_str(body).map_err(|_| {
        // An XML body here is the gateway's own error (an unregistered key, say); its text is the
        // only description, and it never carries the key.
        anyhow::anyhow!(
            "the provider answered a body that is not JSON: {}",
            body.chars().take(200).collect::<String>()
        )
    })?;
    let header = if value["header"].is_object() {
        &value["header"]
    } else {
        &value["result"]
    };
    let code = header["resultCode"].as_str().unwrap_or_default();
    if code == contract.result_codes.no_data {
        return Ok(Answer::NoData);
    }
    if code != contract.result_codes.ok {
        bail!(
            "the provider refused the request: resultCode {code:?} {:?}",
            header["resultMsg"].as_str().unwrap_or_default()
        );
    }
    let body = &value["body"];
    let total = match &body["totalCount"] {
        JsonValue::Number(number) => number.as_u64(),
        JsonValue::String(text) => text.trim().parse().ok(),
        _ => None,
    }
    .context("a 00 answer carries no totalCount")?;
    // One item comes back as an object, several as an array, none as an empty string.
    let items = match &body["items"]["item"] {
        JsonValue::Array(items) => items.clone(),
        JsonValue::Object(_) => vec![body["items"]["item"].clone()],
        _ => Vec::new(),
    };
    Ok(Answer::Rows { items, total })
}

/// The item key of a list row, or why it has none.
pub(crate) fn item_key(item: &JsonValue) -> anyhow::Result<ItemKey> {
    let cltr_mng_no = item["cltrMngNo"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .context("an item has no cltrMngNo")?
        .trim()
        .to_owned();
    let pbct_cdtn_no = match &item["pbctCdtnNo"] {
        JsonValue::Number(number) => number.as_i64(),
        JsonValue::String(text) => text.trim().parse().ok(),
        _ => None,
    }
    .context("an item has no pbctCdtnNo")?;
    Ok(ItemKey {
        cltr_mng_no,
        pbct_cdtn_no,
    })
}

/// Item conditions open in the previous complete pass and absent from this one: their bidding
/// ended. Added to the owed results once each.
pub(crate) fn close_vanished(
    state: &mut State,
    current: BTreeSet<ItemKey>,
    pass_started: DateTime<Utc>,
) -> usize {
    let owed = state
        .pending_results
        .iter()
        .map(|pending| pending.key.clone())
        .collect::<BTreeSet<_>>();
    let vanished = state
        .open_items
        .as_ref()
        .map(|previous| {
            previous
                .difference(&current)
                .filter(|key| !owed.contains(*key))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let count = vanished.len();
    state
        .pending_results
        .extend(vanished.into_iter().map(|key| PendingResult {
            key,
            closed_at_utc: pass_started,
            asks: 0,
            next_ask_at_utc: pass_started,
        }));
    state.open_items = Some(current);
    state.last_complete_item_pass_utc = Some(pass_started);
    count
}

/// Whether the item list is due: never completed, or its interval has passed.
pub(crate) fn item_pass_due(contract: &SourceContract, state: &State, now: DateTime<Utc>) -> bool {
    state
        .last_complete_item_pass_utc
        .is_none_or(|last| now - last >= chrono::Duration::hours(contract.item_list.every_hours))
}

struct Config {
    base_uri: String,
    service_key: String,
    request_spacing: ProviderRequestSpacing,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
    /// The node-exporter textfile directory for the coverage series; none, none written.
    metrics_dir: Option<PathBuf>,
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
            metrics_dir: optional_env_value(&format!("{PREFIX}_METRICS_DIR"))?
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from),
        })
    }
}

/// Sends requests within each API's daily budget and keeps every response as a Bronze line.
pub(crate) struct Fetcher<'a> {
    contract: &'a SourceContract,
    http: reqwest::Client,
    base_uri: String,
    service_key: String,
    spacing: ProviderRequestSpacing,
    sent: usize,
    pub(crate) lines: BTreeMap<Api, Vec<String>>,
}

/// What one request came to.
pub(crate) enum Sent {
    Answer(Answer),
    /// The API's daily budget is spent; nothing was sent.
    OverBudget,
}

impl<'a> Fetcher<'a> {
    pub(crate) fn new(
        contract: &'a SourceContract,
        base_uri: &str,
        service_key: &str,
        spacing: ProviderRequestSpacing,
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
            spacing,
            sent: 0,
            lines: BTreeMap::new(),
        })
    }

    pub(crate) async fn send(
        &mut self,
        state: &mut State,
        api: Api,
        params: &[(&str, String)],
        now: DateTime<Utc>,
    ) -> anyhow::Result<Sent> {
        let limit = self.contract.daily_limit(api)?;
        let spent = state.budget.calls.get(api.name()).copied().unwrap_or(0);
        if spent >= limit || state.budget.quota_refused.contains(api.name()) {
            return Ok(Sent::OverBudget);
        }
        let endpoint = self.contract.endpoint(api);
        self.spacing.wait_before_request(self.sent).await;
        self.sent += 1;
        *state.budget.calls.entry(api.name().to_owned()).or_insert(0) += 1;
        let mut query = vec![
            ("serviceKey", self.service_key.clone()),
            ("resultType", self.contract.result_type.clone()),
        ];
        query.extend(params.iter().map(|(name, value)| (*name, value.clone())));
        let response = self
            .http
            .get(format!("{}{}", self.base_uri, endpoint.path))
            .query(&query)
            .send()
            .await
            // The URL holds the key; an error must never print it.
            .map_err(|error| {
                anyhow::anyhow!("{} request failed: {}", api.name(), error.without_url())
            })?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(|error| {
            anyhow::anyhow!("{} body failed: {}", api.name(), error.without_url())
        })?;
        let line = json!({
            "operation": endpoint.operation,
            "request": params.iter().map(|(name, value)| ((*name).to_owned(), JsonValue::from(value.clone()))).collect::<serde_json::Map<_, _>>(),
            "requested_at_utc": now.to_rfc3339(),
            "http_status": status,
            "body": body,
        });
        self.lines
            .entry(api)
            .or_default()
            .push(serde_json::to_string(&line)?);
        let answer = read_answer(self.contract, status, &body)
            .with_context(|| format!("{} {:?}", api.name(), params))?;
        if answer == Answer::QuotaRefused {
            state.budget.quota_refused.insert(api.name().to_owned());
        }
        Ok(Sent::Answer(answer))
    }

    /// Reads every page of one parameter set. `Ok(None)` when the budget or quota stopped it.
    pub(crate) async fn all_pages(
        &mut self,
        state: &mut State,
        api: Api,
        params: &[(&str, String)],
        now: DateTime<Utc>,
    ) -> anyhow::Result<Option<Vec<JsonValue>>> {
        let page_size = self
            .contract
            .endpoint(api)
            .page_size
            .with_context(|| format!("{} has no page_size", api.name()))?;
        let mut items = Vec::new();
        for page in 1_u64.. {
            let mut paged = params.to_vec();
            paged.push(("pageNo", page.to_string()));
            paged.push(("numOfRows", page_size.to_string()));
            match self.send(state, api, &paged, now).await? {
                Sent::OverBudget | Sent::Answer(Answer::QuotaRefused) => return Ok(None),
                Sent::Answer(Answer::NoData) => return Ok(Some(items)),
                Sent::Answer(Answer::Rows {
                    items: page_items,
                    total,
                }) => {
                    let got = page_items.len();
                    items.extend(page_items);
                    if got == 0 || items.len() as u64 >= total {
                        return Ok(Some(items));
                    }
                }
            }
        }
        Ok(Some(items))
    }
}

fn yyyymmdd(at: DateTime<Utc>, days: i64) -> String {
    (kst_date(at) + chrono::Duration::days(days))
        .format("%Y%m%d")
        .to_string()
}

/// What one run did, for the manifest and the job outcome.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct RunReport {
    pub(crate) item_pass_attempted: bool,
    pub(crate) item_pass_complete: bool,
    pub(crate) open_items: Option<usize>,
    pub(crate) closed_items: usize,
    pub(crate) results_received: usize,
    pub(crate) results_not_yet: usize,
    pub(crate) results_owed: usize,
    /// 물건상세 responses received this run (00 or 03 — both mark the condition detailed).
    pub(crate) details_received: usize,
    /// Open conditions still owed a 물건상세 after this run (budget or an isolated error stopped it).
    pub(crate) details_owed: usize,
    /// 물건상세 requests that errored this run; isolated so they never fail the run.
    pub(crate) details_errored: usize,
    /// Of the details received, those asked again because the provider changed the list row.
    pub(crate) details_refreshed: usize,
    /// Of the details received, those whose media all landed (marked complete after the media step).
    pub(crate) details_completed: usize,
    pub(crate) stopped_by_budget: Vec<String>,
    /// The details received, for the media step to settle; not part of the manifest.
    #[serde(skip)]
    pub(crate) fetched: Vec<Fetched>,
}

/// Runs every due request. Mutates the state only in memory; the caller saves it.
pub(crate) async fn collect(
    contract: &SourceContract,
    fetcher: &mut Fetcher<'_>,
    state: &mut State,
    now: DateTime<Utc>,
) -> anyhow::Result<RunReport> {
    state.roll_budget(now);
    let mut report = RunReport::default();
    let codes = contract.property_codes();

    for api in [Api::AnnouncementList, Api::AnnouncementBidResultList] {
        let endpoint = contract.endpoint(api);
        let item_type = endpoint
            .item_type_code
            .clone()
            .with_context(|| format!("{} has no item_type_code", api.name()))?;
        let window = (
            yyyymmdd(now, -endpoint.opening_window_days_before.unwrap_or(0)),
            yyyymmdd(now, endpoint.opening_window_days_after.unwrap_or(0)),
        );
        for code in &codes {
            let params = [
                ("cltrTypeCd", item_type.clone()),
                ("prptDivCd", code.clone()),
                ("opbdDtStart", window.0.clone()),
                ("opbdDtEnd", window.1.clone()),
            ];
            if fetcher.all_pages(state, api, &params, now).await?.is_none() {
                report.stopped_by_budget.push(api.name().to_owned());
                break;
            }
        }
    }

    if item_pass_due(contract, state, now) {
        report.item_pass_attempted = true;
        let mut keys = BTreeSet::new();
        let mut listed = BTreeMap::new();
        let mut complete = true;
        'pass: for code in &codes {
            for flag in &contract.item_list.private_contract_flags {
                let params = [("prptDivCd", code.clone()), ("pvctTrgtYn", flag.clone())];
                match fetcher
                    .all_pages(state, Api::ItemList, &params, now)
                    .await?
                {
                    Some(items) => {
                        for item in &items {
                            let key = item_key(item)?;
                            listed.insert(key.clone(), listed_of(&contract.item_detail, item));
                            keys.insert(key);
                        }
                    }
                    None => {
                        complete = false;
                        report
                            .stopped_by_budget
                            .push(Api::ItemList.name().to_owned());
                        break 'pass;
                    }
                }
            }
        }
        if complete {
            report.item_pass_complete = true;
            report.open_items = Some(keys.len());
            report.closed_items = close_vanished(state, keys, now);
            state.listed = listed;
        }
    }

    let retry_after =
        chrono::Duration::hours(contract.item_bid_result.retry_after_hours.unwrap_or(24));
    let max_asks = contract.item_bid_result.max_asks.unwrap_or(1);
    state
        .pending_results
        .sort_by_key(|pending| pending.closed_at_utc);
    // At most this many bid-result asks per run, so a large owed backlog drains over many runs
    // instead of one run walking thousands at the request spacing and exceeding the job's time
    // limit (incident 2026-10-26: ~7,600 owed × 0.5s blew past the limit after the daily budget reset).
    let per_run = contract
        .item_bid_result
        .per_run_limit
        .map_or(usize::MAX, |limit| limit as usize);
    let mut asked = 0usize;
    let mut kept = Vec::with_capacity(state.pending_results.len());
    let mut stopped = false;
    for mut pending in std::mem::take(&mut state.pending_results) {
        if stopped || asked >= per_run || pending.next_ask_at_utc > now {
            kept.push(pending);
            continue;
        }
        let params = [
            ("cltrMngNo", pending.key.cltr_mng_no.clone()),
            ("pbctCdtnNo", pending.key.pbct_cdtn_no.to_string()),
        ];
        match fetcher
            .send(state, Api::ItemBidResult, &params, now)
            .await?
        {
            Sent::OverBudget | Sent::Answer(Answer::QuotaRefused) => {
                stopped = true;
                report
                    .stopped_by_budget
                    .push(Api::ItemBidResult.name().to_owned());
                kept.push(pending);
            }
            Sent::Answer(Answer::Rows { .. }) => {
                asked += 1;
                report.results_received += 1;
            }
            Sent::Answer(Answer::NoData) => {
                asked += 1;
                pending.asks += 1;
                if pending.asks >= max_asks {
                    state.results_given_up += 1;
                } else {
                    pending.next_ask_at_utc = now + retry_after;
                    report.results_not_yet += 1;
                    kept.push(pending);
                }
            }
        }
    }
    state.pending_results = kept;
    report.results_owed = state.pending_results.len();

    // 물건상세 (getRlstDtlInf2, ADR-0296 §1 / ADR-0297): the full detail of each open item condition
    // owed one (never complete, or the provider changed its list row), in the order of root ADR-0330
    // §2, within the item-detail daily budget. The response's media is mirrored by `run` after this;
    // a condition is marked complete only once its media all landed (`settle_details`). A detail
    // failure is isolated — counted and the loop stops, but the run still succeeds — so a detail
    // problem (a wrong path, a provider hiccup) never breaks item-list or result collection.
    if let Some(open) = state.open_items.clone() {
        state.details.retain(|key, _| open.contains(key));
        // Only a bounded slice of the owed details per run, so the run finishes within the job's
        // time limit (the full backfill is spread over many runs). Without this the loop walks every
        // open item in one run and the job times out before persisting anything (incident 2026-10-26).
        let per_run = contract
            .item_detail
            .per_run_limit
            .map_or(usize::MAX, |limit| limit as usize);
        let owed = owed_details(state, &open, now);
        let owed_total = owed.len();
        for Owed {
            key,
            digest,
            refresh,
        } in owed.into_iter().take(per_run)
        {
            let params = [
                ("cltrMngNo", key.cltr_mng_no.clone()),
                ("pbctCdtnNo", key.pbct_cdtn_no.to_string()),
            ];
            match fetcher.send(state, Api::ItemDetail, &params, now).await {
                Ok(Sent::OverBudget | Sent::Answer(Answer::QuotaRefused)) => {
                    report
                        .stopped_by_budget
                        .push(Api::ItemDetail.name().to_owned());
                    break;
                }
                // 00 rows or 03 no-data: the provider answered and the line is in Bronze; the media
                // step settles whether the condition is complete.
                Ok(Sent::Answer(Answer::Rows { .. } | Answer::NoData)) => {
                    let line = fetcher
                        .lines
                        .get(&Api::ItemDetail)
                        .map_or(0, |lines| lines.len().saturating_sub(1));
                    report.details_received += 1;
                    report.details_refreshed += usize::from(refresh);
                    report.fetched.push(Fetched {
                        key,
                        digest,
                        refresh,
                        line,
                    });
                }
                Err(error) => {
                    // The request's raw body was already kept as a Bronze line; stop so a broken
                    // endpoint does not spend the day's budget, and let the run still succeed.
                    report.details_errored += 1;
                    tracing::warn!(error = %error, "Onbid item detail fetch failed; isolated from the run");
                    break;
                }
            }
        }
        // Not counting this run's; `run` adds back those whose media did not all land.
        report.details_owed = owed_total - report.details_received;
    }

    report.stopped_by_budget.dedup();
    Ok(report)
}

/// The run's id and every object's `source_snapshot_id`: when the run started.
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

pub(crate) fn plan(
    contract: &SourceContract,
    api: Api,
    bytes: Vec<u8>,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let endpoint = contract.endpoint(api);
    let file_id = file_id(api, at);
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, &endpoint.dataset_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: endpoint.operation.clone(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: file_id.clone(),
            provider_file_name: format!("{file_id}.gz"),
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: CONTENT_TYPE.to_owned(),
    })
    .with_context(|| format!("failed to plan the Bronze object for {}", api.name()))
}

/// The provider file id of one API's object of one run.
pub(crate) fn file_id(api: Api, at: DateTime<Utc>) -> String {
    format!(
        "{}-{}",
        api.name().replace('_', "-"),
        run_id(at).to_ascii_lowercase()
    )
}

/// One landed (or, in a dry run, planned) object, as the manifest names it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestObject {
    pub(crate) role: String,
    pub(crate) dataset_slug: String,
    pub(crate) object_key: String,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: u64,
    pub(crate) lines: usize,
    pub(crate) local_path: String,
    pub(crate) written: bool,
}

fn write_local_copy(output_dir: &Path, plan: &PublicDataBulkFilePlan) -> anyhow::Result<String> {
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?;
    let relative = format!("objects/{name}");
    let path = output_dir.join(&relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&path, &plan.raw_payload)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(relative)
}

/// Commits one API's object: source row, run row, write-once object, ledger row.
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
    output_dir: &Path,
    api: Api,
    lines: &[String],
    at: DateTime<Utc>,
    live: Option<(&Uow, &Storage)>,
) -> anyhow::Result<ManifestObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let endpoint = contract.endpoint(api);
    let bytes = gzip_lines(lines)?;
    let Some((uow, storage)) = live else {
        let plan = plan(
            contract,
            api,
            bytes,
            IngestionRunId::new(Uuid::new_v4()),
            at,
        )?;
        let local = write_local_copy(output_dir, &plan)?;
        return Ok(manifest_object(
            api,
            endpoint,
            &plan,
            lines.len(),
            local,
            false,
        ));
    };
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, endpoint, at)?)
        .await
        .with_context(|| format!("failed to upsert the {} source", endpoint.dataset_slug))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, at, endpoint, lines.len()))
        .await
        .with_context(|| format!("failed to create the {} run", endpoint.dataset_slug))?;
    let plan = plan(contract, api, bytes, run.id, at)?;
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
                record: bronze_object_record(&source, &run, at, &plan, &file_id(api, at)),
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
    .with_context(|| format!("failed to complete the {} run", endpoint.dataset_slug))?;
    Ok(manifest_object(
        api,
        endpoint,
        &plan,
        lines.len(),
        local,
        true,
    ))
}

fn manifest_object(
    api: Api,
    endpoint: &Endpoint,
    plan: &PublicDataBulkFilePlan,
    lines: usize,
    local_path: String,
    written: bool,
) -> ManifestObject {
    ManifestObject {
        role: api.name().to_owned(),
        dataset_slug: endpoint.dataset_slug.clone(),
        object_key: plan.object_key.as_str().to_owned(),
        checksum_sha256: plan.checksum_sha256.clone(),
        size_bytes: plan.size_bytes,
        lines,
        local_path,
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
    endpoint: &Endpoint,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &endpoint.dataset_slug)?,
        name: endpoint.dataset_slug.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: endpoint.dataset_slug.clone(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!("every {} hours", endpoint.every_hours)),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    now: DateTime<Utc>,
    endpoint: &Endpoint,
    lines: usize,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": endpoint.operation, "requests": lines}),
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

/// Collects every due request into Bronze and writes the run's manifest.
///
/// # Errors
/// Returns an error when the configuration is invalid, the provider refuses a request, a Bronze
/// commit fails, or the manifest or state cannot be written.
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
    let before = state.clone();
    let mut fetcher = Fetcher::new(
        &contract,
        &config.base_uri,
        &config.service_key,
        config.request_spacing,
    )?;
    let mut report = match collect(&contract, &mut fetcher, &mut state, now).await {
        Ok(report) => report,
        Err(error) => {
            // The calls were spent; only the counts are kept, so the next run asks again.
            let mut spent = before;
            spent.budget = state.budget;
            spent.save(&state_path)?;
            return Err(error);
        }
    };

    let mut objects = Vec::new();
    let mut media_report: Option<crate::onbid_media::MediaReport> = None;
    if config.live_write {
        live_write_target_preflight().context("Onbid collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for Onbid collection")?;
        let uow = PgBronzeIngestUnitOfWork::new(pool);
        let storage = live_write_bronze_object_storage_from_env()
            .await
            .context("failed to configure object storage for Onbid collection")?;
        // ADR-0296 §2 / ADR-0297: mirror this run's 물건상세 photos & appraisal attachments to the
        // shared content-addressed store, using the just-fetched (fresh, not-yet-expired) URLs, and
        // rewrite the detail lines to point at each stored blob by hash (root ADR-0290 §7) so the
        // detail Bronze object persisted below carries the sha256 refs the Silver photo loader reads.
        // Done BEFORE persist so those refs land in Bronze. The whole step is isolated — a client-build
        // or download failure is logged, leaves the detail lines unchanged, and never fails the run.
        if let Some(detail_lines) = fetcher.lines.get(&Api::ItemDetail).cloned() {
            match reqwest::Client::builder()
                .user_agent(contract.user_agent.clone())
                .timeout(Duration::from_secs(contract.media.download_timeout_seconds))
                .build()
            {
                Ok(http) => {
                    let mut revalidate = vec![false; detail_lines.len()];
                    for fetched in report.fetched.iter().filter(|fetched| fetched.refresh) {
                        if let Some(flag) = revalidate.get_mut(fetched.line) {
                            *flag = true;
                        }
                    }
                    let settings = crate::onbid_media::MirrorSettings {
                        object_prefix: &contract.media.object_prefix,
                        url_host: &contract.media.url_host,
                        max_file_bytes: contract.media.max_file_bytes,
                        daily_limit: contract.media.daily_download_limit,
                        per_run_limit: contract.media.per_run_limit,
                        concurrency: contract.media.concurrency,
                        deadline: std::time::Instant::now()
                            + Duration::from_secs(contract.media.run_time_budget_seconds),
                    };
                    let mirrored = crate::onbid_media::mirror_detail_lines(
                        &detail_lines,
                        &revalidate,
                        &mut state.media,
                        &settings,
                        state.budget.media_downloads,
                        storage.as_ref(),
                        &http,
                    )
                    .await;
                    state.budget.media_downloads = mirrored.downloads_today;
                    report.details_completed =
                        settle_details(&mut state, &report.fetched, &mirrored.outcomes);
                    report.details_owed += report.details_received - report.details_completed;
                    media_report = Some(mirrored.report);
                    fetcher.lines.insert(Api::ItemDetail, mirrored.lines);
                }
                Err(error) => {
                    tracing::warn!(error = %error, "onbid media HTTP client build failed; skipping media");
                }
            }
        }
        for (api, lines) in &fetcher.lines {
            objects.push(
                persist(
                    &contract,
                    &config.output_dir,
                    *api,
                    lines,
                    now,
                    Some((&uow, storage.as_ref())),
                )
                .await?,
            );
        }
    } else {
        for (api, lines) in &fetcher.lines {
            objects.push(
                persist::<PgBronzeIngestUnitOfWork, dyn ObjectStorageService>(
                    &contract,
                    &config.output_dir,
                    *api,
                    lines,
                    now,
                    None,
                )
                .await?,
            );
        }
    }
    let manifest = json!({
        "schema_version": MANIFEST_SCHEMA,
        "source_snapshot_id": run_id(now),
        "collected_at_utc": now.to_rfc3339(),
        "live_write": config.live_write,
        "report": report,
        "calls": state.budget.calls,
        "media": media_report,
        "objects": objects,
    });
    write_json_file(&config.output_dir.join("manifest.json"), &manifest)?;
    // Saved only after every object landed: a failed commit leaves the state as it was, so the
    // next run asks the same item results again.
    if config.live_write {
        prune_media_memo(&mut state);
        state.save(&state_path)?;
        if let Some(dir) = &config.metrics_dir {
            // A metrics write never fails the run; a stale file pages on its own (the rule's age check).
            if let Err(error) = write_metrics(dir, &Coverage::of(&state, now), now) {
                tracing::warn!(error = %error, "Onbid coverage metrics were not written");
            }
        }
    }
    tracing::info!(
        live_write = config.live_write,
        objects = objects.len(),
        item_pass_complete = report.item_pass_complete,
        results_received = report.results_received,
        details_received = report.details_received,
        media_downloaded = media_report.as_ref().map_or(0, |media| media.downloaded),
        media_bytes = media_report.as_ref().map_or(0, |media| media.bytes_stored),
        "Onbid collection succeeded"
    );
    // A durable, grep-able line so the first collection's real photo volume is readable from the
    // persistent run.log (the manifest is deleted after a successful run): this is the measurement
    // that sizes ADR-0296 Stage B before `media.daily_download_limit` is raised.
    if let Some(media) = &media_report {
        println!(
            "onbid-media-summary-json {}",
            serde_json::to_string(media).unwrap_or_default()
        );
    }
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

mod coverage;
#[cfg(test)]
pub(crate) use coverage::{kst14, kst14_of};
use coverage::{listed_of, prune_media_memo, write_metrics};
pub(crate) use coverage::{owed_details, settle_details, Coverage, Fetched, LineOutcome, Owed};

#[cfg(test)]
mod tests;
