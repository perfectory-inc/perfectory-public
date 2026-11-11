//! 주소DB (`rtlDtaDtlSn` 8), Bronze collection with no login and no key (root ADR-0495).
//!
//! `juso-address-db.contract.json` is the wire format and the work: the provider's public download
//! screen API, a list answer (`list.path`, POST JSON) naming the files of a month and a download
//! answer (`download.path`) that is the zip. A run lands the monthly full once, as the base, then
//! every day's change file from its cursor (`state.json` in the state directory: the base and the
//! next day to land) to yesterday in KST, oldest first, each in its own Bronze source
//! (`jusogokr__address_db_monthly_full`, `jusogokr__address_db_daily_change`), byte for byte. The
//! cursor starts at the base's first day (one month replayed onto the base) and moves past a day only
//! after its file is committed; a day the list does not hold is passed only when a later day is
//! landed. The object is named by the provider's file, so landing a file twice is the committer's
//! idempotent success. The per-run cap is the contract's `per_run_cap`.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Utc};
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
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
    BronzeObjectStorageWriter,
};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value, write_json_file};

/// `infra/lakehouse/contracts/juso-address-db.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/juso-address-db.contract.json");
const PROVIDER: &str = "juso.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_JUSO_ADDRESS_DB";
const MANIFEST_SCHEMA: &str = "foundation-platform.juso_address_db_collect.v1";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";
const KST_SECONDS: i32 = 9 * 3600;
pub(crate) const FULL: &str = "address_db_monthly_full";
pub(crate) const DAILY: &str = "address_db_daily_change";

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Item {
    pub(crate) rtl_dta_dtl_sn: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ListAsk {
    pub(crate) path: String,
    pub(crate) expand: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DownloadAsk {
    pub(crate) path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FileKind {
    pub(crate) name_ko: String,
    pub(crate) list_field: String,
    pub(crate) period_field: String,
    pub(crate) req_type: String,
    pub(crate) ctprvn_cd: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PerRunCap {
    pub(crate) max_day_files_per_run: usize,
    pub(crate) max_bytes_per_run: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) user_agent: String,
    pub(crate) request_spacing_seconds: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) item: Item,
    pub(crate) list: ListAsk,
    pub(crate) download: DownloadAsk,
    /// Keyed by the catalog's dataset slug; `$comment` is not a file kind.
    files: BTreeMap<String, JsonValue>,
    pub(crate) per_run_cap: PerRunCap,
    pub(crate) max_file_bytes: u64,
}

impl SourceContract {
    pub(crate) fn file(&self, slug: &str) -> anyhow::Result<FileKind> {
        serde_json::from_value(
            self.files
                .get(slug)
                .with_context(|| format!("the 주소DB contract has no file kind {slug}"))?
                .clone(),
        )
        .with_context(|| format!("the 주소DB contract's file kind {slug} is not valid"))
    }
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    let contract: SourceContract = serde_json::from_str(CONTRACT_JSON)
        .context("juso-address-db.contract.json is not valid")?;
    let slugs: Vec<&String> = contract
        .files
        .keys()
        .filter(|slug| !slug.starts_with('$'))
        .collect();
    ensure!(
        slugs == [DAILY, FULL],
        "the 주소DB contract's file kinds are {slugs:?}, not the collector's {FULL} and {DAILY}"
    );
    contract.file(FULL)?;
    contract.file(DAILY)?;
    Ok(contract)
}

/// One row of a list answer: a month's full or a day's change file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct Listed {
    #[serde(rename = "crtrYm")]
    pub(crate) month: Option<String>,
    #[serde(rename = "crtrYmd")]
    pub(crate) day: Option<String>,
    #[serde(rename = "fileNm")]
    pub(crate) file_name: Option<String>,
    #[serde(rename = "tmprFileNm")]
    pub(crate) real_file_name: Option<String>,
    #[serde(rename = "isExist")]
    pub(crate) exists: Option<String>,
}

impl Listed {
    fn period<'a>(&'a self, kind: &FileKind) -> Option<&'a str> {
        match kind.period_field.as_str() {
            "crtrYm" => self.month.as_deref(),
            _ => self.day.as_deref(),
        }
    }

    fn is_there(&self) -> bool {
        self.exists.as_deref() == Some("Y")
            && self.file_name.as_deref().is_some_and(|n| !n.is_empty())
            && self
                .real_file_name
                .as_deref()
                .is_some_and(|n| !n.is_empty())
    }
}

/// A file the run will land: its kind, its period (`YYYYMM` or `YYYYMMDD`) and its list row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Wanted {
    pub(crate) slug: &'static str,
    pub(crate) period: String,
    pub(crate) listed: Listed,
}

/// The list answer's rows of one kind.
pub(crate) fn listed(answer: &JsonValue, kind: &FileKind) -> anyhow::Result<Vec<Listed>> {
    let rows = answer["results"][&kind.list_field]
        .as_array()
        .with_context(|| format!("the 주소DB list answer has no {}", kind.list_field))?;
    rows.iter()
        .map(|row| serde_json::from_value(row.clone()).context("a 주소DB list row is not valid"))
        .collect()
}

/// The newest full month the list holds.
pub(crate) fn newest_full(rows: &[Listed], kind: &FileKind) -> Option<Wanted> {
    rows.iter()
        .filter(|row| row.is_there())
        .filter_map(|row| {
            let period = row.period(kind)?;
            (period.len() == 6 && period.bytes().all(|b| b.is_ascii_digit())).then(|| Wanted {
                slug: FULL,
                period: period.to_owned(),
                listed: row.clone(),
            })
        })
        .max_by(|a, b| a.period.cmp(&b.period))
}

fn day_of(period: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(period, "%Y%m%d").ok()
}

/// The day files of `month` the list holds from `next` to `last`, oldest first. A list for a month
/// also names the month before; those are left to that month's own ask.
pub(crate) fn days_of_month(
    rows: &[Listed],
    kind: &FileKind,
    month: (i32, u32),
    next: NaiveDate,
    last: NaiveDate,
) -> Vec<Wanted> {
    let mut out: Vec<(NaiveDate, Wanted)> = rows
        .iter()
        .filter(|row| row.is_there())
        .filter_map(|row| {
            let period = row.period(kind)?;
            let day = day_of(period)?;
            ((day.year(), day.month()) == month && day >= next && day <= last).then(|| {
                (
                    day,
                    Wanted {
                        slug: DAILY,
                        period: period.to_owned(),
                        listed: row.clone(),
                    },
                )
            })
        })
        .collect();
    out.sort_by_key(|(day, _)| *day);
    out.dedup_by_key(|(day, _)| *day);
    out.into_iter().map(|(_, wanted)| wanted).collect()
}

/// The months from `next`'s to `last`'s, oldest first.
pub(crate) fn months(next: NaiveDate, last: NaiveDate) -> Vec<(i32, u32)> {
    let mut out = Vec::new();
    let (mut year, mut month) = (next.year(), next.month());
    while (year, month) <= (last.year(), last.month()) {
        out.push((year, month));
        (year, month) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
    }
    out
}

/// Yesterday in KST: a day's change file is made the day after.
pub(crate) fn last_day(now: DateTime<Utc>) -> NaiveDate {
    let kst = FixedOffset::east_opt(KST_SECONDS).unwrap_or_else(|| unreachable!());
    now.with_timezone(&kst).date_naive() - chrono::Days::new(1)
}

/// The collector's own state: the base it landed and the next day to land.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct State {
    pub(crate) base_month: Option<String>,
    pub(crate) next_day: Option<NaiveDate>,
}

fn state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("state.json")
}

pub(crate) fn read_state(state_dir: &Path) -> anyhow::Result<State> {
    let path = state_path(state_dir);
    if !path.exists() {
        return Ok(State {
            base_month: None,
            next_day: None,
        });
    }
    let value: JsonValue = serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("{} is not JSON", path.display()))?;
    let next_day = value["next_day"]
        .as_str()
        .map(|day| NaiveDate::parse_from_str(day, "%Y-%m-%d"))
        .transpose()
        .with_context(|| format!("{} has an unreadable next_day", path.display()))?;
    Ok(State {
        base_month: value["base_month"].as_str().map(str::to_owned),
        next_day,
    })
}

fn write_state(state_dir: &Path, state: &State, at: DateTime<Utc>) -> anyhow::Result<()> {
    std::fs::create_dir_all(state_dir)?;
    write_json_file(
        &state_path(state_dir),
        &json!({"base_month": state.base_month, "next_day": state.next_day.map(|d| d.to_string()),
                "written_at_utc": at.to_rfc3339()}),
    )
}

/// The first day of a `YYYYMM` base: the day files replay from there.
pub(crate) fn base_first_day(month: &str) -> anyhow::Result<NaiveDate> {
    NaiveDate::parse_from_str(&format!("{month}01"), "%Y%m%d")
        .with_context(|| format!("the 주소DB base month {month} is not YYYYMM"))
}

pub(crate) async fn ask_list(
    contract: &SourceContract,
    http: &reqwest::Client,
    base_uri: &str,
    (year, month): (i32, u32),
) -> anyhow::Result<JsonValue> {
    let response = http
        .post(format!("{base_uri}{}", contract.list.path))
        .json(
            &json!({"rtlDtaDtlSn": contract.item.rtl_dta_dtl_sn, "year": year.to_string(),
                      "month": format!("{month:02}"), "expand": contract.list.expand}),
        )
        .send()
        .await
        .context("the 주소DB list request failed")?;
    ensure!(
        response.status().is_success(),
        "the 주소DB list answered HTTP {}",
        response.status()
    );
    response
        .json()
        .await
        .context("the 주소DB list answer is not JSON")
}

pub(crate) async fn download(
    contract: &SourceContract,
    http: &reqwest::Client,
    base_uri: &str,
    wanted: &Wanted,
) -> anyhow::Result<Vec<u8>> {
    let kind = contract.file(wanted.slug)?;
    let query = [
        ("regYmd", wanted.period[..4].to_owned()),
        ("reqType", kind.req_type.clone()),
        ("ctprvnCd", kind.ctprvn_cd.clone()),
        ("stdde", wanted.period.clone()),
        (
            "fileName",
            wanted.listed.file_name.clone().unwrap_or_default(),
        ),
        (
            "realFileName",
            wanted.listed.real_file_name.clone().unwrap_or_default(),
        ),
        ("intFileNo", String::new()),
        ("intNum", String::new()),
    ];
    let response = http
        .get(format!("{base_uri}{}", contract.download.path))
        .query(&query)
        .send()
        .await
        .with_context(|| format!("the 주소DB download of {} failed", wanted.period))?;
    ensure!(
        response.status().is_success(),
        "the 주소DB download of {} answered HTTP {}",
        wanted.period,
        response.status()
    );
    let bytes = Vec::from(response.bytes().await?);
    check_file(contract, wanted, &bytes)?;
    Ok(bytes)
}

pub(crate) fn check_file(
    contract: &SourceContract,
    wanted: &Wanted,
    bytes: &[u8],
) -> anyhow::Result<()> {
    ensure!(
        bytes.starts_with(ZIP_MAGIC),
        "the 주소DB file {} {} is not a zip ({} bytes)",
        wanted.slug,
        wanted.period,
        bytes.len()
    );
    ensure!(
        bytes.len() as u64 <= contract.max_file_bytes,
        "the 주소DB file {} {} is {} bytes, over the contract's {}",
        wanted.slug,
        wanted.period,
        bytes.len(),
        contract.max_file_bytes
    );
    Ok(())
}

/// The Bronze leaf: the provider's stored name (`202610ALLMTCHG00`, `20261029_dailymatchingdata`).
pub(crate) fn provider_file_id(wanted: &Wanted) -> String {
    let real = wanted.listed.real_file_name.as_deref().unwrap_or_default();
    real.strip_suffix(".zip")
        .unwrap_or(real)
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

pub(crate) fn plan(
    wanted: &Wanted,
    bytes: Vec<u8>,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let file_id = provider_file_id(wanted);
    let (period, date) = if wanted.slug == FULL {
        (
            Some(format!("{}-{}", &wanted.period[..4], &wanted.period[4..6])),
            None,
        )
    } else {
        (None, day_of(&wanted.period))
    };
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, wanted.slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: format!("juso_{}", wanted.slug),
            provider_file_period: period,
            provider_snapshot_date: date,
            provider_file_name: format!("{file_id}.zip"),
            provider_file_id: file_id,
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: "application/zip".to_owned(),
    })
    .with_context(|| format!("failed to plan the Bronze object of {}", wanted.slug))
}

/// Commits one file: source row, run row, write-once object, ledger row. Returns the object key and
/// checksum.
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
    wanted: &Wanted,
    bytes: Vec<u8>,
    at: DateTime<Utc>,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<(String, String)>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let kind = contract.file(wanted.slug)?;
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, wanted.slug, &kind, at)?)
        .await
        .with_context(|| format!("failed to upsert the {} source", wanted.slug))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, wanted, at))
        .await
        .with_context(|| format!("failed to create the {} run", wanted.slug))?;
    let mut plan = plan(wanted, bytes, run.id, at)?;
    let record = bronze_object_record(&source, &run, at, &plan, wanted);
    let outcome = BRONZE_COMMITTER
        .commit(
            &writer,
            uow,
            PlannedBronzeObject {
                object_key: plan.object_key.as_str().to_owned(),
                payload: BronzePayload::InMemory(std::mem::take(&mut plan.raw_payload)),
                content_type: plan.content_type.clone(),
                cache_control: BRONZE_CACHE_CONTROL.to_owned(),
                checksum_sha256: plan.checksum_sha256.clone(),
                record,
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
        logical_records_seen: 1,
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {} run", wanted.slug))?;
    Ok((
        plan.object_key.as_str().to_owned(),
        plan.checksum_sha256.clone(),
    ))
}

fn bronze_object_record(
    source: &SourceCatalogEntry,
    run: &IngestionRun,
    at: DateTime<Utc>,
    plan: &PublicDataBulkFilePlan,
    wanted: &Wanted,
) -> BronzeObject {
    let mut request_params = plan.request_params.clone();
    request_params["juso_listed"] = json!({
        "period": wanted.period,
        "file_name": wanted.listed.file_name,
        "real_file_name": wanted.listed.real_file_name,
    });
    BronzeObject {
        id: BronzeObjectId::new(Uuid::new_v4()),
        source_catalog_id: source.id,
        ingestion_run_id: run.id,
        source_record_id: None,
        source_partition_key: Some(plan.source_partition_key.clone()),
        source_identity_key: plan.source_identity_key.clone(),
        dedupe_key: plan.dedupe_key.clone(),
        request_params,
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
        provider_file_name: Some(format!("{}.zip", provider_file_id(wanted))),
        provider_file_id: Some(provider_file_id(wanted)),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    slug: &str,
    kind: &FileKind,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, slug)?,
        name: kind.name_ko.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: slug.to_owned(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::NoAuth,
        payload_format: SourcePayloadFormat::Zip,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(if slug == FULL {
            "landed once as the base".to_owned()
        } else {
            "checked daily, published daily".to_owned()
        }),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    wanted: &Wanted,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({"operation": format!("juso_{}", wanted.slug),
                               "period": wanted.period, "objects": 1}),
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
    request_spacing: ProviderRequestSpacing,
    live_write: bool,
    output_dir: PathBuf,
    state_dir: PathBuf,
}

impl Config {
    fn from_env(contract: &SourceContract) -> anyhow::Result<Self> {
        Ok(Self {
            base_uri: optional_env_value(&format!("{PREFIX}_BASE_URI"))?
                .unwrap_or_else(|| contract.base_url.clone())
                .trim_end_matches('/')
                .to_owned(),
            request_spacing: ProviderRequestSpacing::try_new(Duration::from_secs(
                contract.request_spacing_seconds,
            ))?,
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

pub(crate) struct Live {
    uow: PgBronzeIngestUnitOfWork,
    storage: Box<dyn ObjectStorageService>,
}

/// Why the run stopped landing day files.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Stop {
    /// Every listed day up to yesterday is landed.
    Done,
    /// The per-run day-file cap is reached; the next run goes on.
    FileCap,
    /// The per-run byte cap is reached; the next run goes on.
    ByteCap,
}

/// The run's work against the provider, with an in-memory or live commit.
pub(crate) struct Collect<'a> {
    pub(crate) contract: &'a SourceContract,
    pub(crate) http: reqwest::Client,
    pub(crate) base_uri: String,
    pub(crate) spacing: ProviderRequestSpacing,
    pub(crate) requests: usize,
    pub(crate) bytes: u64,
    pub(crate) landed: Vec<JsonValue>,
}

impl Collect<'_> {
    async fn list(&mut self, month: (i32, u32)) -> anyhow::Result<JsonValue> {
        self.spacing.wait_before_request(self.requests).await;
        self.requests += 1;
        ask_list(self.contract, &self.http, &self.base_uri, month).await
    }

    async fn land(
        &mut self,
        wanted: &Wanted,
        live: Option<&Live>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.spacing.wait_before_request(self.requests).await;
        self.requests += 1;
        let bytes = download(self.contract, &self.http, &self.base_uri, wanted).await?;
        let size = bytes.len();
        self.bytes += size as u64;
        let object = if let Some(live) = live {
            let (key, checksum) = persist(
                self.contract,
                wanted,
                bytes,
                now,
                &live.uow,
                live.storage.as_ref(),
            )
            .await?;
            json!({"object_key": key, "checksum_sha256": checksum, "written": true})
        } else {
            let plan = plan(wanted, bytes, IngestionRunId::new(Uuid::new_v4()), now)?;
            json!({"object_key": plan.object_key.as_str(), "checksum_sha256": plan.checksum_sha256, "written": false})
        };
        self.landed
            .push(json!({"kind": wanted.slug, "period": wanted.period,
                                "size_bytes": size, "object": object}));
        Ok(())
    }

    /// Lands the base when the state has none, then the day files from the cursor to `last`.
    pub(crate) async fn run(
        &mut self,
        state: &mut State,
        last: NaiveDate,
        live: Option<&Live>,
        state_dir: Option<&Path>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Stop> {
        let full = self.contract.file(FULL)?;
        let daily = self.contract.file(DAILY)?;
        let cap = self.contract.per_run_cap.clone();
        let mut lists: BTreeMap<(i32, u32), JsonValue> = BTreeMap::new();
        if state.base_month.is_none() {
            let this_month = (last.year(), last.month());
            let answer = self.list(this_month).await?;
            let mut base = newest_full(&listed(&answer, &full)?, &full);
            lists.insert(this_month, answer);
            if base.is_none() {
                let previous = (last.year() - 1, 12);
                base = newest_full(&listed(&self.list(previous).await?, &full)?, &full);
            }
            let Some(base) = base else {
                bail!("the 주소DB list names no monthly full file this year or the last");
            };
            self.land(&base, live, now).await?;
            state.base_month = Some(base.period.clone());
            state.next_day = Some(base_first_day(&base.period)?);
            if let Some(dir) = state_dir {
                write_state(dir, state, now)?;
            }
        }
        let mut next = state
            .next_day
            .context("the 주소DB state has a base and no next day")?;
        let mut files = 0;
        for month in months(next, last) {
            let answer = match lists.remove(&month) {
                Some(answer) => answer,
                None => self.list(month).await?,
            };
            for wanted in days_of_month(&listed(&answer, &daily)?, &daily, month, next, last) {
                if files >= cap.max_day_files_per_run {
                    return Ok(Stop::FileCap);
                }
                if self.bytes >= cap.max_bytes_per_run {
                    return Ok(Stop::ByteCap);
                }
                self.land(&wanted, live, now).await?;
                files += 1;
                next = day_of(&wanted.period).context("a listed day is a date")?
                    + chrono::Days::new(1);
                state.next_day = Some(next);
                if let Some(dir) = state_dir {
                    write_state(dir, state, now)?;
                }
            }
        }
        Ok(Stop::Done)
    }
}

/// Lands the base once and each day's change file after it.
///
/// # Errors
/// Returns an error when the configuration is invalid, a list or download answer is not the
/// contract's, a Bronze commit fails, or the manifest or the state cannot be written. What was
/// landed before the error stays landed and in the manifest; the cursor stops at it.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let now = Utc::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let live = if config.live_write {
        live_write_target_preflight().context("주소DB collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for 주소DB collection")?;
        Some(Live {
            uow: PgBronzeIngestUnitOfWork::new(pool),
            storage: live_write_bronze_object_storage_from_env()
                .await
                .context("failed to configure object storage for 주소DB collection")?,
        })
    } else {
        None
    };
    let mut state = read_state(&config.state_dir)?;
    let mut collect = Collect {
        contract: &contract,
        http: reqwest::Client::builder()
            .user_agent(contract.user_agent.clone())
            .timeout(Duration::from_secs(contract.request_timeout_seconds))
            .build()
            .context("failed to build the 주소DB HTTP client")?,
        base_uri: config.base_uri.clone(),
        spacing: config.request_spacing,
        requests: 0,
        bytes: 0,
        landed: Vec::new(),
    };
    let outcome = collect
        .run(
            &mut state,
            last_day(now),
            live.as_ref(),
            live.as_ref().map(|_| config.state_dir.as_path()),
            now,
        )
        .await;
    write_json_file(
        &config.output_dir.join("manifest.json"),
        &json!({"schema_version": MANIFEST_SCHEMA, "collected_at_utc": now.to_rfc3339(),
                "requests": collect.requests, "bytes": collect.bytes, "files": collect.landed,
                "base_month": state.base_month, "next_day": state.next_day.map(|d| d.to_string()),
                "stop": outcome.as_ref().map(|stop| format!("{stop:?}")).ok(),
                "failed": outcome.as_ref().err().map(|error| format!("{error:#}"))}),
    )?;
    outcome.map(|_| ())
}

#[cfg(test)]
mod tests;
