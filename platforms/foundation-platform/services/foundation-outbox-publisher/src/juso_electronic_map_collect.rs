//! 주소정보 전자지도 daily changes, Bronze collection (root ADR-0475).
//!
//! `juso-electronic-map.contract.json` is the wire format and the work: one URL
//! (update.juso.go.kr/updateInfo.do), one approval key (`FOUNDATION_PLATFORM_JUSO_APP_KEY`, the
//! runtime-secrets group `juso`), and the request codes (cntc_cd) whose files the 11 kinds are in.
//! Each request code keeps a cursor, the next day to ask (`<code>/cursor.json` in the state
//! directory). A run asks windows of at most ten days, oldest first, from the cursor to yesterday in
//! KST, always naming the window (`req_dt`/`req_dt2`) so an ask can be repeated; it stops at the
//! per-run cap (windows per code, bytes per run). An answer is a run of records, a 100-byte header
//! and a zip each. Every zip lands, byte for byte, in the Bronze of every kind its request code
//! serves (`jusogokr__<kind>`), its object named by the provider's file name, retry flag and making
//! date, so landing a file twice is the committer's idempotent success. The cursor moves past a
//! window only after all of its files are committed.
//!
//! Without a key the run is inert: it asks nothing, writes a manifest saying so and ends `waiting`.
//! A refusal (the key or a request code is not ours) stops every ask at once and fails the run, so
//! the unit's `OnFailure` raises the alert; the next scheduled run asks again by itself. The key is in
//! the URL, so no error, log line or manifest may print a URL.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
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

/// `infra/lakehouse/contracts/juso-electronic-map.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/juso-electronic-map.contract.json");
const PROVIDER: &str = "juso.go.kr";
const PREFIX: &str = "FOUNDATION_PLATFORM_JUSO_ELECTRONIC_MAP";
const MANIFEST_SCHEMA: &str = "foundation-platform.juso_electronic_map_collect.v1";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";
const KST_SECONDS: i32 = 9 * 3600;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Query {
    pub(crate) date_gb: String,
    pub(crate) retry_in: String,
    pub(crate) from_param: String,
    pub(crate) to_param: String,
    pub(crate) date_format: String,
    pub(crate) max_window_days: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AnswerFormat {
    pub(crate) header_bytes: usize,
    pub(crate) fields: BTreeMap<String, [usize; 2]>,
    pub(crate) error_header: String,
    pub(crate) max_file_bytes: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ResultCodes {
    pub(crate) ok: Vec<String>,
    pub(crate) no_files: Vec<String>,
    pub(crate) refused: Vec<String>,
    pub(crate) ours: Vec<String>,
    pub(crate) provider: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Backfill {
    pub(crate) first_day: NaiveDate,
    pub(crate) max_windows_per_run: usize,
    pub(crate) max_bytes_per_run: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RequestCode {
    pub(crate) document_code: String,
    pub(crate) name_ko: String,
    pub(crate) published: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Kind {
    pub(crate) request_code: String,
    pub(crate) name_ko: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceContract {
    pub(crate) base_url: String,
    pub(crate) path: String,
    pub(crate) user_agent: String,
    pub(crate) app_key_env: String,
    pub(crate) request_spacing_seconds: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) query: Query,
    pub(crate) answer: AnswerFormat,
    pub(crate) result_codes: ResultCodes,
    pub(crate) backfill: Backfill,
    pub(crate) request_codes: BTreeMap<String, RequestCode>,
    /// Keyed by the catalog's dataset slug; `$comment` is not a kind.
    pub(crate) kinds: BTreeMap<String, JsonValue>,
}

impl SourceContract {
    /// The kinds a request code's file serves, by dataset slug.
    pub(crate) fn kinds_of(&self, request_code: &str) -> anyhow::Result<Vec<(String, Kind)>> {
        let mut out = Vec::new();
        for (slug, value) in &self.kinds {
            if slug.starts_with('$') {
                continue;
            }
            let kind: Kind = serde_json::from_value(value.clone())
                .with_context(|| format!("the juso contract's kind {slug} is not valid"))?;
            if kind.request_code == request_code {
                out.push((slug.clone(), kind));
            }
        }
        Ok(out)
    }
}

pub(crate) fn source_contract() -> anyhow::Result<SourceContract> {
    let contract: SourceContract = serde_json::from_str(CONTRACT_JSON)
        .context("juso-electronic-map.contract.json is not valid")?;
    ensure!(
        contract.query.max_window_days >= 1,
        "the juso contract's window is empty"
    );
    for code in contract.request_codes.keys() {
        ensure!(
            !contract.kinds_of(code)?.is_empty(),
            "the juso request code {code} serves no kind"
        );
    }
    for slug in contract.kinds.keys().filter(|slug| !slug.starts_with('$')) {
        let kind: Kind = serde_json::from_value(contract.kinds[slug].clone())?;
        ensure!(
            contract.request_codes.contains_key(&kind.request_code),
            "the juso kind {slug} names an unknown request code {}",
            kind.request_code
        );
    }
    Ok(contract)
}

/// What an answer's result code means for the run (the contract's `result_codes`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    Ok,
    NoFiles,
    Refused,
    Ours,
    Provider,
}

pub(crate) fn classify(codes: &ResultCodes, code: &str) -> Outcome {
    let has = |list: &[String]| list.iter().any(|known| known == code);
    if has(&codes.ok) {
        Outcome::Ok
    } else if has(&codes.no_files) {
        Outcome::NoFiles
    } else if has(&codes.refused) {
        Outcome::Refused
    } else if has(&codes.provider) {
        Outcome::Provider
    } else {
        // An unknown code is treated as our own mistake: the run fails and says which code.
        Outcome::Ours
    }
}

/// One file of an answer: its header's fields and its bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Received {
    pub(crate) sequence: String,
    pub(crate) base_date: String,
    pub(crate) file_name: String,
    pub(crate) result_code: String,
    pub(crate) request_code: String,
    pub(crate) retry: bool,
    pub(crate) created_date: String,
    pub(crate) bytes: Vec<u8>,
}

fn field<'h>(format: &AnswerFormat, header: &'h [u8], name: &str) -> anyhow::Result<&'h str> {
    let [start, end] = *format
        .fields
        .get(name)
        .with_context(|| format!("the juso answer format has no field {name}"))?;
    let raw = header
        .get(start..end)
        .with_context(|| format!("the juso header is too short for {name}"))?;
    let text = std::str::from_utf8(raw)
        .with_context(|| format!("the juso header's {name} is not ASCII"))?;
    Ok(text.trim_matches(|c: char| c.is_whitespace() || c == '\0'))
}

/// Reads one header; the size is the file's bytes that follow it.
pub(crate) fn read_header(
    format: &AnswerFormat,
    header: &[u8],
) -> anyhow::Result<(Received, usize)> {
    let size: u64 = field(format, header, "file_size")?
        .parse()
        .context("the juso header's file size is not a number")?;
    ensure!(
        size <= format.max_file_bytes,
        "the juso header announces {size} bytes, more than the provider's {} limit",
        format.max_file_bytes
    );
    let file_name = field(format, header, "file_name")?.to_owned();
    ensure!(
        !file_name.is_empty()
            && file_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'),
        "the juso header names an unexpected file {file_name:?}"
    );
    let retry = match field(format, header, "retry")? {
        "Y" => true,
        "N" | "" => false,
        other => bail!("the juso header's retry flag is {other:?}"),
    };
    Ok((
        Received {
            sequence: field(format, header, "file_sequence")?.to_owned(),
            base_date: field(format, header, "base_date")?.to_owned(),
            file_name,
            result_code: field(format, header, "result_code")?.to_owned(),
            request_code: field(format, header, "request_code")?.to_owned(),
            retry,
            created_date: field(format, header, "created_date")?.to_owned(),
            bytes: Vec::new(),
        },
        usize::try_from(size).context("the juso file size does not fit in memory")?,
    ))
}

/// Splits an answer's body into its files, as far as the buffer holds whole records. Returns the
/// files and how many bytes of `buffer` they took; the rest is the start of the next record.
pub(crate) fn take_records(
    format: &AnswerFormat,
    buffer: &[u8],
) -> anyhow::Result<(Vec<Received>, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while buffer.len() - at >= format.header_bytes {
        let header = &buffer[at..at + format.header_bytes];
        let (mut received, size) = read_header(format, header)?;
        let end = at + format.header_bytes + size;
        if buffer.len() < end {
            break;
        }
        received.bytes = buffer[at + format.header_bytes..end].to_vec();
        out.push(received);
        at = end;
    }
    Ok((out, at))
}

/// The windows a run asks for one request code: from `cursor` to `last`, `days` days each, at most
/// `max` of them.
pub(crate) fn windows(
    cursor: NaiveDate,
    last: NaiveDate,
    days: u32,
    max: usize,
) -> Vec<(NaiveDate, NaiveDate)> {
    let mut out = Vec::new();
    let mut from = cursor;
    while from <= last && out.len() < max {
        let to = (from + chrono::Days::new(u64::from(days) - 1)).min(last);
        out.push((from, to));
        from = to + chrono::Days::new(1);
    }
    out
}

/// Yesterday in KST: the last day whose files the provider has made (after 07:00 KST the next day).
pub(crate) fn last_day(now: DateTime<Utc>) -> NaiveDate {
    let kst = FixedOffset::east_opt(KST_SECONDS).unwrap_or_else(|| unreachable!());
    now.with_timezone(&kst).date_naive() - chrono::Days::new(1)
}

/// The Bronze leaf of a received file: its name, retry flag and making date, in the key's alphabet.
/// The same file asked twice is the same leaf; a re-reflected file of the same day is another.
pub(crate) fn provider_file_id(received: &Received) -> String {
    let name: String = received
        .file_name
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!(
        "{}-{}-{}",
        name.trim_end_matches("-zip"),
        if received.retry { "retry" } else { "daily" },
        received.created_date
    )
}

fn cursor_path(state_dir: &Path, request_code: &str) -> PathBuf {
    state_dir.join(request_code).join("cursor.json")
}

pub(crate) fn read_cursor(
    state_dir: &Path,
    request_code: &str,
    first_day: NaiveDate,
) -> anyhow::Result<NaiveDate> {
    let path = cursor_path(state_dir, request_code);
    if !path.exists() {
        return Ok(first_day);
    }
    let value: JsonValue = serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("{} is not JSON", path.display()))?;
    let day = value["next_day"]
        .as_str()
        .with_context(|| format!("{} has no next_day", path.display()))?;
    NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .with_context(|| format!("{} has an unreadable next_day", path.display()))
}

fn write_cursor(
    state_dir: &Path,
    request_code: &str,
    next: NaiveDate,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let path = cursor_path(state_dir, request_code);
    std::fs::create_dir_all(path.parent().context("a cursor has a directory")?)?;
    write_json_file(
        &path,
        &json!({"next_day": next.to_string(), "written_at_utc": at.to_rfc3339()}),
    )
}

/// The key never leaves this process except in the request; a reqwest error would print the URL.
fn without_key(error: reqwest::Error) -> anyhow::Error {
    anyhow::anyhow!("{}", error.without_url())
}

/// One window's answer: the error header (none when files follow) and the files.
pub(crate) struct Asked {
    pub(crate) error_code: Option<String>,
    pub(crate) files: Vec<Received>,
    pub(crate) bytes: u64,
}

/// Asks one request code for one window and reads the answer record by record.
pub(crate) async fn ask(
    contract: &SourceContract,
    http: &reqwest::Client,
    base_uri: &str,
    app_key: &str,
    request_code: &str,
    (from, to): (NaiveDate, NaiveDate),
) -> anyhow::Result<Asked> {
    let query = &contract.query;
    let mut response = http
        .get(format!("{base_uri}{}", contract.path))
        .query(&[
            ("app_key", app_key),
            ("date_gb", query.date_gb.as_str()),
            ("cntc_cd", request_code),
            ("retry_in", query.retry_in.as_str()),
        ])
        .query(&[
            (
                query.from_param.as_str(),
                from.format(&query.date_format).to_string(),
            ),
            (
                query.to_param.as_str(),
                to.format(&query.date_format).to_string(),
            ),
        ])
        .send()
        .await
        .map_err(without_key)
        .context("the juso request failed")?;
    let status = response.status().as_u16();
    ensure!(
        (200..300).contains(&status),
        "the juso request for {request_code} answered HTTP {status}"
    );
    let error_code = response
        .headers()
        .get(contract.answer.error_header.as_str())
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let mut buffer = Vec::new();
    let mut files = Vec::new();
    let mut bytes = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(without_key)
        .context("the juso answer broke off")?
    {
        bytes += chunk.len() as u64;
        buffer.extend_from_slice(&chunk);
        let (taken, used) = take_records(&contract.answer, &buffer)?;
        files.extend(taken);
        buffer.drain(..used);
    }
    ensure!(
        buffer.is_empty(),
        "the juso answer for {request_code} ended inside a record ({} bytes left)",
        buffer.len()
    );
    Ok(Asked {
        error_code,
        files,
        bytes,
    })
}

/// The code an answer stands for: the error header, else every file's own result code, which must
/// agree.
pub(crate) fn answer_code(asked: &Asked) -> anyhow::Result<String> {
    if let Some(code) = &asked.error_code {
        if asked.files.is_empty() {
            return Ok(code.clone());
        }
    }
    let mut codes = asked.files.iter().map(|file| file.result_code.as_str());
    let Some(first) = codes.next() else {
        // HTTP 200, no error header and no file: the provider said nothing.
        bail!("the juso answer has neither an error code nor a file");
    };
    ensure!(
        codes.all(|code| code == first),
        "the juso answer's files carry different result codes"
    );
    Ok(first.to_owned())
}

/// A file must be the zip its header announces and of the code asked.
pub(crate) fn check_file(received: &Received, request_code: &str) -> anyhow::Result<()> {
    ensure!(
        received.request_code == request_code,
        "the juso answer for {request_code} holds a file of {}",
        received.request_code
    );
    ensure!(
        received.bytes.starts_with(ZIP_MAGIC),
        "the juso file {} is not a zip ({} bytes)",
        received.file_name,
        received.bytes.len()
    );
    Ok(())
}

pub(crate) fn plan(
    kind_slug: &str,
    received: &Received,
    run_id: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let file_id = provider_file_id(received);
    let base = NaiveDate::parse_from_str(&received.base_date, "%Y%m%d").ok();
    plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(PROVIDER, kind_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run_id,
        identity: PublicDataBulkFileIdentity {
            operation: format!("juso_{kind_slug}"),
            provider_file_period: None,
            provider_snapshot_date: base.or(Some(at.date_naive())),
            provider_file_name: format!("{file_id}.zip"),
            provider_file_id: file_id,
            provider_updated_at: None,
        },
        raw_payload: received.bytes.clone(),
        content_type: "application/zip".to_owned(),
    })
    .with_context(|| format!("failed to plan the Bronze object of {kind_slug}"))
}

/// Commits one received file into one kind's Bronze: source row, run row, write-once object, ledger
/// row. Returns the object key and checksum.
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
    kind_slug: &str,
    kind: &Kind,
    received: &Received,
    at: DateTime<Utc>,
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<(String, String)>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(&source_catalog_entry(contract, kind_slug, kind, at)?)
        .await
        .with_context(|| format!("failed to upsert the {kind_slug} source"))?;
    let run = uow
        .create_ingestion_run(&ingestion_run(source.id, kind_slug, received, at))
        .await
        .with_context(|| format!("failed to create the {kind_slug} run"))?;
    let mut plan = plan(kind_slug, received, run.id, at)?;
    let record = bronze_object_record(&source, &run, at, &plan, received);
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
    .with_context(|| format!("failed to complete the {kind_slug} run"))?;
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
    received: &Received,
) -> BronzeObject {
    let file_id = provider_file_id(received);
    let mut request_params = plan.request_params.clone();
    request_params["juso_header"] = json!({
        "file_sequence": received.sequence,
        "base_date": received.base_date,
        "file_name": received.file_name,
        "result_code": received.result_code,
        "request_code": received.request_code,
        "retry": received.retry,
        "created_date": received.created_date,
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
        provider_file_name: Some(received.file_name.clone()),
        provider_file_id: Some(file_id),
        provider_updated_at: None,
        effective_date: None,
        created_at: at,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    kind_slug: &str,
    kind: &Kind,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    let published = contract
        .request_codes
        .get(&kind.request_code)
        .map_or("daily", |code| code.published.as_str());
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, kind_slug)?,
        name: kind.name_ko.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: kind_slug.to_owned(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Zip,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!(
            "daily changes asked daily (cntc_cd {}), published {published}",
            kind.request_code
        )),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

fn ingestion_run(
    source_catalog_id: SourceCatalogId,
    kind_slug: &str,
    received: &Received,
    now: DateTime<Utc>,
) -> IngestionRun {
    IngestionRun {
        id: IngestionRunId::new(Uuid::new_v4()),
        source_catalog_id,
        trigger: IngestionTrigger::Scheduled,
        status: IngestionRunStatus::Running,
        request_params: json!({
            "operation": format!("juso_{kind_slug}"),
            "request_code": received.request_code,
            "base_date": received.base_date,
            "objects": 1,
        }),
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
    app_key: Option<String>,
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
            app_key: optional_env_value(&contract.app_key_env)?
                .map(|key| key.trim().to_owned())
                .filter(|key| !key.is_empty()),
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

struct Live {
    uow: PgBronzeIngestUnitOfWork,
    storage: Box<dyn ObjectStorageService>,
}

/// Why a request code stopped before its last window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Stop {
    /// Every window up to yesterday is asked, or the per-run window cap is reached.
    Done,
    /// The run's byte cap is reached; the next run goes on.
    ByteCap,
    /// The provider refused the key or the code: the whole run stops.
    Refused(String),
    /// The provider failed; this code is asked again next run.
    Provider(String),
}

/// One request code's run: its windows in order, each landed in full before the cursor moves.
#[allow(clippy::too_many_arguments)] // the contract, the run's settings and its running totals
async fn collect_code(
    contract: &SourceContract,
    config: &Config,
    app_key: &str,
    live: Option<&Live>,
    request_code: &str,
    requests: &mut usize,
    bytes: &mut u64,
    now: DateTime<Utc>,
    landed: &mut Vec<JsonValue>,
    asked: &mut Vec<JsonValue>,
) -> anyhow::Result<Stop> {
    let kinds = contract.kinds_of(request_code)?;
    let cursor = read_cursor(&config.state_dir, request_code, contract.backfill.first_day)?;
    let http = reqwest::Client::builder()
        .user_agent(contract.user_agent.clone())
        .timeout(Duration::from_secs(contract.request_timeout_seconds))
        .build()
        .context("failed to build the juso HTTP client")?;
    for window in windows(
        cursor,
        last_day(now),
        contract.query.max_window_days,
        contract.backfill.max_windows_per_run,
    ) {
        if *bytes >= contract.backfill.max_bytes_per_run {
            return Ok(Stop::ByteCap);
        }
        config.request_spacing.wait_before_request(*requests).await;
        *requests += 1;
        let answer = ask(
            contract,
            &http,
            &config.base_uri,
            app_key,
            request_code,
            window,
        )
        .await?;
        *bytes += answer.bytes;
        let code = answer_code(&answer)?;
        asked.push(json!({
            "request_code": request_code,
            "from": window.0.to_string(),
            "to": window.1.to_string(),
            "result_code": code,
            "files": answer.files.len(),
            "bytes": answer.bytes,
        }));
        match classify(&contract.result_codes, &code) {
            Outcome::Ok => {}
            Outcome::NoFiles => {
                if live.is_some() {
                    write_cursor(&config.state_dir, request_code, window.1 + chrono::Days::new(1), now)?;
                }
                continue;
            }
            Outcome::Refused => return Ok(Stop::Refused(code)),
            Outcome::Provider => return Ok(Stop::Provider(code)),
            Outcome::Ours => bail!(
                "the juso request for {request_code} {}..{} was answered {code}, a request this collector should not make",
                window.0,
                window.1
            ),
        }
        for received in &answer.files {
            check_file(received, request_code)?;
            for (slug, kind) in &kinds {
                let object = if let Some(live) = live {
                    let (key, checksum) = persist(
                        contract,
                        slug,
                        kind,
                        received,
                        now,
                        &live.uow,
                        live.storage.as_ref(),
                    )
                    .await?;
                    json!({"object_key": key, "checksum_sha256": checksum, "written": true})
                } else {
                    let plan = plan(slug, received, IngestionRunId::new(Uuid::new_v4()), now)?;
                    json!({"object_key": plan.object_key.as_str(), "checksum_sha256": plan.checksum_sha256, "written": false})
                };
                landed.push(json!({
                    "kind": slug,
                    "request_code": request_code,
                    "base_date": received.base_date,
                    "file_name": received.file_name,
                    "retry": received.retry,
                    "created_date": received.created_date,
                    "size_bytes": received.bytes.len(),
                    "object": object,
                }));
            }
        }
        if live.is_some() {
            write_cursor(
                &config.state_dir,
                request_code,
                window.1 + chrono::Days::new(1),
                now,
            )?;
        }
    }
    Ok(Stop::Done)
}

/// Asks every request code's windows from its cursor and lands each file in its kinds' Bronze.
///
/// # Errors
/// Returns an error when the configuration is invalid, the provider refuses the key or a code, an
/// answer is not the contract's, a Bronze commit fails, or the manifest or a cursor cannot be
/// written. A refusal stops every ask at once; a provider failure stops only its code.
pub async fn run() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let config = Config::from_env(&contract)?;
    let now = Utc::now();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed to create {}", config.output_dir.display()))?;
    let manifest_path = config.output_dir.join("manifest.json");
    let Some(app_key) = config.app_key.clone() else {
        write_json_file(
            &manifest_path,
            &json!({"schema_version": MANIFEST_SCHEMA, "collected_at_utc": now.to_rfc3339(), "inert": true,
                    "reason": format!("{} is not set (runtime-secrets group juso)", contract.app_key_env),
                    "files": [], "asked": [], "stops": {}}),
        )?;
        tracing::info!("juso electronic map collection is inert: no approval key");
        // `waiting`: the data flow map shows the job waiting, with this reason (root ADR-0260).
        println!(
            "foundation-job-outcome waiting for the juso approval key (runtime-secrets group juso)"
        );
        return Ok(());
    };
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("failed to create {}", config.state_dir.display()))?;
    let live = if config.live_write {
        live_write_target_preflight()
            .context("juso electronic map collection live-write preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for juso electronic map collection")?;
        Some(Live {
            uow: PgBronzeIngestUnitOfWork::new(pool),
            storage: live_write_bronze_object_storage_from_env()
                .await
                .context("failed to configure object storage for juso electronic map collection")?,
        })
    } else {
        None
    };
    let mut requests = 0;
    let mut bytes = 0_u64;
    let mut landed = Vec::new();
    let mut asked = Vec::new();
    let mut stops = BTreeMap::new();
    let mut failed = Vec::new();
    let mut refused = None;
    for request_code in contract.request_codes.keys() {
        let stop = collect_code(
            &contract,
            &config,
            &app_key,
            live.as_ref(),
            request_code,
            &mut requests,
            &mut bytes,
            now,
            &mut landed,
            &mut asked,
        )
        .await;
        match stop {
            Ok(stop) => {
                stops.insert(request_code.clone(), format!("{stop:?}"));
                match stop {
                    Stop::Refused(code) => {
                        refused = Some(format!("{request_code}: {code}"));
                        break;
                    }
                    Stop::Provider(code) => failed.push(format!("{request_code}: provider {code}")),
                    Stop::Done | Stop::ByteCap => {}
                }
            }
            Err(error) => {
                tracing::error!(request_code = %request_code, error = %format!("{error:#}"), "juso electronic map collection failed");
                stops.insert(request_code.clone(), "Failed".to_owned());
                failed.push(format!("{request_code}: {error:#}"));
            }
        }
    }
    write_json_file(
        &manifest_path,
        &json!({"schema_version": MANIFEST_SCHEMA, "collected_at_utc": now.to_rfc3339(), "inert": false,
                "requests": requests, "bytes": bytes, "files": landed, "asked": asked, "stops": stops,
                "refused": refused, "failed": failed}),
    )?;
    if let Some(refused) = refused {
        bail!("juso refused the approval key or a request code ({refused}); every ask stopped");
    }
    if !failed.is_empty() {
        bail!(
            "juso electronic map collection failed for {}",
            failed.join("; ")
        );
    }
    println!(
        "foundation-job-outcome {}",
        if landed.is_empty() {
            "unchanged"
        } else {
            "changed"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
