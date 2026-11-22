//! 산업용지 분양·처분·공급 공고와 그 첨부, Bronze 와 공용 미디어 저장소로 (root ADR-0603).
//!
//! One command, one source per run (`..._SOURCE` = `ilis`, `kicox` or `lh`; one job each): the
//! ILIS sale-notice registry, the KICOX sale board, LH land notices. A run reads the source's list
//! newest first, page by page, and asks every listed notice the lakehouse does not record as read
//! with the same list fingerprint (`read_from`: the last read of each notice; the fingerprint is the
//! hash of the list fields the source contract names, so a notice the provider changed in its list
//! is read again), at most `per_run_limit` and until `run_time_budget_seconds`. It stops walking the
//! list at the first page that owes nothing once the reads cover the count the provider states,
//! so a run after the first fill asks one list page. Each detail is kept as served; each attachment
//! still in use is downloaded and stored once, whatever its format, in the shared content-addressed
//! media store (`industrial_complex_notice_collect::store_attachment`, root ADR-0298, ADR-0484 §2),
//! and annotated in the line's `attachments`. `commit_every` reads make one gzip JSON Lines Bronze
//! object under a content-qualified key (root ADR-0152), so a stopped run loses at most one batch.
//! The Silver load (`load-industrial-land-sale-notice-silver`) turns the lines into the read record
//! and the attachment index. Nothing inside a notice or a file is read here (root ADR-0484 §1).

mod factoryon;
mod ilis;
mod kicox;
mod lh;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use collection_application::ports::{BronzeIngestUnitOfWork, CompleteIngestionRunCommand};
use collection_application::{
    plan_public_data_bulk_file, BronzeCommitter, BronzePayload, PlannedBronzeObject,
    PublicDataBulkFileIdentity, PublicDataBulkFilePlan, PublicDataBulkFilePlanInput,
};
use collection_domain::{
    build_bronze_content_object_key, source_slug, BronzeObject, IngestionRun, IngestionRunStatus,
    IngestionTrigger, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat,
};
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use foundation_shared_kernel::ObjectKeyPrefix;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::time::Instant;
use uuid::Uuid;

use crate::bronze_object_storage::{
    live_write_bronze_object_storage_from_env, live_write_target_preflight,
    BronzeObjectStorageWriter,
};
use crate::industrial_complex_notice_collect::{
    gzip_lines, media_prefix, store_attachment, AttachmentReport, StoreOutcome,
};
use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{optional_env_value, required_env_value};

/// `infra/lakehouse/contracts/industrial-land-sale-notice.contract.json`, as compiled in.
pub(crate) const CONTRACT_JSON: &str =
    include_str!("../../../infra/lakehouse/contracts/industrial-land-sale-notice.contract.json");
const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_LAND_SALE_NOTICE";
pub(crate) const USER_AGENT: &str =
    "foundation-platform-industrial-land-sale-notice/1.0 (+https://github.com/perfectory)";
const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";
const BRONZE_COMMITTER: BronzeCommitter = BronzeCommitter::new();
/// The politeness floor of every source here (the ILIS collectors'): the contract may only slow it.
const MIN_REQUEST_SPACING: Duration = Duration::from_secs(2);
/// What the run prints before its summary when the provider refuses the lane's access itself (LH
/// before the owner's data.go.kr usage application): the script ends the run with 78 (EX_CONFIG).
pub(crate) const REFUSED_MARKER: &str = "industrial-land-sale-notice-refused";

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct LaneContract {
    pub(crate) request_spacing_ms: u64,
    pub(crate) per_run_limit: usize,
    pub(crate) commit_every: usize,
    pub(crate) run_time_budget_seconds: u64,
    pub(crate) stop_after_consecutive_failures: u32,
    pub(crate) retry_after_hours: i64,
    pub(crate) full_walk_hour_utc: u32,
    pub(crate) read_from: String,
    pub(crate) attachments: LaneAttachments,
    pub(crate) sources: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct LaneAttachments {
    pub(crate) object_prefix_from: String,
    pub(crate) max_file_bytes: u64,
}

pub(crate) fn lane_contract() -> anyhow::Result<LaneContract> {
    serde_json::from_str(CONTRACT_JSON)
        .context("industrial-land-sale-notice.contract.json is not valid")
}

/// The source a run collects.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Factoryon,
    Ilis,
    Kicox,
    Lh,
}

impl Source {
    pub(crate) fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim() {
            "factoryon" => Ok(Self::Factoryon),
            "ilis" => Ok(Self::Ilis),
            "kicox" => Ok(Self::Kicox),
            "lh" => Ok(Self::Lh),
            other => bail!("{PREFIX}_SOURCE is {other:?}; one of factoryon, ilis, kicox, lh"),
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Factoryon => "factoryon",
            Self::Ilis => "ilis",
            Self::Kicox => "kicox",
            Self::Lh => "lh",
        }
    }
}

/// What every source declares beside its own list and detail rules.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SourceCommon {
    pub(crate) provider: String,
    pub(crate) detail_dataset_slug: String,
}

impl LaneContract {
    pub(crate) fn source_json(&self, source: Source) -> anyhow::Result<&JsonValue> {
        self.sources
            .get(source.name())
            .with_context(|| format!("the lane contract has no source {}", source.name()))
    }

    pub(crate) fn common(&self, source: Source) -> anyhow::Result<SourceCommon> {
        serde_json::from_value(self.source_json(source)?.clone()).with_context(|| {
            format!(
                "source {} lacks provider or detail_dataset_slug",
                source.name()
            )
        })
    }

    /// A source's own value of a lane setting, or the lane's (Factory-on is asked slower, longer
    /// and never walked whole again, root ADR-0603 §8).
    fn own_u64(&self, source: Source, field: &str, lane: u64) -> anyhow::Result<u64> {
        Ok(self.source_json(source)?[field].as_u64().unwrap_or(lane))
    }

    pub(crate) fn spacing_ms(&self, source: Source) -> anyhow::Result<u64> {
        self.own_u64(source, "request_spacing_ms", self.request_spacing_ms)
    }

    pub(crate) fn run_limit(&self, source: Source) -> anyhow::Result<usize> {
        Ok(usize::try_from(self.own_u64(
            source,
            "per_run_limit",
            self.per_run_limit as u64,
        )?)?)
    }

    pub(crate) fn budget_seconds(&self, source: Source) -> anyhow::Result<u64> {
        self.own_u64(
            source,
            "run_time_budget_seconds",
            self.run_time_budget_seconds,
        )
    }

    /// Whether the daily full walk is the source's (`full_walk`, default yes).
    pub(crate) fn walks_whole(&self, source: Source) -> anyhow::Result<bool> {
        Ok(self.source_json(source)?["full_walk"]
            .as_bool()
            .unwrap_or(true))
    }

    /// How many list pages a run walks at least (the recent window a provider changes most).
    pub(crate) fn recent_pages(&self, source: Source) -> anyhow::Result<u32> {
        self.source_json(source)?["list"]["recent_pages"]
            .as_u64()
            .and_then(|pages| u32::try_from(pages).ok())
            .filter(|pages| *pages > 0)
            .with_context(|| format!("source {} names no list.recent_pages", source.name()))
    }

    /// Whether the source's list answers are kept in Bronze (`list.keep_in_bronze`).
    pub(crate) fn keeps_list(&self, source: Source) -> anyhow::Result<bool> {
        self.source_json(source)?["list"]["keep_in_bronze"]
            .as_bool()
            .with_context(|| format!("source {} names no list.keep_in_bronze", source.name()))
    }

    /// The list fields whose values make a listed notice's fingerprint.
    pub(crate) fn fingerprint_fields(&self, source: Source) -> anyhow::Result<Vec<String>> {
        serde_json::from_value(self.source_json(source)?["list"]["fingerprint_fields"].clone())
            .with_context(|| format!("source {} names no list.fingerprint_fields", source.name()))
    }
}

/// A value another contract owns, named `<file>#<pointer>` (the ILIS base URL is the ILIS
/// contract's: one host, one declaration).
pub(crate) fn pointed_text(reference: &str) -> anyhow::Result<String> {
    let (file, pointer) = reference
        .split_once('#')
        .context("a reference is <file>#<pointer>")?;
    let contract: JsonValue = match file {
        "industryland-or-kr.contract.json" => {
            serde_json::from_str(crate::industrial_complex_notice_collect::CONTRACT_JSON)?
        }
        other => bail!("{other} is not a contract this lane reads"),
    };
    contract
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .with_context(|| format!("{file} has no string at {pointer}"))
}

/// A source's base URL: its own `base_url`, or the one `base_url_from` names.
pub(crate) fn base_url_of(source: &JsonValue) -> anyhow::Result<String> {
    if let Some(reference) = source["base_url_from"].as_str() {
        return pointed_text(reference);
    }
    source["base_url"]
        .as_str()
        .map(str::to_owned)
        .context("the source names neither base_url nor base_url_from")
}

/// How the provider is asked: LH through the platform's data.go.kr service key, the others openly.
pub(crate) const fn auth_of(source: Source) -> SourceAuthKind {
    match source {
        Source::Lh => SourceAuthKind::ServiceKey,
        Source::Factoryon | Source::Ilis | Source::Kicox => SourceAuthKind::NoAuth,
    }
}

/// How one notice read ended. `Read` and `Gone` settle it; the others are asked again after
/// `retry_after_hours`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadOutcome {
    /// The detail was read and every attachment settled.
    Read,
    /// The provider says the notice is not there (a 4xx, or ILIS's `code` 4xx with no result):
    /// asking again will not bring it back.
    Gone,
    /// The detail was read but an attachment failed for a reason a later run can get past.
    Incomplete,
    /// The detail itself was not read.
    Failed,
}

impl ReadOutcome {
    const fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Gone => "gone",
            Self::Incomplete => "incomplete",
            Self::Failed => "failed",
        }
    }
}

/// One notice as its list states it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Listed {
    /// The source's own id of the notice (ILIS `par_seq`, KICOX `bbsSeq`, LH `PAN_ID`).
    pub(crate) id: String,
    /// The list row as listed, kept in the line.
    pub(crate) row: JsonValue,
}

/// One page of a source's list, newest first.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ListPage {
    pub(crate) rows: Vec<Listed>,
    /// How many notices the provider says the whole list holds, when it says.
    pub(crate) total: Option<u64>,
    /// No page follows this one.
    pub(crate) last: bool,
}

/// How one attachment is asked for.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FileRequest {
    /// The ILIS download path with this query (the ILIS client builds the same URL).
    IlisQuery(Vec<(String, String)>),
    /// A plain GET of this URL.
    Url(String),
}

/// One attachment a detail lists.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FileRef {
    pub(crate) title: Option<String>,
    pub(crate) provider_file_id: Option<String>,
    pub(crate) provider_file_size: Option<i64>,
    /// `None`: the listing names no file to ask for.
    pub(crate) request: Option<FileRequest>,
    /// The URL recorded beside the bytes.
    pub(crate) source_url: Option<String>,
    /// The provider marks it withdrawn: not asked for.
    pub(crate) withdrawn: bool,
}

/// What asking for one detail came to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DetailRead {
    Found(Detail),
    /// The provider says the notice is not there, with the status it said it with.
    Gone(u16),
}

/// One detail as read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Detail {
    pub(crate) http_status: Option<u16>,
    /// The answer as served (a JSON document, or `{"html": …}` for a page).
    pub(crate) body: JsonValue,
    pub(crate) files: Vec<FileRef>,
}

/// The provider refused the collector or answered a page that is not the one asked for (a 4xx,
/// 429 or a block page; Factory-on's client answers so): the run commits what it read, asks
/// nothing more and fails, so it is seen (root ADR-0603 §8).
#[derive(Debug)]
pub(crate) struct Blocked(pub(crate) String);

impl std::fmt::Display for Blocked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Blocked {}

/// Why a run ended before its list did, if it did.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stop {
    /// `run_time_budget_seconds` passed; the next run continues.
    TimeBudget,
    /// `stop_after_consecutive_failures` details failed in a row, or a later list page failed.
    SiteFailing,
    /// The provider refused the collector ([`Blocked`]): the run fails.
    Blocked,
}

/// What a download came to.
pub(crate) enum Downloaded {
    Bytes(Vec<u8>),
    /// The provider answered 4xx: the file is gone and asking again will not bring it back.
    Gone(u16),
}

/// The client of one source.
pub(crate) enum SourceClient {
    Factoryon(Box<factoryon::Factoryon>),
    Ilis(Box<ilis::Ilis>),
    Kicox(Box<kicox::Kicox>),
    Lh(Box<lh::Lh>),
}

impl SourceClient {
    async fn page(&mut self, page_no: u32) -> anyhow::Result<(ListPage, Option<Vec<u8>>)> {
        match self {
            Self::Factoryon(client) => client.page(page_no).await.map(|page| (page, None)),
            Self::Ilis(client) => client.page(page_no).await,
            Self::Kicox(client) => client.page(page_no).await.map(|page| (page, None)),
            Self::Lh(client) => client.page(page_no).await,
        }
    }

    async fn detail(&mut self, listed: &Listed) -> anyhow::Result<DetailRead> {
        match self {
            Self::Factoryon(client) => client.detail(listed).await,
            Self::Ilis(client) => client.detail(listed).await,
            Self::Kicox(client) => client.detail(listed).await,
            Self::Lh(client) => client.detail(listed).await,
        }
    }

    async fn download(&mut self, request: &FileRequest) -> anyhow::Result<Downloaded> {
        match self {
            Self::Factoryon(client) => client.download(request).await,
            Self::Ilis(client) => client.download(request).await,
            Self::Kicox(client) => client.download(request).await,
            Self::Lh(client) => client.download(request).await,
        }
    }
}

/// The hash of the listed values of `fields`, in that order (a missing field is `null`): what a
/// read is recorded against.
pub(crate) fn fingerprint(row: &JsonValue, fields: &[String]) -> String {
    let values = fields
        .iter()
        .map(|field| row.get(field).cloned().unwrap_or(JsonValue::Null))
        .collect::<Vec<_>>();
    let canonical = JsonValue::Array(values).to_string();
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

/// The last read of one notice: the fingerprint it was read with, how it ended and when (its run).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LastRead {
    pub(crate) print: String,
    pub(crate) settled: bool,
    pub(crate) run: Option<NaiveDateTime>,
}

/// The last read of each notice of one source, from rows of `read_from`. `read` and `gone` settle a
/// notice; any other outcome leaves it owed.
pub(crate) fn last_reads<'a>(
    rows: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
    source: Source,
) -> HashMap<String, LastRead> {
    let mut last: HashMap<String, ((String, i64), LastRead)> = HashMap::new();
    for row in rows {
        if row.get("notice_source").and_then(JsonValue::as_str) != Some(source.name()) {
            continue;
        }
        let (Some(id), Some(print), Some(snapshot)) = (
            row.get("notice_id").and_then(JsonValue::as_str),
            row.get("list_fingerprint").and_then(JsonValue::as_str),
            row.get("source_snapshot_id").and_then(JsonValue::as_str),
        ) else {
            continue;
        };
        let order = (
            snapshot.to_owned(),
            row.get("source_line_number")
                .and_then(JsonValue::as_i64)
                .unwrap_or(0),
        );
        let settled = matches!(
            row.get("read_outcome").and_then(JsonValue::as_str),
            Some("read" | "gone")
        );
        let read = LastRead {
            print: print.to_owned(),
            settled,
            run: NaiveDateTime::parse_from_str(snapshot, "%Y%m%dT%H%M%SZ").ok(),
        };
        match last.get(id) {
            Some((seen, _)) if *seen >= order => {}
            _ => {
                last.insert(id.to_owned(), (order, read));
            }
        }
    }
    last.into_iter().map(|(id, (_, read))| (id, read)).collect()
}

/// Whether a listed notice is owed this run: never read, read with another list fingerprint (the
/// provider changed it in its list), or last read unsettled at least `retry_after_hours` ago (a
/// notice that keeps failing is asked once a day, not every run ahead of the backlog).
pub(crate) fn owes(
    last: Option<&LastRead>,
    print: &str,
    now: DateTime<Utc>,
    retry_after_hours: i64,
) -> bool {
    let Some(last) = last else {
        return true;
    };
    if last.print != print {
        return true;
    }
    if last.settled {
        return false;
    }
    last.run
        .is_none_or(|run| run.and_utc() + chrono::Duration::hours(retry_after_hours) <= now)
}

/// Whether the list walk may stop at a page that owes nothing past the recent pages: every notice
/// the provider says it lists has been asked at least once (a notice asked and still failing does
/// not hold the walk open; one the provider took down may end the first fill early, and the daily
/// full walk reaches the rest). A provider that states no count is taken to list nothing beyond.
pub(crate) fn walk_done(asked_notices: usize, total: Option<u64>) -> bool {
    total.is_none_or(|total| asked_notices as u64 >= total)
}

/// Reads the last reads from the lakehouse (`read_from`'s current snapshot); none when the table
/// holds no snapshot yet. A failure fails the run: it never asks blind.
async fn last_reads_from_lakehouse(
    contract: &LaneContract,
    source: Source,
) -> anyhow::Result<HashMap<String, LastRead>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_INDUSTRIAL_LAND_SALE_NOTICE_READ;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    ensure!(
        contract.read_from == SILVER_INDUSTRIAL_LAND_SALE_NOTICE_READ.table_name,
        "read_from names {}, the collector reads {}",
        contract.read_from,
        SILVER_INDUSTRIAL_LAND_SALE_NOTICE_READ.table_name
    );
    // The unit holds only operations-map.env's read-only token (root ADR-0247), as the ILIS notice
    // collector.
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let Some(snapshot) = catalog
        .load_current_snapshot_manifest_list(&contract.read_from)
        .await
        .with_context(|| format!("failed to resolve {}", contract.read_from))?
    else {
        return Ok(HashMap::new());
    };
    let lakehouse = LakehouseObjectReader::from_env()?;
    let scanned = scan_snapshot_rows(
        &SILVER_INDUSTRIAL_LAND_SALE_NOTICE_READ,
        &lakehouse,
        &snapshot,
    )
    .await?;
    Ok(last_reads(scanned.rows.iter(), source))
}

/// What a run needs to ask: the source's client, the pace and the count of requests sent.
pub(crate) struct Asker {
    pub(crate) client: SourceClient,
    pub(crate) spacing: ProviderRequestSpacing,
    pub(crate) requests: usize,
}

impl Asker {
    async fn pace(&mut self) {
        self.spacing.wait_before_request(self.requests).await;
        self.requests += 1;
    }
}

/// Reads one notice: its detail, then each attachment, annotated in the line's `attachments`
/// (`file_sequence`, `file_title`, `provider_file_id`, `provider_file_size`, `source_url`,
/// `store_outcome`, and for fetched bytes `sha256`, `media_format`, `size_bytes`,
/// `fetched_at_utc`). Returns its Bronze line.
async fn read_notice(
    run: &RunContext<'_>,
    asker: &mut Asker,
    listed: &Listed,
    print: &str,
    report: &mut AttachmentReport,
) -> anyhow::Result<(ReadOutcome, JsonValue)> {
    let requested_at = Utc::now();
    asker.pace().await;
    let detail = match asker.client.detail(listed).await {
        Ok(DetailRead::Found(detail)) => detail,
        Ok(DetailRead::Gone(status)) => {
            return Ok((
                ReadOutcome::Gone,
                line(
                    run.source,
                    listed,
                    print,
                    requested_at,
                    Some(status),
                    ReadOutcome::Gone,
                    None,
                    JsonValue::Null,
                    Vec::new(),
                ),
            ));
        }
        // The provider refused the lane itself: no read is written, the run ends (78 for LH, a
        // failure for a blocked source).
        Err(error) if error.downcast_ref::<lh::Refused>().is_some() || is_blocked(&error) => {
            return Err(error);
        }
        Err(error) => {
            return Ok((
                ReadOutcome::Failed,
                line(
                    run.source,
                    listed,
                    print,
                    requested_at,
                    None,
                    ReadOutcome::Failed,
                    Some(&format!("{error:#}")),
                    JsonValue::Null,
                    Vec::new(),
                ),
            ));
        }
    };
    let mut outcome = ReadOutcome::Read;
    let mut files = Vec::with_capacity(detail.files.len());
    for (index, file_ref) in detail.files.iter().enumerate() {
        let mut file = json!({
            "file_sequence": index + 1,
            "file_title": file_ref.title,
            "provider_file_id": file_ref.provider_file_id,
            "provider_file_size": file_ref.provider_file_size,
            "source_url": file_ref.source_url,
        });
        let stored = read_attachment(run, asker, file_ref, &mut file, report).await?;
        if stored == StoreOutcome::Failed {
            outcome = ReadOutcome::Incomplete;
        }
        file["store_outcome"] = json!(stored);
        files.push(file);
    }
    Ok((
        outcome,
        line(
            run.source,
            listed,
            print,
            requested_at,
            detail.http_status,
            outcome,
            None,
            detail.body,
            files,
        ),
    ))
}

async fn read_attachment(
    run: &RunContext<'_>,
    asker: &mut Asker,
    file_ref: &FileRef,
    file: &mut JsonValue,
    report: &mut AttachmentReport,
) -> anyhow::Result<StoreOutcome> {
    report.listed += 1;
    if file_ref.withdrawn {
        report.withdrawn += 1;
        return Ok(StoreOutcome::Withdrawn);
    }
    let Some(request) = &file_ref.request else {
        return Ok(StoreOutcome::NoReference);
    };
    asker.pace().await;
    let fetched_at = Utc::now();
    let bytes = match asker.client.download(request).await {
        Ok(Downloaded::Bytes(bytes)) => bytes,
        Err(error) if is_blocked(&error) => return Err(error),
        Ok(Downloaded::Gone(status)) => {
            report.gone += 1;
            file["http_status"] = json!(status);
            return Ok(StoreOutcome::Gone);
        }
        Err(error) => {
            report.failed += 1;
            file["error"] = json!(format!("{error:#}"));
            return Ok(StoreOutcome::Failed);
        }
    };
    store_attachment(
        &bytes,
        file,
        fetched_at,
        run.prefix,
        run.storage,
        run.contract.attachments.max_file_bytes,
        report,
    )
    .await
}

fn line(
    source: Source,
    listed: &Listed,
    print: &str,
    requested_at: DateTime<Utc>,
    http_status: Option<u16>,
    outcome: ReadOutcome,
    error: Option<&str>,
    body: JsonValue,
    attachments: Vec<JsonValue>,
) -> JsonValue {
    json!({
        "notice_source": source.name(),
        "notice_id": listed.id,
        "list_fingerprint": print,
        "list_row": listed.row,
        "requested_at_utc": requested_at.to_rfc3339(),
        "http_status": http_status,
        "read_outcome": outcome.name(),
        "error": error,
        "body": body,
        "attachments": attachments,
    })
}

/// What one run shares between its steps.
struct RunContext<'a> {
    contract: &'a LaneContract,
    source: Source,
    prefix: &'a ObjectKeyPrefix,
    storage: Option<&'a dyn ObjectStorageService>,
}

/// One Bronze object's plan: `<dataset>-<run>-<batch>` (or the list's name), content-qualified.
pub(crate) fn plan(
    provider: &str,
    dataset_slug: &str,
    operation: &str,
    file_id: &str,
    file_name: &str,
    bytes: Vec<u8>,
    content_type: &str,
    run: IngestionRunId,
    at: DateTime<Utc>,
) -> anyhow::Result<PublicDataBulkFilePlan> {
    let mut plan = plan_public_data_bulk_file(PublicDataBulkFilePlanInput {
        source_slug: &source_slug(provider, dataset_slug)?,
        ingest_date: at.date_naive(),
        ingestion_run_id: run,
        identity: PublicDataBulkFileIdentity {
            operation: operation.to_owned(),
            provider_file_period: None,
            provider_snapshot_date: Some(at.date_naive()),
            provider_file_id: file_id.to_owned(),
            provider_file_name: file_name.to_owned(),
            provider_updated_at: None,
        },
        raw_payload: bytes,
        content_type: content_type.to_owned(),
    })
    .with_context(|| format!("failed to plan the Bronze object for {dataset_slug}"))?;
    plan.object_key = build_bronze_content_object_key(&plan.object_key, &plan.checksum_sha256)
        .with_context(|| format!("failed to content-qualify {}", plan.object_key.as_str()))?;
    Ok(plan)
}

/// The id of a detail batch object: `<dataset with dashes>-<run>-<batch>`.
pub(crate) fn batch_file_id(dataset_slug: &str, at: DateTime<Utc>, batch: usize) -> String {
    format!(
        "{}-{}-{batch:03}",
        dataset_slug.replace('_', "-"),
        at.format("%Y%m%dt%H%M%Sz")
    )
}

/// The id of a list answer object: `<dataset with dashes>-<page>`; its key adds the answer's hash,
/// so the same page answered the same is the same object.
pub(crate) fn list_file_id(dataset_slug: &str, page_no: u32) -> String {
    format!("{}-{page_no:04}", dataset_slug.replace('_', "-"))
}

/// What a commit wrote: its key and whether this run recorded it (a list answer equal to an earlier
/// one is the same object, and is not this run's).
struct Committed {
    object_key: String,
    new: bool,
}

/// Commits `plan` as one Bronze object of the source `entry` names, in an ingestion run of its own.
async fn persist(
    entry: &SourceCatalogEntry,
    plan: &PublicDataBulkFilePlan,
    request_params: JsonValue,
    records: usize,
    at: DateTime<Utc>,
    uow: &dyn BronzeIngestUnitOfWork,
    storage: &dyn ObjectStorageService,
) -> anyhow::Result<Committed> {
    let writer = BronzeObjectStorageWriter::new(storage);
    let source = uow
        .upsert_source_catalog_entry(entry)
        .await
        .with_context(|| format!("failed to upsert the source {}", entry.slug))?;
    let run = uow
        .create_ingestion_run(&IngestionRun {
            id: IngestionRunId::new(Uuid::new_v4()),
            source_catalog_id: source.id,
            trigger: IngestionTrigger::Scheduled,
            status: IngestionRunStatus::Running,
            request_params,
            started_at: at,
            finished_at: None,
            logical_records_seen: 0,
            objects_written: 0,
            error_message: None,
            created_at: at,
            updated_at: at,
            version: 1,
        })
        .await
        .with_context(|| format!("failed to create the {} run", entry.slug))?;
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
                    logical_record_count: u64::try_from(records).ok(),
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
    uow.complete_ingestion_run(CompleteIngestionRunCommand {
        id: run.id,
        status: IngestionRunStatus::Succeeded,
        finished_at: Utc::now(),
        logical_records_seen: u64::try_from(records).unwrap_or(u64::MAX),
        objects_written: 1,
        error_message: None,
    })
    .await
    .with_context(|| format!("failed to complete the {} run", entry.slug))?;
    let row = uow
        .find_bronze_object_by_object_key(source.id, plan.object_key.as_str())
        .await
        .with_context(|| {
            format!(
                "failed to read the ledger row of {}",
                plan.object_key.as_str()
            )
        })?
        .with_context(|| {
            format!(
                "the ledger holds no row for the committed {}",
                plan.object_key.as_str()
            )
        })?;
    Ok(Committed {
        object_key: plan.object_key.as_str().to_owned(),
        new: row.ingestion_run_id == run.id,
    })
}

fn source_catalog_entry(
    provider: &str,
    dataset_slug: &str,
    name: &str,
    base_url: &str,
    auth_kind: SourceAuthKind,
    job: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(provider, dataset_slug)?,
        name: name.to_owned(),
        provider: provider.to_owned(),
        dataset_name: dataset_slug.to_owned(),
        base_url: Some(base_url.to_owned()),
        auth_kind,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!(
            "on the {job} schedule (orchestration/jobs.v1.json)"
        )),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}

struct Config {
    source: Source,
    live_write: bool,
    output_dir: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            source: Source::parse(&required_env_value(&format!("{PREFIX}_SOURCE"))?)?,
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

/// The client of `source`, configured from the lane contract (and, for LH, the service key).
fn client_for(contract: &LaneContract, source: Source) -> anyhow::Result<SourceClient> {
    let json = contract.source_json(source)?;
    Ok(match source {
        Source::Factoryon => SourceClient::Factoryon(Box::new(factoryon::Factoryon::new(json)?)),
        Source::Ilis => SourceClient::Ilis(Box::new(ilis::Ilis::new(json)?)),
        Source::Kicox => SourceClient::Kicox(Box::new(kicox::Kicox::new(json)?)),
        Source::Lh => SourceClient::Lh(Box::new(lh::Lh::new(json, Utc::now().date_naive())?)),
    })
}

/// The ledger and the Bronze store a live run writes to.
type Live = Option<(PgBronzeIngestUnitOfWork, Box<dyn ObjectStorageService>)>;

/// The store a live run writes the attachments to; none in a dry run.
fn storage_of(live: &Live) -> Option<&dyn ObjectStorageService> {
    live.as_ref().map(|(_, storage)| storage.as_ref())
}

/// Collects the owed notices of one source and their attachments (root ADR-0603). The last stdout
/// lines are `industrial-land-sale-notice-summary-json {json}` and `foundation-job-outcome
/// changed|unchanged`.
///
/// # Errors
/// The first list page could not be read, the lakehouse's last reads could not be read, a Bronze
/// commit failed, or every detail asked failed.
pub async fn run() -> anyhow::Result<()> {
    let contract = lane_contract()?;
    let config = Config::from_env()?;
    let source = config.source;
    let common = contract.common(source)?;
    let fields = contract.fingerprint_fields(source)?;
    let prefix = media_prefix(&contract.attachments.object_prefix_from)?;
    let spacing = ProviderRequestSpacing::try_new(
        Duration::from_millis(contract.spacing_ms(source)?).max(MIN_REQUEST_SPACING),
    )?;
    let started_at = Utc::now();
    let deadline = Instant::now() + Duration::from_secs(contract.budget_seconds(source)?);
    let per_run_limit = contract.run_limit(source)?;
    let recent_pages = contract.recent_pages(source)?;
    let keeps_list = contract.keeps_list(source)?;
    // Once a day the whole list is walked, so a change the provider makes deep in its list is seen
    // (a run of the other hours walks the recent pages and the backlog).
    let full_walk =
        contract.walks_whole(source)? && started_at.hour() == contract.full_walk_hour_utc;
    let last = last_reads_from_lakehouse(&contract, source).await?;
    let settled_before = last.values().filter(|read| read.settled).count();

    let live: Live = if config.live_write {
        live_write_target_preflight()
            .context("sale notice collection live-write target preflight failed")?;
        let pool = PgPool::connect(&required_env_value("DATABASE_URL")?)
            .await
            .context("failed to connect to the database for the sale notice collection")?;
        let storage = live_write_bronze_object_storage_from_env()
            .await
            .context("failed to configure object storage for the sale notice collection")?;
        Some((PgBronzeIngestUnitOfWork::new(pool), storage))
    } else {
        None
    };
    let mut asker = Asker {
        client: client_for(&contract, source)?,
        spacing,
        requests: 0,
    };
    let job = format!("industrial_land_sale_notice_{}_collect", source.name());
    let base_url = base_url_of(contract.source_json(source)?)?;
    let auth = auth_of(source);
    let detail_entry = source_catalog_entry(
        &common.provider,
        &common.detail_dataset_slug,
        &format!("산업용지 분양공고 상세·첨부 ({})", source.name()),
        &base_url,
        auth,
        &job,
        started_at,
    )?;

    let mut report = AttachmentReport::default();
    let mut counts = BTreeMap::<&str, u64>::new();
    let mut objects = Vec::new();
    let mut new_objects = 0_usize;
    let mut list_objects: Vec<Committed> = Vec::new();
    let mut batch = Vec::new();
    let mut consecutive_failures = 0_u32;
    let mut stop = None;
    let mut asked = 0_usize;
    let mut listed_total = None;
    let mut pages = 0_u32;
    let mut asked_ever: HashSet<String> = last.keys().cloned().collect();
    let mut blocked = None;
    'walk: for page_no in 1_u32.. {
        if Instant::now() >= deadline {
            stop = Some(Stop::TimeBudget);
            break;
        }
        asker.pace().await;
        let (page, raw_list) = match asker.client.page(page_no).await {
            Ok(page) => page,
            Err(error) if is_blocked(&error) => {
                blocked = Some(format!("{error:#}"));
                stop = Some(Stop::Blocked);
                break;
            }
            Err(error) => {
                // A refusal, or the first page failing, ends the run: nothing to walk.
                if refused(&error) || page_no == 1 {
                    return Err(error.context(format!("failed to read the {} list", source.name())));
                }
                // A later page failing ends the walk; what was read is kept, the next run continues.
                eprintln!(
                    "the {} list page {page_no} failed: {error:#}",
                    source.name()
                );
                stop = Some(Stop::SiteFailing);
                break;
            }
        };
        pages = page_no;
        listed_total = page.total.or(listed_total);
        if let (true, Some(raw), Some((uow, storage))) = (keeps_list, raw_list, live.as_ref()) {
            list_objects.push(
                persist_list(
                    &contract,
                    source,
                    page_no,
                    raw,
                    started_at,
                    uow,
                    storage.as_ref(),
                )
                .await?,
            );
        }
        let mut owed_here = 0_usize;
        for listed in &page.rows {
            let print = fingerprint(&listed.row, &fields);
            if !owes(
                last.get(&listed.id),
                &print,
                started_at,
                contract.retry_after_hours,
            ) {
                continue;
            }
            owed_here += 1;
            if asked >= per_run_limit {
                break 'walk;
            }
            if Instant::now() >= deadline {
                stop = Some(Stop::TimeBudget);
                break 'walk;
            }
            if consecutive_failures >= contract.stop_after_consecutive_failures {
                stop = Some(Stop::SiteFailing);
                break 'walk;
            }
            let run = RunContext {
                contract: &contract,
                source,
                prefix: &prefix,
                storage: storage_of(&live),
            };
            let (outcome, line) =
                match read_notice(&run, &mut asker, listed, &print, &mut report).await {
                    Ok(read) => read,
                    Err(error) if is_blocked(&error) => {
                        blocked = Some(format!("{error:#}"));
                        stop = Some(Stop::Blocked);
                        break 'walk;
                    }
                    Err(error) => {
                        refused(&error);
                        return Err(error);
                    }
                };
            asked += 1;
            asked_ever.insert(listed.id.clone());
            consecutive_failures = if outcome == ReadOutcome::Failed {
                consecutive_failures + 1
            } else {
                0
            };
            *counts.entry(outcome.name()).or_default() += 1;
            batch.push(line);
            if batch.len() >= contract.commit_every {
                let committed = flush(
                    &config,
                    &common,
                    &detail_entry,
                    &live,
                    &batch,
                    started_at,
                    objects.len(),
                )
                .await?;
                new_objects += usize::from(committed.new);
                objects.push(committed.object_key);
                batch.clear();
            }
        }
        let walked_enough =
            page_no >= recent_pages && owed_here == 0 && walk_done(asked_ever.len(), listed_total);
        if page.last || (!full_walk && walked_enough) {
            break;
        }
    }
    if !batch.is_empty() {
        let committed = flush(
            &config,
            &common,
            &detail_entry,
            &live,
            &batch,
            started_at,
            objects.len(),
        )
        .await?;
        new_objects += usize::from(committed.new);
        objects.push(committed.object_key);
    }
    let list_changed = list_objects.iter().any(|committed| committed.new);

    let summary = json!({
        "schema_version": "foundation-platform.industrial_land_sale_notice_collect.v1",
        "source": source,
        "started_at_utc": started_at.to_rfc3339(),
        "live_write": config.live_write,
        "list_pages": pages,
        "full_walk": full_walk,
        "listed_total": listed_total,
        "settled_before": settled_before,
        "asked_ever": asked_ever.len(),
        "asked": asked,
        "notices": counts,
        "attachments": report,
        "requests_sent": asker.requests,
        "stopped": stop,
        "blocked": blocked,
        "objects": objects,
        "list_objects": list_objects
            .iter()
            .map(|committed| json!({"key": committed.object_key, "new": committed.new}))
            .collect::<Vec<_>>(),
    });
    println!("industrial-land-sale-notice-summary-json {summary}");
    if let Some(reason) = blocked {
        bail!(
            "{} refused the collector ({reason}): stopped at once, nothing more asked (root ADR-0603 §8)",
            common.provider
        );
    }
    let failed = counts.get("failed").copied().unwrap_or(0);
    if asked > 0 && failed == asked as u64 {
        bail!("every one of the {asked} notice details asked failed: the site is not answering");
    }
    println!(
        "foundation-job-outcome {}",
        if config.live_write && (new_objects > 0 || list_changed) {
            "changed"
        } else {
            "unchanged"
        }
    );
    Ok(())
}

/// Whether `error` is a source refusing the collector ([`Blocked`]).
fn is_blocked(error: &anyhow::Error) -> bool {
    error.downcast_ref::<Blocked>().is_some()
}

/// Whether `error` is the provider refusing the lane itself; if so the run prints the marker its
/// script ends with 78 on, before anything else is written.
fn refused(error: &anyhow::Error) -> bool {
    let Some(refusal) = error.downcast_ref::<lh::Refused>() else {
        return false;
    };
    println!("{REFUSED_MARKER} {refusal}");
    true
}

/// Commits one list page's answer as served, under its content-qualified name: the same answer
/// again is the same object (root ADR-0152), so only a page the provider changed is a new one.
async fn persist_list(
    contract: &LaneContract,
    source: Source,
    page_no: u32,
    raw: Vec<u8>,
    at: DateTime<Utc>,
    uow: &PgBronzeIngestUnitOfWork,
    storage: &dyn ObjectStorageService,
) -> anyhow::Result<Committed> {
    let json = contract.source_json(source)?;
    let common = contract.common(source)?;
    let list = &json["list"];
    let dataset = list["dataset_slug"]
        .as_str()
        .context("a list kept in Bronze names its dataset_slug")?;
    let operation = list["operation"]
        .as_str()
        .context("the list names its operation")?;
    let file_id = list_file_id(dataset, page_no);
    let run = IngestionRunId::new(Uuid::new_v4());
    let plan = plan(
        &common.provider,
        dataset,
        operation,
        &file_id,
        &format!("{file_id}.json"),
        raw,
        "application/json",
        run,
        at,
    )?;
    let entry = source_catalog_entry(
        &common.provider,
        dataset,
        &format!("산업용지 분양공고 목록 ({})", source.name()),
        &base_url_of(json)?,
        auth_of(source),
        &format!("industrial_land_sale_notice_{}_collect", source.name()),
        at,
    )?;
    persist(
        &entry,
        &plan,
        json!({"operation": operation}),
        1,
        at,
        uow,
        storage,
    )
    .await
}

async fn flush(
    config: &Config,
    common: &SourceCommon,
    entry: &SourceCatalogEntry,
    live: &Live,
    lines: &[JsonValue],
    at: DateTime<Utc>,
    batch: usize,
) -> anyhow::Result<Committed> {
    let file_id = batch_file_id(&common.detail_dataset_slug, at, batch);
    let plan = plan(
        &common.provider,
        &common.detail_dataset_slug,
        "detail",
        &file_id,
        &format!("{file_id}.gz"),
        gzip_lines(lines)?,
        "application/gzip",
        IngestionRunId::new(Uuid::new_v4()),
        at,
    )?;
    if let Some((uow, storage)) = live {
        return persist(
            entry,
            &plan,
            json!({"operation": "detail", "source": config.source, "batch": batch}),
            lines.len(),
            at,
            uow,
            storage.as_ref(),
        )
        .await;
    }
    // A dry run keeps the batch on disk for an operator to read.
    let name = plan
        .object_key
        .as_str()
        .rsplit('/')
        .next()
        .context("a Bronze object key has a leaf")?
        .to_owned();
    std::fs::create_dir_all(config.output_dir.join("objects"))?;
    std::fs::write(
        config.output_dir.join("objects").join(&name),
        &plan.raw_payload,
    )?;
    Ok(Committed {
        object_key: format!("objects/{name}"),
        new: false,
    })
}

/// The date a source counts its history from, as `YYYY-MM-DD`.
pub(crate) fn contract_date(value: &JsonValue, field: &str) -> anyhow::Result<NaiveDate> {
    let text = value[field]
        .as_str()
        .with_context(|| format!("the contract names no {field}"))?;
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .with_context(|| format!("{field} {text:?} is not YYYY-MM-DD"))
}

#[cfg(test)]
mod tests;
