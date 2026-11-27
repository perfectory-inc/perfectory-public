//! 법원경매 매각물건명세서 PDF, once per version (root ADR-0575 §1-§3).
//!
//! Which versions is computed each run (root ADR-0352): the 물건상세 this run read whose
//! specification the site's own screen would open, less the versions
//! (`silver.court_auction_sale_specification_read`) already stored or gone, soonest opening first,
//! at most the contract's per-run limit. A version is the specification's document id: the court
//! rewrites a specification under a new id, and one id is asked once, so the court's view log (the
//! log step) is written once per version.
//!
//! One version is three requests, as the screen and its viewer make them: the log step on the site's
//! session pool, then the viewer's info and download with our own name. The watermark asked for
//! names no one. The PDF names people, so it is stored under the contract's own prefix, which the
//! parcel edge does not serve. A CAPTCHA or login page ends the phase for the run; it is never
//! solved or bypassed. Each version asked is one Bronze line: its key, outcome and stored hash.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::{ask, Asked, PaceGate, SiteMethod, SiteRequest};
use foundation_shared_kernel::ObjectKeyPrefix;
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::time::Instant;

use super::{may_ask, Asking, Search, Stop};
use crate::media_store::{spool_response, store_spooled_document, MediaStore, Stored};

/// When the screen opens a specification: `date_field` less `days_before`, for one bid kind.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Opening {
    pub(crate) bid_kind: String,
    pub(crate) date_field: String,
    pub(crate) days_before: i64,
}

/// The site's log step: its answer names the viewer and the parameter it opens with.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct LogStep {
    pub(crate) path: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) request_object: String,
    pub(crate) request_constants: BTreeMap<String, String>,
    pub(crate) result_pointer: String,
    pub(crate) url_field: String,
    pub(crate) param_field: String,
}

/// The court's document viewer.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Viewer {
    pub(crate) base_url: String,
    pub(crate) info_path: String,
    pub(crate) info_request_object: String,
    pub(crate) info_constants: BTreeMap<String, String>,
    pub(crate) download_path: String,
    pub(crate) download_request_object: String,
    pub(crate) download_constants: BTreeMap<String, String>,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) watermark_template: String,
}

/// The contract's `sale_specifications`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SaleSpecifications {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) pace_group: String,
    pub(crate) spec_object: String,
    /// The key's name in the Bronze line and Silver -> the field of `spec_object` it is read from.
    pub(crate) key_fields: BTreeMap<String, String>,
    pub(crate) document_id_field: String,
    pub(crate) written_field: String,
    pub(crate) orv_field: String,
    pub(crate) real_kind_field: String,
    pub(crate) real_kind_value: String,
    pub(crate) bid_kind_field: String,
    pub(crate) opens: Vec<Opening>,
    pub(crate) log: LogStep,
    pub(crate) viewer: Viewer,
    pub(crate) user_agent: String,
    pub(crate) captcha_marker: String,
    pub(crate) reads_from: String,
    pub(crate) object_prefix: String,
    pub(crate) max_file_bytes: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) per_run_limit: u32,
    pub(crate) concurrency: usize,
    pub(crate) run_time_budget_seconds: u64,
}

impl SaleSpecifications {
    /// The specifications as the Bronze persist step names a search.
    pub(crate) fn as_search(&self) -> Search {
        Search {
            dataset_slug: self.dataset_slug.clone(),
            operation: self.operation.clone(),
            path: self.log.path.clone(),
            page_size: 0,
            pace_group: self.pace_group.clone(),
            headers: self.log.headers.clone(),
            conditions: Vec::new(),
            date_window: None,
        }
    }

    /// The prefix the PDFs are stored under.
    pub(crate) fn prefix(&self) -> anyhow::Result<ObjectKeyPrefix> {
        ObjectKeyPrefix::parse(&self.object_prefix)
            .context("sale_specifications.object_prefix is not a valid object key prefix")
    }

    /// The watermark the download is asked with: the template, filled with the document's written
    /// time (`HHMM`).
    pub(crate) fn watermark(&self, written_hm: &str) -> String {
        let (hh, mm) = (
            written_hm.get(..2).unwrap_or("00"),
            written_hm.get(2..4).unwrap_or("00"),
        );
        self.viewer
            .watermark_template
            .replace("{hh}", hh)
            .replace("{mm}", mm)
    }
}

/// One specification version this run may ask: its key, its document id and what the log step
/// sends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WantedSpec {
    pub(crate) court_office_code: String,
    pub(crate) case_no: String,
    pub(crate) item_seq: String,
    pub(crate) document_id: String,
    pub(crate) written: Option<String>,
    pub(crate) orv_param: String,
    /// The first day the screen opens it.
    pub(crate) opens_on: NaiveDate,
}

impl WantedSpec {
    fn version(&self) -> (&str, &str, &str, &str) {
        (
            &self.court_office_code,
            &self.case_no,
            &self.item_seq,
            &self.document_id,
        )
    }
}

fn text(value: Option<&JsonValue>) -> Option<String> {
    match value? {
        JsonValue::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        JsonValue::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn ymd(value: Option<&JsonValue>) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(&text(value)?, "%Y%m%d").ok()
}

/// The version a 물건상세 result offers, when the screen would show its specification button: a
/// real property, an `orvParam` and a document id, and an opening rule for its bid kind.
pub(crate) fn spec_of(specs: &SaleSpecifications, result: &JsonValue) -> Option<WantedSpec> {
    let spec = result.get(&specs.spec_object)?.as_object()?;
    if text(spec.get(&specs.real_kind_field)).as_deref() != Some(specs.real_kind_value.as_str()) {
        return None;
    }
    let bid_kind = text(spec.get(&specs.bid_kind_field))?;
    let opens_on = specs
        .opens
        .iter()
        .filter(|opening| opening.bid_kind == bid_kind)
        .filter_map(|opening| {
            ymd(spec.get(&opening.date_field))?
                .checked_sub_signed(chrono::Duration::days(opening.days_before))
        })
        .min()?;
    let key = |name: &str| {
        specs
            .key_fields
            .get(name)
            .and_then(|field| text(spec.get(field)))
    };
    Some(WantedSpec {
        court_office_code: key("court_office_code")?,
        case_no: key("case_no")?,
        item_seq: key("item_seq")?,
        document_id: text(spec.get(&specs.document_id_field))?,
        written: text(spec.get(&specs.written_field)),
        orv_param: text(spec.get(&specs.orv_field))?,
        opens_on,
    })
}

/// The versions this run asks: those of `offered` open on `today` that `reads` does not record as
/// stored or gone, each version once, soonest opening first, then by key; at most `limit`. The same
/// inputs always give the same list (root ADR-0352 §1).
pub(crate) fn frontier<'a>(
    offered: impl IntoIterator<Item = WantedSpec>,
    reads: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
    today: NaiveDate,
    limit: u32,
) -> Vec<WantedSpec> {
    let done: HashSet<(String, String, String, String)> = reads
        .into_iter()
        .filter(|row| {
            matches!(
                row.get("outcome").and_then(JsonValue::as_str),
                Some("stored" | "gone")
            )
        })
        .filter_map(|row| {
            Some((
                text(row.get("court_office_code"))?,
                text(row.get("case_no"))?,
                text(row.get("item_seq"))?,
                text(row.get("document_id"))?,
            ))
        })
        .collect();
    let mut seen = HashSet::new();
    let mut wanted: Vec<WantedSpec> = offered
        .into_iter()
        .filter(|spec| spec.opens_on <= today)
        .filter(|spec| {
            let (court, case, item, document) = spec.version();
            !done.contains(&(
                court.to_owned(),
                case.to_owned(),
                item.to_owned(),
                document.to_owned(),
            ))
        })
        .filter(|spec| {
            let (court, case, item, document) = spec.version();
            seen.insert((
                court.to_owned(),
                case.to_owned(),
                item.to_owned(),
                document.to_owned(),
            ))
        })
        .collect();
    wanted.sort_by(|a, b| (a.opens_on, a.version()).cmp(&(b.opens_on, b.version())));
    wanted.truncate(limit as usize);
    wanted
}

/// Reads the version record the frontier needs from the lakehouse (its current snapshot; a table
/// with none yet holds no row).
pub(crate) async fn reads_from_lakehouse(
    specs: &SaleSpecifications,
) -> anyhow::Result<Vec<serde_json::Map<String, JsonValue>>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_COURT_AUCTION_SALE_SPECIFICATION_READ as READ;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    anyhow::ensure!(
        specs.reads_from == READ.table_name,
        "sale_specifications names {}, the collector reads {}",
        specs.reads_from,
        READ.table_name
    );
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let Some(snapshot) = catalog
        .load_current_snapshot_manifest_list(&specs.reads_from)
        .await
        .with_context(|| format!("failed to resolve {}", specs.reads_from))?
    else {
        return Ok(Vec::new());
    };
    let lakehouse = LakehouseObjectReader::from_env()?;
    Ok(scan_snapshot_rows(&READ, &lakehouse, &snapshot).await?.rows)
}

/// What one version came to.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    /// Stored now, or already in the store (same bytes).
    Stored,
    /// A CAPTCHA or login page: skipped, never solved; the phase stops for the run.
    Captcha,
    /// The log step or the viewer named no document.
    NoDocument,
    /// The viewer answered 4xx: asking again will not bring it back soon.
    Gone,
    /// The file is not a PDF the store takes (over the cap, or not a known format).
    NotStored,
    /// A network failure or 5xx: asked again next run.
    Failed,
}

/// What the specifications of one run came to.
#[derive(Debug, Default, serde::Serialize)]
pub(crate) struct SpecsResult {
    #[serde(skip)]
    pub(crate) lines: Vec<String>,
    pub(crate) asked: u32,
    pub(crate) stored: u32,
    pub(crate) deduplicated: u32,
    pub(crate) captcha: u32,
    pub(crate) failed: u32,
    pub(crate) bytes_stored: u64,
}

/// The run-wide state of the asking.
pub(crate) struct SpecAsking<'a> {
    pub(crate) specs: &'a SaleSpecifications,
    pub(crate) session_headers: &'a BTreeMap<String, String>,
    pub(crate) gate: &'a PaceGate,
    pub(crate) http: reqwest::Client,
    pub(crate) prefix: ObjectKeyPrefix,
    pub(crate) storage: Option<MediaStore<'a>>,
    pub(crate) captcha_seen: AtomicBool,
}

enum Fetched {
    Line {
        line: String,
        outcome: Outcome,
        stored: Option<Stored>,
    },
    Stopped(Stop),
}

/// The log step's body: the key, the document id and `orvParam`, as the screen sends them.
pub(crate) fn log_body(specs: &SaleSpecifications, spec: &WantedSpec) -> JsonValue {
    let mut object = serde_json::Map::new();
    for (name, field) in &specs.key_fields {
        let value = match name.as_str() {
            "court_office_code" => &spec.court_office_code,
            "case_no" => &spec.case_no,
            _ => &spec.item_seq,
        };
        object.insert(field.clone(), json!(value));
    }
    object.insert(specs.orv_field.clone(), json!(spec.orv_param));
    object.insert(specs.document_id_field.clone(), json!(spec.document_id));
    for (field, value) in &specs.log.request_constants {
        object.insert(field.clone(), json!(value));
    }
    json!({ specs.log.request_object.as_str(): object })
}

/// The download's body from the viewer's info answer (`data`), as the viewer's screen builds it
/// (SGVO201M01 `docMerge_setReqParam`): the record of `document_id`, its case and court, and the
/// contract's watermark. `None` when the answer names no such record.
pub(crate) fn download_body(
    specs: &SaleSpecifications,
    data: &JsonValue,
    document_id: &str,
) -> Option<(JsonValue, JsonValue)> {
    let case = data.get("dma_csInf")?;
    let records = data.get("dlt_rcrd")?.as_array()?;
    let record = records
        .iter()
        .find(|record| text(record.get("ecdocId")).as_deref() == Some(document_id))
        .or_else(|| records.first())?;
    let field = |value: &JsonValue, name: &str| text(value.get(name)).unwrap_or_default();
    let written_hm = field(record, "kofileCrtrHm");
    let mut object = serde_json::Map::new();
    object.insert("csNo".to_owned(), json!(field(case, "userCsNo")));
    object.insert("cortNm".to_owned(), json!(field(case, "cortNm")));
    object.insert("ofdocNm".to_owned(), json!(field(record, "ofdocNm")));
    object.insert("outputFileNm".to_owned(), json!(field(record, "docNm")));
    object.insert("wmTxt".to_owned(), json!(specs.watermark(&written_hm)));
    object.insert(
        "purposeCd".to_owned(),
        json!(text(data.pointer("/purposeData/purposeCd")).unwrap_or_else(|| "1".to_owned())),
    );
    object.insert(
        "ecdocLst".to_owned(),
        json!([{
            "cortCd": field(case, "cortCd"),
            "ecdocId": field(record, "ecdocId"),
            "ecdocDtlSeq": field(record, "ecdocDtlSeq"),
        }]),
    );
    for (name, value) in &specs.viewer.download_constants {
        object.insert(name.clone(), json!(value));
    }
    // What the line keeps of the record: its name and written time, never a person's name.
    let facts = json!({
        "document_name": field(record, "docNm"),
        "document_written_ymd": field(record, "kofileCrtrYmd"),
        "document_written_hm": written_hm,
    });
    Some((
        json!({ specs.viewer.download_request_object.as_str(): object }),
        facts,
    ))
}

async fn post(
    on: &SpecAsking<'_>,
    path: &str,
    body: &JsonValue,
) -> Result<reqwest::Response, String> {
    let url = format!("{}{}", on.specs.viewer.base_url.trim_end_matches('/'), path);
    let mut request = on.http.post(url).json(body);
    for (name, value) in &on.specs.viewer.headers {
        request = request.header(name, value);
    }
    request
        .send()
        .await
        .map_err(|error| format!("POST failed: {error}"))
}

/// One version: the log step, the viewer's info, the file.
async fn fetch_one(
    on: &SpecAsking<'_>,
    run: &Asking<'_>,
    deadline: Instant,
    spec: &WantedSpec,
) -> anyhow::Result<Option<(Outcome, Option<Stored>, JsonValue)>> {
    let specs = on.specs;
    let mut facts = json!({});
    let request = SiteRequest {
        method: SiteMethod::Post,
        path: specs.log.path.clone(),
        headers: on
            .session_headers
            .iter()
            .chain(&specs.log.headers)
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        body: Some(log_body(specs, spec)),
    };
    let reply = match ask(
        run.sessions,
        on.gate,
        &run.rules,
        &run.tally,
        &request,
        deadline,
    )
    .await
    {
        Asked::Unfinished => return Ok(None),
        Asked::Data { reply, .. } => reply,
        Asked::Failed { reason, reply } => {
            facts["log_http_status"] = json!(reply.as_ref().map(|reply| reply.status));
            facts["failure"] = json!(reason);
            return Ok(Some((Outcome::Failed, None, facts)));
        }
    };
    facts["log_http_status"] = json!(reply.status);
    let logged: JsonValue = serde_json::from_str(&reply.body).unwrap_or(JsonValue::Null);
    let opened = logged.pointer(&specs.log.result_pointer);
    let (Some(url), Some(param)) = (
        opened.and_then(|found| text(found.get(&specs.log.url_field))),
        opened.and_then(|found| text(found.get(&specs.log.param_field))),
    ) else {
        return Ok(Some((Outcome::NoDocument, None, facts)));
    };
    if !url.starts_with(specs.viewer.base_url.as_str()) {
        facts["failure"] = json!(format!("the log step names a viewer elsewhere: {url}"));
        return Ok(Some((Outcome::Failed, None, facts)));
    }
    let mut info = serde_json::Map::new();
    info.insert(specs.log.param_field.clone(), json!(param));
    for (name, value) in &specs.viewer.info_constants {
        info.insert(name.clone(), json!(value));
    }
    let info_body = json!({ specs.viewer.info_request_object.as_str(): info });
    let answer = match post(on, &specs.viewer.info_path, &info_body).await {
        Ok(response) => response,
        Err(reason) => {
            facts["failure"] = json!(reason);
            return Ok(Some((Outcome::Failed, None, facts)));
        }
    };
    let status = answer.status();
    facts["info_http_status"] = json!(status.as_u16());
    if status.is_client_error() {
        return Ok(Some((Outcome::Gone, None, facts)));
    }
    let page = answer.text().await.unwrap_or_default();
    if page.to_lowercase().contains(&specs.captcha_marker) {
        return Ok(Some((Outcome::Captcha, None, facts)));
    }
    if !status.is_success() {
        return Ok(Some((Outcome::Failed, None, facts)));
    }
    let data = serde_json::from_str::<JsonValue>(&page)
        .ok()
        .and_then(|value| value.get("data").cloned())
        .unwrap_or(JsonValue::Null);
    let Some((download, record)) = download_body(specs, &data, &spec.document_id) else {
        return Ok(Some((Outcome::NoDocument, None, facts)));
    };
    for (name, value) in record.as_object().into_iter().flatten() {
        facts[name] = value.clone();
    }
    let response = match post(on, &specs.viewer.download_path, &download).await {
        Ok(response) => response,
        Err(reason) => {
            facts["failure"] = json!(reason);
            return Ok(Some((Outcome::Failed, None, facts)));
        }
    };
    facts["file_http_status"] = json!(response.status().as_u16());
    if response.status().is_client_error() {
        return Ok(Some((Outcome::Gone, None, facts)));
    }
    if !response.status().is_success() {
        return Ok(Some((Outcome::Failed, None, facts)));
    }
    if response
        .content_length()
        .is_some_and(|len| len > specs.max_file_bytes)
    {
        return Ok(Some((Outcome::NotStored, Some(Stored::Oversize), facts)));
    }
    // The file goes to disk as it arrives and is sent from there: it is not held whole.
    let spooled = match spool_response(response, specs.max_file_bytes).await {
        Ok(spooled) => spooled,
        Err(error) => {
            facts["failure"] = json!(format!("reading the file failed: {error:#}"));
            return Ok(Some((Outcome::Failed, None, facts)));
        }
    };
    let Some(store) = on.storage else {
        facts["media_format"] = json!(spooled.shown.then_some(spooled.format.extension()));
        facts["size_bytes"] = json!(spooled.size_bytes);
        let outcome = if spooled.shown {
            Outcome::Stored
        } else {
            Outcome::NotStored
        };
        return Ok(Some((outcome, None, facts)));
    };
    // A file answer that is not a known format can be the viewer's CAPTCHA or error page.
    if !spooled.shown
        && String::from_utf8_lossy(&spooled.start)
            .to_lowercase()
            .contains(&specs.captcha_marker)
    {
        return Ok(Some((Outcome::Captcha, None, facts)));
    }
    let stored = store_spooled_document(&spooled, &on.prefix, store).await?;
    let outcome = match &stored {
        Stored::New(_) | Stored::Deduplicated(_) => Outcome::Stored,
        Stored::Gone => Outcome::Gone,
        Stored::Oversize | Stored::UnknownFormat => Outcome::NotStored,
    };
    Ok(Some((outcome, Some(stored), facts)))
}

/// Asks one version unless the run must stop.
async fn read_one(
    on: &SpecAsking<'_>,
    run: &Asking<'_>,
    deadline: Instant,
    spec: &WantedSpec,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<Fetched> {
    if Instant::now() >= deadline || on.captcha_seen.load(Ordering::SeqCst) {
        return Ok(Fetched::Stopped(Stop::TimeBudget));
    }
    let at = now();
    if !may_ask(&run.switches, at)? {
        return Ok(Fetched::Stopped(Stop::WindowEnded));
    }
    let Some((outcome, stored, facts)) = fetch_one(on, run, deadline, spec).await? else {
        return Ok(Fetched::Stopped(Stop::TimeBudget));
    };
    if outcome == Outcome::Captcha {
        on.captcha_seen.store(true, Ordering::SeqCst);
        tracing::warn!(
            court = %spec.court_office_code, case = %spec.case_no,
            "courtauction.go.kr sale specification: the viewer asked for a CAPTCHA or a login; skipped, the phase stops for the run"
        );
    }
    let media = match &stored {
        Some(Stored::New(media) | Stored::Deduplicated(media)) => Some(media),
        _ => None,
    };
    let line = json!({
        "operation": on.specs.operation,
        "request": {
            "court_office_code": spec.court_office_code,
            "case_no": spec.case_no,
            "item_seq": spec.item_seq,
            "document_id": spec.document_id,
            "written_date": spec.written,
        },
        "requested_at_utc": at.to_rfc3339(),
        "outcome": outcome,
        "sha256": media.map(|media| media.sha256.clone()),
        "media_format": media.map(|media| media.media_format.clone()),
        "size_bytes": media.map(|media| media.size_bytes),
        "facts": facts,
    });
    Ok(Fetched::Line {
        line: serde_json::to_string(&line)?,
        outcome,
        stored,
    })
}

/// Asks `wanted` until `deadline`, `concurrency` at a time, and keeps the lines in the order asked.
pub(crate) async fn collect_specs(
    on: &SpecAsking<'_>,
    run: &Asking<'_>,
    deadline: Instant,
    wanted: &[WantedSpec],
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(SpecsResult, Option<Stop>)> {
    // Built here, run only as the stream polls them (as `detail::collect_details` notes).
    let reads: Vec<_> = wanted
        .iter()
        .map(|spec| read_one(on, run, deadline, spec, &now))
        .collect();
    let mut answers = stream::iter(reads).buffered(on.specs.concurrency.max(1));
    let mut result = SpecsResult::default();
    let mut stop = None;
    while let Some(read) = answers.next().await {
        match read? {
            Fetched::Stopped(why) => {
                stop.get_or_insert(why);
            }
            Fetched::Line {
                line,
                outcome,
                stored,
            } => {
                result.asked += 1;
                match (&outcome, &stored) {
                    (Outcome::Stored, Some(Stored::New(media))) => {
                        result.stored += 1;
                        result.bytes_stored += media.size_bytes;
                    }
                    (Outcome::Stored, Some(Stored::Deduplicated(_))) => result.deduplicated += 1,
                    (Outcome::Captcha, _) => result.captcha += 1,
                    (Outcome::Failed, _) => result.failed += 1,
                    _ => {}
                }
                result.lines.push(line);
            }
        }
    }
    Ok((result, stop))
}

/// The viewer's client: our own name, the contract's timeout, no cookie.
pub(crate) fn http_client(specs: &SaleSpecifications) -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(specs.user_agent.clone())
        .timeout(Duration::from_secs(specs.request_timeout_seconds))
        .build()
        .context("failed to build the court document viewer HTTP client")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::super::SourceContract;
    use super::*;

    fn contract() -> SourceContract {
        super::super::source_contract().expect("the contract parses")
    }

    fn result(bid_kind: &str, sale: &str, document: &str) -> JsonValue {
        json!({"dspslGdsDxdyInfo": {
            "cortOfcCd": "B999999", "csNo": "20990130099999", "dspslGdsSeq": "1",
            "realMulKind": "1", "bidDvsCd": bid_kind, "dspslDxdyYmd": sale, "bidBgngYmd": "20991201",
            "orvParam": "opaque", "dspslGdsSpcfcEcdocId": document, "gdsSpcfcWrtYmd": "20991026"
        }})
    }

    fn day(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn the_contract_names_the_viewer_and_the_public_document_store() {
        let contract = contract();
        let specs = &contract.sale_specifications;
        assert_eq!(
            specs.reads_from,
            "silver.court_auction_sale_specification_read"
        );
        // Public, as the court lets anyone download them (owner 2026-11-28, root ADR-0626 §6-3).
        let prefix = specs.prefix().expect("prefix");
        assert_eq!(prefix.as_str(), "media");
        assert_eq!(specs.viewer.base_url, "https://ecfs.scourt.go.kr");
    }

    #[test]
    fn a_specification_opens_seven_days_before_its_sale() {
        let contract = contract();
        let specs = &contract.sale_specifications;
        let spec = spec_of(specs, &result("000331", "20991119", "DOC1")).expect("offered");
        assert_eq!(spec.opens_on, day("2099-11-12"));
        assert_eq!(spec.document_id, "DOC1");
        assert_eq!(spec.item_seq, "1");
        // A 기간입찰 opens seven days before its bid start.
        let spec = spec_of(specs, &result("000332", "20991219", "DOC1")).expect("offered");
        assert_eq!(spec.opens_on, day("2099-11-24"));
        // Not a real property, or no document: no button, nothing offered.
        let mut other = result("000331", "20991119", "DOC1");
        other["dspslGdsDxdyInfo"]["realMulKind"] = json!("2");
        assert!(spec_of(specs, &other).is_none());
        assert!(spec_of(specs, &result("000331", "20991119", "")).is_none());
    }

    #[test]
    fn a_version_is_asked_once_and_a_new_document_id_is_a_new_version() {
        let contract = contract();
        let specs = &contract.sale_specifications;
        let offered = vec![
            spec_of(specs, &result("000331", "20991119", "DOC1")).unwrap(),
            spec_of(specs, &result("000331", "20991119", "DOC2")).unwrap(),
            spec_of(specs, &result("000331", "20991230", "DOC3")).unwrap(),
        ];
        let stored = json!({"court_office_code": "B999999", "case_no": "20990130099999",
                            "item_seq": "1", "document_id": "DOC1", "outcome": "stored"});
        let failed = json!({"court_office_code": "B999999", "case_no": "20990130099999",
                            "item_seq": "1", "document_id": "DOC2", "outcome": "failed"});
        let reads = [stored.as_object().unwrap(), failed.as_object().unwrap()];
        let asked = frontier(offered, reads, day("2099-11-13"), 10);
        // DOC1 is stored; DOC2 failed and is asked again; DOC3 is not open yet.
        assert_eq!(
            asked
                .iter()
                .map(|spec| spec.document_id.as_str())
                .collect::<Vec<_>>(),
            ["DOC2"]
        );
    }

    #[test]
    fn the_download_watermark_names_no_one() {
        let contract = contract();
        let specs = &contract.sale_specifications;
        let data = json!({
            "purposeData": {"purposeCd": "1"},
            "dma_csInf": {"userCsNo": "2099타경99999", "cortNm": "가상법원", "cortCd": "999999",
                          "rprsClmntNm": "홍길동"},
            "dlt_rcrd": [{"ecdocId": "DOC1", "ecdocDtlSeq": "1", "docNm": "매각물건명세서", "ofdocNm": "매각물건명세서",
                          "kofileCrtrYmd": "20991026", "kofileCrtrHm": "2359", "cnstrNm": "홍길동"}]
        });
        let (body, facts) = download_body(specs, &data, "DOC1").expect("a body");
        let text = body.to_string() + &facts.to_string();
        assert!(!text.contains("홍길동"), "{text}");
        assert_eq!(
            body["dma_downloadOtpt"]["wmTxt"],
            json!("개인정보유출주의 제출일시: 23:59")
        );
        assert_eq!(
            body["dma_downloadOtpt"]["ecdocLst"][0]["ecdocId"],
            json!("DOC1")
        );
    }

    #[test]
    fn the_log_step_sends_the_key_the_document_and_orv_param() {
        let contract = contract();
        let specs = &contract.sale_specifications;
        let spec = spec_of(specs, &result("000331", "20991119", "DOC1")).unwrap();
        let body = log_body(specs, &spec);
        let log = &body["dma_dspslGdsSpecLog"];
        assert_eq!(log["cortOfcCd"], json!("B999999"));
        assert_eq!(log["dspslGdsSeq"], json!("1"));
        assert_eq!(log["dspslGdsSpcfcEcdocId"], json!("DOC1"));
        assert_eq!(log["orvParam"], json!("opaque"));
        assert_eq!(log["cortAuctnMbrsId"], json!("NONUSER"));
    }
}
