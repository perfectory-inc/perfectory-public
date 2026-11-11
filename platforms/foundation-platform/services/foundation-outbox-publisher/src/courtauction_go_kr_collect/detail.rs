//! 법원경매 물건상세, the run's second half (root ADR-0290 §7, ADR-0352, ADR-0384).
//!
//! After a list pass that was not cut short, the same run asks the 물건상세 of the properties due,
//! on the same session pool and tally as the lists (root ADR-0289): the contract's `goods_detail`
//! pace, a block renews the session and asks again, the operator's window and the run's deadline
//! end it. Which properties is computed each run, never stored
//! (root ADR-0352): this run's in-progress list rows against the last read the lakehouse records
//! (`silver.court_auction_item_detail`), never-read first, then what the contract's recrawl rule
//! says is due, soonest sale date first, at most the contract's per-run limit.
//!
//! The response carries the case's photos as base64 (`csPicLst[].picFile`, 99.5% of its bytes).
//! Each is decoded and stored once in the shared content-addressed media store
//! (`media_store::store_bytes`, root ADR-0298); the Bronze line keeps the response with the value
//! replaced by `sha256:<hex>`, so the line and the store together give the response back. Each
//! stored photo then gets its display ladder (`media_renditions::ensure_ladders`, root ADR-0427 §7)
//! once the night's Bronze has landed.

use std::collections::{BTreeMap, HashMap};

use anyhow::Context as _;
use base64::Engine as _;
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::{ask, Asked, PaceGate, SiteMethod, SiteRequest};
use collection_domain::court_auction::{due, fingerprint, CaseView, RecrawlRule};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{MediaFormat, ObjectKeyPrefix};
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest as _, Sha256};
use tokio::time::Instant;

use super::{may_ask, Asking, Collection, Search, SourceContract, Stop, KST_HOURS};
use crate::media_renditions::RenditionBudget;
use crate::media_store::{store_bytes, Stored};

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PhotoFields {
    pub(crate) list_field: String,
    pub(crate) file_field: String,
    pub(crate) object_prefix_from: String,
    pub(crate) max_file_bytes: u64,
    /// How many of one answer's photos are stored at once (root ADR-0540 §2).
    pub(crate) store_concurrency: usize,
    /// The bound of the night's ladder step (root ADR-0427 §7); what it leaves, the backlog job
    /// takes from the Silver photo index.
    pub(crate) renditions: RenditionBudget,
}

/// The contract's `item_detail` (root ADR-0384).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ItemDetail {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
    /// The `collection.pace` group the details are paced in.
    pub(crate) pace_group: String,
    /// The headers this endpoint is sent besides the session's.
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) request_object: String,
    pub(crate) request_constants: BTreeMap<String, String>,
    /// Request field -> the list row field it is taken from.
    pub(crate) key_fields: BTreeMap<String, String>,
    pub(crate) in_progress_field: String,
    pub(crate) in_progress_value: String,
    pub(crate) sale_date_field: String,
    pub(crate) status_field: String,
    pub(crate) result_pointer: String,
    pub(crate) per_run_limit: u32,
    pub(crate) read_from: String,
    pub(crate) photos: PhotoFields,
}

impl ItemDetail {
    /// The detail as the Bronze persist step names a search: its dataset, operation and path.
    pub(crate) fn as_search(&self) -> Search {
        Search {
            dataset_slug: self.dataset_slug.clone(),
            operation: self.operation.clone(),
            path: self.path.clone(),
            page_size: 0,
            pace_group: self.pace_group.clone(),
            headers: self.headers.clone(),
            conditions: Vec::new(),
            date_window: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ChangeFields {
    pub(crate) item_search: Vec<String>,
}

/// A property of the list: the detail's request key, in the contract's request field order.
pub(crate) type Key = Vec<String>;

/// One in-progress property this run's list holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Wanted {
    pub(crate) key: Key,
    pub(crate) sale_date: Option<NaiveDate>,
    pub(crate) status_code: String,
    pub(crate) fingerprint: String,
}

/// What the lakehouse records of a property's last read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LastRead {
    pub(crate) day: NaiveDate,
    pub(crate) fingerprint: Option<String>,
}

fn text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(text) if !text.trim().is_empty() => Some(text.trim().to_owned()),
        JsonValue::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// The in-progress properties of this run's item-search lines, one per key, the later answer
/// winning (as the Silver load reads them).
pub(crate) fn wanted_from_lines(
    lines: &[String],
    detail: &ItemDetail,
    rules: &Collection,
) -> Vec<Wanted> {
    let mut found: BTreeMap<Key, Wanted> = BTreeMap::new();
    for line in lines {
        let Ok(line) = serde_json::from_str::<JsonValue>(line) else {
            continue;
        };
        if line["http_status"] != json!(200) {
            continue;
        }
        let Some(body) = line["body"]
            .as_str()
            .and_then(|body| serde_json::from_str::<JsonValue>(body).ok())
        else {
            continue;
        };
        for row in body["data"]["dlt_srchResult"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if text(&row[&detail.in_progress_field]).as_deref()
                != Some(detail.in_progress_value.as_str())
            {
                continue;
            }
            let key: Option<Key> = detail
                .key_fields
                .values()
                .map(|field| text(&row[field]))
                .collect();
            let Some(key) = key else {
                continue;
            };
            let sale_date = text(&row[&detail.sale_date_field])
                .and_then(|day| NaiveDate::parse_from_str(&day, "%Y%m%d").ok());
            found.insert(
                key.clone(),
                Wanted {
                    key,
                    sale_date,
                    status_code: text(&row[&detail.status_field]).unwrap_or_default(),
                    fingerprint: hex(&fingerprint(row, &rules.change_fields.item_search)),
                },
            );
        }
    }
    found.into_values().collect()
}

/// The properties this run asks, in order: never read first, then the due ones, each soonest sale
/// date first (an undated one last), then by key; at most `limit`. The same inputs always give the
/// same list (root ADR-0352 §1).
pub(crate) fn frontier(
    wanted: Vec<Wanted>,
    last: &HashMap<Key, LastRead>,
    rule: &RecrawlRule,
    today: NaiveDate,
    limit: u32,
) -> Vec<Wanted> {
    let mut due_now: Vec<(bool, Wanted)> = wanted
        .into_iter()
        .filter_map(|wanted| {
            let read = last.get(&wanted.key);
            let case = CaseView {
                status_code: &wanted.status_code,
                sale_date: wanted.sale_date,
                last_read: read.map(|read| read.day),
                row_changed: read
                    .is_some_and(|read| read.fingerprint.as_deref() != Some(&wanted.fingerprint)),
            };
            due(rule, &case, today).then_some((read.is_some(), wanted))
        })
        .collect();
    due_now.sort_by(|(a_read, a), (b_read, b)| {
        (a_read, a.sale_date.is_none(), a.sale_date, &a.key).cmp(&(
            b_read,
            b.sale_date.is_none(),
            b.sale_date,
            &b.key,
        ))
    });
    due_now
        .into_iter()
        .take(limit as usize)
        .map(|(_, wanted)| wanted)
        .collect()
}

/// The KST day a run id (`20261104T160500Z`) started on.
pub(crate) fn run_day(run_id: &str) -> Option<NaiveDate> {
    let at = DateTime::parse_from_str(&format!("{run_id} +0000"), "%Y%m%dT%H%M%SZ %z").ok()?;
    Some((at.with_timezone(&Utc) + chrono::Duration::hours(KST_HOURS)).date_naive())
}

/// The last read of each property from rows of `silver.court_auction_item_detail`.
pub(crate) fn last_reads_of<'a>(
    rows: impl IntoIterator<Item = &'a serde_json::Map<String, JsonValue>>,
) -> HashMap<Key, LastRead> {
    let mut out: HashMap<Key, (String, LastRead)> = HashMap::new();
    for row in rows {
        let key: Option<Key> = ["court_office_code", "case_no", "property_seq"]
            .iter()
            .map(|column| text(&row[*column]))
            .collect();
        let (Some(key), Some(snapshot)) = (key, text(&row["source_snapshot_id"])) else {
            continue;
        };
        let Some(day) = run_day(&snapshot) else {
            continue;
        };
        let read = LastRead {
            day,
            fingerprint: text(&row["list_fingerprint"]),
        };
        match out.get(&key) {
            Some((seen, _)) if *seen >= snapshot => {}
            _ => {
                out.insert(key, (snapshot, read));
            }
        }
    }
    out.into_iter()
        .map(|(key, (_, read))| (key, read))
        .collect()
}

/// Reads the last reads from the lakehouse (`read_from`'s current snapshot); none when the table
/// holds no snapshot yet.
pub(crate) async fn last_reads_from_lakehouse(
    detail: &ItemDetail,
) -> anyhow::Result<HashMap<Key, LastRead>> {
    use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};
    use lakehouse_domain::SILVER_COURT_AUCTION_ITEM_DETAIL;
    use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

    anyhow::ensure!(
        detail.read_from == SILVER_COURT_AUCTION_ITEM_DETAIL.table_name,
        "item_detail.read_from names {}, the collector reads {}",
        detail.read_from,
        SILVER_COURT_AUCTION_ITEM_DETAIL.table_name
    );
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let Some(snapshot) = catalog
        .load_current_snapshot_manifest_list(&detail.read_from)
        .await
        .with_context(|| format!("failed to resolve {}", detail.read_from))?
    else {
        return Ok(HashMap::new());
    };
    let lakehouse = LakehouseObjectReader::from_env()?;
    let scanned =
        scan_snapshot_rows(&SILVER_COURT_AUCTION_ITEM_DETAIL, &lakehouse, &snapshot).await?;
    Ok(last_reads_of(scanned.rows.iter()))
}

/// The prefix the photos are stored under: the value the contract points at (one store, its
/// writer's contract is its one record, root ADR-0296 §2).
pub(crate) fn photo_prefix(detail: &ItemDetail) -> anyhow::Result<ObjectKeyPrefix> {
    let (file, pointer) = detail
        .photos
        .object_prefix_from
        .split_once('#')
        .context("photos.object_prefix_from is <file>#<pointer>")?;
    anyhow::ensure!(
        file == "data-go-kr-onbid.contract.json",
        "photos.object_prefix_from names {file}; the store's writer contract is data-go-kr-onbid.contract.json"
    );
    let onbid: JsonValue = serde_json::from_str(crate::data_go_kr_onbid_collect::CONTRACT_JSON)
        .context("data-go-kr-onbid.contract.json is not valid")?;
    let prefix = onbid
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .with_context(|| format!("data-go-kr-onbid.contract.json has no string at {pointer}"))?;
    ObjectKeyPrefix::parse(prefix).context("the media prefix is not a valid object key prefix")
}

/// What one run's photos came to.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub(crate) struct PhotoReport {
    pub(crate) photos: u64,
    pub(crate) stored: u64,
    pub(crate) deduplicated: u64,
    pub(crate) bytes_stored: u64,
    pub(crate) not_stored: u64,
    /// Each stored (or already-stored) photo once, `(sha256, media_format)`, in the order read: the
    /// sources of the night's ladder step.
    #[serde(skip)]
    pub(crate) sources: Vec<(String, String)>,
}

impl PhotoReport {
    /// Adds another answer's report to this one.
    fn absorb(&mut self, other: Self) {
        self.photos += other.photos;
        self.stored += other.stored;
        self.deduplicated += other.deduplicated;
        self.bytes_stored += other.bytes_stored;
        self.not_stored += other.not_stored;
        self.sources.extend(other.sources);
    }
}

/// What the store answered for a received photo: what the line names (for a raster no screen reads,
/// its display master with the received file as `source`, root ADR-0503).
#[derive(Clone, Debug)]
pub(crate) struct Named {
    sha256: String,
    media_format: String,
    size_bytes: u64,
    source: Option<crate::media_store::SourceMedia>,
}

/// The photos a run has stored or found stored, by the received bytes' hash: a case's properties
/// answer the same photos, so one already here is neither asked of the store nor a ladder source
/// again (root ADR-0540 §2). Two answers in flight with the same new photo both ask the store, which
/// deduplicates by content.
pub(crate) type SeenPhotos = std::sync::Mutex<HashMap<String, Named>>;

/// Stores one decoded photo, keeping where it sits in the answer.
async fn store_one<'a>(
    index: usize,
    bytes: &[u8],
    received: &'a String,
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
    max_file_bytes: u64,
) -> (usize, &'a String, anyhow::Result<Stored>) {
    let stored = store_bytes(bytes, prefix, storage, max_file_bytes).await;
    (index, received, stored)
}

/// Rewrites one response's photos in place: each `file_field` value is decoded, stored once (when
/// `storage` is given, `fields.store_concurrency` at a time; one in `seen` is not stored again) and
/// replaced by `sha256:<hex>` of what the store holds, with `sha256`, `media_format` and
/// `size_bytes` beside it (and the received file as `source_*`, root ADR-0503). A value that does
/// not decode, is over the cap or is not an image is left out of the store and of the line (`null`),
/// and counted. Returns how many photos it has.
pub(crate) async fn rewrite_photos(
    result: &mut JsonValue,
    fields: &PhotoFields,
    prefix: &ObjectKeyPrefix,
    storage: Option<&dyn ObjectStorageService>,
    seen: &SeenPhotos,
    report: &mut PhotoReport,
) -> anyhow::Result<u64> {
    let Some(photos) = result
        .get_mut(&fields.list_field)
        .and_then(JsonValue::as_array_mut)
    else {
        return Ok(0);
    };
    // Decoded first, in list order: (index, bytes, received hash, format) of every photo that is an
    // image under the cap.
    let mut decoded = Vec::new();
    let mut count = 0;
    for (index, photo) in photos.iter_mut().enumerate() {
        let Some(encoded) = photo.get(&fields.file_field).and_then(JsonValue::as_str) else {
            continue;
        };
        count += 1;
        report.photos += 1;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .ok();
        let format = bytes.as_deref().and_then(MediaFormat::sniff);
        match (bytes, format) {
            (Some(bytes), Some(format)) if bytes.len() as u64 <= fields.max_file_bytes => {
                let received = hex(&Sha256::digest(&bytes));
                decoded.push((index, bytes, received, format));
            }
            _ => {
                report.not_stored += 1;
                photo[fields.file_field.as_str()] = JsonValue::Null;
            }
        }
    }
    // What each photo's line names, by its place in the list; none when the store refused it.
    let mut named: HashMap<usize, Named> = HashMap::new();
    // A photo the answer lists twice is stored once and named from its first place.
    let mut copies: Vec<(usize, usize)> = Vec::new();
    if let Some(storage) = storage {
        let fresh: Vec<_> = {
            let seen = seen
                .lock()
                .map_err(|_| anyhow::anyhow!("the seen photos lock was poisoned"))?;
            let mut first_of: HashMap<&String, usize> = HashMap::new();
            decoded
                .iter()
                .filter(|(index, _, received, _)| {
                    if let Some(known) = seen.get(received) {
                        report.deduplicated += 1;
                        named.insert(*index, known.clone());
                        return false;
                    }
                    if let Some(first) = first_of.get(received) {
                        report.deduplicated += 1;
                        copies.push((*index, *first));
                        return false;
                    }
                    first_of.insert(received, *index);
                    true
                })
                .collect()
        };
        // Built here, run only as the stream polls them: no closure lives in the stream (one did and
        // broke the binary's Send bound, "FnOnce is not general enough", as `collect_details` notes).
        let stores: Vec<_> = fresh
            .iter()
            .map(|(index, bytes, received, _)| {
                store_one(
                    *index,
                    bytes,
                    received,
                    prefix,
                    storage,
                    fields.max_file_bytes,
                )
            })
            .collect();
        let mut stored = stream::iter(stores).buffer_unordered(fields.store_concurrency.max(1));
        let mut done = Vec::new();
        while let Some(outcome) = stored.next().await {
            done.push(outcome);
        }
        // In list order, so the ladder sources keep the order read.
        done.sort_by_key(|(index, ..)| *index);
        for (index, received, stored) in done {
            let media = match stored? {
                Stored::New(media) => {
                    report.stored += 1;
                    report.bytes_stored += media.size_bytes;
                    media
                }
                Stored::Deduplicated(media) => {
                    report.deduplicated += 1;
                    media
                }
                Stored::Oversize | Stored::UnknownFormat | Stored::Gone => continue,
            };
            let entry = Named {
                sha256: media.sha256,
                media_format: media.media_format,
                size_bytes: media.size_bytes,
                source: media.source,
            };
            let first = seen
                .lock()
                .map_err(|_| anyhow::anyhow!("the seen photos lock was poisoned"))?
                .insert(received.clone(), entry.clone())
                .is_none();
            if first {
                report
                    .sources
                    .push((entry.sha256.clone(), entry.media_format.clone()));
            }
            named.insert(index, entry);
        }
        for (index, first) in copies {
            if let Some(entry) = named.get(&first).cloned() {
                named.insert(index, entry);
            }
        }
    } else {
        for (index, bytes, received, format) in &decoded {
            named.insert(
                *index,
                Named {
                    sha256: received.clone(),
                    media_format: format.extension().to_owned(),
                    size_bytes: bytes.len() as u64,
                    source: None,
                },
            );
        }
    }
    for (index, ..) in decoded {
        let photo = &mut photos[index];
        let Some(entry) = named.remove(&index) else {
            report.not_stored += 1;
            photo[fields.file_field.as_str()] = JsonValue::Null;
            continue;
        };
        photo[fields.file_field.as_str()] = json!(format!("sha256:{}", entry.sha256));
        photo["sha256"] = json!(entry.sha256);
        photo["media_format"] = json!(entry.media_format);
        photo["size_bytes"] = json!(entry.size_bytes);
        if let Some(source) = entry.source {
            photo["source_sha256"] = json!(source.sha256);
            photo["source_media_format"] = json!(source.media_format);
            photo["source_size_bytes"] = json!(source.size_bytes);
        }
    }
    Ok(count)
}

/// The detail's result object of an answer the asking took as data, or why there is none.
pub(crate) fn read_result(detail: &ItemDetail, body: &str) -> Result<JsonValue, String> {
    let value: JsonValue =
        serde_json::from_str(body).map_err(|error| format!("a body that is not JSON: {error}"))?;
    match value.pointer(&detail.result_pointer) {
        Some(JsonValue::Object(_)) => Ok(value),
        _ => Err(format!("no object at {}", detail.result_pointer)),
    }
}

/// What the detail phase of one run did.
#[derive(Debug, Default)]
pub(crate) struct DetailResult {
    pub(crate) lines: Vec<String>,
    pub(crate) asked: u32,
    pub(crate) failed: u32,
    pub(crate) photos: PhotoReport,
}

/// The request body of one property.
pub(crate) fn request_body(detail: &ItemDetail, key: &Key) -> JsonValue {
    let mut object = serde_json::Map::new();
    for (field, value) in detail.key_fields.keys().zip(key) {
        object.insert(field.clone(), json!(value));
    }
    for (field, value) in &detail.request_constants {
        object.insert(field.clone(), json!(value));
    }
    object.insert("srchInfo".to_owned(), json!({}));
    json!({ detail.request_object.as_str(): object })
}

/// Where a run's photos go: the shared store's prefix, and the store itself on a live run.
pub(crate) struct PhotoStore<'a> {
    pub(crate) prefix: ObjectKeyPrefix,
    pub(crate) storage: Option<&'a dyn ObjectStorageService>,
}

/// One property's asking, before its answer is read.
enum Sent {
    Answered {
        at: DateTime<Utc>,
        body: JsonValue,
        asked: Asked,
    },
    Stopped(Stop),
}

/// Asks one property's 물건상세 unless the operator's window is shut. `now` is asked when the
/// request is about to go, as the list searches do (a future runs only when polled).
async fn send(
    asking: &Asking<'_>,
    gate: &PaceGate,
    deadline: Instant,
    detail: &ItemDetail,
    headers: &[(String, String)],
    wanted: &Wanted,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
) -> anyhow::Result<Sent> {
    let at = now();
    if !may_ask(&asking.switches, at)? {
        return Ok(Sent::Stopped(Stop::WindowEnded));
    }
    let body = request_body(detail, &wanted.key);
    let request = SiteRequest {
        method: SiteMethod::Post,
        path: detail.path.clone(),
        headers: headers.to_vec(),
        body: Some(body.clone()),
    };
    Ok(
        match ask(
            asking.sessions,
            gate,
            &asking.rules,
            &asking.tally,
            &request,
            deadline,
        )
        .await
        {
            Asked::Unfinished => Sent::Stopped(Stop::TimeBudget),
            asked => Sent::Answered { at, body, asked },
        },
    )
}

/// One property's read, as its Bronze line, or why the run stops.
enum Read {
    Line {
        line: String,
        failed: bool,
        photos: PhotoReport,
    },
    Stopped(Stop),
}

/// Asks one property's 물건상세 and turns the answer into its Bronze line, its photos stored (root
/// ADR-0384 §4) as it goes, so the next answers are asked while these are stored.
#[allow(clippy::too_many_arguments)]
async fn read_one(
    asking: &Asking<'_>,
    gate: &PaceGate,
    deadline: Instant,
    detail: &ItemDetail,
    headers: &[(String, String)],
    wanted: &Wanted,
    now: &(dyn Fn() -> DateTime<Utc> + Sync),
    store: &PhotoStore<'_>,
    seen: &SeenPhotos,
) -> anyhow::Result<Read> {
    let (at, body, asked) = match send(asking, gate, deadline, detail, headers, wanted, now).await?
    {
        Sent::Answered { at, body, asked } => (at, body, asked),
        Sent::Stopped(why) => return Ok(Read::Stopped(why)),
    };
    let mut line = json!({
        "operation": detail.operation,
        "request": body,
        "requested_at_utc": at.to_rfc3339(),
        "list_fingerprint": wanted.fingerprint,
    });
    let read = match asked {
        Asked::Data { reply, .. } => {
            line["http_status"] = json!(reply.status);
            read_result(detail, &reply.body).map_err(|reason| (reason, Some(reply.body)))
        }
        Asked::Failed { reason, reply } => {
            line["http_status"] = json!(reply.as_ref().map(|reply| reply.status));
            Err((reason, reply.map(|reply| reply.body)))
        }
        Asked::Unfinished => return Ok(Read::Stopped(Stop::TimeBudget)),
    };
    let mut photos = PhotoReport::default();
    let failed = read.is_err();
    match read {
        Err((reason, text)) => {
            line["read_outcome"] = json!("failed");
            line["failure"] = json!(reason);
            line["body"] = json!(text);
        }
        Ok(mut value) => {
            let photo_count = match value.pointer_mut(&detail.result_pointer) {
                Some(found) => {
                    rewrite_photos(
                        found,
                        &detail.photos,
                        &store.prefix,
                        store.storage,
                        seen,
                        &mut photos,
                    )
                    .await?
                }
                None => 0,
            };
            line["read_outcome"] = json!("read");
            line["photo_count"] = json!(photo_count);
            line["body"] = json!(serde_json::to_string(&value)?);
        }
    }
    Ok(Read::Line {
        line: serde_json::to_string(&line)?,
        failed,
        photos,
    })
}

/// Asks the 물건상세 of `asked` until `deadline`, as many in flight as the detail's pace group
/// allows, each answer read and its photos stored while the next are asked (root ADR-0540 §2; an
/// answer is a few MB of base64, so only the ones in flight are held), and keeps the lines in the
/// order asked. `now` is asked before every request, as the list searches do.
pub(crate) async fn collect_details(
    contract: &SourceContract,
    asking: &Asking<'_>,
    deadline: Instant,
    asked: &[Wanted],
    store: &PhotoStore<'_>,
    now: impl Fn() -> DateTime<Utc> + Sync,
) -> anyhow::Result<(DetailResult, Option<Stop>)> {
    let detail = &contract.item_detail;
    let rule = contract
        .collection
        .pace
        .get(&detail.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", detail.pace_group))?;
    let gate = asking
        .gates
        .get(&detail.pace_group)
        .with_context(|| format!("no collection.pace group {:?}", detail.pace_group))?;
    let headers: Vec<(String, String)> = contract
        .collection
        .session
        .headers
        .iter()
        .chain(&detail.headers)
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    // Built here, run only as the stream polls them: no closure lives across an await (a closure in
    // the stream broke the binary's Send bound, "FnOnce is not general enough").
    let seen = SeenPhotos::default();
    let reads: Vec<_> = asked
        .iter()
        .map(|wanted| {
            read_one(
                asking, gate, deadline, detail, &headers, wanted, &now, store, &seen,
            )
        })
        .collect();
    let mut answers = stream::iter(reads).buffered(rule.max.max(1) as usize);
    let mut result = DetailResult::default();
    while let Some(read) = answers.next().await {
        match read? {
            Read::Stopped(why) => return Ok((result, Some(why))),
            Read::Line {
                line,
                failed,
                photos,
            } => {
                result.asked += 1;
                result.failed += u32::from(failed);
                result.photos.absorb(photos);
                result.lines.push(line);
            }
        }
    }
    Ok((result, None))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    fn contract() -> SourceContract {
        super::super::source_contract().expect("the contract parses")
    }

    fn row(sa: &str, seq: &str, sale: &str, progress: &str) -> JsonValue {
        json!({"boCd": "B999999", "saNo": sa, "maemulSer": seq, "mokmulSer": "1", "mulJinYn": progress,
               "maeGiil": sale, "jinstatCd": "0002100001", "minmaePrice": "70000000"})
    }

    fn line(rows: Vec<JsonValue>) -> String {
        let body = json!({"data": {"ipcheck": true, "dlt_srchResult": rows}});
        json!({"http_status": 200, "body": body.to_string()}).to_string()
    }

    #[test]
    fn the_contract_names_the_detail_and_its_photo_store() {
        let contract = contract();
        let detail = &contract.item_detail;
        assert_eq!(detail.read_from, "silver.court_auction_item_detail");
        assert_eq!(
            photo_prefix(detail).expect("prefix").as_str(),
            "onbid/media",
            "the shared store the parcel edge serves (root ADR-0296 §5)"
        );
        // A safety bound above every in-progress property: the run's deadline ends the asking
        // (root ADR-0540 §2).
        assert!(detail.per_run_limit >= 40_000);
        assert!(detail.photos.store_concurrency >= 1);
        let body = request_body(
            detail,
            &vec!["B999999".into(), "20990130000001".into(), "1".into()],
        );
        assert_eq!(
            body,
            json!({"dma_srchGdsDtlSrch": {"cortOfcCd": "B999999", "csNo": "20990130000001",
                   "dspslGdsSeq": "1", "pgmId": "PGJ15BM01", "srchInfo": {}}})
        );
    }

    #[test]
    fn only_in_progress_properties_are_wanted_once_each() {
        let contract = contract();
        let lines = [
            line(vec![
                row("20990130000001", "1", "20990115", "Y"),
                row("20990130000002", "1", "20990115", "N"),
            ]),
            line(vec![row("20990130000001", "1", "20990116", "Y")]),
        ];
        let wanted = wanted_from_lines(&lines, &contract.item_detail, &contract.collection);
        assert_eq!(wanted.len(), 1);
        assert_eq!(
            wanted[0].sale_date,
            NaiveDate::from_ymd_opt(2099, 1, 16),
            "the later answer wins"
        );
    }

    #[test]
    fn the_frontier_asks_never_read_first_then_due_soonest_sale_first_and_stops_at_the_limit() {
        let contract = contract();
        let rule = &contract.collection.recrawl;
        let today = NaiveDate::from_ymd_opt(2099, 1, 10).unwrap();
        let wanted = |sa: &str, sale: (u32, u32)| Wanted {
            key: vec!["B999999".into(), sa.into(), "1".into()],
            sale_date: NaiveDate::from_ymd_opt(2099, sale.0, sale.1),
            status_code: "0002100001".into(),
            fingerprint: "f".into(),
        };
        let read_today = |fp: &str| LastRead {
            day: today,
            fingerprint: Some(fp.into()),
        };
        let mut last = HashMap::new();
        last.insert(wanted("READ-SAME", (1, 20)).key, read_today("f"));
        last.insert(wanted("READ-CHANGED", (1, 12)).key, read_today("other"));
        let all = vec![
            wanted("NEW-LATE", (2, 1)),
            wanted("READ-SAME", (1, 20)),
            wanted("NEW-SOON", (1, 11)),
            wanted("READ-CHANGED", (1, 12)),
        ];
        let asked: Vec<String> = frontier(all.clone(), &last, rule, today, 10)
            .into_iter()
            .map(|w| w.key[1].clone())
            .collect();
        assert_eq!(
            asked,
            ["NEW-SOON", "NEW-LATE", "READ-CHANGED"],
            "read and unchanged is not due"
        );
        assert_eq!(
            frontier(all.clone(), &last, rule, today, 2).len(),
            2,
            "the per-run limit holds"
        );
        assert_eq!(
            frontier(all.clone(), &last, rule, today, 10),
            frontier(all, &last, rule, today, 10)
        );
    }

    #[test]
    fn the_last_read_is_the_newest_run_and_its_day_is_kst() {
        let rows: Vec<serde_json::Map<String, JsonValue>> = [
            ("20990101T160500Z", "a"),
            ("20990103T160500Z", "b"),
        ]
        .iter()
        .map(|(snapshot, fp)| {
            json!({"court_office_code": "B999999", "case_no": "20990130000001", "property_seq": 1,
                   "source_snapshot_id": snapshot, "list_fingerprint": fp})
            .as_object()
            .cloned()
            .unwrap()
        })
        .collect();
        let last = last_reads_of(rows.iter());
        let read = &last[&vec![
            "B999999".to_owned(),
            "20990130000001".to_owned(),
            "1".to_owned(),
        ]];
        assert_eq!(
            read.day,
            NaiveDate::from_ymd_opt(2099, 1, 4).unwrap(),
            "16:05 UTC is the next KST day"
        );
        assert_eq!(read.fingerprint.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn a_photo_is_replaced_by_its_hash_and_a_broken_one_is_counted() {
        let contract = contract();
        let fields = &contract.item_detail.photos;
        // A minimal JPEG start (the sniff reads the magic bytes) and a value that is not base64.
        let jpeg =
            base64::engine::general_purpose::STANDARD.encode([0xff_u8, 0xd8, 0xff, 0xe0, 0, 0x10]);
        let mut result = json!({"csPicLst": [
            {"picFile": jpeg, "picTitlNm": "synthetic.jpg", "cortAuctnPicDvsCd": "000241", "cortAuctnPicSeq": "1"},
            {"picFile": "not base64 !", "cortAuctnPicSeq": "2"}
        ]});
        let mut report = PhotoReport::default();
        let prefix = photo_prefix(&contract.item_detail).unwrap();
        let count = rewrite_photos(
            &mut result,
            fields,
            &prefix,
            None,
            &SeenPhotos::default(),
            &mut report,
        )
        .await
        .unwrap();
        assert_eq!(count, 2);
        let first = &result["csPicLst"][0];
        assert!(first["picFile"].as_str().unwrap().starts_with("sha256:"));
        assert_eq!(first["media_format"], json!("jpg"));
        assert_eq!(first["size_bytes"], json!(6));
        assert_eq!(
            result["csPicLst"][1]["picFile"],
            JsonValue::Null,
            "no base64 is kept in Bronze"
        );
        assert_eq!((report.photos, report.not_stored), (2, 1));
    }

    #[tokio::test]
    async fn every_stored_photo_is_a_ladder_source_once_in_the_shared_store() {
        let contract = contract();
        let fields = &contract.item_detail.photos;
        let root = tempfile::tempdir().unwrap();
        let storage =
            foundation_outbox::object_storage::FileObjectStorage::new(root.path()).unwrap();
        let mut pixels = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut pixels, 90)
            .encode(&[90_u8; 8 * 8 * 3], 8, 8, image::ExtendedColorType::Rgb8)
            .unwrap();
        let jpeg = base64::engine::general_purpose::STANDARD.encode(&pixels);
        // The same photo twice in one case (and so in the store once).
        let mut result = json!({"csPicLst": [
            {"picFile": jpeg, "cortAuctnPicSeq": "1"},
            {"picFile": jpeg, "cortAuctnPicSeq": "2"}
        ]});
        let seen = SeenPhotos::default();
        let mut report = PhotoReport::default();
        let prefix = photo_prefix(&contract.item_detail).unwrap();
        rewrite_photos(
            &mut result,
            fields,
            &prefix,
            Some(&storage),
            &seen,
            &mut report,
        )
        .await
        .unwrap();
        assert_eq!((report.stored, report.deduplicated), (1, 1));
        let sha = hex(&Sha256::digest(&pixels));
        assert_eq!(report.sources, vec![(sha.clone(), "jpg".to_owned())]);
        // Another property of the case answers the same photo later in the run: not stored or a
        // ladder source again (root ADR-0540 §2).
        let mut again = json!({"csPicLst": [{"picFile": jpeg, "cortAuctnPicSeq": "1"}]});
        let mut later = PhotoReport::default();
        rewrite_photos(
            &mut again,
            fields,
            &prefix,
            Some(&storage),
            &seen,
            &mut later,
        )
        .await
        .unwrap();
        assert_eq!(
            (later.stored, later.deduplicated, later.sources.len()),
            (0, 1, 0)
        );
        assert_eq!(again["csPicLst"][0]["sha256"], json!(sha.clone()));
        assert!(storage
            .read_object_sha256(&format!("onbid/media/sha256/{sha}.jpg"))
            .await
            .unwrap()
            .is_some());
        // The night's budget is the contract's (root ADR-0427 §7).
        assert_eq!(fields.renditions.per_run_limit, 1500);
    }

    #[test]
    fn a_detail_answer_without_its_result_is_a_failure() {
        let contract = contract();
        let detail = &contract.item_detail;
        assert!(read_result(detail, r#"{"data": {"dma_result": {}}}"#).is_ok());
        assert!(read_result(detail, r#"{"data": {}}"#).is_err());
        assert!(read_result(detail, "not json").is_err());
        assert!(
            contract.collection.pace.contains_key(&detail.pace_group),
            "the detail names a known pace group"
        );
    }
}
