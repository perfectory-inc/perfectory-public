//! 법원경매 물건상세, the run's second half (root ADR-0290 §7, ADR-0352, ADR-0384).
//!
//! After a complete night of the list searches, the same collector asks the 물건상세 of the
//! properties due, with the same manners (one connection, the spacing floor, the named User-Agent,
//! the window, stop on any refusal). Which properties is computed each run, never stored
//! (root ADR-0352): this run's in-progress list rows against the last read the lakehouse records
//! (`silver.court_auction_item_detail`), never-read first, then what the contract's recrawl rule
//! says is due, soonest sale date first, at most the contract's per-run limit.
//!
//! The response carries the case's photos as base64 (`csPicLst[].picFile`, 99.5% of its bytes).
//! Each is decoded and stored once in the shared content-addressed media store
//! (`onbid_media::store_bytes`, root ADR-0298); the Bronze line keeps the response with the value
//! replaced by `sha256:<hex>`, so the line and the store together give the response back.

use std::collections::{BTreeMap, HashMap};

use anyhow::Context as _;
use base64::Engine as _;
use chrono::{DateTime, NaiveDate, Utc};
use collection_domain::court_auction::{due, fingerprint, CaseView, RecrawlRule};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{MediaFormat, ObjectKeyPrefix};
use reqwest::header::{ACCEPT, CONTENT_TYPE, RETRY_AFTER};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest as _, Sha256};

use super::{Collector, Search, SourceContract, Stop, KST_HOURS};
use crate::onbid_media::{store_bytes, Stored};

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PhotoFields {
    pub(crate) list_field: String,
    pub(crate) file_field: String,
    pub(crate) object_prefix_from: String,
    pub(crate) max_file_bytes: u64,
}

/// The contract's `item_detail` (root ADR-0384).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ItemDetail {
    pub(crate) dataset_slug: String,
    pub(crate) operation: String,
    pub(crate) path: String,
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
            conditions: Vec::new(),
            date_window: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ChangeFields {
    pub(crate) item_search: Vec<String>,
}

/// The parts of the contract's `collection` the detail phase reads (root ADR-0290 §2-§3).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CollectionRules {
    pub(crate) change_fields: ChangeFields,
    pub(crate) recrawl: RecrawlRule,
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
    rules: &CollectionRules,
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
        LakehouseCatalogConfig::from_env().context("failed to configure the Iceberg catalog")?,
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
}

/// Rewrites one response's photos in place: each `file_field` value is decoded, stored once (when
/// `storage` is given) and replaced by `sha256:<hex>`, with `sha256`, `media_format` and
/// `size_bytes` beside it. A value that does not decode, is over the cap or is not an image is
/// left out of the store and of the line (`null`), and counted. Returns how many photos it has.
pub(crate) async fn rewrite_photos(
    result: &mut JsonValue,
    fields: &PhotoFields,
    prefix: &ObjectKeyPrefix,
    storage: Option<&dyn ObjectStorageService>,
    report: &mut PhotoReport,
) -> anyhow::Result<u64> {
    let Some(photos) = result
        .get_mut(&fields.list_field)
        .and_then(JsonValue::as_array_mut)
    else {
        return Ok(0);
    };
    let mut count = 0;
    for photo in photos.iter_mut() {
        let Some(encoded) = photo.get(&fields.file_field).and_then(JsonValue::as_str) else {
            continue;
        };
        count += 1;
        report.photos += 1;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .ok();
        let format = bytes.as_deref().and_then(MediaFormat::sniff);
        let (Some(bytes), Some(format)) = (bytes, format) else {
            report.not_stored += 1;
            photo[fields.file_field.as_str()] = JsonValue::Null;
            continue;
        };
        if bytes.len() as u64 > fields.max_file_bytes {
            report.not_stored += 1;
            photo[fields.file_field.as_str()] = JsonValue::Null;
            continue;
        }
        let sha256 = hex(&Sha256::digest(&bytes));
        if let Some(storage) = storage {
            match store_bytes(&bytes, prefix, storage, fields.max_file_bytes).await? {
                Stored::New(media) => {
                    report.stored += 1;
                    report.bytes_stored += media.size_bytes;
                }
                Stored::Deduplicated(_) => report.deduplicated += 1,
                Stored::Oversize | Stored::UnknownFormat | Stored::Gone => {
                    report.not_stored += 1;
                    photo[fields.file_field.as_str()] = JsonValue::Null;
                    continue;
                }
            }
        }
        photo[fields.file_field.as_str()] = json!(format!("sha256:{sha256}"));
        photo["sha256"] = json!(sha256);
        photo["media_format"] = json!(format.extension());
        photo["size_bytes"] = json!(bytes.len() as u64);
    }
    Ok(count)
}

/// What one detail answer came to.
pub(crate) enum Answer {
    Result(JsonValue),
    /// The site refused: the run stops.
    Refused(String),
    /// Not a refusal, and no result either: the read is recorded as failed and the run goes on.
    Failed(String),
}

/// Reads a detail response: the refusal signals of the list searches, then the result object.
pub(crate) fn read_answer(
    contract: &SourceContract,
    detail: &ItemDetail,
    status: u16,
    retry_after: bool,
    content_type: &str,
    text: &str,
) -> Answer {
    if contract.refusal.http_statuses.contains(&status) {
        return Answer::Refused(format!("HTTP {status}"));
    }
    if retry_after {
        return Answer::Refused("Retry-After".to_owned());
    }
    if !(200..300).contains(&status) {
        return Answer::Refused(format!("HTTP {status}"));
    }
    if !content_type.contains("json") {
        return Answer::Refused(format!("a {content_type:?} page where JSON belongs"));
    }
    let Ok(value) = serde_json::from_str::<JsonValue>(text) else {
        return Answer::Refused("a body that is not JSON".to_owned());
    };
    if value["data"]["ipcheck"] == JsonValue::Bool(false) {
        return Answer::Refused("ipcheck=false".to_owned());
    }
    match value.pointer(&detail.result_pointer) {
        Some(JsonValue::Object(_)) => Answer::Result(value),
        _ => Answer::Failed(format!("no object at {}", detail.result_pointer)),
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

impl Collector<'_> {
    /// POSTs one property's 물건상세 and returns (status, retry-after, content type, body).
    async fn detail_response(
        &mut self,
        detail: &ItemDetail,
        body: &JsonValue,
    ) -> anyhow::Result<(u16, bool, String, String)> {
        let response = self
            .send(
                |http, url| {
                    http.post(url)
                        .header(CONTENT_TYPE, "application/json;charset=UTF-8")
                        .header(ACCEPT, "application/json")
                        .json(body)
                },
                &detail.path,
            )
            .await?;
        let status = response.status().as_u16();
        let retry_after = response.headers().contains_key(RETRY_AFTER);
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        let text = response.text().await.unwrap_or_default();
        Ok((status, retry_after, content_type, text))
    }
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

/// Asks the 물건상세 of `asked`, in order, stopping at the first refusal or the end of the window.
/// `now` and `allowed` are asked before every request, as the list searches do.
pub(crate) async fn collect_details(
    contract: &SourceContract,
    collector: &mut Collector<'_>,
    asked: &[Wanted],
    store: &PhotoStore<'_>,
    now: impl Fn() -> DateTime<Utc>,
    allowed: impl Fn(DateTime<Utc>) -> anyhow::Result<bool>,
) -> anyhow::Result<(DetailResult, Option<Stop>)> {
    let detail = &contract.item_detail;
    let mut result = DetailResult::default();
    for wanted in asked {
        let at = now();
        if collector.quiet_hours_waived.is_none() && !allowed(at)? {
            return Ok((result, Some(Stop::QuietHoursEnded)));
        }
        let body = request_body(detail, &wanted.key);
        let (status, retry_after, content_type, text) =
            collector.detail_response(detail, &body).await?;
        result.asked += 1;
        let mut line = json!({
            "operation": detail.operation,
            "request": body,
            "requested_at_utc": at.to_rfc3339(),
            "http_status": status,
            "list_fingerprint": wanted.fingerprint,
        });
        match read_answer(contract, detail, status, retry_after, &content_type, &text) {
            Answer::Refused(reason) => {
                line["read_outcome"] = json!("refused");
                result.lines.push(serde_json::to_string(&line)?);
                return Ok((result, Some(Stop::Refused(reason))));
            }
            Answer::Failed(reason) => {
                result.failed += 1;
                line["read_outcome"] = json!("failed");
                line["failure"] = json!(reason);
                line["body"] = json!(text);
            }
            Answer::Result(mut value) => {
                let photo_count = match value.pointer_mut(&detail.result_pointer) {
                    Some(found) => {
                        rewrite_photos(
                            found,
                            &detail.photos,
                            &store.prefix,
                            store.storage,
                            &mut result.photos,
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
        result.lines.push(serde_json::to_string(&line)?);
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
        // The bound the contract measured against the unit's 300 minutes (root ADR-0384).
        assert_eq!(detail.per_run_limit, 1800);
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
        let count = rewrite_photos(&mut result, fields, &prefix, None, &mut report)
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

    #[test]
    fn a_detail_answer_without_its_result_is_a_failure_and_ipcheck_is_a_refusal() {
        let contract = contract();
        let detail = &contract.item_detail;
        let read =
            |text: &str| read_answer(&contract, detail, 200, false, "application/json", text);
        assert!(matches!(
            read(r#"{"data": {"dma_result": {}}}"#),
            Answer::Result(_)
        ));
        assert!(matches!(read(r#"{"data": {}}"#), Answer::Failed(_)));
        assert!(matches!(
            read(r#"{"data": {"ipcheck": false}}"#),
            Answer::Refused(_)
        ));
        assert!(matches!(
            read_answer(&contract, detail, 429, false, "application/json", "{}"),
            Answer::Refused(_)
        ));
    }
}
