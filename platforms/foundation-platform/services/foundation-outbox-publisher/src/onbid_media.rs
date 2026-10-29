//! 온비드 물건상세(getRlstDtlInf2)의 사진·감정평가서 바이너리를 공용 content-addressed 미디어
//! 저장소에 담는다 (ADR-0296 §2 / ADR-0297, root ADR-0290 §7). 쓰기는 내용주소 write-once
//! (`put_object` CreateOnly + sha256), Bronze 객체별 원장 행이 아니다 (ADR-0298).
//!
//! 두 부분이다: 물건상세 응답에서 받을 URL 을 고르는 순수 함수 [`candidate_urls`], 그리고 그것을
//! 하루 예산 안에서 받아 `<prefix>/sha256/<hash>.<ext>` 로 한 번만 저장하는 [`mirror`]. 형식은
//! 바이트 매직으로 판정하고(`foundation_shared_kernel::MediaFormat::sniff`), 키는 내용 해시라서
//! 같은 사진은 한 번만 저장된다. 다운로드 실패는 격리된다 — 미디어 미러링은 수집 run 을 깨지 않는다.
//!
//! 실물 확인(2026-10-26 운영 서비스키 호출): 사진은 `potoUrlList[].urlAdr`, 감정평가서는
//! `apslEvlClgList[].urlAdr`, 둘 다 호스트 `www.onbid.co.kr`. 만료 링크는 HTML 오류 페이지를 주므로
//! 매직바이트 판정이 걸러낸다.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use anyhow::Context as _;
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{content_addressed_key, MediaFormat, ObjectKeyPrefix, Sha256Hex};
use futures_util::StreamExt as _;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};

use crate::data_go_kr_onbid_collect::{LineOutcome, MediaRef};

/// Media objects never change once stored (the key is their content hash), so they cache forever.
const MEDIA_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

const PHOTO_FIELD: &str = "potoUrlList";
const APPRAISAL_FIELD: &str = "apslEvlClgList";

/// The photo and appraisal-attachment URLs of one 물건상세 response, on the allowed host only:
/// photos first (what the screen shows), then appraisals, each once.
///
/// `detail` is the parsed `getRlstDtlInf2` response (`{"body":{"items":{"item": obj | [obj]}}}`).
/// A URL on any other host is dropped (the provider's URLs are all `www.onbid.co.kr`; refusing the
/// rest keeps a tampered or unexpected response from making us fetch an arbitrary host).
pub(crate) fn candidate_urls(detail: &JsonValue, host: &str) -> Vec<String> {
    let mut urls: Vec<String> = Vec::new();
    for field in [PHOTO_FIELD, APPRAISAL_FIELD] {
        for url in field_urls(detail, field, host) {
            if !urls.contains(&url) {
                urls.push(url);
            }
        }
    }
    urls
}

fn detail_items(detail: &JsonValue) -> Vec<&JsonValue> {
    match &detail["body"]["items"]["item"] {
        JsonValue::Array(array) => array.iter().collect(),
        item @ JsonValue::Object(_) => vec![item],
        _ => Vec::new(),
    }
}

fn field_urls(detail: &JsonValue, field: &str, host: &str) -> Vec<String> {
    detail_items(detail)
        .into_iter()
        .filter_map(|entry| entry[field].as_array())
        .flatten()
        .filter_map(|element| element["urlAdr"].as_str())
        .filter(|url| host_is(url, host))
        .map(str::to_owned)
        .collect()
}

fn host_is(url: &str, host: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .host_str()
                .map(|found| found.eq_ignore_ascii_case(host))
        })
        .unwrap_or(false)
}

/// What one run's media mirroring did, for the manifest.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct MediaReport {
    /// New objects stored this run.
    pub(crate) downloaded: usize,
    /// Downloads whose content was already stored (same hash).
    pub(crate) deduplicated: usize,
    /// Downloads larger than the contract's `max_file_bytes` (safety valve; not stored).
    pub(crate) skipped_oversize: usize,
    /// Downloads whose bytes matched no known media signature (e.g. an expired link's HTML).
    pub(crate) skipped_unknown_format: usize,
    /// Downloads the provider refused with a 4xx (a link that is gone); not retried.
    pub(crate) skipped_gone: usize,
    /// Downloads that errored (timeout, 5xx, network, store); isolated, never fail the run, and the
    /// condition is fetched again by a later run.
    pub(crate) errored: usize,
    /// URLs already stored (`state.media`), not downloaded again.
    pub(crate) reused: usize,
    /// Detail lines whose media all landed, and those left for a later run.
    pub(crate) lines_complete: usize,
    pub(crate) lines_incomplete: usize,
    /// The step's time budget stopped it before every line was started.
    pub(crate) time_budget_reached: bool,
    /// Bytes of the new objects stored this run.
    pub(crate) bytes_stored: u64,
    /// The daily download budget ran out before every URL was tried.
    pub(crate) budget_exhausted: bool,
    /// This run's own cap (`per_run_limit`) stopped it before every URL was tried, while the day's
    /// budget still had room — the rest are left for the day's next runs.
    pub(crate) per_run_reached: bool,
}

/// The content-addressed facts a stored (or already-present) media file yields for the Silver photo
/// index. Identical bytes always give the same three, so a re-seen or deduplicated file still maps.
pub(crate) struct StoredMedia {
    pub(crate) sha256: String,
    /// `MediaFormat::extension` of the stored file.
    pub(crate) media_format: String,
    pub(crate) size_bytes: u64,
}

impl StoredMedia {
    fn memo(&self, cltr_mng_no: &str) -> MediaRef {
        MediaRef {
            sha256: self.sha256.clone(),
            media_format: self.media_format.clone(),
            size_bytes: self.size_bytes,
            cltr_mng_no: cltr_mng_no.to_owned(),
        }
    }
}

impl From<&MediaRef> for StoredMedia {
    fn from(known: &MediaRef) -> Self {
        Self {
            sha256: known.sha256.clone(),
            media_format: known.media_format.clone(),
            size_bytes: known.size_bytes,
        }
    }
}

enum Stored {
    New(StoredMedia),
    Deduplicated(StoredMedia),
    Oversize,
    UnknownFormat,
    /// The provider answered 4xx: the link is gone, asking again will not bring it back.
    Gone,
}

/// How many downloads one run may attempt, and whether the day's budget (not this run's own cap) is
/// what would stop it. The cap is `min(per_run_limit, daily_limit - already_downloaded)`; a tie goes
/// to the day (if hitting the cap also exhausts the day, the next run waits for the KST reset, not
/// just the next hourly run).
fn run_download_bound(
    daily_limit: u32,
    per_run_limit: u32,
    already_downloaded: u32,
) -> (u32, bool) {
    let daily_remaining = daily_limit.saturating_sub(already_downloaded);
    let cap = daily_remaining.min(per_run_limit);
    (cap, cap == daily_remaining)
}

/// The media step's settings, from the contract's `media` (root ADR-0296 §2, ADR-0330 §3).
pub(crate) struct MirrorSettings<'a> {
    pub(crate) object_prefix: &'a str,
    pub(crate) url_host: &'a str,
    pub(crate) max_file_bytes: u64,
    pub(crate) daily_limit: u32,
    pub(crate) per_run_limit: u32,
    pub(crate) concurrency: usize,
    /// No line's downloads start after this instant.
    pub(crate) deadline: Instant,
}

/// What one run's media step came to.
pub(crate) struct Mirrored {
    pub(crate) report: MediaReport,
    /// The day's download attempts so far, to persist.
    pub(crate) downloads_today: u32,
    /// The detail lines, rewritten to point at each stored blob by hash.
    pub(crate) lines: Vec<String>,
    /// Per detail line, in the same order: whether its media all landed.
    pub(crate) outcomes: Vec<LineOutcome>,
}

/// Why a line's downloads were not started.
fn stop_reason(report: &mut MediaReport, deadline: Instant, capped_by_day: bool) {
    if Instant::now() >= deadline {
        report.time_budget_reached = true;
    } else if capped_by_day {
        // The next run waits for the KST budget reset, not just the next hour.
        report.budget_exhausted = true;
    } else {
        report.per_run_reached = true;
    }
}

/// Mirrors the photos and appraisal attachments of this run's 물건상세 responses, one detail line
/// (one condition) at a time in the order the lines were asked (root ADR-0330 §1, §3).
///
/// For each line, a URL already in `memo` is not downloaded again unless `revalidate[line]` (the
/// provider changed that condition); the rest are downloaded `concurrency` at a time and stored once
/// by content hash. A line is complete when every URL is stored or permanently skipped (an unknown
/// format, over `max_file_bytes`, a 4xx); a timeout, 5xx or store error leaves it incomplete, so the
/// condition is fetched again by a later run. A line is started only when its downloads fit what is
/// left of `min(per_run_limit, daily_limit - already_downloaded)` and the deadline has not passed —
/// so the step's wall time is at most the budget plus one line, and no condition is left half
/// mirrored by a cap. Every attempt counts toward the caps, failed ones too (incident 2026-10-26:
/// failures that did not count walked hundreds of slow links past the job's time limit). Every
/// failure is isolated: media mirroring never fails the collection run.
pub(crate) async fn mirror_detail_lines(
    detail_lines: &[String],
    revalidate: &[bool],
    memo: &mut BTreeMap<String, MediaRef>,
    settings: &MirrorSettings<'_>,
    already_downloaded: u32,
    storage: &dyn ObjectStorageService,
    http: &reqwest::Client,
) -> Mirrored {
    let mut report = MediaReport::default();
    let mut outcomes = vec![LineOutcome::default(); detail_lines.len()];
    let prefix = match ObjectKeyPrefix::parse(settings.object_prefix) {
        Ok(prefix) => prefix,
        Err(error) => {
            tracing::warn!(error = %error, "onbid media object_prefix is invalid; skipping media");
            return Mirrored {
                report,
                downloads_today: already_downloaded,
                lines: detail_lines.to_vec(),
                outcomes,
            };
        }
    };
    let (this_run_cap, capped_by_day) = run_download_bound(
        settings.daily_limit,
        settings.per_run_limit,
        already_downloaded,
    );
    let mut attempts: u32 = 0;
    let mut stopped = false;
    // The content facts of each resolved URL, so the detail Bronze lines can be rewritten to point at
    // the stored blob by hash (root ADR-0290 §7). A URL seen in several lines is downloaded once.
    let mut resolved: HashMap<String, StoredMedia> = HashMap::new();
    for (index, line) in detail_lines.iter().enumerate() {
        let Some(response) = line_response(line) else {
            continue;
        };
        let cltr = detail_items(&response)
            .first()
            .and_then(|item| item["cltrMngNo"].as_str())
            .unwrap_or_default()
            .trim()
            .to_owned();
        let fresh = revalidate.get(index).copied().unwrap_or(false);
        let mut wanted = Vec::new();
        for url in candidate_urls(&response, settings.url_host) {
            if resolved.contains_key(&url) {
                continue;
            }
            match memo.get(&url).filter(|_| !fresh) {
                Some(known) => {
                    report.reused += 1;
                    resolved.insert(url, StoredMedia::from(known));
                }
                None => wanted.push(url),
            }
        }
        let mut complete = true;
        if !wanted.is_empty() {
            let count = u32::try_from(wanted.len()).unwrap_or(u32::MAX);
            if stopped
                || attempts.saturating_add(count) > this_run_cap
                || Instant::now() >= settings.deadline
            {
                if !stopped {
                    stop_reason(&mut report, settings.deadline, capped_by_day);
                }
                // Later lines still settle when all their media is already stored.
                stopped = true;
                complete = false;
            } else {
                attempts += count;
                let results = futures_util::stream::iter(wanted)
                    .map(|url| async {
                        let stored = download_and_store(
                            &url,
                            &prefix,
                            storage,
                            http,
                            settings.max_file_bytes,
                        )
                        .await;
                        (url, stored)
                    })
                    .buffer_unordered(settings.concurrency.max(1))
                    .collect::<Vec<_>>()
                    .await;
                for (url, stored) in results {
                    match stored {
                        Ok(Stored::New(media)) => {
                            report.downloaded += 1;
                            report.bytes_stored += media.size_bytes;
                            memo.insert(url.clone(), media.memo(&cltr));
                            resolved.insert(url, media);
                        }
                        Ok(Stored::Deduplicated(media)) => {
                            report.deduplicated += 1;
                            memo.insert(url.clone(), media.memo(&cltr));
                            resolved.insert(url, media);
                        }
                        Ok(Stored::Oversize) => report.skipped_oversize += 1,
                        Ok(Stored::UnknownFormat) => report.skipped_unknown_format += 1,
                        Ok(Stored::Gone) => report.skipped_gone += 1,
                        Err(error) => {
                            complete = false;
                            report.errored += 1;
                            tracing::warn!(error = %error, "onbid media download failed; isolated from the run");
                        }
                    }
                }
            }
        }
        let photos = field_urls(&response, PHOTO_FIELD, settings.url_host)
            .iter()
            .filter(|url| resolved.contains_key(*url))
            .count();
        outcomes[index] = LineOutcome {
            complete,
            photos: u32::try_from(photos).unwrap_or(u32::MAX),
        };
        if complete {
            report.lines_complete += 1;
        } else {
            report.lines_incomplete += 1;
        }
    }
    Mirrored {
        report,
        downloads_today: already_downloaded.saturating_add(attempts),
        lines: rewrite_detail_lines(detail_lines, &resolved),
        outcomes,
    }
}

/// The provider's response a detail Bronze line carries, when it parses.
fn line_response(line: &str) -> Option<JsonValue> {
    let record = serde_json::from_str::<JsonValue>(line).ok()?;
    serde_json::from_str::<JsonValue>(record["body"].as_str()?).ok()
}

/// Rewrites each 물건상세 Bronze line so every stored photo/appraisal entry points at its blob by
/// content hash (root ADR-0290 §7): `urlAdr` becomes `sha256:<hash>` and the entry gains `sha256`,
/// `media_format` and `size_bytes`, which the Silver photo loader reads. An entry whose download did
/// not resolve (failed, over-cap, or an unknown format) keeps its original `urlAdr` and no hash, so
/// the loader skips it. A line that does not parse, or whose items hold no resolved media, passes
/// through unchanged.
fn rewrite_detail_lines(
    detail_lines: &[String],
    resolved: &HashMap<String, StoredMedia>,
) -> Vec<String> {
    detail_lines
        .iter()
        .map(|line| rewrite_one_line(line, resolved).unwrap_or_else(|| line.clone()))
        .collect()
}

/// Returns the rewritten line, or `None` when nothing changed (so the caller keeps the original).
fn rewrite_one_line(line: &str, resolved: &HashMap<String, StoredMedia>) -> Option<String> {
    let mut record = serde_json::from_str::<JsonValue>(line).ok()?;
    let body = record.get("body")?.as_str()?;
    let mut response = serde_json::from_str::<JsonValue>(body).ok()?;
    let item = response
        .get_mut("body")?
        .get_mut("items")?
        .get_mut("item")?;
    let mut changed = false;
    match item {
        JsonValue::Array(array) => {
            for entry in array.iter_mut() {
                changed |= rewrite_item(entry, resolved);
            }
        }
        JsonValue::Object(_) => changed |= rewrite_item(item, resolved),
        _ => {}
    }
    if !changed {
        return None;
    }
    let new_body = serde_json::to_string(&response).ok()?;
    record
        .as_object_mut()?
        .insert("body".to_owned(), JsonValue::String(new_body));
    serde_json::to_string(&record).ok()
}

/// Points each resolved media entry of one item at its stored blob; returns whether anything changed.
fn rewrite_item(item: &mut JsonValue, resolved: &HashMap<String, StoredMedia>) -> bool {
    let mut changed = false;
    for field in ["potoUrlList", "apslEvlClgList"] {
        let Some(list) = item.get_mut(field).and_then(JsonValue::as_array_mut) else {
            continue;
        };
        for entry in list.iter_mut() {
            let Some(url) = entry
                .get("urlAdr")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            let Some(media) = resolved.get(&url) else {
                continue;
            };
            let Some(object) = entry.as_object_mut() else {
                continue;
            };
            object.insert("sha256".to_owned(), JsonValue::String(media.sha256.clone()));
            object.insert(
                "media_format".to_owned(),
                JsonValue::String(media.media_format.clone()),
            );
            object.insert(
                "size_bytes".to_owned(),
                JsonValue::Number(media.size_bytes.into()),
            );
            object.insert(
                "urlAdr".to_owned(),
                JsonValue::String(format!("sha256:{}", media.sha256)),
            );
            changed = true;
        }
    }
    changed
}

async fn download_and_store(
    url: &str,
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
    http: &reqwest::Client,
    max_file_bytes: u64,
) -> anyhow::Result<Stored> {
    let response = http.get(url).send().await.context("media GET failed")?;
    if response.status().is_client_error() {
        return Ok(Stored::Gone);
    }
    anyhow::ensure!(
        response.status().is_success(),
        "media GET answered HTTP {}",
        response.status().as_u16()
    );
    // Refuse an over-cap file by its declared length before reading the body, so a huge response
    // never lands whole in memory on this small host (root: the 5.89 GB single-PUT stall).
    if response
        .content_length()
        .is_some_and(|len| len > max_file_bytes)
    {
        return Ok(Stored::Oversize);
    }
    let bytes = response
        .bytes()
        .await
        .context("reading the media body failed")?;
    // A chunked response carries no length; the same cap still holds once the bytes are in hand.
    if bytes.len() as u64 > max_file_bytes {
        return Ok(Stored::Oversize);
    }
    let Some(format) = MediaFormat::sniff(&bytes) else {
        return Ok(Stored::UnknownFormat);
    };
    let hash_hex = sha256_hex(&bytes);
    let hash = Sha256Hex::parse(&hash_hex).context("sha-256 is not canonical")?;
    let key =
        content_addressed_key(prefix, &hash, format).context("media object key is invalid")?;
    let media = StoredMedia {
        sha256: hash_hex.clone(),
        media_format: format.extension().to_owned(),
        size_bytes: bytes.len() as u64,
    };
    // The key is the content hash: if it exists, the stored bytes are identical, so skip the write.
    if storage
        .read_object_sha256(key.as_str())
        .await
        .context("media head failed")?
        .is_some()
    {
        return Ok(Stored::Deduplicated(media));
    }
    match storage
        .put_object(PutObjectRequest {
            key: key.as_str().to_owned(),
            body: bytes.to_vec(),
            content_type: format.content_type().to_owned(),
            cache_control: MEDIA_CACHE_CONTROL.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(hash_hex),
        })
        .await
    {
        Ok(()) => Ok(Stored::New(media)),
        // A concurrent run stored the same content first: identical bytes, so it is a dedup, not a fault.
        Err(PublishError::ObjectAlreadyExists { .. }) => Ok(Stored::Deduplicated(media)),
        Err(error) => Err(error).context("media put_object failed"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::{candidate_urls, host_is, run_download_bound};
    use serde_json::json;

    #[test]
    fn run_download_bound_takes_the_smaller_of_the_two_caps_and_names_the_day_on_a_tie() {
        // per-run cap is smaller: this run stops at it, more remain for the day's next runs.
        assert_eq!(run_download_bound(500, 50, 0), (50, false));
        // the day's remaining is smaller: this run stops at it and the day is spent.
        assert_eq!(run_download_bound(500, 50, 470), (30, true));
        // a tie goes to the day (the next run waits for the KST reset, not just the next hour).
        assert_eq!(run_download_bound(500, 50, 450), (50, true));
        // the day is already spent: nothing this run, and it is the day that stopped it.
        assert_eq!(run_download_bound(500, 50, 500), (0, true));
        // the daily limit itself is below the per-run cap (the small-start case): the day bounds it.
        assert_eq!(run_download_bound(20, 50, 0), (20, true));
    }

    fn detail(item: serde_json::Value) -> serde_json::Value {
        json!({"body": {"items": {"item": item}}})
    }

    #[test]
    fn picks_photo_and_appraisal_urls_on_the_allowed_host_only() {
        let d = detail(json!({
            "potoUrlList": [
                {"urlAdr": "https://www.onbid.co.kr/op/a.jpg"},
                {"urlAdr": "https://www.onbid.co.kr/op/b.jpg"},
                {"urlAdr": "https://evil.example.test/x.jpg"}
            ],
            "apslEvlClgList": [{"urlAdr": "https://www.onbid.co.kr/op/appraisal.pdf", "apslEvlOrgNm": "x"}],
            "thnlImgUrlAdr": "https://www.onbid.co.kr/op/thumb.jpg"
        }));
        let urls = candidate_urls(&d, "www.onbid.co.kr");
        assert_eq!(
            urls,
            vec![
                "https://www.onbid.co.kr/op/a.jpg".to_owned(),
                "https://www.onbid.co.kr/op/b.jpg".to_owned(),
                "https://www.onbid.co.kr/op/appraisal.pdf".to_owned(),
            ],
            "only potoUrlList + apslEvlClgList urlAdr on the allowed host, photos first, each once"
        );
    }

    #[test]
    fn a_single_item_object_is_handled_like_a_one_element_list() {
        let d = detail(json!({"potoUrlList": [{"urlAdr": "https://www.onbid.co.kr/op/only.jpg"}]}));
        assert_eq!(candidate_urls(&d, "www.onbid.co.kr").len(), 1);
    }

    #[test]
    fn no_media_fields_yields_nothing() {
        assert!(candidate_urls(&detail(json!({"cltrMngNo": "A-1"})), "www.onbid.co.kr").is_empty());
        assert!(candidate_urls(&json!({"body": {}}), "www.onbid.co.kr").is_empty());
    }

    #[test]
    fn host_match_is_exact_and_rejects_lookalikes() {
        assert!(host_is("https://www.onbid.co.kr/x.jpg", "www.onbid.co.kr"));
        assert!(host_is("https://WWW.ONBID.CO.KR/x.jpg", "www.onbid.co.kr"));
        assert!(!host_is(
            "https://www.onbid.co.kr.evil.test/x.jpg",
            "www.onbid.co.kr"
        ));
        assert!(!host_is("not a url", "www.onbid.co.kr"));
    }

    #[test]
    fn rewrite_points_resolved_media_at_its_hash_and_leaves_the_rest() {
        use super::{rewrite_detail_lines, StoredMedia};
        use std::collections::HashMap;

        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let body = detail(json!([{
            "cltrMngNo": "A-1", "pbctCdtnNo": 1,
            "potoUrlList": [
                {"urlAdr": "https://www.onbid.co.kr/op/a.jpg"},
                {"urlAdr": "https://www.onbid.co.kr/op/missing.jpg"}
            ],
            "apslEvlClgList": [{"urlAdr": "https://www.onbid.co.kr/op/x.pdf"}]
        }]))
        .to_string();
        let line = json!({ "body": body }).to_string();

        let mut resolved = HashMap::new();
        resolved.insert(
            "https://www.onbid.co.kr/op/a.jpg".to_owned(),
            StoredMedia {
                sha256: a.clone(),
                media_format: "jpg".to_owned(),
                size_bytes: 10,
            },
        );
        resolved.insert(
            "https://www.onbid.co.kr/op/x.pdf".to_owned(),
            StoredMedia {
                sha256: b.clone(),
                media_format: "pdf".to_owned(),
                size_bytes: 20,
            },
        );

        let out = rewrite_detail_lines(&[line], &resolved);
        let record: serde_json::Value = serde_json::from_str(&out[0]).unwrap();
        let response: serde_json::Value =
            serde_json::from_str(record["body"].as_str().unwrap()).unwrap();
        let item = &response["body"]["items"]["item"][0];
        let photos = item["potoUrlList"].as_array().unwrap();

        // The resolved photo now points at its blob by hash, with format and size for the loader.
        assert_eq!(photos[0]["sha256"], json!(a));
        assert_eq!(photos[0]["media_format"], json!("jpg"));
        assert_eq!(photos[0]["size_bytes"], json!(10));
        assert_eq!(photos[0]["urlAdr"], json!(format!("sha256:{a}")));
        // The unresolved photo keeps its link and has no hash, so the Silver loader skips it.
        assert!(photos[1].get("sha256").is_none());
        assert_eq!(
            photos[1]["urlAdr"],
            json!("https://www.onbid.co.kr/op/missing.jpg")
        );
        // The appraisal attachment is rewritten the same way.
        assert_eq!(item["apslEvlClgList"][0]["sha256"], json!(b));
        assert_eq!(item["apslEvlClgList"][0]["media_format"], json!("pdf"));
    }

    /// A detail Bronze line whose item lists `photos` (paths on the mock host).
    fn line(base: &str, cltr: &str, photos: &[&str]) -> String {
        let urls = photos
            .iter()
            .map(|photo| json!({"urlAdr": format!("{base}{photo}")}))
            .collect::<Vec<_>>();
        let body = detail(json!({"cltrMngNo": cltr, "pbctCdtnNo": 1, "potoUrlList": urls}));
        json!({"body": body.to_string()}).to_string()
    }

    #[tokio::test]
    async fn a_line_settles_whole_reuses_stored_urls_and_stops_at_a_line_boundary(
    ) -> anyhow::Result<()> {
        use super::{mirror_detail_lines, MirrorSettings};
        use crate::data_go_kr_onbid_collect::MediaRef;
        use std::collections::BTreeMap;
        use std::time::{Duration, Instant};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        for (name, byte) in [
            ("/a.jpg", 1_u8),
            ("/b.jpg", 2),
            ("/c.jpg", 3),
            ("/d.jpg", 4),
        ] {
            Mock::given(method("GET"))
                .and(path(name))
                .respond_with(
                    ResponseTemplate::new(200).set_body_bytes(vec![0xFF, 0xD8, 0xFF, byte]),
                )
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/broken.jpg"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/gone.jpg"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let base = server.uri();
        let host = reqwest::Url::parse(&base)?
            .host_str()
            .unwrap_or_default()
            .to_owned();
        let root = std::env::temp_dir().join(format!("onbid-media-{}", uuid::Uuid::new_v4()));
        let storage = foundation_outbox::FileObjectStorage::new(root)?;
        let http = reqwest::Client::new();
        let settings = |per_run_limit| MirrorSettings {
            object_prefix: "onbid/media",
            url_host: &host,
            max_file_bytes: 1024,
            daily_limit: 100,
            per_run_limit,
            concurrency: 2,
            deadline: Instant::now() + Duration::from_secs(60),
        };
        let mut memo = BTreeMap::new();
        // A URL stored by an earlier run, for an item still open.
        memo.insert(
            format!("{base}/known.jpg"),
            MediaRef {
                sha256: "e".repeat(64),
                media_format: "jpg".to_owned(),
                size_bytes: 9,
                cltr_mng_no: "K".to_owned(),
            },
        );
        let lines = vec![
            line(&base, "K", &["/known.jpg", "/a.jpg", "/gone.jpg"]),
            line(&base, "X", &["/broken.jpg", "/b.jpg"]),
            line(&base, "Y", &["/c.jpg", "/d.jpg"]),
            line(&base, "K", &["/known.jpg"]),
        ];

        // A cap of 4: lines 0 and 1 fit (2 + 2), line 2 (2 more) does not start, line 3 is all memo.
        let mirrored =
            mirror_detail_lines(&lines, &[], &mut memo, &settings(4), 0, &storage, &http).await;
        let outcomes = mirrored
            .outcomes
            .iter()
            .map(|outcome| (outcome.complete, outcome.photos))
            .collect::<Vec<_>>();
        // known + a stored, gone skipped for good: complete with two photos. broken is a 503: the
        // line waits for a later run. Line 2 was never started; line 3 needed no download.
        assert_eq!(outcomes, [(true, 2), (false, 1), (false, 0), (true, 1)]);
        assert_eq!(mirrored.downloads_today, 4);
        assert_eq!(mirrored.report.reused, 1);
        assert_eq!(mirrored.report.skipped_gone, 1);
        assert_eq!(mirrored.report.errored, 1);
        assert!(mirrored.report.per_run_reached);
        assert!(memo.contains_key(&format!("{base}/a.jpg")));
        assert!(!memo.contains_key(&format!("{base}/c.jpg")));
        // The reused URL is rewritten to its hash like a downloaded one.
        assert!(mirrored.lines[3].contains(&"e".repeat(64)));

        // A refreshed line downloads again even what the memo knows; identical bytes are a dedup.
        let mirrored = mirror_detail_lines(
            &lines[..1],
            &[true],
            &mut memo,
            &settings(10),
            0,
            &storage,
            &http,
        )
        .await;
        assert_eq!(mirrored.report.reused, 0);
        assert_eq!(mirrored.report.deduplicated, 1);
        Ok(())
    }
}
