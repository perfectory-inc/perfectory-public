//! 온비드 물건상세(getRlstDtlInf2)의 사진·감정평가서 바이너리를 공용 content-addressed 미디어
//! 저장소에 담는다 (ADR-0296 §2 / ADR-0297, root ADR-0290 §7). 쓰기는 내용주소 write-once
//! (`put_object` CreateOnly + sha256), Bronze 객체별 원장 행이 아니다 (ADR-0298).
//!
//! 두 부분이다: 물건상세 응답에서 받을 URL 을 종류별로 고르는 순수 함수 [`candidate_urls`], 그리고
//! 그것을 하루 예산 안에서 (모든 줄의 사진 먼저, 그다음 감정평가서) 받아 `<prefix>/sha256/<hash>.<ext>` 로 한 번만 저장하는 [`mirror`]. 형식은
//! 바이트 매직으로 판정하고(`foundation_shared_kernel::MediaFormat::sniff`), 키는 내용 해시라서
//! 같은 사진은 한 번만 저장된다. 다운로드 실패는 격리된다 — 미디어 미러링은 수집 run 을 깨지 않는다.
//!
//! 실물 확인(2026-10-26 운영 서비스키 호출): 사진은 `potoUrlList[].urlAdr`, 감정평가서는
//! `apslEvlClgList[].urlAdr`, 둘 다 호스트 `www.onbid.co.kr`. 만료 링크는 HTML 오류 페이지를 주므로
//! 매직바이트 판정이 걸러낸다.
//!
//! 실측(2026-11-06, 공개 GET): `potoUrlList[].urlAdr` 는 썸네일(178×134)이다. 같은 첨부의 원본은
//! 계약의 `media.fields.originals` 규칙으로 URL 을 바꿔 썸네일 다음에 받고, `<필드>Original` 로 썸네일
//! 옆에 적는다 — 둘 다 남긴다 (root ADR-0424).

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

use crate::data_go_kr_onbid_collect::{
    LineOutcome, MediaFields, MediaRef, OriginalRule, RenditionBudget,
};
use crate::media_renditions::{self, Laddered};

const PHOTO_FIELD: &str = "potoUrlList";
/// The key beside a list field (`<field>Original`) naming the originals of its thumbnails (root
/// ADR-0424).
const ORIGINAL_SUFFIX: &str = "Original";
#[cfg(test)]
const APPRAISAL_FIELD: &str = "apslEvlClgList";

/// The media kinds in the order a run downloads them: every line's photos (what the screen shows,
/// ~7% of the bytes) before any line's appraisals (root ADR-0339 footnote 2026-10-29).
#[cfg(test)]
const FIELDS_IN_ORDER: [&str; 2] = [PHOTO_FIELD, APPRAISAL_FIELD];

/// The URLs of one media `field` (`potoUrlList` or `apslEvlClgList`) of one 물건상세 response, on
/// the allowed host only, each once.
///
/// `detail` is the parsed `getRlstDtlInf2` response (`{"body":{"items":{"item": obj | [obj]}}}`).
/// A URL on any other host is dropped (the provider's URLs are all `www.onbid.co.kr`; refusing the
/// rest keeps a tampered or unexpected response from making us fetch an arbitrary host).
pub(crate) fn candidate_urls(detail: &JsonValue, field: &str, host: &str) -> Vec<String> {
    let mut urls: Vec<String> = Vec::new();
    for url in field_urls(detail, field, host) {
        if !urls.contains(&url) {
            urls.push(url);
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
        .flat_map(|entry| urls_of(&entry[field]))
        .filter(|url| host_is(url, host))
        .collect()
}

/// The URLs a media field holds, in order: a `[{urlAdr}]` list (photos, appraisals, attachments), or
/// a `|`-joined string or a list of strings (location and cadastral maps, 360-degree photos, video;
/// root ADR-0340 §4).
fn urls_of(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(text) => text
            .split('|')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_owned)
            .collect(),
        JsonValue::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().or_else(|| item["urlAdr"].as_str()))
            .map(|url| url.trim().to_owned())
            .filter(|url| !url.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

/// One pass of a run's media step: a field's own URLs, or the originals of a field's thumbnails.
#[derive(Clone, Copy, Debug)]
enum Pass<'a> {
    Field(&'a str),
    Original(&'a str, &'a OriginalRule),
}

impl Pass<'_> {
    /// The URLs this pass downloads from one 물건상세 response, each once.
    fn urls(&self, detail: &JsonValue, host: &str) -> Vec<String> {
        match *self {
            Pass::Field(field) => candidate_urls(detail, field, host),
            Pass::Original(field, rule) => {
                let mut urls: Vec<String> = Vec::new();
                for url in candidate_urls(detail, field, host)
                    .iter()
                    .filter_map(|url| original_url(url, rule))
                {
                    if !urls.contains(&url) {
                        urls.push(url);
                    }
                }
                urls
            }
        }
    }
}

/// Every media pass of `fields` in the order a run downloads them: photos first (what the screen
/// shows), then the photos' originals (root ADR-0424), then the other lists and the maps (root
/// ADR-0339 footnote, ADR-0340 §4).
fn fields_in_order(fields: &MediaFields) -> Vec<Pass<'_>> {
    std::iter::once(Pass::Field(PHOTO_FIELD))
        .chain(
            fields
                .originals
                .iter()
                .map(|(field, rule)| Pass::Original(field.as_str(), rule)),
        )
        .chain(
            fields
                .list
                .keys()
                .map(String::as_str)
                .filter(|field| *field != PHOTO_FIELD)
                .map(Pass::Field),
        )
        .chain(fields.pipe.keys().map(String::as_str).map(Pass::Field))
        .collect()
}

/// The full-size original of a thumbnail `url` (root ADR-0424): on a URL whose path ends with the
/// rule's `path_suffix`, each of its `query` keys set to its value, the other parameters kept.
/// `None` when the rule does not apply or changes nothing.
pub(crate) fn original_url(url: &str, rule: &OriginalRule) -> Option<String> {
    let mut parsed = reqwest::Url::parse(url).ok()?;
    if rule.path_suffix.is_empty() || !parsed.path().ends_with(&rule.path_suffix) {
        return None;
    }
    let kept = parsed
        .query_pairs()
        .filter(|(key, _)| !rule.query.contains_key(key.as_ref()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    parsed
        .query_pairs_mut()
        .clear()
        .extend_pairs(kept)
        .extend_pairs(&rule.query);
    let original = parsed.to_string();
    (original != url).then_some(original)
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
    /// Originals downloaded through `media.fields.originals` (root ADR-0424), and of those the ones
    /// the rule no longer reaches: an HTML page, or an image no larger than the provider's thumbnail
    /// (root ADR-0427 §4). Those are not recorded as originals, so the screen keeps the thumbnail.
    pub(crate) originals_attempted: usize,
    pub(crate) originals_rule_mismatched: usize,
    /// Photo sources (the original, else the thumbnail) whose rendition ladder was already there,
    /// written now (and its bytes), not renderable (bytes that do not decode), failed in the store,
    /// or left for a later run by the ladder budget (root ADR-0427 §2).
    pub(crate) ladders_present: usize,
    pub(crate) ladders_written: usize,
    pub(crate) ladder_bytes_stored: u64,
    pub(crate) ladders_unrenderable: usize,
    pub(crate) ladders_errored: usize,
    pub(crate) ladders_deferred: usize,
}

/// The content-addressed facts a stored (or already-present) media file yields for the Silver photo
/// index. Identical bytes always give the same three, so a re-seen or deduplicated file still maps.
pub(crate) struct StoredMedia {
    pub(crate) sha256: String,
    /// `MediaFormat::extension` of the stored file.
    pub(crate) media_format: String,
    pub(crate) size_bytes: u64,
    /// The longest side its header declares, when downloaded this run and readable.
    pub(crate) longest_px: Option<u32>,
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
            longest_px: None,
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
    /// Which response fields are media (root ADR-0340 §4).
    pub(crate) fields: &'a MediaFields,
    /// The photos' rendition ladder step (root ADR-0427); `None` leaves it out.
    pub(crate) renditions: Option<&'a RenditionBudget>,
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

/// Mirrors the photos and appraisal attachments of this run's 물건상세 responses: first the photos of
/// every line, then the appraisals of every line, each pass one detail line (one item) at a time in
/// the order the lines were asked (root ADR-0330 §1, §3; photos first by root ADR-0339 footnote
/// 2026-10-29 — 93% of the bytes are appraisals, so one shared budget let a few items' PDFs hold
/// back every other item's photos).
///
/// For each line, a URL already in `memo` is not downloaded again unless `revalidate[line]` (the
/// provider changed that item); the rest are downloaded `concurrency` at a time and stored once by
/// content hash. A line is complete when every URL of both kinds is stored or permanently skipped
/// (an unknown format, over `max_file_bytes`, a 4xx); a timeout, 5xx or store error leaves it
/// incomplete, so the item is fetched again by a later run. A line's photos (or appraisals) are
/// started only when they fit what is left of `min(per_run_limit, daily_limit - already_downloaded)`
/// and the deadline has not passed; once one does not, nothing more starts — so the step's wall time
/// is at most the budget plus one line's group, and no kind of an item is left half mirrored by a
/// cap. A stored photo is written into its line (and so reaches Silver) whether or not the line's
/// appraisals landed. Every attempt counts toward the caps, failed ones too (incident 2026-10-26:
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
    let responses = detail_lines
        .iter()
        .map(|line| line_response(line))
        .collect::<Vec<_>>();
    // Per line: every URL of the kinds passed so far landed or was skipped for good.
    let mut landed = vec![true; detail_lines.len()];
    for pass in fields_in_order(settings.fields) {
        for (index, response) in responses.iter().enumerate() {
            let Some(response) = response else {
                continue;
            };
            let cltr = detail_items(response)
                .first()
                // An announcement line (공고상세) has no item; its media are memoized under its number.
                .and_then(|item| {
                    item["cltrMngNo"]
                        .as_str()
                        .or_else(|| item["pbancMngNo"].as_str())
                })
                .unwrap_or_default()
                .trim()
                .to_owned();
            let fresh = revalidate.get(index).copied().unwrap_or(false);
            let mut wanted = Vec::new();
            for url in pass.urls(response, settings.url_host) {
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
            if wanted.is_empty() {
                continue;
            }
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
                landed[index] = false;
                continue;
            }
            attempts += count;
            let results = futures_util::stream::iter(wanted)
                .map(|url| async {
                    let stored =
                        download_and_store(&url, &prefix, storage, http, settings.max_file_bytes)
                            .await;
                    (url, stored)
                })
                .buffer_unordered(settings.concurrency.max(1))
                .collect::<Vec<_>>()
                .await;
            for (url, stored) in results {
                if let Pass::Original(_, rule) = pass {
                    report.originals_attempted += 1;
                    if !reaches_original(&stored, rule) {
                        // Not memoized either: a later visit asks the rule again.
                        report.originals_rule_mismatched += 1;
                        match stored {
                            Ok(Stored::New(media)) => {
                                report.downloaded += 1;
                                report.bytes_stored += media.size_bytes;
                            }
                            Ok(Stored::Deduplicated(_)) => report.deduplicated += 1,
                            _ => report.skipped_unknown_format += 1,
                        }
                        continue;
                    }
                }
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
                        landed[index] = false;
                        report.errored += 1;
                        tracing::warn!(error = %error, "onbid media download failed; isolated from the run");
                    }
                }
            }
        }
        if matches!(pass, Pass::Field(PHOTO_FIELD)) {
            for (outcome, landed) in outcomes.iter_mut().zip(&landed) {
                outcome.photos_complete = *landed;
            }
        }
    }
    if let Some(budget) = settings.renditions {
        render_ladders(
            &responses,
            &resolved,
            settings,
            budget,
            storage,
            &mut report,
            &mut landed,
        )
        .await;
    }
    for (index, response) in responses.iter().enumerate() {
        let Some(response) = response else {
            continue;
        };
        let photos = field_urls(response, PHOTO_FIELD, settings.url_host)
            .iter()
            .filter(|url| resolved.contains_key(*url))
            .count();
        outcomes[index].complete = landed[index];
        outcomes[index].photos = u32::try_from(photos).unwrap_or(u32::MAX);
        if landed[index] {
            report.lines_complete += 1;
        } else {
            report.lines_incomplete += 1;
        }
    }
    Mirrored {
        report,
        downloads_today: already_downloaded.saturating_add(attempts),
        lines: rewrite_detail_lines(detail_lines, &resolved, settings.fields),
        outcomes,
    }
}

/// Whether an original download is one: not an HTML page, and not an image whose header says it is
/// no larger than the provider's thumbnail (root ADR-0427 §4; the rule's `thumbnail_longest_px`). An
/// image whose header does not read is let through: its size cannot be told.
fn reaches_original(stored: &anyhow::Result<Stored>, rule: &OriginalRule) -> bool {
    match stored {
        Ok(Stored::New(media) | Stored::Deduplicated(media)) => media
            .longest_px
            .is_none_or(|longest| longest > rule.thumbnail_longest_px),
        Ok(Stored::UnknownFormat) => false,
        _ => true,
    }
}

/// What the screen shows of one line's photos, in order and each once: the original where it
/// landed, else the thumbnail (root ADR-0424 §6). These are the files a rendition ladder is made
/// from.
fn photo_sources(
    response: &JsonValue,
    resolved: &HashMap<String, StoredMedia>,
    settings: &MirrorSettings<'_>,
) -> Vec<(String, String)> {
    let rule = settings.fields.originals.get(PHOTO_FIELD);
    let mut sources: Vec<(String, String)> = Vec::new();
    for url in field_urls(response, PHOTO_FIELD, settings.url_host) {
        let shown = rule
            .and_then(|rule| original_url(&url, rule))
            .and_then(|original| resolved.get(&original))
            .or_else(|| resolved.get(&url));
        if let Some(media) = shown.filter(|media| media_renditions::is_source(&media.media_format))
        {
            if !sources.iter().any(|(sha, _)| *sha == media.sha256) {
                sources.push((media.sha256.clone(), media.media_format.clone()));
            }
        }
    }
    sources
}

/// The rendition ladder of every line's photo sources (root ADR-0427 §2), line by line in the
/// order the lines were asked, `budget.concurrency` sources at a time. A source whose ladder is
/// already there costs one HEAD. No line starts once `budget.per_run_limit` sources were taken or
/// `budget.run_time_budget_seconds` passed; such a line, and a line whose ladder the store refused,
/// stays incomplete, so the item is fetched again by a later run (root ADR-0330 §1). A source that
/// does not decode settles the line: the screen shows the file itself.
async fn render_ladders(
    responses: &[Option<JsonValue>],
    resolved: &HashMap<String, StoredMedia>,
    settings: &MirrorSettings<'_>,
    budget: &RenditionBudget,
    storage: &dyn ObjectStorageService,
    report: &mut MediaReport,
    landed: &mut [bool],
) {
    let spec = match media_renditions::spec() {
        Ok(spec) => spec,
        Err(error) => {
            tracing::warn!(error = %error, "no rendition ladder this run");
            return;
        }
    };
    let deadline = Instant::now() + std::time::Duration::from_secs(budget.run_time_budget_seconds);
    let mut settled: HashMap<String, bool> = HashMap::new();
    let mut taken: u32 = 0;
    let mut stopped = false;
    for (index, response) in responses.iter().enumerate() {
        let Some(response) = response else {
            continue;
        };
        let mut wanted = Vec::new();
        for (sha, format) in photo_sources(response, resolved, settings) {
            match settled.get(&sha) {
                Some(true) => {}
                Some(false) => landed[index] = false,
                None => wanted.push((sha, format)),
            }
        }
        if wanted.is_empty() {
            continue;
        }
        let count = u32::try_from(wanted.len()).unwrap_or(u32::MAX);
        if stopped
            || taken.saturating_add(count) > budget.per_run_limit
            || Instant::now() >= deadline
        {
            stopped = true;
            landed[index] = false;
            report.ladders_deferred += wanted.len();
            continue;
        }
        taken += count;
        let results = futures_util::stream::iter(wanted)
            .map(|(sha, format)| async move {
                let outcome = media_renditions::ensure_ladder(
                    settings.object_prefix,
                    &sha,
                    &format,
                    spec,
                    storage,
                )
                .await;
                (sha, outcome)
            })
            .buffer_unordered(budget.concurrency.max(1))
            .collect::<Vec<_>>()
            .await;
        for (sha, outcome) in results {
            let ok = match outcome {
                Ok(Laddered::Present) => {
                    report.ladders_present += 1;
                    true
                }
                Ok(Laddered::Written(bytes)) => {
                    report.ladders_written += 1;
                    report.ladder_bytes_stored += bytes;
                    true
                }
                Ok(Laddered::Unrenderable) => {
                    report.ladders_unrenderable += 1;
                    true
                }
                Err(error) => {
                    report.ladders_errored += 1;
                    tracing::warn!(error = %error, "a rendition ladder failed; isolated from the run");
                    false
                }
            };
            landed[index] &= ok;
            settled.insert(sha, ok);
        }
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
    fields: &MediaFields,
) -> Vec<String> {
    detail_lines
        .iter()
        .map(|line| rewrite_one_line(line, resolved, fields).unwrap_or_else(|| line.clone()))
        .collect()
}

/// Returns the rewritten line, or `None` when nothing changed (so the caller keeps the original).
fn rewrite_one_line(
    line: &str,
    resolved: &HashMap<String, StoredMedia>,
    fields: &MediaFields,
) -> Option<String> {
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
                changed |= rewrite_item(entry, resolved, fields);
            }
        }
        JsonValue::Object(_) => changed |= rewrite_item(item, resolved, fields),
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
///
/// A `|`-joined field keeps its text; beside it, `<field>Media` lists each resolved URL's blob in the
/// field's order (`sequence` = its position), which the Silver media loader reads (root ADR-0340 §4).
fn rewrite_item(
    item: &mut JsonValue,
    resolved: &HashMap<String, StoredMedia>,
    fields: &MediaFields,
) -> bool {
    let mut changed = false;
    for field in fields.pipe.keys() {
        let stored = urls_of(&item[field.as_str()])
            .iter()
            .enumerate()
            .filter_map(|(sequence, url)| {
                resolved.get(url).map(|media| {
                    serde_json::json!({
                        "sequence": sequence,
                        "sha256": media.sha256,
                        "media_format": media.media_format,
                        "size_bytes": media.size_bytes,
                    })
                })
            })
            .collect::<Vec<_>>();
        if let (false, Some(object)) = (stored.is_empty(), item.as_object_mut()) {
            object.insert(format!("{field}Media"), JsonValue::Array(stored));
            changed = true;
        }
    }
    for (field, rule) in &fields.originals {
        let originals = item[field.as_str()]
            .as_array()
            .map(|list| {
                list.iter()
                    .enumerate()
                    .filter_map(|(sequence, entry)| {
                        let url = entry["urlAdr"].as_str()?.trim();
                        let original = resolved.get(&original_url(url, rule)?)?;
                        // The same bytes as the thumbnail: that file already is the original.
                        if resolved
                            .get(url)
                            .is_some_and(|thumbnail| thumbnail.sha256 == original.sha256)
                        {
                            return None;
                        }
                        Some(serde_json::json!({
                            "sequence": sequence,
                            "sha256": original.sha256,
                            "media_format": original.media_format,
                            "size_bytes": original.size_bytes,
                        }))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let (false, Some(object)) = (originals.is_empty(), item.as_object_mut()) {
            object.insert(
                format!("{field}{ORIGINAL_SUFFIX}"),
                JsonValue::Array(originals),
            );
            changed = true;
        }
    }
    for field in fields.list.keys() {
        let Some(list) = item
            .get_mut(field.as_str())
            .and_then(JsonValue::as_array_mut)
        else {
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
    let hash = Sha256Hex::of(&bytes);
    let hash_hex = hash.as_str().to_owned();
    let key =
        content_addressed_key(prefix, &hash, format).context("media object key is invalid")?;
    let media = StoredMedia {
        sha256: hash_hex.clone(),
        media_format: format.extension().to_owned(),
        size_bytes: bytes.len() as u64,
        longest_px: foundation_media_renditions::dimensions(&bytes)
            .map(|(width, height)| width.max(height)),
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
            // Never changes once stored (the key is its content hash): the edge's one cache policy.
            cache_control: media_renditions::cache_control()?.to_owned(),
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

#[cfg(test)]
mod tests;
