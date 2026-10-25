//! 온비드 물건상세(getRlstDtlInf2)의 사진·감정평가서 바이너리를 공용 content-addressed 미디어
//! 저장소에 담는다 (ADR-0296 §2 / ADR-0297, root ADR-0290 §7).
//!
//! 두 부분이다: 물건상세 응답에서 받을 URL 을 고르는 순수 함수 [`candidate_urls`], 그리고 그것을
//! 하루 예산 안에서 받아 `<prefix>/sha256/<hash>.<ext>` 로 한 번만 저장하는 [`mirror`]. 형식은
//! 바이트 매직으로 판정하고(`foundation_shared_kernel::MediaFormat::sniff`), 키는 내용 해시라서
//! 같은 사진은 한 번만 저장된다. 다운로드 실패는 격리된다 — 미디어 미러링은 수집 run 을 깨지 않는다.
//!
//! 실물 확인(2026-10-26 운영 서비스키 호출): 사진은 `potoUrlList[].urlAdr`, 감정평가서는
//! `apslEvlClgList[].urlAdr`, 둘 다 호스트 `www.onbid.co.kr`. 만료 링크는 HTML 오류 페이지를 주므로
//! 매직바이트 판정이 걸러낸다.

use anyhow::Context as _;
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{content_addressed_key, MediaFormat, ObjectKeyPrefix, Sha256Hex};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};

/// Media objects never change once stored (the key is their content hash), so they cache forever.
const MEDIA_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// The photo and appraisal-attachment URLs of one 물건상세 response, on the allowed host only.
///
/// `detail` is the parsed `getRlstDtlInf2` response (`{"body":{"items":{"item": obj | [obj]}}}`).
/// A URL on any other host is dropped (the provider's URLs are all `www.onbid.co.kr`; refusing the
/// rest keeps a tampered or unexpected response from making us fetch an arbitrary host).
pub(crate) fn candidate_urls(detail: &JsonValue, host: &str) -> Vec<String> {
    let item = &detail["body"]["items"]["item"];
    let items = match item {
        JsonValue::Array(array) => array.clone(),
        JsonValue::Object(_) => vec![item.clone()],
        _ => Vec::new(),
    };
    let mut urls = Vec::new();
    for entry in &items {
        for field in ["potoUrlList", "apslEvlClgList"] {
            let Some(list) = entry[field].as_array() else {
                continue;
            };
            for element in list {
                if let Some(url) = element["urlAdr"].as_str() {
                    if host_is(url, host) {
                        urls.push(url.to_owned());
                    }
                }
            }
        }
    }
    urls.sort();
    urls.dedup();
    urls
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
    /// Downloads that errored; isolated, never fail the run.
    pub(crate) errored: usize,
    /// Bytes of the new objects stored this run.
    pub(crate) bytes_stored: u64,
    /// The daily download budget ran out before every URL was tried.
    pub(crate) budget_exhausted: bool,
}

enum Stored {
    New(u64),
    Deduplicated,
    Oversize,
    UnknownFormat,
}

/// Mirrors the photos and appraisal attachments of this run's 물건상세 responses.
///
/// Reads the fresh getRlstDtlInf2 response lines (before KAMCO's links expire), collects the media
/// URLs on the allowed host, and stores each once by content hash — within the day's remaining
/// budget (`daily_limit - already_downloaded`). Returns the run's [`MediaReport`] and the new
/// `media_downloads` count to persist. Every failure (a bad prefix, a download, a store) is isolated
/// and counted, so media mirroring never fails the collection run.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn mirror_detail_lines(
    detail_lines: &[String],
    object_prefix: &str,
    url_host: &str,
    max_file_bytes: u64,
    daily_limit: u32,
    already_downloaded: u32,
    storage: &dyn ObjectStorageService,
    http: &reqwest::Client,
) -> (MediaReport, u32) {
    let mut report = MediaReport::default();
    let prefix = match ObjectKeyPrefix::parse(object_prefix) {
        Ok(prefix) => prefix,
        Err(error) => {
            tracing::warn!(error = %error, "onbid media object_prefix is invalid; skipping media");
            return (report, already_downloaded);
        }
    };
    let mut urls = Vec::new();
    for line in detail_lines {
        let Ok(record) = serde_json::from_str::<JsonValue>(line) else {
            continue;
        };
        let Some(body) = record["body"].as_str() else {
            continue;
        };
        let Ok(response) = serde_json::from_str::<JsonValue>(body) else {
            continue;
        };
        urls.extend(candidate_urls(&response, url_host));
    }
    urls.sort();
    urls.dedup();

    let mut budget = daily_limit.saturating_sub(already_downloaded);
    for url in &urls {
        if budget == 0 {
            report.budget_exhausted = true;
            break;
        }
        match download_and_store(url, &prefix, storage, http, max_file_bytes).await {
            Ok(Stored::New(bytes)) => {
                budget -= 1;
                report.downloaded += 1;
                report.bytes_stored += bytes;
            }
            Ok(Stored::Deduplicated) => {
                budget -= 1;
                report.deduplicated += 1;
            }
            Ok(Stored::Oversize) => {
                budget -= 1;
                report.skipped_oversize += 1;
            }
            Ok(Stored::UnknownFormat) => {
                budget -= 1;
                report.skipped_unknown_format += 1;
            }
            Err(error) => {
                report.errored += 1;
                tracing::warn!(error = %error, "onbid media download failed; isolated from the run");
            }
        }
    }
    (report, daily_limit.saturating_sub(budget))
}

async fn download_and_store(
    url: &str,
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
    http: &reqwest::Client,
    max_file_bytes: u64,
) -> anyhow::Result<Stored> {
    let response = http.get(url).send().await.context("media GET failed")?;
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
    // The key is the content hash: if it exists, the stored bytes are identical, so skip the write.
    if storage
        .read_object_sha256(key.as_str())
        .await
        .context("media head failed")?
        .is_some()
    {
        return Ok(Stored::Deduplicated);
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
        Ok(()) => Ok(Stored::New(bytes.len() as u64)),
        // A concurrent run stored the same content first: identical bytes, so it is a dedup, not a fault.
        Err(PublishError::ObjectAlreadyExists { .. }) => Ok(Stored::Deduplicated),
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
    use super::{candidate_urls, host_is};
    use serde_json::json;

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
                "https://www.onbid.co.kr/op/appraisal.pdf".to_owned(),
                "https://www.onbid.co.kr/op/b.jpg".to_owned(),
            ],
            "only potoUrlList + apslEvlClgList urlAdr on the allowed host, deduped and sorted"
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
}
