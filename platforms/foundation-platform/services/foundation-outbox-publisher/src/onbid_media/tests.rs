use super::{candidate_urls, host_is, run_download_bound, FIELDS_IN_ORDER, PHOTO_FIELD};
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

/// The contract's media fields (root ADR-0340 §4).
fn fields() -> crate::data_go_kr_onbid_collect::MediaFields {
    crate::data_go_kr_onbid_collect::source_contract()
        .map(|contract| contract.media.fields)
        .unwrap_or_default()
}

#[test]
fn the_maps_and_360_photos_are_media_too_and_land_beside_their_field() {
    use super::{fields_in_order, rewrite_detail_lines, StoredMedia};
    use std::collections::HashMap;

    let item = json!({
        "cltrMngNo": "A-1", "pbctCdtnNo": 1,
        "lmapUrlAdrList": "https://www.onbid.co.kr/op/map.jpg|https://www.onbid.co.kr/op/map2.jpg",
        "lrmUrlAdrList": ["https://www.onbid.co.kr/op/lrm.jpg"],
        "poto360DgrUrlList": "",
        "vdoUrlAdrList": "https://evil.example.test/v.mp4"
    });
    let media_fields = fields();
    let order = fields_in_order(&media_fields);
    assert!(
        matches!(order.first(), Some(super::Pass::Field("potoUrlList"))),
        "photos first"
    );
    assert!(
        matches!(order.get(1), Some(super::Pass::Original("potoUrlList", _))),
        "then the photos' originals"
    );
    let urls = order
        .iter()
        .flat_map(|pass| pass.urls(&detail(item.clone()), "www.onbid.co.kr"))
        .collect::<Vec<_>>();
    assert_eq!(
        urls,
        vec![
            "https://www.onbid.co.kr/op/map.jpg".to_owned(),
            "https://www.onbid.co.kr/op/map2.jpg".to_owned(),
            "https://www.onbid.co.kr/op/lrm.jpg".to_owned(),
        ]
    );
    let hash = "c".repeat(64);
    let resolved = HashMap::from([(
        "https://www.onbid.co.kr/op/map2.jpg".to_owned(),
        StoredMedia {
            sha256: hash.clone(),
            media_format: "jpg".to_owned(),
            size_bytes: 5,
            longest_px: None,
        },
    )]);
    let line = json!({"body": detail(item).to_string()}).to_string();
    let out = rewrite_detail_lines(&[line], &resolved, &fields());
    let record: serde_json::Value = serde_json::from_str(&out[0]).unwrap();
    let response: serde_json::Value =
        serde_json::from_str(record["body"].as_str().unwrap()).unwrap();
    let rewritten = &response["body"]["items"]["item"];
    // The text stays as received; the stored one is listed beside it at its position.
    assert!(rewritten["lmapUrlAdrList"]
        .as_str()
        .unwrap()
        .contains("map.jpg"));
    assert_eq!(
        rewritten["lmapUrlAdrListMedia"],
        json!([{"sequence": 1, "sha256": hash, "media_format": "jpg", "size_bytes": 5}])
    );
    assert!(rewritten.get("lrmUrlAdrListMedia").is_none());
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
    let urls = FIELDS_IN_ORDER
        .iter()
        .flat_map(|field| candidate_urls(&d, field, "www.onbid.co.kr"))
        .collect::<Vec<_>>();
    assert_eq!(
        urls,
        vec![
            "https://www.onbid.co.kr/op/a.jpg".to_owned(),
            "https://www.onbid.co.kr/op/b.jpg".to_owned(),
            "https://www.onbid.co.kr/op/appraisal.pdf".to_owned(),
        ],
        "only potoUrlList + apslEvlClgList urlAdr on the allowed host, photos first"
    );
}

#[test]
fn a_single_item_object_is_handled_like_a_one_element_list_and_a_repeat_is_asked_once() {
    let d = detail(json!({"potoUrlList": [
        {"urlAdr": "https://www.onbid.co.kr/op/only.jpg"},
        {"urlAdr": "https://www.onbid.co.kr/op/only.jpg"}
    ]}));
    assert_eq!(candidate_urls(&d, PHOTO_FIELD, "www.onbid.co.kr").len(), 1);
}

#[test]
fn no_media_fields_yields_nothing() {
    for field in FIELDS_IN_ORDER {
        assert!(candidate_urls(
            &detail(json!({"cltrMngNo": "A-1"})),
            field,
            "www.onbid.co.kr"
        )
        .is_empty());
        assert!(candidate_urls(&json!({"body": {}}), field, "www.onbid.co.kr").is_empty());
    }
}

/// A provider attachment URL of the shape the 물건상세 photo list carries (synthetic numbers): the
/// shape of every `potoUrlList[].urlAdr` in the 175 item-detail Bronze objects (2026-11-11, root
/// ADR-0486). The first version of this test wrote the web screen's `dnldImgFile.do` instead, so the
/// rule it held matched no URL the API gives and no original was ever asked for (2026-11-06..11).
const THUMBNAIL: &str = "https://www.onbid.co.kr/op/cm/syc/filemng/filemngprcs/FileMngPrcsController/dnldFile.do?atchFileLstNo=99999&atchSn=2&hashCrpsNo=99999&downloadImageKind=THNL_NM";

#[test]
fn a_thumbnail_url_becomes_its_original_and_any_other_url_has_none() {
    // 2026-11-06: the stored photos were the provider's 178x134 thumbnails (root ADR-0424).
    let media_fields = fields();
    let rule = &media_fields.originals[PHOTO_FIELD];
    assert_eq!(
        super::original_url(THUMBNAIL, rule).as_deref(),
        Some("https://www.onbid.co.kr/op/cm/syc/filemng/filemngprcs/FileMngPrcsController/dnldFile.do?atchFileLstNo=99999&atchSn=2&hashCrpsNo=99999&downloadImageKind=CLG_FILE_NM")
    );
    assert_eq!(
        super::original_url("https://www.onbid.co.kr/op/a.jpg", rule),
        None
    );
    assert_eq!(super::original_url("not a url", rule), None);
}

#[test]
fn an_original_lands_beside_its_thumbnail_unless_it_is_the_same_file() {
    use super::{original_url, rewrite_detail_lines, StoredMedia};
    use std::collections::HashMap;

    let media_fields = fields();
    let rule = &media_fields.originals[PHOTO_FIELD];
    let second = THUMBNAIL.replace("atchSn=2", "atchSn=3");
    let stored = |c: char, size| StoredMedia {
        sha256: c.to_string().repeat(64),
        media_format: "jpg".to_owned(),
        size_bytes: size,
        longest_px: None,
    };
    let resolved = HashMap::from([
        (THUMBNAIL.to_owned(), stored('a', 10)),
        (original_url(THUMBNAIL, rule).unwrap(), stored('b', 1000)),
        (second.clone(), stored('c', 20)),
        // The provider made no thumbnail of this one: the same bytes either way.
        (original_url(&second, rule).unwrap(), stored('c', 20)),
    ]);
    let body = detail(json!({"cltrMngNo": "A-1", "pbctCdtnNo": 1, "potoUrlList": [
        {"urlAdr": THUMBNAIL}, {"urlAdr": second}
    ]}));
    let line = json!({"body": body.to_string()}).to_string();
    let out = rewrite_detail_lines(&[line], &resolved, &media_fields);
    let record: serde_json::Value = serde_json::from_str(&out[0]).unwrap();
    let response: serde_json::Value =
        serde_json::from_str(record["body"].as_str().unwrap()).unwrap();
    let item = &response["body"]["items"]["item"];
    assert_eq!(
        item["potoUrlListOriginal"],
        json!([{"sequence": 0, "sha256": "b".repeat(64), "media_format": "jpg", "size_bytes": 1000}])
    );
    // The thumbnails keep their own entries, both kept (append-only, root ADR-0298).
    assert_eq!(item["potoUrlList"][0]["sha256"], json!("a".repeat(64)));
    assert_eq!(item["potoUrlList"][1]["sha256"], json!("c".repeat(64)));
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
            longest_px: None,
        },
    );
    resolved.insert(
        "https://www.onbid.co.kr/op/x.pdf".to_owned(),
        StoredMedia {
            sha256: b.clone(),
            media_format: "pdf".to_owned(),
            size_bytes: 20,
            longest_px: None,
        },
    );

    let out = rewrite_detail_lines(&[line], &resolved, &fields());
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
async fn a_line_settles_whole_reuses_stored_urls_and_stops_at_a_line_boundary() -> anyhow::Result<()>
{
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
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0xFF, 0xD8, 0xFF, byte]))
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
    let media_fields = fields();
    let settings = |per_run_limit| MirrorSettings {
        object_prefix: "onbid/media",
        url_host: &host,
        max_file_bytes: 1024,
        daily_limit: 100,
        per_run_limit,
        concurrency: 2,
        deadline: Instant::now() + Duration::from_secs(60),
        fields: &media_fields,
        renditions: None,
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

#[tokio::test]
async fn a_run_that_runs_out_mid_way_stores_every_items_photos_before_any_appraisal(
) -> anyhow::Result<()> {
    use super::{mirror_detail_lines, MirrorSettings};
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    // Each path answers its own bytes: a JPEG for `.jpg`, a PDF for `.pdf`.
    struct ByPath;
    impl Respond for ByPath {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let path = request.url.path();
            let magic: &[u8] = if path.ends_with(".pdf") {
                b"%PDF-"
            } else {
                &[0xFF, 0xD8, 0xFF]
            };
            ResponseTemplate::new(200).set_body_bytes([magic, path.as_bytes()].concat())
        }
    }
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ByPath)
        .mount(&server)
        .await;
    let base = server.uri();
    let host = reqwest::Url::parse(&base)?
        .host_str()
        .unwrap_or_default()
        .to_owned();
    // Four items in asking order, each with two photos and one appraisal.
    let lines = ["A", "B", "C", "D"]
        .iter()
        .map(|cltr| {
            let photos = (1..=2)
                .map(|n| json!({"urlAdr": format!("{base}/{cltr}{n}.jpg")}))
                .collect::<Vec<_>>();
            let body = detail(json!({
                "cltrMngNo": cltr, "pbctCdtnNo": 1, "potoUrlList": photos,
                "apslEvlClgList": [{"urlAdr": format!("{base}/{cltr}.pdf")}]
            }));
            json!({"body": body.to_string()}).to_string()
        })
        .collect::<Vec<_>>();
    let root = std::env::temp_dir().join(format!("onbid-media-{}", uuid::Uuid::new_v4()));
    let storage = foundation_outbox::FileObjectStorage::new(root)?;
    // Room for 9 downloads: one shared budget would have spent it on A, B and C whole and left D
    // without a photo; photos first spends 8 on every item's photos and 1 on A's appraisal.
    let media_fields = fields();
    let settings = MirrorSettings {
        object_prefix: "onbid/media",
        url_host: &host,
        max_file_bytes: 1024,
        daily_limit: 100,
        per_run_limit: 9,
        concurrency: 2,
        deadline: Instant::now() + Duration::from_secs(60),
        fields: &media_fields,
        renditions: None,
    };
    let mirrored = mirror_detail_lines(
        &lines,
        &[],
        &mut BTreeMap::new(),
        &settings,
        0,
        &storage,
        &reqwest::Client::new(),
    )
    .await;
    let outcomes = mirrored
        .outcomes
        .iter()
        .map(|outcome| (outcome.photos_complete, outcome.complete, outcome.photos))
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes,
        [
            (true, true, 2),
            (true, false, 2),
            (true, false, 2),
            (true, false, 2)
        ],
        "every item's photos landed; only A's appraisal did, so A alone is complete"
    );
    assert_eq!(mirrored.downloads_today, 9);
    assert!(mirrored.report.per_run_reached);
    // The last item's photos point at their blobs (the Silver photo loader reads `sha256`) though
    // its appraisal did not land and so keeps its link.
    let record: serde_json::Value = serde_json::from_str(&mirrored.lines[3])?;
    let response: serde_json::Value =
        serde_json::from_str(record["body"].as_str().unwrap_or_default())?;
    let item = &response["body"]["items"]["item"];
    assert!(item["potoUrlList"]
        .as_array()
        .is_some_and(|photos| photos.iter().all(|photo| photo["sha256"].is_string())));
    assert!(item["apslEvlClgList"][0].get("sha256").is_none());
    Ok(())
}

#[tokio::test]
async fn every_lines_thumbnails_land_before_any_original_and_an_original_completes_its_line(
) -> anyhow::Result<()> {
    use super::{mirror_detail_lines, MirrorSettings};
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    // The provider's endpoint: a thumbnail when downloadImageKind=THNL_NM, else the original.
    struct Attachment;
    impl Respond for Attachment {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let query = request.url.query().unwrap_or_default();
            let size = if query.contains("downloadImageKind=THNL_NM") {
                1
            } else {
                9
            };
            ResponseTemplate::new(200).set_body_bytes(
                [&[0xFF, 0xD8, 0xFF][..], query.as_bytes(), &vec![0; size]].concat(),
            )
        }
    }
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(Attachment)
        .mount(&server)
        .await;
    let base = server.uri();
    let host = reqwest::Url::parse(&base)?
        .host_str()
        .unwrap_or_default()
        .to_owned();
    let thumbnail = THUMBNAIL.replace("https://www.onbid.co.kr", &base);
    let lines = [("A", "atchSn=2"), ("B", "atchSn=3")]
        .iter()
        .map(|(cltr, attachment)| {
            let url = thumbnail.replace("atchSn=2", attachment);
            let body = detail(
                json!({"cltrMngNo": cltr, "pbctCdtnNo": 1, "potoUrlList": [{"urlAdr": url}]}),
            );
            json!({"body": body.to_string()}).to_string()
        })
        .collect::<Vec<_>>();
    let root = std::env::temp_dir().join(format!("onbid-media-{}", uuid::Uuid::new_v4()));
    let storage = foundation_outbox::FileObjectStorage::new(root)?;
    let media_fields = fields();
    let settings = |per_run_limit| MirrorSettings {
        object_prefix: "onbid/media",
        url_host: &host,
        max_file_bytes: 1024,
        daily_limit: 100,
        per_run_limit,
        concurrency: 2,
        deadline: Instant::now() + Duration::from_secs(60),
        fields: &media_fields,
        renditions: None,
    };
    let mut memo = BTreeMap::new();
    // Room for 2: both items' thumbnails, no original; the lines wait for a later run.
    let first = mirror_detail_lines(
        &lines,
        &[],
        &mut memo,
        &settings(2),
        0,
        &storage,
        &reqwest::Client::new(),
    )
    .await;
    let outcomes = first
        .outcomes
        .iter()
        .map(|outcome| (outcome.photos_complete, outcome.complete, outcome.originals))
        .collect::<Vec<_>>();
    assert_eq!(outcomes, [(true, false, 0), (true, false, 0)]);
    // The next run reuses the thumbnails and fetches only the originals.
    let next = mirror_detail_lines(
        &lines,
        &[],
        &mut memo,
        &settings(10),
        0,
        &storage,
        &reqwest::Client::new(),
    )
    .await;
    assert_eq!((next.report.reused, next.report.downloaded), (2, 2));
    assert!(next.outcomes.iter().all(|outcome| outcome.complete));
    // Each line's photo now shows at full size (root ADR-0486's coverage series).
    assert!(next.outcomes.iter().all(|outcome| outcome.originals == 1));
    let record: serde_json::Value = serde_json::from_str(&next.lines[1])?;
    let response: serde_json::Value =
        serde_json::from_str(record["body"].as_str().unwrap_or_default())?;
    let item = &response["body"]["items"]["item"];
    assert_eq!(item["potoUrlListOriginal"][0]["sequence"], json!(0));
    assert_ne!(
        item["potoUrlListOriginal"][0]["sha256"],
        item["potoUrlList"][0]["sha256"]
    );
    Ok(())
}

/// A real JPEG of `width` x `height` (the monitor reads its header; the ladder decodes it).
fn jpeg(width: u32, height: u32) -> Vec<u8> {
    let mut out = Vec::new();
    let pixels = vec![120_u8; usize::try_from(width * height * 3).unwrap_or_default()];
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 80)
        .encode(&pixels, width, height, image::ExtendedColorType::Rgb8)
        .unwrap_or_default();
    out
}

// Root ADR-0427 §4: the provider changed the original rule and answers with the thumbnail's size.
#[tokio::test]
async fn a_thumbnail_sized_original_is_counted_and_the_ladder_is_made_from_the_thumbnail(
) -> anyhow::Result<()> {
    use super::{mirror_detail_lines, MirrorSettings};
    use crate::data_go_kr_onbid_collect::RenditionBudget;
    use crate::media_renditions::spec;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};
    use wiremock::matchers::{method, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let thumbnail_bytes = jpeg(178, 134);
    Mock::given(method("GET"))
        .and(query_param("downloadImageKind", "THNL_NM"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(thumbnail_bytes.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("downloadImageKind", "CLG_FILE_NM"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(jpeg(160, 120)))
        .mount(&server)
        .await;
    let base = server.uri();
    let host = reqwest::Url::parse(&base)?
        .host_str()
        .unwrap_or_default()
        .to_owned();
    let url = THUMBNAIL.replace("https://www.onbid.co.kr", &base);
    let body = detail(json!({"cltrMngNo": "A", "pbctCdtnNo": 1, "potoUrlList": [{"urlAdr": url}]}));
    let lines = [json!({"body": body.to_string()}).to_string()];
    let root = std::env::temp_dir().join(format!("onbid-media-{}", uuid::Uuid::new_v4()));
    let storage = foundation_outbox::FileObjectStorage::new(&root)?;
    let media_fields = fields();
    let budget = RenditionBudget {
        per_run_limit: 10,
        concurrency: 2,
        run_time_budget_seconds: 60,
    };
    let settings = MirrorSettings {
        object_prefix: "onbid/media",
        url_host: &host,
        max_file_bytes: 1 << 20,
        daily_limit: 100,
        per_run_limit: 10,
        concurrency: 2,
        deadline: Instant::now() + Duration::from_secs(60),
        fields: &media_fields,
        renditions: Some(&budget),
    };
    let mut memo = BTreeMap::new();
    let mirrored = mirror_detail_lines(
        &lines,
        &[],
        &mut memo,
        &settings,
        0,
        &storage,
        &reqwest::Client::new(),
    )
    .await;
    let report = &mirrored.report;
    assert_eq!(
        (report.originals_attempted, report.originals_rule_mismatched),
        (1, 1)
    );
    assert_eq!((report.ladders_written, report.ladders_deferred), (1, 0));
    assert!(mirrored.outcomes[0].complete);
    assert_eq!(mirrored.outcomes[0].originals, 0);
    // Not recorded as the original, and not memoized: the screen keeps the thumbnail.
    assert_eq!(memo.len(), 1);
    let record: serde_json::Value = serde_json::from_str(&mirrored.lines[0])?;
    let response: serde_json::Value =
        serde_json::from_str(record["body"].as_str().unwrap_or_default())?;
    assert!(response["body"]["items"]["item"]
        .get("potoUrlListOriginal")
        .is_none());
    let manifest = storage.get_object_bytes(
        foundation_shared_kernel::rendition_manifest_key(
            &foundation_shared_kernel::ObjectKeyPrefix::parse("onbid/media")?,
            &spec()?.version,
            &foundation_shared_kernel::Sha256Hex::of(&thumbnail_bytes),
        )?
        .as_str(),
    )?;
    assert!(!manifest.is_empty());
    Ok(())
}
