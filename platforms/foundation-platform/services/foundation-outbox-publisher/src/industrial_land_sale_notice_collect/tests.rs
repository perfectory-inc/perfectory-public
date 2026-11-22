//! The fixtures are synthetic (ids `999…`, `가상` names, 2099 dates, a reserved host): the shapes
//! measured 2026-11-22 on the ILIS list and detail, the KICOX board and post pages, and the LH usage
//! guides' answer examples.

use anyhow::Context;
use chrono::TimeZone;
use serde_json::{json, Map, Value as JsonValue};

use super::{
    batch_file_id, fingerprint, ilis, kicox, lane_contract, last_reads, lh, list_file_id, owes,
    plan, walk_done, FileRequest, Listed, Source,
};

const KICOX_LIST: &str = include_str!("fixtures/kicox_list.html");
const KICOX_POST: &str = include_str!("fixtures/kicox_post.html");

fn row(value: JsonValue) -> anyhow::Result<Map<String, JsonValue>> {
    value.as_object().cloned().context("a row is an object")
}

fn source_rules<T: serde::de::DeserializeOwned>(source: Source) -> anyhow::Result<T> {
    Ok(serde_json::from_value(
        lane_contract()?.source_json(source)?.clone(),
    )?)
}

#[test]
fn every_source_of_the_lane_contract_parses_into_its_client() -> anyhow::Result<()> {
    let contract = lane_contract()?;
    for source in [Source::Ilis, Source::Kicox, Source::Lh] {
        let common = contract.common(source)?;
        collection_domain::source_slug(&common.provider, &common.detail_dataset_slug)?;
        assert!(!contract.fingerprint_fields(source)?.is_empty());
        assert!(contract.recent_pages(source)? >= 1);
        // The sources that take notices down or change their statuses keep their list answers.
        assert_eq!(contract.keeps_list(source)?, source != Source::Kicox);
        super::client_for(&contract, source)?;
    }
    assert!(contract.full_walk_hour_utc < 24 && contract.retry_after_hours > 0);
    assert_eq!(Source::parse("kicox")?, Source::Kicox);
    assert!(Source::parse("lh-web").is_err());
    Ok(())
}

#[test]
fn a_fingerprint_hashes_the_named_fields_in_order_and_nothing_else() {
    let fields = ["title".to_owned(), "sk_cnt".to_owned()];
    let one = json!({"title": "가상 공고", "sk_cnt": 2, "rn": 1, "hit_cnt": 5});
    let renumbered = json!({"title": "가상 공고", "sk_cnt": 2, "rn": 9, "hit_cnt": 7});
    let changed = json!({"title": "가상 공고", "sk_cnt": 3, "rn": 1});
    assert_eq!(
        fingerprint(&one, &fields),
        fingerprint(&renumbered, &fields)
    );
    assert_ne!(fingerprint(&one, &fields), fingerprint(&changed, &fields));
    assert_ne!(
        fingerprint(&one, &fields),
        fingerprint(&one, &["sk_cnt".to_owned(), "title".to_owned()])
    );
}

#[test]
fn the_last_read_of_each_notice_of_the_source_decides_whether_it_is_owed() -> anyhow::Result<()> {
    let rows = [
        row(
            json!({"notice_source": "ilis", "notice_id": "1", "list_fingerprint": "a", "read_outcome": "read", "source_snapshot_id": "20990101T000000Z", "source_line_number": 1}),
        )?,
        // A later read of the same notice that did not settle it: it is owed again, once a day.
        row(
            json!({"notice_source": "ilis", "notice_id": "1", "list_fingerprint": "a", "read_outcome": "incomplete", "source_snapshot_id": "20990102T000000Z", "source_line_number": 1}),
        )?,
        row(
            json!({"notice_source": "ilis", "notice_id": "2", "list_fingerprint": "b", "read_outcome": "read", "source_snapshot_id": "20990102T000000Z", "source_line_number": 2}),
        )?,
        // The provider said it is not there: settled.
        row(
            json!({"notice_source": "ilis", "notice_id": "4", "list_fingerprint": "d", "read_outcome": "gone", "source_snapshot_id": "20990102T000000Z", "source_line_number": 3}),
        )?,
        // Another source's read of the same id is not this source's.
        row(
            json!({"notice_source": "kicox", "notice_id": "3", "list_fingerprint": "c", "read_outcome": "read", "source_snapshot_id": "20990102T000000Z", "source_line_number": 3}),
        )?,
    ];
    let last = last_reads(rows.iter(), Source::Ilis);
    assert_eq!(last.len(), 3);
    let at = |hour| {
        chrono::Utc
            .with_ymd_and_hms(2099, 1, 2, hour, 0, 0)
            .single()
            .context("an instant")
    };
    // Failed an hour ago: not yet; a day after that run: asked again.
    assert!(!owes(last.get("1"), "a", at(1)?, 24));
    assert!(owes(
        last.get("1"),
        "a",
        at(0)? + chrono::Duration::hours(24),
        24
    ));
    assert!(!owes(last.get("2"), "b", at(1)?, 24));
    assert!(!owes(last.get("4"), "d", at(1)?, 24));
    // The provider changed the notice's list fields: it is read again at once.
    assert!(owes(last.get("2"), "b2", at(1)?, 24));
    assert!(owes(last.get("3"), "c", at(1)?, 24));
    assert!(owes(None, "a", at(1)?, 24));
    assert!(walk_done(5, Some(5)) && !walk_done(4, Some(5)) && walk_done(0, None));
    assert!(last_reads(rows.iter(), Source::Lh).is_empty());
    Ok(())
}

#[test]
fn a_taken_down_ilis_notice_answers_code_400_and_no_result() {
    // Measured 2026-11-22 (par_seq 832): HTTP 200, this body.
    assert_eq!(
        ilis::gone_code(&json!({"code": 400, "msg": "요청처리를 실패하였습니다.", "result": null})),
        Some(400)
    );
    assert_eq!(ilis::gone_code(&json!({"code": 200, "result": null})), None);
    assert_eq!(ilis::gone_code(&json!({"code": 500})), None);
}

#[test]
fn the_ilis_list_is_read_newest_first_and_a_detail_names_its_files() -> anyhow::Result<()> {
    let rules: ilis::Rules = source_rules(Source::Ilis)?;
    let answer = json!({"code": 200, "result": {"cnt": 3, "dataList": [
        {"par_seq": 99901, "title": "가상 공고 1"},
        {"par_seq": "99910", "title": "가상 공고 10"},
        {"par_seq": 99902, "title": "가상 공고 2"},
        {"title": "번호 없는 줄"}
    ]}});
    let page = ilis::list_page(&rules.list, &answer)?;
    assert_eq!(
        page.rows
            .iter()
            .map(|listed| listed.id.as_str())
            .collect::<Vec<_>>(),
        ["99910", "99902", "99901"]
    );
    assert_eq!((page.total, page.last), (Some(3), true));

    let detail = json!({"code": 200, "result": {"par_seq": 99901, "saveFiles": [
        {"file_path": "/smUpload/209901", "real_file_name": "00000000-0000-4000-8000-000000000001", "file_name": "붙임01. 공고문", "file_extension": ".pdf", "file_size": 514611, "use_yn": "Y"},
        {"file_path": "/smUpload/209901", "real_file_name": "00000000-0000-4000-8000-000000000002", "file_name": "내린 파일", "file_size": "12", "use_yn": "N"},
        {"file_name": "이름만 있는 파일", "use_yn": "Y"}
    ]}});
    let files = ilis::files_of("https://ilis.example.invalid", &rules, &detail);
    assert_eq!(files.len(), 3);
    assert_eq!(files[0].title.as_deref(), Some("붙임01. 공고문"));
    assert_eq!(files[0].provider_file_size, Some(514_611));
    assert_eq!(
        files[0].request,
        Some(FileRequest::IlisQuery(vec![
            (
                "real_file_name".to_owned(),
                "00000000-0000-4000-8000-000000000001".to_owned()
            ),
            ("file_path".to_owned(), "/smUpload/209901".to_owned()),
        ]))
    );
    assert_eq!(
        files[0].source_url.as_deref(),
        Some("https://ilis.example.invalid/cmm/file/download?real_file_name=00000000-0000-4000-8000-000000000001&file_path=%2FsmUpload%2F209901")
    );
    assert!(!files[0].withdrawn && files[1].withdrawn);
    assert_eq!(files[1].provider_file_size, Some(12));
    assert!(files[2].request.is_none() && files[2].source_url.is_none());
    Ok(())
}

#[test]
fn a_kicox_board_page_lists_its_posts_and_a_post_page_names_its_files() -> anyhow::Result<()> {
    let rules: kicox::Rules = source_rules(Source::Kicox)?;
    let patterns = kicox::Patterns::new()?;
    let page = kicox::list_page(&patterns, &rules.list, 1, KICOX_LIST);
    assert_eq!(
        page.rows,
        [
            Listed {
                id: "99902".to_owned(),
                row: json!({"bbs_seq": "99902", "title": "가상일반산업단지 산업시설용지 2차 분양공고", "registered_on": "2099-01-02", "has_attachment": true, "views": "38"}),
            },
            Listed {
                id: "99901".to_owned(),
                row: json!({"bbs_seq": "99901", "title": "가상 지원센터 & 사무실 임대 공고", "registered_on": "2099-01-01", "has_attachment": false, "views": "7"}),
            },
        ]
    );
    assert_eq!(page.total, Some(1013));
    assert!(!page.last);
    // Page 102 of ten rows covers 1,013 posts.
    assert!(kicox::list_page(&patterns, &rules.list, 102, KICOX_LIST).last);
    // The views are not part of what a read is recorded against.
    let fields = lane_contract()?.fingerprint_fields(Source::Kicox)?;
    let mut viewed = page.rows[0].row.clone();
    viewed["views"] = json!("39");
    assert_eq!(
        fingerprint(&page.rows[0].row, &fields),
        fingerprint(&viewed, &fields)
    );

    let files = kicox::files_of(&patterns, &rules, KICOX_POST)?;
    assert_eq!(files.len(), 3);
    assert_eq!(
        files[0].title.as_deref(),
        Some("01. 분양공고문.pdf [pdf, 590 KB]")
    );
    assert_eq!(
        files[0].provider_file_id.as_deref(),
        Some("99999/0f0e0d0c-0b0a-4999-8999-000000000001")
    );
    assert_eq!(
        files[0].request,
        Some(FileRequest::Url(
            "https://www.kicox.or.kr/api/fnFile/commonFileDownload/99999/0f0e0d0c-0b0a-4999-8999-000000000001".to_owned()
        ))
    );
    assert_eq!(
        files[1].title.as_deref(),
        Some("02. 신청서.hwp [hwp, 31 KB]")
    );
    assert!(files[2].request.is_none());
    // A page without a post (an error page) is not a read.
    assert!(kicox::files_of(&patterns, &rules, KICOX_LIST).is_err());
    Ok(())
}

#[test]
fn an_lh_answer_lists_notices_files_on_its_host_only_and_a_refusal_is_named() -> anyhow::Result<()>
{
    let rules: lh::Rules = source_rules(Source::Lh)?;
    let list = json!([
        {"dsSch": [{"PAN_ST_DT": "20990101", "PAGE": "1", "PG_SZ": "100"}]},
        {"resHeader": [{"SS_CODE": "Y", "RS_DTTM": "20990102000000"}],
         "dsList": [
            {"PAN_ID": "9999999902", "PAN_NM": "가상 산업시설용지 공급 공고", "UPP_AIS_TP_CD": "01", "AIS_TP_CD": "99", "SPL_INF_TP_CD": "999", "CCR_CNNT_SYS_DS_CD": "99", "PAN_SS": "공고중", "ALL_CNT": "2"},
            {"PAN_ID": "9999999901", "PAN_NM": "가상 지원시설용지 공급 공고", "UPP_AIS_TP_CD": "01", "SPL_INF_TP_CD": "999", "CCR_CNNT_SYS_DS_CD": "99", "ALL_CNT": "2"}
         ]}
    ]);
    let page = lh::list_page(&rules, 1, &list)?;
    assert_eq!(
        page.rows
            .iter()
            .map(|listed| listed.id.as_str())
            .collect::<Vec<_>>(),
        ["9999999902", "9999999901"]
    );
    assert_eq!((page.total, page.last), (Some(2), true));
    assert_eq!(
        lh::notice_params(&page.rows[0])?,
        [
            ("SPL_INF_TP_CD", "999"),
            ("CCR_CNNT_SYS_DS_CD", "99"),
            ("PAN_ID", "9999999902"),
            ("UPP_AIS_TP_CD", "01"),
            ("AIS_TP_CD", "99"),
        ]
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
    );
    assert_eq!(lh::notice_params(&page.rows[1])?.len(), 4);
    assert!(lh::list_page(&rules, 1, &json!([{"resHeader": [{"SS_CODE": "N"}]}])).is_err());

    let detail = json!([
        {"dsSch": [{"PAN_ID": "9999999902"}]},
        {"dsAhflInfo": [
            {"CMN_AHFL_NM": "가상공고문.pdf", "AHFL_URL": "https://apply.lh.or.kr/lhapply/lhFile.do?fileid=99999901", "SL_PAN_AHFL_DS_CD_NM": "공고문(PDF)"},
            {"CMN_AHFL_NM": "다른곳.pdf", "AHFL_URL": "https://elsewhere.example.invalid/f?fileid=1"}
         ],
         "resHeader": [{"SS_CODE": "Y"}]}
    ]);
    let files = lh::files_of(&rules, &detail);
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].provider_file_id.as_deref(), Some("99999901"));
    assert!(matches!(files[0].request, Some(FileRequest::Url(_))));
    // Recorded, never asked: a response cannot send the collector to another host.
    assert!(files[1].request.is_none() && files[1].source_url.is_some());

    let codes = rules.refusal_codes.clone();
    assert_eq!(
        lh::refusal(&codes, "<OpenAPI_ServiceResponse><cmmMsgHeader><returnAuthMsg>SERVICE_KEY_IS_NOT_REGISTERED_ERROR</returnAuthMsg></cmmMsgHeader></OpenAPI_ServiceResponse>").as_deref(),
        Some("SERVICE_KEY_IS_NOT_REGISTERED_ERROR")
    );
    assert_eq!(
        lh::refusal(&codes, "[{\"resHeader\":[{\"SS_CODE\":\"Y\"}]}]"),
        None
    );
    Ok(())
}

#[test]
fn a_batch_lands_under_a_content_qualified_key() -> anyhow::Result<()> {
    let at = chrono::Utc
        .with_ymd_and_hms(2099, 1, 2, 3, 4, 5)
        .single()
        .context("an instant")?;
    let id = batch_file_id("industrial_land_sale_notice_detail", at, 7);
    assert_eq!(
        id,
        "industrial-land-sale-notice-detail-20990102t030405z-007"
    );
    assert_eq!(
        list_file_id("industrial_land_sale_notice_list", 3),
        "industrial-land-sale-notice-list-0003"
    );
    let planned = plan(
        "kicox.or.kr",
        "industrial_land_sale_notice_detail",
        "detail",
        &id,
        &format!("{id}.gz"),
        b"{}\n".to_vec(),
        "application/gzip",
        foundation_shared_kernel::ids::IngestionRunId::new(uuid::Uuid::nil()),
        at,
    )?;
    let key = planned.object_key.as_str();
    assert!(
        key.contains("kicoxorkr__industrial_land_sale_notice_detail"),
        "{key}"
    );
    assert_eq!(
        collection_domain::bronze_content_object_key_checksum(key),
        Some(planned.checksum_sha256.as_str())
    );
    // The Silver load reads the run id back from the same key.
    assert_eq!(
        foundation_publisher_silver::industrial_land_sale_notice_stage::snapshot_id(key)?,
        "20990102T030405Z"
    );
    Ok(())
}
