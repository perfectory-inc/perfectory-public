use std::{collections::HashSet, time::Duration};

use collection_infrastructure::{IlisIndustrialComplexApiClient, IlisIndustrialComplexApiConfig};
use foundation_outbox::object_storage::FileObjectStorage;
use foundation_outbox::ObjectStorageService;
use serde_json::{json, Value as JsonValue};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    attachment_prefix, frontier, listed_notices, read_notice, settled_notices, source_contract,
    source_url, Asker, AttachmentReport, ReadOutcome,
};
use crate::provider_request_spacing::ProviderRequestSpacing;

// Synthetic only: made-up notice sequences, complex codes and file ids (public repository).

fn row(
    notice: i64,
    snapshot: &str,
    line: i64,
    outcome: &str,
) -> serde_json::Map<String, JsonValue> {
    json!({
        "notice_sequence": notice,
        "source_snapshot_id": snapshot,
        "source_line_number": line,
        "read_outcome": outcome,
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

#[test]
fn a_notice_is_settled_only_by_its_newest_read() {
    let rows = [
        row(1, "20261101t000000z", 1, "read"),
        row(2, "20261101t000000z", 2, "incomplete"),
        row(2, "20261102t000000z", 1, "read"),
        row(3, "20261102t000000z", 1, "read"),
        row(3, "20261103t000000z", 1, "failed"),
    ];
    let settled = settled_notices(rows.iter());
    assert_eq!(settled, HashSet::from([1, 2]));
}

#[test]
fn the_frontier_is_the_unsettled_notices_newest_first_within_the_limit() {
    let listed = [5, 3, 9, 7, 9, 1];
    let settled = HashSet::from([7]);
    assert_eq!(frontier(&listed, &settled, 3), vec![9, 5, 3]);
    assert_eq!(frontier(&listed, &settled, 10), vec![9, 5, 3, 1]);
}

#[test]
fn the_contract_points_at_the_shared_media_store_and_its_list() -> anyhow::Result<()> {
    let contract = source_contract()?;
    assert_eq!(
        attachment_prefix(&contract.attachments)?.as_str(),
        "onbid/media"
    );
    assert_eq!(
        contract.notice_detail.read_from,
        lakehouse_domain::SILVER_INDUSTRIAL_COMPLEX_NOTICE_DETAIL.table_name
    );
    assert!(contract.notice_detail.commit_every <= contract.notice_detail.per_run_limit);
    let payload = json!({"result": {"dataList": [{"ref_seq": 99_002}, {"ref_seq": "99001"}, {}]}});
    assert_eq!(
        listed_notices(&contract.notice_list, &payload)?,
        vec![99_002, 99_001]
    );
    assert!(listed_notices(&contract.notice_list, &json!({"result": null})).is_err());
    Ok(())
}

#[tokio::test]
async fn a_notice_stores_every_attachment_once_and_annotates_its_line() -> anyhow::Result<()> {
    let mut contract = source_contract()?;
    let server = MockServer::start().await;
    contract.base_url = server.uri();
    let pdf = b"%PDF-1.7 synthetic".to_vec();
    let mut hwp = vec![0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
    hwp.extend_from_slice(b"....HWP Document File");
    let detail = json!({"code": 200, "result": {"ref_seq": 99_001, "danji_cd": "999990", "saveFiles": [
        {"real_file_name": "f-pdf", "file_path": "/up/1", "file_extension": ".pdf", "use_yn": "Y"},
        {"real_file_name": "f-hwp", "file_path": "/up/1", "file_extension": ".hwp", "use_yn": "Y"},
        {"real_file_name": "f-old", "file_path": "/up/1", "file_extension": ".pdf", "use_yn": "N"},
        {"real_file_name": "f-gone", "file_path": "/up/1", "file_extension": ".pdf", "use_yn": "Y"},
        {"real_file_name": "f-again", "file_path": "/up/1", "file_extension": ".pdf", "use_yn": "Y"}
    ]}});
    Mock::given(method("GET"))
        .and(path("/il/danref/det.do/99001"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&detail))
        .mount(&server)
        .await;
    for (name, body) in [("f-pdf", &pdf), ("f-hwp", &hwp), ("f-again", &pdf)] {
        Mock::given(method("GET"))
            .and(path("/cmm/file/download"))
            .and(query_param("real_file_name", name))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/cmm/file/download"))
        .and(query_param("real_file_name", "f-gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let client = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
        base_uri: server.uri(),
        user_agent: "test".to_owned(),
    })?;
    let mut asker = Asker {
        client: &client,
        spacing: ProviderRequestSpacing::try_new(Duration::from_millis(1))?,
        requests: 0,
    };
    let root = tempfile::tempdir()?;
    let storage = FileObjectStorage::new(root.path())?;
    let prefix = attachment_prefix(&contract.attachments)?;
    let mut report = AttachmentReport::default();
    let (outcome, line) = read_notice(
        &contract,
        &mut asker,
        99_001,
        &prefix,
        Some(&storage as &dyn ObjectStorageService),
        &mut report,
    )
    .await?;

    assert_eq!(
        outcome,
        ReadOutcome::Read,
        "gone and withdrawn files settle"
    );
    assert_eq!(line["read_outcome"], "read");
    let files = line["body"]["result"]["saveFiles"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let outcomes: Vec<&str> = files
        .iter()
        .map(|file| file["store_outcome"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        outcomes,
        ["stored", "stored", "withdrawn", "gone", "deduplicated"]
    );
    assert_eq!(files[1]["media_format"], "hwp");
    assert_eq!(files[3]["http_status"], 404);
    assert_eq!(files[0]["file_sequence"], 1);
    assert_eq!(
        files[0]["source_url"],
        source_url(
            &contract.base_url,
            &contract.attachments,
            &[
                ("real_file_name".to_owned(), "f-pdf".to_owned()),
                ("file_path".to_owned(), "/up/1".to_owned()),
            ],
        )?
    );
    let pdf_key = format!(
        "onbid/media/sha256/{}.pdf",
        files[0]["sha256"].as_str().unwrap_or_default()
    );
    assert!(storage.read_object_sha256(&pdf_key).await?.is_some());
    assert_eq!(
        (
            report.stored,
            report.deduplicated,
            report.gone,
            report.withdrawn
        ),
        (2, 1, 1, 1)
    );
    assert_eq!(
        asker.requests, 5,
        "one detail and four downloads; the withdrawn file is not asked"
    );
    Ok(())
}

#[tokio::test]
async fn a_failed_download_leaves_the_notice_owed() -> anyhow::Result<()> {
    let mut contract = source_contract()?;
    let server = MockServer::start().await;
    contract.base_url = server.uri();
    Mock::given(method("GET"))
        .and(path("/il/danref/det.do/99002"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"result": {"saveFiles": [
                {"real_file_name": "f-busy", "file_path": "/up/2", "use_yn": "Y"}
            ]}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/cmm/file/download"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/il/danref/det.do/99003"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let client = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
        base_uri: server.uri(),
        user_agent: "test".to_owned(),
    })?;
    let mut asker = Asker {
        client: &client,
        spacing: ProviderRequestSpacing::try_new(Duration::from_millis(1))?,
        requests: 0,
    };
    let prefix = attachment_prefix(&contract.attachments)?;
    let mut report = AttachmentReport::default();
    let (outcome, line) =
        read_notice(&contract, &mut asker, 99_002, &prefix, None, &mut report).await?;
    assert_eq!(outcome, ReadOutcome::Incomplete);
    assert_eq!(
        line["body"]["result"]["saveFiles"][0]["store_outcome"],
        "failed"
    );
    let (outcome, line) =
        read_notice(&contract, &mut asker, 99_003, &prefix, None, &mut report).await?;
    assert_eq!(outcome, ReadOutcome::Failed);
    assert!(line["body"].is_null());
    Ok(())
}
