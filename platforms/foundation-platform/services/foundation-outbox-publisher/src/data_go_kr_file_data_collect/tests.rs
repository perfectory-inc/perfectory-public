use std::time::Duration;

use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_outbox::ObjectStorageService;

use super::{
    fetch, fetch_past, fetch_standard, openapi_check, owed_past, pass_id, pass_objects, plan,
    provider_file_id, read_columns, read_metadata, read_past_editions, selected, source_contract,
    Announced, ContractFile, SourceContract, PREFIX,
};
use crate::provider_request_spacing::ProviderRequestSpacing;

// Synthetic only: made-up file ids and payloads with no real rows (public repository).
const FILE_ID: &str = "FILE_000000000000001";
const OTHER_ID: &str = "FILE_000000000000002";

fn metadata(id: &str) -> serde_json::Value {
    json!({"atchFileId": id, "status": "True", "dataSetFileDetailInfo": {"updtDt": "2099-01-01 00:00:00.000"}})
}

fn zip_bytes() -> Vec<u8> {
    let mut bytes = b"PK\x03\x04".to_vec();
    bytes.extend_from_slice(b"synthetic");
    bytes
}

fn spacing() -> anyhow::Result<ProviderRequestSpacing> {
    ProviderRequestSpacing::try_new(Duration::from_millis(1))
}

fn file<'c>(contract: &'c SourceContract, slug: &str) -> anyhow::Result<&'c ContractFile> {
    Ok(selected(contract, slug)?[0])
}

async fn portal(
    file: &ContractFile,
    id: &str,
    body: Vec<u8>,
    server: &MockServer,
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    Mock::given(method("GET"))
        .and(path(contract.metadata.path.clone()))
        .and(query_param(
            "publicDataPk",
            file.metadata_query["publicDataPk"].clone(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(metadata(id)))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(contract.download.path.clone()))
        .and(query_param("atchFileId", id))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(server)
        .await;
    Ok(())
}

#[test]
fn the_metadata_names_the_file_and_nothing_else_is_trusted() -> anyhow::Result<()> {
    let announced = read_metadata(&metadata(FILE_ID).to_string())?;
    assert_eq!(announced.attach_file_id, FILE_ID);
    assert!(read_metadata(r#"{"status": "True"}"#).is_err());
    assert!(read_metadata(r#"{"atchFileId": "../../etc"}"#).is_err());
    assert!(read_metadata("<html>").is_err());
    Ok(())
}

#[test]
fn a_job_names_its_files_and_an_unknown_or_repeated_name_refuses() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let picked = selected(&contract, "airport, port")?;
    assert_eq!(
        picked
            .iter()
            .map(|f| f.dataset_slug.as_str())
            .collect::<Vec<_>>(),
        ["airport", "port"]
    );
    assert!(selected(&contract, "airport,airport").is_err());
    assert!(selected(&contract, "no_such_file").is_err());
    assert!(selected(&contract, " , ").is_err());
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn every_file_is_its_own_source_and_one_object_named_by_its_bytes() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let announced = Announced {
        attach_file_id: FILE_ID.to_owned(),
        updated_at: None,
    };
    let at =
        chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")?.with_timezone(&chrono::Utc);
    let run = foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4());
    // The ledger keeps the portal's file id; the key names the dataset and the bytes (root ADR-0152).
    let factory = file(&contract, "registered_factory")?;
    assert_eq!(
        provider_file_id(factory, &announced),
        "registered-factory-file-000000000000001"
    );
    assert_eq!(
        plan(factory, zip_bytes(), run, at)?.object_key.as_str(),
        format!(
            "bronze/source=datagokr__registered_factory/registered-factory--sha256-{}.zip",
            sha256(&zip_bytes())
        )
    );
    let airport = plan(file(&contract, "airport")?, b"a,b\n".to_vec(), run, at)?;
    assert_eq!(
        (airport.object_key.as_str(), airport.content_type.as_str()),
        (
            format!(
                "bronze/source=datagokr__airport/airport--sha256-{}.csv",
                sha256(b"a,b\n")
            )
            .as_str(),
            "text/csv"
        )
    );
    // The same bytes planned on another day, in another run, are the same object.
    let later =
        chrono::DateTime::parse_from_rfc3339("2099-02-03T04:05:06Z")?.with_timezone(&chrono::Utc);
    let again = foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4());
    assert_eq!(
        plan(file(&contract, "airport")?, b"a,b\n".to_vec(), again, later)?
            .object_key
            .as_str(),
        airport.object_key.as_str()
    );
    for each in contract.files.values() {
        // Every file plans a canonical Bronze key (the slug is the catalog's dataset_slug).
        plan(each, zip_bytes(), run, at)?;
    }
    assert!(PREFIX.starts_with("FOUNDATION_PLATFORM_"));
    Ok(())
}

#[tokio::test]
async fn a_new_file_is_fetched_once_and_the_landed_one_is_not_fetched_again() -> anyhow::Result<()>
{
    let contract = source_contract()?;
    let factory = file(&contract, "registered_factory")?;
    let airport = file(&contract, "airport")?;
    let server = MockServer::start().await;
    portal(factory, FILE_ID, zip_bytes(), &server).await?;
    portal(airport, OTHER_ID, "공항명\n".as_bytes().to_vec(), &server).await?;
    let mut requests = 0;
    let fetched = fetch(
        &contract,
        factory,
        &server.uri(),
        spacing()?,
        &mut requests,
        None,
    )
    .await?;
    let (announced, bytes) = fetched.ok_or_else(|| anyhow::anyhow!("a new file is fetched"))?;
    assert_eq!(
        (announced.attach_file_id.as_str(), bytes),
        (FILE_ID, zip_bytes())
    );
    assert!(fetch(
        &contract,
        factory,
        &server.uri(),
        spacing()?,
        &mut requests,
        Some(FILE_ID)
    )
    .await?
    .is_none());
    let other = fetch(
        &contract,
        airport,
        &server.uri(),
        spacing()?,
        &mut requests,
        None,
    )
    .await?;
    assert_eq!(
        other.map(|(a, _)| a.attach_file_id).as_deref(),
        Some(OTHER_ID)
    );
    assert_eq!(
        requests, 5,
        "one metadata ask per file per check, one download per new file"
    );
    let downloads = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| request.url.path() == contract.download.path)
        .count();
    assert_eq!(downloads, 2, "the landed file was not asked for again");
    Ok(())
}

#[tokio::test]
async fn a_download_that_is_not_the_file_is_refused() -> anyhow::Result<()> {
    let contract = source_contract()?;
    for (slug, body) in [
        ("registered_factory", b"<html>login</html>".to_vec()),
        ("airport", b"  <!DOCTYPE html><html>notice</html>".to_vec()),
        ("airport", Vec::new()),
    ] {
        let target = file(&contract, slug)?;
        let server = MockServer::start().await;
        portal(target, FILE_ID, body, &server).await?;
        let mut requests = 0;
        assert!(
            fetch(
                &contract,
                target,
                &server.uri(),
                spacing()?,
                &mut requests,
                None
            )
            .await
            .is_err(),
            "{slug}"
        );
    }
    Ok(())
}

/// A synthetic column-list answer: the service table, its column list and the row count.
fn columns_answer(total: u64) -> serde_json::Value {
    json!({
        "fileName": "합성표준데이터",
        "totalCount": total,
        "columList": [{"columCode": "SCHOOL_ID", "columNm": "학교ID"}],
        "tableVO": {"svcTableNm": "tn_pubr_synthetic_svc", "colNmList": ["SCHOOL_ID", "LNMADR"]}
    })
}

fn synthetic_school(id: &str) -> serde_json::Value {
    json!({"SCHOOL_ID": id, "LNMADR": "합성도 합성시 가상동 1", "INSTT_NM": "합성기관"})
}

/// Mounts the keyless download: the column list, then each page's rows (`perPage` the contract's).
async fn standard_download(
    file: &ContractFile,
    total: u64,
    pages: &[Vec<serde_json::Value>],
    server: &MockServer,
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let download = &contract.standard_download;
    let pk = file
        .standard
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("a standard file"))?
        .public_data_pk
        .clone();
    Mock::given(method("GET"))
        .and(path(download.columns_path.clone()))
        .and(query_param("pk", pk.clone()))
        .and(query_param("ext", "CSV"))
        .respond_with(ResponseTemplate::new(200).set_body_json(columns_answer(total)))
        .mount(server)
        .await;
    for (index, rows) in pages.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path(download.rows_path.clone()))
            .and(query_param("publicDataPk", pk.clone()))
            .and(query_param("svcTableNm", "tn_pubr_synthetic_svc"))
            .and(query_param("colNmList", "LNMADR"))
            .and(query_param("perPage", download.page_size.to_string()))
            .and(query_param("page", (index + 1).to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(rows))
            .mount(server)
            .await;
    }
    Ok(())
}

#[tokio::test]
async fn a_standard_data_pass_asks_the_current_table_and_lands_only_when_its_rows_change(
) -> anyhow::Result<()> {
    use std::io::Read;
    let contract = source_contract()?;
    let school = file(&contract, "school_location")?;
    let server = MockServer::start().await;
    let rows = [
        synthetic_school("S9999000001"),
        synthetic_school("S9999000002"),
        synthetic_school("S9999000003"),
    ];
    standard_download(school, 3, &[rows.to_vec()], &server).await?;
    let mut requests = 0;
    let (announced, count, lines) = fetch_standard(
        &contract,
        school,
        &server.uri(),
        spacing()?,
        &mut requests,
        None,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("a new pass lands"))?;
    assert_eq!((requests, count), (2, 3), "the column list, then one page");
    // The edition is its rows, whatever order the pages give them in.
    let reordered = [rows[2].clone(), rows[0].clone(), rows[1].clone()];
    assert_eq!(announced.attach_file_id, pass_id(&reordered));
    let kinds = lines
        .iter()
        .map(|line| Ok(serde_json::from_str::<serde_json::Value>(line)?["kind"].clone()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(
        kinds,
        [json!("columns"), json!("rows")],
        "every answer's raw line"
    );
    assert!(fetch_standard(
        &contract,
        school,
        &server.uri(),
        spacing()?,
        &mut requests,
        Some(&announced.attach_file_id),
    )
    .await?
    .is_none());
    let at =
        chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")?.with_timezone(&chrono::Utc);
    let dir = tempfile::tempdir()?;
    let mut objects = pass_objects::<PgBronzeIngestUnitOfWork, dyn ObjectStorageService>(
        &contract,
        school,
        &announced,
        at,
        dir.path(),
        None,
    )?;
    objects.push_group(&lines).await?;
    let landed = objects.finish().await?;
    assert_eq!(landed.len(), 1, "a pass is one object");
    assert!(landed[0]
        .object_key
        .starts_with("bronze/source=datagokr__school_location/school-location--sha256-"));
    let mut text = String::new();
    flate2::read::GzDecoder::new(
        std::fs::read(
            dir.path()
                .join(landed[0].local_path.as_deref().unwrap_or_default()),
        )?
        .as_slice(),
    )
    .read_to_string(&mut text)?;
    assert_eq!(
        text,
        format!(
            "{}
",
            lines.join(
                "
"
            )
        )
    );
    Ok(())
}

#[tokio::test]
async fn a_standard_data_pass_refuses_a_short_pass_and_a_table_past_its_cap() -> anyhow::Result<()>
{
    let contract = source_contract()?;
    let school = file(&contract, "school_location")?;
    let mut requests = 0;
    // Rows that end before the column list's total are not a pass.
    let short = MockServer::start().await;
    standard_download(school, 5, &[vec![synthetic_school("S9999000001")]], &short).await?;
    assert!(fetch_standard(
        &contract,
        school,
        &short.uri(),
        spacing()?,
        &mut requests,
        None
    )
    .await
    .is_err());
    // A table needing more pages than the cap refuses before any row is asked.
    let long = MockServer::start().await;
    let cap = school.standard.as_ref().map_or(0, |std| std.max_pages);
    standard_download(
        school,
        (cap + 1) * contract.standard_download.page_size,
        &[],
        &long,
    )
    .await?;
    let before = requests;
    let error = fetch_standard(
        &contract,
        school,
        &long.uri(),
        spacing()?,
        &mut requests,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a table past its cap refuses"))?;
    assert!(format!("{error:#}").contains("per-run cap"));
    assert_eq!(requests - before, 1, "only the column list was asked");
    Ok(())
}

#[tokio::test]
async fn the_openapi_only_cross_checks_and_never_fails_a_file() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let school = file(&contract, "school_location")?;
    let mut requests = 0;
    assert_eq!(
        openapi_check(
            &contract,
            school,
            "http://127.0.0.1:9",
            None,
            spacing()?,
            &mut requests,
            3
        )
        .await["status"],
        "skipped",
        "no key, no request"
    );
    assert_eq!(requests, 0);
    // The gateway's refusal of an unregistered key is recorded, not an error.
    let refused = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({"OpenAPI_ServiceResponse": {
            "cmmMsgHeader": {"errMsg": "SERVICE_KEY_IS_NOT_REGISTERED_ERROR", "returnReasonCode": "30"}}})))
        .mount(&refused)
        .await;
    let check = openapi_check(
        &contract,
        school,
        &refused.uri(),
        Some("synthetic-key"),
        spacing()?,
        &mut requests,
        3,
    )
    .await;
    assert_eq!(check["status"], "skipped");
    assert!(check["reason"].as_str().is_some_and(|reason| reason
        .contains("SERVICE_KEY_IS_NOT_REGISTERED_ERROR")
        && !reason.contains("synthetic-key")));
    // A registered key compares the gateway's total with the landed pass.
    let api = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            school
                .openapi
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("an openapi operation"))?
                .path
                .clone(),
        ))
        .and(query_param("numOfRows", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"response": {
            "header": {"resultCode": "00", "resultMsg": "NORMAL_SERVICE"},
            "body": {"items": [synthetic_school("S9999000001")], "totalCount": "3", "numOfRows": "1", "pageNo": "1"}
        }})))
        .mount(&api)
        .await;
    for (rows, status) in [(3, "matches"), (4, "differs")] {
        let check = openapi_check(
            &contract,
            school,
            &api.uri(),
            Some("synthetic-key"),
            spacing()?,
            &mut requests,
            rows,
        )
        .await;
        assert_eq!(check["status"], status);
    }
    Ok(())
}

#[test]
fn the_column_list_names_the_current_table_or_refuses() -> anyhow::Result<()> {
    let (table, columns, total) = read_columns(&columns_answer(12).to_string())?;
    assert_eq!(
        (table.as_str(), columns, total),
        (
            "tn_pubr_synthetic_svc",
            vec!["SCHOOL_ID".to_owned(), "LNMADR".to_owned()],
            12
        )
    );
    assert!(read_columns(
        r#"{"totalCount": 1, "tableVO": {"svcTableNm": "x; drop", "colNmList": ["A"]}}"#
    )
    .is_err());
    assert!(read_columns(
        r#"{"totalCount": 1, "tableVO": {"svcTableNm": "tn_x", "colNmList": []}}"#
    )
    .is_err());
    assert!(read_columns("<html>").is_err());
    Ok(())
}

/// A synthetic 주기성 과거 데이터 answer: newest first, as the portal lists them, behind a header row.
fn past_list(rows: &[(&str, &str)]) -> String {
    let body = rows
        .iter()
        .map(|(key, day)| {
            format!(
                r##"<tr><td><a href="#layer_data_infomation" class="openFileDetailPopup open-modal" data-public-pk="{key}" data-public-detail-sn="1">합성 파일_20990101</a></td><td>{day}</td></tr>"##
            )
        })
        .collect::<String>();
    format!("<div><p>미리보기</p><table><thead><tr><th>데이터명</th><th>등록일</th></tr></thead><tbody>{body}</tbody></table></div>")
}

const PAST_A: &str = "uddi:00000000-0000-4000-8000-00000000000a";
const PAST_B: &str = "uddi:00000000-0000-4000-8000-00000000000b";
const PAST_C: &str = "uddi:00000000-0000-4000-8000-00000000000c_209901011200";

#[test]
fn the_past_edition_list_is_read_oldest_first_and_a_page_that_is_not_it_refuses(
) -> anyhow::Result<()> {
    let listed = read_past_editions(&past_list(&[
        (PAST_C, "2099-03-01"),
        (PAST_A, "2099-01-01"),
        (PAST_B, "2099-02-01"),
    ]))?;
    assert_eq!(
        listed
            .iter()
            .map(|e| (e.detail_pk.as_str(), e.registered.as_str()))
            .collect::<Vec<_>>(),
        [
            (PAST_A, "2099-01-01"),
            (PAST_B, "2099-02-01"),
            (PAST_C, "2099-03-01")
        ]
    );
    assert!(read_past_editions("<html><body>점검 중</body></html>").is_err());
    assert!(read_past_editions(&past_list(&[("../../etc", "2099-01-01")])).is_err());
    assert!(read_past_editions(&past_list(&[(PAST_A, "")])).is_err());
    // A run asks what it has not asked, oldest first, up to its cap.
    let asked: std::collections::BTreeMap<String, String> =
        [(PAST_A.to_owned(), FILE_ID.to_owned())]
            .into_iter()
            .collect();
    assert_eq!(
        owed_past(&listed, &asked, 1)
            .iter()
            .map(|e| e.detail_pk.as_str())
            .collect::<Vec<_>>(),
        [PAST_B]
    );
    assert_eq!(owed_past(&listed, &asked, 5).len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_past_edition_is_asked_by_its_own_key_lands_as_what_arrived_and_a_landed_file_is_not_fetched_again(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let insurance = file(&contract, "employment_insurance_workplace")?;
    assert!(insurance.past_editions.is_some());
    let server = MockServer::start().await;
    for (key, id) in [(PAST_A, FILE_ID), (PAST_B, OTHER_ID)] {
        Mock::given(method("GET"))
            .and(path(contract.metadata.path.clone()))
            .and(query_param("publicDataDetailPk", key))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata(id)))
            .mount(&server)
            .await;
    }
    // An earlier edition of a zip file that arrives as one CSV.
    Mock::given(method("GET"))
        .and(path(contract.download.path.clone()))
        .and(query_param("atchFileId", FILE_ID))
        .respond_with(ResponseTemplate::new(200).set_body_bytes("사업장명\n".as_bytes().to_vec()))
        .mount(&server)
        .await;
    let editions = read_past_editions(&past_list(&[
        (PAST_B, "2099-02-01"),
        (PAST_A, "2099-01-01"),
    ]))?;
    let mut requests = 0;
    let landed = std::collections::BTreeSet::from([OTHER_ID.to_owned()]);
    let (announced, bytes) = fetch_past(
        &contract,
        insurance,
        &editions[0],
        &server.uri(),
        spacing()?,
        &mut requests,
        &landed,
    )
    .await?;
    let bytes = bytes.ok_or_else(|| anyhow::anyhow!("a new edition is fetched"))?;
    let at =
        chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")?.with_timezone(&chrono::Utc);
    let run = foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4());
    assert_eq!(announced.attach_file_id, FILE_ID);
    let planned = plan(insurance, bytes, run, at)?;
    assert_eq!(
        (planned.object_key.as_str(), planned.content_type.as_str()),
        (
            format!(
                "bronze/source=datagokr__employment_insurance_workplace/employment-insurance-workplace--sha256-{}.csv",
                sha256("사업장명\n".as_bytes())
            )
            .as_str(),
            "text/csv"
        )
    );
    let (same, none) = fetch_past(
        &contract,
        insurance,
        &editions[1],
        &server.uri(),
        spacing()?,
        &mut requests,
        &landed,
    )
    .await?;
    assert_eq!((same.attach_file_id.as_str(), none), (OTHER_ID, None));
    assert_eq!(requests, 3, "two metadata asks and one download");
    Ok(())
}
