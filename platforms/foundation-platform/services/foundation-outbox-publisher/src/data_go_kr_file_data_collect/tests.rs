use std::time::Duration;

use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    fetch, fetch_eum, fetch_standard, openapi_check, pass_id, plan, provider_file_id, read_columns,
    read_eum_file_info, read_eum_list, read_metadata, selected, source_contract, Announced,
    ContractFile, SourceContract, PREFIX,
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

#[test]
fn every_file_is_its_own_source_and_one_object_named_by_its_portal_id() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let announced = Announced {
        attach_file_id: FILE_ID.to_owned(),
        updated_at: None,
    };
    let at =
        chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")?.with_timezone(&chrono::Utc);
    let run = foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4());
    // The registered factory keeps the key it had before ADR-0401.
    let factory = file(&contract, "registered_factory")?;
    assert_eq!(
        provider_file_id(factory, &announced),
        "registered-factory-file-000000000000001"
    );
    assert_eq!(
        plan(factory, &announced, zip_bytes(), run, at)?
            .object_key
            .as_str(),
        "bronze/source=datagokr__registered_factory/registered-factory-file-000000000000001.zip"
    );
    let airport = plan(
        file(&contract, "airport")?,
        &announced,
        b"a,b\n".to_vec(),
        run,
        at,
    )?;
    assert_eq!(
        (airport.object_key.as_str(), airport.content_type.as_str()),
        (
            "bronze/source=datagokr__airport/airport-file-000000000000001.csv",
            "text/csv"
        )
    );
    for each in contract.files.values() {
        // Every file plans a canonical Bronze key (the slug is the catalog's dataset_slug).
        plan(each, &announced, zip_bytes(), run, at)?;
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

/// A synthetic 토지이음 list page: two files, the newer first, as the page writes them.
const EUM_LIST: &[u8] = b"<td class=\"left\">synthetic_20990201.zip</td><a onclick=\"$.fn.dataDownload('902');\"><td class=\"left\">synthetic_20990101.zip</td><a onclick=\"$.fn.dataDownload('901');\">";

/// A synthetic 토지이음 file-information answer: JSON whose `fileList` is XML with escaped closing tags.
fn eum_file_info(size: usize) -> String {
    format!(
        r#"{{"fileList":"<?xml version='1.0' encoding='EUC-KR' ?><root><node><fileId>902<\/fileId><fileNm>999_20990201_00000.zip<\/fileNm><fileSize>{size}<\/fileSize><key>SyNtHeTiC%2Fkey.0f<\/key><refDt>20990201<\/refDt><\/node><\/root>"}}"#
    )
}

#[test]
fn the_eum_list_names_its_newest_file_and_the_file_information_its_one_time_key(
) -> anyhow::Result<()> {
    assert_eq!(read_eum_list(EUM_LIST)?, 902);
    assert!(read_eum_list(b"<html>no files</html>").is_err());
    let download = read_eum_file_info(eum_file_info(9).as_bytes())?;
    assert_eq!(
        (
            download.key.as_str(),
            download.file_name.as_str(),
            download.size,
            download.reference_date.as_deref()
        ),
        (
            "SyNtHeTiC%2Fkey.0f",
            "999_20990201_00000.zip",
            9,
            Some("20990201")
        )
    );
    assert!(read_eum_file_info(b"<!doctype html><title>error</title>").is_err());
    assert!(read_eum_file_info(
        eum_file_info(9)
            .replace("SyNtHeTiC%2Fkey.0f", "a&b=c")
            .as_bytes()
    )
    .is_err());
    assert!(read_eum_file_info(
        eum_file_info(9)
            .replace("999_20990201_00000.zip", "../x.zip")
            .as_bytes()
    )
    .is_err());
    Ok(())
}

#[tokio::test]
async fn an_eum_file_is_fetched_by_its_one_time_key_only_when_its_list_names_a_new_one(
) -> anyhow::Result<()> {
    let contract = source_contract()?;
    let target = file(&contract, "land_use_activity_restriction")?;
    let eum = &contract.eum_open_data;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(eum.list_path.clone()))
        .and(query_param("dataCd", "007"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(EUM_LIST.to_vec()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(eum.file_info_path.clone()))
        .and(wiremock::matchers::header_exists("referer"))
        .and(wiremock::matchers::body_string_contains("fileId=902"))
        .respond_with(ResponseTemplate::new(200).set_body_string(eum_file_info(zip_bytes().len())))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(eum.download_path.clone()))
        .and(query_param("filename", "999_20990201_00000.zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zip_bytes()))
        .mount(&server)
        .await;
    let mut requests = 0;
    let uri = server.uri();
    let (announced, bytes) = fetch_eum(
        &contract,
        target,
        (&uri, &uri),
        spacing()?,
        &mut requests,
        None,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("a new file is fetched"))?;
    assert_eq!(
        (
            announced.attach_file_id.as_str(),
            announced.updated_at.as_deref(),
            bytes
        ),
        ("EUM_007_902", Some("20990201"), zip_bytes())
    );
    assert_eq!(requests, 3, "the list, the file information, the download");
    assert!(fetch_eum(
        &contract,
        target,
        (&uri, &uri),
        spacing()?,
        &mut requests,
        Some("EUM_007_902")
    )
    .await?
    .is_none());
    assert_eq!(requests, 4, "a landed file is only listed again");
    Ok(())
}

#[tokio::test]
async fn an_eum_download_shorter_than_announced_or_not_a_zip_is_refused() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let target = file(&contract, "development_activity_permit")?;
    let eum = &contract.eum_open_data;
    for (announced, body) in [
        (zip_bytes().len() + 1, zip_bytes()),
        (17, b"<html>error</html>".to_vec()),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(eum.list_path.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(EUM_LIST.to_vec()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(eum.file_info_path.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_string(eum_file_info(announced)))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(eum.download_path.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
        let mut requests = 0;
        let uri = server.uri();
        assert!(fetch_eum(
            &contract,
            target,
            (&uri, &uri),
            spacing()?,
            &mut requests,
            None
        )
        .await
        .is_err());
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
    let (announced, count, bytes) = fetch_standard(
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
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes.as_slice()).read_to_string(&mut text)?;
    let kinds = text
        .lines()
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
    let planned = plan(
        school,
        &announced,
        bytes,
        foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4()),
        at,
    )?;
    assert!(planned
        .object_key
        .as_str()
        .starts_with("bronze/source=datagokr__school_location/school-location-rows-"));
    assert_eq!(planned.content_type, "application/gzip");
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
