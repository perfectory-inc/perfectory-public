use std::time::Duration;

use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{
    fetch, fetch_pages, pass_id, plan, provider_file_id, read_metadata, selected, source_contract,
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

/// A synthetic 표준데이터 answer: `response` wrapper, `body.items` the array (no real rows).
fn standard_page(rows: &[serde_json::Value], total: usize) -> serde_json::Value {
    json!({"response": {
        "header": {"resultCode": "00", "resultMsg": "NORMAL_SERVICE"},
        "body": {"items": rows, "totalCount": total.to_string(), "numOfRows": "2", "pageNo": "1"}
    }})
}

fn synthetic_school(id: &str) -> serde_json::Value {
    json!({"schoolId": id, "schoolNm": "합성학교", "lnmadr": "합성도 합성시 가상동 1"})
}

async fn standard_api(
    file: &ContractFile,
    pages: &[serde_json::Value],
    server: &MockServer,
) -> anyhow::Result<()> {
    let api = file
        .openapi
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("an openapi file"))?;
    for (index, page) in pages.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path(api.path.clone()))
            .and(query_param("pageNo", (index + 1).to_string()))
            .and(query_param("type", "json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(page))
            .mount(server)
            .await;
    }
    Ok(())
}

#[tokio::test]
async fn a_standard_data_pass_reads_every_page_and_lands_only_when_its_rows_change(
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
    standard_api(
        school,
        &[standard_page(&rows[..2], 3), standard_page(&rows[2..], 3)],
        &server,
    )
    .await?;
    let mut requests = 0;
    let (announced, bytes) = fetch_pages(
        &contract,
        school,
        &server.uri(),
        Some("synthetic-key"),
        spacing()?,
        &mut requests,
        None,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("a new pass lands"))?;
    assert_eq!(
        requests, 2,
        "one request per page, stopping at the reported total"
    );
    // The edition is its rows, whatever order the pages give them in.
    let reordered = [rows[2].clone(), rows[0].clone(), rows[1].clone()];
    assert_eq!(announced.attach_file_id, pass_id(&reordered));
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes.as_slice()).read_to_string(&mut text)?;
    assert_eq!(text.lines().count(), 2, "every page's raw line");
    assert!(
        !text.contains("synthetic-key"),
        "the key never reaches Bronze"
    );
    assert!(fetch_pages(
        &contract,
        school,
        &server.uri(),
        Some("synthetic-key"),
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
async fn a_standard_data_pass_refuses_a_refusal_a_short_pass_and_no_key() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let school = file(&contract, "school_location")?;
    let mut requests = 0;
    // The gateway's refusal of an unregistered key is an error naming it, not "no rows".
    let refused = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({"OpenAPI_ServiceResponse": {
            "cmmMsgHeader": {"errMsg": "SERVICE_KEY_IS_NOT_REGISTERED_ERROR", "returnReasonCode": "30"}}})))
        .mount(&refused)
        .await;
    let error = fetch_pages(
        &contract,
        school,
        &refused.uri(),
        Some("k"),
        spacing()?,
        &mut requests,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("a refusal fails"))?;
    assert!(format!("{error:#}").contains("SERVICE_KEY_IS_NOT_REGISTERED_ERROR"));
    // Rows that end before the reported total are not a pass.
    let short = MockServer::start().await;
    standard_api(
        school,
        &[
            standard_page(&[synthetic_school("S9999000001")], 5),
            standard_page(&[], 5),
        ],
        &short,
    )
    .await?;
    assert!(fetch_pages(
        &contract,
        school,
        &short.uri(),
        Some("k"),
        spacing()?,
        &mut requests,
        None
    )
    .await
    .is_err());
    // More pages than the cap refuse rather than run on.
    let long = MockServer::start().await;
    let cap = school.openapi.as_ref().map_or(0, |api| api.max_pages);
    let pages = (0..cap)
        .map(|_| standard_page(&[synthetic_school("S9999000001")], 1_000_000))
        .collect::<Vec<_>>();
    standard_api(school, &pages, &long).await?;
    assert!(fetch_pages(
        &contract,
        school,
        &long.uri(),
        Some("k"),
        spacing()?,
        &mut requests,
        None
    )
    .await
    .is_err());
    assert!(fetch_pages(
        &contract,
        school,
        &long.uri(),
        None,
        spacing()?,
        &mut requests,
        None
    )
    .await
    .is_err());
    Ok(())
}
