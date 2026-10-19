use std::time::Duration;

use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{fetch, plan, provider_file_id, read_metadata, source_contract, Announced, PREFIX};
use crate::provider_request_spacing::ProviderRequestSpacing;

// Synthetic only: a made-up file id and a zip header with no real rows (public repository).
const FILE_ID: &str = "FILE_000000000000001";

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

async fn portal(id: &str, body: Vec<u8>) -> anyhow::Result<MockServer> {
    let contract = source_contract()?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(contract.file.metadata_path.clone()))
        .and(query_param("publicDataPk", "15105482"))
        .respond_with(ResponseTemplate::new(200).set_body_json(metadata(id)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(contract.file.download_path.clone()))
        .and(query_param("atchFileId", id))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .mount(&server)
        .await;
    Ok(server)
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
fn a_file_is_one_object_named_by_its_portal_id() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let announced = Announced {
        attach_file_id: FILE_ID.to_owned(),
        updated_at: None,
    };
    assert_eq!(
        provider_file_id(&announced),
        "registered-factory-file-000000000000001"
    );
    let at =
        chrono::DateTime::parse_from_rfc3339("2099-01-02T03:04:05Z")?.with_timezone(&chrono::Utc);
    let planned = plan(
        &contract,
        &announced,
        zip_bytes(),
        foundation_shared_kernel::ids::IngestionRunId::new(Uuid::new_v4()),
        at,
    )?;
    assert_eq!(
        planned.object_key.as_str(),
        "bronze/source=datagokr__registered_factory/registered-factory-file-000000000000001.zip"
    );
    assert!(PREFIX.starts_with("FOUNDATION_PLATFORM_"));
    Ok(())
}

#[tokio::test]
async fn a_new_file_is_fetched_once_and_the_landed_one_is_not_fetched_again() -> anyhow::Result<()>
{
    let contract = source_contract()?;
    let server = portal(FILE_ID, zip_bytes()).await?;
    let fetched = fetch(&contract, &server.uri(), spacing()?, None).await?;
    let (announced, bytes) = fetched.ok_or_else(|| anyhow::anyhow!("a new file is fetched"))?;
    assert_eq!(
        (announced.attach_file_id.as_str(), bytes),
        (FILE_ID, zip_bytes())
    );
    assert!(fetch(&contract, &server.uri(), spacing()?, Some(FILE_ID))
        .await?
        .is_none());
    let downloads = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| request.url.path() == contract.file.download_path)
        .count();
    assert_eq!(downloads, 1, "the landed file was not asked for again");
    Ok(())
}

#[tokio::test]
async fn a_download_that_is_not_a_zip_is_refused() -> anyhow::Result<()> {
    let contract = source_contract()?;
    let server = portal(FILE_ID, b"<html>login</html>".to_vec()).await?;
    assert!(fetch(&contract, &server.uri(), spacing()?, None)
        .await
        .is_err());
    Ok(())
}
