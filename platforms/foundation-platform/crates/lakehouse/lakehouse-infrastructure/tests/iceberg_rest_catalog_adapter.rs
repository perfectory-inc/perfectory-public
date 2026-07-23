//! Contract tests for the Iceberg REST catalog adapter.

use std::error::Error;

use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_domain::SILVER_INDUSTRIAL_COMPLEXES;
use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogConfig, LakehouseCatalogProvider,
};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> LakehouseCatalogConfig {
    LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        catalog_uri: server.uri(),
        warehouse: "foundation-platform".to_owned(),
        catalog_token: Some("secret-token".to_owned()),
    }
}

fn config_with_uri(catalog_uri: String) -> LakehouseCatalogConfig {
    LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        catalog_uri,
        warehouse: "foundation-platform".to_owned(),
        catalog_token: Some("secret-token".to_owned()),
    }
}

async fn mount_catalog_config(server: &MockServer, prefix: &str) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(query_param("warehouse", "foundation-platform"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header("x-iceberg-access-delegation", "vended-credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "overrides": {
                "prefix": prefix
            },
            "defaults": {}
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn loads_current_snapshot_through_catalog_config_prefix() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path("/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00001.json",
            "metadata": {
                "current-snapshot-id": 123_456_789
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;

    assert_eq!(snapshot.table_name, "silver.industrial_complexes");
    assert_eq!(snapshot.snapshot_id, "123456789");
    assert_eq!(
        snapshot.metadata_location,
        "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00001.json"
    );
    Ok(())
}

#[tokio::test]
async fn accepts_catalog_uri_that_already_ends_with_v1() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00001.json",
            "metadata": {
                "current-snapshot-id": 123_456_789
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config_with_uri(format!("{}/v1", server.uri())))?;
    let snapshot = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await?;

    assert!(snapshot.is_some());
    Ok(())
}

#[tokio::test]
async fn missing_table_returns_none_instead_of_infrastructure_error() -> Result<(), Box<dyn Error>>
{
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .and(header("authorization", "Bearer secret-token"))
        .and(header("x-iceberg-access-delegation", "vended-credentials"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .get_current_snapshot(SILVER_INDUSTRIAL_COMPLEXES.table_name)
        .await?;

    assert!(snapshot.is_none());
    Ok(())
}

#[tokio::test]
async fn load_table_retries_transient_failures_until_success() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // One transient 503, then the real table: the adapter must retry past the failure.
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00003.json",
            "metadata": {
                "current-snapshot-id": 42
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist after retry"))?;

    assert_eq!(snapshot.snapshot_id, "42");
    Ok(())
}

#[tokio::test]
async fn load_table_does_not_retry_malformed_json_payloads() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // expect(1): a decode failure is fatal — only transport-level body failures retry.
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("<html>not json</html>"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let error = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected JSON decode failure"))?;

    assert!(
        !error.to_string().contains("attempts"),
        "decode failures must not be retried: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn load_table_fails_fast_on_non_retryable_status() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    // expect(1): a 401 must not be retried even though the policy allows multiple attempts.
    Mock::given(method("GET"))
        .and(path(
            "/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes",
        ))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let error = catalog
        .get_current_snapshot("silver.industrial_complexes")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected non-retryable load table failure"))?;

    assert!(
        error
            .to_string()
            .contains("Iceberg REST load table failed with status 401"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn ensure_table_returns_existing_snapshot_without_cloudflare_business_api(
) -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    mount_catalog_config(&server, "cloudflare-catalog-prefix").await;
    Mock::given(method("GET"))
        .and(path("/v1/cloudflare-catalog-prefix/namespaces/silver/tables/industrial_complexes"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header(
            "x-iceberg-access-delegation",
            "vended-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metadata-location": "r2://foundation-platform-lakehouse/silver/industrial_complexes/metadata/00002.json",
            "metadata": {
                "current-snapshot-id": "987654321"
            }
        })))
        .mount(&server)
        .await;

    let catalog = IcebergRestCatalog::new(config(&server))?;
    let snapshot = catalog.ensure_table(&SILVER_INDUSTRIAL_COMPLEXES).await?;

    assert_eq!(snapshot.table_name, "silver.industrial_complexes");
    assert_eq!(snapshot.snapshot_id, "987654321");
    Ok(())
}
