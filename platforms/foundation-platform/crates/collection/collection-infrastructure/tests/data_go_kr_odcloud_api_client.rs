//! Contract tests for the data.go.kr `ODCloud` file API client.

use std::time::Duration;

use collection_infrastructure::{
    DataGoKrOdCloudApiClient, DataGoKrOdCloudApiConfig, DataGoKrRequestPolicy,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn odcloud_client_fetches_file_api_pages_with_page_and_per_page() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "currentCount": 1,
        "data": [
            {
                "단지명": "SYNTHETIC-COMPLEX",
                "주소": "SYNTHETIC-CITY"
            }
        ],
        "matchCount": 1,
        "page": 2,
        "perPage": 50,
        "totalCount": 1
    });
    let body = serde_json::to_vec(&payload)?;

    Mock::given(method("GET"))
        .and(path("/api/15117154/v1/uddi:sample"))
        .and(query_param("serviceKey", "decoded-service-key"))
        .and(query_param("page", "2"))
        .and(query_param("perPage", "50"))
        .and(query_param("returnType", "JSON"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrOdCloudApiClient::new_with_policy(
        &DataGoKrOdCloudApiConfig {
            base_uri: format!("{}/api", server.uri()),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let page = client.fetch_page("15117154/v1/uddi:sample", 2, 50).await?;

    assert_eq!(page.raw_payload, body);
    assert_eq!(page.payload["totalCount"], 1);
    assert_eq!(page.logical_record_count, 1);
    Ok(())
}

fn sample_client(server_uri: &str, max_attempts: u32) -> TestResult<DataGoKrOdCloudApiClient> {
    Ok(DataGoKrOdCloudApiClient::new_with_policy(
        &DataGoKrOdCloudApiConfig {
            base_uri: format!("{server_uri}/api"),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(
            max_attempts,
            Duration::from_secs(5),
            Duration::ZERO,
            Duration::ZERO,
        )?,
    )?)
}

// Golden migration-regression tests (design spec §6): pin the legacy behavior that response
// shape failures and non-retryable statuses are never retried and keep their CollectionError
// mapping, so the resilience-core migration must keep them green unchanged.

#[tokio::test]
async fn odcloud_client_does_not_retry_missing_data_array() -> TestResult {
    let server = MockServer::start().await;

    // expect(1): a multi-attempt policy must still call the provider exactly once.
    Mock::given(method("GET"))
        .and(path("/api/15117154/v1/uddi:sample"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(serde_json::to_vec(&json!({ "totalCount": 0 }))?),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = sample_client(&server.uri(), 3)?;

    let error = client
        .fetch_page("15117154/v1/uddi:sample", 1, 50)
        .await
        .err()
        .ok_or("expected missing data array failure")?;
    assert!(
        error.to_string().contains("omitted data array"),
        "unexpected error mapping: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn odcloud_client_fails_fast_on_non_retryable_http_status() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/15117154/v1/uddi:sample"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let client = sample_client(&server.uri(), 3)?;

    let error = client
        .fetch_page("15117154/v1/uddi:sample", 1, 50)
        .await
        .err()
        .ok_or("expected non-retryable HTTP failure")?;
    assert!(
        error
            .to_string()
            .contains("data.go.kr ODCloud request returned HTTP 404"),
        "unexpected error mapping: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn odcloud_client_retries_transient_failures_until_success() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({ "data": [], "totalCount": 0 });

    Mock::given(method("GET"))
        .and(path("/api/15117154/v1/uddi:sample"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/15117154/v1/uddi:sample"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(serde_json::to_vec(&payload)?),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = sample_client(&server.uri(), 3)?;

    let page = client.fetch_page("15117154/v1/uddi:sample", 1, 50).await?;
    assert_eq!(page.logical_record_count, 0);
    Ok(())
}

#[tokio::test]
async fn odcloud_client_opens_circuit_after_exhausted_transient_failure() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/15117154/v1/uddi:sample"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let client = sample_client(&server.uri(), 1)?;

    let first_error = client
        .fetch_page("15117154/v1/uddi:sample", 1, 50)
        .await
        .err()
        .ok_or("expected first transient failure")?;
    assert!(
        first_error.to_string().contains("failed after 1 attempts"),
        "unexpected first error: {first_error}"
    );

    let second_error = client
        .fetch_page("15117154/v1/uddi:sample", 1, 50)
        .await
        .err()
        .ok_or("expected circuit breaker rejection")?;
    assert!(
        second_error.to_string().contains("circuit breaker is open"),
        "unexpected second error: {second_error}"
    );
    Ok(())
}
