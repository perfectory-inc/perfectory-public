//! Contract tests for the `VWorld` NED attribute API client.

use std::{collections::BTreeMap, time::Duration};

use collection_infrastructure::{
    VWorldNedAttributeClient, VWorldNedAttributeConfig, VWorldRequestPolicy,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn ned_attribute_client_fetches_ladfrl_list_json_with_key_domain_and_pagination() -> TestResult
{
    let server = MockServer::start().await;
    let payload = json!({
        "ladfrlVOList": {
            "field": [
                {
                    "pnu": "9999900601100010000",
                    "ldCodeNm": "SYNTHETIC-DISTRICT-ALPHA",
                    "lndcgrCodeNm": "site"
                }
            ],
            "pageNo": "1",
            "numOfRows": "10",
            "totalCount": "1"
        }
    });
    let body = serde_json::to_vec(&payload)?;

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .and(query_param("key", "vworld-api-key"))
        .and(query_param("domain", "localhost"))
        .and(query_param("format", "json"))
        .and(query_param("pnu", "9999900601100010000"))
        .and(query_param("pageNo", "1"))
        .and(query_param("numOfRows", "10"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server, Some("localhost"))?;

    let page = client
        .fetch_json_page(
            "ladfrlList",
            &BTreeMap::from([("pnu".to_owned(), "9999900601100010000".to_owned())]),
            1,
            10,
        )
        .await?;

    assert_eq!(page.raw_payload, body);
    assert_eq!(
        page.payload["ladfrlVOList"]["field"][0]["pnu"],
        "9999900601100010000"
    );
    Ok(())
}

#[tokio::test]
async fn ned_attribute_client_accepts_ladfrl_list_success_with_empty_error_fields() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "ladfrlVOList": {
            "pageNo": "1",
            "ladfrlVOList": [
                {
                    "pnu": "9999900801105800001",
                    "ldCodeNm": "SYNTHETIC-DISTRICT-BETA",
                    "lndpclAr": "52887.4"
                }
            ],
            "totalCount": "1",
            "error": "",
            "message": "",
            "numOfRows": "10"
        }
    });

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .and(query_param("key", "vworld-api-key"))
        .and(query_param("format", "json"))
        .and(query_param("pnu", "9999900801105800001"))
        .and(query_param("pageNo", "1"))
        .and(query_param("numOfRows", "10"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = test_client(&server, None)?;

    let page = client
        .fetch_json_page(
            "ladfrlList",
            &BTreeMap::from([("pnu".to_owned(), "9999900801105800001".to_owned())]),
            1,
            10,
        )
        .await?;

    assert_eq!(
        page.payload["ladfrlVOList"]["ladfrlVOList"][0]["pnu"],
        "9999900801105800001"
    );
    Ok(())
}

#[tokio::test]
async fn ned_attribute_client_rejects_vworld_error_envelope_without_retrying() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "status": "PARAM_REQUIRED",
            "message": "key is required"
        }
    });

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server, None)?;

    let error = client
        .fetch_json_page(
            "ladfrlList",
            &BTreeMap::from([("pnu".to_owned(), "9999900601100010000".to_owned())]),
            1,
            10,
        )
        .await
        .err()
        .ok_or("expected VWorld provider error")?;

    assert!(
        error.to_string().contains("PARAM_REQUIRED"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn ned_attribute_client_rejects_result_code_error_envelope_without_retrying() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "resultCode": "URL_TYPE",
            "resultMsg": "invalid URL"
        }
    });

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .and(query_param("key", "vworld-api-key"))
        .and(query_param("format", "json"))
        .and(query_param("pnu", "9999900601100010000"))
        .and(query_param("pageNo", "1"))
        .and(query_param("numOfRows", "10"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = test_client(&server, None)?;

    let error = client
        .fetch_json_page(
            "ladfrlList",
            &BTreeMap::from([("pnu".to_owned(), "9999900601100010000".to_owned())]),
            1,
            10,
        )
        .await
        .err()
        .ok_or_else(|| {
            std::io::Error::other("VWorld resultCode error envelope must fail the page fetch")
        })?;

    assert!(
        error.to_string().contains("URL_TYPE"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn ned_attribute_client_rejects_ladfrl_list_error_envelope_without_retrying() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "ladfrlVOList": {
            "error": "PARAM_REQUIRED",
            "message": "key is required"
        }
    });

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .and(query_param("key", "vworld-api-key"))
        .and(query_param("format", "json"))
        .and(query_param("pnu", "9999900601100010000"))
        .and(query_param("pageNo", "1"))
        .and(query_param("numOfRows", "10"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = test_client(&server, None)?;

    let error = client
        .fetch_json_page(
            "ladfrlList",
            &BTreeMap::from([("pnu".to_owned(), "9999900601100010000".to_owned())]),
            1,
            10,
        )
        .await
        .err()
        .ok_or("VWorld NED error envelope must fail the page fetch")?;

    assert!(
        error.to_string().contains("PARAM_REQUIRED"),
        "unexpected error: {error}"
    );
    Ok(())
}

// Golden migration-regression tests (design spec §6): pin retry attempt counts and
// non-retryable status mapping before the resilience-core swap.

#[tokio::test]
async fn ned_attribute_client_retries_transient_failures_with_exact_attempt_count() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({ "ladfrlVOList": { "ladfrlVOList": [] } });

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(serde_json::to_vec(&payload)?),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server, None)?;
    let page = client
        .fetch_json_page("ladfrlList", &BTreeMap::new(), 1, 100)
        .await?;
    assert_eq!(page.payload, payload);
    Ok(())
}

#[tokio::test]
async fn ned_attribute_client_fails_fast_on_non_retryable_http_status() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/ned/data/ladfrlList"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server, None)?;
    let error = client
        .fetch_json_page("ladfrlList", &BTreeMap::new(), 1, 100)
        .await
        .err()
        .ok_or("expected non-retryable HTTP failure")?;
    assert!(
        error
            .to_string()
            .contains("VWorld NED attribute request returned HTTP 404"),
        "unexpected error mapping: {error}"
    );
    Ok(())
}

fn test_client(
    server: &MockServer,
    domain: Option<&str>,
) -> Result<VWorldNedAttributeClient, Box<dyn std::error::Error + Send + Sync>> {
    Ok(VWorldNedAttributeClient::new_with_policy(
        &VWorldNedAttributeConfig {
            base_uri: format!("{}/ned/data", server.uri()),
            api_key: "vworld-api-key".to_owned(),
            domain: domain.map(ToOwned::to_owned),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        VWorldRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?)
}
