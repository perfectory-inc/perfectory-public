//! Contract tests for the `apis.data.go.kr` service API client.

use std::{collections::BTreeMap, time::Duration};

use collection_application::{
    PublicDataBronzePageRequest, PublicDataFixedQueryParam, PublicDataPartitionField,
};
use collection_infrastructure::{
    DataGoKrRequestPolicy, DataGoKrServiceApiClient, DataGoKrServiceApiConfig,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn service_api_client_fetches_any_apis_data_go_kr_operation_with_query_params() -> TestResult
{
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "000",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": {
                "items": {
                    "item": [
                        {
                            "tradeId": "11680-202605-1"
                        }
                    ]
                },
                "totalCount": 1
            }
        }
    });
    let body = serde_json::to_vec(&payload)?;

    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .and(query_param("serviceKey", "decoded-service-key"))
        .and(query_param("LAWD_CD", "11680"))
        .and(query_param("DEAL_YMD", "202605"))
        .and(query_param("pageNo", "3"))
        .and(query_param("numOfRows", "100"))
        .and(query_param("_type", "json"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let page = client
        .fetch_page(&PublicDataBronzePageRequest {
            operation: "getTradeInfo".to_owned(),
            partition_fields: vec![PublicDataPartitionField {
                name: "lawd".to_owned(),
                value: "11680".to_owned(),
            }],
            query_params: BTreeMap::from([
                ("LAWD_CD".to_owned(), "11680".to_owned()),
                ("DEAL_YMD".to_owned(), "202605".to_owned()),
            ]),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "_type".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: 3,
            num_of_rows: 100,
        })
        .await?;

    assert_eq!(page.raw_payload, body);
    assert_eq!(page.payload["response"]["header"]["resultCode"], "000");
    Ok(())
}

#[tokio::test]
async fn service_api_client_uses_request_specific_format_query_param_name() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "00",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": {
                "items": {
                    "item": []
                },
                "totalCount": 0
            }
        }
    });
    let body = serde_json::to_vec(&payload)?;

    Mock::given(method("GET"))
        .and(path("/getRTMSDataSvcInduTrade"))
        .and(query_param("serviceKey", "decoded-service-key"))
        .and(query_param("LAWD_CD", "11680"))
        .and(query_param("DEAL_YMD", "202605"))
        .and(query_param("pageNo", "1"))
        .and(query_param("numOfRows", "100"))
        .and(query_param("type", "json"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(1, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let page = client
        .fetch_page(&PublicDataBronzePageRequest {
            operation: "getRTMSDataSvcInduTrade".to_owned(),
            partition_fields: vec![PublicDataPartitionField {
                name: "lawd".to_owned(),
                value: "11680".to_owned(),
            }],
            query_params: BTreeMap::from([
                ("LAWD_CD".to_owned(), "11680".to_owned()),
                ("DEAL_YMD".to_owned(), "202605".to_owned()),
            ]),
            format_query_param: Some(PublicDataFixedQueryParam {
                name: "type".to_owned(),
                value: "json".to_owned(),
            }),
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        })
        .await?;

    assert_eq!(page.raw_payload, body);
    Ok(())
}

fn sample_request() -> PublicDataBronzePageRequest {
    PublicDataBronzePageRequest {
        operation: "getTradeInfo".to_owned(),
        partition_fields: vec![PublicDataPartitionField {
            name: "lawd".to_owned(),
            value: "11680".to_owned(),
        }],
        query_params: BTreeMap::from([
            ("LAWD_CD".to_owned(), "11680".to_owned()),
            ("DEAL_YMD".to_owned(), "202605".to_owned()),
        ]),
        format_query_param: Some(PublicDataFixedQueryParam {
            name: "_type".to_owned(),
            value: "json".to_owned(),
        }),
        page_param_name: "pageNo".to_owned(),
        size_param_name: "numOfRows".to_owned(),
        page_no: 1,
        num_of_rows: 100,
    }
}

// Golden migration-regression tests (design spec §6): the next four tests pin the legacy
// client behavior — provider envelope errors are never retried, transient failures are
// retried with an exact attempt count, and non-retryable failures map to `CollectionError`
// verbatim — so the resilience-core migration must keep them green unchanged.

#[tokio::test]
async fn service_api_client_does_not_retry_provider_envelope_errors() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "22",
                "resultMsg": "LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS ERROR."
            }
        }
    });

    // expect(1): a multi-attempt policy must still call the provider exactly once.
    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(serde_json::to_vec(&payload)?),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let error = client
        .fetch_page(&sample_request())
        .await
        .err()
        .ok_or("expected envelope error")?;
    assert!(
        error
            .to_string()
            .contains("resultCode=22 resultMsg=LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS ERROR."),
        "unexpected envelope error mapping: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn service_api_client_retries_transient_failures_with_exact_attempt_count() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "000",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": { "items": { "item": [] }, "totalCount": 0 }
        }
    });

    // Two transient failures, then success: a 3-attempt policy must consume exactly 2 + 1.
    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(serde_json::to_vec(&payload)?),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let page = client.fetch_page(&sample_request()).await?;
    assert_eq!(page.payload["response"]["header"]["resultCode"], "000");
    Ok(())
}

#[tokio::test]
async fn service_api_client_fails_fast_on_non_retryable_http_status() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let error = client
        .fetch_page(&sample_request())
        .await
        .err()
        .ok_or("expected non-retryable HTTP failure")?;
    assert!(
        error
            .to_string()
            .contains("data.go.kr service API request returned HTTP 404"),
        "unexpected error mapping: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn service_api_client_does_not_retry_json_parse_failures() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/html")
                .set_body_string("<html>not json</html>"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let error = client
        .fetch_page(&sample_request())
        .await
        .err()
        .ok_or("expected JSON parse failure")?;
    assert!(
        error
            .to_string()
            .contains("data.go.kr service API response JSON parse failed"),
        "unexpected error mapping: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn service_api_client_opens_circuit_after_exhausted_transient_failure() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(1, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;
    let request = PublicDataBronzePageRequest {
        operation: "getTradeInfo".to_owned(),
        partition_fields: vec![PublicDataPartitionField {
            name: "lawd".to_owned(),
            value: "11680".to_owned(),
        }],
        query_params: BTreeMap::from([
            ("LAWD_CD".to_owned(), "11680".to_owned()),
            ("DEAL_YMD".to_owned(), "202605".to_owned()),
        ]),
        format_query_param: Some(PublicDataFixedQueryParam {
            name: "_type".to_owned(),
            value: "json".to_owned(),
        }),
        page_param_name: "pageNo".to_owned(),
        size_param_name: "numOfRows".to_owned(),
        page_no: 1,
        num_of_rows: 100,
    };

    let first_error = client
        .fetch_page(&request)
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected first transient failure"))?;
    assert!(
        first_error.to_string().contains("failed after 1 attempts"),
        "unexpected first error: {first_error}"
    );

    let second_error = client
        .fetch_page(&request)
        .await
        .err()
        .ok_or("expected circuit breaker failure")?;
    assert!(
        second_error.to_string().contains("circuit breaker is open"),
        "unexpected second error: {second_error}"
    );

    Ok(())
}

#[tokio::test]
async fn service_api_client_allows_configured_circuit_failure_threshold() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/getTradeInfo"))
        .respond_with(ResponseTemplate::new(503))
        .expect(2)
        .mount(&server)
        .await;

    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(1, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?
            .with_circuit_breaker(2, Duration::from_secs(30))?,
    )?;
    let request = PublicDataBronzePageRequest {
        operation: "getTradeInfo".to_owned(),
        partition_fields: vec![PublicDataPartitionField {
            name: "lawd".to_owned(),
            value: "11680".to_owned(),
        }],
        query_params: BTreeMap::from([
            ("LAWD_CD".to_owned(), "11680".to_owned()),
            ("DEAL_YMD".to_owned(), "202605".to_owned()),
        ]),
        format_query_param: Some(PublicDataFixedQueryParam {
            name: "_type".to_owned(),
            value: "json".to_owned(),
        }),
        page_param_name: "pageNo".to_owned(),
        size_param_name: "numOfRows".to_owned(),
        page_no: 1,
        num_of_rows: 100,
    };

    let first_error = client
        .fetch_page(&request)
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected first transient failure"))?;
    assert!(
        first_error.to_string().contains("failed after 1 attempts"),
        "unexpected first error: {first_error}"
    );

    let second_error = client
        .fetch_page(&request)
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected second transient failure"))?;
    assert!(
        !second_error.to_string().contains("circuit breaker is open"),
        "unexpected second error: {second_error}"
    );

    Ok(())
}

// Security regression (audit 2026-06-12 / Codex finding 1): a transport failure must not leak
// the decoded service key value into the error string. reqwest's Display appends the full
// request URL — which carries serviceKey=<value> — so the client must redact query-param
// values at its error boundary before the message reaches any error chain, log, or evidence.
#[tokio::test]
async fn service_api_transport_error_does_not_leak_service_key_value() -> TestResult {
    // Bind then drop a listener to obtain a definitely-closed loopback port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let closed_port = listener.local_addr()?.port();
    drop(listener);

    let secret = "SUPER-SECRET-DECODED-KEY-9f8e7d6c";
    let client = DataGoKrServiceApiClient::new_with_policy(
        &DataGoKrServiceApiConfig {
            base_uri: format!("http://127.0.0.1:{closed_port}"),
            service_key: secret.to_owned(),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        DataGoKrRequestPolicy::new(1, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let error = client
        .fetch_page(&PublicDataBronzePageRequest {
            operation: "getTradeInfo".to_owned(),
            partition_fields: vec![PublicDataPartitionField {
                name: "lawd".to_owned(),
                value: "11680".to_owned(),
            }],
            query_params: BTreeMap::new(),
            format_query_param: None,
            page_param_name: "pageNo".to_owned(),
            size_param_name: "numOfRows".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        })
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected a transport failure against closed port"))?;

    let message = error.to_string();
    assert!(
        !message.contains(secret),
        "service key value leaked into transport error: {message}"
    );
    assert!(
        message.contains("serviceKey=[redacted]") || !message.contains("serviceKey="),
        "if the URL appears, the service key must be redacted: {message}"
    );
    Ok(())
}
