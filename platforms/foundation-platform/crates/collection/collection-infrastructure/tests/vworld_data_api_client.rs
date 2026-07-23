//! Contract tests for the `VWorld` 2D Data API client.

use std::time::Duration;

use collection_infrastructure::{
    VWorldDataApiClient, VWorldDataApiConfig, VWorldDataFeatureRequest, VWorldRequestPolicy,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn data_api_client_fetches_cadastral_geojson_with_attr_filter_and_domain() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "service": {
                "name": "data",
                "version": "2.0",
                "operation": "GetFeature"
            },
            "status": "OK",
            "record": {
                "total": "1",
                "current": "1"
            },
            "page": {
                "total": "1",
                "current": "1",
                "size": "10"
            },
            "result": {
                "featureCollection": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {
                                "pnu": "9999900801105800001",
                                "jibun": "580-1 site"
                            },
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": []
                            }
                        }
                    ]
                }
            }
        }
    });
    let body = serde_json::to_vec(&payload)?;

    Mock::given(method("GET"))
        .and(path("/req/data"))
        .and(query_param("key", "vworld-api-key"))
        .and(query_param("domain", "localhost"))
        .and(query_param("service", "data"))
        .and(query_param("request", "GetFeature"))
        .and(query_param("data", "LP_PA_CBND_BUBUN"))
        .and(query_param("format", "json"))
        .and(query_param("attrFilter", "pnu:=:9999900801105800001"))
        .and(query_param("columns", "pnu,jibun,bonbun,bubun,ag_geom"))
        .and(query_param("geometry", "true"))
        .and(query_param("attribute", "true"))
        .and(query_param("crs", "EPSG:4326"))
        .and(query_param("page", "1"))
        .and(query_param("size", "10"))
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
        .fetch_feature_page(&VWorldDataFeatureRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: Some("pnu:=:9999900801105800001".to_owned()),
            columns: vec![
                "pnu".to_owned(),
                "jibun".to_owned(),
                "bonbun".to_owned(),
                "bubun".to_owned(),
                "ag_geom".to_owned(),
            ],
            geometry: true,
            attribute: true,
            crs: Some("EPSG:4326".to_owned()),
            page: 1,
            size: 10,
        })
        .await?;

    assert_eq!(page.raw_payload, body);
    assert_eq!(page.payload["response"]["status"], "OK");
    assert_eq!(
        page.payload["response"]["result"]["featureCollection"]["features"][0]["properties"]["pnu"],
        "9999900801105800001"
    );
    Ok(())
}

#[tokio::test]
async fn data_api_client_rejects_error_envelope_without_retrying() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "status": "ERROR",
            "error": {
                "code": "INCORRECT_KEY",
                "text": "domain does not match"
            }
        }
    });

    Mock::given(method("GET"))
        .and(path("/req/data"))
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
        .fetch_feature_page(&VWorldDataFeatureRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: Some("pnu:=:9999900801105800001".to_owned()),
            columns: Vec::new(),
            geometry: true,
            attribute: true,
            crs: None,
            page: 1,
            size: 10,
        })
        .await
        .err()
        .ok_or("VWorld Data API error envelope must fail the page fetch")?;

    assert!(
        error.to_string().contains("INCORRECT_KEY"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn data_api_client_opens_circuit_after_exhausted_transient_failure() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/req/data"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDataApiClient::new_with_policy(
        &VWorldDataApiConfig {
            base_uri: server.uri(),
            api_key: "vworld-api-key".to_owned(),
            domain: None,
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        VWorldRequestPolicy::new(1, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;
    let request = VWorldDataFeatureRequest {
        dataset: "LP_PA_CBND_BUBUN".to_owned(),
        attr_filter: Some("pnu:=:9999900801105800001".to_owned()),
        columns: Vec::new(),
        geometry: true,
        attribute: true,
        crs: None,
        page: 1,
        size: 10,
    };

    let first_error = client
        .fetch_feature_page(&request)
        .await
        .err()
        .ok_or("expected first transient failure")?;
    assert!(
        first_error.to_string().contains("failed after 1 attempts"),
        "unexpected first error: {first_error}"
    );

    let second_error = client
        .fetch_feature_page(&request)
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
async fn data_api_client_rejects_missing_attr_filter() -> TestResult {
    let server = MockServer::start().await;
    let client = test_client(&server, None)?;

    let error = client
        .fetch_feature_page(&VWorldDataFeatureRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: None,
            columns: Vec::new(),
            geometry: true,
            attribute: true,
            crs: None,
            page: 1,
            size: 10,
        })
        .await
        .err()
        .ok_or("VWorld Data API request without attr_filter must fail")?;

    assert!(
        error.to_string().contains("attr_filter is required"),
        "unexpected error: {error}"
    );
    Ok(())
}

// Golden migration-regression tests (design spec §6): pin retry attempt counts and
// non-retryable status mapping before the resilience-core swap.

#[tokio::test]
async fn data_api_client_retries_transient_failures_with_exact_attempt_count() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({ "response": { "status": "OK" } });

    Mock::given(method("GET"))
        .and(path("/req/data"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/req/data"))
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
        .fetch_feature_page(&VWorldDataFeatureRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: Some("pnu:=:9999900801105800001".to_owned()),
            columns: Vec::new(),
            geometry: true,
            attribute: true,
            crs: None,
            page: 1,
            size: 10,
        })
        .await?;
    assert_eq!(page.payload, payload);
    Ok(())
}

#[tokio::test]
async fn data_api_client_fails_fast_on_non_retryable_http_status() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/req/data"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server, None)?;
    let error = client
        .fetch_feature_page(&VWorldDataFeatureRequest {
            dataset: "LP_PA_CBND_BUBUN".to_owned(),
            attr_filter: Some("pnu:=:9999900801105800001".to_owned()),
            columns: Vec::new(),
            geometry: true,
            attribute: true,
            crs: None,
            page: 1,
            size: 10,
        })
        .await
        .err()
        .ok_or("expected non-retryable HTTP failure")?;
    assert!(
        error
            .to_string()
            .contains("VWorld Data API request returned HTTP 404"),
        "unexpected error mapping: {error}"
    );
    Ok(())
}

fn test_client(
    server: &MockServer,
    domain: Option<&str>,
) -> Result<VWorldDataApiClient, Box<dyn std::error::Error + Send + Sync>> {
    Ok(VWorldDataApiClient::new_with_policy(
        &VWorldDataApiConfig {
            base_uri: server.uri(),
            api_key: "vworld-api-key".to_owned(),
            domain: domain.map(ToOwned::to_owned),
            user_agent: "foundation-platform-test/1.0".to_owned(),
        },
        VWorldRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?)
}
