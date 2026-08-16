//! Contract tests for the ILIS (industryland.or.kr) industrial-complex API client.

use collection_infrastructure::{
    IlisIndustrialComplexApiClient, IlisIndustrialComplexApiConfig, IlisRequestPolicy,
};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TEST_USER_AGENT: &str = "foundation-platform-industrial-complex-address-source/1.0";

fn config(base_uri: String) -> IlisIndustrialComplexApiConfig {
    IlisIndustrialComplexApiConfig {
        base_uri,
        user_agent: TEST_USER_AGENT.to_owned(),
    }
}

#[tokio::test]
async fn ilis_client_posts_the_page_body_and_returns_the_raw_bytes() -> TestResult {
    let server = MockServer::start().await;
    // Synthetic shape only: a captured provider response is private operational evidence.
    let payload = json!({ "result": { "dataList": [{ "danji_cd": "99999" }] } });
    let body = serde_json::to_vec(&payload)?;
    Mock::given(method("POST"))
        .and(path("/il/danji/list.do"))
        .and(header("user-agent", TEST_USER_AGENT))
        .and(body_json(json!({ "pageNo": 1, "pageSize": 2000 })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = IlisIndustrialComplexApiClient::new(&config(server.uri()))?;

    let page = client.fetch_page("il/danji/list.do", 1, 2000).await?;

    assert_eq!(page.raw_payload, body);
    assert_eq!(page.payload, payload);
    Ok(())
}

/// The detail fallback is a GET with the complex code in the path, and it is the only reason this
/// client has a second verb at all: the bulk list omits a handful of complexes the profile carries.
#[tokio::test]
async fn ilis_client_fetches_one_complex_detail_by_code() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({ "result": { "data": { "danji_cd": "99999" } } });
    Mock::given(method("GET"))
        .and(path("/il/danji/det/info.do/99999"))
        .and(header("user-agent", TEST_USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(serde_json::to_vec(&payload)?))
        .expect(1)
        .mount(&server)
        .await;
    let client = IlisIndustrialComplexApiClient::new(&config(server.uri()))?;

    let page = client.fetch_detail("il/danji/det/info.do", "99999").await?;

    assert_eq!(page.payload, payload);
    Ok(())
}

/// A complex code reaches the provider inside a URL path, so anything path-shaped must be refused
/// before the request rather than sent.
#[tokio::test]
async fn ilis_client_rejects_a_path_shaped_complex_code() -> TestResult {
    let server = MockServer::start().await;
    let client = IlisIndustrialComplexApiClient::new(&config(server.uri()))?;

    for code in ["", "  ", "../../etc/passwd", "99999/extra", "99 999"] {
        let error = client
            .fetch_detail("il/danji/det/info.do", code)
            .await
            .err()
            .ok_or("expected a rejected complex code")?;
        assert!(
            error.to_string().contains("ASCII alphanumeric"),
            "unexpected error for {code:?}: {error}"
        );
    }
    Ok(())
}

/// The single attempt is the point: this lane's request budget is counted by hand, so one failing
/// request must not quietly become several at the provider.
#[tokio::test]
async fn ilis_client_does_not_retry_a_failed_request_behind_the_callers_back() -> TestResult {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/il/danji/list.do"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let client = IlisIndustrialComplexApiClient::new(&config(server.uri()))?;

    let error = client
        .fetch_page("il/danji/list.do", 1, 2000)
        .await
        .err()
        .ok_or("expected a 500 to fail the call")?;

    assert!(
        error.to_string().contains("500"),
        "unexpected error: {error}"
    );
    // Dropping the server verifies the `.expect(1)` above: exactly one request was served.
    drop(server);
    Ok(())
}

#[tokio::test]
async fn ilis_client_rejects_a_blank_user_agent() -> TestResult {
    let error = IlisIndustrialComplexApiClient::new(&IlisIndustrialComplexApiConfig {
        base_uri: "https://example.invalid".to_owned(),
        user_agent: "   ".to_owned(),
    })
    .err()
    .ok_or("expected a blank user agent to be rejected")?;

    assert!(
        error.to_string().contains("user_agent"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn ilis_client_rejects_a_zero_page_size_before_any_request() -> TestResult {
    let server = MockServer::start().await;
    let client = IlisIndustrialComplexApiClient::new(&config(server.uri()))?;

    let error = client
        .fetch_page("il/danji/list.do", 1, 0)
        .await
        .err()
        .ok_or("expected a zero page size to be rejected")?;

    assert!(
        error.to_string().contains("pageSize"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn ilis_client_rejects_a_non_json_body() -> TestResult {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/il/danji/list.do"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>maintenance</html>"))
        .mount(&server)
        .await;
    let client = IlisIndustrialComplexApiClient::new(&config(server.uri()))?;

    let error = client
        .fetch_page("il/danji/list.do", 1, 2000)
        .await
        .err()
        .ok_or("expected an HTML body to fail the call")?;

    assert!(
        error.to_string().contains("JSON parse failed"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn ilis_default_policy_allows_exactly_one_attempt() {
    assert_eq!(IlisRequestPolicy::default().max_attempts, 1);
}
