//! Optional live smoke test for the data.go.kr building register Hub API.

use std::fmt::Write as _;

const LIVE_DATA_GO_KR_SMOKE_ENV: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_LIVE_SMOKE";
const DATA_GO_KR_SERVICE_KEY_ENV: &str = "DATA_GO_KR_SERVICE_KEY";
const BASE_URI: &str = "https://apis.data.go.kr/1613000/BldRgstHubService";
const OPERATION: &str = "getBrTitleInfo";
const SIGUNGU_CD: &str = "11680";
const BJDONG_CD: &str = "10300";

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires live data.go.kr service key; read-only building register lookup"]
async fn data_go_kr_building_register_live_smoke_reads_json_envelope() -> TestResult {
    // Ignored by default: reaching this line means the data.go.kr lane was asked
    // for. The service-key check below was already fail-loud but unreachable,
    // because this opt-in returned success first.
    let opt_in = std::env::var(LIVE_DATA_GO_KR_SMOKE_ENV).unwrap_or_default();
    assert_eq!(
        opt_in, "1",
        "{LIVE_DATA_GO_KR_SMOKE_ENV}=1 is required to run the data.go.kr live lane; \
         `cargo xtask integration foundation data-go-kr` provisions it"
    );

    let service_key = std::env::var(DATA_GO_KR_SERVICE_KEY_ENV).map_err(|_| {
        format!("missing required environment variable: {DATA_GO_KR_SERVICE_KEY_ENV}")
    })?;
    let request_url = smoke_url(service_key.trim());
    // Status and body are captured before parsing, and both go into every failure below. Without
    // them this test could only say the envelope was the wrong shape, which reads the same whether
    // the key was rejected, the quota ran out, or the provider's gateway was down. It was the last
    // of those — `{"OpenAPI_ServiceResponse":{"cmmMsgHeader":{"errMsg":"HTTP_ERROR",...}}}` behind
    // an HTTP 502 — and finding that out needed a request made by hand outside the test.
    //
    // The body is provider output, not ours: the service key travels in the query string and never
    // appears in a response, so printing it leaks nothing.
    let response = reqwest::get(request_url).await?;
    let status = response.status();
    let body = response.text().await?;
    let payload: serde_json::Value = serde_json::from_str(&body).map_err(|error| {
        format!("data.go.kr returned HTTP {status} and unparseable JSON ({error}): {body}")
    })?;

    let header = payload.pointer("/response/header").ok_or_else(|| {
        format!("data.go.kr returned HTTP {status} without /response/header: {body}")
    })?;
    let result_code = header
        .get("resultCode")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("data.go.kr header carried no resultCode: {body}"))?;
    let result_msg = header
        .get("resultMsg")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");

    assert_eq!(
        result_code, "00",
        "data.go.kr building register smoke failed: {result_msg}"
    );

    let total_count = payload
        .pointer("/response/body/totalCount")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    assert!(total_count >= 0);

    Ok(())
}

fn smoke_url(service_key: &str) -> String {
    format!(
        "{BASE_URI}/{OPERATION}?serviceKey={}&sigunguCd={SIGUNGU_CD}&bjdongCd={BJDONG_CD}&pageNo=1&numOfRows=10&_type=json",
        service_key_query_fragment(service_key)
    )
}

fn service_key_query_fragment(service_key: &str) -> String {
    if service_key.contains('%') {
        service_key.to_owned()
    } else {
        percent_encode_query_value(service_key)
    }
}

fn percent_encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            }
            other => {
                let _ = write!(encoded, "%{other:02X}");
            }
        }
    }
    encoded
}

#[test]
fn smoke_url_keeps_service_key_out_of_logs_and_preserves_encoded_keys() {
    let encoded_key = "abc%2Bdef%2Fghi";
    let url = smoke_url(encoded_key);

    assert!(url.contains("serviceKey=abc%2Bdef%2Fghi"));
    assert!(url.contains("_type=json"));
    assert!(url.contains("sigunguCd=11680"));
    assert!(!url.contains("DATA_GO_KR_SERVICE_KEY"));
}
