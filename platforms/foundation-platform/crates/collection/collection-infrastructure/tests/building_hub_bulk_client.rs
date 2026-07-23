//! Contract tests for the `hub.go.kr` bulk-file client.

use collection_infrastructure::{
    parse_building_hub_bulk_inventory, BuildingHubBulkClient, BuildingHubBulkConfig,
    BuildingHubBulkDownloadRequest,
};
use wiremock::matchers::{body_string_contains, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn hub_bulk_inventory_parser_extracts_official_file_identity() -> TestResult {
    let html = r#"
        <li>
            <div class="block">
                <p class="tagset danger">building-register</p>
            </div>
            <div class="block flex-1">
                <p class="tit">main-title-register (2026-04)</p>
                <div class="position">
                    <span>provider-file-period</span>
                    <span class="detail">2026-05</span>
                </div>
            </div>
            <div class="block">
                <button type="button" onclick="javascript:fnLgcptPop('04','0403','main-title-register (2026-04)','provider')">detail</button>
                <button type="button" onclick="fnDownloadPop('04','0403','OPN209912310000000008')">download all</button>
            </div>
        </li>
    "#;

    let items = parse_building_hub_bulk_inventory(html)?;

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].category_name, "building-register");
    assert_eq!(items[0].service_name, "main-title-register");
    assert_eq!(items[0].service_period_label, "2026-04");
    assert_eq!(items[0].provider_file_period, "2026-05");
    assert_eq!(items[0].task_group_code, "04");
    assert_eq!(items[0].task_code, "0403");
    assert_eq!(items[0].file_id, "OPN209912310000000008");
    Ok(())
}

#[test]
fn hub_bulk_inventory_parser_ignores_script_function_definition() -> TestResult {
    let html = r#"
        <script>
        function fnDownloadPop(a,b,c) {
            return a + b + c;
        }
        </script>
        <li>
            <p class="tagset danger">building-register</p>
            <p class="tit">main title (2026-04)</p>
            <span class="detail">2026-05</span>
            <button type="button" onclick="fnDownloadPop('03','0303','OPN209912310000000008')">download</button>
        </li>
    "#;

    let items = parse_building_hub_bulk_inventory(html)?;

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].task_group_code, "03");
    assert_eq!(items[0].task_code, "0303");
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_fetches_all_inventory_pages() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"))
        .and(query_param("pageIndex", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"
            <li>
                <p class="tagset danger">building-register</p>
                <p class="tit">main title (2026-04)</p>
                <span class="detail">2026-05</span>
                <button type="button" onclick="fnDownloadPop('03','0303','OPN209912310000000008')">download</button>
            </li>
            <div class="pagination">
                <a href="?pageIndex=2" onclick="searchList(2);return false;">2</a>
            </div>
            "#,
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"))
        .and(query_param("pageIndex", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"
            <li>
                <p class="tagset danger">energy-monthly</p>
                <p class="tit">electricity usage (2026-01)</p>
                <span class="detail">2026-05</span>
                <button type="button" onclick="fnDownloadPop('08','0801','OPN209912310000000006')">download</button>
            </li>
            "#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let items = client.fetch_inventory().await?;

    assert_eq!(items.len(), 2);
    assert_eq!(items[0].task_group_code, "03");
    assert_eq!(items[0].task_code, "0303");
    assert_eq!(items[1].task_group_code, "08");
    assert_eq!(items[1].task_code, "0801");
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_retries_transient_inventory_failures_until_success() -> TestResult {
    let server = MockServer::start().await;

    // One transient 503, then the real inventory page: the walk must retry past the failure.
    Mock::given(method("GET"))
        .and(path("/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"
            <li>
                <p class="tagset danger">building-register</p>
                <p class="tit">main title (2026-04)</p>
                <span class="detail">2026-05</span>
                <button type="button" onclick="fnDownloadPop('03','0303','OPN209912310000000008')">download</button>
            </li>
            "#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let items = client.fetch_inventory().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].file_id, "OPN209912310000000008");
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_downloads_provider_file_by_official_form_field() -> TestResult {
    let server = MockServer::start().await;
    let body = b"PK\x03\x04provider zip bytes".to_vec();

    Mock::given(method("POST"))
        .and(path("/cmm/fms/fileOpnDown.do"))
        .and(body_string_contains("srvrFileNm=OPN209912310000000008"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header(
                    "content-disposition",
                    "attachment; filename=\"building_register_main_202605.zip\"",
                )
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let file = client
        .fetch_file(&BuildingHubBulkDownloadRequest {
            file_id: "OPN209912310000000008".to_owned(),
        })
        .await?;

    assert_eq!(file.raw_payload, body);
    assert_eq!(file.content_type, "application/zip");
    assert_eq!(file.provider_file_name, "building_register_main_202605.zip");
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_opens_provider_file_stream_by_official_form_field() -> TestResult {
    let server = MockServer::start().await;
    let body = b"PK\x03\x04provider zip bytes".to_vec();

    Mock::given(method("POST"))
        .and(path("/cmm/fms/fileOpnDown.do"))
        .and(body_string_contains("srvrFileNm=OPN209912310000000008"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header(
                    "content-disposition",
                    "attachment; filename=\"building_register_main_202605.zip\"",
                )
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let mut file = client
        .open_file_stream(&BuildingHubBulkDownloadRequest {
            file_id: "OPN209912310000000008".to_owned(),
        })
        .await?;

    assert_eq!(file.content_type, "application/zip");
    assert_eq!(file.provider_file_name, "building_register_main_202605.zip");
    assert_eq!(file.expected_size_bytes, Some(body.len() as u64));

    let mut streamed = Vec::new();
    while let Some(chunk) = file.next_chunk().await? {
        streamed.extend_from_slice(&chunk);
    }
    assert_eq!(streamed, body);
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_retries_transient_download_handshake_failures_until_success() -> TestResult
{
    let server = MockServer::start().await;
    let body = b"PK\x03\x04hub zip bytes".to_vec();

    // One transient 503, then the real file: the handshake must retry past the failure.
    Mock::given(method("POST"))
        .and(path("/cmm/fms/fileOpnDown.do"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/cmm/fms/fileOpnDown.do"))
        .and(body_string_contains("srvrFileNm=OPN209912310000000008"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header("content-disposition", "attachment; filename=\"file.zip\"")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let file = client
        .fetch_file(&BuildingHubBulkDownloadRequest {
            file_id: "OPN209912310000000008".to_owned(),
        })
        .await?;

    assert_eq!(file.raw_payload, body);
    assert_eq!(file.provider_file_name, "file.zip");
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_fails_fast_on_non_retryable_download_status() -> TestResult {
    let server = MockServer::start().await;

    // expect(1): a 404 must not be retried even though the policy allows multiple attempts.
    Mock::given(method("POST"))
        .and(path("/cmm/fms/fileOpnDown.do"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let error = client
        .fetch_file(&BuildingHubBulkDownloadRequest {
            file_id: "OPN209912310000000008".to_owned(),
        })
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected non-retryable download failure"))?;

    assert!(
        error
            .to_string()
            .contains("hub.go.kr bulk file request returned HTTP 404"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn hub_bulk_client_rejects_html_login_page_as_download_payload() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/cmm/fms/fileOpnDown.do"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/html; charset=UTF-8")
                .set_body_string("<html><title>login</title></html>"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = BuildingHubBulkClient::new(&BuildingHubBulkConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
    })?;

    let error = client
        .fetch_file(&BuildingHubBulkDownloadRequest {
            file_id: "OPN209912310000000008".to_owned(),
        })
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected HTML login response rejection"))?;

    assert!(
        error.to_string().contains("returned HTML"),
        "unexpected error: {error}"
    );
    Ok(())
}
