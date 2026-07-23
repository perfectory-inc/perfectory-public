//! Contract tests for the `VWorld` provider dataset-file inventory client.

use collection_infrastructure::{
    parse_vworld_dataset_file_inventory_page, VWorldDatasetFileClient, VWorldDatasetFileConfig,
    VWorldDatasetFileDownloadRequest, VWorldDatasetFileInventorySelector, VWorldDatasetFileKind,
    VWorldDatasetLoginClient, VWorldDatasetLoginConfig,
};
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn parser_extracts_file_identity_and_download_kind_from_provider_detail_html() -> TestResult {
    let html = provider_detail_page_html(
        r#"
        <li>
            <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9101"><label for="chkDs">select</label></div>
            <div class="item row">
                <div class="less">
                    <div class="format"><span class="shp">SHP</span></div>
                    <div class="tit min">SYNTHETIC_SMALL.zip</div>
                </div>
                <div class="txt">
                    <span>size<em>50</em>MB</span>
                    <span>kind<em>data</em></span>
                    <span>base<em class="xxs">2099-12</em></span>
                    <span>updated<em class="xxs">2099-12-13</em></span>
                </div>
            </div>
            <div class="btns"><button type="button" onClick="listFnc.download('30563', '9101', '1000' );">download</button></div>
        </li>
        <li>
            <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9102"><label for="chkDs">select</label></div>
            <div class="item row">
                <div class="less">
                    <div class="format"><span class="shp">SHP</span></div>
                    <div class="tit min">SYNTHETIC_LARGE.zip</div>
                </div>
                <div class="txt">
                    <span>size<em>650</em>MB</span>
                    <span>kind<em>data</em></span>
                    <span>base<em class="xxs">2099-12</em></span>
                    <span>updated<em class="xxs">2099-12-14</em></span>
                </div>
            </div>
            <div class="btns"><button type="button" onClick="listFnc.download('30563', '9102', '600000' );">download</button></div>
        </li>
        "#,
        2,
        1,
        3,
    );
    let selector = VWorldDatasetFileInventorySelector {
        svc_cde: "MK".to_owned(),
        ds_id: "30563".to_owned(),
    };

    let page = parse_vworld_dataset_file_inventory_page(&selector, &html)?;

    assert_eq!(page.total_file_count, 2);
    assert_eq!(page.current_page_index, 1);
    assert_eq!(page.total_page_count, 3);
    assert_eq!(page.files.len(), 2);

    let first = &page.files[0];
    assert_eq!(first.svc_cde, "MK");
    assert_eq!(first.ds_id, "30563");
    assert_eq!(first.download_ds_id, "30563");
    assert_eq!(first.file_no, "9101");
    assert_eq!(first.provider_file_name, "SYNTHETIC_SMALL.zip");
    assert_eq!(first.file_format, "SHP");
    assert_eq!(first.size_mb_label, "50");
    assert_eq!(first.size_kib, 1_000);
    assert_eq!(first.provider_file_kind, "data");
    assert_eq!(first.base_ym, "2099-12");
    assert_eq!(first.updated_at, "2099-12-13");
    assert_eq!(
        first.download_kind,
        VWorldDatasetFileKind::SingleResourceFile
    );

    let second = &page.files[1];
    assert_eq!(second.file_no, "9102");
    assert_eq!(
        second.download_kind,
        VWorldDatasetFileKind::SelectionArchive
    );
    Ok(())
}

#[test]
fn parser_keeps_provider_500mb_class_file_as_single_resource() -> TestResult {
    let html = provider_detail_page_html(
        r#"
        <li>
            <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9001"><label for="chkDs">select</label></div>
            <div class="item row">
                <div class="less">
                    <div class="format"><span class="csv">CSV</span></div>
                    <div class="tit min">SYNTHETIC_SINGLE_20991231.zip</div>
                </div>
                <div class="txt">
                    <span>size<em>500</em>MB</span>
                    <span>kind<em>data</em></span>
                    <span>base<em class="xxs">2099-12-31</em></span>
                    <span>updated<em class="xxs">2099-12-30</em></span>
                </div>
            </div>
            <div class="btns"><button type="button" onClick="listFnc.download('20991231DS99991', '9001', '500000' );">download</button></div>
        </li>
        "#,
        1,
        1,
        1,
    );
    let selector = VWorldDatasetFileInventorySelector {
        svc_cde: "NA".to_owned(),
        ds_id: "14".to_owned(),
    };

    let page = parse_vworld_dataset_file_inventory_page(&selector, &html)?;

    assert_eq!(page.files.len(), 1);
    assert_eq!(
        page.files[0].download_kind,
        VWorldDatasetFileKind::SingleResourceFile
    );
    Ok(())
}

#[test]
fn parser_preserves_provider_download_dataset_id_when_it_differs_from_detail_ds_id() -> TestResult {
    let html = provider_detail_page_html(
        r#"
        <li>
            <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9104"><label for="chkDs">select</label></div>
            <div class="item row">
                <div class="less">
                    <div class="format"><span class="csv">CSV</span></div>
                    <div class="tit min">SYNTHETIC_SELECTION_2099.zip</div>
                </div>
                <div class="txt">
                    <span>size<em>50</em>MB</span>
                    <span>kind<em>data</em></span>
                    <span>base<em class="xxs">2099-12</em></span>
                    <span>updated<em class="xxs">2099-12-13</em></span>
                </div>
            </div>
            <div class="btns"><button type="button" onClick="listFnc.download('20991231DS99992', '9104', '1000' );">download</button></div>
        </li>
        "#,
        1,
        1,
        1,
    );
    let selector = VWorldDatasetFileInventorySelector {
        svc_cde: "NA".to_owned(),
        ds_id: "4".to_owned(),
    };

    let page = parse_vworld_dataset_file_inventory_page(&selector, &html)?;

    assert_eq!(page.files.len(), 1);
    assert_eq!(page.files[0].ds_id, "4");
    assert_eq!(page.files[0].download_ds_id, "20991231DS99992");
    assert_eq!(page.files[0].file_no, "9104");
    Ok(())
}

#[tokio::test]
async fn client_fetches_every_provider_file_inventory_page() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/dtmk/dtmk_ntads_s002.do"))
        .and(query_param("svcCde", "MK"))
        .and(query_param("dsId", "30563"))
        .and(query_param("datPageIndex", "1"))
        .and(query_param("datPageSize", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_string(provider_detail_page_html(
            r#"
            <li>
                <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9101"><label for="chkDs">select</label></div>
                <div class="item row"><div class="less"><div class="format"><span class="shp">SHP</span></div><div class="tit min">first.zip</div></div><div class="txt"><span>size<em>50</em>MB</span><span>kind<em>data</em></span><span>base<em>2099-12</em></span><span>updated<em>2099-12-13</em></span></div></div>
                <div class="btns"><button type="button" onClick="listFnc.download('30563', '9101', '1000' );">download</button></div>
            </li>
            "#,
            2,
            1,
            2,
        )))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dtmk/dtmk_ntads_s002.do"))
        .and(query_param("svcCde", "MK"))
        .and(query_param("dsId", "30563"))
        .and(query_param("datPageIndex", "2"))
        .and(query_param("datPageSize", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_string(provider_detail_page_html(
            r#"
            <li>
                <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9103"><label for="chkDs">select</label></div>
                <div class="item row"><div class="less"><div class="format"><span class="shp">SHP</span></div><div class="tit min">second.zip</div></div><div class="txt"><span>size<em>51</em>MB</span><span>kind<em>data</em></span><span>base<em>2099-12</em></span><span>updated<em>2099-12-14</em></span></div></div>
                <div class="btns"><button type="button" onClick="listFnc.download('30563', '9103', '2000' );">download</button></div>
            </li>
            "#,
            2,
            2,
            2,
        )))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let files = client
        .fetch_dataset_file_inventory(&VWorldDatasetFileInventorySelector {
            svc_cde: "MK".to_owned(),
            ds_id: "30563".to_owned(),
        })
        .await?;

    assert_eq!(files.len(), 2);
    assert_eq!(files[0].file_no, "9101");
    assert_eq!(files[1].file_no, "9103");
    Ok(())
}

#[tokio::test]
async fn client_downloads_single_resource_file_with_provider_download_dataset_id() -> TestResult {
    let server = MockServer::start().await;
    let body = b"PK\x03\x04vworld zip bytes".to_vec();

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .and(query_param("ds_id", "20991231DS99992"))
        .and(query_param("fileNo", "9104"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header(
                    "content-disposition",
                    "attachment; filename=\"SYNTHETIC_SELECTION_2099.zip\"",
                )
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let file = client
        .fetch_file(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "20991231DS99992".to_owned(),
            file_no: "9104".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
        })
        .await?;

    assert_eq!(file.raw_payload, body);
    assert_eq!(file.content_type, "application/zip");
    assert_eq!(file.provider_file_name, "SYNTHETIC_SELECTION_2099.zip");
    Ok(())
}

#[tokio::test]
async fn client_opens_single_resource_file_stream_with_provider_download_dataset_id() -> TestResult
{
    let server = MockServer::start().await;
    let body = b"PK\x03\x04vworld zip bytes".to_vec();

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .and(query_param("ds_id", "20991231DS99992"))
        .and(query_param("fileNo", "9104"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header(
                    "content-disposition",
                    "attachment; filename=\"SYNTHETIC_SELECTION_2099.zip\"",
                )
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let mut file = client
        .open_file_stream(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "20991231DS99992".to_owned(),
            file_no: "9104".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
        })
        .await?;

    assert_eq!(file.content_type, "application/zip");
    assert_eq!(file.provider_file_name, "SYNTHETIC_SELECTION_2099.zip");
    assert_eq!(file.expected_size_bytes, Some(body.len() as u64));

    let mut streamed = Vec::new();
    while let Some(chunk) = file.next_chunk().await? {
        streamed.extend_from_slice(&chunk);
    }
    assert_eq!(streamed, body);
    Ok(())
}

#[tokio::test]
async fn client_opens_file_stream_with_inventory_filename_when_header_omits_filename() -> TestResult
{
    let server = MockServer::start().await;
    let body = b"PK\x03\x04vworld zip bytes".to_vec();

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .and(query_param("ds_id", "30017"))
        .and(query_param("fileNo", "1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let mut file = client
        .open_file_stream_with_provider_file_name_fallback(
            &VWorldDatasetFileDownloadRequest {
                download_ds_id: "30017".to_owned(),
                file_no: "1".to_owned(),
                download_kind: VWorldDatasetFileKind::SingleResourceFile,
            },
            "센서스 공간정보 테이블 정의서.hwp",
        )
        .await?;

    assert_eq!(file.content_type, "application/zip");
    assert_eq!(file.provider_file_name, "센서스 공간정보 테이블 정의서.hwp");
    assert_eq!(file.expected_size_bytes, Some(body.len() as u64));

    let mut streamed = Vec::new();
    while let Some(chunk) = file.next_chunk().await? {
        streamed.extend_from_slice(&chunk);
    }
    assert_eq!(streamed, body);
    Ok(())
}

#[tokio::test]
async fn client_rejects_selection_archive_downloads_as_raon_agent_required() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile2.do"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let error = client
        .fetch_file(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "20991231DS99991".to_owned(),
            file_no: "42".to_owned(),
            download_kind: VWorldDatasetFileKind::SelectionArchive,
        })
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected selection archive rejection"))?;

    let message = error.to_string();
    assert!(
        message.contains("requires RAON/KUpload desktop agent"),
        "unexpected error: {error}"
    );
    assert!(
        message.contains("download_ds_id=20991231DS99991"),
        "unexpected error: {error}"
    );
    assert!(message.contains("file_no=42"), "unexpected error: {error}");
    Ok(())
}

#[tokio::test]
async fn client_reclassifies_empty_single_resource_response_when_selection_page_requires_raon(
) -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .and(query_param("ds_id", "20991231DS99991"))
        .and(query_param("fileNo", "9001"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "0")
                .set_body_bytes(Vec::new()),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dtmk/downloadDtnaResourceFile.do"))
        .and(query_param("ds_file_sq", "20991231DS999919001"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"
            <html>
              <script>
                var G_UploadID = "raon";
                RAONKUPLOAD.SetViewType("list");
                AddUploadedFile('1', 'SYNTHETIC_SINGLE_20991231.zip', '/filestore/down_store/dtna/209912/synthetic-fixture.zip', '4096', '20991231DS99991|9001', G_UploadID);
              </script>
            </html>
            "#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let error = client
        .open_file_stream(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "20991231DS99991".to_owned(),
            file_no: "9001".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
        })
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected RAON reclassification"))?;

    let message = error.to_string();
    assert!(
        message.contains("requires RAON/KUpload desktop agent"),
        "unexpected error: {error}"
    );
    assert!(
        message.contains("download_ds_id=20991231DS99991"),
        "unexpected error: {error}"
    );
    assert!(
        message.contains("file_no=9001"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn client_retries_transient_inventory_failures_until_success() -> TestResult {
    let server = MockServer::start().await;

    // One transient 503, then the real detail page: the inventory walk must retry past it.
    Mock::given(method("GET"))
        .and(path("/dtmk/dtmk_ntads_s002.do"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dtmk/dtmk_ntads_s002.do"))
        .and(query_param("svcCde", "MK"))
        .and(query_param("dsId", "30563"))
        .respond_with(ResponseTemplate::new(200).set_body_string(provider_detail_page_html(
            r#"
            <li>
                <div class="check only"><input type="checkbox" id="chkDs" name="chkDs" value="9101"><label for="chkDs">select</label></div>
                <div class="item row"><div class="less"><div class="format"><span class="shp">SHP</span></div><div class="tit min">first.zip</div></div><div class="txt"><span>size<em>50</em>MB</span><span>kind<em>data</em></span><span>base<em>2099-12</em></span><span>updated<em>2099-12-13</em></span></div></div>
                <div class="btns"><button type="button" onClick="listFnc.download('30563', '9101', '1000' );">download</button></div>
            </li>
            "#,
            1,
            1,
            1,
        )))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let files = client
        .fetch_dataset_file_inventory(&VWorldDatasetFileInventorySelector {
            svc_cde: "MK".to_owned(),
            ds_id: "30563".to_owned(),
        })
        .await?;

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_no, "9101");
    Ok(())
}

#[tokio::test]
async fn client_retries_transient_download_handshake_failures_until_success() -> TestResult {
    let server = MockServer::start().await;
    let body = b"PK\x03\x04vworld zip bytes".to_vec();

    // One transient 503, then the real file: the handshake must retry past the failure.
    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .and(query_param("ds_id", "30563"))
        .and(query_param("fileNo", "9101"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header("content-disposition", "attachment; filename=\"file.zip\"")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let file = client
        .fetch_file(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "30563".to_owned(),
            file_no: "9101".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
        })
        .await?;

    assert_eq!(file.raw_payload, body);
    assert_eq!(file.provider_file_name, "file.zip");
    Ok(())
}

#[tokio::test]
async fn client_fails_fast_on_non_retryable_download_status() -> TestResult {
    let server = MockServer::start().await;

    // expect(1): a 404 must not be retried even though the policy allows multiple attempts.
    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let error = client
        .fetch_file(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "30563".to_owned(),
            file_no: "9101".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
        })
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected non-retryable download failure"))?;

    assert!(
        error
            .to_string()
            .contains("VWorld dataset file request returned HTTP 404"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn login_client_never_retries_server_errors() -> TestResult {
    let server = MockServer::start().await;

    // expect(1): the non-idempotent login POST must run exactly once, even on a 5xx.
    Mock::given(method("POST"))
        .and(path("/v4po_usrlogin_a004.do"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetLoginClient::new(&VWorldDatasetLoginConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        username: "user".to_owned(),
        password: "pass".to_owned(),
    })?;

    let error = client
        .fetch_cookie_header()
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected login failure"))?;

    assert!(
        error
            .to_string()
            .contains("VWorld dataset login returned HTTP 503"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn client_rejects_html_login_response_as_download_payload() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/html; charset=UTF-8")
                .set_body_string("<html><title>login</title></html>"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: None,
    })?;

    let error = client
        .fetch_file(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "30563".to_owned(),
            file_no: "9101".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
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

#[tokio::test]
async fn client_sends_provider_session_cookie_when_configured() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/dtmk/downloadResourceFile.do"))
        .and(query_param("ds_id", "30563"))
        .and(query_param("fileNo", "9101"))
        .and(header("cookie", "JSESSIONID=test-session"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/zip")
                .insert_header("content-disposition", "attachment; filename=\"file.zip\"")
                .set_body_bytes(b"PK\x03\x04bytes".to_vec()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetFileClient::new(&VWorldDatasetFileConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        page_size: 100,
        cookie_header: Some("JSESSIONID=test-session".to_owned()),
    })?;

    let file = client
        .fetch_file(&VWorldDatasetFileDownloadRequest {
            download_ds_id: "30563".to_owned(),
            file_no: "9101".to_owned(),
            download_kind: VWorldDatasetFileKind::SingleResourceFile,
        })
        .await?;

    assert_eq!(file.provider_file_name, "file.zip");
    Ok(())
}

#[tokio::test]
async fn login_client_posts_encoded_credentials_and_returns_cookie_header() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v4po_usrlogin_a004.do"))
        .and(header("x-requested-with", "XMLHttpRequest"))
        .and(body_string_contains("usrIdeE=dXNlcg%3D%3D"))
        .and(body_string_contains("usrPwdE=cGFzcw%3D%3D"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("set-cookie", "PJSESSIONID=session-one; Path=/; HttpOnly")
                .append_header("set-cookie", "SSCSID=session-two; Path=/; HttpOnly")
                .set_body_json(serde_json::json!({
                    "resultMap": {
                        "result": "success",
                        "msg": "login ok"
                    }
                })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetLoginClient::new(&VWorldDatasetLoginConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        username: "user".to_owned(),
        password: "pass".to_owned(),
    })?;

    let cookie_header = client.fetch_cookie_header().await?;

    assert_eq!(cookie_header, "PJSESSIONID=session-one; SSCSID=session-two");
    Ok(())
}

#[tokio::test]
async fn login_client_rejects_provider_login_failure_without_leaking_credentials() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v4po_usrlogin_a004.do"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resultMap": {
                "result": "error",
                "msg": "invalid credentials"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetLoginClient::new(&VWorldDatasetLoginConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        username: "user".to_owned(),
        password: "pass".to_owned(),
    })?;

    let error = client
        .fetch_cookie_header()
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected login failure"))?;
    let error_message = error.to_string();

    assert!(error_message.contains("VWorld dataset login failed"));
    assert!(!error_message.contains("user"));
    assert!(!error_message.contains("pass"));
    Ok(())
}

#[tokio::test]
async fn login_client_reports_provider_failure_reason_without_leaking_credentials() -> TestResult {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v4po_usrlogin_a004.do"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resultMap": {
                "result": "locked",
                "msg": "password expired for alice-id secret-token"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetLoginClient::new(&VWorldDatasetLoginConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        username: "alice-id".to_owned(),
        password: "secret-token".to_owned(),
    })?;

    let error = client
        .fetch_cookie_header()
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("expected login failure"))?;
    let error_message = error.to_string();

    assert!(error_message.contains("result=locked"));
    assert!(error_message.contains("password expired"));
    assert!(!error_message.contains("alice-id"));
    assert!(!error_message.contains("secret-token"));
    Ok(())
}

#[tokio::test]
async fn login_client_accepts_expired_password_session_when_provider_issues_cookies() -> TestResult
{
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v4po_usrlogin_a004.do"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("set-cookie", "PJSESSIONID=session-one; Path=/; HttpOnly")
                .append_header("set-cookie", "SSCSID=session-two; Path=/; HttpOnly")
                .set_body_json(serde_json::json!({
                    "resultMap": {
                        "result": "expirePw",
                        "msg": "password expired"
                    }
                })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = VWorldDatasetLoginClient::new(&VWorldDatasetLoginConfig {
        base_uri: server.uri(),
        user_agent: "foundation-platform-test/1.0".to_owned(),
        username: "user".to_owned(),
        password: "pass".to_owned(),
    })?;

    let cookie_header = client.fetch_cookie_header().await?;

    assert_eq!(cookie_header, "PJSESSIONID=session-one; SSCSID=session-two");
    Ok(())
}

fn provider_detail_page_html(files_html: &str, total: u64, current: u64, pages: u64) -> String {
    format!(
        r#"
        <div class="count br">
            <span>Total <b>{total}</b> rows (<b>{current}</b>/ {pages} page )</span>
        </div>
        <div class="list bd box hover">
            <ul>{files_html}</ul>
        </div>
        "#
    )
}
