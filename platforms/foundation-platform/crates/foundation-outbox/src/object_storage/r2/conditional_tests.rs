//! Loopback-only S3 protocol checks of the version-conditioned write. No environment credentials
//! or external endpoints are used.

use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{retry::RetryConfig, BehaviorVersion, Region};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Answers one request with `response` and hands back the request's text.
async fn answer_once(
    response: String,
) -> TestResult<(String, tokio::task::JoinHandle<TestResult<String>>)> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut buffer = [0; 4096];
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                return Err("mock S3 client disconnected before request headers".into());
            }
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8(bytes[..header_end].to_vec())?;
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then_some(value.trim())
            })
            .unwrap_or("0")
            .parse::<usize>()?;
        while bytes.len() < header_end + length {
            let mut buffer = [0; 4096];
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                return Err("mock S3 client disconnected before request body".into());
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
        socket.write_all(response.as_bytes()).await?;
        socket.shutdown().await?;
        Ok(headers)
    });
    Ok((endpoint, server))
}

fn storage(endpoint: String) -> R2ObjectStorage {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("auto"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .retry_config(RetryConfig::disabled())
        .credentials_provider(Credentials::new(
            "local-access",
            "local-secret",
            None,
            None,
            "fixture",
        ))
        .build();
    R2ObjectStorage {
        client: aws_sdk_s3::Client::from_conf(config),
        bucket_name: "bucket".to_owned(),
        namespace: crate::object_storage::R2KeyNamespace::Production,
    }
}

fn reply(status: u16, extra_headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} Mock\r\nConnection: close\r\nContent-Type: application/xml\r\n{extra_headers}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn pointer_write() -> PutObjectRequest {
    PutObjectRequest {
        key: "pointer.json".to_owned(),
        body: b"{}\n".to_vec(),
        content_type: "application/json".to_owned(),
        cache_control: "no-cache".to_owned(),
        write_mode: ObjectWriteMode::OverwriteAllowed,
        sha256: None,
    }
}

async fn conditional_put(
    response: String,
) -> TestResult<(Result<ConditionalWrite, PublishError>, String)> {
    let (endpoint, server) = answer_once(response).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        storage(endpoint).put_object_if_match(pointer_write(), "\"seen\""),
    )
    .await?;
    let request = tokio::time::timeout(Duration::from_secs(15), server).await???;
    Ok((result, request))
}

#[tokio::test]
async fn a_conditional_put_sends_if_match_with_the_version_read() -> TestResult {
    let (result, request) = conditional_put(reply(200, "ETag: \"next\"\r\n", "")).await?;
    assert_eq!(result?, ConditionalWrite::Written);
    assert!(request.starts_with("PUT /bucket/pointer.json"), "{request}");
    assert!(
        request
            .to_ascii_lowercase()
            .contains("\r\nif-match: \"seen\"\r\n"),
        "{request}"
    );
    assert!(
        !request.to_ascii_lowercase().contains("if-none-match"),
        "{request}"
    );
    Ok(())
}

#[tokio::test]
async fn a_moved_version_is_reported_and_not_an_error() -> TestResult {
    let moved = "<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>";
    let (result, _) = conditional_put(reply(412, "", moved)).await?;
    assert_eq!(result?, ConditionalWrite::VersionChanged);
    let gone =
        "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
    let (result, _) = conditional_put(reply(404, "", gone)).await?;
    assert_eq!(result?, ConditionalWrite::VersionChanged);
    // Any other failure stays an error, never a silent "moved".
    let denied = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
    let (result, _) = conditional_put(reply(403, "", denied)).await?;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn a_create_only_request_cannot_be_version_conditioned() -> TestResult {
    let mut request = pointer_write();
    request.write_mode = ObjectWriteMode::CreateOnly;
    let result = storage("http://127.0.0.1:9".to_owned())
        .put_object_if_match(request, "\"seen\"")
        .await;
    assert!(matches!(result, Err(PublishError::Infrastructure(_))));
    Ok(())
}

#[tokio::test]
async fn a_read_returns_the_bytes_with_their_version() -> TestResult {
    let (endpoint, server) = answer_once(reply(200, "ETag: \"seen\"\r\n", "{}\n")).await?;
    let (bytes, e_tag) = tokio::time::timeout(
        Duration::from_secs(15),
        storage(endpoint).get_object_bytes_and_e_tag("pointer.json"),
    )
    .await??;
    let request = tokio::time::timeout(Duration::from_secs(15), server).await???;
    assert!(request.starts_with("GET /bucket/pointer.json"), "{request}");
    assert_eq!(bytes, b"{}\n");
    assert_eq!(e_tag, "\"seen\"");
    Ok(())
}
