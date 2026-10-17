//! Loopback-only checks that an R2 request cannot wait forever for an answer, and that a request
//! carrying a body is not cut off by that bound. No environment credentials or external endpoints.

use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{retry::RetryConfig, BehaviorVersion, Region};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SHORT_FIRST_BYTE: Duration = Duration::from_millis(200);

/// Accepts one connection and reads its request headers; answers `response` after `delay`, or
/// never when `response` is `None` (the socket is held open until the test ends).
async fn server(
    response: Option<String>,
    delay: Duration,
) -> TestResult<(String, tokio::task::JoinHandle<TestResult>)> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            let mut buffer = [0; 4096];
            let count = socket.read(&mut buffer).await?;
            if count == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
        let Some(response) = response else {
            tokio::time::sleep(Duration::from_mins(1)).await;
            return Ok(());
        };
        // Drain the small body, then answer late.
        let mut buffer = [0; 4096];
        if let Ok(read) =
            tokio::time::timeout(Duration::from_millis(100), socket.read(&mut buffer)).await
        {
            read?;
        }
        tokio::time::sleep(delay).await;
        socket.write_all(response.as_bytes()).await?;
        socket.shutdown().await?;
        Ok(())
    });
    Ok((endpoint, task))
}

/// A client like production's, but with a short first-byte bound and no SDK retry.
fn storage(endpoint: String) -> R2ObjectStorage {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("auto"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .retry_config(RetryConfig::disabled())
        .timeout_config(request_timeouts(Some(SHORT_FIRST_BYTE)))
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
        read_policy: crate::object_storage::r2::read_retry::R2ReadPolicy::ADR_0102,
    }
}

#[test]
fn the_production_client_bounds_connect_and_first_byte() -> TestResult {
    let storage = R2ObjectStorage::from_config(R2ObjectStorageConfig {
        bucket_name: "bucket".to_owned(),
        endpoint: "http://127.0.0.1:9".to_owned(),
        region: "auto".to_owned(),
        access_key_id: "local-access".to_owned(),
        secret_access_key: "local-secret".to_owned(),
        namespace: crate::object_storage::R2KeyNamespace::Production,
    });
    let timeouts = storage
        .client
        .config()
        .timeout_config()
        .ok_or("the R2 client has no timeout config")?;
    assert_eq!(timeouts.connect_timeout(), Some(R2_CONNECT_TIMEOUT));
    assert_eq!(timeouts.read_timeout(), Some(R2_FIRST_BYTE_TIMEOUT));
    Ok(())
}

#[tokio::test]
async fn a_head_whose_answer_never_starts_fails_instead_of_waiting() -> TestResult {
    let (endpoint, _server) = server(None, Duration::ZERO).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        storage(endpoint).object_exists("manifest.json"),
    )
    .await
    .map_err(|_| "the HEAD was still waiting after 15s: no first-byte bound")?;
    assert!(
        result.is_err(),
        "a silent server is not an answer: {result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn an_upload_answered_after_the_first_byte_bound_still_succeeds() -> TestResult {
    let response =
        "HTTP/1.1 200 OK\r\nConnection: close\r\nETag: \"next\"\r\nContent-Length: 0\r\n\r\n";
    let (endpoint, server) = server(Some(response.to_owned()), SHORT_FIRST_BYTE * 3).await?;
    let write = PutObjectRequest {
        key: "pointer.json".to_owned(),
        body: b"{}\n".to_vec(),
        content_type: "application/json".to_owned(),
        cache_control: "no-cache".to_owned(),
        write_mode: ObjectWriteMode::OverwriteAllowed,
        sha256: None,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        storage(endpoint).put_object_if_match(write, "\"seen\""),
    )
    .await?;
    assert_eq!(result?, ConditionalWrite::Written);
    tokio::time::timeout(Duration::from_secs(15), server).await???;
    Ok(())
}
