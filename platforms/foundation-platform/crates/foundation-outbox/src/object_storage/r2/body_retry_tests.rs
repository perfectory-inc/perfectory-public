//! Loopback-only checks that a whole-object read survives a body that breaks after its headers
//! (root ADR-0102). No environment credentials or external endpoints are used.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{retry::RetryConfig, BehaviorVersion, Region};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BODY: &[u8] = b"hello world";

/// Serves GETs of `BODY`: the first `broken` answers send the headers and three bytes of the
/// body, then close; the rest send it whole. Counts the requests.
async fn serve(broken: usize) -> TestResult<(String, Arc<AtomicUsize>)> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&requests);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut buffer = [0; 4096];
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(count) => request.extend_from_slice(&buffer[..count]),
                }
            }
            let seen = counted.fetch_add(1, Ordering::SeqCst);
            let head = format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nETag: \"v{seen}\"\r\nContent-Length: {}\r\n\r\n",
                BODY.len()
            );
            let body = if seen < broken { &BODY[..3] } else { BODY };
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body).await;
            let _ = socket.shutdown().await;
        }
    });
    Ok((endpoint, requests))
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

#[tokio::test]
async fn a_body_that_breaks_once_is_read_again_whole() -> TestResult {
    let (endpoint, requests) = serve(1).await?;
    let bytes = storage(endpoint)
        .get_object_bytes("packs/g2/9999900000.pack")
        .await?;
    assert_eq!(bytes, BODY);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "one broken body, one retry"
    );
    Ok(())
}

#[tokio::test]
async fn a_body_that_keeps_breaking_fails_after_the_bounded_attempts() -> TestResult {
    let (endpoint, requests) = serve(usize::MAX).await?;
    let error = storage(endpoint)
        .get_object_bytes("packs/g2/9999900000.pack")
        .await
        .err()
        .ok_or("a body that never arrived whole was returned")?;
    assert!(
        error.to_string().contains("failed to read R2 object body"),
        "{error}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), R2_RANGE_READ_MAX_ATTEMPTS);
    Ok(())
}

#[tokio::test]
async fn a_versioned_read_retried_returns_the_bytes_and_tag_of_one_answer() -> TestResult {
    let (endpoint, requests) = serve(1).await?;
    let (bytes, e_tag) = storage(endpoint)
        .get_object_bytes_and_e_tag("serving/manifest.json")
        .await?;
    assert_eq!(bytes, BODY);
    assert_eq!(e_tag, "\"v1\"", "the tag of the answer whose body arrived");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    Ok(())
}
