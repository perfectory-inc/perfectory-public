//! The one read policy (root ADR-0102), against loopback servers. No credentials or external
//! endpoints are used.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::body_retry_tests::{fast_policy, serve, storage};
use super::read_retry::{self, R2ReadPolicy, ReadRequest};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn the_wait_doubles_with_half_of_it_random_and_stops_at_the_cap() {
    let policy = R2ReadPolicy::ADR_0102;
    assert_eq!(policy.delay(1, 0.0), Duration::from_millis(100));
    assert_eq!(policy.delay(1, 1.0), Duration::from_millis(200));
    assert_eq!(policy.delay(2, 0.0), Duration::from_millis(200));
    assert_eq!(policy.delay(3, 1.0), Duration::from_millis(800));
    assert_eq!(policy.delay(20, 0.0), Duration::from_secs(15));
    assert_eq!(policy.delay(20, 1.0), Duration::from_secs(30));
    assert_eq!(policy.delay(usize::MAX, 0.5), Duration::from_millis(22_500));
}

/// Answers the first `first` requests with `status` and an S3 error body, the rest with `OK` and
/// a body. Counts the requests.
async fn serve_status(
    status: &'static str,
    first: usize,
) -> TestResult<(String, Arc<AtomicUsize>)> {
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
            let (line, body): (&str, &[u8]) = if seen < first {
                (
                    status,
                    b"<Error><Code>Fixture</Code><Message>fixture</Message></Error>",
                )
            } else {
                ("200 OK", b"whole")
            };
            let head = format!(
                "HTTP/1.1 {line}\r\nConnection: close\r\nContent-Type: application/xml\r\nETag: \"v{seen}\"\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body).await;
            let _ = socket.shutdown().await;
        }
    });
    Ok((endpoint, requests))
}

#[tokio::test]
async fn an_answer_that_will_not_change_is_not_asked_again() -> TestResult {
    let (endpoint, requests) = serve_status("404 Not Found", usize::MAX).await?;
    let error = storage(endpoint)
        .get_object_bytes("packs/g2/9999900000.pack")
        .await
        .err()
        .ok_or("a 404 was returned as bytes")?;
    assert!(
        error.to_string().contains("failed to read R2 object"),
        "{error}"
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "a 404 does not become a 200 by asking again"
    );
    Ok(())
}

#[tokio::test]
async fn a_server_error_is_asked_again() -> TestResult {
    let (endpoint, requests) = serve_status("503 Service Unavailable", 2).await?;
    let bytes = storage(endpoint)
        .get_object_bytes("packs/g2/9999900000.pack")
        .await?;
    assert_eq!(bytes, b"whole");
    assert_eq!(requests.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn a_range_whose_body_breaks_is_asked_again() -> TestResult {
    let (endpoint, requests) = serve(1).await?;
    storage(endpoint)
        .get_object_range("bronze/2026/fixture.zip", 0, 10)
        .await?;
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    Ok(())
}

/// The seekable reader's blocking read asked again at once before; it now waits like every read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_blocking_read_waits_before_it_asks_again() -> TestResult {
    let (endpoint, requests) = serve(1).await?;
    let client = storage(endpoint).client;
    let policy = R2ReadPolicy {
        max_attempts: fast_policy().max_attempts,
        min_wait: Duration::from_millis(80),
        max_wait: Duration::from_millis(80),
    };
    let handle = tokio::runtime::Handle::current();
    let counted = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::clone(&counted);
    let started = Instant::now();
    let answer = tokio::task::spawn_blocking(move || {
        let request = ReadRequest {
            key: "silver/handoff/fixture.parquet",
            range: Some("bytes=0-10"),
            if_match: None,
        };
        let count = || {
            attempts.fetch_add(1, Ordering::SeqCst);
        };
        read_retry::read_blocking(&handle, &client, "bucket", &policy, request, &count)
    })
    .await??;
    assert_eq!(answer.bytes.len(), 11);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        counted.load(Ordering::SeqCst),
        2,
        "the caller counts every request"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "it asked again without waiting"
    );
    Ok(())
}
