//! 루프백 서버로 읽기 정책을 시험한다. 자격 증명이나 외부 주소는 쓰지 않는다.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BODY: &[u8] = b"photo bytes";

#[derive(Clone, Copy)]
enum Answer {
    Broken,
    Status(&'static str, &'static str),
    Whole,
}

/// 요청마다 `script` 의 다음 답(끝나면 마지막 답)을 준다. 요청 수를 센다.
async fn serve(script: Vec<Answer>) -> TestResult<(String, Arc<AtomicUsize>)> {
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
            let answer = script
                .get(seen)
                .or_else(|| script.last())
                .copied()
                .unwrap_or(Answer::Whole);
            let (status, body, length): (&str, Vec<u8>, usize) = match answer {
                Answer::Broken => ("200 OK", BODY[..3].to_vec(), BODY.len()),
                Answer::Whole => ("200 OK", BODY.to_vec(), BODY.len()),
                Answer::Status(status, code) => {
                    let xml =
                        format!("<Error><Code>{code}</Code><Message>fixture</Message></Error>");
                    let length = xml.len();
                    (status, xml.into_bytes(), length)
                }
            };
            let head = format!(
                "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: image/webp\r\nContent-Length: {length}\r\n\r\n"
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            let _ = socket.shutdown().await;
        }
    });
    Ok((endpoint, requests))
}

fn loopback_client(endpoint: &str) -> aws_sdk_s3::Client {
    let account = R2Account {
        account_id: "fixture".to_owned(),
        access_key: "local-access".to_owned(),
        secret_key: "local-secret".to_owned(),
    };
    client_at(
        &account,
        endpoint,
        ClientProfile {
            app_name: "fixture",
            sdk_attempts: 1,
            attempt_timeout: Duration::from_secs(5),
        },
    )
}

const FAST: ReadPolicy = ReadPolicy {
    max_attempts: 4,
    min_wait: Duration::from_millis(2),
    max_wait: Duration::from_millis(8),
};

#[test]
fn the_wait_doubles_with_half_of_it_random_and_stops_at_the_cap() {
    let policy = ReadPolicy::ADR_0102;
    assert_eq!(policy.delay(1, 0.0), Duration::from_millis(100));
    assert_eq!(policy.delay(1, 1.0), Duration::from_millis(200));
    assert_eq!(policy.delay(3, 1.0), Duration::from_millis(800));
    assert_eq!(policy.delay(20, 1.0), Duration::from_secs(30));
}

#[tokio::test]
async fn a_body_that_breaks_once_is_read_again_whole() -> TestResult {
    let (endpoint, requests) = serve(vec![Answer::Broken, Answer::Whole]).await?;
    let object = read_object(
        &loopback_client(&endpoint),
        "bucket",
        "listings/1/photo.webp",
        &FAST,
    )
    .await
    .map_err(|error| format!("{error:?}"))?;
    assert_eq!(object.bytes, BODY);
    assert_eq!(object.content_type.as_deref(), Some("image/webp"));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn a_missing_object_is_not_asked_again() -> TestResult {
    let (endpoint, requests) = serve(vec![Answer::Status("404 Not Found", "NoSuchKey")]).await?;
    let result = read_object(
        &loopback_client(&endpoint),
        "bucket",
        "listings/1/photo.webp",
        &FAST,
    )
    .await;
    assert!(matches!(result, Err(ReadError::NotFound)), "{result:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_server_error_is_asked_again_and_a_body_that_never_arrives_fails() -> TestResult {
    let (endpoint, requests) = serve(vec![
        Answer::Status("503 Service Unavailable", "SlowDown"),
        Answer::Whole,
    ])
    .await?;
    read_object(
        &loopback_client(&endpoint),
        "bucket",
        "listings/1/photo.webp",
        &FAST,
    )
    .await
    .map_err(|error| format!("{error:?}"))?;
    assert_eq!(requests.load(Ordering::SeqCst), 2);

    let (endpoint, requests) = serve(vec![Answer::Broken]).await?;
    let result = read_object(
        &loopback_client(&endpoint),
        "bucket",
        "listings/1/photo.webp",
        &FAST,
    )
    .await;
    assert!(matches!(result, Err(ReadError::Failed(_))), "{result:?}");
    assert_eq!(requests.load(Ordering::SeqCst), FAST.max_attempts);
    Ok(())
}
