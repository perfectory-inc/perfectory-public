//! The one write policy (root ADR-0102, ADR-0261), against loopback servers. No credentials or
//! external endpoints are used.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::body_retry_tests::storage;
use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// What the server does with one request: answer with a status line, or close without answering.
#[derive(Clone, Copy)]
enum Answer {
    Status(&'static str),
    Close,
}

/// Answers each PUT with the next of `puts` and each HEAD with the next of `heads` (the last one
/// repeats). Records the methods it saw, in order.
async fn serve(
    puts: Vec<Answer>,
    heads: Vec<Answer>,
) -> TestResult<(String, Arc<Mutex<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        let (mut put, mut head) = (0_usize, 0_usize);
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut buffer = [0; 4096];
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(count) => request.extend_from_slice(&buffer[..count]),
                }
            }
            let method = String::from_utf8_lossy(&request)
                .split(' ')
                .next()
                .unwrap_or_default()
                .to_owned();
            let answer = if method == "HEAD" {
                head += 1;
                heads[(head - 1).min(heads.len() - 1)]
            } else {
                put += 1;
                puts[(put - 1).min(puts.len() - 1)]
            };
            if let Ok(mut methods) = log.lock() {
                methods.push(method.clone());
            }
            if let Answer::Status(line) = answer {
                // Read the whole body first: an answer sent before it is a send the client sees
                // fail (Answer::Close's case, and R2's early 412).
                let text = String::from_utf8_lossy(&request).to_ascii_lowercase();
                let length = text
                    .split("content-length:")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map_or(request.len(), |at| at + 4);
                let mut received = request.len() - header_end;
                while received < length {
                    let mut buffer = [0; 16384];
                    match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => received += count,
                    }
                }
                let body: &[u8] = if line.starts_with('2') || method == "HEAD" {
                    b""
                } else {
                    b"<Error><Code>Fixture</Code><Message>fixture</Message></Error>"
                };
                let head_line = format!(
                    "HTTP/1.1 {line}\r\nConnection: close\r\nContent-Type: application/xml\r\nETag: \"v1\"\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head_line.as_bytes()).await;
                let _ = socket.write_all(body).await;
            }
            let _ = socket.shutdown().await;
        }
    });
    Ok((endpoint, seen))
}

fn request(write_mode: ObjectWriteMode) -> PutObjectRequest {
    PutObjectRequest {
        key: "packs/documents/g2/9999900000.pack".to_owned(),
        body: vec![7; 64 * 1024],
        content_type: "application/octet-stream".to_owned(),
        cache_control: "public, max-age=31536000, immutable".to_owned(),
        write_mode,
        sha256: Some("00".repeat(32)),
    }
}

fn methods(seen: &Arc<Mutex<Vec<String>>>) -> TestResult<Vec<String>> {
    Ok(seen
        .lock()
        .map_err(|_| "the request log is poisoned")?
        .clone())
}

#[tokio::test]
async fn a_put_that_got_no_answer_is_sent_again() -> TestResult {
    let (endpoint, seen) = serve(vec![Answer::Close, Answer::Status("200 OK")], vec![]).await?;
    storage(endpoint)
        .put_object(request(ObjectWriteMode::OverwriteAllowed))
        .await?;
    assert_eq!(methods(&seen)?, ["PUT", "PUT"]);
    Ok(())
}

#[tokio::test]
async fn a_create_only_put_without_an_answer_whose_key_exists_is_reconciled_not_resent(
) -> TestResult {
    // 2026-10-16: the by-PNU bake re-sent a pack an earlier attempt had written; R2's 412 never
    // reached the client and six attempts failed as `dispatch failure`.
    let (endpoint, seen) = serve(vec![Answer::Close], vec![Answer::Status("200 OK")]).await?;
    let error = storage(endpoint)
        .put_object(request(ObjectWriteMode::CreateOnly))
        .await
        .err()
        .ok_or("the write succeeded")?;
    assert!(
        matches!(error, PublishError::ObjectAlreadyExists { .. }),
        "{error}"
    );
    assert_eq!(methods(&seen)?, ["PUT", "HEAD"]);
    Ok(())
}

#[tokio::test]
async fn a_create_only_put_without_an_answer_whose_key_is_absent_is_sent_again() -> TestResult {
    let (endpoint, seen) = serve(
        vec![Answer::Close, Answer::Status("200 OK")],
        vec![Answer::Status("404 Not Found")],
    )
    .await?;
    storage(endpoint)
        .put_object(request(ObjectWriteMode::CreateOnly))
        .await?;
    assert_eq!(methods(&seen)?, ["PUT", "HEAD", "PUT"]);
    Ok(())
}

#[tokio::test]
async fn an_answer_that_will_not_change_is_not_sent_again() -> TestResult {
    let (endpoint, seen) = serve(vec![Answer::Status("403 Forbidden")], vec![]).await?;
    let error = storage(endpoint)
        .put_object(request(ObjectWriteMode::OverwriteAllowed))
        .await
        .err()
        .ok_or("a 403 was a success")?;
    assert!(
        error.to_string().contains("http_status=Some(403)"),
        "{error}"
    );
    assert_eq!(methods(&seen)?, ["PUT"]);
    // A create-only write meeting a real 412 is the collision the caller reconciles.
    let (endpoint, seen) = serve(vec![Answer::Status("412 Precondition Failed")], vec![]).await?;
    let error = storage(endpoint)
        .put_object(request(ObjectWriteMode::CreateOnly))
        .await
        .err()
        .ok_or("a 412 was a success")?;
    assert!(
        matches!(error, PublishError::ObjectAlreadyExists { .. }),
        "{error}"
    );
    assert_eq!(methods(&seen)?, ["PUT"]);
    Ok(())
}

#[tokio::test]
async fn the_attempts_are_bounded_and_the_error_names_its_cause() -> TestResult {
    let (endpoint, seen) = serve(vec![Answer::Close], vec![]).await?;
    let error = storage(endpoint)
        .put_object(request(ObjectWriteMode::OverwriteAllowed))
        .await
        .err()
        .ok_or("a server that never answers took a write")?;
    assert_eq!(
        methods(&seen)?.len(),
        super::body_retry_tests::fast_policy().max_attempts
    );
    // Not the bare "dispatch failure" of 2026-10-16: the source chain follows it.
    let message = error.to_string();
    let after = message
        .split("dispatch failure")
        .nth(1)
        .ok_or_else(|| format!("no dispatch failure in {message}"))?;
    assert!(after.starts_with(": "), "the cause is missing: {message}");
    Ok(())
}
