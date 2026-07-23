//! Contract tests for HTTP webhook outbox publishing.

use std::{
    error::Error,
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use chrono::{TimeZone, Utc};
use foundation_outbox::{
    broadcaster::EventEnvelope, webhook::WebhookBroadcaster, EventBroadcaster, OutboxScope,
    PublishError,
};
use serde_json::{json, Value};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn webhook_broadcaster_posts_event_envelope_with_trace_headers() -> TestResult {
    let server = OneShotHttpServer::spawn(202)?;
    let endpoint = server.url();
    let event_id = Uuid::now_v7();
    let occurred_at = Utc
        .with_ymd_and_hms(2026, 5, 18, 12, 0, 0)
        .single()
        .ok_or("test timestamp must be valid")?;
    let event = EventEnvelope {
        event_id,
        event_type: "catalog.industrial_complex.gold_pointer.published.v1".to_owned(),
        payload: json!({
            "type": "catalog.industrial_complex.gold_pointer.published.v1",
            "complex_id": "ic-001"
        }),
        occurred_at,
        scope: OutboxScope::Catalog,
    };
    let broadcaster = WebhookBroadcaster::builder()
        .endpoint("gongzzang", &endpoint)?
        .build()?;

    broadcaster.publish(&event).await?;

    let request = server.join()?;
    assert!(request.starts_with("POST /catalog-events HTTP/1.1"));
    assert!(request.contains("x-foundation-platform-event-id: "));
    assert!(request.contains(&event_id.to_string()));
    assert!(request.contains(
        "x-foundation-platform-event-type: catalog.industrial_complex.gold_pointer.published.v1"
    ));
    assert!(request.contains("x-foundation-platform-outbox-scope: catalog"));

    let body = json_body(&request)?;
    assert_eq!(body["event_id"], event_id.to_string());
    assert_eq!(
        body["event_type"],
        "catalog.industrial_complex.gold_pointer.published.v1"
    );
    assert_eq!(body["scope"], "catalog");
    assert_eq!(body["occurred_at"], "2026-05-18T12:00:00Z");
    assert_eq!(body["payload"]["complex_id"], "ic-001");
    Ok(())
}

#[tokio::test]
async fn webhook_broadcaster_signs_event_body_with_timestamped_hmac() -> TestResult {
    let server = OneShotHttpServer::spawn(202)?;
    let endpoint = server.url();
    let event_id = Uuid::now_v7();
    let occurred_at = Utc
        .with_ymd_and_hms(2026, 5, 28, 12, 0, 0)
        .single()
        .ok_or("test timestamp must be valid")?;
    let event = EventEnvelope {
        event_id,
        event_type: "catalog.parcel_marker_anchor.snapshot.published.v1".to_owned(),
        payload: json!({
            "type": "catalog.parcel_marker_anchor.snapshot.published.v1",
            "anchor_snapshot_id": "anchor-snapshot-20260528T120000Z"
        }),
        occurred_at,
        scope: OutboxScope::Catalog,
    };
    let broadcaster = WebhookBroadcaster::builder()
        .endpoint("gongzzang", &endpoint)?
        .signature_secret("unit-test-webhook-secret")?
        .build()?;

    broadcaster.publish(&event).await?;

    let request = server.join()?;
    let timestamp = header_value(&request, "x-foundation-platform-timestamp")
        .ok_or("timestamp header missing")?;
    let signature = header_value(&request, "x-foundation-platform-signature")
        .ok_or("signature header missing")?;
    let body = raw_body(&request)?;
    assert_eq!(
        signature,
        format!(
            "v1={}",
            foundation_outbox::webhook::sign_webhook_body(
                "unit-test-webhook-secret",
                timestamp,
                body
            )?
        )
    );
    Ok(())
}

#[test]
fn webhook_hmac_signature_matches_sha256_test_vector() -> TestResult {
    assert_eq!(
        foundation_outbox::webhook::sign_webhook_body(
            "unit-test-webhook-secret",
            "1700000000",
            r#"{"a":1}"#
        )?,
        "f2ff2919035b24f2ef31ef34cf5f040d55c61ffd98419485406a5f9b8999339c"
    );
    Ok(())
}

#[tokio::test]
async fn webhook_broadcaster_rejects_unsuccessful_response_for_retry() -> TestResult {
    let server = OneShotHttpServer::spawn(503)?;
    let endpoint = server.url();
    let broadcaster = WebhookBroadcaster::builder()
        .endpoint("dawneer", &endpoint)?
        .build()?;
    let event = EventEnvelope {
        event_id: Uuid::now_v7(),
        event_type: "catalog.complex.updated.v1".to_owned(),
        payload: json!({ "type": "catalog.complex.updated.v1" }),
        occurred_at: Utc::now(),
        scope: OutboxScope::Catalog,
    };

    let error = match broadcaster.publish(&event).await {
        Ok(()) => return Err("503 response must fail publication".into()),
        Err(error) => error,
    };

    match error {
        PublishError::Broadcaster(message) => {
            assert!(message.contains("dawneer"));
            assert!(message.contains("503"));
        }
        other => {
            return Err(format!("expected broadcaster error, got {other}").into());
        }
    }
    let _request = server.join()?;
    Ok(())
}

#[tokio::test]
async fn webhook_broadcaster_rejects_plain_http_remote_endpoints() -> TestResult {
    let Err(error) =
        WebhookBroadcaster::builder().endpoint("external", "http://example.com/events")
    else {
        return Err("remote HTTP URL must be rejected".into());
    };

    match error {
        PublishError::Infrastructure(message) => {
            assert!(message.contains("https"));
        }
        other => {
            return Err(format!("expected infrastructure error, got {other}").into());
        }
    }
    Ok(())
}

struct OneShotHttpServer {
    listener: TcpListener,
    status: u16,
}

impl OneShotHttpServer {
    fn spawn(status: u16) -> TestResult<StartedServer> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = Self { listener, status };
        let handle = thread::spawn(move || server.accept_one());
        Ok(StartedServer {
            url: format!("http://{addr}/catalog-events"),
            handle,
        })
    }

    fn accept_one(self) -> TestResult<String> {
        let (mut stream, _) = self.listener.accept()?;
        let mut buffer = [0_u8; 16_384];
        let mut request = Vec::new();
        loop {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request);
                let content_length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .and_then(|raw| raw.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map_or(request.len(), |index| index + 4);
                while request.len().saturating_sub(header_end) < content_length {
                    let read = stream.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                break;
            }
        }

        let reason = if self.status < 400 {
            "Accepted"
        } else {
            "Unavailable"
        };
        let response = format!(
            "HTTP/1.1 {} {}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            self.status, reason
        );
        stream.write_all(response.as_bytes())?;
        String::from_utf8(request).map_err(Into::into)
    }
}

struct StartedServer {
    url: String,
    handle: thread::JoinHandle<TestResult<String>>,
}

impl StartedServer {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn join(self) -> TestResult<String> {
        self.handle
            .join()
            .map_err(|_| "HTTP server thread panicked")?
    }
}

fn json_body(request: &str) -> TestResult<Value> {
    serde_json::from_str(raw_body(request)?).map_err(Into::into)
}

fn raw_body(request: &str) -> TestResult<&str> {
    let (_, body) = request
        .split_once("\r\n\r\n")
        .ok_or("HTTP request missing body separator")?;
    Ok(body)
}

fn header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    let expected_prefix = format!("{name}: ");
    request
        .lines()
        .find_map(|line| line.strip_prefix(expected_prefix.as_str()))
}
