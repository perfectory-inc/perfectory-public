//! Contract tests for Rust-side lakehouse lineage event publishing.

use std::{
    error::Error,
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use foundation_outbox::{lineage::LakehouseLineagePublisher, PublishError};
use serde_json::{json, Value};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const LINEAGE_EVENT_FIXTURE: &str =
    include_str!("../../../docs/events/lineage/lakehouse-lineage-event.v1.example.json");

#[tokio::test]
async fn lineage_publisher_posts_valid_event_to_receiver() -> TestResult {
    let server = OneShotHttpServer::spawn(202)?;
    let endpoint = server.url();
    let event: Value = serde_json::from_str(LINEAGE_EVENT_FIXTURE)?;
    let publisher = LakehouseLineagePublisher::builder()
        .endpoint(&endpoint)?
        .auth_token("test-lineage-token")
        .build()?;

    let status = publisher.publish(&event).await?;
    assert_eq!(status, 202);

    let request = server.join()?;
    assert!(request.starts_with("POST /api/v1/lineage HTTP/1.1"));
    let headers = request.to_ascii_lowercase();
    assert!(headers.contains("content-type: application/json"));
    assert!(headers.contains("authorization: bearer test-lineage-token"));

    let body = json_body(&request)?;
    assert_eq!(
        body["schema_version"],
        "foundation-platform.lakehouse_lineage_event.v1"
    );
    assert_eq!(
        body["event_type"],
        "lakehouse.lineage.dataset_materialized.v1"
    );
    assert_eq!(body["producer"], "foundation-platform.lakehouse");
    assert_eq!(
        body["output_dataset"]["qualified_name"],
        "gold.complex_catalog"
    );
    assert_eq!(body["source_snapshot_truncated"], false);
    assert_eq!(body["quality_metrics"]["row_count"], 2);
    assert!(body["column_lineage"]
        .as_array()
        .is_some_and(|items| !items.is_empty()));
    Ok(())
}

#[tokio::test]
async fn lineage_publisher_rejects_truncated_source_lineage_before_network() -> TestResult {
    let mut event: Value = serde_json::from_str(LINEAGE_EVENT_FIXTURE)?;
    event["source_snapshot_truncated"] = json!(true);

    let publisher = LakehouseLineagePublisher::builder()
        .endpoint("https://lineage.example.invalid/api/v1/lineage")?
        .build()?;

    let error = match publisher.publish(&event).await {
        Ok(status) => {
            return Err(format!(
                "truncated source lineage must fail validation, got status {status}"
            )
            .into())
        }
        Err(error) => error,
    };

    match error {
        PublishError::Infrastructure(message) => {
            assert!(message.contains("source_snapshot_truncated"));
        }
        other => {
            return Err(format!("expected validation error, got: {other}").into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn lineage_publisher_rejects_missing_openlineage_mapping_before_network() -> TestResult {
    let mut event: Value = serde_json::from_str(LINEAGE_EVENT_FIXTURE)?;
    let event_object = event
        .as_object_mut()
        .ok_or_else(|| std::io::Error::other("lineage fixture must be a JSON object"))?;
    event_object.remove("openlineage_mapping");

    let publisher = LakehouseLineagePublisher::builder()
        .endpoint("https://lineage.example.invalid/api/v1/lineage")?
        .build()?;

    let error = match publisher.publish(&event).await {
        Ok(status) => {
            return Err(format!(
                "missing OpenLineage mapping must fail validation, got status {status}"
            )
            .into())
        }
        Err(error) => error,
    };

    match error {
        PublishError::Infrastructure(message) => {
            assert!(message.contains("openlineage_mapping.event_type"));
        }
        other => {
            return Err(format!("expected validation error, got: {other}").into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn lineage_publisher_rejects_plain_http_remote_endpoint() -> TestResult {
    let Err(error) =
        LakehouseLineagePublisher::builder().endpoint("http://example.com/api/v1/lineage")
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
            url: format!("http://{addr}/api/v1/lineage"),
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
    let (_, body) = request
        .split_once("\r\n\r\n")
        .ok_or("HTTP request missing body separator")?;
    serde_json::from_str(body).map_err(Into::into)
}
