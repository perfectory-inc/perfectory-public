//! Loopback-only S3 protocol checks. No environment credentials or external endpoints are used.

use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{retry::RetryConfig, BehaviorVersion, Region};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Exchange {
    starts_with: String,
    contains: Vec<String>,
    excludes: Vec<String>,
    response: String,
}

fn exchange(method: &str, path: &str, status: u16, body: &str) -> Exchange {
    Exchange {
        starts_with: format!("{method} /bucket/{path}"),
        contains: Vec::new(),
        excludes: Vec::new(),
        response: format!(
            "HTTP/1.1 {status} Mock\r\nConnection: close\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{body}",
            body.len(),
        ),
    }
}

fn head(path: &str, size: u64, tag: &str) -> Exchange {
    let mut next = exchange("HEAD", path, 200, "");
    next.response = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {size}\r\nETag: \"{tag}\"\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n\r\n",
    );
    next
}

fn request(limit: u64, size: u64) -> CreateOnlyCopyObjectRequest {
    CreateOnlyCopyObjectRequest {
        source_key: "source.bin".to_owned(),
        destination_key: "destination.bin".to_owned(),
        size_bytes: size,
        single_copy_max_bytes: limit,
    }
}

fn copy_success() -> &'static str {
    "<CopyObjectResult><ETag>\"destination\"</ETag></CopyObjectResult>"
}

async fn run_mock(
    request: CreateOnlyCopyObjectRequest,
    exchanges: Vec<Exchange>,
) -> TestResult<Result<(), PublishError>> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        for expected in exchanges {
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
            let request = String::from_utf8(bytes)?;
            assert!(request.starts_with(&expected.starts_with), "{request}");
            for required in expected.contains {
                assert!(
                    request.contains(&required),
                    "missing {required:?}: {request}"
                );
            }
            for forbidden in expected.excludes {
                assert!(
                    !request.contains(&forbidden),
                    "unexpected {forbidden:?}: {request}"
                );
            }
            socket.write_all(expected.response.as_bytes()).await?;
            socket.shutdown().await?;
        }
        TestResult::Ok(())
    });
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
    let storage = R2ObjectStorage {
        client: aws_sdk_s3::Client::from_conf(config),
        bucket_name: "bucket".to_owned(),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        storage.copy_object_create_only(request),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(15), server).await???;
    Ok(result)
}

#[tokio::test]
async fn single_copy_sends_destination_conditions_and_pins_source() -> TestResult {
    let mut copy = exchange("PUT", "destination.bin", 200, copy_success());
    copy.contains = vec![
        "\r\nif-none-match: *\r\n".to_owned(),
        "\r\ncf-copy-destination-if-none-match: *\r\n".to_owned(),
        "\r\nx-amz-copy-source-if-match: \"source\"\r\n".to_owned(),
        "\r\nx-amz-copy-source: bucket/source.bin\r\n".to_owned(),
    ];
    run_mock(
        request(6, 6),
        vec![
            exchange("HEAD", "destination.bin", 404, ""),
            head("source.bin", 6, "source"),
            copy,
        ],
    )
    .await??;
    let duplicate = run_mock(
        request(6, 6),
        vec![head("destination.bin", 6, "destination")],
    )
    .await?;
    assert!(matches!(
        duplicate,
        Err(PublishError::ObjectAlreadyExists { .. })
    ));
    Ok(())
}

fn multipart_start(size: u64) -> Vec<Exchange> {
    vec![
        exchange("HEAD", "destination.bin", 404, ""),
        head("source.bin", size, "source"),
        exchange("POST", "destination.bin?uploads", 200, "<InitiateMultipartUploadResult><UploadId>local-upload</UploadId></InitiateMultipartUploadResult>"),
    ]
}

#[tokio::test]
async fn multipart_copy_sends_bounded_ranges_then_conditional_completion() -> TestResult {
    let part_bytes = 64 * 1024 * 1024;
    let mut exchanges = multipart_start(part_bytes + 1);
    for (part, range) in [(1, "bytes=0-67108863"), (2, "bytes=67108864-67108864")] {
        let mut copy = exchange(
            "PUT",
            "destination.bin?",
            200,
            "<CopyPartResult><ETag>\"part\"</ETag></CopyPartResult>",
        );
        copy.contains = vec![
            format!("partNumber={part}"),
            format!("\r\nx-amz-copy-source-range: {range}\r\n"),
            "\r\nx-amz-copy-source-if-match: \"source\"\r\n".to_owned(),
        ];
        exchanges.push(copy);
    }
    let mut complete = exchange("POST", "destination.bin?", 200, "<CompleteMultipartUploadResult><ETag>\"destination\"</ETag></CompleteMultipartUploadResult>");
    complete.contains = vec![
        "uploadId=local-upload".to_owned(),
        "\r\nif-none-match: *\r\n".to_owned(),
        "<PartNumber>1</PartNumber>".to_owned(),
        "<PartNumber>2</PartNumber>".to_owned(),
    ];
    exchanges.push(complete);
    run_mock(request(2, part_bytes + 1), exchanges).await??;
    Ok(())
}

#[tokio::test]
async fn multipart_copy_aborts_on_part_failure_without_completing() -> TestResult {
    let mut exchanges = multipart_start(6);
    exchanges.push(exchange(
        "PUT",
        "destination.bin?",
        400,
        "<Error><Code>InvalidRequest</Code><Message>source changed</Message></Error>",
    ));
    let mut abort = exchange("DELETE", "destination.bin?", 204, "");
    abort.contains = vec!["uploadId=local-upload".to_owned()];
    exchanges.push(abort);
    assert!(run_mock(request(2, 6), exchanges).await?.is_err());
    Ok(())
}

fn rehash(path: &str, tag: &str, body: &str) -> Vec<Exchange> {
    vec![
        head(path, body.len() as u64, tag),
        exchange("GET", path, 206, body),
        head(path, body.len() as u64, tag),
    ]
}

#[tokio::test]
async fn explicit_header_rejection_requires_absence_and_full_rehash() -> TestResult {
    for destination_bytes in ["abcdef", "ghijkl"] {
        let mut exchanges = vec![
            exchange("HEAD", "destination.bin", 404, ""),
            head("source.bin", 6, "source"),
            exchange("PUT", "destination.bin", 501, "<Error><Code>NotImplemented</Code><Message>If-None-Match header is not supported</Message></Error>"),
        ];
        exchanges.extend(rehash("source.bin", "source", "abcdef"));
        exchanges.push(exchange("HEAD", "destination.bin", 404, ""));
        let mut fallback = exchange("PUT", "destination.bin", 200, copy_success());
        fallback.excludes = vec![
            "\r\nif-none-match:".to_owned(),
            "\r\ncf-copy-destination-if-none-match:".to_owned(),
        ];
        exchanges.push(fallback);
        exchanges.extend(rehash("destination.bin", "destination", destination_bytes));
        let result = run_mock(request(6, 6), exchanges).await?;
        assert_eq!(result.is_ok(), destination_bytes == "abcdef");
    }
    Ok(())
}

#[tokio::test]
async fn fallback_refuses_destination_that_appeared_after_rejection() -> TestResult {
    let mut exchanges = vec![
        exchange("HEAD", "destination.bin", 404, ""),
        head("source.bin", 6, "source"),
        exchange("PUT", "destination.bin", 501, "<Error><Code>NotImplemented</Code><Message>If-None-Match header is not supported</Message></Error>"),
    ];
    exchanges.extend(rehash("source.bin", "source", "abcdef"));
    exchanges.push(head("destination.bin", 6, "destination"));
    assert!(matches!(
        run_mock(request(6, 6), exchanges).await?,
        Err(PublishError::ObjectAlreadyExists { .. })
    ));
    Ok(())
}
