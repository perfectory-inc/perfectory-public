//! The staging namespace (root ADR-0177): the key builder, the wire check, and the real client
//! against a loopback S3 stand-in. No environment credentials or external endpoints are used.

use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::super::{R2ObjectStorage, R2ObjectStorageConfig};
use super::*;
use crate::object_storage::{
    ObjectStorageService as _, ObjectStorageStreamingService as _, ObjectWriteMode,
    PutObjectRequest, R2InventoryRequest, StreamingPutObjectRequest,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const STAGING: R2KeyNamespace = R2KeyNamespace::Staging {
    single_put_bytes_max: 8 * 1024 * 1024,
};

#[test]
fn staging_puts_every_key_under_its_prefix() {
    assert_eq!(
        STAGING.physical_key("bronze/vworld/a.zip").ok().as_deref(),
        Some("staging/bronze/vworld/a.zip")
    );
    assert_eq!(
        R2KeyNamespace::Production
            .physical_key("bronze/vworld/a.zip")
            .ok()
            .as_deref(),
        Some("bronze/vworld/a.zip")
    );
}

#[test]
fn a_key_that_names_the_prefix_or_is_not_relative_is_refused_in_both_namespaces() {
    for namespace in [STAGING, R2KeyNamespace::Production] {
        for key in ["staging/bronze/a.zip", "staging/", "", "/bronze/a.zip"] {
            assert!(
                namespace.physical_key(key).is_err(),
                "{namespace:?} accepted {key:?}"
            );
        }
    }
}

#[test]
fn a_listing_sees_only_its_own_namespace() {
    assert_eq!(STAGING.logical_key("staging/bronze/a"), Some("bronze/a"));
    assert_eq!(STAGING.logical_key("bronze/a"), None);
    assert_eq!(
        R2KeyNamespace::Production.logical_key("bronze/a"),
        Some("bronze/a")
    );
    assert_eq!(
        R2KeyNamespace::Production.logical_key("staging/bronze/a"),
        None
    );
}

#[test]
fn only_the_staging_environment_selects_staging() {
    assert_eq!(
        R2KeyNamespace::from_settings(None, None).ok(),
        Some(R2KeyNamespace::Production)
    );
    assert_eq!(
        R2KeyNamespace::from_settings(Some("production"), None).ok(),
        Some(R2KeyNamespace::Production)
    );
    assert!(matches!(
        R2KeyNamespace::from_settings(Some("staging"), None),
        Ok(R2KeyNamespace::Staging { .. })
    ));
    assert_eq!(
        R2KeyNamespace::from_settings(Some("staging"), Some("67108864")).ok(),
        Some(R2KeyNamespace::Staging {
            single_put_bytes_max: 67_108_864
        })
    );
}

#[test]
fn the_multipart_threshold_cannot_change_production_and_stays_in_range() {
    assert!(R2KeyNamespace::from_settings(Some("production"), Some("67108864")).is_err());
    assert!(R2KeyNamespace::from_settings(None, Some("67108864")).is_err());
    for out_of_range in ["0", "1048576", "8589934592", "many"] {
        assert!(
            R2KeyNamespace::from_settings(Some("staging"), Some(out_of_range)).is_err(),
            "{out_of_range}"
        );
    }
    let production = R2KeyNamespace::Production;
    assert!(!production.requires_multipart(64 * 1024 * 1024));
    assert!(STAGING.requires_multipart(8 * 1024 * 1024));
    assert!(!STAGING.requires_multipart(8 * 1024 * 1024 - 1));
}

/// The wire check alone, as if a call site had bypassed the rewrite: staging refuses anything
/// outside its prefix, production anything inside it.
#[test]
fn the_wire_check_refuses_a_request_that_left_its_namespace() {
    let wire = |namespace: R2KeyNamespace, method: &str, uri: &str, copy: Option<&str>| {
        namespace.admits_on_wire("bucket", method, uri, copy)
    };
    let host = "https://account.r2.cloudflarestorage.com";
    assert!(wire(
        STAGING,
        "PUT",
        &format!("{host}/bucket/staging/bronze/a"),
        None
    )
    .is_ok());
    assert!(wire(STAGING, "PUT", &format!("{host}/bucket/bronze/a"), None).is_err());
    assert!(wire(
        STAGING,
        "DELETE",
        &format!("{host}/bucket/gold/manifest.json"),
        None
    )
    .is_err());
    assert!(wire(
        STAGING,
        "GET",
        &format!("{host}/bucket/bronze/a?x-id=GetObject"),
        None
    )
    .is_err());
    assert!(wire(STAGING, "POST", &format!("{host}/bucket/?delete"), None).is_err());
    assert!(wire(
        STAGING,
        "GET",
        &format!("{host}/bucket?list-type=2&prefix=bronze%2F"),
        None
    )
    .is_err());
    assert!(wire(
        STAGING,
        "GET",
        &format!("{host}/bucket?list-type=2&prefix=staging%2Fbronze%2F"),
        None
    )
    .is_ok());
    assert!(wire(
        STAGING,
        "PUT",
        &format!("{host}/bucket/staging/b"),
        Some("bucket/bronze/a")
    )
    .is_err());
    assert!(wire(STAGING, "PUT", &format!("{host}/other/staging/a"), None).is_err());

    let production = R2KeyNamespace::Production;
    assert!(wire(production, "PUT", &format!("{host}/bucket/bronze/a"), None).is_ok());
    assert!(wire(
        production,
        "PUT",
        &format!("{host}/bucket/staging/bronze/a"),
        None
    )
    .is_err());
    assert!(wire(
        production,
        "PUT",
        &format!("{host}/bucket/bronze/b"),
        Some("bucket/staging/a")
    )
    .is_err());
}

#[test]
fn a_serialized_request_is_rewritten_into_staging() -> Result<(), String> {
    assert_eq!(
        STAGING.rewrite("/bronze/a.zip?x-id=PutObject", None)?,
        ("/staging/bronze/a.zip?x-id=PutObject".to_owned(), None)
    );
    assert_eq!(
        STAGING.rewrite("/?list-type=2&prefix=bronze%2F", None)?.0,
        "/?list-type=2&prefix=staging%2Fbronze%2F"
    );
    assert_eq!(
        STAGING.rewrite("/?list-type=2&max-keys=10", None)?.0,
        "/?list-type=2&max-keys=10&prefix=staging%2F"
    );
    assert_eq!(
        STAGING
            .rewrite("/b?x-id=CopyObject", Some("bucket/bronze/a"))?
            .1,
        Some("bucket/staging/bronze/a".to_owned())
    );
    assert_eq!(
        R2KeyNamespace::Production.rewrite("/bronze/a.zip", None)?.0,
        "/bronze/a.zip"
    );
    assert!(R2KeyNamespace::Production
        .rewrite("/staging/a.zip", None)
        .is_err());
    Ok(())
}

// --- the real client, as `from_config` builds it, against a loopback S3 ------------------------

/// Answers each request in turn with the next response and hands back every request's head.
async fn serve(
    responses: Vec<String>,
) -> TestResult<(String, tokio::task::JoinHandle<TestResult<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut heads = Vec::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        for response in responses {
            let (mut socket, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            let header_end = loop {
                let count = socket.read(&mut buffer).await?;
                if count == 0 {
                    return Err("client disconnected before request headers".into());
                }
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let head = String::from_utf8(bytes[..header_end].to_vec())?;
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then_some(value.trim())
                })
                .unwrap_or("0")
                .parse::<usize>()?;
            while bytes.len() < header_end + length {
                let count = socket.read(&mut buffer).await?;
                if count == 0 {
                    return Err("client disconnected before request body".into());
                }
                bytes.extend_from_slice(&buffer[..count]);
            }
            socket.write_all(response.as_bytes()).await?;
            socket.shutdown().await?;
            heads.push(head);
        }
        Ok(heads)
    });
    Ok((endpoint, server))
}

fn reply(status: u16, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} Mock\r\nConnection: close\r\nContent-Type: application/xml\r\n{headers}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn client(endpoint: String, namespace: R2KeyNamespace) -> R2ObjectStorage {
    R2ObjectStorage::from_config(R2ObjectStorageConfig {
        bucket_name: "bucket".to_owned(),
        endpoint,
        region: "auto".to_owned(),
        access_key_id: "local-access".to_owned(),
        secret_access_key: "local-secret".to_owned(),
        namespace,
    })
}

fn put(key: &str) -> PutObjectRequest {
    PutObjectRequest {
        key: key.to_owned(),
        body: b"{}\n".to_vec(),
        content_type: "application/json".to_owned(),
        cache_control: "no-cache".to_owned(),
        write_mode: ObjectWriteMode::OverwriteAllowed,
        sha256: None,
    }
}

#[tokio::test]
async fn a_staging_write_lands_under_the_prefix() -> TestResult {
    let (endpoint, server) = serve(vec![reply(200, "ETag: \"e\"\r\n", "")]).await?;
    tokio::time::timeout(
        Duration::from_secs(15),
        client(endpoint, STAGING).put_object(put("bronze/vworld/a.json")),
    )
    .await??;
    let heads = tokio::time::timeout(Duration::from_secs(15), server).await???;
    assert!(
        heads[0].starts_with("PUT /bucket/staging/bronze/vworld/a.json"),
        "{}",
        heads[0]
    );
    Ok(())
}

#[tokio::test]
async fn a_staging_listing_asks_inside_the_prefix_and_answers_logical_keys() -> TestResult {
    let body = "<ListBucketResult><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>\
                <Contents><Key>staging/bronze/a.zip</Key><Size>3</Size></Contents></ListBucketResult>";
    let (endpoint, server) = serve(vec![reply(200, "", body)]).await?;
    let report = tokio::time::timeout(
        Duration::from_secs(15),
        client(endpoint, STAGING)
            .inventory_audit(R2InventoryRequest::new(Some("bronze/"), Some(100))?),
    )
    .await??;
    let heads = tokio::time::timeout(Duration::from_secs(15), server).await???;
    assert!(
        heads[0].contains("prefix=staging%2Fbronze%2F"),
        "{}",
        heads[0]
    );
    let keys: Vec<_> = report
        .objects()
        .iter()
        .map(|object| object.key.as_str())
        .collect();
    assert_eq!(keys, ["bronze/a.zip"]);
    Ok(())
}

/// The path the 2026-10-10 incident took on real R2 (root ADR-0177): a spooled body big enough to
/// go multipart. In staging the threshold is low enough for a sample file to take it.
#[tokio::test]
async fn a_staging_stream_over_the_threshold_goes_multipart_under_the_prefix() -> TestResult {
    let created =
        "<InitiateMultipartUploadResult><UploadId>u-1</UploadId></InitiateMultipartUploadResult>";
    let completed =
        "<CompleteMultipartUploadResult><ETag>\"m\"</ETag></CompleteMultipartUploadResult>";
    let (endpoint, server) = serve(vec![
        reply(200, "", created),
        reply(200, "ETag: \"p1\"\r\n", ""),
        reply(200, "", completed),
    ])
    .await?;
    let size = 9 * 1024 * 1024;
    tokio::time::timeout(
        Duration::from_secs(30),
        client(endpoint, STAGING).put_streaming_object(StreamingPutObjectRequest {
            key: "bronze/vworld/big.zip".to_owned(),
            content_type: "application/zip".to_owned(),
            cache_control: "no-cache".to_owned(),
            size_bytes: size,
            body: ByteStream::from(vec![7_u8; usize::try_from(size)?]),
            write_mode: ObjectWriteMode::CreateOnly,
        }),
    )
    .await??;
    let heads = tokio::time::timeout(Duration::from_secs(30), server).await???;
    assert!(
        heads[0].starts_with("POST /bucket/staging/bronze/vworld/big.zip?uploads"),
        "{}",
        heads[0]
    );
    assert!(
        heads[1].starts_with("PUT /bucket/staging/bronze/vworld/big.zip?"),
        "{}",
        heads[1]
    );
    assert!(
        heads[2].starts_with("POST /bucket/staging/bronze/vworld/big.zip?"),
        "{}",
        heads[2]
    );
    Ok(())
}

#[tokio::test]
async fn production_refuses_a_staging_key_before_anything_is_sent() -> TestResult {
    // The server would accept the write: only the client's refusal can make it fail.
    let (endpoint, server) = serve(vec![reply(200, "ETag: \"e\"\r\n", "")]).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        client(endpoint, R2KeyNamespace::Production).put_object(put("staging/bronze/a.json")),
    )
    .await?;
    assert!(result.is_err(), "production wrote into staging");
    assert!(!server.is_finished(), "the request reached the server");
    server.abort();
    Ok(())
}
