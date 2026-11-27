use std::time::Duration;

use super::super::{R2ObjectStorage, R2ObjectStorageConfig};
use super::*;
use crate::object_storage::{ObjectStorageService, ObjectWriteMode, PutObjectRequest};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn only_a_put_body_is_a_write() {
    assert_eq!(body_of_write("PUT", None, Some("1024")), 1024);
    assert_eq!(body_of_write("put", None, Some(" 7 ")), 7);
    assert_eq!(
        body_of_write("PUT", Some("100"), Some("180")),
        100,
        "aws-chunked: the payload, not the frames"
    );
    assert_eq!(body_of_write("GET", None, Some("1024")), 0);
    assert_eq!(
        body_of_write("POST", None, Some("512")),
        0,
        "completing a multipart upload sends no data"
    );
    assert_eq!(body_of_write("PUT", None, None), 0);
    assert_eq!(body_of_write("PUT", None, Some("many")), 0);
}

/// Serves `status` to one request on a local port; returns its endpoint.
async fn serve_once(status: u16) -> Result<String, std::io::Error> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = vec![0_u8; 64 * 1024];
            let _read = stream.read(&mut buffer).await;
            let reply = format!(
                "HTTP/1.1 {status} Mock\r\nConnection: close\r\nETag: \"e\"\r\nContent-Length: 0\r\n\r\n"
            );
            let _written = stream.write_all(reply.as_bytes()).await;
        }
    });
    Ok(endpoint)
}

#[tokio::test]
async fn an_accepted_put_adds_its_body() -> TestResult {
    let endpoint = serve_once(200).await?;
    let storage = R2ObjectStorage::from_config(R2ObjectStorageConfig {
        bucket_name: "bucket".to_owned(),
        endpoint,
        region: "auto".to_owned(),
        access_key_id: "local-access".to_owned(),
        secret_access_key: "local-secret".to_owned(),
        session_token: None,
        namespace: super::super::R2KeyNamespace::Production,
    });
    let before = r2_bytes_written();
    tokio::time::timeout(
        Duration::from_secs(15),
        storage.put_object(PutObjectRequest {
            key: "bronze/written/a.json".to_owned(),
            body: vec![b'x'; 4096],
            content_type: "application/json".to_owned(),
            cache_control: "no-cache".to_owned(),
            write_mode: ObjectWriteMode::OverwriteAllowed,
            sha256: None,
        }),
    )
    .await??;
    // Other tests write in parallel through the same counter: it grew by this body at least.
    assert!(r2_bytes_written() - before >= 4096);
    Ok(())
}
