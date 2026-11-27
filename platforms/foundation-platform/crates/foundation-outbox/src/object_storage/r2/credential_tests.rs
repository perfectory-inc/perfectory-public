//! A client with a `credential_process` asks it again once the credentials it gave expire, so a run longer
//! than their time limit keeps writing (W101c, 2026-11-27: onbid's first AWS run outlived its 1 h key).
#![cfg(unix)]

use std::path::Path;
use std::time::Duration;

use super::{R2KeyNamespace, R2ObjectStorage, R2ObjectStorageConfig};
use crate::object_storage::{ObjectStorageService, ObjectWriteMode, PutObjectRequest};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Answers `requests` requests with 200 on a local port; returns its endpoint.
async fn serve(requests: usize) -> Result<String, std::io::Error> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        for _ in 0..requests {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buffer = vec![0_u8; 64 * 1024];
            let _read = stream.read(&mut buffer).await;
            let reply =
                "HTTP/1.1 200 OK\r\nConnection: close\r\nETag: \"e\"\r\nContent-Length: 0\r\n\r\n";
            let _written = stream.write_all(reply.as_bytes()).await;
        }
    });
    Ok(endpoint)
}

/// A credential process that counts its runs in `counter` and gives credentials expiring in `lifetime`.
fn process(counter: &Path, lifetime: chrono::Duration) -> String {
    let expiration =
        (chrono::Utc::now() + lifetime).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let answer = format!(
        r#"{{"Version": 1, "AccessKeyId": "temporary", "SecretAccessKey": "secret", "SessionToken": "session", "Expiration": "{expiration}"}}"#
    );
    format!(
        "echo asked >> '{}'; printf '%s' '{answer}'",
        counter.display()
    )
}

/// Two writes through a client whose credential process gives credentials of `lifetime`; how often it ran.
async fn runs_for_two_writes(
    lifetime: chrono::Duration,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let scratch = tempfile::tempdir()?;
    let counter = scratch.path().join("asked");
    let storage = R2ObjectStorage::from_config(R2ObjectStorageConfig {
        bucket_name: "bucket".to_owned(),
        endpoint: serve(2).await?,
        region: "auto".to_owned(),
        access_key_id: "unused".to_owned(),
        secret_access_key: "unused".to_owned(),
        session_token: None,
        credential_process: Some(process(&counter, lifetime)),
        namespace: R2KeyNamespace::Production,
    });
    for key in ["bronze/refresh/a.json", "bronze/refresh/b.json"] {
        tokio::time::timeout(
            Duration::from_secs(15),
            storage.put_object(PutObjectRequest {
                key: key.to_owned(),
                body: b"{}\n".to_vec(),
                content_type: "application/json".to_owned(),
                cache_control: "no-cache".to_owned(),
                write_mode: ObjectWriteMode::OverwriteAllowed,
                sha256: None,
            }),
        )
        .await??;
    }
    Ok(std::fs::read_to_string(&counter)?.lines().count())
}

#[tokio::test]
async fn credentials_that_expire_inside_a_run_are_asked_again() -> TestResult {
    // A time limit shorter than the run: the second write must not use the first, expired, credentials.
    assert_eq!(runs_for_two_writes(chrono::Duration::seconds(2)).await?, 2);
    Ok(())
}

#[tokio::test]
async fn credentials_that_outlive_the_run_are_asked_once() -> TestResult {
    assert_eq!(runs_for_two_writes(chrono::Duration::hours(1)).await?, 1);
    Ok(())
}
