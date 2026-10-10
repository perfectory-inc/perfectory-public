use bytes::Bytes;
use collection_domain::CollectionError;
use futures_util::{stream, StreamExt as _};

use super::{spool_payload, SpoolDir, SPOOL_MIN_FREE_BYTES};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn body(
    bytes: &'static [u8],
) -> futures_util::stream::BoxStream<'static, Result<Bytes, CollectionError>> {
    stream::iter([Ok(Bytes::from_static(bytes))]).boxed()
}

/// Room for one 8-byte file beside the kept margin, not two.
fn room_for_one(_: &std::path::Path) -> anyhow::Result<u64> {
    Ok(SPOOL_MIN_FREE_BYTES + 12)
}

/// Root ADR-0168: the free-space check counts the files still in flight, so four 1.5 GiB files at
/// once cannot each pass on a disk that holds one; a finished file gives its room back.
#[tokio::test]
async fn files_in_flight_count_against_the_free_space() -> TestResult {
    let temp = tempfile::tempdir()?;
    let spool = SpoolDir::with_free_bytes(&temp.path().join("spool"), room_for_one)?;

    let first = spool_payload(&spool, body(b"PK\x03\x04aaaa"), "application/zip", 8, "f-1").await?;
    let second = spool_payload(&spool, body(b"PK\x03\x04bbbb"), "application/zip", 8, "f-2").await;
    let error = second.err().ok_or("a second file in flight must not fit")?;
    assert!(
        format!("{error:#}").contains("files in flight 8"),
        "unexpected error: {error:#}"
    );

    drop(first);
    let third = spool_payload(&spool, body(b"PK\x03\x04cccc"), "application/zip", 8, "f-3").await?;
    assert_eq!(third.size_bytes, 8);
    drop(third);
    assert_eq!(
        std::fs::read_dir(spool.path())?.count(),
        0,
        "every spool file is removed"
    );
    Ok(())
}

/// The spooled body reads back the bytes that were hashed, and the file goes when the body does.
#[tokio::test]
async fn the_spooled_body_reads_back_and_removes_its_file() -> TestResult {
    let temp = tempfile::tempdir()?;
    let spool = SpoolDir::with_free_bytes(&temp.path().join("spool"), |_| Ok(u64::MAX))?;
    let spooled = std::sync::Arc::new(
        spool_payload(&spool, body(b"PK\x03\x04data"), "application/zip", 8, "f-1").await?,
    );
    let path = spooled.path().to_owned();
    assert!(path.is_file());
    let read = std::sync::Arc::clone(&spooled)
        .body_stream()
        .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    assert_eq!(read, b"PK\x03\x04data");
    drop(spooled);
    assert!(!path.exists(), "the spool file outlived its last owner");
    Ok(())
}

/// An HTML page in place of the file, or a body longer than declared, leaves nothing behind.
#[tokio::test]
async fn a_refused_body_leaves_no_file() -> TestResult {
    let temp = tempfile::tempdir()?;
    let spool = SpoolDir::with_free_bytes(&temp.path().join("spool"), |_| Ok(u64::MAX))?;
    assert!(
        spool_payload(&spool, body(b"<html>no</html>"), "text/html", 15, "f-1")
            .await
            .is_err()
    );
    assert!(spool_payload(
        &spool,
        body(b"PK\x03\x04toolong"),
        "application/zip",
        4,
        "f-2"
    )
    .await
    .is_err());
    assert_eq!(std::fs::read_dir(spool.path())?.count(), 0);
    Ok(())
}

/// 2026-10-10: the daily sweep aborted on its first spooled file because the upload asked the body
/// once more after it had ended and `unfold` panics then. The body keeps answering `None`.
#[tokio::test]
async fn an_ended_body_can_be_asked_again() -> TestResult {
    let temp = tempfile::tempdir()?;
    let spool = SpoolDir::with_free_bytes(&temp.path().join("spool"), |_| Ok(u64::MAX / 2))?;
    let file = spool_payload(&spool, body(b"PKdddd"), "application/zip", 8, "f-4").await?;
    let mut body = std::sync::Arc::new(file).body_stream();
    let mut read = Vec::new();
    while let Some(chunk) = body.next().await {
        read.extend_from_slice(&chunk?);
    }
    assert_eq!(read, b"PKdddd");
    assert!(body.next().await.is_none(), "asked again after the end");
    assert!(body.next().await.is_none(), "and again");
    Ok(())
}
