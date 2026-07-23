use std::fs;

use aws_sdk_s3::primitives::ByteStream;

use super::{
    r2_range_header, r2_range_windows, sha256_hex, validate_r2_bronze_key_migration_pair,
    validate_r2_rehash_identity_stable, FileObjectStorage, ObjectStorageService,
    ObjectStorageStreamingService, ObjectWriteMode, PutObjectRequest, R2ObjectVersionFingerprint,
    R2RangeHasher, StreamingPutObjectRequest,
};
use crate::errors::PublishError;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn file_object_storage_writes_and_reads_exact_bytes() -> TestResult {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-file-object-storage-{}",
        uuid::Uuid::new_v4()
    ));
    let storage = FileObjectStorage::new(&root)?;
    let body = br#"{"response":{"body":{"items":{"item":[{"pnu":"1168010300"}]}}}}"#.to_vec();

    storage
        .put_object(PutObjectRequest {
            key: "bronze/source=molit-building-register/part-000001.json".to_owned(),
            body: body.clone(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::OverwriteAllowed,
            sha256: None,
        })
        .await?;

    let stored =
        storage.get_object_bytes("bronze/source=molit-building-register/part-000001.json")?;
    assert_eq!(stored, body);
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn file_object_storage_streaming_write_persists_chunks_without_full_body_request(
) -> TestResult {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-file-object-storage-stream-{}",
        uuid::Uuid::new_v4()
    ));
    let storage = FileObjectStorage::new(&root)?;

    storage
        .put_streaming_object(StreamingPutObjectRequest {
            key: "bronze/source=test/run=1/file.zip".to_owned(),
            content_type: "application/zip".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            size_bytes: 6,
            body: ByteStream::from_static(b"abcdef"),
            write_mode: ObjectWriteMode::OverwriteAllowed,
        })
        .await?;

    let bytes = fs::read(root.join("bronze/source=test/run=1/file.zip"))?;
    assert_eq!(bytes, b"abcdef");
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn file_object_storage_rejects_path_traversal_keys() -> TestResult {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-file-object-storage-{}",
        uuid::Uuid::new_v4()
    ));
    let storage = FileObjectStorage::new(&root)?;

    let error = storage
        .put_object(PutObjectRequest {
            key: "../gold/manifest.json".to_owned(),
            body: b"{}".to_vec(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::OverwriteAllowed,
            sha256: None,
        })
        .await
        .err()
        .ok_or("expected traversal key rejection")?;

    assert!(
        error.to_string().contains("object key"),
        "unexpected error: {error}"
    );
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn file_object_storage_create_only_rejects_existing_key() -> TestResult {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-file-object-storage-create-only-{}",
        uuid::Uuid::new_v4()
    ));
    let storage = FileObjectStorage::new(&root)?;
    let key = "bronze/source=test/part-000001.json".to_owned();

    // First CreateOnly write succeeds.
    storage
        .put_object(PutObjectRequest {
            key: key.clone(),
            body: b"first".to_vec(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: None,
        })
        .await?;

    // Second CreateOnly write to the same key fails with ObjectAlreadyExists and
    // does NOT mutate the stored bytes.
    let error = storage
        .put_object(PutObjectRequest {
            key: key.clone(),
            body: b"second".to_vec(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: None,
        })
        .await
        .err()
        .ok_or("expected CreateOnly overwrite rejection")?;
    match error {
        PublishError::ObjectAlreadyExists { key: collided } => assert_eq!(collided, key),
        other => return Err(format!("unexpected error: {other}").into()),
    }
    assert_eq!(storage.get_object_bytes(&key)?, b"first");

    // OverwriteAllowed to the same key still overwrites (existing behaviour).
    storage
        .put_object(PutObjectRequest {
            key: key.clone(),
            body: b"third".to_vec(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::OverwriteAllowed,
            sha256: None,
        })
        .await?;
    assert_eq!(storage.get_object_bytes(&key)?, b"third");

    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn file_object_storage_read_sha256_rehashes_existing_bytes_and_is_none_when_absent(
) -> TestResult {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-file-object-storage-sha256-{}",
        uuid::Uuid::new_v4()
    ));
    let storage = FileObjectStorage::new(&root)?;
    let key = "bronze/source=test/part-000001.json".to_owned();
    let body = br#"{"pnu":"1168010300"}"#.to_vec();

    // Absent key => no stored checksum (mirrors R2 returning no head metadata).
    assert_eq!(storage.read_object_sha256(&key).await?, None);

    // The local adapter rehashes the stored bytes, so the read-back equals the canonical
    // lowercase-hex SHA-256 of exactly what was written. (No side-car metadata is kept.)
    storage
        .put_object(PutObjectRequest {
            key: key.clone(),
            body: body.clone(),
            content_type: "application/json".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            // sha256 metadata is irrelevant locally: read-back rehashes the bytes.
            sha256: Some(sha256_hex(&body)),
        })
        .await?;

    assert_eq!(
        storage.read_object_sha256(&key).await?,
        Some(sha256_hex(&body))
    );

    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn file_object_storage_streaming_create_only_rejects_existing_key() -> TestResult {
    let root = std::env::temp_dir().join(format!(
        "foundation-platform-file-object-storage-stream-create-only-{}",
        uuid::Uuid::new_v4()
    ));
    let storage = FileObjectStorage::new(&root)?;
    let key = "bronze/source=test/run=1/file.zip".to_owned();

    storage
        .put_streaming_object(StreamingPutObjectRequest {
            key: key.clone(),
            content_type: "application/zip".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            size_bytes: 6,
            body: ByteStream::from_static(b"abcdef"),
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await?;

    let error = storage
        .put_streaming_object(StreamingPutObjectRequest {
            key: key.clone(),
            content_type: "application/zip".to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            size_bytes: 6,
            body: ByteStream::from_static(b"ghijkl"),
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await
        .err()
        .ok_or("expected streaming CreateOnly overwrite rejection")?;
    match error {
        PublishError::ObjectAlreadyExists { key: collided } => assert_eq!(collided, key),
        other => return Err(format!("unexpected error: {other}").into()),
    }
    assert_eq!(
        fs::read(root.join("bronze/source=test/run=1/file.zip"))?,
        b"abcdef"
    );

    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn r2_412_is_already_exists_only_for_create_only() {
    // The R2 adapter sets If-None-Match: * only under CreateOnly and maps the
    // resulting 412 to ObjectAlreadyExists. We assert that decision via the pure
    // classifier (fabricating a real aws-sdk-s3 response is impractical here; the
    // status-extraction wiring is covered by the live R2 contract tests).
    assert!(super::is_create_only_already_exists_response(
        ObjectWriteMode::CreateOnly,
        Some(412),
        None
    ));
    // A different status under CreateOnly is NOT a write-once collision.
    assert!(!super::is_create_only_already_exists_response(
        ObjectWriteMode::CreateOnly,
        Some(409),
        None
    ));
    // No raw response (dispatch/timeout) is not a collision.
    assert!(!super::is_create_only_already_exists_response(
        ObjectWriteMode::CreateOnly,
        None,
        None
    ));
    // OverwriteAllowed never treats a 412 as a collision.
    assert!(!super::is_create_only_already_exists_response(
        ObjectWriteMode::OverwriteAllowed,
        Some(412),
        None
    ));
}

#[test]
fn r2_precondition_failed_code_is_already_exists_for_create_only() {
    // Live R2 streaming PutObject can surface the conditional-write collision as a
    // service error code even when the SDK raw-response status is unavailable to our
    // mapper. Keep the recoverable commit protocol keyed to the provider error
    // semantics, not only to the optional HTTP status extraction.
    assert!(super::is_create_only_already_exists_response(
        ObjectWriteMode::CreateOnly,
        None,
        Some("PreconditionFailed")
    ));
    assert!(!super::is_create_only_already_exists_response(
        ObjectWriteMode::OverwriteAllowed,
        None,
        Some("PreconditionFailed")
    ));
    assert!(!super::is_create_only_already_exists_response(
        ObjectWriteMode::CreateOnly,
        None,
        Some("AccessDenied")
    ));
}

#[test]
fn r2_range_headers_are_inclusive_and_provider_compatible() -> TestResult {
    assert_eq!(r2_range_header(0, 15)?, "bytes=0-15");
    assert_eq!(r2_range_header(16, 31)?, "bytes=16-31");
    assert!(r2_range_header(32, 31).is_err());
    Ok(())
}

#[test]
fn r2_range_windows_cover_object_without_overlap() -> TestResult {
    let windows = r2_range_windows(35, 16)?;
    assert_eq!(windows, vec![(0, 15), (16, 31), (32, 34)]);
    assert!(r2_range_windows(1, 0).is_err());
    Ok(())
}

#[test]
fn r2_range_hasher_matches_whole_payload_without_buffering_it() -> TestResult {
    let payload = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let range_length = i64::try_from(payload.len())?;
    let expected_size_bytes = u64::try_from(payload.len())?;
    let mut hasher = R2RangeHasher::new("bronze/source=test/file.zip", range_length)?;

    hasher.push(0, 15, &payload[0..16])?;
    hasher.push(16, 31, &payload[16..32])?;
    hasher.push(32, 35, &payload[32..36])?;

    let result = hasher.finish()?;
    assert_eq!(result.size_bytes, expected_size_bytes);
    assert_eq!(result.checksum_sha256, sha256_hex(payload));
    Ok(())
}

#[test]
fn r2_range_hasher_rejects_missing_overlapping_and_short_ranges() -> TestResult {
    let mut missing = R2RangeHasher::new("bronze/source=test/file.zip", 8)?;
    missing.push(0, 3, b"abcd")?;
    assert!(missing.push(5, 7, b"fgh").is_err());

    let mut overlapping = R2RangeHasher::new("bronze/source=test/file.zip", 8)?;
    overlapping.push(0, 3, b"abcd")?;
    assert!(overlapping.push(3, 7, b"defgh").is_err());

    let mut short = R2RangeHasher::new("bronze/source=test/file.zip", 8)?;
    assert!(short.push(0, 3, b"abc").is_err());

    let mut incomplete = R2RangeHasher::new("bronze/source=test/file.zip", 8)?;
    incomplete.push(0, 3, b"abcd")?;
    assert!(incomplete.finish().is_err());
    Ok(())
}

#[test]
fn r2_rehash_rejects_object_identity_change_between_head_reads() -> TestResult {
    let before = R2ObjectVersionFingerprint {
        content_length: 12,
        e_tag: Some("etag-before".to_owned()),
        last_modified: Some("2026-07-14T12:00:00Z".to_owned()),
    };
    validate_r2_rehash_identity_stable(
        "bronze/source=vworldkr__parcel/file.zip",
        &before,
        &before,
    )?;

    for after in [
        R2ObjectVersionFingerprint {
            content_length: 13,
            ..before.clone()
        },
        R2ObjectVersionFingerprint {
            e_tag: Some("etag-after".to_owned()),
            ..before.clone()
        },
        R2ObjectVersionFingerprint {
            last_modified: Some("2026-07-14T12:00:01Z".to_owned()),
            ..before.clone()
        },
    ] {
        let result = validate_r2_rehash_identity_stable(
            "bronze/source=vworldkr__parcel/file.zip",
            &before,
            &after,
        );
        let error = match result {
            Ok(()) => return Err("object identity drift during range rehash was accepted".into()),
            Err(error) => error,
        };
        assert!(error.to_string().contains("changed during bounded rehash"));
    }
    Ok(())
}

#[test]
fn r2_bronze_key_migration_pair_accepts_only_legacy_to_canonical_shape() -> TestResult {
    let old_key = "bronze/source=molit-building-register/ingest_date=2026-05-18/run_id=018f0000-0000-7000-8000-000000000001/partition=operation=getBrTitleInfo/page=000001/part-000001.json";
    let new_key = "bronze/source=molit-building-register/run_id=018f0000-0000-7000-8000-000000000001/partition=operation=getBrTitleInfo/page=000001/part-000001.json";

    validate_r2_bronze_key_migration_pair(old_key, new_key)?;
    assert!(validate_r2_bronze_key_migration_pair(new_key, new_key).is_err());
    assert!(validate_r2_bronze_key_migration_pair(old_key, old_key).is_err());
    assert!(validate_r2_bronze_key_migration_pair(old_key, "../gold/manifest.json").is_err());
    Ok(())
}
