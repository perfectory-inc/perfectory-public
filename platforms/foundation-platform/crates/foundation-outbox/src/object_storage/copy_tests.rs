use super::{
    copy::{copy_plan, CopyPlan},
    create_only_copy_and_rehash, sha256_hex, CreateOnlyCopyObjectRequest, FileObjectStorage,
    ObjectStorageService, ObjectStorageStreamingService, ObjectWriteMode, PutObjectRequest,
    StreamingObjectRehash, SINGLE_COPY_MAX_BYTES,
};
use crate::errors::PublishError;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn request(limit: u64) -> CreateOnlyCopyObjectRequest {
    CreateOnlyCopyObjectRequest {
        source_key: "source/archive.bin".to_owned(),
        destination_key: "destination/archive.bin".to_owned(),
        size_bytes: 6,
        single_copy_max_bytes: limit,
    }
}

fn expected() -> StreamingObjectRehash {
    StreamingObjectRehash {
        checksum_sha256: sha256_hex(b"abcdef"),
        size_bytes: 6,
        observed_e_tag: Some("source-entity-tag".to_owned()),
        observed_last_modified: None,
    }
}

async fn source(storage: &FileObjectStorage) -> Result<(), PublishError> {
    storage
        .put_object(PutObjectRequest {
            key: request(6).source_key,
            body: b"abcdef".to_vec(),
            content_type: "application/octet-stream".to_owned(),
            cache_control: "no-store".to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: None,
        })
        .await
}

#[test]
fn copy_plan_obeys_boundary_and_bounds_part_count() -> TestResult {
    assert_eq!(copy_plan(&request(6))?, CopyPlan::Single);
    assert!(matches!(
        copy_plan(&request(5))?,
        CopyPlan::Multipart { .. }
    ));
    let mut large = request(SINGLE_COPY_MAX_BYTES);
    large.size_bytes = SINGLE_COPY_MAX_BYTES;
    assert_eq!(copy_plan(&large)?, CopyPlan::Single);
    large.size_bytes += 1;
    let CopyPlan::Multipart {
        part_bytes,
        part_count,
    } = copy_plan(&large)?
    else {
        return Err("large copy must use multipart".into());
    };
    assert!(part_bytes >= 5 * 1024 * 1024);
    assert!(part_bytes <= SINGLE_COPY_MAX_BYTES);
    assert!(part_count > 1 && part_count <= 10_000);
    large.size_bytes = SINGLE_COPY_MAX_BYTES * 10_000;
    let CopyPlan::Multipart { part_count, .. } = copy_plan(&large)? else {
        return Err("largest copy must use multipart".into());
    };
    assert!(part_count <= 10_000);
    large.size_bytes += 1;
    assert!(copy_plan(&large).is_err());
    assert!(copy_plan(&request(0)).is_err());
    assert!(copy_plan(&request(SINGLE_COPY_MAX_BYTES + 1)).is_err());
    Ok(())
}

#[tokio::test]
async fn file_copy_both_paths_refuse_second_copy_and_helper_reconciles_bytes() -> TestResult {
    for limit in [6, 2] {
        let root = std::env::temp_dir().join(format!("create-only-copy-{}", uuid::Uuid::new_v4()));
        let storage = FileObjectStorage::new(&root)?;
        source(&storage).await?;
        let copy = request(limit);
        storage.copy_object_create_only(copy.clone()).await?;
        assert_eq!(storage.get_object_bytes(&copy.destination_key)?, b"abcdef");
        assert!(matches!(
            storage.copy_object_create_only(copy.clone()).await,
            Err(PublishError::ObjectAlreadyExists { .. })
        ));
        let readback =
            create_only_copy_and_rehash(&storage, &storage, copy.clone(), &expected()).await?;
        assert_eq!(readback.checksum_sha256, expected().checksum_sha256);
        assert_eq!(readback.size_bytes, 6);
        assert_ne!(readback.observed_e_tag, expected().observed_e_tag);
        std::fs::write(root.join(&copy.destination_key), b"ghijkl")?;
        assert!(
            create_only_copy_and_rehash(&storage, &storage, copy, &expected())
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root)?;
    }
    Ok(())
}

#[tokio::test]
async fn copy_helper_rejects_wrong_readback_after_success_and_invalid_ledger_before_write(
) -> TestResult {
    let root = std::env::temp_dir().join(format!("create-only-copy-{}", uuid::Uuid::new_v4()));
    let storage = FileObjectStorage::new(&root)?;
    source(&storage).await?;
    let mut wrong_size = expected();
    wrong_size.size_bytes = 7;
    assert!(
        create_only_copy_and_rehash(&storage, &storage, request(6), &wrong_size)
            .await
            .is_err()
    );
    assert!(!root.join(request(6).destination_key).exists());
    let mut wrong_hash = expected();
    wrong_hash.checksum_sha256 = sha256_hex(b"ghijkl");
    assert!(
        create_only_copy_and_rehash(&storage, &storage, request(6), &wrong_hash)
            .await
            .is_err()
    );
    assert!(root.join(request(6).destination_key).exists());
    std::fs::write(root.join(request(6).destination_key), b"abc")?;
    assert!(
        create_only_copy_and_rehash(&storage, &storage, request(6), &expected())
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn file_copy_refuses_source_size_mismatch_without_creating_destination() -> TestResult {
    let root = std::env::temp_dir().join(format!("create-only-copy-{}", uuid::Uuid::new_v4()));
    let storage = FileObjectStorage::new(&root)?;
    source(&storage).await?;
    let mut copy = request(6);
    copy.size_bytes = 5;
    assert!(storage.copy_object_create_only(copy.clone()).await.is_err());
    assert!(!root.join(&copy.destination_key).exists());
    copy.source_key = "../outside".to_owned();
    assert!(storage.copy_object_create_only(copy).await.is_err());
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn adapters_without_copy_support_fail_closed() {
    let storage = super::LoggingObjectStorage;
    let error = storage.copy_object_create_only(request(6)).await;
    assert!(matches!(error, Err(PublishError::Infrastructure(_))));
}

#[tokio::test]
async fn failed_local_copy_leaves_no_partial_destination_and_retry_succeeds() -> TestResult {
    let root = std::env::temp_dir().join(format!("failed-copy-{}", uuid::Uuid::new_v4()));
    let storage = FileObjectStorage::new(&root)?;
    source(&storage).await?;
    let mut copy = request(SINGLE_COPY_MAX_BYTES);
    copy.size_bytes = 64 * 1024 * 1024;
    let source_file = std::fs::OpenOptions::new()
        .write(true)
        .open(root.join(&copy.source_key))?;
    source_file.set_len(copy.size_bytes)?;
    let mut copying = storage.copy_object_create_only(copy.clone());
    let mut interrupted = false;
    let mut interruption = Ok(());
    let result = std::future::poll_fn(|cx| {
        let result = copying.as_mut().poll(cx);
        // Intervene after creation, while a real filesystem copy is in flight. This observes
        // either the old final path or the new temporary file, without depending on its name.
        if !interrupted
            && matches!(result, std::task::Poll::Pending)
            && std::fs::read_dir(root.join("destination"))
                .is_ok_and(|mut files| files.next().is_some())
        {
            interruption = source_file.set_len(0);
            interrupted = true;
        }
        result
    })
    .await;
    drop(copying);
    interruption?;
    assert!(
        interrupted,
        "copy must be interrupted after creating its output"
    );
    assert!(result.is_err());
    assert!(
        !root.join(&copy.destination_key).exists(),
        "failed copy published partial bytes"
    );
    assert_eq!(std::fs::read_dir(root.join("destination"))?.count(), 0);
    drop(source_file);
    std::fs::write(root.join(&copy.source_key), b"abcdef")?;
    create_only_copy_and_rehash(&storage, &storage, request(6), &expected()).await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
