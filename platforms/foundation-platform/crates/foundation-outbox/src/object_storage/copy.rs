//! Shared immutable-copy planning and full-byte readback reconciliation.

use crate::errors::PublishError;

use super::{
    requests::{MAX_MULTIPART_PARTS, MAX_MULTIPART_PART_BYTES},
    CreateOnlyCopyObjectRequest, ObjectStorageStreamingService, StreamingObjectRehash,
    SINGLE_COPY_MAX_BYTES,
};

const COPY_PART_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CopyPlan {
    Single,
    Multipart { part_bytes: u64, part_count: u64 },
}

pub(super) fn copy_plan(request: &CreateOnlyCopyObjectRequest) -> Result<CopyPlan, PublishError> {
    if request.source_key.trim().is_empty()
        || request.destination_key.trim().is_empty()
        || request.source_key == request.destination_key
    {
        return Err(PublishError::Infrastructure(
            "create-only copy requires distinct, nonempty source and destination keys".to_owned(),
        ));
    }
    if request.single_copy_max_bytes == 0 || request.single_copy_max_bytes > SINGLE_COPY_MAX_BYTES {
        return Err(PublishError::Infrastructure(format!(
            "single-copy threshold must be between 1 and {SINGLE_COPY_MAX_BYTES} bytes"
        )));
    }
    if request.size_bytes <= request.single_copy_max_bytes {
        return Ok(CopyPlan::Single);
    }
    // Equal-sized parts except the last; 64 MiB normally, larger only to stay below 10,000 parts.
    // Copy requests contain ranges only, so large part sizes never allocate large client buffers.
    let part_bytes =
        COPY_PART_BYTES.max(request.size_bytes.div_ceil(u64::from(MAX_MULTIPART_PARTS)));
    if part_bytes > MAX_MULTIPART_PART_BYTES {
        return Err(PublishError::Infrastructure(
            "create-only copy exceeds 10,000 parts of at most 5 GiB".to_owned(),
        ));
    }
    Ok(CopyPlan::Multipart {
        part_bytes,
        part_count: request.size_bytes.div_ceil(part_bytes),
    })
}

/// Copies an immutable object and rehashes every destination byte against source ledger evidence.
///
/// A create-only collision is recoverable only when the destination's full SHA-256 and byte count
/// both match `expected`. Metadata checksums, ETags, and object names are never sufficient. Source
/// and destination ETags/timestamps may differ, so the returned value carries destination evidence.
///
/// # Errors
/// Returns `PublishError` for invalid source evidence, non-collision write errors, missing readback,
/// or any mismatch. A mismatching destination remains untouched for investigation.
pub async fn create_only_copy_and_rehash(
    writer: &dyn ObjectStorageStreamingService,
    reader: &dyn ObjectStorageStreamingService,
    request: CreateOnlyCopyObjectRequest,
    expected: &StreamingObjectRehash,
) -> Result<StreamingObjectRehash, PublishError> {
    copy_plan(&request)?;
    if request.size_bytes != expected.size_bytes
        || expected.checksum_sha256.len() != 64
        || !expected
            .checksum_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PublishError::Infrastructure(
            "create-only copy requires matching source ledger size and lowercase SHA-256"
                .to_owned(),
        ));
    }
    let destination_key = request.destination_key.clone();
    match writer.copy_object_create_only(request).await {
        Ok(()) => {}
        Err(PublishError::ObjectAlreadyExists { key }) if key == destination_key => {}
        Err(error) => return Err(error),
    }
    verify_copy_readback(reader, &destination_key, expected).await
}

pub(super) async fn verify_copy_readback(
    reader: &dyn ObjectStorageStreamingService,
    key: &str,
    expected: &StreamingObjectRehash,
) -> Result<StreamingObjectRehash, PublishError> {
    let actual = reader
        .read_object_sha256_and_size_by_rehash(key)
        .await?
        .ok_or_else(|| {
            PublishError::Infrastructure(format!("copied destination {key} is absent at readback"))
        })?;
    if actual.size_bytes != expected.size_bytes
        || actual.checksum_sha256 != expected.checksum_sha256
    {
        return Err(PublishError::Infrastructure(format!(
            "copied destination {key} full-byte SHA-256 or size differs from source evidence"
        )));
    }
    Ok(actual)
}
