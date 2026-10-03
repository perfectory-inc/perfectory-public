//! Compare-and-swap of one mutable object: read it with its version, then replace it only while
//! that version is still the stored one (`If-Match`). A pointer such as a serving manifest is
//! moved this way, so two writers that read the same pointer cannot both replace it and silently
//! drop the other's move.

use aws_sdk_s3::error::ProvideErrorMetadata as _;

use crate::{
    errors::PublishError,
    object_storage::{ByteStream, ObjectWriteMode, PutObjectRequest},
};

use super::{
    map_r2_put_error, R2HttpStatus, R2ObjectStorage, HTTP_NOT_FOUND, HTTP_PRECONDITION_FAILED,
    R2_PRECONDITION_FAILED_CODE, SHA256_METADATA_KEY,
};

/// What a write conditioned on the stored version did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalWrite {
    /// The stored version was the expected one and the object now holds the new bytes.
    Written,
    /// Another writer replaced (or removed) the object since it was read; nothing was written.
    VersionChanged,
}

impl R2ObjectStorage {
    /// Reads an object's exact bytes together with the `ETag` that names this version of it.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the read or omits the `ETag`.
    pub async fn get_object_bytes_and_e_tag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, String), PublishError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!("failed to read R2 object {key}: {error}"))
            })?;
        let e_tag = output.e_tag().map(ToOwned::to_owned).ok_or_else(|| {
            PublishError::Broadcaster(format!("R2 read of {key} returned no ETag"))
        })?;
        let body = output.body.collect().await.map_err(|error| {
            PublishError::Broadcaster(format!("failed to read R2 object body {key}: {error}"))
        })?;
        Ok((body.into_bytes().to_vec(), e_tag))
    }

    /// Replaces an object only while its stored version is `expected_e_tag` (`If-Match`).
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the request is not an overwrite or the provider fails the
    /// write for any reason other than the version having moved.
    pub async fn put_object_if_match(
        &self,
        request: PutObjectRequest,
        expected_e_tag: &str,
    ) -> Result<ConditionalWrite, PublishError> {
        if request.write_mode != ObjectWriteMode::OverwriteAllowed {
            return Err(PublishError::Infrastructure(format!(
                "a version-conditioned write of {} replaces the object; it cannot be create-only",
                request.key
            )));
        }
        let key = request.key;
        let mut builder = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .content_type(request.content_type)
            .cache_control(request.cache_control)
            .if_match(expected_e_tag)
            .body(ByteStream::from(request.body));
        if let Some(sha256) = request.sha256 {
            builder = builder.metadata(SHA256_METADATA_KEY, sha256);
        }
        match builder.send().await {
            Ok(_) => Ok(ConditionalWrite::Written),
            Err(error) => {
                let status = error.raw_response().map(R2HttpStatus::status_code);
                let code = error.as_service_error().and_then(|error| error.code());
                if version_moved(status, code) {
                    Ok(ConditionalWrite::VersionChanged)
                } else {
                    Err(map_r2_put_error(
                        &key,
                        &error,
                        "conditionally replace",
                        ObjectWriteMode::OverwriteAllowed,
                    ))
                }
            }
        }
    }
}

/// `412` (another version is stored) or `404` (the object was removed) under `If-Match`.
fn version_moved(status: Option<u16>, code: Option<&str>) -> bool {
    matches!(status, Some(HTTP_PRECONDITION_FAILED | HTTP_NOT_FOUND))
        || matches!(code, Some(R2_PRECONDITION_FAILED_CODE | "NoSuchKey"))
}

#[cfg(test)]
#[path = "conditional_tests.rs"]
mod tests;
