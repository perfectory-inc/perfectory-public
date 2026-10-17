//! The one way this crate writes a whole object to R2: the read policy's attempts and waits
//! (root ADR-0102, ADR-0261), applied to `PutObject`.
//!
//! A PUT that never got an answer (`dispatch failure`, a reset, a timeout), or got a `5xx` or `429`
//! that outlived the SDK's adaptive retry, is sent again as a new request after the same
//! equal-jitter backoff a read waits. A real answer that will not change (`403`, `400`, a `412` of
//! a create-only write) is returned at once.
//!
//! A create-only write (`If-None-Match: *`) that got no answer may have landed, or met an object
//! that was there already: R2 can answer `412` before it has read the body and close the
//! connection, which the client sees as a failed send, not as the `412` (2026-10-16, the by-PNU
//! bake re-sending a pack an earlier attempt of the same shard had written, six times). So before
//! asking again it asks whether the key exists; if it does, the write is
//! [`PublishError::ObjectAlreadyExists`], and the caller's reconcile compares the stored bytes.

use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::primitives::ByteStream;

use super::read_retry::{jitter_unit, request_error_is_transient, R2ReadPolicy};
use super::{is_create_only_already_exists_response, map_r2_put_error, R2HttpStatus};
use crate::errors::PublishError;
use crate::object_storage::{ObjectWriteMode, PutObjectRequest};

const HTTP_NOT_FOUND: u16 = 404;

enum Attempt {
    Done,
    Again(String),
    Final(PublishError),
}

async fn attempt(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    request: &PutObjectRequest,
    sha256_metadata_key: &str,
) -> Attempt {
    let mut put = client
        .put_object()
        .bucket(bucket)
        .key(&request.key)
        .content_type(&request.content_type)
        .cache_control(&request.cache_control)
        .body(ByteStream::from(request.body.clone()));
    if matches!(request.write_mode, ObjectWriteMode::CreateOnly) {
        put = put.if_none_match("*");
    }
    if let Some(sha256) = &request.sha256 {
        put = put.metadata(sha256_metadata_key, sha256);
    }
    match put
        .customize()
        .config_override(super::body_request_timeouts())
        .send()
        .await
    {
        Ok(_) => Attempt::Done,
        Err(error) => {
            let status = error.raw_response().map(R2HttpStatus::status_code);
            let code = error
                .as_service_error()
                .and_then(aws_sdk_s3::error::ProvideErrorMetadata::code);
            if !is_create_only_already_exists_response(request.write_mode, status, code)
                && request_error_is_transient(&error)
            {
                Attempt::Again(format!(
                    "failed to write R2 object {}: {} (http_status={status:?})",
                    request.key,
                    DisplayErrorContext(&error)
                ))
            } else {
                Attempt::Final(map_r2_put_error(
                    &request.key,
                    &error,
                    "write",
                    request.write_mode,
                ))
            }
        }
    }
}

/// Whether `key` exists now: `Some(true)` or `Some(false)` from a real answer, `None` when the
/// question itself got none.
async fn exists(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Option<bool> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => Some(true),
        Err(error) => (error.raw_response().map(R2HttpStatus::status_code) == Some(HTTP_NOT_FOUND))
            .then_some(false),
    }
}

/// Writes one whole object through the policy.
pub async fn write(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    policy: &R2ReadPolicy,
    request: PutObjectRequest,
    sha256_metadata_key: &str,
) -> Result<(), PublishError> {
    let create_only = matches!(request.write_mode, ObjectWriteMode::CreateOnly);
    let mut last = format!("R2 write policy allows no attempt for {}", request.key);
    for number in 1..=policy.max_attempts {
        match attempt(client, bucket, &request, sha256_metadata_key).await {
            Attempt::Done => return Ok(()),
            Attempt::Final(error) => return Err(error),
            Attempt::Again(message) => {
                if create_only && exists(client, bucket, &request.key).await == Some(true) {
                    tracing::warn!(
                        key = %request.key,
                        attempt = number,
                        error = %message,
                        "a create-only R2 write got no answer and the key exists: reconciling"
                    );
                    return Err(PublishError::ObjectAlreadyExists { key: request.key });
                }
                if number < policy.max_attempts {
                    tracing::warn!(
                        key = %request.key,
                        attempt = number,
                        max_attempts = policy.max_attempts,
                        error = %message,
                        "writing to R2 again after a transient failure"
                    );
                    tokio::time::sleep(policy.delay(number, jitter_unit())).await;
                }
                last = message;
            }
        }
    }
    Err(PublishError::Broadcaster(last))
}
