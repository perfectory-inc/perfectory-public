//! Cloudflare R2 (S3-compatible) object storage adapter and its supporting helpers.

use std::{env, time::Duration};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    Client,
};
use sha2::{Digest as _, Sha256};

use crate::errors::PublishError;

use super::{
    ByteStream, ObjectStorageService, ObjectStorageSmokeReport, ObjectStorageStreamingService,
    ObjectWriteMode, PutObjectRequest, R2InventoryAuditReport, R2InventoryObject,
    R2InventoryReport, R2InventoryRequest, StreamingObjectRehash, StreamingPutObjectRequest,
};

/// Default R2 object key used by the smoke command.
pub const DEFAULT_R2_SMOKE_OBJECT_KEY: &str = "gold/_smoke/foundation-platform-r2-smoke.json";

const R2_SMOKE_CONTENT_TYPE: &str = "application/json";
const R2_SMOKE_CACHE_CONTROL: &str = "no-store, max-age=0";
const CANONICAL_MANIFEST_POINTER_OBJECT_KEY: &str = "gold/manifest.json";
const R2_RANGE_READ_CHUNK_BYTES: i64 = 16 * 1024 * 1024;
const R2_RANGE_READ_MAX_ATTEMPTS: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct R2ObjectVersionFingerprint {
    pub(super) content_length: i64,
    pub(super) e_tag: Option<String>,
    pub(super) last_modified: Option<String>,
}

pub(super) fn validate_r2_rehash_identity_stable(
    key: &str,
    before: &R2ObjectVersionFingerprint,
    after: &R2ObjectVersionFingerprint,
) -> Result<(), PublishError> {
    if before != after {
        return Err(PublishError::Broadcaster(format!(
            "R2 object {key} changed during bounded rehash"
        )));
    }
    Ok(())
}

pub(super) struct R2RangeHasher {
    key: String,
    expected_size_bytes: i64,
    next_start: i64,
    hasher: Sha256,
}

impl R2RangeHasher {
    pub(super) fn new(key: &str, expected_size_bytes: i64) -> Result<Self, PublishError> {
        if expected_size_bytes < 0 {
            return Err(PublishError::Broadcaster(format!(
                "R2 object {key} reported negative content length {expected_size_bytes}"
            )));
        }
        Ok(Self {
            key: key.to_owned(),
            expected_size_bytes,
            next_start: 0,
            hasher: Sha256::new(),
        })
    }

    pub(super) fn push(&mut self, start: i64, end: i64, chunk: &[u8]) -> Result<(), PublishError> {
        if start != self.next_start || end < start || end >= self.expected_size_bytes {
            return Err(PublishError::Broadcaster(format!(
                "R2 object {} returned non-contiguous byte range {start}-{end}; expected start {} within size {}",
                self.key, self.next_start, self.expected_size_bytes
            )));
        }
        let expected_len = usize::try_from(end - start + 1).map_err(|_| {
            PublishError::Infrastructure(format!(
                "R2 object {} byte range {start}-{end} is too large",
                self.key
            ))
        })?;
        if chunk.len() != expected_len {
            return Err(PublishError::Broadcaster(format!(
                "R2 object {} byte range {start}-{end} returned {} bytes, expected {expected_len}",
                self.key,
                chunk.len()
            )));
        }
        self.hasher.update(chunk);
        self.next_start = end.checked_add(1).ok_or_else(|| {
            PublishError::Infrastructure(format!(
                "R2 object {} byte range end overflowed i64",
                self.key
            ))
        })?;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<StreamingObjectRehash, PublishError> {
        if self.next_start != self.expected_size_bytes {
            return Err(PublishError::Broadcaster(format!(
                "R2 object {} rehash ended at byte {}, expected {}",
                self.key, self.next_start, self.expected_size_bytes
            )));
        }
        Ok(StreamingObjectRehash {
            checksum_sha256: format!("{:x}", self.hasher.finalize()),
            size_bytes: u64::try_from(self.expected_size_bytes).map_err(|_| {
                PublishError::Infrastructure(format!(
                    "R2 object {} content length does not fit u64",
                    self.key
                ))
            })?,
            observed_e_tag: None,
            observed_last_modified: None,
        })
    }
}

/// User-metadata key under which the SHA-256 checksum is stored. The S3/R2 wire form is
/// `x-amz-meta-sha256`; the SDK takes the bare `sha256` name and adds the `x-amz-meta-` prefix.
const SHA256_METADATA_KEY: &str = "sha256";

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
/// Cloudflare R2 configuration for the S3-compatible SDK client.
pub struct R2ObjectStorageConfig {
    /// R2 bucket name.
    pub bucket_name: String,
    /// R2 S3-compatible endpoint URL.
    pub endpoint: String,
    /// Region value passed to the SDK, usually `auto` for R2.
    pub region: String,
    /// R2 access key id.
    pub access_key_id: String,
    /// R2 secret access key.
    pub secret_access_key: String,
}

impl R2ObjectStorageConfig {
    /// Builds R2 object storage configuration from `R2_*` environment variables.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when a required R2 environment variable is missing.
    pub fn from_env() -> Result<Self, PublishError> {
        let endpoint = optional_env("R2_ENDPOINT")?.map_or_else(
            || {
                required_env("R2_ACCOUNT_ID")
                    .map(|account_id| format!("https://{account_id}.r2.cloudflarestorage.com"))
            },
            Ok,
        )?;

        Ok(Self {
            bucket_name: required_env("R2_BUCKET_NAME")?,
            endpoint,
            region: optional_env("R2_REGION")?.unwrap_or_else(|| "auto".to_owned()),
            access_key_id: required_env("R2_ACCESS_KEY_ID")?,
            secret_access_key: required_env("R2_SECRET_ACCESS_KEY")?,
        })
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
/// Cloudflare R2 object storage adapter.
pub struct R2ObjectStorage {
    client: Client,
    bucket_name: String,
}

impl R2ObjectStorage {
    /// Creates an R2 storage adapter from explicit configuration.
    #[must_use]
    pub fn from_config(config: R2ObjectStorageConfig) -> Self {
        let credentials = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            None,
            None,
            "foundation-platform-r2",
        );
        let storage_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region))
            .endpoint_url(config.endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        Self {
            client: Client::from_conf(storage_config),
            bucket_name: config.bucket_name,
        }
    }

    /// Creates an R2 storage adapter from `R2_*` environment variables.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the required R2 configuration is missing.
    pub fn from_env() -> Result<Self, PublishError> {
        Ok(Self::from_config(R2ObjectStorageConfig::from_env()?))
    }

    /// Performs a write/read/delete round trip against R2 using a dedicated smoke object key.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the key is unsafe, R2 rejects any operation, or the read
    /// content differs from the written payload.
    pub async fn round_trip_smoke(
        &self,
        key: String,
        body: Vec<u8>,
    ) -> Result<ObjectStorageSmokeReport, PublishError> {
        validate_r2_smoke_object_key(&key)?;
        if body.is_empty() {
            return Err(PublishError::Infrastructure(
                "R2 smoke body must not be empty".to_owned(),
            ));
        }

        <Self as ObjectStorageService>::put_object(
            self,
            PutObjectRequest {
                key: key.clone(),
                body: body.clone(),
                content_type: R2_SMOKE_CONTENT_TYPE.to_owned(),
                cache_control: R2_SMOKE_CACHE_CONTROL.to_owned(),
                // Smoke object: mutable, re-runnable, stays OverwriteAllowed.
                write_mode: ObjectWriteMode::OverwriteAllowed,
                // Smoke does not stamp a Bronze checksum.
                sha256: None,
            },
        )
        .await?;

        let read_result = self.get_object_bytes(&key).await;
        let delete_result = self.delete_object(&key).await;
        let read_body = read_result?;
        delete_result?;

        if read_body != body {
            return Err(PublishError::Infrastructure(format!(
                "R2 smoke object key {key} did not round-trip exactly"
            )));
        }

        Ok(ObjectStorageSmokeReport {
            key,
            bytes_verified: body.len(),
            put_request_count: 1,
            get_request_count: 1,
            delete_request_count: 1,
        })
    }

    /// Reads an object from R2.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the read or the body stream fails.
    pub async fn get_object_bytes(&self, key: &str) -> Result<Vec<u8>, PublishError> {
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
        let body = output.body.collect().await.map_err(|error| {
            PublishError::Broadcaster(format!("failed to read R2 object body {key}: {error}"))
        })?;
        Ok(body.into_bytes().to_vec())
    }

    /// Reads a large object from R2 with bounded byte-range retries.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the read, the object length is invalid,
    /// or any byte range cannot be read exactly after retrying.
    pub async fn get_object_bytes_range_retried(&self, key: &str) -> Result<Vec<u8>, PublishError> {
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!("failed to head R2 object {key}: {error}"))
            })?;
        let content_length = output.content_length().ok_or_else(|| {
            PublishError::Broadcaster(format!("R2 object {key} did not report content length"))
        })?;
        if content_length < 0 {
            return Err(PublishError::Broadcaster(format!(
                "R2 object {key} reported negative content length {content_length}"
            )));
        }

        let capacity = usize::try_from(content_length).map_err(|_| {
            PublishError::Infrastructure(format!(
                "R2 object {key} content length does not fit in memory on this platform"
            ))
        })?;
        let mut body = Vec::with_capacity(capacity);
        for (start, end) in r2_range_windows(content_length, R2_RANGE_READ_CHUNK_BYTES)? {
            let chunk = self.get_object_range_with_retries(key, start, end).await?;
            let expected_len = usize::try_from(end - start + 1).map_err(|_| {
                PublishError::Infrastructure(format!(
                    "R2 object {key} byte range {start}-{end} is too large"
                ))
            })?;
            if chunk.len() != expected_len {
                return Err(PublishError::Broadcaster(format!(
                    "R2 object {key} byte range {start}-{end} returned {} bytes, expected {expected_len}",
                    chunk.len()
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn get_object_range_with_retries(
        &self,
        key: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<u8>, PublishError> {
        let range = r2_range_header(start, end)?;
        for attempt in 1..=R2_RANGE_READ_MAX_ATTEMPTS {
            let result = self
                .client
                .get_object()
                .bucket(&self.bucket_name)
                .key(key)
                .range(range.clone())
                .send()
                .await
                .map_err(|error| {
                    PublishError::Broadcaster(format!(
                        "failed to read R2 object range {key} {range}: {error}"
                    ))
                });

            match result {
                Ok(output) => match output.body.collect().await {
                    Ok(body) => return Ok(body.into_bytes().to_vec()),
                    Err(error) if attempt < R2_RANGE_READ_MAX_ATTEMPTS => {
                        tracing::warn!(
                            key,
                            range,
                            attempt,
                            max_attempts = R2_RANGE_READ_MAX_ATTEMPTS,
                            error = %error,
                            "retrying R2 byte-range body read after transient failure"
                        );
                    }
                    Err(error) => {
                        return Err(PublishError::Broadcaster(format!(
                            "failed to read R2 object range body {key} {range}: {error}"
                        )));
                    }
                },
                Err(error) if attempt < R2_RANGE_READ_MAX_ATTEMPTS => {
                    tracing::warn!(
                        key,
                        range,
                        attempt,
                        max_attempts = R2_RANGE_READ_MAX_ATTEMPTS,
                        error = %error,
                        "retrying R2 byte-range request after transient failure"
                    );
                }
                Err(error) => return Err(error),
            }

            tokio::time::sleep(r2_range_read_retry_delay(attempt)).await;
        }

        unreachable!("R2 range retry loop always returns on success or final failure")
    }

    /// Lists one bounded page of R2 objects without mutating the bucket.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when R2 rejects the list request.
    pub async fn inventory(
        &self,
        request: R2InventoryRequest,
    ) -> Result<R2InventoryReport, PublishError> {
        let mut operation = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .delimiter("/")
            .max_keys(request.max_keys());

        if let Some(prefix) = request.prefix() {
            operation = operation.prefix(prefix);
        }

        let output = operation.send().await.map_err(|error| {
            PublishError::Broadcaster(format!("failed to list R2 inventory: {error}"))
        })?;

        let common_prefixes = output
            .common_prefixes()
            .iter()
            .filter_map(|prefix| prefix.prefix().map(ToOwned::to_owned))
            .collect();
        let objects = output
            .contents()
            .iter()
            .filter_map(|object| {
                object.key().map(|key| R2InventoryObject {
                    key: key.to_owned(),
                    size_bytes: object.size().unwrap_or_default(),
                    e_tag: object.e_tag().map(str::to_owned),
                    last_modified: object.last_modified().map(ToString::to_string),
                })
            })
            .collect();

        Ok(R2InventoryReport {
            prefix: request.prefix().map(ToOwned::to_owned),
            max_keys: request.max_keys(),
            key_count: output.key_count().unwrap_or_default(),
            is_truncated: output.is_truncated().unwrap_or(false),
            common_prefixes,
            objects,
        })
    }

    /// Recursively lists all R2 objects under a prefix without mutating the bucket.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when R2 rejects any page request.
    pub async fn inventory_audit(
        &self,
        request: R2InventoryRequest,
    ) -> Result<R2InventoryAuditReport, PublishError> {
        let mut next_cursor = None::<String>;
        let mut list_request_count = 0usize;
        let mut objects = Vec::new();

        loop {
            list_request_count += 1;
            let mut operation = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .max_keys(request.max_keys());

            if let Some(prefix) = request.prefix() {
                operation = operation.prefix(prefix);
            }
            if let Some(cursor) = next_cursor.as_deref() {
                operation = operation.continuation_token(cursor);
            }

            let output = operation.send().await.map_err(|error| {
                PublishError::Broadcaster(format!("failed to list R2 inventory audit: {error}"))
            })?;

            objects.extend(output.contents().iter().filter_map(|object| {
                object.key().map(|key| R2InventoryObject {
                    key: key.to_owned(),
                    size_bytes: object.size().unwrap_or_default(),
                    e_tag: object.e_tag().map(str::to_owned),
                    last_modified: object.last_modified().map(ToString::to_string),
                })
            }));

            next_cursor = output.next_continuation_token().map(ToOwned::to_owned);
            if next_cursor.is_none() {
                break;
            }
        }

        Ok(R2InventoryAuditReport {
            prefix: request.prefix().map(ToOwned::to_owned),
            max_keys: request.max_keys(),
            list_request_count,
            objects,
        })
    }

    /// Deletes an object from R2.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the delete operation.
    pub async fn delete_object(&self, key: &str) -> Result<(), PublishError> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!("failed to delete R2 object {key}: {error}"))
            })?;
        Ok(())
    }

    /// Copies one legacy date-partitioned Bronze object to its canonical key.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when either key is unsafe, the source is not a legacy
    /// `ingest_date` Bronze key, the target is not the canonical Bronze key shape, or R2 rejects
    /// the copy request.
    pub async fn copy_legacy_date_partitioned_bronze_object(
        &self,
        old_key: &str,
        new_key: &str,
    ) -> Result<(), PublishError> {
        validate_r2_bronze_key_migration_pair(old_key, new_key)?;
        let copy_source = format!("{}/{}", self.bucket_name, old_key);
        self.client
            .copy_object()
            .bucket(&self.bucket_name)
            .copy_source(copy_source)
            .key(new_key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!(
                    "failed to copy R2 Bronze object {old_key} to {new_key}: {error}"
                ))
            })?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStorageService for R2ObjectStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        let key = request.key;
        let mut builder = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .content_type(request.content_type)
            .cache_control(request.cache_control)
            .body(ByteStream::from(request.body));
        // CreateOnly => S3/R2 conditional write: the PUT only succeeds when the key does
        // not yet exist, and a colliding key returns `412 Precondition Failed`.
        if matches!(request.write_mode, ObjectWriteMode::CreateOnly) {
            builder = builder.if_none_match("*");
        }
        // Stamp `x-amz-meta-sha256` so a later CreateOnly collision can be reconciled by checksum
        // via `head_object` (read_object_sha256) without re-downloading the body.
        if let Some(sha256) = request.sha256 {
            builder = builder.metadata(SHA256_METADATA_KEY, sha256);
        }
        builder
            .send()
            .await
            .map_err(|error| map_r2_put_error(&key, &error, "write", request.write_mode))?;
        Ok(())
    }

    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError> {
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!(
                    "failed to head R2 object for checksum {key}: {error}"
                ))
            })?;
        // R2 returns user metadata with the `x-amz-meta-` prefix already stripped by the SDK, so the
        // bare `sha256` key is what we stamped on write. Absent => the object has no stored checksum.
        Ok(output
            .metadata()
            .and_then(|metadata| metadata.get(SHA256_METADATA_KEY))
            .map(ToOwned::to_owned))
    }
}

#[async_trait]
impl ObjectStorageStreamingService for R2ObjectStorage {
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError> {
        let key = request.key;
        let content_length = i64::try_from(request.size_bytes).map_err(|error| {
            PublishError::Infrastructure(format!(
                "R2 object {key} size_bytes does not fit S3 content length: {error}"
            ))
        })?;
        let mut builder = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .content_type(request.content_type)
            .cache_control(request.cache_control)
            .content_length(content_length)
            .body(request.body);
        // CreateOnly => conditional write (see put_object above). Streaming bulk cannot
        // pre-compute x-amz-meta-sha256 (sha is known only post-stream); the NEXT task
        // reconciles a 412 via DB/ledger/GET-rehash.
        if matches!(request.write_mode, ObjectWriteMode::CreateOnly) {
            builder = builder.if_none_match("*");
        }
        builder
            .send()
            .await
            .map_err(|error| map_r2_put_error(&key, &error, "stream", request.write_mode))?;
        Ok(())
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        // A streaming write never stamped x-amz-meta-sha256, so reconcile by reading the object's
        // bytes back and rehashing them. HEAD first so an object that vanished after the 412 (a
        // TOCTOU race) maps cleanly to Ok(None) instead of a hard error; then read with bounded
        // byte-range retries (the bulk objects are multi-gigabyte) and rehash.
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await;
        let head = match head {
            Ok(head) => head,
            Err(error) if is_r2_not_found(&error) => return Ok(None),
            Err(error) => {
                return Err(PublishError::Broadcaster(format!(
                    "failed to head R2 object for rehash {key}: {error}"
                )))
            }
        };
        let content_length = head.content_length().ok_or_else(|| {
            PublishError::Broadcaster(format!("R2 object {key} did not report content length"))
        })?;
        let before = R2ObjectVersionFingerprint {
            content_length,
            e_tag: head.e_tag().map(str::to_owned),
            last_modified: head.last_modified().map(ToString::to_string),
        };
        let mut hasher = R2RangeHasher::new(key, content_length)?;
        for (start, end) in r2_range_windows(content_length, R2_RANGE_READ_CHUNK_BYTES)? {
            let chunk = self.get_object_range_with_retries(key, start, end).await?;
            hasher.push(start, end, &chunk)?;
        }
        let after = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!(
                    "failed to re-head R2 object after rehash {key}: {error}"
                ))
            })?;
        let after = R2ObjectVersionFingerprint {
            content_length: after.content_length().ok_or_else(|| {
                PublishError::Broadcaster(format!(
                    "R2 object {key} did not report content length after rehash"
                ))
            })?,
            e_tag: after.e_tag().map(str::to_owned),
            last_modified: after.last_modified().map(ToString::to_string),
        };
        validate_r2_rehash_identity_stable(key, &before, &after)?;
        let mut rehash = hasher.finish()?;
        rehash.observed_e_tag = before.e_tag;
        rehash.observed_last_modified = before.last_modified;
        Ok(Some(rehash))
    }
}

/// Classifies whether an aws-sdk-s3 `HeadObject` error is a `404`/`NoSuchKey` (object absent).
///
/// Split out as a pure status check so the streaming rehash can map a vanished object to `Ok(None)`
/// (a TOCTOU race after a `412`) instead of failing loud, while any other failure stays an error.
fn is_r2_not_found<E, R>(error: &aws_sdk_s3::error::SdkError<E, R>) -> bool
where
    R: R2HttpStatus,
{
    error.raw_response().map(R2HttpStatus::status_code) == Some(HTTP_NOT_FOUND)
}

/// HTTP status code R2 returns for a `HeadObject` whose key does not exist.
const HTTP_NOT_FOUND: u16 = 404;

/// Maps an aws-sdk-s3 `PutObject` send error to a [`PublishError`].
///
/// Under [`ObjectWriteMode::CreateOnly`] a `412 Precondition Failed` (the response to
/// our `If-None-Match: *` conditional write when the key already exists) becomes
/// [`PublishError::ObjectAlreadyExists`] so the caller can run the write-once reconcile
/// protocol; every other failure (and all `OverwriteAllowed` failures) stays a
/// [`PublishError::Broadcaster`] error with the original behaviour.
fn map_r2_put_error<E, R>(
    key: &str,
    error: &aws_sdk_s3::error::SdkError<E, R>,
    verb: &str,
    write_mode: ObjectWriteMode,
) -> PublishError
where
    E: std::fmt::Display + aws_sdk_s3::error::ProvideErrorMetadata,
    R: R2HttpStatus,
{
    let status = error.raw_response().map(R2HttpStatus::status_code);
    let service_error_code = error.as_service_error().and_then(|error| error.code());
    if is_create_only_already_exists_response(write_mode, status, service_error_code) {
        return PublishError::ObjectAlreadyExists {
            key: key.to_owned(),
        };
    }
    PublishError::Broadcaster(format!("failed to {verb} R2 object {key}: {error}"))
}

/// Classifies whether an R2 `PutObject` failure is a `CreateOnly` write-once collision.
///
/// Returns `true` only when the request used [`ObjectWriteMode::CreateOnly`] and the raw
/// response carried `412 Precondition Failed` — the response R2 sends when our
/// `If-None-Match: *` conditional write hits an existing key. This is split out as a pure
/// function so it can be unit-tested without fabricating an aws-sdk-s3 response.
pub(super) fn is_create_only_already_exists_response(
    write_mode: ObjectWriteMode,
    status: Option<u16>,
    service_error_code: Option<&str>,
) -> bool {
    matches!(write_mode, ObjectWriteMode::CreateOnly)
        && (status == Some(HTTP_PRECONDITION_FAILED)
            || service_error_code == Some(R2_PRECONDITION_FAILED_CODE))
}

/// HTTP status code returned by R2 when a conditional `If-None-Match: *` write hits an
/// existing key.
const HTTP_PRECONDITION_FAILED: u16 = 412;
const R2_PRECONDITION_FAILED_CODE: &str = "PreconditionFailed";

/// Read access to the raw HTTP status of an aws-smithy response.
///
/// A trait keeps [`map_r2_put_error`] decoupled from the concrete SDK response type.
trait R2HttpStatus {
    fn status_code(&self) -> u16;
}

impl R2HttpStatus for aws_sdk_s3::config::http::HttpResponse {
    fn status_code(&self) -> u16 {
        self.status().as_u16()
    }
}

fn optional_env(name: &str) -> Result<Option<String>, PublishError> {
    match env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(PublishError::Infrastructure(format!(
            "invalid {name} environment variable: {error}"
        ))),
    }
}

fn required_env(name: &str) -> Result<String, PublishError> {
    optional_env(name)?.ok_or_else(|| {
        PublishError::Infrastructure(format!("{name} environment variable is required"))
    })
}

/// Validates that an R2 smoke key cannot mutate the runtime manifest pointer.
///
/// # Errors
///
/// Returns `PublishError` when the key is empty, absolute, path-like, or targets
/// `gold/manifest.json`.
pub fn validate_r2_smoke_object_key(key: &str) -> Result<(), PublishError> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(r2_smoke_key_error("must not be empty"));
    }
    if trimmed != key {
        return Err(r2_smoke_key_error(
            "must not contain leading or trailing whitespace",
        ));
    }
    if trimmed.starts_with('/') {
        return Err(r2_smoke_key_error("must be a relative object key"));
    }
    if trimmed.contains('\\') || trimmed.contains("..") {
        return Err(r2_smoke_key_error(
            "must not contain path traversal or backslash segments",
        ));
    }
    if trimmed == CANONICAL_MANIFEST_POINTER_OBJECT_KEY {
        return Err(r2_smoke_key_error(
            "must not target the runtime pointer gold/manifest.json",
        ));
    }
    Ok(())
}

fn r2_smoke_key_error(reason: &str) -> PublishError {
    PublishError::Infrastructure(format!("R2 smoke object key {reason}"))
}

/// Validates one legacy Bronze key migration pair.
///
/// # Errors
///
/// Returns `PublishError` when either key is unsafe, source and target do not match the supported
/// legacy-to-canonical transform, or the target still contains an `ingest_date` path segment.
pub fn validate_r2_bronze_key_migration_pair(
    old_key: &str,
    new_key: &str,
) -> Result<(), PublishError> {
    validate_relative_r2_object_key(old_key, "old_key")?;
    validate_relative_r2_object_key(new_key, "new_key")?;
    let Some((source, tail)) = legacy_date_partitioned_bronze_tail(old_key) else {
        return Err(PublishError::Infrastructure(
            "R2 Bronze migration old_key must be bronze/source=<source>/ingest_date=<date>/run_id=<run_id>/partition=<partition>".to_owned(),
        ));
    };
    let expected_new_key = format!("bronze/source={source}/{tail}");
    if new_key != expected_new_key {
        return Err(PublishError::Infrastructure(format!(
            "R2 Bronze migration new_key must equal canonical key {expected_new_key}"
        )));
    }
    if new_key.contains("/ingest_date=") {
        return Err(PublishError::Infrastructure(
            "R2 Bronze migration new_key must not contain ingest_date".to_owned(),
        ));
    }
    Ok(())
}

fn legacy_date_partitioned_bronze_tail(key: &str) -> Option<(&str, &str)> {
    let rest = key.strip_prefix("bronze/source=")?;
    let (source, rest) = rest.split_once("/ingest_date=")?;
    let (_date, tail) = rest.split_once('/')?;
    if !tail.starts_with("run_id=") || !tail.contains("/partition=") {
        return None;
    }
    Some((source, tail))
}

fn validate_relative_r2_object_key(key: &str, field: &str) -> Result<(), PublishError> {
    if key.trim().is_empty() {
        return Err(PublishError::Infrastructure(format!(
            "R2 Bronze migration {field} must not be empty"
        )));
    }
    if key.trim() != key {
        return Err(PublishError::Infrastructure(format!(
            "R2 Bronze migration {field} must not contain leading or trailing whitespace"
        )));
    }
    if key.starts_with('/') || key.contains('\\') || key.contains("..") {
        return Err(PublishError::Infrastructure(format!(
            "R2 Bronze migration {field} must be a clean provider-relative key"
        )));
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(PublishError::Infrastructure(format!(
            "R2 Bronze migration {field} must not contain empty, '.', or '..' path segments"
        )));
    }
    Ok(())
}

pub(super) fn r2_range_header(start: i64, end: i64) -> Result<String, PublishError> {
    if start < 0 || end < 0 || start > end {
        return Err(PublishError::Infrastructure(format!(
            "R2 byte range must be non-negative and ordered, got {start}-{end}"
        )));
    }
    Ok(format!("bytes={start}-{end}"))
}

pub(super) fn r2_range_windows(
    content_length: i64,
    chunk_bytes: i64,
) -> Result<Vec<(i64, i64)>, PublishError> {
    if content_length < 0 {
        return Err(PublishError::Infrastructure(format!(
            "R2 content length must be non-negative, got {content_length}"
        )));
    }
    if chunk_bytes <= 0 {
        return Err(PublishError::Infrastructure(format!(
            "R2 range chunk bytes must be positive, got {chunk_bytes}"
        )));
    }
    let mut windows = Vec::new();
    let mut start = 0_i64;
    while start < content_length {
        let end = (start + chunk_bytes - 1).min(content_length - 1);
        windows.push((start, end));
        start = end + 1;
    }
    Ok(windows)
}

const fn r2_range_read_retry_delay(attempt: usize) -> Duration {
    match attempt {
        0 | 1 => Duration::from_millis(250),
        2 => Duration::from_millis(500),
        3 => Duration::from_secs(1),
        _ => Duration::from_secs(2),
    }
}
