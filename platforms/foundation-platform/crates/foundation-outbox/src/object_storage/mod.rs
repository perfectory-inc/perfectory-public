//! Object storage adapters used by Catalog runtime artifact publishing.
//!
//! This module is split into focused submodules but its public surface is unchanged:
//! every type, trait, function, and constant that was reachable as
//! `foundation_outbox::object_storage::X` before the split is re-exported here at the
//! same path.

use async_trait::async_trait;

use crate::errors::PublishError;

mod file;
mod inventory;
mod logging;
mod r2;
mod requests;

pub use file::FileObjectStorage;
pub use inventory::{
    normalize_r2_inventory_prefix, R2InventoryAuditReport, R2InventoryObject, R2InventoryReport,
    R2InventoryRequest, DEFAULT_R2_INVENTORY_MAX_KEYS, MAX_R2_INVENTORY_MAX_KEYS,
};
pub use logging::LoggingObjectStorage;
pub use r2::{
    validate_r2_bronze_key_migration_pair, validate_r2_smoke_object_key, R2ObjectStorage,
    R2ObjectStorageConfig, DEFAULT_R2_SMOKE_OBJECT_KEY,
};
pub use requests::{
    ByteStream, ObjectStorageSmokeReport, ObjectWriteMode, PutObjectRequest, StreamingObjectRehash,
    StreamingPutObjectRequest,
};

// Pure helpers exercised by the unit tests via `super::`. Brought into the
// `object_storage` namespace only under `cfg(test)` so the in-module test submodule
// resolves them at `super::*` exactly as before, with no unused import in normal builds.
#[cfg(test)]
use r2::{
    is_create_only_already_exists_response, r2_range_header, r2_range_windows,
    validate_r2_rehash_identity_stable, R2ObjectVersionFingerprint, R2RangeHasher,
};

#[async_trait]
#[allow(clippy::module_name_repetitions)]
/// Provider-neutral object storage write port.
pub trait ObjectStorageService: Send + Sync {
    /// Writes an object to the configured storage provider.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the write or cannot be reached.
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError>;

    /// Reads back the stored SHA-256 checksum of an existing object, when present.
    ///
    /// Backs the recoverable Bronze commit protocol (ADR 0016): when a `CreateOnly` write collides
    /// and no `bronze_object` row exists yet, the committer compares this stored checksum against the
    /// payload it just computed to decide RECOVER vs quarantine. R2 reads it from the
    /// `x-amz-meta-sha256` user metadata via `head_object`; the local adapter rehashes the existing
    /// file bytes. Returns `Ok(None)` when the object exists but carries no checksum metadata.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the read or cannot be reached.
    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError>;
}

#[async_trait]
#[allow(clippy::module_name_repetitions)]
/// Provider-neutral streaming object storage write port for large immutable objects.
pub trait ObjectStorageStreamingService: Send + Sync {
    /// Streams an object to the configured storage provider.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the write or cannot be reached.
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError>;

    /// Reads an existing object's bytes back and rehashes them to obtain its checksum + size.
    ///
    /// Backs the *streaming* recoverable Bronze commit protocol (ADR 0016): a streaming write never
    /// stamps `x-amz-meta-sha256` (the checksum is known only post-stream), so a `CreateOnly`
    /// collision with no `bronze_object` row cannot be reconciled by HEAD metadata like the
    /// in-memory page path — the committer must read the existing object back and rehash it. R2
    /// reads it with bounded byte-range retries; the local adapter reads the file. Returns
    /// `Ok(None)` only when the object is absent at read time (a TOCTOU race after the `412`).
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the provider rejects the read or the body cannot be read.
    async fn read_object_sha256_and_size_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError>;
}

/// Lowercase hex SHA-256 of `bytes` (the canonical Bronze checksum form). Used by the local
/// adapter's `read_object_sha256` to rehash an existing file's stored bytes.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    use sha2::Digest as _;

    sha2::Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}

#[cfg(test)]
mod tests;
