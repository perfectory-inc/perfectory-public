//! Object storage adapter that logs writes without mutating external storage.

use async_trait::async_trait;

use crate::errors::PublishError;

use super::{
    ObjectStorageService, ObjectStorageStreamingService, PutObjectRequest, StreamingObjectRehash,
    StreamingPutObjectRequest,
};

#[derive(Clone, Debug, Default)]
#[allow(clippy::module_name_repetitions)]
/// Object storage adapter that logs writes without mutating external storage.
pub struct LoggingObjectStorage;

#[async_trait]
impl ObjectStorageService for LoggingObjectStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        tracing::info!(
            object_key = %request.key,
            content_type = %request.content_type,
            cache_control = %request.cache_control,
            bytes = request.body.len(),
            write_mode = ?request.write_mode,
            has_sha256 = request.sha256.is_some(),
            "object storage write skipped by logging adapter"
        );
        Ok(())
    }

    async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
        // The logging adapter never persists objects, so it has no stored checksum to report.
        Ok(None)
    }
}

#[async_trait]
impl ObjectStorageStreamingService for LoggingObjectStorage {
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError> {
        tracing::info!(
            object_key = %request.key,
            content_type = %request.content_type,
            cache_control = %request.cache_control,
            bytes = request.size_bytes,
            write_mode = ?request.write_mode,
            "streaming object storage write skipped by logging adapter"
        );
        Ok(())
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        _key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        // The logging adapter never persists objects, so there is nothing to read back and rehash.
        Ok(None)
    }
}
