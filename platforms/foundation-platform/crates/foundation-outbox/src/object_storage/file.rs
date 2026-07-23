//! Object storage adapter that writes provider-relative keys under a local filesystem root.

use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use tokio::io::{self, AsyncWriteExt as _};

use crate::errors::PublishError;

use super::{
    sha256_hex, ObjectStorageService, ObjectStorageStreamingService, ObjectWriteMode,
    PutObjectRequest, StreamingObjectRehash, StreamingPutObjectRequest,
};

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
/// Object storage adapter that writes provider-relative keys under a local filesystem root.
pub struct FileObjectStorage {
    root: PathBuf,
}

impl FileObjectStorage {
    /// Creates a local filesystem object storage adapter rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the root directory cannot be created.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, PublishError> {
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to create file object storage root {}: {error}",
                root.display()
            ))
        })?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// Reads bytes back from a previously written provider-relative key.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when the key is invalid or the file cannot be read.
    pub fn get_object_bytes(&self, key: &str) -> Result<Vec<u8>, PublishError> {
        let path = self.resolve_key_path(key)?;
        fs::read(&path).map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to read local object {}: {error}",
                path.display()
            ))
        })
    }

    fn resolve_key_path(&self, key: &str) -> Result<PathBuf, PublishError> {
        validate_file_object_key(key)?;
        Ok(self
            .root
            .join(key.replace('/', std::path::MAIN_SEPARATOR_STR)))
    }
}

#[async_trait]
impl ObjectStorageService for FileObjectStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        let path = self.resolve_key_path(&request.key)?;
        let parent = path.parent().ok_or_else(|| {
            PublishError::Infrastructure(format!(
                "local object key {} did not resolve to a file path",
                request.key
            ))
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to create local object parent {}: {error}",
                parent.display()
            ))
        })?;
        match request.write_mode {
            // CreateOnly => local parity with R2 conditional write: `create_new(true)`
            // fails with `AlreadyExists` if the key is already present, which maps to
            // ObjectAlreadyExists for the caller's write-once reconcile.
            ObjectWriteMode::CreateOnly => {
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .map_err(|error| map_file_create_only_error(&request.key, &path, &error))?;
                file.write_all(&request.body).map_err(|error| {
                    PublishError::Infrastructure(format!(
                        "failed to write local object {}: {error}",
                        path.display()
                    ))
                })?;
            }
            // OverwriteAllowed => current unconditional overwrite behaviour.
            ObjectWriteMode::OverwriteAllowed => {
                fs::write(&path, request.body).map_err(|error| {
                    PublishError::Infrastructure(format!(
                        "failed to write local object {}: {error}",
                        path.display()
                    ))
                })?;
            }
        }
        // `request.sha256` is intentionally not persisted as side-car metadata locally: the local
        // adapter's `read_object_sha256` rehashes the stored bytes (cheap on the filesystem), so it
        // stays consistent with whatever was actually written, with no extra file to keep in sync.
        Ok(())
    }

    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, PublishError> {
        let path = self.resolve_key_path(key)?;
        match fs::read(&path) {
            // Cheap locally: rehash the existing bytes rather than storing side-car metadata.
            Ok(bytes) => Ok(Some(sha256_hex(&bytes))),
            // No object at the key => no checksum to report (mirrors R2 returning no metadata).
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(PublishError::Infrastructure(format!(
                "failed to read local object for checksum {}: {error}",
                path.display()
            ))),
        }
    }
}

#[async_trait]
impl ObjectStorageStreamingService for FileObjectStorage {
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError> {
        let path = self.resolve_key_path(&request.key)?;
        let parent = path.parent().ok_or_else(|| {
            PublishError::Infrastructure(format!(
                "local object key {} did not resolve to a file path",
                request.key
            ))
        })?;
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to create local object parent {}: {error}",
                parent.display()
            ))
        })?;

        let mut file = match request.write_mode {
            // CreateOnly => fail-on-exists, mirroring the non-streaming path and R2.
            ObjectWriteMode::CreateOnly => tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|error| map_file_create_only_error(&request.key, &path, &error))?,
            // OverwriteAllowed => current truncate/overwrite behaviour.
            ObjectWriteMode::OverwriteAllowed => {
                tokio::fs::File::create(&path).await.map_err(|error| {
                    PublishError::Infrastructure(format!(
                        "failed to create local object {}: {error}",
                        path.display()
                    ))
                })?
            }
        };
        let mut reader = request.body.into_async_read();
        let written = io::copy(&mut reader, &mut file).await.map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to stream local object {}: {error}",
                path.display()
            ))
        })?;
        file.flush().await.map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to flush local object {}: {error}",
                path.display()
            ))
        })?;
        if written != request.size_bytes {
            return Err(PublishError::Infrastructure(format!(
                "streamed local object {} wrote {written} bytes but expected {}",
                path.display(),
                request.size_bytes
            )));
        }
        Ok(())
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        let path = self.resolve_key_path(key)?;
        let before = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(PublishError::Infrastructure(format!(
                    "failed to stat local object before rehash {}: {error}",
                    path.display()
                )))
            }
        };
        let before_modified = before.modified().map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to read local object modification time before rehash {}: {error}",
                path.display()
            ))
        })?;
        let bytes = fs::read(&path).map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to read local object for rehash {}: {error}",
                path.display()
            ))
        })?;
        let after = fs::metadata(&path).map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to stat local object after rehash {}: {error}",
                path.display()
            ))
        })?;
        let after_modified = after.modified().map_err(|error| {
            PublishError::Infrastructure(format!(
                "failed to read local object modification time after rehash {}: {error}",
                path.display()
            ))
        })?;
        if before.len() != after.len()
            || before_modified != after_modified
            || bytes.len() as u64 != before.len()
        {
            return Err(PublishError::Infrastructure(format!(
                "local object changed during bounded rehash {}",
                path.display()
            )));
        }
        let observed_last_modified: chrono::DateTime<chrono::Utc> = before_modified.into();
        let checksum_sha256 = sha256_hex(&bytes);
        Ok(Some(StreamingObjectRehash {
            observed_e_tag: Some(checksum_sha256.clone()),
            checksum_sha256,
            size_bytes: bytes.len() as u64,
            observed_last_modified: Some(observed_last_modified.to_rfc3339()),
        }))
    }
}

/// Maps a `create_new(true)` open error for a `CreateOnly` local write.
///
/// An `AlreadyExists` io error means the key was already present (write-once
/// violation) and becomes [`PublishError::ObjectAlreadyExists`]; anything else stays a
/// generic infrastructure error.
fn map_file_create_only_error(key: &str, path: &Path, error: &std::io::Error) -> PublishError {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        return PublishError::ObjectAlreadyExists {
            key: key.to_owned(),
        };
    }
    PublishError::Infrastructure(format!(
        "failed to create local object {}: {error}",
        path.display()
    ))
}

fn validate_file_object_key(key: &str) -> Result<(), PublishError> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(file_object_key_error("must not be empty"));
    }
    if trimmed != key {
        return Err(file_object_key_error(
            "must not contain leading or trailing whitespace",
        ));
    }
    if trimmed.starts_with('/') || trimmed.contains(':') {
        return Err(file_object_key_error("must be a relative provider key"));
    }
    if trimmed.contains('\\') || trimmed.contains("..") {
        return Err(file_object_key_error(
            "must not contain path traversal or backslash segments",
        ));
    }
    if trimmed
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(file_object_key_error(
            "must not contain empty, '.', or '..' path segments",
        ));
    }
    Ok(())
}

fn file_object_key_error(reason: &str) -> PublishError {
    PublishError::Infrastructure(format!("local object key {reason}"))
}
