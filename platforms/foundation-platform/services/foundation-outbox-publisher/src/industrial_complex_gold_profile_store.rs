//! Where industrial-complex Gold profile objects live, for every command that touches them.
//!
//! One definition shared by the command that writes the objects
//! (`export-industrial-complex-gold-profiles`) and the commands that publish pointers at them
//! (`publish-industrial-complex-gold-pointer{,s}`). The store a pointer is checked against and the
//! store the export wrote to are then the same store by construction, rather than two enums that
//! agree until one of them is edited.
//!
//! The `r2` driver is the lakehouse connection (root ADR-0039): profile artifacts live beside the
//! other Gold artifacts a consumer fetches, under `gold/`. Which bucket a run actually reached is
//! reported by `bucket`, because a shared cargo target once ran a stale binary and the summary's
//! bucket label was the only clue (root ADR-0039 decision 5).

use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use foundation_outbox::{
    object_storage::R2ObjectStorageConfig,
    object_storage::{ObjectWriteMode, PutObjectRequest},
    EvidenceByteReader, FileObjectStorage, ObjectStorageService, PublishError, R2ObjectStorage,
};

use crate::r2_layout::is_industrial_complex_gold_profile_key;

/// Content type every profile object is stored with.
pub(crate) const PROFILE_CONTENT_TYPE: &str = "application/json; charset=utf-8";
/// Cache policy every profile object is stored with; the objects are immutable and content-addressed.
pub(crate) const PROFILE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

const LOCAL_DRIVER: &str = "local";
const R2_DRIVER: &str = "r2";

/// Which store a command was pointed at.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProfileStoreConfig {
    /// A directory on this machine, used for rehearsal and tests.
    Local {
        /// Root the object keys are resolved under.
        root: PathBuf,
    },
    /// The lakehouse R2 bucket, configured from `FOUNDATION_PLATFORM_R2_LAKEHOUSE_*`.
    R2,
}

impl ProfileStoreConfig {
    /// Parses a driver name and its optional local root.
    ///
    /// `root` is required by the local driver and ignored by the R2 driver, whose bucket comes from
    /// the lakehouse connection rather than from a caller-supplied path.
    ///
    /// # Errors
    /// Returns an error when the driver is unknown or the local driver has no root.
    pub(crate) fn parse(driver: &str, root: Option<PathBuf>) -> anyhow::Result<Self> {
        match driver.trim().to_ascii_lowercase().as_str() {
            LOCAL_DRIVER => Ok(Self::Local {
                root: root.context("the local driver requires an output root")?,
            }),
            R2_DRIVER => Ok(Self::R2),
            other => {
                bail!("storage driver must be '{LOCAL_DRIVER}' or '{R2_DRIVER}', got '{other}'")
            }
        }
    }
}

/// An opened profile object store.
#[derive(Clone)]
pub(crate) enum ProfileObjectStore {
    /// Objects under a local root.
    Local(FileObjectStorage),
    /// Objects in the named lakehouse bucket.
    R2(Box<R2ObjectStorage>, String),
}

impl ProfileObjectStore {
    /// Opens the store described by `config`.
    ///
    /// # Errors
    /// Returns an error when the local root cannot be used or the lakehouse connection is not
    /// configured.
    pub(crate) fn open(config: &ProfileStoreConfig) -> anyhow::Result<Self> {
        match config {
            ProfileStoreConfig::Local { root } => {
                Ok(Self::Local(FileObjectStorage::new(root).with_context(
                    || format!("failed to configure local profile root {}", root.display()),
                )?))
            }
            ProfileStoreConfig::R2 => {
                let config = R2ObjectStorageConfig::from_env()
                    .context("failed to configure the R2 profile store")?;
                let bucket_name = config.bucket_name.clone();
                Ok(Self::R2(
                    Box::new(R2ObjectStorage::from_config(config)),
                    bucket_name,
                ))
            }
        }
    }

    /// Name of the driver this store was opened with.
    pub(crate) const fn storage_driver(&self) -> &'static str {
        match self {
            Self::Local(_) => LOCAL_DRIVER,
            Self::R2(_, _) => R2_DRIVER,
        }
    }

    /// The bucket this store reaches, when it is a bucket at all.
    pub(crate) fn bucket(&self) -> Option<&str> {
        match self {
            Self::Local(_) => None,
            Self::R2(_, bucket_name) => Some(bucket_name.as_str()),
        }
    }

    /// Writes one profile object create-only, reusing an existing object only when the bytes match.
    ///
    /// Returns whether the object was newly created. A key that this repository would not itself
    /// have produced is refused before any write (root ADR-0039 decision 4): the lakehouse bucket
    /// also holds the Bronze collection originals and the Iceberg tables.
    ///
    /// # Errors
    /// Returns an error when the key is not canonical, the provider rejects the write, or the key
    /// already holds different bytes.
    pub(crate) async fn write_create_only(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<bool> {
        ensure!(
            is_industrial_complex_gold_profile_key(key),
            "refusing to write {key} : it is not a canonical industrial-complex Gold profile key"
        );
        let request = PutObjectRequest {
            key: key.to_owned(),
            body: body.to_vec(),
            content_type: PROFILE_CONTENT_TYPE.to_owned(),
            cache_control: PROFILE_CACHE_CONTROL.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(sha256.to_owned()),
        };
        let outcome = match self {
            Self::Local(storage) => storage.put_object(request).await,
            Self::R2(storage, _) => storage.put_object(request).await,
        };
        match outcome {
            Ok(()) => Ok(true),
            Err(PublishError::ObjectAlreadyExists { key: existing }) if existing == key => {
                let stored = self.read_bytes(key).await?;
                ensure!(
                    stored == body,
                    "profile object {key} already exists with different exact bytes"
                );
                Ok(false)
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to write profile object {key}"))
            }
        }
    }

    /// Reads the exact stored bytes of one object.
    ///
    /// # Errors
    /// Returns an error when the object is absent or the provider rejects the read.
    pub(crate) async fn read_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Local(storage) => storage.read_evidence_bytes(key).await,
            Self::R2(storage, _) => storage.read_evidence_bytes(key).await,
        }
        .with_context(|| format!("failed to read profile object {key}"))
    }
}

/// Reads a local root out of an optional raw value.
pub(crate) fn local_root(raw: Option<String>) -> Option<PathBuf> {
    raw.map(|value| Path::new(value.as_str()).to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::{ProfileObjectStore, ProfileStoreConfig};
    use std::path::PathBuf;

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "foundation-platform-gold-profile-store-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    #[test]
    fn parses_both_drivers_and_refuses_anything_else() -> anyhow::Result<()> {
        let root = temporary_root("parse");
        assert_eq!(
            ProfileStoreConfig::parse("local", Some(root.clone()))?,
            ProfileStoreConfig::Local { root }
        );
        assert_eq!(
            ProfileStoreConfig::parse("R2", None)?,
            ProfileStoreConfig::R2
        );
        assert!(ProfileStoreConfig::parse("local", None).is_err());
        assert!(ProfileStoreConfig::parse("s3", None).is_err());
        Ok(())
    }

    /// The lakehouse bucket also holds Bronze originals and the Iceberg tables, so a key this
    /// module would not itself have produced is refused before the provider is ever called.
    #[tokio::test]
    async fn refuses_to_write_a_key_it_would_not_have_produced() -> anyhow::Result<()> {
        let root = temporary_root("non-canonical");
        let store = ProfileObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;

        let result = store
            .write_create_only("bronze/vworld/2026/raw.jsonl", b"{}\n", &"a".repeat(64))
            .await;

        let written = root.join("bronze/vworld/2026/raw.jsonl");
        let refused = result.is_err();
        let leaked = written.exists();
        std::fs::remove_dir_all(&root)?;
        assert!(refused, "a non-canonical key was written");
        assert!(!leaked, "a refused write still produced the object");
        Ok(())
    }
}
