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

use crate::profile_gateway_contract::{complex_documents_policy, profile_gateway_policy};
use crate::r2_layout::is_industrial_complex_gold_profile_key;

const LOCAL_DRIVER: &str = "local";
const R2_DRIVER: &str = "r2";

/// Which store a command was pointed at.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProfileStoreConfig {
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
    pub fn parse(driver: &str, root: Option<PathBuf>) -> anyhow::Result<Self> {
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
pub enum ProfileObjectStore {
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
    pub fn open(config: &ProfileStoreConfig) -> anyhow::Result<Self> {
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
    pub const fn storage_driver(&self) -> &'static str {
        match self {
            Self::Local(_) => LOCAL_DRIVER,
            Self::R2(_, _) => R2_DRIVER,
        }
    }

    /// The bucket this store reaches, when it is a bucket at all.
    pub fn bucket(&self) -> Option<&str> {
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
    pub async fn write_create_only(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<bool> {
        ensure!(
            is_industrial_complex_gold_profile_key(key),
            "refusing to write {key} : it is not a canonical industrial-complex Gold profile key"
        );
        let request = profile_put_request(key, body, sha256)?;
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
    pub async fn read_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Local(storage) => storage.read_evidence_bytes(key).await,
            Self::R2(storage, _) => storage.read_evidence_bytes(key).await,
        }
        .with_context(|| format!("failed to read profile object {key}"))
    }
}

impl ProfileObjectStore {
    /// The current industrial-complex index's bytes, or `None` before the first one (root ADR-0456).
    ///
    /// # Errors
    /// Returns an error when the provider rejects the probe or the read.
    pub async fn read_index(&self) -> anyhow::Result<Option<Vec<u8>>> {
        let key = complex_documents_policy()?.index_object.as_str();
        let stored = match self {
            Self::Local(storage) => storage.read_object_sha256(key).await,
            Self::R2(storage, _) => storage.read_object_sha256(key).await,
        }
        .with_context(|| format!("failed to probe the complex index {key}"))?;
        match stored {
            None => Ok(None),
            Some(_) => self.read_bytes(key).await.map(Some),
        }
    }

    /// Publishes one index generation: its history object create-only first, then the index it
    /// replaces. A run that dies between the two leaves the old index served and the history ahead,
    /// which the next run's same generation reuses byte for byte.
    ///
    /// # Errors
    /// Returns an error when either write is refused or the history object holds other bytes.
    pub async fn publish_index(
        &self,
        generation: u64,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<()> {
        let policy = complex_documents_policy()?;
        let history = format!("{}/{generation}.json", policy.index_history_dir);
        for (key, write_mode) in [
            (history.as_str(), ObjectWriteMode::CreateOnly),
            (
                policy.index_object.as_str(),
                ObjectWriteMode::OverwriteAllowed,
            ),
        ] {
            let request = PutObjectRequest {
                key: key.to_owned(),
                body: body.to_vec(),
                content_type: profile_gateway_policy()?.content_type.clone(),
                cache_control: policy.index_cache_control.clone(),
                write_mode,
                sha256: Some(sha256.to_owned()),
            };
            let outcome = match self {
                Self::Local(storage) => storage.put_object(request).await,
                Self::R2(storage, _) => storage.put_object(request).await,
            };
            match outcome {
                Ok(()) => {}
                Err(PublishError::ObjectAlreadyExists { key: existing }) if existing == key => {
                    ensure!(
                        self.read_bytes(key).await? == body,
                        "complex index history {key} already exists with different bytes"
                    );
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("failed to write {key}"));
                }
            }
        }
        Ok(())
    }
}

fn profile_put_request(key: &str, body: &[u8], sha256: &str) -> anyhow::Result<PutObjectRequest> {
    let policy = profile_gateway_policy()?;
    Ok(PutObjectRequest {
        key: key.to_owned(),
        body: body.to_vec(),
        content_type: policy.content_type.clone(),
        cache_control: policy.cache_control.clone(),
        write_mode: ObjectWriteMode::CreateOnly,
        sha256: Some(sha256.to_owned()),
    })
}

/// Reads a local root out of an optional raw value.
pub fn local_root(raw: Option<String>) -> Option<PathBuf> {
    raw.map(|value| Path::new(value.as_str()).to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::{profile_put_request, ProfileObjectStore, ProfileStoreConfig};
    use crate::profile_gateway_contract::{complex_documents_policy, profile_gateway_policy};
    use foundation_outbox::object_storage::ObjectWriteMode;
    use std::path::PathBuf;

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "foundation-platform-gold-profile-store-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    #[test]
    fn upload_request_metadata_comes_from_profile_gateway_contract() -> anyhow::Result<()> {
        let request = profile_put_request(
            "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json",
            b"{}\n",
            &"a".repeat(64),
        )?;
        let policy = profile_gateway_policy()?;

        assert_eq!(request.content_type, policy.content_type);
        assert_eq!(request.cache_control, policy.cache_control);
        assert_eq!(request.write_mode, ObjectWriteMode::CreateOnly);
        Ok(())
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

    /// Root ADR-0456: each generation stays in the history, create-only, and only the index is
    /// replaced; a re-run of the same generation with other bytes is refused.
    #[tokio::test]
    async fn an_index_generation_is_kept_in_the_history_and_replaces_the_index(
    ) -> anyhow::Result<()> {
        let root = temporary_root("index");
        let store = ProfileObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
        let policy = complex_documents_policy()?;
        let before = store.read_index().await?;
        store
            .publish_index(
                1,
                b"one
",
                &"1".repeat(64),
            )
            .await?;
        store
            .publish_index(
                2,
                b"two
",
                &"2".repeat(64),
            )
            .await?;
        let again = store
            .publish_index(
                2,
                b"two
",
                &"2".repeat(64),
            )
            .await;
        let other = store
            .publish_index(
                1,
                b"other
",
                &"3".repeat(64),
            )
            .await;
        let index = store.read_index().await?;
        let first = std::fs::read(root.join(format!("{}/1.json", policy.index_history_dir)))?;
        std::fs::remove_dir_all(&root)?;
        assert_eq!(before, None);
        assert!(
            again.is_ok(),
            "the same generation's bytes are an idempotent re-run"
        );
        assert!(other.is_err(), "a generation's history never changes");
        assert_eq!(
            index.as_deref(),
            Some(
                &b"two
"[..]
            )
        );
        assert_eq!(
            first,
            b"one
"
        );
        Ok(())
    }
}
