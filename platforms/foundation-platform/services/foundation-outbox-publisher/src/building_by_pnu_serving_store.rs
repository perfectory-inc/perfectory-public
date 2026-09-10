//! Where building by-PNU serving objects live, for every command that touches them.
//!
//! One definition shared by the command that bakes the objects
//! (`export-building-by-pnu-serving`) and the command that points the gateway at a generation
//! (`publish-building-by-pnu-serving-manifest`). The store a manifest is checked against and the
//! store the export wrote to are then the same store by construction.
//!
//! The `r2` driver is the lakehouse connection: serving objects live under `serving/` in the
//! same bucket the gateway Worker binds (root ADR-0100). The lakehouse bucket also holds the
//! Bronze originals and the Iceberg tables, so a key this lane would not itself have produced
//! is refused before any write.
//!
//! Two write modes exist on purpose. Objects inside a generation are create-only — a re-export
//! of the same snapshot is an idempotent re-run. The manifest is the lane's one mutable object
//! (`OverwriteAllowed`), exactly like the tile pipeline's `gold/manifest.json` pointer; and a
//! delta re-bake inside the current generation overwrites objects only when the caller says so.

use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{ensure, Context};
use foundation_outbox::{
    object_storage::R2ObjectStorageConfig,
    object_storage::{
        ObjectWriteMode, PutObjectRequest, R2InventoryRequest, MAX_R2_INVENTORY_MAX_KEYS,
    },
    EvidenceByteReader, FileObjectStorage, ObjectStorageService, PublishError, R2ObjectStorage,
};

use crate::building_by_pnu_gateway_contract::building_by_pnu_gateway_policy;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{
    building_by_pnu_serving_generation_prefix, is_building_by_pnu_serving_manifest_key,
    is_building_by_pnu_serving_object_key,
};

/// An opened building by-PNU serving object store.
#[derive(Clone)]
pub(crate) enum BuildingServingObjectStore {
    /// Objects under a local root, used for rehearsal and tests. The root rides along because
    /// listing a generation walks the directory the adapter does not expose.
    Local(FileObjectStorage, PathBuf),
    /// Objects in the named lakehouse bucket.
    R2(Box<R2ObjectStorage>, String),
}

impl BuildingServingObjectStore {
    /// Opens the store described by `config`.
    ///
    /// # Errors
    /// Returns an error when the local root cannot be used or the lakehouse connection is not
    /// configured.
    pub(crate) fn open(config: &ProfileStoreConfig) -> anyhow::Result<Self> {
        match config {
            ProfileStoreConfig::Local { root } => Ok(Self::Local(
                FileObjectStorage::new(root).with_context(|| {
                    format!("failed to configure local serving root {}", root.display())
                })?,
                root.clone(),
            )),
            ProfileStoreConfig::R2 => {
                let config = R2ObjectStorageConfig::from_env()
                    .context("failed to configure the R2 serving store")?;
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
            Self::Local(..) => "local",
            Self::R2(_, _) => "r2",
        }
    }

    /// The bucket this store reaches, when it is a bucket at all.
    pub(crate) fn bucket(&self) -> Option<&str> {
        match self {
            Self::Local(..) => None,
            Self::R2(_, bucket_name) => Some(bucket_name.as_str()),
        }
    }

    /// Every canonical serving object key already present in one generation's directory.
    ///
    /// The bucket is the record (root ADR-0062): a resumed bake asks the store what exists
    /// instead of keeping a ledger beside it that a crash would leave disagreeing. Non-canonical
    /// keys under the prefix are ignored — the inventory audit reports them, this lane does not
    /// serve them.
    ///
    /// A sharded run passes its PNU prefix so the listing walks only its own key range —
    /// object file names begin with the PNU, so the range is a plain prefix extension. Without
    /// it, every shard of a national bake would page through the whole generation.
    ///
    /// # Errors
    /// Returns an error when the generation violates the contract grammar or the provider
    /// rejects a list request. A network failure is not an empty generation.
    pub(crate) async fn list_existing_generation_keys(
        &self,
        generation: u64,
        pnu_prefix: Option<&str>,
    ) -> anyhow::Result<HashSet<String>> {
        let generation_prefix = building_by_pnu_serving_generation_prefix(generation)?;
        let prefix = match pnu_prefix {
            Some(pnu_prefix) => format!("{generation_prefix}{pnu_prefix}"),
            None => generation_prefix.clone(),
        };
        let keys = match self {
            Self::Local(_, root) => {
                // 로컬은 세대 디렉터리를 읽고 파일명으로 범위를 거른다 — prefix 가 디렉터리
                // 경계와 어긋나도(샤드 프리픽스가 붙으면 그렇다) 같은 키 집합이 나와야 한다.
                let directory = root.join(&generation_prefix);
                match std::fs::read_dir(&directory) {
                    Ok(entries) => entries
                        .map(|entry| {
                            entry
                                .map(|entry| {
                                    format!(
                                        "{generation_prefix}{}",
                                        entry.file_name().to_string_lossy()
                                    )
                                })
                                .with_context(|| {
                                    format!("failed to list local serving directory {prefix}")
                                })
                        })
                        .filter(|key| {
                            key.as_ref()
                                .map(|key| key.starts_with(prefix.as_str()))
                                .unwrap_or(true)
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("failed to list local serving directory {prefix}")
                        })
                    }
                }
            }
            Self::R2(storage, _) => {
                let request =
                    R2InventoryRequest::new(Some(&prefix), Some(MAX_R2_INVENTORY_MAX_KEYS))
                        .context("failed to build the serving generation list request")?;
                let report = storage
                    .inventory_audit(request)
                    .await
                    .with_context(|| format!("failed to list serving generation {prefix}"))?;
                report
                    .objects()
                    .iter()
                    .map(|object| object.key.clone())
                    .collect()
            }
        };
        Ok(keys
            .into_iter()
            .filter(|key| is_building_by_pnu_serving_object_key(key))
            .collect())
    }

    /// Writes one serving object create-only, reusing an existing object only when the bytes
    /// match. Returns whether the object was newly created.
    ///
    /// # Errors
    /// Returns an error when the key is not canonical, the provider rejects the write, or the
    /// key already holds different bytes.
    pub(crate) async fn write_object_create_only(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<bool> {
        ensure!(
            is_building_by_pnu_serving_object_key(key),
            "refusing to write {key} : it is not a canonical building by-PNU serving key"
        );
        let request = self.object_put_request(key, body, sha256, ObjectWriteMode::CreateOnly)?;
        match self.put(request).await {
            Ok(()) => Ok(true),
            Err(PublishError::ObjectAlreadyExists { key: existing }) if existing == key => {
                let stored = self.read_bytes(key).await?;
                ensure!(
                    stored == body,
                    "serving object {key} already exists with different exact bytes; \
                     a delta re-bake must say so explicitly"
                );
                Ok(false)
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to write serving object {key}"))
            }
        }
    }

    /// Overwrites one serving object in place — the delta re-bake path inside a generation the
    /// manifest already points at. The caller states that intent by calling this method.
    ///
    /// # Errors
    /// Returns an error when the key is not canonical or the provider rejects the write.
    pub(crate) async fn write_object_overwrite(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            is_building_by_pnu_serving_object_key(key),
            "refusing to overwrite {key} : it is not a canonical building by-PNU serving key"
        );
        let request =
            self.object_put_request(key, body, sha256, ObjectWriteMode::OverwriteAllowed)?;
        self.put(request)
            .await
            .with_context(|| format!("failed to overwrite serving object {key}"))
    }

    /// Writes the serving manifest — the lane's one deliberately mutable object.
    ///
    /// # Errors
    /// Returns an error when the key is not the contract manifest key or the provider rejects
    /// the write.
    pub(crate) async fn write_manifest(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            is_building_by_pnu_serving_manifest_key(key),
            "refusing to write {key} : it is not the building by-PNU serving manifest key"
        );
        let policy = building_by_pnu_gateway_policy()?;
        let request = PutObjectRequest {
            key: key.to_owned(),
            body: body.to_vec(),
            content_type: policy.content_type.clone(),
            cache_control: policy.manifest_cache_control.clone(),
            write_mode: ObjectWriteMode::OverwriteAllowed,
            sha256: Some(sha256.to_owned()),
        };
        self.put(request)
            .await
            .with_context(|| format!("failed to write the serving manifest {key}"))
    }

    /// Reads the exact stored bytes of one object.
    ///
    /// # Errors
    /// Returns an error when the object is absent or the provider rejects the read.
    pub(crate) async fn read_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Local(storage, _) => storage.read_evidence_bytes(key).await,
            Self::R2(storage, _) => storage.read_evidence_bytes(key).await,
        }
        .with_context(|| format!("failed to read serving object {key}"))
    }

    fn object_put_request(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
        write_mode: ObjectWriteMode,
    ) -> anyhow::Result<PutObjectRequest> {
        let policy = building_by_pnu_gateway_policy()?;
        Ok(PutObjectRequest {
            key: key.to_owned(),
            body: body.to_vec(),
            content_type: policy.content_type.clone(),
            cache_control: policy.cache_control.clone(),
            write_mode,
            sha256: Some(sha256.to_owned()),
        })
    }

    async fn put(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        match self {
            Self::Local(storage, _) => storage.put_object(request).await,
            Self::R2(storage, _) => storage.put_object(request).await,
        }
    }
}

/// Reads a local root out of an optional raw value.
pub(crate) fn local_root(raw: Option<String>) -> Option<PathBuf> {
    raw.map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::BuildingServingObjectStore;
    use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
    use std::path::PathBuf;

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "foundation-platform-building-serving-store-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    const OBJECT_KEY: &str = "serving/buildings/by-pnu/v1/9999900000100000000.json";
    const MANIFEST_KEY: &str = "serving/buildings/by-pnu/manifest.json";
    const CHECKSUM: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[tokio::test]
    async fn create_only_reuses_identical_bytes_and_refuses_different_bytes() -> anyhow::Result<()>
    {
        let root = temporary_root("create-only");
        let store =
            BuildingServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;

        let created = store
            .write_object_create_only(OBJECT_KEY, b"{}\n", CHECKSUM)
            .await?;
        let reused = store
            .write_object_create_only(OBJECT_KEY, b"{}\n", CHECKSUM)
            .await?;
        let collided = store
            .write_object_create_only(OBJECT_KEY, b"{\"other\":1}\n", CHECKSUM)
            .await;

        std::fs::remove_dir_all(&root)?;
        assert!(created, "first write did not create the object");
        assert!(!reused, "identical re-write was reported as a fresh create");
        assert!(collided.is_err(), "differing bytes were silently accepted");
        Ok(())
    }

    #[tokio::test]
    async fn overwrite_replaces_bytes_only_through_the_explicit_method() -> anyhow::Result<()> {
        let root = temporary_root("overwrite");
        let store =
            BuildingServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;
        store
            .write_object_create_only(OBJECT_KEY, b"{}\n", CHECKSUM)
            .await?;

        store
            .write_object_overwrite(OBJECT_KEY, b"{\"revised\":true}\n", CHECKSUM)
            .await?;
        let stored = store.read_bytes(OBJECT_KEY).await?;

        std::fs::remove_dir_all(&root)?;
        assert_eq!(stored, b"{\"revised\":true}\n");
        Ok(())
    }

    #[tokio::test]
    async fn each_write_path_refuses_the_other_lanes_keys() -> anyhow::Result<()> {
        let root = temporary_root("refusal");
        let store =
            BuildingServingObjectStore::open(&ProfileStoreConfig::Local { root: root.clone() })?;

        let manifest_as_object = store
            .write_object_create_only(MANIFEST_KEY, b"{}\n", CHECKSUM)
            .await;
        let object_as_manifest = store.write_manifest(OBJECT_KEY, b"{}\n", CHECKSUM).await;
        let foreign = store
            .write_object_create_only("bronze/vworld/2026/raw.jsonl", b"{}\n", CHECKSUM)
            .await;
        store
            .write_manifest(MANIFEST_KEY, b"{}\n", CHECKSUM)
            .await?;

        let leaked = root.join("bronze/vworld/2026/raw.jsonl").exists();
        std::fs::remove_dir_all(&root)?;
        assert!(manifest_as_object.is_err(), "manifest key passed as object");
        assert!(object_as_manifest.is_err(), "object key passed as manifest");
        assert!(foreign.is_err(), "a foreign key was written");
        assert!(!leaked, "a refused write still produced the object");
        Ok(())
    }
}
