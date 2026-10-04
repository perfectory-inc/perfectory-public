//! Where by-PNU serving objects live, for every command of both lanes that touches them.
//!
//! One definition shared by the bake (`export-{parcel,building}-by-pnu-serving`), the publish
//! (`publish-...-manifest`) and the state report, so the store a manifest is checked against and
//! the store the export wrote to are the same store by construction (root ADR-0096, ADR-0100).
//!
//! The `r2` driver is the lakehouse connection: serving objects live under `serving/` in the
//! bucket the gateway Workers bind. That bucket also holds the Bronze originals and the Iceberg
//! tables, so a key this lane would not itself have produced is refused before any write.
//!
//! Every object is create-only: base objects, patch objects and tombstones, and the history copy
//! of each replaced manifest (root ADR-0141). There is no overwrite path. The manifest is the one
//! mutable object, and it is replaced only over the version the publish read (compare-and-swap).

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use foundation_outbox::{
    object_storage::R2ObjectStorageConfig,
    object_storage::{
        ConditionalWrite, ObjectWriteMode, PutObjectRequest, R2InventoryRequest,
        MAX_R2_INVENTORY_MAX_KEYS,
    },
    EvidenceByteReader, FileObjectStorage, ObjectStorageService, PublishError, R2ObjectStorage,
};

use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};
use crate::by_pnu_serving_generations;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{by_pnu, by_pnu_packs};

/// An opened by-PNU serving object store of one lane.
#[derive(Clone)]
pub(crate) struct ByPnuServingStore {
    lane: ByPnuLane,
    backend: Backend,
    /// Stands in for another publish moving the manifest between this publish's read and its
    /// write (tests only).
    #[cfg(test)]
    race_before_manifest_write: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    /// Stands in for a manifest write whose answer was lost after the store accepted it: the
    /// write lands, and the call still fails (tests only).
    #[cfg(test)]
    fail_after_manifest_write: bool,
}

/// The manifest write lost its compare-and-swap: another publish moved the manifest after this
/// one read it, and nothing was written. Every other write failure leaves it unknown whether the
/// write landed.
#[derive(Debug)]
pub(crate) struct ManifestMoved {
    key: String,
    found: &'static str,
}

impl std::fmt::Display for ManifestMoved {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "the serving manifest {} changed while this publish ran ({}); another publish moved \
             it. Nothing was written — rerun against the manifest now served",
            self.key, self.found
        )
    }
}

impl std::error::Error for ManifestMoved {}

#[derive(Clone)]
enum Backend {
    /// Objects under a local root, for rehearsal and tests. The root rides along because listing
    /// walks the directory the adapter does not expose.
    Local(FileObjectStorage, PathBuf),
    /// Objects in the named lakehouse bucket.
    R2(Box<R2ObjectStorage>, String),
}

impl ByPnuServingStore {
    /// Opens the store described by `config` for `lane`.
    ///
    /// # Errors
    /// Returns an error when the local root cannot be used or the lakehouse connection is not
    /// configured.
    pub(crate) fn open(lane: ByPnuLane, config: &ProfileStoreConfig) -> anyhow::Result<Self> {
        let backend = match config {
            ProfileStoreConfig::Local { root } => Backend::Local(
                FileObjectStorage::new(root).with_context(|| {
                    format!("failed to configure local serving root {}", root.display())
                })?,
                root.clone(),
            ),
            ProfileStoreConfig::R2 => {
                let config = R2ObjectStorageConfig::from_env()
                    .context("failed to configure the R2 serving store")?;
                let bucket_name = config.bucket_name.clone();
                Backend::R2(Box::new(R2ObjectStorage::from_config(config)), bucket_name)
            }
        };
        Ok(Self {
            lane,
            backend,
            #[cfg(test)]
            race_before_manifest_write: None,
            #[cfg(test)]
            fail_after_manifest_write: false,
        })
    }

    /// Makes every manifest write land and then fail, as a lost answer would (tests only).
    #[cfg(test)]
    pub(crate) fn with_failure_after_manifest_write(mut self) -> Self {
        self.fail_after_manifest_write = true;
        self
    }

    /// Runs `race` right before every manifest write, as another writer would (tests only).
    #[cfg(test)]
    pub(crate) fn with_race_before_manifest_write(
        mut self,
        race: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        self.race_before_manifest_write = Some(std::sync::Arc::new(race));
        self
    }

    pub(crate) const fn lane(&self) -> ByPnuLane {
        self.lane
    }

    /// Name of the driver this store was opened with.
    pub(crate) const fn storage_driver(&self) -> &'static str {
        match self.backend {
            Backend::Local(..) => "local",
            Backend::R2(..) => "r2",
        }
    }

    /// The bucket this store reaches, when it is a bucket at all.
    pub(crate) fn bucket(&self) -> Option<&str> {
        match &self.backend {
            Backend::Local(..) => None,
            Backend::R2(_, bucket_name) => Some(bucket_name.as_str()),
        }
    }

    /// Every canonical base object key already present in one generation, optionally limited to
    /// the PNUs starting with `pnu_prefix` (a shard). Patch keys under the generation are not
    /// base keys and are left out.
    ///
    /// The bucket is the record (root ADR-0062): a resumed bake asks the store what exists
    /// instead of keeping a ledger beside it.
    ///
    /// # Errors
    /// Returns an error when the generation violates the grammar or the provider rejects a list
    /// request. A network failure is not an empty generation.
    pub(crate) async fn list_existing_generation_keys(
        &self,
        generation: u64,
        pnu_prefix: Option<&str>,
    ) -> anyhow::Result<HashSet<String>> {
        let directory = by_pnu::generation_prefix(self.lane, generation)?;
        let keys = self.list_directory(&directory, pnu_prefix).await?;
        Ok(keys
            .into_iter()
            .filter(|key| by_pnu::is_object_key(self.lane, key))
            .collect())
    }

    /// Every canonical object key (documents and tombstones) of one patch generation.
    ///
    /// # Errors
    /// Returns an error when the numbers violate the grammar or the listing fails.
    pub(crate) async fn list_patch_keys(
        &self,
        generation: u64,
        patch: u64,
        pnu_prefix: Option<&str>,
    ) -> anyhow::Result<HashSet<String>> {
        let directory = by_pnu::patch_prefix(self.lane, generation, patch)?;
        let keys = self.list_directory(&directory, pnu_prefix).await?;
        Ok(keys
            .into_iter()
            .filter(|key| {
                by_pnu::parse_patch_object_key(self.lane, key)
                    .is_some_and(|(g, p, _)| g == generation && p == patch)
            })
            .collect())
    }

    /// Every patch generation of base `generation` that holds at least one object, published or
    /// not: a new patch is numbered above all of them, so a half-written or rolled-back patch
    /// directory is never reused.
    ///
    /// # Errors
    /// Returns an error when the listing fails or is cut short.
    pub(crate) async fn list_patches_with_objects(
        &self,
        generation: u64,
    ) -> anyhow::Result<BTreeSet<u64>> {
        let prefix = by_pnu::patch_directories_prefix(self.lane, generation)?;
        match &self.backend {
            Backend::Local(_, root) => {
                let base = by_pnu::generation_prefix(self.lane, generation)?;
                let mut patches = BTreeSet::new();
                for (name, is_dir) in read_directory(&root.join(&base))? {
                    let candidate = format!("{base}{name}/");
                    if is_dir && !read_directory(&root.join(&candidate))?.is_empty() {
                        if let Some(patch) =
                            by_pnu::patch_of_prefix(self.lane, generation, &candidate)
                        {
                            patches.insert(patch);
                        }
                    }
                }
                Ok(patches)
            }
            Backend::R2(storage, _) => {
                let request =
                    R2InventoryRequest::new(Some(&prefix), Some(MAX_R2_INVENTORY_MAX_KEYS))
                        .context("failed to build the patch directory listing request")?;
                let page = storage
                    .inventory(request)
                    .await
                    .with_context(|| format!("failed to list the patch directories {prefix}"))?;
                ensure!(
                    !page.is_truncated(),
                    "the listing of {prefix} was cut short; which patches hold objects is unknown"
                );
                Ok(page
                    .common_prefixes()
                    .iter()
                    .filter_map(|common| by_pnu::patch_of_prefix(self.lane, generation, common))
                    .collect())
            }
        }
    }

    /// Every generation that holds at least one object, published or not (see
    /// `by_pnu_serving_generations`). Read-only.
    ///
    /// # Errors
    /// Returns an error when the listing fails or is cut short.
    pub(crate) async fn list_generations_with_objects(&self) -> anyhow::Result<BTreeSet<u64>> {
        let lane = self.lane;
        match &self.backend {
            Backend::Local(_, root) => by_pnu_serving_generations::in_directory(root, lane),
            Backend::R2(storage, _) => by_pnu_serving_generations::in_bucket(storage, lane).await,
        }
    }

    /// The first base object of `generation` in key order, when there is one — the state report
    /// reads it to learn which document schema a v1 manifest's generation holds.
    ///
    /// # Errors
    /// Returns an error when the listing fails.
    pub(crate) async fn first_object_key(&self, generation: u64) -> anyhow::Result<Option<String>> {
        let directory = by_pnu::generation_prefix(self.lane, generation)?;
        let keys = match &self.backend {
            Backend::Local(..) => self.list_directory(&directory, None).await?,
            Backend::R2(storage, _) => {
                let request = R2InventoryRequest::new(Some(&directory), Some(1))
                    .context("failed to build the first-object listing request")?;
                let page = storage
                    .inventory(request)
                    .await
                    .with_context(|| format!("failed to list {directory}"))?;
                page.objects()
                    .iter()
                    .map(|object| object.key.clone())
                    .collect()
            }
        };
        Ok(keys
            .into_iter()
            .filter(|key| by_pnu::is_object_key(self.lane, key))
            .min())
    }

    async fn list_directory(
        &self,
        directory: &str,
        pnu_prefix: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        let prefix = format!("{directory}{}", pnu_prefix.unwrap_or_default());
        match &self.backend {
            Backend::Local(_, root) => Ok(read_directory(&root.join(directory))?
                .into_iter()
                .filter(|(_, is_dir)| !is_dir)
                .map(|(name, _)| format!("{directory}{name}"))
                .filter(|key| key.starts_with(prefix.as_str()))
                .collect()),
            Backend::R2(storage, _) => {
                let request =
                    R2InventoryRequest::new(Some(&prefix), Some(MAX_R2_INVENTORY_MAX_KEYS))
                        .context("failed to build the serving list request")?;
                let report = storage
                    .inventory_audit(request)
                    .await
                    .with_context(|| format!("failed to list serving objects under {prefix}"))?;
                Ok(report
                    .objects()
                    .iter()
                    .map(|object| object.key.clone())
                    .collect())
            }
        }
    }

    /// Writes one base object, patch object or tombstone create-only, reusing an existing object
    /// only when the bytes match. Returns whether the object was newly created.
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
            by_pnu::is_object_key(self.lane, key) || by_pnu::is_patch_object_key(self.lane, key),
            "refusing to write {key} : it is not a canonical {} by-PNU serving key",
            self.lane.noun()
        );
        let policy = self.lane.policy()?;
        self.create_only(
            key,
            body,
            sha256,
            &policy.content_type,
            &policy.cache_control,
        )
        .await
    }

    /// Writes one section pack create-only (root ADR-0147), reusing an existing pack only when
    /// the bytes match. Returns whether the pack was newly created.
    ///
    /// # Errors
    /// Returns an error when the key is not a canonical pack key of the lane, the provider rejects
    /// the write, or the key already holds different bytes.
    pub(crate) async fn write_pack_create_only(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<bool> {
        ensure!(
            by_pnu_packs::parse_pack_key(self.lane, key).is_some(),
            "refusing to write {key} : it is not a canonical {} section pack key",
            self.lane.noun()
        );
        let policy = section_pack_policy()?;
        self.create_only(
            key,
            body,
            sha256,
            &policy.content_type,
            &policy.cache_control,
        )
        .await
    }

    /// Every pack key of one section's base generation (`patch` `None`) or of one patch of it.
    ///
    /// # Errors
    /// Returns an error when the numbers violate the grammar or the listing fails.
    pub(crate) async fn list_pack_keys(
        &self,
        section: &str,
        generation: u64,
        patch: Option<u64>,
    ) -> anyhow::Result<BTreeSet<String>> {
        let directory = by_pnu_packs::directory(self.lane, section, generation, patch)?;
        let keys = self.list_directory(&directory, None).await?;
        Ok(keys
            .into_iter()
            .filter(|key| {
                by_pnu_packs::parse_pack_key(self.lane, key).is_some_and(|parsed| {
                    parsed.section == section
                        && parsed.generation == generation
                        && parsed.patch == patch
                })
            })
            .collect())
    }

    /// Every generation of `section` holding at least one pack, and every patch number used
    /// under any of them, published or not: a new generation or patch is numbered above them, so
    /// a half-written directory is never reused.
    ///
    /// # Errors
    /// Returns an error when the listing fails.
    pub(crate) async fn list_pack_numbers(
        &self,
        section: &str,
    ) -> anyhow::Result<(BTreeSet<u64>, BTreeSet<u64>)> {
        let root = format!("{}/{section}/", self.lane.section_packs()?.root);
        let keys = match &self.backend {
            Backend::Local(_, local) => walk_files(&local.join(&root), &root)?,
            Backend::R2(storage, _) => {
                let request = R2InventoryRequest::new(Some(&root), Some(MAX_R2_INVENTORY_MAX_KEYS))
                    .context("failed to build the pack listing request")?;
                storage
                    .inventory_audit(request)
                    .await
                    .with_context(|| format!("failed to list section packs under {root}"))?
                    .objects()
                    .iter()
                    .map(|object| object.key.clone())
                    .collect()
            }
        };
        let (mut generations, mut patches) = (BTreeSet::new(), BTreeSet::new());
        for key in keys {
            if let Some(parsed) = by_pnu_packs::parse_pack_key(self.lane, &key) {
                generations.insert(parsed.generation);
                patches.extend(parsed.patch);
            }
        }
        Ok((generations, patches))
    }

    /// Stores the exact bytes of a manifest about to be replaced under its content-addressed
    /// history key, create-only. A rollback later names that key (root ADR-0141 §4).
    ///
    /// # Errors
    /// Returns an error when the key is not a canonical history key or the write fails.
    pub(crate) async fn write_manifest_history(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            by_pnu::is_manifest_history_key(self.lane, key),
            "refusing to write {key} : it is not a canonical {} manifest history key",
            self.lane.noun()
        );
        let policy = self.lane.policy()?;
        self.create_only(
            key,
            body,
            sha256,
            &policy.content_type,
            &policy.manifest_cache_control,
        )
        .await
        .map(|_| ())
    }

    async fn create_only(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
        content_type: &str,
        cache_control: &str,
    ) -> anyhow::Result<bool> {
        let request = PutObjectRequest {
            key: key.to_owned(),
            body: body.to_vec(),
            content_type: content_type.to_owned(),
            cache_control: cache_control.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(sha256.to_owned()),
        };
        match self.put(request).await {
            Ok(()) => Ok(true),
            Err(PublishError::ObjectAlreadyExists { key: existing }) if existing == key => {
                let stored = self.read_bytes(key).await?;
                ensure!(
                    stored == body,
                    "serving object {key} already exists with different exact bytes; objects are \
                     create-only and a changed document belongs in a new patch generation"
                );
                Ok(false)
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to write serving object {key}"))
            }
        }
    }

    /// Reads the serving manifest with the version a later [`Self::write_manifest`] must still
    /// find: the R2 `ETag`, or locally the SHA-256 of the bytes.
    ///
    /// # Errors
    /// Returns an error when the manifest is absent or the provider rejects the read.
    pub(crate) async fn read_manifest(&self) -> anyhow::Result<(Vec<u8>, String)> {
        let key = by_pnu::manifest_key(self.lane)?;
        match &self.backend {
            Backend::Local(..) => {
                let bytes = self.read_bytes(key).await?;
                let version = local_version(&bytes);
                Ok((bytes, version))
            }
            Backend::R2(storage, _) => storage
                .get_object_bytes_and_e_tag(key)
                .await
                .with_context(|| format!("failed to read the serving manifest {key}")),
        }
    }

    /// Writes the serving manifest — the lane's one deliberately mutable object — as a
    /// compare-and-swap: over the version `expected` names (R2 `If-Match`), or, with no
    /// expected version, only where no manifest exists yet (`If-None-Match: *`).
    ///
    /// The local backend compares then writes; it is a rehearsal store with one writer, and the
    /// comparison is what its tests exercise.
    ///
    /// # Errors
    /// Returns an error when the key is not the contract manifest key, the stored manifest is no
    /// longer the expected version (another publish moved it), or the write fails.
    pub(crate) async fn write_manifest(
        &self,
        key: &str,
        body: &[u8],
        sha256: &str,
        expected: Option<&str>,
    ) -> anyhow::Result<()> {
        ensure!(
            by_pnu::is_manifest_key(self.lane, key),
            "refusing to write {key} : it is not the {} by-PNU serving manifest key",
            self.lane.noun()
        );
        #[cfg(test)]
        if let Some(race) = &self.race_before_manifest_write {
            race();
        }
        let policy = self.lane.policy()?;
        let request = PutObjectRequest {
            key: key.to_owned(),
            body: body.to_vec(),
            content_type: policy.content_type.clone(),
            cache_control: policy.manifest_cache_control.clone(),
            write_mode: if expected.is_some() {
                ObjectWriteMode::OverwriteAllowed
            } else {
                ObjectWriteMode::CreateOnly
            },
            sha256: Some(sha256.to_owned()),
        };
        let lost = |found: &'static str| {
            anyhow::Error::new(ManifestMoved {
                key: key.to_owned(),
                found,
            })
        };
        #[cfg(test)]
        if self.fail_after_manifest_write {
            let written = match &self.backend {
                Backend::Local(storage, _) => storage.put_object(request).await,
                Backend::R2(..) => Ok(()),
            };
            return written
                .map_err(anyhow::Error::from)
                .and_then(|()| Err(anyhow::anyhow!("connection reset after the write was sent")));
        }
        let Some(expected) = expected else {
            return match self.put(request).await {
                Ok(()) => Ok(()),
                Err(PublishError::ObjectAlreadyExists { .. }) => {
                    Err(lost("a manifest appeared where none was"))
                }
                Err(error) => Err(error)
                    .with_context(|| format!("failed to write the serving manifest {key}")),
            };
        };
        match &self.backend {
            Backend::Local(storage, _) => {
                let current = storage.read_evidence_bytes(key).await.ok();
                if current.as_deref().map(local_version).as_deref() != Some(expected) {
                    return Err(lost("its version is no longer the one read"));
                }
                storage
                    .put_object(request)
                    .await
                    .with_context(|| format!("failed to write the serving manifest {key}"))
            }
            Backend::R2(storage, _) => match storage
                .put_object_if_match(request, expected)
                .await
                .with_context(|| format!("failed to write the serving manifest {key}"))?
            {
                ConditionalWrite::Written => Ok(()),
                ConditionalWrite::VersionChanged => {
                    Err(lost("R2 refused the write: If-Match no longer holds"))
                }
            },
        }
    }

    /// Reads the exact stored bytes of one object.
    ///
    /// # Errors
    /// Returns an error when the object is absent or the provider rejects the read.
    pub(crate) async fn read_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match &self.backend {
            Backend::Local(storage, _) => storage.read_evidence_bytes(key).await,
            Backend::R2(storage, _) => storage.read_evidence_bytes(key).await,
        }
        .with_context(|| format!("failed to read serving object {key}"))
    }

    async fn put(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        match &self.backend {
            Backend::Local(storage, _) => storage.put_object(request).await,
            Backend::R2(storage, _) => storage.put_object(request).await,
        }
    }
}

/// The version of a locally stored manifest: the SHA-256 of its bytes.
fn local_version(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

/// `(name, is_directory)` of every entry of a local directory; a missing directory is empty.
fn read_directory(directory: &Path) -> anyhow::Result<Vec<(String, bool)>> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to list {}", directory.display()))
        }
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to list {}", directory.display()))?;
        names.push((
            entry.file_name().to_string_lossy().into_owned(),
            entry.path().is_dir(),
        ));
    }
    Ok(names)
}

/// Every file key under a local directory, recursively, named relative to the store root.
fn walk_files(directory: &Path, key_prefix: &str) -> anyhow::Result<Vec<String>> {
    let mut keys = Vec::new();
    for (name, is_dir) in read_directory(directory)? {
        if is_dir {
            keys.extend(walk_files(
                &directory.join(&name),
                &format!("{key_prefix}{name}/"),
            )?);
        } else {
            keys.push(format!("{key_prefix}{name}"));
        }
    }
    Ok(keys)
}

/// Reads a local root out of an optional raw value.
pub(crate) fn local_root(raw: Option<String>) -> Option<PathBuf> {
    raw.map(PathBuf::from)
}

/// Refuses a removed switch the environment still sets, whatever its value: the ADR-0099 §3
/// overwrite and repoint paths are gone (root ADR-0141 §1), and a caller still setting them
/// expects a behaviour that no longer exists.
///
/// # Errors
/// Returns an error naming the first removed switch that is set.
pub(crate) fn refuse_removed_switches(lane: ByPnuLane, names: &[&str]) -> anyhow::Result<()> {
    refuse_removed_switches_in(lane, names, |variable| std::env::var_os(variable).is_some())
}

/// [`refuse_removed_switches`] over a caller-supplied environment.
///
/// # Errors
/// Returns an error naming the first removed switch `is_set` reports.
pub(crate) fn refuse_removed_switches_in(
    lane: ByPnuLane,
    names: &[&str],
    is_set: impl Fn(&str) -> bool,
) -> anyhow::Result<()> {
    for name in names {
        let variable = lane.env(name);
        if is_set(&variable) {
            bail!(
                "{variable} is set, but the in-place overwrite and repoint paths were removed \
                 (root ADR-0141): changed documents go into a new patch generation"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ByPnuServingStore;
    use crate::by_pnu_gateway_contract::ByPnuLane;
    use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
    use crate::r2_layout::by_pnu;
    use std::path::PathBuf;

    const PNU: &str = "9999900000100000000";
    const CHECKSUM: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const LANES: [ByPnuLane; 2] = [ByPnuLane::Parcel, ByPnuLane::Building];

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "foundation-platform-by-pnu-serving-store-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    #[tokio::test]
    async fn create_only_reuses_identical_bytes_and_refuses_different_bytes() -> anyhow::Result<()>
    {
        for lane in LANES {
            let root = temporary_root("create-only");
            let store =
                ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
            for key in [
                by_pnu::object_key(lane, 1, PNU)?,
                by_pnu::patch_object_key(lane, 1, 1, PNU)?,
            ] {
                let created = store
                    .write_object_create_only(&key, b"{}\n", CHECKSUM)
                    .await?;
                let reused = store
                    .write_object_create_only(&key, b"{}\n", CHECKSUM)
                    .await?;
                let collided = store
                    .write_object_create_only(&key, b"{\"other\":1}\n", CHECKSUM)
                    .await;
                assert!(created, "first write did not create {key}");
                assert!(
                    !reused,
                    "identical re-write of {key} was reported as a fresh create"
                );
                assert!(
                    collided.is_err(),
                    "differing bytes at {key} were silently accepted"
                );
            }
            std::fs::remove_dir_all(&root)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn each_write_path_refuses_keys_that_are_not_its_own() -> anyhow::Result<()> {
        for lane in LANES {
            let root = temporary_root("refusal");
            let store =
                ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
            let manifest = by_pnu::manifest_key(lane)?;
            let object = by_pnu::object_key(lane, 1, PNU)?;
            let other_lane = if lane == ByPnuLane::Parcel {
                ByPnuLane::Building
            } else {
                ByPnuLane::Parcel
            };

            assert!(store
                .write_object_create_only(manifest, b"{}\n", CHECKSUM)
                .await
                .is_err());
            assert!(store
                .write_manifest(&object, b"{}\n", CHECKSUM, None)
                .await
                .is_err());
            assert!(store
                .write_object_create_only(
                    &by_pnu::object_key(other_lane, 1, PNU)?,
                    b"{}\n",
                    CHECKSUM
                )
                .await
                .is_err());
            assert!(store
                .write_object_create_only("bronze/vworld/2026/raw.jsonl", b"{}\n", CHECKSUM)
                .await
                .is_err());
            assert!(store
                .write_manifest_history(&object, b"{}\n", CHECKSUM)
                .await
                .is_err());
            store
                .write_manifest(manifest, b"{}\n", CHECKSUM, None)
                .await?;
            let leaked = root.join("bronze/vworld/2026/raw.jsonl").exists();
            std::fs::remove_dir_all(&root)?;
            assert!(!leaked, "a refused write still produced the object");
        }
        Ok(())
    }

    /// The manifest moves only over the version that was read; a lost race writes nothing.
    #[tokio::test]
    async fn the_manifest_is_replaced_only_over_the_version_read() -> anyhow::Result<()> {
        let lane = ByPnuLane::Parcel;
        let root = temporary_root("manifest-cas");
        let store =
            ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
        let key = by_pnu::manifest_key(lane)?;
        store.write_manifest(key, b"one\n", CHECKSUM, None).await?;
        let created_twice = store.write_manifest(key, b"two\n", CHECKSUM, None).await;
        let (bytes, version) = store.read_manifest().await?;
        let stale = store
            .write_manifest(key, b"two\n", CHECKSUM, Some("not-the-version"))
            .await;
        store
            .write_manifest(key, b"two\n", CHECKSUM, Some(&version))
            .await?;
        let replayed = store
            .write_manifest(key, b"three\n", CHECKSUM, Some(&version))
            .await;
        let (last, _) = store.read_manifest().await?;
        std::fs::remove_dir_all(&root)?;

        assert_eq!(bytes, b"one\n");
        for (label, result) in [
            ("a second first write", created_twice),
            ("a write over an unknown version", stale),
            ("a write over a replaced version", replayed),
        ] {
            let error = result.err().map(|error| format!("{error:#}"));
            assert!(
                error
                    .as_deref()
                    .is_some_and(|message| message.contains("changed while this publish ran")),
                "{label} was not refused as a lost race: {error:?}"
            );
        }
        assert_eq!(last, b"two\n");
        Ok(())
    }

    #[tokio::test]
    async fn listings_separate_base_objects_from_patches() -> anyhow::Result<()> {
        let lane = ByPnuLane::Parcel;
        let root = temporary_root("listing");
        let store =
            ByPnuServingStore::open(lane, &ProfileStoreConfig::Local { root: root.clone() })?;
        store
            .write_object_create_only(&by_pnu::object_key(lane, 3, PNU)?, b"{}\n", CHECKSUM)
            .await?;
        for patch in [1, 4] {
            store
                .write_object_create_only(
                    &by_pnu::patch_object_key(lane, 3, patch, PNU)?,
                    b"{}\n",
                    CHECKSUM,
                )
                .await?;
        }
        std::fs::create_dir_all(root.join(by_pnu::patch_prefix(lane, 3, 6)?))?;

        let base = store.list_existing_generation_keys(3, None).await?;
        let patch_four = store.list_patch_keys(3, 4, None).await?;
        let patches = store.list_patches_with_objects(3).await?;
        let first = store.first_object_key(3).await?;
        std::fs::remove_dir_all(&root)?;

        assert_eq!(base.len(), 1, "patch objects were listed as base objects");
        assert_eq!(patch_four.len(), 1);
        // An empty patch directory is not a patch, exactly as R2 has no empty directories.
        assert_eq!(patches.into_iter().collect::<Vec<_>>(), vec![1, 4]);
        assert_eq!(first, Some(by_pnu::object_key(lane, 3, PNU)?));
        Ok(())
    }
}
