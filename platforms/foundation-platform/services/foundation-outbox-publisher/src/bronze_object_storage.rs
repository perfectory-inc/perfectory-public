use std::path::PathBuf;

use anyhow::{bail, Context};
use async_trait::async_trait;
use collection_application::{
    bronze_catalog_recovery::{
        BronzeCatalogRecoveryObjectReader, BronzeCatalogRecoveryStorageError, ExistingBronzeObject,
    },
    BronzeRawObjectWriter, BronzeStorageError, BronzeWriteMode, BronzeWriteOutcome,
    BronzeWriteRequest,
};
use foundation_outbox::{
    errors::PublishError,
    object_storage::{ObjectWriteMode, PutObjectRequest},
    FileObjectStorage, ObjectStorageService, ObjectStorageStreamingService, R2ObjectStorage,
};
use lakehouse_domain::LakehouseOwnerService;

use crate::public_data_control_support::optional_env_value;

/// Thin services-layer adapter bridging the low-level [`ObjectStorageService`] port to the
/// [`BronzeRawObjectWriter`] write seam the [`collection_application::BronzeCommitter`] depends on.
///
/// `catalog-application` must not depend on the concrete object storage adapter, so the committer owns a
/// narrow write port and this adapter maps it to `put_object`. It maps the committer's
/// [`BronzeWriteMode`] to the low-level [`ObjectWriteMode`], stamps `x-amz-meta-sha256`, and
/// translates a `CreateOnly` collision ([`PublishError::ObjectAlreadyExists`]) into
/// [`BronzeWriteOutcome::AlreadyExists`] so the committer runs the recoverable commit protocol
/// instead of failing (ADR 0016).
pub struct BronzeObjectStorageWriter<'a, Storage: ?Sized> {
    storage: &'a Storage,
}

impl<'a, Storage> BronzeObjectStorageWriter<'a, Storage>
where
    Storage: ObjectStorageService + ?Sized,
{
    /// Wraps an existing object storage port as a Bronze raw-object writer.
    pub const fn new(storage: &'a Storage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<Storage> BronzeRawObjectWriter for BronzeObjectStorageWriter<'_, Storage>
where
    Storage: ObjectStorageService + ?Sized,
{
    async fn write_object(
        &self,
        request: BronzeWriteRequest,
    ) -> Result<BronzeWriteOutcome, BronzeStorageError> {
        let result = self
            .storage
            .put_object(PutObjectRequest {
                key: request.key,
                body: request.body,
                content_type: request.content_type,
                cache_control: request.cache_control,
                write_mode: map_write_mode(request.write_mode),
                sha256: request.sha256,
            })
            .await;
        match result {
            Ok(()) => Ok(BronzeWriteOutcome::Written),
            // A CreateOnly collision is NOT a failure: report it so the committer reconciles by
            // checksum (idempotent success / recover / quarantine).
            Err(PublishError::ObjectAlreadyExists { .. }) => Ok(BronzeWriteOutcome::AlreadyExists),
            Err(error) => Err(BronzeStorageError(error.to_string())),
        }
    }

    async fn read_object_sha256(&self, key: &str) -> Result<Option<String>, BronzeStorageError> {
        self.storage
            .read_object_sha256(key)
            .await
            .map_err(|error| BronzeStorageError(error.to_string()))
    }
}

/// Read-only adapter used to verify existing Bronze bytes before Catalog metadata recovery.
pub(crate) struct BronzeCatalogRecoveryObjectStorageReader<'a, Storage: ?Sized> {
    storage: &'a Storage,
}

impl<'a, Storage> BronzeCatalogRecoveryObjectStorageReader<'a, Storage>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    /// Wraps an existing bounded-memory object storage reader.
    pub(crate) const fn new(storage: &'a Storage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<Storage> BronzeCatalogRecoveryObjectReader
    for BronzeCatalogRecoveryObjectStorageReader<'_, Storage>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    async fn read_existing_object(
        &self,
        key: &str,
    ) -> Result<Option<ExistingBronzeObject>, BronzeCatalogRecoveryStorageError> {
        let object = self
            .storage
            .read_object_sha256_and_size_by_rehash(key)
            .await
            .map_err(|error| BronzeCatalogRecoveryStorageError(error.to_string()))?;
        object
            .map(|object| {
                let observed_r2_etag = object.observed_e_tag.ok_or_else(|| {
                    BronzeCatalogRecoveryStorageError(format!(
                        "object storage did not report ETag for {key}"
                    ))
                })?;
                if observed_r2_etag.trim().is_empty() || observed_r2_etag.trim() != observed_r2_etag
                {
                    return Err(BronzeCatalogRecoveryStorageError(format!(
                        "object storage returned a non-canonical ETag for {key}"
                    )));
                }
                let observed = object.observed_last_modified.ok_or_else(|| {
                    BronzeCatalogRecoveryStorageError(format!(
                        "object storage did not report last_modified for {key}"
                    ))
                })?;
                let observed_r2_last_modified = chrono::DateTime::parse_from_rfc3339(&observed)
                    .map_err(|error| {
                        BronzeCatalogRecoveryStorageError(format!(
                            "object storage returned invalid last_modified for {key}: {error}"
                        ))
                    })?
                    .with_timezone(&chrono::Utc);
                Ok(ExistingBronzeObject {
                    checksum_sha256: object.checksum_sha256,
                    size_bytes: object.size_bytes,
                    observed_r2_etag,
                    observed_r2_last_modified,
                })
            })
            .transpose()
    }
}

/// Maps the committer's write-mode choice to the low-level object-storage policy.
const fn map_write_mode(mode: BronzeWriteMode) -> ObjectWriteMode {
    match mode {
        BronzeWriteMode::CreateOnly => ObjectWriteMode::CreateOnly,
        BronzeWriteMode::OverwriteAllowed => ObjectWriteMode::OverwriteAllowed,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BronzeObjectStorageDriver {
    R2,
    Local(PathBuf),
}

pub async fn bronze_object_storage_from_env() -> anyhow::Result<Box<dyn ObjectStorageService>> {
    let driver = optional_env_value("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER")?;
    let local_root = optional_env_value("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT")?;
    match bronze_object_storage_driver_from_options(driver.as_deref(), local_root.as_deref())? {
        BronzeObjectStorageDriver::R2 => Ok(Box::new(R2ObjectStorage::from_env()?)),
        BronzeObjectStorageDriver::Local(root) => Ok(Box::new(FileObjectStorage::new(root)?)),
    }
}

pub async fn bronze_streaming_object_storage_from_env(
) -> anyhow::Result<Box<dyn ObjectStorageStreamingService>> {
    let driver = optional_env_value("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER")?;
    let local_root = optional_env_value("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT")?;
    match bronze_object_storage_driver_from_options(driver.as_deref(), local_root.as_deref())? {
        BronzeObjectStorageDriver::R2 => Ok(Box::new(R2ObjectStorage::from_env()?)),
        BronzeObjectStorageDriver::Local(root) => Ok(Box::new(FileObjectStorage::new(root)?)),
    }
}

pub fn bronze_object_storage_driver_from_options(
    driver: Option<&str>,
    local_root: Option<&str>,
) -> anyhow::Result<BronzeObjectStorageDriver> {
    let normalized_driver = driver.unwrap_or("r2").trim().to_ascii_lowercase();
    match normalized_driver.as_str() {
        "r2" => Ok(BronzeObjectStorageDriver::R2),
        "local" => {
            let root = local_root
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .with_context(|| {
                    "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT is required when local Bronze object storage is selected"
                })?;
            Ok(BronzeObjectStorageDriver::Local(PathBuf::from(root)))
        }
        "" => bail!("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER must not be empty"),
        other => bail!(
            "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER must be 'r2' or 'local', got '{other}'"
        ),
    }
}

/// Validates a live-write Bronze target *before* the first put and logs exactly what a live run
/// will write to. A misconfigured target (missing R2 credentials, wrong/empty driver) fails fast
/// here instead of mid-run, after a multi-gigabyte download has already streamed from the provider.
///
/// Resolves the storage driver from `FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER` (the same env the
/// real storage builders read), and for `r2` requires the full R2 environment that
/// [`R2ObjectStorage::from_env`] needs (`R2_BUCKET_NAME`, `R2_ACCESS_KEY_ID`,
/// `R2_SECRET_ACCESS_KEY`, and at least one of `R2_ENDPOINT` / `R2_ACCOUNT_ID`). For `r2` it ALSO
/// asserts that `R2_BUCKET_NAME` is *exactly* the Foundation Platform production bucket
/// ([`LakehouseOwnerService::FoundationPlatform::production_r2_bucket_name`]): presence is not enough, the
/// bucket must be the right one. A wrong-but-present bucket would otherwise pass env-presence checks
/// and let a direct live-write subcommand stream Bronze objects into the wrong bucket — this is the
/// same exact-bucket gate the registry preflight (`verify_foundation_platform_r2_bucket_env`) enforces,
/// pulled into the shared preflight so every live-write entrypoint is covered, not just the
/// registry-gated `national-data-collection-run`. Emits one structured log line naming the resolved
/// driver and bucket-or-local-root so an operator can confirm the target from the logs before
/// a large provider download streams to it.
///
/// Call this from a live-write ingest path only when live write is enabled, before the first put.
///
/// # Errors
/// - the driver env is empty or not `r2`/`local`, or `local` is selected without
///   `FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT`;
/// - the driver is `r2` and any required R2 environment variable is missing/blank;
/// - the driver is `r2` and `R2_BUCKET_NAME` is not exactly the Foundation Platform production bucket.
pub fn live_write_target_preflight() -> anyhow::Result<()> {
    let driver = optional_env_value("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER")?;
    let local_root = optional_env_value("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT")?;
    let resolved =
        bronze_object_storage_driver_from_options(driver.as_deref(), local_root.as_deref())?;
    let (driver_label, target) = match &resolved {
        BronzeObjectStorageDriver::R2 => {
            require_r2_env(&|name: &str| std::env::var(name))?;
            let bucket = std::env::var("R2_BUCKET_NAME")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "<unset>".to_owned());
            // Presence is not enough: the bucket must be *exactly* the Foundation Platform production
            // bucket. Without this, a direct live-write subcommand with a wrong-but-present bucket
            // would stream Bronze objects into the wrong R2 bucket. Mirrors the registry preflight's
            // `verify_foundation_platform_r2_bucket_env` exact-bucket check.
            let expected = LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name();
            if bucket != expected {
                bail!("live-write preflight: R2_BUCKET_NAME must be {expected}, got {bucket}");
            }
            ("r2", bucket)
        }
        BronzeObjectStorageDriver::Local(root) => {
            ("local", root.to_string_lossy().replace('\\', "/"))
        }
    };
    tracing::info!(
        driver = driver_label,
        bucket = %target,
        live_write = true,
        "live-write preflight: driver={driver_label} bucket={target} live_write=true"
    );
    Ok(())
}

/// Pure R2-env validation backing [`live_write_target_preflight`], parameterized over an env lookup
/// so it can be unit-tested deterministically without mutating process-global environment. Mirrors
/// the national ledger-execute `require_r2_env` contract: the three R2 credential vars are all
/// required, plus at least one of the two addressing vars (`R2_ENDPOINT` or `R2_ACCOUNT_ID`).
///
/// # Errors
/// Returns the name of the first missing/blank required variable (or the addressing pair) so the
/// failure message points the operator straight at what to set.
fn require_r2_env<F, E>(lookup: &F) -> anyhow::Result<()>
where
    F: Fn(&str) -> Result<String, E>,
{
    fn present<F, E>(lookup: &F, name: &str) -> bool
    where
        F: Fn(&str) -> Result<String, E>,
    {
        lookup(name).is_ok_and(|value| !value.trim().is_empty())
    }

    for name in ["R2_BUCKET_NAME", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"] {
        if !present(lookup, name) {
            bail!("live-write preflight: missing required R2 environment variable: {name}");
        }
    }
    if !present(lookup, "R2_ENDPOINT") && !present(lookup, "R2_ACCOUNT_ID") {
        bail!(
            "live-write preflight: missing required R2 addressing environment variable: \
             R2_ENDPOINT or R2_ACCOUNT_ID"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::PathBuf};

    use collection_application::bronze_catalog_recovery::BronzeCatalogRecoveryObjectReader;
    use foundation_outbox::FileObjectStorage;
    use lakehouse_domain::LakehouseOwnerService;
    use sha2::{Digest as _, Sha256};

    use super::{
        live_write_target_preflight, require_r2_env, BronzeCatalogRecoveryObjectStorageReader,
    };

    #[tokio::test]
    async fn catalog_recovery_reader_maps_existing_storage_rehash_without_writing() {
        let root = PathBuf::from("target/bronze-catalog-recovery-reader-test");
        let key = "bronze/source=vworldkr__parcel/fixture.zip";
        let path = root.join(key.replace('/', std::path::MAIN_SEPARATOR_STR));
        if root.exists() {
            fs::remove_dir_all(&root).expect("remove stale recovery reader fixture");
        }
        fs::create_dir_all(path.parent().expect("fixture parent"))
            .expect("create recovery reader fixture parent");
        fs::write(&path, b"existing-bronze-bytes").expect("write recovery reader fixture");
        let storage = FileObjectStorage::new(&root).expect("file object storage");
        let reader = BronzeCatalogRecoveryObjectStorageReader::new(&storage);

        let observed = reader
            .read_existing_object(key)
            .await
            .expect("read existing Bronze object")
            .expect("existing object must be returned");
        let expected_checksum = format!("{:x}", Sha256::digest(b"existing-bronze-bytes"));

        assert_eq!(observed.checksum_sha256, expected_checksum);
        assert_eq!(observed.size_bytes, 21);
        assert!(reader
            .read_existing_object("bronze/source=vworldkr__parcel/missing.zip")
            .await
            .expect("missing read must not fail")
            .is_none());
        fs::remove_dir_all(root).expect("remove recovery reader fixture");
    }

    fn lookup_from(map: BTreeMap<String, String>) -> impl Fn(&str) -> Result<String, ()> {
        move |name: &str| map.get(name).cloned().ok_or(())
    }

    fn full_r2_env() -> BTreeMap<String, String> {
        [
            ("R2_BUCKET_NAME", "foundation-platform-bronze"),
            ("R2_ACCESS_KEY_ID", "test-access"),
            ("R2_SECRET_ACCESS_KEY", "test-secret"),
            ("R2_ENDPOINT", "https://account.r2.cloudflarestorage.com"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
    }

    #[test]
    fn preflight_r2_env_with_all_present_is_ok() {
        assert!(require_r2_env(&lookup_from(full_r2_env())).is_ok());
    }

    #[test]
    fn preflight_r2_env_accepts_account_id_instead_of_endpoint() {
        let mut env = full_r2_env();
        env.remove("R2_ENDPOINT");
        env.insert("R2_ACCOUNT_ID".to_owned(), "test-account".to_owned());
        assert!(require_r2_env(&lookup_from(env)).is_ok());
    }

    #[test]
    fn preflight_r2_env_missing_secret_names_the_missing_var() {
        let mut env = full_r2_env();
        env.remove("R2_SECRET_ACCESS_KEY");

        let error = require_r2_env(&lookup_from(env))
            .expect_err("a missing R2 credential must fail the preflight");
        let message = error.to_string();
        assert!(
            message.contains("R2_SECRET_ACCESS_KEY"),
            "message must name the missing var: {message}"
        );
    }

    #[test]
    fn preflight_r2_env_blank_bucket_is_treated_as_missing() {
        let mut env = full_r2_env();
        env.insert("R2_BUCKET_NAME".to_owned(), "   ".to_owned());

        let error = require_r2_env(&lookup_from(env))
            .expect_err("a blank R2 bucket must fail the preflight");
        assert!(
            error.to_string().contains("R2_BUCKET_NAME"),
            "message must name the blank var: {error}"
        );
    }

    #[test]
    fn preflight_r2_env_without_any_addressing_var_is_rejected() {
        let mut env = full_r2_env();
        env.remove("R2_ENDPOINT");

        let error =
            require_r2_env(&lookup_from(env)).expect_err("R2 needs an endpoint or an account id");
        assert!(
            error.to_string().contains("R2_ENDPOINT or R2_ACCOUNT_ID"),
            "message must name the addressing pair: {error}"
        );
    }

    /// End-to-end env-based check: with `driver=r2` and a required R2 var unset, the public
    /// preflight (which reads real process env) fails and names the missing var. Mutates
    /// process-global env, so it is serialized against other env-mutating tests and restores every
    /// variable it touched on exit.
    #[test]
    fn live_write_target_preflight_r2_missing_env_fails_naming_the_var() {
        const VARS: [&str; 6] = [
            "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER",
            "R2_BUCKET_NAME",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_ENDPOINT",
            "R2_ACCOUNT_ID",
        ];
        let _guard = super::test_support::env_lock();
        let saved: Vec<(&str, Option<String>)> = VARS
            .iter()
            .map(|name| (*name, std::env::var(name).ok()))
            .collect();

        for name in VARS {
            std::env::remove_var(name);
        }
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "r2");
        // Use the real expected bucket so the missing-credential check is the unambiguous failure
        // cause (the exact-bucket assertion runs only after the env-presence check passes).
        std::env::set_var(
            "R2_BUCKET_NAME",
            LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name(),
        );
        std::env::set_var("R2_ACCESS_KEY_ID", "test-access");
        std::env::set_var("R2_ENDPOINT", "https://account.r2.cloudflarestorage.com");
        // R2_SECRET_ACCESS_KEY intentionally left unset.

        let result = live_write_target_preflight();

        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }

        let error = result.expect_err("missing R2_SECRET_ACCESS_KEY must fail the preflight");
        assert!(
            error.to_string().contains("R2_SECRET_ACCESS_KEY"),
            "message must name the missing var: {error}"
        );
    }

    /// The R2 env vars the full-valid preflight needs, in the order they are set/restored.
    const PREFLIGHT_R2_VARS: [&str; 6] = [
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER",
        "R2_BUCKET_NAME",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT",
        "R2_ACCOUNT_ID",
    ];

    /// Runs `live_write_target_preflight()` with a fully valid R2 environment except that
    /// `R2_BUCKET_NAME` is set to `bucket`. Saves and restores every variable it touches under the
    /// env lock so it cannot race the other env-mutating preflight tests.
    fn preflight_with_bucket(bucket: &str) -> anyhow::Result<()> {
        let _guard = super::test_support::env_lock();
        let saved: Vec<(&str, Option<String>)> = PREFLIGHT_R2_VARS
            .iter()
            .map(|name| (*name, std::env::var(name).ok()))
            .collect();

        for name in PREFLIGHT_R2_VARS {
            std::env::remove_var(name);
        }
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "r2");
        std::env::set_var("R2_BUCKET_NAME", bucket);
        std::env::set_var("R2_ACCESS_KEY_ID", "test-access");
        std::env::set_var("R2_SECRET_ACCESS_KEY", "test-secret");
        std::env::set_var("R2_ENDPOINT", "https://account.r2.cloudflarestorage.com");

        let result = live_write_target_preflight();

        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        result
    }

    /// A wrong-but-present `R2_BUCKET_NAME` must fail the preflight, and the message must name both
    /// the expected Foundation Platform production bucket and the actual wrong value. Without this gate a
    /// direct live-write subcommand could stream Bronze objects into the wrong bucket.
    #[test]
    fn live_write_target_preflight_r2_wrong_bucket_is_rejected_naming_expected_and_actual() {
        let expected = LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name();
        let wrong = "foundation-platform-bronze";

        let error = preflight_with_bucket(wrong)
            .expect_err("a wrong-but-present R2_BUCKET_NAME must fail the preflight");
        let message = error.to_string();
        assert!(
            message.contains(expected),
            "message must name the expected bucket {expected}: {message}"
        );
        assert!(
            message.contains(wrong),
            "message must name the actual wrong bucket {wrong}: {message}"
        );
    }

    /// The exact Foundation Platform production bucket with otherwise-valid R2 env passes the preflight.
    #[test]
    fn live_write_target_preflight_r2_correct_bucket_is_ok() {
        let expected = LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name();
        assert!(
            preflight_with_bucket(expected).is_ok(),
            "the exact Foundation Platform production bucket must pass the preflight"
        );
    }
}

#[cfg(test)]
mod test_support {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Serializes tests that mutate process-global environment variables so they cannot race each
    /// other (env is shared across the whole test binary).
    pub(super) fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
