use std::path::PathBuf;

use crate::public_data_control_support::optional_env_value;
use crate::runtime_environment::{
    validate_bronze_driver, validate_foundation_r2_bucket, RuntimeEnvironment,
};
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

/// Builds a Bronze object-storage adapter for a live write after enforcing the shared runtime
/// and bucket preflight. Callers may still run the preflight earlier to fail before a provider
/// download; this second boundary protects the actual write adapter from future call-site drift.
pub async fn live_write_bronze_object_storage_from_env(
) -> anyhow::Result<Box<dyn ObjectStorageService>> {
    live_write_target_preflight()?;
    bronze_object_storage_from_env().await
}

/// Builds a streaming Bronze object-storage adapter for a live write after enforcing the shared
/// runtime and bucket preflight. Keeping the check in the adapter boundary makes it impossible
/// for a new streaming write path to silently select a wrong environment by omitting a manual
/// preflight call.
pub async fn live_write_bronze_streaming_object_storage_from_env(
) -> anyhow::Result<Box<dyn ObjectStorageStreamingService>> {
    live_write_target_preflight()?;
    bronze_streaming_object_storage_from_env().await
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
/// [`R2ObjectStorage::from_env`] needs (`FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET`, `FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID`,
/// `FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY`, and at least one of `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT` / `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID`). For `r2` it also
/// requires the bucket governed by `FOUNDATION_PLATFORM_RUNTIME_ENV`; production continues to use
/// the lakehouse domain's exact production-bucket SSOT. A wrong-but-present bucket would otherwise
/// pass env-presence checks and let a direct live-write subcommand stream Bronze objects into the
/// wrong environment. The shared preflight covers every live-write entrypoint that calls it.
///
/// Call this from a live-write ingest path only when live write is enabled, before the first put.
///
/// # Errors
/// - the driver env is empty or not `r2`/`local`, or `local` is selected without
///   `FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT`;
/// - the driver is `r2` and any required R2 environment variable is missing/blank;
/// - `FOUNDATION_PLATFORM_RUNTIME_ENV` is missing or invalid;
/// - the driver is `r2` and `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET` is not the bucket governed by that runtime environment.
pub fn live_write_target_preflight() -> anyhow::Result<()> {
    let runtime_environment = RuntimeEnvironment::from_env_with_execution_context()?;
    let driver = optional_env_value("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER")?;
    let local_root = optional_env_value("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT")?;
    let resolved =
        bronze_object_storage_driver_from_options(driver.as_deref(), local_root.as_deref())?;
    let (driver_label, target) = match &resolved {
        BronzeObjectStorageDriver::R2 => {
            validate_bronze_driver(runtime_environment, "r2")?;
            require_r2_env(&|name: &str| std::env::var(name))?;
            let bucket = std::env::var("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "<unset>".to_owned());
            validate_foundation_r2_bucket(runtime_environment, &bucket)?;
            ("r2", bucket)
        }
        BronzeObjectStorageDriver::Local(root) => {
            validate_bronze_driver(runtime_environment, "local")?;
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
/// required, plus at least one of the two addressing vars (`FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT` or `FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID`).
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

    for name in [
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY",
    ] {
        if !present(lookup, name) {
            bail!("live-write preflight: missing required R2 environment variable: {name}");
        }
    }
    if !present(lookup, "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT")
        && !present(lookup, "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID")
    {
        bail!(
            "live-write preflight: missing required R2 addressing environment variable: \
             FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT or FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID"
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
    use crate::runtime_environment::{
        ExecutionContext, RuntimeEnvironment, EXECUTION_CONTEXT_ENV, PRELAUNCH_SHARED_ENV,
        RUNTIME_ENVIRONMENT_ENV,
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
            (
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
                "foundation-platform-bronze",
            ),
            (
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID",
                "test-access",
            ),
            (
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY",
                "test-secret",
            ),
            (
                "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
                "https://account.r2.cloudflarestorage.com",
            ),
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
        env.remove("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT");
        env.insert(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID".to_owned(),
            "test-account".to_owned(),
        );
        assert!(require_r2_env(&lookup_from(env)).is_ok());
    }

    #[test]
    fn preflight_r2_env_missing_secret_names_the_missing_var() {
        let mut env = full_r2_env();
        env.remove("FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY");

        let error = require_r2_env(&lookup_from(env))
            .expect_err("a missing R2 credential must fail the preflight");
        let message = error.to_string();
        assert!(
            message.contains("FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY"),
            "message must name the missing var: {message}"
        );
    }

    #[test]
    fn preflight_r2_env_blank_bucket_is_treated_as_missing() {
        let mut env = full_r2_env();
        env.insert(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET".to_owned(),
            "   ".to_owned(),
        );

        let error = require_r2_env(&lookup_from(env))
            .expect_err("a blank R2 bucket must fail the preflight");
        assert!(
            error
                .to_string()
                .contains("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"),
            "message must name the blank var: {error}"
        );
    }

    #[test]
    fn preflight_r2_env_without_any_addressing_var_is_rejected() {
        let mut env = full_r2_env();
        env.remove("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT");

        let error =
            require_r2_env(&lookup_from(env)).expect_err("R2 needs an endpoint or an account id");
        assert!(
            error.to_string().contains("FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT or FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID"),
            "message must name the addressing pair: {error}"
        );
    }

    /// End-to-end env-based check: with `driver=r2` and a required R2 var unset, the public
    /// preflight (which reads real process env) fails and names the missing var. Mutates
    /// process-global env, so it is serialized against other env-mutating tests and restores every
    /// variable it touched on exit.
    #[test]
    fn live_write_target_preflight_r2_missing_env_fails_naming_the_var() {
        const VARS: [&str; 9] = [
            "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER",
            RUNTIME_ENVIRONMENT_ENV,
            EXECUTION_CONTEXT_ENV,
            PRELAUNCH_SHARED_ENV,
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID",
        ];
        let _guard = crate::test_support::env_lock();
        let saved: Vec<(&str, Option<String>)> = VARS
            .iter()
            .map(|name| (*name, std::env::var(name).ok()))
            .collect();

        for name in VARS {
            std::env::remove_var(name);
        }
        std::env::set_var(RUNTIME_ENVIRONMENT_ENV, "production");
        std::env::set_var(EXECUTION_CONTEXT_ENV, "developer");
        std::env::set_var(PRELAUNCH_SHARED_ENV, "1");
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "r2");
        // Use the real expected bucket so the missing-credential check is the unambiguous failure
        // cause (the exact-bucket assertion runs only after the env-presence check passes).
        std::env::set_var(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
            LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name(),
        );
        std::env::set_var(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID",
            "test-access",
        );
        std::env::set_var(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
            "https://account.r2.cloudflarestorage.com",
        );
        // FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY intentionally left unset.

        let result = live_write_target_preflight();

        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }

        let error = result.expect_err("missing FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY must fail the preflight");
        assert!(
            error
                .to_string()
                .contains("FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY"),
            "message must name the missing var: {error}"
        );
    }

    /// The R2 env vars the full-valid preflight needs, in the order they are set/restored.
    const PREFLIGHT_R2_VARS: [&str; 9] = [
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER",
        RUNTIME_ENVIRONMENT_ENV,
        EXECUTION_CONTEXT_ENV,
        PRELAUNCH_SHARED_ENV,
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
        "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID",
    ];

    /// Runs `live_write_target_preflight()` with a fully valid R2 environment except that
    /// `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET` is set to `bucket`. Saves and restores every variable it touches under the
    /// env lock so it cannot race the other env-mutating preflight tests.
    fn preflight_with_bucket(runtime: RuntimeEnvironment, bucket: &str) -> anyhow::Result<()> {
        let _guard = crate::test_support::env_lock();
        let saved: Vec<(&str, Option<String>)> = PREFLIGHT_R2_VARS
            .iter()
            .map(|name| (*name, std::env::var(name).ok()))
            .collect();

        for name in PREFLIGHT_R2_VARS {
            std::env::remove_var(name);
        }
        std::env::set_var(RUNTIME_ENVIRONMENT_ENV, runtime.wire_name());
        std::env::set_var(
            EXECUTION_CONTEXT_ENV,
            ExecutionContext::Developer.wire_name(),
        );
        std::env::set_var(
            PRELAUNCH_SHARED_ENV,
            if matches!(runtime, RuntimeEnvironment::Production) {
                "1"
            } else {
                "0"
            },
        );
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "r2");
        std::env::set_var("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET", bucket);
        std::env::set_var(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_ACCESS_KEY_ID",
            "test-access",
        );
        std::env::set_var(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_RUNTIME_SECRET_ACCESS_KEY",
            "test-secret",
        );
        std::env::set_var(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
            "https://account.r2.cloudflarestorage.com",
        );

        let result = live_write_target_preflight();

        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        result
    }

    /// A wrong-but-present `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET` must fail the preflight, and the message must name both
    /// the expected Foundation Platform production bucket and the actual wrong value. Without this gate a
    /// direct live-write subcommand could stream Bronze objects into the wrong bucket.
    #[test]
    fn live_write_target_preflight_r2_wrong_bucket_is_rejected_naming_expected_and_actual() {
        let expected = LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name();
        let wrong = "foundation-platform-bronze";

        let error = preflight_with_bucket(RuntimeEnvironment::Production, wrong).expect_err(
            "a wrong-but-present FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET must fail the preflight",
        );
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
            preflight_with_bucket(RuntimeEnvironment::Production, expected).is_ok(),
            "the exact Foundation Platform production bucket must pass the preflight"
        );
    }

    #[test]
    fn live_write_target_preflight_accepts_the_dedicated_local_r2_bucket() {
        let expected = RuntimeEnvironment::Local.foundation_r2_bucket();
        assert!(
            preflight_with_bucket(RuntimeEnvironment::Local, expected).is_ok(),
            "the local runtime must use its dedicated R2 bucket"
        );
    }

    #[tokio::test]
    async fn live_write_storage_requires_runtime_context_before_building_an_adapter() {
        const VARS: [&str; 5] = [
            "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER",
            "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT",
            RUNTIME_ENVIRONMENT_ENV,
            EXECUTION_CONTEXT_ENV,
            PRELAUNCH_SHARED_ENV,
        ];
        let _guard = crate::test_support::async_env_lock().await;
        let saved: Vec<(&str, Option<String>)> = VARS
            .iter()
            .map(|name| (*name, std::env::var(name).ok()))
            .collect();

        for name in VARS {
            std::env::remove_var(name);
        }
        std::env::set_var("FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER", "local");
        std::env::set_var(
            "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT",
            "target/bronze-live-write-context-test",
        );

        let error = match super::live_write_bronze_object_storage_from_env().await {
            Ok(_) => panic!("live storage construction must require explicit runtime context"),
            Err(error) => error,
        };

        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }

        assert!(
            error.to_string().contains(RUNTIME_ENVIRONMENT_ENV),
            "error must identify the missing runtime context: {error}"
        );
    }
}
