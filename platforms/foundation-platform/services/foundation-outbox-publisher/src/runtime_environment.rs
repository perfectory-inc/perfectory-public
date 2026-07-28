//! Runtime environment policy for operational backend selection.
//!
//! The canonical developer environment uses a dedicated Cloudflare R2 bucket, not a local
//! object-store emulator. Before launch, a developer process may explicitly target production
//! through the guarded pre-launch acknowledgement. Unit tests may still use fakes, but live
//! operational commands must make both their backend target and process context explicit.

use anyhow::{bail, Context};
use lakehouse_domain::LakehouseOwnerService;

/// Environment variable carrying the canonical runtime environment name.
pub const RUNTIME_ENVIRONMENT_ENV: &str = "FOUNDATION_PLATFORM_RUNTIME_ENV";
/// Environment variable identifying where the process is executing, independently of its backend target.
pub const EXECUTION_CONTEXT_ENV: &str = "FOUNDATION_PLATFORM_EXECUTION_CONTEXT";
/// Explicit acknowledgement required when a developer process targets production before launch.
pub const PRELAUNCH_SHARED_ENV: &str = "FOUNDATION_PLATFORM_PRELAUNCH_SHARED";

/// Canonical deployment environments understood by Foundation operational commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeEnvironment {
    /// Developer environment using a dedicated R2 bucket.
    Local,
    /// CI environment using isolated test resources.
    Ci,
    /// Pre-production staging environment.
    Staging,
    /// Production environment.
    Production,
}

/// Process location/role. This is deliberately separate from [`RuntimeEnvironment`]: a developer
/// process may temporarily target production before launch, but it must say so explicitly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionContext {
    /// A command launched from a developer workstation.
    Developer,
    /// A disposable CI/test runner.
    Ci,
    /// A deployed service or operator runner outside a developer workstation.
    Service,
}

impl ExecutionContext {
    /// Parses the canonical process-context names.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw.trim() {
            "developer" => Ok(Self::Developer),
            "ci" => Ok(Self::Ci),
            "service" => Ok(Self::Service),
            "" => bail!("{EXECUTION_CONTEXT_ENV} must not be empty"),
            other => bail!(
                "{EXECUTION_CONTEXT_ENV} must be one of developer, ci, service; got '{other}'"
            ),
        }
    }

    /// Reads the required process context from the environment.
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var(EXECUTION_CONTEXT_ENV).with_context(|| {
            format!("{EXECUTION_CONTEXT_ENV} is required for operational commands")
        })?;
        Self::parse(&raw)
    }

    /// Returns the stable wire value.
    #[cfg(test)]
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Developer => "developer",
            Self::Ci => "ci",
            Self::Service => "service",
        }
    }
}

impl RuntimeEnvironment {
    /// Parses a canonical environment name.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or unknown value. Aliases are deliberately rejected so an
    /// environment name cannot drift between manifests and runtime code.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw.trim() {
            "local" => Ok(Self::Local),
            "ci" => Ok(Self::Ci),
            "staging" => Ok(Self::Staging),
            "production" => Ok(Self::Production),
            "" => bail!("{RUNTIME_ENVIRONMENT_ENV} must not be empty"),
            other => bail!(
                "{RUNTIME_ENVIRONMENT_ENV} must be one of local, ci, staging, production; got '{other}'"
            ),
        }
    }

    /// Reads the required runtime environment from the process environment.
    ///
    /// # Errors
    ///
    /// Returns an error when the variable is missing or invalid.
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var(RUNTIME_ENVIRONMENT_ENV).with_context(|| {
            format!("{RUNTIME_ENVIRONMENT_ENV} is required for operational commands")
        })?;
        Self::parse(&raw)
    }

    /// Reads and validates both the backend target and the process context.
    ///
    /// A developer process targeting production is accepted only with the explicit pre-launch
    /// acknowledgement. This prevents a copied production bucket from silently becoming a
    /// developer default while preserving the requested pre-launch sharing policy.
    pub fn from_env_with_execution_context() -> anyhow::Result<Self> {
        let environment = Self::from_env()?;
        let context = ExecutionContext::from_env()?;
        let prelaunch_shared = prelaunch_shared_from_env()?;
        validate_execution_context(environment, context, prelaunch_shared)?;
        Ok(environment)
    }

    /// Returns the stable wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Ci => "ci",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }

    /// Returns the environment-specific Foundation R2 bucket.
    ///
    /// Production deliberately delegates to the lakehouse domain SSOT instead of duplicating its
    /// governed bucket name here.
    #[must_use]
    pub const fn foundation_r2_bucket(self) -> &'static str {
        match self {
            Self::Local => "foundation-platform-lakehouse-dev",
            Self::Ci => "foundation-platform-lakehouse-ci",
            Self::Staging => "foundation-platform-lakehouse-staging",
            Self::Production => {
                LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name()
            }
        }
    }

    /// Returns whether the runtime is allowed to use a non-R2 catalog storage adapter.
    #[must_use]
    pub const fn allows_catalog_non_r2(self) -> bool {
        matches!(self, Self::Local | Self::Ci)
    }

    /// Returns whether the runtime is allowed to use file-backed Bronze storage.
    #[must_use]
    pub const fn allows_bronze_file_storage(self) -> bool {
        matches!(self, Self::Local | Self::Ci)
    }
}

/// Parses the explicit pre-launch acknowledgement flag.
fn prelaunch_shared_from_env() -> anyhow::Result<bool> {
    match std::env::var(PRELAUNCH_SHARED_ENV)
        .unwrap_or_default()
        .trim()
    {
        "" | "0" => Ok(false),
        "1" => Ok(true),
        other => bail!("{PRELAUNCH_SHARED_ENV} must be 0 or 1, got '{other}'"),
    }
}

/// Validates the relationship between process location and backend target.
///
/// The pre-launch acknowledgement is intentionally narrow: it is valid only for a developer
/// process targeting production. CI and deployed services must use their own normal contexts.
pub fn validate_execution_context(
    environment: RuntimeEnvironment,
    context: ExecutionContext,
    prelaunch_shared: bool,
) -> anyhow::Result<()> {
    if prelaunch_shared
        && !(matches!(environment, RuntimeEnvironment::Production)
            && matches!(context, ExecutionContext::Developer))
    {
        bail!(
            "{PRELAUNCH_SHARED_ENV}=1 is valid only for developer execution targeting production"
        );
    }

    if matches!(environment, RuntimeEnvironment::Production)
        && matches!(context, ExecutionContext::Developer)
        && !prelaunch_shared
    {
        bail!(
            "developer execution targeting production requires {PRELAUNCH_SHARED_ENV}=1 until launch"
        );
    }

    if matches!(environment, RuntimeEnvironment::Ci) && !matches!(context, ExecutionContext::Ci) {
        bail!("runtime environment ci requires {EXECUTION_CONTEXT_ENV}=ci");
    }

    if matches!(environment, RuntimeEnvironment::Local) && matches!(context, ExecutionContext::Ci) {
        bail!("runtime environment local cannot use {EXECUTION_CONTEXT_ENV}=ci");
    }

    Ok(())
}

/// Validates the Catalog publisher object-storage driver for an environment.
///
/// # Errors
///
/// Returns an error when staging or production selects the logging adapter, or when the driver
/// name is unknown.
pub fn validate_catalog_driver(
    environment: RuntimeEnvironment,
    driver: &str,
) -> anyhow::Result<()> {
    match driver {
        "r2" => Ok(()),
        "log" if environment.allows_catalog_non_r2() => Ok(()),
        "log" => bail!(
            "{RUNTIME_ENVIRONMENT_ENV}={} requires FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=r2; log is local/CI only",
            environment.wire_name()
        ),
        other => bail!(
            "FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER must be 'r2' or 'log', got '{other}'"
        ),
    }
}

/// Validates the Bronze object-storage driver for an environment.
///
/// # Errors
///
/// Returns an error when staging or production selects file-backed storage, or when the driver
/// name is unknown.
pub fn validate_bronze_driver(environment: RuntimeEnvironment, driver: &str) -> anyhow::Result<()> {
    match driver {
        "r2" => Ok(()),
        "local" if environment.allows_bronze_file_storage() => Ok(()),
        "local" => bail!(
            "{RUNTIME_ENVIRONMENT_ENV}={} requires FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2; local is local/CI-only",
            environment.wire_name()
        ),
        other => bail!(
            "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER must be 'r2' or 'local', got '{other}'"
        ),
    }
}

/// Checks that an R2 bucket matches the runtime's governed bucket.
///
/// # Errors
///
/// Returns an error when the configured bucket is empty or belongs to another environment.
pub fn validate_foundation_r2_bucket(
    environment: RuntimeEnvironment,
    bucket: &str,
) -> anyhow::Result<()> {
    let bucket = bucket.trim();
    if bucket.is_empty() {
        bail!("FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET must not be empty")
    }
    let expected = environment.foundation_r2_bucket();
    if bucket != expected {
        bail!(
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET must be {expected} for runtime environment {}, got {bucket}",
            environment.wire_name()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_bronze_driver, validate_catalog_driver, validate_foundation_r2_bucket};
    use super::{
        validate_execution_context, ExecutionContext, RuntimeEnvironment, EXECUTION_CONTEXT_ENV,
        RUNTIME_ENVIRONMENT_ENV,
    };

    #[test]
    fn parses_only_canonical_environment_names() {
        assert_eq!(
            RuntimeEnvironment::parse("local").unwrap(),
            RuntimeEnvironment::Local
        );
        assert_eq!(
            RuntimeEnvironment::parse("ci").unwrap(),
            RuntimeEnvironment::Ci
        );
        assert_eq!(
            RuntimeEnvironment::parse("staging").unwrap(),
            RuntimeEnvironment::Staging
        );
        assert_eq!(
            RuntimeEnvironment::parse("production").unwrap(),
            RuntimeEnvironment::Production
        );
        assert!(RuntimeEnvironment::parse("prod").is_err());
        assert!(RuntimeEnvironment::parse("").is_err());
        assert!(RuntimeEnvironment::parse("qa")
            .unwrap_err()
            .to_string()
            .contains(RUNTIME_ENVIRONMENT_ENV));
    }

    #[test]
    fn parses_only_canonical_execution_context_names() {
        assert_eq!(
            ExecutionContext::parse("developer").unwrap(),
            ExecutionContext::Developer
        );
        assert_eq!(ExecutionContext::parse("ci").unwrap(), ExecutionContext::Ci);
        assert_eq!(
            ExecutionContext::parse("service").unwrap(),
            ExecutionContext::Service
        );
        assert!(ExecutionContext::parse("").is_err());
        assert!(ExecutionContext::parse("local")
            .unwrap_err()
            .to_string()
            .contains(EXECUTION_CONTEXT_ENV));
    }

    #[test]
    fn developer_production_requires_explicit_prelaunch_acknowledgement() {
        assert!(validate_execution_context(
            RuntimeEnvironment::Production,
            ExecutionContext::Developer,
            false,
        )
        .is_err());
        assert!(validate_execution_context(
            RuntimeEnvironment::Production,
            ExecutionContext::Developer,
            true,
        )
        .is_ok());
        assert!(validate_execution_context(
            RuntimeEnvironment::Production,
            ExecutionContext::Service,
            false,
        )
        .is_ok());
        assert!(validate_execution_context(
            RuntimeEnvironment::Local,
            ExecutionContext::Developer,
            false,
        )
        .is_ok());
    }

    #[test]
    fn prelaunch_acknowledgement_cannot_leak_to_other_contexts() {
        assert!(validate_execution_context(
            RuntimeEnvironment::Local,
            ExecutionContext::Developer,
            true,
        )
        .is_err());
        assert!(
            validate_execution_context(RuntimeEnvironment::Ci, ExecutionContext::Ci, true,)
                .is_err()
        );
    }

    #[test]
    fn maps_non_production_buckets_without_duplicating_production_ssot() {
        assert_eq!(
            RuntimeEnvironment::Local.foundation_r2_bucket(),
            "foundation-platform-lakehouse-dev"
        );
        assert_eq!(
            RuntimeEnvironment::Ci.foundation_r2_bucket(),
            "foundation-platform-lakehouse-ci"
        );
        assert_eq!(
            RuntimeEnvironment::Staging.foundation_r2_bucket(),
            "foundation-platform-lakehouse-staging"
        );
        assert_eq!(
            RuntimeEnvironment::Production.foundation_r2_bucket(),
            "foundation-platform-lakehouse-prod"
        );
    }

    #[test]
    fn production_rejects_local_and_logging_drivers() {
        assert!(validate_bronze_driver(RuntimeEnvironment::Production, "local").is_err());
        assert!(validate_catalog_driver(RuntimeEnvironment::Production, "log").is_err());
        assert!(validate_bronze_driver(RuntimeEnvironment::Local, "local").is_ok());
        assert!(validate_catalog_driver(RuntimeEnvironment::Ci, "log").is_ok());
    }

    #[test]
    fn bucket_must_match_runtime_environment() {
        assert!(validate_foundation_r2_bucket(
            RuntimeEnvironment::Local,
            "foundation-platform-lakehouse-dev"
        )
        .is_ok());
        assert!(validate_foundation_r2_bucket(
            RuntimeEnvironment::Local,
            "foundation-platform-lakehouse-prod"
        )
        .is_err());
    }
}
