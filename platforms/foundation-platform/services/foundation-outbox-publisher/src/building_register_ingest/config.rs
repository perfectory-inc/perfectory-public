//! Environment-derived configuration and source identity for the building-register ingest.
//!
//! Resolves the canonical Bronze `source_slug`, the request window, retry policy, and region
//! parameters from `FOUNDATION_PLATFORM_BUILDING_REGISTER_*` / `DATA_GO_KR_*` environment variables.

use anyhow::Context;
use collection_application::BuildingRegisterPageRequest;
use collection_domain::building_register_dataset_slug;
use collection_infrastructure::DataGoKrRequestPolicy;
use foundation_shared_kernel::ids::IngestionRunId;
use uuid::Uuid;

use crate::provider_request_spacing::ProviderRequestSpacing;
use crate::public_data_control_support::{
    optional_duration_millis_env, optional_duration_seconds_env, optional_env_value,
    optional_positive_u32_env, optional_u32_env, optional_u64_env, required_env_value,
};

use super::{
    DEFAULT_MAX_PAGES, DEFAULT_OPERATION, DEFAULT_SMOKE_BJDONG_CD, DEFAULT_SMOKE_SIGUNGU_CD,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildingRegisterIngestConfig {
    pub(crate) source_slug: String,
    pub(crate) base_uri: String,
    pub(crate) service_key: String,
    pub(crate) request: BuildingRegisterPageRequest,
    pub(crate) max_pages: u32,
    pub(crate) allow_partial_page_window: bool,
    pub(crate) request_spacing: Option<ProviderRequestSpacing>,
    pub(crate) request_policy: DataGoKrRequestPolicy,
    pub(crate) live_write: Option<String>,
}

impl BuildingRegisterIngestConfig {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let live_write = optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_LIVE_WRITE")?;
        let default_policy = DataGoKrRequestPolicy::default();
        let max_attempts =
            optional_positive_u32_env("FOUNDATION_PLATFORM_DATA_GO_KR_MAX_ATTEMPTS")?
                .unwrap_or_else(|| default_policy.max_attempts());
        let request_timeout = optional_duration_seconds_env(
            "FOUNDATION_PLATFORM_DATA_GO_KR_REQUEST_TIMEOUT_SECONDS",
        )?
        .unwrap_or_else(|| default_policy.request_timeout());
        let initial_backoff = optional_duration_millis_env(
            "FOUNDATION_PLATFORM_DATA_GO_KR_RETRY_INITIAL_BACKOFF_MS",
        )?
        .unwrap_or_else(|| default_policy.initial_backoff());
        let max_backoff =
            optional_duration_millis_env("FOUNDATION_PLATFORM_DATA_GO_KR_RETRY_MAX_BACKOFF_MS")?
                .unwrap_or_else(|| default_policy.max_backoff());
        let (sigungu_cd, bjdong_cd) = building_register_region_from_options(
            optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD")?,
            optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD")?,
            live_write.as_deref(),
        )?;

        let operation = optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION")?
            .unwrap_or_else(|| DEFAULT_OPERATION.to_owned());

        Ok(Self {
            source_slug: building_register_source_slug_for_operation(&operation)?,
            base_uri: optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_BASE_URI")?
                .unwrap_or_else(|| super::DEFAULT_BASE_URI.to_owned()),
            service_key: required_env_value("DATA_GO_KR_SERVICE_KEY")?,
            request: BuildingRegisterPageRequest {
                operation,
                sigungu_cd,
                bjdong_cd,
                page_no: optional_u32_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_NO")?
                    .unwrap_or(1),
                num_of_rows: optional_u32_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS")?
                    .unwrap_or(100),
            },
            max_pages: optional_positive_u32_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_MAX_PAGES",
            )?
            .unwrap_or(DEFAULT_MAX_PAGES),
            allow_partial_page_window: partial_page_window_enabled(
                optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_PARTIAL_PAGE_WINDOW")?
                    .as_deref(),
            ),
            request_spacing: ProviderRequestSpacing::optional_from_millis(
                optional_u64_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_MIN_PAGE_INTERVAL_MS")?.or(
                    optional_u64_env("FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS")?,
                ),
            )?,
            request_policy: DataGoKrRequestPolicy::new(
                max_attempts,
                request_timeout,
                initial_backoff,
                max_backoff,
            )?,
            live_write,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildingRegisterSourceIdentity {
    pub(crate) source_slug: String,
}

impl BuildingRegisterSourceIdentity {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let operation = optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION")?
            .unwrap_or_else(|| DEFAULT_OPERATION.to_owned());
        Ok(Self {
            source_slug: building_register_source_slug_for_operation(&operation)?,
        })
    }
}

/// Resolves the canonical Bronze `source_slug` for a building-register run.
///
/// The building-register ingest collects exactly one `getBr*` operation per run, so its slug is the
/// SPECIFIC sub-type (e.g. `datagokr__building_register_main` for `getBrTitleInfo`) resolved through
/// the operation->dataset_slug map + generator (ADR 0014 Consequences — never a bare
/// `datagokr__building_register`). An explicit `FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG` env still
/// wins (used by local-proof / national-run wrappers) for an exact override.
fn building_register_source_slug_for_operation(operation: &str) -> anyhow::Result<String> {
    let dataset_slug = building_register_dataset_slug(operation).with_context(|| {
        format!("building-register operation has no registered dataset_slug: {operation}")
    })?;
    crate::public_data_control_support::resolve_canonical_source_slug(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG",
        optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG")?,
        super::PROVIDER,
        dataset_slug,
    )
}

pub(crate) fn live_write_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

pub(crate) fn partial_page_window_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

pub(crate) fn building_register_region_from_options(
    sigungu_cd: Option<String>,
    bjdong_cd: Option<String>,
    live_write: Option<&str>,
) -> anyhow::Result<(String, String)> {
    if live_write_enabled(live_write) {
        let sigungu_cd = sigungu_cd.with_context(|| {
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD is required when live write is enabled"
        })?;
        let bjdong_cd = bjdong_cd.with_context(|| {
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD is required when live write is enabled"
        })?;
        return Ok((sigungu_cd, bjdong_cd));
    }

    Ok((
        sigungu_cd.unwrap_or_else(|| DEFAULT_SMOKE_SIGUNGU_CD.to_owned()),
        bjdong_cd.unwrap_or_else(|| DEFAULT_SMOKE_BJDONG_CD.to_owned()),
    ))
}

pub(crate) fn reconcile_run_id_from_env() -> anyhow::Result<IngestionRunId> {
    let raw = required_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_RECONCILE_RUN_ID")?;
    let uuid = Uuid::parse_str(&raw)
        .with_context(|| "FOUNDATION_PLATFORM_BUILDING_REGISTER_RECONCILE_RUN_ID must be a UUID")?;
    Ok(IngestionRunId::new(uuid))
}
