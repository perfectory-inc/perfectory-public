//! Building-register Bronze ingestion command.
//!
//! Hosts the three CLI entry points — live ingest [`run`], page-count [`probe_page_count`], and
//! [`reconcile`] — and wires the focused submodules together:
//!
//! - [`config`]   — env-derived configuration + source identity
//! - [`plan`]     — live page planning + page-count probe
//! - [`persist`]  — live write to object storage + catalog metadata
//! - [`reconcile`] (module) — rebuild metadata from immutable Bronze objects
//! - [`model`]    — catalog/Bronze entity builders shared by persist + reconcile

use anyhow::Context;
use chrono::Utc;
use collection_application::BuildingRegisterPageRequest;
use collection_infrastructure::{
    DataGoKrBuildingRegisterClient, DataGoKrBuildingRegisterConfig, PgBronzeIngestRepository,
    PgBronzeIngestUnitOfWork,
};
use foundation_outbox::R2ObjectStorage;
use foundation_shared_kernel::ids::IngestionRunId;
use sqlx::PgPool;
use std::path::Path;
use uuid::Uuid;

use crate::public_data_control_support::{optional_env_value, required_env_value};

mod config;
mod model;
mod persist;
mod plan;
mod reconcile;

// Re-exported at crate visibility for the sibling `building_register_page_count_batch` module,
// which consumes the config + page-count-probe surface as `crate::building_register_ingest::*`.
pub(crate) use config::BuildingRegisterIngestConfig;
pub(crate) use plan::{
    page_count_probe_from_response_metadata, write_page_count_probe_output,
    BuildingRegisterPageCountProbe,
};

use config::{reconcile_run_id_from_env, BuildingRegisterSourceIdentity};
use model::{total_logical_record_count, total_size_bytes};
use persist::persist_plans;
use plan::plan_pages;
use reconcile::reconcile_building_register_run_with_adapters;

// Re-imported into this module's namespace so the `tests` submodule (a child of this module)
// keeps using `super::` paths unchanged after the split. Private + test-gated: the names are only
// reached through `super::` from `tests`, which can see this module's private items.
#[cfg(test)]
use collection_application::{BuildingRegisterBronzePagePlan, BuildingRegisterBronzePagePlanInput};
#[cfg(test)]
use config::{
    building_register_region_from_options, live_write_enabled, partial_page_window_enabled,
};
#[cfg(test)]
use model::{
    batch_request_params, bronze_object, ingestion_run, schema_profiles_for_plans,
    source_catalog_entry,
};
#[cfg(test)]
use persist::persist_plans_with_adapters;
#[cfg(test)]
use plan::{
    effective_page_size_from_response_metadata, json_u64_pointer, page_requests_for_batch,
    should_stop_after_page, BuildingRegisterPlannedPage,
};
#[cfg(test)]
use reconcile::BuildingRegisterBronzeObjectReader;

const SOURCE_NAME: &str = "data.go.kr Building Register (BldRgstHubService)";
const PROVIDER: &str = "data.go.kr";
const DATASET_NAME: &str = "building_register";
pub(super) const DEFAULT_BASE_URI: &str = "https://apis.data.go.kr/1613000/BldRgstHubService";
pub(super) const DEFAULT_OPERATION: &str = "getBrTitleInfo";
const DEFAULT_SMOKE_SIGUNGU_CD: &str = "11680";
const DEFAULT_SMOKE_BJDONG_CD: &str = "10300";
const DEFAULT_MAX_PAGES: u32 = 1;
// `BRONZE_JSON_CONTENT_TYPE` is still consumed by `model::bronze_object` (the reconcile path's Bronze
// metadata builder). The Bronze cache-control constant moved entirely into the shared PageCollector
// loop (ADR 0017), which is now the only place that builds the per-page commit input.
const BRONZE_JSON_CONTENT_TYPE: &str = "application/json";

/// Runs one building-register Bronze ingestion batch.
pub async fn run() -> anyhow::Result<()> {
    let config = BuildingRegisterIngestConfig::from_env()?;
    let client = DataGoKrBuildingRegisterClient::new_with_policy(
        &DataGoKrBuildingRegisterConfig {
            base_uri: config.base_uri.clone(),
            service_key: config.service_key.clone(),
        },
        config.request_policy,
    )?;
    let run_id = IngestionRunId::new(Uuid::new_v4());
    let now = Utc::now();
    let pages = plan_pages(&config, &client, run_id, now.date_naive()).await?;

    if !config::live_write_enabled(config.live_write.as_deref()) {
        let plans: Vec<_> = pages.iter().map(|page| page.plan.clone()).collect();
        tracing::info!(
            objects_planned = plans.len(),
            logical_records_seen = total_logical_record_count(&plans),
            total_size_bytes = total_size_bytes(&plans),
            first_object_key = plans.first().map(|plan| plan.object_key.as_str()),
            last_object_key = plans.last().map(|plan| plan.object_key.as_str()),
            "building-register Bronze ingest dry run succeeded"
        );
        return Ok(());
    }

    persist_plans(&config, run_id, now, &pages).await
}

/// Probes the first provider page and reports the exact page count needed for a scope.
pub async fn probe_page_count() -> anyhow::Result<()> {
    let config = BuildingRegisterIngestConfig::from_env()?;
    let client = DataGoKrBuildingRegisterClient::new_with_policy(
        &DataGoKrBuildingRegisterConfig {
            base_uri: config.base_uri.clone(),
            service_key: config.service_key.clone(),
        },
        config.request_policy,
    )?;
    let fetched_page = client
        .fetch_page(&BuildingRegisterPageRequest {
            page_no: 1,
            ..config.request.clone()
        })
        .await
        .context("failed to fetch data.go.kr building-register page count probe")?;
    let probe = page_count_probe_from_response_metadata(&config.request, &fetched_page.payload)?;

    tracing::info!(
        operation = %probe.operation,
        sigungu_cd = %probe.sigungu_cd,
        bjdong_cd = %probe.bjdong_cd,
        requested_page_size = probe.requested_page_size,
        effective_page_size = probe.effective_page_size,
        provider_total_count = probe.provider_total_count,
        required_pages = probe.required_pages,
        "building-register page count probe succeeded"
    );

    if let Some(output_path) =
        optional_env_value("FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_COUNT_PROBE_OUTPUT_PATH")?
    {
        write_page_count_probe_output(Path::new(&output_path), &probe)?;
    }

    Ok(())
}

/// Reconciles a previously created building-register ingestion run from immutable Bronze objects.
pub async fn reconcile() -> anyhow::Result<()> {
    let source_identity = BuildingRegisterSourceIdentity::from_env()?;
    let run_id = reconcile_run_id_from_env()?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for building-register reconcile")?;
    let repo = PgBronzeIngestRepository::new(pool.clone());
    let uow = PgBronzeIngestUnitOfWork::new(pool);
    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 for building-register Bronze reconcile")?;

    let report = reconcile_building_register_run_with_adapters(
        &source_identity,
        run_id,
        &repo,
        &uow,
        &storage,
    )
    .await?;

    tracing::info!(
        run_id = %report.run_id,
        objects_expected = report.objects_expected,
        objects_repaired = report.objects_repaired,
        schema_profiles_upserted = report.schema_profiles_upserted,
        logical_records_seen = report.logical_records_seen,
        "building-register Bronze reconcile succeeded"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
