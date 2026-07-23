//! Live write path: persists planned Bronze pages to object storage + catalog metadata.
//!
//! Writes each page's raw payload to object storage, records Bronze metadata + schema profiles
//! through the unit of work, and completes the ingestion run. On any failure it marks the run
//! Failed with a truncated terminal message instead of leaving it Running.

use anyhow::{bail, Context};
use chrono::Utc;
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_application::{BuildingRegisterBronzePagePlan, BuildingRegisterPageRequest};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::{BronzeObjectId, IngestionRunId, SourceCatalogId};
use sqlx::PgPool;
use uuid::Uuid;

use crate::bronze_object_storage::bronze_object_storage_from_env;
use crate::bronze_schema_profile::CandidateKeyOverride;
use crate::page_collector::{collect_planned_pages, CollectablePage, PageCollectorLane};
use crate::public_data_control_support::required_env_value;

use super::config::BuildingRegisterIngestConfig;
use super::model::{batch_request_params, ingestion_run, source_catalog_entry};
use super::plan::BuildingRegisterPlannedPage;

pub(super) async fn persist_plans(
    config: &BuildingRegisterIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[BuildingRegisterPlannedPage],
) -> anyhow::Result<()> {
    if pages.is_empty() {
        bail!("building-register ingest produced no Bronze page plans");
    }

    // Live-write path (`run` already gated on live_write before calling this): validate + log the
    // resolved R2 target before the first put, instead of failing mid-run on a misconfigured target.
    crate::bronze_object_storage::live_write_target_preflight()
        .context("building-register live-write target preflight failed")?;
    let database_url = required_env_value("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database for building-register ingest")?;
    let uow = collection_infrastructure::PgBronzeIngestUnitOfWork::new(pool);
    let storage = bronze_object_storage_from_env()
        .await
        .context("failed to configure object storage for building-register Bronze ingest")?;

    let report =
        persist_plans_with_adapters(config, run_id, started_at, pages, &uow, storage.as_ref())
            .await?;

    tracing::info!(
        run_id = %report.run_id,
        last_object_key = ?report.last_object_key,
        last_object_checksum_sha256 = report.last_object_checksum_sha256.as_deref().unwrap_or(""),
        last_bronze_object_id = ?report.last_bronze_object_id,
        logical_record_count = report.logical_records_seen,
        objects_written = report.objects_written,
        "building-register Bronze ingest live write succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BuildingRegisterPersistReport {
    pub(super) run_id: IngestionRunId,
    pub(super) last_object_key: Option<String>,
    /// Lowercase hex sha256 of the last Bronze object written (pairs with `last_object_key`); echoed
    /// in the run summary so the parent ledger-execute event carries a real checksum, not empty.
    pub(super) last_object_checksum_sha256: Option<String>,
    pub(super) last_bronze_object_id: Option<BronzeObjectId>,
    pub(super) logical_records_seen: u64,
    pub(super) objects_written: u64,
}

pub(super) async fn persist_plans_with_adapters<Uow, Storage>(
    config: &BuildingRegisterIngestConfig,
    run_id: IngestionRunId,
    started_at: chrono::DateTime<Utc>,
    pages: &[BuildingRegisterPlannedPage],
    uow: &Uow,
    storage: &Storage,
) -> anyhow::Result<BuildingRegisterPersistReport>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    if pages.is_empty() {
        bail!("building-register ingest produced no Bronze page plans");
    }

    // The compiled plans the planning stage already produced drive the run-level request snapshot;
    // the per-page commit + schema-profile + accounting now lives in the shared PageCollector loop
    // (ADR 0017), which hands each page's RAW identity to the BronzeCommitter (ADR 0016). The loop
    // body, ordering, `objects_written` accounting, commit-error -> terminal-failure mapping, run
    // lifecycle, and schema-profile gathering are identical across page lanes, so they live ONCE in
    // `collect_planned_pages`; this lane supplies only its catalog identity + per-source declaration.
    let plans: Vec<BuildingRegisterBronzePagePlan> =
        pages.iter().map(|page| page.plan.clone()).collect();

    let collectable_pages: Vec<CollectablePage<BuildingRegisterPageRequest>> = pages
        .iter()
        .map(|page| CollectablePage {
            plan: page.plan.clone(),
            request: page.request.clone(),
            raw_payload: page.raw_payload.clone(),
            payload: page.payload.clone(),
        })
        .collect();

    let report = collect_planned_pages(
        &BuildingRegisterLane,
        source_catalog_entry(config, started_at),
        ingestion_run(
            // The collector overrides `source_catalog_id` from the upserted source, so any placeholder
            // is fine here; pass the building-register run identity + request snapshot as before.
            SourceCatalogId::new(Uuid::nil()),
            run_id,
            started_at,
            batch_request_params(config, &plans),
        ),
        &config.source_slug,
        started_at,
        &collectable_pages,
        uow,
        storage,
    )
    .await?;

    Ok(BuildingRegisterPersistReport {
        run_id: report.run_id,
        last_object_key: report.last_object_key,
        // The shared collection report carries the last committed object's checksum, which this lane
        // echoes in its summary so the parent ledger-execute event has a real checksum, not empty.
        last_object_checksum_sha256: report.last_object_checksum_sha256,
        last_bronze_object_id: report.last_bronze_object_id,
        logical_records_seen: report.logical_records_seen,
        objects_written: report.objects_written,
    })
}

/// Building-register page lane declaration (ADR 0017): the only per-source bits the shared
/// [`collect_planned_pages`] loop needs — the lane label used in commit-error context, and the
/// candidate-key override (building-register re-scores the `mgmBldrgstPk` management number, the
/// data.go.kr building primary key). The loop, accounting, commit handoff, run lifecycle, and
/// schema-profile gathering all live in the collector.
struct BuildingRegisterLane;

impl PageCollectorLane for BuildingRegisterLane {
    type Request = BuildingRegisterPageRequest;

    fn lane_label(&self) -> &str {
        "building-register"
    }

    fn candidate_key_override(&self) -> CandidateKeyOverride {
        CandidateKeyOverride::EndsWith("mgmBldrgstPk")
    }
}
