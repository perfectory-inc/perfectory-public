#![allow(
    clippy::nursery,
    clippy::pedantic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity
)]
#![cfg_attr(
    test,
    allow(
        clippy::err_expect,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used
    )
)]

//! foundation-platform outbox publisher service entrypoint.
//!
//! The service publishes pending Catalog outbox rows and can run an explicit R2
//! smoke test for the vector tile manifest storage path.

use std::{env, fs, future::Future, path::Path, pin::Pin, sync::Arc, thread};

use anyhow::{bail, Context};
use catalog_application::{RebuildParcelMarkerAnchors, RebuildParcelMarkerAnchorsInput};
use catalog_infrastructure::PgParcelMarkerAnchorRebuilder;
use foundation_outbox::{
    object_storage::{
        validate_r2_smoke_object_key, ObjectStorageSmokeReport, R2InventoryRequest,
        DEFAULT_R2_SMOKE_OBJECT_KEY,
    },
    CatalogEventBroadcaster, EventBroadcaster, LoggingBroadcaster, LoggingObjectStorage,
    ObjectStorageService, OutboxScope, OutboxWorker, PgVectorTileManifestReader, PublisherConfig,
    R2ObjectStorage, WebhookBroadcaster,
};
use sqlx::PgPool;
use tokio::sync::watch;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod administrative_spatial_scope_registry;
mod bounded_live_ingestion_gate_check;
mod bronze_catalog_recovery_evidence;
mod bronze_catalog_recovery_execute;
mod bronze_catalog_recovery_manifest;
mod bronze_object_storage;
mod bronze_schema_profile;
mod building_hub_bronze_catalog_recovery;
mod building_hub_bulk_collection_plan;
mod building_hub_bulk_ingest;
mod building_register_floor_silver_export;
mod building_register_ingest;
mod building_register_local_bronze_proof;
mod building_register_page_count_batch;
mod building_register_page_count_plan_writer;
mod building_register_smoke;
mod building_register_unit_area_silver_export;
mod building_register_unit_silver_export;
mod bulk_streaming_bronze;
mod canonical_silver_gold_cutover_evidence;
mod github_actions_secret_configurator;
mod github_cutover_artifact_fetch;
mod github_cutover_dispatch;
mod industrial_complex_canonical_source_readiness;
mod industrial_complex_catalog_import;
mod industrial_complex_gold_pointer_publish;
mod industrial_complex_silver_export;
mod ingestion_run_recovery;
mod lakehouse_quality_rules_evaluate;
mod lakehouse_registry_control;
mod loopback_http;
#[cfg(test)]
mod main_tests;
mod national_bronze_object_manifest;
mod national_data_collection_async;
mod national_data_collection_coverage_ledger_check;
mod national_data_collection_ledger_execute;
mod national_data_collection_ledger_resume;
mod national_data_collection_plan_compile;
mod national_data_collection_rollout_approval_check;
mod national_data_collection_run;
mod national_data_collection_scope_writer;
mod national_data_collection_shard_manifest_writer;
mod national_page_count_plan_writer;
mod official_administrative_boundary_source_snapshot;
mod page_collector;
mod page_count_plan_contract;
mod pagination_guard;
mod parcel_marker_anchor_artifact_export;
mod parcel_marker_anchor_pbf_artifact_build;
mod parcel_marker_anchor_pbf_manifest_promote;
mod parcel_marker_anchor_streaming_rebuild;
mod postgis_anchor_pbf_regional_proof_check;
mod postgis_mirror_dlq_cutover_evidence;
mod postgis_parcel_boundary_mirror_national_rebuild;
mod postgis_parcel_boundary_mirror_rebuild;
mod provider_acquisition_import;
mod provider_acquisition_plan;
mod provider_file_bronze_catalog_recovery;
mod provider_lane;
mod provider_rate_limiter;
mod provider_request_spacing;
mod public_api_metric_writer;
mod public_data_bronze_lane_orchestrator;
mod public_data_bronze_lane_registry;
mod public_data_control_support;
mod public_provider_rate_controller;
mod public_provider_rate_policy;
mod publish_lakehouse_lineage_event;
mod r2_billing_export_collect;
mod r2_billing_usage_metrics;
mod r2_bronze_key_cleanup_candidates;
mod r2_bronze_key_migration;
mod r2_bronze_key_migration_plan;
mod r2_cleanup_verify;
mod r2_command_support;
mod r2_delete_candidates;
mod r2_inventory_audit;
mod r2_layout;
mod real_transaction_ingest;
mod regional_data_serving_load_check;
mod remote_lakehouse_job;
mod rt_molit_real_transaction_export_collection_plan;
mod rt_molit_real_transaction_export_ingest;
mod silver_gold_national_promotion_execution;
mod silver_gold_national_promotion_plan;
mod trino_ready_wait;
mod vworld_bronze_catalog_recovery;
mod vworld_cadastral_ingest;
mod vworld_cadastral_silver_export;
mod vworld_cadastral_silver_shard_export;
mod vworld_cadastral_smoke;
mod vworld_dataset_collection_plan;
mod vworld_dataset_file_ingest;
mod vworld_dataset_file_inventory;
mod vworld_land_register_ingest;
mod vworld_ned_attribute_ingest;
mod vworld_page_count_batch;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const TOKIO_WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;

type CommandFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    AuditR2Inventory,
    BuildParcelMarkerAnchorAggregatePbfArtifacts,
    BuildParcelMarkerAnchorPbfArtifacts,
    BuildingRegisterSmoke,
    DeleteR2Candidates,
    ExportBuildingRegisterFloorSilverHandoff,
    ExportBuildingRegisterUnitAreaSilverHandoff,
    ExportBuildingRegisterUnitSilverHandoff,
    ExportIndustrialComplexSilverHandoff,
    ExportParcelMarkerAnchorArtifacts,
    ExportVWorldCadastralSilverHandoff,
    ExportVWorldCadastralSilverHandoffShard,
    ExecuteNationalDataCollectionAsync,
    ExecuteNationalDataCollectionLedger,
    ExecuteRtMolitRealTransactionExportPlan,
    ExecuteSilverGoldNationalPromotion,
    EvaluateLakehouseQualityRules,
    IngestBuildingHubBulkCollection,
    IngestBuildingHubBulkFile,
    PlanBuildingHubBulkCollection,
    PlanProviderAcquisitionJobs,
    PlanRtMolitRealTransactionExports,
    PlanVWorldDatasetCollection,
    CheckBoundedLiveIngestionGate,
    CheckPostgisAnchorPbfRegionalProof,
    CheckRegionalDataServingLoad,
    CheckSilverGoldNationalPromotionExecution,
    CheckSilverGoldNationalPromotionPlan,
    CheckNationalDataCollectionCoverageLedger,
    ResumeNationalDataCollectionLedger,
    RunNationalDataCollection,
    RunRemoteLakehouseJob,
    CollectBuildingHubBronzeCatalogRecoveryInventory,
    CollectVWorldBronzeCatalogRecoveryInventory,
    CompileNationalDataCollectionPlan,
    CompileBuildingHubBronzeCatalogRecoveryManifest,
    CompileVWorldBronzeCatalogRecoveryManifest,
    RecoverBronzeCatalog,
    CheckNationalDataCollectionRolloutApproval,
    CheckAdministrativeSpatialScopeRegistry,
    CheckIndustrialComplexCanonicalSourceReadiness,
    CheckNationalBronzeObjectManifest,
    ConfigureGitHubActionsSecrets,
    FetchGithubCutoverArtifact,
    DispatchGithubCutoverWorkflow,
    ProviderRateController,
    CollectR2BillingExport,
    RunPublicDataBronzeCollectionLanes,
    WriteAdministrativeSpatialScopeRegistry,
    WriteOfficialAdministrativeBoundarySourceSnapshot,
    WriteBuildingRegisterPageCountPlan,
    WriteCanonicalSilverGoldCutoverEvidence,
    WriteNationalBronzeObjectManifest,
    WriteNationalDataCollectionRolloutApproval,
    WriteNationalDataCollectionScope,
    WriteNationalDataCollectionShardManifest,
    WriteNationalPageCountPlan,
    WritePostgisMirrorDlqCutoverEvidence,
    WritePublicApiDependencyMetric,
    WritePublicApiQuotaMetric,
    WriteR2BronzeKeyCleanupCandidates,
    WriteR2BronzeKeyMigrationPlan,
    WriteSilverGoldNationalPromotionPlan,
    ImportIndustrialComplexCatalogSeed,
    ImportProviderAcquisitionLanding,
    IngestBuildingRegister,
    IngestRealTransaction,
    IngestRtMolitRealTransactionExport,
    IngestVWorldCadastral,
    IngestVWorldDatasetFiles,
    IngestVWorldLandRegister,
    IngestVWorldNedAttribute,
    InventoryVWorldDatasetFiles,
    ProbeBuildingRegisterPageCount,
    ProbeBuildingRegisterPageCountBatch,
    ProbeVWorldPageCountBatch,
    PublishIndustrialComplexGoldPointer,
    PublishLakehouseLineageEvent,
    PublishOutboxOnce,
    R2BillingUsageMetrics,
    PromoteParcelMarkerAnchorRuntimeManifest,
    PromoteParcelMarkerAnchorPbfManifest,
    RebuildParcelMarkerAnchors,
    RebuildParcelMarkerAnchorsStreaming,
    RebuildPostgisParcelBoundaryMirror,
    RebuildPostgisParcelBoundaryMirrorNational,
    AbandonIngestionRun,
    ReconcileBuildingRegister,
    RecordLakehouseBronzeRunEvidence,
    RunBuildingRegisterLocalBronzeProof,
    RunPublisher,
    InventoryR2,
    MigrateR2BronzeKeys,
    SeedLakehouseRegistry,
    SmokeR2,
    SmokeVWorldCadastral,
    VerifyLakehouseRegistry,
    VerifyR2Cleanup,
    WaitTrinoReady,
}

fn main() -> anyhow::Result<()> {
    let command = parse_command(env::args())?;
    if command_requires_expanded_stack(command) {
        return run_command_with_expanded_stack(command);
    }
    run_command_runtime(command)
}

fn run_command_with_expanded_stack(command: Command) -> anyhow::Result<()> {
    let handle = thread::Builder::new()
        .name("outbox-publisher-command".to_owned())
        .stack_size(TOKIO_WORKER_STACK_BYTES)
        .spawn(move || run_command_runtime(command))
        .context("failed to spawn outbox-publisher command thread")?;
    handle
        .join()
        .map_err(|_| anyhow::anyhow!("outbox-publisher command thread panicked"))?
}

fn run_command_runtime(command: Command) -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_WORKER_STACK_BYTES)
        .build()
        .context("failed to build outbox-publisher Tokio runtime")?
        .block_on(run_command(command))
}

const fn command_requires_expanded_stack(command: Command) -> bool {
    matches!(
        command,
        Command::ExportParcelMarkerAnchorArtifacts
            | Command::BuildParcelMarkerAnchorPbfArtifacts
            | Command::BuildParcelMarkerAnchorAggregatePbfArtifacts
            | Command::PromoteParcelMarkerAnchorRuntimeManifest
    )
}

async fn run_command(command: Command) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().json())
        .init();

    let future: CommandFuture = match command {
        Command::AuditR2Inventory => Box::pin(r2_inventory_audit::run()),
        Command::BuildParcelMarkerAnchorAggregatePbfArtifacts => {
            Box::pin(parcel_marker_anchor_pbf_artifact_build::aggregate::run())
        }
        Command::BuildParcelMarkerAnchorPbfArtifacts => {
            Box::pin(parcel_marker_anchor_pbf_artifact_build::run())
        }
        Command::BuildingRegisterSmoke => Box::pin(async { building_register_smoke::run() }),
        Command::DeleteR2Candidates => Box::pin(r2_delete_candidates::run()),
        Command::ExportBuildingRegisterFloorSilverHandoff => {
            Box::pin(async { building_register_floor_silver_export::run() })
        }
        Command::ExportBuildingRegisterUnitAreaSilverHandoff => {
            Box::pin(async { building_register_unit_area_silver_export::run() })
        }
        Command::ExportBuildingRegisterUnitSilverHandoff => {
            Box::pin(building_register_unit_silver_export::run())
        }
        Command::ExportIndustrialComplexSilverHandoff => {
            Box::pin(industrial_complex_silver_export::run())
        }
        Command::ExportParcelMarkerAnchorArtifacts => {
            Box::pin(parcel_marker_anchor_artifact_export::run())
        }
        Command::ExportVWorldCadastralSilverHandoff => {
            Box::pin(async { vworld_cadastral_silver_export::run() })
        }
        Command::ExportVWorldCadastralSilverHandoffShard => {
            Box::pin(vworld_cadastral_silver_shard_export::run())
        }
        Command::ExecuteNationalDataCollectionAsync => {
            Box::pin(national_data_collection_async::run())
        }
        Command::ExecuteNationalDataCollectionLedger => {
            Box::pin(async { national_data_collection_ledger_execute::run() })
        }
        Command::ExecuteRtMolitRealTransactionExportPlan => {
            Box::pin(rt_molit_real_transaction_export_collection_plan::run_execute())
        }
        Command::ExecuteSilverGoldNationalPromotion => {
            Box::pin(async { silver_gold_national_promotion_execution::run_execute() })
        }
        Command::EvaluateLakehouseQualityRules => {
            Box::pin(async { lakehouse_quality_rules_evaluate::run() })
        }
        Command::ImportIndustrialComplexCatalogSeed => {
            Box::pin(industrial_complex_catalog_import::run())
        }
        Command::ImportProviderAcquisitionLanding => Box::pin(provider_acquisition_import::run()),
        Command::IngestBuildingHubBulkCollection => {
            Box::pin(building_hub_bulk_ingest::run_collection())
        }
        Command::IngestBuildingHubBulkFile => Box::pin(building_hub_bulk_ingest::run()),
        Command::PlanBuildingHubBulkCollection => {
            Box::pin(building_hub_bulk_collection_plan::run())
        }
        Command::PlanProviderAcquisitionJobs => Box::pin(provider_acquisition_plan::run()),
        Command::PlanRtMolitRealTransactionExports => {
            Box::pin(async { rt_molit_real_transaction_export_collection_plan::run() })
        }
        Command::PlanVWorldDatasetCollection => Box::pin(vworld_dataset_collection_plan::run()),
        Command::CheckNationalDataCollectionCoverageLedger => {
            Box::pin(async { national_data_collection_coverage_ledger_check::run() })
        }
        Command::ResumeNationalDataCollectionLedger => {
            Box::pin(async { national_data_collection_ledger_resume::run() })
        }
        Command::RunNationalDataCollection => {
            Box::pin(async { national_data_collection_run::run() })
        }
        Command::RunRemoteLakehouseJob => Box::pin(remote_lakehouse_job::run()),
        Command::CollectBuildingHubBronzeCatalogRecoveryInventory => {
            Box::pin(building_hub_bronze_catalog_recovery::collect_inventory())
        }
        Command::CollectVWorldBronzeCatalogRecoveryInventory => {
            Box::pin(vworld_bronze_catalog_recovery::collect_inventory())
        }
        Command::CompileNationalDataCollectionPlan => {
            Box::pin(async { national_data_collection_plan_compile::run() })
        }
        Command::CompileBuildingHubBronzeCatalogRecoveryManifest => {
            Box::pin(building_hub_bronze_catalog_recovery::run())
        }
        Command::CompileVWorldBronzeCatalogRecoveryManifest => {
            Box::pin(vworld_bronze_catalog_recovery::run())
        }
        Command::RecoverBronzeCatalog => Box::pin(bronze_catalog_recovery_execute::run()),
        Command::CheckNationalDataCollectionRolloutApproval => {
            Box::pin(async { national_data_collection_rollout_approval_check::run() })
        }
        Command::CheckAdministrativeSpatialScopeRegistry => {
            Box::pin(async { administrative_spatial_scope_registry::check() })
        }
        Command::CheckBoundedLiveIngestionGate => {
            Box::pin(async { bounded_live_ingestion_gate_check::run() })
        }
        Command::CheckPostgisAnchorPbfRegionalProof => {
            Box::pin(async { postgis_anchor_pbf_regional_proof_check::run() })
        }
        Command::CheckRegionalDataServingLoad => {
            Box::pin(async { regional_data_serving_load_check::run() })
        }
        Command::CheckSilverGoldNationalPromotionExecution => {
            Box::pin(async { silver_gold_national_promotion_execution::run_check() })
        }
        Command::CheckSilverGoldNationalPromotionPlan => {
            Box::pin(async { silver_gold_national_promotion_plan::run_check() })
        }
        Command::WriteNationalDataCollectionScope => {
            Box::pin(async { national_data_collection_scope_writer::run() })
        }
        Command::CheckNationalBronzeObjectManifest => {
            Box::pin(async { national_bronze_object_manifest::run_check() })
        }
        Command::CheckIndustrialComplexCanonicalSourceReadiness => {
            Box::pin(industrial_complex_canonical_source_readiness::run())
        }
        Command::ConfigureGitHubActionsSecrets => {
            Box::pin(async { github_actions_secret_configurator::run() })
        }
        Command::FetchGithubCutoverArtifact => Box::pin(github_cutover_artifact_fetch::run()),
        Command::DispatchGithubCutoverWorkflow => Box::pin(github_cutover_dispatch::run()),
        Command::ProviderRateController => {
            Box::pin(async { public_provider_rate_controller::run() })
        }
        Command::CollectR2BillingExport => Box::pin(r2_billing_export_collect::run()),
        Command::RunPublicDataBronzeCollectionLanes => {
            Box::pin(public_data_bronze_lane_orchestrator::run())
        }
        Command::WriteAdministrativeSpatialScopeRegistry => {
            Box::pin(async { administrative_spatial_scope_registry::write() })
        }
        Command::WriteOfficialAdministrativeBoundarySourceSnapshot => {
            Box::pin(async { official_administrative_boundary_source_snapshot::write() })
        }
        Command::WritePostgisMirrorDlqCutoverEvidence => {
            Box::pin(async { postgis_mirror_dlq_cutover_evidence::run() })
        }
        Command::WritePublicApiDependencyMetric => {
            Box::pin(async { public_api_metric_writer::run_dependency() })
        }
        Command::WritePublicApiQuotaMetric => {
            Box::pin(async { public_api_metric_writer::run_quota() })
        }
        Command::WriteBuildingRegisterPageCountPlan => {
            Box::pin(async { building_register_page_count_plan_writer::run() })
        }
        Command::WriteCanonicalSilverGoldCutoverEvidence => {
            Box::pin(async { canonical_silver_gold_cutover_evidence::run() })
        }
        Command::WriteNationalBronzeObjectManifest => {
            Box::pin(async { national_bronze_object_manifest::run_write() })
        }
        Command::WriteNationalDataCollectionRolloutApproval => {
            Box::pin(async { national_data_collection_rollout_approval_check::write() })
        }
        Command::WriteNationalDataCollectionShardManifest => {
            Box::pin(async { national_data_collection_shard_manifest_writer::run() })
        }
        Command::WriteNationalPageCountPlan => {
            Box::pin(async { national_page_count_plan_writer::run() })
        }
        Command::WriteSilverGoldNationalPromotionPlan => {
            Box::pin(async { silver_gold_national_promotion_plan::run_write() })
        }
        Command::PublishIndustrialComplexGoldPointer => {
            Box::pin(industrial_complex_gold_pointer_publish::run())
        }
        Command::PublishLakehouseLineageEvent => Box::pin(publish_lakehouse_lineage_event::run()),
        Command::PublishOutboxOnce => Box::pin(run_publisher_once()),
        Command::R2BillingUsageMetrics => Box::pin(async { r2_billing_usage_metrics::run() }),
        Command::PromoteParcelMarkerAnchorPbfManifest => {
            Box::pin(parcel_marker_anchor_pbf_manifest_promote::run())
        }
        Command::PromoteParcelMarkerAnchorRuntimeManifest => {
            Box::pin(parcel_marker_anchor_pbf_manifest_promote::run_runtime())
        }
        Command::RebuildParcelMarkerAnchors => Box::pin(run_parcel_marker_anchor_rebuild()),
        Command::RebuildParcelMarkerAnchorsStreaming => {
            Box::pin(parcel_marker_anchor_streaming_rebuild::run())
        }
        Command::RebuildPostgisParcelBoundaryMirror => {
            Box::pin(async { postgis_parcel_boundary_mirror_rebuild::run() })
        }
        Command::RebuildPostgisParcelBoundaryMirrorNational => {
            Box::pin(postgis_parcel_boundary_mirror_national_rebuild::run())
        }
        Command::IngestBuildingRegister => Box::pin(building_register_ingest::run()),
        Command::RunBuildingRegisterLocalBronzeProof => {
            Box::pin(async { building_register_local_bronze_proof::run() })
        }
        Command::IngestRealTransaction => Box::pin(real_transaction_ingest::run()),
        Command::IngestRtMolitRealTransactionExport => {
            Box::pin(rt_molit_real_transaction_export_ingest::run())
        }
        Command::IngestVWorldCadastral => Box::pin(vworld_cadastral_ingest::run()),
        Command::IngestVWorldDatasetFiles => Box::pin(vworld_dataset_file_ingest::run()),
        Command::IngestVWorldLandRegister => Box::pin(vworld_land_register_ingest::run()),
        Command::IngestVWorldNedAttribute => Box::pin(vworld_ned_attribute_ingest::run()),
        Command::InventoryVWorldDatasetFiles => Box::pin(vworld_dataset_file_inventory::run()),
        Command::ProbeBuildingRegisterPageCount => {
            Box::pin(building_register_ingest::probe_page_count())
        }
        Command::ProbeBuildingRegisterPageCountBatch => {
            Box::pin(building_register_page_count_batch::run())
        }
        Command::ProbeVWorldPageCountBatch => Box::pin(vworld_page_count_batch::run()),
        Command::AbandonIngestionRun => Box::pin(ingestion_run_recovery::run()),
        Command::ReconcileBuildingRegister => Box::pin(building_register_ingest::reconcile()),
        Command::RecordLakehouseBronzeRunEvidence => {
            Box::pin(lakehouse_registry_control::record_bronze_run_evidence())
        }
        Command::RunPublisher => Box::pin(run_publisher()),
        Command::InventoryR2 => Box::pin(run_r2_inventory()),
        Command::MigrateR2BronzeKeys => Box::pin(r2_bronze_key_migration::run()),
        Command::WriteR2BronzeKeyCleanupCandidates => {
            Box::pin(async { r2_bronze_key_cleanup_candidates::run() })
        }
        Command::WriteR2BronzeKeyMigrationPlan => {
            Box::pin(async { r2_bronze_key_migration_plan::run() })
        }
        Command::SeedLakehouseRegistry => Box::pin(lakehouse_registry_control::seed()),
        Command::SmokeR2 => Box::pin(run_r2_smoke()),
        Command::SmokeVWorldCadastral => Box::pin(vworld_cadastral_smoke::run()),
        Command::VerifyLakehouseRegistry => Box::pin(lakehouse_registry_control::verify()),
        Command::VerifyR2Cleanup => Box::pin(async { r2_cleanup_verify::run() }),
        Command::WaitTrinoReady => Box::pin(async { trino_ready_wait::run() }),
    };
    future.await
}

async fn run_publisher() -> anyhow::Result<()> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")?;
    let fallback_broadcaster = fallback_broadcaster()?;
    let catalog_broadcaster = catalog_broadcaster(pool.clone(), fallback_broadcaster).await?;
    let config = PublisherConfig::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let catalog_worker = OutboxWorker::new(pool, catalog_broadcaster, config, OutboxScope::Catalog);
    let catalog_task = tokio::spawn(async move { catalog_worker.run(shutdown_rx).await });

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")?;
    shutdown_tx
        .send(true)
        .context("failed to send shutdown signal")?;

    catalog_task.await.context("catalog worker task failed")??;

    Ok(())
}

async fn run_r2_smoke() -> anyhow::Result<()> {
    let key = r2_smoke_object_key()?;
    let storage =
        R2ObjectStorage::from_env().context("failed to configure R2 object storage for smoke")?;
    let body = format!("foundation-platform r2 smoke\nkey={key}\n").into_bytes();
    let report = storage
        .round_trip_smoke(key, body)
        .await
        .context("R2 smoke round trip failed")?;

    tracing::info!(
        object_key = %report.key,
        bytes_verified = report.bytes_verified,
        put_request_count = report.put_request_count,
        get_request_count = report.get_request_count,
        delete_request_count = report.delete_request_count,
        "R2 smoke round trip succeeded"
    );

    if let Some(metrics_path) = optional_env_value("FOUNDATION_PLATFORM_R2_SMOKE_METRICS_PATH")? {
        write_r2_smoke_metrics(Path::new(&metrics_path), &report)?;
    }

    Ok(())
}

async fn run_publisher_once() -> anyhow::Result<()> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")?;
    let fallback_broadcaster = fallback_broadcaster()?;
    let catalog_broadcaster = catalog_broadcaster(pool.clone(), fallback_broadcaster).await?;
    let config = PublisherConfig::default();
    let catalog_stats = OutboxWorker::new(pool, catalog_broadcaster, config, OutboxScope::Catalog)
        .tick()
        .await
        .context("catalog outbox publish-once tick failed")?;
    tracing::info!(
        catalog_published = catalog_stats.published,
        catalog_retried = catalog_stats.retried,
        catalog_dead_lettered = catalog_stats.dead_lettered,
        "outbox publish-once succeeded"
    );
    Ok(())
}

async fn run_parcel_marker_anchor_rebuild() -> anyhow::Result<()> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let source_snapshot_id =
        env::var("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_SOURCE_SNAPSHOT_ID")
            .context("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_SOURCE_SNAPSHOT_ID is required")?;
    let algorithm_version = env::var("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_ALGORITHM_VERSION")
        .unwrap_or_else(|_| "postgis-st_maximuminscribedcircle-v1".to_owned());
    let request_id = optional_env_value("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_REQUEST_ID")?;

    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to database")?;
    let use_case =
        RebuildParcelMarkerAnchors::new(Arc::new(PgParcelMarkerAnchorRebuilder::new(pool)));
    let report = use_case
        .execute(RebuildParcelMarkerAnchorsInput {
            source_snapshot_id,
            algorithm_version,
            requested_by_staff_id: None,
            request_id,
        })
        .await
        .context("parcel marker anchor rebuild failed")?;

    tracing::info!(
        generation_run_id = %report.generation_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        source_table = %report.source_table,
        algorithm = report.algorithm.wire_name(),
        algorithm_version = %report.algorithm_version,
        scanned_row_count = report.scanned_row_count,
        loaded_row_count = report.loaded_row_count,
        rejected_row_count = report.rejected_row_count,
        superseded_row_count = report.superseded_row_count,
        "parcel marker anchor rebuild succeeded"
    );

    if let Some(summary_path) =
        optional_env_value("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_REBUILD_SUMMARY_PATH")?
    {
        write_parcel_marker_anchor_rebuild_summary(Path::new(&summary_path), &report)?;
    }

    Ok(())
}

fn write_parcel_marker_anchor_rebuild_summary(
    path: &Path,
    report: &catalog_application::ports::ParcelMarkerAnchorRebuildReport,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("parcel marker anchor rebuild summary path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create parcel marker anchor rebuild summary directory {}",
            parent.display()
        )
    })?;
    let summary = serde_json::json!({
        "schema_version": "foundation-platform.parcel_marker_anchor_rebuild_summary.v1",
        "generated_at_utc": chrono::Utc::now().to_rfc3339(),
        "generation_run_id": report.generation_run_id,
        "source_snapshot_id": report.source_snapshot_id,
        "source_table": report.source_table,
        "algorithm": report.algorithm.wire_name(),
        "algorithm_version": report.algorithm_version,
        "scanned_row_count": report.scanned_row_count,
        "loaded_row_count": report.loaded_row_count,
        "rejected_row_count": report.rejected_row_count,
        "superseded_row_count": report.superseded_row_count,
    });
    let payload = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize parcel marker anchor rebuild summary")?;
    fs::write(path, payload).with_context(|| {
        format!(
            "failed to write parcel marker anchor rebuild summary {}",
            path.display()
        )
    })
}

fn write_r2_smoke_metrics(path: &Path, report: &ObjectStorageSmokeReport) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("R2 smoke metrics path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create R2 smoke metrics directory {}",
            parent.display()
        )
    })?;
    fs::write(path, report.to_prometheus_metrics("live_r2_smoke"))
        .with_context(|| format!("failed to write R2 smoke metrics file {}", path.display()))
}

async fn run_r2_inventory() -> anyhow::Result<()> {
    let request = r2_inventory_request()?;
    let storage = R2ObjectStorage::from_env()
        .context("failed to configure R2 object storage for inventory")?;
    let report = storage
        .inventory(request)
        .await
        .context("R2 inventory failed")?;

    tracing::info!(
        prefix = report.prefix().unwrap_or("<root>"),
        max_keys = report.max_keys(),
        key_count = report.key_count(),
        is_truncated = report.is_truncated(),
        common_prefixes = ?report.common_prefixes(),
        objects = ?report.objects(),
        "R2 inventory succeeded"
    );

    Ok(())
}

fn r2_inventory_request() -> anyhow::Result<R2InventoryRequest> {
    let prefix = optional_env_value("FOUNDATION_PLATFORM_R2_INVENTORY_PREFIX")?;
    let max_keys = optional_env_value("FOUNDATION_PLATFORM_R2_INVENTORY_MAX_KEYS")?
        .map(|raw| raw.parse::<i32>())
        .transpose()
        .context("FOUNDATION_PLATFORM_R2_INVENTORY_MAX_KEYS must be an integer")?;

    R2InventoryRequest::new(prefix.as_deref(), max_keys).context("invalid R2 inventory request")
}

async fn catalog_broadcaster(
    pool: PgPool,
    fallback: Arc<dyn EventBroadcaster>,
) -> anyhow::Result<Arc<dyn EventBroadcaster>> {
    let object_storage: Arc<dyn ObjectStorageService> = match object_storage_driver()?.as_str() {
        "log" => Arc::new(LoggingObjectStorage),
        "r2" => {
            Arc::new(R2ObjectStorage::from_env().context("failed to configure R2 object storage")?)
        }
        driver => {
            bail!(
                "FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER must be 'log' or 'r2', got '{driver}'"
            );
        }
    };

    Ok(Arc::new(CatalogEventBroadcaster::new(
        Arc::new(PgVectorTileManifestReader::new(pool)),
        object_storage,
        fallback,
    )))
}

fn fallback_broadcaster() -> anyhow::Result<Arc<dyn EventBroadcaster>> {
    let Some(raw_endpoint_specs) =
        optional_env_value("FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_ENDPOINTS")?
    else {
        return Ok(Arc::new(LoggingBroadcaster));
    };
    let signature_secret = required_env_value("FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET")?;

    let mut builder = WebhookBroadcaster::builder()
        .signature_secret(signature_secret.as_str())
        .context("invalid FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_SECRET")?;
    for (name, url) in parse_webhook_endpoint_specs(raw_endpoint_specs.as_str())? {
        builder = builder
            .endpoint(name.as_str(), url.as_str())
            .context("invalid FOUNDATION_PLATFORM_OUTBOX_WEBHOOK_ENDPOINTS")?;
    }

    Ok(Arc::new(builder.build().context(
        "failed to configure outbox webhook broadcaster",
    )?))
}

fn parse_webhook_endpoint_specs(raw: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut specs = Vec::new();
    for raw_part in raw.split(';') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }

        let (name, url) = part
            .split_once('=')
            .context("webhook endpoint must use name=url format")?;
        let name = name.trim();
        let url = url.trim();
        if name.is_empty() {
            bail!("webhook endpoint name must not be empty");
        }
        if url.is_empty() {
            bail!("webhook endpoint url must not be empty");
        }

        specs.push((name.to_owned(), url.to_owned()));
    }

    if specs.is_empty() {
        bail!("at least one webhook endpoint is required");
    }

    Ok(specs)
}

fn object_storage_driver() -> anyhow::Result<String> {
    let driver = env::var("FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER")
        .unwrap_or_else(|_| "log".to_owned())
        .trim()
        .to_ascii_lowercase();
    if driver.is_empty() {
        bail!("FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER must not be empty");
    }
    Ok(driver)
}

fn parse_command<I, S>(args: I) -> anyhow::Result<Command>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    let _program = args.next();
    let command = args.next().map(|arg| arg.as_ref().to_owned());
    if let Some(extra) = args.next() {
        bail!(
            "unexpected argument '{}' for foundation-outbox-publisher",
            extra.as_ref()
        );
    }

    match command.as_deref() {
        None | Some("run") => Ok(Command::RunPublisher),
        Some("audit-r2-inventory") => Ok(Command::AuditR2Inventory),
        Some("building-register-smoke") => Ok(Command::BuildingRegisterSmoke),
        Some("delete-r2-candidates") => Ok(Command::DeleteR2Candidates),
        Some("ingest-building-register") => Ok(Command::IngestBuildingRegister),
        Some("run-building-register-local-bronze-proof") => {
            Ok(Command::RunBuildingRegisterLocalBronzeProof)
        }
        Some("ingest-building-hub-bulk-collection") => Ok(Command::IngestBuildingHubBulkCollection),
        Some("ingest-building-hub-bulk-file") => Ok(Command::IngestBuildingHubBulkFile),
        Some("plan-building-hub-bulk-collection") => Ok(Command::PlanBuildingHubBulkCollection),
        Some("plan-rt-molit-real-transaction-exports") => {
            Ok(Command::PlanRtMolitRealTransactionExports)
        }
        Some("plan-vworld-dataset-collection") => Ok(Command::PlanVWorldDatasetCollection),
        Some("check-national-data-collection-coverage-ledger") => {
            Ok(Command::CheckNationalDataCollectionCoverageLedger)
        }
        Some("execute-national-data-collection-ledger") => {
            Ok(Command::ExecuteNationalDataCollectionLedger)
        }
        Some("execute-rt-molit-real-transaction-export-plan") => {
            Ok(Command::ExecuteRtMolitRealTransactionExportPlan)
        }
        Some("execute-silver-gold-national-promotion") => {
            Ok(Command::ExecuteSilverGoldNationalPromotion)
        }
        Some("evaluate-lakehouse-quality-rules") => Ok(Command::EvaluateLakehouseQualityRules),
        Some("resume-national-data-collection-ledger") => {
            Ok(Command::ResumeNationalDataCollectionLedger)
        }
        Some("run-national-data-collection") => Ok(Command::RunNationalDataCollection),
        Some("run-remote-lakehouse-job") => Ok(Command::RunRemoteLakehouseJob),
        Some("collect-building-hub-bronze-catalog-recovery-inventory") => {
            Ok(Command::CollectBuildingHubBronzeCatalogRecoveryInventory)
        }
        Some("collect-vworld-bronze-catalog-recovery-inventory") => {
            Ok(Command::CollectVWorldBronzeCatalogRecoveryInventory)
        }
        Some("compile-national-data-collection-plan") => {
            Ok(Command::CompileNationalDataCollectionPlan)
        }
        Some("compile-building-hub-bronze-catalog-recovery-manifest") => {
            Ok(Command::CompileBuildingHubBronzeCatalogRecoveryManifest)
        }
        Some("compile-vworld-bronze-catalog-recovery-manifest") => {
            Ok(Command::CompileVWorldBronzeCatalogRecoveryManifest)
        }
        Some("recover-bronze-catalog") => Ok(Command::RecoverBronzeCatalog),
        Some("check-national-data-collection-rollout-approval") => {
            Ok(Command::CheckNationalDataCollectionRolloutApproval)
        }
        Some("check-administrative-spatial-scope-registry") => {
            Ok(Command::CheckAdministrativeSpatialScopeRegistry)
        }
        Some("check-bounded-live-ingestion-gate") => Ok(Command::CheckBoundedLiveIngestionGate),
        Some("check-postgis-anchor-pbf-regional-proof") => {
            Ok(Command::CheckPostgisAnchorPbfRegionalProof)
        }
        Some("check-regional-data-serving-load") => Ok(Command::CheckRegionalDataServingLoad),
        Some("check-silver-gold-national-promotion-execution") => {
            Ok(Command::CheckSilverGoldNationalPromotionExecution)
        }
        Some("check-silver-gold-national-promotion-plan") => {
            Ok(Command::CheckSilverGoldNationalPromotionPlan)
        }
        Some("write-national-data-collection-scope") => {
            Ok(Command::WriteNationalDataCollectionScope)
        }
        Some("check-national-bronze-object-manifest") => {
            Ok(Command::CheckNationalBronzeObjectManifest)
        }
        Some("configure-github-actions-secrets") => Ok(Command::ConfigureGitHubActionsSecrets),
        Some("fetch-github-cutover-artifact") => Ok(Command::FetchGithubCutoverArtifact),
        Some("dispatch-github-cutover-workflow") => Ok(Command::DispatchGithubCutoverWorkflow),
        Some("provider-rate-controller") => Ok(Command::ProviderRateController),
        Some("collect-r2-billing-export") => Ok(Command::CollectR2BillingExport),
        Some("run-public-data-bronze-collection-lanes") => {
            Ok(Command::RunPublicDataBronzeCollectionLanes)
        }
        Some("write-administrative-spatial-scope-registry") => {
            Ok(Command::WriteAdministrativeSpatialScopeRegistry)
        }
        Some("write-official-administrative-boundary-source-snapshot") => {
            Ok(Command::WriteOfficialAdministrativeBoundarySourceSnapshot)
        }
        Some("write-postgis-mirror-dlq-cutover-evidence") => {
            Ok(Command::WritePostgisMirrorDlqCutoverEvidence)
        }
        Some("write-public-api-dependency-metric") => Ok(Command::WritePublicApiDependencyMetric),
        Some("write-public-api-quota-metric") => Ok(Command::WritePublicApiQuotaMetric),
        Some("write-building-register-page-count-plan") => {
            Ok(Command::WriteBuildingRegisterPageCountPlan)
        }
        Some("write-canonical-silver-gold-cutover-evidence") => {
            Ok(Command::WriteCanonicalSilverGoldCutoverEvidence)
        }
        Some("write-national-bronze-object-manifest") => {
            Ok(Command::WriteNationalBronzeObjectManifest)
        }
        Some("write-national-data-collection-rollout-approval") => {
            Ok(Command::WriteNationalDataCollectionRolloutApproval)
        }
        Some("check-industrial-complex-canonical-source-readiness") => {
            Ok(Command::CheckIndustrialComplexCanonicalSourceReadiness)
        }
        Some("write-national-data-collection-shard-manifest") => {
            Ok(Command::WriteNationalDataCollectionShardManifest)
        }
        Some("write-national-page-count-plan") => Ok(Command::WriteNationalPageCountPlan),
        Some("write-silver-gold-national-promotion-plan") => {
            Ok(Command::WriteSilverGoldNationalPromotionPlan)
        }
        Some("plan-provider-acquisition-jobs") => Ok(Command::PlanProviderAcquisitionJobs),
        Some("inventory-vworld-dataset-files") => Ok(Command::InventoryVWorldDatasetFiles),
        Some("ingest-real-transaction") => Ok(Command::IngestRealTransaction),
        Some("ingest-rt-molit-real-transaction-export") => {
            Ok(Command::IngestRtMolitRealTransactionExport)
        }
        Some("probe-building-register-page-count") => Ok(Command::ProbeBuildingRegisterPageCount),
        Some("probe-building-register-page-count-batch") => {
            Ok(Command::ProbeBuildingRegisterPageCountBatch)
        }
        Some("probe-vworld-page-count-batch") => Ok(Command::ProbeVWorldPageCountBatch),
        Some("build-parcel-marker-anchor-pbf-artifacts") => {
            Ok(Command::BuildParcelMarkerAnchorPbfArtifacts)
        }
        Some("build-parcel-marker-anchor-aggregate-pbf-artifacts") => {
            Ok(Command::BuildParcelMarkerAnchorAggregatePbfArtifacts)
        }
        Some("ingest-vworld-cadastral") => Ok(Command::IngestVWorldCadastral),
        Some("ingest-vworld-dataset-files") => Ok(Command::IngestVWorldDatasetFiles),
        Some("ingest-vworld-land-register") => Ok(Command::IngestVWorldLandRegister),
        Some("ingest-vworld-ned-attribute") => Ok(Command::IngestVWorldNedAttribute),
        Some("export-industrial-complex-silver-handoff") => {
            Ok(Command::ExportIndustrialComplexSilverHandoff)
        }
        Some("export-building-register-floor-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterFloorSilverHandoff)
        }
        Some("export-building-register-unit-area-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterUnitAreaSilverHandoff)
        }
        Some("export-building-register-unit-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterUnitSilverHandoff)
        }
        Some("export-parcel-marker-anchor-artifacts") => {
            Ok(Command::ExportParcelMarkerAnchorArtifacts)
        }
        Some("export-vworld-cadastral-silver-handoff") => {
            Ok(Command::ExportVWorldCadastralSilverHandoff)
        }
        Some("export-vworld-cadastral-silver-handoff-shard") => {
            Ok(Command::ExportVWorldCadastralSilverHandoffShard)
        }
        Some("execute-national-data-collection-async") => {
            Ok(Command::ExecuteNationalDataCollectionAsync)
        }
        Some("import-industrial-complex-catalog-seed") => {
            Ok(Command::ImportIndustrialComplexCatalogSeed)
        }
        Some("import-provider-acquisition-landing") => {
            Ok(Command::ImportProviderAcquisitionLanding)
        }
        Some("publish-industrial-complex-gold-pointer") => {
            Ok(Command::PublishIndustrialComplexGoldPointer)
        }
        Some("publish-lakehouse-lineage-event") => Ok(Command::PublishLakehouseLineageEvent),
        Some("publish-outbox-once") => Ok(Command::PublishOutboxOnce),
        Some("r2-billing-usage-metrics") => Ok(Command::R2BillingUsageMetrics),
        Some("promote-parcel-marker-anchor-pbf-manifest") => {
            Ok(Command::PromoteParcelMarkerAnchorPbfManifest)
        }
        Some("promote-parcel-marker-anchor-runtime-manifest") => {
            Ok(Command::PromoteParcelMarkerAnchorRuntimeManifest)
        }
        Some("rebuild-parcel-marker-anchors") => Ok(Command::RebuildParcelMarkerAnchors),
        Some("rebuild-parcel-marker-anchors-streaming") => {
            Ok(Command::RebuildParcelMarkerAnchorsStreaming)
        }
        Some("rebuild-postgis-parcel-boundary-mirror") => {
            Ok(Command::RebuildPostgisParcelBoundaryMirror)
        }
        Some("rebuild-postgis-parcel-boundary-mirror-national") => {
            Ok(Command::RebuildPostgisParcelBoundaryMirrorNational)
        }
        Some("abandon-ingestion-run") => Ok(Command::AbandonIngestionRun),
        Some("reconcile-building-register") => Ok(Command::ReconcileBuildingRegister),
        Some("record-lakehouse-bronze-run-evidence") => {
            Ok(Command::RecordLakehouseBronzeRunEvidence)
        }
        Some("inventory-r2") => Ok(Command::InventoryR2),
        Some("migrate-r2-bronze-keys") => Ok(Command::MigrateR2BronzeKeys),
        Some("write-r2-bronze-key-cleanup-candidates") => {
            Ok(Command::WriteR2BronzeKeyCleanupCandidates)
        }
        Some("write-r2-bronze-key-migration-plan") => Ok(Command::WriteR2BronzeKeyMigrationPlan),
        Some("seed-lakehouse-registry") => Ok(Command::SeedLakehouseRegistry),
        Some("smoke-r2") => Ok(Command::SmokeR2),
        Some("smoke-vworld-cadastral") => Ok(Command::SmokeVWorldCadastral),
        Some("verify-lakehouse-registry") => Ok(Command::VerifyLakehouseRegistry),
        Some("verify-r2-cleanup") => Ok(Command::VerifyR2Cleanup),
        Some("wait-trino-ready") => Ok(Command::WaitTrinoReady),
        Some(other) => bail!("unknown outbox-publisher command '{other}'"),
    }
}

fn r2_smoke_object_key() -> anyhow::Result<String> {
    let key = env::var("FOUNDATION_PLATFORM_R2_SMOKE_OBJECT_KEY")
        .unwrap_or_else(|_| DEFAULT_R2_SMOKE_OBJECT_KEY.to_owned());
    validate_r2_smoke_object_key(&key)?;
    Ok(key)
}

#[cfg(test)]
mod main_command_tests;

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use foundation_outbox::object_storage::ObjectStorageSmokeReport;

    use super::{parse_webhook_endpoint_specs, write_r2_smoke_metrics};

    #[test]
    fn r2_smoke_metrics_writer_persists_operation_counts() -> anyhow::Result<()> {
        let path = PathBuf::from("target/outbox-publisher-main-tests/r2-smoke-metrics.prom");
        if path.exists() {
            fs::remove_file(&path)?;
        }
        let report = ObjectStorageSmokeReport {
            key: "gold/_smoke/foundation-platform-r2-smoke-test.json".to_owned(),
            bytes_verified: 512,
            put_request_count: 1,
            get_request_count: 1,
            delete_request_count: 1,
        };

        write_r2_smoke_metrics(&path, &report)?;

        let metrics = fs::read_to_string(&path)?;
        assert!(metrics.contains(
            "foundation_platform_r2_smoke_request_total{source=\"live_r2_smoke\",operation=\"put\"} 1"
        ));
        assert!(metrics
            .contains("foundation_platform_r2_smoke_bytes_verified{source=\"live_r2_smoke\"} 512"));
        assert!(!metrics.contains("foundation-platform-r2-smoke-test.json"));
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn webhook_endpoint_specs_parse_name_url_pairs() -> anyhow::Result<()> {
        let specs = parse_webhook_endpoint_specs(
            "gongzzang=https://gongzzang.example.invalid/foundation-platform/events; \
             dawneer=https://dawneer.example.invalid/foundation-platform/events",
        )?;

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].0, "gongzzang");
        assert_eq!(
            specs[0].1,
            "https://gongzzang.example.invalid/foundation-platform/events"
        );
        assert_eq!(specs[1].0, "dawneer");
        assert_eq!(
            specs[1].1,
            "https://dawneer.example.invalid/foundation-platform/events"
        );
        Ok(())
    }

    #[test]
    fn webhook_endpoint_specs_reject_empty_name() -> anyhow::Result<()> {
        let error = match parse_webhook_endpoint_specs("=https://example.test/events") {
            Ok(specs) => anyhow::bail!("expected parse failure, got {specs:?}"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("name"));
        Ok(())
    }

    #[test]
    fn webhook_endpoint_specs_reject_missing_url() -> anyhow::Result<()> {
        let error = match parse_webhook_endpoint_specs("gongzzang=") {
            Ok(specs) => anyhow::bail!("expected parse failure, got {specs:?}"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("url"));
        Ok(())
    }
}
