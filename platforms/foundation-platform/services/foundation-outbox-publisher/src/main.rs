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

mod administrative_boundary_postgis_publish;
mod administrative_boundary_runtime_promote;
mod administrative_boundary_static_release_publish;
mod administrative_spatial_scope_registry;
use foundation_publisher_tiles::boundary_static_release_publish;
mod bounded_live_ingestion_gate_check;
use foundation_publisher_collection::bronze_catalog_recovery_execute;
use foundation_publisher_collection::bronze_catalog_recovery_manifest;
use foundation_publisher_collection::bronze_object_members;
use foundation_publisher_collection::bronze_object_storage;
use foundation_publisher_collection::bronze_schema_profile;
mod building_hub_bronze_catalog_recovery;
use foundation_publisher_collection::building_hub_bulk_collection_plan;
use foundation_publisher_collection::building_hub_bulk_ingest;
use foundation_publisher_collection::building_register_ingest;
use foundation_publisher_silver::building_register_floor_silver_export;
mod building_register_local_bronze_proof;
use foundation_publisher_collection::building_register_page_count_batch;
use foundation_publisher_collection::building_register_page_count_plan_writer;
mod building_register_smoke;
use foundation_publisher_silver::hub_provider_release;
mod building_register_title_silver_export;
mod building_register_unit_area_silver_export;
mod building_register_unit_silver_export;
mod canonical_release_proof;
mod canonical_silver_gold_cutover_evidence;
mod code_go_kr_legal_dong_collect;
mod courtauction_go_kr_collect;
mod data_go_kr_onbid_collect;
mod data_go_kr_registered_factory_collect;
mod silver_load_objects;
use foundation_publisher_collection::collection_job_requeue;
mod dbase_table;
mod github_actions_secret_configurator;
mod github_cutover_artifact_fetch;
mod github_cutover_dispatch;
mod industrial_complex_address_resolution_build;
mod industrial_complex_address_source_collect;
mod industrial_complex_boundary_postgis_publish;
mod industrial_complex_boundary_runtime_promote;
mod industrial_complex_boundary_silver_export;
mod industrial_complex_boundary_static_release_publish;
mod industrial_complex_bronze_raw_jsonl_export;
mod industrial_complex_canonical_load;
mod industrial_complex_canonical_source_readiness;
mod industrial_complex_catalog_import;
mod industrial_complex_gold_pointer_publish;
mod industrial_complex_gold_profile_export;
use foundation_publisher_core::industrial_complex_gold_profile_store;
mod industrial_complex_silver_export;
use foundation_publisher_collection::ingestion_run_recovery;
#[cfg(test)]
mod test_crosswalk;
use foundation_publisher_silver::lakehouse_engine_contract;
mod lakehouse_inventory;
mod lakehouse_quality_rules_evaluate;
mod lakehouse_registry_control;
use foundation_publisher_core::lakehouse_snapshot_scan;
use foundation_publisher_silver::land_use_silver_export;
mod loopback_http;
#[cfg(test)]
mod main_tests;
mod onbid_media;
use foundation_publisher_by_pnu::building_by_pnu_serving_export;
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_by_pnu::by_pnu_serving_rebase;
use foundation_publisher_by_pnu::by_pnu_serving_state;
use foundation_publisher_core::by_pnu_gateway_contract;
#[cfg(test)]
#[cfg(test)]
use foundation_publisher_core::test_support;
mod handoff_object_support;
use foundation_publisher_tiles::lakehouse_tile_bake;
mod lineage_review_items_load;
mod lineage_steward_fold;
mod map_edit_handoff_export;
use foundation_publisher_collection::national_bronze_object_manifest;
use foundation_publisher_collection::national_data_collection_async;
use foundation_publisher_collection::national_data_collection_coverage_ledger_check;
use foundation_publisher_collection::national_data_collection_ledger_execute;
use foundation_publisher_collection::national_data_collection_ledger_resume;
use foundation_publisher_collection::national_data_collection_plan_compile;
use foundation_publisher_collection::national_data_collection_rollout_approval_check;
use foundation_publisher_collection::national_data_collection_run;
use foundation_publisher_collection::national_data_collection_scope_writer;
use foundation_publisher_collection::national_data_collection_shard_manifest_writer;
use foundation_publisher_collection::national_page_count_plan_writer;
mod official_administrative_boundary_source_snapshot;
use foundation_publisher_collection::page_collector;
use foundation_publisher_parcel_anchor::parcel_boundary_handoff;
mod parcel_boundary_postgis_publish;
mod parcel_boundary_runtime_promote;
mod parcel_boundary_static_release_publish;
use foundation_publisher_by_pnu::parcel_by_pnu_serving_export;
use foundation_publisher_parcel_anchor::parcel_marker_anchor_artifact_export;
use foundation_publisher_parcel_anchor::parcel_marker_anchor_pbf_artifact_build;
use foundation_publisher_parcel_anchor::parcel_marker_anchor_streaming_rebuild;
mod parcel_projection_digest;
mod parcel_publication_contract;
mod parcel_publication_evidence_sealer;
mod parcel_publication_evidence_writer;
mod postgis_anchor_pbf_regional_proof_check;
mod postgis_mirror_dlq_cutover_evidence;
mod postgis_parcel_boundary_mirror_national_rebuild;
mod postgis_parcel_boundary_mirror_rebuild;
use foundation_publisher_collection::provider_acquisition_import;
use foundation_publisher_collection::provider_acquisition_plan;
use foundation_publisher_collection::provider_file_bronze_catalog_recovery;
use foundation_publisher_collection::provider_request_spacing;
use foundation_publisher_collection::public_api_metric_writer;
use foundation_publisher_collection::public_data_bronze_lane_orchestrator;
use foundation_publisher_collection::public_provider_rate_controller;
use foundation_publisher_core::public_data_control_support;
mod publish_lakehouse_lineage_event;
mod publisher_runtime;
#[cfg(test)]
use publisher_runtime::{parse_webhook_endpoint_specs, write_r2_smoke_metrics};
use publisher_runtime::{
    run_parcel_marker_anchor_rebuild, run_publisher, run_publisher_once, run_r2_inventory,
    run_r2_smoke,
};
mod r2_billing_export_collect;
mod r2_billing_usage_metrics;
mod r2_bronze_key_cleanup_candidates;
mod r2_bronze_key_migration;
mod r2_bronze_key_migration_plan;
mod r2_cleanup_verify;
use foundation_publisher_core::r2_command_support;
mod r2_delete_candidates;
mod r2_inventory_audit;
use foundation_publisher_collection::real_transaction_ingest;
use foundation_publisher_core::r2_layout;
mod regional_data_serving_load_check;
use foundation_publisher_collection::rt_molit_real_transaction_collect;
use foundation_publisher_collection::rt_molit_real_transaction_export_collection_plan;
use foundation_publisher_collection::rt_molit_real_transaction_export_ingest;
use foundation_publisher_core::runtime_environment;
use foundation_publisher_silver::remote_lakehouse_job;
mod serving_source_lineage_register;
mod shapefile_polygon_reader;
mod silver_gold_national_promotion_execution;
mod silver_gold_national_promotion_plan;
mod spatial_tile_wap_command;
mod staging_namespace;
use foundation_publisher_tiles::static_release_readdress;
use foundation_publisher_tiles::static_release_toolchain;
use foundation_publisher_tiles::tile_derivative_object_storage;
mod trino_ready_wait;
mod unit_official_price_projection_load;
use foundation_publisher_collection::vworld_bronze_catalog_recovery;
use foundation_publisher_collection::vworld_cadastral_ingest;
use foundation_publisher_collection::vworld_cadastral_silver_export;
use foundation_publisher_collection::vworld_cadastral_silver_shard_export;
use foundation_publisher_collection::vworld_cadastral_smoke;
use foundation_publisher_collection::vworld_dataset_collection_plan;
use foundation_publisher_collection::vworld_dataset_file_ingest;
use foundation_publisher_collection::vworld_dataset_file_inventory;
use foundation_publisher_collection::vworld_land_register_ingest;
use foundation_publisher_collection::vworld_ned_attribute_ingest;
use foundation_publisher_collection::vworld_page_count_batch;
use foundation_publisher_tiles::vector_tile_runtime_promote;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const TOKIO_WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;

type CommandFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    AuditR2Inventory,
    BuildParcelMarkerAnchorAggregatePbfArtifacts,
    BuildParcelMarkerAnchorPbfArtifacts,
    BuildIndustrialComplexAddressResolution,
    BuildingRegisterSmoke,
    CollectCodeGoKrLegalDong,
    CollectDataGoKrOnbid,
    CollectRtMolitRealTransaction,
    CollectCourtauctionGoKr,
    CollectDataGoKrRegisteredFactory,
    PlanSilverLoadObjects,
    CollectIndustrialComplexAddressSource,
    DeleteR2Candidates,
    ExportBuildingRegisterFloorSilverHandoff,
    StageBuildingRegisterFloorInputs,
    SelectBuildingRegisterFloorInputs,
    RunBuildingRegisterFloorCycle,
    StopBuildingRegisterFloorCycle,
    StopGoldPanelRebuild,
    StopLakehouseMaintenance,
    RunSilverRefresh,
    StopSilverRefresh,
    StopParcelTileBake,
    ExportBuildingRegisterApartmentPriceSilverHandoff,
    ExportBuildingRegisterExclusiveUnitSilverHandoff,
    ExportBuildingRegisterUnitAreaSilverHandoff,
    ExportBuildingRegisterTitleSilverHandoff,
    ExportBuildingRegisterUnitSilverHandoff,
    ExportIndustrialComplexBoundarySilverHandoff,
    ExportIndustrialComplexBronzeRawJsonl,
    ExportIndustrialComplexGoldProfiles,
    ExportIndustrialComplexSilverHandoff,
    ExportParcelByPnuServing,
    ExportBuildingByPnuServing,
    SectionPacks(building_by_pnu_serving_export::section_packs::PackCommand),
    ExportParcelMarkerAnchorArtifacts,
    ExportVWorldCadastralSilverHandoff,
    ExportVWorldCadastralShapefileSilverHandoff,
    ExportVWorldCadastralSilverHandoffShard,
    ExportLandUsePlanSilverHandoff,
    ExportLandUseZoneCodeSilverHandoff,
    ExportLandIndividualPriceSilverHandoff,
    ExportLandCharacteristicSilverHandoff,
    ExportLandForestSilverHandoff,
    ExportLandTransferHistorySilverHandoff,
    ExportLandRightRegistrationSilverHandoff,
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
    CheckR2RuntimeTarget,
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
    PublishAdministrativeBoundaryPostgis,
    PublishAdministrativeBoundaryStaticRelease,
    PublishIndustrialComplexBoundaryPostgis,
    PublishIndustrialComplexBoundaryStaticRelease,
    ProveIndustrialComplexBoundaryStaticReleaseMutationGuards,
    PublishParcelBoundaryPostgis,
    PublishParcelBoundaryStaticRelease,
    ReaddressStaticRelease,
    ExportMapEditHandoff,
    BakeLakehouseTiles,
    LoadUnitOfficialPriceProjection,
    LoadLineageReviewItems,
    MeasureBronzeObjectMembers,
    ClearStagingNamespace,
    ExportLineageStewardFold,
    ExportLegalDongStewardDecisions,
    RecordLineageStewardFolds,
    SealParcelPublicationEvidence,
    WriteParcelPublicationEvidence,
    PromoteAdministrativeBoundaryRuntime,
    PromoteIndustrialComplexBoundaryRuntime,
    PromoteParcelBoundaryRuntime,
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
    WriteCanonicalReleaseProof,
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
    LoadIndustrialComplexCanonical,
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
    ProbeSpatialTileWap,
    ProbeVWorldPageCountBatch,
    PublishIndustrialComplexGoldPointer,
    PublishIndustrialComplexGoldPointers,
    PublishLakehouseLineageEvent,
    PublishParcelByPnuServingManifest,
    PublishBuildingByPnuServingManifest,
    ShowParcelByPnuServingState,
    ShowBuildingByPnuServingState,
    VerifyParcelByPnuServingRebase,
    PublishOutboxOnce,
    R2BillingUsageMetrics,
    RebuildParcelMarkerAnchors,
    RebuildParcelMarkerAnchorsStreaming,
    RebuildPostgisParcelBoundaryMirror,
    RebuildPostgisParcelBoundaryMirrorNational,
    RebuildPostgisParcelBoundaryMirrorNationalFromContract,
    AbandonIngestionRun,
    ReconcileBuildingRegister,
    RecordLakehouseBronzeRunEvidence,
    RequeueDeadLetteredCollectionJob,
    RegisterServingSourceLineage,
    RunBuildingRegisterLocalBronzeProof,
    RunPublisher,
    /// `job-run`: one listed job through the runner (root ADR-0270 §2).
    JobRun,
    InventoryLakehouse,
    InventoryR2,
    MigrateR2BronzeKeys,
    SeedLakehouseRegistry,
    SmokeR2,
    SmokeVWorldCadastral,
    VerifyStaticReleaseToolchain,
    VerifyLakehouseRegistry,
    VerifyR2Cleanup,
    ValidateTileDerivativeR2,
    WaitTrinoReady,
}

fn main() -> anyhow::Result<()> {
    let command = parse_command(env::args())?;
    if command == Command::JobRun {
        // No runtime: the runner waits on its child itself, and its exit code is the outcome's.
        std::process::exit(i32::from(job_run_from_env()));
    }
    let result = if command_requires_expanded_stack(command) {
        run_command_with_expanded_stack(command)
    } else {
        run_command_runtime(command)
    };
    result
        .map_err(exit_if_refused)
        .map_err(exit_if_provider_side)
}

/// A provider that did not serve or refuses the account exits with its own code and one outcome
/// line (`foundation_publisher_core::provider_outcome`, root ADR-0363), so the staging smoke and
/// the jobs tell the provider's side from the release's.
fn exit_if_provider_side(error: anyhow::Error) -> anyhow::Error {
    if let Some((line, code)) = foundation_publisher_core::provider_outcome::provider_side(&error) {
        eprintln!("Error: {error:?}");
        eprintln!("{line}");
        std::process::exit(code);
    }
    error
}

/// A refusal of the inputs exits `REFUSED_EXIT_CODE`, so the bakes do not retry it; any other
/// error returns to exit 1, which they retry (`foundation_publisher_core::refusal`).
fn exit_if_refused(error: anyhow::Error) -> anyhow::Error {
    use foundation_publisher_core::refusal::{is_refused, REFUSED_EXIT_CODE};
    if is_refused(&error) {
        eprintln!("Error: {error:?}");
        std::process::exit(REFUSED_EXIT_CODE);
    }
    error
}

/// `job-run`: the job and paths from the environment (root ADR-0270 §2); the exit code is the one
/// the outcome line means, and a run that cannot be described still ends on a `failed` line.
fn job_run_from_env() -> u8 {
    use std::io::Write as _;
    let mut out = std::io::stdout().lock();
    match foundation_jobs::job_run::JobRun::from_env(|name| env::var(name).ok()) {
        Ok(run) => foundation_jobs::job_run::execute(&run, &mut out).exit_code(),
        Err(reason) => {
            let failed = foundation_jobs::runner::Outcome::Failed(reason);
            let _ = writeln!(out, "{}", failed.line());
            failed.exit_code()
        }
    }
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
    )
}

async fn run_command(command: Command) -> anyhow::Result<()> {
    let log_writer = if command == Command::SelectBuildingRegisterFloorInputs {
        // Selection stdout is one bounded JSON document even with verbose SQL logging enabled.
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::stderr)
    } else {
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::stdout)
    };
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().json().with_writer(log_writer))
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
        Command::BuildIndustrialComplexAddressResolution => {
            Box::pin(async { industrial_complex_address_resolution_build::run() })
        }
        Command::CollectCodeGoKrLegalDong => Box::pin(code_go_kr_legal_dong_collect::run()),
        Command::CollectDataGoKrOnbid => Box::pin(data_go_kr_onbid_collect::run()),
        Command::CollectRtMolitRealTransaction => {
            Box::pin(rt_molit_real_transaction_collect::run())
        }
        Command::CollectCourtauctionGoKr => Box::pin(courtauction_go_kr_collect::run()),
        Command::CollectDataGoKrRegisteredFactory => {
            Box::pin(data_go_kr_registered_factory_collect::run())
        }
        Command::PlanSilverLoadObjects => Box::pin(silver_load_objects::run()),
        Command::CollectIndustrialComplexAddressSource => {
            Box::pin(industrial_complex_address_source_collect::run())
        }
        Command::DeleteR2Candidates => Box::pin(r2_delete_candidates::run()),
        Command::ExportBuildingRegisterFloorSilverHandoff => {
            Box::pin(building_register_floor_silver_export::run())
        }
        Command::StageBuildingRegisterFloorInputs => {
            Box::pin(building_register_floor_silver_export::stage_inputs())
        }
        Command::SelectBuildingRegisterFloorInputs => {
            Box::pin(building_register_floor_silver_export::select_inputs_cli())
        }
        Command::ExportBuildingRegisterUnitAreaSilverHandoff => {
            Box::pin(building_register_unit_area_silver_export::run())
        }
        Command::ExportBuildingRegisterTitleSilverHandoff => {
            Box::pin(building_register_title_silver_export::run())
        }
        Command::ExportBuildingRegisterUnitSilverHandoff => {
            Box::pin(building_register_unit_silver_export::run())
        }
        Command::ExportIndustrialComplexBoundarySilverHandoff => {
            Box::pin(async { industrial_complex_boundary_silver_export::run() })
        }
        Command::ExportIndustrialComplexBronzeRawJsonl => {
            Box::pin(async { industrial_complex_bronze_raw_jsonl_export::run() })
        }
        Command::ExportIndustrialComplexGoldProfiles => {
            Box::pin(industrial_complex_gold_profile_export::run())
        }
        Command::ExportIndustrialComplexSilverHandoff => {
            Box::pin(industrial_complex_silver_export::run())
        }
        Command::ExportBuildingByPnuServing => Box::pin(building_by_pnu_serving_export::run()),
        Command::SectionPacks(command) => Box::pin(command.run()),
        Command::ExportParcelByPnuServing => Box::pin(parcel_by_pnu_serving_export::run()),
        Command::ShowParcelByPnuServingState => Box::pin(by_pnu_serving_state::run_parcel()),
        Command::ShowBuildingByPnuServingState => Box::pin(by_pnu_serving_state::run_building()),
        Command::VerifyParcelByPnuServingRebase => Box::pin(by_pnu_serving_rebase::run_parcel()),
        Command::ExportParcelMarkerAnchorArtifacts => {
            Box::pin(parcel_marker_anchor_artifact_export::run())
        }
        Command::ExportVWorldCadastralSilverHandoff => {
            Box::pin(async { vworld_cadastral_silver_export::run() })
        }
        Command::ExportVWorldCadastralShapefileSilverHandoff => {
            Box::pin(foundation_publisher_silver::vworld_cadastral_shapefile_silver_export::run())
        }
        Command::ExportVWorldCadastralSilverHandoffShard => {
            Box::pin(vworld_cadastral_silver_shard_export::run())
        }
        Command::ExportBuildingRegisterExclusiveUnitSilverHandoff => Box::pin(
            foundation_publisher_silver::building_register_exclusive_unit_silver_export::run(),
        ),
        Command::ExportBuildingRegisterApartmentPriceSilverHandoff => Box::pin(
            foundation_publisher_silver::building_register_apartment_price_silver_export::run(),
        ),
        Command::ExportLandUsePlanSilverHandoff => Box::pin(land_use_silver_export::run_plan()),
        Command::ExportLandUseZoneCodeSilverHandoff => {
            Box::pin(land_use_silver_export::run_zone_code())
        }
        Command::ExportLandRightRegistrationSilverHandoff => {
            Box::pin(land_use_silver_export::run_land_right_registration())
        }
        Command::ExportLandTransferHistorySilverHandoff => {
            Box::pin(land_use_silver_export::run_land_transfer_history())
        }
        Command::ExportLandForestSilverHandoff => {
            Box::pin(land_use_silver_export::run_land_forest())
        }
        Command::ExportLandCharacteristicSilverHandoff => {
            Box::pin(land_use_silver_export::run_land_characteristic())
        }
        Command::ExportLandIndividualPriceSilverHandoff => {
            Box::pin(land_use_silver_export::run_price())
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
        Command::LoadIndustrialComplexCanonical => {
            Box::pin(industrial_complex_canonical_load::run())
        }
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
        Command::StopBuildingRegisterFloorCycle => {
            Box::pin(async { remote_lakehouse_job::stop_floor_cycle() })
        }
        Command::StopGoldPanelRebuild => {
            Box::pin(async { remote_lakehouse_job::stop_gold_panel_rebuild() })
        }
        Command::StopLakehouseMaintenance => {
            Box::pin(async { remote_lakehouse_job::stop_lakehouse_maintenance() })
        }
        Command::RunBuildingRegisterFloorCycle => Box::pin(remote_lakehouse_job::run_floor_cycle()),
        Command::RunSilverRefresh => Box::pin(remote_lakehouse_job::run_silver_refresh()),
        Command::StopSilverRefresh => {
            Box::pin(async { remote_lakehouse_job::stop_silver_refresh() })
        }
        Command::StopParcelTileBake => {
            Box::pin(async { remote_lakehouse_job::stop_parcel_tile_bake() })
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
        Command::PublishAdministrativeBoundaryPostgis => {
            Box::pin(administrative_boundary_postgis_publish::run())
        }
        Command::PublishAdministrativeBoundaryStaticRelease => {
            Box::pin(administrative_boundary_static_release_publish::run())
        }
        Command::PublishIndustrialComplexBoundaryPostgis => {
            Box::pin(industrial_complex_boundary_postgis_publish::run())
        }
        Command::PublishIndustrialComplexBoundaryStaticRelease => {
            Box::pin(industrial_complex_boundary_static_release_publish::run())
        }
        Command::ProveIndustrialComplexBoundaryStaticReleaseMutationGuards => Box::pin(
            industrial_complex_boundary_static_release_publish::run_local_mutation_guard_proof(),
        ),
        Command::PublishParcelBoundaryPostgis => Box::pin(parcel_boundary_postgis_publish::run()),
        Command::ReaddressStaticRelease => Box::pin(static_release_readdress::run()),
        Command::ExportMapEditHandoff => Box::pin(map_edit_handoff_export::run()),
        Command::BakeLakehouseTiles => Box::pin(lakehouse_tile_bake::run()),
        Command::PublishParcelBoundaryStaticRelease => {
            Box::pin(parcel_boundary_static_release_publish::run())
        }
        Command::LoadUnitOfficialPriceProjection => {
            Box::pin(unit_official_price_projection_load::run())
        }
        Command::LoadLineageReviewItems => Box::pin(lineage_review_items_load::run()),
        Command::MeasureBronzeObjectMembers => Box::pin(bronze_object_members::run()),
        Command::ClearStagingNamespace => Box::pin(staging_namespace::clear()),
        Command::ExportLineageStewardFold => Box::pin(lineage_steward_fold::export()),
        Command::ExportLegalDongStewardDecisions => {
            Box::pin(lineage_steward_fold::export_legal_dong())
        }
        Command::RecordLineageStewardFolds => Box::pin(lineage_steward_fold::record()),
        Command::SealParcelPublicationEvidence => {
            Box::pin(parcel_publication_evidence_sealer::run())
        }
        Command::WriteParcelPublicationEvidence => {
            Box::pin(parcel_publication_evidence_writer::run())
        }
        Command::PromoteAdministrativeBoundaryRuntime => {
            Box::pin(administrative_boundary_runtime_promote::run())
        }
        Command::PromoteIndustrialComplexBoundaryRuntime => {
            Box::pin(industrial_complex_boundary_runtime_promote::run())
        }
        Command::PromoteParcelBoundaryRuntime => Box::pin(parcel_boundary_runtime_promote::run()),
        Command::CheckBoundedLiveIngestionGate => {
            Box::pin(async { bounded_live_ingestion_gate_check::run() })
        }
        Command::CheckR2RuntimeTarget => {
            Box::pin(async { bronze_object_storage::live_write_target_preflight() })
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
        Command::WriteCanonicalReleaseProof => Box::pin(async { canonical_release_proof::run() }),
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
        Command::PublishBuildingByPnuServingManifest => Box::pin(
            by_pnu_serving_manifest_publish::run(by_pnu_gateway_contract::ByPnuLane::Building),
        ),
        Command::PublishParcelByPnuServingManifest => Box::pin(
            by_pnu_serving_manifest_publish::run(by_pnu_gateway_contract::ByPnuLane::Parcel),
        ),
        Command::PublishIndustrialComplexGoldPointers => {
            Box::pin(industrial_complex_gold_pointer_publish::run_from_export_summary())
        }
        Command::PublishLakehouseLineageEvent => Box::pin(publish_lakehouse_lineage_event::run()),
        Command::PublishOutboxOnce => Box::pin(run_publisher_once()),
        Command::R2BillingUsageMetrics => Box::pin(async { r2_billing_usage_metrics::run() }),
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
        Command::RebuildPostgisParcelBoundaryMirrorNationalFromContract => {
            Box::pin(postgis_parcel_boundary_mirror_national_rebuild::run_from_contract())
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
        Command::ProbeSpatialTileWap => Box::pin(async { spatial_tile_wap_command::run() }),
        Command::ProbeVWorldPageCountBatch => Box::pin(vworld_page_count_batch::run()),
        Command::AbandonIngestionRun => Box::pin(ingestion_run_recovery::run()),
        Command::ReconcileBuildingRegister => Box::pin(building_register_ingest::reconcile()),
        Command::RecordLakehouseBronzeRunEvidence => {
            Box::pin(lakehouse_registry_control::record_bronze_run_evidence())
        }
        Command::RequeueDeadLetteredCollectionJob => Box::pin(collection_job_requeue::run()),
        Command::RegisterServingSourceLineage => Box::pin(serving_source_lineage_register::run()),
        Command::RunPublisher => Box::pin(run_publisher()),
        // main() runs it before any runtime starts and exits with its outcome.
        Command::JobRun => Box::pin(async { anyhow::bail!("job-run does not run in the runtime") }),
        Command::InventoryLakehouse => Box::pin(lakehouse_inventory::run()),
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
        Command::VerifyStaticReleaseToolchain => Box::pin(async {
            static_release_toolchain::verify(std::time::Duration::from_secs(30))
                .await
                .map(|_| ())
        }),
        Command::VerifyLakehouseRegistry => Box::pin(lakehouse_registry_control::verify()),
        Command::VerifyR2Cleanup => Box::pin(async { r2_cleanup_verify::run() }),
        Command::ValidateTileDerivativeR2 => {
            Box::pin(async { tile_derivative_object_storage::validate_environment() })
        }
        Command::WaitTrinoReady => Box::pin(async { trino_ready_wait::run() }),
    };
    future.await
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
        Some("job-run") => Ok(Command::JobRun),
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
        Some("stop-building-register-floor-cycle") => Ok(Command::StopBuildingRegisterFloorCycle),
        Some("stop-gold-panel-rebuild") => Ok(Command::StopGoldPanelRebuild),
        Some("stop-lakehouse-maintenance") => Ok(Command::StopLakehouseMaintenance),
        Some("run-building-register-floor-cycle") => Ok(Command::RunBuildingRegisterFloorCycle),
        Some("run-silver-refresh") => Ok(Command::RunSilverRefresh),
        Some("stop-silver-refresh") => Ok(Command::StopSilverRefresh),
        Some("stop-parcel-tile-bake") => Ok(Command::StopParcelTileBake),
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
        Some("publish-administrative-boundary-postgis") => {
            Ok(Command::PublishAdministrativeBoundaryPostgis)
        }
        Some("publish-administrative-boundary-static-release") => {
            Ok(Command::PublishAdministrativeBoundaryStaticRelease)
        }
        Some("publish-industrial-complex-boundary-postgis") => {
            Ok(Command::PublishIndustrialComplexBoundaryPostgis)
        }
        Some("publish-industrial-complex-boundary-static-release") => {
            Ok(Command::PublishIndustrialComplexBoundaryStaticRelease)
        }
        Some("prove-industrial-complex-boundary-static-release-mutation-guards") => {
            Ok(Command::ProveIndustrialComplexBoundaryStaticReleaseMutationGuards)
        }
        Some("readdress-static-release") => Ok(Command::ReaddressStaticRelease),
        Some("export-map-edit-handoff") => Ok(Command::ExportMapEditHandoff),
        Some("bake-lakehouse-tiles") => Ok(Command::BakeLakehouseTiles),
        Some("publish-parcel-boundary-postgis") => Ok(Command::PublishParcelBoundaryPostgis),
        Some("publish-parcel-boundary-static-release") => {
            Ok(Command::PublishParcelBoundaryStaticRelease)
        }
        Some("load-unit-official-price-projection") => Ok(Command::LoadUnitOfficialPriceProjection),
        Some("load-lineage-review-items") => Ok(Command::LoadLineageReviewItems),
        Some("measure-bronze-object-members") => Ok(Command::MeasureBronzeObjectMembers),
        Some("clear-staging-namespace") => Ok(Command::ClearStagingNamespace),
        Some("export-lineage-steward-fold") => Ok(Command::ExportLineageStewardFold),
        Some("export-legal-dong-steward-decisions") => Ok(Command::ExportLegalDongStewardDecisions),
        Some("record-lineage-steward-folds") => Ok(Command::RecordLineageStewardFolds),
        Some("seal-parcel-publication-evidence") => Ok(Command::SealParcelPublicationEvidence),
        Some("write-parcel-publication-evidence") => Ok(Command::WriteParcelPublicationEvidence),
        Some("promote-administrative-boundary-runtime") => {
            Ok(Command::PromoteAdministrativeBoundaryRuntime)
        }
        Some("promote-industrial-complex-boundary-runtime") => {
            Ok(Command::PromoteIndustrialComplexBoundaryRuntime)
        }
        Some("promote-parcel-boundary-runtime") => Ok(Command::PromoteParcelBoundaryRuntime),
        Some("check-bounded-live-ingestion-gate") => Ok(Command::CheckBoundedLiveIngestionGate),
        Some("check-r2-runtime-target") => Ok(Command::CheckR2RuntimeTarget),
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
        Some("write-canonical-release-proof") => Ok(Command::WriteCanonicalReleaseProof),
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
        Some("probe-spatial-tile-wap") => Ok(Command::ProbeSpatialTileWap),
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
        Some("build-industrial-complex-address-resolution") => {
            Ok(Command::BuildIndustrialComplexAddressResolution)
        }
        Some("collect-code-go-kr-legal-dong") => Ok(Command::CollectCodeGoKrLegalDong),
        Some("collect-data-go-kr-onbid") => Ok(Command::CollectDataGoKrOnbid),
        Some("collect-rt-molit-real-transaction") => Ok(Command::CollectRtMolitRealTransaction),
        Some("collect-courtauction-go-kr") => Ok(Command::CollectCourtauctionGoKr),
        Some("collect-data-go-kr-registered-factory") => {
            Ok(Command::CollectDataGoKrRegisteredFactory)
        }
        Some("plan-silver-load-objects") => Ok(Command::PlanSilverLoadObjects),
        Some("collect-industrial-complex-address-source") => {
            Ok(Command::CollectIndustrialComplexAddressSource)
        }
        Some("export-industrial-complex-boundary-silver-handoff") => {
            Ok(Command::ExportIndustrialComplexBoundarySilverHandoff)
        }
        Some("export-industrial-complex-bronze-raw-jsonl") => {
            Ok(Command::ExportIndustrialComplexBronzeRawJsonl)
        }
        Some("export-industrial-complex-gold-profiles") => {
            Ok(Command::ExportIndustrialComplexGoldProfiles)
        }
        Some("export-industrial-complex-silver-handoff") => {
            Ok(Command::ExportIndustrialComplexSilverHandoff)
        }
        Some("export-building-register-floor-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterFloorSilverHandoff)
        }
        Some("stage-building-register-floor-inputs") => {
            Ok(Command::StageBuildingRegisterFloorInputs)
        }
        Some("select-building-register-floor-inputs") => {
            Ok(Command::SelectBuildingRegisterFloorInputs)
        }
        Some("export-building-register-unit-area-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterUnitAreaSilverHandoff)
        }
        Some("export-building-register-title-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterTitleSilverHandoff)
        }
        Some("export-building-register-unit-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterUnitSilverHandoff)
        }
        Some("export-building-by-pnu-serving") => Ok(Command::ExportBuildingByPnuServing),
        Some("publish-building-by-pnu-serving-manifest") => {
            Ok(Command::PublishBuildingByPnuServingManifest)
        }
        Some("export-parcel-by-pnu-serving") => Ok(Command::ExportParcelByPnuServing),
        Some("show-parcel-by-pnu-serving-state") => Ok(Command::ShowParcelByPnuServingState),
        Some("show-building-by-pnu-serving-state") => Ok(Command::ShowBuildingByPnuServingState),
        Some("verify-parcel-by-pnu-serving-rebase") => Ok(Command::VerifyParcelByPnuServingRebase),
        Some("export-parcel-marker-anchor-artifacts") => {
            Ok(Command::ExportParcelMarkerAnchorArtifacts)
        }
        Some("export-vworld-cadastral-silver-handoff") => {
            Ok(Command::ExportVWorldCadastralSilverHandoff)
        }
        Some("export-vworld-cadastral-shapefile-silver-handoff") => {
            Ok(Command::ExportVWorldCadastralShapefileSilverHandoff)
        }
        Some("export-land-use-plan-silver-handoff") => Ok(Command::ExportLandUsePlanSilverHandoff),
        Some("export-building-register-exclusive-unit-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterExclusiveUnitSilverHandoff)
        }
        Some("export-building-register-apartment-price-silver-handoff") => {
            Ok(Command::ExportBuildingRegisterApartmentPriceSilverHandoff)
        }
        Some("export-land-use-zone-code-silver-handoff") => {
            Ok(Command::ExportLandUseZoneCodeSilverHandoff)
        }
        Some("export-land-right-registration-silver-handoff") => {
            Ok(Command::ExportLandRightRegistrationSilverHandoff)
        }
        Some("export-land-transfer-history-silver-handoff") => {
            Ok(Command::ExportLandTransferHistorySilverHandoff)
        }
        Some("export-land-forest-silver-handoff") => Ok(Command::ExportLandForestSilverHandoff),
        Some("export-land-characteristic-silver-handoff") => {
            Ok(Command::ExportLandCharacteristicSilverHandoff)
        }
        Some("export-land-individual-price-silver-handoff") => {
            Ok(Command::ExportLandIndividualPriceSilverHandoff)
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
        Some("load-industrial-complex-canonical") => Ok(Command::LoadIndustrialComplexCanonical),
        Some("publish-industrial-complex-gold-pointer") => {
            Ok(Command::PublishIndustrialComplexGoldPointer)
        }
        Some("publish-parcel-by-pnu-serving-manifest") => {
            Ok(Command::PublishParcelByPnuServingManifest)
        }
        Some("publish-industrial-complex-gold-pointers") => {
            Ok(Command::PublishIndustrialComplexGoldPointers)
        }
        Some("publish-lakehouse-lineage-event") => Ok(Command::PublishLakehouseLineageEvent),
        Some("publish-outbox-once") => Ok(Command::PublishOutboxOnce),
        Some("r2-billing-usage-metrics") => Ok(Command::R2BillingUsageMetrics),
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
        Some("rebuild-postgis-parcel-boundary-mirror-national-from-contract") => {
            Ok(Command::RebuildPostgisParcelBoundaryMirrorNationalFromContract)
        }
        Some("abandon-ingestion-run") => Ok(Command::AbandonIngestionRun),
        Some("reconcile-building-register") => Ok(Command::ReconcileBuildingRegister),
        Some("record-lakehouse-bronze-run-evidence") => {
            Ok(Command::RecordLakehouseBronzeRunEvidence)
        }
        Some("requeue-dead-lettered-collection-job") => {
            Ok(Command::RequeueDeadLetteredCollectionJob)
        }
        Some("register-serving-source-lineage") => Ok(Command::RegisterServingSourceLineage),
        Some("inventory-lakehouse") => Ok(Command::InventoryLakehouse),
        Some("inventory-r2") => Ok(Command::InventoryR2),
        Some("migrate-r2-bronze-keys") => Ok(Command::MigrateR2BronzeKeys),
        Some("write-r2-bronze-key-cleanup-candidates") => {
            Ok(Command::WriteR2BronzeKeyCleanupCandidates)
        }
        Some("write-r2-bronze-key-migration-plan") => Ok(Command::WriteR2BronzeKeyMigrationPlan),
        Some("seed-lakehouse-registry") => Ok(Command::SeedLakehouseRegistry),
        Some("smoke-r2") => Ok(Command::SmokeR2),
        Some("smoke-vworld-cadastral") => Ok(Command::SmokeVWorldCadastral),
        Some("verify-static-release-toolchain") => Ok(Command::VerifyStaticReleaseToolchain),
        Some("verify-lakehouse-registry") => Ok(Command::VerifyLakehouseRegistry),
        Some("verify-r2-cleanup") => Ok(Command::VerifyR2Cleanup),
        Some("validate-tile-derivative-r2") => Ok(Command::ValidateTileDerivativeR2),
        Some("wait-trino-ready") => Ok(Command::WaitTrinoReady),
        Some(other) => building_by_pnu_serving_export::section_packs::PackCommand::parse(other)
            .map(Command::SectionPacks)
            .with_context(|| format!("unknown outbox-publisher command '{other}'")),
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
#[path = "main_runtime_tests.rs"]
mod tests;
