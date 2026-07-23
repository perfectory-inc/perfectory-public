use super::{command_requires_expanded_stack, parse_command, Command};

#[test]
fn default_command_runs_publisher() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher"])?,
        Command::RunPublisher
    );
    Ok(())
}

#[test]
fn smoke_r2_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "smoke-r2"])?,
        Command::SmokeR2
    );
    Ok(())
}

#[test]
fn inventory_r2_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "inventory-r2"])?,
        Command::InventoryR2
    );
    Ok(())
}

#[test]
fn plan_provider_acquisition_jobs_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "plan-provider-acquisition-jobs",
        ])?,
        Command::PlanProviderAcquisitionJobs
    );
    Ok(())
}

#[test]
fn import_provider_acquisition_landing_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "import-provider-acquisition-landing",
        ])?,
        Command::ImportProviderAcquisitionLanding
    );
    Ok(())
}

#[test]
fn seed_lakehouse_registry_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "seed-lakehouse-registry"])?,
        Command::SeedLakehouseRegistry
    );
    Ok(())
}

#[test]
fn verify_lakehouse_registry_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "verify-lakehouse-registry"])?,
        Command::VerifyLakehouseRegistry
    );
    Ok(())
}

#[test]
fn record_lakehouse_bronze_run_evidence_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "record-lakehouse-bronze-run-evidence",
        ])?,
        Command::RecordLakehouseBronzeRunEvidence
    );
    Ok(())
}

#[test]
fn run_remote_lakehouse_job_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "run-remote-lakehouse-job",])?,
        Command::RunRemoteLakehouseJob
    );
    Ok(())
}

#[test]
fn run_national_data_collection_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "run-national-data-collection"
        ])?,
        Command::RunNationalDataCollection
    );
    Ok(())
}

#[test]
fn rt_molit_real_transaction_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "ingest-rt-molit-real-transaction-export"
        ])?,
        Command::IngestRtMolitRealTransactionExport
    );
    Ok(())
}

#[test]
fn plan_rt_molit_real_transaction_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "plan-rt-molit-real-transaction-exports"
        ])?,
        Command::PlanRtMolitRealTransactionExports
    );
    Ok(())
}

#[test]
fn execute_rt_molit_real_transaction_export_plan_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "execute-rt-molit-real-transaction-export-plan"
        ])?,
        Command::ExecuteRtMolitRealTransactionExportPlan
    );
    Ok(())
}

#[test]
fn write_national_data_collection_scope_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-national-data-collection-scope"
        ])?,
        Command::WriteNationalDataCollectionScope
    );
    Ok(())
}

#[test]
fn administrative_spatial_scope_registry_commands_are_explicit() -> anyhow::Result<()> {
    let cases = [
        (
            "check-administrative-spatial-scope-registry",
            Command::CheckAdministrativeSpatialScopeRegistry,
        ),
        (
            "write-administrative-spatial-scope-registry",
            Command::WriteAdministrativeSpatialScopeRegistry,
        ),
        (
            "write-official-administrative-boundary-source-snapshot",
            Command::WriteOfficialAdministrativeBoundarySourceSnapshot,
        ),
        (
            "check-bounded-live-ingestion-gate",
            Command::CheckBoundedLiveIngestionGate,
        ),
        (
            "check-postgis-anchor-pbf-regional-proof",
            Command::CheckPostgisAnchorPbfRegionalProof,
        ),
        (
            "check-regional-data-serving-load",
            Command::CheckRegionalDataServingLoad,
        ),
        (
            "rebuild-postgis-parcel-boundary-mirror",
            Command::RebuildPostgisParcelBoundaryMirror,
        ),
        (
            "write-postgis-mirror-dlq-cutover-evidence",
            Command::WritePostgisMirrorDlqCutoverEvidence,
        ),
        (
            "evaluate-lakehouse-quality-rules",
            Command::EvaluateLakehouseQualityRules,
        ),
        (
            "run-building-register-local-bronze-proof",
            Command::RunBuildingRegisterLocalBronzeProof,
        ),
    ];
    for (raw, expected) in cases {
        assert_eq!(
            parse_command(["foundation-outbox-publisher", raw])?,
            expected
        );
    }
    Ok(())
}

#[test]
fn write_national_bronze_object_manifest_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-national-bronze-object-manifest"
        ])?,
        Command::WriteNationalBronzeObjectManifest
    );
    Ok(())
}

#[test]
fn check_national_bronze_object_manifest_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "check-national-bronze-object-manifest"
        ])?,
        Command::CheckNationalBronzeObjectManifest
    );
    Ok(())
}

#[test]
fn check_silver_gold_national_promotion_plan_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "check-silver-gold-national-promotion-plan"
        ])?,
        Command::CheckSilverGoldNationalPromotionPlan
    );
    Ok(())
}

#[test]
fn write_silver_gold_national_promotion_plan_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-silver-gold-national-promotion-plan"
        ])?,
        Command::WriteSilverGoldNationalPromotionPlan
    );
    Ok(())
}

#[test]
fn write_canonical_silver_gold_cutover_evidence_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-canonical-silver-gold-cutover-evidence"
        ])?,
        Command::WriteCanonicalSilverGoldCutoverEvidence
    );
    Ok(())
}

#[test]
fn write_r2_bronze_key_migration_plan_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-r2-bronze-key-migration-plan"
        ])?,
        Command::WriteR2BronzeKeyMigrationPlan
    );
    Ok(())
}

#[test]
fn write_r2_bronze_key_cleanup_candidates_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-r2-bronze-key-cleanup-candidates"
        ])?,
        Command::WriteR2BronzeKeyCleanupCandidates
    );
    Ok(())
}

#[test]
fn verify_r2_cleanup_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "verify-r2-cleanup"])?,
        Command::VerifyR2Cleanup
    );
    Ok(())
}

#[test]
fn delete_r2_candidates_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "delete-r2-candidates"])?,
        Command::DeleteR2Candidates
    );
    Ok(())
}

#[test]
fn audit_r2_inventory_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "audit-r2-inventory"])?,
        Command::AuditR2Inventory
    );
    Ok(())
}

#[test]
fn collect_r2_billing_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "collect-r2-billing-export"])?,
        Command::CollectR2BillingExport
    );
    Ok(())
}

#[test]
fn r2_billing_usage_metrics_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "r2-billing-usage-metrics"])?,
        Command::R2BillingUsageMetrics
    );
    Ok(())
}

#[test]
fn public_api_metric_writer_commands_are_explicit() -> anyhow::Result<()> {
    let cases = [
        (
            "write-public-api-quota-metric",
            Command::WritePublicApiQuotaMetric,
        ),
        (
            "write-public-api-dependency-metric",
            Command::WritePublicApiDependencyMetric,
        ),
    ];
    for (raw, expected) in cases {
        assert_eq!(
            parse_command(["foundation-outbox-publisher", raw])?,
            expected
        );
    }
    Ok(())
}

#[test]
fn wait_trino_ready_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "wait-trino-ready"])?,
        Command::WaitTrinoReady
    );
    Ok(())
}

#[test]
fn publish_lakehouse_lineage_event_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "publish-lakehouse-lineage-event"
        ])?,
        Command::PublishLakehouseLineageEvent
    );
    Ok(())
}

#[test]
fn check_industrial_complex_canonical_source_readiness_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "check-industrial-complex-canonical-source-readiness"
        ])?,
        Command::CheckIndustrialComplexCanonicalSourceReadiness
    );
    Ok(())
}

#[test]
fn check_silver_gold_national_promotion_execution_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "check-silver-gold-national-promotion-execution"
        ])?,
        Command::CheckSilverGoldNationalPromotionExecution
    );
    Ok(())
}

#[test]
fn execute_silver_gold_national_promotion_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "execute-silver-gold-national-promotion"
        ])?,
        Command::ExecuteSilverGoldNationalPromotion
    );
    Ok(())
}

#[test]
fn write_national_data_collection_rollout_approval_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-national-data-collection-rollout-approval"
        ])?,
        Command::WriteNationalDataCollectionRolloutApproval
    );
    Ok(())
}

#[test]
fn resume_national_data_collection_ledger_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "resume-national-data-collection-ledger"
        ])?,
        Command::ResumeNationalDataCollectionLedger
    );
    Ok(())
}

#[test]
fn write_national_data_collection_shard_manifest_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-national-data-collection-shard-manifest"
        ])?,
        Command::WriteNationalDataCollectionShardManifest
    );
    Ok(())
}

#[test]
fn execute_national_data_collection_ledger_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "execute-national-data-collection-ledger"
        ])?,
        Command::ExecuteNationalDataCollectionLedger
    );
    Ok(())
}

#[test]
fn write_building_register_page_count_plan_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-building-register-page-count-plan"
        ])?,
        Command::WriteBuildingRegisterPageCountPlan
    );
    Ok(())
}

#[test]
fn write_national_page_count_plan_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-national-page-count-plan"
        ])?,
        Command::WriteNationalPageCountPlan
    );
    Ok(())
}

#[test]
fn provider_rate_controller_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "provider-rate-controller"])?,
        Command::ProviderRateController
    );
    Ok(())
}

#[test]
fn unknown_command_is_rejected() -> anyhow::Result<()> {
    let error = match parse_command(["foundation-outbox-publisher", "publish-now"]) {
        Ok(command) => anyhow::bail!("expected parse failure, got {command:?}"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("unknown outbox-publisher command"));
    Ok(())
}

#[test]
fn remaining_commands_are_explicit() -> anyhow::Result<()> {
    let cases = [
        ("ingest-building-register", Command::IngestBuildingRegister),
        (
            "ingest-building-hub-bulk-file",
            Command::IngestBuildingHubBulkFile,
        ),
        ("building-register-smoke", Command::BuildingRegisterSmoke),
        (
            "ingest-building-hub-bulk-collection",
            Command::IngestBuildingHubBulkCollection,
        ),
        (
            "plan-building-hub-bulk-collection",
            Command::PlanBuildingHubBulkCollection,
        ),
        (
            "plan-vworld-dataset-collection",
            Command::PlanVWorldDatasetCollection,
        ),
        (
            "compile-national-data-collection-plan",
            Command::CompileNationalDataCollectionPlan,
        ),
        (
            "check-national-data-collection-coverage-ledger",
            Command::CheckNationalDataCollectionCoverageLedger,
        ),
        (
            "check-national-data-collection-rollout-approval",
            Command::CheckNationalDataCollectionRolloutApproval,
        ),
        ("provider-rate-controller", Command::ProviderRateController),
        (
            "run-public-data-bronze-collection-lanes",
            Command::RunPublicDataBronzeCollectionLanes,
        ),
        (
            "dispatch-github-cutover-workflow",
            Command::DispatchGithubCutoverWorkflow,
        ),
        (
            "fetch-github-cutover-artifact",
            Command::FetchGithubCutoverArtifact,
        ),
        (
            "configure-github-actions-secrets",
            Command::ConfigureGitHubActionsSecrets,
        ),
        (
            "inventory-vworld-dataset-files",
            Command::InventoryVWorldDatasetFiles,
        ),
        (
            "ingest-vworld-dataset-files",
            Command::IngestVWorldDatasetFiles,
        ),
        (
            "execute-national-data-collection-async",
            Command::ExecuteNationalDataCollectionAsync,
        ),
        ("ingest-real-transaction", Command::IngestRealTransaction),
        (
            "probe-building-register-page-count",
            Command::ProbeBuildingRegisterPageCount,
        ),
        (
            "probe-building-register-page-count-batch",
            Command::ProbeBuildingRegisterPageCountBatch,
        ),
        (
            "probe-vworld-page-count-batch",
            Command::ProbeVWorldPageCountBatch,
        ),
        (
            "ingest-vworld-land-register",
            Command::IngestVWorldLandRegister,
        ),
        ("ingest-vworld-cadastral", Command::IngestVWorldCadastral),
        (
            "ingest-vworld-ned-attribute",
            Command::IngestVWorldNedAttribute,
        ),
        ("smoke-vworld-cadastral", Command::SmokeVWorldCadastral),
        (
            "reconcile-building-register",
            Command::ReconcileBuildingRegister,
        ),
        (
            "export-industrial-complex-silver-handoff",
            Command::ExportIndustrialComplexSilverHandoff,
        ),
        (
            "export-building-register-floor-silver-handoff",
            Command::ExportBuildingRegisterFloorSilverHandoff,
        ),
        (
            "export-parcel-marker-anchor-artifacts",
            Command::ExportParcelMarkerAnchorArtifacts,
        ),
        (
            "build-parcel-marker-anchor-pbf-artifacts",
            Command::BuildParcelMarkerAnchorPbfArtifacts,
        ),
        (
            "build-parcel-marker-anchor-aggregate-pbf-artifacts",
            Command::BuildParcelMarkerAnchorAggregatePbfArtifacts,
        ),
        (
            "promote-parcel-marker-anchor-pbf-manifest",
            Command::PromoteParcelMarkerAnchorPbfManifest,
        ),
        (
            "promote-parcel-marker-anchor-runtime-manifest",
            Command::PromoteParcelMarkerAnchorRuntimeManifest,
        ),
        (
            "export-vworld-cadastral-silver-handoff",
            Command::ExportVWorldCadastralSilverHandoff,
        ),
        (
            "export-vworld-cadastral-silver-handoff-shard",
            Command::ExportVWorldCadastralSilverHandoffShard,
        ),
        (
            "import-industrial-complex-catalog-seed",
            Command::ImportIndustrialComplexCatalogSeed,
        ),
        ("migrate-r2-bronze-keys", Command::MigrateR2BronzeKeys),
        (
            "publish-industrial-complex-gold-pointer",
            Command::PublishIndustrialComplexGoldPointer,
        ),
        ("publish-outbox-once", Command::PublishOutboxOnce),
        (
            "rebuild-parcel-marker-anchors",
            Command::RebuildParcelMarkerAnchors,
        ),
        (
            "rebuild-postgis-parcel-boundary-mirror-national",
            Command::RebuildPostgisParcelBoundaryMirrorNational,
        ),
        (
            "rebuild-parcel-marker-anchors-streaming",
            Command::RebuildParcelMarkerAnchorsStreaming,
        ),
    ];

    for (command_name, expected) in cases {
        assert_eq!(
            parse_command(["foundation-outbox-publisher", command_name])?,
            expected,
            "command {command_name} parsed unexpectedly"
        );
    }
    Ok(())
}

#[test]
fn artifact_batch_commands_run_with_expanded_stack() {
    assert!(command_requires_expanded_stack(
        Command::ExportParcelMarkerAnchorArtifacts
    ));
    assert!(command_requires_expanded_stack(
        Command::BuildParcelMarkerAnchorPbfArtifacts
    ));
    assert!(command_requires_expanded_stack(
        Command::BuildParcelMarkerAnchorAggregatePbfArtifacts
    ));
    assert!(command_requires_expanded_stack(
        Command::PromoteParcelMarkerAnchorRuntimeManifest
    ));
}
