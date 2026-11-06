use super::{parse_command, Command};

#[test]
fn floor_cycle_command_accepts_no_manual_selection_arguments() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["publisher", "stop-building-register-floor-cycle"])?,
        Command::StopBuildingRegisterFloorCycle
    );
    assert!(parse_command(["publisher", "stop-building-register-floor-cycle", "all"]).is_err());
    assert_eq!(
        parse_command(["publisher", "stop-gold-panel-rebuild"])?,
        Command::StopGoldPanelRebuild
    );
    assert!(parse_command(["publisher", "stop-gold-panel-rebuild", "parcel"]).is_err());
    assert_eq!(
        parse_command(["publisher", "stop-lakehouse-maintenance"])?,
        Command::StopLakehouseMaintenance
    );
    assert!(parse_command(["publisher", "stop-lakehouse-maintenance", "all"]).is_err());
    assert_eq!(
        parse_command(["publisher", "stop-unit-price-versions"])?,
        Command::StopUnitPriceVersions
    );
    assert!(parse_command(["publisher", "stop-unit-price-versions", "all"]).is_err());
    assert_eq!(
        parse_command(["publisher", "run-building-register-floor-cycle"])?,
        Command::RunBuildingRegisterFloorCycle
    );
    assert!(parse_command([
        "publisher",
        "run-building-register-floor-cycle",
        "other.zip"
    ])
    .is_err());
    Ok(())
}

#[test]
fn silver_refresh_takes_its_lane_from_the_environment_not_an_argument() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["publisher", "run-silver-refresh"])?,
        Command::RunSilverRefresh
    );
    assert!(parse_command([
        "publisher",
        "run-silver-refresh",
        "building-register-titles"
    ])
    .is_err());
    assert_eq!(
        parse_command(["publisher", "stop-silver-refresh"])?,
        Command::StopSilverRefresh
    );
    assert!(parse_command([
        "publisher",
        "stop-silver-refresh",
        "building-register-titles"
    ])
    .is_err());
    Ok(())
}

#[test]
fn the_parcel_tile_bake_cleanup_takes_no_argument() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["publisher", "stop-parcel-tile-bake"])?,
        Command::StopParcelTileBake
    );
    assert!(parse_command(["publisher", "stop-parcel-tile-bake", "parcels"]).is_err());
    Ok(())
}

#[test]
fn static_release_readdress_command_is_explicit_and_accepts_no_arguments() {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "readdress-static-release"]).unwrap(),
        Command::ReaddressStaticRelease
    );
    assert!(parse_command([
        "foundation-outbox-publisher",
        "readdress-static-release",
        "unexpected"
    ])
    .is_err());
}

#[test]
fn map_edit_handoff_export_command_is_explicit_and_accepts_no_arguments() {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "export-map-edit-handoff"]).unwrap(),
        Command::ExportMapEditHandoff
    );
    assert!(parse_command([
        "foundation-outbox-publisher",
        "export-map-edit-handoff",
        "unexpected"
    ])
    .is_err());
}

#[test]
fn lakehouse_tile_bake_command_is_explicit_and_accepts_no_arguments() {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "bake-lakehouse-tiles"]).unwrap(),
        Command::BakeLakehouseTiles
    );
    assert!(parse_command(["foundation-outbox-publisher", "bake-lakehouse-tiles", "x"]).is_err());
}

#[test]
fn unit_official_price_projection_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "load-unit-official-price-projection"
        ])?,
        Command::LoadUnitOfficialPriceProjection
    );
    Ok(())
}

#[test]
fn lineage_review_load_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "load-lineage-review-items"])?,
        Command::LoadLineageReviewItems
    );
    Ok(())
}

#[test]
fn bronze_object_member_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "measure-bronze-object-members"
        ])?,
        Command::MeasureBronzeObjectMembers
    );
    Ok(())
}

#[test]
fn staging_namespace_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "clear-staging-namespace"])?,
        Command::ClearStagingNamespace
    );
    Ok(())
}

#[test]
fn lineage_steward_fold_commands_are_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "export-lineage-steward-fold"])?,
        Command::ExportLineageStewardFold
    );
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-legal-dong-steward-decisions"
        ])?,
        Command::ExportLegalDongStewardDecisions
    );
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "record-lineage-steward-folds"
        ])?,
        Command::RecordLineageStewardFolds
    );
    Ok(())
}

#[test]
fn exclusive_unit_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-register-exclusive-unit-silver-handoff"
        ])?,
        Command::ExportBuildingRegisterExclusiveUnitSilverHandoff
    );
    Ok(())
}

#[test]
fn permit_basis_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-permit-basis-outline-silver-handoff"
        ])?,
        Command::ExportBuildingPermitBasisOutlineSilverHandoff
    );
    Ok(())
}

#[test]
fn register_master_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-register-master-silver-handoff"
        ])?,
        Command::ExportBuildingRegisterMasterSilverHandoff
    );
    Ok(())
}

#[test]
fn permit_demolition_loss_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-permit-demolition-loss-silver-handoff"
        ])?,
        Command::ExportBuildingPermitDemolitionLossSilverHandoff
    );
    Ok(())
}

#[test]
fn energy_monthly_streaming_export_commands_are_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-energy-monthly-electricity-silver-handoff"
        ])?,
        Command::ExportBuildingEnergyMonthlyElectricitySilverHandoff
    );
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-energy-monthly-gas-silver-handoff"
        ])?,
        Command::ExportBuildingEnergyMonthlyGasSilverHandoff
    );
    Ok(())
}

#[test]
fn register_sub_parcel_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-register-sub-parcel-silver-handoff"
        ])?,
        Command::ExportBuildingRegisterSubParcelSilverHandoff
    );
    Ok(())
}

#[test]
fn permit_parking_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-permit-parking-silver-handoff"
        ])?,
        Command::ExportBuildingPermitParkingSilverHandoff
    );
    Ok(())
}

#[test]
fn permit_annex_parking_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-permit-annex-parking-silver-handoff"
        ])?,
        Command::ExportBuildingPermitAnnexParkingSilverHandoff
    );
    Ok(())
}

#[test]
fn permit_building_outline_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-permit-building-outline-silver-handoff"
        ])?,
        Command::ExportBuildingPermitBuildingOutlineSilverHandoff
    );
    Ok(())
}

#[test]
fn apartment_price_streaming_export_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-register-apartment-price-silver-handoff"
        ])?,
        Command::ExportBuildingRegisterApartmentPriceSilverHandoff
    );
    Ok(())
}

#[test]
fn forest_ledger_commands_are_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-land-forest-silver-handoff"
        ])?,
        Command::ExportLandForestSilverHandoff
    );
    Ok(())
}

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
fn seal_parcel_publication_evidence_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "seal-parcel-publication-evidence",
        ])?,
        Command::SealParcelPublicationEvidence
    );
    Ok(())
}

#[test]
fn write_parcel_publication_evidence_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "write-parcel-publication-evidence",
        ])?,
        Command::WriteParcelPublicationEvidence
    );
    Ok(())
}

#[test]
fn inventory_lakehouse_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "inventory-lakehouse"])?,
        Command::InventoryLakehouse
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
fn run_remote_lakehouse_job_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "run-remote-lakehouse-job",])?,
        Command::RunRemoteLakehouseJob
    );
    Ok(())
}

#[test]
fn administrative_spatial_scope_registry_commands_are_explicit() -> anyhow::Result<()> {
    let cases = [
        (
            "write-administrative-spatial-scope-registry",
            Command::WriteAdministrativeSpatialScopeRegistry,
        ),
        (
            "write-official-administrative-boundary-source-snapshot",
            Command::WriteOfficialAdministrativeBoundarySourceSnapshot,
        ),
        (
            "publish-administrative-boundary-postgis",
            Command::PublishAdministrativeBoundaryPostgis,
        ),
        (
            "publish-industrial-complex-boundary-postgis",
            Command::PublishIndustrialComplexBoundaryPostgis,
        ),
        (
            "publish-parcel-boundary-postgis",
            Command::PublishParcelBoundaryPostgis,
        ),
        (
            "promote-administrative-boundary-runtime",
            Command::PromoteAdministrativeBoundaryRuntime,
        ),
        (
            "promote-industrial-complex-boundary-runtime",
            Command::PromoteIndustrialComplexBoundaryRuntime,
        ),
        (
            "rebuild-postgis-parcel-boundary-mirror",
            Command::RebuildPostgisParcelBoundaryMirror,
        ),
        (
            "evaluate-lakehouse-quality-rules",
            Command::EvaluateLakehouseQualityRules,
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
fn derivative_r2_preflight_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "validate-tile-derivative-r2",])?,
        Command::ValidateTileDerivativeR2
    );
    Ok(())
}

#[test]
fn static_release_toolchain_preflight_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "verify-static-release-toolchain",
        ])?,
        Command::VerifyStaticReleaseToolchain
    );
    Ok(())
}

#[test]
fn industrial_complex_static_release_publish_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "publish-industrial-complex-boundary-static-release",
        ])?,
        Command::PublishIndustrialComplexBoundaryStaticRelease
    );
    Ok(())
}

#[test]
fn administrative_static_release_publish_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "publish-administrative-boundary-static-release",
        ])?,
        Command::PublishAdministrativeBoundaryStaticRelease
    );
    Ok(())
}

#[test]
fn parcel_static_release_publish_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "publish-parcel-boundary-static-release",
        ])?,
        Command::PublishParcelBoundaryStaticRelease
    );
    Ok(())
}

#[test]
fn industrial_complex_static_release_mutation_guard_proof_command_is_explicit() -> anyhow::Result<()>
{
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "prove-industrial-complex-boundary-static-release-mutation-guards",
        ])?,
        Command::ProveIndustrialComplexBoundaryStaticReleaseMutationGuards
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
        (
            "ingest-building-hub-bulk-file",
            Command::IngestBuildingHubBulkFile,
        ),
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
        ("provider-rate-controller", Command::ProviderRateController),
        (
            "run-public-data-bronze-collection-lanes",
            Command::RunPublicDataBronzeCollectionLanes,
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
            "export-industrial-complex-silver-handoff",
            Command::ExportIndustrialComplexSilverHandoff,
        ),
        (
            "export-building-register-floor-silver-handoff",
            Command::ExportBuildingRegisterFloorSilverHandoff,
        ),
        (
            "stage-building-register-floor-inputs",
            Command::StageBuildingRegisterFloorInputs,
        ),
        (
            "export-vworld-cadastral-silver-handoff-shard",
            Command::ExportVWorldCadastralSilverHandoffShard,
        ),
        (
            "load-industrial-complex-canonical",
            Command::LoadIndustrialComplexCanonical,
        ),
        ("migrate-r2-bronze-keys", Command::MigrateR2BronzeKeys),
        (
            "publish-industrial-complex-gold-pointer",
            Command::PublishIndustrialComplexGoldPointer,
        ),
        ("publish-outbox-once", Command::PublishOutboxOnce),
        (
            "rebuild-postgis-parcel-boundary-mirror-national",
            Command::RebuildPostgisParcelBoundaryMirrorNational,
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

/// The producer of `gold/industrial-complex/profiles/{artifact_id}.json`. Its absence is what made
/// `publish-industrial-complex-gold-pointer` able to point at an object nobody had written.
#[test]
fn export_industrial_complex_gold_profiles_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-industrial-complex-gold-profiles",
        ])?,
        Command::ExportIndustrialComplexGoldProfiles
    );
    Ok(())
}

/// The producer of `serving/parcels/by-pnu/v{generation}/{pnu}.json` (root ADR-0096). Without an
/// explicit spelling the manifest publisher could point at a generation nobody had baked.
#[test]
fn export_parcel_by_pnu_serving_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-parcel-by-pnu-serving"
        ])?,
        Command::ExportParcelByPnuServing
    );
    Ok(())
}

/// The scheduled by-PNU bakes ask these before scanning anything (root ADR-0122).
#[test]
fn show_by_pnu_serving_state_commands_are_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "show-parcel-by-pnu-serving-state",
        ])?,
        Command::ShowParcelByPnuServingState
    );
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "show-building-by-pnu-serving-state",
        ])?,
        Command::ShowBuildingByPnuServingState
    );
    Ok(())
}

/// The operator's verified re-base of the parcel lane (root ADR-0146 §1).
#[test]
fn verify_parcel_by_pnu_serving_rebase_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "verify-parcel-by-pnu-serving-rebase",
        ])?,
        Command::VerifyParcelByPnuServingRebase
    );
    Ok(())
}

/// The pointer that pins the served generation (root ADR-0096); publishing it is a separate
/// command so a half-run export cannot leave the gateway aimed at a partial bake.
#[test]
fn publish_parcel_by_pnu_serving_manifest_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "publish-parcel-by-pnu-serving-manifest",
        ])?,
        Command::PublishParcelByPnuServingManifest
    );
    Ok(())
}

#[test]
fn export_industrial_complex_bronze_raw_jsonl_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-industrial-complex-bronze-raw-jsonl",
        ])?,
        Command::ExportIndustrialComplexBronzeRawJsonl
    );
    Ok(())
}

/// The reading half of the `silver.industrial_complex_boundaries` producer.
#[test]
fn export_industrial_complex_boundary_silver_handoff_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-industrial-complex-boundary-silver-handoff",
        ])?,
        Command::ExportIndustrialComplexBoundarySilverHandoff
    );
    Ok(())
}

/// The two halves of the industrial-complex address source: collect it into Bronze, then derive
/// the resolution the producer above refuses to run without.
#[test]
fn industrial_complex_address_source_commands_are_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "collect-industrial-complex-address-source",
        ])?,
        Command::CollectIndustrialComplexAddressSource
    );
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "build-industrial-complex-address-resolution",
        ])?,
        Command::BuildIndustrialComplexAddressResolution
    );
    Ok(())
}

/// The 법정동 code change source collector (root ADR-0143): one subcommand, two stages by env.
#[test]
fn code_go_kr_legal_dong_collect_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "collect-code-go-kr-legal-dong"
        ])?,
        Command::CollectCodeGoKrLegalDong
    );
    Ok(())
}

/// The Onbid public-auction collector (root ADR-0280): one subcommand, every due API in one run.
#[test]
fn data_go_kr_onbid_collect_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "collect-data-go-kr-onbid"])?,
        Command::CollectDataGoKrOnbid
    );
    Ok(())
}

#[test]
fn rt_molit_real_transaction_collect_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "collect-rt-molit-real-transaction"
        ])?,
        Command::CollectRtMolitRealTransaction
    );
    Ok(())
}

/// The data.go.kr file-data collector (root ADR-0280, ADR-0401).
#[test]
fn data_go_kr_file_data_collect_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "collect-data-go-kr-file-data"
        ])?,
        Command::CollectDataGoKrFileData
    );
    Ok(())
}

/// The Silver load plan: what a table still owes from its Bronze sources, read from R2 (ADR-0280).
#[test]
fn plan_silver_load_objects_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "plan-silver-load-objects"])?,
        Command::PlanSilverLoadObjects
    );
    Ok(())
}

/// The media reconciler's read-only listing of one lakehouse prefix (root ADR-0387).
#[test]
fn list_lakehouse_objects_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "list-lakehouse-objects"])?,
        Command::ListLakehouseObjects
    );
    Ok(())
}

/// The court auction list collector (root ADR-0280, ADR-0281).
#[test]
fn courtauction_go_kr_collect_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "collect-courtauction-go-kr"])?,
        Command::CollectCourtauctionGoKr
    );
    Ok(())
}

/// The producer of `serving/buildings/by-pnu/v{generation}/{pnu}.json` (root ADR-0100). Without an
/// explicit spelling the manifest publisher could point at a generation nobody had baked.
#[test]
fn export_building_by_pnu_serving_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-building-by-pnu-serving"
        ])?,
        Command::ExportBuildingByPnuServing
    );
    Ok(())
}

/// The pointer that pins the served generation (root ADR-0100); publishing it is a separate
/// command so a half-run export cannot leave the gateway aimed at a partial bake.
#[test]
fn publish_building_by_pnu_serving_manifest_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "publish-building-by-pnu-serving-manifest",
        ])?,
        Command::PublishBuildingByPnuServingManifest
    );
    Ok(())
}

#[test]
fn every_silver_refresh_lane_exports_through_a_publisher_command() -> anyhow::Result<()> {
    let exports = crate::remote_lakehouse_job::silver_refresh_export_commands()?;
    assert!(!exports.is_empty());
    for export in exports {
        // The export is a publisher command the release can start.
        assert!(
            parse_command(["publisher", export.as_str()]).is_ok(),
            "{export} is not a publisher command"
        );
    }
    Ok(())
}

#[test]
fn the_retired_catalog_loaders_are_not_commands() {
    // ADR-0194 §4: nothing writes catalog.parcel, catalog.building or catalog.building_unit.
    for retired in [
        "load-parcel-catalog-projection",
        "load-building-catalog-projection",
        "load-building-unit-catalog-projection",
        "load-building-unit-building-link",
    ] {
        assert!(parse_command(["publisher", retired]).is_err(), "{retired}");
    }
}
