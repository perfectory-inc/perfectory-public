//! The Gold versions builds' and takeover sides' commands, apart from `main_command_tests`
//! (one file stays under the repository's line limit).

use crate::{parse_command, Command};

#[test]
fn build_versions_commands_are_explicit() -> anyhow::Result<()> {
    for (name, command) in [
        (
            "build-parcel-register-masters-versions",
            Command::BuildParcelRegisterMastersVersions,
        ),
        (
            "build-parcel-demolitions-versions",
            Command::BuildParcelDemolitionsVersions,
        ),
        (
            "build-parcel-energy-versions",
            Command::BuildParcelEnergyVersions,
        ),
        (
            "build-parcel-sub-parcels-versions",
            Command::BuildParcelSubParcelsVersions,
        ),
        (
            "build-parcel-building-permits-versions",
            Command::BuildParcelBuildingPermitsVersions,
        ),
        (
            "build-lineage-review-queue-versions",
            Command::BuildLineageReviewQueueVersions,
        ),
        (
            "build-parcel-ownership-versions",
            Command::BuildParcelOwnershipVersions,
        ),
        (
            "build-parcel-real-transaction-versions",
            Command::BuildParcelRealTransactionVersions,
        ),
        (
            "build-parcel-panel-versions",
            Command::BuildParcelPanelVersions,
        ),
        (
            "build-parcel-factories-versions",
            Command::BuildParcelFactoriesVersions,
        ),
        (
            "build-parcel-public-auction-versions",
            Command::BuildParcelPublicAuctionVersions,
        ),
        ("build-unit-price-versions", Command::BuildUnitPriceVersions),
        ("build-place-id-versions", Command::BuildPlaceIdVersions),
        (
            "build-unit-official-price-versions",
            Command::BuildUnitOfficialPriceVersions,
        ),
        (
            "export-unit-official-price-takeover-sides",
            Command::ExportUnitOfficialPriceTakeoverSides,
        ),
        (
            "build-parcel-court-auction-versions",
            Command::BuildParcelCourtAuctionVersions,
        ),
        (
            "export-parcel-boundary-handoff",
            Command::ExportParcelBoundaryHandoff,
        ),
        (
            "build-parcel-boundary-served-versions",
            Command::BuildParcelBoundaryServedVersions,
        ),
    ] {
        assert_eq!(
            parse_command(["foundation-outbox-publisher", name])?,
            command,
            "{name}"
        );
    }
    Ok(())
}

#[test]
fn build_parcel_nearby_versions_command_is_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "build-parcel-nearby-versions"
        ])?,
        Command::BuildParcelNearbyVersions
    );
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "run-parcel-matching-gate"])?,
        Command::RunParcelMatchingGate
    );
    assert_eq!(
        parse_command([
            "foundation-outbox-publisher",
            "export-gold-versions-change-set"
        ])?,
        Command::ExportGoldVersionsChangeSet
    );
    Ok(())
}

#[test]
fn gold_takeover_side_commands_are_explicit() -> anyhow::Result<()> {
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "export-gold-takeover-sides"])?,
        Command::ExportGoldTakeoverSides
    );
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "show-gold-takeover-switch"])?,
        Command::ShowGoldTakeoverSwitch
    );
    assert_eq!(
        parse_command(["foundation-outbox-publisher", "query-takeover-sides"])?,
        Command::QueryTakeoverSides
    );
    Ok(())
}
