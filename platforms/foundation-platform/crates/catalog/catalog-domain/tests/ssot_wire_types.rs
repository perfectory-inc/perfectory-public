//! Contract tests for Catalog SSOT wire values.

use catalog_domain::{
    BlueprintKind, DigitalTwinAssetKind, IndustryAssignmentKind, SpatialLayerKind,
};

#[test]
fn catalog_ssot_wire_enums_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(BlueprintKind::MasterPlan.wire_name(), "master_plan");
    assert_eq!(
        BlueprintKind::from_wire("master_plan")?,
        BlueprintKind::MasterPlan
    );

    assert_eq!(
        SpatialLayerKind::ParcelBoundary.wire_name(),
        "parcel_boundary"
    );
    assert_eq!(
        SpatialLayerKind::from_wire("parcel_boundary")?,
        SpatialLayerKind::ParcelBoundary
    );

    assert_eq!(DigitalTwinAssetKind::Tileset3d.wire_name(), "tileset_3d");
    assert_eq!(
        DigitalTwinAssetKind::from_wire("tileset_3d")?,
        DigitalTwinAssetKind::Tileset3d
    );

    assert_eq!(IndustryAssignmentKind::Restricted.wire_name(), "restricted");
    assert_eq!(
        IndustryAssignmentKind::from_wire("restricted")?,
        IndustryAssignmentKind::Restricted
    );

    assert!(BlueprintKind::from_wire("legacy_blueprint").is_err());
    assert!(SpatialLayerKind::from_wire("legacy_layer").is_err());
    assert!(DigitalTwinAssetKind::from_wire("legacy_asset").is_err());
    assert!(IndustryAssignmentKind::from_wire("legacy_assignment").is_err());
    Ok(())
}
