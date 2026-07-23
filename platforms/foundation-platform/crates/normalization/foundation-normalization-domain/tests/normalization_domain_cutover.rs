//! Ownership-level smoke test for the public deterministic Normalization API.

use foundation_normalization_domain::{
    normalize_building_register_floor, BuildingRegisterFloorKind,
    BuildingRegisterFloorNormalizationStatus, RawBuildingRegisterFloor, SemanticConceptId,
};

#[test]
fn deterministic_rules_are_owned_by_normalization_domain() {
    let normalized = normalize_building_register_floor(RawBuildingRegisterFloor {
        floor_type_code: "10",
        floor_type_name: "지하",
        floor_number: "1",
        floor_label: Some("지1"),
    });

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor_index, Some(-1));
    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(SemanticConceptId::FloorNumber.as_str(), "floor_number");
}
