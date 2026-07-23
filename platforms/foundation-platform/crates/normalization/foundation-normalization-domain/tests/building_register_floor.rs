//! Building-register floor normalization examples captured from provider data.

use foundation_normalization_domain::{
    normalize_building_register_floor, BuildingRegisterFloorKind,
    BuildingRegisterFloorNormalizationReason, BuildingRegisterFloorNormalizationStatus,
    NormalizedBuildingRegisterFloor, RawBuildingRegisterFloor,
};

fn normalize(
    floor_type_code: &str,
    floor_type_name: &str,
    floor_number: &str,
    floor_label: Option<&str>,
) -> NormalizedBuildingRegisterFloor {
    normalize_building_register_floor(RawBuildingRegisterFloor {
        floor_type_code,
        floor_type_name,
        floor_number,
        floor_label,
    })
}

#[test]
fn normalizes_basement_shorthand_without_suffix() {
    let normalized = normalize("10", "지하", "1", Some("지1"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, Some(-1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지하 1층"));
    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedBasementShorthand
    );
}

#[test]
fn normalizes_basement_shorthand_with_suffix() {
    let normalized = normalize("10", "지하", "1", Some("지1층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, Some(-1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지하 1층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedBasementShorthand
    );
}

#[test]
fn normalizes_basement_full_label() {
    let normalized = normalize("10", "지하", "1", Some("지하1층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, Some(-1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지하 1층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedExactLabel
    );
}

#[test]
fn normalizes_basement_generic_label_using_floor_number() {
    let normalized = normalize("10", "지하", "2", Some("지층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor_number, Some(2));
    assert_eq!(normalized.floor_index, Some(-2));
    assert_eq!(normalized.display_ko.as_deref(), Some("지하 2층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedBasementGenericLabelWithNumber
    );
}

#[test]
fn normalizes_basement_generic_jiha_label_using_floor_number() {
    let normalized = normalize("10", "지하", "1", Some("지하층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, Some(-1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지하 1층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedBasementGenericLabelWithNumber
    );
}

#[test]
fn rejects_basement_label_that_looks_above_ground() {
    let normalized = normalize("10", "지하", "1", Some("1층"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::LabelKindMismatch
    );
}

#[test]
fn accepts_exclusive_unit_floor_without_label() {
    let normalized = normalize("20", "지상", "15", None);

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::AboveGround);
    assert_eq!(normalized.floor_number, Some(15));
    assert_eq!(normalized.floor_index, Some(15));
    assert_eq!(normalized.display_ko.as_deref(), Some("지상 15층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedLabelAbsentUsingTypeAndNumber
    );
}

#[test]
fn normalizes_above_ground_prefixed_label() {
    let normalized = normalize("20", "지상", "15", Some("지상15층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::AboveGround);
    assert_eq!(normalized.floor_number, Some(15));
    assert_eq!(normalized.floor_index, Some(15));
    assert_eq!(normalized.display_ko.as_deref(), Some("지상 15층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedExactLabel
    );
}

#[test]
fn normalizes_rooftop_generic_label_without_number() {
    let normalized = normalize("30", "옥탑", "0", Some("옥탑층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Rooftop);
    assert_eq!(normalized.floor_number, None);
    assert_eq!(normalized.floor_index, None);
    assert_eq!(normalized.display_ko.as_deref(), Some("옥탑"));
    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedExactLabel
    );
}

#[test]
fn normalizes_rooftop_numeric_label() {
    let normalized = normalize("30", "옥탑", "1", Some("옥탑1층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Rooftop);
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, None);
    assert_eq!(normalized.display_ko.as_deref(), Some("옥탑 1층"));
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedExactLabel
    );
}

#[test]
fn does_not_guess_special_provider_floor_numbers() {
    let normalized = normalize("30", "옥탑", "9001", Some("(내)1"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::Rooftop);
    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::SpecialProviderFloorNumber
    );
}

#[test]
fn normalizes_all_floors_as_special_kind() {
    let normalized = normalize("40", "각층", "0", Some("각층"));

    assert_eq!(normalized.kind, BuildingRegisterFloorKind::AllFloors);
    assert_eq!(normalized.floor_number, None);
    assert_eq!(normalized.floor_index, None);
    assert_eq!(normalized.display_ko.as_deref(), Some("각층"));
    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::AcceptedSpecialFloorKind
    );
}
