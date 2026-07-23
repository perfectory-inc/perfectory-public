use super::*;

fn accepted(kind: BuildingRegisterFloorKind, number: u16) -> NormalizedBuildingRegisterFloor {
    let (display, index) = match kind {
        BuildingRegisterFloorKind::Basement => (
            format!("지하 {number}층"),
            i16::try_from(number).ok().map(|v| -v),
        ),
        _ => (format!("지상 {number}층"), i16::try_from(number).ok()),
    };
    NormalizedBuildingRegisterFloor {
        kind,
        floor_number: Some(number),
        floor_index: index,
        display_ko: Some(display),
        status: BuildingRegisterFloorNormalizationStatus::Accepted,
        reason: BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
    }
}

fn conflict(kind: BuildingRegisterFloorKind) -> NormalizedBuildingRegisterFloor {
    NormalizedBuildingRegisterFloor {
        kind,
        floor_number: None,
        floor_index: None,
        display_ko: None,
        status: BuildingRegisterFloorNormalizationStatus::ProposalRequired,
        reason: BuildingRegisterFloorNormalizationReason::LabelNumberMismatch,
    }
}

fn row(
    provider: Option<u16>,
    label: Option<u16>,
    per_row: NormalizedBuildingRegisterFloor,
) -> FloorRowEvidence {
    FloorRowEvidence {
        provider_number: provider,
        label_number: label,
        attic_candidate: false,
        per_row,
    }
}

fn attic_row() -> FloorRowEvidence {
    let mut per_row = conflict(BuildingRegisterFloorKind::AboveGround);
    per_row.reason = BuildingRegisterFloorNormalizationReason::LabelKindMismatch;
    FloorRowEvidence {
        provider_number: None,
        label_number: None,
        attic_candidate: true,
        per_row,
    }
}

#[test]
fn labels_win_when_provider_numbers_repeat_and_labels_form_clean_sequence() {
    // Provider num is a broken counter {1,1,1}; labels {1,2,3} are the real floors;
    // title count 3 confirms the labels. The unsettled 2층/3층 rows resolve.
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(1), Some(2), conflict(k)),
        row(Some(1), Some(3), conflict(k)),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(3),
            below_ground: None,
        },
    );

    assert_eq!(out[0], accepted(k, 1));
    for (index, expected_floor) in [(1, 2), (2, 3)] {
        assert_eq!(
            out[index].status,
            BuildingRegisterFloorNormalizationStatus::Accepted
        );
        assert_eq!(out[index].floor_number, Some(expected_floor));
        assert_eq!(
            out[index].reason,
            BuildingRegisterFloorNormalizationReason::ResolvedByBuildingWitnessMajority
        );
        assert_eq!(
            out[index].display_ko.as_deref(),
            Some(format!("지상 {expected_floor}층").as_str())
        );
    }
}

#[test]
fn resolves_even_without_a_title_count_when_one_source_is_clean() {
    // No 표제부 witness, but labels form clean 1..3 while provider nums do not.
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(1), Some(2), conflict(k)),
        row(Some(9), Some(3), conflict(k)),
    ];
    let out = resolve_building_floors(&rows, BuildingFloorCounts::default());

    assert_eq!(out[1].floor_number, Some(2));
    assert_eq!(out[2].floor_number, Some(3));
    assert_eq!(
        out[2].reason,
        BuildingRegisterFloorNormalizationReason::ResolvedByBuildingWitnessMajority
    );
}

#[test]
fn provider_numbers_win_over_title_when_provider_and_labels_agree() {
    // Owner's 21-vs-20 case: provider nums {1,2} and labels {1,2} agree and are
    // clean; the title count (3) is the odd witness out. The majority (num+label)
    // wins — but since num and label already agree, nothing was in conflict, so
    // there is nothing to change. Abstention here means "no false rewrite".
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(2), Some(2), accepted(k, 2)),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(3),
            below_ground: None,
        },
    );
    assert_eq!(out[0], accepted(k, 1));
    assert_eq!(out[1], accepted(k, 2));
}

#[test]
fn abstains_when_neither_source_is_clean() {
    // Provider {1,5} and labels {2,7} are both non-contiguous: cannot tell which
    // is right, so the conflict stays a proposal (no guessing).
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(2), conflict(k)),
        row(Some(5), Some(7), conflict(k)),
    ];
    let out = resolve_building_floors(&rows, BuildingFloorCounts::default());
    for result in &out {
        assert_eq!(
            result.status,
            BuildingRegisterFloorNormalizationStatus::ProposalRequired
        );
    }
}

#[test]
fn resolves_basement_with_negative_index() {
    let k = BuildingRegisterFloorKind::Basement;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(1), Some(2), conflict(k)),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: None,
            below_ground: Some(2),
        },
    );
    assert_eq!(out[1].floor_number, Some(2));
    assert_eq!(out[1].floor_index, Some(-2));
    assert_eq!(out[1].display_ko.as_deref(), Some("지하 2층"));
}

#[test]
fn places_attic_as_top_floor_when_title_is_one_above_numbered_floors() {
    // Numbered floors 1..3, 표제부 지상 4 -> the 다락 is the missing 4th floor.
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(2), Some(2), accepted(k, 2)),
        row(Some(3), Some(3), accepted(k, 3)),
        attic_row(),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(4),
            below_ground: None,
        },
    );
    assert_eq!(out[3].kind, BuildingRegisterFloorKind::AboveGround);
    assert_eq!(out[3].floor_number, Some(4));
    assert_eq!(out[3].display_ko.as_deref(), Some("지상 4층"));
    assert_eq!(
        out[3].reason,
        BuildingRegisterFloorNormalizationReason::ResolvedAtticAsTopFloor
    );
}

#[test]
fn places_attic_as_rooftop_when_numbered_floors_fill_the_title_count() {
    // Numbered floors 1..3 already equal 표제부 지상 3 -> the 다락 sits above them.
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(2), Some(2), accepted(k, 2)),
        row(Some(3), Some(3), accepted(k, 3)),
        attic_row(),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(3),
            below_ground: None,
        },
    );
    assert_eq!(out[3].kind, BuildingRegisterFloorKind::Rooftop);
    assert_eq!(out[3].display_ko.as_deref(), Some("옥탑"));
    assert_eq!(
        out[3].reason,
        BuildingRegisterFloorNormalizationReason::ResolvedAtticAsRooftop
    );
}

#[test]
fn abstains_on_attic_without_a_title_count_or_when_ambiguous() {
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![row(Some(1), Some(1), accepted(k, 1)), attic_row()];
    // No 표제부 witness: cannot place the attic.
    let out = resolve_building_floors(&rows, BuildingFloorCounts::default());
    assert_eq!(
        out[1].status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
    // Title count leaves a two-floor gap: still ambiguous.
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(3),
            below_ground: None,
        },
    );
    assert_eq!(
        out[1].status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
}

#[test]
fn does_not_place_multiple_attics() {
    // Two 다락 rows cannot be distinguished by the single title witness.
    let k = BuildingRegisterFloorKind::AboveGround;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        attic_row(),
        attic_row(),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(2),
            below_ground: None,
        },
    );
    assert_eq!(
        out[1].status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
    assert_eq!(
        out[2].status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
}

#[test]
fn leaves_non_conflict_proposals_untouched() {
    // A kind-mismatch proposal is not a num-vs-label conflict; this pass must
    // not touch it even when a sibling conflict resolves.
    let k = BuildingRegisterFloorKind::AboveGround;
    let mut kind_mismatch = conflict(k);
    kind_mismatch.reason = BuildingRegisterFloorNormalizationReason::LabelKindMismatch;
    let rows = vec![
        row(Some(1), Some(1), accepted(k, 1)),
        row(Some(1), Some(2), conflict(k)),
        row(None, None, kind_mismatch.clone()),
    ];
    let out = resolve_building_floors(
        &rows,
        BuildingFloorCounts {
            above_ground: Some(2),
            below_ground: None,
        },
    );
    assert_eq!(out[1].floor_number, Some(2)); // conflict resolved
    assert_eq!(out[2], kind_mismatch); // kind-mismatch untouched
}
