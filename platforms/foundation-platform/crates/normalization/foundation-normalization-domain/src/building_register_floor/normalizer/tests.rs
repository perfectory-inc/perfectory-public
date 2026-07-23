use super::*;

fn normalize(
    floor_type_code: &'static str,
    floor_type_name: &'static str,
    floor_number: &'static str,
    floor_label: Option<&'static str>,
) -> NormalizedBuildingRegisterFloor {
    normalize_building_register_floor(RawBuildingRegisterFloor {
        floor_type_code,
        floor_type_name,
        floor_number,
        floor_label,
    })
}

#[test]
fn accepts_hangul_above_ground_floor_labels() {
    let normalized = normalize("20", "지상", "1", Some("일층"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, Some(1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지상 1층"));
}

#[test]
fn accepts_building_wing_prefix_above_ground_labels() {
    for label in ["A동1층", "가동1층", "1동 1층", "1층(증축)", "1층(A동)"] {
        let normalized = normalize("20", "지상", "1", Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(normalized.floor_number, Some(1), "label={label}");
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some("지상 1층"),
            "label={label}"
        );
    }
}

#[test]
fn strips_leading_building_wing_before_matching_the_floor() {
    // A6: A동/가동/1동 wing prefixes (bare, spaced, or wrapping the floor in
    // parentheses) are annex markers; the floor after them is matched.
    for (code, name, number, label, display) in [
        ("10", "지하", "1", "A동 지1", "지하 1층"),
        ("10", "지하", "1", "가동지1층", "지하 1층"),
        ("10", "지하", "1", "B동지하1층", "지하 1층"),
        ("20", "지상", "1", "A동1층", "지상 1층"),
        ("20", "지상", "2", "1동 2층", "지상 2층"),
        ("20", "지상", "1", "A동(1층)", "지상 1층"),
        ("20", "지상", "1", "B동(1층)", "지상 1층"),
        ("20", "지상", "1", "A동1", "지상 1층"),
    ] {
        let normalized = normalize(code, name, number, Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some(display),
            "label={label}"
        );
    }
}

#[test]
fn keeps_bare_building_wing_labels_without_a_floor_as_proposals() {
    // A동 / 가동 / 1동 with no floor part carry no floor identity to recover.
    for label in ["A동", "가동", "1동", "나동"] {
        let normalized = normalize("20", "지상", "1", Some(label));
        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::ProposalRequired,
            "label={label}"
        );
    }
}

#[test]
fn keeps_embedded_label_number_mismatch_as_proposal() {
    let normalized = normalize("20", "지상", "1", Some("B동2층"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::ProposalRequired
    );
    assert_eq!(
        normalized.reason,
        BuildingRegisterFloorNormalizationReason::LabelNumberMismatch
    );
}

#[test]
fn accepts_rooftop_shorthand_label() {
    let normalized = normalize("30", "옥탑", "1", Some("옥1"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.display_ko.as_deref(), Some("옥탑 1층"));
}

#[test]
fn accepts_generic_basement_label_with_provider_number() {
    let normalized = normalize("10", "지하", "1", Some("지하"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.floor_index, Some(-1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지하 1층"));
}

#[test]
fn accepts_label_number_when_provider_number_is_zero() {
    for (code, name, label, display, index) in [
        ("20", "지상", "1층", "지상 1층", Some(1)),
        ("10", "지하", "지하1층", "지하 1층", Some(-1)),
        ("30", "옥탑", "옥탑 1층", "옥탑 1층", None),
    ] {
        let normalized = normalize(code, name, "0", Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterFloorNormalizationReason::AcceptedLabelNumberUsingInvalidProviderNumber,
            "label={label}"
        );
        assert_eq!(normalized.floor_number, Some(1), "label={label}");
        assert_eq!(normalized.floor_index, index, "label={label}");
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some(display),
            "label={label}"
        );
    }
}

#[test]
fn accepts_short_basement_and_generic_above_ground_labels() {
    for (code, name, label, display, index) in [
        ("10", "지하", "지", "지하 1층", Some(-1)),
        ("20", "지상", "지상층", "지상 1층", Some(1)),
    ] {
        let normalized = normalize(code, name, "1", Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(normalized.floor_number, Some(1), "label={label}");
        assert_eq!(normalized.floor_index, index, "label={label}");
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some(display),
            "label={label}"
        );
    }
}

#[test]
fn accepts_common_korean_floor_suffix_typo() {
    let normalized = normalize("20", "지상", "1", Some("1충"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(normalized.floor_number, Some(1));
    assert_eq!(normalized.display_ko.as_deref(), Some("지상 1층"));
}

#[test]
fn accepts_basement_labels_with_parenthetical_annotations() {
    // A1: (부속)/(B동) annotations are dropped before floor matching.
    for (number, label, index, display) in [
        ("1", "지1(부속)", -1, "지하 1층"),
        ("1", "지층(B동)", -1, "지하 1층"),
        ("2", "지하2층(별관)", -2, "지하 2층"),
    ] {
        let normalized = normalize("10", "지하", number, Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(normalized.floor_index, Some(index), "label={label}");
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some(display),
            "label={label}"
        );
    }
}

#[test]
fn accepts_labels_with_leading_inner_annotation() {
    // A2: a leading 内 annotation is dropped (내지하2층 -> 지하2층).
    for (code, name, label, number, display) in [
        ("10", "지하", "내지하2층", "2", "지하 2층"),
        ("10", "지하", "내지하1층", "1", "지하 1층"),
        ("20", "지상", "내3층", "3", "지상 3층"),
    ] {
        let normalized = normalize(code, name, number, Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some(display),
            "label={label}"
        );
    }
}

#[test]
fn accepts_floor_suffix_typo_cheu() {
    // A5: 츠 -> 층 typo repair.
    let normalized = normalize("20", "지상", "3", Some("3츠"));

    assert_eq!(
        normalized.status,
        BuildingRegisterFloorNormalizationStatus::Accepted
    );
    assert_eq!(normalized.floor_number, Some(3));
    assert_eq!(normalized.display_ko.as_deref(), Some("지상 3층"));
}

#[test]
fn accepts_generic_rooftop_structure_labels_without_a_level() {
    // 옥탑0 / 탑 / 옥상 / 지붕층 denote the rooftop with no reliable level; the
    // rooftop is accepted with an unspecified level regardless of the provider
    // number quirk, never inventing a level from it.
    for (number, label) in [
        ("0", "옥탑0층"),
        ("1", "옥탑0층"),
        ("1", "옥탑0"),
        ("0", "탑"),
        ("0", "탑층"),
        ("0", "옥상"),
        ("0", "옥상층"),
        ("0", "지붕층"),
    ] {
        let normalized = normalize("30", "옥탑", number, Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label} number={number}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterFloorNormalizationReason::AcceptedRooftopStructure,
            "label={label} number={number}"
        );
        assert_eq!(normalized.floor_number, None, "label={label}");
        assert_eq!(normalized.floor_index, None, "label={label}");
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some("옥탑"),
            "label={label}"
        );
    }
}

#[test]
fn accepts_rooftop_structure_taking_level_from_the_label() {
    // The label's rooftop level is authoritative over a stray provider number
    // (옥탑1층 with provider 3 -> 옥탑 1층; 옥탑2층 with provider 1 -> 옥탑 2층).
    for (number, label, level, display) in [
        ("3", "옥탑1층", 1, "옥탑 1층"),
        ("1", "옥탑2층", 2, "옥탑 2층"),
        ("1", "옥탑 4층", 4, "옥탑 4층"),
    ] {
        let normalized = normalize("30", "옥탑", number, Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::Accepted,
            "label={label}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterFloorNormalizationReason::AcceptedRooftopStructure,
            "label={label}"
        );
        assert_eq!(normalized.floor_number, Some(level), "label={label}");
        assert_eq!(
            normalized.display_ko.as_deref(),
            Some(display),
            "label={label}"
        );
    }
}

#[test]
fn keeps_non_rooftop_top_floor_labels_as_proposals() {
    // 다락 (attic) / 계단 (stairwell) / bare numbers are not rooftop structures,
    // even when the provider coded the row as 옥탑; they stay proposals so the
    // vocabulary decision is not pre-empted.
    for (number, label) in [("0", "다락"), ("0", "다락층"), ("0", "계단"), ("0", "6층")] {
        let normalized = normalize("30", "옥탑", number, Some(label));

        assert_eq!(
            normalized.status,
            BuildingRegisterFloorNormalizationStatus::ProposalRequired,
            "label={label}"
        );
    }
}
