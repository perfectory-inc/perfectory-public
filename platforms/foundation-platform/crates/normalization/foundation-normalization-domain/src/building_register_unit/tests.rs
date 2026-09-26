use super::*;
use crate::building_register_floor::BuildingRegisterFloorKind;

fn raw<'a>(
    dong: &'a str,
    unit_name: &'a str,
    floor: RawBuildingRegisterFloor<'a>,
) -> RawBuildingRegisterUnit<'a> {
    RawBuildingRegisterUnit {
        dong_name: dong,
        unit_name,
        floor,
    }
}

fn above_ground(number: &'static str) -> RawBuildingRegisterFloor<'static> {
    RawBuildingRegisterFloor {
        floor_type_code: "20",
        floor_type_name: "지상",
        floor_number: number,
        floor_label: None,
    }
}

fn basement(number: &'static str) -> RawBuildingRegisterFloor<'static> {
    RawBuildingRegisterFloor {
        floor_type_code: "10",
        floor_type_name: "지하",
        floor_number: number,
        floor_label: None,
    }
}

#[test]
fn extracts_unit_number_from_common_shapes() {
    for (name, expected) in [
        ("624호", 624),
        ("2621", 2621),
        ("6층 630호", 630),
        ("2-026호", 26),
        ("1-087호", 87),
        ("H705", 705),
        ("오1604", 1604),
        ("비01호", 1),
        ("지층7호", 7),
        ("A동 3905호", 3905),
        ("102(교습소)", 102),
    ] {
        let normalized = normalize_building_register_unit(raw("102동", name, above_ground("6")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, Some(expected), "name={name}");
    }
}

#[test]
fn extracts_unit_number_from_floor_scoped_six_digit_codes() {
    for (floor_number, name, expected) in [("1", "101107", 107), ("2", "201202", 202)] {
        let normalized = normalize_building_register_unit(raw(
            "main-building-1",
            name,
            above_ground(floor_number),
        ));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, Some(expected), "name={name}");
    }
}

#[test]
fn rejects_six_digit_codes_that_do_not_match_the_floor_context() {
    for (floor_number, name) in [("2", "301202"), ("1", "101000")] {
        let normalized = normalize_building_register_unit(raw(
            "main-building-1",
            name,
            above_ground(floor_number),
        ));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
    }
}

#[test]
fn keeps_unit_name_without_a_number_as_proposal() {
    for name in ["나형지층", "지층", ""] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("1")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
    }
}

#[test]
fn preserves_explicit_non_numeric_unit_labels() {
    for (name, expected_label) in [("가호", "가호"), ("나호", "나호"), ("A호", "A호")] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("1")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
        assert_eq!(normalized.unit_label_ko.as_deref(), Some(expected_label));
        assert_eq!(
            normalized.reason,
            BuildingRegisterUnitReason::AcceptedUnitLabel,
            "name={name}"
        );
    }
}

#[test]
fn rejects_generic_or_location_labels_as_unit_labels() {
    for name in ["호", "지층", "지하층", ".", "상가"] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("1")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(normalized.unit_label_ko, None, "name={name}");
    }
}

#[test]
fn extracts_floor_prefixed_labels_when_floor_fields_agree() {
    for (name, floor, expected) in [
        ("지층가호", basement("1"), "가호"),
        ("지하나호", basement("1"), "나호"),
        ("지층 가", basement("2"), "가호"),
        ("지하층 나호", basement("1"), "나호"),
        ("지층-나호", basement("1"), "나호"),
        ("일층나", above_ground("1"), "나호"),
        ("이층가호", above_ground("2"), "가호"),
        ("지층에이호", basement("1"), "A호"),
        ("지층비호", basement("1"), "B호"),
        ("지층B", basement("1"), "B호"),
        ("지층 A호", basement("1"), "A호"),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, floor));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
        assert_eq!(
            normalized.unit_label_ko.as_deref(),
            Some(expected),
            "name={name}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterUnitReason::AcceptedUnitLabel,
            "name={name}"
        );
    }
}

#[test]
fn keeps_floor_prefixed_labels_with_disagreeing_floor_fields_as_proposal() {
    for (name, floor) in [
        ("지층가호", above_ground("1")),
        ("일층가호", above_ground("2")),
        ("일층가호", above_ground("")),
        ("이층나호", basement("1")),
        ("옥탑가호", above_ground("3")),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, floor));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(normalized.unit_label_ko, None, "name={name}");
    }
}

#[test]
fn extracts_je_prefixed_and_bare_expanded_labels() {
    for (name, expected) in [
        ("제가호", "가호"),
        ("제나호", "나호"),
        ("제A호", "A호"),
        ("가", "가호"),
        ("나", "나호"),
        ("B", "B호"),
        ("b", "B호"),
        ("에이호", "A호"),
        ("에이", "A호"),
        ("비", "B호"),
        ("씨", "C호"),
        ("에프호", "F호"),
        ("에이치호", "H호"),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("1")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(
            normalized.unit_label_ko.as_deref(),
            Some(expected),
            "name={name}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterUnitReason::AcceptedUnitLabel,
            "name={name}"
        );
    }
}

#[test]
fn preserves_sedae_suffix_labels_without_merging_into_ho() {
    for (name, expected) in [
        ("가세대", "가세대"),
        ("나세대", "나세대"),
        ("에이세대", "A세대"),
        ("B세대", "B세대"),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("2")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(
            normalized.unit_label_ko.as_deref(),
            Some(expected),
            "name={name}"
        );
    }
}

#[test]
fn extracts_first_run_for_paren_floor_annotated_names() {
    // `N(M층)` = 호수 N + 층 괄호 표기. 마지막 숫자런(층)을 호수로 오추출하면
    // 같은 층 전체가 한 번호로 붕괴하므로 첫 숫자런이 단위 식별자다.
    for (name, floor, expected) in [
        ("15(2층)", above_ground("2"), 15),
        ("105(1층)", above_ground("1"), 105),
        ("7(지하1층)", basement("1"), 7),
        ("44(2층)", above_ground("3"), 44),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, floor));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, Some(expected), "name={name}");
    }
}

#[test]
fn derives_whitespace_compact_unit_designation() {
    for (name, expected) in [
        ("D07-01호", Some("D07-01호")),
        ("2층 제 3호", Some("2층제3호")),
        ("가149호-1", Some("가149호-1")),
        ("624호", Some("624호")),
        ("아파트 501", Some("아파트501")),
        ("가", Some("가")),
        ("15(2층)", Some("15(2층)")),
        ("", None),
        ("   ", None),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("1")));
        assert_eq!(
            normalized.unit_designation.as_deref(),
            expected,
            "name={name}"
        );
    }
}

#[test]
fn unit_designation_helper_matches_normalizer_designation() {
    for (name, expected) in [
        ("1층 102호", Some("1층102호")),
        ("D07-01호", Some("D07-01호")),
        ("아파트 501", Some("아파트501")),
        ("", None),
        ("   ", None),
    ] {
        assert_eq!(
            building_register_unit_designation(name).as_deref(),
            expected,
            "name={name}"
        );
        let normalized = normalize_building_register_unit(raw("", name, above_ground("1")));
        assert_eq!(
            normalized.unit_designation,
            building_register_unit_designation(name),
            "name={name}"
        );
    }
}

#[test]
fn rejects_ambiguous_or_non_label_shapes_in_expanded_path() {
    // 이(2/E 중의성)·지(지하 충돌)·한글숫자 호·I/O/l(1/0 오입력)·형/동 접미
    // ·미지 접두(특)·역순 표기는 전부 proposal로 남는다.
    for (name, floor) in [
        ("이", above_ground("1")),
        ("지", above_ground("1")),
        ("제이호", above_ground("1")),
        ("일층일호", above_ground("1")),
        ("이층이호", above_ground("2")),
        ("I", above_ground("1")),
        ("O", above_ground("1")),
        ("l", above_ground("1")),
        ("지층비이호", basement("1")),
        ("특에이호", basement("1")),
        ("비호지층", basement("1")),
        ("A형", above_ground("1")),
        ("가동", above_ground("1")),
        ("지하호", basement("1")),
        ("전층", above_ground("1")),
        ("세대", above_ground("1")),
        ("에이바이", above_ground("1")),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, floor));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(normalized.unit_label_ko, None, "name={name}");
    }
}

#[test]
fn basement_unit_carries_negative_floor_not_a_unit_marker() {
    // 7호 on 지하 1층 -> unit 7, floor -1 (basement lives in the floor, not the 호).
    let normalized = normalize_building_register_unit(raw("", "7호", basement("1")));
    assert_eq!(normalized.unit_number, Some(7));
    assert_eq!(normalized.floor.kind, BuildingRegisterFloorKind::Basement);
    assert_eq!(normalized.floor.floor_index, Some(-1));
}

#[test]
fn carries_the_conservative_normalized_designation() {
    // ADR-0107 §1: 정규형 지정자는 행에서 파생된다. 값의 정본은 골든 벡터
    // (unit_designation_normalization 시험)이고, 여기서는 배선만 확인한다.
    for (dong, name, expected) in [
        ("102동", "6층 630호", Some("630")),
        ("", "1층 6-2호", Some("6-2")),
        ("", "지층비01호", Some("BB1")),
        ("에이동", "에이동 304호", Some("304")),
        ("", "  ", None),
    ] {
        let normalized = normalize_building_register_unit(raw(dong, name, above_ground("6")));
        assert_eq!(
            normalized.unit_designation_normalized.as_deref(),
            expected,
            "name={name}"
        );
    }
}

#[test]
fn canonical_dong_join_key_collapses_variants() {
    for (raw, expected) in [
        ("201동", "201"),
        ("제 201동", "201"),
        ("제201동", "201"),
        ("506", "506"),
        ("A동", "A"),
        ("101동", "101"),
        ("", ""),
        ("동", ""),
        ("국일관드림펠리스", "국일관드림펠리스"),
    ] {
        assert_eq!(canonical_dong_join_key(raw), expected, "raw={raw}");
    }
}

#[test]
fn keeps_dong_name_as_join_text() {
    let normalized =
        normalize_building_register_unit(raw("국일관드림펠리스", "1-087호", above_ground("1")));
    assert_eq!(
        normalized.dong_join_name.as_deref(),
        Some("국일관드림펠리스")
    );
    assert_eq!(normalized.unit_number, Some(87));

    let empty = normalize_building_register_unit(raw("  ", "101호", above_ground("1")));
    assert_eq!(empty.dong_join_name, None);
}

#[test]
fn floor_word_names_are_floor_spaces_not_unit_numbers() {
    // `4층`류 호명은 층 공간이지 호번호가 아니다. 마지막 숫자런 추출이 층 숫자를
    // 호수로 읽으면 같은 건물의 층 이름 행 전체가 가짜 호번호를 얻는다
    // (2026-09-25 운영 전수조사에서 확인된 오분류 급).
    for (name, floor) in [
        ("4층", above_ground("4")),
        ("9층 전층", above_ground("9")),
        ("2층전체", above_ground("2")),
        ("지상3층", above_ground("3")),
        ("지하1층", basement("1")),
        ("지하 2층 일부", basement("2")),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, floor));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterUnitReason::AcceptedFloorSpace,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
        assert_eq!(normalized.unit_label_ko, None, "name={name}");
    }
}

#[test]
fn floor_word_names_with_disagreeing_floor_fields_stay_proposals() {
    // 이름은 층을 말하는데 명시 층 칸이 다르게 말하면 확정하지 않는다.
    for (name, floor) in [
        ("4층", above_ground("3")),
        ("지하1층", above_ground("1")),
        ("2층", above_ground("")),
        ("지상5층", basement("1")),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, floor));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
    }
}

#[test]
fn merged_unit_names_are_proposals_not_last_run_units() {
    // `401,2` = 401호와 402호가 합쳐진 호. 마지막 숫자런(2)을 호번호로 읽으면
    // 존재하지 않는 2호가 생긴다. 병합 호는 구조적 결정이 필요하므로 proposal.
    for name in ["401,2", "103,5", "401,402", "1001,2호", "301.302"] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("4")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::ProposalRequired,
            "name={name}"
        );
        assert_eq!(
            normalized.reason,
            BuildingRegisterUnitReason::MergedUnitName,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, None, "name={name}");
    }
}

#[test]
fn paren_annotation_after_the_unit_number_is_not_the_unit() {
    // `303호(32A평형)` — 괄호는 평형·동·음역 주석이다. 마지막 숫자런 추출이
    // 괄호 안 숫자(32)를 집으면 같은 평형의 모든 호가 한 번호로 붕괴한다
    // (운영 3,176행 급, 2026-09-25 전수조사).
    for (name, expected) in [
        ("303호(32A평형)", 303),
        ("7호(2층)", 7),
        ("201(1동)", 201),
        ("1409(에이)호", 1409),
        ("105호(D형)", 105),
    ] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("3")));
        assert_eq!(
            normalized.status,
            BuildingRegisterUnitStatus::Accepted,
            "name={name}"
        );
        assert_eq!(normalized.unit_number, Some(expected), "name={name}");
    }
}

#[test]
fn decimal_like_names_that_are_not_merges_keep_extraction() {
    // 붙임표 하위 호(108-1호)와 층 괄호 표기는 병합 호가 아니다 — 기존 추출 유지.
    for (name, expected) in [("108-1호", 1), ("2-026호", 26), ("15(2층)", 15)] {
        let normalized = normalize_building_register_unit(raw("", name, above_ground("2")));
        assert_eq!(normalized.unit_number, Some(expected), "name={name}");
    }
}
