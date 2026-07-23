use super::{
    accepted, compact_label, parse_floor_number, proposal, BuildingRegisterFloorKind,
    BuildingRegisterFloorNormalizationReason, NormalizedBuildingRegisterFloor,
};
pub(super) fn normalize_all_floors(label: Option<&str>) -> NormalizedBuildingRegisterFloor {
    match label {
        Some(raw) if raw.trim().is_empty() => proposal(
            BuildingRegisterFloorKind::AllFloors,
            BuildingRegisterFloorNormalizationReason::EmptyLabel,
        ),
        Some(raw) if compact_label(raw) != "각층" => proposal(
            BuildingRegisterFloorKind::AllFloors,
            BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
        ),
        _ => accepted(
            BuildingRegisterFloorKind::AllFloors,
            None,
            None,
            Some("각층".to_owned()),
            BuildingRegisterFloorNormalizationReason::AcceptedSpecialFloorKind,
        ),
    }
}

pub(super) fn normalize_special_floor_kind(
    kind: BuildingRegisterFloorKind,
    floor_number_raw: &str,
    label: Option<&str>,
) -> NormalizedBuildingRegisterFloor {
    let floor_number = match floor_number_raw.trim() {
        "" | "0" => None,
        _ => match parse_floor_number(floor_number_raw) {
            Ok(number) if number <= 300 => Some(number),
            Ok(_) => {
                return proposal(
                    kind,
                    BuildingRegisterFloorNormalizationReason::SpecialProviderFloorNumber,
                )
            }
            Err(reason) => return proposal(kind, reason),
        },
    };

    let display_ko = match kind {
        BuildingRegisterFloorKind::MultiFloorLower => Some("복수층(하층)".to_owned()),
        BuildingRegisterFloorKind::MultiFloorUpper => Some("복수층(상층)".to_owned()),
        _ => None,
    };

    match label {
        Some(raw) if raw.trim().is_empty() => {
            proposal(kind, BuildingRegisterFloorNormalizationReason::EmptyLabel)
        }
        _ => accepted(
            kind,
            floor_number,
            None,
            display_ko,
            BuildingRegisterFloorNormalizationReason::AcceptedSpecialFloorKind,
        ),
    }
}

pub(super) fn normalize_above_ground(
    floor_number: u16,
    label: Option<String>,
) -> NormalizedBuildingRegisterFloor {
    let display = format!("지상 {floor_number}층");
    let floor_index = i16::try_from(floor_number).ok();

    let Some(label) = label else {
        return accepted(
            BuildingRegisterFloorKind::AboveGround,
            Some(floor_number),
            floor_index,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedLabelAbsentUsingTypeAndNumber,
        );
    };

    if label == "지상" || label == "지상층" {
        return accepted(
            BuildingRegisterFloorKind::AboveGround,
            Some(floor_number),
            floor_index,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedLabelAbsentUsingTypeAndNumber,
        );
    }

    if let Some(label_number) = parse_numeric_label(&label, Some("지상")) {
        return if label_number == floor_number {
            accepted(
                BuildingRegisterFloorKind::AboveGround,
                Some(floor_number),
                floor_index,
                Some(display),
                BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
            )
        } else {
            proposal(
                BuildingRegisterFloorKind::AboveGround,
                BuildingRegisterFloorNormalizationReason::LabelNumberMismatch,
            )
        };
    }

    if label.starts_with('지') || label.starts_with("옥탑") {
        return proposal(
            BuildingRegisterFloorKind::AboveGround,
            BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
        );
    }

    match parse_numeric_label(&label, None).or_else(|| parse_embedded_floor_label(&label)) {
        Some(label_number) if label_number == floor_number => accepted(
            BuildingRegisterFloorKind::AboveGround,
            Some(floor_number),
            floor_index,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
        ),
        Some(_) => proposal(
            BuildingRegisterFloorKind::AboveGround,
            BuildingRegisterFloorNormalizationReason::LabelNumberMismatch,
        ),
        None => proposal(
            BuildingRegisterFloorKind::AboveGround,
            BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
        ),
    }
}

pub(super) fn normalize_basement(
    floor_number: u16,
    label: Option<String>,
) -> NormalizedBuildingRegisterFloor {
    let display = format!("지하 {floor_number}층");
    let floor_index = i16::try_from(floor_number).ok().map(|number| -number);

    let Some(label) = label else {
        return accepted(
            BuildingRegisterFloorKind::Basement,
            Some(floor_number),
            floor_index,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedLabelAbsentUsingTypeAndNumber,
        );
    };

    if label == "지층" || label == "지하층" || label == "지하" || label == "지" {
        return accepted(
            BuildingRegisterFloorKind::Basement,
            Some(floor_number),
            floor_index,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedBasementGenericLabelWithNumber,
        );
    }

    if !label.starts_with('지') {
        return proposal(
            BuildingRegisterFloorKind::Basement,
            BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
        );
    }

    let (expected_reason, label_number) = parse_numeric_label(&label, Some("지하")).map_or_else(
        || {
            (
                BuildingRegisterFloorNormalizationReason::AcceptedBasementShorthand,
                parse_numeric_label(&label, Some("지")),
            )
        },
        |number| {
            (
                BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
                Some(number),
            )
        },
    );

    match label_number {
        Some(label_number) if label_number == floor_number => accepted(
            BuildingRegisterFloorKind::Basement,
            Some(floor_number),
            floor_index,
            Some(display),
            expected_reason,
        ),
        Some(_) => proposal(
            BuildingRegisterFloorKind::Basement,
            BuildingRegisterFloorNormalizationReason::LabelNumberMismatch,
        ),
        None => proposal(
            BuildingRegisterFloorKind::Basement,
            BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
        ),
    }
}

pub(super) fn normalize_rooftop(
    floor_number: u16,
    label: Option<String>,
) -> NormalizedBuildingRegisterFloor {
    let display = format!("옥탑 {floor_number}층");

    let Some(label) = label else {
        return accepted(
            BuildingRegisterFloorKind::Rooftop,
            Some(floor_number),
            None,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedLabelAbsentUsingTypeAndNumber,
        );
    };

    if label == "옥탑" || label == "옥탑층" {
        return accepted(
            BuildingRegisterFloorKind::Rooftop,
            Some(floor_number),
            None,
            Some(display),
            BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
        );
    }

    // Rooftop level printed on the label agrees with the provider number: exact.
    if let Some(label_number) = parse_numeric_label(&label, Some("옥탑"))
        .or_else(|| parse_numeric_label(&label, Some("옥")))
    {
        if label_number == floor_number {
            return accepted(
                BuildingRegisterFloorKind::Rooftop,
                Some(floor_number),
                None,
                Some(display),
                BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
            );
        }
    }

    // The rooftop level is nominal, so any rooftop-structure label (옥탑/옥상/지붕/탑)
    // is accepted as a rooftop even when the label and provider numbers disagree
    // (옥탑0, 옥탑1층 with a stray provider number, 탑, 옥상, 지붕층).
    if is_rooftop_structure_label(&label) {
        return accept_rooftop_structure(&label);
    }

    // Not a rooftop structure: 다락 (attic), 계단 (stairwell), bare numbers, etc.
    proposal(
        BuildingRegisterFloorKind::Rooftop,
        BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
    )
}

/// True when a rooftop-kind label denotes a rooftop structure (옥탑/옥상/지붕/탑),
/// as opposed to a distinct top-of-building space such as 다락 (attic) or 계단
/// (stairwell), which stay proposals for policy/vocabulary review.
fn is_rooftop_structure_label(label: &str) -> bool {
    ["옥탑", "옥상", "지붕", "탑"]
        .iter()
        .any(|token| label.contains(token))
}

/// Accepts a rooftop-kind row as a rooftop structure. The rooftop level is taken
/// from the label when it carries a definite one (옥탑2층 -> 2) and left unspecified
/// otherwise (옥탑0 / 탑 / 옥상 -> generic 옥탑), never invented from the provider
/// number field, which is unreliable for rooftop rows.
fn accept_rooftop_structure(label: &str) -> NormalizedBuildingRegisterFloor {
    let level = rooftop_label_number(label);
    let display = level.map_or_else(|| "옥탑".to_owned(), |number| format!("옥탑 {number}층"));
    accepted(
        BuildingRegisterFloorKind::Rooftop,
        level,
        None,
        Some(display),
        BuildingRegisterFloorNormalizationReason::AcceptedRooftopStructure,
    )
}

pub(super) fn normalize_rooftop_without_number(
    label: Option<&str>,
) -> NormalizedBuildingRegisterFloor {
    let Some(label) = label else {
        return proposal(
            BuildingRegisterFloorKind::Rooftop,
            BuildingRegisterFloorNormalizationReason::InvalidFloorNumber,
        );
    };

    if label.trim().is_empty() {
        return proposal(
            BuildingRegisterFloorKind::Rooftop,
            BuildingRegisterFloorNormalizationReason::EmptyLabel,
        );
    }

    let compacted = compact_label(label);
    match compacted.as_str() {
        "옥탑" | "옥탑층" => accepted(
            BuildingRegisterFloorKind::Rooftop,
            None,
            None,
            Some("옥탑".to_owned()),
            BuildingRegisterFloorNormalizationReason::AcceptedExactLabel,
        ),
        other if is_rooftop_structure_label(other) => accept_rooftop_structure(other),
        _ => proposal(
            BuildingRegisterFloorKind::Rooftop,
            BuildingRegisterFloorNormalizationReason::LabelKindMismatch,
        ),
    }
}

pub(super) fn normalize_zero_provider_floor_number(
    kind: BuildingRegisterFloorKind,
    label: Option<&str>,
) -> NormalizedBuildingRegisterFloor {
    if matches!(kind, BuildingRegisterFloorKind::Rooftop) {
        let Some(raw_label) = label else {
            return proposal(
                BuildingRegisterFloorKind::Rooftop,
                BuildingRegisterFloorNormalizationReason::InvalidFloorNumber,
            );
        };
        if raw_label.trim().is_empty() {
            return proposal(
                BuildingRegisterFloorKind::Rooftop,
                BuildingRegisterFloorNormalizationReason::EmptyLabel,
            );
        }
        let compacted = compact_label(raw_label);
        if let Some(label_number) = rooftop_label_number(&compacted) {
            return accept_numbered_floor(
                BuildingRegisterFloorKind::Rooftop,
                label_number,
                BuildingRegisterFloorNormalizationReason::AcceptedLabelNumberUsingInvalidProviderNumber,
            );
        }
        return normalize_rooftop_without_number(Some(compacted.as_str()));
    }

    let Some(raw_label) = label else {
        return proposal(
            kind,
            BuildingRegisterFloorNormalizationReason::InvalidFloorNumber,
        );
    };
    if raw_label.trim().is_empty() {
        return proposal(kind, BuildingRegisterFloorNormalizationReason::EmptyLabel);
    }

    let compacted = compact_label(raw_label);
    let label_number = match kind {
        BuildingRegisterFloorKind::AboveGround => above_ground_label_number(&compacted),
        BuildingRegisterFloorKind::Basement => basement_label_number(&compacted),
        BuildingRegisterFloorKind::Rooftop => rooftop_label_number(&compacted),
        BuildingRegisterFloorKind::AllFloors
        | BuildingRegisterFloorKind::MultiFloorLower
        | BuildingRegisterFloorKind::MultiFloorUpper
        | BuildingRegisterFloorKind::Unknown => None,
    };

    label_number.map_or_else(
        || {
            proposal(
                kind,
                BuildingRegisterFloorNormalizationReason::InvalidFloorNumber,
            )
        },
        |label_number| {
            accept_numbered_floor(
            kind,
            label_number,
            BuildingRegisterFloorNormalizationReason::AcceptedLabelNumberUsingInvalidProviderNumber,
            )
        },
    )
}

fn above_ground_label_number(label: &str) -> Option<u16> {
    if label.starts_with('지') && !label.starts_with("지상") {
        return None;
    }
    if label.starts_with('옥') {
        return None;
    }
    parse_numeric_label(label, Some("지상"))
        .or_else(|| parse_numeric_label(label, None))
        .or_else(|| parse_embedded_floor_label(label))
        .filter(|number| (1..=300).contains(number))
}

fn basement_label_number(label: &str) -> Option<u16> {
    if !label.starts_with('지') {
        return None;
    }
    parse_numeric_label(label, Some("지하"))
        .or_else(|| parse_numeric_label(label, Some("지")))
        .filter(|number| (1..=300).contains(number))
}

fn rooftop_label_number(label: &str) -> Option<u16> {
    if !label.starts_with('옥') {
        return None;
    }
    parse_numeric_label(label, Some("옥탑"))
        .or_else(|| parse_numeric_label(label, Some("옥")))
        .filter(|number| (1..=300).contains(number))
}

fn accept_numbered_floor(
    kind: BuildingRegisterFloorKind,
    floor_number: u16,
    reason: BuildingRegisterFloorNormalizationReason,
) -> NormalizedBuildingRegisterFloor {
    match kind {
        BuildingRegisterFloorKind::AboveGround => accepted(
            BuildingRegisterFloorKind::AboveGround,
            Some(floor_number),
            i16::try_from(floor_number).ok(),
            Some(format!("지상 {floor_number}층")),
            reason,
        ),
        BuildingRegisterFloorKind::Basement => accepted(
            BuildingRegisterFloorKind::Basement,
            Some(floor_number),
            i16::try_from(floor_number).ok().map(|number| -number),
            Some(format!("지하 {floor_number}층")),
            reason,
        ),
        BuildingRegisterFloorKind::Rooftop => accepted(
            BuildingRegisterFloorKind::Rooftop,
            Some(floor_number),
            None,
            Some(format!("옥탑 {floor_number}층")),
            reason,
        ),
        _ => proposal(
            kind,
            BuildingRegisterFloorNormalizationReason::UnknownFloorType,
        ),
    }
}

pub(super) fn parse_numeric_label(label: &str, prefix: Option<&str>) -> Option<u16> {
    let without_prefix = match prefix {
        Some(prefix) => label.strip_prefix(prefix)?,
        None => label,
    };
    let numeric = without_prefix.strip_suffix('층').unwrap_or(without_prefix);

    if numeric.is_empty() {
        return None;
    }

    parse_floor_number_token(numeric)
}

pub(super) fn parse_embedded_floor_label(label: &str) -> Option<u16> {
    if label.chars().filter(|value| *value == '층').count() != 1 {
        return None;
    }
    let floor_suffix_index = label.find('층')?;
    let before_floor_suffix = &label[..floor_suffix_index];
    let token = trailing_floor_number_token(before_floor_suffix)?;
    parse_floor_number_token(token)
}

fn trailing_floor_number_token(value: &str) -> Option<&str> {
    let mut start = value.len();
    let mut found = false;
    for (index, ch) in value.char_indices().rev() {
        if is_floor_number_token_char(ch) {
            start = index;
            found = true;
        } else {
            break;
        }
    }
    found.then(|| &value[start..])
}

const fn is_floor_number_token_char(value: char) -> bool {
    value.is_ascii_digit()
        || matches!(
            value,
            '영' | '공'
                | '일'
                | '이'
                | '삼'
                | '사'
                | '오'
                | '육'
                | '륙'
                | '칠'
                | '팔'
                | '구'
                | '십'
                | '백'
        )
}

fn parse_floor_number_token(raw: &str) -> Option<u16> {
    let token = raw.trim();
    if token.is_empty() {
        return None;
    }
    if let Ok(number) = token.parse::<u16>() {
        return Some(number);
    }
    parse_korean_floor_number(token)
}

fn parse_korean_floor_number(token: &str) -> Option<u16> {
    let mut total = 0u16;
    let mut pending_digit = None::<u16>;

    for ch in token.chars() {
        match ch {
            '영' | '공' => pending_digit = Some(0),
            '일' => pending_digit = Some(1),
            '이' => pending_digit = Some(2),
            '삼' => pending_digit = Some(3),
            '사' => pending_digit = Some(4),
            '오' => pending_digit = Some(5),
            '육' | '륙' => pending_digit = Some(6),
            '칠' => pending_digit = Some(7),
            '팔' => pending_digit = Some(8),
            '구' => pending_digit = Some(9),
            '십' => {
                total = total.checked_add(pending_digit.unwrap_or(1) * 10)?;
                pending_digit = None;
            }
            '백' => {
                total = total.checked_add(pending_digit.unwrap_or(1) * 100)?;
                pending_digit = None;
            }
            _ => return None,
        }
    }

    total = total.checked_add(pending_digit.unwrap_or(0))?;
    (1..=300).contains(&total).then_some(total)
}
