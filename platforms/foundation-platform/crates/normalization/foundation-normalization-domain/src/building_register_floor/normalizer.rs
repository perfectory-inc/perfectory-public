mod rules;

use super::{
    BuildingRegisterFloorKind, BuildingRegisterFloorNormalizationReason,
    BuildingRegisterFloorNormalizationStatus, NormalizedBuildingRegisterFloor,
    RawBuildingRegisterFloor,
};
use rules::{
    normalize_above_ground, normalize_all_floors, normalize_basement, normalize_rooftop,
    normalize_special_floor_kind, normalize_zero_provider_floor_number, parse_embedded_floor_label,
    parse_numeric_label,
};

/// Normalize one building-register floor row with deterministic provider rules.
#[must_use]
pub fn normalize_building_register_floor(
    raw: RawBuildingRegisterFloor<'_>,
) -> NormalizedBuildingRegisterFloor {
    let floor_type_code = raw.floor_type_code.trim();
    let floor_type_name = raw.floor_type_name.trim();
    let kind = match resolve_floor_kind(floor_type_code, floor_type_name) {
        Ok(kind) => kind,
        Err(reason) => return proposal(BuildingRegisterFloorKind::Unknown, reason),
    };

    if matches!(kind, BuildingRegisterFloorKind::AllFloors) {
        return normalize_all_floors(raw.floor_label);
    }

    if matches!(
        kind,
        BuildingRegisterFloorKind::MultiFloorLower | BuildingRegisterFloorKind::MultiFloorUpper
    ) {
        return normalize_special_floor_kind(kind, raw.floor_number, raw.floor_label);
    }

    let floor_number = match parse_floor_number(raw.floor_number) {
        Ok(number) => number,
        Err(reason) => return proposal(kind, reason),
    };

    if floor_number == 0 {
        return normalize_zero_provider_floor_number(kind, raw.floor_label);
    }

    if floor_number > 300 {
        return proposal(
            kind,
            BuildingRegisterFloorNormalizationReason::SpecialProviderFloorNumber,
        );
    }

    let label = match raw.floor_label {
        Some(label) if label.trim().is_empty() => {
            return proposal(kind, BuildingRegisterFloorNormalizationReason::EmptyLabel)
        }
        Some(label) => Some(compact_label(label)),
        None => None,
    };

    match kind {
        BuildingRegisterFloorKind::AboveGround => normalize_above_ground(floor_number, label),
        BuildingRegisterFloorKind::Basement => normalize_basement(floor_number, label),
        BuildingRegisterFloorKind::Rooftop => normalize_rooftop(floor_number, label),
        BuildingRegisterFloorKind::AllFloors
        | BuildingRegisterFloorKind::MultiFloorLower
        | BuildingRegisterFloorKind::MultiFloorUpper
        | BuildingRegisterFloorKind::Unknown => proposal(
            kind,
            BuildingRegisterFloorNormalizationReason::UnknownFloorType,
        ),
    }
}

/// Extracts the two conflicting floor-number witnesses from a raw row for
/// building-level resolution: the provider numeric field and the number carried
/// by the label, each when parseable.
#[must_use]
pub fn building_register_floor_evidence_numbers(
    raw: RawBuildingRegisterFloor<'_>,
) -> (Option<u16>, Option<u16>) {
    let provider = parse_floor_number(raw.floor_number).ok();
    let label = raw
        .floor_label
        .map(compact_label)
        .and_then(|label| label_floor_number(&label));
    (provider, label)
}

/// True when a floor label denotes a 다락 (attic).
///
/// The attic's canonical floor kind (above-ground top floor vs rooftop structure)
/// is not decidable from the row alone; [`super::resolve_building_floors`] places
/// it using the 표제부 지상층수 witness.
#[must_use]
pub fn building_register_floor_label_is_attic(label: Option<&str>) -> bool {
    label.is_some_and(|label| compact_label(label).contains("다락"))
}

/// Best-effort floor number carried by a label, independent of the provider kind.
fn label_floor_number(label: &str) -> Option<u16> {
    for prefix in ["지상", "지하", "옥탑", "지", "옥"] {
        if let Some(number) = parse_numeric_label(label, Some(prefix)) {
            return Some(number);
        }
    }
    parse_numeric_label(label, None).or_else(|| parse_embedded_floor_label(label))
}

fn resolve_floor_kind(
    floor_type_code: &str,
    floor_type_name: &str,
) -> Result<BuildingRegisterFloorKind, BuildingRegisterFloorNormalizationReason> {
    let code_kind = floor_kind_from_code(floor_type_code);
    let name_kind = floor_kind_from_name(floor_type_name);

    match (code_kind, name_kind) {
        (Some(code_kind), Some(name_kind)) if code_kind == name_kind => Ok(code_kind),
        (Some(_), Some(_)) => Err(BuildingRegisterFloorNormalizationReason::TypeCodeNameMismatch),
        _ => Err(BuildingRegisterFloorNormalizationReason::UnknownFloorType),
    }
}

fn floor_kind_from_code(code: &str) -> Option<BuildingRegisterFloorKind> {
    match code {
        "20" => Some(BuildingRegisterFloorKind::AboveGround),
        "10" => Some(BuildingRegisterFloorKind::Basement),
        "30" => Some(BuildingRegisterFloorKind::Rooftop),
        "40" => Some(BuildingRegisterFloorKind::AllFloors),
        "21" => Some(BuildingRegisterFloorKind::MultiFloorLower),
        "22" => Some(BuildingRegisterFloorKind::MultiFloorUpper),
        _ => None,
    }
}

fn floor_kind_from_name(name: &str) -> Option<BuildingRegisterFloorKind> {
    match name {
        "지상" => Some(BuildingRegisterFloorKind::AboveGround),
        "지하" => Some(BuildingRegisterFloorKind::Basement),
        "옥탑" => Some(BuildingRegisterFloorKind::Rooftop),
        "각층" => Some(BuildingRegisterFloorKind::AllFloors),
        "복수층(하층)" => Some(BuildingRegisterFloorKind::MultiFloorLower),
        "복수층(상층)" => Some(BuildingRegisterFloorKind::MultiFloorUpper),
        _ => None,
    }
}

fn parse_floor_number(raw: &str) -> Result<u16, BuildingRegisterFloorNormalizationReason> {
    raw.trim()
        .parse::<u16>()
        .map_err(|_| BuildingRegisterFloorNormalizationReason::InvalidFloorNumber)
}

fn compact_label(raw: &str) -> String {
    // A6: drop a leading building-wing token such as A동 / 가동 / 1동. Wing names are
    // annexes, not floor identity, and may wrap the floor in parentheses (A동(1층)).
    // A1: drop parenthetical annotations such as (부속), (B동), (증축).
    // A2: drop a leading 内 annotation such as 내지하2층 -> 지하2층.
    // A5: repair the 충/츠 -> 층 data-entry typos.
    let without_wing = strip_leading_building_wing(raw.trim());
    let without_parens = strip_parentheses(without_wing);
    let trimmed = without_parens.trim();
    let body = match trimmed.strip_prefix('내') {
        Some(rest) if !rest.trim().is_empty() => rest,
        _ => trimmed,
    };
    body.chars()
        .filter(|value| !value.is_whitespace())
        .map(|value| match value {
            '충' | '츠' => '층',
            other => other,
        })
        .collect()
}

/// Removes a leading building-wing token (`A동`, `가동`, `1동`, `101동`) and any
/// following separator, returning the remaining floor label.
///
/// The wing name is an annex marker, not floor identity. A name is a run of ASCII
/// letters/digits or a single hangul syllable immediately followed by `동`; anything
/// else (for example `지층(B동)`, where the `동` belongs to a parenthetical note) is
/// left for [`strip_parentheses`] to handle.
fn strip_leading_building_wing(label: &str) -> &str {
    let Some(dong_offset) = label.find('동') else {
        return label;
    };
    let name = &label[..dong_offset];
    if !is_building_wing_name(name) {
        return label;
    }
    label[dong_offset + '동'.len_utf8()..].trim_start()
}

/// True when `name` is a building-wing identifier: a non-empty run of ASCII
/// alphanumerics, or a single hangul syllable.
fn is_building_wing_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    if name.chars().all(|value| value.is_ascii_alphanumeric()) {
        return true;
    }
    let mut chars = name.chars();
    matches!(
        (chars.next(), chars.next()),
        (Some(value), None) if ('가'..='힣').contains(&value)
    )
}

/// Resolves parentheses in a floor label (half- and full-width).
///
/// Parentheses usually carry non-floor annotations such as `지1(부속)` or `지층(B동)`,
/// which are dropped. But when the label is *entirely* parenthesized (for example the
/// `(1층)` left after a wing prefix is stripped from `A동(1층)`), the parentheses wrap
/// the floor itself, so their contents are unwrapped instead of dropped.
fn strip_parentheses(raw: &str) -> String {
    let mut depth: usize = 0;
    let mut outside = String::with_capacity(raw.len());
    let mut inside = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '(' | '\u{FF08}' => depth += 1,
            ')' | '\u{FF09}' => depth = depth.saturating_sub(1),
            _ if depth == 0 => outside.push(ch),
            _ => inside.push(ch),
        }
    }
    if outside.trim().is_empty() {
        inside
    } else {
        outside
    }
}

const fn accepted(
    kind: BuildingRegisterFloorKind,
    floor_number: Option<u16>,
    floor_index: Option<i16>,
    display_ko: Option<String>,
    reason: BuildingRegisterFloorNormalizationReason,
) -> NormalizedBuildingRegisterFloor {
    NormalizedBuildingRegisterFloor {
        kind,
        floor_number,
        floor_index,
        display_ko,
        status: BuildingRegisterFloorNormalizationStatus::Accepted,
        reason,
    }
}

const fn proposal(
    kind: BuildingRegisterFloorKind,
    reason: BuildingRegisterFloorNormalizationReason,
) -> NormalizedBuildingRegisterFloor {
    NormalizedBuildingRegisterFloor {
        kind,
        floor_number: None,
        floor_index: None,
        display_ko: None,
        status: BuildingRegisterFloorNormalizationStatus::ProposalRequired,
        reason,
    }
}

#[cfg(test)]
mod tests;
