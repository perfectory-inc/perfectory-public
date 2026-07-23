//! Building-register unit (전유부 호) normalization.
//!
//! A 전유부 row is one unit (호). Its identity is `(PNU + 동 + 호번호)`, and its
//! floor comes from the explicit 층 fields — never guessed from the 호 number,
//! which is not a reliable floor identifier. Basement is carried by the
//! signed floor (지하 = negative), so the 호 number stays a plain number.
//!
//! The government 전유부 carries no 동 *code*, only the 동 *name* text; the link to
//! a building (동) is recovered by joining `(PNU + 동명)` to 표제부. So this module
//! keeps the 동 name as an opaque join text and focuses on extracting the unit
//! number and reusing the shared floor rules.

use serde::{Deserialize, Serialize};

use crate::building_register_floor::{
    normalize_building_register_floor, BuildingRegisterFloorKind, NormalizedBuildingRegisterFloor,
    RawBuildingRegisterFloor,
};

mod proposal;

pub use proposal::{
    validate_building_register_unit_proposal, validate_building_register_unit_target_identity,
    validate_building_register_unit_target_identity_matches, BUILDING_REGISTER_UNIT_SCHEMA_VERSION,
};

/// Raw 전유부 unit fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawBuildingRegisterUnit<'a> {
    /// 동명칭 — building/wing name (`102동`, `국일관드림펠리스`, or empty).
    pub dong_name: &'a str,
    /// 호명칭 — unit name (`624호`, `2621`, `6층 630호`, `2-026호`, ...).
    pub unit_name: &'a str,
    /// 층 fields (층구분코드/명/번호); the unit's floor, reused from floor rules.
    pub floor: RawBuildingRegisterFloor<'a>,
}

/// Whether the unit identity was deterministically extracted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BuildingRegisterUnitStatus {
    /// A unit number was extracted.
    Accepted,
    /// No unit number could be extracted; a proposal/review path may handle it.
    ProposalRequired,
}

impl BuildingRegisterUnitStatus {
    /// Stable wire representation.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::ProposalRequired => "proposal_required",
        }
    }
}

/// Machine-readable reason for the unit decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BuildingRegisterUnitReason {
    /// A numeric unit number was extracted from the 호명.
    AcceptedNumericUnit,
    /// A safe non-numeric unit label was preserved.
    AcceptedUnitLabel,
    /// The 호명 was empty.
    EmptyUnitName,
    /// The 호명 carried no extractable unit number (for example `나형지층`).
    NoUnitNumber,
}

impl BuildingRegisterUnitReason {
    /// Stable wire representation.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::AcceptedNumericUnit => "accepted_numeric_unit",
            Self::AcceptedUnitLabel => "accepted_unit_label",
            Self::EmptyUnitName => "empty_unit_name",
            Self::NoUnitNumber => "no_unit_number",
        }
    }
}

/// Deterministic interpretation of one 전유부 unit row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedBuildingRegisterUnit {
    /// 동 join text (`매칭 키`; empty → `None`). Not a code — joined to 표제부 by
    /// `(PNU + 동명)` to recover the building key.
    pub dong_join_name: Option<String>,
    /// Extracted unit number (호번호).
    pub unit_number: Option<u32>,
    /// Explicit non-numeric unit label (`가호`, `A호`, ...), when deterministically safe.
    pub unit_label_ko: Option<String>,
    /// Whitespace-compacted raw 호명 — the collision-free matching designation.
    /// Derived from `unit_name_raw`; preserves every distinctive token
    /// (`D07-01호`, `아파트501`). `None` when the raw name is empty.
    pub unit_designation: Option<String>,
    /// The unit's floor, normalized by the shared floor rules (지하 = negative index).
    pub floor: NormalizedBuildingRegisterFloor,
    /// Deterministic status.
    pub status: BuildingRegisterUnitStatus,
    /// Machine-readable reason for the status.
    pub reason: BuildingRegisterUnitReason,
}

/// Canonical 동 join key used to link a 호 to its building across registers.
///
/// Strips whitespace, a leading `제`, and a trailing `동` so表기 변형 collapse to one
/// key: `제 201동` / `201동` / `201` → `201`. Returns an empty string for a nameless
/// 동 (single-building parcels), which callers resolve by a single-building fallback.
#[must_use]
pub fn canonical_dong_join_key(dong_name: &str) -> String {
    let without_whitespace: String = dong_name
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect();
    let without_je = without_whitespace
        .strip_prefix('제')
        .unwrap_or(&without_whitespace);
    without_je
        .strip_suffix('동')
        .unwrap_or(without_je)
        .to_owned()
}

/// Whitespace-compacted raw 호명 — the collision-free matching designation.
///
/// Shared by every register-side dataset (전유부, 전유공용면적) and the auction
/// matcher. Content is preserved verbatim; only whitespace is removed.
/// `None` when empty.
#[must_use]
pub fn building_register_unit_designation(unit_name: &str) -> Option<String> {
    let compact: String = unit_name
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect();
    (!compact.is_empty()).then_some(compact)
}

/// Normalizes one 전유부 unit row with deterministic rules.
#[must_use]
pub fn normalize_building_register_unit(
    raw: RawBuildingRegisterUnit<'_>,
) -> NormalizedBuildingRegisterUnit {
    let floor = normalize_building_register_floor(raw.floor);
    let dong_join_name = {
        let trimmed = raw.dong_name.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    };

    let unit_name = raw.unit_name.trim();
    if unit_name.is_empty() {
        return unit(
            dong_join_name,
            None,
            None,
            None,
            floor,
            BuildingRegisterUnitStatus::ProposalRequired,
            BuildingRegisterUnitReason::EmptyUnitName,
        );
    }

    let unit_designation = building_register_unit_designation(unit_name);

    let unit_number = extract_paren_floor_annotated_unit_number(unit_name)
        .or_else(|| extract_unit_number(unit_name))
        .or_else(|| extract_floor_scoped_unit_number(unit_name, &floor));

    match unit_number {
        Some(number) => unit(
            dong_join_name,
            Some(number),
            None,
            unit_designation,
            floor,
            BuildingRegisterUnitStatus::Accepted,
            BuildingRegisterUnitReason::AcceptedNumericUnit,
        ),
        None => match extract_explicit_unit_label_ko(unit_name)
            .or_else(|| extract_expanded_unit_label_ko(unit_name, &floor))
        {
            Some(label) => unit(
                dong_join_name,
                None,
                Some(label),
                unit_designation,
                floor,
                BuildingRegisterUnitStatus::Accepted,
                BuildingRegisterUnitReason::AcceptedUnitLabel,
            ),
            None => unit(
                dong_join_name,
                None,
                None,
                unit_designation,
                floor,
                BuildingRegisterUnitStatus::ProposalRequired,
                BuildingRegisterUnitReason::NoUnitNumber,
            ),
        },
    }
}

/// `N(M층)` / `N(지하M층)` — the leading run is the unit and the parenthesis is a
/// floor annotation, so last-run extraction would wrongly return the floor and
/// collapse a whole floor of units onto one number.
fn extract_paren_floor_annotated_unit_number(unit_name: &str) -> Option<u32> {
    let compact: String = unit_name
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect();
    let body = compact.strip_suffix(')')?;
    let (unit_part, floor_part) = body.split_once('(')?;
    if unit_part.is_empty()
        || unit_part.len() > 5
        || !unit_part.bytes().all(|value| value.is_ascii_digit())
    {
        return None;
    }
    let floor_digits = floor_part
        .strip_prefix("지하")
        .unwrap_or(floor_part)
        .strip_suffix('층')?;
    if floor_digits.is_empty()
        || floor_digits.len() > 3
        || !floor_digits.bytes().all(|value| value.is_ascii_digit())
    {
        return None;
    }
    unit_part.parse::<u32>().ok()
}

/// Extracts the unit number as the last maximal run of digits in the 호명.
///
/// Floor prefixes come first (`6층`, `2-`, block letters), so the trailing digit
/// run is the unit part: `624호`→624, `2-026호`→26, `6층630호`→630, `H705`→705,
/// `지층7호`→7. `나형지층` (no digits) yields `None`. Runs longer than five digits
/// are treated as codes, not unit numbers.
fn extract_unit_number(unit_name: &str) -> Option<u32> {
    let mut last: Option<u32> = None;
    let mut current = String::new();
    for value in unit_name.chars() {
        if value.is_ascii_digit() {
            current.push(value);
        } else {
            flush_unit_run(&current, &mut last);
            current.clear();
        }
    }
    flush_unit_run(&current, &mut last);
    last
}

fn extract_floor_scoped_unit_number(
    unit_name: &str,
    floor: &NormalizedBuildingRegisterFloor,
) -> Option<u32> {
    if floor.kind != BuildingRegisterFloorKind::AboveGround {
        return None;
    }

    let floor_number = u32::from(floor.floor_number?);
    let compact: String = unit_name
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect();
    if compact.len() != 6 || !compact.bytes().all(|value| value.is_ascii_digit()) {
        return None;
    }

    let prefix = compact[0..3].parse::<u32>().ok()?;
    let suffix = compact[3..6].parse::<u32>().ok()?;
    let expected_prefix = floor_number.checked_mul(100)?.checked_add(1)?;

    if prefix == expected_prefix && suffix / 100 == floor_number && suffix % 100 != 0 {
        Some(suffix)
    } else {
        None
    }
}

fn flush_unit_run(run: &str, last: &mut Option<u32>) {
    if run.is_empty() || run.len() > 5 {
        return;
    }
    if let Ok(number) = run.parse::<u32>() {
        *last = Some(number);
    }
}

fn extract_explicit_unit_label_ko(unit_name: &str) -> Option<String> {
    let compact: String = unit_name
        .chars()
        .filter(|value| !value.is_whitespace())
        .collect();
    if compact.is_empty() || compact.chars().any(|value| value.is_ascii_digit()) {
        return None;
    }

    let prefix = compact.strip_suffix('호')?;
    if prefix.chars().count() != 1 {
        return None;
    }

    let value = prefix.chars().next()?;
    if is_hangul_syllable(value) {
        return Some(format!("{prefix}호"));
    }
    if value.is_ascii_alphabetic() {
        return Some(format!("{}호", value.to_ascii_uppercase()));
    }
    None
}

fn is_hangul_syllable(value: char) -> bool {
    ('가'..='힣').contains(&value)
}

/// What the explicit floor fields must say for a stripped floor-word prefix
/// to be trusted (the prefix is parsing context only; floor stays field-owned).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FloorPrefixExpectation {
    Basement,
    Rooftop,
    AboveGround(u16),
}

/// Floor-word prefixes that may precede an explicit unit label (`지층가호`,
/// `일층나`). Longest-first so `지하층` wins over `지하`.
const FLOOR_LABEL_PREFIXES: [(&str, FloorPrefixExpectation); 16] = [
    ("지하층", FloorPrefixExpectation::Basement),
    ("반지하", FloorPrefixExpectation::Basement),
    ("옥탑층", FloorPrefixExpectation::Rooftop),
    ("지하", FloorPrefixExpectation::Basement),
    ("지층", FloorPrefixExpectation::Basement),
    ("옥탑", FloorPrefixExpectation::Rooftop),
    ("일층", FloorPrefixExpectation::AboveGround(1)),
    ("이층", FloorPrefixExpectation::AboveGround(2)),
    ("삼층", FloorPrefixExpectation::AboveGround(3)),
    ("사층", FloorPrefixExpectation::AboveGround(4)),
    ("오층", FloorPrefixExpectation::AboveGround(5)),
    ("육층", FloorPrefixExpectation::AboveGround(6)),
    ("칠층", FloorPrefixExpectation::AboveGround(7)),
    ("팔층", FloorPrefixExpectation::AboveGround(8)),
    ("구층", FloorPrefixExpectation::AboveGround(9)),
    ("십층", FloorPrefixExpectation::AboveGround(10)),
];

/// The 14 canonical Korean unit-ordering letters (가나다순 호 라벨).
const GANADA_UNIT_LETTERS: [char; 14] = [
    '가', '나', '다', '라', '마', '바', '사', '아', '자', '차', '카', '타', '파', '하',
];

/// Expanded label extraction: strips one floor-word prefix (trusted only when
/// it agrees with the explicit floor fields) and a leading `제`, then reads a
/// single ganada/ASCII/phonetic letter with an optional `호`/`세대` suffix.
/// Produces a normalized derived label (`지층가호`→`가호`, `에이호`→`A호`);
/// the raw 호명 stays in `unit_name_raw`.
fn extract_expanded_unit_label_ko(
    unit_name: &str,
    floor: &NormalizedBuildingRegisterFloor,
) -> Option<String> {
    let compact: String = unit_name
        .chars()
        .filter(|value| !value.is_whitespace() && *value != '-')
        .collect();
    if compact.is_empty() {
        return None;
    }

    let (rest, floor_expectation) = strip_floor_label_prefix(&compact);
    if let Some(expected) = floor_expectation {
        if !floor_prefix_matches(expected, floor) {
            return None;
        }
    }
    let rest = match rest.strip_prefix('제') {
        Some(after_je) if !after_je.is_empty() => after_je,
        _ => rest,
    };
    parse_expanded_label_token(rest)
}

fn strip_floor_label_prefix(compact: &str) -> (&str, Option<FloorPrefixExpectation>) {
    for (prefix, expectation) in FLOOR_LABEL_PREFIXES {
        if let Some(rest) = compact.strip_prefix(prefix) {
            if !rest.is_empty() {
                return (rest, Some(expectation));
            }
        }
    }
    (compact, None)
}

fn floor_prefix_matches(
    expected: FloorPrefixExpectation,
    floor: &NormalizedBuildingRegisterFloor,
) -> bool {
    match expected {
        FloorPrefixExpectation::Basement => floor.kind == BuildingRegisterFloorKind::Basement,
        FloorPrefixExpectation::Rooftop => floor.kind == BuildingRegisterFloorKind::Rooftop,
        FloorPrefixExpectation::AboveGround(number) => {
            floor.kind == BuildingRegisterFloorKind::AboveGround
                && floor.floor_number == Some(number)
        }
    }
}

fn parse_expanded_label_token(token: &str) -> Option<String> {
    let (core, suffix) = token.strip_suffix("세대").map_or_else(
        || (token.strip_suffix('호').unwrap_or(token), "호"),
        |before_sedae| (before_sedae, "세대"),
    );
    if core.is_empty() {
        return None;
    }

    if let Some(letter) = transliterate_phonetic_letter(core) {
        return Some(format!("{letter}{suffix}"));
    }

    let mut chars = core.chars();
    let letter = chars.next()?;
    if chars.next().is_some() {
        return None;
    }
    if GANADA_UNIT_LETTERS.contains(&letter) {
        return Some(format!("{letter}{suffix}"));
    }
    if letter.is_ascii_alphabetic() {
        // `l`/`I`/`O` read as 1/0 typos, not labels.
        if letter == 'l' {
            return None;
        }
        let upper = letter.to_ascii_uppercase();
        if upper == 'I' || upper == 'O' {
            return None;
        }
        return Some(format!("{upper}{suffix}"));
    }
    None
}

/// Fixed transliteration table for phonetic letter spellings. `이`(2/E) and
/// `지`(지하) are deliberately absent — both are ambiguous.
fn transliterate_phonetic_letter(core: &str) -> Option<char> {
    match core {
        "에이" => Some('A'),
        "비" => Some('B'),
        "씨" => Some('C'),
        "디" => Some('D'),
        "에프" => Some('F'),
        "에이치" => Some('H'),
        _ => None,
    }
}

const fn unit(
    dong_join_name: Option<String>,
    unit_number: Option<u32>,
    unit_label_ko: Option<String>,
    unit_designation: Option<String>,
    floor: NormalizedBuildingRegisterFloor,
    status: BuildingRegisterUnitStatus,
    reason: BuildingRegisterUnitReason,
) -> NormalizedBuildingRegisterUnit {
    NormalizedBuildingRegisterUnit {
        dong_join_name,
        unit_number,
        unit_label_ko,
        unit_designation,
        floor,
        status,
        reason,
    }
}

#[cfg(test)]
mod tests;
