//! Building-level contradiction resolution for building-register floors.
//!
//! The per-row normalizer ([`super::normalize_building_register_floor`]) resolves
//! rows whose independent fields agree, but some rows carry a genuine num-vs-label conflict
//! (provider floor number disagrees with the number embedded in the label) that
//! a single row cannot settle. This module settles those conflicts at the level
//! of one building (동) by consulting three independent witnesses and requiring a
//! majority, abstaining when they disagree.
//!
//! Witnesses for a floor kind (above-ground / basement) within one building:
//! - A: the multiset of provider floor numbers
//! - B: the multiset of label-derived floor numbers
//! - C: the building-title floor count (지상층수 / 지하층수), an independent register
//!
//! Domain invariant: a building's floors of one kind are contiguous 1..N. The
//! source whose distinct values form that clean 1..N sequence (and, when a
//! title count is present, whose length matches it) is authoritative for the
//! building; conflicted rows are re-derived from it. When neither source is
//! cleanly authoritative the rows are left as proposals for human review — this
//! module never guesses.
//!
//! This is the data-fusion VOTE / MDM survivorship pattern summarized in
//! `docs/canonical-property-data-platform-northstar.md`, kept deterministic and auditable.
//! The executable invariants live in this module and its unit tests.

use super::{
    BuildingRegisterFloorKind, BuildingRegisterFloorNormalizationReason,
    BuildingRegisterFloorNormalizationStatus, NormalizedBuildingRegisterFloor,
};

/// One row's evidence for building-level resolution.
///
/// `provider_number` and `label_number` are the two conflicting witnesses already
/// extracted from the raw provider fields; `per_row` is the row-level result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FloorRowEvidence {
    /// Provider floor number parsed from the numeric field, when valid.
    pub provider_number: Option<u16>,
    /// Floor number parsed from the label, when the label carried one.
    pub label_number: Option<u16>,
    /// Whether the label denotes a 다락 (attic), whose kind is placed by the
    /// building's 표제부 지상층수 rather than the row alone.
    pub attic_candidate: bool,
    /// Row-level normalization result.
    pub per_row: NormalizedBuildingRegisterFloor,
}

/// Building-title floor counts, the independent third witness.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BuildingFloorCounts {
    /// 지상층수 from the building title register, when known.
    pub above_ground: Option<u16>,
    /// 지하층수 from the building title register, when known.
    pub below_ground: Option<u16>,
}

/// Resolves a building's floor rows, upgrading num-vs-label conflicts that the
/// building context can settle and leaving everything else unchanged.
///
/// Returns one result per input row, in the same order. Rows the per-row
/// normalizer already accepted, and proposals this pass cannot settle, are
/// returned unchanged.
#[must_use]
pub fn resolve_building_floors(
    rows: &[FloorRowEvidence],
    counts: BuildingFloorCounts,
) -> Vec<NormalizedBuildingRegisterFloor> {
    let above_source = authoritative_source(
        rows,
        BuildingRegisterFloorKind::AboveGround,
        counts.above_ground,
    );
    let below_source = authoritative_source(
        rows,
        BuildingRegisterFloorKind::Basement,
        counts.below_ground,
    );

    // Phase 1: settle per-row num-vs-label conflicts.
    let mut resolved: Vec<NormalizedBuildingRegisterFloor> = rows
        .iter()
        .map(|row| {
            let source = match row.per_row.kind {
                BuildingRegisterFloorKind::AboveGround => above_source,
                BuildingRegisterFloorKind::Basement => below_source,
                _ => None,
            };
            resolve_row(row, source)
        })
        .collect();

    // Phase 2: place 다락 (attic) rows into above-ground or rooftop using the
    // 표제부 지상층수 witness. Only a single unsettled attic per building is placed;
    // multiple attics stay proposals since the witness cannot distinguish them.
    place_attic_rows(rows, &mut resolved, counts.above_ground);
    resolved
}

/// Classifies a lone unresolved 다락 (attic) row against the building's numbered
/// above-ground floors and the 표제부 지상층수.
fn place_attic_rows(
    rows: &[FloorRowEvidence],
    resolved: &mut [NormalizedBuildingRegisterFloor],
    title_above: Option<u16>,
) {
    let attic_indices: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(index, row)| {
            row.attic_candidate
                && resolved[*index].status
                    == BuildingRegisterFloorNormalizationStatus::ProposalRequired
        })
        .map(|(index, _)| index)
        .collect();
    let [attic_index] = attic_indices.as_slice() else {
        return;
    };

    let observed_max = resolved
        .iter()
        .filter(|row| row.kind == BuildingRegisterFloorKind::AboveGround)
        .filter_map(|row| row.floor_number)
        .max()
        .unwrap_or(0);

    if let Some(placed) = place_attic(title_above, observed_max) {
        resolved[*attic_index] = placed;
    }
}

/// Decides where a 다락 sits given the 표제부 지상층수 and the top numbered floor.
fn place_attic(
    title_above: Option<u16>,
    observed_max: u16,
) -> Option<NormalizedBuildingRegisterFloor> {
    let ground_count = title_above?;
    if observed_max.checked_add(1) == Some(ground_count) {
        // 표제부 counts one more floor than the numbered rows: the 다락 is that floor.
        Some(NormalizedBuildingRegisterFloor {
            kind: BuildingRegisterFloorKind::AboveGround,
            floor_number: Some(ground_count),
            floor_index: i16::try_from(ground_count).ok(),
            display_ko: Some(format!("지상 {ground_count}층")),
            status: BuildingRegisterFloorNormalizationStatus::Accepted,
            reason: BuildingRegisterFloorNormalizationReason::ResolvedAtticAsTopFloor,
        })
    } else if observed_max == ground_count {
        // The numbered floors already fill the 표제부 count: the 다락 is above them.
        Some(NormalizedBuildingRegisterFloor {
            kind: BuildingRegisterFloorKind::Rooftop,
            floor_number: None,
            floor_index: None,
            display_ko: Some("옥탑".to_owned()),
            status: BuildingRegisterFloorNormalizationStatus::Accepted,
            reason: BuildingRegisterFloorNormalizationReason::ResolvedAtticAsRooftop,
        })
    } else {
        None
    }
}

/// Which source (if any) is authoritative for a kind within this building.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthoritativeSource {
    ProviderNumber,
    LabelNumber,
}

fn authoritative_source(
    rows: &[FloorRowEvidence],
    kind: BuildingRegisterFloorKind,
    title_count: Option<u16>,
) -> Option<AuthoritativeSource> {
    let of_kind: Vec<&FloorRowEvidence> =
        rows.iter().filter(|row| row.per_row.kind == kind).collect();
    // Only worth resolving when at least one row of this kind is an unsettled
    // num-vs-label conflict.
    let has_conflict = of_kind.iter().any(|row| is_number_conflict(row));
    if !has_conflict {
        return None;
    }

    let provider_seq: Vec<u16> = of_kind
        .iter()
        .filter_map(|row| row.provider_number)
        .collect();
    let label_seq: Vec<u16> = of_kind.iter().filter_map(|row| row.label_number).collect();

    let provider_ok = is_clean_contiguous(&provider_seq, title_count);
    let label_ok = is_clean_contiguous(&label_seq, title_count);

    match (provider_ok, label_ok) {
        (true, false) => Some(AuthoritativeSource::ProviderNumber),
        (false, true) => Some(AuthoritativeSource::LabelNumber),
        // Both clean (they agree) or neither clean (cannot break the tie): abstain.
        _ => None,
    }
}

/// A row still unsettled specifically because number and label disagree.
fn is_number_conflict(row: &FloorRowEvidence) -> bool {
    row.per_row.status == BuildingRegisterFloorNormalizationStatus::ProposalRequired
        && row.per_row.reason == BuildingRegisterFloorNormalizationReason::LabelNumberMismatch
}

/// True when the distinct values form a clean 1..=k contiguous run, and (when a
/// title count is known) k equals it.
fn is_clean_contiguous(values: &[u16], title_count: Option<u16>) -> bool {
    if values.is_empty() {
        return false;
    }
    let mut distinct: Vec<u16> = values.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    let is_contiguous = distinct
        .iter()
        .enumerate()
        .all(|(index, &value)| u16::try_from(index + 1).is_ok_and(|expected| value == expected));
    if !is_contiguous {
        return false;
    }
    title_count.is_none_or(|count| u16::try_from(distinct.len()).is_ok_and(|len| len == count))
}

fn resolve_row(
    row: &FloorRowEvidence,
    source: Option<AuthoritativeSource>,
) -> NormalizedBuildingRegisterFloor {
    let Some(source) = source else {
        return row.per_row.clone();
    };
    if !is_number_conflict(row) {
        return row.per_row.clone();
    }
    let chosen = match source {
        AuthoritativeSource::ProviderNumber => row.provider_number,
        AuthoritativeSource::LabelNumber => row.label_number,
    };
    let Some(number) = chosen.filter(|value| *value >= 1) else {
        return row.per_row.clone();
    };

    let kind = row.per_row.kind;
    let (display_ko, floor_index) = match kind {
        BuildingRegisterFloorKind::AboveGround => {
            (format!("지상 {number}층"), i16::try_from(number).ok())
        }
        BuildingRegisterFloorKind::Basement => (
            format!("지하 {number}층"),
            i16::try_from(number).ok().map(|value| -value),
        ),
        _ => return row.per_row.clone(),
    };

    NormalizedBuildingRegisterFloor {
        kind,
        floor_number: Some(number),
        floor_index,
        display_ko: Some(display_ko),
        status: BuildingRegisterFloorNormalizationStatus::Accepted,
        reason: BuildingRegisterFloorNormalizationReason::ResolvedByBuildingWitnessMajority,
    }
}

#[cfg(test)]
mod tests;
