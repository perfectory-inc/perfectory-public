//! hub.go.kr 표제부 (building-register main) title floor-count parsing.
//!
//! The 표제부 register carries one row per building (동) with the authoritative
//! 지상층수 / 지하층수 counts. These counts are the independent third witness used
//! by [`foundation_normalization_domain::resolve_building_floors`] to break num-vs-label ties that
//! the floor rows alone cannot settle.

use std::collections::HashMap;

use foundation_normalization_domain::BuildingFloorCounts;

/// Provider management key column (shared with 층별개요 for the 동-level join).
const MGM_BLDRGST_PK_INDEX: usize = 0;
/// 주부속구분명 column (`주건축물` / `부속건축물`).
const MAIN_ANNEX_KIND_INDEX: usize = 24;
/// 호수 column (unit count on the title card; `0` is meaningful — no units).
/// The card can be unfilled (`0`) or stale, so consumers must treat it as
/// supporting evidence rather than an authoritative unit count.
const TITLE_UNIT_COUNT_INDEX: usize = 40;
/// 지상층수 column (above-ground floor count, excludes 옥탑).
const GROUND_FLOOR_COUNT_INDEX: usize = 43;
/// 지하층수 column (basement floor count).
const BASEMENT_FLOOR_COUNT_INDEX: usize = 44;
const MIN_FIELD_COUNT: usize = BASEMENT_FLOOR_COUNT_INDEX + 1;
/// PK and register kind are the minimum identity fields.
const MIN_LINK_FIELD_COUNT: usize = 4;

/// Parses one hub.go.kr 표제부 (`mart_djy_03`) TXT line into the building management
/// key and its title floor counts.
///
/// The provider file is UTF-8 pipe-delimited text with no header. Returns `None`
/// when the line is too short or the management key is empty. Each count is
/// best-effort: an empty, non-numeric, or zero count becomes `None` so it never
/// forces a spurious match in downstream resolution.
#[must_use]
pub fn parse_building_title_floor_counts_from_hub_bulk_text_line(
    line: &str,
) -> Option<(String, BuildingFloorCounts)> {
    let fields = line.split('|').collect::<Vec<_>>();
    if fields.len() < MIN_FIELD_COUNT {
        return None;
    }
    let mgm_bldrgst_pk = fields[MGM_BLDRGST_PK_INDEX].trim();
    if mgm_bldrgst_pk.is_empty() {
        return None;
    }
    let counts = BuildingFloorCounts {
        above_ground: parse_positive_count(fields[GROUND_FLOOR_COUNT_INDEX]),
        below_ground: parse_positive_count(fields[BASEMENT_FLOOR_COUNT_INDEX]),
    };
    Some((mgm_bldrgst_pk.to_owned(), counts))
}

/// Parses a floor count, keeping only positive values. Zero means "no floors of
/// this kind" and is treated as absent so it never matches an observed sequence.
fn parse_positive_count(raw: &str) -> Option<u16> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    value.parse::<u16>().ok().filter(|count| *count >= 1)
}

/// How a 호 was linked to its building.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingLink {
    /// 표제부 management key of the building, when resolved.
    pub building_mgm_bldrgst_pk: Option<String>,
    /// How the link was made.
    pub method: &'static str,
    /// Raw 주부속구분명 of the linked building (`주건축물` / `부속건축물`).
    pub building_main_or_annex: Option<String>,
    /// Unit count on the linked building's title card (호수; `0` = no units).
    pub building_title_unit_count: Option<u32>,
}

/// One 표제부 line reduced to its building-link entry plus title attributes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingTitleLinkEntry {
    /// Provider register kind; a unit parent must be a collective-building title (`3`).
    pub register_kind_code: String,
    /// 표제부 management key.
    pub mgm_bldrgst_pk: String,
    /// Raw 주부속구분명, when present.
    pub main_or_annex: Option<String>,
    /// 호수 on the title card; blank/non-numeric → `None`, `0` stays `Some(0)`.
    pub title_unit_count: Option<u32>,
}

/// 중복 PK는 하나로 보되, 다른 PK가 관측된 키는 이후에도 모호한 상태를 유지한다.
#[derive(Debug)]
enum BuildingKeyCandidate<T> {
    Unique(T),
    Ambiguous,
}

impl<T: PartialEq> BuildingKeyCandidate<T> {
    fn observe(&mut self, pk: &T) {
        if matches!(self, Self::Unique(existing) if existing != pk) {
            *self = Self::Ambiguous;
        }
    }

    const fn value(&self) -> Option<&T> {
        match self {
            Self::Unique(value) => Some(value),
            Self::Ambiguous => None,
        }
    }
}

#[derive(Debug)]
struct TitleAttributes {
    register_kind: BuildingKeyCandidate<String>,
    main_or_annex: Option<BuildingKeyCandidate<String>>,
    unit_count: Option<BuildingKeyCandidate<u32>>,
}

fn observe_optional<T: PartialEq>(slot: &mut Option<BuildingKeyCandidate<T>>, value: Option<T>) {
    if let Some(value) = value {
        match slot {
            Some(candidate) => candidate.observe(&value),
            None => *slot = Some(BuildingKeyCandidate::Unique(value)),
        }
    }
}

/// Source title index keyed only by opaque register PK. Names and parcel proximity cannot resolve a parent.
#[derive(Debug, Default)]
pub struct BuildingTitleKeyIndex {
    /// Repeated nonempty observations must agree, independently of input order.
    attrs_by_pk: HashMap<String, TitleAttributes>,
}

impl BuildingTitleKeyIndex {
    /// Creates an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct title register keys, including conflicting observations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.attrs_by_pk.len()
    }

    /// Whether the index has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.attrs_by_pk.is_empty()
    }

    /// Observes a title register PK and its source attributes.
    /// 같은 PK의 모순된 부가속성은 비워 두며 입력 순서에 의존하지 않는다.
    pub fn insert(&mut self, entry: BuildingTitleLinkEntry) {
        let BuildingTitleLinkEntry {
            register_kind_code,
            mgm_bldrgst_pk,
            main_or_annex,
            title_unit_count,
        } = entry;
        match self.attrs_by_pk.entry(mgm_bldrgst_pk) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let attrs = entry.get_mut();
                attrs.register_kind.observe(&register_kind_code);
                observe_optional(&mut attrs.main_or_annex, main_or_annex);
                observe_optional(&mut attrs.unit_count, title_unit_count);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(TitleAttributes {
                    register_kind: BuildingKeyCandidate::Unique(register_kind_code),
                    main_or_annex: main_or_annex.map(BuildingKeyCandidate::Unique),
                    unit_count: title_unit_count.map(BuildingKeyCandidate::Unique),
                });
            }
        }
    }

    fn link_for(&self, pk: &str, method: &'static str) -> BuildingLink {
        let attrs = self.attrs_by_pk.get(pk);
        let main_or_annex = attrs
            .and_then(|attrs| attrs.main_or_annex.as_ref())
            .and_then(BuildingKeyCandidate::value)
            .cloned();
        let title_unit_count = attrs
            .and_then(|attrs| attrs.unit_count.as_ref())
            .and_then(BuildingKeyCandidate::value)
            .copied();
        BuildingLink {
            building_mgm_bldrgst_pk: Some(pk.to_owned()),
            method,
            building_main_or_annex: main_or_annex,
            building_title_unit_count: title_unit_count,
        }
    }

    /// Resolves an explicit source parent only to a consistent collective-building title.
    ///
    /// # Errors
    /// Returns a machine-readable reason when the title is missing or has an invalid kind.
    pub fn resolve_parent_pk(&self, pk: &str) -> Result<BuildingLink, &'static str> {
        self.checked_link_for(pk, "parent_key")
    }

    fn checked_link_for(
        &self,
        pk: &str,
        method: &'static str,
    ) -> Result<BuildingLink, &'static str> {
        let attrs = self.attrs_by_pk.get(pk).ok_or("parent_title_missing")?;
        if attrs.register_kind.value().map(String::as_str) != Some("3") {
            return Err("parent_title_kind_mismatch");
        }
        Ok(self.link_for(pk, method))
    }
}

/// Parses one hub.go.kr 표제부 TXT line into a building-link entry with title
/// attributes, or `None` when it is too short.
///
/// The attribute columns beyond the link columns are best-effort: short lines
/// yield `None` attributes.
#[must_use]
pub fn parse_building_title_building_link_from_hub_bulk_text_line(
    line: &str,
) -> Option<BuildingTitleLinkEntry> {
    let fields = line.split('|').collect::<Vec<_>>();
    if fields.len() < MIN_LINK_FIELD_COUNT {
        return None;
    }
    let mgm_bldrgst_pk = fields[MGM_BLDRGST_PK_INDEX].trim();
    if mgm_bldrgst_pk.is_empty() {
        return None;
    }
    let main_or_annex = fields
        .get(MAIN_ANNEX_KIND_INDEX)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let title_unit_count = fields
        .get(TITLE_UNIT_COUNT_INDEX)
        .map(|value| value.trim())
        .and_then(|value| value.parse::<u32>().ok());
    Some(BuildingTitleLinkEntry {
        register_kind_code: fields[3].trim().to_owned(),
        mgm_bldrgst_pk: mgm_bldrgst_pk.to_owned(),
        main_or_annex,
        title_unit_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line_with(pk: &str, ground: &str, basement: &str) -> String {
        let mut fields = vec![String::new(); MIN_FIELD_COUNT];
        fields[MGM_BLDRGST_PK_INDEX] = pk.to_owned();
        fields[GROUND_FLOOR_COUNT_INDEX] = ground.to_owned();
        fields[BASEMENT_FLOOR_COUNT_INDEX] = basement.to_owned();
        fields.join("|")
    }

    #[test]
    fn parses_ground_and_basement_counts() -> Result<(), &'static str> {
        let line = line_with("100211753", "14", "6");
        let (pk, counts) = parse_building_title_floor_counts_from_hub_bulk_text_line(&line)
            .ok_or("valid title line should parse floor counts")?;
        assert_eq!(pk, "100211753");
        assert_eq!(counts.above_ground, Some(14));
        assert_eq!(counts.below_ground, Some(6));
        Ok(())
    }

    #[test]
    fn treats_zero_and_empty_counts_as_absent() -> Result<(), &'static str> {
        let line = line_with("1002121184", "2", "0");
        let (_, counts) = parse_building_title_floor_counts_from_hub_bulk_text_line(&line)
            .ok_or("zero basement count line should parse")?;
        assert_eq!(counts.above_ground, Some(2));
        assert_eq!(counts.below_ground, None);

        let blank = line_with("1002121184", "", "");
        let (_, blank_counts) = parse_building_title_floor_counts_from_hub_bulk_text_line(&blank)
            .ok_or("blank floor count line should parse")?;
        assert_eq!(blank_counts.above_ground, None);
        assert_eq!(blank_counts.below_ground, None);
        Ok(())
    }

    #[test]
    fn rejects_short_lines_and_empty_keys() {
        assert!(parse_building_title_floor_counts_from_hub_bulk_text_line("100|1|2").is_none());
        let empty_key = line_with("", "3", "1");
        assert!(parse_building_title_floor_counts_from_hub_bulk_text_line(&empty_key).is_none());
    }

    fn entry(pk: &str) -> BuildingTitleLinkEntry {
        BuildingTitleLinkEntry {
            register_kind_code: "3".to_owned(),
            mgm_bldrgst_pk: pk.to_owned(),
            main_or_annex: None,
            title_unit_count: None,
        }
    }

    #[test]
    fn link_parse_carries_annex_kind_and_title_unit_count() -> Result<(), &'static str> {
        let mut fields = vec![String::new(); 45];
        fields[MGM_BLDRGST_PK_INDEX] = "100211753".to_owned();
        fields[MAIN_ANNEX_KIND_INDEX] = "부속건축물".to_owned();
        fields[TITLE_UNIT_COUNT_INDEX] = "0".to_owned();
        let line = fields.join("|");

        let entry = parse_building_title_building_link_from_hub_bulk_text_line(&line)
            .ok_or("valid title line should parse a link entry")?;
        assert_eq!(entry.mgm_bldrgst_pk, "100211753");
        assert_eq!(entry.main_or_annex.as_deref(), Some("부속건축물"));
        // "0" is meaningful here (no units) — must stay Some(0), not None.
        assert_eq!(entry.title_unit_count, Some(0));

        // Short line (link columns only): attrs are best-effort None.
        let short = line
            .split('|')
            .take(MIN_LINK_FIELD_COUNT)
            .collect::<Vec<_>>()
            .join("|");
        let short_entry = parse_building_title_building_link_from_hub_bulk_text_line(&short)
            .ok_or("short line with link columns should still parse")?;
        assert_eq!(short_entry.main_or_annex, None);
        assert_eq!(short_entry.title_unit_count, None);
        Ok(())
    }

    #[test]
    fn explicit_parent_requires_consistent_title_kind_three() {
        for kind in ["", "1", "2", "4"] {
            let mut index = BuildingTitleKeyIndex::new();
            let mut title = entry("parent");
            title.register_kind_code = kind.to_owned();
            index.insert(title);
            assert_eq!(
                index.resolve_parent_pk("parent"),
                Err("parent_title_kind_mismatch")
            );
        }
        for kinds in [["3", "2", "3"], ["2", "3", "3"]] {
            let mut index = BuildingTitleKeyIndex::new();
            for kind in kinds {
                let mut title = entry("parent");
                title.register_kind_code = kind.to_owned();
                index.insert(title);
            }
            assert_eq!(
                index.resolve_parent_pk("parent"),
                Err("parent_title_kind_mismatch")
            );
            assert_eq!(
                index.resolve_parent_pk("absent"),
                Err("parent_title_missing")
            );
        }
    }

    #[test]
    fn repeated_title_attributes_are_order_independent_and_conflicts_stay_null(
    ) -> Result<(), &'static str> {
        for reverse in [false, true] {
            let mut observations = [
                (Some("main"), Some(20)),
                (None, None),
                (Some("annex"), Some(30)),
                (Some("main"), Some(20)),
            ];
            if reverse {
                observations.reverse();
            }
            let mut index = BuildingTitleKeyIndex::new();
            for (annex, count) in observations {
                let mut title = entry("parent");
                title.main_or_annex = annex.map(str::to_owned);
                title.title_unit_count = count;
                index.insert(title);
            }
            let direct = index.resolve_parent_pk("parent")?;
            assert_eq!(direct.building_mgm_bldrgst_pk.as_deref(), Some("parent"));
            assert_eq!(direct.building_main_or_annex, None);
            assert_eq!(direct.building_title_unit_count, None);
        }
        for observations in [[None, Some(0)], [Some(0), None]] {
            let mut index = BuildingTitleKeyIndex::new();
            for count in observations {
                let mut title = entry("parent");
                title.title_unit_count = count;
                index.insert(title);
            }
            assert_eq!(
                index.resolve_parent_pk("parent")?.building_title_unit_count,
                Some(0)
            );
        }
        Ok(())
    }
}
