//! hub.go.kr 표제부 (building-register main) title floor-count parsing.
//!
//! The 표제부 register carries one row per building (동) with the authoritative
//! 지상층수 / 지하층수 counts. These counts are the independent third witness used
//! by [`foundation_normalization_domain::resolve_building_floors`] to break num-vs-label ties that
//! the floor rows alone cannot settle.

use std::collections::HashMap;

use foundation_normalization_domain::{canonical_dong_join_key, BuildingFloorCounts};
use foundation_shared_kernel::pnu::hub_register_parcel_key;

/// Provider management key column (shared with 층별개요 for the 동-level join).
const MGM_BLDRGST_PK_INDEX: usize = 0;
/// PNU code columns (시군구/법정동/대지구분/본번/부번).
const PNU_SIGUNGU_INDEX: usize = 8;
const PNU_BEOPJEONGDONG_INDEX: usize = 9;
const PNU_DAEJI_KIND_INDEX: usize = 10;
const PNU_BONBEON_INDEX: usize = 11;
const PNU_BUBEON_INDEX: usize = 12;
/// 동명칭 column.
const DONG_NAME_INDEX: usize = 22;
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
/// Minimum columns needed to extract the building link (PK + PNU + 동명).
const MIN_LINK_FIELD_COUNT: usize = DONG_NAME_INDEX + 1;

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
    /// Register-internal parcel key (hub-native composition; **not** a PNU —
    /// ADR 0023). Total even for block parcels, so 전유부↔표제부 links never break.
    pub register_parcel_key: String,
    /// Canonical 동 join key.
    pub canonical_dong: String,
    /// 표제부 management key.
    pub mgm_bldrgst_pk: String,
    /// Raw 주부속구분명, when present.
    pub main_or_annex: Option<String>,
    /// 호수 on the title card; blank/non-numeric → `None`, `0` stays `Some(0)`.
    pub title_unit_count: Option<u32>,
}

/// Index of 표제부 buildings for linking 전유부 호 to their building by
/// `(PNU + canonical 동명)`, with a single-building fallback for nameless 동.
#[derive(Debug, Default)]
pub struct BuildingTitleKeyIndex {
    by_parcel_dong: HashMap<(String, String), String>,
    /// `register_parcel_key` -> (distinct 동 count, pk when exactly one).
    single_building_by_parcel: HashMap<String, (u32, Option<String>)>,
    /// pk -> (주부속구분명, title 호수). First entry per pk wins.
    attrs_by_pk: HashMap<String, (Option<String>, Option<u32>)>,
}

impl BuildingTitleKeyIndex {
    /// Creates an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of `(PNU, 동명)` entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_parcel_dong.len()
    }

    /// Whether the index has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_parcel_dong.is_empty()
    }

    /// Inserts one 표제부 building. The first management key seen for a
    /// `(PNU, canonical 동명)` wins, as does the first attribute set per key.
    pub fn insert(&mut self, entry: BuildingTitleLinkEntry) {
        let BuildingTitleLinkEntry {
            register_parcel_key,
            canonical_dong,
            mgm_bldrgst_pk,
            main_or_annex,
            title_unit_count,
        } = entry;
        self.attrs_by_pk
            .entry(mgm_bldrgst_pk.clone())
            .or_insert((main_or_annex, title_unit_count));
        let key = (register_parcel_key.clone(), canonical_dong);
        let is_new_dong = !self.by_parcel_dong.contains_key(&key);
        self.by_parcel_dong
            .entry(key)
            .or_insert_with(|| mgm_bldrgst_pk.clone());
        if is_new_dong {
            let entry = self
                .single_building_by_parcel
                .entry(register_parcel_key)
                .or_insert((0, None));
            entry.0 += 1;
            entry.1 = if entry.0 == 1 {
                Some(mgm_bldrgst_pk)
            } else {
                None
            };
        }
    }

    fn link_for(&self, pk: &str, method: &'static str) -> BuildingLink {
        let (main_or_annex, title_unit_count) =
            self.attrs_by_pk.get(pk).cloned().unwrap_or((None, None));
        BuildingLink {
            building_mgm_bldrgst_pk: Some(pk.to_owned()),
            method,
            building_main_or_annex: main_or_annex,
            building_title_unit_count: title_unit_count,
        }
    }

    /// Resolves a 호's building via `(register parcel key + canonical 동명)`,
    /// falling back to the single building on the parcel when the 동명 does not
    /// match. Keys are the hub-native parcel composition, not standard PNUs.
    #[must_use]
    pub fn resolve(&self, register_parcel_key: &str, dong_name: &str) -> BuildingLink {
        let canonical = canonical_dong_join_key(dong_name);
        if !canonical.is_empty() {
            if let Some(pk) = self
                .by_parcel_dong
                .get(&(register_parcel_key.to_owned(), canonical))
            {
                return self.link_for(pk, "canonical_dong");
            }
        }
        match self.single_building_by_parcel.get(register_parcel_key) {
            Some((1, Some(pk))) => self.link_for(pk, "single_building_fallback"),
            _ => BuildingLink {
                building_mgm_bldrgst_pk: None,
                method: "unresolved",
                building_main_or_annex: None,
                building_title_unit_count: None,
            },
        }
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
    let register_parcel_key = hub_register_parcel_key(
        fields[PNU_SIGUNGU_INDEX],
        fields[PNU_BEOPJEONGDONG_INDEX],
        fields[PNU_DAEJI_KIND_INDEX],
        fields[PNU_BONBEON_INDEX],
        fields[PNU_BUBEON_INDEX],
    );
    let canonical_dong = canonical_dong_join_key(fields[DONG_NAME_INDEX].trim());
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
        register_parcel_key,
        canonical_dong,
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

    fn entry(parcel_key: &str, dong: &str, pk: &str) -> BuildingTitleLinkEntry {
        BuildingTitleLinkEntry {
            register_parcel_key: parcel_key.to_owned(),
            canonical_dong: dong.to_owned(),
            mgm_bldrgst_pk: pk.to_owned(),
            main_or_annex: None,
            title_unit_count: None,
        }
    }

    #[test]
    fn link_parse_carries_annex_kind_and_title_unit_count() -> Result<(), &'static str> {
        let mut fields = vec![String::new(); 45];
        fields[MGM_BLDRGST_PK_INDEX] = "100211753".to_owned();
        fields[PNU_SIGUNGU_INDEX] = "99999".to_owned();
        fields[PNU_BEOPJEONGDONG_INDEX] = "01101".to_owned();
        fields[PNU_DAEJI_KIND_INDEX] = "0".to_owned();
        fields[PNU_BONBEON_INDEX] = "0734".to_owned();
        fields[PNU_BUBEON_INDEX] = "0000".to_owned();
        fields[DONG_NAME_INDEX] = "301동".to_owned();
        fields[MAIN_ANNEX_KIND_INDEX] = "부속건축물".to_owned();
        fields[TITLE_UNIT_COUNT_INDEX] = "0".to_owned();
        let line = fields.join("|");

        let entry = parse_building_title_building_link_from_hub_bulk_text_line(&line)
            .ok_or("valid title line should parse a link entry")?;
        // 내부 조인 키는 허브 조립 그대로 유지 (표준 PNU 아님 — ADR 0023).
        assert_eq!(entry.register_parcel_key, "9999901101007340000");
        assert_eq!(entry.canonical_dong, "301");
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
    fn resolve_carries_building_title_attrs() {
        let mut index = BuildingTitleKeyIndex::new();
        let mut annex = entry("pnuA", "301", "pkA3");
        annex.main_or_annex = Some("부속건축물".to_owned());
        annex.title_unit_count = Some(0);
        index.insert(annex);

        let hit = index.resolve("pnuA", "301동");
        assert_eq!(hit.building_mgm_bldrgst_pk.as_deref(), Some("pkA3"));
        assert_eq!(hit.building_main_or_annex.as_deref(), Some("부속건축물"));
        assert_eq!(hit.building_title_unit_count, Some(0));

        let miss = index.resolve("pnuZ", "1동");
        assert_eq!(miss.building_main_or_annex, None);
        assert_eq!(miss.building_title_unit_count, None);
    }

    #[test]
    fn building_index_links_by_canonical_dong_and_single_fallback() {
        let mut index = BuildingTitleKeyIndex::new();
        // Parcel A: two buildings 101동 / 102동.
        index.insert(entry("pnuA", "101", "pkA1"));
        index.insert(entry("pnuA", "102", "pkA2"));
        // Parcel B: one nameless building.
        index.insert(entry("pnuB", "", "pkB"));

        // Canonical 동명 match ("제 101동" -> "101").
        let hit = index.resolve("pnuA", "제 101동");
        assert_eq!(hit.building_mgm_bldrgst_pk.as_deref(), Some("pkA1"));
        assert_eq!(hit.method, "canonical_dong");

        // Nameless 동 on a single-building parcel falls back.
        let fallback = index.resolve("pnuB", "가동");
        assert_eq!(fallback.building_mgm_bldrgst_pk.as_deref(), Some("pkB"));
        assert_eq!(fallback.method, "single_building_fallback");

        // Non-matching 동 on a multi-building parcel is unresolved.
        let miss = index.resolve("pnuA", "307동");
        assert_eq!(miss.building_mgm_bldrgst_pk, None);
        assert_eq!(miss.method, "unresolved");
    }
}
