//! 표제부 (building-register title) Bronze-to-Silver normalization plan (root ADR-0073).
//!
//! One row per building (동), main and annex alike. The 표제부 is the building's own card:
//! use, structure, floor counts, total floor area, and the approval date — the facts
//! `catalog.building` projects. The floor/unit/area plans treat this register as a witness;
//! this plan treats it as the subject.
//!
//! Field indexes were measured off the real July 2026 national snapshot (77 pipe fields,
//! 8,051,204 rows), and the two area candidates were disambiguated by property, not guessed:
//! an 11-floor tower shows field 28 = 12,877 = floors x field 26 (footprint), so field 28 is
//! the total floor area (root ADR-0073).

use chrono::{DateTime, Utc};
use foundation_shared_kernel::pnu::{
    hub_register_parcel_key, standard_pnu_from_hub_register_codes,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::building_register_row_identity::row_identity;

// Shared with `building_register_title.rs` (the witness reader) by value, not by import:
// both were measured off the same file, and the parity test below keeps them agreeing.
const MGM_BLDRGST_PK_INDEX: usize = 0;
const REGISTER_KIND_NAME_INDEX: usize = 2;
const REGISTER_TYPE_NAME_INDEX: usize = 4;
const JIBUN_ADDRESS_INDEX: usize = 5;
const ROAD_ADDRESS_INDEX: usize = 6;
const PNU_SIGUNGU_INDEX: usize = 8;
const PNU_BEOPJEONGDONG_INDEX: usize = 9;
const PNU_DAEJI_KIND_INDEX: usize = 10;
const PNU_BONBEON_INDEX: usize = 11;
const PNU_BUBEON_INDEX: usize = 12;
const DONG_NAME_INDEX: usize = 22;
const MAIN_ANNEX_KIND_INDEX: usize = 24;
const BUILDING_AREA_INDEX: usize = 26;
const FLOOR_AREA_INDEX: usize = 28;
const STRUCTURE_CODE_INDEX: usize = 31;
const STRUCTURE_NAME_INDEX: usize = 32;
const PURPOSE_CODE_INDEX: usize = 34;
const PURPOSE_NAME_INDEX: usize = 35;
const PURPOSE_DETAIL_INDEX: usize = 36;
const ROOF_CODE_INDEX: usize = 37;
const ROOF_NAME_INDEX: usize = 38;
const TITLE_UNIT_COUNT_INDEX: usize = 40;
const GROUND_FLOOR_COUNT_INDEX: usize = 43;
const BASEMENT_FLOOR_COUNT_INDEX: usize = 44;
const APPROVAL_DATE_INDEX: usize = 60;
const MIN_FIELD_COUNT: usize = APPROVAL_DATE_INDEX + 1;

/// Parsed 표제부 source row, one per provider line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterTitleSourceRow {
    /// Stable row-level source lineage id (the Bronze object key).
    pub source_record_id: String,
    /// Provider building management primary key.
    pub mgm_bldrgst_pk: String,
    /// Standard 19-digit PNU (대지구분 1/2); `None` for block parcels (ADR 0023).
    pub pnu: Option<String>,
    /// Register-internal parcel key (hub-native composition; not a PNU).
    pub register_parcel_key: String,
    /// Raw 동명칭.
    pub dong_name_raw: String,
    /// Raw 주부속구분명 (`주건축물` / `부속건축물` / empty).
    pub main_or_annex_name_raw: String,
    /// Raw 대장구분명.
    pub register_kind_name_raw: String,
    /// Raw 대장종류명.
    pub register_type_name_raw: String,
    /// Raw 대지위치 (지번 주소 문장).
    pub jibun_address_raw: String,
    /// Raw 도로명 주소 문장.
    pub road_address_raw: String,
    /// Raw 구조 code.
    pub structure_code_raw: String,
    /// Raw 구조명.
    pub structure_name_raw: String,
    /// Raw 주용도 code.
    pub purpose_code_raw: String,
    /// Raw 주용도명.
    pub purpose_name_raw: String,
    /// Raw 기타용도.
    pub purpose_detail_raw: String,
    /// Raw 지붕 code.
    pub roof_code_raw: String,
    /// Raw 지붕명.
    pub roof_name_raw: String,
    /// Raw 건축면적 text.
    pub building_area_raw: String,
    /// Raw 연면적 text.
    pub floor_area_raw: String,
    /// Raw 지상층수 text.
    pub ground_floor_raw: String,
    /// Raw 지하층수 text.
    pub basement_floor_raw: String,
    /// Raw 호수 text (title-card unit count; evidence, not gospel).
    pub title_unit_count_raw: String,
    /// Raw 사용승인일 (`yyyymmdd`).
    pub approval_date_raw: String,
    /// 1-based source line number inside the Bronze object.
    pub source_line_number: Option<u64>,
}

/// Input required to normalize 표제부 source rows into Silver rows.
pub struct BuildingRegisterTitleSilverRowsInput<'a> {
    /// Parsed provider source rows ordered by the caller.
    pub records: &'a [BuildingRegisterTitleSourceRow],
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// Bronze object key that carried these source rows.
    pub bronze_object_key: &'a str,
    /// UTC timestamp from which these source facts are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.building_register_titles` row prepared from one source row.
#[derive(Clone, Debug, PartialEq)]
pub struct BuildingRegisterTitleSilverRow {
    /// Stable Silver row id.
    pub title_row_id: String,
    /// Provider building management primary key.
    pub mgm_bldrgst_pk: String,
    /// Standard 19-digit PNU; null for block parcels (ADR 0023).
    pub pnu: Option<String>,
    /// Register-internal parcel key.
    pub register_parcel_key: String,
    /// Raw 동명칭.
    pub dong_name_raw: String,
    /// Canonical main/annex wire value: `main`, `annex`, or `unknown`.
    pub main_or_annex_kind: String,
    /// Raw 주부속구분명.
    pub main_or_annex_name_raw: String,
    /// Raw 대장구분명.
    pub register_kind_name_raw: String,
    /// Raw 대장종류명.
    pub register_type_name_raw: String,
    /// Raw 대지위치.
    pub jibun_address_raw: String,
    /// Raw 도로명 주소.
    pub road_address_raw: String,
    /// Raw 구조 code.
    pub structure_code_raw: String,
    /// Raw 구조명.
    pub structure_name_raw: String,
    /// Raw 주용도 code.
    pub purpose_code_raw: String,
    /// Raw 주용도명.
    pub purpose_name_raw: String,
    /// Raw 기타용도.
    pub purpose_detail_raw: String,
    /// Raw 지붕 code.
    pub roof_code_raw: String,
    /// Raw 지붕명.
    pub roof_name_raw: String,
    /// 건축면적 in ㎡; `None` when unstated or non-positive.
    pub building_area_m2: Option<f64>,
    /// 연면적 in ㎡; `None` when unstated or non-positive.
    pub floor_area_m2: Option<f64>,
    /// 지상층수; `None` when unparsable.
    pub ground_floor_count: Option<u16>,
    /// 지하층수; `None` when unparsable.
    pub basement_floor_count: Option<u16>,
    /// 호수 on the title card; `Some(0)` is meaningful (no units).
    pub title_unit_count: Option<u32>,
    /// Raw 사용승인일 (`yyyymmdd`).
    pub approval_date_raw: String,
    /// Year extracted from the approval date when it parses as a plausible date.
    pub approval_year: Option<i32>,
    /// Normalization status wire value.
    pub normalization_status: String,
    /// Normalization reason wire value.
    pub normalization_reason: String,
    /// Source-snapshot lineage id.
    pub source_snapshot_id: String,
    /// Bronze object key that carried this source row.
    pub bronze_object_key: String,
    /// 1-based source line number inside the Bronze object.
    pub source_line_number: Option<u64>,
    /// UTC timestamp from which this fact is valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when this row entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
    /// Lowercase SHA-256 checksum of the row payload excluding this checksum field.
    pub row_checksum_sha256: String,
}

/// Error returned while normalizing 표제부 rows into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BuildingRegisterTitleSilverPlanError {
    /// Input data cannot be represented as a Silver 표제부 row.
    #[error("invalid building-register title Silver input: {0}")]
    InvalidInput(String),
}

/// Parses one hub.go.kr 표제부 (`mart_djy_03`) TXT line into a Silver source row.
///
/// # Errors
/// Returns an error when lineage is invalid, the line has fewer fields than the approval-date
/// column requires, or the management key is empty.
pub fn parse_building_register_title_source_row_from_hub_bulk_text_line(
    line: &str,
    bronze_object_key: &str,
    one_based_line_number: u64,
) -> Result<BuildingRegisterTitleSourceRow, BuildingRegisterTitleSilverPlanError> {
    if bronze_object_key.trim().is_empty() {
        return Err(BuildingRegisterTitleSilverPlanError::InvalidInput(
            "bronze_object_key must not be empty".to_owned(),
        ));
    }
    if one_based_line_number == 0 {
        return Err(BuildingRegisterTitleSilverPlanError::InvalidInput(
            "source line number must be 1-based".to_owned(),
        ));
    }

    let fields = line.split('|').collect::<Vec<_>>();
    if fields.len() < MIN_FIELD_COUNT {
        return Err(BuildingRegisterTitleSilverPlanError::InvalidInput(format!(
            "hub.go.kr 표제부 line {one_based_line_number} has {} fields, expected at least {MIN_FIELD_COUNT}",
            fields.len()
        )));
    }

    let mgm_bldrgst_pk = fields[MGM_BLDRGST_PK_INDEX].trim();
    if mgm_bldrgst_pk.is_empty() {
        return Err(BuildingRegisterTitleSilverPlanError::InvalidInput(
            "hub.go.kr 표제부 management key must not be empty".to_owned(),
        ));
    }

    Ok(BuildingRegisterTitleSourceRow {
        source_record_id: bronze_object_key.to_owned(),
        mgm_bldrgst_pk: mgm_bldrgst_pk.to_owned(),
        pnu: standard_pnu_from_hub_register_codes(
            fields[PNU_SIGUNGU_INDEX],
            fields[PNU_BEOPJEONGDONG_INDEX],
            fields[PNU_DAEJI_KIND_INDEX],
            fields[PNU_BONBEON_INDEX],
            fields[PNU_BUBEON_INDEX],
        ),
        register_parcel_key: hub_register_parcel_key(
            fields[PNU_SIGUNGU_INDEX],
            fields[PNU_BEOPJEONGDONG_INDEX],
            fields[PNU_DAEJI_KIND_INDEX],
            fields[PNU_BONBEON_INDEX],
            fields[PNU_BUBEON_INDEX],
        ),
        dong_name_raw: fields[DONG_NAME_INDEX].trim().to_owned(),
        main_or_annex_name_raw: fields[MAIN_ANNEX_KIND_INDEX].trim().to_owned(),
        register_kind_name_raw: fields[REGISTER_KIND_NAME_INDEX].trim().to_owned(),
        register_type_name_raw: fields[REGISTER_TYPE_NAME_INDEX].trim().to_owned(),
        jibun_address_raw: fields[JIBUN_ADDRESS_INDEX].trim().to_owned(),
        road_address_raw: fields[ROAD_ADDRESS_INDEX].trim().to_owned(),
        structure_code_raw: fields[STRUCTURE_CODE_INDEX].trim().to_owned(),
        structure_name_raw: fields[STRUCTURE_NAME_INDEX].trim().to_owned(),
        purpose_code_raw: fields[PURPOSE_CODE_INDEX].trim().to_owned(),
        purpose_name_raw: fields[PURPOSE_NAME_INDEX].trim().to_owned(),
        purpose_detail_raw: fields[PURPOSE_DETAIL_INDEX].trim().to_owned(),
        roof_code_raw: fields[ROOF_CODE_INDEX].trim().to_owned(),
        roof_name_raw: fields[ROOF_NAME_INDEX].trim().to_owned(),
        building_area_raw: fields[BUILDING_AREA_INDEX].trim().to_owned(),
        floor_area_raw: fields[FLOOR_AREA_INDEX].trim().to_owned(),
        ground_floor_raw: fields[GROUND_FLOOR_COUNT_INDEX].trim().to_owned(),
        basement_floor_raw: fields[BASEMENT_FLOOR_COUNT_INDEX].trim().to_owned(),
        title_unit_count_raw: fields[TITLE_UNIT_COUNT_INDEX].trim().to_owned(),
        approval_date_raw: fields[APPROVAL_DATE_INDEX].trim().to_owned(),
        source_line_number: Some(one_based_line_number),
    })
}

/// Normalizes 표제부 source rows into Silver rows.
///
/// # Errors
/// Returns an error when required lineage is empty or checksum serialization fails.
pub fn normalize_building_register_title_silver_rows(
    input: &BuildingRegisterTitleSilverRowsInput<'_>,
) -> Result<Vec<BuildingRegisterTitleSilverRow>, BuildingRegisterTitleSilverPlanError> {
    if input.source_snapshot_id.trim().is_empty() {
        return Err(BuildingRegisterTitleSilverPlanError::InvalidInput(
            "source_snapshot_id must not be empty".to_owned(),
        ));
    }
    if input.bronze_object_key.trim().is_empty() {
        return Err(BuildingRegisterTitleSilverPlanError::InvalidInput(
            "bronze_object_key must not be empty".to_owned(),
        ));
    }
    input
        .records
        .iter()
        .map(|record| build_silver_row(record, input))
        .collect()
}

/// Serializes one Silver 표제부 row into its canonical JSONL line.
///
/// # Errors
/// Returns an error when JSON serialization fails.
pub fn building_register_title_silver_row_to_jsonl(
    row: &BuildingRegisterTitleSilverRow,
) -> Result<String, BuildingRegisterTitleSilverPlanError> {
    serde_json::to_string(&row_to_json_value(row))
        .map_err(|error| BuildingRegisterTitleSilverPlanError::InvalidInput(error.to_string()))
}

/// `주건축물`/`부속건축물` to the wire values the contract's quality gate allows.
fn main_or_annex_wire(raw: &str) -> &'static str {
    match raw {
        "주건축물" => "main",
        "부속건축물" => "annex",
        _ => "unknown",
    }
}

/// Parses a raw area text into a positive finite ㎡ value.
///
/// Zero is not an area a standing building has: the register writes `0` where it states
/// nothing, and carrying it forward would let a zero-area building look measured.
fn parse_area_m2(raw: &str) -> Option<f64> {
    raw.parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn parse_floor_count(raw: &str) -> Option<u16> {
    raw.parse::<u16>().ok()
}

fn parse_title_unit_count(raw: &str) -> Option<u32> {
    raw.parse::<u32>().ok()
}

/// Extracts the year from a `yyyymmdd` approval date, refusing implausible centuries.
fn parse_approval_year(raw: &str) -> Option<i32> {
    if raw.len() != 8 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let year = raw[..4].parse::<i32>().ok()?;
    (1800..=2100).contains(&year).then_some(year)
}

fn build_silver_row(
    record: &BuildingRegisterTitleSourceRow,
    input: &BuildingRegisterTitleSilverRowsInput<'_>,
) -> Result<BuildingRegisterTitleSilverRow, BuildingRegisterTitleSilverPlanError> {
    let main_or_annex_kind = main_or_annex_wire(&record.main_or_annex_name_raw);
    // The kind decides the status: 552 of 8,051,204 measured rows state no 주부속구분, and a
    // row loaded as `unknown` says so rather than being guessed into `main`. Everything else
    // on the card is descriptive and absence is recorded per column, not per row.
    let (status, reason) = if main_or_annex_kind == "unknown" {
        ("accepted", "main_annex_unmarked")
    } else {
        ("accepted", "accepted_title")
    };

    let mut row = BuildingRegisterTitleSilverRow {
        title_row_id: row_identity(
            "building-register-title",
            record.source_record_id.as_str(),
            record.source_line_number,
        ),
        mgm_bldrgst_pk: record.mgm_bldrgst_pk.clone(),
        pnu: record.pnu.clone(),
        register_parcel_key: record.register_parcel_key.clone(),
        dong_name_raw: record.dong_name_raw.clone(),
        main_or_annex_kind: main_or_annex_kind.to_owned(),
        main_or_annex_name_raw: record.main_or_annex_name_raw.clone(),
        register_kind_name_raw: record.register_kind_name_raw.clone(),
        register_type_name_raw: record.register_type_name_raw.clone(),
        jibun_address_raw: record.jibun_address_raw.clone(),
        road_address_raw: record.road_address_raw.clone(),
        structure_code_raw: record.structure_code_raw.clone(),
        structure_name_raw: record.structure_name_raw.clone(),
        purpose_code_raw: record.purpose_code_raw.clone(),
        purpose_name_raw: record.purpose_name_raw.clone(),
        purpose_detail_raw: record.purpose_detail_raw.clone(),
        roof_code_raw: record.roof_code_raw.clone(),
        roof_name_raw: record.roof_name_raw.clone(),
        building_area_m2: parse_area_m2(&record.building_area_raw),
        floor_area_m2: parse_area_m2(&record.floor_area_raw),
        ground_floor_count: parse_floor_count(&record.ground_floor_raw),
        basement_floor_count: parse_floor_count(&record.basement_floor_raw),
        title_unit_count: parse_title_unit_count(&record.title_unit_count_raw),
        approval_date_raw: record.approval_date_raw.clone(),
        approval_year: parse_approval_year(&record.approval_date_raw),
        normalization_status: status.to_owned(),
        normalization_reason: reason.to_owned(),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        bronze_object_key: input.bronze_object_key.to_owned(),
        source_line_number: record.source_line_number,
        valid_from_utc: input.valid_from_utc,
        ingested_at_utc: input.ingested_at_utc,
        row_checksum_sha256: String::new(),
    };
    // Not the unit plan's pnu/block invariant. That invariant says "no standard PNU only for
    // block parcels", and it holds for 전유부 — but the first national title run refused on a
    // real row whose 대지구분코드 is simply unstated (register_parcel_key byte 10 padded to `0`
    // from an empty code, 본번/부번 0000). A title without a stated parcel kind has no standard
    // PNU, and recording `None` is the honest row; the projection later counts it as an orphan
    // rather than this plan refusing the whole snapshot over it.
    row.row_checksum_sha256 = row_checksum(&row)?;
    Ok(row)
}

fn insert_string(record: &mut JsonMap<String, JsonValue>, key: &str, value: &str) {
    record.insert(key.to_owned(), JsonValue::String(value.to_owned()));
}

fn insert_optional_string(record: &mut JsonMap<String, JsonValue>, key: &str, value: Option<&str>) {
    record.insert(
        key.to_owned(),
        value.map_or(JsonValue::Null, |text| JsonValue::String(text.to_owned())),
    );
}

fn insert_optional_number<T>(record: &mut JsonMap<String, JsonValue>, key: &str, value: Option<T>)
where
    JsonValue: From<T>,
{
    record.insert(
        key.to_owned(),
        value.map_or(JsonValue::Null, JsonValue::from),
    );
}

fn row_to_json_value(row: &BuildingRegisterTitleSilverRow) -> JsonValue {
    let mut record = JsonMap::new();
    insert_string(&mut record, "title_row_id", &row.title_row_id);
    insert_string(&mut record, "mgm_bldrgst_pk", &row.mgm_bldrgst_pk);
    insert_optional_string(&mut record, "pnu", row.pnu.as_deref());
    insert_string(&mut record, "register_parcel_key", &row.register_parcel_key);
    insert_string(&mut record, "dong_name_raw", &row.dong_name_raw);
    insert_string(&mut record, "main_or_annex_kind", &row.main_or_annex_kind);
    insert_string(
        &mut record,
        "main_or_annex_name_raw",
        &row.main_or_annex_name_raw,
    );
    insert_string(
        &mut record,
        "register_kind_name_raw",
        &row.register_kind_name_raw,
    );
    insert_string(
        &mut record,
        "register_type_name_raw",
        &row.register_type_name_raw,
    );
    insert_string(&mut record, "jibun_address_raw", &row.jibun_address_raw);
    insert_string(&mut record, "road_address_raw", &row.road_address_raw);
    insert_string(&mut record, "structure_code_raw", &row.structure_code_raw);
    insert_string(&mut record, "structure_name_raw", &row.structure_name_raw);
    insert_string(&mut record, "purpose_code_raw", &row.purpose_code_raw);
    insert_string(&mut record, "purpose_name_raw", &row.purpose_name_raw);
    insert_string(&mut record, "purpose_detail_raw", &row.purpose_detail_raw);
    insert_string(&mut record, "roof_code_raw", &row.roof_code_raw);
    insert_string(&mut record, "roof_name_raw", &row.roof_name_raw);
    insert_optional_number(&mut record, "building_area_m2", row.building_area_m2);
    insert_optional_number(&mut record, "floor_area_m2", row.floor_area_m2);
    insert_optional_number(&mut record, "ground_floor_count", row.ground_floor_count);
    insert_optional_number(
        &mut record,
        "basement_floor_count",
        row.basement_floor_count,
    );
    insert_optional_number(&mut record, "title_unit_count", row.title_unit_count);
    insert_string(&mut record, "approval_date_raw", &row.approval_date_raw);
    insert_optional_number(&mut record, "approval_year", row.approval_year);
    insert_string(
        &mut record,
        "normalization_status",
        &row.normalization_status,
    );
    insert_string(
        &mut record,
        "normalization_reason",
        &row.normalization_reason,
    );
    insert_string(&mut record, "source_snapshot_id", &row.source_snapshot_id);
    insert_string(&mut record, "bronze_object_key", &row.bronze_object_key);
    insert_optional_number(&mut record, "source_line_number", row.source_line_number);
    insert_string(
        &mut record,
        "valid_from_utc",
        &row.valid_from_utc.to_rfc3339(),
    );
    insert_string(
        &mut record,
        "ingested_at_utc",
        &row.ingested_at_utc.to_rfc3339(),
    );
    insert_string(&mut record, "row_checksum_sha256", &row.row_checksum_sha256);
    JsonValue::Object(record)
}

fn row_checksum(
    row: &BuildingRegisterTitleSilverRow,
) -> Result<String, BuildingRegisterTitleSilverPlanError> {
    let mut payload = row_to_json_value(row);
    if let JsonValue::Object(record) = &mut payload {
        record.remove("row_checksum_sha256");
    }
    let line = serde_json::to_string(&payload)
        .map_err(|error| BuildingRegisterTitleSilverPlanError::InvalidInput(error.to_string()))?;
    Ok(Sha256::digest(line.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut checksum, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn line_with(overrides: &[(usize, &str)]) -> String {
        let mut fields = vec![String::new(); 77];
        fields[MGM_BLDRGST_PK_INDEX] = "1002121184".to_owned();
        fields[REGISTER_KIND_NAME_INDEX] = "일반".to_owned();
        fields[REGISTER_TYPE_NAME_INDEX] = "일반건축물".to_owned();
        fields[JIBUN_ADDRESS_INDEX] = "서울특별시 종로구 부암동 220-1번지".to_owned();
        fields[ROAD_ADDRESS_INDEX] = "서울특별시 종로구 창의문로 170".to_owned();
        fields[PNU_SIGUNGU_INDEX] = "99999".to_owned();
        fields[PNU_BEOPJEONGDONG_INDEX] = "00001".to_owned();
        fields[PNU_DAEJI_KIND_INDEX] = "0".to_owned();
        fields[PNU_BONBEON_INDEX] = "0001".to_owned();
        fields[PNU_BUBEON_INDEX] = "0000".to_owned();
        fields[MAIN_ANNEX_KIND_INDEX] = "주건축물".to_owned();
        // 값은 좌표로 오독될 수 없는 대역에서 고른다(공개 픽스처 안전 가드): 한국 경도
        // 대역 안의 소수는 면적이어도 좌표로 읽힌다. 성질(연면적 = 2층 x 바닥면적)은 유지.
        fields[BUILDING_AREA_INDEX] = "81.7".to_owned();
        fields[FLOOR_AREA_INDEX] = "163.4".to_owned();
        fields[STRUCTURE_CODE_INDEX] = "11".to_owned();
        fields[STRUCTURE_NAME_INDEX] = "벽돌구조".to_owned();
        fields[PURPOSE_CODE_INDEX] = "03000".to_owned();
        fields[PURPOSE_NAME_INDEX] = "제1종근린생활시설".to_owned();
        fields[ROOF_CODE_INDEX] = "10".to_owned();
        fields[ROOF_NAME_INDEX] = "(철근)콘크리트".to_owned();
        fields[TITLE_UNIT_COUNT_INDEX] = "0".to_owned();
        fields[GROUND_FLOOR_COUNT_INDEX] = "2".to_owned();
        fields[BASEMENT_FLOOR_COUNT_INDEX] = "0".to_owned();
        fields[APPROVAL_DATE_INDEX] = "19710217".to_owned();
        for (index, value) in overrides {
            fields[*index] = (*value).to_owned();
        }
        fields.join("|")
    }

    type TestResult = Result<(), BuildingRegisterTitleSilverPlanError>;

    fn input(
        records: &[BuildingRegisterTitleSourceRow],
    ) -> BuildingRegisterTitleSilverRowsInput<'_> {
        BuildingRegisterTitleSilverRowsInput {
            records,
            source_snapshot_id: "hubgokr__building_register_main:2026-07",
            bronze_object_key: "bronze/source=hubgokr__building_register_main/x.zip",
            valid_from_utc: Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 0).unwrap(),
            ingested_at_utc: Utc.with_ymd_and_hms(2026, 9, 3, 0, 0, 0).unwrap(),
        }
    }

    fn parse(
        line: &str,
    ) -> Result<BuildingRegisterTitleSourceRow, BuildingRegisterTitleSilverPlanError> {
        parse_building_register_title_source_row_from_hub_bulk_text_line(
            line,
            "bronze/source=hubgokr__building_register_main/x.zip",
            7,
        )
    }

    #[test]
    fn the_measured_sample_line_round_trips() -> TestResult {
        // The exact shape of the first real line opened on 2026-09-03, reserved-band PNU.
        let record = parse(&line_with(&[]))?;

        assert_eq!(record.mgm_bldrgst_pk, "1002121184");
        assert_eq!(record.purpose_code_raw, "03000");
        assert_eq!(record.structure_name_raw, "벽돌구조");
        assert_eq!(record.floor_area_raw, "163.4");
        assert_eq!(record.approval_date_raw, "19710217");
        assert_eq!(record.pnu.as_deref(), Some("9999900001100010000"));
        Ok(())
    }

    #[test]
    fn a_normalized_row_carries_the_projected_facts() -> TestResult {
        let records = [parse(&line_with(&[]))?];

        let rows = normalize_building_register_title_silver_rows(&input(&records))?;

        let row = &rows[0];
        assert_eq!(row.main_or_annex_kind, "main");
        assert_eq!(row.floor_area_m2, Some(163.4));
        assert_eq!(row.building_area_m2, Some(81.7));
        assert_eq!(row.ground_floor_count, Some(2));
        assert_eq!(row.basement_floor_count, Some(0));
        assert_eq!(row.approval_year, Some(1971));
        assert_eq!(row.normalization_status, "accepted");
        assert_eq!(row.normalization_reason, "accepted_title");
        assert!(!row.title_row_id.is_empty());
        assert_eq!(row.row_checksum_sha256.len(), 64);
        Ok(())
    }

    #[test]
    fn an_unmarked_main_annex_kind_is_unknown_and_says_so() -> TestResult {
        // 552 of 8,051,204 measured rows state no 주부속구분. They load as `unknown` with a
        // reason, rather than being guessed into `main`.
        let records = [parse(&line_with(&[(MAIN_ANNEX_KIND_INDEX, "")]))?];

        let rows = normalize_building_register_title_silver_rows(&input(&records))?;

        assert_eq!(rows[0].main_or_annex_kind, "unknown");
        assert_eq!(rows[0].normalization_reason, "main_annex_unmarked");
        Ok(())
    }

    #[test]
    fn a_zero_area_is_unstated_not_measured() -> TestResult {
        // The register writes 0 where it states nothing. Carried forward, a zero-area
        // building would look measured.
        let records = [parse(&line_with(&[
            (BUILDING_AREA_INDEX, "0"),
            (FLOOR_AREA_INDEX, "0"),
        ]))?];

        let rows = normalize_building_register_title_silver_rows(&input(&records))?;

        assert_eq!(rows[0].building_area_m2, None);
        assert_eq!(rows[0].floor_area_m2, None);
        Ok(())
    }

    #[test]
    fn an_implausible_approval_date_yields_no_year() -> TestResult {
        for raw in ["", "0", "00000000", "99999999", "2020-1-1"] {
            let records = [parse(&line_with(&[(APPROVAL_DATE_INDEX, raw)]))?];
            let rows = normalize_building_register_title_silver_rows(&input(&records))?;
            assert_eq!(rows[0].approval_year, None, "raw={raw:?}");
        }
        Ok(())
    }

    #[test]
    fn an_unstated_parcel_kind_yields_no_pnu_and_still_loads() -> TestResult {
        // Measured on the first national run: a real title row carries an empty 대지구분코드
        // (register key byte 10 pads to `0`, 본번/부번 0000). The unit plan's pnu/block
        // invariant read that as a contradiction and refused the whole snapshot; for titles it
        // is an honest `pnu: None`, and the projection counts it as an orphan later.
        let records = [parse(&line_with(&[
            (PNU_DAEJI_KIND_INDEX, ""),
            (PNU_BONBEON_INDEX, "0000"),
            (PNU_BUBEON_INDEX, "0000"),
        ]))?];

        let rows = normalize_building_register_title_silver_rows(&input(&records))?;

        assert_eq!(rows[0].pnu, None);
        assert_eq!(rows[0].normalization_status, "accepted");
        assert!(!rows[0].register_parcel_key.is_empty());
        Ok(())
    }

    #[test]
    fn two_lines_of_one_object_never_share_a_row_id() -> TestResult {
        let a = parse_building_register_title_source_row_from_hub_bulk_text_line(
            &line_with(&[]),
            "bronze/x.zip",
            1,
        )?;
        let b = parse_building_register_title_source_row_from_hub_bulk_text_line(
            &line_with(&[]),
            "bronze/x.zip",
            2,
        )?;

        let rows = normalize_building_register_title_silver_rows(&input(&[a, b]))?;

        assert_ne!(rows[0].title_row_id, rows[1].title_row_id);
        Ok(())
    }

    #[test]
    fn a_short_line_is_refused_with_its_field_count() {
        let result = parse_building_register_title_source_row_from_hub_bulk_text_line(
            "1002121184|x|y",
            "bronze/x.zip",
            1,
        );

        assert!(
            matches!(&result, Err(error) if error.to_string().contains("expected at least")),
            "a line without the approval date cannot be a title row: {result:?}"
        );
    }

    #[test]
    fn an_empty_management_key_is_refused() {
        let result = parse_building_register_title_source_row_from_hub_bulk_text_line(
            &line_with(&[(MGM_BLDRGST_PK_INDEX, "  ")]),
            "bronze/x.zip",
            1,
        );

        assert!(
            matches!(&result, Err(error) if error.to_string().contains("management key")),
            "the management key is the natural key downstream: {result:?}"
        );
    }

    #[test]
    fn the_checksum_excludes_itself_and_is_stable() -> TestResult {
        let records = [parse(&line_with(&[]))?];
        let rows_a = normalize_building_register_title_silver_rows(&input(&records))?;
        let rows_b = normalize_building_register_title_silver_rows(&input(&records))?;

        assert_eq!(rows_a[0].row_checksum_sha256, rows_b[0].row_checksum_sha256);
        let jsonl = building_register_title_silver_row_to_jsonl(&rows_a[0])?;
        assert!(jsonl.contains("row_checksum_sha256"));
        Ok(())
    }

    #[test]
    fn the_witness_reader_and_this_plan_agree_on_the_shared_indexes() {
        // `building_register_title.rs` reads floor counts as a witness off the same file. The
        // two modules hold the indexes by value; this test is what keeps them one measurement.
        let line = line_with(&[]);
        let Some(counts) =
            crate::building_register_title::parse_building_title_floor_counts_from_hub_bulk_text_line(
                &line,
            )
        else {
            unreachable!("the witness reader must parse the measured sample line");
        };

        assert_eq!(counts.1.above_ground, Some(2));
        assert_eq!(counts.1.below_ground, None, "0 은 witness 쪽에선 None");
        assert_eq!(counts.0, "1002121184");
    }
}
