//! Silver normalization helpers for official building-register unit-area
//! (전유공용면적, `mart_djy_06`) rows.
//!
//! Area rows attach to 전유부 rows by the provider's shared `mgm_bldrgst_pk`,
//! so this plan performs no
//! entity matching of its own: it preserves raw provider fields, derives the
//! shared `unit_designation`, converts parcel identity per ADR 0023, and types
//! the 전유/공용 split plus the ㎡ area value.

use chrono::{DateTime, Utc};
use foundation_normalization_domain::{
    building_register_unit_designation, normalize_building_register_floor, RawBuildingRegisterFloor,
};
use foundation_shared_kernel::pnu::{
    hub_register_parcel_key, standard_pnu_from_hub_register_codes,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::building_register_unit_silver_plan::{
    validate_pnu_block_invariant, BuildingRegisterUnitSilverPlanError,
};

/// Column layout of a hub.go.kr 전유공용면적 (`mart_djy_06`) TXT line (39 columns).
const MGM_BLDRGST_PK_INDEX: usize = 0;
const REGISTER_KIND_NAME_INDEX: usize = 2;
const REGISTER_TYPE_NAME_INDEX: usize = 4;
const SIGUNGU_CODE_INDEX: usize = 8;
const BEOPJEONGDONG_CODE_INDEX: usize = 9;
const DAEJI_KIND_INDEX: usize = 10;
const BONBEON_INDEX: usize = 11;
const BUBEON_INDEX: usize = 12;
const DONG_NAME_INDEX: usize = 21;
const UNIT_NAME_INDEX: usize = 22;
const FLOOR_TYPE_CODE_INDEX: usize = 23;
const FLOOR_TYPE_NAME_INDEX: usize = 24;
const FLOOR_NUMBER_INDEX: usize = 25;
const AREA_KIND_CODE_INDEX: usize = 26;
const AREA_KIND_NAME_INDEX: usize = 27;
const MAIN_OR_ANNEX_NAME_INDEX: usize = 29;
const FLOOR_LABEL_INDEX: usize = 30;
const STRUCTURE_NAME_INDEX: usize = 32;
const USAGE_CODE_INDEX: usize = 34;
const USAGE_NAME_INDEX: usize = 35;
const USAGE_DETAIL_INDEX: usize = 36;
const AREA_M2_INDEX: usize = 37;
const CREATED_DATE_INDEX: usize = 38;
const MIN_FIELD_COUNT: usize = CREATED_DATE_INDEX + 1;

/// Parsed source-side unit-area fields before deterministic normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterUnitAreaSourceRow {
    /// Stable row-level source lineage id.
    pub source_record_id: String,
    /// Provider unit management primary key — shared with the 전유부 register.
    pub mgm_bldrgst_pk: String,
    /// Raw 대장구분명 (`집합` / `일반`).
    pub register_kind_name_raw: String,
    /// Raw 대장종류명 (`전유부` / `표제부`).
    pub register_type_name_raw: String,
    /// Standard 19-digit PNU (대지구분 1/2); `None` for block parcels (ADR 0023).
    pub pnu: Option<String>,
    /// Register-internal parcel key (hub-native composition; not a PNU).
    pub register_parcel_key: String,
    /// Raw 동명칭.
    pub dong_name_raw: String,
    /// Raw 호명칭.
    pub unit_name_raw: String,
    /// Raw 층구분 code.
    pub floor_type_code_raw: String,
    /// Raw 층구분 name.
    pub floor_type_name_raw: String,
    /// Raw 층번호.
    pub floor_number_raw: String,
    /// Raw 층 표시명 (`4층` / `각층`); display echo only, never fed to the
    /// floor normalizer.
    pub floor_label_raw: String,
    /// Raw 전유공용구분 code (`1` 전유 / `2` 공용).
    pub area_kind_code_raw: String,
    /// Raw 전유공용구분 name.
    pub area_kind_name_raw: String,
    /// Raw 주부속구분명 (`주건축물` / `부속건축물`).
    pub main_or_annex_name_raw: String,
    /// Raw 구조명.
    pub structure_name_raw: String,
    /// Raw 용도 code.
    pub usage_code_raw: String,
    /// Raw 용도명.
    pub usage_name_raw: String,
    /// Raw 기타용도 detail (e.g. `계단실,복도,펌프실`).
    pub usage_detail_raw: String,
    /// Raw 면적(㎡) text.
    pub area_m2_raw: String,
    /// Raw 생성일자 (`YYYYMMDD`).
    pub created_date_raw: String,
    /// 1-based source line number inside the Bronze object when available.
    pub source_line_number: Option<u64>,
}

/// Input required to normalize unit-area source rows into Silver rows.
pub struct BuildingRegisterUnitAreaSilverRowsInput<'a> {
    /// Parsed provider source rows ordered by the caller.
    pub records: &'a [BuildingRegisterUnitAreaSourceRow],
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// Bronze object key that carried these source rows.
    pub bronze_object_key: &'a str,
    /// UTC timestamp from which these source facts are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.building_register_unit_areas` row prepared from one source row.
#[derive(Clone, Debug, PartialEq)]
pub struct BuildingRegisterUnitAreaSilverRow {
    /// Stable Silver row id.
    pub area_row_id: String,
    /// Provider unit management primary key — join key to `silver.building_register_units`.
    pub mgm_bldrgst_pk: String,
    /// Raw 대장구분명 (`집합` / `일반`).
    pub register_kind_name_raw: String,
    /// Raw 대장종류명 (`전유부` / `표제부`) — diagnostic for PK-join misses.
    pub register_type_name_raw: String,
    /// Standard 19-digit PNU (대지구분 1/2); null for block parcels (ADR 0023).
    pub pnu: Option<String>,
    /// Register-internal parcel key (hub-native composition; **not** a PNU).
    pub register_parcel_key: String,
    /// Raw 동명칭.
    pub dong_name_raw: String,
    /// Raw 호명칭.
    pub unit_name_raw: String,
    /// Whitespace-compacted raw 호명 — same designation rule as the unit table.
    pub unit_designation: Option<String>,
    /// Canonical floor kind wire value.
    pub floor_kind: String,
    /// Signed floor position: above-ground positive, basement negative.
    pub floor_index: Option<i16>,
    /// Canonical floor number when accepted.
    pub floor_number: Option<u16>,
    /// Raw 층 표시명 echo (`4층` / `각층`).
    pub floor_label_raw: String,
    /// Typed 전유/공용 split: `exclusive`, `common`, or `unknown`.
    pub area_kind: String,
    /// Raw 전유공용구분 name.
    pub area_kind_name_raw: String,
    /// Raw 주부속구분명.
    pub main_or_annex_name_raw: String,
    /// Raw 구조명.
    pub structure_name_raw: String,
    /// Raw 용도 code.
    pub usage_code_raw: String,
    /// Raw 용도명.
    pub usage_name_raw: String,
    /// Raw 기타용도 detail.
    pub usage_detail_raw: String,
    /// Parsed 면적 in ㎡; null when the raw text is not a valid non-negative number.
    pub area_m2: Option<f64>,
    /// Raw 면적 text preserved for lineage.
    pub area_m2_raw: String,
    /// Raw 생성일자 (`YYYYMMDD`).
    pub created_date_raw: String,
    /// Normalization status wire value.
    pub normalization_status: String,
    /// Normalization reason wire value.
    pub normalization_reason: String,
    /// Source-snapshot lineage id.
    pub source_snapshot_id: String,
    /// Bronze object key that carried this source row.
    pub bronze_object_key: String,
    /// 1-based source line number inside the Bronze object when available.
    pub source_line_number: Option<u64>,
    /// UTC timestamp from which this fact is valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when this row entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
    /// Lowercase SHA-256 checksum of the row payload excluding this checksum field.
    pub row_checksum_sha256: String,
}

/// Parses one hub.go.kr 전유공용면적 TXT line into a Silver source row.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when lineage is invalid, the
/// line has fewer fields than the official 39 columns, or the management key is
/// empty.
pub fn parse_building_register_unit_area_source_row_from_hub_bulk_text_line(
    line: &str,
    bronze_object_key: &str,
    one_based_line_number: u64,
) -> Result<BuildingRegisterUnitAreaSourceRow, BuildingRegisterUnitSilverPlanError> {
    if bronze_object_key.trim().is_empty() {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "bronze_object_key must not be empty".to_owned(),
        ));
    }
    if one_based_line_number == 0 {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "source line number must be 1-based".to_owned(),
        ));
    }

    let fields = line.split('|').collect::<Vec<_>>();
    if fields.len() < MIN_FIELD_COUNT {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(format!(
            "hub.go.kr 전유공용면적 line {one_based_line_number} has {} fields, expected at least {MIN_FIELD_COUNT}",
            fields.len()
        )));
    }

    let mgm_bldrgst_pk = fields[MGM_BLDRGST_PK_INDEX].trim();
    if mgm_bldrgst_pk.is_empty() {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "hub.go.kr 전유공용면적 management key must not be empty".to_owned(),
        ));
    }

    Ok(BuildingRegisterUnitAreaSourceRow {
        source_record_id: format!("{bronze_object_key}#line-{one_based_line_number:06}"),
        mgm_bldrgst_pk: mgm_bldrgst_pk.to_owned(),
        register_kind_name_raw: fields[REGISTER_KIND_NAME_INDEX].trim().to_owned(),
        register_type_name_raw: fields[REGISTER_TYPE_NAME_INDEX].trim().to_owned(),
        pnu: standard_pnu_from_hub_register_codes(
            fields[SIGUNGU_CODE_INDEX],
            fields[BEOPJEONGDONG_CODE_INDEX],
            fields[DAEJI_KIND_INDEX],
            fields[BONBEON_INDEX],
            fields[BUBEON_INDEX],
        ),
        register_parcel_key: hub_register_parcel_key(
            fields[SIGUNGU_CODE_INDEX],
            fields[BEOPJEONGDONG_CODE_INDEX],
            fields[DAEJI_KIND_INDEX],
            fields[BONBEON_INDEX],
            fields[BUBEON_INDEX],
        ),
        dong_name_raw: fields[DONG_NAME_INDEX].trim().to_owned(),
        unit_name_raw: fields[UNIT_NAME_INDEX].trim().to_owned(),
        floor_type_code_raw: fields[FLOOR_TYPE_CODE_INDEX].trim().to_owned(),
        floor_type_name_raw: fields[FLOOR_TYPE_NAME_INDEX].trim().to_owned(),
        floor_number_raw: fields[FLOOR_NUMBER_INDEX].trim().to_owned(),
        floor_label_raw: fields[FLOOR_LABEL_INDEX].trim().to_owned(),
        area_kind_code_raw: fields[AREA_KIND_CODE_INDEX].trim().to_owned(),
        area_kind_name_raw: fields[AREA_KIND_NAME_INDEX].trim().to_owned(),
        main_or_annex_name_raw: fields[MAIN_OR_ANNEX_NAME_INDEX].trim().to_owned(),
        structure_name_raw: fields[STRUCTURE_NAME_INDEX].trim().to_owned(),
        usage_code_raw: fields[USAGE_CODE_INDEX].trim().to_owned(),
        usage_name_raw: fields[USAGE_NAME_INDEX].trim().to_owned(),
        usage_detail_raw: fields[USAGE_DETAIL_INDEX].trim().to_owned(),
        area_m2_raw: fields[AREA_M2_INDEX].trim().to_owned(),
        created_date_raw: fields[CREATED_DATE_INDEX].trim().to_owned(),
        source_line_number: Some(one_based_line_number),
    })
}

/// Normalizes unit-area source rows into Silver rows.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when required lineage is empty
/// or row JSON serialization fails while computing checksums.
pub fn normalize_building_register_unit_area_silver_rows(
    input: &BuildingRegisterUnitAreaSilverRowsInput<'_>,
) -> Result<Vec<BuildingRegisterUnitAreaSilverRow>, BuildingRegisterUnitSilverPlanError> {
    if input.source_snapshot_id.trim().is_empty() || input.bronze_object_key.trim().is_empty() {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "source_snapshot_id and bronze_object_key must not be empty".to_owned(),
        ));
    }

    input
        .records
        .iter()
        .map(|record| build_area_silver_row(record, input))
        .collect()
}

/// Typed 전유/공용 split from the provider code (`1` 전유 / `2` 공용), cross-checked
/// against the provider name — the same standard the floor normalizer applies to
/// its code/name pair. A non-empty name that disagrees with the code refuses to
/// guess; an empty name trusts the code.
fn area_kind_wire(area_kind_code_raw: &str, area_kind_name_raw: &str) -> &'static str {
    let from_code = match area_kind_code_raw {
        "1" => "exclusive",
        "2" => "common",
        _ => return "unknown",
    };
    let name_agrees = match area_kind_name_raw {
        "" => true,
        "전유" => from_code == "exclusive",
        "공용" => from_code == "common",
        _ => false,
    };
    if name_agrees {
        from_code
    } else {
        "unknown"
    }
}

/// Whether the code/name pair disagrees (both present, different kinds).
fn area_kind_code_name_mismatch(area_kind_code_raw: &str, area_kind_name_raw: &str) -> bool {
    matches!(area_kind_code_raw, "1" | "2")
        && area_kind_wire(area_kind_code_raw, area_kind_name_raw) == "unknown"
}

/// Parses the raw 면적 text into a non-negative finite ㎡ value.
fn parse_area_m2(area_m2_raw: &str) -> Option<f64> {
    area_m2_raw
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn build_area_silver_row(
    record: &BuildingRegisterUnitAreaSourceRow,
    input: &BuildingRegisterUnitAreaSilverRowsInput<'_>,
) -> Result<BuildingRegisterUnitAreaSilverRow, BuildingRegisterUnitSilverPlanError> {
    let floor = normalize_building_register_floor(RawBuildingRegisterFloor {
        floor_type_code: &record.floor_type_code_raw,
        floor_type_name: &record.floor_type_name_raw,
        floor_number: &record.floor_number_raw,
        floor_label: None,
    });
    let area_kind = area_kind_wire(&record.area_kind_code_raw, &record.area_kind_name_raw);
    let area_m2 = parse_area_m2(&record.area_m2_raw);

    // 면적 의미론이 상태를 지배한다: 층 해석 실패는 상태를 낮추지 않는다 (층은
    // 서술 속성, 면적/구분이 이 테이블의 사실). 층 미상률은 Trino 게이트로 계측.
    let (status, reason) = if area_m2.is_none() {
        ("proposal_required", "invalid_area")
    } else if area_kind_code_name_mismatch(&record.area_kind_code_raw, &record.area_kind_name_raw) {
        ("proposal_required", "area_kind_code_name_mismatch")
    } else if area_kind == "unknown" {
        ("proposal_required", "unknown_area_kind")
    } else if area_kind == "exclusive" {
        ("accepted", "accepted_exclusive_area")
    } else {
        ("accepted", "accepted_common_area")
    };

    let mut row = BuildingRegisterUnitAreaSilverRow {
        area_row_id: format!("building-register-unit-area:{}", record.source_record_id),
        mgm_bldrgst_pk: record.mgm_bldrgst_pk.clone(),
        register_kind_name_raw: record.register_kind_name_raw.clone(),
        register_type_name_raw: record.register_type_name_raw.clone(),
        pnu: record.pnu.clone(),
        register_parcel_key: record.register_parcel_key.clone(),
        dong_name_raw: record.dong_name_raw.clone(),
        unit_name_raw: record.unit_name_raw.clone(),
        unit_designation: building_register_unit_designation(&record.unit_name_raw),
        floor_kind: floor.kind.wire_name().to_owned(),
        floor_index: floor.floor_index,
        floor_number: floor.floor_number,
        floor_label_raw: record.floor_label_raw.clone(),
        area_kind: area_kind.to_owned(),
        area_kind_name_raw: record.area_kind_name_raw.clone(),
        main_or_annex_name_raw: record.main_or_annex_name_raw.clone(),
        structure_name_raw: record.structure_name_raw.clone(),
        usage_code_raw: record.usage_code_raw.clone(),
        usage_name_raw: record.usage_name_raw.clone(),
        usage_detail_raw: record.usage_detail_raw.clone(),
        area_m2,
        area_m2_raw: record.area_m2_raw.clone(),
        created_date_raw: record.created_date_raw.clone(),
        normalization_status: status.to_owned(),
        normalization_reason: reason.to_owned(),
        source_snapshot_id: input.source_snapshot_id.to_owned(),
        bronze_object_key: input.bronze_object_key.to_owned(),
        source_line_number: record.source_line_number,
        valid_from_utc: input.valid_from_utc,
        ingested_at_utc: input.ingested_at_utc,
        row_checksum_sha256: String::new(),
    };
    validate_pnu_block_invariant(row.pnu.as_deref(), &row.register_parcel_key)?;
    row.row_checksum_sha256 = row_checksum(&row)?;
    Ok(row)
}

fn row_to_json_value(row: &BuildingRegisterUnitAreaSilverRow) -> JsonValue {
    let mut record = JsonMap::new();
    insert_string(&mut record, "area_row_id", &row.area_row_id);
    insert_string(&mut record, "mgm_bldrgst_pk", &row.mgm_bldrgst_pk);
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
    insert_optional_string(&mut record, "pnu", row.pnu.as_deref());
    insert_string(&mut record, "register_parcel_key", &row.register_parcel_key);
    insert_string(&mut record, "dong_name_raw", &row.dong_name_raw);
    insert_string(&mut record, "unit_name_raw", &row.unit_name_raw);
    insert_optional_string(
        &mut record,
        "unit_designation",
        row.unit_designation.as_deref(),
    );
    insert_string(&mut record, "floor_kind", &row.floor_kind);
    insert_optional_number(&mut record, "floor_index", row.floor_index);
    insert_optional_number(&mut record, "floor_number", row.floor_number);
    insert_string(&mut record, "floor_label_raw", &row.floor_label_raw);
    insert_string(&mut record, "area_kind", &row.area_kind);
    insert_string(&mut record, "area_kind_name_raw", &row.area_kind_name_raw);
    insert_string(
        &mut record,
        "main_or_annex_name_raw",
        &row.main_or_annex_name_raw,
    );
    insert_string(&mut record, "structure_name_raw", &row.structure_name_raw);
    insert_string(&mut record, "usage_code_raw", &row.usage_code_raw);
    insert_string(&mut record, "usage_name_raw", &row.usage_name_raw);
    insert_string(&mut record, "usage_detail_raw", &row.usage_detail_raw);
    insert_optional_number(&mut record, "area_m2", row.area_m2);
    insert_string(&mut record, "area_m2_raw", &row.area_m2_raw);
    insert_string(&mut record, "created_date_raw", &row.created_date_raw);
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
        &row.valid_from_utc
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    );
    insert_string(
        &mut record,
        "ingested_at_utc",
        &row.ingested_at_utc
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    );
    insert_string(&mut record, "row_checksum_sha256", &row.row_checksum_sha256);
    JsonValue::Object(record)
}

fn insert_string(record: &mut JsonMap<String, JsonValue>, key: &str, value: &str) {
    record.insert(key.to_owned(), JsonValue::String(value.to_owned()));
}

fn insert_optional_string(record: &mut JsonMap<String, JsonValue>, key: &str, value: Option<&str>) {
    record.insert(
        key.to_owned(),
        value.map_or(JsonValue::Null, |value| JsonValue::String(value.to_owned())),
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

fn row_checksum(
    row: &BuildingRegisterUnitAreaSilverRow,
) -> Result<String, BuildingRegisterUnitSilverPlanError> {
    let mut payload = row_to_json_value(row);
    if let JsonValue::Object(record) = &mut payload {
        record.remove("row_checksum_sha256");
    }
    let line = serde_json::to_string(&payload)
        .map_err(|error| BuildingRegisterUnitSilverPlanError::InvalidInput(error.to_string()))?;
    Ok(Sha256::digest(line.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut checksum, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        },
    ))
}

/// Serializes a Silver unit-area row to one compact JSON line.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when JSON serialization fails.
pub fn building_register_unit_area_silver_row_to_jsonl(
    row: &BuildingRegisterUnitAreaSilverRow,
) -> Result<String, BuildingRegisterUnitSilverPlanError> {
    serde_json::to_string(&row_to_json_value(row))
        .map_err(|error| BuildingRegisterUnitSilverPlanError::InvalidInput(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    /// Synthetic 39-field line that exercises the provider's documented layout.
    fn area_line(fields_override: &[(usize, &str)]) -> String {
        let mut fields = vec![String::new(); MIN_FIELD_COUNT];
        fields[MGM_BLDRGST_PK_INDEX] = "SYNTHETIC-AREA-PK-0001".to_owned();
        fields[1] = "2".to_owned();
        fields[REGISTER_KIND_NAME_INDEX] = "집합".to_owned();
        fields[3] = "4".to_owned();
        fields[REGISTER_TYPE_NAME_INDEX] = "전유부".to_owned();
        fields[SIGUNGU_CODE_INDEX] = "99999".to_owned();
        fields[BEOPJEONGDONG_CODE_INDEX] = "00301".to_owned();
        fields[DAEJI_KIND_INDEX] = "0".to_owned();
        fields[BONBEON_INDEX] = "0171".to_owned();
        fields[BUBEON_INDEX] = "0000".to_owned();
        fields[DONG_NAME_INDEX] = "SYNTHETIC-BUILDING".to_owned();
        fields[UNIT_NAME_INDEX] = "SYNTHETIC-UNIT-416".to_owned();
        fields[FLOOR_TYPE_CODE_INDEX] = "20".to_owned();
        fields[FLOOR_TYPE_NAME_INDEX] = "지상".to_owned();
        fields[FLOOR_NUMBER_INDEX] = "4".to_owned();
        fields[AREA_KIND_CODE_INDEX] = "1".to_owned();
        fields[AREA_KIND_NAME_INDEX] = "전유".to_owned();
        fields[MAIN_OR_ANNEX_NAME_INDEX] = "주건축물".to_owned();
        fields[FLOOR_LABEL_INDEX] = "4층".to_owned();
        fields[STRUCTURE_NAME_INDEX] = "철골철근콘크리트구조".to_owned();
        fields[USAGE_CODE_INDEX] = "14202".to_owned();
        fields[USAGE_NAME_INDEX] = "오피스텔".to_owned();
        fields[USAGE_DETAIL_INDEX] = "오피스텔".to_owned();
        fields[AREA_M2_INDEX] = "42.125".to_owned();
        fields[CREATED_DATE_INDEX] = "20991231".to_owned();
        for (index, value) in fields_override {
            fields[*index] = (*value).to_owned();
        }
        fields.join("|")
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const BRONZE_KEY: &str =
        "bronze/source=hubgokr__building_register_exclusive_common_area/synthetic-fixture.zip";

    fn parse(
        line: &str,
    ) -> Result<BuildingRegisterUnitAreaSourceRow, BuildingRegisterUnitSilverPlanError> {
        parse_building_register_unit_area_source_row_from_hub_bulk_text_line(line, BRONZE_KEY, 1)
    }

    fn normalize(
        records: &[BuildingRegisterUnitAreaSourceRow],
    ) -> Result<Vec<BuildingRegisterUnitAreaSilverRow>, Box<dyn std::error::Error>> {
        Ok(normalize_building_register_unit_area_silver_rows(
            &BuildingRegisterUnitAreaSilverRowsInput {
                records,
                source_snapshot_id: "synthetic-building-register-unit-area-20991231",
                bronze_object_key: BRONZE_KEY,
                valid_from_utc: DateTime::parse_from_rfc3339("2099-12-31T00:00:00Z")?.to_utc(),
                ingested_at_utc: DateTime::parse_from_rfc3339("2100-01-01T00:00:00Z")?.to_utc(),
            },
        )?)
    }

    #[test]
    fn parses_full_39_field_line_with_standard_pnu_and_register_key() -> TestResult {
        let record = parse(&area_line(&[]))?;

        assert_eq!(record.mgm_bldrgst_pk, "SYNTHETIC-AREA-PK-0001");
        assert_eq!(record.register_kind_name_raw, "집합");
        assert_eq!(record.register_type_name_raw, "전유부");
        // 허브 대지구분 0(대지) → 표준 1; 내부 키는 허브 조립 유지 (ADR 0023).
        assert_eq!(record.pnu.as_deref(), Some("9999900301101710000"));
        assert_eq!(record.register_parcel_key, "9999900301001710000");
        assert_eq!(record.dong_name_raw, "SYNTHETIC-BUILDING");
        assert_eq!(record.unit_name_raw, "SYNTHETIC-UNIT-416");
        assert_eq!(record.floor_type_code_raw, "20");
        assert_eq!(record.floor_type_name_raw, "지상");
        assert_eq!(record.floor_number_raw, "4");
        assert_eq!(record.floor_label_raw, "4층");
        assert_eq!(record.area_kind_code_raw, "1");
        assert_eq!(record.area_kind_name_raw, "전유");
        assert_eq!(record.main_or_annex_name_raw, "주건축물");
        assert_eq!(record.structure_name_raw, "철골철근콘크리트구조");
        assert_eq!(record.usage_code_raw, "14202");
        assert_eq!(record.usage_name_raw, "오피스텔");
        assert_eq!(record.usage_detail_raw, "오피스텔");
        assert_eq!(record.area_m2_raw, "42.125");
        assert_eq!(record.created_date_raw, "20991231");
        assert_eq!(record.source_line_number, Some(1));
        Ok(())
    }

    #[test]
    fn rejects_short_line_and_empty_management_key() {
        let short = "a|b|c";
        assert!(
            parse_building_register_unit_area_source_row_from_hub_bulk_text_line(
                short,
                "bronze/key.zip",
                1
            )
            .is_err()
        );

        let empty_pk = area_line(&[(MGM_BLDRGST_PK_INDEX, "")]);
        assert!(
            parse_building_register_unit_area_source_row_from_hub_bulk_text_line(
                &empty_pk,
                "bronze/key.zip",
                1
            )
            .is_err()
        );
    }

    #[test]
    fn normalizes_exclusive_row_with_area_and_floor() -> TestResult {
        let records = [parse(&area_line(&[]))?];
        let rows = normalize(&records)?;

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.area_row_id,
            "building-register-unit-area:bronze/source=hubgokr__building_register_exclusive_common_area/synthetic-fixture.zip#line-000001"
        );
        assert_eq!(row.area_kind, "exclusive");
        assert_eq!(row.area_m2, Some(42.125));
        assert_eq!(row.unit_designation.as_deref(), Some("SYNTHETIC-UNIT-416"));
        assert_eq!(row.floor_kind, "above_ground");
        assert_eq!(row.floor_index, Some(4));
        assert_eq!(row.floor_number, Some(4));
        assert_eq!(row.normalization_status, "accepted");
        assert_eq!(row.normalization_reason, "accepted_exclusive_area");
        assert_eq!(row.row_checksum_sha256.len(), 64);
        Ok(())
    }

    #[test]
    fn normalizes_common_all_floors_row_without_degrading_status() -> TestResult {
        // 공용 각층 행: 호명 없음 + 층번호 0 이어도 면적 의미론으로 accepted.
        let line = area_line(&[
            (UNIT_NAME_INDEX, ""),
            (FLOOR_TYPE_CODE_INDEX, "40"),
            (FLOOR_TYPE_NAME_INDEX, "각층"),
            (FLOOR_NUMBER_INDEX, "0"),
            (AREA_KIND_CODE_INDEX, "2"),
            (AREA_KIND_NAME_INDEX, "공용"),
            (FLOOR_LABEL_INDEX, "각층"),
            (USAGE_DETAIL_INDEX, "계단실,복도,펌프실"),
            (AREA_M2_INDEX, "30.108"),
        ]);
        let records = [parse(&line)?];
        let rows = normalize(&records)?;

        let row = &rows[0];
        assert_eq!(row.area_kind, "common");
        assert_eq!(row.area_m2, Some(30.108));
        assert_eq!(row.unit_designation, None);
        assert_eq!(row.floor_kind, "all_floors");
        assert_eq!(row.usage_detail_raw, "계단실,복도,펌프실");
        assert_eq!(row.normalization_status, "accepted");
        assert_eq!(row.normalization_reason, "accepted_common_area");
        Ok(())
    }

    #[test]
    fn routes_invalid_area_and_unknown_kind_to_proposal() -> TestResult {
        let bad_area = [parse(&area_line(&[(AREA_M2_INDEX, "abc")]))?];
        let rows = normalize(&bad_area)?;
        assert_eq!(rows[0].area_m2, None);
        assert_eq!(rows[0].area_m2_raw, "abc");
        assert_eq!(rows[0].normalization_status, "proposal_required");
        assert_eq!(rows[0].normalization_reason, "invalid_area");

        let negative_area = [parse(&area_line(&[(AREA_M2_INDEX, "-1.5")]))?];
        let rows = normalize(&negative_area)?;
        assert_eq!(rows[0].area_m2, None);
        assert_eq!(rows[0].normalization_reason, "invalid_area");

        let unknown_kind = [parse(&area_line(&[
            (AREA_KIND_CODE_INDEX, "9"),
            (AREA_KIND_NAME_INDEX, ""),
        ]))?];
        let rows = normalize(&unknown_kind)?;
        assert_eq!(rows[0].area_kind, "unknown");
        assert_eq!(rows[0].normalization_status, "proposal_required");
        assert_eq!(rows[0].normalization_reason, "unknown_area_kind");
        Ok(())
    }

    #[test]
    fn routes_area_kind_code_name_mismatch_to_proposal() -> TestResult {
        // 층구분과 같은 잣대: 코드와 이름이 서로 다른 구분을 말하면 확정하지 않는다.
        let flipped_common = [parse(&area_line(&[
            (AREA_KIND_CODE_INDEX, "1"),
            (AREA_KIND_NAME_INDEX, "공용"),
        ]))?];
        let rows = normalize(&flipped_common)?;
        assert_eq!(rows[0].area_kind, "unknown");
        assert_eq!(rows[0].normalization_status, "proposal_required");
        assert_eq!(rows[0].normalization_reason, "area_kind_code_name_mismatch");

        let flipped_exclusive = [parse(&area_line(&[
            (AREA_KIND_CODE_INDEX, "2"),
            (AREA_KIND_NAME_INDEX, "전유"),
        ]))?];
        let rows = normalize(&flipped_exclusive)?;
        assert_eq!(rows[0].area_kind, "unknown");
        assert_eq!(rows[0].normalization_reason, "area_kind_code_name_mismatch");

        // 이름 필드는 비어 있을 수 있으므로 이 경우 provider code를 사용한다.
        let blank_name = [parse(&area_line(&[(AREA_KIND_NAME_INDEX, "")]))?];
        let rows = normalize(&blank_name)?;
        assert_eq!(rows[0].area_kind, "exclusive");
        assert_eq!(rows[0].normalization_status, "accepted");
        Ok(())
    }

    #[test]
    fn block_parcel_rows_keep_null_pnu_and_pass_invariant() -> TestResult {
        let line = area_line(&[(DAEJI_KIND_INDEX, "2")]);
        let record = parse(&line)?;
        assert_eq!(record.pnu, None);
        assert_eq!(record.register_parcel_key, "9999900301201710000");

        let records = [record];
        let rows = normalize(&records)?;
        assert_eq!(rows[0].pnu, None);
        Ok(())
    }

    #[test]
    fn checksum_is_deterministic_and_field_sensitive() -> TestResult {
        let records = [parse(&area_line(&[]))?];
        let first = normalize(&records)?;
        let second = normalize(&records)?;
        assert_eq!(first[0].row_checksum_sha256, second[0].row_checksum_sha256);

        let changed = [parse(&area_line(&[(AREA_M2_INDEX, "70.698")]))?];
        let third = normalize(&changed)?;
        assert_ne!(first[0].row_checksum_sha256, third[0].row_checksum_sha256);
        Ok(())
    }

    #[test]
    fn serializes_row_to_jsonl_with_all_contract_fields() -> TestResult {
        let records = [parse(&area_line(&[]))?];
        let rows = normalize(&records)?;
        let jsonl = building_register_unit_area_silver_row_to_jsonl(&rows[0])?;
        let value = serde_json::from_str::<JsonValue>(&jsonl)?;

        assert_eq!(value["mgm_bldrgst_pk"], "SYNTHETIC-AREA-PK-0001");
        assert_eq!(value["pnu"], "9999900301101710000");
        assert_eq!(value["register_parcel_key"], "9999900301001710000");
        assert_eq!(value["area_kind"], "exclusive");
        assert_eq!(value["area_m2"], 42.125);
        assert_eq!(value["unit_designation"], "SYNTHETIC-UNIT-416");
        assert_eq!(value["normalization_status"], "accepted");
        assert_eq!(value["valid_from_utc"], "2099-12-31T00:00:00Z");
        assert!(value["row_checksum_sha256"].is_string());
        Ok(())
    }
}
