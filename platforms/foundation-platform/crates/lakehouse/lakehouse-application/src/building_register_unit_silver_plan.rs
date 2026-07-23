//! Silver normalization helpers for official building-register unit (전유부 호) rows.

use std::collections::BTreeMap;

use foundation_normalization_domain::{
    normalize_building_register_unit, BuildingRegisterUnitReason, NormalizedBuildingRegisterUnit,
    RawBuildingRegisterFloor, RawBuildingRegisterUnit,
};

use crate::building_register_title::BuildingTitleKeyIndex;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::pnu::{
    hub_register_parcel_key, standard_pnu_from_hub_register_codes,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Column layout of a hub.go.kr 전유부 (`mart_djy_09`) TXT line (27 columns).
const MGM_BLDRGST_PK_INDEX: usize = 0;
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
const MIN_FIELD_COUNT: usize = FLOOR_NUMBER_INDEX + 1;

/// Parsed source-side unit fields before deterministic normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterUnitSourceRow {
    /// Stable row-level source lineage id.
    pub source_record_id: String,
    /// Provider unit management primary key (호 레벨, 표제부와 별도 체계).
    pub mgm_bldrgst_pk: String,
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
    /// 1-based source line number inside the Bronze object when available.
    pub source_line_number: Option<u64>,
}

/// Input required to normalize building-register unit source rows into Silver rows.
pub struct BuildingRegisterUnitSilverRowsInput<'a> {
    /// Parsed provider source rows ordered by the caller.
    pub records: &'a [BuildingRegisterUnitSourceRow],
    /// Source-snapshot lineage id for this normalization batch.
    pub source_snapshot_id: &'a str,
    /// Bronze object key that carried these source rows.
    pub bronze_object_key: &'a str,
    /// UTC timestamp from which these source facts are valid.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Silver `silver.building_register_units` row prepared from one source row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterUnitSilverRow {
    /// Stable Silver row id.
    pub unit_row_id: String,
    /// Provider unit management primary key.
    pub mgm_bldrgst_pk: String,
    /// Standard 19-digit PNU (대지구분 1/2); null for block parcels (ADR 0023).
    pub pnu: Option<String>,
    /// Register-internal parcel key (hub-native composition; **not** a PNU).
    /// Total for every row — the register's own join/scope key.
    pub register_parcel_key: String,
    /// 동 join text (매칭 키; empty → null).
    pub dong_join_name: Option<String>,
    /// Raw 동명칭.
    pub dong_name_raw: String,
    /// Raw 호명칭.
    pub unit_name_raw: String,
    /// Extracted unit number (호번호).
    pub unit_number: Option<u32>,
    /// Explicit non-numeric unit label, when deterministically safe.
    pub unit_label_ko: Option<String>,
    /// Whitespace-compacted raw 호명 — collision-free matching designation
    /// (`D07-01호`, `아파트501`). Derived from `unit_name_raw`; null when empty.
    pub unit_designation: Option<String>,
    /// Canonical floor kind wire value.
    pub floor_kind: String,
    /// Signed floor position: above-ground positive, basement negative.
    pub floor_index: Option<i16>,
    /// Canonical floor number when accepted.
    pub floor_number: Option<u16>,
    /// 표제부 management key of the building this 호 belongs to, when linked.
    pub building_mgm_bldrgst_pk: Option<String>,
    /// How the building link was made: `canonical_dong`, `single_building_fallback`,
    /// or `unresolved`.
    pub building_link_method: String,
    /// Raw 주부속구분명 of the linked building (`주건축물` / `부속건축물`).
    pub building_main_or_annex: Option<String>,
    /// Unit count (호수) on the linked building's title card; `0` = no units,
    /// but the card is sometimes unfilled — evidence, not gospel.
    pub building_title_unit_count: Option<u32>,
    /// Normalization status wire value.
    pub normalization_status: String,
    /// Normalization reason wire value.
    pub normalization_reason: String,
    /// Active staff-approved normalization application id, when this row was overridden.
    pub normalization_application_id: Option<String>,
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

/// Staff-approved unit normalization override consumed by the Silver handoff pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterUnitSilverOverride {
    /// Target Silver row id, equal to `BuildingRegisterUnitSilverRow.unit_row_id`.
    pub target_unit_row_id: String,
    /// Staff-approved application id that produced this override.
    pub application_id: Option<String>,
    /// Approved unit number.
    pub unit_number: Option<u32>,
    /// Approved non-numeric unit label.
    pub unit_label_ko: Option<String>,
    /// Approved parent building management key, when known.
    pub building_mgm_bldrgst_pk: Option<String>,
    /// Approved building-link method.
    pub building_link_method: String,
    /// Approved normalization status.
    pub normalization_status: String,
    /// Approved normalization reason.
    pub normalization_reason: String,
}

/// Prevalidated index of active unit overrides keyed by Silver row id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterUnitSilverOverrideIndex {
    overrides_by_row: BTreeMap<String, BuildingRegisterUnitSilverOverride>,
}

impl BuildingRegisterUnitSilverOverrideIndex {
    /// Builds an override index and rejects duplicate target rows.
    ///
    /// # Errors
    /// Returns `BuildingRegisterUnitSilverPlanError` when an override is invalid or duplicated.
    pub fn new(
        overrides: &[BuildingRegisterUnitSilverOverride],
    ) -> Result<Self, BuildingRegisterUnitSilverPlanError> {
        let mut overrides_by_row = BTreeMap::new();
        for override_record in overrides {
            validate_unit_override(override_record)?;
            if overrides_by_row
                .insert(
                    override_record.target_unit_row_id.clone(),
                    override_record.clone(),
                )
                .is_some()
            {
                return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(format!(
                    "duplicate building-register unit override for {}",
                    override_record.target_unit_row_id
                )));
            }
        }
        Ok(Self { overrides_by_row })
    }

    /// Applies a matching override to a single Silver row.
    ///
    /// # Errors
    /// Returns `BuildingRegisterUnitSilverPlanError` when checksum recomputation fails.
    pub fn apply_to_row(
        &self,
        row: &mut BuildingRegisterUnitSilverRow,
    ) -> Result<bool, BuildingRegisterUnitSilverPlanError> {
        let Some(override_record) = self.overrides_by_row.get(row.unit_row_id.as_str()) else {
            return Ok(false);
        };
        row.unit_number = override_record.unit_number;
        row.unit_label_ko = override_record.unit_label_ko.clone();
        row.building_mgm_bldrgst_pk = override_record.building_mgm_bldrgst_pk.clone();
        row.building_link_method = override_record.building_link_method.clone();
        row.normalization_status = override_record.normalization_status.clone();
        row.normalization_reason = override_record.normalization_reason.clone();
        row.normalization_application_id = override_record.application_id.clone();
        row.row_checksum_sha256 = row_checksum(row)?;
        Ok(true)
    }
}

/// Error returned while normalizing building-register units into Silver rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BuildingRegisterUnitSilverPlanError {
    /// Input data cannot be represented as a Silver building-register unit row.
    #[error("invalid building-register unit Silver input: {0}")]
    InvalidInput(String),
}

/// Parses one hub.go.kr 전유부 TXT line into a Silver source row.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when lineage is invalid, the line has fewer
/// fields than the official 전유부 columns require, or the management key is empty.
pub fn parse_building_register_unit_source_row_from_hub_bulk_text_line(
    line: &str,
    bronze_object_key: &str,
    one_based_line_number: u64,
) -> Result<BuildingRegisterUnitSourceRow, BuildingRegisterUnitSilverPlanError> {
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
            "hub.go.kr 전유부 line {one_based_line_number} has {} fields, expected at least {MIN_FIELD_COUNT}",
            fields.len()
        )));
    }

    let mgm_bldrgst_pk = fields[MGM_BLDRGST_PK_INDEX].trim();
    if mgm_bldrgst_pk.is_empty() {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "hub.go.kr 전유부 management key must not be empty".to_owned(),
        ));
    }

    Ok(BuildingRegisterUnitSourceRow {
        source_record_id: format!("{bronze_object_key}#line-{one_based_line_number:06}"),
        mgm_bldrgst_pk: mgm_bldrgst_pk.to_owned(),
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
        source_line_number: Some(one_based_line_number),
    })
}

/// Normalizes building-register unit source rows into Silver rows.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when required lineage is empty or row JSON
/// serialization fails while computing checksums.
pub fn normalize_building_register_unit_silver_rows(
    input: &BuildingRegisterUnitSilverRowsInput<'_>,
) -> Result<Vec<BuildingRegisterUnitSilverRow>, BuildingRegisterUnitSilverPlanError> {
    normalize_building_register_unit_silver_rows_with_building_keys(
        input,
        &BuildingTitleKeyIndex::new(),
    )
}

/// Normalizes unit source rows into Silver rows and links each 호 to its building
/// via the 표제부 `(PNU + 동명)` index.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when required lineage is empty or row JSON
/// serialization fails while computing checksums.
pub fn normalize_building_register_unit_silver_rows_with_building_keys(
    input: &BuildingRegisterUnitSilverRowsInput<'_>,
    building_keys: &BuildingTitleKeyIndex,
) -> Result<Vec<BuildingRegisterUnitSilverRow>, BuildingRegisterUnitSilverPlanError> {
    if input.source_snapshot_id.trim().is_empty() || input.bronze_object_key.trim().is_empty() {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "source_snapshot_id and bronze_object_key must not be empty".to_owned(),
        ));
    }

    input
        .records
        .iter()
        .map(|record| build_silver_row(record, input, building_keys))
        .collect()
}

/// Applies active staff-approved unit overrides to Silver rows and recomputes row checksums.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when override identity or machine fields are
/// invalid, duplicate, or cannot be serialized for checksum recomputation.
pub fn apply_building_register_unit_silver_overrides(
    rows: &mut [BuildingRegisterUnitSilverRow],
    overrides: &[BuildingRegisterUnitSilverOverride],
) -> Result<usize, BuildingRegisterUnitSilverPlanError> {
    let index = BuildingRegisterUnitSilverOverrideIndex::new(overrides)?;
    let mut applied = 0usize;
    for row in rows {
        if index.apply_to_row(row)? {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Parses one active `normalization_application.after_snapshot` into a Silver unit override.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when required identity or proposed fields are
/// missing or invalid.
pub fn building_register_unit_silver_override_from_application_snapshot(
    snapshot: &JsonValue,
) -> Result<BuildingRegisterUnitSilverOverride, BuildingRegisterUnitSilverPlanError> {
    let target_identity = snapshot
        .get("target_identity")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            BuildingRegisterUnitSilverPlanError::InvalidInput(
                "application after_snapshot.target_identity must be an object".to_owned(),
            )
        })?;
    let target_unit_row_id = target_identity
        .get("raw_record_id")
        .and_then(JsonValue::as_str)
        .or_else(|| {
            target_identity
                .get("silver_row_id")
                .and_then(JsonValue::as_str)
        })
        .map(str::to_owned)
        .ok_or_else(|| {
            BuildingRegisterUnitSilverPlanError::InvalidInput(
                "application target_identity.raw_record_id or silver_row_id must be a string"
                    .to_owned(),
            )
        })?;
    let proposed_record = snapshot
        .get("proposed_record")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            BuildingRegisterUnitSilverPlanError::InvalidInput(
                "application after_snapshot.proposed_record must be an object".to_owned(),
            )
        })?;
    let unit_number = proposed_record
        .get("unit_number")
        .and_then(JsonValue::as_u64)
        .map(u32::try_from)
        .transpose()
        .map_err(|_| {
            BuildingRegisterUnitSilverPlanError::InvalidInput(
                "proposed_record.unit_number exceeds u32".to_owned(),
            )
        })?;
    let building_mgm_bldrgst_pk = proposed_record
        .get("building_mgm_bldrgst_pk")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let unit_label_ko = proposed_record
        .get("unit_label_ko")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let override_record = BuildingRegisterUnitSilverOverride {
        target_unit_row_id,
        application_id: None,
        unit_number,
        unit_label_ko,
        building_mgm_bldrgst_pk,
        building_link_method: required_proposed_text(proposed_record, "building_link_method")?,
        normalization_status: required_proposed_text(proposed_record, "normalization_status")?,
        normalization_reason: required_proposed_text(proposed_record, "normalization_reason")?,
    };
    validate_unit_override(&override_record)?;
    Ok(override_record)
}

fn required_proposed_text(
    proposed_record: &JsonMap<String, JsonValue>,
    field: &str,
) -> Result<String, BuildingRegisterUnitSilverPlanError> {
    proposed_record
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            BuildingRegisterUnitSilverPlanError::InvalidInput(format!(
                "proposed_record.{field} must be a string"
            ))
        })
}

fn validate_unit_override(
    override_record: &BuildingRegisterUnitSilverOverride,
) -> Result<(), BuildingRegisterUnitSilverPlanError> {
    validate_non_empty("target_unit_row_id", &override_record.target_unit_row_id)?;
    validate_non_empty(
        "building_link_method",
        &override_record.building_link_method,
    )?;
    validate_non_empty(
        "normalization_status",
        &override_record.normalization_status,
    )?;
    validate_non_empty(
        "normalization_reason",
        &override_record.normalization_reason,
    )?;
    if let Some(application_id) = &override_record.application_id {
        validate_non_empty("application_id", application_id)?;
    }
    if !matches!(
        override_record.normalization_status.as_str(),
        "accepted" | "proposal_required"
    ) {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
            "normalization_status must be accepted or proposal_required".to_owned(),
        ));
    }
    Ok(())
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), BuildingRegisterUnitSilverPlanError> {
    if value.trim().is_empty() {
        return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn build_silver_row(
    record: &BuildingRegisterUnitSourceRow,
    input: &BuildingRegisterUnitSilverRowsInput<'_>,
    building_keys: &BuildingTitleKeyIndex,
) -> Result<BuildingRegisterUnitSilverRow, BuildingRegisterUnitSilverPlanError> {
    let normalized = normalize_building_register_unit(RawBuildingRegisterUnit {
        dong_name: &record.dong_name_raw,
        unit_name: &record.unit_name_raw,
        floor: RawBuildingRegisterFloor {
            floor_type_code: &record.floor_type_code_raw,
            floor_type_name: &record.floor_type_name_raw,
            floor_number: &record.floor_number_raw,
            floor_label: None,
        },
    });
    let building_link = building_keys.resolve(&record.register_parcel_key, &record.dong_name_raw);

    let mut row = BuildingRegisterUnitSilverRow {
        unit_row_id: format!("building-register-unit:{}", record.source_record_id),
        mgm_bldrgst_pk: record.mgm_bldrgst_pk.clone(),
        pnu: record.pnu.clone(),
        register_parcel_key: record.register_parcel_key.clone(),
        dong_join_name: normalized.dong_join_name.clone(),
        dong_name_raw: record.dong_name_raw.clone(),
        unit_name_raw: record.unit_name_raw.clone(),
        unit_number: normalized.unit_number,
        unit_label_ko: normalized.unit_label_ko.clone(),
        unit_designation: normalized.unit_designation.clone(),
        floor_kind: normalized.floor.kind.wire_name().to_owned(),
        floor_index: normalized.floor.floor_index,
        floor_number: normalized.floor.floor_number,
        building_mgm_bldrgst_pk: building_link.building_mgm_bldrgst_pk,
        building_link_method: building_link.method.to_owned(),
        building_main_or_annex: building_link.building_main_or_annex,
        building_title_unit_count: building_link.building_title_unit_count,
        normalization_status: normalized.status.wire_name().to_owned(),
        normalization_reason: unit_reason_wire(&normalized),
        normalization_application_id: None,
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

/// ADR 0023 재유입 차단 불변식: 표준 `pnu`가 없는 행은 블록 필지(내부 키의
/// 대지구분 자리가 `2`)뿐이어야 하고, 블록 필지는 표준 `pnu`를 가질 수 없다.
/// 조립 함수 드리프트를 소스에서 시끄럽게 잡는다. 전유부·전유공용면적 plan 공용.
pub(crate) fn validate_pnu_block_invariant(
    pnu: Option<&str>,
    register_parcel_key: &str,
) -> Result<(), BuildingRegisterUnitSilverPlanError> {
    let is_block = register_parcel_key.as_bytes().get(10) == Some(&b'2');
    if pnu.is_none() == is_block {
        return Ok(());
    }
    Err(BuildingRegisterUnitSilverPlanError::InvalidInput(format!(
        "pnu/block invariant violated: pnu={pnu:?}, register_parcel_key={register_parcel_key}"
    )))
}

fn unit_reason_wire(normalized: &NormalizedBuildingRegisterUnit) -> String {
    match normalized.reason {
        BuildingRegisterUnitReason::AcceptedNumericUnit => "accepted_numeric_unit",
        BuildingRegisterUnitReason::AcceptedUnitLabel => "accepted_unit_label",
        BuildingRegisterUnitReason::EmptyUnitName => "empty_unit_name",
        BuildingRegisterUnitReason::NoUnitNumber => "no_unit_number",
    }
    .to_owned()
}

fn row_to_json_value(row: &BuildingRegisterUnitSilverRow) -> JsonValue {
    let mut record = JsonMap::new();
    insert_string(&mut record, "unit_row_id", &row.unit_row_id);
    insert_string(&mut record, "mgm_bldrgst_pk", &row.mgm_bldrgst_pk);
    insert_optional_string(&mut record, "pnu", row.pnu.as_deref());
    insert_string(&mut record, "register_parcel_key", &row.register_parcel_key);
    insert_optional_string(&mut record, "dong_join_name", row.dong_join_name.as_deref());
    insert_string(&mut record, "dong_name_raw", &row.dong_name_raw);
    insert_string(&mut record, "unit_name_raw", &row.unit_name_raw);
    insert_optional_number(&mut record, "unit_number", row.unit_number);
    insert_optional_string(&mut record, "unit_label_ko", row.unit_label_ko.as_deref());
    insert_optional_string(
        &mut record,
        "unit_designation",
        row.unit_designation.as_deref(),
    );
    insert_string(&mut record, "floor_kind", &row.floor_kind);
    insert_optional_number(&mut record, "floor_index", row.floor_index);
    insert_optional_number(&mut record, "floor_number", row.floor_number);
    insert_optional_string(
        &mut record,
        "building_mgm_bldrgst_pk",
        row.building_mgm_bldrgst_pk.as_deref(),
    );
    insert_string(
        &mut record,
        "building_link_method",
        &row.building_link_method,
    );
    insert_optional_string(
        &mut record,
        "building_main_or_annex",
        row.building_main_or_annex.as_deref(),
    );
    insert_optional_number(
        &mut record,
        "building_title_unit_count",
        row.building_title_unit_count,
    );
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
    insert_optional_string(
        &mut record,
        "normalization_application_id",
        row.normalization_application_id.as_deref(),
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

/// Serializes a Silver unit row to one compact JSON line.
///
/// # Errors
/// Returns `BuildingRegisterUnitSilverPlanError` when JSON serialization fails.
pub fn building_register_unit_silver_row_to_jsonl(
    row: &BuildingRegisterUnitSilverRow,
) -> Result<String, BuildingRegisterUnitSilverPlanError> {
    serde_json::to_string(&row_to_json_value(row))
        .map_err(|error| BuildingRegisterUnitSilverPlanError::InvalidInput(error.to_string()))
}

fn row_checksum(
    row: &BuildingRegisterUnitSilverRow,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn line(dong: &str, unit: &str, floor_code: &str, floor_name: &str, floor_no: &str) -> String {
        let mut fields = vec![String::new(); MIN_FIELD_COUNT];
        fields[MGM_BLDRGST_PK_INDEX] = "1002129933".to_owned();
        fields[SIGUNGU_CODE_INDEX] = "99999".to_owned();
        fields[BEOPJEONGDONG_CODE_INDEX] = "00401".to_owned();
        fields[DAEJI_KIND_INDEX] = "0".to_owned();
        fields[BONBEON_INDEX] = "0089".to_owned();
        fields[BUBEON_INDEX] = "0004".to_owned();
        fields[DONG_NAME_INDEX] = dong.to_owned();
        fields[UNIT_NAME_INDEX] = unit.to_owned();
        fields[FLOOR_TYPE_CODE_INDEX] = floor_code.to_owned();
        fields[FLOOR_TYPE_NAME_INDEX] = floor_name.to_owned();
        fields[FLOOR_NUMBER_INDEX] = floor_no.to_owned();
        fields.join("|")
    }

    fn normalize_one(
        raw_line: &str,
    ) -> Result<BuildingRegisterUnitSilverRow, Box<dyn std::error::Error>> {
        let record = parse_building_register_unit_source_row_from_hub_bulk_text_line(
            raw_line,
            "bronze/source=hubgokr__building_register_exclusive_unit/x.zip",
            1,
        )?;
        let valid_from_utc = DateTime::parse_from_rfc3339("2026-06-20T00:00:00Z")?.to_utc();
        let ingested_at_utc = DateTime::parse_from_rfc3339("2026-07-05T00:00:00Z")?.to_utc();
        let mut rows =
            normalize_building_register_unit_silver_rows(&BuildingRegisterUnitSilverRowsInput {
                records: std::slice::from_ref(&record),
                source_snapshot_id: "hub-2026-06",
                bronze_object_key: "bronze/source=hubgokr__building_register_exclusive_unit/x.zip",
                valid_from_utc,
                ingested_at_utc,
            })?;
        rows.pop().ok_or_else(|| {
            std::io::Error::other("single-row unit normalization should produce one row").into()
        })
    }

    #[test]
    fn parses_and_normalizes_a_unit_row() -> Result<(), Box<dyn std::error::Error>> {
        let row = normalize_one(&line("102동", "624호", "20", "지상", "6"))?;
        // 표준 PNU: 허브 대지구분 0(대지) → 표준 1(일반) — ADR 0023.
        assert_eq!(row.pnu.as_deref(), Some("9999900401100890004"));
        // 내부 조인 키는 허브 조립 그대로 (PNU 아님).
        assert_eq!(row.register_parcel_key, "9999900401000890004");
        assert_eq!(row.dong_join_name.as_deref(), Some("102동"));
        assert_eq!(row.unit_number, Some(624));
        assert_eq!(row.floor_kind, "above_ground");
        assert_eq!(row.floor_index, Some(6));
        assert_eq!(row.normalization_status, "accepted");
        assert_eq!(row.row_checksum_sha256.len(), 64);
        Ok(())
    }

    #[test]
    fn pnu_block_invariant_rejects_drift() {
        // 불변식: pnu 빈값 ⟺ 내부 키 대지구분 '2'(블록). 조립이 한쪽만 바뀌는
        // 미래 드리프트를 소스에서 시끄럽게 잡는다 (ADR 0023 재유입 차단).
        assert!(
            validate_pnu_block_invariant(Some("9999900401100890004"), "9999900401000890004")
                .is_ok()
        );
        assert!(validate_pnu_block_invariant(None, "9999900901205290000").is_ok());
        // 블록인데 pnu가 있음 / 블록이 아닌데 pnu가 없음 — 둘 다 거부.
        assert!(
            validate_pnu_block_invariant(Some("9999900901205290000"), "9999900901205290000")
                .is_err()
        );
        assert!(validate_pnu_block_invariant(None, "9999900401000890004").is_err());
    }

    #[test]
    fn block_parcels_have_no_standard_pnu_but_keep_register_key(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut fields = vec![String::new(); MIN_FIELD_COUNT];
        fields[MGM_BLDRGST_PK_INDEX] = "1002129999".to_owned();
        fields[SIGUNGU_CODE_INDEX] = "99999".to_owned();
        fields[BEOPJEONGDONG_CODE_INDEX] = "00901".to_owned();
        fields[DAEJI_KIND_INDEX] = "2".to_owned(); // 허브 2 = 블록
        fields[BONBEON_INDEX] = "0529".to_owned();
        fields[BUBEON_INDEX] = "0000".to_owned();
        fields[UNIT_NAME_INDEX] = "101호".to_owned();
        fields[FLOOR_TYPE_CODE_INDEX] = "20".to_owned();
        fields[FLOOR_NUMBER_INDEX] = "1".to_owned();
        let row = normalize_one(&fields.join("|"))?;

        // 블록은 표준 PNU가 존재하지 않는다 — 날조 금지 (ADR 0023).
        assert_eq!(row.pnu, None);
        assert_eq!(row.register_parcel_key, "9999900901205290000");
        let jsonl = building_register_unit_silver_row_to_jsonl(&row)?;
        assert!(jsonl.contains("\"pnu\":null"), "{jsonl}");
        assert!(
            jsonl.contains("\"register_parcel_key\":\"9999900901205290000\""),
            "{jsonl}"
        );
        Ok(())
    }

    #[test]
    fn basement_unit_gets_negative_floor() -> Result<(), Box<dyn std::error::Error>> {
        let row = normalize_one(&line("", "7호", "10", "지하", "1"))?;
        assert_eq!(row.unit_number, Some(7));
        assert_eq!(row.floor_kind, "basement");
        assert_eq!(row.floor_index, Some(-1));
        assert_eq!(row.dong_join_name, None);
        Ok(())
    }

    #[test]
    fn preserves_non_numeric_unit_label_in_silver_row() -> Result<(), Box<dyn std::error::Error>> {
        let row = normalize_one(&line("", "가호", "20", "above", "1"))?;
        assert_eq!(row.unit_number, None);
        assert_eq!(row.unit_label_ko.as_deref(), Some("가호"));
        assert_eq!(row.normalization_status, "accepted");
        assert_eq!(row.normalization_reason, "accepted_unit_label");
        Ok(())
    }

    #[test]
    fn carries_building_title_attrs_in_silver_row() -> Result<(), Box<dyn std::error::Error>> {
        use crate::building_register_title::BuildingTitleLinkEntry;

        let record = parse_building_register_unit_source_row_from_hub_bulk_text_line(
            &line("301동", "", "20", "above", "1"),
            "bronze/source=hubgokr__building_register_exclusive_unit/x.zip",
            1,
        )?;
        let mut index = BuildingTitleKeyIndex::new();
        index.insert(BuildingTitleLinkEntry {
            register_parcel_key: record.register_parcel_key.clone(),
            canonical_dong: "301".to_owned(),
            mgm_bldrgst_pk: "1002110000".to_owned(),
            main_or_annex: Some("부속건축물".to_owned()),
            title_unit_count: Some(0),
        });
        let valid_from_utc = DateTime::parse_from_rfc3339("2026-06-20T00:00:00Z")?.to_utc();
        let ingested_at_utc = DateTime::parse_from_rfc3339("2026-07-05T00:00:00Z")?.to_utc();
        let rows = normalize_building_register_unit_silver_rows_with_building_keys(
            &BuildingRegisterUnitSilverRowsInput {
                records: std::slice::from_ref(&record),
                source_snapshot_id: "hub-2026-06",
                bronze_object_key: "bronze/source=hubgokr__building_register_exclusive_unit/x.zip",
                valid_from_utc,
                ingested_at_utc,
            },
            &index,
        )?;
        let row = rows.first().ok_or("one row expected")?;
        assert_eq!(row.building_mgm_bldrgst_pk.as_deref(), Some("1002110000"));
        assert_eq!(row.building_main_or_annex.as_deref(), Some("부속건축물"));
        assert_eq!(row.building_title_unit_count, Some(0));
        let jsonl = building_register_unit_silver_row_to_jsonl(row)?;
        assert!(
            jsonl.contains("\"building_main_or_annex\":\"부속건축물\""),
            "{jsonl}"
        );
        assert!(jsonl.contains("\"building_title_unit_count\":0"), "{jsonl}");

        // Unlinked rows carry nulls.
        let unlinked = normalize_one(&line("", "624호", "20", "above", "6"))?;
        assert_eq!(unlinked.building_main_or_annex, None);
        assert_eq!(unlinked.building_title_unit_count, None);
        Ok(())
    }

    #[test]
    fn carries_unit_designation_in_silver_row() -> Result<(), Box<dyn std::error::Error>> {
        let row = normalize_one(&line("", "D07-01 호", "20", "above", "1"))?;
        assert_eq!(row.unit_designation.as_deref(), Some("D07-01호"));
        let jsonl = building_register_unit_silver_row_to_jsonl(&row)?;
        assert!(
            jsonl.contains("\"unit_designation\":\"D07-01호\""),
            "{jsonl}"
        );

        let empty = normalize_one(&line("", "", "20", "above", "1"))?;
        assert_eq!(empty.unit_designation, None);
        Ok(())
    }

    #[test]
    fn active_unit_override_updates_silver_row_and_recomputes_checksum(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut row = normalize_one(&line("A", "unit", "20", "above", "1"))?;
        let original_checksum = row.row_checksum_sha256.clone();
        let override_record = BuildingRegisterUnitSilverOverride {
            target_unit_row_id: row.unit_row_id.clone(),
            application_id: Some("normalization-application-approved-1".to_owned()),
            unit_number: Some(101),
            unit_label_ko: None,
            building_mgm_bldrgst_pk: Some("building-pk-approved".to_owned()),
            building_link_method: "canonical_dong".to_owned(),
            normalization_status: "accepted".to_owned(),
            normalization_reason: "accepted_numeric_unit".to_owned(),
        };

        let applied = apply_building_register_unit_silver_overrides(
            std::slice::from_mut(&mut row),
            std::slice::from_ref(&override_record),
        )?;

        assert_eq!(applied, 1);
        assert_eq!(row.unit_number, Some(101));
        assert_eq!(
            row.building_mgm_bldrgst_pk.as_deref(),
            Some("building-pk-approved")
        );
        assert_eq!(row.building_link_method, "canonical_dong");
        assert_eq!(row.normalization_status, "accepted");
        assert_eq!(row.normalization_reason, "accepted_numeric_unit");
        assert_ne!(row.row_checksum_sha256, original_checksum);
        Ok(())
    }

    #[test]
    fn parses_application_snapshot_into_unit_override() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = serde_json::json!({
            "target_identity": {
                "raw_record_id": "building-register-unit:bronze/source=x.zip#line-000001"
            },
            "proposed_record": {
                "unit_number": 101,
                "building_mgm_bldrgst_pk": "building-pk-approved",
                "building_link_method": "canonical_dong",
                "normalization_status": "accepted",
                "normalization_reason": "accepted_numeric_unit"
            }
        });

        let override_record =
            building_register_unit_silver_override_from_application_snapshot(&snapshot)?;

        assert_eq!(
            override_record.target_unit_row_id,
            "building-register-unit:bronze/source=x.zip#line-000001"
        );
        assert_eq!(override_record.unit_number, Some(101));
        assert_eq!(
            override_record.building_mgm_bldrgst_pk.as_deref(),
            Some("building-pk-approved")
        );
        assert_eq!(override_record.building_link_method, "canonical_dong");
        Ok(())
    }

    #[test]
    fn parses_application_snapshot_with_silver_row_id_into_unit_override(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = serde_json::json!({
            "target_identity": {
                "silver_row_id": "building-register-unit:bronze/source=synthetic-fixture.zip#line-000001",
                "source_line_number": 1
            },
            "proposed_record": {
                "unit_number": null,
                "building_mgm_bldrgst_pk": "SYNTHETIC-BUILDING-PK-0001",
                "building_link_method": "canonical_dong",
                "normalization_status": "proposal_required",
                "normalization_reason": "manual review required"
            }
        });

        let override_record =
            building_register_unit_silver_override_from_application_snapshot(&snapshot)?;

        assert_eq!(
            override_record.target_unit_row_id,
            "building-register-unit:bronze/source=synthetic-fixture.zip#line-000001"
        );
        assert_eq!(override_record.unit_number, None);
        assert_eq!(
            override_record.building_mgm_bldrgst_pk.as_deref(),
            Some("SYNTHETIC-BUILDING-PK-0001")
        );
        assert_eq!(override_record.normalization_status, "proposal_required");
        Ok(())
    }

    #[test]
    fn numberless_unit_name_is_a_proposal() -> Result<(), Box<dyn std::error::Error>> {
        let row = normalize_one(&line("A", ".", "20", "above", "1"))?;
        assert_eq!(row.unit_number, None);
        assert_eq!(row.unit_label_ko, None);
        assert_eq!(row.normalization_status, "proposal_required");
        assert_eq!(row.normalization_reason, "no_unit_number");
        Ok(())
    }
}
