//! hub.go.kr bulk TXT source-row parsing for building-register floor Silver normalization.

use super::{
    validate_lineage_part, BuildingRegisterFloorSilverPlanError, BuildingRegisterFloorSourceRow,
};

const MGM_BLDRGST_PK_INDEX: usize = 0;
const FLOOR_TYPE_CODE_INDEX: usize = 18;
const FLOOR_TYPE_NAME_INDEX: usize = 19;
const FLOOR_NUMBER_INDEX: usize = 20;
const FLOOR_LABEL_INDEX: usize = 21;
const MIN_FIELD_COUNT: usize = FLOOR_LABEL_INDEX + 1;

/// Parses one hub.go.kr building-register floor-overview TXT line into a Silver source row.
///
/// The provider file is UTF-8 pipe-delimited text with no header. This parser only extracts the
/// fields required by the floor normalizer and keeps source identity tied to the Bronze object key
/// plus 1-based line number.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when lineage is invalid, the line has fewer
/// fields than the official floor columns require, or the stable provider row identity is empty.
/// Floor attributes are preserved as raw provider values even when empty so the deterministic
/// normalizer can route incomplete rows to proposal review instead of dropping the batch.
pub fn parse_building_register_floor_source_row_from_hub_bulk_text_line(
    line: &str,
    bronze_object_key: &str,
    one_based_line_number: u64,
) -> Result<BuildingRegisterFloorSourceRow, BuildingRegisterFloorSilverPlanError> {
    validate_lineage_part("bronze_object_key", bronze_object_key)?;
    if one_based_line_number == 0 {
        return Err(BuildingRegisterFloorSilverPlanError::InvalidInput(
            "source line number must be 1-based".to_owned(),
        ));
    }

    let fields = line.split('|').collect::<Vec<_>>();
    if fields.len() < MIN_FIELD_COUNT {
        return Err(BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
            "hub.go.kr building-register floor line {one_based_line_number} has {} fields, expected at least {MIN_FIELD_COUNT}",
            fields.len()
        )));
    }

    Ok(BuildingRegisterFloorSourceRow {
        source_record_id: format!("{bronze_object_key}#line-{one_based_line_number:06}"),
        mgm_bldrgst_pk: required_field(&fields, MGM_BLDRGST_PK_INDEX, "mgm_bldrgst_pk")?,
        floor_type_code_raw: raw_field(&fields, FLOOR_TYPE_CODE_INDEX),
        floor_type_name_raw: raw_field(&fields, FLOOR_TYPE_NAME_INDEX),
        floor_number_raw: raw_field(&fields, FLOOR_NUMBER_INDEX),
        floor_label_raw: optional_field(&fields, FLOOR_LABEL_INDEX),
        source_line_number: Some(one_based_line_number),
    })
}

fn required_field(
    fields: &[&str],
    index: usize,
    name: &'static str,
) -> Result<String, BuildingRegisterFloorSilverPlanError> {
    let value = fields[index].trim();
    if value.is_empty() {
        return Err(BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
            "hub.go.kr building-register floor {name} field must not be empty"
        )));
    }
    Ok(value.to_owned())
}

fn raw_field(fields: &[&str], index: usize) -> String {
    fields[index].trim().to_owned()
}

fn optional_field(fields: &[&str], index: usize) -> Option<String> {
    let value = fields[index].trim();
    (!value.is_empty()).then(|| value.to_owned())
}
