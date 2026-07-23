//! data.go.kr source-row parsing for building-register floor Silver normalization.

use serde_json::{Map as JsonMap, Value as JsonValue};

use super::{
    validate_lineage_part, BuildingRegisterFloorSilverPlanError, BuildingRegisterFloorSourceRow,
};

/// Parses data.go.kr building-register floor-overview JSON into Silver source rows.
///
/// This parser is deliberately narrow: it only accepts the public-data envelope used by
/// `BldRgstHubService` JSON pages and only extracts the floor identity fields needed by the
/// deterministic Silver normalizer. It does not infer missing provider fields.
///
/// # Errors
/// Returns `BuildingRegisterFloorSilverPlanError` when the envelope shape is invalid, required
/// row fields are missing, or provider values are not scalar strings/numbers.
pub fn parse_building_register_floor_source_rows_from_public_data_json(
    payload: &JsonValue,
    bronze_object_key: &str,
) -> Result<Vec<BuildingRegisterFloorSourceRow>, BuildingRegisterFloorSilverPlanError> {
    validate_lineage_part("bronze_object_key", bronze_object_key)?;
    let Some(item_value) = payload.pointer("/response/body/items/item") else {
        return Ok(Vec::new());
    };

    match item_value {
        JsonValue::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, item)| parse_public_data_floor_item(item, bronze_object_key, index + 1))
            .collect(),
        JsonValue::Object(_) => {
            parse_public_data_floor_item(item_value, bronze_object_key, 1).map(|row| vec![row])
        }
        JsonValue::Null => Ok(Vec::new()),
        _ => Err(BuildingRegisterFloorSilverPlanError::InvalidInput(
            "response.body.items.item must be an object, array, null, or omitted".to_owned(),
        )),
    }
}

fn parse_public_data_floor_item(
    item: &JsonValue,
    bronze_object_key: &str,
    one_based_index: usize,
) -> Result<BuildingRegisterFloorSourceRow, BuildingRegisterFloorSilverPlanError> {
    let record = item.as_object().ok_or_else(|| {
        BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
            "response.body.items.item[{one_based_index}] must be an object"
        ))
    })?;
    let source_line_number = u64::try_from(one_based_index).map_err(|_| {
        BuildingRegisterFloorSilverPlanError::InvalidInput(
            "source item index exceeds u64".to_owned(),
        )
    })?;

    Ok(BuildingRegisterFloorSourceRow {
        source_record_id: format!("{bronze_object_key}#item-{one_based_index:06}"),
        mgm_bldrgst_pk: required_provider_scalar(record, "mgmBldrgstPk", one_based_index)?,
        floor_type_code_raw: required_provider_scalar(record, "flrGbCd", one_based_index)?,
        floor_type_name_raw: required_provider_scalar(record, "flrGbCdNm", one_based_index)?,
        floor_number_raw: required_provider_scalar(record, "flrNo", one_based_index)?,
        floor_label_raw: optional_provider_scalar(record, "flrNoNm", one_based_index)?,
        source_line_number: Some(source_line_number),
    })
}

fn required_provider_scalar(
    record: &JsonMap<String, JsonValue>,
    field: &'static str,
    one_based_index: usize,
) -> Result<String, BuildingRegisterFloorSilverPlanError> {
    let value = record.get(field).ok_or_else(|| {
        BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
            "response.body.items.item[{one_based_index}] omitted required field {field}"
        ))
    })?;
    provider_scalar_to_string(value, field, one_based_index)
}

fn optional_provider_scalar(
    record: &JsonMap<String, JsonValue>,
    field: &'static str,
    one_based_index: usize,
) -> Result<Option<String>, BuildingRegisterFloorSilverPlanError> {
    match record.get(field) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(value) => provider_scalar_to_string(value, field, one_based_index).map(Some),
    }
}

fn provider_scalar_to_string(
    value: &JsonValue,
    field: &'static str,
    one_based_index: usize,
) -> Result<String, BuildingRegisterFloorSilverPlanError> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Bool(_) | JsonValue::Array(_) | JsonValue::Object(_) | JsonValue::Null => {
            Err(BuildingRegisterFloorSilverPlanError::InvalidInput(format!(
                "response.body.items.item[{one_based_index}].{field} must be a string or number"
            )))
        }
    }
}
