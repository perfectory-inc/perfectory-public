//! Building-register-unit proposal schema validation.

use serde_json::Value as JsonValue;

use crate::NormalizationError;

/// Stable schema used by reviewed building-register-unit proposals.
pub const BUILDING_REGISTER_UNIT_SCHEMA_VERSION: &str = "building_register_unit.normalized.v1";

/// Validates the persisted schema and proposed-record contract for a unit override.
///
/// # Errors
/// Returns [`NormalizationError::InvalidInput`] when either schema version, the target identity,
/// or the proposed record violates the existing unit override contract.
pub fn validate_building_register_unit_proposal(
    target_schema_version: &str,
    proposal_schema_version: &str,
    target_identity: &JsonValue,
    proposed_record: &JsonValue,
) -> Result<(), NormalizationError> {
    if target_schema_version != BUILDING_REGISTER_UNIT_SCHEMA_VERSION {
        return Err(NormalizationError::InvalidInput(format!(
            "target_schema_version must be {BUILDING_REGISTER_UNIT_SCHEMA_VERSION}"
        )));
    }
    if proposal_schema_version != BUILDING_REGISTER_UNIT_SCHEMA_VERSION {
        return Err(NormalizationError::InvalidInput(format!(
            "proposal_schema_version must be {BUILDING_REGISTER_UNIT_SCHEMA_VERSION}"
        )));
    }
    validate_building_register_unit_target_identity(target_identity)?;
    let proposed_record = proposed_record.as_object().ok_or_else(|| {
        NormalizationError::InvalidInput(
            "building_register_unit proposed_record must be a JSON object".to_owned(),
        )
    })?;
    let normalization_status = proposed_record
        .get("normalization_status")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            NormalizationError::InvalidInput(
                "building_register_unit proposed_record.normalization_status is required"
                    .to_owned(),
            )
        })?;
    if !matches!(normalization_status, "accepted" | "proposal_required") {
        return Err(NormalizationError::InvalidInput(
            "building_register_unit proposed_record.normalization_status must be accepted or proposal_required"
                .to_owned(),
        ));
    }
    if let Some(unit_number) = proposed_record.get("unit_number") {
        if !(unit_number.is_null() || unit_number.as_u64().is_some()) {
            return Err(NormalizationError::InvalidInput(
                "building_register_unit proposed_record.unit_number must be null or unsigned integer"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

/// Validates the stable v1 identity for a building-register-unit override.
///
/// # Errors
/// Returns [`NormalizationError::InvalidInput`] unless the identity contains exactly the
/// non-empty string fields `source_system` and `raw_record_id`.
pub fn validate_building_register_unit_target_identity(
    target_identity: &JsonValue,
) -> Result<(), NormalizationError> {
    parse_target_identity(target_identity).map(|_| ())
}

/// Validates a v1 unit identity and matches it to its submitted source record.
///
/// # Errors
/// Returns [`NormalizationError::InvalidInput`] when the identity shape is invalid or either
/// identity value differs from the submitted `source_system` or `raw_record_id`.
pub fn validate_building_register_unit_target_identity_matches(
    target_identity: &JsonValue,
    source_system: &str,
    raw_record_id: &str,
) -> Result<(), NormalizationError> {
    let (identity_source_system, identity_raw_record_id) = parse_target_identity(target_identity)?;
    if identity_source_system != source_system {
        return Err(NormalizationError::InvalidInput(
            "building_register_unit target_identity.source_system must match source_system"
                .to_owned(),
        ));
    }
    if identity_raw_record_id != raw_record_id {
        return Err(NormalizationError::InvalidInput(
            "building_register_unit target_identity.raw_record_id must match raw_record_id"
                .to_owned(),
        ));
    }
    Ok(())
}

fn parse_target_identity(target_identity: &JsonValue) -> Result<(&str, &str), NormalizationError> {
    let target_identity = target_identity.as_object().ok_or_else(|| {
        NormalizationError::InvalidInput("target_identity must be a JSON object".to_owned())
    })?;
    let source_system = required_identity_string(target_identity, "source_system")?;
    let raw_record_id = required_identity_string(target_identity, "raw_record_id")?;
    if target_identity.len() != 2 {
        return Err(NormalizationError::InvalidInput(
            "building_register_unit target_identity must contain exactly source_system and raw_record_id"
                .to_owned(),
        ));
    }
    Ok((source_system, raw_record_id))
}

fn required_identity_string<'a>(
    target_identity: &'a serde_json::Map<String, JsonValue>,
    field: &str,
) -> Result<&'a str, NormalizationError> {
    let value = target_identity.get(field).ok_or_else(|| {
        NormalizationError::InvalidInput(format!(
            "building_register_unit target_identity.{field} is required"
        ))
    })?;
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            NormalizationError::InvalidInput(format!(
                "building_register_unit target_identity.{field} must be a non-empty string"
            ))
        })
}
