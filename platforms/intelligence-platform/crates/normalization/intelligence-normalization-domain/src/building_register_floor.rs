use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

use intelligence_contracts::TraceContext;

use crate::NormalizationRequest;

const INPUT_SCHEMA_VERSION: &str = "foundation-platform.floor_entity_context_pack.v1";
const TARGET_KIND: &str = "building_register_floor";
const TARGET_SCHEMA_VERSION: &str = "building_register_floor.normalized.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterFloorProposalInputContext {
    pub tenant_id: String,
    pub trace_id: String,
    pub human_user_id: String,
    pub product_id: String,
}

#[derive(Debug, Error)]
pub enum BuildingRegisterFloorProposalInputError {
    #[error("invalid building-register floor proposal input JSON at line {line}: {message}")]
    InvalidJson { line: usize, message: String },
    #[error("invalid building-register floor proposal input at line {line}: {message}")]
    InvalidRow { line: usize, message: String },
}

#[derive(Debug, Deserialize)]
struct BuildingRegisterFloorContextPack {
    schema_version: String,
    context_pack_id: String,
    source_system: String,
    target: BuildingRegisterFloorContextTarget,
    building_identity_candidate: Value,
    entity_impact: Value,
    semantic_contract: Value,
    target_raw_floor: Value,
    current_deterministic_normalization: Value,
    same_building_floor_sequence: Value,
    building_title_context: Value,
    unit_context_summary: Value,
    policy_context: Value,
    allowed_output_contract: Value,
    trace: Value,
}

#[derive(Debug, Deserialize)]
struct BuildingRegisterFloorContextTarget {
    target_kind: String,
    raw_record_id: String,
    silver_row_id: String,
    bronze_object_key: String,
    row_checksum_sha256: String,
    source_snapshot_id: String,
    source_line_number: u64,
}

pub fn building_register_floor_requests_from_jsonl(
    jsonl: &str,
    context: &BuildingRegisterFloorProposalInputContext,
) -> Result<Vec<NormalizationRequest>, BuildingRegisterFloorProposalInputError> {
    jsonl
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line.trim().trim_start_matches('\u{feff}');
            (!line.is_empty()).then_some((index + 1, line))
        })
        .map(|(line, row)| {
            let row: BuildingRegisterFloorContextPack =
                serde_json::from_str(row).map_err(|error| {
                    BuildingRegisterFloorProposalInputError::InvalidJson {
                        line,
                        message: error.to_string(),
                    }
                })?;
            row.into_request(context).map_err(|message| {
                BuildingRegisterFloorProposalInputError::InvalidRow { line, message }
            })
        })
        .collect()
}

impl BuildingRegisterFloorContextPack {
    fn into_request(
        self,
        context: &BuildingRegisterFloorProposalInputContext,
    ) -> Result<NormalizationRequest, String> {
        validate_equal("schema_version", &self.schema_version, INPUT_SCHEMA_VERSION)?;
        validate_non_empty("context_pack_id", &self.context_pack_id)?;
        validate_non_empty("source_system", &self.source_system)?;
        validate_equal("target.target_kind", &self.target.target_kind, TARGET_KIND)?;
        validate_non_empty("target.raw_record_id", &self.target.raw_record_id)?;
        validate_non_empty("target.source_snapshot_id", &self.target.source_snapshot_id)?;
        validate_non_empty("target.bronze_object_key", &self.target.bronze_object_key)?;
        validate_non_empty("target.silver_row_id", &self.target.silver_row_id)?;
        let row_checksum_sha256 = validate_sha256(
            "target.row_checksum_sha256",
            &self.target.row_checksum_sha256,
        )?;

        validate_object(
            "building_identity_candidate",
            &self.building_identity_candidate,
        )?;
        validate_object("entity_impact", &self.entity_impact)?;
        validate_object("semantic_contract", &self.semantic_contract)?;
        validate_object("target_raw_floor", &self.target_raw_floor)?;
        validate_object(
            "current_deterministic_normalization",
            &self.current_deterministic_normalization,
        )?;
        validate_non_empty_array(
            "same_building_floor_sequence",
            &self.same_building_floor_sequence,
        )?;
        validate_object("building_title_context", &self.building_title_context)?;
        validate_object("unit_context_summary", &self.unit_context_summary)?;
        validate_object("policy_context", &self.policy_context)?;
        validate_object("allowed_output_contract", &self.allowed_output_contract)?;
        validate_object("trace", &self.trace)?;

        let mgm_bldrgst_pk = required_string(
            "building_identity_candidate.mgm_bldrgst_pk",
            &self.building_identity_candidate,
        )?;
        let source_confidence = required_string(
            "building_identity_candidate.source_confidence",
            &self.building_identity_candidate,
        )?;

        let raw_record_id = self.target.raw_record_id.clone();
        let silver_row_id = self.target.silver_row_id.clone();
        let bronze_object_key = self.target.bronze_object_key.clone();

        Ok(NormalizationRequest {
            tenant_id: context.tenant_id.clone(),
            source_system: self.source_system.clone(),
            raw_record_id: raw_record_id.clone(),
            raw_record: json!({
                "schema_version": self.schema_version,
                "context_pack_id": self.context_pack_id,
                "target": {
                    "target_kind": self.target.target_kind,
                    "raw_record_id": raw_record_id,
                    "silver_row_id": silver_row_id,
                    "bronze_object_key": bronze_object_key,
                    "row_checksum_sha256": self.target.row_checksum_sha256,
                    "source_snapshot_id": self.target.source_snapshot_id,
                    "source_line_number": self.target.source_line_number,
                },
                "building_identity_candidate": self.building_identity_candidate,
                "entity_impact": self.entity_impact,
                "semantic_contract": self.semantic_contract,
                "target_raw_floor": self.target_raw_floor,
                "current_deterministic_normalization": self.current_deterministic_normalization,
                "same_building_floor_sequence": self.same_building_floor_sequence,
                "building_title_context": self.building_title_context,
                "unit_context_summary": self.unit_context_summary,
                "policy_context": self.policy_context,
                "allowed_output_contract": self.allowed_output_contract,
                "trace": self.trace,
            }),
            trace_context: TraceContext {
                trace_id: context.trace_id.clone(),
                tenant_id: context.tenant_id.clone(),
                human_user_id: context.human_user_id.clone(),
                product_id: context.product_id.clone(),
            },
            target_schema: building_register_floor_target_schema(),
            target_schema_version: TARGET_SCHEMA_VERSION.to_string(),
            raw_object_key: Some(self.target.bronze_object_key),
            raw_checksum_sha256: Some(row_checksum_sha256),
            target_kind: TARGET_KIND.to_string(),
            target_identity: json!({
                "mgm_bldrgst_pk": mgm_bldrgst_pk,
                "source_confidence": source_confidence,
                "source_record_id": self.target.raw_record_id,
                "silver_row_id": self.target.silver_row_id,
                "entity_impact": self.entity_impact,
            }),
            dictionaries: Default::default(),
        })
    }
}

fn building_register_floor_target_schema() -> Value {
    json!({
        "type": "object",
        "required": ["floor_kind", "floor_number", "floor_index", "floor_display_ko"],
        "properties": {
            "floor_kind": {
                "type": "string",
                "enum": [
                    "above_ground",
                    "basement",
                    "rooftop",
                    "all_floors",
                    "multi_floor_lower",
                    "multi_floor_upper",
                    "unknown"
                ]
            },
            "floor_number": {"type": ["integer", "null"]},
            "floor_index": {"type": ["integer", "null"]},
            "floor_display_ko": {
                "type": ["string", "null"],
                "description": floor_display_ko_description()
            },
            "proposal_required": {"type": "boolean"}
        },
        "additionalProperties": false
    })
}

fn floor_display_ko_description() -> String {
    format!(
        "Use canonical Korean display such as {}, {}, {}, {}, or {}. Do not emit {}.",
        "\u{C9C0}\u{C0C1} 1\u{CE35}",
        "\u{C9C0}\u{D558} 1\u{CE35}",
        "\u{C625}\u{D0D1} 1\u{CE35}",
        "\u{C625}\u{D0D1}",
        "\u{AC01}\u{CE35}",
        "\u{C9C0}\u{D558} 1 \u{CE35}"
    )
}

fn validate_equal(field: &str, actual: &str, expected: &str) -> Result<(), String> {
    if actual != expected {
        return Err(format!("{field} must be {expected}"));
    }
    Ok(())
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} is required"));
    }
    Ok(())
}

fn validate_object(field: &str, value: &Value) -> Result<(), String> {
    if !value.is_object() {
        return Err(format!("{field} must be an object"));
    }
    Ok(())
}

fn validate_non_empty_array(field: &str, value: &Value) -> Result<(), String> {
    let Some(values) = value.as_array() else {
        return Err(format!("{field} must be an array"));
    };
    if values.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

fn required_string(field: &str, value: &Value) -> Result<String, String> {
    let field_name = field.rsplit('.').next().unwrap_or(field);
    let Some(value) = value.get(field_name).and_then(Value::as_str) else {
        return Err(format!("{field} is required"));
    };
    validate_non_empty(field, value)?;
    Ok(value.to_string())
}

fn validate_sha256(field: &str, checksum: &str) -> Result<String, String> {
    if checksum.len() != 64 || !checksum.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(format!("{field} must be a 64-character hex SHA-256"));
    }
    Ok(checksum.to_ascii_lowercase())
}
