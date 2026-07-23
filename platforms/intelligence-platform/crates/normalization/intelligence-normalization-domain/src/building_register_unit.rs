use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;

use intelligence_contracts::TraceContext;

use crate::NormalizationRequest;

const INPUT_SCHEMA_VERSION: &str = "foundation-platform.unit_entity_context_pack.v1";
const TARGET_KIND: &str = "building_register_unit";
const TARGET_SCHEMA_VERSION: &str = "building_register_unit.normalized.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildingRegisterUnitProposalInputContext {
    pub tenant_id: String,
    pub trace_id: String,
    pub human_user_id: String,
    pub product_id: String,
}

#[derive(Debug, Error)]
pub enum BuildingRegisterUnitProposalInputError {
    #[error("invalid building-register unit proposal input JSON at line {line}: {message}")]
    InvalidJson { line: usize, message: String },
    #[error("invalid building-register unit proposal input at line {line}: {message}")]
    InvalidRow { line: usize, message: String },
}

#[derive(Debug, Deserialize)]
struct BuildingRegisterUnitContextPack {
    schema_version: String,
    context_pack_id: String,
    source_system: String,
    target: BuildingRegisterUnitContextTarget,
    unit_identity_candidate: Value,
    current_deterministic_normalization: Value,
    same_scope_unit_summary: Value,
    entity_context: Value,
    second_pass_decision: Value,
    policy_context: Value,
    allowed_output_contract: Value,
    trace: Value,
}

#[derive(Debug, Deserialize)]
struct BuildingRegisterUnitContextTarget {
    target_kind: String,
    silver_row_id: String,
    bronze_object_key: String,
    row_checksum_sha256: String,
    source_snapshot_id: String,
    source_line_number: Option<u64>,
}

pub fn building_register_unit_requests_from_jsonl(
    jsonl: &str,
    context: &BuildingRegisterUnitProposalInputContext,
) -> Result<Vec<NormalizationRequest>, BuildingRegisterUnitProposalInputError> {
    jsonl
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line.trim().trim_start_matches('\u{feff}');
            (!line.is_empty()).then_some((index + 1, line))
        })
        .map(|(line, row)| {
            let row: BuildingRegisterUnitContextPack =
                serde_json::from_str(row).map_err(|error| {
                    BuildingRegisterUnitProposalInputError::InvalidJson {
                        line,
                        message: error.to_string(),
                    }
                })?;
            row.into_request(context).map_err(|message| {
                BuildingRegisterUnitProposalInputError::InvalidRow { line, message }
            })
        })
        .collect()
}

impl BuildingRegisterUnitContextPack {
    fn into_request(
        self,
        context: &BuildingRegisterUnitProposalInputContext,
    ) -> Result<NormalizationRequest, String> {
        validate_equal("schema_version", &self.schema_version, INPUT_SCHEMA_VERSION)?;
        validate_non_empty("context_pack_id", &self.context_pack_id)?;
        validate_non_empty("source_system", &self.source_system)?;
        validate_equal("target.target_kind", &self.target.target_kind, TARGET_KIND)?;
        validate_non_empty("target.silver_row_id", &self.target.silver_row_id)?;
        validate_non_empty("target.source_snapshot_id", &self.target.source_snapshot_id)?;
        validate_non_empty("target.bronze_object_key", &self.target.bronze_object_key)?;
        let row_checksum_sha256 = validate_sha256(
            "target.row_checksum_sha256",
            &self.target.row_checksum_sha256,
        )?;

        validate_object("unit_identity_candidate", &self.unit_identity_candidate)?;
        validate_object(
            "current_deterministic_normalization",
            &self.current_deterministic_normalization,
        )?;
        validate_object("same_scope_unit_summary", &self.same_scope_unit_summary)?;
        validate_object("entity_context", &self.entity_context)?;
        validate_object("second_pass_decision", &self.second_pass_decision)?;
        validate_object("policy_context", &self.policy_context)?;
        validate_object("allowed_output_contract", &self.allowed_output_contract)?;
        validate_object("trace", &self.trace)?;

        let mgm_bldrgst_pk = required_string(
            "unit_identity_candidate.mgm_bldrgst_pk",
            &self.unit_identity_candidate,
        )?;
        let pnu = required_string("unit_identity_candidate.pnu", &self.unit_identity_candidate)?;
        let entity_context_key =
            required_string("entity_context.entity_context_key", &self.entity_context)?;
        let source_line_number = self.target.source_line_number.unwrap_or_default();
        let silver_row_id = self.target.silver_row_id.clone();
        let bronze_object_key = self.target.bronze_object_key.clone();

        Ok(NormalizationRequest {
            tenant_id: context.tenant_id.clone(),
            source_system: self.source_system.clone(),
            raw_record_id: silver_row_id.clone(),
            raw_record: json!({
                "schema_version": self.schema_version,
                "context_pack_id": self.context_pack_id,
                "target": {
                    "target_kind": self.target.target_kind,
                    "silver_row_id": silver_row_id,
                    "bronze_object_key": bronze_object_key,
                    "row_checksum_sha256": self.target.row_checksum_sha256,
                    "source_snapshot_id": self.target.source_snapshot_id,
                    "source_line_number": self.target.source_line_number,
                },
                "unit_identity_candidate": self.unit_identity_candidate,
                "current_deterministic_normalization": self.current_deterministic_normalization,
                "same_scope_unit_summary": self.same_scope_unit_summary,
                "entity_context": self.entity_context,
                "second_pass_decision": self.second_pass_decision,
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
            target_schema: building_register_unit_target_schema(),
            target_schema_version: TARGET_SCHEMA_VERSION.to_string(),
            raw_object_key: Some(self.target.bronze_object_key),
            raw_checksum_sha256: Some(row_checksum_sha256.clone()),
            target_kind: TARGET_KIND.to_string(),
            target_identity: json!({
                "silver_row_id": self.target.silver_row_id,
                "mgm_bldrgst_pk": mgm_bldrgst_pk,
                "pnu": pnu,
                "entity_context_key": entity_context_key,
                "row_checksum_sha256": row_checksum_sha256,
                "source_line_number": source_line_number,
            }),
            dictionaries: Default::default(),
        })
    }
}

fn building_register_unit_target_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "unit_number",
            "building_mgm_bldrgst_pk",
            "building_link_method",
            "normalization_status",
            "normalization_reason"
        ],
        "properties": {
            "unit_number": {"type": ["integer", "null"], "minimum": 1},
            "building_mgm_bldrgst_pk": {"type": ["string", "null"]},
            "building_link_method": {"type": "string"},
            "normalization_status": {
                "type": "string",
                "enum": ["accepted", "proposal_required"]
            },
            "normalization_reason": {"type": "string"},
            "review_message_ko": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
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
