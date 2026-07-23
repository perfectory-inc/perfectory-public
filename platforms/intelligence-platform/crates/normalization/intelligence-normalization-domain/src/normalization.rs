use std::collections::BTreeMap;

use intelligence_contracts::TraceContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::chat_policy::validate_korean_answer;

fn default_schema_version() -> String {
    "v1".to_string()
}

fn default_policy_id() -> String {
    "normalization-proposal-policy".to_string()
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationValidationResult {
    pub accepted: bool,
    pub raw_record_id: String,
    pub confidence: f64,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationRequest {
    pub tenant_id: String,
    pub source_system: String,
    pub raw_record_id: String,
    pub raw_record: Value,
    pub trace_context: TraceContext,
    pub target_schema: Value,
    #[serde(default = "default_schema_version")]
    pub target_schema_version: String,
    #[serde(default)]
    pub raw_object_key: Option<String>,
    #[serde(default)]
    pub raw_checksum_sha256: Option<String>,
    pub target_kind: String,
    pub target_identity: Value,
    #[serde(default)]
    pub dictionaries: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationProposal {
    pub raw_record_id: String,
    pub proposed_record: Value,
    pub confidence: f64,
    pub reasons: Vec<String>,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default = "default_policy_id")]
    pub policy_id: String,
    #[serde(default = "default_schema_version")]
    pub policy_version: String,
    #[serde(default)]
    pub model_profile_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub prompt_id: Option<String>,
    #[serde(default)]
    pub prompt_version: Option<String>,
}
/// Builds the local outbound retry key used by intelligence-platform.
///
/// Foundation Platform computes the authoritative durable proposal key after intake; this key only
/// deduplicates local submission attempts before the Foundation Platform response is received.
pub fn normalization_idempotency_key(request: &NormalizationRequest) -> String {
    let components = [
        request.tenant_id.as_str(),
        request.target_kind.as_str(),
        request.raw_record_id.as_str(),
        request.target_schema_version.as_str(),
    ];
    let mut hasher = Sha256::new();
    for component in components {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    format!("normalization-v1-{:x}", hasher.finalize())
}

pub fn validate_normalization_proposal(
    request: &NormalizationRequest,
    proposal: &NormalizationProposal,
) -> NormalizationValidationResult {
    validate_normalization_proposal_with_minimum_confidence(request, proposal, 0.85)
}

pub fn validate_normalization_proposal_with_minimum_confidence(
    request: &NormalizationRequest,
    proposal: &NormalizationProposal,
    minimum_confidence: f64,
) -> NormalizationValidationResult {
    let mut errors = Vec::new();

    if proposal.raw_record_id != request.raw_record_id {
        errors.push("proposal.raw_record_id does not match request.raw_record_id".to_string());
    }

    if proposal.schema_version != request.target_schema_version {
        errors.push(
            "proposal.schema_version does not match request.target_schema_version".to_string(),
        );
    }

    match request.target_schema.get("required") {
        Some(Value::Array(required_fields)) => {
            if let Some(proposed_record) = proposal.proposed_record.as_object() {
                for field in required_fields {
                    if let Some(field_name) = field.as_str() {
                        if !proposed_record.contains_key(field_name) {
                            errors.push(format!("missing required field: {field_name}"));
                        }
                    }
                }
            } else {
                errors.push("proposal.proposed_record must be an object".to_string());
            }
        }
        Some(_) => {
            errors.push("target_schema.required must be a list".to_string());
        }
        None => {}
    }

    validate_additional_properties(request, &proposal.proposed_record, &mut errors);

    if request.target_kind == "building_register_floor" {
        validate_building_register_floor_proposal(&proposal.proposed_record, &mut errors);
    }
    if request.target_kind == "building_register_unit" {
        validate_building_register_unit_proposal(&proposal.proposed_record, &mut errors);
    }

    if !(0.0..=1.0).contains(&proposal.confidence) {
        errors.push("confidence must be between 0 and 1".to_string());
    } else if proposal.confidence < minimum_confidence {
        errors.push("confidence below minimum threshold".to_string());
    }

    validate_required_locale(request, proposal, &mut errors);

    NormalizationValidationResult {
        accepted: errors.is_empty(),
        raw_record_id: request.raw_record_id.clone(),
        confidence: proposal.confidence,
        errors,
    }
}

fn validate_additional_properties(
    request: &NormalizationRequest,
    proposed_record: &Value,
    errors: &mut Vec<String>,
) {
    if request
        .target_schema
        .get("additionalProperties")
        .and_then(Value::as_bool)
        != Some(false)
    {
        return;
    }

    let Some(proposed_record) = proposed_record.as_object() else {
        return;
    };
    let Some(allowed_properties) = request
        .target_schema
        .get("properties")
        .and_then(Value::as_object)
    else {
        return;
    };

    for field_name in proposed_record.keys() {
        if !allowed_properties.contains_key(field_name) {
            errors.push(format!("unexpected proposed_record field: {field_name}"));
        }
    }
}

fn validate_building_register_floor_proposal(proposed_record: &Value, errors: &mut Vec<String>) {
    let Some(record) = proposed_record.as_object() else {
        return;
    };

    let floor_kind = record.get("floor_kind").and_then(Value::as_str);
    match floor_kind {
        Some(
            "above_ground" | "basement" | "rooftop" | "all_floors" | "multi_floor_lower"
            | "multi_floor_upper" | "unknown",
        ) => {}
        _ => errors.push("building_register_floor.floor_kind is not canonical".to_string()),
    }
    validate_building_register_floor_index(record, floor_kind, errors);

    let Some(display) = record.get("floor_display_ko") else {
        return;
    };
    if display.is_null() {
        return;
    }
    let Some(display) = display.as_str() else {
        errors.push("building_register_floor.floor_display_ko is not canonical".to_string());
        return;
    };
    if !is_canonical_floor_display_ko(display) {
        errors.push("building_register_floor.floor_display_ko is not canonical".to_string());
    }
}

fn validate_building_register_unit_proposal(proposed_record: &Value, errors: &mut Vec<String>) {
    let Some(record) = proposed_record.as_object() else {
        return;
    };

    match record.get("unit_number") {
        Some(Value::Null) => {}
        Some(Value::Number(number)) if number.as_u64().is_some_and(|value| value > 0) => {}
        Some(_) => errors.push("building_register_unit.unit_number must be positive".to_string()),
        None => {}
    }

    validate_optional_non_empty_unit_string(
        record,
        "building_mgm_bldrgst_pk",
        "building_register_unit.building_mgm_bldrgst_pk must not be empty",
        errors,
    );
    validate_required_non_empty_unit_string(
        record,
        "building_link_method",
        "building_register_unit.building_link_method must not be empty",
        errors,
    );
    validate_required_non_empty_unit_string(
        record,
        "normalization_reason",
        "building_register_unit.normalization_reason must not be empty",
        errors,
    );

    match record.get("normalization_status").and_then(Value::as_str) {
        Some("accepted" | "proposal_required") => {}
        _ => {
            errors.push("building_register_unit.normalization_status is not canonical".to_string())
        }
    }
}

fn validate_required_non_empty_unit_string(
    record: &serde_json::Map<String, Value>,
    field: &str,
    message: &str,
    errors: &mut Vec<String>,
) {
    match record.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => {}
        _ => errors.push(message.to_string()),
    }
}

fn validate_optional_non_empty_unit_string(
    record: &serde_json::Map<String, Value>,
    field: &str,
    message: &str,
    errors: &mut Vec<String>,
) {
    match record.get(field) {
        None | Some(Value::Null) => {}
        Some(Value::String(value)) if !value.trim().is_empty() => {}
        _ => errors.push(message.to_string()),
    }
}

fn validate_building_register_floor_index(
    record: &serde_json::Map<String, Value>,
    floor_kind: Option<&str>,
    errors: &mut Vec<String>,
) {
    let floor_index = record.get("floor_index").and_then(Value::as_i64);
    let floor_number = record.get("floor_number").and_then(Value::as_i64);

    match (floor_kind, floor_index) {
        (Some("basement"), Some(index)) if index >= 0 => errors.push(
            "building_register_floor.floor_index is inconsistent with floor_kind".to_string(),
        ),
        (Some("above_ground"), Some(index)) if index <= 0 => errors.push(
            "building_register_floor.floor_index is inconsistent with floor_kind".to_string(),
        ),
        _ => {}
    }

    if matches!(floor_kind, Some("basement" | "above_ground")) {
        if let (Some(number), Some(index)) = (floor_number, floor_index) {
            if number > 0 && number != index.abs() {
                errors.push(
                    "building_register_floor.floor_number is inconsistent with floor_index"
                        .to_string(),
                );
            }
        }
    }
}

fn validate_required_locale(
    request: &NormalizationRequest,
    proposal: &NormalizationProposal,
    errors: &mut Vec<String>,
) {
    let required_locale = request
        .raw_record
        .pointer("/allowed_output_contract/required_locale")
        .and_then(Value::as_str);
    if required_locale != Some("ko-KR") {
        return;
    }

    let reasons = proposal.reasons.join("\n");
    if !validate_korean_answer(&reasons).passed {
        errors.push("proposal.reasons must be ko-KR".to_string());
    }
}

fn is_canonical_floor_display_ko(display: &str) -> bool {
    let trimmed = display.trim();
    if trimmed.is_empty() || trimmed != display {
        return false;
    }
    if contains_suspicious_display_artifact(trimmed) || !contains_hangul_syllable(trimmed) {
        return false;
    }
    if trimmed.contains(" \u{CE35}") {
        return false;
    }

    if matches!(trimmed, "\u{AC01}\u{CE35}" | "\u{C625}\u{D0D1}") {
        return true;
    }
    if trimmed.starts_with("\u{BCF5}\u{C218}\u{CE35}") {
        return true;
    }

    let numbered_prefix = trimmed.starts_with("\u{C9C0}\u{C0C1} ")
        || trimmed.starts_with("\u{C9C0}\u{D558} ")
        || trimmed.starts_with("\u{C625}\u{D0D1} ");
    numbered_prefix
        && trimmed.ends_with('\u{CE35}')
        && trimmed.chars().any(|ch| ch.is_ascii_digit())
}

fn contains_suspicious_display_artifact(value: &str) -> bool {
    value.contains('?')
        || value.contains('\u{FFFD}')
        || value.contains("u{")
        || value.contains("\\u")
        || value.contains("U+")
        || value.contains("&#")
        || value.contains("%u")
}

fn contains_hangul_syllable(value: &str) -> bool {
    value
        .chars()
        .any(|ch| ('\u{AC00}'..='\u{D7A3}').contains(&ch))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn request() -> NormalizationRequest {
        NormalizationRequest {
            tenant_id: "tenant-1".to_string(),
            source_system: "foundation-platform-r2".to_string(),
            raw_record_id: "raw-1".to_string(),
            raw_record: json!({"name": "Acme"}),
            trace_context: TraceContext {
                trace_id: "trace-1".to_string(),
                tenant_id: "tenant-1".to_string(),
                human_user_id: "user-1".to_string(),
                product_id: "foundation-platform".to_string(),
            },
            target_schema: json!({"required": ["normalized_name"]}),
            target_schema_version: "v1".to_string(),
            raw_object_key: None,
            raw_checksum_sha256: None,
            target_kind: "industrial_complex".to_string(),
            target_identity: json!({"industrial_complex_id": "complex-1"}),
            dictionaries: BTreeMap::new(),
        }
    }

    fn proposal() -> NormalizationProposal {
        NormalizationProposal {
            raw_record_id: "raw-1".to_string(),
            proposed_record: json!({"normalized_name": "Acme"}),
            confidence: 0.91,
            reasons: vec!["field matched source name".to_string()],
            schema_version: "v1".to_string(),
            policy_id: default_policy_id(),
            policy_version: "v1".to_string(),
            model_profile_id: None,
            model_id: None,
            prompt_id: None,
            prompt_version: None,
        }
    }

    fn building_register_floor_request() -> NormalizationRequest {
        NormalizationRequest {
            tenant_id: "tenant-1".to_string(),
            source_system: "foundation-platform.silver.building_register_floors".to_string(),
            raw_record_id: "floor-raw-1".to_string(),
            raw_record: json!({"raw_floor": {"floor_label_raw": "지1층"}}),
            trace_context: TraceContext {
                trace_id: "trace-1".to_string(),
                tenant_id: "tenant-1".to_string(),
                human_user_id: "service:intelligence-platform".to_string(),
                product_id: "foundation-platform".to_string(),
            },
            target_schema: json!({"required": ["floor_kind", "floor_number", "floor_index", "floor_display_ko"]}),
            target_schema_version: "building_register_floor.normalized.v1".to_string(),
            raw_object_key: Some(
                "bronze/source=datagokr__building_register_floor_overview/page-000001.json"
                    .to_string(),
            ),
            raw_checksum_sha256: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            target_kind: "building_register_floor".to_string(),
            target_identity: json!({"mgm_bldrgst_pk": "11680-floor-raw-1"}),
            dictionaries: BTreeMap::new(),
        }
    }

    fn building_register_floor_proposal() -> NormalizationProposal {
        NormalizationProposal {
            raw_record_id: "floor-raw-1".to_string(),
            proposed_record: json!({
                "floor_kind": "basement",
                "floor_number": 1,
                "floor_index": -1,
                "floor_display_ko": "지하 1층",
                "proposal_required": false
            }),
            confidence: 0.95,
            reasons: vec!["floor label normalized from source fields".to_string()],
            schema_version: "building_register_floor.normalized.v1".to_string(),
            policy_id: default_policy_id(),
            policy_version: "v1".to_string(),
            model_profile_id: None,
            model_id: None,
            prompt_id: None,
            prompt_version: None,
        }
    }

    fn building_register_unit_request() -> NormalizationRequest {
        NormalizationRequest {
            tenant_id: "tenant-1".to_string(),
            source_system: "foundation-platform.silver.building_register_units".to_string(),
            raw_record_id: "unit-row-1".to_string(),
            raw_record: json!({
                "allowed_output_contract": {
                    "required_locale": "ko-KR"
                }
            }),
            trace_context: TraceContext {
                trace_id: "trace-1".to_string(),
                tenant_id: "tenant-1".to_string(),
                human_user_id: "service:intelligence-platform".to_string(),
                product_id: "foundation-platform".to_string(),
            },
            target_schema: json!({
                "required": [
                    "unit_number",
                    "building_mgm_bldrgst_pk",
                    "building_link_method",
                    "normalization_status",
                    "normalization_reason"
                ],
                "properties": {
                    "unit_number": {},
                    "building_mgm_bldrgst_pk": {},
                    "building_link_method": {},
                    "normalization_status": {},
                    "normalization_reason": {},
                    "review_message_ko": {}
                },
                "additionalProperties": false
            }),
            target_schema_version: "building_register_unit.normalized.v1".to_string(),
            raw_object_key: Some(
                "bronze/source=hubgokr__building_register_exclusive_unit/OPN.zip".to_string(),
            ),
            raw_checksum_sha256: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            target_kind: "building_register_unit".to_string(),
            target_identity: json!({"silver_row_id": "unit-row-1"}),
            dictionaries: BTreeMap::new(),
        }
    }

    fn building_register_unit_proposal() -> NormalizationProposal {
        NormalizationProposal {
            raw_record_id: "unit-row-1".to_string(),
            proposed_record: json!({
                "unit_number": 301,
                "building_mgm_bldrgst_pk": "building-pk-1",
                "building_link_method": "canonical_dong",
                "normalization_status": "accepted",
                "normalization_reason": "numeric_unit_name_with_context"
            }),
            confidence: 0.95,
            reasons: vec!["\u{AC19}\u{C740} \u{BC94}\u{C704}\u{C758} \u{D638}\u{C2E4} \u{C21C}\u{BC88}\u{ACFC} \u{B3D9}/\u{CE35} \u{B9E5}\u{B77D}\u{C744} \u{ADFC}\u{AC70}\u{B85C} \u{D310}\u{B2E8}\u{D588}\u{C2B5}\u{B2C8}\u{B2E4}.".to_string()],
            schema_version: "building_register_unit.normalized.v1".to_string(),
            policy_id: default_policy_id(),
            policy_version: "v1".to_string(),
            model_profile_id: None,
            model_id: None,
            prompt_id: None,
            prompt_version: None,
        }
    }

    #[test]
    fn accepts_valid_proposal() {
        let result = validate_normalization_proposal(&request(), &proposal());

        assert!(result.accepted);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn rejects_schema_version_mismatch() {
        let mut proposal = proposal();
        proposal.schema_version = "v2".to_string();

        let result = validate_normalization_proposal(&request(), &proposal);

        assert!(!result.accepted);
        assert!(result.errors.contains(
            &"proposal.schema_version does not match request.target_schema_version".to_string()
        ));
    }

    #[test]
    fn rejects_missing_required_field() {
        let mut proposal = proposal();
        proposal.proposed_record = json!({});

        let result = validate_normalization_proposal(&request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"missing required field: normalized_name".to_string()));
    }

    #[test]
    fn accepts_canonical_building_register_floor_proposal() {
        let result = validate_normalization_proposal(
            &building_register_floor_request(),
            &building_register_floor_proposal(),
        );

        assert!(result.accepted, "{:?}", result.errors);
    }

    #[test]
    fn rejects_non_canonical_building_register_floor_kind() {
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["floor_kind"] = json!("underground");

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_floor.floor_kind is not canonical".to_string()));
    }

    #[test]
    fn rejects_non_canonical_building_register_floor_display_spacing() {
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["floor_display_ko"] = json!("지하 1 층");

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_floor.floor_display_ko is not canonical".to_string()));
    }

    #[test]
    fn rejects_literal_unicode_escape_building_register_floor_display() {
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["floor_display_ko"] = json!("u{C9C0}u{D558} 1u{CE35}");

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_floor.floor_display_ko is not canonical".to_string()));
    }

    #[test]
    fn rejects_ascii_only_building_register_floor_display() {
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["floor_display_ko"] = json!("B1 floor");

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_floor.floor_display_ko is not canonical".to_string()));
    }

    #[test]
    fn rejects_question_mark_building_register_floor_display() {
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["floor_display_ko"] = json!("??? 1\u{CE35}");

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_floor.floor_display_ko is not canonical".to_string()));
    }

    #[test]
    fn rejects_extra_building_register_floor_field_when_schema_is_closed() {
        let mut request = building_register_floor_request();
        request.target_schema = json!({
            "required": ["floor_kind", "floor_number", "floor_index", "floor_display_ko"],
            "properties": {
                "floor_kind": {},
                "floor_number": {},
                "floor_index": {},
                "floor_display_ko": {},
                "proposal_required": {}
            },
            "additionalProperties": false
        });
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["totally_bogus_field"] = json!(true);

        let result = validate_normalization_proposal(&request, &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"unexpected proposed_record field: totally_bogus_field".to_string()));
    }

    #[test]
    fn accepts_canonical_building_register_unit_proposal() {
        let result = validate_normalization_proposal(
            &building_register_unit_request(),
            &building_register_unit_proposal(),
        );

        assert!(result.accepted, "{:?}", result.errors);
    }

    #[test]
    fn rejects_non_canonical_building_register_unit_status() {
        let mut proposal = building_register_unit_proposal();
        proposal.proposed_record["normalization_status"] = json!("normalized");

        let result = validate_normalization_proposal(&building_register_unit_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_unit.normalization_status is not canonical".to_string()));
    }

    #[test]
    fn rejects_non_positive_building_register_unit_number() {
        let mut proposal = building_register_unit_proposal();
        proposal.proposed_record["unit_number"] = json!(0);

        let result = validate_normalization_proposal(&building_register_unit_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"building_register_unit.unit_number must be positive".to_string()));
    }

    #[test]
    fn rejects_blank_building_register_unit_link_method() {
        let mut proposal = building_register_unit_proposal();
        proposal.proposed_record["building_link_method"] = json!("  ");

        let result = validate_normalization_proposal(&building_register_unit_request(), &proposal);

        assert!(!result.accepted);
        assert!(result.errors.contains(
            &"building_register_unit.building_link_method must not be empty".to_string()
        ));
    }

    #[test]
    fn rejects_confidence_above_one() {
        let mut proposal = building_register_floor_proposal();
        proposal.confidence = 42.0;

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"confidence must be between 0 and 1".to_string()));
    }

    #[test]
    fn rejects_basement_floor_with_positive_floor_index() {
        let mut proposal = building_register_floor_proposal();
        proposal.proposed_record["floor_kind"] = json!("basement");
        proposal.proposed_record["floor_number"] = json!(1);
        proposal.proposed_record["floor_index"] = json!(999);

        let result = validate_normalization_proposal(&building_register_floor_request(), &proposal);

        assert!(!result.accepted);
        assert!(result.errors.contains(
            &"building_register_floor.floor_index is inconsistent with floor_kind".to_string()
        ));
    }

    #[test]
    fn rejects_english_reasons_when_request_requires_korean() {
        let mut request = building_register_floor_request();
        request.raw_record["allowed_output_contract"] = json!({"required_locale": "ko-KR"});
        let mut proposal = building_register_floor_proposal();
        proposal.reasons = vec!["floor label normalized from source fields".to_string()];

        let result = validate_normalization_proposal(&request, &proposal);

        assert!(!result.accepted);
        assert!(result
            .errors
            .contains(&"proposal.reasons must be ko-KR".to_string()));
    }

    #[test]
    fn builds_stable_idempotency_key() {
        let key = normalization_idempotency_key(&request());
        assert!(key.starts_with("normalization-v1-"));
        assert_eq!(key.len(), "normalization-v1-".len() + 64);
        assert_eq!(key, normalization_idempotency_key(&request()));
    }

    #[test]
    fn idempotency_key_does_not_collide_when_components_contain_separator() {
        let mut left = request();
        left.tenant_id = "tenant:industrial".to_owned();
        left.target_kind = "complex".to_owned();

        let mut right = request();
        right.tenant_id = "tenant".to_owned();
        right.target_kind = "industrial:complex".to_owned();

        assert_ne!(
            normalization_idempotency_key(&left),
            normalization_idempotency_key(&right)
        );
    }
}
