// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_domain::{
    building_register_unit_requests_from_jsonl, BuildingRegisterUnitProposalInputContext,
};
use serde_json::json;

#[test]
fn maps_launch_v1_unit_context_pack_and_preserves_entity_context() {
    let jsonl = format!("{}\n", sample_foundation_context_pack());
    let requests = building_register_unit_requests_from_jsonl(
        &jsonl,
        &BuildingRegisterUnitProposalInputContext {
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-unit-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
    )
    .unwrap();

    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(
        request.source_system,
        "foundation-platform.silver.building_register_units"
    );
    assert_eq!(request.raw_record_id, "building-register-unit:line-101");
    assert_eq!(
        request.raw_object_key,
        Some("bronze/source=hubgokr__building_register_exclusive_unit/OPN.zip".to_string())
    );
    assert_eq!(
        request.raw_checksum_sha256,
        Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())
    );
    assert_eq!(request.target_kind, "building_register_unit");
    assert_eq!(
        request.target_schema_version,
        "building_register_unit.normalized.v1"
    );
    assert_eq!(
        request.target_schema["required"],
        json!([
            "unit_number",
            "building_mgm_bldrgst_pk",
            "building_link_method",
            "normalization_status",
            "normalization_reason"
        ])
    );
    assert_eq!(request.target_schema["additionalProperties"], false);
    assert_eq!(
        request.target_identity,
        json!({
            "silver_row_id": "building-register-unit:line-101",
            "mgm_bldrgst_pk": "unit-pk-101",
            "pnu": "9999900601100010000",
            "entity_context_key": "9999900601100010000|building-pk-1|101동|3",
            "row_checksum_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "source_line_number": 101
        })
    );
    assert_eq!(
        request.raw_record["second_pass_decision"]["status"],
        "ai_required"
    );
    assert_eq!(
        request.raw_record["entity_context"]["neighbor_unit_examples"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        request.raw_record["allowed_output_contract"]["required_locale"],
        "ko-KR"
    );
    assert_eq!(request.trace_context.trace_id, "trace-unit-1");
}

#[test]
fn rejects_retired_prelaunch_unit_context_pack() {
    let mut row = sample_foundation_context_pack();
    row["schema_version"] = json!("foundation-platform.unit_entity_context_pack.v2");
    let jsonl = format!("{row}\n");

    let error = building_register_unit_requests_from_jsonl(
        &jsonl,
        &BuildingRegisterUnitProposalInputContext {
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-unit-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("schema_version must be foundation-platform.unit_entity_context_pack.v1"),
        "{error}"
    );
}

#[test]
fn accepts_utf8_bom_on_first_unit_jsonl_line() {
    let jsonl = format!("\u{feff}{}\n", sample_foundation_context_pack());

    let requests = building_register_unit_requests_from_jsonl(
        &jsonl,
        &BuildingRegisterUnitProposalInputContext {
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-unit-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
    )
    .unwrap();

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].raw_record_id, "building-register-unit:line-101");
}

fn sample_foundation_context_pack() -> serde_json::Value {
    json!({
        "schema_version": "foundation-platform.unit_entity_context_pack.v1",
        "context_pack_id": "unit-context-pack:pack-1",
        "source_system": "foundation-platform.silver.building_register_units",
        "target": {
            "target_kind": "building_register_unit",
            "silver_row_id": "building-register-unit:line-101",
            "bronze_object_key": "bronze/source=hubgokr__building_register_exclusive_unit/OPN.zip",
            "row_checksum_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "source_snapshot_id": "snapshot-1",
            "source_line_number": 101
        },
        "unit_identity_candidate": {
            "mgm_bldrgst_pk": "unit-pk-101",
            "pnu": "9999900601100010000",
            "dong_join_name": "101동",
            "dong_name_raw": "101동",
            "unit_name_raw": "301호",
            "unit_number": null,
            "floor_kind": "above_ground",
            "floor_index": 3,
            "floor_number": 3,
            "building_mgm_bldrgst_pk": "building-pk-1",
            "building_link_method": "canonical_dong"
        },
        "current_deterministic_normalization": {
            "status": "proposal_required",
            "reason": "numeric_unit_name_with_context",
            "unit_number": null,
            "building_mgm_bldrgst_pk": "building-pk-1",
            "building_link_method": "canonical_dong"
        },
        "same_scope_unit_summary": {
            "scope_key": "9999900601100010000|building-pk-1|101동|3",
            "accepted_unit_count": 2,
            "min_unit_number": 301,
            "max_unit_number": 302,
            "distinct_unit_number_count": 2
        },
        "entity_context": {
            "entity_context_key": "9999900601100010000|building-pk-1|101동|3",
            "same_scope_accepted_unit_count": 2,
            "same_building_accepted_unit_count": 20,
            "neighbor_unit_examples": [
                {"unit_name_raw": "301호", "unit_number": 301},
                {"unit_name_raw": "302호", "unit_number": 302}
            ],
            "conflict_flags": []
        },
        "second_pass_decision": {
            "status": "ai_required",
            "reason": "numeric_unit_name_with_context",
            "ai_required": true
        },
        "policy_context": {
            "policy_id": "foundation-platform.unit-normalization",
            "policy_version": "v1",
            "default_locale": "ko-KR",
            "machine_values_language": "en-US",
            "ai_role": "proposal_only",
            "decision_owner": "foundation-platform",
            "canonical_write_path": "proposal_inbox_human_review_then_command"
        },
        "allowed_output_contract": {
            "required_locale": "ko-KR",
            "machine_fields": [
                "unit_number",
                "building_mgm_bldrgst_pk",
                "building_link_method",
                "normalization_status",
                "normalization_reason"
            ],
            "localized_fields": ["review_message_ko"]
        },
        "trace": {
            "valid_from_utc": "2026-07-01T00:00:00Z",
            "ingested_at_utc": "2026-07-01T01:00:00Z"
        }
    })
}
