// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use intelligence_normalization_domain::{
    building_register_floor_requests_from_jsonl, BuildingRegisterFloorProposalInputContext,
};
use serde_json::json;

#[test]
fn maps_foundation_floor_context_pack_jsonl_to_normalization_request() {
    let jsonl = format!("{}\n", sample_foundation_context_pack());
    let requests = building_register_floor_requests_from_jsonl(
        &jsonl,
        &BuildingRegisterFloorProposalInputContext {
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
    )
    .unwrap();

    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(
        request.source_system,
        "foundation-platform.silver.building_register_floors"
    );
    assert_eq!(request.raw_record_id, "line-43");
    assert_eq!(
        request.raw_object_key,
        Some("bronze/source=datagokr__building_register_floor/page-000001.json".to_string())
    );
    assert_eq!(
        request.raw_checksum_sha256,
        Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())
    );
    assert_eq!(request.target_kind, "building_register_floor");
    assert_eq!(
        request.target_identity,
        json!({
            "mgm_bldrgst_pk": "11680-raw-1",
            "source_confidence": "provider_primary_key",
            "source_record_id": "line-43",
            "silver_row_id": "building-register-floor:line-43",
            "entity_impact": {
                "entity_type": "building",
                "entity_key": "11680-raw-1",
                "consistency_domains": ["floor"]
            }
        })
    );
    assert_eq!(
        request.target_schema_version,
        "building_register_floor.normalized.v1"
    );
    assert_eq!(
        request.target_schema["required"],
        json!([
            "floor_kind",
            "floor_number",
            "floor_index",
            "floor_display_ko"
        ])
    );
    assert_eq!(
        request.target_schema["properties"]["floor_kind"]["enum"],
        json!([
            "above_ground",
            "basement",
            "rooftop",
            "all_floors",
            "multi_floor_lower",
            "multi_floor_upper",
            "unknown"
        ])
    );
    let display_description = request.target_schema["properties"]["floor_display_ko"]
        ["description"]
        .as_str()
        .unwrap();
    assert!(display_description.contains("\u{C9C0}\u{C0C1} 1\u{CE35}"));
    assert!(display_description.contains("\u{C9C0}\u{D558} 1\u{CE35}"));
    assert!(display_description.contains("\u{C625}\u{D0D1} 1\u{CE35}"));
    assert!(
        !display_description.contains('?'),
        "floor_display_ko description must not contain mojibake/question-mark replacement artifacts: {display_description}"
    );
    assert_eq!(
        request.raw_record["target_raw_floor"]["floor_label_raw"],
        "\u{C9C0}1\u{CE35}"
    );
    assert_eq!(
        request.raw_record["current_deterministic_normalization"]["status"],
        "proposal_required"
    );
    assert_eq!(
        request.raw_record["same_building_floor_sequence"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        request.raw_record["same_building_floor_sequence"][0]["floor_display_ko"],
        "\u{C9C0}\u{C0C1} 1\u{CE35}"
    );
    assert_eq!(
        request.raw_record["allowed_output_contract"]["required_locale"],
        "ko-KR"
    );
    assert_eq!(request.trace_context.trace_id, "trace-floor-1");
}

#[test]
fn rejects_non_floor_proposal_input_rows() {
    let mut row = sample_foundation_context_pack();
    row["target"]["target_kind"] = json!("industrial_complex");
    let jsonl = format!("{row}\n");

    let error = building_register_floor_requests_from_jsonl(
        &jsonl,
        &BuildingRegisterFloorProposalInputContext {
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("target_kind must be building_register_floor"),
        "{error}"
    );
}

#[test]
fn accepts_utf8_bom_on_first_jsonl_line() {
    let jsonl = format!("\u{feff}{}\n", sample_foundation_context_pack());

    let requests = building_register_floor_requests_from_jsonl(
        &jsonl,
        &BuildingRegisterFloorProposalInputContext {
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
    )
    .unwrap();

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].raw_record_id, "line-43");
}

fn sample_foundation_context_pack() -> serde_json::Value {
    json!({
        "schema_version": "foundation-platform.floor_entity_context_pack.v1",
        "context_pack_id": "floor-context-pack:pack-1",
        "source_system": "foundation-platform.silver.building_register_floors",
        "target": {
            "target_kind": "building_register_floor",
            "raw_record_id": "line-43",
            "silver_row_id": "building-register-floor:line-43",
            "bronze_object_key": "bronze/source=datagokr__building_register_floor/page-000001.json",
            "row_checksum_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "source_snapshot_id": "snapshot-1",
            "source_line_number": 43
        },
        "building_identity_candidate": {
            "mgm_bldrgst_pk": "11680-raw-1",
            "source_confidence": "provider_primary_key"
        },
        "entity_impact": {
            "entity_type": "building",
            "entity_key": "11680-raw-1",
            "consistency_domains": ["floor"]
        },
        "semantic_contract": {
            "source_slug": "datagokr__building_register_floor",
            "field_mappings": [
                {
                    "field_path": "floor_label_raw",
                    "concept_id": "building.floor.raw_label",
                    "required_for_entity_context": true
                }
            ],
            "entity_impacts": [
                {
                    "entity_type": "building",
                    "entity_key_fields": ["mgm_bldrgst_pk"],
                    "consistency_domains": ["floor"]
                }
            ]
        },
        "target_raw_floor": {
            "floor_type_code_raw": "10",
            "floor_type_name_raw": "\u{C9C0}\u{D558}",
            "floor_number_raw": "1",
            "floor_label_raw": "\u{C9C0}1\u{CE35}"
        },
        "current_deterministic_normalization": {
            "floor_kind": "unknown",
            "floor_number": null,
            "floor_index": null,
            "floor_display_ko": null,
            "status": "proposal_required",
            "reason": "label_kind_mismatch"
        },
        "same_building_floor_sequence": [
            {
                "mgm_bldrgst_pk": "11680-raw-1",
                "source_record_id": "line-42",
                "silver_row_id": "building-register-floor:line-42",
                "floor_type_code_raw": "20",
                "floor_type_name_raw": "\u{C9C0}\u{C0C1}",
                "floor_number_raw": "1",
                "floor_label_raw": "\u{C9C0}\u{C0C1}1\u{CE35}",
                "floor_kind": "above_ground",
                "floor_number": 1,
                "floor_index": 1,
                "floor_display_ko": "\u{C9C0}\u{C0C1} 1\u{CE35}",
                "normalization_status": "normalized",
                "normalization_reason": "deterministic_rule",
                "source_line_number": 42
            },
            {
                "mgm_bldrgst_pk": "11680-raw-1",
                "source_record_id": "line-43",
                "silver_row_id": "building-register-floor:line-43",
                "floor_type_code_raw": "10",
                "floor_type_name_raw": "\u{C9C0}\u{D558}",
                "floor_number_raw": "1",
                "floor_label_raw": "\u{C9C0}1\u{CE35}",
                "floor_kind": "unknown",
                "floor_number": null,
                "floor_index": null,
                "floor_display_ko": null,
                "normalization_status": "proposal_required",
                "normalization_reason": "label_kind_mismatch",
                "source_line_number": 43
            }
        ],
        "building_title_context": {
            "status": "not_available_in_current_handoff"
        },
        "unit_context_summary": {
            "status": "not_available_in_current_handoff"
        },
        "policy_context": {
            "policy_id": "foundation-platform.floor-normalization",
            "policy_version": "v1",
            "default_locale": "ko-KR",
            "machine_values_language": "en-US"
        },
        "allowed_output_contract": {
            "required_locale": "ko-KR",
            "machine_fields": [
                "floor_kind",
                "floor_number",
                "floor_index",
                "normalization_status",
                "normalization_reason"
            ],
            "localized_fields": [
                "floor_display_ko",
                "review_message_ko"
            ]
        },
        "trace": {
            "valid_from_utc": "2026-07-01T00:00:00Z",
            "ingested_at_utc": "2026-07-01T01:00:00Z"
        }
    })
}
