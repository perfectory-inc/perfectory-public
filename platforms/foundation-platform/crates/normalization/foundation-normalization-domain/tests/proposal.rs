//! Compatibility tests for Normalization-owned proposal governance types.

use foundation_normalization_domain::{
    compute_normalization_proposal_content_hash, compute_normalization_proposal_key,
    validate_building_register_unit_proposal, validate_normalization_json_object,
    NormalizationError, NormalizationProposalKeyInput, NormalizationProposalStatus,
    NormalizationReviewDecision, NormalizationTargetKind,
};
use serde_json::json;

#[test]
fn target_kind_wire_names_remain_stable() {
    let values = [
        (
            NormalizationTargetKind::IndustrialComplex,
            "industrial_complex",
        ),
        (
            NormalizationTargetKind::BuildingRegisterFloor,
            "building_register_floor",
        ),
        (
            NormalizationTargetKind::BuildingRegisterUnit,
            "building_register_unit",
        ),
    ];

    for (value, expected) in values {
        assert_eq!(value.wire_name(), expected);
        assert_eq!(
            serde_json::to_string(&value).ok(),
            Some(format!("\"{expected}\""))
        );
    }
}

#[test]
fn proposal_status_wire_names_remain_stable() {
    let values = [
        (NormalizationProposalStatus::PendingReview, "pending_review"),
        (NormalizationProposalStatus::Approved, "approved"),
        (NormalizationProposalStatus::Rejected, "rejected"),
        (NormalizationProposalStatus::Superseded, "superseded"),
        (NormalizationProposalStatus::Applied, "applied"),
        (NormalizationProposalStatus::ApplyFailed, "apply_failed"),
        (NormalizationProposalStatus::RolledBack, "rolled_back"),
    ];

    for (value, expected) in values {
        assert_eq!(value.wire_name(), expected);
        assert_eq!(
            serde_json::to_string(&value).ok(),
            Some(format!("\"{expected}\""))
        );
    }
}

#[test]
fn review_decision_wire_names_remain_stable() {
    let values = [
        (NormalizationReviewDecision::Approved, "approved"),
        (NormalizationReviewDecision::Rejected, "rejected"),
        (NormalizationReviewDecision::NeedsChanges, "needs_changes"),
    ];

    for (value, expected) in values {
        assert_eq!(value.wire_name(), expected);
        assert_eq!(
            serde_json::to_string(&value).ok(),
            Some(format!("\"{expected}\""))
        );
    }
}

#[test]
fn proposal_key_and_content_hash_exact_vectors_remain_stable() -> Result<(), NormalizationError> {
    let input = proposal_key_input();

    assert_eq!(
        compute_normalization_proposal_key(&input)?,
        "normprop:v1:ff44297e863178ee659b03372ead2d16a071ab061587420a9f1cc65a48537cb2"
    );
    assert_eq!(
        compute_normalization_proposal_content_hash(&input.proposed_record)?.0,
        "17aee10c5e3215ee2346742080b8b90458fe54747ba19f5c983b6e3da865a4bc"
    );
    Ok(())
}

#[test]
fn proposal_key_is_stable_for_reordered_json_objects() -> Result<(), NormalizationError> {
    let left = proposal_key_input();
    let mut right = left.clone();
    right.proposed_record = json!({"name":"A","area_m2":10});

    assert_eq!(
        compute_normalization_proposal_key(&left)?,
        compute_normalization_proposal_key(&right)?
    );
    Ok(())
}

#[test]
fn flexible_normalization_fields_must_be_json_objects() {
    let result = validate_normalization_json_object("evidence", &json!(["not", "object"]));

    assert_eq!(
        result,
        Err(NormalizationError::InvalidInput(
            "evidence must be a JSON object".to_owned()
        ))
    );
}

#[test]
fn building_register_unit_proposal_accepts_the_existing_schema_contract(
) -> Result<(), NormalizationError> {
    validate_building_register_unit_proposal(
        "building_register_unit.normalized.v1",
        "building_register_unit.normalized.v1",
        &unit_target_identity(),
        &json!({
            "normalization_status":"proposal_required",
            "unit_number":null
        }),
    )
}

#[test]
fn building_register_unit_proposal_rejects_schema_drift_with_exact_messages() {
    let cases = [
        (
            "wrong",
            "building_register_unit.normalized.v1",
            "target_schema_version must be building_register_unit.normalized.v1",
        ),
        (
            "building_register_unit.normalized.v1",
            "wrong",
            "proposal_schema_version must be building_register_unit.normalized.v1",
        ),
    ];

    for (target_schema, proposal_schema, message) in cases {
        assert_eq!(
            validate_building_register_unit_proposal(
                target_schema,
                proposal_schema,
                &unit_target_identity(),
                &json!({"normalization_status":"accepted"}),
            ),
            Err(NormalizationError::InvalidInput(message.to_owned()))
        );
    }
}

#[test]
fn building_register_unit_proposed_record_preserves_exact_validation_messages() {
    let cases = [
        (
            json!(null),
            "building_register_unit proposed_record must be a JSON object",
        ),
        (
            json!({}),
            "building_register_unit proposed_record.normalization_status is required",
        ),
        (
            json!({"normalization_status":"unknown"}),
            "building_register_unit proposed_record.normalization_status must be accepted or proposal_required",
        ),
        (
            json!({"normalization_status":"accepted","unit_number":-1}),
            "building_register_unit proposed_record.unit_number must be null or unsigned integer",
        ),
    ];

    for (proposed_record, message) in cases {
        assert_eq!(
            validate_building_register_unit_proposal(
                "building_register_unit.normalized.v1",
                "building_register_unit.normalized.v1",
                &unit_target_identity(),
                &proposed_record,
            ),
            Err(NormalizationError::InvalidInput(message.to_owned()))
        );
    }
}

#[test]
fn building_register_unit_target_identity_enforces_the_exact_v1_shape() {
    let cases = [
        (
            json!({"raw_record_id":"unit-1"}),
            "building_register_unit target_identity.source_system is required",
        ),
        (
            json!({"source_system":"source-a"}),
            "building_register_unit target_identity.raw_record_id is required",
        ),
        (
            json!({"source_system":"  ","raw_record_id":"unit-1"}),
            "building_register_unit target_identity.source_system must be a non-empty string",
        ),
        (
            json!({"source_system":"source-a","raw_record_id":""}),
            "building_register_unit target_identity.raw_record_id must be a non-empty string",
        ),
        (
            json!({"source_system":1,"raw_record_id":"unit-1"}),
            "building_register_unit target_identity.source_system must be a non-empty string",
        ),
        (
            json!({"source_system":"source-a","raw_record_id":{"id":"unit-1"}}),
            "building_register_unit target_identity.raw_record_id must be a non-empty string",
        ),
        (
            json!({"source_system":"source-a","raw_record_id":"unit-1","source":"alias"}),
            "building_register_unit target_identity must contain exactly source_system and raw_record_id",
        ),
    ];

    for (target_identity, message) in cases {
        assert_eq!(
            validate_building_register_unit_proposal(
                "building_register_unit.normalized.v1",
                "building_register_unit.normalized.v1",
                &target_identity,
                &json!({"normalization_status":"accepted"}),
            ),
            Err(NormalizationError::InvalidInput(message.to_owned()))
        );
    }
}

#[test]
fn building_register_unit_target_identity_accepts_equivalent_key_order(
) -> Result<(), NormalizationError> {
    for target_identity in [
        json!({"source_system":"source-a","raw_record_id":"unit-1"}),
        json!({"raw_record_id":"unit-1","source_system":"source-a"}),
    ] {
        validate_building_register_unit_proposal(
            "building_register_unit.normalized.v1",
            "building_register_unit.normalized.v1",
            &target_identity,
            &json!({"normalization_status":"accepted"}),
        )?;
    }
    Ok(())
}

fn proposal_key_input() -> NormalizationProposalKeyInput {
    NormalizationProposalKeyInput {
        source_system: "foundation-platform-r2".to_owned(),
        raw_record_id: "raw-1".to_owned(),
        raw_checksum_sha256: Some("a".repeat(64)),
        target_kind: NormalizationTargetKind::IndustrialComplex,
        target_identity: json!({"complex_id":"00000000-0000-0000-0000-000000000001"}),
        target_schema_version: "industrial_complex.normalized.v1".to_owned(),
        proposal_schema_version: "industrial_complex.normalized.v1".to_owned(),
        policy_id: "normalization-policy".to_owned(),
        policy_version: "v1".to_owned(),
        model_profile_id: Some("local-ko".to_owned()),
        prompt_id: Some("normalize-industrial-complex".to_owned()),
        prompt_version: Some("v1".to_owned()),
        proposed_record: json!({"area_m2":10,"name":"A"}),
    }
}

fn unit_target_identity() -> serde_json::Value {
    json!({
        "source_system":"foundation-platform.silver.building_register_units",
        "raw_record_id":"unit-1"
    })
}
