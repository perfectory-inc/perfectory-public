use std::sync::Arc;

use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationProposalReviewCommand,
    NormalizationProposalSubmissionCommand, NormalizationUnitOfWork, SubmitNormalizationProposal,
};
use foundation_normalization_domain::{
    NormalizationProposalStatus, NormalizationReviewDecision, NormalizationTargetKind,
};
use foundation_normalization_infrastructure::PgNormalizationUnitOfWork;
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::TestResult;

pub async fn apply_building_register_unit_proposal(
    pool: &PgPool,
    normalization_uow: &PgNormalizationUnitOfWork,
    reviewer: PrincipalId,
) -> TestResult<(Uuid, Uuid)> {
    let use_case =
        SubmitNormalizationProposal::new(Arc::new(PgNormalizationUnitOfWork::new(pool.clone())));
    let proposal = use_case.execute(valid_unit_command()).await?;
    let review_record = normalization_uow
        .review_normalization_proposal(NormalizationProposalReviewCommand {
            id: Uuid::now_v7(),
            proposal_id: proposal.id,
            reviewer_principal_id: reviewer,
            decision: NormalizationReviewDecision::Approved,
            reason: "unit normalization smoke approval".to_owned(),
        })
        .await?;
    assert_eq!(review_record.status, NormalizationProposalStatus::Approved);

    let application_id = Uuid::now_v7();
    let applied = normalization_uow
        .apply_normalization_proposal(NormalizationApplicationCommand {
            id: application_id,
            proposal_id: proposal.id,
            expected_version: 1,
            applied_by_principal_id: reviewer,
        })
        .await?;
    assert_eq!(applied.proposal_id, proposal.id);
    assert_eq!(
        applied.target_kind,
        NormalizationTargetKind::BuildingRegisterUnit
    );
    assert_eq!(applied.target_id, None);
    Ok((proposal.id, application_id))
}

fn valid_unit_command() -> NormalizationProposalSubmissionCommand {
    let raw_record_id = format!(
        "building-register-unit:bronze/source=hubgokr__building_register_exclusive_unit/fixture.zip#line-{}",
        Uuid::new_v4().simple()
    );
    NormalizationProposalSubmissionCommand {
        id: Uuid::now_v7(),
        proposal_key: String::new(),
        submitted_by_service: "intelligence-platform".to_owned(),
        submitted_by_principal_id: PrincipalId::new(Uuid::now_v7()),
        source_system: "foundation-platform.silver.building_register_units".to_owned(),
        raw_record_id: raw_record_id.clone(),
        raw_object_key: Some(
            "bronze/source=hubgokr__building_register_exclusive_unit/fixture.zip".to_owned(),
        ),
        raw_checksum_sha256: Some("c".repeat(64)),
        bronze_object_id: None,
        target_kind: NormalizationTargetKind::BuildingRegisterUnit,
        target_identity: json!({
            "source_system": "foundation-platform.silver.building_register_units",
            "raw_record_id": raw_record_id
        }),
        target_schema_version: "building_register_unit.normalized.v1".to_owned(),
        proposal_schema_version: "building_register_unit.normalized.v1".to_owned(),
        proposed_record: json!({
            "building_link_method": "canonical_dong",
            "building_mgm_bldrgst_pk": "SYNTHETIC-BUILDING-PK-0001",
            "normalization_reason": "no_unit_number",
            "normalization_status": "proposal_required",
            "review_message_ko": "관리자 검토가 필요해요.",
            "unit_number": null
        }),
        proposed_record_sha256: String::new(),
        proposed_patch: None,
        confidence: 0.95,
        evidence: json!({"source":"unit-apply-ledger-test"}),
        validation: json!({"accepted":true}),
        model_profile_id: Some("normalization-ko".to_owned()),
        model_id: Some("qwen3.6".to_owned()),
        prompt_id: Some("building-register-unit-normalize".to_owned()),
        prompt_version: Some("v1".to_owned()),
        policy_id: "building-register-unit-normalization".to_owned(),
        policy_version: "v1".to_owned(),
        trace_id: format!("trace-unit-{}", Uuid::new_v4()),
        status: NormalizationProposalStatus::PendingReview,
    }
}
