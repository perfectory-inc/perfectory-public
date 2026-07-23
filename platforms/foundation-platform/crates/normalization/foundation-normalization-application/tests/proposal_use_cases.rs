//! Tests for Normalization proposal governance use cases.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use foundation_normalization_application::{
    ApplyNormalizationProposal, NormalizationApplicationCommand, NormalizationApplicationRecord,
    NormalizationProposalRecord, NormalizationProposalReviewCommand,
    NormalizationProposalSubmissionCommand, NormalizationRollbackCommand,
    NormalizationRollbackRecord, NormalizationUnitOfWork, ReviewNormalizationProposal,
    RollbackNormalizationApplication, SubmitNormalizationProposal,
};
use foundation_normalization_domain::{
    NormalizationError, NormalizationProposalStatus, NormalizationReviewDecision,
    NormalizationTargetKind,
};
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::json;
use uuid::Uuid;

#[derive(Default)]
struct RecordingUow {
    inserted: Mutex<Vec<NormalizationProposalSubmissionCommand>>,
    reviewed: Mutex<Vec<NormalizationProposalReviewCommand>>,
    applied: Mutex<Vec<NormalizationApplicationCommand>>,
    rolled_back: Mutex<Vec<NormalizationRollbackCommand>>,
}

#[async_trait]
impl NormalizationUnitOfWork for RecordingUow {
    async fn submit_normalization_proposal(
        &self,
        command: NormalizationProposalSubmissionCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        self.inserted
            .lock()
            .map_err(|_| NormalizationError::Persistence("inserted mutex poisoned".to_owned()))?
            .push(command.clone());
        Ok(NormalizationProposalRecord {
            id: Uuid::nil(),
            proposal_key: command.proposal_key,
            status: command.status,
            created: true,
        })
    }

    async fn review_normalization_proposal(
        &self,
        command: NormalizationProposalReviewCommand,
    ) -> Result<NormalizationProposalRecord, NormalizationError> {
        self.reviewed
            .lock()
            .map_err(|_| NormalizationError::Persistence("reviewed mutex poisoned".to_owned()))?
            .push(command.clone());
        let status = match command.decision {
            NormalizationReviewDecision::Approved => NormalizationProposalStatus::Approved,
            NormalizationReviewDecision::Rejected => NormalizationProposalStatus::Rejected,
            NormalizationReviewDecision::NeedsChanges => NormalizationProposalStatus::PendingReview,
        };
        Ok(NormalizationProposalRecord {
            id: command.proposal_id,
            proposal_key: "normprop:v1:reviewed".to_owned(),
            status,
            created: false,
        })
    }

    async fn apply_normalization_proposal(
        &self,
        command: NormalizationApplicationCommand,
    ) -> Result<NormalizationApplicationRecord, NormalizationError> {
        self.applied
            .lock()
            .map_err(|_| NormalizationError::Persistence("applied mutex poisoned".to_owned()))?
            .push(command.clone());
        Ok(NormalizationApplicationRecord {
            id: command.id,
            proposal_id: command.proposal_id,
            target_kind: NormalizationTargetKind::IndustrialComplex,
            target_id: Some(Uuid::now_v7()),
        })
    }

    async fn rollback_normalization_application(
        &self,
        command: NormalizationRollbackCommand,
    ) -> Result<NormalizationRollbackRecord, NormalizationError> {
        self.rolled_back
            .lock()
            .map_err(|_| NormalizationError::Persistence("rolled_back mutex poisoned".to_owned()))?
            .push(command.clone());
        Ok(NormalizationRollbackRecord {
            id: command.id,
            rollback_of: command.application_id,
            target_kind: NormalizationTargetKind::IndustrialComplex,
            target_id: Some(Uuid::now_v7()),
        })
    }
}

#[tokio::test]
async fn submit_computes_exact_key_and_hash_and_persists_pending_review(
) -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = SubmitNormalizationProposal::new(uow.clone());

    let record = use_case.execute(valid_command_without_key()).await?;

    assert_eq!(
        record.proposal_key,
        "normprop:v1:ff44297e863178ee659b03372ead2d16a071ab061587420a9f1cc65a48537cb2"
    );
    assert_eq!(record.status, NormalizationProposalStatus::PendingReview);
    let inserted = inserted_commands(&uow)?;
    assert_eq!(inserted.len(), 1);
    assert_eq!(
        inserted[0].proposed_record_sha256,
        "17aee10c5e3215ee2346742080b8b90458fe54747ba19f5c983b6e3da865a4bc"
    );
    assert_eq!(inserted[0].submitted_by_service, "intelligence-platform");
    Ok(())
}

#[tokio::test]
async fn submit_forces_pending_review_even_if_client_supplies_a_later_status(
) -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = SubmitNormalizationProposal::new(uow.clone());
    let mut command = valid_command_without_key();
    command.status = NormalizationProposalStatus::Approved;

    let record = use_case.execute(command).await?;

    assert_eq!(record.status, NormalizationProposalStatus::PendingReview);
    assert_eq!(
        inserted_commands(&uow)?[0].status,
        NormalizationProposalStatus::PendingReview
    );
    Ok(())
}

#[tokio::test]
async fn submit_rejects_non_object_evidence_before_persistence() -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = SubmitNormalizationProposal::new(uow.clone());
    let mut command = valid_command_without_key();
    command.evidence = json!(["not", "object"]);

    let result = use_case.execute(command).await;

    assert_eq!(
        result,
        Err(NormalizationError::InvalidInput(
            "evidence must be a JSON object".to_owned()
        ))
    );
    assert!(inserted_commands(&uow)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn submit_rejects_invalid_unit_identity_before_persistence() -> Result<(), NormalizationError>
{
    let uow = Arc::new(RecordingUow::default());
    let use_case = SubmitNormalizationProposal::new(uow.clone());
    let mut command = valid_unit_command_without_key();
    command.target_identity["unexpected"] = json!(true);

    assert_eq!(
        use_case.execute(command).await,
        Err(NormalizationError::InvalidInput(
            "building_register_unit target_identity must contain exactly source_system and raw_record_id"
                .to_owned()
        ))
    );
    assert!(inserted_commands(&uow)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn submit_rejects_unit_identity_mismatch_before_persistence() -> Result<(), NormalizationError>
{
    let uow = Arc::new(RecordingUow::default());
    let use_case = SubmitNormalizationProposal::new(uow.clone());
    let cases = [
        (
            "other-source",
            "unit-1",
            "building_register_unit target_identity.source_system must match source_system",
        ),
        (
            "foundation-platform.silver.building_register_units",
            "other-unit",
            "building_register_unit target_identity.raw_record_id must match raw_record_id",
        ),
    ];

    for (identity_source, identity_raw_record_id, message) in cases {
        let mut command = valid_unit_command_without_key();
        command.target_identity = json!({
            "source_system": identity_source,
            "raw_record_id": identity_raw_record_id
        });

        assert_eq!(
            use_case.execute(command).await,
            Err(NormalizationError::InvalidInput(message.to_owned()))
        );
    }
    assert!(inserted_commands(&uow)?.is_empty());
    Ok(())
}

#[test]
fn review_requires_non_empty_reason_and_reviewer() {
    assert_eq!(
        ReviewNormalizationProposal::validate_reason(""),
        Err(NormalizationError::InvalidInput(
            "reason must not be empty".to_owned()
        ))
    );
    assert_eq!(
        ReviewNormalizationProposal::validate_reviewer_principal_id(PrincipalId::new(Uuid::nil())),
        Err(NormalizationError::InvalidInput(
            "reviewer_principal_id must not be nil".to_owned()
        ))
    );
}

#[tokio::test]
async fn review_trims_reason_and_records_decision() -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = ReviewNormalizationProposal::new(uow.clone());
    let proposal_id = Uuid::now_v7();

    let record = use_case
        .execute(NormalizationProposalReviewCommand {
            id: Uuid::now_v7(),
            proposal_id,
            reviewer_principal_id: PrincipalId::new(Uuid::now_v7()),
            decision: NormalizationReviewDecision::Rejected,
            reason: "  raw evidence did not support the proposal  ".to_owned(),
        })
        .await?;

    assert_eq!(record.status, NormalizationProposalStatus::Rejected);
    let reviewed = reviewed_commands(&uow)?;
    assert_eq!(
        reviewed[0].reason,
        "raw evidence did not support the proposal"
    );
    assert_eq!(reviewed[0].decision, NormalizationReviewDecision::Rejected);
    Ok(())
}

#[test]
fn apply_rejects_proposal_that_is_not_approved() {
    assert_eq!(
        ApplyNormalizationProposal::validate_status(NormalizationProposalStatus::PendingReview),
        Err(NormalizationError::InvalidState(
            "proposal must be approved before apply".to_owned()
        ))
    );
}

#[tokio::test]
async fn apply_dispatches_through_normalization_unit_of_work() -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = ApplyNormalizationProposal::new(uow.clone());
    let proposal_id = Uuid::now_v7();
    let applied_by_principal_id = PrincipalId::new(Uuid::now_v7());

    let record = use_case
        .execute(NormalizationApplicationCommand {
            id: Uuid::now_v7(),
            proposal_id,
            expected_version: 7,
            applied_by_principal_id,
        })
        .await?;

    assert_eq!(record.proposal_id, proposal_id);
    let applied = applied_commands(&uow)?;
    assert_eq!(applied[0].expected_version, 7);
    assert_eq!(applied[0].applied_by_principal_id, applied_by_principal_id);
    Ok(())
}

#[tokio::test]
async fn apply_rejects_invalid_version_and_principal_before_persistence(
) -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = ApplyNormalizationProposal::new(uow.clone());
    let mut command = NormalizationApplicationCommand {
        id: Uuid::now_v7(),
        proposal_id: Uuid::now_v7(),
        expected_version: 0,
        applied_by_principal_id: PrincipalId::new(Uuid::now_v7()),
    };

    assert_eq!(
        use_case.execute(command.clone()).await,
        Err(NormalizationError::InvalidInput(
            "expected_version must be positive".to_owned()
        ))
    );
    command.expected_version = 1;
    command.applied_by_principal_id = PrincipalId::new(Uuid::nil());
    assert_eq!(
        use_case.execute(command).await,
        Err(NormalizationError::InvalidInput(
            "applied_by_principal_id must not be nil".to_owned()
        ))
    );
    assert!(applied_commands(&uow)?.is_empty());
    Ok(())
}

#[test]
fn rollback_requires_applied_application_snapshot() {
    assert_eq!(
        RollbackNormalizationApplication::validate_before_snapshot(&serde_json::Value::Null),
        Err(NormalizationError::InvalidInput(
            "before_snapshot must be a JSON object".to_owned()
        ))
    );
}

#[tokio::test]
async fn rollback_trims_reason_and_dispatches_application() -> Result<(), NormalizationError> {
    let uow = Arc::new(RecordingUow::default());
    let use_case = RollbackNormalizationApplication::new(uow.clone());
    let application_id = Uuid::now_v7();

    let record = use_case
        .execute(NormalizationRollbackCommand {
            id: Uuid::now_v7(),
            application_id,
            expected_current_version: 8,
            reason: "  operator rollback after review  ".to_owned(),
            rolled_back_by_principal_id: PrincipalId::new(Uuid::now_v7()),
        })
        .await?;

    assert_eq!(record.rollback_of, application_id);
    let rolled_back = rolled_back_commands(&uow)?;
    assert_eq!(rolled_back[0].reason, "operator rollback after review");
    assert_eq!(rolled_back[0].expected_current_version, 8);
    Ok(())
}

fn valid_command_without_key() -> NormalizationProposalSubmissionCommand {
    NormalizationProposalSubmissionCommand {
        id: Uuid::now_v7(),
        proposal_key: String::new(),
        submitted_by_service: "intelligence-platform".to_owned(),
        submitted_by_principal_id: PrincipalId::new(Uuid::now_v7()),
        source_system: "foundation-platform-r2".to_owned(),
        raw_record_id: "raw-1".to_owned(),
        raw_object_key: Some("bronze/source=x/page-000001.json".to_owned()),
        raw_checksum_sha256: Some("a".repeat(64)),
        bronze_object_id: None,
        target_kind: NormalizationTargetKind::IndustrialComplex,
        target_identity: json!({"complex_id":"00000000-0000-0000-0000-000000000001"}),
        target_schema_version: "industrial_complex.normalized.v1".to_owned(),
        proposal_schema_version: "industrial_complex.normalized.v1".to_owned(),
        proposed_record: json!({"area_m2":10,"name":"A"}),
        proposed_record_sha256: String::new(),
        proposed_patch: None,
        confidence: 0.91,
        evidence: json!({"fields":["name","area_m2"]}),
        validation: json!({"accepted":true}),
        model_profile_id: Some("local-ko".to_owned()),
        model_id: Some("local-model".to_owned()),
        prompt_id: Some("normalize-industrial-complex".to_owned()),
        prompt_version: Some("v1".to_owned()),
        policy_id: "normalization-policy".to_owned(),
        policy_version: "v1".to_owned(),
        trace_id: "trace-1".to_owned(),
        status: NormalizationProposalStatus::PendingReview,
    }
}

fn valid_unit_command_without_key() -> NormalizationProposalSubmissionCommand {
    let mut command = valid_command_without_key();
    "foundation-platform.silver.building_register_units".clone_into(&mut command.source_system);
    "unit-1".clone_into(&mut command.raw_record_id);
    command.target_kind = NormalizationTargetKind::BuildingRegisterUnit;
    command.target_identity = json!({
        "source_system":"foundation-platform.silver.building_register_units",
        "raw_record_id":"unit-1"
    });
    "building_register_unit.normalized.v1".clone_into(&mut command.target_schema_version);
    "building_register_unit.normalized.v1".clone_into(&mut command.proposal_schema_version);
    command.proposed_record = json!({
        "normalization_status":"accepted",
        "unit_number":1
    });
    command
}

fn inserted_commands(
    uow: &RecordingUow,
) -> Result<Vec<NormalizationProposalSubmissionCommand>, NormalizationError> {
    Ok(uow
        .inserted
        .lock()
        .map_err(|_| NormalizationError::Persistence("inserted mutex poisoned".to_owned()))?
        .clone())
}

fn reviewed_commands(
    uow: &RecordingUow,
) -> Result<Vec<NormalizationProposalReviewCommand>, NormalizationError> {
    Ok(uow
        .reviewed
        .lock()
        .map_err(|_| NormalizationError::Persistence("reviewed mutex poisoned".to_owned()))?
        .clone())
}

fn applied_commands(
    uow: &RecordingUow,
) -> Result<Vec<NormalizationApplicationCommand>, NormalizationError> {
    Ok(uow
        .applied
        .lock()
        .map_err(|_| NormalizationError::Persistence("applied mutex poisoned".to_owned()))?
        .clone())
}

fn rolled_back_commands(
    uow: &RecordingUow,
) -> Result<Vec<NormalizationRollbackCommand>, NormalizationError> {
    Ok(uow
        .rolled_back
        .lock()
        .map_err(|_| NormalizationError::Persistence("rolled_back mutex poisoned".to_owned()))?
        .clone())
}
