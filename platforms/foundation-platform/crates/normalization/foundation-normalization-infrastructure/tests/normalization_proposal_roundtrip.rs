//! `PostgreSQL` round-trip tests for normalization proposal and review persistence.

#[path = "support/industrial_complex_fixture.rs"]
#[allow(dead_code)]
mod industrial_complex_fixture;
#[allow(dead_code)]
mod support;

use std::sync::Arc;

use foundation_normalization_application::{
    NormalizationProposalReviewCommand, NormalizationUnitOfWork, SubmitNormalizationProposal,
};
use foundation_normalization_domain::{
    NormalizationProposalStatus, NormalizationReviewDecision, NormalizationTargetKind,
};
use foundation_normalization_infrastructure::PgNormalizationUnitOfWork;
use serde_json::json;
use uuid::Uuid;

use industrial_complex_fixture::{create_pending_fixture, sample_complex, valid_command};
use support::{run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn duplicate_submission_returns_existing_proposal() -> TestResult {
    run_in_disposable_database("normalization_proposal_idempotency", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let use_case = SubmitNormalizationProposal::new(Arc::new(PgNormalizationUnitOfWork::new(
            pool.clone(),
        )));
        let command = valid_command(&sample_complex());

        let first = use_case.execute(command.clone()).await?;
        let second = use_case.execute(command).await?;

        assert_eq!(first.id, second.id);
        assert_eq!(second.status, NormalizationProposalStatus::PendingReview);
        assert!(!second.created);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn building_register_floor_submission_persists_in_proposal_inbox() -> TestResult {
    run_in_disposable_database("normalization_floor_submission", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let use_case = SubmitNormalizationProposal::new(Arc::new(PgNormalizationUnitOfWork::new(
            pool.clone(),
        )));
        let mut command = valid_command(&sample_complex());
        command.raw_record_id = format!(
            "datagokr__building_register_floor/11110/10100/page-000001#{}",
            Uuid::new_v4()
        );
        command.target_kind = NormalizationTargetKind::BuildingRegisterFloor;
        command.target_identity = json!({
            "source_system": "foundation-platform-r2-bronze",
            "raw_record_id": command.raw_record_id,
        });
        command.target_schema_version = "building_register_floor.normalized.v1".to_owned();
        command.proposal_schema_version = "building_register_floor.normalized.v1".to_owned();
        command.proposed_record = json!({
            "floor_label": "지하 1층",
            "floor_kind": "basement",
            "floor_number": 1,
            "proposal_required": false
        });
        command.policy_id = "building-register-floor-normalization".to_owned();
        command.policy_version = "v1".to_owned();
        command.prompt_id = Some("building-register-floor-normalize".to_owned());
        command.prompt_version = Some("v1".to_owned());
        let expected_principal_id = command.submitted_by_principal_id.as_uuid();
        let expected_service = command.submitted_by_service.clone();
        let expected_trace_id = command.trace_id.clone();

        let proposal = use_case.execute(command).await?;
        let stored_target_kind: String = sqlx::query_scalar(
            "SELECT target_kind FROM catalog.normalization_proposal WHERE id = $1",
        )
        .bind(proposal.id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(stored_target_kind, "building_register_floor");

        let stored_submission_audit: (Uuid, String, String) = sqlx::query_as(
            "SELECT submitted_by_principal_id, submitted_by_service, trace_id
             FROM catalog.normalization_proposal_submission_audit
             WHERE proposal_id = $1",
        )
        .bind(proposal.id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(stored_submission_audit.0, expected_principal_id);
        assert_eq!(stored_submission_audit.1, expected_service);
        assert_eq!(stored_submission_audit.2, expected_trace_id);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn review_decisions_persist_exact_status_transitions_and_audit_rows() -> TestResult {
    run_in_disposable_database("normalization_review_transitions", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let cases = [
            (
                NormalizationReviewDecision::Approved,
                NormalizationProposalStatus::Approved,
            ),
            (
                NormalizationReviewDecision::Rejected,
                NormalizationProposalStatus::Rejected,
            ),
            (
                NormalizationReviewDecision::NeedsChanges,
                NormalizationProposalStatus::PendingReview,
            ),
        ];

        for (decision, expected_status) in cases {
            let fixture = create_pending_fixture(&pool).await?;
            let reason = format!("review transition for {}", decision.wire_name());
            let reviewed = fixture
                .normalization_uow
                .review_normalization_proposal(NormalizationProposalReviewCommand {
                    id: Uuid::now_v7(),
                    proposal_id: fixture.proposal_id,
                    reviewer_principal_id: fixture.principal_id,
                    decision,
                    reason: reason.clone(),
                })
                .await?;
            let stored: (String, Uuid, String) = sqlx::query_as(
                "SELECT decision, reviewer_principal_id, reason
                 FROM catalog.normalization_proposal_review
                 WHERE proposal_id = $1",
            )
            .bind(fixture.proposal_id)
            .fetch_one(&pool)
            .await?;

            assert_eq!(reviewed.status, expected_status);
            assert_eq!(stored.0, decision.wire_name());
            assert_eq!(stored.1, fixture.principal_id.as_uuid());
            assert_eq!(stored.2, reason);
        }
        Ok(())
    })
    .await
}
