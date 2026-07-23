//! `PostgreSQL` late-failure coverage for normalization review, apply, and rollback.

#[path = "support/atomicity_assertions.rs"]
mod atomicity_assertions;
#[path = "support/failure_triggers.rs"]
mod failure_triggers;
#[path = "support/industrial_complex_fixture.rs"]
mod industrial_complex_fixture;
mod support;

use foundation_normalization_application::{
    NormalizationApplicationCommand, NormalizationProposalReviewCommand,
    NormalizationRollbackCommand, NormalizationUnitOfWork,
};
use foundation_normalization_domain::{NormalizationProposalStatus, NormalizationReviewDecision};
use serde_json::json;
use uuid::Uuid;

use atomicity_assertions::{
    assert_all_transaction_state_unchanged, assert_forced_failure, assert_four_surfaces_unchanged,
};
use failure_triggers::{
    ApplicationInsertFailureTrigger, OutboxFailureTrigger, ProposalUpdateFailureTrigger,
    FORCED_APPLICATION_INSERT_FAILURE, FORCED_OUTBOX_FAILURE, FORCED_PROPOSAL_UPDATE_FAILURE,
};
use industrial_complex_fixture::{
    approve_fixture, create_pending_fixture, load_transaction_state, NormalizationFixture,
};
use support::{database_count_with_prefix, run_in_disposable_database, TestResult};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn panicking_atomicity_body_drops_fixture_and_trigger_database() -> TestResult {
    let suffix = Uuid::new_v4().simple().to_string();
    let label = format!("norm_panic_{}", &suffix[..12]);
    let result = run_in_disposable_database(label.as_str(), |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        ProposalUpdateFailureTrigger::install(&pool, fixture.proposal_id, "approved").await?;
        assert!(
            std::hint::black_box(false),
            "intentional normalization cleanup proof"
        );
        Ok(())
    })
    .await;

    let Err(error) = result else {
        return Err(std::io::Error::other("intentional panic should propagate").into());
    };
    assert!(error
        .to_string()
        .contains("intentional normalization cleanup proof"));
    assert_eq!(database_count_with_prefix(label.as_str()).await?, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn review_update_failure_rolls_back_all_earlier_writes() -> TestResult {
    run_in_disposable_database("normalization_review_update", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let trigger =
            ProposalUpdateFailureTrigger::install(&pool, fixture.proposal_id, "approved").await?;

        let result = fixture
            .normalization_uow
            .review_normalization_proposal(NormalizationProposalReviewCommand {
                id: Uuid::now_v7(),
                proposal_id: fixture.proposal_id,
                reviewer_principal_id: fixture.principal_id,
                decision: NormalizationReviewDecision::Approved,
                reason: "late failure review atomicity".to_owned(),
            })
            .await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_PROPOSAL_UPDATE_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_all_transaction_state_unchanged(&before, &after);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn review_does_not_publish_an_unapproved_outbox_event() -> TestResult {
    run_in_disposable_database("normalization_review_no_event", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        let before = load_transaction_state(&pool, &fixture).await?;

        let reviewed = fixture
            .normalization_uow
            .review_normalization_proposal(NormalizationProposalReviewCommand {
                id: Uuid::now_v7(),
                proposal_id: fixture.proposal_id,
                reviewer_principal_id: fixture.principal_id,
                decision: NormalizationReviewDecision::Approved,
                reason: "review without a public event".to_owned(),
            })
            .await?;

        assert_eq!(reviewed.status, NormalizationProposalStatus::Approved);
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_eq!(after.proposal_row["status"], json!("approved"));
        assert_eq!(json_array_len(&after.review_rows)?, 1);
        assert_eq!(after.application_rows, before.application_rows);
        assert_eq!(after.canonical_row, before.canonical_row);
        assert_eq!(after.outbox_rows, before.outbox_rows);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn apply_outbox_failure_rolls_back_all_earlier_writes() -> TestResult {
    run_in_disposable_database("normalization_apply_outbox", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let trigger = OutboxFailureTrigger::install(
            &pool,
            "complex_id",
            fixture.complex.id.to_string().as_str(),
        )
        .await?;

        let result = apply_fixture(&fixture, Uuid::now_v7()).await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_OUTBOX_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_four_surfaces_unchanged(&before, &after);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn apply_ledger_insert_failure_rolls_back_catalog_and_normalization_state() -> TestResult {
    run_in_disposable_database("normalization_apply_ledger", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let application_id = Uuid::now_v7();
        let trigger = ApplicationInsertFailureTrigger::install(&pool, application_id).await?;

        let result = apply_fixture(&fixture, application_id).await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_APPLICATION_INSERT_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_four_surfaces_unchanged(&before, &after);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn apply_status_update_failure_rolls_back_catalog_outbox_and_ledger() -> TestResult {
    run_in_disposable_database("normalization_apply_status", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let trigger =
            ProposalUpdateFailureTrigger::install(&pool, fixture.proposal_id, "applied").await?;

        let result = apply_fixture(&fixture, Uuid::now_v7()).await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_PROPOSAL_UPDATE_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_four_surfaces_unchanged(&before, &after);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_outbox_failure_rolls_back_all_earlier_writes() -> TestResult {
    run_in_disposable_database("normalization_rollback_outbox", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let application_id = Uuid::now_v7();
        apply_fixture(&fixture, application_id).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let trigger = OutboxFailureTrigger::install(
            &pool,
            "complex_id",
            fixture.complex.id.to_string().as_str(),
        )
        .await?;

        let result = rollback_fixture(&fixture, application_id, Uuid::now_v7()).await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_OUTBOX_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_four_surfaces_unchanged(&before, &after);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_ledger_insert_failure_rolls_back_catalog_and_normalization_state() -> TestResult {
    run_in_disposable_database("normalization_rollback_ledger", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let application_id = Uuid::now_v7();
        apply_fixture(&fixture, application_id).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let rollback_id = Uuid::now_v7();
        let trigger = ApplicationInsertFailureTrigger::install(&pool, rollback_id).await?;

        let result = rollback_fixture(&fixture, application_id, rollback_id).await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_APPLICATION_INSERT_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_four_surfaces_unchanged(&before, &after);
        Ok(())
    })
    .await
}

#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn rollback_status_update_failure_rolls_back_catalog_outbox_and_ledger() -> TestResult {
    run_in_disposable_database("normalization_rollback_status", |pool| async move {
        MIGRATOR.run(&pool).await?;
        let fixture = create_pending_fixture(&pool).await?;
        approve_fixture(&fixture).await?;
        let application_id = Uuid::now_v7();
        apply_fixture(&fixture, application_id).await?;
        let before = load_transaction_state(&pool, &fixture).await?;
        let trigger =
            ProposalUpdateFailureTrigger::install(&pool, fixture.proposal_id, "rolled_back")
                .await?;

        let result = rollback_fixture(&fixture, application_id, Uuid::now_v7()).await;
        trigger.remove(&pool).await?;

        assert_forced_failure(result, FORCED_PROPOSAL_UPDATE_FAILURE)?;
        let after = load_transaction_state(&pool, &fixture).await?;
        assert_four_surfaces_unchanged(&before, &after);
        Ok(())
    })
    .await
}

async fn apply_fixture(
    fixture: &NormalizationFixture,
    application_id: Uuid,
) -> Result<
    foundation_normalization_application::NormalizationApplicationRecord,
    foundation_normalization_domain::NormalizationError,
> {
    fixture
        .normalization_uow
        .apply_normalization_proposal(NormalizationApplicationCommand {
            id: application_id,
            proposal_id: fixture.proposal_id,
            expected_version: 1,
            applied_by_principal_id: fixture.principal_id,
        })
        .await
}

async fn rollback_fixture(
    fixture: &NormalizationFixture,
    application_id: Uuid,
    rollback_id: Uuid,
) -> Result<
    foundation_normalization_application::NormalizationRollbackRecord,
    foundation_normalization_domain::NormalizationError,
> {
    fixture
        .normalization_uow
        .rollback_normalization_application(NormalizationRollbackCommand {
            id: rollback_id,
            application_id,
            expected_current_version: 2,
            reason: "late failure rollback atomicity".to_owned(),
            rolled_back_by_principal_id: fixture.principal_id,
        })
        .await
}

fn json_array_len(value: &serde_json::Value) -> TestResult<usize> {
    value
        .as_array()
        .map(Vec::len)
        .ok_or_else(|| std::io::Error::other("expected JSON array").into())
}
