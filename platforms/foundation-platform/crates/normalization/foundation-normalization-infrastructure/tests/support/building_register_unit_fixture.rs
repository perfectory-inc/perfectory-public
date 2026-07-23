use std::sync::Arc;
use std::time::Duration;

use foundation_normalization_application::{
    ActiveBuildingRegisterUnitOverrideReader, NormalizationApplicationCommand,
    NormalizationProposalReviewCommand, NormalizationProposalSubmissionCommand,
    NormalizationRollbackCommand, NormalizationUnitOfWork, SubmitNormalizationProposal,
};
use foundation_normalization_domain::{
    NormalizationProposalStatus, NormalizationReviewDecision, NormalizationTargetKind,
};
use foundation_normalization_infrastructure::{
    PgActiveBuildingRegisterUnitOverrideReader, PgNormalizationUnitOfWork,
};
use foundation_shared_kernel::ids::PrincipalId;
use serde_json::{json, Value as JsonValue};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::sync::Barrier;
use uuid::Uuid;

use crate::support::TestResult;

#[derive(Debug)]
pub struct Ledger {
    pub before_snapshot: JsonValue,
    pub after_snapshot: JsonValue,
}

pub fn target_identity(label: &str) -> JsonValue {
    json!({
        "source_system": "foundation-platform.silver.building_register_units",
        "raw_record_id": format!("{label}-{}", Uuid::new_v4().simple())
    })
}

pub async fn submit_approved(
    pool: &PgPool,
    principal: PrincipalId,
    identity: &JsonValue,
    unit_number: u64,
) -> TestResult<Uuid> {
    let raw_record_id = identity["raw_record_id"]
        .as_str()
        .ok_or_else(|| std::io::Error::other("raw_record_id must be a string"))?;
    let use_case =
        SubmitNormalizationProposal::new(Arc::new(PgNormalizationUnitOfWork::new(pool.clone())));
    let proposal = use_case
        .execute(NormalizationProposalSubmissionCommand {
            id: Uuid::now_v7(),
            proposal_key: String::new(),
            submitted_by_service: "intelligence-platform".to_owned(),
            submitted_by_principal_id: PrincipalId::new(Uuid::now_v7()),
            source_system: "foundation-platform.silver.building_register_units".to_owned(),
            raw_record_id: raw_record_id.to_owned(),
            raw_object_key: None,
            raw_checksum_sha256: Some("c".repeat(64)),
            bronze_object_id: None,
            target_kind: NormalizationTargetKind::BuildingRegisterUnit,
            target_identity: identity.clone(),
            target_schema_version: "building_register_unit.normalized.v1".to_owned(),
            proposal_schema_version: "building_register_unit.normalized.v1".to_owned(),
            proposed_record: json!({
                "normalization_status": "accepted",
                "unit_number": unit_number
            }),
            proposed_record_sha256: String::new(),
            proposed_patch: None,
            confidence: 0.95,
            evidence: json!({"source":"unit-transaction-test"}),
            validation: json!({"accepted":true}),
            model_profile_id: Some("normalization-ko".to_owned()),
            model_id: Some("qwen3.6".to_owned()),
            prompt_id: Some("building-register-unit-normalize".to_owned()),
            prompt_version: Some("v1".to_owned()),
            policy_id: "building-register-unit-normalization".to_owned(),
            policy_version: "v1".to_owned(),
            trace_id: format!("trace-unit-{}", Uuid::new_v4()),
            status: NormalizationProposalStatus::PendingReview,
        })
        .await?;
    let uow = PgNormalizationUnitOfWork::new(pool.clone());
    uow.review_normalization_proposal(NormalizationProposalReviewCommand {
        id: Uuid::now_v7(),
        proposal_id: proposal.id,
        reviewer_principal_id: principal,
        decision: NormalizationReviewDecision::Approved,
        reason: "unit transaction approval".to_owned(),
    })
    .await?;
    Ok(proposal.id)
}

pub fn spawn_apply(
    pool: PgPool,
    barrier: Arc<Barrier>,
    proposal_id: Uuid,
    application_id: Uuid,
    principal: PrincipalId,
) -> tokio::task::JoinHandle<Result<(), foundation_normalization_domain::NormalizationError>> {
    tokio::spawn(async move {
        barrier.wait().await;
        apply(&pool, proposal_id, application_id, principal).await
    })
}

pub async fn apply(
    pool: &PgPool,
    proposal_id: Uuid,
    application_id: Uuid,
    principal: PrincipalId,
) -> Result<(), foundation_normalization_domain::NormalizationError> {
    PgNormalizationUnitOfWork::new(pool.clone())
        .apply_normalization_proposal(NormalizationApplicationCommand {
            id: application_id,
            proposal_id,
            expected_version: 1,
            applied_by_principal_id: principal,
        })
        .await?;
    Ok(())
}

pub async fn rollback(
    pool: &PgPool,
    application_id: Uuid,
    rollback_id: Uuid,
    principal: PrincipalId,
) -> TestResult {
    PgNormalizationUnitOfWork::new(pool.clone())
        .rollback_normalization_application(NormalizationRollbackCommand {
            id: rollback_id,
            application_id,
            expected_current_version: 1,
            reason: "unit transaction rollback".to_owned(),
            rolled_back_by_principal_id: principal,
        })
        .await?;
    Ok(())
}

pub async fn acquire_target_lock(
    tx: &mut Transaction<'_, Postgres>,
    identity: &JsonValue,
) -> TestResult {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
            hashtextextended(
                'foundation.normalization.building_register_unit:' || ($1::jsonb)::text,
                0
            )
         )",
    )
    .bind(identity)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn acquire_proposal_lock(
    tx: &mut Transaction<'_, Postgres>,
    proposal_id: Uuid,
) -> TestResult {
    sqlx::query(
        "SELECT id
         FROM catalog.normalization_proposal
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(proposal_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn wait_for_advisory_waiters(pool: &PgPool, expected: i64) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND NOT granted
                   AND database = (
                       SELECT oid FROM pg_database WHERE datname = current_database()
                   )",
            )
            .fetch_one(pool)
            .await?;
            if count >= expected {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| std::io::Error::other("apply transactions did not wait on target lock"))??;
    Ok(())
}

pub async fn wait_for_transaction_lock_waiters(pool: &PgPool, expected: i64) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND wait_event IN ('transactionid', 'tuple')",
            )
            .fetch_one(pool)
            .await?;
            if count >= expected {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| std::io::Error::other("apply transaction did not wait on proposal lock"))??;
    Ok(())
}

pub async fn load_ledger(pool: &PgPool, application_id: Uuid) -> TestResult<Ledger> {
    let row = sqlx::query(
        "SELECT before_snapshot, after_snapshot
         FROM catalog.normalization_application
         WHERE id = $1",
    )
    .bind(application_id)
    .fetch_one(pool)
    .await?;
    Ok(Ledger {
        before_snapshot: row.try_get("before_snapshot")?,
        after_snapshot: row.try_get("after_snapshot")?,
    })
}

pub async fn force_application_order(pool: &PgPool, older: Uuid, newer: Uuid) -> TestResult {
    sqlx::query(
        "UPDATE catalog.normalization_application
         SET applied_at = CASE id
             WHEN $1 THEN TIMESTAMPTZ '2099-01-01 00:00:00+00'
             WHEN $2 THEN TIMESTAMPTZ '2099-01-02 00:00:00+00'
         END
         WHERE id IN ($1, $2)",
    )
    .bind(older)
    .bind(newer)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_same_application_time(pool: &PgPool, first: Uuid, second: Uuid) -> TestResult {
    sqlx::query(
        "UPDATE catalog.normalization_application
         SET applied_at = TIMESTAMPTZ '2100-01-01 00:00:00+00'
         WHERE id IN ($1, $2)",
    )
    .bind(first)
    .bind(second)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_application_predecessor(
    pool: &PgPool,
    application_id: Uuid,
    predecessor_snapshot: &JsonValue,
) -> TestResult {
    sqlx::query(
        "UPDATE catalog.normalization_application
         SET before_snapshot = jsonb_build_object('active_override', $2::jsonb)
         WHERE id = $1",
    )
    .bind(application_id)
    .bind(predecessor_snapshot)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_application_before_snapshot(
    pool: &PgPool,
    application_id: Uuid,
    before_snapshot: &JsonValue,
) -> TestResult {
    sqlx::query(
        "UPDATE catalog.normalization_application
         SET before_snapshot = $2
         WHERE id = $1",
    )
    .bind(application_id)
    .bind(before_snapshot)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_application_after_proposal_id(
    pool: &PgPool,
    application_id: Uuid,
    proposal_id: Uuid,
) -> TestResult {
    sqlx::query(
        "UPDATE catalog.normalization_application
         SET after_snapshot = jsonb_set(
             after_snapshot,
             ARRAY['proposal_id'],
             to_jsonb($2::text)
         )
         WHERE id = $1",
    )
    .bind(application_id)
    .bind(proposal_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn assert_active_application(
    reader: &PgActiveBuildingRegisterUnitOverrideReader,
    expected_id: Uuid,
) -> TestResult {
    let active = reader
        .list_active_building_register_unit_overrides()
        .await?;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].application_id, expected_id);
    Ok(())
}
