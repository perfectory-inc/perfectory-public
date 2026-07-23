//! Proposal submission and locked proposal state persistence.

use foundation_normalization_application::{
    NormalizationProposalRecord, NormalizationProposalSubmissionCommand,
};
use foundation_normalization_domain::{NormalizationError, NormalizationProposalStatus};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::postgres_error::map_sqlx;
use crate::row_mapping::{
    parse_status, parse_target_kind, row_to_proposal_record, ProposalForApply,
};

pub async fn submit(
    pool: &PgPool,
    command: NormalizationProposalSubmissionCommand,
) -> Result<NormalizationProposalRecord, NormalizationError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let row = sqlx::query(
        "INSERT INTO catalog.normalization_proposal
         (id, proposal_key, submitted_by_service, source_system, raw_record_id,
          raw_object_key, raw_checksum_sha256, bronze_object_id, target_kind,
          target_identity, target_schema_version, proposal_schema_version, proposed_record,
          proposed_record_sha256, proposed_patch, confidence, evidence, validation,
          model_profile_id, model_id, prompt_id, prompt_version, policy_id, policy_version,
          trace_id, status)
         VALUES
         ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
          $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
          $25, $26)
         ON CONFLICT (proposal_key) DO NOTHING
         RETURNING id, proposal_key, status",
    )
    .bind(command.id)
    .bind(command.proposal_key.as_str())
    .bind(command.submitted_by_service.as_str())
    .bind(command.source_system.as_str())
    .bind(command.raw_record_id.as_str())
    .bind(command.raw_object_key.as_deref())
    .bind(command.raw_checksum_sha256.as_deref())
    .bind(command.bronze_object_id)
    .bind(command.target_kind.wire_name())
    .bind(&command.target_identity)
    .bind(command.target_schema_version.as_str())
    .bind(command.proposal_schema_version.as_str())
    .bind(&command.proposed_record)
    .bind(command.proposed_record_sha256.as_str())
    .bind(command.proposed_patch.as_ref())
    .bind(command.confidence)
    .bind(&command.evidence)
    .bind(&command.validation)
    .bind(command.model_profile_id.as_deref())
    .bind(command.model_id.as_deref())
    .bind(command.prompt_id.as_deref())
    .bind(command.prompt_version.as_deref())
    .bind(command.policy_id.as_str())
    .bind(command.policy_version.as_str())
    .bind(command.trace_id.as_str())
    .bind(command.status.wire_name())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx)?;

    if let Some(row) = row {
        sqlx::query(
            "INSERT INTO catalog.normalization_proposal_submission_audit
             (proposal_id, submitted_by_principal_id, submitted_by_service, trace_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (proposal_id) DO NOTHING",
        )
        .bind(command.id)
        .bind(command.submitted_by_principal_id.as_uuid())
        .bind(command.submitted_by_service.as_str())
        .bind(command.trace_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let record = row_to_proposal_record(&row, true)?;
        tx.commit().await.map_err(map_sqlx)?;
        return Ok(record);
    }

    let existing = sqlx::query(
        "SELECT id, proposal_key, status
         FROM catalog.normalization_proposal
         WHERE proposal_key = $1",
    )
    .bind(command.proposal_key.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(map_sqlx)?;
    let record = row_to_proposal_record(&existing, false)?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(record)
}

pub async fn lock_for_apply(
    tx: &mut Transaction<'_, Postgres>,
    proposal_id: Uuid,
) -> Result<ProposalForApply, NormalizationError> {
    let row = sqlx::query(
        "SELECT status, target_kind, target_identity, target_schema_version,
                proposal_schema_version, proposed_record
         FROM catalog.normalization_proposal
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(proposal_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or(NormalizationError::ProposalNotFound)?;

    let target_kind: String = row.try_get("target_kind").map_err(map_sqlx)?;
    let status: String = row.try_get("status").map_err(map_sqlx)?;
    Ok(ProposalForApply {
        status: parse_status(status.as_str())?,
        target_kind: parse_target_kind(target_kind.as_str())?,
        target_identity: row.try_get("target_identity").map_err(map_sqlx)?,
        target_schema_version: row.try_get("target_schema_version").map_err(map_sqlx)?,
        proposal_schema_version: row.try_get("proposal_schema_version").map_err(map_sqlx)?,
        proposed_record: row.try_get("proposed_record").map_err(map_sqlx)?,
    })
}

pub async fn mark_applied(
    tx: &mut Transaction<'_, Postgres>,
    proposal_id: Uuid,
) -> Result<(), NormalizationError> {
    mark_status(tx, proposal_id, NormalizationProposalStatus::Applied).await
}

pub async fn mark_rolled_back(
    tx: &mut Transaction<'_, Postgres>,
    proposal_id: Uuid,
) -> Result<(), NormalizationError> {
    mark_status(tx, proposal_id, NormalizationProposalStatus::RolledBack).await
}

async fn mark_status(
    tx: &mut Transaction<'_, Postgres>,
    proposal_id: Uuid,
    status: NormalizationProposalStatus,
) -> Result<(), NormalizationError> {
    sqlx::query(
        "UPDATE catalog.normalization_proposal
         SET status = $1, updated_at = now()
         WHERE id = $2",
    )
    .bind(status.wire_name())
    .bind(proposal_id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}
