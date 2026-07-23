//! Human proposal review persistence.

use foundation_normalization_application::{
    NormalizationProposalRecord, NormalizationProposalReviewCommand,
};
use foundation_normalization_domain::{
    NormalizationError, NormalizationProposalStatus, NormalizationReviewDecision,
};
use sqlx::{PgPool, Row};

use crate::postgres_error::map_sqlx;
use crate::row_mapping::row_to_proposal_record;

pub async fn review(
    pool: &PgPool,
    command: NormalizationProposalReviewCommand,
) -> Result<NormalizationProposalRecord, NormalizationError> {
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let existing = sqlx::query(
        "SELECT id, proposal_key, status
         FROM catalog.normalization_proposal
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(command.proposal_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx)?
    .ok_or(NormalizationError::ProposalNotFound)?;

    let current_status: String = existing.try_get("status").map_err(map_sqlx)?;
    if current_status != NormalizationProposalStatus::PendingReview.wire_name() {
        return Err(NormalizationError::InvalidState(
            "normalization proposal is not pending_review".to_owned(),
        ));
    }

    sqlx::query(
        "INSERT INTO catalog.normalization_proposal_review
         (id, proposal_id, reviewer_principal_id, decision, reason)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(command.id)
    .bind(command.proposal_id)
    .bind(command.reviewer_principal_id.as_uuid())
    .bind(command.decision.wire_name())
    .bind(command.reason.as_str())
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx)?;

    let updated = sqlx::query(
        "UPDATE catalog.normalization_proposal
         SET status = $1, updated_at = now()
         WHERE id = $2
         RETURNING id, proposal_key, status",
    )
    .bind(review_decision_status(command.decision).wire_name())
    .bind(command.proposal_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(map_sqlx)?;

    tx.commit().await.map_err(map_sqlx)?;
    row_to_proposal_record(&updated, false)
}

const fn review_decision_status(
    decision: NormalizationReviewDecision,
) -> NormalizationProposalStatus {
    match decision {
        NormalizationReviewDecision::Approved => NormalizationProposalStatus::Approved,
        NormalizationReviewDecision::Rejected => NormalizationProposalStatus::Rejected,
        NormalizationReviewDecision::NeedsChanges => NormalizationProposalStatus::PendingReview,
    }
}
