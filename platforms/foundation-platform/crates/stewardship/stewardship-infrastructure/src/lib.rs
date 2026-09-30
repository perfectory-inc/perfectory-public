//! `PostgreSQL` store for lineage review items, claims, decisions and approvals (root ADR-0115).
//!
//! Every write runs in one transaction that locks the review item first, then reruns the domain
//! rules against the rows stored at that instant. The tables enforce what must hold even if a
//! future caller skips this crate: decisions, approvals and folds are append-only, and the approver
//! of a decision cannot be its decider (`20260930120000_lineage_stewardship.sql`).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use stewardship_application::{
    DecideCommand, DecideOutcome, DecisionRecord, LineageStewardshipStore, ReviewItemFilter,
    ReviewItemView,
};
use stewardship_domain::{
    check_approval, check_decision, check_idempotency_key, claim_expiry, Claim, Outcome,
    ReviewItem, ReviewStatus, StewardshipError,
};
use uuid::Uuid;

/// The `PostgreSQL` implementation of [`LineageStewardshipStore`].
#[derive(Clone)]
pub struct PgLineageStewardshipStore {
    pool: PgPool,
}

impl PgLineageStewardshipStore {
    /// Creates a store over the Foundation database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_sqlx(error: sqlx::Error) -> StewardshipError {
    StewardshipError::Persistence(error.to_string())
}

const DECISION_COLUMNS: &str = "d.decision_id, d.item_id, d.subject_code, d.outcome, \
     d.predecessor_code, d.decided_by, d.decided_at, d.requires_approval, \
     d.supersedes_decision_id, a.verdict";

fn parse_status(value: &str) -> Result<ReviewStatus, StewardshipError> {
    match value {
        "needs_review" => Ok(ReviewStatus::NeedsReview),
        "pending" => Ok(ReviewStatus::Pending),
        "sample" => Ok(ReviewStatus::Sample),
        other => Err(StewardshipError::InvalidItem(format!(
            "unknown status {other}"
        ))),
    }
}

const fn status_wire(status: ReviewStatus) -> &'static str {
    match status {
        ReviewStatus::NeedsReview => "needs_review",
        ReviewStatus::Pending => "pending",
        ReviewStatus::Sample => "sample",
    }
}

fn parse_outcome(value: &str) -> Result<Outcome, StewardshipError> {
    match value {
        "link" => Ok(Outcome::Link),
        "not_a_link" => Ok(Outcome::NotALink),
        "unsure" => Ok(Outcome::Unsure),
        "escalate" => Ok(Outcome::Escalate),
        other => Err(StewardshipError::InvalidItem(format!(
            "unknown outcome {other}"
        ))),
    }
}

const fn outcome_wire(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Link => "link",
        Outcome::NotALink => "not_a_link",
        Outcome::Unsure => "unsure",
        Outcome::Escalate => "escalate",
    }
}

fn row_to_item(row: &PgRow) -> Result<ReviewItem, StewardshipError> {
    Ok(ReviewItem {
        item_id: row.try_get("item_id").map_err(map_sqlx)?,
        subject_code: row.try_get("subject_code").map_err(map_sqlx)?,
        status: parse_status(&row.try_get::<String, _>("status").map_err(map_sqlx)?)?,
        candidates_json: row.try_get("candidates_json").map_err(map_sqlx)?,
    })
}

fn row_to_decision(row: &PgRow) -> Result<DecisionRecord, StewardshipError> {
    Ok(DecisionRecord {
        decision_id: row.try_get("decision_id").map_err(map_sqlx)?,
        item_id: row.try_get("item_id").map_err(map_sqlx)?,
        subject_code: row.try_get("subject_code").map_err(map_sqlx)?,
        outcome: parse_outcome(&row.try_get::<String, _>("outcome").map_err(map_sqlx)?)?,
        predecessor_code: row.try_get("predecessor_code").map_err(map_sqlx)?,
        decided_by: row.try_get("decided_by").map_err(map_sqlx)?,
        decided_at: row.try_get("decided_at").map_err(map_sqlx)?,
        requires_approval: row.try_get("requires_approval").map_err(map_sqlx)?,
        approval: row.try_get("verdict").map_err(map_sqlx)?,
        supersedes_decision_id: row.try_get("supersedes_decision_id").map_err(map_sqlx)?,
    })
}

async fn live_claim<'e, E>(
    executor: E,
    item_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Option<Claim>, StewardshipError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        "SELECT claimed_by, expires_at FROM catalog.lineage_review_claim
         WHERE item_id = $1 AND expires_at > $2",
    )
    .bind(item_id)
    .bind(now)
    .fetch_optional(executor)
    .await
    .map_err(map_sqlx)?;
    row.map(|row| {
        Ok(Claim {
            claimed_by: row.try_get("claimed_by").map_err(map_sqlx)?,
            expires_at: row.try_get("expires_at").map_err(map_sqlx)?,
        })
    })
    .transpose()
}

async fn decisions_of<'e, E>(
    executor: E,
    item_ids: &[Uuid],
) -> Result<Vec<DecisionRecord>, StewardshipError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(&format!(
        "SELECT {DECISION_COLUMNS}
         FROM catalog.lineage_steward_decision d
         LEFT JOIN catalog.lineage_steward_approval a USING (decision_id)
         WHERE d.item_id = ANY($1)
         ORDER BY d.decided_at DESC, d.decision_id DESC"
    ))
    .bind(item_ids)
    .fetch_all(executor)
    .await
    .map_err(map_sqlx)?;
    rows.iter().map(row_to_decision).collect()
}

async fn decision_by_id(
    tx: &mut Transaction<'_, Postgres>,
    decision_id: Uuid,
) -> Result<Option<DecisionRecord>, StewardshipError> {
    let row = sqlx::query(&format!(
        "SELECT {DECISION_COLUMNS}
         FROM catalog.lineage_steward_decision d
         LEFT JOIN catalog.lineage_steward_approval a USING (decision_id)
         WHERE d.decision_id = $1"
    ))
    .bind(decision_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    row.as_ref().map(row_to_decision).transpose()
}

/// The decision a key already produced, if the same steward used it before.
async fn replay(
    tx: &mut Transaction<'_, Postgres>,
    decider: Uuid,
    key: &str,
    request_sha256: &str,
) -> Result<Option<DecisionRecord>, StewardshipError> {
    let row = sqlx::query(
        "SELECT decision_id, request_sha256 FROM catalog.lineage_steward_decision
         WHERE decided_by = $1 AND idempotency_key = $2",
    )
    .bind(decider)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    let Some(row) = row else { return Ok(None) };
    let stored: String = row.try_get("request_sha256").map_err(map_sqlx)?;
    if stored != request_sha256 {
        return Err(StewardshipError::IdempotencyConflict);
    }
    decision_by_id(tx, row.try_get("decision_id").map_err(map_sqlx)?).await
}

#[async_trait]
impl LineageStewardshipStore for PgLineageStewardshipStore {
    async fn list_items(
        &self,
        filter: ReviewItemFilter,
        now: DateTime<Utc>,
    ) -> Result<Vec<ReviewItemView>, StewardshipError> {
        // An item is decided when a lineage-writing decision on its current evidence stands:
        // not superseded, and not refused by a second person.
        let rows = sqlx::query(
            "SELECT i.item_id, i.subject_code, i.status, i.candidates_json
             FROM catalog.lineage_review_item i
             WHERE ($1::text IS NULL OR i.status = $1)
               AND ($2::text IS NULL OR starts_with(i.subject_code, $2))
               AND ($3::text IS NULL OR i.subject_code > $3)
               AND (NOT $4 OR NOT EXISTS (
                    SELECT 1 FROM catalog.lineage_steward_decision d
                    LEFT JOIN catalog.lineage_steward_approval a USING (decision_id)
                    WHERE d.item_id = i.item_id
                      AND d.evidence_etag = i.evidence_etag
                      AND d.outcome IN ('link', 'not_a_link')
                      AND a.verdict IS DISTINCT FROM 'rejected'
                      AND NOT EXISTS (
                          SELECT 1 FROM catalog.lineage_steward_decision s
                          WHERE s.supersedes_decision_id = d.decision_id)))
             ORDER BY i.subject_code
             LIMIT $5",
        )
        .bind(filter.status.map(status_wire))
        .bind(filter.code_prefix)
        .bind(filter.after_subject_code)
        .bind(filter.undecided_only)
        .bind(i64::from(filter.limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let items = rows
            .iter()
            .map(row_to_item)
            .collect::<Result<Vec<_>, _>>()?;
        let ids = items.iter().map(|item| item.item_id).collect::<Vec<_>>();
        let decisions = decisions_of(&self.pool, &ids).await?;
        let claims = sqlx::query(
            "SELECT item_id, claimed_by, expires_at FROM catalog.lineage_review_claim
             WHERE item_id = ANY($1) AND expires_at > $2",
        )
        .bind(&ids)
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let mut views = Vec::with_capacity(items.len());
        for item in items {
            let mut claim = None;
            for row in &claims {
                if row.try_get::<Uuid, _>("item_id").map_err(map_sqlx)? == item.item_id {
                    claim = Some(Claim {
                        claimed_by: row.try_get("claimed_by").map_err(map_sqlx)?,
                        expires_at: row.try_get("expires_at").map_err(map_sqlx)?,
                    });
                }
            }
            let own = decisions
                .iter()
                .filter(|d| d.item_id == item.item_id)
                .cloned()
                .collect();
            views.push(ReviewItemView {
                item,
                claim,
                decisions: own,
            });
        }
        Ok(views)
    }

    async fn get_item(
        &self,
        item_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ReviewItemView, StewardshipError> {
        let row = sqlx::query(
            "SELECT item_id, subject_code, status, candidates_json
             FROM catalog.lineage_review_item WHERE item_id = $1",
        )
        .bind(item_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?
        .ok_or(StewardshipError::ItemNotFound)?;
        Ok(ReviewItemView {
            item: row_to_item(&row)?,
            claim: live_claim(&self.pool, item_id, now).await?,
            decisions: decisions_of(&self.pool, &[item_id]).await?,
        })
    }

    async fn claim(
        &self,
        item_id: Uuid,
        steward: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Claim, StewardshipError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        lock_item(&mut tx, item_id).await?;
        if let Some(held) = live_claim(&mut *tx, item_id, now).await? {
            if held.claimed_by != steward {
                return Err(StewardshipError::ClaimedByAnother {
                    until: held.expires_at,
                });
            }
        }
        let claim = Claim {
            claimed_by: steward,
            expires_at: claim_expiry(now),
        };
        sqlx::query(
            "INSERT INTO catalog.lineage_review_claim (item_id, claimed_by, claimed_at, expires_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (item_id) DO UPDATE
             SET claimed_by = EXCLUDED.claimed_by, claimed_at = EXCLUDED.claimed_at,
                 expires_at = EXCLUDED.expires_at",
        )
        .bind(item_id)
        .bind(steward)
        .bind(now)
        .bind(claim.expires_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(claim)
    }

    async fn release(&self, item_id: Uuid, steward: Uuid) -> Result<(), StewardshipError> {
        sqlx::query(
            "DELETE FROM catalog.lineage_review_claim WHERE item_id = $1 AND claimed_by = $2",
        )
        .bind(item_id)
        .bind(steward)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn decide(&self, command: DecideCommand) -> Result<DecideOutcome, StewardshipError> {
        let request_sha256 = command.draft.request_sha256()?;
        if !command.dry_run {
            check_idempotency_key(&command.idempotency_key)?;
        }
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        if !command.dry_run {
            if let Some(previous) = replay(
                &mut tx,
                command.decider,
                &command.idempotency_key,
                &request_sha256,
            )
            .await?
            {
                return Ok(DecideOutcome::Replayed(previous));
            }
        }
        let item = lock_item(&mut tx, command.item_id).await?;
        let claim = live_claim(&mut *tx, command.item_id, command.now).await?;
        let requires_approval =
            check_decision(&item, &command.draft, command.decider, claim, command.now)?;

        check_references(&mut tx, &command, &item.subject_code).await?;
        if command.dry_run {
            tx.rollback().await.map_err(map_sqlx)?;
            return Ok(DecideOutcome::WouldRecord { requires_approval });
        }

        let decision_id = Uuid::now_v7();
        let draft = &command.draft;
        let inserted = sqlx::query(
            "INSERT INTO catalog.lineage_steward_decision
             (decision_id, item_id, unit, subject_code, outcome, predecessor_code, reason_code, note,
              evidence_etag, idempotency_key, request_sha256, decided_by, decided_at,
              supersedes_decision_id, requires_approval)
             SELECT $1, i.item_id, i.unit, i.subject_code, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
             FROM catalog.lineage_review_item i WHERE i.item_id = $13",
        )
        .bind(decision_id)
        .bind(outcome_wire(draft.outcome))
        .bind(draft.predecessor_pnu.as_deref())
        .bind(draft.reason_code.map(reason_wire))
        .bind(draft.note.trim())
        .bind(&draft.evidence_etag)
        .bind(&command.idempotency_key)
        .bind(&request_sha256)
        .bind(command.decider)
        .bind(command.now)
        .bind(draft.supersedes_decision_id)
        .bind(requires_approval)
        .bind(command.item_id)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            // A concurrent request with the same key won the unique index; answer with its result.
            if error
                .as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
            {
                drop(tx);
                let mut retry = self.pool.begin().await.map_err(map_sqlx)?;
                return replay(
                    &mut retry,
                    command.decider,
                    &command.idempotency_key,
                    &request_sha256,
                )
                .await?
                .map(DecideOutcome::Replayed)
                .ok_or_else(|| map_sqlx(error));
            }
            return Err(map_sqlx(error));
        }
        sqlx::query(
            "DELETE FROM catalog.lineage_review_claim WHERE item_id = $1 AND claimed_by = $2",
        )
        .bind(command.item_id)
        .bind(command.decider)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let record = decision_by_id(&mut tx, decision_id).await?.ok_or_else(|| {
            StewardshipError::Persistence("inserted decision is not readable".to_owned())
        })?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(DecideOutcome::Recorded(record))
    }

    async fn approve(
        &self,
        decision_id: Uuid,
        approver: Uuid,
        accept: bool,
        note: String,
    ) -> Result<DecisionRecord, StewardshipError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
        sqlx::query(
            "SELECT 1 FROM catalog.lineage_steward_decision WHERE decision_id = $1 FOR UPDATE",
        )
        .bind(decision_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| StewardshipError::InvalidState("decision not found".to_owned()))?;
        let decision = decision_by_id(&mut tx, decision_id)
            .await?
            .ok_or_else(|| StewardshipError::InvalidState("decision not found".to_owned()))?;
        check_approval(decision.decided_by, decision.requires_approval, approver)?;
        if decision.approval.is_some() {
            return Err(StewardshipError::InvalidState(
                "decision already has a verdict".to_owned(),
            ));
        }
        if note.chars().count() > stewardship_domain::NOTE_MAX_CHARS {
            return Err(StewardshipError::InvalidInput(
                "note is too long".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO catalog.lineage_steward_approval (approval_id, decision_id, verdict, note, approved_by)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(Uuid::now_v7())
        .bind(decision_id)
        .bind(if accept { "approved" } else { "rejected" })
        .bind(note.trim())
        .bind(approver)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx)?;
        let record = decision_by_id(&mut tx, decision_id).await?.ok_or_else(|| {
            StewardshipError::Persistence("approved decision is not readable".to_owned())
        })?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(record)
    }
}

/// The rules that need other stored decisions: a superseded decision belongs to this item, and
/// one older parcel is not already the same land as another newer one (ADR-0115 §6).
async fn check_references(
    tx: &mut Transaction<'_, Postgres>,
    command: &DecideCommand,
    subject_code: &str,
) -> Result<(), StewardshipError> {
    if let Some(superseded) = command.draft.supersedes_decision_id {
        let target = decision_by_id(tx, superseded).await?;
        if target.is_none_or(|d| d.item_id != command.item_id) {
            return Err(StewardshipError::InvalidInput(
                "supersedes_decision_id names no decision of this item".to_owned(),
            ));
        }
    }
    if let (Outcome::Link, Some(predecessor)) = (
        command.draft.outcome,
        command.draft.predecessor_pnu.as_deref(),
    ) {
        // One older parcel cannot be the same land as two newer ones (ADR-0115 §6).
        let taken = sqlx::query(
            "SELECT d.subject_code FROM catalog.lineage_steward_decision d
             LEFT JOIN catalog.lineage_steward_approval a USING (decision_id)
             WHERE d.outcome = 'link' AND d.predecessor_code = $1 AND d.subject_code <> $2
               AND a.verdict IS DISTINCT FROM 'rejected'
               AND NOT EXISTS (SELECT 1 FROM catalog.lineage_steward_decision s
                               WHERE s.supersedes_decision_id = d.decision_id)
             LIMIT 1",
        )
        .bind(predecessor)
        .bind(subject_code)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        if let Some(row) = taken {
            return Err(StewardshipError::PredecessorTaken {
                predecessor: predecessor.to_owned(),
                other: row.try_get("subject_code").map_err(map_sqlx)?,
            });
        }
    }
    Ok(())
}

async fn lock_item(
    tx: &mut Transaction<'_, Postgres>,
    item_id: Uuid,
) -> Result<ReviewItem, StewardshipError> {
    let row = sqlx::query(
        "SELECT item_id, subject_code, status, candidates_json
         FROM catalog.lineage_review_item WHERE item_id = $1 FOR UPDATE",
    )
    .bind(item_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?
    .ok_or(StewardshipError::ItemNotFound)?;
    row_to_item(&row)
}

const fn reason_wire(reason: stewardship_domain::ReasonCode) -> &'static str {
    use stewardship_domain::ReasonCode;
    match reason {
        ReasonCode::BuildingRegister => "building_register",
        ReasonCode::OwnershipRecord => "ownership_record",
        ReasonCode::SiteSurvey => "site_survey",
        ReasonCode::OfficialDocument => "official_document",
        ReasonCode::CadastralMap => "cadastral_map",
        ReasonCode::Other => "other",
    }
}
