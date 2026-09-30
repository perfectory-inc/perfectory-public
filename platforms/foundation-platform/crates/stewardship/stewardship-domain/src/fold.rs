//! How a standing steward decision becomes a `silver.parcel_lineage` row (root ADR-0115 §9).
//!
//! The lakehouse is canonical: the database only holds a decision until the fold appends it to the
//! lineage. This module is the one place that says what that row looks like; the Spark job appends
//! the rows as given and the Python readers (`lineage_review_queue.steward_resolved`) read them back.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Candidate, Outcome, ReasonCode, StewardshipError};

/// The only schema the fold job reads.
pub const FOLD_HANDOFF_SCHEMA_VERSION: &str = "foundation-platform.lineage_steward_fold_handoff.v1";

/// Evidence kind of every steward row; the readers key on it.
pub const STEWARD_EVIDENCE_KIND: &str = "steward";

/// A decision ready to fold, as the store reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoldableDecision {
    /// Decision id.
    pub decision_id: Uuid,
    /// The parcel.
    pub subject_code: String,
    /// `Link` or `NotALink`; nothing else folds.
    pub outcome: Outcome,
    /// The linked candidate.
    pub predecessor_code: Option<String>,
    /// Why.
    pub reason_code: Option<ReasonCode>,
    /// Who.
    pub decided_by: Uuid,
    /// When.
    pub decided_at: DateTime<Utc>,
    /// The evidence it was made on.
    pub evidence_etag: String,
    /// The key that made it.
    pub idempotency_key: String,
    /// The item's candidates, to find the linked candidate's relation.
    pub candidates_json: String,
    /// Earlier cadastral snapshot of the item's lineage.
    pub from_snapshot_id: String,
    /// Later cadastral snapshot of the item's lineage.
    pub to_snapshot_id: String,
}

/// What the fold decision records in `evidence_ref`, read back by `steward_ref`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StewardEvidence {
    /// Decision id.
    pub decision_id: String,
    /// Who decided.
    pub decided_by: String,
    /// When, RFC 3339; the readers order standing decisions by it.
    pub decided_at: String,
    /// Why.
    pub reason_code: Option<ReasonCode>,
    /// The evidence fingerprint the decision stands on.
    pub evidence_etag: String,
    /// The request's idempotency key, so a rebuilt database cannot fold it twice unseen.
    pub idempotency_key: String,
}

/// One lineage row to append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StewardLineageRow {
    /// Decision id (the fold records it back).
    pub decision_id: String,
    /// Older PNU; empty for "not a link".
    pub predecessor_pnu: Option<String>,
    /// The parcel.
    pub successor_pnu: String,
    /// The linked candidate's relation, or `other`.
    pub relation: String,
    /// `official` for a link (a person confirmed it), `pending` for "not a link" (links nothing).
    pub grade: String,
    /// Always [`STEWARD_EVIDENCE_KIND`].
    pub evidence_kind: String,
    /// [`StewardEvidence`] as JSON.
    pub evidence_ref: String,
    /// Earlier snapshot.
    pub from_snapshot_id: String,
    /// Later snapshot.
    pub to_snapshot_id: String,
    /// The decision date.
    pub effective_date: String,
}

/// The file the fold job reads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FoldHandoff {
    /// Must be [`FOLD_HANDOFF_SCHEMA_VERSION`].
    pub schema_version: String,
    /// How many rows follow.
    pub row_count: usize,
    /// The rows.
    pub rows: Vec<StewardLineageRow>,
}

/// The lineage row of one decision.
///
/// # Errors
/// Returns [`StewardshipError::InvalidState`] for an outcome that does not fold, or a link whose
/// candidate is no longer among the item's candidates.
pub fn lineage_row(decision: &FoldableDecision) -> Result<StewardLineageRow, StewardshipError> {
    let (predecessor, relation, grade) =
        match (decision.outcome, decision.predecessor_code.as_deref()) {
            (Outcome::Link, Some(predecessor)) => {
                let candidates: Vec<Candidate> = serde_json::from_str(&decision.candidates_json)
                    .map_err(|error| StewardshipError::InvalidItem(error.to_string()))?;
                let relation = candidates
                    .iter()
                    .find(|c| c.predecessor_pnu == predecessor)
                    .map(|c| c.relation.clone())
                    .ok_or_else(|| {
                        StewardshipError::InvalidState(format!(
                            "decision {} links {predecessor}, which is no longer a candidate",
                            decision.decision_id
                        ))
                    })?;
                (Some(predecessor.to_owned()), relation, "official")
            }
            (Outcome::NotALink, None) => (None, "other".to_owned(), "pending"),
            _ => {
                return Err(StewardshipError::InvalidState(format!(
                    "decision {} does not write lineage",
                    decision.decision_id
                )))
            }
        };
    let evidence = StewardEvidence {
        decision_id: decision.decision_id.to_string(),
        decided_by: decision.decided_by.to_string(),
        decided_at: decision.decided_at.to_rfc3339(),
        reason_code: decision.reason_code,
        evidence_etag: decision.evidence_etag.clone(),
        idempotency_key: decision.idempotency_key.clone(),
    };
    Ok(StewardLineageRow {
        decision_id: evidence.decision_id.clone(),
        predecessor_pnu: predecessor,
        successor_pnu: decision.subject_code.clone(),
        relation,
        grade: grade.to_owned(),
        evidence_kind: STEWARD_EVIDENCE_KIND.to_owned(),
        evidence_ref: serde_json::to_string(&evidence)
            .map_err(|error| StewardshipError::InvalidState(error.to_string()))?,
        from_snapshot_id: decision.from_snapshot_id.clone(),
        to_snapshot_id: decision.to_snapshot_id.clone(),
        effective_date: decision.decided_at.date_naive().to_string(),
    })
}
