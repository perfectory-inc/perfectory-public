//! Pure rules for stewards deciding parcel-lineage review items (root ADR-0115).
//!
//! A review item is a parcel the lineage derivation could not settle (root ADR-0113 §10). A steward
//! decides it through one API; this crate owns what that API may accept. It has no database, HTTP,
//! clock, or lakehouse dependency: time comes in as an argument and every rule is a function.

use std::fmt;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// How long a claim holds an item for one steward (ADR-0115 §5).
pub const CLAIM_TTL_MINUTES: i64 = 30;

/// Longest free-text note a decision or approval may carry.
pub const NOTE_MAX_CHARS: usize = 2000;

/// Shortest and longest accepted idempotency key.
pub const IDEMPOTENCY_KEY_CHARS: (usize, usize) = (8, 200);

/// Why a review item is on the queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    /// Area and category match uniquely but ownership differs.
    NeedsReview,
    /// Nothing matched.
    Pending,
    /// An automatic link sent to a person as a quality sample (ADR-0115 §10).
    Sample,
}

/// What a steward decided (ADR-0115 §2).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// The parcel is the same land as the named candidate.
    Link,
    /// None of the candidates is the same land.
    NotALink,
    /// This steward cannot tell; another steward takes it.
    Unsure,
    /// Hand the item to an adjudicator.
    Escalate,
}

impl Outcome {
    /// Whether this outcome becomes a lineage row when folded (ADR-0115 §9).
    #[must_use]
    pub const fn writes_lineage(self) -> bool {
        matches!(self, Self::Link | Self::NotALink)
    }
}

/// The fixed reasons a lineage-writing decision cites (ADR-0115 §2).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    /// The building register ties the two numbers.
    BuildingRegister,
    /// Ownership records tie the two numbers.
    OwnershipRecord,
    /// Someone looked on site.
    SiteSurvey,
    /// A government document states it.
    OfficialDocument,
    /// The cadastral map shows it.
    CadastralMap,
    /// Anything else; the note must say what.
    Other,
}

/// One candidate predecessor the derivation found for an item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    /// The older PNU.
    pub predecessor_pnu: String,
    /// Lineage relation of the row.
    pub relation: String,
    /// Lineage grade of the row.
    pub grade: String,
    /// What the evidence was.
    pub evidence_kind: String,
    /// Where the evidence is.
    pub evidence_ref: String,
    /// Whether this candidate is the link currently in effect (only on sample items).
    #[serde(default)]
    pub in_effect: bool,
}

/// A review item as the API sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReviewItem {
    /// Stable per parcel: `UUIDv5` of `lineage-review:<unit>:<pnu>`.
    pub item_id: Uuid,
    /// The parcel to decide.
    pub subject_code: String,
    /// Why it is on the queue.
    pub status: ReviewStatus,
    /// The candidates exactly as the queue wrote them.
    pub candidates_json: String,
}

impl ReviewItem {
    /// The candidates, parsed.
    ///
    /// # Errors
    /// Returns [`StewardshipError::InvalidItem`] when the stored JSON is not a candidate list.
    pub fn candidates(&self) -> Result<Vec<Candidate>, StewardshipError> {
        serde_json::from_str(&self.candidates_json)
            .map_err(|error| StewardshipError::InvalidItem(format!("candidates_json: {error}")))
    }

    /// The fingerprint of what a steward saw (ADR-0115 §3).
    #[must_use]
    pub fn evidence_etag(&self) -> String {
        evidence_etag(self.status, &self.candidates_json)
    }
}

/// SHA-256 hex of `<status>\n<candidates_json>`, the bytes as stored.
///
/// The queue job computes the same value from the same bytes; the Python twin is
/// `lineage_review_queue.evidence_etag`, and the shared test vector pins both.
#[must_use]
pub fn evidence_etag(status: ReviewStatus, candidates_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(status_wire(status).as_bytes());
    hasher.update(b"\n");
    hasher.update(candidates_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

const fn status_wire(status: ReviewStatus) -> &'static str {
    match status {
        ReviewStatus::NeedsReview => "needs_review",
        ReviewStatus::Pending => "pending",
        ReviewStatus::Sample => "sample",
    }
}

/// A decision as a steward submits it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecisionDraft {
    /// What was decided.
    pub outcome: Outcome,
    /// The candidate chosen; only for [`Outcome::Link`].
    pub predecessor_pnu: Option<String>,
    /// Why; required when the outcome writes lineage.
    pub reason_code: Option<ReasonCode>,
    /// Free text.
    #[serde(default)]
    pub note: String,
    /// The fingerprint of the item the steward decided on.
    pub evidence_etag: String,
    /// The decision this one replaces, if any.
    pub supersedes_decision_id: Option<Uuid>,
}

impl DecisionDraft {
    /// SHA-256 of the draft's canonical JSON: what an idempotency key is bound to.
    ///
    /// # Errors
    /// Returns [`StewardshipError::InvalidInput`] if the draft cannot be serialized.
    pub fn request_sha256(&self) -> Result<String, StewardshipError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| StewardshipError::InvalidInput(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// Who holds an item right now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Claim {
    /// The steward holding it.
    pub claimed_by: Uuid,
    /// When the hold ends.
    pub expires_at: DateTime<Utc>,
}

/// The expiry of a claim taken at `now`.
#[must_use]
pub fn claim_expiry(now: DateTime<Utc>) -> DateTime<Utc> {
    now + Duration::minutes(CLAIM_TTL_MINUTES)
}

/// Why a decision or approval was refused.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StewardshipError {
    /// The request is malformed.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The stored item is malformed.
    #[error("invalid review item: {0}")]
    InvalidItem(String),
    /// The item changed since the steward looked at it.
    #[error("the evidence changed since it was read; current etag {current}")]
    StaleEvidence {
        /// The etag now.
        current: String,
    },
    /// The chosen predecessor is not one of the item's candidates.
    #[error("{0} is not a candidate of this item")]
    NotACandidate(String),
    /// Another steward holds the item.
    #[error("the item is claimed by another steward until {until}")]
    ClaimedByAnother {
        /// When the other claim ends.
        until: DateTime<Utc>,
    },
    /// The same idempotency key came with a different body.
    #[error("the idempotency key was already used for a different request")]
    IdempotencyConflict,
    /// A decider tried to approve their own decision.
    #[error("a decision cannot be approved by the steward who made it")]
    SelfApproval,
    /// The older parcel is already decided to be another parcel's same land.
    #[error("{predecessor} is already the same land as {other}")]
    PredecessorTaken {
        /// The older PNU.
        predecessor: String,
        /// The parcel it is already linked to.
        other: String,
    },
    /// The review item does not exist.
    #[error("review item not found")]
    ItemNotFound,
    /// The decision does not exist or needs no approval.
    #[error("{0}")]
    InvalidState(String),
    /// The store failed.
    #[error("persistence failure: {0}")]
    Persistence(String),
}

impl fmt::Display for ReviewStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(status_wire(*self))
    }
}

/// Everything the API checks before it stores a decision; the dry run runs exactly this
/// (ADR-0115 §6). `claim` is the live claim on the item, if any.
///
/// Returns whether the decision needs a second person (ADR-0115 §7).
///
/// # Errors
/// Returns the first rule the draft breaks.
pub fn check_decision(
    item: &ReviewItem,
    draft: &DecisionDraft,
    decider: Uuid,
    claim: Option<Claim>,
    now: DateTime<Utc>,
) -> Result<bool, StewardshipError> {
    let current = item.evidence_etag();
    if draft.evidence_etag != current {
        return Err(StewardshipError::StaleEvidence { current });
    }
    if let Some(claim) = claim {
        if claim.claimed_by != decider && claim.expires_at > now {
            return Err(StewardshipError::ClaimedByAnother {
                until: claim.expires_at,
            });
        }
    }
    if draft.note.chars().count() > NOTE_MAX_CHARS {
        return Err(StewardshipError::InvalidInput(format!(
            "note is longer than {NOTE_MAX_CHARS} characters"
        )));
    }
    let candidates = item.candidates()?;
    match (draft.outcome, draft.predecessor_pnu.as_deref()) {
        (Outcome::Link, None) => {
            return Err(StewardshipError::InvalidInput(
                "link names a candidate".to_owned(),
            ));
        }
        (Outcome::Link, Some(pnu)) => {
            if !candidates.iter().any(|c| c.predecessor_pnu == pnu) {
                return Err(StewardshipError::NotACandidate(pnu.to_owned()));
            }
        }
        (_, Some(_)) => {
            return Err(StewardshipError::InvalidInput(
                "only link names a candidate".to_owned(),
            ));
        }
        (_, None) => {}
    }
    if draft.outcome.writes_lineage() && draft.reason_code.is_none() {
        return Err(StewardshipError::InvalidInput(
            "a lineage decision cites a reason".to_owned(),
        ));
    }
    if draft.reason_code == Some(ReasonCode::Other) && draft.note.trim().is_empty() {
        return Err(StewardshipError::InvalidInput(
            "reason other needs a note".to_owned(),
        ));
    }
    Ok(requires_approval(&candidates, draft))
}

/// A decision against a link already in effect needs a second person (ADR-0115 §7).
#[must_use]
pub fn requires_approval(candidates: &[Candidate], draft: &DecisionDraft) -> bool {
    let Some(effective) = candidates.iter().find(|c| c.in_effect) else {
        return false;
    };
    match draft.outcome {
        Outcome::NotALink => true,
        Outcome::Link => {
            draft.predecessor_pnu.as_deref() != Some(effective.predecessor_pnu.as_str())
        }
        Outcome::Unsure | Outcome::Escalate => false,
    }
}

/// Checks an idempotency key's shape.
///
/// # Errors
/// Returns [`StewardshipError::InvalidInput`] when the key is too short, too long, or not printable ASCII.
pub fn check_idempotency_key(key: &str) -> Result<(), StewardshipError> {
    let (min, max) = IDEMPOTENCY_KEY_CHARS;
    if key.len() < min || key.len() > max || !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(StewardshipError::InvalidInput(format!(
            "Idempotency-Key must be {min}-{max} printable ASCII characters"
        )));
    }
    Ok(())
}

/// A second person may approve; the decider may not (ADR-0115 §7).
///
/// # Errors
/// Returns [`StewardshipError::SelfApproval`] or [`StewardshipError::InvalidState`].
pub fn check_approval(
    decided_by: Uuid,
    requires: bool,
    approver: Uuid,
) -> Result<(), StewardshipError> {
    if !requires {
        return Err(StewardshipError::InvalidState(
            "this decision needs no approval".to_owned(),
        ));
    }
    if decided_by == approver {
        return Err(StewardshipError::SelfApproval);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
