//! Pure rules for stewards deciding review items (root ADR-0115, ADR-0263).
//!
//! A review item is what an automatic derivation could not settle: a parcel's lineage (root
//! ADR-0113 §10, kind [`ItemKind::Parcel`]) or the pair of an abolished 법정동 code (root ADR-0144 §3,
//! kind [`ItemKind::LegalDong`]). A steward decides it through one API; this crate owns what that API
//! may accept. It has no database, HTTP,
//! clock, or lakehouse dependency: time comes in as an argument and every rule is a function.

use std::fmt;

/// The review queue file the lakehouse hands to the steward database.
pub mod handoff;

/// How a standing decision becomes a lineage row.
pub mod fold;

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
    /// A 법정동 change the data did not pair; a steward may pair it (ADR-0144 §3.4).
    Steward,
    /// A 법정동 split into several codes: the data's to record, never a steward's (ADR-0144 §4).
    Split,
    /// A 법정동 change waiting for the data that decides it (ADR-0144 §4).
    AwaitingData,
}

/// Every status, in wire order.
pub const STATUSES: [ReviewStatus; 6] = [
    ReviewStatus::NeedsReview,
    ReviewStatus::Pending,
    ReviewStatus::Sample,
    ReviewStatus::Steward,
    ReviewStatus::Split,
    ReviewStatus::AwaitingData,
];

impl ReviewStatus {
    /// Whether the data, not a person, settles an item in this status (ADR-0144 §4): it needs no
    /// human action, so the default review list leaves it out.
    #[must_use]
    pub const fn data_decides(self) -> bool {
        matches!(self, Self::Split | Self::AwaitingData)
    }
}

/// What kind of thing a review item decides; the `unit` column (ADR-0263).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    /// Where a parcel came from: the subject is the newer PNU, a candidate an older one.
    Parcel,
    /// Where an abolished 법정동 code went: the subject is the old code, a candidate a new one.
    LegalDong,
}

impl ItemKind {
    /// The `unit` value stored for this kind.
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Parcel => "parcel",
            Self::LegalDong => "legal_dong",
        }
    }

    /// The kind a stored `unit` names.
    ///
    /// # Errors
    /// Returns [`StewardshipError::InvalidItem`] for an unknown unit.
    pub fn from_wire(unit: &str) -> Result<Self, StewardshipError> {
        match unit {
            "parcel" => Ok(Self::Parcel),
            "legal_dong" => Ok(Self::LegalDong),
            other => Err(StewardshipError::InvalidItem(format!(
                "unknown unit {other}"
            ))),
        }
    }

    /// Whether `code` is a well-formed subject or candidate of this kind.
    #[must_use]
    pub fn is_code(self, code: &str) -> bool {
        let digits = match self {
            Self::Parcel => 19,
            Self::LegalDong => 10,
        };
        code.len() == digits && code.bytes().all(|b| b.is_ascii_digit())
    }

    /// Whether `status` belongs to this kind.
    #[must_use]
    pub const fn has_status(self, status: ReviewStatus) -> bool {
        match self {
            Self::Parcel => matches!(
                status,
                ReviewStatus::NeedsReview | ReviewStatus::Pending | ReviewStatus::Sample
            ),
            Self::LegalDong => matches!(
                status,
                ReviewStatus::Steward | ReviewStatus::Split | ReviewStatus::AwaitingData
            ),
        }
    }
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

/// One new code the pairing run offers for an abolished 법정동 code (ADR-0263).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegalDongCandidate {
    /// The new 10-digit code.
    pub code: String,
    /// Its full name in the code table.
    pub name: String,
    /// The day the code table says it was created, `YYYYMMDD` or empty.
    pub created_date: String,
}

/// What the pairing run recorded about an abolished 법정동 code, stored as the item's
/// `evidence_json` (root ADR-0263). `legal_dong_code_change_pairs.review_handoff` writes it.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegalDongEvidence {
    /// The old code's full name in the code table.
    pub name: String,
    /// `sido`, `sigungu`, `eupmyeondong` or `ri`.
    pub level: String,
    /// The day the code table says it was abolished, `YYYYMMDD` or empty.
    pub abolished_date: String,
    /// Why the date + name rule did not pair it: `ambiguous_name_match` or `no_candidate`.
    pub rule: String,
    /// What the 지번 step saw.
    pub jibun: String,
    /// Where its 지번 went when it split: new code → how many.
    pub split_into: std::collections::BTreeMap<String, u64>,
    /// The evidence it waits for, when awaiting data.
    pub waiting_for: String,
    /// Whether the run read the official parcel-number history (`VWorld` 30527): `read` or
    /// `not_collected`.
    pub official_history: String,
    /// How many of its parcels the official rows move without deciding the pair.
    pub official_partial_moves: u64,
}

/// A review item as the API sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReviewItem {
    /// Stable per subject: `UUIDv5` of `lineage-review:<unit>:<subject_code>`.
    pub item_id: Uuid,
    /// What the item decides.
    pub kind: ItemKind,
    /// The parcel (newer PNU) or the abolished 법정동 code to decide.
    pub subject_code: String,
    /// Why it is on the queue.
    pub status: ReviewStatus,
    /// The candidates exactly as the queue wrote them.
    pub candidates_json: String,
    /// [`ItemKind::LegalDong`] only: the evidence the pairing run recorded, a JSON object.
    pub evidence_json: Option<String>,
}

impl ReviewItem {
    /// The parcel candidates, parsed; a 법정동 item has none of this shape.
    ///
    /// # Errors
    /// Returns [`StewardshipError::InvalidItem`] when the stored JSON is not a candidate list.
    pub fn candidates(&self) -> Result<Vec<Candidate>, StewardshipError> {
        if self.kind != ItemKind::Parcel {
            return Ok(Vec::new());
        }
        serde_json::from_str(&self.candidates_json)
            .map_err(|error| StewardshipError::InvalidItem(format!("candidates_json: {error}")))
    }

    /// The 법정동 candidates, parsed; a parcel item has none of this shape.
    ///
    /// # Errors
    /// Returns [`StewardshipError::InvalidItem`] when the stored JSON is not a candidate list.
    pub fn legal_dong_candidates(&self) -> Result<Vec<LegalDongCandidate>, StewardshipError> {
        if self.kind != ItemKind::LegalDong {
            return Ok(Vec::new());
        }
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
        ReviewStatus::Steward => "steward",
        ReviewStatus::Split => "split",
        ReviewStatus::AwaitingData => "awaiting_data",
    }
}

/// The status a stored value names.
///
/// # Errors
/// Returns [`StewardshipError::InvalidItem`] for an unknown status.
pub fn status_from_wire(value: &str) -> Result<ReviewStatus, StewardshipError> {
    STATUSES
        .into_iter()
        .find(|status| status_wire(*status) == value)
        .ok_or_else(|| StewardshipError::InvalidItem(format!("unknown status {value}")))
}

/// A decision as a steward submits it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecisionDraft {
    /// What was decided.
    pub outcome: Outcome,
    /// The candidate chosen; only for [`Outcome::Link`]. The older PNU of a parcel item, the new
    /// code of a 법정동 item. Serialized under its first name so request hashes stay stable.
    #[serde(rename = "predecessor_pnu")]
    pub candidate_code: Option<String>,
    /// Why, as a code; optional.
    pub reason_code: Option<ReasonCode>,
    /// Free text; optional unless the reason is `other`.
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
    /// A 법정동 item the data decides (a split, or one awaiting data) cannot be paired by a person.
    #[error("{subject} is {status}; the data decides it, not a steward (ADR-0144 §4)")]
    DataDecides {
        /// The abolished code.
        subject: String,
        /// Its status.
        status: ReviewStatus,
    },
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
    if item.kind == ItemKind::LegalDong
        && draft.outcome.writes_lineage()
        && item.status != ReviewStatus::Steward
    {
        return Err(StewardshipError::DataDecides {
            subject: item.subject_code.clone(),
            status: item.status,
        });
    }
    let candidates = item.candidates()?;
    match (draft.outcome, draft.candidate_code.as_deref()) {
        (Outcome::Link, None) => {
            return Err(StewardshipError::InvalidInput(
                "link names a candidate".to_owned(),
            ));
        }
        (Outcome::Link, Some(code)) => check_candidate(item, &candidates, code)?,
        (_, Some(_)) => {
            return Err(StewardshipError::InvalidInput(
                "only link names a candidate".to_owned(),
            ));
        }
        (_, None) => {}
    }
    // A reason and a note are optional for every kind: who, when, what and the evidence it stood on
    // are recorded with the decision. Only `other` must say what it is (root ADR-0268).
    if draft.reason_code == Some(ReasonCode::Other) && draft.note.trim().is_empty() {
        return Err(StewardshipError::InvalidInput(
            "the reason other needs a note saying what".to_owned(),
        ));
    }
    Ok(requires_approval(&candidates, draft))
}

/// A link names one of the item's candidates. A 법정동 item the run offered no candidate for takes
/// any other well-formed code; the pairing run checks it against the full code table (ADR-0263).
fn check_candidate(
    item: &ReviewItem,
    candidates: &[Candidate],
    code: &str,
) -> Result<(), StewardshipError> {
    let listed = match item.kind {
        ItemKind::Parcel => candidates.iter().any(|c| c.predecessor_pnu == code),
        ItemKind::LegalDong => {
            let offered = item.legal_dong_candidates()?;
            if offered.is_empty() {
                item.kind.is_code(code) && code != item.subject_code
            } else {
                offered.iter().any(|c| c.code == code)
            }
        }
    };
    if listed {
        Ok(())
    } else {
        Err(StewardshipError::NotACandidate(code.to_owned()))
    }
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
            draft.candidate_code.as_deref() != Some(effective.predecessor_pnu.as_str())
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
