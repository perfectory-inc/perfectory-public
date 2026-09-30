//! Wire contract of the parcel-lineage steward API (root ADR-0115).
//!
//! The staff console (Dawneer, root ADR-0114) uses these types by path, so a change here breaks its
//! build in the same CI run instead of in production.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Why an item is on the review queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageReviewStatus {
    /// Area and category match uniquely but ownership differs.
    NeedsReview,
    /// Nothing matched.
    Pending,
    /// An automatic link sampled for quality review.
    Sample,
}

/// What a steward decided.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageDecisionOutcome {
    /// The parcel is the same land as the named candidate.
    Link,
    /// None of the candidates is the same land.
    NotALink,
    /// This steward cannot tell; another steward takes it.
    Unsure,
    /// Hand the item to an adjudicator.
    Escalate,
}

/// The fixed reasons a lineage-writing decision cites.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageReasonCode {
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
    /// Anything else; the note says what.
    Other,
}

/// One candidate predecessor the derivation found.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct LineageReviewCandidate {
    /// The older PNU.
    pub predecessor_pnu: String,
    /// Lineage relation.
    pub relation: String,
    /// Lineage grade.
    pub grade: String,
    /// What the evidence was.
    pub evidence_kind: String,
    /// Where the evidence is.
    pub evidence_ref: String,
    /// Whether this candidate is the link currently in effect.
    pub in_effect: bool,
}

/// Who holds an item.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct LineageReviewClaim {
    /// The steward's principal id.
    pub claimed_by: String,
    /// When the claim lapses.
    pub expires_at: DateTime<Utc>,
}

/// A stored decision.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct LineageStewardDecision {
    /// Decision id.
    pub decision_id: String,
    /// What was decided.
    pub outcome: LineageDecisionOutcome,
    /// The linked candidate, for a link.
    pub predecessor_pnu: Option<String>,
    /// The steward's principal id.
    pub decided_by: String,
    /// When.
    pub decided_at: DateTime<Utc>,
    /// Whether a second person must approve before it takes effect.
    pub requires_approval: bool,
    /// `approved` or `rejected`, once a second person has ruled.
    pub approval: Option<String>,
    /// The decision this one replaces.
    pub supersedes_decision_id: Option<String>,
}

/// A review item with its claim and decisions.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct LineageReviewItem {
    /// Stable item id.
    pub item_id: String,
    /// The parcel to decide.
    pub subject_pnu: String,
    /// Why it is on the queue.
    pub status: LineageReviewStatus,
    /// Send this back with a decision; a changed value means the evidence changed.
    pub evidence_etag: String,
    /// Candidate predecessors with their evidence.
    pub candidates: Vec<LineageReviewCandidate>,
    /// The live claim, if any.
    pub claim: Option<LineageReviewClaim>,
    /// Every decision, newest first.
    pub decisions: Vec<LineageStewardDecision>,
}

/// A page of review items.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct LineageReviewItemPage {
    /// Items in parcel order.
    pub items: Vec<LineageReviewItem>,
    /// Pass as `after` for the next page; absent on the last page.
    pub next_after: Option<String>,
}

/// A steward's decision on an item.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LineageDecisionRequest {
    /// What was decided.
    pub outcome: LineageDecisionOutcome,
    /// The chosen candidate; only for `link`.
    pub predecessor_pnu: Option<String>,
    /// Required for `link` and `not_a_link`.
    pub reason_code: Option<LineageReasonCode>,
    /// Free text; required when the reason is `other`.
    #[serde(default)]
    pub note: String,
    /// The `evidence_etag` of the item as read.
    pub evidence_etag: String,
    /// The decision this one replaces.
    pub supersedes_decision_id: Option<String>,
}

/// What a decision call did.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageDecisionDisposition {
    /// Stored now.
    Recorded,
    /// The same Idempotency-Key and body came before; this is that result.
    Replayed,
    /// A dry run passed every check; nothing was stored.
    DryRunPassed,
}

/// The result of a decision call.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct LineageDecisionResponse {
    /// What the call did.
    pub disposition: LineageDecisionDisposition,
    /// Whether a second person must approve.
    pub requires_approval: bool,
    /// The stored decision; absent for a dry run.
    pub decision: Option<LineageStewardDecision>,
}

/// A second person's verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LineageApprovalVerdict {
    /// The decision takes effect.
    Approved,
    /// The decision does not take effect.
    Rejected,
}

/// A second person's ruling on a decision that needs one.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LineageApprovalRequest {
    /// The ruling.
    pub verdict: LineageApprovalVerdict,
    /// Free text.
    #[serde(default)]
    pub note: String,
}
