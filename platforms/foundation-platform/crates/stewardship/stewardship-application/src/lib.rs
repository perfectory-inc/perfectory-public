//! The port a steward-facing API uses to read review items and record decisions (root ADR-0115).
//!
//! Every write is one atomic call so the store can lock the item, rerun the domain rules against
//! what is stored at that instant, and insert — two stewards deciding the same parcel at once
//! cannot both win.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use stewardship_domain::{
    Claim, DecisionDraft, Outcome, ReviewItem, ReviewStatus, StewardshipError,
};
use uuid::Uuid;

/// A decision as stored.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionRecord {
    /// Decision id.
    pub decision_id: Uuid,
    /// The item it decides.
    pub item_id: Uuid,
    /// The parcel it decides.
    pub subject_code: String,
    /// What was decided.
    pub outcome: Outcome,
    /// The linked candidate, for a link.
    pub predecessor_code: Option<String>,
    /// Who decided.
    pub decided_by: Uuid,
    /// When.
    pub decided_at: DateTime<Utc>,
    /// Whether a second person must approve before it takes effect.
    pub requires_approval: bool,
    /// The approval verdict, once given (`approved` or `rejected`).
    pub approval: Option<String>,
    /// The decision this one replaces.
    pub supersedes_decision_id: Option<Uuid>,
}

/// A queue row with who holds it and its latest decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReviewItemView {
    /// The item.
    pub item: ReviewItem,
    /// The live claim, if any.
    pub claim: Option<Claim>,
    /// Every decision on it, newest first.
    pub decisions: Vec<DecisionRecord>,
}

/// Which items to list.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReviewItemFilter {
    /// Only this status.
    pub status: Option<ReviewStatus>,
    /// Only parcels whose PNU starts with this prefix (a sido or sigungu code).
    pub code_prefix: Option<String>,
    /// Only items nobody has decided yet.
    pub undecided_only: bool,
    /// Keyset cursor: items with a larger subject code.
    pub after_subject_code: Option<String>,
    /// Page size.
    pub limit: u32,
}

/// A decision request with its identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecideCommand {
    /// The item.
    pub item_id: Uuid,
    /// The draft as submitted.
    pub draft: DecisionDraft,
    /// The steward.
    pub decider: Uuid,
    /// The request's idempotency key.
    pub idempotency_key: String,
    /// Run every check but store nothing (ADR-0115 §6).
    pub dry_run: bool,
    /// The instant the checks run at.
    pub now: DateTime<Utc>,
}

/// What a decide call produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecideOutcome {
    /// Stored now.
    Recorded(DecisionRecord),
    /// The same key and body came before; this is that result.
    Replayed(DecisionRecord),
    /// A dry run passed; `requires_approval` is what a real call would record.
    WouldRecord {
        /// Whether a second person would have to approve.
        requires_approval: bool,
    },
}

/// Durable storage for review items, claims, decisions and approvals.
#[async_trait]
pub trait LineageStewardshipStore: Send + Sync {
    /// Lists items in subject-code order.
    ///
    /// # Errors
    /// Returns [`StewardshipError`] when persistence fails.
    async fn list_items(
        &self,
        filter: ReviewItemFilter,
        now: DateTime<Utc>,
    ) -> Result<Vec<ReviewItemView>, StewardshipError>;

    /// Reads one item.
    ///
    /// # Errors
    /// Returns [`StewardshipError::ItemNotFound`] when it does not exist.
    async fn get_item(
        &self,
        item_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ReviewItemView, StewardshipError>;

    /// Takes or renews the claim for `steward`; refuses while another steward's claim is live.
    ///
    /// # Errors
    /// Returns [`StewardshipError::ClaimedByAnother`] or [`StewardshipError::ItemNotFound`].
    async fn claim(
        &self,
        item_id: Uuid,
        steward: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Claim, StewardshipError>;

    /// Gives up `steward`'s claim; releasing a claim one does not hold changes nothing.
    ///
    /// # Errors
    /// Returns [`StewardshipError`] when persistence fails.
    async fn release(&self, item_id: Uuid, steward: Uuid) -> Result<(), StewardshipError>;

    /// Checks and, unless it is a dry run, records a decision atomically.
    ///
    /// # Errors
    /// Returns the rule the decision breaks, or a persistence failure.
    async fn decide(&self, command: DecideCommand) -> Result<DecideOutcome, StewardshipError>;

    /// Records a second person's verdict on a decision that needs one.
    ///
    /// # Errors
    /// Returns [`StewardshipError::SelfApproval`], [`StewardshipError::InvalidState`] or a
    /// persistence failure.
    async fn approve(
        &self,
        decision_id: Uuid,
        approver: Uuid,
        accept: bool,
        note: String,
    ) -> Result<DecisionRecord, StewardshipError>;
}
