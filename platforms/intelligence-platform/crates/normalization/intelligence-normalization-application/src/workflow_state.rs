use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use intelligence_contracts::TraceContext;
use intelligence_normalization_domain::normalization::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

fn default_false() -> bool {
    false
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationProposalSubmission {
    pub request: NormalizationRequest,
    pub proposal: NormalizationProposal,
    pub validation: NormalizationValidationResult,
    pub trace_context: TraceContext,
    #[serde(default = "default_false")]
    pub commit_allowed: bool,
    #[serde(default = "default_true")]
    pub requires_human_review: bool,
    #[serde(default)]
    pub submission_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationRunResult {
    pub proposal: NormalizationProposal,
    pub validation: NormalizationValidationResult,
    #[serde(default = "default_false")]
    pub commit_allowed: bool,
    #[serde(default = "default_true")]
    pub requires_human_review: bool,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FoundationSubmissionStatus {
    Queued,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct FoundationSubmissionResult {
    pub submission_id: String,
    pub status: FoundationSubmissionStatus,
    #[serde(default = "default_true")]
    pub review_required: bool,
    #[serde(default = "default_platform")]
    pub platform: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationSubmissionRunResult {
    pub generation: NormalizationRunResult,
    pub submission_attempted: bool,
    #[serde(default)]
    pub submission_result: Option<FoundationSubmissionResult>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub outbox_status: Option<NormalizationOutboxStatus>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

fn default_platform() -> String {
    "foundation-platform".to_string()
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationOutboxStatus {
    Accepted,
    #[default]
    Pending,
    InFlight,
    Sent,
    FailedRetryable,
    FailedTerminal,
    DeadLetter,
    ReconcileRequired,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NormalizationOutboxRecord {
    pub outbox_id: String,
    pub idempotency_key: String,
    pub submission: NormalizationProposalSubmission,
    #[serde(default)]
    pub status: NormalizationOutboxStatus,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub submission_result: Option<FoundationSubmissionResult>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub payload_fingerprint: String,
    #[serde(default)]
    pub claimed_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl NormalizationOutboxRecord {
    pub fn new(idempotency_key: String, submission: NormalizationProposalSubmission) -> Self {
        let now = Utc::now();
        let fingerprint_bytes =
            serde_json::to_vec(&submission).unwrap_or_else(|_| b"unserializable".to_vec());
        let payload_fingerprint = format!("{:x}", Sha256::digest(&fingerprint_bytes));

        Self {
            outbox_id: Uuid::new_v4().to_string(),
            idempotency_key,
            submission,
            status: NormalizationOutboxStatus::Pending,
            attempts: 0,
            submission_result: None,
            last_error: None,
            payload_fingerprint,
            claimed_until: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct NormalizationAuditEvent {
    pub event_id: String,
    pub event_type: String,
    pub trace_context: TraceContext,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl NormalizationAuditEvent {
    pub fn new(
        event_type: impl Into<String>,
        trace_context: TraceContext,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            event_type: event_type.into(),
            trace_context,
            metadata,
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxAcquireResult {
    Acquired,
    AlreadyInFlight,
    AlreadySent,
    PayloadMismatch,
}

impl OutboxAcquireResult {
    pub fn is_acquired(&self) -> bool {
        matches!(self, Self::Acquired)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReconcileQueueStats {
    pub depth: u64,
    pub oldest_age_seconds: f64,
}

#[derive(Debug, Error)]
pub enum OutboxTransitionError {
    #[error("outbox record was not found")]
    NotFound,
    #[error("outbox transition was rejected (current status: {current:?}): {message}")]
    Rejected {
        current: NormalizationOutboxStatus,
        message: String,
    },
    #[error("outbox store failed: {message}")]
    StoreFailed { message: String },
}

impl OutboxTransitionError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::NotFound => "outbox record was not found",
            Self::Rejected { .. } => "outbox transition was rejected",
            Self::StoreFailed { .. } => "outbox store failed",
        }
    }
}

#[async_trait]
/// Port for durable normalization outbox persistence.
///
/// Implementations atomically acquire new records, preserve idempotency-key
/// payload matching, and use their own clock for leases and ordering.
pub trait NormalizationOutboxPort: Send + Sync {
    /// Atomically registers a record and acquires a delivery lease.
    ///
    /// A different fingerprint returns `PayloadMismatch`; matching `Sent`
    /// returns `AlreadySent`; matching non-terminal records return
    /// `AlreadyInFlight`; matching terminal records return `Rejected` so an
    /// operator must intervene. New records become `InFlight` with
    /// `claimed_until = now + lease`.
    async fn enqueue(
        &self,
        record: NormalizationOutboxRecord,
        lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError>;
    /// Returns the record only when it is currently `Sent`.
    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError>;
    /// Transitions an `InFlight` record to `Sent` and increments `attempts`
    /// exactly once. Every other source state returns `Rejected`.
    async fn mark_sent(
        &self,
        idempotency_key: &str,
        result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError>;
    /// Transitions an `InFlight` record to `FailedRetryable` and increments
    /// `attempts` exactly once. Every other source state returns `Rejected`.
    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError>;
    /// Transitions an `InFlight` record to `DeadLetter` and increments
    /// `attempts` exactly once. Every other source state returns `Rejected`.
    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError>;
    /// Transitions an `InFlight` record to `FailedTerminal` and increments
    /// `attempts` exactly once. Every other source state returns `Rejected`.
    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError>;
    /// Transitions an `InFlight` record to `ReconcileRequired` and increments
    /// `attempts` exactly once. Every other source state returns `Rejected`.
    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError>;
    /// Claims at most `limit` `Pending` or `FailedRetryable` records, plus
    /// expired-lease `InFlight` records, oldest `updated_at` first. Claims set
    /// a fresh lease and never increment `attempts`; only `mark_*` methods do.
    async fn claim_next_pending(
        &self,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError>;
}

#[async_trait]
pub trait NormalizationReconcileQueuePort: Send + Sync {
    async fn stats(&self) -> Result<ReconcileQueueStats, OutboxTransitionError>;
}

#[async_trait]
pub trait NormalizationAuditPort: Send + Sync {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError>;
}
