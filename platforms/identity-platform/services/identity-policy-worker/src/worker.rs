//! Worker-local delivery ports and leased outbox orchestration.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use identity_contracts::IdentityEventV1;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

/// Maximum persisted delivery attempt count.
pub const MAX_ATTEMPTS: i32 = 1_000;

/// A row returned after the repository has committed its lease transaction.
#[derive(Clone, Debug)]
pub struct LeasedOutboxEvent {
    /// Stable outbox event identifier.
    pub event_id: Uuid,
    /// Event type stored beside the payload.
    pub event_type: String,
    /// Versioned event payload.
    pub payload: Value,
    /// UTC domain occurrence timestamp.
    pub occurred_at: DateTime<Utc>,
    /// Number of previously recorded delivery failures.
    pub attempt_count: i32,
    /// Fencing token generated for this row lease.
    pub claim_token: Uuid,
}

/// A leased event whose payload and type match the published Identity v1 contract.
#[derive(Clone, Debug)]
pub struct ValidatedOutboxEvent {
    /// Stable receiver idempotency key.
    pub event_id: Uuid,
    /// Validated Identity v1 event type.
    pub event_type: String,
    /// Deserialized published payload.
    pub payload: IdentityEventV1,
    /// UTC domain occurrence timestamp.
    pub occurred_at: DateTime<Utc>,
    /// Fencing token required for the delivery state update.
    pub claim_token: Uuid,
}

impl TryFrom<LeasedOutboxEvent> for ValidatedOutboxEvent {
    type Error = ValidationError;

    fn try_from(row: LeasedOutboxEvent) -> Result<Self, Self::Error> {
        let payload: IdentityEventV1 =
            serde_json::from_value(row.payload).map_err(|_| ValidationError)?;
        if row.event_type != event_type(&payload) || !has_supported_schema_version(&payload) {
            return Err(ValidationError);
        }
        Ok(Self {
            event_id: row.event_id,
            event_type: row.event_type,
            payload,
            occurred_at: row.occurred_at,
            claim_token: row.claim_token,
        })
    }
}

/// Opaque invalid-payload failure that cannot expose event content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("identity event payload is invalid")]
pub struct ValidationError;

/// Parameters for one short lease-claim transaction.
#[derive(Clone, Debug)]
pub struct ClaimRequest {
    /// Unique owner written into leased rows.
    pub lease_owner: String,
    /// Fresh fencing token written into this row lease.
    pub claim_token: Uuid,
    /// Duration after which another worker may reclaim a row.
    pub lease_duration: Duration,
}

/// Persistence port owned by this deployable.
#[async_trait]
pub trait OutboxRepository: Send + Sync {
    /// Claims due rows and returns only after the lease transaction commits.
    async fn claim_due(
        &self,
        request: &ClaimRequest,
    ) -> Result<Option<LeasedOutboxEvent>, RepositoryError>;

    /// Records successful delivery and clears the owned lease.
    async fn mark_published(
        &self,
        event_id: Uuid,
        lease_owner: &str,
        claim_token: Uuid,
    ) -> Result<(), RepositoryError>;

    /// Records one failed attempt, its next delay, and clears the owned lease.
    async fn record_failure(
        &self,
        event_id: Uuid,
        lease_owner: &str,
        claim_token: Uuid,
        retry_after: Duration,
        error_code: &'static str,
    ) -> Result<(), RepositoryError>;
}

/// Network publication port owned by this deployable.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publishes one already validated Identity event.
    async fn publish(&self, event: &ValidatedOutboxEvent) -> Result<(), PublishError>;
}

/// Bounded repository failure categories safe for structured logs.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RepositoryError {
    /// A transaction could not be started.
    #[error("repository.begin_failed")]
    Begin,
    /// Due rows could not be claimed or decoded.
    #[error("repository.claim_failed")]
    Claim,
    /// A claim transaction could not be committed.
    #[error("repository.commit_failed")]
    Commit,
    /// Delivery state could not be updated.
    #[error("repository.update_failed")]
    Update,
    /// The row was no longer owned by this worker.
    #[error("repository.lease_lost")]
    LostLease,
}

/// Typed network publication failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PublishError {
    /// Request construction, timeout, or transport failed.
    #[error("publisher.request_failed")]
    Request,
    /// Receiver returned a non-success HTTP status.
    #[error("publisher.non_success")]
    NonSuccessStatus {
        /// Numeric HTTP response status.
        status: u16,
    },
}

impl PublishError {
    /// Returns the bounded non-secret code persisted for this failure.
    #[must_use]
    pub const fn error_code(self) -> &'static str {
        match self {
            Self::Request => "publisher.request_failed",
            Self::NonSuccessStatus { .. } => "publisher.non_success",
        }
    }
}

/// Delivery behavior for one worker process.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Unique lease owner for this process.
    pub worker_id: String,
    /// Maximum rows claimed by one tick.
    pub batch_size: usize,
    /// Lease lifetime for claimed rows.
    pub lease_duration: Duration,
    /// Delay after the first failed attempt.
    pub base_backoff: Duration,
    /// Maximum delay for any failed attempt.
    pub max_backoff: Duration,
}

/// Publishes leased outbox rows without executing Identity commands.
pub struct DeliveryWorker {
    repository: Arc<dyn OutboxRepository>,
    publisher: Arc<dyn EventPublisher>,
    options: WorkerOptions,
}

impl DeliveryWorker {
    /// Creates a delivery worker from deployable-local ports.
    #[must_use]
    pub fn new(
        repository: Arc<dyn OutboxRepository>,
        publisher: Arc<dyn EventPublisher>,
        options: WorkerOptions,
    ) -> Self {
        Self {
            repository,
            publisher,
            options,
        }
    }

    /// Claims and processes at most the configured number of independent row leases.
    ///
    /// # Errors
    /// Returns a bounded repository error when claim or state persistence fails.
    pub async fn tick(&self) -> Result<TickStats, WorkerError> {
        let mut stats = TickStats::default();
        for _ in 0..self.options.batch_size {
            let Some(row_stats) = self.process_next().await? else {
                break;
            };
            stats.add(row_stats);
        }
        Ok(stats)
    }

    pub(crate) const fn max_rows_per_cycle(&self) -> usize {
        self.options.batch_size
    }

    pub(crate) async fn process_next(&self) -> Result<Option<TickStats>, WorkerError> {
        let Some(row) = self
            .repository
            .claim_due(&ClaimRequest {
                lease_owner: self.options.worker_id.clone(),
                claim_token: Uuid::new_v4(),
                lease_duration: self.options.lease_duration,
            })
            .await?
        else {
            return Ok(None);
        };
        let mut stats = TickStats {
            claimed: 1,
            ..TickStats::default()
        };

        let event_id = row.event_id;
        let attempt_count = row.attempt_count;
        let claim_token = row.claim_token;
        if let Ok(event) = ValidatedOutboxEvent::try_from(row) {
            match self.publisher.publish(&event).await {
                Ok(()) => {
                    self.repository
                        .mark_published(event.event_id, &self.options.worker_id, event.claim_token)
                        .await?;
                    stats.published = 1;
                }
                Err(error) => {
                    self.record_failure(event_id, claim_token, attempt_count, error.error_code())
                        .await?;
                    stats.failed = 1;
                }
            }
        } else {
            self.record_failure(
                event_id,
                claim_token,
                attempt_count,
                "event.invalid_payload",
            )
            .await?;
            stats.failed = 1;
        }
        Ok(Some(stats))
    }

    async fn record_failure(
        &self,
        event_id: Uuid,
        claim_token: Uuid,
        attempt_count: i32,
        error_code: &'static str,
    ) -> Result<(), RepositoryError> {
        self.repository
            .record_failure(
                event_id,
                &self.options.worker_id,
                claim_token,
                retry_delay(
                    attempt_count,
                    self.options.base_backoff,
                    self.options.max_backoff,
                ),
                error_code,
            )
            .await
    }
}

/// Counts from one bounded worker tick.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TickStats {
    /// Rows leased by the claim transaction.
    pub claimed: u32,
    /// Rows delivered and marked published.
    pub published: u32,
    /// Rows whose failures were persisted for retry.
    pub failed: u32,
}

impl TickStats {
    pub(crate) const fn add(&mut self, other: Self) {
        self.claimed = self.claimed.saturating_add(other.claimed);
        self.published = self.published.saturating_add(other.published);
        self.failed = self.failed.saturating_add(other.failed);
    }
}

/// Bounded worker orchestration failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkerError {
    /// Persistence operation failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

impl WorkerError {
    /// Returns a bounded non-secret code for structured logs.
    #[must_use]
    pub const fn error_code(self) -> &'static str {
        match self {
            Self::Repository(error) => match error {
                RepositoryError::Begin => "repository.begin_failed",
                RepositoryError::Claim => "repository.claim_failed",
                RepositoryError::Commit => "repository.commit_failed",
                RepositoryError::Update => "repository.update_failed",
                RepositoryError::LostLease => "repository.lease_lost",
            },
        }
    }
}

fn retry_delay(attempt_count: i32, base: Duration, maximum: Duration) -> Duration {
    let bounded_attempt_count = attempt_count.clamp(0, MAX_ATTEMPTS);
    let exponent = u32::try_from(bounded_attempt_count)
        .unwrap_or(u32::MAX)
        .min(31);
    let factor = 1_u128.checked_shl(exponent).unwrap_or(u128::MAX);
    let millis = base
        .as_millis()
        .saturating_mul(factor)
        .min(maximum.as_millis());
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

const fn event_type(event: &IdentityEventV1) -> &'static str {
    match event {
        IdentityEventV1::StaffInvited(_) => "identity.staff.invited.v1",
        IdentityEventV1::StaffRoleAssigned(_) => "identity.staff.role_assigned.v1",
        IdentityEventV1::StaffSessionRevoked(_) => "identity.staff.session_revoked.v1",
    }
}

const fn has_supported_schema_version(event: &IdentityEventV1) -> bool {
    match event {
        IdentityEventV1::StaffInvited(payload) => payload.schema_version == 1,
        IdentityEventV1::StaffRoleAssigned(payload) => payload.schema_version == 1,
        IdentityEventV1::StaffSessionRevoked(payload) => payload.schema_version == 1,
    }
}
