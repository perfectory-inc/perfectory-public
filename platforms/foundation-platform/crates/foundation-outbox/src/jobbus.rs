//! Collection-job dispatch port (`JobBus`).
//!
//! `JobBus` is the dispatch half of the Collection Event Fabric (gongzzang ADR-0047, adopted by
//! foundation-platform ADR-0013). It is intentionally a **separate** trait from [`EventBroadcaster`]:
//! a broadcaster is publish-only (fan-out of `raw_written` notifications), whereas a collection
//! worker must *claim* work, then `ack`/`nack` it. The trait is transport-neutral so the backing
//! store can move JSONL ledger → Postgres → Kafka without changing callers — only an adapter is
//! swapped, never the domain code.
//!
//! Durable adapters are later implementations of this same trait. A Postgres bus's [`poll`] performs
//! a committed claiming `UPDATE … RETURNING` (using `FOR UPDATE SKIP LOCKED` only to stop two
//! pollers grabbing the same row) that transitions rows to in-flight and bumps `attempt`; [`ack`]
//! and [`nack`] are then separate statements matched by `(job_id, attempt)`, and exhausted or
//! poison failures dead-letter into the existing `catalog.outbox_quarantine` table. The lease does
//! **not** span the `ack` call — `poll` hands back no transaction handle. A JSONL-ledger bus maps
//! the same way (poll = planned-minus-succeeded; ack = append `job_succeeded`).
//!
//! This module ships the trait plus an in-memory reference implementation ([`InMemoryJobBus`]) that
//! is the executable specification of the publish → poll(lease) → ack/nack → DLQ contract.
//!
//! [`EventBroadcaster`]: crate::broadcaster::EventBroadcaster
//! [`poll`]: JobBus::poll
//! [`ack`]: JobBus::ack
//! [`nack`]: JobBus::nack

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::events::catalog_v1::CollectionRawWrittenV1;
use thiserror::Error;

/// Error returned by a [`JobBus`] adapter.
#[derive(Debug, Error)]
pub enum JobBusError {
    /// The backing store or transport failed (DB, file, broker, serialization).
    #[error("job bus backend error: {0}")]
    Backend(String),
    /// The operation conflicted with the current lease state: acking/nacking a job that is not
    /// currently leased, whose lease attempt has been superseded by a redelivery, or publishing a
    /// `job_id` that already exists.
    #[error("job bus lease conflict: {0}")]
    Conflict(String),
}

/// A unit of collection work to dispatch.
///
/// Carries the stable dispatch identity plus an opaque, provider-specific [`spec`](Self::spec)
/// (the original ledger row). The identity fields are what a durable adapter indexes as columns;
/// `spec` is what the worker interprets to actually call the provider and write Bronze.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionJob {
    /// Job identity / primary key. A durable adapter enforces uniqueness on this column.
    pub job_id: String,
    /// Logical collection-scope identity (e.g. `scope:legal-dong:1111010100`). Partition key.
    pub scope_unit_id: String,
    /// Shard the job belongs to (e.g. `national-shard-0001`).
    pub shard_id: String,
    /// Data provider, for example `data.go.kr`.
    pub provider: String,
    /// Provider endpoint to collect (operation name).
    pub endpoint: String,
    /// Endpoint slug (stable routing identity, e.g. `data-go-kr-building-register-getBrTitleInfo`).
    pub endpoint_slug: String,
    /// Per-job idempotency key carried on the ledger row.
    pub idempotency_key: String,
    /// Request fingerprint (lowercase hex SHA-256) — the collection dedup/reuse key.
    pub request_fingerprint_sha256: String,
    /// Schema version of the request fingerprint algorithm
    /// (e.g. `foundation-platform.bronze_request_fingerprint.v1`).
    pub request_fingerprint_schema_version: String,
    /// Collection snapshot/run id this job belongs to.
    pub collection_snapshot_id: String,
    /// Provider-specific job specification — the ADR-0047 `provider_request` projection of the
    /// original ledger row (e.g. `sigungu_cd`, `bjdong_cd`, `page_start`, `page_end`, `num_of_rows`).
    /// The executor owns deserialization and must fail the job with a clear error (never panic) on a
    /// malformed `spec`; the bus treats it as opaque.
    pub spec: serde_json::Value,
}

/// A claim token returned by [`JobBus::poll`], identifying a leased job and its attempt number.
///
/// `attempt` is part of the token: a durable adapter must reject an `ack`/`nack` whose `attempt` no
/// longer matches the job's current in-flight attempt (a superseded/expired lease).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobLease {
    /// Job that was leased.
    pub job_id: String,
    /// 1-based number of times this job has been leased (1 on first delivery).
    pub attempt: u32,
}

/// A job handed to a consumer for execution, together with its lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeasedJob {
    /// Claim token to `ack`/`nack` the job with.
    pub lease: JobLease,
    /// The job payload to execute.
    pub job: CollectionJob,
}

/// The Bronze-write result a consumer reports when it `ack`s a leased job.
///
/// Combined with the leased [`CollectionJob`]'s identity, this is exactly the payload of a
/// `collection.raw_written` claim-check notification. The integrity digest
/// (`bronze_checksum_sha256`) is the producer-computed sha256 and MUST be non-empty (ADR-0047 OQ-5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionSuccess {
    /// R2 Bronze object key — the Claim-Check pointer (last key when `bronze_object_count > 1`).
    pub bronze_object_key: String,
    /// Number of Bronze objects (pages/parts) this write produced (`1` for a single-object write).
    pub bronze_object_count: u64,
    /// Lowercase hex SHA-256 of the raw Bronze bytes (producer-computed; never trust `ETag`).
    pub bronze_checksum_sha256: String,
    /// Size in bytes of the raw Bronze object.
    pub bronze_size_bytes: u64,
    /// Logical source record count contained in the written object(s).
    pub source_record_count: u64,
    /// Number of provider requests consumed to produce this write.
    pub request_count: u64,
    /// Whether the object was satisfied by reuse of an already-collected Bronze object.
    pub reused_bronze_object: bool,
    /// Data license/usage terms for the collected source, or `None` until a license is sourced
    /// (no provider records a license today — honestly `None`, never fabricated).
    pub license: Option<String>,
    /// EPSG code for spatial sources (e.g. `EPSG:4326`), or `None` for attribute-only sources.
    pub srid: Option<String>,
    /// UTC timestamp when the upstream provider data was fetched.
    pub fetched_at_utc: DateTime<Utc>,
}

impl CollectionSuccess {
    /// Build the `collection.raw_written` payload from the leased job's identity plus this result.
    ///
    /// `occurred_at` is the event emit time (distinct from `fetched_at_utc`).
    #[must_use]
    pub fn into_raw_written(
        &self,
        job: &CollectionJob,
        occurred_at: DateTime<Utc>,
    ) -> CollectionRawWrittenV1 {
        CollectionRawWrittenV1 {
            schema_version: 1,
            collection_snapshot_id: job.collection_snapshot_id.clone(),
            job_id: job.job_id.clone(),
            scope_unit_id: job.scope_unit_id.clone(),
            provider: job.provider.clone(),
            endpoint: job.endpoint.clone(),
            endpoint_slug: job.endpoint_slug.clone(),
            bronze_object_key: self.bronze_object_key.clone(),
            bronze_object_count: self.bronze_object_count,
            bronze_checksum_sha256: self.bronze_checksum_sha256.clone(),
            bronze_size_bytes: self.bronze_size_bytes,
            source_record_count: self.source_record_count,
            request_count: self.request_count,
            request_fingerprint_sha256: job.request_fingerprint_sha256.clone(),
            request_fingerprint_schema_version: job.request_fingerprint_schema_version.clone(),
            license: self.license.clone(),
            srid: self.srid.clone(),
            reused_bronze_object: self.reused_bronze_object,
            fetched_at_utc: self.fetched_at_utc,
            occurred_at,
        }
    }
}

/// How the caller classifies a job failure, which governs retry vs. immediate dead-letter.
///
/// Per gongzzang ADR-0047, poison failures (e.g. HTTP 400/401/403, schema rejection, invalid auth
/// key) must go **straight to the DLQ** without burning further provider quota; only transient
/// failures are retried up to the bus's attempt budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureDisposition {
    /// Transient failure (timeout, 5xx, throttle); eligible for retry up to the attempt budget.
    Retryable,
    /// Permanent failure (bad request, auth, schema); dead-letter immediately, no further attempts.
    Poison,
}

/// Failure detail recorded when a leased job is `nack`ed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobFailure {
    /// Whether the failure is transient (retryable) or permanent (poison → immediate DLQ).
    pub disposition: FailureDisposition,
    /// Stable failure code. Durable adapters that dead-letter into `catalog.outbox_quarantine`
    /// must keep this matching `^[a-z0-9][a-z0-9._:-]{1,127}$`.
    pub code: String,
    /// Human-readable failure message (must not contain secrets or raw payload bytes).
    pub message: String,
}

/// What a [`JobBus::nack`] did with the failed job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NackOutcome {
    /// The job was returned to the queue for another attempt.
    Retried,
    /// The failure was poison, or the retry budget was exhausted; the job was routed to the DLQ.
    DeadLettered,
}

/// Producer-side sink a [`JobBus`] impl calls to emit a `collection.raw_written` claim-check event.
///
/// This is the **producer** seam (collection write → fabric), distinct from the consumer-fan-out
/// [`EventBroadcaster`]. In production a sink writes the event to `catalog.outbox_event` (so the
/// existing outbox worker fans it out); for tests and local dev use [`RecordingRawWrittenSink`].
///
/// [`EventBroadcaster`]: crate::broadcaster::EventBroadcaster
#[async_trait]
pub trait RawWrittenSink: Send + Sync {
    /// Emit one `collection.raw_written` event.
    ///
    /// # Errors
    /// Returns [`JobBusError::Backend`] if the event cannot be durably handed off.
    async fn emit(&self, event: &CollectionRawWrittenV1) -> Result<(), JobBusError>;
}

/// In-memory [`RawWrittenSink`] that records emitted events for tests and local development.
#[derive(Debug, Default)]
pub struct RecordingRawWrittenSink {
    events: Mutex<Vec<CollectionRawWrittenV1>>,
}

impl RecordingRawWrittenSink {
    /// Create an empty recording sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of the events emitted so far.
    #[must_use]
    pub fn emitted(&self) -> Vec<CollectionRawWrittenV1> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Number of events emitted so far.
    #[must_use]
    pub fn count(&self) -> usize {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[async_trait]
impl RawWrittenSink for RecordingRawWrittenSink {
    async fn emit(&self, event: &CollectionRawWrittenV1) -> Result<(), JobBusError> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
        Ok(())
    }
}

/// Dispatch port for collection jobs: publish, claim (lease), and ack/nack.
///
/// Implementations must give at-least-once delivery with leasing — a job claimed by [`poll`] must
/// not be handed to another consumer until it is `nack`ed (and re-queued) or its lease otherwise
/// expires. `ack`/`nack` are keyed by the [`JobLease`] from [`poll`], **including its `attempt`**:
/// an `ack`/`nack` for a job that is not currently leased, or whose `attempt` has been superseded
/// by a redelivery, must return [`JobBusError::Conflict`].
///
/// [`poll`]: JobBus::poll
#[async_trait]
pub trait JobBus: Send + Sync {
    /// Enqueue a collection job for dispatch.
    ///
    /// # Errors
    /// Returns [`JobBusError::Conflict`] if a job with the same `job_id` already exists, or
    /// [`JobBusError::Backend`] if the job cannot be durably enqueued.
    async fn publish(&self, job: CollectionJob) -> Result<(), JobBusError>;

    /// Claim up to `max` pending jobs, leasing them to this consumer.
    ///
    /// # Errors
    /// Returns [`JobBusError::Backend`] if the backing store cannot be queried.
    async fn poll(&self, max: usize) -> Result<Vec<LeasedJob>, JobBusError>;

    /// Mark a leased job as successfully completed, reporting the Bronze-write result.
    ///
    /// The `success` result, combined with the leased job's identity, is what a durable adapter
    /// records and turns into a `collection.raw_written` claim-check event (via a
    /// [`RawWrittenSink`]).
    ///
    /// # Errors
    /// Returns [`JobBusError::Conflict`] if the lease is not currently held or its `attempt` has
    /// been superseded, or [`JobBusError::Backend`] on a store failure.
    async fn ack(&self, lease: &JobLease, success: &CollectionSuccess) -> Result<(), JobBusError>;

    /// Mark a leased job as failed. The bus dead-letters immediately on a
    /// [`FailureDisposition::Poison`] failure, and otherwise retries a
    /// [`FailureDisposition::Retryable`] failure until the attempt budget is exhausted, reporting
    /// the decision via [`NackOutcome`].
    ///
    /// # Errors
    /// Returns [`JobBusError::Conflict`] if the lease is not currently held or its `attempt` has
    /// been superseded, or [`JobBusError::Backend`] on a store failure.
    async fn nack(
        &self,
        lease: &JobLease,
        failure: &JobFailure,
    ) -> Result<NackOutcome, JobBusError>;
}

/// In-memory reference [`JobBus`] for tests and local development.
///
/// Models the full publish → poll(lease) → ack/nack → DLQ contract without any external store,
/// including `job_id` uniqueness and `attempt` lease fencing, so durable adapters inherit those
/// invariants as an executable specification.
#[derive(Debug)]
pub struct InMemoryJobBus {
    inner: Mutex<JobBusState>,
    max_attempts: u32,
}

#[derive(Debug, Default)]
struct JobBusState {
    pending: VecDeque<PendingJob>,
    in_flight: HashMap<String, PendingJob>,
    completed: Vec<String>,
    dead_letters: Vec<(CollectionJob, JobFailure)>,
}

impl JobBusState {
    fn knows_job(&self, job_id: &str) -> bool {
        self.in_flight.contains_key(job_id)
            || self.completed.iter().any(|id| id == job_id)
            || self.pending.iter().any(|p| p.job.job_id == job_id)
            || self
                .dead_letters
                .iter()
                .any(|(job, _)| job.job_id == job_id)
    }
}

#[derive(Debug, Clone)]
struct PendingJob {
    job: CollectionJob,
    /// Attempts already made (0 before the first lease).
    attempts: u32,
}

impl InMemoryJobBus {
    /// Create an empty bus that dead-letters a job after `max_attempts` failed deliveries.
    ///
    /// `max_attempts` must be at least 1.
    #[must_use]
    pub fn new(max_attempts: u32) -> Self {
        Self {
            inner: Mutex::new(JobBusState::default()),
            max_attempts: max_attempts.max(1),
        }
    }

    /// Number of jobs that have been `ack`ed.
    #[must_use]
    pub fn completed_count(&self) -> usize {
        self.lock().completed.len()
    }

    /// Number of jobs that were dead-lettered (poison or retry budget exhausted).
    #[must_use]
    pub fn dead_letter_count(&self) -> usize {
        self.lock().dead_letters.len()
    }

    /// Number of jobs currently waiting to be claimed.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.lock().pending.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, JobBusState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl JobBus for InMemoryJobBus {
    async fn publish(&self, job: CollectionJob) -> Result<(), JobBusError> {
        let job_id = job.job_id.clone();
        let mut state = self.lock();
        let duplicate = state.knows_job(&job_id);
        if !duplicate {
            state.pending.push_back(PendingJob { job, attempts: 0 });
        }
        drop(state);
        if duplicate {
            Err(JobBusError::Conflict(format!(
                "publish for duplicate job_id: {job_id}"
            )))
        } else {
            Ok(())
        }
    }

    async fn poll(&self, max: usize) -> Result<Vec<LeasedJob>, JobBusError> {
        let mut state = self.lock();
        let mut leased = Vec::new();
        while leased.len() < max {
            let Some(mut pending) = state.pending.pop_front() else {
                break;
            };
            pending.attempts = pending.attempts.saturating_add(1);
            let lease = JobLease {
                job_id: pending.job.job_id.clone(),
                attempt: pending.attempts,
            };
            let job = pending.job.clone();
            state.in_flight.insert(pending.job.job_id.clone(), pending);
            leased.push(LeasedJob { lease, job });
        }
        drop(state);
        Ok(leased)
    }

    // `_success` is intentionally discarded: this in-memory reference models lease mechanics only.
    // The success-carrying half of the contract (turning `CollectionSuccess` into a
    // `collection.raw_written` event via a `RawWrittenSink`) is exercised by `LedgerJobBus`.
    async fn ack(&self, lease: &JobLease, _success: &CollectionSuccess) -> Result<(), JobBusError> {
        let mut state = self.lock();
        let current_attempt = state
            .in_flight
            .get(&lease.job_id)
            .map(|pending| pending.attempts);
        let result = if current_attempt == Some(lease.attempt) {
            state.in_flight.remove(&lease.job_id);
            state.completed.push(lease.job_id.clone());
            Ok(())
        } else {
            Err(JobBusError::Conflict(format!(
                "ack for job not leased at attempt {}: {}",
                lease.attempt, lease.job_id
            )))
        };
        drop(state);
        result
    }

    async fn nack(
        &self,
        lease: &JobLease,
        failure: &JobFailure,
    ) -> Result<NackOutcome, JobBusError> {
        let mut state = self.lock();
        let current_attempt = state
            .in_flight
            .get(&lease.job_id)
            .map(|pending| pending.attempts);
        let result = if current_attempt == Some(lease.attempt) {
            match state.in_flight.remove(&lease.job_id) {
                Some(pending) => {
                    let dead = failure.disposition == FailureDisposition::Poison
                        || pending.attempts >= self.max_attempts;
                    if dead {
                        state.dead_letters.push((pending.job, failure.clone()));
                        Ok(NackOutcome::DeadLettered)
                    } else {
                        state.pending.push_back(pending);
                        Ok(NackOutcome::Retried)
                    }
                }
                None => Err(JobBusError::Backend(format!(
                    "nack lost in-flight entry for {}",
                    lease.job_id
                ))),
            }
        } else {
            Err(JobBusError::Conflict(format!(
                "nack for job not leased at attempt {}: {}",
                lease.attempt, lease.job_id
            )))
        };
        drop(state);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CollectionJob, CollectionSuccess, FailureDisposition, InMemoryJobBus, JobBus, JobFailure,
        JobLease, LeasedJob, NackOutcome, RawWrittenSink, RecordingRawWrittenSink,
    };
    use chrono::{DateTime, Utc};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn fixed_time() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap_or_default()
    }

    fn job(id: &str) -> CollectionJob {
        CollectionJob {
            job_id: id.to_owned(),
            scope_unit_id: "scope:legal-dong:1111010100".to_owned(),
            shard_id: "national-shard-0001".to_owned(),
            provider: "data.go.kr".to_owned(),
            endpoint: "getBrTitleInfo".to_owned(),
            endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".to_owned(),
            idempotency_key: format!("idem-{id}"),
            request_fingerprint_sha256:
                "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".to_owned(),
            request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
                .to_owned(),
            collection_snapshot_id: "registry:test".to_owned(),
            spec: serde_json::json!({ "page_start": 1, "page_end": 1 }),
        }
    }

    fn success() -> CollectionSuccess {
        CollectionSuccess {
            bronze_object_key: "bronze/source=x/page=0001/part-0001.json".to_owned(),
            bronze_object_count: 1,
            bronze_checksum_sha256:
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            bronze_size_bytes: 4_096,
            source_record_count: 42,
            request_count: 1,
            reused_bronze_object: false,
            license: None,
            srid: None,
            fetched_at_utc: fixed_time(),
        }
    }

    fn transient() -> JobFailure {
        JobFailure {
            disposition: FailureDisposition::Retryable,
            code: "provider.error".to_owned(),
            message: "provider returned 500".to_owned(),
        }
    }

    fn poison() -> JobFailure {
        JobFailure {
            disposition: FailureDisposition::Poison,
            code: "provider.bad_request".to_owned(),
            message: "provider returned 400".to_owned(),
        }
    }

    fn first_lease(leased: Vec<LeasedJob>) -> Result<JobLease, Box<dyn std::error::Error>> {
        Ok(leased
            .into_iter()
            .next()
            .ok_or("expected at least one leased job")?
            .lease)
    }

    #[tokio::test]
    async fn publish_poll_ack_lifecycle() -> TestResult {
        let bus = InMemoryJobBus::new(3);
        bus.publish(job("a")).await?;
        bus.publish(job("b")).await?;

        let leased = bus.poll(10).await?;
        assert_eq!(leased.len(), 2);
        for l in &leased {
            assert_eq!(l.lease.attempt, 1);
            bus.ack(&l.lease, &success()).await?;
        }

        assert_eq!(bus.completed_count(), 2);
        assert!(bus.poll(10).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn publish_rejects_duplicate_job_id() -> TestResult {
        let bus = InMemoryJobBus::new(3);
        bus.publish(job("a")).await?;
        assert!(bus.publish(job("a")).await.is_err());
        // Still only one copy is queued.
        assert_eq!(bus.pending_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn poll_respects_max_and_leases_exclusively() -> TestResult {
        let bus = InMemoryJobBus::new(3);
        for id in ["a", "b", "c"] {
            bus.publish(job(id)).await?;
        }

        let first = bus.poll(2).await?;
        assert_eq!(first.len(), 2);
        // Leased jobs are not redelivered before ack/nack (SKIP LOCKED-style lease).
        let second = bus.poll(2).await?;
        assert_eq!(second.len(), 1);
        assert!(bus.poll(2).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn transient_nack_retries_then_dead_letters() -> TestResult {
        let bus = InMemoryJobBus::new(2);
        bus.publish(job("a")).await?;

        let lease1 = first_lease(bus.poll(1).await?)?;
        assert_eq!(lease1.attempt, 1);
        assert_eq!(bus.nack(&lease1, &transient()).await?, NackOutcome::Retried);

        let lease2 = first_lease(bus.poll(1).await?)?;
        assert_eq!(lease2.attempt, 2);
        assert_eq!(
            bus.nack(&lease2, &transient()).await?,
            NackOutcome::DeadLettered
        );

        assert_eq!(bus.dead_letter_count(), 1);
        assert_eq!(bus.completed_count(), 0);
        assert!(bus.poll(1).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn poison_nack_dead_letters_immediately() -> TestResult {
        // Generous attempt budget, but poison must skip the retry ladder entirely.
        let bus = InMemoryJobBus::new(5);
        bus.publish(job("a")).await?;

        let lease = first_lease(bus.poll(1).await?)?;
        assert_eq!(lease.attempt, 1);
        assert_eq!(
            bus.nack(&lease, &poison()).await?,
            NackOutcome::DeadLettered
        );

        assert_eq!(bus.dead_letter_count(), 1);
        // No re-delivery: poison did not return the job to pending.
        assert!(bus.poll(1).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn retried_job_can_be_acked_on_later_attempt() -> TestResult {
        let bus = InMemoryJobBus::new(3);
        bus.publish(job("a")).await?;

        let lease1 = first_lease(bus.poll(1).await?)?;
        assert_eq!(bus.nack(&lease1, &transient()).await?, NackOutcome::Retried);

        let lease2 = first_lease(bus.poll(1).await?)?;
        assert_eq!(lease2.attempt, 2);
        bus.ack(&lease2, &success()).await?;
        assert_eq!(bus.completed_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn stale_lease_ack_conflicts_after_redelivery() -> TestResult {
        let bus = InMemoryJobBus::new(3);
        bus.publish(job("a")).await?;

        let lease1 = first_lease(bus.poll(1).await?)?;
        bus.nack(&lease1, &transient()).await?; // back to pending, attempts=1
        let _lease2 = first_lease(bus.poll(1).await?)?; // re-leased at attempt 2

        // The stale attempt-1 token must not ack the live attempt-2 in-flight job.
        assert!(bus.ack(&lease1, &success()).await.is_err());
        assert!(bus.nack(&lease1, &transient()).await.is_err());
        assert_eq!(bus.completed_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn nack_redelivers_fifo_behind_other_pending() -> TestResult {
        let bus = InMemoryJobBus::new(3);
        bus.publish(job("a")).await?;
        bus.publish(job("b")).await?;

        let lease_a = first_lease(bus.poll(1).await?)?;
        assert_eq!(lease_a.job_id, "a");
        bus.nack(&lease_a, &transient()).await?; // 'a' goes to the back of the queue

        // Next poll yields 'b' (FIFO), not the just-nacked 'a' (no head-of-line monopoly).
        let next = first_lease(bus.poll(1).await?)?;
        assert_eq!(next.job_id, "b");
        Ok(())
    }

    #[tokio::test]
    async fn ack_or_nack_without_lease_conflicts() -> TestResult {
        let bus = InMemoryJobBus::new(2);
        bus.publish(job("a")).await?;
        let lease = first_lease(bus.poll(1).await?)?;
        bus.ack(&lease, &success()).await?;
        // Second ack on the same lease is a conflict — the job is no longer leased.
        assert!(bus.ack(&lease, &success()).await.is_err());
        assert!(bus.nack(&lease, &transient()).await.is_err());
        Ok(())
    }

    #[test]
    fn collection_success_into_raw_written_maps_identity_and_artifact() {
        let raw = success().into_raw_written(&job("job-x"), fixed_time());

        // Identity comes from the job; artifact + lineage come from the success.
        assert_eq!(raw.job_id, "job-x");
        assert_eq!(raw.scope_unit_id, "scope:legal-dong:1111010100");
        assert_eq!(raw.provider, "data.go.kr");
        assert_eq!(
            raw.endpoint_slug,
            "data-go-kr-building-register-getBrTitleInfo"
        );
        assert_eq!(
            raw.request_fingerprint_schema_version,
            "foundation-platform.bronze_request_fingerprint.v1"
        );
        assert_eq!(raw.bronze_object_count, 1);
        assert_eq!(raw.source_record_count, 42);
        assert!(!raw.bronze_checksum_sha256.is_empty());
        assert_eq!(raw.fetched_at_utc, fixed_time());
        assert_eq!(raw.occurred_at, fixed_time());
    }

    #[tokio::test]
    async fn recording_raw_written_sink_records_events() -> TestResult {
        let sink = RecordingRawWrittenSink::new();
        let raw = success().into_raw_written(&job("job-a"), fixed_time());
        sink.emit(&raw).await?;
        sink.emit(&raw).await?;

        assert_eq!(sink.count(), 2);
        let emitted = sink.emitted();
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].job_id, "job-a");
        Ok(())
    }
}
