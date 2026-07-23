//! `JobBus` implementation backed by the existing JSONL collection ledger (Slice 3-A, option A).
//!
//! This wraps the file-based ledger primitives ([`LedgerEntry`], [`EventWriter`],
//! [`succeeded_event`]/[`failed_event`]) behind the transport-neutral
//! [`JobBus`](foundation_outbox::JobBus) trait, and on a successful `ack` emits a
//! `collection.raw_written` claim-check event through an injected
//! [`RawWrittenSink`](foundation_outbox::RawWrittenSink).
//!
//! Status: this is the ledger-backed dispatch trait impl, kept for a future FULL migration where it
//! replaces the executor's `select_pending_jobs` + dispatch loop (toward `PostgresJobBus`, option B).
//! It is **not** the path that 3-B used to ship operational `raw_written`: 3-B connected the producer
//! seam directly in the async executor via `OutboxRawWrittenSink` (see `national_data_collection_async`),
//! WITHOUT wiring this bus. So `LedgerJobBus` is still unwired (`#![allow(dead_code)]`, constructed
//! only by tests). `from_planned` already enforces the OQ-2 request-cap quota gate (via the shared
//! `select_pending_jobs`) so wiring it later cannot lose that gate. Postgres outbox / DB-backed
//! quarantine remain option B.
#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use super::events::{failed_event, succeeded_event, EventWriter};
use super::ledger::LedgerEntry;
use super::JobSuccessReport;
use async_trait::async_trait;
use chrono::Utc;
use foundation_outbox::{
    CollectionJob, CollectionSuccess, FailureDisposition, JobBus, JobBusError, JobFailure,
    JobLease, LeasedJob, NackOutcome, RawWrittenSink,
};

/// A `JobBus` seeded from a planned JSONL collection ledger.
pub(crate) struct LedgerJobBus {
    state: Mutex<LedgerBusState>,
    event_writer: EventWriter,
    compiler_input_hash: String,
    sink: Arc<dyn RawWrittenSink>,
    max_attempts: u32,
}

#[derive(Default)]
struct LedgerBusState {
    pending: VecDeque<PendingEntry>,
    in_flight: HashMap<String, PendingEntry>,
}

#[derive(Clone)]
struct PendingEntry {
    entry: LedgerEntry,
    attempts: u32,
}

impl LedgerJobBus {
    /// Seed a bus from planned ledger entries via the shared selection + quota gate.
    ///
    /// **Quota gate (OQ-2):** selection is delegated to the single SSOT `select_pending_jobs`
    /// (`status == "planned"`, skip already-succeeded, accumulate `request_count_estimate` under
    /// `request_cap`, cap at `max_jobs`). This is the *same* gate the async executor uses, so wiring
    /// this bus into a command cannot silently lose the request-cap enforcement. (ADR-0047 OQ-2 keeps
    /// the gate in `select_pending_jobs` pre-Kafka; moving it consumer-side is a Kafka-cutover task.)
    ///
    /// `job_id` is assumed unique across `entries` (the planner's primary key); the Postgres
    /// `collection_job` PK enforces this for free in option B.
    ///
    /// # Errors
    /// Returns the `select_pending_jobs` error if there are no pending jobs or the first pending job
    /// alone exceeds `request_cap`.
    pub(crate) fn from_planned(
        entries: Vec<LedgerEntry>,
        succeeded_job_ids: &BTreeSet<String>,
        max_jobs: usize,
        request_cap: u64,
        event_writer: EventWriter,
        compiler_input_hash: String,
        sink: Arc<dyn RawWrittenSink>,
        max_attempts: u32,
    ) -> anyhow::Result<Self> {
        let pending =
            super::ledger::select_pending_jobs(&entries, succeeded_job_ids, max_jobs, request_cap)?
                .into_iter()
                .map(|entry| PendingEntry { entry, attempts: 0 })
                .collect();
        Ok(Self {
            state: Mutex::new(LedgerBusState {
                pending,
                in_flight: HashMap::new(),
            }),
            event_writer,
            compiler_input_hash,
            sink,
            max_attempts: max_attempts.max(1),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LedgerBusState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

// NOTE: the JSONL `succeeded_event` hardcodes `reused_bronze_object = false` (events.rs), so the
// ledger projection does not yet carry reuse status even when `success.reused_bronze_object` is true.
// The authoritative `collection.raw_written` DOES carry the correct value (via `into_raw_written`);
// propagating reuse status into the JSONL ledger event is deferred (tracked with Slice 2d).
fn success_report(success: &CollectionSuccess) -> JobSuccessReport {
    JobSuccessReport {
        provider_request_count: success.request_count,
        source_record_count: success.source_record_count,
        bronze_size_bytes: success.bronze_size_bytes,
        last_object_key: success.bronze_object_key.clone(),
        last_checksum_sha256: success.bronze_checksum_sha256.clone(),
    }
}

#[async_trait]
impl JobBus for LedgerJobBus {
    /// Not supported: a ledger-backed bus is seeded from the plan ([`from_planned`]); jobs are not
    /// published at runtime.
    ///
    /// [`from_planned`]: LedgerJobBus::from_planned
    async fn publish(&self, _job: CollectionJob) -> Result<(), JobBusError> {
        Err(JobBusError::Backend(
            "LedgerJobBus is seeded from the plan ledger; runtime publish is not supported"
                .to_owned(),
        ))
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
                job_id: pending.entry.job_id.clone(),
                attempt: pending.attempts,
            };
            let job = super::to_collection_job(&pending.entry);
            state
                .in_flight
                .insert(pending.entry.job_id.clone(), pending);
            leased.push(LeasedJob { lease, job });
        }
        drop(state);
        Ok(leased)
    }

    async fn ack(&self, lease: &JobLease, success: &CollectionSuccess) -> Result<(), JobBusError> {
        // Claim check under the lock; release it before any I/O or await.
        let entry: LedgerEntry = {
            let mut state = self.lock();
            let current_attempt = state.in_flight.get(&lease.job_id).map(|p| p.attempts);
            if current_attempt == Some(lease.attempt) {
                match state.in_flight.remove(&lease.job_id) {
                    Some(pending) => Ok(pending.entry),
                    None => Err(JobBusError::Backend(format!(
                        "ack lost in-flight entry for {}",
                        lease.job_id
                    ))),
                }
            } else {
                Err(JobBusError::Conflict(format!(
                    "ack for job not leased at attempt {}: {}",
                    lease.attempt, lease.job_id
                )))
            }
        }?;

        // Ledger is SSOT: record the success event first, then derive the raw_written notification.
        let report = success_report(success);
        self.event_writer
            .write_event(&succeeded_event(&entry, &self.compiler_input_hash, &report))
            .map_err(|error| JobBusError::Backend(error.to_string()))?;

        // The job_succeeded ledger event above is now durable and authoritative. If sink.emit fails,
        // the job is already out of in_flight: recovery is the downstream re-derive from the event
        // log (ADR-0047 at-least-once), NOT an ack retry. True producer-side atomicity (raw_written
        // written to catalog.outbox_event in one unit with the success record) is Slice 3-B.
        let job = super::to_collection_job(&entry);
        let raw_written = success.into_raw_written(&job, Utc::now());
        self.sink.emit(&raw_written).await?;
        Ok(())
    }

    async fn nack(
        &self,
        lease: &JobLease,
        failure: &JobFailure,
    ) -> Result<NackOutcome, JobBusError> {
        // Decide outcome under the lock; only a terminal failure carries the entry out for a
        // job_failed event. (Postgres quarantine reuse is Slice 3-B.)
        let (outcome, terminal_entry): (NackOutcome, Option<LedgerEntry>) = {
            let mut state = self.lock();
            let current_attempt = state.in_flight.get(&lease.job_id).map(|p| p.attempts);
            if current_attempt == Some(lease.attempt) {
                match state.in_flight.remove(&lease.job_id) {
                    Some(pending) => {
                        let dead = failure.disposition == FailureDisposition::Poison
                            || pending.attempts >= self.max_attempts;
                        if dead {
                            Ok((NackOutcome::DeadLettered, Some(pending.entry)))
                        } else {
                            state.pending.push_back(pending);
                            Ok((NackOutcome::Retried, None))
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
            }
        }?;

        if let Some(entry) = terminal_entry {
            self.event_writer
                .write_event(&failed_event(
                    &entry,
                    &self.compiler_input_hash,
                    failure.message.clone(),
                ))
                .map_err(|error| JobBusError::Backend(error.to_string()))?;
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use foundation_outbox::{
        CollectionSuccess, FailureDisposition, JobBus, JobFailure, NackOutcome,
        RecordingRawWrittenSink,
    };

    use super::super::events::EventWriter;
    use super::super::ledger::LedgerEntry;
    use super::LedgerJobBus;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn fixed_time() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap_or_default()
    }

    fn ledger_entry(job_id: &str) -> LedgerEntry {
        LedgerEntry {
            job_id: job_id.to_owned(),
            provider: "data.go.kr".to_owned(),
            endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".to_owned(),
            endpoint: "getBrTitleInfo".to_owned(),
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11110".to_owned(),
            bjdong_cd: "10100".to_owned(),
            lawd_cd: String::new(),
            deal_ymd: String::new(),
            scope_unit_id: "scope:legal-dong:1111010100".to_owned(),
            shard_id: "national-shard-0001".to_owned(),
            idempotency_key: format!("test/{job_id}"),
            source_slug: format!("source-{job_id}"),
            request_fingerprint_sha256: "a".repeat(64),
            request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
                .to_owned(),
            collection_snapshot_id: "registry:test".to_owned(),
            status: "planned".to_owned(),
            page_start: Some(1),
            page_end: Some(1),
            max_pages: 1,
            num_of_rows: 100,
            request_count_estimate: 1,
        }
    }

    fn success() -> CollectionSuccess {
        CollectionSuccess {
            bronze_object_key: "bronze/source=x/page-000001.json".to_owned(),
            bronze_object_count: 1,
            bronze_checksum_sha256: "b".repeat(64),
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

    fn temp_event_writer() -> Result<(EventWriter, std::path::PathBuf), Box<dyn std::error::Error>>
    {
        let path = std::env::temp_dir().join(format!(
            "foundation-platform-ledger-job-bus-{}.jsonl",
            uuid::Uuid::new_v4()
        ));
        let writer = EventWriter::open(&path)?;
        Ok((writer, path))
    }

    fn bus_with(
        entries: Vec<LedgerEntry>,
        succeeded: BTreeSet<String>,
        sink: Arc<RecordingRawWrittenSink>,
        max_attempts: u32,
    ) -> Result<(LedgerJobBus, std::path::PathBuf), Box<dyn std::error::Error>> {
        let (writer, path) = temp_event_writer()?;
        let bus = LedgerJobBus::from_planned(
            entries,
            &succeeded,
            100, // max_jobs
            100, // request_cap (each test entry estimates 1 request)
            writer,
            "compiler-input-hash".to_owned(),
            sink,
            max_attempts,
        )?;
        Ok((bus, path))
    }

    #[tokio::test]
    async fn poll_skips_already_succeeded_then_ack_emits_raw_written() -> TestResult {
        let sink = Arc::new(RecordingRawWrittenSink::new());
        let succeeded = BTreeSet::from(["job-a".to_owned()]);
        let (bus, log_path) = bus_with(
            vec![ledger_entry("job-a"), ledger_entry("job-b")],
            succeeded,
            Arc::clone(&sink),
            3,
        )?;

        // job-a already succeeded → only job-b is leasable.
        let leased = bus.poll(10).await?;
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].job.job_id, "job-b");
        assert_eq!(leased[0].lease.attempt, 1);

        bus.ack(&leased[0].lease, &success()).await?;

        // raw_written reached the sink with real identity + non-empty checksum.
        assert_eq!(sink.count(), 1);
        let emitted = sink.emitted();
        assert_eq!(emitted[0].job_id, "job-b");
        assert_eq!(emitted[0].bronze_checksum_sha256, "b".repeat(64));
        assert!(!emitted[0].bronze_checksum_sha256.is_empty());

        // The ledger event log recorded job_succeeded for the same job.
        let log = std::fs::read_to_string(&log_path)?;
        let _ = std::fs::remove_file(&log_path);
        assert!(log.contains("job_succeeded"));
        assert!(log.contains("job-b"));
        Ok(())
    }

    #[tokio::test]
    async fn poison_nack_dead_letters_and_writes_failed_event() -> TestResult {
        let sink = Arc::new(RecordingRawWrittenSink::new());
        let (bus, log_path) = bus_with(
            vec![ledger_entry("job-a")],
            BTreeSet::new(),
            Arc::clone(&sink),
            5,
        )?;

        let leased = bus.poll(1).await?;
        let outcome = bus.nack(&leased[0].lease, &poison()).await?;
        assert_eq!(outcome, NackOutcome::DeadLettered);

        // No raw_written on failure; no redelivery for poison.
        assert_eq!(sink.count(), 0);
        assert!(bus.poll(1).await?.is_empty());

        let log = std::fs::read_to_string(&log_path)?;
        let _ = std::fs::remove_file(&log_path);
        assert!(log.contains("job_failed"));
        Ok(())
    }

    #[tokio::test]
    async fn transient_nack_redelivers_until_budget_exhausted() -> TestResult {
        let sink = Arc::new(RecordingRawWrittenSink::new());
        let (bus, log_path) = bus_with(
            vec![ledger_entry("job-a")],
            BTreeSet::new(),
            Arc::clone(&sink),
            2,
        )?;

        let lease1 = bus.poll(1).await?[0].lease.clone();
        assert_eq!(bus.nack(&lease1, &transient()).await?, NackOutcome::Retried);
        let lease2 = bus.poll(1).await?[0].lease.clone();
        assert_eq!(lease2.attempt, 2);
        assert_eq!(
            bus.nack(&lease2, &transient()).await?,
            NackOutcome::DeadLettered
        );

        let _ = std::fs::remove_file(&log_path);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_publish_is_unsupported() -> TestResult {
        let sink = Arc::new(RecordingRawWrittenSink::new());
        let (bus, log_path) = bus_with(
            vec![ledger_entry("job-a")],
            BTreeSet::new(),
            Arc::clone(&sink),
            3,
        )?;
        let job = super::super::to_collection_job(&ledger_entry("job-z"));
        assert!(bus.publish(job).await.is_err());
        let _ = std::fs::remove_file(&log_path);
        Ok(())
    }

    #[tokio::test]
    async fn from_planned_enforces_request_cap() -> TestResult {
        // OQ-2: the request_cap quota gate is enforced at seed time via select_pending_jobs.
        // A cap below the first job's request estimate (1) must be rejected, not silently dropped.
        let sink = Arc::new(RecordingRawWrittenSink::new());
        let (writer, path) = temp_event_writer()?;
        let result = LedgerJobBus::from_planned(
            vec![ledger_entry("job-a")],
            &BTreeSet::new(),
            100, // max_jobs
            0,   // request_cap below the estimate -> rejected by the quota gate
            writer,
            "compiler-input-hash".to_owned(),
            sink,
            3,
        );
        let _ = std::fs::remove_file(&path);
        assert!(result.is_err());
        Ok(())
    }
}
