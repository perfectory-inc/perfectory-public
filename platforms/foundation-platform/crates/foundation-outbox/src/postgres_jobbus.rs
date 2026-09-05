//! PostgreSQL-backed collection JobBus.
//!
//! `PostgresJobBus` is the durable dispatch adapter for the Collection Event Fabric. It uses one
//! row per job, `FOR UPDATE SKIP LOCKED` for concurrent polling, attempt fencing for stale leases,
//! and an atomic transaction that marks a job complete together with its `collection.raw_written`
//! outbox event. Dead-lettered jobs remain queryable in `catalog.collection_job` with their failure
//! detail; no in-memory or JSONL state is involved.

use chrono::{Duration, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgPool, Row};
use uuid::Uuid;

use crate::jobbus::{
    CollectionJob, CollectionSuccess, FailureDisposition, JobBus, JobBusError, JobFailure,
    JobLease, LeasedJob, NackOutcome,
};
use crate::outbox_raw_written_sink::encode_outbox_row;

/// Durable Postgres [`JobBus`] with lease fencing and retry/dead-letter state.
#[derive(Clone)]
pub struct PostgresJobBus {
    pool: PgPool,
    max_attempts: u32,
    lease_timeout: Duration,
}

impl PostgresJobBus {
    /// Create a Postgres [`JobBus`].
    ///
    /// `max_attempts` is clamped to one. A claimed row becomes eligible for redelivery after
    /// `lease_timeout` if the worker process dies without acking or nacking it.
    #[must_use]
    pub fn new(pool: PgPool, max_attempts: u32, lease_timeout: Duration) -> Self {
        Self {
            pool,
            max_attempts: max_attempts.max(1),
            lease_timeout,
        }
    }

    /// Claim one specific job for a plan-bound worker.
    ///
    /// This is intentionally narrower than [`JobBus::poll`]: the national plan executor publishes
    /// a selected ledger row and must not accidentally claim another run's pending job from the
    /// shared queue. The same lease-expiry and attempt-fencing rules apply.
    ///
    /// # Errors
    /// Returns [`JobBusError::Backend`] when the database cannot claim or decode the row.
    pub async fn claim_job(&self, job_id: &str) -> Result<Option<LeasedJob>, JobBusError> {
        let mut tx = self.pool.begin().await.map_err(Self::backend)?;
        let row = sqlx::query(
            "WITH claimable AS (
                 SELECT job_id
                 FROM catalog.collection_job
                 WHERE job_id = $1
                   AND (
                       state = 'pending'
                       OR (
                           state = 'in_flight'
                           AND leased_at IS NOT NULL
                           AND leased_at <= now() - ($2 * interval '1 second')
                       )
                   )
                   AND available_at <= now()
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE catalog.collection_job AS job
             SET state = 'in_flight',
                 attempt = job.attempt + 1,
                 leased_at = now(),
                 updated_at = now()
             FROM claimable
             WHERE job.job_id = claimable.job_id
             RETURNING job.*",
        )
        .bind(job_id)
        .bind(self.lease_timeout.num_seconds().max(1))
        .fetch_optional(&mut *tx)
        .await
        .map_err(Self::backend)?;
        tx.commit().await.map_err(Self::backend)?;

        row.map(|row| {
            let job = job_from_row(&row)?;
            let attempt = attempt_from_row(&row)?;
            Ok(LeasedJob {
                lease: JobLease {
                    job_id: job.job_id.clone(),
                    attempt,
                },
                job,
            })
        })
        .transpose()
    }

    /// Read the persisted state for restart reconciliation.
    ///
    /// A completed job cannot be claimed again. Bulk collectors use this small read to distinguish
    /// an already-acknowledged idempotent replay from a missing or dead-lettered job.
    ///
    /// # Errors
    /// Returns [`JobBusError::Backend`] when the database cannot read the job state.
    pub async fn job_state(&self, job_id: &str) -> Result<Option<String>, JobBusError> {
        sqlx::query_scalar("SELECT state FROM catalog.collection_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(Self::backend)
    }

    /// Publish a job unless the same immutable job is already present.
    ///
    /// This is the restart-safe seeding operation used by the plan executor. It is deliberately
    /// separate from [`JobBus::publish`], whose duplicate-id conflict behavior remains useful for
    /// detecting an accidental producer bug. A duplicate with different payload or identity is
    /// rejected rather than silently reusing the row.
    ///
    /// # Errors
    /// Returns [`JobBusError::Conflict`] when the existing row has different immutable data, or
    /// [`JobBusError::Backend`] when the database cannot read or write the row.
    pub async fn ensure_published(&self, job: CollectionJob) -> Result<(), JobBusError> {
        match self.publish(job.clone()).await {
            Ok(()) => Ok(()),
            Err(JobBusError::Conflict(_)) => {
                let row = sqlx::query("SELECT * FROM catalog.collection_job WHERE job_id = $1")
                    .bind(&job.job_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(Self::backend)?;
                let Some(row) = row else {
                    return Err(Self::conflict(format!(
                        "job {} disappeared while ensuring publication",
                        job.job_id
                    )));
                };
                let existing = job_from_row(&row)?;
                if existing == job {
                    Ok(())
                } else {
                    Err(Self::conflict(format!(
                        "job_id {} already exists with different immutable payload",
                        job.job_id
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Return one dead-lettered job to the pending queue with a fresh attempt budget.
    ///
    /// Dead-lettering is terminal for every consumer (ADR 0013): bulk ingest bails on a
    /// `dead_lettered` row, so a job that died on a transient failure blocks its source lane
    /// until an operator intervenes. This is that operator path. Only a `dead_lettered` row
    /// is eligible; `last_failure` is kept as history until the next nack overwrites it.
    ///
    /// Returns `false` when the job does not exist or is not dead-lettered.
    ///
    /// # Errors
    /// Returns [`JobBusError::Backend`] when the database cannot update the row.
    pub async fn requeue_dead_lettered(&self, job_id: &str) -> Result<bool, JobBusError> {
        let result = sqlx::query(
            "UPDATE catalog.collection_job
             SET state = 'pending', attempt = 0, dead_lettered_at = NULL,
                 leased_at = NULL, available_at = now(), updated_at = now()
             WHERE job_id = $1 AND state = 'dead_lettered'",
        )
        .bind(job_id)
        .execute(&self.pool)
        .await
        .map_err(Self::backend)?;
        Ok(result.rows_affected() > 0)
    }

    fn backend(error: impl std::fmt::Display) -> JobBusError {
        JobBusError::Backend(error.to_string())
    }

    fn conflict(message: impl Into<String>) -> JobBusError {
        JobBusError::Conflict(message.into())
    }
}

#[async_trait::async_trait]
impl JobBus for PostgresJobBus {
    async fn publish(&self, job: CollectionJob) -> Result<(), JobBusError> {
        let result = sqlx::query(
            "INSERT INTO catalog.collection_job (
                 job_id, scope_unit_id, shard_id, provider, endpoint, endpoint_slug,
                 idempotency_key, request_fingerprint_sha256, request_fingerprint_schema_version,
                 collection_snapshot_id, spec
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             ON CONFLICT (job_id) DO NOTHING",
        )
        .bind(&job.job_id)
        .bind(&job.scope_unit_id)
        .bind(&job.shard_id)
        .bind(&job.provider)
        .bind(&job.endpoint)
        .bind(&job.endpoint_slug)
        .bind(&job.idempotency_key)
        .bind(&job.request_fingerprint_sha256)
        .bind(&job.request_fingerprint_schema_version)
        .bind(&job.collection_snapshot_id)
        .bind(&job.spec)
        .execute(&self.pool)
        .await
        .map_err(Self::backend)?;

        if result.rows_affected() == 0 {
            return Err(Self::conflict(format!(
                "publish for duplicate job_id: {}",
                job.job_id
            )));
        }
        Ok(())
    }

    async fn poll(&self, max: usize) -> Result<Vec<LeasedJob>, JobBusError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await.map_err(Self::backend)?;
        let rows = sqlx::query(
            "WITH claimable AS (
                 SELECT job_id
                 FROM catalog.collection_job
                 WHERE (
                     state = 'pending'
                     OR (
                         state = 'in_flight'
                         AND leased_at IS NOT NULL
                         AND leased_at <= now() - ($2 * interval '1 second')
                     )
                 )
                   AND available_at <= now()
                 ORDER BY available_at, created_at, job_id
                 FOR UPDATE SKIP LOCKED
                 LIMIT $1
             )
             UPDATE catalog.collection_job AS job
             SET state = 'in_flight',
                 attempt = job.attempt + 1,
                 leased_at = now(),
                 updated_at = now()
             FROM claimable
             WHERE job.job_id = claimable.job_id
             RETURNING job.*",
        )
        .bind(i64::try_from(max).map_err(Self::backend)?)
        .bind(self.lease_timeout.num_seconds().max(1))
        .fetch_all(&mut *tx)
        .await
        .map_err(Self::backend)?;
        tx.commit().await.map_err(Self::backend)?;

        rows.into_iter()
            .map(|row| {
                let job = job_from_row(&row)?;
                let attempt = attempt_from_row(&row)?;
                Ok(LeasedJob {
                    lease: JobLease {
                        job_id: job.job_id.clone(),
                        attempt,
                    },
                    job,
                })
            })
            .collect()
    }

    async fn ack(&self, lease: &JobLease, success: &CollectionSuccess) -> Result<(), JobBusError> {
        let mut tx = self.pool.begin().await.map_err(Self::backend)?;
        let Some(row) =
            sqlx::query("SELECT * FROM catalog.collection_job WHERE job_id = $1 FOR UPDATE")
                .bind(&lease.job_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(Self::backend)?
        else {
            return Err(Self::conflict(format!(
                "ack for unknown job at attempt {}: {}",
                lease.attempt, lease.job_id
            )));
        };
        ensure_lease(&row, lease)?;

        let job = job_from_row(&row)?;
        let raw_written = success.into_raw_written(&job, Utc::now());
        let (type_tag, payload) = encode_outbox_row(&raw_written)?;
        let success_json = success_json(success);

        sqlx::query(
            "UPDATE catalog.collection_job
             SET state = 'completed', completed_at = now(), leased_at = NULL,
                 success = $2, updated_at = now()
             WHERE job_id = $1 AND state = 'in_flight' AND attempt = $3",
        )
        .bind(&lease.job_id)
        .bind(success_json)
        .bind(i32::try_from(lease.attempt).map_err(Self::backend)?)
        .execute(&mut *tx)
        .await
        .map_err(Self::backend)?;

        sqlx::query(
            "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at)
             VALUES ($1, $2, $3, now())",
        )
        .bind(Uuid::new_v4())
        .bind(type_tag)
        .bind(payload)
        .execute(&mut *tx)
        .await
        .map_err(Self::backend)?;

        tx.commit().await.map_err(Self::backend)
    }

    async fn nack(
        &self,
        lease: &JobLease,
        failure: &JobFailure,
    ) -> Result<NackOutcome, JobBusError> {
        let mut tx = self.pool.begin().await.map_err(Self::backend)?;
        let Some(row) = sqlx::query(
            "SELECT state, attempt FROM catalog.collection_job WHERE job_id = $1 FOR UPDATE",
        )
        .bind(&lease.job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(Self::backend)?
        else {
            return Err(Self::conflict(format!(
                "nack for unknown job at attempt {}: {}",
                lease.attempt, lease.job_id
            )));
        };
        ensure_lease(&row, lease)?;

        let attempt = attempt_from_row(&row)?;
        let dead =
            failure.disposition == FailureDisposition::Poison || attempt >= self.max_attempts;
        let failure_json = failure_json(failure);
        let (state, outcome) = if dead {
            ("dead_lettered", NackOutcome::DeadLettered)
        } else {
            ("pending", NackOutcome::Retried)
        };

        sqlx::query(
            "UPDATE catalog.collection_job
             SET state = $2, available_at = now(), leased_at = NULL,
                 dead_lettered_at = CASE WHEN $2 = 'dead_lettered' THEN now() ELSE dead_lettered_at END,
                 last_failure = $3, updated_at = now()
             WHERE job_id = $1 AND state = 'in_flight' AND attempt = $4",
        )
        .bind(&lease.job_id)
        .bind(state)
        .bind(failure_json)
        .bind(i32::try_from(lease.attempt).map_err(Self::backend)?)
        .execute(&mut *tx)
        .await
        .map_err(Self::backend)?;

        tx.commit().await.map_err(Self::backend)?;
        Ok(outcome)
    }
}

fn ensure_lease(row: &PgRow, lease: &JobLease) -> Result<(), JobBusError> {
    let state: String = row.try_get("state").map_err(PostgresJobBus::backend)?;
    let attempt = attempt_from_row(row)?;
    if state != "in_flight" || attempt != lease.attempt {
        return Err(PostgresJobBus::conflict(format!(
            "job {} is not leased at attempt {} (state={state}, current_attempt={attempt})",
            lease.job_id, lease.attempt
        )));
    }
    Ok(())
}

fn attempt_from_row(row: &PgRow) -> Result<u32, JobBusError> {
    let attempt: i32 = row.try_get("attempt").map_err(PostgresJobBus::backend)?;
    u32::try_from(attempt)
        .map_err(|error| JobBusError::Backend(PostgresJobError::InvalidAttempt(error).to_string()))
}

#[derive(Debug, thiserror::Error)]
enum PostgresJobError {
    #[error("invalid persisted job attempt: {0}")]
    InvalidAttempt(#[source] std::num::TryFromIntError),
}

fn job_from_row(row: &PgRow) -> Result<CollectionJob, JobBusError> {
    Ok(CollectionJob {
        job_id: row.try_get("job_id").map_err(PostgresJobBus::backend)?,
        scope_unit_id: row
            .try_get("scope_unit_id")
            .map_err(PostgresJobBus::backend)?,
        shard_id: row.try_get("shard_id").map_err(PostgresJobBus::backend)?,
        provider: row.try_get("provider").map_err(PostgresJobBus::backend)?,
        endpoint: row.try_get("endpoint").map_err(PostgresJobBus::backend)?,
        endpoint_slug: row
            .try_get("endpoint_slug")
            .map_err(PostgresJobBus::backend)?,
        idempotency_key: row
            .try_get("idempotency_key")
            .map_err(PostgresJobBus::backend)?,
        request_fingerprint_sha256: row
            .try_get("request_fingerprint_sha256")
            .map_err(PostgresJobBus::backend)?,
        request_fingerprint_schema_version: row
            .try_get("request_fingerprint_schema_version")
            .map_err(PostgresJobBus::backend)?,
        collection_snapshot_id: row
            .try_get("collection_snapshot_id")
            .map_err(PostgresJobBus::backend)?,
        spec: row.try_get("spec").map_err(PostgresJobBus::backend)?,
    })
}

fn success_json(success: &CollectionSuccess) -> Value {
    json!({
        "bronze_object_key": success.bronze_object_key,
        "bronze_object_count": success.bronze_object_count,
        "bronze_checksum_sha256": success.bronze_checksum_sha256,
        "bronze_size_bytes": success.bronze_size_bytes,
        "source_record_count": success.source_record_count,
        "request_count": success.request_count,
        "reused_bronze_object": success.reused_bronze_object,
        "license": success.license,
        "srid": success.srid,
        "fetched_at_utc": success.fetched_at_utc,
    })
}

fn failure_json(failure: &JobFailure) -> Value {
    json!({
        "disposition": match failure.disposition {
            FailureDisposition::Retryable => "retryable",
            FailureDisposition::Poison => "poison",
        },
        "code": failure.code,
        "message": failure.message,
    })
}
