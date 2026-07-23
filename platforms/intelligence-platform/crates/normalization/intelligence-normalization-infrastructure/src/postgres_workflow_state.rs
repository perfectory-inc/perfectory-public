//! PostgreSQL-backed implementation of [`NormalizationOutboxPort`] and
//! [`NormalizationAuditPort`].
//!
//! Uses `sqlx` with runtime query binding only — no compile-time `query!`
//! macros — so the workspace builds without a live database.
//!
//! # Clock rule
//!
//! All lease expiry timestamps are set **and** compared on the DB clock
//! (`now()`).  Application-side `Utc::now()` is never mixed with SQL
//! `now()` comparisons to avoid flake under clock skew.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use intelligence_normalization_application::{
    FoundationSubmissionResult, NormalizationAuditEvent, NormalizationAuditPort,
    NormalizationOutboxPort, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationProposalSubmission, NormalizationReconcileQueuePort, OutboxAcquireResult,
    OutboxTransitionError, ReconcileQueueStats,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors produced during configuration validation or pool initialisation.
///
/// Trait-method errors are reported as [`OutboxTransitionError::StoreFailed`]
/// so callers depend only on the core port error type.
#[derive(Debug, thiserror::Error)]
pub enum PostgresWorkflowStateError {
    /// The supplied configuration was invalid (e.g. empty URL, zero timeout).
    #[error("postgres workflow state config is invalid")]
    InvalidConfig,
    /// A database operation failed during `connect` or migration.
    #[error("postgres workflow state failed: {message}")]
    StoreFailed { message: String },
}

impl PostgresWorkflowStateError {
    /// Returns a static, safe-to-expose message that cannot leak internal state.
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "postgres workflow state config is invalid",
            Self::StoreFailed { .. } => "postgres workflow state failed",
        }
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for [`PostgresWorkflowState`].
#[derive(Clone, Debug)]
pub struct PostgresWorkflowStateConfig {
    database_url: String,
    timeout_seconds: u64,
    pub max_connections: u32,
}

impl PostgresWorkflowStateConfig {
    /// Creates a validated config.
    ///
    /// Returns [`PostgresWorkflowStateError::InvalidConfig`] when
    /// `database_url` is empty/whitespace or `timeout_seconds` is zero.
    /// `max_connections` defaults to 10; use [`Self::with_max_connections`]
    /// to override.
    pub fn new(
        database_url: impl Into<String>,
        timeout_seconds: u64,
    ) -> Result<Self, PostgresWorkflowStateError> {
        let url = database_url.into();
        if url.trim().is_empty() || timeout_seconds == 0 {
            return Err(PostgresWorkflowStateError::InvalidConfig);
        }
        Ok(Self {
            database_url: url,
            timeout_seconds,
            max_connections: 10,
        })
    }

    /// Overrides the maximum pool size.
    ///
    /// Returns [`PostgresWorkflowStateError::InvalidConfig`] when `n` is zero.
    pub fn with_max_connections(mut self, n: u32) -> Result<Self, PostgresWorkflowStateError> {
        if n == 0 {
            return Err(PostgresWorkflowStateError::InvalidConfig);
        }
        self.max_connections = n;
        Ok(self)
    }

    /// Returns the database URL.
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Returns the acquire-timeout in seconds.
    pub fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    /// Returns the maximum pool connection count.
    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Postgres-backed implementation of both workflow state ports.
///
/// Shares a `PgPool` internally; `Clone` is intentionally omitted — callers
/// should wrap in `Arc<PostgresWorkflowState>` and share the `Arc`.
pub struct PostgresWorkflowState {
    pool: PgPool,
}

impl std::fmt::Debug for PostgresWorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresWorkflowState")
            .field("pool", &"PgPool { .. }")
            .finish()
    }
}

impl PostgresWorkflowState {
    /// Connects to Postgres, runs embedded migrations, and returns a ready adapter.
    ///
    /// Migrations are embedded at compile time via `sqlx::migrate!`; no live
    /// database is required to build the crate.
    pub async fn connect(
        config: PostgresWorkflowStateConfig,
    ) -> Result<Self, PostgresWorkflowStateError> {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(config.timeout_seconds))
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|e| PostgresWorkflowStateError::StoreFailed {
                message: e.to_string(),
            })?;

        sqlx::migrate!("../../../migrations")
            .run(&pool)
            .await
            .map_err(|e| PostgresWorkflowStateError::StoreFailed {
                message: e.to_string(),
            })?;

        Ok(Self { pool })
    }

    /// Truncates the outbox and audit tables.
    ///
    /// # Warning — test-support only
    ///
    /// This method is intended **exclusively** for integration-test harnesses
    /// that need a clean slate before each run against a shared database
    /// container.  It must never be called from production code.
    #[doc(hidden)]
    pub async fn truncate_for_tests(&self) -> Result<(), sqlx::Error> {
        sqlx::query("TRUNCATE ip_normalization_outbox, ip_normalization_audit_events")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Shared implementation for the three failure-mark transitions.
    ///
    /// Issues one UPDATE that sets `status` to `status_str`, increments
    /// `attempts`, nulls `claimed_until`, and stores `error` in `last_error`,
    /// requiring that the current status is `in_flight`.  Returns the updated
    /// row or delegates to [`resolve_rejected_or_not_found`] on 0-rows-affected.
    async fn mark_failure_status(
        &self,
        idempotency_key: &str,
        error: String,
        status: NormalizationOutboxStatus,
        transition_name: &str,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        let status_str = status_to_str(status);
        let row = sqlx::query(&format!(
            "UPDATE ip_normalization_outbox \
             SET status = '{status_str}', \
                 attempts = attempts + 1, \
                 last_error = $2, \
                 claimed_until = NULL, \
                 updated_at = now() \
             WHERE idempotency_key = $1 AND status = 'in_flight' \
             RETURNING {OUTBOX_COLS}"
        ))
        .bind(idempotency_key)
        .bind(&error)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        match row {
            Some(r) => row_to_record(&r),
            None => {
                Err(
                    resolve_rejected_or_not_found(&self.pool, idempotency_key, transition_name)
                        .await,
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Row ↔ domain mapping helpers
// ---------------------------------------------------------------------------

fn status_from_str(s: &str) -> Result<NormalizationOutboxStatus, OutboxTransitionError> {
    match s {
        "accepted" => Ok(NormalizationOutboxStatus::Accepted),
        "pending" => Ok(NormalizationOutboxStatus::Pending),
        "in_flight" => Ok(NormalizationOutboxStatus::InFlight),
        "sent" => Ok(NormalizationOutboxStatus::Sent),
        "failed_retryable" => Ok(NormalizationOutboxStatus::FailedRetryable),
        "failed_terminal" => Ok(NormalizationOutboxStatus::FailedTerminal),
        "dead_letter" => Ok(NormalizationOutboxStatus::DeadLetter),
        "reconcile_required" => Ok(NormalizationOutboxStatus::ReconcileRequired),
        other => Err(OutboxTransitionError::StoreFailed {
            message: format!("unknown outbox status in database: {other}"),
        }),
    }
}

fn store_failed(message: impl Into<String>) -> OutboxTransitionError {
    OutboxTransitionError::StoreFailed {
        message: message.into(),
    }
}

/// Maps a [`NormalizationOutboxStatus`] variant to its SQL column literal.
fn status_to_str(status: NormalizationOutboxStatus) -> &'static str {
    match status {
        NormalizationOutboxStatus::Accepted => "accepted",
        NormalizationOutboxStatus::Pending => "pending",
        NormalizationOutboxStatus::InFlight => "in_flight",
        NormalizationOutboxStatus::Sent => "sent",
        NormalizationOutboxStatus::FailedRetryable => "failed_retryable",
        NormalizationOutboxStatus::FailedTerminal => "failed_terminal",
        NormalizationOutboxStatus::DeadLetter => "dead_letter",
        NormalizationOutboxStatus::ReconcileRequired => "reconcile_required",
    }
}

fn row_to_record(
    row: &sqlx::postgres::PgRow,
) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
    let id: Uuid = row.try_get("id").map_err(|e| store_failed(e.to_string()))?;
    let idempotency_key: String = row
        .try_get("idempotency_key")
        .map_err(|e| store_failed(e.to_string()))?;
    let payload: serde_json::Value = row
        .try_get("payload")
        .map_err(|e| store_failed(e.to_string()))?;
    let submission: NormalizationProposalSubmission = serde_json::from_value(payload)
        .map_err(|e| store_failed(format!("payload deserialize: {e}")))?;
    let payload_fingerprint: String = row
        .try_get("payload_fingerprint")
        .map_err(|e| store_failed(e.to_string()))?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| store_failed(e.to_string()))?;
    let status = status_from_str(&status_str)?;
    let attempts_i32: i32 = row
        .try_get("attempts")
        .map_err(|e| store_failed(e.to_string()))?;
    let attempts: u32 =
        u32::try_from(attempts_i32).map_err(|_| store_failed("attempts value out of u32 range"))?;
    let submission_result_json: Option<serde_json::Value> = row
        .try_get("submission_result")
        .map_err(|e| store_failed(e.to_string()))?;
    let submission_result: Option<FoundationSubmissionResult> = submission_result_json
        .map(|v| {
            serde_json::from_value(v)
                .map_err(|e| store_failed(format!("submission_result deserialize: {e}")))
        })
        .transpose()?;
    let last_error: Option<String> = row
        .try_get("last_error")
        .map_err(|e| store_failed(e.to_string()))?;
    let claimed_until: Option<DateTime<Utc>> = row
        .try_get("claimed_until")
        .map_err(|e| store_failed(e.to_string()))?;
    let created_at: DateTime<Utc> = row
        .try_get("created_at")
        .map_err(|e| store_failed(e.to_string()))?;
    let updated_at: DateTime<Utc> = row
        .try_get("updated_at")
        .map_err(|e| store_failed(e.to_string()))?;

    Ok(NormalizationOutboxRecord {
        outbox_id: id.to_string(),
        idempotency_key,
        submission,
        status,
        attempts,
        submission_result,
        last_error,
        payload_fingerprint,
        claimed_until,
        created_at,
        updated_at,
    })
}

/// Resolves a mark_* 0-rows-affected outcome: looks up the row by
/// `idempotency_key` and returns `NotFound` if absent or
/// `Rejected { current }` if present (with a non-`in_flight` status).
async fn resolve_rejected_or_not_found(
    pool: &PgPool,
    idempotency_key: &str,
    transition: &str,
) -> OutboxTransitionError {
    let result =
        sqlx::query("SELECT status FROM ip_normalization_outbox WHERE idempotency_key = $1")
            .bind(idempotency_key)
            .fetch_optional(pool)
            .await;

    match result {
        Err(e) => store_failed(e.to_string()),
        Ok(None) => OutboxTransitionError::NotFound,
        Ok(Some(row)) => {
            match row.try_get::<String, _>("status") {
                Err(e) => store_failed(e.to_string()),
                Ok(s) => match status_from_str(&s) {
                    Ok(current) => {
                        // InFlight means the UPDATE's WHERE predicate matched when
                        // the query ran but the row was claimed by another worker
                        // between the UPDATE and this fallback SELECT.
                        let msg = if current == NormalizationOutboxStatus::InFlight {
                            format!("{transition} requires InFlight status; status changed concurrently")
                        } else {
                            format!("{transition} requires InFlight status")
                        };
                        OutboxTransitionError::Rejected {
                            current,
                            message: msg,
                        }
                    }
                    Err(e) => e,
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Outbox port
// ---------------------------------------------------------------------------

/// Columns returned by all SELECT/RETURNING queries on the outbox table.
const OUTBOX_COLS: &str = "id, idempotency_key, payload, payload_fingerprint, status, \
                            attempts, submission_result, last_error, claimed_until, \
                            created_at, updated_at";

#[async_trait]
impl NormalizationOutboxPort for PostgresWorkflowState {
    async fn enqueue(
        &self,
        record: NormalizationOutboxRecord,
        lease: Duration,
    ) -> Result<OutboxAcquireResult, OutboxTransitionError> {
        let id = record
            .outbox_id
            .parse::<Uuid>()
            .map_err(|e| store_failed(format!("outbox_id is not a valid UUID: {e}")))?;

        let payload = serde_json::to_value(&record.submission)
            .map_err(|e| store_failed(format!("submission serialize: {e}")))?;

        let lease_secs = lease.as_secs_f64();

        let result = sqlx::query(
            r#"
            INSERT INTO ip_normalization_outbox (
                id, idempotency_key, aggregateid,
                payload, payload_fingerprint, ce_id,
                status, attempts,
                submission_result, last_error,
                claimed_until, created_at, updated_at
            ) VALUES (
                $1, $2, $2,
                $3, $4, $1,
                'in_flight', 0,
                NULL, NULL,
                now() + make_interval(secs => $5), $6, now()
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            "#,
        )
        .bind(id)
        .bind(&record.idempotency_key)
        .bind(payload)
        .bind(&record.payload_fingerprint)
        .bind(lease_secs)
        .bind(record.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        if result.rows_affected() == 1 {
            return Ok(OutboxAcquireResult::Acquired);
        }

        // Conflict on idempotency_key: read the existing row to apply matrix.
        // safe: outbox rows are never deleted, so the conflicting row cannot vanish.
        let existing = sqlx::query(
            "SELECT status, payload_fingerprint \
             FROM ip_normalization_outbox \
             WHERE idempotency_key = $1",
        )
        .bind(&record.idempotency_key)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        let existing_fp: String = existing
            .try_get("payload_fingerprint")
            .map_err(|e| store_failed(e.to_string()))?;
        let existing_status_str: String = existing
            .try_get("status")
            .map_err(|e| store_failed(e.to_string()))?;
        let existing_status = status_from_str(&existing_status_str)?;

        // PayloadMismatch is checked FIRST, regardless of status.
        if existing_fp != record.payload_fingerprint {
            return Ok(OutboxAcquireResult::PayloadMismatch);
        }

        // Same fingerprint: branch on status.
        match existing_status {
            NormalizationOutboxStatus::DeadLetter
            | NormalizationOutboxStatus::FailedTerminal
            | NormalizationOutboxStatus::ReconcileRequired => {
                Err(OutboxTransitionError::Rejected {
                    current: existing_status,
                    message: "record is terminal; operator action required".to_string(),
                })
            }
            NormalizationOutboxStatus::Sent => Ok(OutboxAcquireResult::AlreadySent),
            _ => Ok(OutboxAcquireResult::AlreadyInFlight),
        }
    }

    async fn get_sent(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<NormalizationOutboxRecord>, OutboxTransitionError> {
        let row = sqlx::query(&format!(
            "SELECT {OUTBOX_COLS} \
             FROM ip_normalization_outbox \
             WHERE idempotency_key = $1 AND status = 'sent'"
        ))
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        row.map(|r| row_to_record(&r)).transpose()
    }

    async fn mark_sent(
        &self,
        idempotency_key: &str,
        result: FoundationSubmissionResult,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        let submission_result_json = serde_json::to_value(&result)
            .map_err(|e| store_failed(format!("submission_result serialize: {e}")))?;

        let row = sqlx::query(&format!(
            "UPDATE ip_normalization_outbox \
             SET status = 'sent', \
                 attempts = attempts + 1, \
                 submission_result = $2, \
                 last_error = NULL, \
                 claimed_until = NULL, \
                 updated_at = now() \
             WHERE idempotency_key = $1 AND status = 'in_flight' \
             RETURNING {OUTBOX_COLS}"
        ))
        .bind(idempotency_key)
        .bind(submission_result_json)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        match row {
            Some(r) => row_to_record(&r),
            None => {
                Err(resolve_rejected_or_not_found(&self.pool, idempotency_key, "mark_sent").await)
            }
        }
    }

    async fn mark_retryable_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::FailedRetryable,
            "mark_retryable_failure",
        )
        .await
    }

    async fn mark_dead_letter(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::DeadLetter,
            "mark_dead_letter",
        )
        .await
    }

    async fn mark_terminal_failure(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::FailedTerminal,
            "mark_terminal_failure",
        )
        .await
    }

    async fn mark_reconcile_required(
        &self,
        idempotency_key: &str,
        error: String,
    ) -> Result<NormalizationOutboxRecord, OutboxTransitionError> {
        self.mark_failure_status(
            idempotency_key,
            error,
            NormalizationOutboxStatus::ReconcileRequired,
            "mark_reconcile_required",
        )
        .await
    }

    async fn claim_next_pending(
        &self,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<NormalizationOutboxRecord>, OutboxTransitionError> {
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| store_failed("claim_next_pending: limit overflows i64"))?;
        let lease_secs = lease.as_secs_f64();

        // NOTE: UPDATE…FROM does not preserve the CTE's ORDER BY, so returned
        // rows may arrive in any order when limit > 1.  Because the contract
        // suite's ordering scenario uses limit=1, the CTE's ORDER BY + LIMIT=1
        // guarantees the oldest row is the one selected.  For limit > 1, all
        // claimed rows receive the same updated_at (= now()), so secondary
        // ordering is not observable by callers.
        let rows = sqlx::query(&format!(
            r#"WITH claimable AS (
                SELECT idempotency_key FROM ip_normalization_outbox
                WHERE status IN ('pending','failed_retryable')
                   OR (status = 'in_flight' AND claimed_until <= now())
                ORDER BY updated_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE ip_normalization_outbox outbox
            SET status = 'in_flight',
                claimed_until = now() + make_interval(secs => $2),
                updated_at = now()
            FROM claimable
            WHERE outbox.idempotency_key = claimable.idempotency_key
            RETURNING {OUTBOX_COLS_PREFIXED}"#,
            OUTBOX_COLS_PREFIXED = prefixed_outbox_cols()
        ))
        .bind(limit_i64)
        .bind(lease_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        rows.iter().map(row_to_record).collect()
    }
}

#[async_trait]
impl NormalizationReconcileQueuePort for PostgresWorkflowState {
    async fn stats(&self) -> Result<ReconcileQueueStats, OutboxTransitionError> {
        let row = sqlx::query(
            "SELECT COUNT(*)::bigint AS depth, \
                    COALESCE(EXTRACT(EPOCH FROM (now() - MIN(updated_at))), 0)::double precision \
                        AS oldest_age_seconds \
             FROM ip_normalization_outbox \
             WHERE status = 'reconcile_required'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        let depth_i64: i64 = row
            .try_get("depth")
            .map_err(|e| store_failed(e.to_string()))?;
        let depth = u64::try_from(depth_i64)
            .map_err(|_| store_failed("reconcile depth value out of u64 range"))?;
        let oldest_age_seconds: f64 = row
            .try_get("oldest_age_seconds")
            .map_err(|e| store_failed(e.to_string()))?;

        Ok(ReconcileQueueStats {
            depth,
            oldest_age_seconds: oldest_age_seconds.max(0.0),
        })
    }
}

/// Returns the OUTBOX_COLS list prefixed with `outbox.` for use in
/// UPDATE…FROM…RETURNING queries where column names could be ambiguous.
fn prefixed_outbox_cols() -> String {
    OUTBOX_COLS
        .split(", ")
        .map(|col| format!("outbox.{col}"))
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Audit port
// ---------------------------------------------------------------------------

#[async_trait]
impl NormalizationAuditPort for PostgresWorkflowState {
    async fn append(&self, event: NormalizationAuditEvent) -> Result<(), OutboxTransitionError> {
        // Require a well-formed UUID so audit events remain correlatable.
        // Core always generates UUIDs; a malformed id indicates a programming
        // error, not a transient failure.
        let event_id =
            event
                .event_id
                .parse::<Uuid>()
                .map_err(|_| OutboxTransitionError::StoreFailed {
                    message: format!("audit event_id is not a valid uuid: {}", event.event_id),
                })?;

        let trace_context = serde_json::to_value(&event.trace_context)
            .map_err(|e| store_failed(format!("trace_context serialize: {e}")))?;
        let metadata = serde_json::to_value(&event.metadata)
            .map_err(|e| store_failed(format!("metadata serialize: {e}")))?;

        sqlx::query(
            "INSERT INTO ip_normalization_audit_events \
             (event_id, event_type, trace_context, metadata, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(event_id)
        .bind(&event.event_type)
        .bind(trace_context)
        .bind(metadata)
        .bind(event.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| store_failed(e.to_string()))?;

        Ok(())
    }
}
