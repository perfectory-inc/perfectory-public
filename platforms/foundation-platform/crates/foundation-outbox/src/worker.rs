use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use tokio::{sync::watch, time};
use uuid::Uuid;

use crate::{
    broadcaster::{EventBroadcaster, EventEnvelope},
    config::PublisherConfig,
    errors::PublishError,
};

const QUARANTINE_TABLE: &str = "catalog.outbox_quarantine";
const QUARANTINE_CONSUMER_KEY: &str = "outbox-publisher";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Bounded context whose outbox table is being polled.
pub enum OutboxScope {
    /// Catalog schema outbox.
    Catalog,
}

impl OutboxScope {
    /// Returns the fully qualified outbox table name.
    #[must_use]
    pub const fn table_qualified(self) -> &'static str {
        match self {
            Self::Catalog => "catalog.outbox_event",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
        }
    }
}

#[derive(Clone)]
/// Polls one foundation-platform outbox table and publishes pending rows.
pub struct OutboxWorker {
    pool: PgPool,
    broadcaster: Arc<dyn EventBroadcaster>,
    config: PublisherConfig,
    scope: OutboxScope,
    worker_id: Uuid,
}

impl OutboxWorker {
    /// Creates an outbox worker for one scope.
    #[must_use]
    pub fn new(
        pool: PgPool,
        broadcaster: Arc<dyn EventBroadcaster>,
        config: PublisherConfig,
        scope: OutboxScope,
    ) -> Self {
        Self {
            pool,
            broadcaster,
            config,
            scope,
            worker_id: Uuid::new_v4(),
        }
    }

    /// 한 번의 polling 주기 동안 잠긴 outbox row를 발행해요.
    ///
    /// # Errors
    ///
    /// DB 트랜잭션, row 조회, 상태 갱신, 커밋 중 오류가 발생하면 오류를 반환해요.
    pub async fn tick(&self) -> Result<TickStats, PublishError> {
        let sql = OutboxSql::for_scope(self.scope);
        // Claim rows in a short transaction, then release the DB locks before
        // calling an external broadcaster. The lease prevents another worker
        // from selecting the same rows while the network call is in flight.
        let rows = self.claim_pending_rows(&sql).await?;
        let mut stats = TickStats::default();

        for row in rows {
            let row_stats = self.publish_pending_row(&sql, row).await?;
            stats.merge(row_stats);
        }
        Ok(stats)
    }

    async fn claim_pending_rows(
        &self,
        sql: &OutboxSql,
    ) -> Result<Vec<PendingOutboxRow>, PublishError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;

        let lease_seconds = self.config.lease_duration.as_secs_f64();
        let rows = sqlx::query(&sql.claim)
            .bind(self.config.max_retries)
            .bind(self.config.batch_size)
            .bind(self.worker_id)
            .bind(lease_seconds)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?
            .iter()
            .map(PendingOutboxRow::from_pg_row)
            .collect::<Result<Vec<_>, _>>()?;

        tx.commit()
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
        Ok(rows)
    }

    async fn publish_pending_row(
        &self,
        sql: &OutboxSql,
        row: PendingOutboxRow,
    ) -> Result<TickStats, PublishError> {
        let retry_count = row.retry_count;
        let event = row.into_event(self.scope);

        match self.broadcaster.publish(&event).await {
            Ok(()) => {
                mark_published(&self.pool, sql, event.event_id, self.worker_id).await?;
                Ok(TickStats {
                    published: 1,
                    ..TickStats::default()
                })
            }
            Err(error) => {
                self.record_publish_failure(sql, &event, retry_count, &error)
                    .await
            }
        }
    }

    async fn record_publish_failure(
        &self,
        sql: &OutboxSql,
        event: &EventEnvelope,
        retry_count: i32,
        error: &PublishError,
    ) -> Result<TickStats, PublishError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
        let next_attempt_count = retry_count.saturating_add(1);
        increment_retry_count(&mut tx, sql, event.event_id, self.worker_id).await?;

        let mut stats = TickStats {
            retried: 1,
            ..TickStats::default()
        };
        if next_attempt_count >= self.config.max_retries {
            self.quarantine_exhausted_retry(&mut tx, sql, event, error, next_attempt_count)
                .await?;
            stats.dead_lettered = 1;
        }
        tx.commit()
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
        Ok(stats)
    }

    async fn quarantine_exhausted_retry(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sql: &OutboxSql,
        event: &EventEnvelope,
        error: &PublishError,
        attempt_count: i32,
    ) -> Result<(), PublishError> {
        let failure_message = error.to_string();
        sqlx::query(&sql.quarantine)
            .bind(Uuid::new_v4())
            .bind(self.scope.table_qualified())
            .bind(event.event_id)
            .bind(QUARANTINE_CONSUMER_KEY)
            .bind(&event.event_type)
            .bind(event.payload.clone())
            .bind(failure_code(error))
            .bind(failure_message.clone())
            .bind(attempt_count)
            .bind(json!({
                "scope": self.scope.label(),
                "max_retries": self.config.max_retries,
            }))
            .execute(&mut **tx)
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
        tracing::error!(
            event_id = %event.event_id,
            event_type = %event.event_type,
            scope = ?self.scope,
            error = %failure_message,
            "outbox event reached max retries"
        );
        Ok(())
    }

    /// shutdown 신호를 받을 때까지 주기적으로 outbox row를 발행해요.
    ///
    /// 개별 `tick` 실패(일시적 DB 오류 등)는 worker를 종료시키지 않고 로깅한 뒤 다음 poll에서
    /// 재시도해요. 매 tick 앞의 `poll_interval` sleep이 자연스러운 backoff 역할을 하므로,
    /// 일시 장애가 지나가면 발행이 자동으로 재개돼요. (이전에는 첫 오류에 worker가 영구 종료돼
    /// shutdown 전까지 발행이 조용히 멈췄어요 — audit/Codex finding.)
    ///
    /// # Errors
    ///
    /// 현재 정상 경로에서는 오류를 반환하지 않고, shutdown 신호를 받으면 `Ok(())`로 종료해요.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), PublishError> {
        loop {
            tokio::select! {
                () = time::sleep(self.config.poll_interval) => {
                    if let Err(error) = self.tick().await {
                        tracing::error!(
                            scope = ?self.scope,
                            error = %error,
                            "outbox worker tick failed; retrying on next poll interval"
                        );
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

struct PendingOutboxRow {
    event_id: Uuid,
    event_type: String,
    payload: Value,
    occurred_at: DateTime<Utc>,
    retry_count: i32,
}

impl PendingOutboxRow {
    fn from_pg_row(row: &PgRow) -> Result<Self, PublishError> {
        Ok(Self {
            event_id: row
                .try_get("event_id")
                .map_err(|error| PublishError::Infrastructure(error.to_string()))?,
            event_type: row
                .try_get("type")
                .map_err(|error| PublishError::Infrastructure(error.to_string()))?,
            payload: row
                .try_get("payload")
                .map_err(|error| PublishError::Infrastructure(error.to_string()))?,
            occurred_at: row
                .try_get("occurred_at")
                .map_err(|error| PublishError::Infrastructure(error.to_string()))?,
            retry_count: row
                .try_get("retry_count")
                .map_err(|error| PublishError::Infrastructure(error.to_string()))?,
        })
    }

    fn into_event(self, scope: OutboxScope) -> EventEnvelope {
        EventEnvelope {
            event_id: self.event_id,
            event_type: self.event_type,
            payload: self.payload,
            occurred_at: self.occurred_at,
            scope,
        }
    }
}

struct OutboxSql {
    claim: String,
    publish: String,
    retry: String,
    quarantine: String,
}

impl OutboxSql {
    fn for_scope(scope: OutboxScope) -> Self {
        let table = scope.table_qualified();
        Self {
            claim: format!(
                "WITH candidates AS (
                    SELECT event_id
                    FROM {table}
                    WHERE published_at IS NULL
                      AND retry_count < $1
                      AND (lease_until IS NULL OR lease_until <= now())
                    ORDER BY occurred_at
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                 )
                 UPDATE {table} AS outbox
                 SET lease_owner = $3,
                     lease_until = now() + ($4::double precision * interval '1 second')
                 FROM candidates
                 WHERE outbox.event_id = candidates.event_id
                 RETURNING outbox.event_id, outbox.type, outbox.payload,
                           outbox.occurred_at, outbox.retry_count"
            ),
            publish: format!(
                "UPDATE {table}
                 SET published_at = now(),
                     lease_owner = NULL,
                     lease_until = NULL
                 WHERE event_id = $1 AND lease_owner = $2"
            ),
            retry: format!(
                "UPDATE {table}
                 SET retry_count = retry_count + 1,
                     lease_owner = NULL,
                     lease_until = NULL
                 WHERE event_id = $1 AND lease_owner = $2"
            ),
            quarantine: format!(
                "INSERT INTO {QUARANTINE_TABLE} (
                    id,
                    source_outbox_table,
                    event_id,
                    consumer_key,
                    event_type,
                    payload,
                    failure_stage,
                    failure_code,
                    failure_message,
                    attempt_count,
                    first_failed_at,
                    last_failed_at,
                    lineage
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6, 'retry_exhausted', $7, $8, $9, now(), now(), $10
                 )
                 ON CONFLICT (source_outbox_table, event_id, consumer_key)
                 DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    payload = EXCLUDED.payload,
                    failure_stage = EXCLUDED.failure_stage,
                    failure_code = EXCLUDED.failure_code,
                    failure_message = EXCLUDED.failure_message,
                    attempt_count = EXCLUDED.attempt_count,
                    last_failed_at = EXCLUDED.last_failed_at,
                    next_retry_at = NULL,
                    resolved_at = NULL,
                    resolution_kind = NULL,
                    resolution_note = NULL,
                    lineage = EXCLUDED.lineage,
                    updated_at = now(),
                    version = {QUARANTINE_TABLE}.version + 1"
            ),
        }
    }
}

async fn mark_published(
    pool: &PgPool,
    sql: &OutboxSql,
    event_id: Uuid,
    worker_id: Uuid,
) -> Result<(), PublishError> {
    let result = sqlx::query(&sql.publish)
        .bind(event_id)
        .bind(worker_id)
        .execute(pool)
        .await
        .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
    if result.rows_affected() != 1 {
        return Err(PublishError::Infrastructure(
            "outbox publish lease was lost before acknowledgement".to_owned(),
        ));
    }
    Ok(())
}

async fn increment_retry_count(
    tx: &mut Transaction<'_, Postgres>,
    sql: &OutboxSql,
    event_id: Uuid,
    worker_id: Uuid,
) -> Result<(), PublishError> {
    let result = sqlx::query(&sql.retry)
        .bind(event_id)
        .bind(worker_id)
        .execute(&mut **tx)
        .await
        .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
    if result.rows_affected() != 1 {
        return Err(PublishError::Infrastructure(
            "outbox retry lease was lost before acknowledgement".to_owned(),
        ));
    }
    Ok(())
}

const fn failure_code(error: &PublishError) -> &'static str {
    match error {
        PublishError::Broadcaster(_) => "broadcaster.error",
        PublishError::Infrastructure(_) => "infrastructure.error",
        // Task: CreateOnly write-once collision. Reachable once Bronze flips to
        // CreateOnly (NEXT task); kept as a distinct code for that observability.
        PublishError::ObjectAlreadyExists { .. } => "object.already_exists",
    }
}

#[derive(Clone, Copy, Default, Debug)]
/// Publication counts produced by a single polling tick.
pub struct TickStats {
    /// Number of rows successfully published and marked with `published_at`.
    pub published: u32,
    /// Number of rows whose broadcaster failed and whose retry count was incremented.
    pub retried: u32,
    /// Number of rows that reached the configured retry limit during this tick.
    pub dead_lettered: u32,
}

impl TickStats {
    const fn merge(&mut self, other: Self) {
        self.published = self.published.saturating_add(other.published);
        self.retried = self.retried.saturating_add(other.retried);
        self.dead_lettered = self.dead_lettered.saturating_add(other.dead_lettered);
    }
}
