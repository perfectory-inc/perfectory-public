//! `OutboxRepository` `Postgres` 구현체.
//!
//! 본 trait 의 [`OutboxRepository::save`] 는 caller 트랜잭션 *밖* 에서 단순
//! `INSERT` — `pool` 사용. 진짜 transactional outbox 는 6 도메인 `PgRepository`
//! (`Business Verification Queue`/`Listing Review Queue`/etc) 가 자기 트랜잭션 안에서 *raw SQL* 로 `outbox_event` `INSERT`
//! 직접 수행 (T5-T10).
//!
//! `DB` 컬럼 `created_at` ↔ entity 필드 `occurred_at` 매핑 — `INSERT` 시
//! `event.occurred_at` 을 `created_at` 컬럼에 바인드, `SELECT` 시 `created_at` 을
//! `occurred_at` 별칭으로 읽어요.

#![allow(clippy::module_name_repetitions)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use outbox_event_domain::entity::OutboxEvent;
use outbox_event_domain::repository::{OutboxRepository, RepoError};
use shared_kernel::id::{Id, OutboxEventMarker};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tracing::instrument;

use crate::error_map::map_sqlx_err;

/// `OutboxEvent` `Aggregate` 의 `Postgres` 저장소.
///
/// 본 저장소의 [`OutboxRepository::save`] 는 단순 `pool` `INSERT` 로 동작해요.
/// `Aggregate` save 와 같은 트랜잭션을 보장해야 하는 transactional outbox 는
/// 각 도메인 `PgRepository` 가 자기 트랜잭션 안에서 raw SQL 로 직접 `INSERT`
/// 해요 (T5-T10 에서).
#[derive(Debug, Clone)]
pub struct PgOutboxRepository {
    pool: PgPool,
}

impl PgOutboxRepository {
    /// 새 저장소를 만들어요.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Fails closed when the delivery-lease migration is missing.
    ///
    /// The publisher must not start and only discover a missing schema on its first tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema probe fails or migration 30018 is incomplete.
    pub async fn validate_delivery_lease_schema(&self) -> Result<(), RepoError> {
        let row = sqlx::query(DELIVERY_LEASE_SCHEMA_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        let has_lease_owner: bool = row.try_get("has_lease_owner").map_err(map_sqlx_err)?;
        let has_lease_until: bool = row.try_get("has_lease_until").map_err(map_sqlx_err)?;
        let has_claimable_index: bool = row.try_get("has_claimable_index").map_err(map_sqlx_err)?;
        validate_delivery_lease_schema_flags(has_lease_owner, has_lease_until, has_claimable_index)
            .map_err(RepoError::Database)
    }
}

const DELIVERY_LEASE_SCHEMA_SQL: &str = r"
select
    exists (
        select 1
        from information_schema.columns
        where table_schema = current_schema()
          and table_name = 'outbox_event'
          and column_name = 'lease_owner'
    ) as has_lease_owner,
    exists (
        select 1
        from information_schema.columns
        where table_schema = current_schema()
          and table_name = 'outbox_event'
          and column_name = 'lease_until'
    ) as has_lease_until,
    exists (
        select 1
        from pg_indexes
        where schemaname = current_schema()
          and tablename = 'outbox_event'
          and indexname = 'outbox_claimable_idx'
    ) as has_claimable_index
";

fn validate_delivery_lease_schema_flags(
    has_lease_owner: bool,
    has_lease_until: bool,
    has_claimable_index: bool,
) -> Result<(), String> {
    let mut missing = Vec::new();
    if !has_lease_owner {
        missing.push("outbox_event.lease_owner");
    }
    if !has_lease_until {
        missing.push("outbox_event.lease_until");
    }
    if !has_claimable_index {
        missing.push("outbox_claimable_idx");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "outbox delivery lease migration 30018 is not applied; missing {}",
            missing.join(", ")
        ))
    }
}

/// `select` 절에서 모든 `outbox_event` 컬럼을 일관되게 읽기 위한 상수.
///
/// `DB` 의 `created_at` 컬럼을 entity 필드명인 `occurred_at` 별칭으로 읽어요.
const OUTBOX_COLUMNS: &str = "event.id, event.event_type, event.aggregate_kind, \
    event.aggregate_id, event.payload, event.correlation_id, \
    event.created_at as occurred_at, event.published_at";

/// `PgRow` → [`OutboxEvent`] 변환. 8 필드 round-trip.
///
/// entity 의 `occurred_at` 필드는 `DB` 의 `created_at` 컬럼이 별칭으로 노출된
/// 값이에요 — [`OUTBOX_COLUMNS`] 참고.
fn row_to_outbox(row: &PgRow) -> Result<OutboxEvent, RepoError> {
    let id_str: String = row
        .try_get("id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let event_type: String = row
        .try_get("event_type")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let aggregate_kind: String = row
        .try_get("aggregate_kind")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let aggregate_id: String = row
        .try_get("aggregate_id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let payload: serde_json::Value = row
        .try_get("payload")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let correlation_id: String = row
        .try_get("correlation_id")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let occurred_at: DateTime<Utc> = row
        .try_get("occurred_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;
    let published_at: Option<DateTime<Utc>> = row
        .try_get("published_at")
        .map_err(|e| RepoError::Database(e.to_string()))?;

    let id = Id::<OutboxEventMarker>::try_from_str(&id_str)
        .map_err(|e| RepoError::Database(format!("malformed outbox id: {e}")))?;

    // `OutboxEvent` 는 직접 struct 리터럴로 생성 — entity 가 모든 필드 `pub` 이고
    // `from_domain` 외 다른 생성자 없음. `DB` round-trip 은 이미 검증된 값이므로
    // 도메인 invariants 재검증 불필요.
    Ok(OutboxEvent {
        id,
        event_type,
        aggregate_kind,
        aggregate_id,
        payload,
        occurred_at,
        published_at,
        correlation_id,
    })
}

#[async_trait]
impl OutboxRepository for PgOutboxRepository {
    #[instrument(
        skip(self, event),
        fields(
            event_id = %event.id.as_str(),
            event_type = %event.event_type,
            aggregate_kind = %event.aggregate_kind,
            correlation_id = %event.correlation_id,
        )
    )]
    async fn save(&self, event: &OutboxEvent) -> Result<(), RepoError> {
        sqlx::query(
            r"
            insert into outbox_event (
                id, aggregate_kind, aggregate_id, event_type, payload,
                correlation_id, created_at, published_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            ",
        )
        .bind(event.id.as_str())
        .bind(&event.aggregate_kind)
        .bind(&event.aggregate_id)
        .bind(&event.event_type)
        .bind(&event.payload)
        .bind(&event.correlation_id)
        .bind(event.occurred_at)
        .bind(event.published_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    #[instrument(skip(self), fields(limit))]
    async fn claim_unpublished(
        &self,
        limit: u32,
        worker_id: &str,
        lease_for: Duration,
    ) -> Result<Vec<OutboxEvent>, RepoError> {
        let lease_seconds = i64::try_from(lease_for.as_secs())
            .map_err(|_| RepoError::Database("outbox lease duration is too large".to_owned()))?;
        let sql = format!(
            r"
            with candidates as (
                select id
                from outbox_event
                where published_at is null
                  and (lease_until is null or lease_until <= now())
                order by created_at asc
                limit $1
                for update skip locked
            ), claimed as (
                update outbox_event as event
                set lease_owner = $2,
                    lease_until = now() + ($3::bigint * interval '1 second')
                from candidates
                where event.id = candidates.id
                returning {OUTBOX_COLUMNS}
            )
            select * from claimed order by occurred_at asc
            "
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .bind(worker_id)
            .bind(lease_seconds)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_outbox).collect()
    }

    #[instrument(skip(self), fields(event_id = %id.as_str()))]
    async fn mark_published(
        &self,
        id: &Id<OutboxEventMarker>,
        worker_id: &str,
        at: DateTime<Utc>,
    ) -> Result<(), RepoError> {
        let result = sqlx::query(
            "update outbox_event \
             set published_at = $1, lease_owner = null, lease_until = null \
             where id = $2 and published_at is null and lease_owner = $3",
        )
        .bind(at)
        .bind(id.as_str())
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        if result.rows_affected() == 0 {
            return Err(RepoError::NotFound);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::validate_delivery_lease_schema_flags;

    #[test]
    fn delivery_lease_schema_requires_all_migration_objects() {
        assert!(validate_delivery_lease_schema_flags(true, true, true).is_ok());

        for flags in [
            (false, true, true),
            (true, false, true),
            (true, true, false),
        ] {
            let result = validate_delivery_lease_schema_flags(flags.0, flags.1, flags.2);
            assert!(matches!(result, Err(error) if error.contains("30018")));
        }
    }
}
