//! `PostgreSQL` adapter for Identity's leased transactional outbox.

use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::worker::{ClaimRequest, LeasedOutboxEvent, OutboxRepository, RepositoryError};

/// Atomic due-row claim that stores leases before the transaction commits.
pub const CLAIM_DUE_SQL: &str = "WITH due AS (
         SELECT event_id
         FROM identity.outbox_event
         WHERE published_at IS NULL
           AND attempt_count < 1000
           AND next_attempt_at <= now()
           AND (lease_expires_at IS NULL OR lease_expires_at <= now())
         ORDER BY next_attempt_at, occurred_at, event_id
         LIMIT 1
         FOR UPDATE SKIP LOCKED
     )
     UPDATE identity.outbox_event AS outbox
     SET lease_owner = $1,
         claim_token = $2,
         lease_expires_at = now() + ($3::double precision * INTERVAL '1 second')
     FROM due
     WHERE outbox.event_id = due.event_id
     RETURNING outbox.event_id, outbox.type, outbox.payload, outbox.occurred_at,
               outbox.attempt_count, outbox.claim_token";

/// Fenced success update that only accepts the current unexpired claim.
pub const MARK_PUBLISHED_SQL: &str = "UPDATE identity.outbox_event
     SET published_at = now(), lease_owner = NULL, claim_token = NULL, lease_expires_at = NULL,
         last_error_code = NULL
     WHERE event_id = $1
       AND lease_owner = $2
       AND claim_token = $3
       AND published_at IS NULL
       AND lease_expires_at > now()";

/// Fenced failure update that only accepts the current unexpired claim.
pub const RECORD_FAILURE_SQL: &str = "UPDATE identity.outbox_event
     SET attempt_count = LEAST(attempt_count, 999) + 1,
         next_attempt_at = now() + ($4::double precision * INTERVAL '1 second'),
         lease_owner = NULL,
         claim_token = NULL,
         lease_expires_at = NULL,
         last_error_code = $5
     WHERE event_id = $1
       AND lease_owner = $2
       AND claim_token = $3
       AND published_at IS NULL
       AND lease_expires_at > now()";

/// `PostgreSQL` implementation of the worker-local outbox repository port.
#[derive(Clone)]
pub struct PgOutboxRepository {
    pool: PgPool,
    operation_timeout: Duration,
}

impl PgOutboxRepository {
    /// Creates an Identity outbox repository.
    #[must_use]
    pub const fn new(pool: PgPool, operation_timeout: Duration) -> Self {
        Self {
            pool,
            operation_timeout,
        }
    }
}

#[async_trait]
impl OutboxRepository for PgOutboxRepository {
    async fn claim_due(
        &self,
        request: &ClaimRequest,
    ) -> Result<Option<LeasedOutboxEvent>, RepositoryError> {
        let mut transaction = bounded_operation(
            self.operation_timeout,
            self.pool.begin(),
            RepositoryError::Begin,
        )
        .await?;
        let row = bounded_operation(
            self.operation_timeout,
            sqlx::query(CLAIM_DUE_SQL)
                .bind(&request.lease_owner)
                .bind(request.claim_token)
                .bind(request.lease_duration.as_secs_f64())
                .fetch_optional(&mut *transaction),
            RepositoryError::Claim,
        )
        .await?;
        let event = row.as_ref().map(map_claimed_row).transpose()?;
        bounded_operation(
            self.operation_timeout,
            transaction.commit(),
            RepositoryError::Commit,
        )
        .await?;
        Ok(event)
    }

    async fn mark_published(
        &self,
        event_id: Uuid,
        lease_owner: &str,
        claim_token: Uuid,
    ) -> Result<(), RepositoryError> {
        let result = bounded_operation(
            self.operation_timeout,
            sqlx::query(MARK_PUBLISHED_SQL)
                .bind(event_id)
                .bind(lease_owner)
                .bind(claim_token)
                .execute(&self.pool),
            RepositoryError::Update,
        )
        .await?;
        exactly_one_row(result.rows_affected())
    }

    async fn record_failure(
        &self,
        event_id: Uuid,
        lease_owner: &str,
        claim_token: Uuid,
        retry_after: Duration,
        error_code: &'static str,
    ) -> Result<(), RepositoryError> {
        if error_code.len() > 64 {
            return Err(RepositoryError::Update);
        }
        let result = bounded_operation(
            self.operation_timeout,
            sqlx::query(RECORD_FAILURE_SQL)
                .bind(event_id)
                .bind(lease_owner)
                .bind(claim_token)
                .bind(retry_after.as_secs_f64())
                .bind(error_code)
                .execute(&self.pool),
            RepositoryError::Update,
        )
        .await?;
        exactly_one_row(result.rows_affected())
    }
}

async fn bounded_operation<T, E, F>(
    operation_timeout: Duration,
    future: F,
    error: RepositoryError,
) -> Result<T, RepositoryError>
where
    F: Future<Output = Result<T, E>>,
{
    tokio::time::timeout(operation_timeout, future)
        .await
        .map_err(|_| error)?
        .map_err(|_| error)
}

fn map_claimed_row(row: &sqlx::postgres::PgRow) -> Result<LeasedOutboxEvent, RepositoryError> {
    Ok(LeasedOutboxEvent {
        event_id: row
            .try_get::<Uuid, _>("event_id")
            .map_err(|_| RepositoryError::Claim)?,
        event_type: row
            .try_get::<String, _>("type")
            .map_err(|_| RepositoryError::Claim)?,
        payload: row
            .try_get::<Value, _>("payload")
            .map_err(|_| RepositoryError::Claim)?,
        occurred_at: row
            .try_get::<DateTime<Utc>, _>("occurred_at")
            .map_err(|_| RepositoryError::Claim)?,
        attempt_count: row
            .try_get::<i32, _>("attempt_count")
            .map_err(|_| RepositoryError::Claim)?,
        claim_token: row
            .try_get::<Uuid, _>("claim_token")
            .map_err(|_| RepositoryError::Claim)?,
    })
}

const fn exactly_one_row(rows_affected: u64) -> Result<(), RepositoryError> {
    match rows_affected {
        0 => Err(RepositoryError::LostLease),
        1 => Ok(()),
        _ => Err(RepositoryError::Update),
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use super::{bounded_operation, exactly_one_row};
    use crate::worker::RepositoryError;

    #[test]
    fn zero_updated_rows_is_a_typed_lost_lease() {
        assert_eq!(exactly_one_row(0), Err(RepositoryError::LostLease));
    }

    #[test]
    fn multiple_updated_rows_is_an_update_error() {
        assert_eq!(exactly_one_row(2), Err(RepositoryError::Update));
    }

    #[tokio::test]
    async fn repository_operation_timeouts_preserve_each_boundary_error() {
        for error in [
            RepositoryError::Begin,
            RepositoryError::Claim,
            RepositoryError::Commit,
            RepositoryError::Update,
        ] {
            let result =
                bounded_operation(Duration::from_millis(1), pending::<Result<(), ()>>(), error)
                    .await;

            assert_eq!(result, Err(error));
        }
    }
}
