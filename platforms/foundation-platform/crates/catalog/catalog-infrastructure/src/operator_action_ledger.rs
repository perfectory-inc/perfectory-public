//! The Postgres operator action ledger, `catalog.operator_action` (root ADR-0282 §2).

use async_trait::async_trait;
use catalog_application::ports::{
    OperatorActionLedger, OperatorActionLedgerError, OperatorActionRecord,
};
use sqlx::PgPool;

/// Appends operator requests to `catalog.operator_action`; the table's triggers keep it append-only.
pub struct PgOperatorActionLedger {
    pool: PgPool,
}

impl PgOperatorActionLedger {
    /// Creates the ledger over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OperatorActionLedger for PgOperatorActionLedger {
    async fn record(&self, record: &OperatorActionRecord) -> Result<(), OperatorActionLedgerError> {
        sqlx::query(
            "INSERT INTO catalog.operator_action
                 (id, job_id, action, drop_pending, actor, reason, requested_at, expires_at,
                  window_start, window_end)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(record.id)
        .bind(&record.job_id)
        .bind(record.action.as_str())
        .bind(record.drop_pending)
        .bind(record.actor.to_string())
        .bind(&record.reason)
        .bind(record.requested_at)
        .bind(record.expires_at)
        .bind(&record.window_start)
        .bind(&record.window_end)
        .execute(&self.pool)
        .await
        .map_err(|error| OperatorActionLedgerError::Unavailable(error.to_string()))?;
        Ok(())
    }
}
