//! The Postgres operator action ledger, `catalog.operator_action` (root ADR-0282 §2).

use async_trait::async_trait;
use catalog_application::ports::{
    AuditAppend, OperatorActionLedger, OperatorActionLedgerError, OperatorActionRecord,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::audit_events::{append_in, begin_bounded};

/// Appends operator requests to `catalog.operator_action`; the table's triggers keep it append-only
/// and write each request's audit row in the same transaction (root ADR-0569 §3).
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
        let unavailable =
            |error: sqlx::Error| OperatorActionLedgerError::Unavailable(error.to_string());
        // The audit trigger waits for the chain's lock; a request waits two seconds at most and is
        // then refused whole, ledger row and audit row together.
        let mut tx = begin_bounded(&self.pool).await.map_err(unavailable)?;
        sqlx::query(
            "INSERT INTO catalog.operator_action
                 (id, job_id, action, drop_pending, actor, reason, requested_at, expires_at,
                  window_start, window_end, edition, lanes, generation)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
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
        .bind(&record.edition)
        .bind(&record.lanes)
        .bind(record.generation)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn record_switch(
        &self,
        record: &OperatorActionRecord,
        failure: Option<&str>,
    ) -> Result<(), OperatorActionLedgerError> {
        let unavailable =
            |error: sqlx::Error| OperatorActionLedgerError::Unavailable(error.to_string());
        let runner = record.job_id == catalog_domain::operator_action::CUTOVER_SUBJECT;
        let event = AuditAppend {
            event_id: Uuid::new_v4(),
            occurred_at: chrono::Utc::now(),
            source: "dawneer".to_owned(),
            source_event_id: format!("operator_action:{}:switched", record.id),
            action: format!("operator_action.{}", record.action.as_str()),
            operation: "execute".to_owned(),
            actor_kind: "staff".to_owned(),
            actor_staff_id: Some(record.actor.as_uuid()),
            object_type: if runner { "lane" } else { "job" }.to_owned(),
            object_id: if runner {
                record
                    .lanes
                    .clone()
                    .unwrap_or_else(|| record.job_id.clone())
            } else {
                record.job_id.clone()
            },
            after: None,
            reason: None,
            applied_via: Some(("api".to_owned(), record.id.to_string())),
            result: if failure.is_some() {
                "failed"
            } else {
                "succeeded"
            }
            .to_owned(),
            error: failure.map(|why| why.chars().take(1000).collect()),
            change_id: Some(record.id),
        };
        let mut tx = begin_bounded(&self.pool).await.map_err(unavailable)?;
        append_in(&mut tx, &event).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
}
