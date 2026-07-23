use async_trait::async_trait;
use chrono::Utc;
use shared_kernel::id::{AuditLogMarker, Id, OutboxEventMarker, SystemAlertMarker};
use shared_kernel::mutation::MutationContext;
use system_alert_domain::repository::{RepoError, SystemAlertRepository};
use system_alert_domain::SystemAlert;
use tracing::instrument;

use super::rows::{row_to_alert, COLUMNS};
use super::PgSystemAlertRepository;
use crate::error_map::map_sqlx_err;

#[async_trait]
impl SystemAlertRepository for PgSystemAlertRepository {
    #[allow(clippy::needless_pass_by_value)]
    #[instrument(skip(self, alert, ctx), fields(
        alert_id = %alert.id.as_str(),
        severity = %alert.severity.as_db_str(),
        ctx_action = %ctx.action,
        correlation_id = %ctx.correlation_id,
        events_count = ctx.events.len(),
    ))]
    async fn save(&self, alert: &SystemAlert, ctx: MutationContext) -> Result<(), RepoError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        sqlx::query(
            r"
            insert into system_alert (
                id, severity, source, title, detail, metadata,
                acknowledged_at, acknowledged_by, resolved_at, created_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            on conflict (id) do update set
                severity = excluded.severity,
                source = excluded.source,
                title = excluded.title,
                detail = excluded.detail,
                metadata = excluded.metadata,
                acknowledged_at = excluded.acknowledged_at,
                acknowledged_by = excluded.acknowledged_by,
                resolved_at = excluded.resolved_at
            ",
        )
        .bind(alert.id.as_str())
        .bind(alert.severity.as_db_str())
        .bind(&alert.source)
        .bind(&alert.title)
        .bind(alert.detail.as_deref())
        .bind(&alert.metadata)
        .bind(alert.acknowledged_at)
        .bind(alert.acknowledged_by.as_ref().map(Id::as_str))
        .bind(alert.resolved_at)
        .bind(alert.created_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        let audit_id = Id::<AuditLogMarker>::new();
        let occurred_at = ctx.occurred_at.unwrap_or_else(Utc::now);
        sqlx::query(
            r"
            insert into audit_log (
                id, actor_id, action, resource_kind, resource_id,
                before_state, after_state, ip_address, user_agent,
                correlation_id, created_at
            )
            values ($1, $2, $3, 'system_alert', $4, NULL, $5, $6::inet, $7, $8, $9)
            ",
        )
        .bind(audit_id.as_str())
        .bind(ctx.actor_id.as_ref().map(Id::as_str))
        .bind(&ctx.action)
        .bind(alert.id.as_str())
        .bind(&ctx.metadata)
        .bind(ctx.client_ip.as_deref())
        .bind(ctx.user_agent.as_deref())
        .bind(&ctx.correlation_id)
        .bind(occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        for event in &ctx.events {
            let outbox_id = Id::<OutboxEventMarker>::new();
            sqlx::query(
                r"
                insert into outbox_event (
                    id, aggregate_kind, aggregate_id, event_type, payload,
                    correlation_id, created_at, published_at
                )
                values ($1, 'system_alert', $2, $3, $4, $5, $6, NULL)
                ",
            )
            .bind(outbox_id.as_str())
            .bind(alert.id.as_str())
            .bind(event.event_type())
            .bind(event.payload())
            .bind(&ctx.correlation_id)
            .bind(event.occurred_at())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        }

        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(())
    }

    #[instrument(skip(self), fields(alert_id = %id.as_str()))]
    async fn find_by_id(
        &self,
        id: &Id<SystemAlertMarker>,
    ) -> Result<Option<SystemAlert>, RepoError> {
        let sql = format!("select {COLUMNS} from system_alert where id = $1");
        let row = sqlx::query(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_alert).transpose()
    }

    #[instrument(skip(self), fields(limit))]
    async fn find_unacknowledged(&self, limit: u32) -> Result<Vec<SystemAlert>, RepoError> {
        let sql = format!(
            "select {COLUMNS} from system_alert \
             where acknowledged_at is null \
             order by \
                 case severity \
                     when 'critical' then 0 \
                     when 'error' then 1 \
                     when 'warning' then 2 \
                     when 'info' then 3 \
                 end, \
                 created_at desc \
             limit $1"
        );
        let rows = sqlx::query(&sql)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_alert).collect()
    }
}
