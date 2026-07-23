use async_trait::async_trait;
use chrono::{DateTime, Utc};
use featured_content_domain::repository::{FeaturedContentRepository, RepoError};
use featured_content_domain::{FeaturedContent, FeaturedContentFeatureKind};
use shared_kernel::id::{AuditLogMarker, FeaturedContentMarker, Id, OutboxEventMarker};
use shared_kernel::mutation::MutationContext;
use tracing::instrument;

use super::rows::{row_to_featured, COLUMNS};
use super::PgFeaturedContentRepository;
use crate::error_map::map_sqlx_err;

#[async_trait]
impl FeaturedContentRepository for PgFeaturedContentRepository {
    #[allow(clippy::needless_pass_by_value)]
    #[instrument(skip(self, featured_content, ctx), fields(
        featured_content_id = %featured_content.id.as_str(),
        feature_kind = %featured_content.feature_kind.as_db_str(),
        ctx_action = %ctx.action,
        correlation_id = %ctx.correlation_id,
        events_count = ctx.events.len(),
    ))]
    async fn save(
        &self,
        featured_content: &FeaturedContent,
        ctx: MutationContext,
    ) -> Result<(), RepoError> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        sqlx::query(
            r"
            insert into featured_content (
                id, target_kind, target_id, feature_kind, weight,
                starts_at, ends_at, purchased_by,
                impression_count, click_count, created_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            on conflict (id) do update set
                target_kind = excluded.target_kind,
                target_id = excluded.target_id,
                feature_kind = excluded.feature_kind,
                weight = excluded.weight,
                starts_at = excluded.starts_at,
                ends_at = excluded.ends_at,
                purchased_by = excluded.purchased_by,
                impression_count = excluded.impression_count,
                click_count = excluded.click_count
            ",
        )
        .bind(featured_content.id.as_str())
        .bind(featured_content.target_kind.as_db_str())
        .bind(&featured_content.target_id)
        .bind(featured_content.feature_kind.as_db_str())
        .bind(featured_content.weight)
        .bind(featured_content.starts_at)
        .bind(featured_content.ends_at)
        .bind(featured_content.purchased_by.as_ref().map(Id::as_str))
        .bind(featured_content.impression_count)
        .bind(featured_content.click_count)
        .bind(featured_content.created_at)
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
            values ($1, $2, $3, 'featured_content', $4, NULL, $5, $6::inet, $7, $8, $9)
            ",
        )
        .bind(audit_id.as_str())
        .bind(ctx.actor_id.as_ref().map(Id::as_str))
        .bind(&ctx.action)
        .bind(featured_content.id.as_str())
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
                values ($1, 'featured_content', $2, $3, $4, $5, $6, NULL)
                ",
            )
            .bind(outbox_id.as_str())
            .bind(featured_content.id.as_str())
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

    #[instrument(skip(self), fields(featured_content_id = %id.as_str()))]
    async fn find_by_id(
        &self,
        id: &Id<FeaturedContentMarker>,
    ) -> Result<Option<FeaturedContent>, RepoError> {
        let sql = format!("select {COLUMNS} from featured_content where id = $1");
        let row = sqlx::query(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_featured).transpose()
    }

    #[instrument(skip(self), fields(feature_kind = %feature_kind.as_db_str()))]
    async fn find_active(
        &self,
        feature_kind: FeaturedContentFeatureKind,
        at: DateTime<Utc>,
    ) -> Result<Vec<FeaturedContent>, RepoError> {
        let sql = format!(
            "select {COLUMNS} from featured_content \
             where feature_kind = $1 and starts_at <= $2 and $2 < ends_at \
             order by weight desc, created_at asc"
        );
        let rows = sqlx::query(&sql)
            .bind(feature_kind.as_db_str())
            .bind(at)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_featured).collect()
    }
}
