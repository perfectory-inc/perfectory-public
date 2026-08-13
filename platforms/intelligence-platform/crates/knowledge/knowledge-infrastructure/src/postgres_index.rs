use std::time::Duration;

use async_trait::async_trait;
use knowledge_application::{
    IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef, SearchHit, TenantScope,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

#[derive(Debug, thiserror::Error)]
pub enum PostgresKnowledgeIndexError {
    #[error("postgres knowledge index config is invalid")]
    InvalidConfig,
    #[error("postgres knowledge index failed: {message}")]
    StoreFailed { message: String },
}

impl PostgresKnowledgeIndexError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "postgres knowledge index config is invalid",
            Self::StoreFailed { .. } => "postgres knowledge index failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PostgresKnowledgeIndexConfig {
    database_url: String,
    timeout_seconds: u64,
    max_connections: u32,
}

impl PostgresKnowledgeIndexConfig {
    pub fn new(
        database_url: impl Into<String>,
        timeout_seconds: u64,
    ) -> Result<Self, PostgresKnowledgeIndexError> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() || timeout_seconds == 0 {
            return Err(PostgresKnowledgeIndexError::InvalidConfig);
        }
        Ok(Self {
            database_url,
            timeout_seconds,
            max_connections: 10,
        })
    }

    pub fn with_max_connections(
        mut self,
        max_connections: u32,
    ) -> Result<Self, PostgresKnowledgeIndexError> {
        if max_connections == 0 {
            return Err(PostgresKnowledgeIndexError::InvalidConfig);
        }
        self.max_connections = max_connections;
        Ok(self)
    }
}

pub struct PostgresKnowledgeIndex {
    pool: PgPool,
}

impl std::fmt::Debug for PostgresKnowledgeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresKnowledgeIndex")
            .field("pool", &"PgPool { .. }")
            .finish()
    }
}

impl PostgresKnowledgeIndex {
    pub async fn connect(
        config: PostgresKnowledgeIndexConfig,
    ) -> Result<Self, PostgresKnowledgeIndexError> {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(config.timeout_seconds))
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|error| PostgresKnowledgeIndexError::StoreFailed {
                message: error.to_string(),
            })?;

        sqlx::migrate!("../../../migrations")
            .run(&pool)
            .await
            .map_err(|error| PostgresKnowledgeIndexError::StoreFailed {
                message: error.to_string(),
            })?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl KnowledgeIndexPort for PostgresKnowledgeIndex {
    async fn index_chunks(
        &self,
        release: &ReleaseRef,
        chunks: Vec<IndexedChunk>,
    ) -> Result<u64, KnowledgeIndexError> {
        let mut tx = self.pool.begin().await.map_err(store_failed_from)?;

        sqlx::query(
            r#"
            INSERT INTO ip_knowledge_release (tenant_id, product_id, release_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (tenant_id, product_id, release_id) DO NOTHING
            "#,
        )
        .bind(&release.scope.tenant_id)
        .bind(&release.scope.product_id)
        .bind(&release.release_id)
        .execute(&mut *tx)
        .await
        .map_err(store_failed_from)?;

        let mut written = 0_u64;
        for indexed in &chunks {
            sqlx::query(
                r#"
                INSERT INTO ip_knowledge_chunk (
                    tenant_id, product_id, release_id, source_id, chunk_ordinal,
                    heading_path, body, source_snapshot_id, content_checksum_sha256
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (tenant_id, product_id, release_id, source_id, chunk_ordinal)
                DO UPDATE SET
                    heading_path = EXCLUDED.heading_path,
                    body = EXCLUDED.body,
                    source_snapshot_id = EXCLUDED.source_snapshot_id,
                    content_checksum_sha256 = EXCLUDED.content_checksum_sha256
                "#,
            )
            .bind(&release.scope.tenant_id)
            .bind(&release.scope.product_id)
            .bind(&release.release_id)
            .bind(&indexed.chunk.source_id)
            .bind(indexed.chunk.chunk_ordinal)
            .bind(&indexed.chunk.heading_path)
            .bind(&indexed.chunk.body)
            .bind(&indexed.source_snapshot_id)
            .bind(&indexed.chunk.content_checksum_sha256)
            .execute(&mut *tx)
            .await
            .map_err(store_failed_from)?;
            written += 1;
        }

        tx.commit().await.map_err(store_failed_from)?;
        Ok(written)
    }

    async fn activate_release(&self, release: &ReleaseRef) -> Result<(), KnowledgeIndexError> {
        let mut tx = self.pool.begin().await.map_err(store_failed_from)?;

        sqlx::query(
            r#"
            UPDATE ip_knowledge_release
            SET is_active = false
            WHERE tenant_id = $1 AND product_id = $2 AND is_active
            "#,
        )
        .bind(&release.scope.tenant_id)
        .bind(&release.scope.product_id)
        .execute(&mut *tx)
        .await
        .map_err(store_failed_from)?;

        let updated = sqlx::query(
            r#"
            UPDATE ip_knowledge_release
            SET is_active = true, activated_at = now()
            WHERE tenant_id = $1 AND product_id = $2 AND release_id = $3
            "#,
        )
        .bind(&release.scope.tenant_id)
        .bind(&release.scope.product_id)
        .bind(&release.release_id)
        .execute(&mut *tx)
        .await
        .map_err(store_failed_from)?;

        if updated.rows_affected() == 0 {
            return Err(KnowledgeIndexError::InvalidRequest {
                message: format!("release {} does not exist", release.release_id),
            });
        }

        tx.commit().await.map_err(store_failed_from)?;
        Ok(())
    }

    async fn search(
        &self,
        scope: &TenantScope,
        query: &str,
        limit: u32,
    ) -> Result<Vec<SearchHit>, KnowledgeIndexError> {
        if query.trim().is_empty() {
            return Err(KnowledgeIndexError::InvalidRequest {
                message: "query must be non-empty".to_string(),
            });
        }
        let rows = sqlx::query(
            r#"
            SELECT
                c.source_id,
                c.chunk_ordinal,
                c.heading_path,
                c.body,
                c.release_id
            FROM ip_knowledge_chunk c
            JOIN ip_knowledge_release r
              ON r.tenant_id = c.tenant_id
             AND r.product_id = c.product_id
             AND r.release_id = c.release_id
            WHERE c.tenant_id = $1
              AND c.product_id = $2
              AND r.is_active
              AND c.search_vector @@ websearch_to_tsquery('simple', $3)
            ORDER BY
                ts_rank(c.search_vector, websearch_to_tsquery('simple', $3)) DESC,
                c.source_id,
                c.chunk_ordinal
            LIMIT $4
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.product_id)
        .bind(query)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(store_failed_from)?;

        rows.iter().map(row_to_hit).collect()
    }

    async fn purge_scope(&self, scope: &TenantScope) -> Result<u64, KnowledgeIndexError> {
        if !scope.is_scaffold() {
            return Err(KnowledgeIndexError::ScopeNotPurgeable {
                tenant_id: scope.tenant_id.clone(),
            });
        }
        let mut tx = self.pool.begin().await.map_err(store_failed_from)?;

        let removed =
            sqlx::query("DELETE FROM ip_knowledge_chunk WHERE tenant_id = $1 AND product_id = $2")
                .bind(&scope.tenant_id)
                .bind(&scope.product_id)
                .execute(&mut *tx)
                .await
                .map_err(store_failed_from)?
                .rows_affected();

        sqlx::query("DELETE FROM ip_knowledge_release WHERE tenant_id = $1 AND product_id = $2")
            .bind(&scope.tenant_id)
            .bind(&scope.product_id)
            .execute(&mut *tx)
            .await
            .map_err(store_failed_from)?;

        tx.commit().await.map_err(store_failed_from)?;
        Ok(removed)
    }
}

fn row_to_hit(row: &sqlx::postgres::PgRow) -> Result<SearchHit, KnowledgeIndexError> {
    Ok(SearchHit {
        source_id: row.try_get("source_id").map_err(store_failed_from)?,
        chunk_ordinal: row.try_get("chunk_ordinal").map_err(store_failed_from)?,
        heading_path: row.try_get("heading_path").map_err(store_failed_from)?,
        body: row.try_get("body").map_err(store_failed_from)?,
        release_id: row.try_get("release_id").map_err(store_failed_from)?,
    })
}

fn store_failed_from(error: sqlx::Error) -> KnowledgeIndexError {
    KnowledgeIndexError::StoreUnavailable {
        message: error.to_string(),
    }
}
