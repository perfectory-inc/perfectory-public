use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use knowledge_application::{KnowledgeProjectionError, KnowledgeSourceRegistryPort};
use knowledge_domain::{KnowledgeSourceRecord, KnowledgeSourceUpserted};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

#[derive(Debug, thiserror::Error)]
pub enum PostgresKnowledgeSourceRegistryError {
    #[error("postgres knowledge source registry config is invalid")]
    InvalidConfig,
    #[error("postgres knowledge source registry failed: {message}")]
    StoreFailed { message: String },
}

impl PostgresKnowledgeSourceRegistryError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "postgres knowledge source registry config is invalid",
            Self::StoreFailed { .. } => "postgres knowledge source registry failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PostgresKnowledgeSourceRegistryConfig {
    database_url: String,
    timeout_seconds: u64,
    max_connections: u32,
}

impl PostgresKnowledgeSourceRegistryConfig {
    pub fn new(
        database_url: impl Into<String>,
        timeout_seconds: u64,
    ) -> Result<Self, PostgresKnowledgeSourceRegistryError> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() || timeout_seconds == 0 {
            return Err(PostgresKnowledgeSourceRegistryError::InvalidConfig);
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
    ) -> Result<Self, PostgresKnowledgeSourceRegistryError> {
        if max_connections == 0 {
            return Err(PostgresKnowledgeSourceRegistryError::InvalidConfig);
        }
        self.max_connections = max_connections;
        Ok(self)
    }
}

pub struct PostgresKnowledgeSourceRegistry {
    pool: PgPool,
}

impl std::fmt::Debug for PostgresKnowledgeSourceRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresKnowledgeSourceRegistry")
            .field("pool", &"PgPool { .. }")
            .finish()
    }
}

impl PostgresKnowledgeSourceRegistry {
    pub async fn connect(
        config: PostgresKnowledgeSourceRegistryConfig,
    ) -> Result<Self, PostgresKnowledgeSourceRegistryError> {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(config.timeout_seconds))
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|error| PostgresKnowledgeSourceRegistryError::StoreFailed {
                message: error.to_string(),
            })?;

        sqlx::migrate!("../../../migrations")
            .run(&pool)
            .await
            .map_err(|error| PostgresKnowledgeSourceRegistryError::StoreFailed {
                message: error.to_string(),
            })?;

        Ok(Self { pool })
    }

    #[doc(hidden)]
    pub async fn truncate_for_tests(&self) -> Result<(), sqlx::Error> {
        sqlx::query("TRUNCATE ip_source_registry")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl KnowledgeSourceRegistryPort for PostgresKnowledgeSourceRegistry {
    async fn upsert_source(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<KnowledgeSourceRecord, KnowledgeProjectionError> {
        let metadata = serde_json::to_value(&event.metadata)
            .map_err(|error| store_failed(error.to_string()))?;
        let row = sqlx::query(
            r#"
            WITH upsert AS (
                INSERT INTO ip_source_registry (
                    tenant_id,
                    product_id,
                    source_id,
                    source_kind,
                    source_uri,
                    content_uri,
                    content_checksum_sha256,
                    last_event_id,
                    last_seen_at_millis,
                    metadata,
                    source_version,
                    created_at,
                    updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, now(), now()
                )
                ON CONFLICT (tenant_id, product_id, source_id)
                DO UPDATE SET
                    source_kind = EXCLUDED.source_kind,
                    source_uri = EXCLUDED.source_uri,
                    content_uri = EXCLUDED.content_uri,
                    content_checksum_sha256 = EXCLUDED.content_checksum_sha256,
                    last_event_id = EXCLUDED.last_event_id,
                    last_seen_at_millis = EXCLUDED.last_seen_at_millis,
                    metadata = EXCLUDED.metadata,
                    source_version = ip_source_registry.source_version + 1,
                    updated_at = now()
                WHERE ip_source_registry.last_event_id <> EXCLUDED.last_event_id
                RETURNING
                    tenant_id,
                    product_id,
                    source_id,
                    source_kind,
                    source_uri,
                    content_uri,
                    content_checksum_sha256,
                    last_event_id,
                    last_seen_at_millis,
                    metadata,
                    source_version
            )
            SELECT
                tenant_id,
                product_id,
                source_id,
                source_kind,
                source_uri,
                content_uri,
                content_checksum_sha256,
                last_event_id,
                last_seen_at_millis,
                metadata,
                source_version
            FROM upsert
            UNION ALL
            SELECT
                tenant_id,
                product_id,
                source_id,
                source_kind,
                source_uri,
                content_uri,
                content_checksum_sha256,
                last_event_id,
                last_seen_at_millis,
                metadata,
                source_version
            FROM ip_source_registry
            WHERE tenant_id = $1
              AND product_id = $2
              AND source_id = $3
              AND last_event_id = $8
              AND NOT EXISTS (SELECT 1 FROM upsert)
            "#,
        )
        .bind(&event.tenant_id)
        .bind(&event.product_id)
        .bind(&event.source_id)
        .bind(&event.source_kind)
        .bind(&event.source_uri)
        .bind(&event.content_uri)
        .bind(&event.content_checksum_sha256)
        .bind(&event.event_id)
        .bind(event.occurred_at_millis)
        .bind(metadata)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| store_failed(error.to_string()))?;

        row_to_record(&row)
    }

    async fn get_source(
        &self,
        tenant_id: &str,
        product_id: &str,
        source_id: &str,
    ) -> Result<Option<KnowledgeSourceRecord>, KnowledgeProjectionError> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                product_id,
                source_id,
                source_kind,
                source_uri,
                content_uri,
                content_checksum_sha256,
                last_event_id,
                last_seen_at_millis,
                metadata,
                source_version
            FROM ip_source_registry
            WHERE tenant_id = $1 AND product_id = $2 AND source_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(product_id)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| store_failed(error.to_string()))?;

        row.as_ref().map(row_to_record).transpose()
    }
}

fn row_to_record(
    row: &sqlx::postgres::PgRow,
) -> Result<KnowledgeSourceRecord, KnowledgeProjectionError> {
    let metadata_json: serde_json::Value = row
        .try_get("metadata")
        .map_err(|error| store_failed(error.to_string()))?;
    let metadata: BTreeMap<String, String> = serde_json::from_value(metadata_json)
        .map_err(|error| store_failed(format!("metadata deserialize: {error}")))?;
    let version_i64: i64 = row
        .try_get("source_version")
        .map_err(|error| store_failed(error.to_string()))?;
    let version =
        u64::try_from(version_i64).map_err(|_| store_failed("source_version out of range"))?;

    Ok(KnowledgeSourceRecord {
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|error| store_failed(error.to_string()))?,
        product_id: row
            .try_get("product_id")
            .map_err(|error| store_failed(error.to_string()))?,
        source_id: row
            .try_get("source_id")
            .map_err(|error| store_failed(error.to_string()))?,
        source_kind: row
            .try_get("source_kind")
            .map_err(|error| store_failed(error.to_string()))?,
        source_uri: row
            .try_get("source_uri")
            .map_err(|error| store_failed(error.to_string()))?,
        content_uri: row
            .try_get("content_uri")
            .map_err(|error| store_failed(error.to_string()))?,
        content_checksum_sha256: row
            .try_get("content_checksum_sha256")
            .map_err(|error| store_failed(error.to_string()))?,
        last_event_id: row
            .try_get("last_event_id")
            .map_err(|error| store_failed(error.to_string()))?,
        last_seen_at_millis: row
            .try_get("last_seen_at_millis")
            .map_err(|error| store_failed(error.to_string()))?,
        metadata,
        version,
    })
}

fn store_failed(message: impl Into<String>) -> KnowledgeProjectionError {
    KnowledgeProjectionError::StoreUnavailable {
        message: message.into(),
    }
}
