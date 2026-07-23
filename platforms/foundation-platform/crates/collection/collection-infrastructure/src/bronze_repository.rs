//! `SQLx` repository and unit-of-work implementations for Collection Bronze ingestion metadata.

use async_trait::async_trait;
use collection_application::{
    bronze_catalog_recovery::{
        ApplyBronzeCatalogRecoveryCommand, BronzeCatalogRecoveryCatalogWriter,
    },
    ports::{BronzeIngestRepository, BronzeIngestUnitOfWork, CompleteIngestionRunCommand},
};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, IngestionRunStatus, SchemaProfile,
    SourceCatalogEntry,
};
use foundation_shared_kernel::ids::{IngestionRunId, SourceCatalogId};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::row_map::{
    row_to_bronze_object, row_to_ingestion_run, row_to_schema_profile, row_to_source_catalog_entry,
};

const SOURCE_CATALOG_COLUMNS: &str = "id, slug, name, provider, dataset_name, base_url, auth_kind,
 payload_format, license_name, license_url, terms_url, collection_frequency, is_active,
 created_at, updated_at, version";

const INGESTION_RUN_COLUMNS: &str = "id, source_catalog_id, trigger, status, request_params,
 started_at, finished_at, logical_records_seen, objects_written, error_message, created_at,
 updated_at, version";

const BRONZE_OBJECT_COLUMNS: &str = "id, source_catalog_id, ingestion_run_id, source_record_id,
 source_partition_key, source_identity_key, dedupe_key, request_params, object_key,
 checksum_sha256, content_type, size_bytes, logical_record_count, collected_at, snapshot_period,
 snapshot_date, snapshot_granularity, snapshot_basis, provider_file_id, provider_file_name,
 provider_updated_at, effective_date, created_at";

const SCHEMA_PROFILE_COLUMNS: &str = "id, source_catalog_id, ingestion_run_id, field_path,
 observed_type, nonnull_count, null_count, sample_values, candidate_key_score, profiled_at,
 created_at, updated_at, version";

fn map_sqlx(error: &sqlx::Error) -> CollectionError {
    CollectionError::Infrastructure(error.to_string())
}

/// `PostgreSQL` implementation of Bronze ingestion read-only queries.
pub struct PgBronzeIngestRepository {
    pool: PgPool,
}

impl PgBronzeIngestRepository {
    /// Creates a repository backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `PostgreSQL` implementation of Bronze ingestion mutation operations.
pub struct PgBronzeIngestUnitOfWork {
    pool: PgPool,
}

impl PgBronzeIngestUnitOfWork {
    /// Creates a unit-of-work backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BronzeIngestRepository for PgBronzeIngestRepository {
    async fn find_source_catalog_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<SourceCatalogEntry>, CollectionError> {
        let query = format!(
            "SELECT {SOURCE_CATALOG_COLUMNS}
             FROM catalog.source_catalog
             WHERE slug = $1"
        );
        let row = sqlx::query(&query)
            .bind(slug)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        row.as_ref().map(row_to_source_catalog_entry).transpose()
    }

    async fn find_ingestion_run(
        &self,
        id: IngestionRunId,
    ) -> Result<Option<IngestionRun>, CollectionError> {
        let query = format!(
            "SELECT {INGESTION_RUN_COLUMNS}
             FROM catalog.ingestion_run
             WHERE id = $1"
        );
        let row = sqlx::query(&query)
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        row.as_ref().map(row_to_ingestion_run).transpose()
    }

    async fn list_bronze_objects_by_run(
        &self,
        run_id: IngestionRunId,
    ) -> Result<Vec<BronzeObject>, CollectionError> {
        let query = format!(
            "SELECT {BRONZE_OBJECT_COLUMNS}
             FROM catalog.bronze_object
             WHERE ingestion_run_id = $1
             ORDER BY collected_at, id"
        );
        let rows = sqlx::query(&query)
            .bind(run_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        rows.iter().map(row_to_bronze_object).collect()
    }

    async fn find_bronze_object_by_source_partition_key(
        &self,
        source_catalog_id: SourceCatalogId,
        source_partition_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        let query = format!(
            "SELECT {BRONZE_OBJECT_COLUMNS}
             FROM catalog.bronze_object
             WHERE source_catalog_id = $1 AND source_partition_key = $2
             ORDER BY collected_at DESC, created_at DESC, id DESC
             LIMIT 1"
        );
        let row = sqlx::query(&query)
            .bind(source_catalog_id.as_uuid())
            .bind(source_partition_key.trim())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        row.as_ref().map(row_to_bronze_object).transpose()
    }

    async fn list_schema_profiles_by_run(
        &self,
        run_id: IngestionRunId,
    ) -> Result<Vec<SchemaProfile>, CollectionError> {
        let query = format!(
            "SELECT {SCHEMA_PROFILE_COLUMNS}
             FROM catalog.schema_profile
             WHERE ingestion_run_id = $1
             ORDER BY field_path"
        );
        let rows = sqlx::query(&query)
            .bind(run_id.as_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        rows.iter().map(row_to_schema_profile).collect()
    }
}

#[async_trait]
impl BronzeIngestUnitOfWork for PgBronzeIngestUnitOfWork {
    async fn upsert_source_catalog_entry(
        &self,
        entry: &SourceCatalogEntry,
    ) -> Result<SourceCatalogEntry, CollectionError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| map_sqlx(&error))?;
        upsert_source_catalog_entry_on(&mut connection, entry).await
    }

    async fn create_ingestion_run(
        &self,
        run: &IngestionRun,
    ) -> Result<IngestionRun, CollectionError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| map_sqlx(&error))?;
        create_ingestion_run_on(&mut connection, run).await
    }

    async fn complete_ingestion_run(
        &self,
        command: CompleteIngestionRunCommand,
    ) -> Result<IngestionRun, CollectionError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| map_sqlx(&error))?;
        complete_ingestion_run_on(&mut connection, &command).await
    }

    async fn find_bronze_object_by_object_key(
        &self,
        source_catalog_id: SourceCatalogId,
        object_key: &str,
    ) -> Result<Option<BronzeObject>, CollectionError> {
        let query = format!(
            "SELECT {BRONZE_OBJECT_COLUMNS}
             FROM catalog.bronze_object
             WHERE source_catalog_id = $1 AND object_key = $2
             ORDER BY collected_at DESC, created_at DESC, id DESC
             LIMIT 1"
        );
        let row = sqlx::query(&query)
            .bind(source_catalog_id.as_uuid())
            .bind(object_key.trim())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        row.as_ref().map(row_to_bronze_object).transpose()
    }

    async fn record_bronze_object(
        &self,
        object: &BronzeObject,
    ) -> Result<BronzeObject, CollectionError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| map_sqlx(&error))?;
        record_bronze_object_on(&mut connection, object).await
    }

    async fn upsert_schema_profile(
        &self,
        profile: &SchemaProfile,
    ) -> Result<SchemaProfile, CollectionError> {
        let nonnull_count = u64_to_i64("nonnull_count", profile.nonnull_count)?;
        let null_count = u64_to_i64("null_count", profile.null_count)?;
        let query = format!(
            "INSERT INTO catalog.schema_profile
             ({SCHEMA_PROFILE_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT (ingestion_run_id, field_path) DO UPDATE
             SET observed_type = EXCLUDED.observed_type,
                 nonnull_count = EXCLUDED.nonnull_count,
                 null_count = EXCLUDED.null_count,
                 sample_values = EXCLUDED.sample_values,
                 candidate_key_score = EXCLUDED.candidate_key_score,
                 profiled_at = EXCLUDED.profiled_at,
                 updated_at = now(),
                 version = catalog.schema_profile.version + 1
             RETURNING {SCHEMA_PROFILE_COLUMNS}"
        );
        let row = sqlx::query(&query)
            .bind(profile.id.as_uuid())
            .bind(profile.source_catalog_id.as_uuid())
            .bind(profile.ingestion_run_id.as_uuid())
            .bind(profile.field_path.trim())
            .bind(profile.observed_type.wire_name())
            .bind(nonnull_count)
            .bind(null_count)
            .bind(&profile.sample_values)
            .bind(profile.candidate_key_score)
            .bind(profile.profiled_at)
            .bind(profile.created_at)
            .bind(profile.updated_at)
            .bind(profile.version)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| map_sqlx(&error))?;

        row_to_schema_profile(&row)
    }
}

#[async_trait]
impl BronzeCatalogRecoveryCatalogWriter for PgBronzeIngestUnitOfWork {
    async fn apply_recovery(
        &self,
        command: ApplyBronzeCatalogRecoveryCommand,
    ) -> Result<IngestionRun, CollectionError> {
        validate_recovery_command(&command)?;
        let ApplyBronzeCatalogRecoveryCommand {
            source,
            mut run,
            mut objects,
            completion,
        } = command;

        let mut transaction = self.pool.begin().await.map_err(|error| map_sqlx(&error))?;
        let source = upsert_source_catalog_entry_on(&mut transaction, &source).await?;
        run.source_catalog_id = source.id;
        let run = create_ingestion_run_on(&mut transaction, &run).await?;

        for object in &mut objects {
            object.source_catalog_id = source.id;
            object.ingestion_run_id = run.id;
            record_bronze_object_on(&mut transaction, object).await?;
        }

        let completed = complete_ingestion_run_on(&mut transaction, &completion).await?;
        transaction
            .commit()
            .await
            .map_err(|error| map_sqlx(&error))?;
        Ok(completed)
    }
}

async fn upsert_source_catalog_entry_on(
    connection: &mut PgConnection,
    entry: &SourceCatalogEntry,
) -> Result<SourceCatalogEntry, CollectionError> {
    let query = format!(
        "INSERT INTO catalog.source_catalog
         ({SOURCE_CATALOG_COLUMNS})
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
         ON CONFLICT (slug) DO UPDATE
         SET name = EXCLUDED.name,
             provider = EXCLUDED.provider,
             dataset_name = EXCLUDED.dataset_name,
             base_url = EXCLUDED.base_url,
             auth_kind = EXCLUDED.auth_kind,
             payload_format = EXCLUDED.payload_format,
             license_name = EXCLUDED.license_name,
             license_url = EXCLUDED.license_url,
             terms_url = EXCLUDED.terms_url,
             collection_frequency = EXCLUDED.collection_frequency,
             is_active = EXCLUDED.is_active,
             updated_at = now(),
             version = catalog.source_catalog.version + 1
         RETURNING {SOURCE_CATALOG_COLUMNS}"
    );
    let row = sqlx::query(&query)
        .bind(entry.id.as_uuid())
        .bind(entry.slug.trim())
        .bind(entry.name.trim())
        .bind(entry.provider.trim())
        .bind(entry.dataset_name.trim())
        .bind(entry.base_url.as_deref())
        .bind(entry.auth_kind.wire_name())
        .bind(entry.payload_format.wire_name())
        .bind(entry.license_name.as_deref())
        .bind(entry.license_url.as_deref())
        .bind(entry.terms_url.as_deref())
        .bind(entry.collection_frequency.as_deref())
        .bind(entry.is_active)
        .bind(entry.created_at)
        .bind(entry.updated_at)
        .bind(entry.version)
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| map_sqlx(&error))?;

    row_to_source_catalog_entry(&row)
}

async fn create_ingestion_run_on(
    connection: &mut PgConnection,
    run: &IngestionRun,
) -> Result<IngestionRun, CollectionError> {
    let logical_records_seen = u64_to_i64("logical_records_seen", run.logical_records_seen)?;
    let objects_written = u64_to_i64("objects_written", run.objects_written)?;
    let query = format!(
        "INSERT INTO catalog.ingestion_run
         ({INGESTION_RUN_COLUMNS})
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         RETURNING {INGESTION_RUN_COLUMNS}"
    );
    let row = sqlx::query(&query)
        .bind(run.id.as_uuid())
        .bind(run.source_catalog_id.as_uuid())
        .bind(run.trigger.wire_name())
        .bind(run.status.wire_name())
        .bind(&run.request_params)
        .bind(run.started_at)
        .bind(run.finished_at)
        .bind(logical_records_seen)
        .bind(objects_written)
        .bind(run.error_message.as_deref())
        .bind(run.created_at)
        .bind(run.updated_at)
        .bind(run.version)
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| map_sqlx(&error))?;

    row_to_ingestion_run(&row)
}

async fn complete_ingestion_run_on(
    connection: &mut PgConnection,
    command: &CompleteIngestionRunCommand,
) -> Result<IngestionRun, CollectionError> {
    validate_terminal_status(command.status)?;
    let logical_records_seen = u64_to_i64("logical_records_seen", command.logical_records_seen)?;
    let objects_written = u64_to_i64("objects_written", command.objects_written)?;
    let query = format!(
        "UPDATE catalog.ingestion_run
         SET status = $2,
             finished_at = $3,
             logical_records_seen = $4,
             objects_written = $5,
             error_message = $6,
             updated_at = now(),
             version = version + 1
         WHERE id = $1
         RETURNING {INGESTION_RUN_COLUMNS}"
    );
    let row = sqlx::query(&query)
        .bind(command.id.as_uuid())
        .bind(command.status.wire_name())
        .bind(command.finished_at)
        .bind(logical_records_seen)
        .bind(objects_written)
        .bind(command.error_message.as_deref())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| map_sqlx(&error))?
        .ok_or_else(|| CollectionError::IngestionRunNotFound(command.id.to_string()))?;

    row_to_ingestion_run(&row)
}

async fn record_bronze_object_on(
    connection: &mut PgConnection,
    object: &BronzeObject,
) -> Result<BronzeObject, CollectionError> {
    let size_bytes = u64_to_i64("size_bytes", object.size_bytes)?;
    let logical_record_count = object
        .logical_record_count
        .map(|count| u64_to_i64("logical_record_count", count))
        .transpose()?;
    let query = record_bronze_object_query();
    let row = sqlx::query(&query)
        .bind(object.id.as_uuid())
        .bind(object.source_catalog_id.as_uuid())
        .bind(object.ingestion_run_id.as_uuid())
        .bind(object.source_record_id.map(|id| id.as_uuid()))
        .bind(object.source_partition_key.as_deref())
        .bind(object.source_identity_key.trim())
        .bind(object.dedupe_key.trim())
        .bind(&object.request_params)
        .bind(object.object_key.as_str())
        .bind(object.checksum_sha256.trim())
        .bind(object.content_type.trim())
        .bind(size_bytes)
        .bind(logical_record_count)
        .bind(object.collected_at)
        .bind(object.snapshot_period.as_deref())
        .bind(object.snapshot_date)
        .bind(object.snapshot_granularity.as_str())
        .bind(object.snapshot_basis.as_str())
        .bind(object.provider_file_id.as_deref())
        .bind(object.provider_file_name.as_deref())
        .bind(object.provider_updated_at)
        .bind(object.effective_date)
        .bind(object.created_at)
        .bind(Uuid::now_v7())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| map_sqlx(&error))?
        .ok_or_else(|| {
            CollectionError::Infrastructure(format!(
                "bronze object upsert returned no row (source_catalog_id={}, dedupe_key={:?})",
                object.source_catalog_id, object.dedupe_key
            ))
        })?;

    row_to_bronze_object(&row)
}

fn validate_recovery_command(
    command: &ApplyBronzeCatalogRecoveryCommand,
) -> Result<(), CollectionError> {
    if command.run.status != IngestionRunStatus::Running {
        return Err(CollectionError::Infrastructure(
            "Bronze Catalog recovery run must start in running status".to_owned(),
        ));
    }
    if command.completion.id != command.run.id {
        return Err(CollectionError::Infrastructure(
            "Bronze Catalog recovery completion run id does not match".to_owned(),
        ));
    }
    if command.completion.status != IngestionRunStatus::Succeeded {
        return Err(CollectionError::Infrastructure(
            "Bronze Catalog recovery atomic batch must complete as succeeded".to_owned(),
        ));
    }
    let object_count = u64::try_from(command.objects.len()).map_err(|_| {
        CollectionError::Infrastructure("Bronze Catalog recovery object count overflow".to_owned())
    })?;
    if command.completion.logical_records_seen != object_count
        || command.completion.objects_written != 0
    {
        return Err(CollectionError::Infrastructure(
            "Bronze Catalog recovery completion metrics are inconsistent".to_owned(),
        ));
    }
    if command.run.source_catalog_id != command.source.id
        || command.objects.iter().any(|object| {
            object.source_catalog_id != command.source.id
                || object.ingestion_run_id != command.run.id
        })
    {
        return Err(CollectionError::Infrastructure(
            "Bronze Catalog recovery batch ids are inconsistent".to_owned(),
        ));
    }
    Ok(())
}

fn record_bronze_object_query() -> String {
    format!(
        "WITH existing AS (
                 SELECT id, object_key
                 FROM catalog.bronze_object
                 WHERE source_catalog_id = $2 AND (dedupe_key = $7 OR object_key = $9)
                 ORDER BY
                     CASE WHEN dedupe_key = $7 THEN 0 ELSE 1 END,
                     collected_at DESC,
                     created_at DESC,
                     id DESC
                 LIMIT 1
             ),
             updated AS (
                 UPDATE catalog.bronze_object
                 SET ingestion_run_id = $3,
                     source_record_id = $4,
                     source_partition_key = $5,
                     source_identity_key = $6,
                     dedupe_key = $7,
                     request_params = $8,
                     object_key = $9,
                     checksum_sha256 = $10,
                     content_type = $11,
                     size_bytes = $12,
                     logical_record_count = $13,
                     collected_at = $14,
                     snapshot_period = $15,
                     snapshot_date = $16,
                     snapshot_granularity = $17,
                     snapshot_basis = $18,
                     provider_file_id = $19,
                     provider_file_name = $20,
                     provider_updated_at = $21,
                     effective_date = $22,
                     created_at = $23
                 WHERE id = (SELECT id FROM existing)
                 RETURNING {BRONZE_OBJECT_COLUMNS}
             ),
             inserted AS (
                 INSERT INTO catalog.bronze_object
                 ({BRONZE_OBJECT_COLUMNS})
                 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                        $17, $18, $19, $20, $21, $22, $23
                 WHERE NOT EXISTS (SELECT 1 FROM updated)
                 ON CONFLICT (source_catalog_id, dedupe_key) DO UPDATE
                 SET source_record_id = EXCLUDED.source_record_id,
                     source_partition_key = EXCLUDED.source_partition_key,
                     source_identity_key = EXCLUDED.source_identity_key,
                     request_params = EXCLUDED.request_params,
                     checksum_sha256 = EXCLUDED.checksum_sha256,
                     content_type = EXCLUDED.content_type,
                     size_bytes = EXCLUDED.size_bytes,
                     logical_record_count = EXCLUDED.logical_record_count,
                     snapshot_period = EXCLUDED.snapshot_period,
                     snapshot_date = EXCLUDED.snapshot_date,
                     snapshot_granularity = EXCLUDED.snapshot_granularity,
                     snapshot_basis = EXCLUDED.snapshot_basis,
                     provider_file_id = EXCLUDED.provider_file_id,
                     provider_file_name = EXCLUDED.provider_file_name,
                     provider_updated_at = EXCLUDED.provider_updated_at,
                     effective_date = EXCLUDED.effective_date,
                     -- Adopt the latest re-ingested upload's location and run so the row
                     -- always points at the object this run actually wrote; otherwise a
                     -- re-ingest (the bulk path uploads before this upsert) leaves its new
                     -- object orphaned while the row references a stale object_key. The
                     -- displaced object key is recorded below for delayed orphan audit.
                     object_key = EXCLUDED.object_key,
                     ingestion_run_id = EXCLUDED.ingestion_run_id
                 RETURNING {BRONZE_OBJECT_COLUMNS}
             ),
             committed AS (
                 SELECT {BRONZE_OBJECT_COLUMNS} FROM updated
                 UNION ALL
                 SELECT {BRONZE_OBJECT_COLUMNS} FROM inserted
             ),
             orphan_candidate AS (
                 INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at)
                 SELECT $24,
                        'catalog.bronze_object.orphan_candidate.displaced.v1',
                        jsonb_build_object(
                            'type', 'catalog.bronze_object.orphan_candidate.displaced.v1',
                            'schema_version', 1,
                            'source_catalog_id', $2::uuid::text,
                            'dedupe_key', $7::text,
                            'displaced_object_key', existing.object_key,
                            'replacement_object_key', inserted.object_key,
                            'replacement_ingestion_run_id', inserted.ingestion_run_id::text,
                            'displaced_at', now()
                        ),
                        now()
                 FROM existing
                 CROSS JOIN committed AS inserted
                 WHERE existing.object_key IS DISTINCT FROM inserted.object_key
                 RETURNING 1
             )
             SELECT {BRONZE_OBJECT_COLUMNS} FROM committed
             LIMIT 1"
    )
}

fn validate_terminal_status(status: IngestionRunStatus) -> Result<(), CollectionError> {
    match status {
        IngestionRunStatus::Succeeded
        | IngestionRunStatus::Failed
        | IngestionRunStatus::Cancelled => Ok(()),
        IngestionRunStatus::Planned | IngestionRunStatus::Running => {
            Err(CollectionError::InvalidIngestionRunCompletion(format!(
                "{} is not a terminal status",
                status.wire_name()
            )))
        }
    }
}

fn u64_to_i64(field_name: &str, value: u64) -> Result<i64, CollectionError> {
    i64::try_from(value).map_err(|_| {
        CollectionError::Infrastructure(format!(
            "{field_name} {value} overflows i64 (Postgres BIGINT)"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::record_bronze_object_query;

    #[test]
    fn record_bronze_object_query_records_displaced_key_as_orphan_candidate() {
        let query = record_bronze_object_query();

        assert!(query.contains("catalog.outbox_event"));
        assert!(query.contains("catalog.bronze_object.orphan_candidate.displaced.v1"));
        assert!(query.contains("existing.object_key IS DISTINCT FROM inserted.object_key"));
        assert!(!query.to_ascii_uppercase().contains("DELETE FROM"));
    }
}
