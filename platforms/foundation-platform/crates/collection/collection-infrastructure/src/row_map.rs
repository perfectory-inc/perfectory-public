//! `SQLx` row mapping for Collection-owned persistence records.

use chrono::{DateTime, Utc};
use collection_domain::{
    BronzeObject, CollectionError, IngestionRun, IngestionRunStatus, IngestionTrigger,
    SchemaObservedType, SchemaProfile, SnapshotBasis, SnapshotGranularity, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
use foundation_shared_kernel::ids::{
    BronzeObjectId, IngestionRunId, SchemaProfileId, SourceCatalogId, SourceRecordId,
};
use foundation_shared_kernel::ObjectKey;
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

pub fn row_to_source_catalog_entry(row: &PgRow) -> Result<SourceCatalogEntry, CollectionError> {
    let auth_kind_raw: String = row.try_get("auth_kind").map_err(|error| map_sqlx(&error))?;
    let auth_kind = SourceAuthKind::from_wire(&auth_kind_raw)
        .map_err(|e| CollectionError::Infrastructure(e.to_string()))?;
    let payload_format_raw: String = row
        .try_get("payload_format")
        .map_err(|error| map_sqlx(&error))?;
    let payload_format = SourcePayloadFormat::from_wire(&payload_format_raw)
        .map_err(|e| CollectionError::Infrastructure(e.to_string()))?;
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(
            row.try_get::<Uuid, _>("id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        slug: row.try_get("slug").map_err(|error| map_sqlx(&error))?,
        name: row.try_get("name").map_err(|error| map_sqlx(&error))?,
        provider: row.try_get("provider").map_err(|error| map_sqlx(&error))?,
        dataset_name: row
            .try_get("dataset_name")
            .map_err(|error| map_sqlx(&error))?,
        base_url: row.try_get("base_url").map_err(|error| map_sqlx(&error))?,
        auth_kind,
        payload_format,
        license_name: row
            .try_get("license_name")
            .map_err(|error| map_sqlx(&error))?,
        license_url: row
            .try_get("license_url")
            .map_err(|error| map_sqlx(&error))?,
        terms_url: row.try_get("terms_url").map_err(|error| map_sqlx(&error))?,
        collection_frequency: row
            .try_get("collection_frequency")
            .map_err(|error| map_sqlx(&error))?,
        is_active: row.try_get("is_active").map_err(|error| map_sqlx(&error))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|error| map_sqlx(&error))?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(|error| map_sqlx(&error))?,
        version: row.try_get("version").map_err(|error| map_sqlx(&error))?,
    })
}

pub fn row_to_ingestion_run(row: &PgRow) -> Result<IngestionRun, CollectionError> {
    let trigger_raw: String = row.try_get("trigger").map_err(|error| map_sqlx(&error))?;
    let trigger = IngestionTrigger::from_wire(&trigger_raw)
        .map_err(|e| CollectionError::Infrastructure(e.to_string()))?;
    let status_raw: String = row.try_get("status").map_err(|error| map_sqlx(&error))?;
    let status = IngestionRunStatus::from_wire(&status_raw)
        .map_err(|e| CollectionError::Infrastructure(e.to_string()))?;
    Ok(IngestionRun {
        id: IngestionRunId::new(
            row.try_get::<Uuid, _>("id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        source_catalog_id: SourceCatalogId::new(
            row.try_get::<Uuid, _>("source_catalog_id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        trigger,
        status,
        request_params: row
            .try_get("request_params")
            .map_err(|error| map_sqlx(&error))?,
        started_at: row
            .try_get::<DateTime<Utc>, _>("started_at")
            .map_err(|error| map_sqlx(&error))?,
        finished_at: row
            .try_get("finished_at")
            .map_err(|error| map_sqlx(&error))?,
        logical_records_seen: i64_to_u64_named(
            "logical_records_seen",
            row.try_get("logical_records_seen")
                .map_err(|error| map_sqlx(&error))?,
        )?,
        objects_written: i64_to_u64_named(
            "objects_written",
            row.try_get("objects_written")
                .map_err(|error| map_sqlx(&error))?,
        )?,
        error_message: row
            .try_get("error_message")
            .map_err(|error| map_sqlx(&error))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|error| map_sqlx(&error))?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(|error| map_sqlx(&error))?,
        version: row.try_get("version").map_err(|error| map_sqlx(&error))?,
    })
}

pub fn row_to_bronze_object(row: &PgRow) -> Result<BronzeObject, CollectionError> {
    let object_key_raw: String = row
        .try_get("object_key")
        .map_err(|error| map_sqlx(&error))?;
    let object_key = ObjectKey::parse(&object_key_raw)
        .map_err(|e| CollectionError::Infrastructure(e.to_string()))?;
    let logical_record_count = row
        .try_get::<Option<i64>, _>("logical_record_count")
        .map_err(|error| map_sqlx(&error))?
        .map(|value| i64_to_u64_named("logical_record_count", value))
        .transpose()?;
    Ok(BronzeObject {
        id: BronzeObjectId::new(
            row.try_get::<Uuid, _>("id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        source_catalog_id: SourceCatalogId::new(
            row.try_get::<Uuid, _>("source_catalog_id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        ingestion_run_id: IngestionRunId::new(
            row.try_get::<Uuid, _>("ingestion_run_id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(|error| map_sqlx(&error))?
            .map(SourceRecordId::new),
        source_partition_key: row
            .try_get("source_partition_key")
            .map_err(|error| map_sqlx(&error))?,
        source_identity_key: row
            .try_get("source_identity_key")
            .map_err(|error| map_sqlx(&error))?,
        dedupe_key: row
            .try_get("dedupe_key")
            .map_err(|error| map_sqlx(&error))?,
        request_params: row
            .try_get("request_params")
            .map_err(|error| map_sqlx(&error))?,
        object_key,
        checksum_sha256: row
            .try_get("checksum_sha256")
            .map_err(|error| map_sqlx(&error))?,
        content_type: row
            .try_get("content_type")
            .map_err(|error| map_sqlx(&error))?,
        size_bytes: i64_to_u64_named(
            "size_bytes",
            row.try_get("size_bytes")
                .map_err(|error| map_sqlx(&error))?,
        )?,
        logical_record_count,
        collected_at: row
            .try_get::<DateTime<Utc>, _>("collected_at")
            .map_err(|error| map_sqlx(&error))?,
        snapshot_period: row
            .try_get("snapshot_period")
            .map_err(|error| map_sqlx(&error))?,
        snapshot_date: row
            .try_get("snapshot_date")
            .map_err(|error| map_sqlx(&error))?,
        snapshot_granularity: SnapshotGranularity::from_wire(
            &row.try_get::<String, _>("snapshot_granularity")
                .map_err(|error| map_sqlx(&error))?,
        )
        .map_err(|error| CollectionError::Infrastructure(error.to_string()))?,
        snapshot_basis: SnapshotBasis::from_wire(
            &row.try_get::<String, _>("snapshot_basis")
                .map_err(|error| map_sqlx(&error))?,
        )
        .map_err(|error| CollectionError::Infrastructure(error.to_string()))?,
        provider_file_id: row
            .try_get("provider_file_id")
            .map_err(|error| map_sqlx(&error))?,
        provider_file_name: row
            .try_get("provider_file_name")
            .map_err(|error| map_sqlx(&error))?,
        provider_updated_at: row
            .try_get("provider_updated_at")
            .map_err(|error| map_sqlx(&error))?,
        effective_date: row
            .try_get("effective_date")
            .map_err(|error| map_sqlx(&error))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|error| map_sqlx(&error))?,
    })
}

pub fn row_to_schema_profile(row: &PgRow) -> Result<SchemaProfile, CollectionError> {
    let observed_type_raw: String = row
        .try_get("observed_type")
        .map_err(|error| map_sqlx(&error))?;
    let observed_type = SchemaObservedType::from_wire(&observed_type_raw)
        .map_err(|e| CollectionError::Infrastructure(e.to_string()))?;
    Ok(SchemaProfile {
        id: SchemaProfileId::new(
            row.try_get::<Uuid, _>("id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        source_catalog_id: SourceCatalogId::new(
            row.try_get::<Uuid, _>("source_catalog_id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        ingestion_run_id: IngestionRunId::new(
            row.try_get::<Uuid, _>("ingestion_run_id")
                .map_err(|error| map_sqlx(&error))?,
        ),
        field_path: row
            .try_get("field_path")
            .map_err(|error| map_sqlx(&error))?,
        observed_type,
        nonnull_count: i64_to_u64_named(
            "nonnull_count",
            row.try_get("nonnull_count")
                .map_err(|error| map_sqlx(&error))?,
        )?,
        null_count: i64_to_u64_named(
            "null_count",
            row.try_get("null_count")
                .map_err(|error| map_sqlx(&error))?,
        )?,
        sample_values: row
            .try_get("sample_values")
            .map_err(|error| map_sqlx(&error))?,
        candidate_key_score: row
            .try_get("candidate_key_score")
            .map_err(|error| map_sqlx(&error))?,
        profiled_at: row
            .try_get::<DateTime<Utc>, _>("profiled_at")
            .map_err(|error| map_sqlx(&error))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|error| map_sqlx(&error))?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(|error| map_sqlx(&error))?,
        version: row.try_get("version").map_err(|error| map_sqlx(&error))?,
    })
}

fn map_sqlx(error: &sqlx::Error) -> CollectionError {
    CollectionError::Infrastructure(error.to_string())
}

fn i64_to_u64_named(field_name: &str, value: i64) -> Result<u64, CollectionError> {
    u64::try_from(value).map_err(|_| {
        CollectionError::Infrastructure(format!(
            "{field_name} {value} is negative in DB (CHECK constraint should have caught this)"
        ))
    })
}
