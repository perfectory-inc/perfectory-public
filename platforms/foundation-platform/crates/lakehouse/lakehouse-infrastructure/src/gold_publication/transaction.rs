//! Atomic `PostgreSQL` transaction for industrial-complex Gold publication.

use foundation_shared_kernel::ids::ComplexId;
use lakehouse_application::PublishIndustrialComplexGoldPointerCommand;
use lakehouse_domain::{IndustrialComplexGoldPointer, LakehouseError};
use serde_json::Value as JsonValue;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::gold_pointer_published_envelope;
use super::row_mapping::{row_to_gold_pointer, u64_to_i64, GOLD_POINTER_COLUMNS};
use crate::postgres_error::map_sqlx;

pub(super) async fn publish_industrial_complex_gold_pointer(
    pool: &PgPool,
    command: PublishIndustrialComplexGoldPointerCommand,
) -> Result<IndustrialComplexGoldPointer, LakehouseError> {
    command.validate()?;
    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    lock_industrial_complex(&mut tx, command.complex_id).await?;

    let existing_current_version =
        lock_existing_gold_pointer_version(&mut tx, command.complex_id).await?;
    if existing_current_version != command.expected_current_version {
        return Err(
            LakehouseError::IndustrialComplexGoldPointerVersionConflict {
                expected: command.expected_current_version.clone(),
                current: existing_current_version,
            },
        );
    }

    let source_record_id = Uuid::now_v7();
    let profile_file_asset_id = Uuid::now_v7();
    let spatial_locator_file_asset_id = command
        .spatial_locator_object_key
        .as_ref()
        .map(|_| Uuid::now_v7());

    insert_source_record(&mut tx, source_record_id, &command).await?;
    insert_file_asset(
        &mut tx,
        profile_file_asset_id,
        source_record_id,
        &command.profile_object_key,
        "application/json",
        command.profile_size_bytes,
        Some(command.profile_checksum_sha256.as_str()),
    )
    .await?;
    if let (Some(file_asset_id), Some(object_key), Some(size_bytes)) = (
        spatial_locator_file_asset_id,
        command.spatial_locator_object_key.as_deref(),
        command.spatial_locator_size_bytes,
    ) {
        insert_file_asset(
            &mut tx,
            file_asset_id,
            source_record_id,
            object_key,
            "application/vnd.apache.parquet",
            size_bytes,
            None,
        )
        .await?;
    }

    upsert_gold_pointer(
        &mut tx,
        source_record_id,
        profile_file_asset_id,
        spatial_locator_file_asset_id,
        existing_current_version.as_deref(),
        &command,
    )
    .await?;
    let pointer = load_gold_pointer(&mut tx, command.complex_id).await?;
    insert_outbox_event(&mut tx, pointer.published_event()).await?;
    tx.commit().await.map_err(map_sqlx)?;
    Ok(pointer)
}

async fn lock_industrial_complex(
    tx: &mut Transaction<'_, Postgres>,
    complex_id: ComplexId,
) -> Result<(), LakehouseError> {
    let exists: Option<Uuid> = sqlx::query_scalar(
        "SELECT id
         FROM catalog.industrial_complex
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(complex_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)?;

    exists.map_or_else(
        || {
            Err(LakehouseError::IndustrialComplexNotFound(
                complex_id.to_string(),
            ))
        },
        |_| Ok(()),
    )
}

async fn lock_existing_gold_pointer_version(
    tx: &mut Transaction<'_, Postgres>,
    complex_id: ComplexId,
) -> Result<Option<String>, LakehouseError> {
    sqlx::query_scalar(
        "SELECT current_version
         FROM catalog.industrial_complex_gold_pointer
         WHERE complex_id = $1
         FOR UPDATE",
    )
    .bind(complex_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx)
}

async fn insert_source_record(
    tx: &mut Transaction<'_, Postgres>,
    source_record_id: Uuid,
    command: &PublishIndustrialComplexGoldPointerCommand,
) -> Result<(), LakehouseError> {
    sqlx::query(
        "INSERT INTO catalog.source_record
         (id, source, source_url, external_id, checksum_sha256, raw_object_key)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(source_record_id)
    .bind(command.source.as_str())
    .bind(command.source_url.as_deref())
    .bind(command.source_external_id.as_deref())
    .bind(None::<&str>)
    .bind(None::<&str>)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_file_asset(
    tx: &mut Transaction<'_, Postgres>,
    file_asset_id: Uuid,
    source_record_id: Uuid,
    object_key: &str,
    mime_type: &str,
    size_bytes: u64,
    checksum_sha256: Option<&str>,
) -> Result<(), LakehouseError> {
    let insert = sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, checksum_sha256, title,
          source_record_id, visibility, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'internal', 1)",
    )
    .bind(file_asset_id)
    .bind(object_key)
    .bind(mime_type)
    .bind(u64_to_i64("size_bytes", size_bytes)?)
    .bind(checksum_sha256)
    .bind(None::<&str>)
    .bind(source_record_id)
    .execute(&mut **tx)
    .await;

    match insert {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(database_error))
            if database_error.code().as_deref() == Some("23505") =>
        {
            Err(LakehouseError::ObjectKeyConflict(object_key.to_owned()))
        }
        Err(error) => Err(map_sqlx(error)),
    }
}

async fn upsert_gold_pointer(
    tx: &mut Transaction<'_, Postgres>,
    source_record_id: Uuid,
    profile_file_asset_id: Uuid,
    spatial_locator_file_asset_id: Option<Uuid>,
    previous_version: Option<&str>,
    command: &PublishIndustrialComplexGoldPointerCommand,
) -> Result<(), LakehouseError> {
    sqlx::query(
        "INSERT INTO catalog.industrial_complex_gold_pointer
         (complex_id, current_version, previous_version, profile_file_asset_id,
          spatial_locator_file_asset_id, source_record_id, source_snapshot_id,
          iceberg_snapshot_id, profile_row_count, profile_checksum_sha256,
          published_at, updated_at, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), 1)
         ON CONFLICT (complex_id) DO UPDATE
         SET current_version = EXCLUDED.current_version,
             previous_version = EXCLUDED.previous_version,
             profile_file_asset_id = EXCLUDED.profile_file_asset_id,
             spatial_locator_file_asset_id = EXCLUDED.spatial_locator_file_asset_id,
             source_record_id = EXCLUDED.source_record_id,
             source_snapshot_id = EXCLUDED.source_snapshot_id,
             iceberg_snapshot_id = EXCLUDED.iceberg_snapshot_id,
             profile_row_count = EXCLUDED.profile_row_count,
             profile_checksum_sha256 = EXCLUDED.profile_checksum_sha256,
             published_at = EXCLUDED.published_at,
             updated_at = now(),
             version = catalog.industrial_complex_gold_pointer.version + 1",
    )
    .bind(command.complex_id.as_uuid())
    .bind(command.current_version.as_str())
    .bind(previous_version)
    .bind(profile_file_asset_id)
    .bind(spatial_locator_file_asset_id)
    .bind(source_record_id)
    .bind(command.source_snapshot_id.as_str())
    .bind(command.iceberg_snapshot_id.as_str())
    .bind(u64_to_i64("profile_row_count", command.profile_row_count)?)
    .bind(command.profile_checksum_sha256.as_str())
    .bind(command.published_at)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

async fn load_gold_pointer(
    tx: &mut Transaction<'_, Postgres>,
    complex_id: ComplexId,
) -> Result<IndustrialComplexGoldPointer, LakehouseError> {
    let query = format!(
        "SELECT {GOLD_POINTER_COLUMNS}
         FROM catalog.industrial_complex_gold_pointer gp
         JOIN catalog.file_asset profile_file ON profile_file.id = gp.profile_file_asset_id
         LEFT JOIN catalog.file_asset spatial_file
                ON spatial_file.id = gp.spatial_locator_file_asset_id
         WHERE gp.complex_id = $1"
    );
    let row = sqlx::query(&query)
        .bind(complex_id.as_uuid())
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx)?;
    row_to_gold_pointer(&row)
}

async fn insert_outbox_event(
    tx: &mut Transaction<'_, Postgres>,
    event: lakehouse_domain::IndustrialComplexGoldPointerPublished,
) -> Result<(), LakehouseError> {
    let envelope = serde_json::to_value(gold_pointer_published_envelope(event))
        .map_err(|error| LakehouseError::Persistence(format!("serde encode: {error}")))?;
    let type_tag = envelope
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LakehouseError::Persistence("Gold event is missing type tag".to_owned()))?;
    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at)
         VALUES ($1, $2, $3, now())",
    )
    .bind(Uuid::now_v7())
    .bind(type_tag)
    .bind(&envelope)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}
