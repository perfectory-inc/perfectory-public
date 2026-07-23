//! `PostgreSQL` row mapping for industrial-complex Gold pointers.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, FileAssetId, SourceRecordId};
use foundation_shared_kernel::ObjectKey;
use lakehouse_domain::{IndustrialComplexGoldPointer, LakehouseError};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::postgres_error::map_sqlx;

pub(super) const GOLD_POINTER_COLUMNS: &str = "gp.complex_id, gp.current_version,
 gp.previous_version, gp.profile_file_asset_id, profile_file.object_key AS profile_object_key,
 gp.spatial_locator_file_asset_id, spatial_file.object_key AS spatial_locator_object_key,
 gp.source_record_id, gp.source_snapshot_id, gp.iceberg_snapshot_id, gp.profile_row_count,
 gp.profile_checksum_sha256, gp.published_at, gp.updated_at, gp.version";

pub(super) fn row_to_gold_pointer(
    row: &PgRow,
) -> Result<IndustrialComplexGoldPointer, LakehouseError> {
    let profile_object_key = ObjectKey::parse(
        &row.try_get::<String, _>("profile_object_key")
            .map_err(map_sqlx)?,
    )
    .map_err(|error| LakehouseError::Persistence(error.to_string()))?;
    let spatial_locator_object_key = row
        .try_get::<Option<String>, _>("spatial_locator_object_key")
        .map_err(map_sqlx)?
        .map(|raw| ObjectKey::parse(&raw))
        .transpose()
        .map_err(|error| LakehouseError::Persistence(error.to_string()))?;

    Ok(IndustrialComplexGoldPointer {
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        current_version: row.try_get("current_version").map_err(map_sqlx)?,
        previous_version: row.try_get("previous_version").map_err(map_sqlx)?,
        profile_file_asset_id: FileAssetId::new(
            row.try_get::<Uuid, _>("profile_file_asset_id")
                .map_err(map_sqlx)?,
        ),
        profile_object_key,
        spatial_locator_file_asset_id: row
            .try_get::<Option<Uuid>, _>("spatial_locator_file_asset_id")
            .map_err(map_sqlx)?
            .map(FileAssetId::new),
        spatial_locator_object_key,
        source_record_id: SourceRecordId::new(
            row.try_get::<Uuid, _>("source_record_id")
                .map_err(map_sqlx)?,
        ),
        source_snapshot_id: row.try_get("source_snapshot_id").map_err(map_sqlx)?,
        iceberg_snapshot_id: row.try_get("iceberg_snapshot_id").map_err(map_sqlx)?,
        profile_row_count: i64_to_u64(
            "profile_row_count",
            row.try_get("profile_row_count").map_err(map_sqlx)?,
        )?,
        profile_checksum_sha256: row.try_get("profile_checksum_sha256").map_err(map_sqlx)?,
        published_at: row
            .try_get::<DateTime<Utc>, _>("published_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub(super) fn u64_to_i64(field: &str, value: u64) -> Result<i64, LakehouseError> {
    i64::try_from(value)
        .map_err(|_| LakehouseError::InvalidContract(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn i64_to_u64(field: &str, value: i64) -> Result<u64, LakehouseError> {
    u64::try_from(value).map_err(|_| {
        LakehouseError::Persistence(format!("{field} must not be negative in database"))
    })
}
