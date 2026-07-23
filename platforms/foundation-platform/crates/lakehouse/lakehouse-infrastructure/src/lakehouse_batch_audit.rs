//! `PostgreSQL` audit sink for validated lakehouse batch run summaries.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::ports::{
    LakehouseBatchRunAudit, LakehouseBatchRunAuditCommand, LakehouseBatchRunRecord,
    LakehouseBatchRunRepository,
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseError, LakehouseTableContract,
    SparkRunSummary, SparkRunTarget, SparkRunWriteDisposition, SparkRunWriteMode,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::postgres_error::map_sqlx;

/// `PostgreSQL` implementation of lakehouse batch run audit persistence.
pub struct PgLakehouseBatchRunAudit {
    pool: PgPool,
}

/// `PostgreSQL` implementation of lakehouse batch run audit read access.
pub struct PgLakehouseBatchRunRepository {
    pool: PgPool,
}

impl PgLakehouseBatchRunAudit {
    /// Creates an audit sink backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl PgLakehouseBatchRunRepository {
    /// Creates a lakehouse batch run repository backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LakehouseBatchRunAudit for PgLakehouseBatchRunAudit {
    async fn record_spark_run_summary(
        &self,
        command: LakehouseBatchRunAuditCommand,
    ) -> Result<(), LakehouseError> {
        let summary = &command.summary;
        let contract = find_lakehouse_contract(&summary.contract)?;
        summary
            .validate_for_contract(contract)
            .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))?;

        let summary_json = serde_json::to_value(summary)
            .map_err(|error| LakehouseError::Persistence(format!("serde encode: {error}")))?;
        if !summary_json.is_object() {
            return Err(LakehouseError::Persistence(
                "lakehouse batch run summary did not encode as a JSON object".to_owned(),
            ));
        }
        let row_count = u64_to_i64("row_count", summary.row_count)?;
        let persisted_row_count = summary
            .persisted_row_count
            .map(|count| u64_to_i64("persisted_row_count", count))
            .transpose()?;
        let source_snapshot_count =
            u64_to_i64("source_snapshot_count", summary.source_snapshot_count)?;
        let target = AuditTarget::from_summary(summary);

        sqlx::query(
            "INSERT INTO catalog.lakehouse_batch_run
             (id, schema_version, job_name, contract, created_at, input_kind, input_path,
              target_kind, target_path, target_catalog, target_namespace, target_table,
              target_qualified_table, write_mode, write_disposition, row_count,
              persisted_row_count, source_snapshot_count, source_snapshot_ids,
              source_snapshot_truncated, summary_json, recorded_by_staff_id, request_id)
             VALUES
             ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
              $17, $18, $19, $20, $21, $22, $23)
             ON CONFLICT (
                 job_name,
                 contract,
                 created_at,
                 write_disposition,
                 input_path,
                 target_path,
                 target_qualified_table
             )
             DO UPDATE
             SET schema_version = EXCLUDED.schema_version,
                 input_kind = EXCLUDED.input_kind,
                 target_kind = EXCLUDED.target_kind,
                 target_catalog = EXCLUDED.target_catalog,
                 target_namespace = EXCLUDED.target_namespace,
                 target_table = EXCLUDED.target_table,
                 write_mode = EXCLUDED.write_mode,
                 row_count = EXCLUDED.row_count,
                 persisted_row_count = EXCLUDED.persisted_row_count,
                 source_snapshot_count = EXCLUDED.source_snapshot_count,
                 source_snapshot_ids = EXCLUDED.source_snapshot_ids,
                 source_snapshot_truncated = EXCLUDED.source_snapshot_truncated,
                 summary_json = EXCLUDED.summary_json",
        )
        .bind(Uuid::now_v7())
        .bind(summary.schema_version.trim())
        .bind(summary.job_name.trim())
        .bind(summary.contract.trim())
        .bind(summary.created_at_utc)
        .bind(summary.input.kind.trim())
        .bind(summary.input.path.trim())
        .bind(target.kind)
        .bind(target.path)
        .bind(target.catalog)
        .bind(target.namespace)
        .bind(target.table)
        .bind(target.qualified_table)
        .bind(write_mode_wire(summary.write_mode))
        .bind(write_disposition_wire(summary.write_disposition))
        .bind(row_count)
        .bind(persisted_row_count)
        .bind(source_snapshot_count)
        .bind(&summary.source_snapshot_ids)
        .bind(summary.source_snapshot_truncated)
        .bind(summary_json)
        .bind(command.recorded_by_staff_id.as_uuid())
        .bind(command.request_id.as_deref())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(())
    }
}

#[async_trait]
impl LakehouseBatchRunRepository for PgLakehouseBatchRunRepository {
    async fn latest_promotion_candidate(
        &self,
        contract: &'static LakehouseTableContract,
    ) -> Result<Option<LakehouseBatchRunRecord>, LakehouseError> {
        let row = sqlx::query_as::<_, LakehouseBatchRunRow>(
            "SELECT id,
                    schema_version,
                    job_name,
                    contract,
                    created_at,
                    write_disposition,
                    row_count,
                    persisted_row_count,
                    source_snapshot_ids,
                    summary_json,
                    recorded_by_staff_id,
                    request_id,
                    recorded_at
             FROM catalog.lakehouse_batch_run
             WHERE contract = $1
               AND source_snapshot_truncated = false
               AND persisted_row_count = row_count
               AND write_disposition <> 'validate_only'
             ORDER BY created_at DESC, recorded_at DESC, id DESC
             LIMIT 1",
        )
        .bind(contract.table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;

        row.map(|row| row.into_record(contract)).transpose()
    }
}

#[derive(sqlx::FromRow)]
struct LakehouseBatchRunRow {
    id: Uuid,
    schema_version: String,
    job_name: String,
    contract: String,
    created_at: DateTime<Utc>,
    write_disposition: String,
    row_count: i64,
    persisted_row_count: Option<i64>,
    source_snapshot_ids: Vec<String>,
    summary_json: serde_json::Value,
    recorded_by_staff_id: Uuid,
    request_id: Option<String>,
    recorded_at: DateTime<Utc>,
}

impl LakehouseBatchRunRow {
    fn into_record(
        self,
        contract: &'static LakehouseTableContract,
    ) -> Result<LakehouseBatchRunRecord, LakehouseError> {
        let summary = SparkRunSummary::from_json_value(self.summary_json)
            .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))?;
        summary
            .validate_for_contract(contract)
            .map_err(|error| LakehouseError::InvalidLakehouseBatchRun(error.to_string()))?;

        Ok(LakehouseBatchRunRecord {
            id: self.id,
            schema_version: self.schema_version,
            job_name: self.job_name,
            contract: self.contract,
            created_at_utc: self.created_at,
            write_disposition: parse_write_disposition(&self.write_disposition)?,
            row_count: i64_to_u64("row_count", self.row_count)?,
            persisted_row_count: self
                .persisted_row_count
                .map(|count| i64_to_u64("persisted_row_count", count))
                .transpose()?,
            source_snapshot_ids: self.source_snapshot_ids,
            summary,
            recorded_by_staff_id: StaffId::new(self.recorded_by_staff_id),
            request_id: self.request_id,
            recorded_at_utc: self.recorded_at,
        })
    }
}

struct AuditTarget<'a> {
    kind: &'static str,
    path: Option<&'a str>,
    catalog: Option<&'a str>,
    namespace: Option<&'a str>,
    table: Option<&'a str>,
    qualified_table: Option<&'a str>,
}

impl<'a> AuditTarget<'a> {
    const fn from_summary(summary: &'a SparkRunSummary) -> Self {
        match &summary.target {
            SparkRunTarget::Parquet { path } => Self {
                kind: "parquet",
                path: Some(path.as_str()),
                catalog: None,
                namespace: None,
                table: None,
                qualified_table: None,
            },
            SparkRunTarget::Iceberg {
                catalog,
                namespace,
                table,
                qualified_table,
            } => Self {
                kind: "iceberg",
                path: None,
                catalog: Some(catalog.as_str()),
                namespace: Some(namespace.as_str()),
                table: Some(table.as_str()),
                qualified_table: Some(qualified_table.as_str()),
            },
        }
    }
}

fn find_lakehouse_contract(
    table_name: &str,
) -> Result<&'static LakehouseTableContract, LakehouseError> {
    industrial_complex_lakehouse_contract_by_table_name(table_name).ok_or_else(|| {
        LakehouseError::InvalidLakehouseBatchRun(format!(
            "unknown lakehouse table contract: {table_name}"
        ))
    })
}

const fn write_mode_wire(write_mode: SparkRunWriteMode) -> &'static str {
    match write_mode {
        SparkRunWriteMode::Parquet => "parquet",
        SparkRunWriteMode::Iceberg => "iceberg",
    }
}

const fn write_disposition_wire(disposition: SparkRunWriteDisposition) -> &'static str {
    match disposition {
        SparkRunWriteDisposition::ValidateOnly => "validate_only",
        SparkRunWriteDisposition::ParquetOverwrite => "parquet_overwrite",
        SparkRunWriteDisposition::IcebergAppend => "iceberg_append",
        SparkRunWriteDisposition::IcebergOverwrite => "iceberg_overwrite",
    }
}

fn parse_write_disposition(wire: &str) -> Result<SparkRunWriteDisposition, LakehouseError> {
    match wire {
        "validate_only" => Ok(SparkRunWriteDisposition::ValidateOnly),
        "parquet_overwrite" => Ok(SparkRunWriteDisposition::ParquetOverwrite),
        "iceberg_append" => Ok(SparkRunWriteDisposition::IcebergAppend),
        "iceberg_overwrite" => Ok(SparkRunWriteDisposition::IcebergOverwrite),
        _ => Err(LakehouseError::InvalidLakehouseBatchRun(format!(
            "unknown lakehouse batch write_disposition: {wire}"
        ))),
    }
}

fn u64_to_i64(field_name: &str, value: u64) -> Result<i64, LakehouseError> {
    i64::try_from(value).map_err(|_| {
        LakehouseError::Persistence(format!(
            "{field_name} {value} overflows i64 (Postgres BIGINT)"
        ))
    })
}

fn i64_to_u64(field_name: &str, value: i64) -> Result<u64, LakehouseError> {
    u64::try_from(value).map_err(|_| {
        LakehouseError::Persistence(format!(
            "{field_name} {value} is negative in DB (CHECK constraint should have caught this)"
        ))
    })
}
