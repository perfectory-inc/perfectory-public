//! `PostgreSQL` round-trip tests for lakehouse batch run audit metadata.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::ports::{
    LakehouseBatchRunAudit, LakehouseBatchRunAuditCommand, LakehouseBatchRunRepository,
};
use lakehouse_domain::{
    LakehouseError, SparkRunInput, SparkRunSummary, SparkRunTarget, SparkRunWriteDisposition,
    SparkRunWriteMode, SILVER_INDUSTRIAL_COMPLEXES,
};
use lakehouse_infrastructure::{PgLakehouseBatchRunAudit, PgLakehouseBatchRunRepository};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const TEST_ADVISORY_LOCK_KEY: i64 = 0x6c61_6b65_686f_7573;
const TEST_INPUT_PREFIX: &str = "/workspace/infra/lakehouse/spark/fixtures/bronze/%";
const TEST_TARGET_PREFIX: &str = "/workspace/target/lakehouse/smoke/silver/%";

async fn pool() -> Result<Option<PgPool>, sqlx::Error> {
    let Some(url) = std::env::var("DATABASE_URL").ok() else {
        return Ok(None);
    };

    PgPool::connect(&url).await.map(Some)
}

async fn lock_lakehouse_batch_run_tests(
    pool: &PgPool,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(TEST_ADVISORY_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

async fn clear_test_lakehouse_batch_runs(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM catalog.lakehouse_batch_run
         WHERE job_name = 'industrial_complex_bronze_to_silver'
           AND contract = 'silver.industrial_complexes'
           AND input_path LIKE $1
           AND target_path LIKE $2",
    )
    .bind(TEST_INPUT_PREFIX)
    .bind(TEST_TARGET_PREFIX)
    .execute(pool)
    .await?;
    Ok(())
}

fn parsed_utc(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}

fn summary(suffix: &str, created_at: &str) -> Result<SparkRunSummary, chrono::ParseError> {
    Ok(SparkRunSummary {
        schema_version: "foundation-platform.spark_run_summary.v1".to_owned(),
        job_name: "industrial_complex_bronze_to_silver".to_owned(),
        contract: "silver.industrial_complexes".to_owned(),
        created_at_utc: parsed_utc(created_at)?,
        input: SparkRunInput {
            kind: "bronze_jsonl".to_owned(),
            path: format!("/workspace/infra/lakehouse/spark/fixtures/bronze/{suffix}.jsonl"),
        },
        target: SparkRunTarget::Parquet {
            path: format!("/workspace/target/lakehouse/smoke/silver/{suffix}"),
        },
        write_mode: SparkRunWriteMode::Parquet,
        write_disposition: SparkRunWriteDisposition::ParquetOverwrite,
        iceberg_readback_validation: None,
        row_count: 2,
        persisted_row_count: Some(2),
        quality_metrics: BTreeMap::from([
            ("row_count".to_owned(), 2),
            ("complex_id__null_count".to_owned(), 0),
            ("complex_id__empty_count".to_owned(), 0),
            ("official_complex_code__null_count".to_owned(), 0),
            ("official_complex_code__empty_count".to_owned(), 0),
            ("complex_name__null_count".to_owned(), 0),
            ("complex_name__empty_count".to_owned(), 0),
            ("complex_name_normalized__null_count".to_owned(), 0),
            ("complex_name_normalized__empty_count".to_owned(), 0),
            ("complex_kind__null_count".to_owned(), 0),
            ("complex_kind__empty_count".to_owned(), 0),
            ("status__null_count".to_owned(), 0),
            ("status__empty_count".to_owned(), 0),
            ("sido_code__null_count".to_owned(), 0),
            ("sido_code__empty_count".to_owned(), 0),
            ("sigungu_code__null_count".to_owned(), 0),
            ("sigungu_code__empty_count".to_owned(), 0),
            ("source_record_id__null_count".to_owned(), 0),
            ("source_record_id__empty_count".to_owned(), 0),
            ("source_snapshot_id__null_count".to_owned(), 0),
            ("source_snapshot_id__empty_count".to_owned(), 0),
            ("valid_from_utc__null_count".to_owned(), 0),
            ("ingested_at_utc__null_count".to_owned(), 0),
            ("row_checksum_sha256__null_count".to_owned(), 0),
            ("row_checksum_sha256__empty_count".to_owned(), 0),
            ("invalid_complex_kind_count".to_owned(), 0),
            ("invalid_status_count".to_owned(), 0),
            ("invalid_official_area_count".to_owned(), 0),
            ("invalid_complex_id_count".to_owned(), 0),
            ("invalid_checksum_count".to_owned(), 0),
        ]),
        column_count: SILVER_INDUSTRIAL_COMPLEXES.columns.len(),
        columns: SILVER_INDUSTRIAL_COMPLEXES
            .columns
            .iter()
            .map(|column| column.name.to_owned())
            .collect(),
        required_columns: SILVER_INDUSTRIAL_COMPLEXES
            .columns
            .iter()
            .filter(|column| column.required)
            .map(|column| column.name.to_owned())
            .collect(),
        source_snapshot_count: 1,
        source_snapshot_ids: vec![format!("bronze-snapshot-{suffix}")],
        source_snapshot_truncated: false,
    })
}

fn parquet_target_path(summary: &SparkRunSummary) -> &str {
    match &summary.target {
        SparkRunTarget::Parquet { path } => path,
        SparkRunTarget::Iceberg { .. } => "",
    }
}

const fn audit_command(
    summary: SparkRunSummary,
    recorded_by_staff_id: StaffId,
    request_id: Option<String>,
) -> LakehouseBatchRunAuditCommand {
    LakehouseBatchRunAuditCommand {
        summary,
        recorded_by_staff_id,
        request_id,
    }
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn records_validated_spark_run_summary_for_lakehouse_audit(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    let _lock_tx = lock_lakehouse_batch_run_tests(&pool).await?;
    clear_test_lakehouse_batch_runs(&pool).await?;
    let audit = PgLakehouseBatchRunAudit::new(pool.clone());
    let suffix = Uuid::now_v7().simple().to_string();
    let summary = summary(&suffix, "2026-05-14T05:27:05Z")?;
    let staff_id = StaffId::new(Uuid::now_v7());
    let request_id = format!("audit-{suffix}");

    audit
        .record_spark_run_summary(audit_command(
            summary.clone(),
            staff_id,
            Some(request_id.clone()),
        ))
        .await?;

    let row: (
        String,
        String,
        i64,
        i64,
        serde_json::Value,
        Uuid,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT contract, target_kind, row_count, persisted_row_count, summary_json,
                recorded_by_staff_id, request_id
         FROM catalog.lakehouse_batch_run
         WHERE target_path = $1",
    )
    .bind(parquet_target_path(&summary))
    .fetch_one(&pool)
    .await?;

    assert_eq!(row.0, "silver.industrial_complexes");
    assert_eq!(row.1, "parquet");
    assert_eq!(row.2, 2);
    assert_eq!(row.3, 2);
    assert_eq!(
        row.4["schema_version"],
        "foundation-platform.spark_run_summary.v1"
    );
    assert_eq!(row.5, staff_id.as_uuid());
    assert_eq!(row.6.as_deref(), Some(request_id.as_str()));

    sqlx::query("DELETE FROM catalog.lakehouse_batch_run WHERE target_path = $1")
        .bind(parquet_target_path(&summary))
        .execute(&pool)
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn loads_latest_promotion_candidate_from_validated_audit_rows(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    let _lock_tx = lock_lakehouse_batch_run_tests(&pool).await?;
    clear_test_lakehouse_batch_runs(&pool).await?;
    let audit = PgLakehouseBatchRunAudit::new(pool.clone());
    let repository = PgLakehouseBatchRunRepository::new(pool.clone());
    let suffix = Uuid::now_v7().simple().to_string();
    let older = summary(&format!("{suffix}-older"), "2026-05-14T05:27:05Z")?;
    let newer = summary(&format!("{suffix}-newer"), "2026-05-14T05:28:05Z")?;
    let older_staff_id = StaffId::new(Uuid::now_v7());
    let newer_staff_id = StaffId::new(Uuid::now_v7());

    audit
        .record_spark_run_summary(audit_command(
            older.clone(),
            older_staff_id,
            Some("older-request".to_owned()),
        ))
        .await?;
    audit
        .record_spark_run_summary(audit_command(
            newer.clone(),
            newer_staff_id,
            Some("newer-request".to_owned()),
        ))
        .await?;

    let candidate = repository
        .latest_promotion_candidate(&SILVER_INDUSTRIAL_COMPLEXES)
        .await?
        .ok_or_else(|| LakehouseError::Persistence("promotion candidate missing".to_owned()))?;

    assert_eq!(candidate.contract, SILVER_INDUSTRIAL_COMPLEXES.table_name);
    assert_eq!(candidate.created_at_utc, newer.created_at_utc);
    assert_eq!(candidate.row_count, newer.row_count);
    assert_eq!(candidate.persisted_row_count, Some(newer.row_count));
    assert_eq!(candidate.source_snapshot_ids, newer.source_snapshot_ids);
    assert_eq!(candidate.summary, newer);
    assert_eq!(candidate.recorded_by_staff_id, newer_staff_id);
    assert_eq!(candidate.request_id.as_deref(), Some("newer-request"));

    sqlx::query("DELETE FROM catalog.lakehouse_batch_run WHERE target_path IN ($1, $2)")
        .bind(parquet_target_path(&older))
        .bind(parquet_target_path(&candidate.summary))
        .execute(&pool)
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn re_recording_an_older_batch_does_not_make_it_the_latest_candidate(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    let _lock_tx = lock_lakehouse_batch_run_tests(&pool).await?;
    clear_test_lakehouse_batch_runs(&pool).await?;
    let audit = PgLakehouseBatchRunAudit::new(pool.clone());
    let repository = PgLakehouseBatchRunRepository::new(pool.clone());
    let suffix = Uuid::now_v7().simple().to_string();
    let older = summary(&format!("{suffix}-older"), "2026-05-14T05:27:05Z")?;
    let newer = summary(&format!("{suffix}-newer"), "2026-05-14T05:28:05Z")?;
    let older_staff_id = StaffId::new(Uuid::now_v7());
    let newer_staff_id = StaffId::new(Uuid::now_v7());

    audit
        .record_spark_run_summary(audit_command(
            older.clone(),
            older_staff_id,
            Some("older-request".to_owned()),
        ))
        .await?;
    audit
        .record_spark_run_summary(audit_command(
            newer.clone(),
            newer_staff_id,
            Some("newer-request".to_owned()),
        ))
        .await?;
    audit
        .record_spark_run_summary(audit_command(
            older.clone(),
            older_staff_id,
            Some("older-request".to_owned()),
        ))
        .await?;

    let candidate = repository
        .latest_promotion_candidate(&SILVER_INDUSTRIAL_COMPLEXES)
        .await?
        .ok_or_else(|| LakehouseError::Persistence("promotion candidate missing".to_owned()))?;

    assert_eq!(candidate.created_at_utc, newer.created_at_utc);
    assert_eq!(candidate.summary, newer);
    assert_eq!(candidate.recorded_by_staff_id, newer_staff_id);

    sqlx::query("DELETE FROM catalog.lakehouse_batch_run WHERE target_path IN ($1, $2)")
        .bind(parquet_target_path(&older))
        .bind(parquet_target_path(&newer))
        .execute(&pool)
        .await?;

    Ok(())
}
