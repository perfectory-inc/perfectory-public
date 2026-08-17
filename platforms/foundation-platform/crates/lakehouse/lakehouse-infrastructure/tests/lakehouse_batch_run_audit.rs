//! `PostgreSQL` round-trip tests for lakehouse batch run audit metadata.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::ports::{
    LakehouseBatchRunAudit, LakehouseBatchRunAuditCommand, LakehouseBatchRunRepository,
};
use lakehouse_domain::{
    required_quality_metric_names, LakehouseError, SparkRunInput, SparkRunSummary, SparkRunTarget,
    SparkRunWriteDisposition, SparkRunWriteMode, SILVER_INDUSTRIAL_COMPLEXES,
};
use lakehouse_infrastructure::{PgLakehouseBatchRunAudit, PgLakehouseBatchRunRepository};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const TEST_ADVISORY_LOCK_KEY: i64 = 0x6c61_6b65_686f_7573;
const TEST_INPUT_PREFIX: &str = "/workspace/infra/lakehouse/spark/fixtures/bronze/%";
const TEST_TARGET_PREFIX: &str = "/workspace/target/lakehouse/smoke/silver/%";

/// Connection for this `#[ignore]`d live suite. A missing `DATABASE_URL` aborts
/// the test instead of yielding `None`: these tests run only when a harness asked
/// for them, so an absent database is a provisioning failure — not a reason to
/// report success.
async fn pool() -> Result<PgPool, sqlx::Error> {
    let url =
        std::env::var("DATABASE_URL").map_err(|error| sqlx::Error::Configuration(error.into()))?;

    PgPool::connect(&url).await
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

/// A clean run's quality metrics, read off the contract rather than spelled out here.
///
/// These suites are about the Postgres round-trip, not about which metrics the contract demands.
/// A hand-written copy of that list made them fail for a reason that had nothing to do with the
/// database: the industrial-complex contract stopped requiring a region, the required metric set
/// moved with it, and this copy — reachable only behind `--ignored` — still named
/// `sido_code__null_count`. Deriving it means the next contract change cannot leave this file
/// behind. What the list should contain is proven by `lakehouse_spark_run_summary.rs`, which
/// still spells every metric out.
fn clean_quality_metrics() -> BTreeMap<String, u64> {
    // The Spark job gates emptiness on every string column, not only the required ones, so a real
    // summary carries those too. Derived from the same contract for the same reason.
    let optional_string_empty_counts = SILVER_INDUSTRIAL_COMPLEXES
        .columns
        .iter()
        .filter(|column| column.logical_type == "string")
        .map(|column| format!("{}__empty_count", column.name));

    required_quality_metric_names(&SILVER_INDUSTRIAL_COMPLEXES)
        .into_iter()
        .chain(optional_string_empty_counts)
        .map(|metric| {
            // `row_count` is the one required metric that counts rows rather than defects, and the
            // fixture has two rows. Everything else is a defect count, and a clean run has none.
            let value = u64::from(metric == "row_count") * 2;
            (metric, value)
        })
        .collect()
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
        quality_metrics: clean_quality_metrics(),
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

/// The fixture below is only useful if the audit would accept it, and what the audit checks first
/// is `validate_for_contract` — which needs no database at all.
///
/// This case is not `#[ignore]`d on purpose. Every suite in this file is, so the whole file used to
/// be invisible to `cargo test --locked --workspace --all-features`; the contract drift that broke
/// it was found only by CI's `-- --ignored` lane, on a claim of local green. Nothing about a
/// mismatched quality-metric set needs Postgres to detect, so this asserts it where an ordinary
/// test run will see it.
#[test]
fn the_fixture_summary_satisfies_the_contract_it_names() -> Result<(), Box<dyn std::error::Error>> {
    summary("fixture", "2026-05-14T05:27:05Z")?
        .validate_for_contract(&SILVER_INDUSTRIAL_COMPLEXES)?;
    Ok(())
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
    let pool = pool().await?;
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
    let pool = pool().await?;
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
    let pool = pool().await?;
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
