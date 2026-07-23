//! Contract tests for the provider-neutral lakehouse catalog port.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::StaffId;
use lakehouse_application::ports::{
    LakehouseBatchRunAudit, LakehouseBatchRunAuditCommand, LakehouseCatalog, LakehouseTableSnapshot,
};
use lakehouse_domain::{
    LakehouseError, LakehouseTableContract, SparkRunInput, SparkRunSummary, SparkRunTarget,
    SparkRunWriteDisposition, SparkRunWriteMode, SILVER_INDUSTRIAL_COMPLEXES,
    SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES,
};
use uuid::Uuid;

struct RecordingLakehouseCatalog;

#[derive(Default)]
struct RecordingBatchRunAudit {
    contracts: Mutex<Vec<String>>,
}

#[async_trait]
impl LakehouseCatalog for RecordingLakehouseCatalog {
    async fn ensure_table(
        &self,
        contract: &'static LakehouseTableContract,
    ) -> Result<LakehouseTableSnapshot, LakehouseError> {
        Ok(LakehouseTableSnapshot {
            table_name: contract.table_name.to_owned(),
            snapshot_id: "123456789".to_owned(),
            metadata_location: "r2://foundation-platform-lakehouse/metadata/metadata.json"
                .to_owned(),
        })
    }

    async fn get_current_snapshot(
        &self,
        table_name: &str,
    ) -> Result<Option<LakehouseTableSnapshot>, LakehouseError> {
        Ok(Some(LakehouseTableSnapshot {
            table_name: table_name.to_owned(),
            snapshot_id: "123456789".to_owned(),
            metadata_location: "r2://foundation-platform-lakehouse/metadata/metadata.json"
                .to_owned(),
        }))
    }
}

#[async_trait]
impl LakehouseBatchRunAudit for RecordingBatchRunAudit {
    async fn record_spark_run_summary(
        &self,
        command: LakehouseBatchRunAuditCommand,
    ) -> Result<(), LakehouseError> {
        self.contracts
            .lock()
            .map_err(|_| LakehouseError::Persistence("audit mutex poisoned".to_owned()))?
            .push(command.summary.contract);
        Ok(())
    }
}

fn parsed_utc(value: &str) -> Result<DateTime<Utc>, LakehouseError> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc))
        .map_err(|error| LakehouseError::Persistence(format!("test timestamp parse: {error}")))
}

fn spark_summary() -> Result<SparkRunSummary, LakehouseError> {
    Ok(SparkRunSummary {
        schema_version: "foundation-platform.spark_run_summary.v1".to_owned(),
        job_name: "industrial_complex_bronze_to_silver".to_owned(),
        contract: "silver.industrial_complexes".to_owned(),
        created_at_utc: parsed_utc("2026-05-14T05:27:05Z")?,
        input: SparkRunInput {
            kind: "bronze_jsonl".to_owned(),
            path: "/workspace/infra/lakehouse/spark/fixtures/bronze/industrial_complexes.jsonl"
                .to_owned(),
        },
        target: SparkRunTarget::Parquet {
            path: "/workspace/target/lakehouse/smoke/silver/industrial_complexes".to_owned(),
        },
        write_mode: SparkRunWriteMode::Parquet,
        write_disposition: SparkRunWriteDisposition::ParquetOverwrite,
        iceberg_readback_validation: None,
        row_count: 2,
        persisted_row_count: Some(2),
        quality_metrics: BTreeMap::from([("row_count".to_owned(), 2)]),
        column_count: 21,
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
        source_snapshot_ids: vec!["bronze-snapshot-2026-05-14".to_owned()],
        source_snapshot_truncated: false,
    })
}

#[tokio::test]
async fn port_ensures_table_from_static_lakehouse_contract() -> Result<(), LakehouseError> {
    let catalog = RecordingLakehouseCatalog;

    let snapshot = catalog.ensure_table(&SILVER_INDUSTRIAL_COMPLEXES).await?;

    assert_eq!(snapshot.table_name, "silver.industrial_complexes");
    assert_eq!(snapshot.snapshot_id, "123456789");
    assert!(snapshot.metadata_location.starts_with("r2://"));
    Ok(())
}

#[tokio::test]
async fn port_names_iceberg_snapshot_without_cloudflare_specific_fields(
) -> Result<(), LakehouseError> {
    let catalog = RecordingLakehouseCatalog;

    let snapshot = catalog
        .get_current_snapshot(SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES.table_name)
        .await?
        .ok_or_else(|| {
            LakehouseError::Persistence("test catalog returned no snapshot".to_owned())
        })?;

    assert_eq!(snapshot.table_name, "silver.industrial_complex_boundaries");
    assert!(!snapshot.metadata_location.contains("cloudflare"));
    Ok(())
}

#[tokio::test]
async fn port_records_provider_neutral_spark_run_summary() -> Result<(), LakehouseError> {
    let audit = RecordingBatchRunAudit::default();
    let summary = spark_summary()?;

    audit
        .record_spark_run_summary(LakehouseBatchRunAuditCommand {
            summary,
            recorded_by_staff_id: StaffId::new(Uuid::now_v7()),
            request_id: Some("catalog-port-test".to_owned()),
        })
        .await?;

    let contracts = {
        audit
            .contracts
            .lock()
            .map_err(|_| LakehouseError::Persistence("audit mutex poisoned".to_owned()))?
            .clone()
    };
    assert_eq!(contracts.as_slice(), ["silver.industrial_complexes"]);
    Ok(())
}
