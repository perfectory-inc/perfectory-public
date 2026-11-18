//! `load-industrial-complex-notice-silver`: the Silver half of the ILIS notice lane (root ADR-0484).
//!
//! `industrial-complex-notice-silver-load.sh` runs it after `plan-silver-load-objects` fetched the
//! notice detail objects the read table does not record. For each object, oldest first, `DuckDB`
//! inside the publisher builds its rows (`foundation_publisher_silver::industrial_complex_notice_stage`)
//! and the publisher appends, each in one Iceberg commit that records the object even when it holds
//! no row (root ADR-0559, ADR-0560): first the attachments the media store holds that are new or
//! changed, to `silver.industrial_complex_notice_attachment`; then every read, to
//! `silver.industrial_complex_notice_detail`, whose record the next plan is decided from. It was the
//! Spark job `industryland_or_kr.py`. The last stdout line is
//! `industrial-complex-notice-summary-json {json}`, the one the Spark job printed.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    industrial_complex_notice_stage::{
        notice_source, planned_objects, refuse_repeated, stage_object, ATTACHMENT_TABLE,
        DETAIL_TABLE,
    },
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::public_data_control_support::required_env_value;

const PLAN_DIR_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_NOTICE_PLAN_DIR";

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no contract for {table}"))
}

/// The snapshot `main` of `table` is at now; `None` for a table with none.
async fn head(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<Option<i64>> {
    Ok(catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id))
}

fn locked(stage: &Mutex<DuckDbParquetStage>) -> anyhow::Result<MutexGuard<'_, DuckDbParquetStage>> {
    stage
        .lock()
        .map_err(|_| anyhow::anyhow!("an earlier step of the load panicked"))
}

/// Appends one object's file (or none) to `table` on `expected_head`, recording the object.
async fn append(
    committer: &LakehouseDataCommitter,
    catalog: &IcebergRestCatalog,
    table: &str,
    file: Option<&StagedFile>,
    object_key: &str,
    expected_head: Option<i64>,
) -> anyhow::Result<bool> {
    let already_ingested = catalog
        .load_ingested_batch_objects(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    let outcome = committer
        .append_at_head(
            DataAppend {
                table_name: table,
                contract: contract(table)?,
                files: file.map_or(&[][..], std::slice::from_ref),
                batch: &[object_key.to_owned()],
                already_ingested: &already_ingested,
                summary: std::collections::BTreeMap::new(),
            },
            expected_head,
        )
        .await?;
    Ok(matches!(outcome, AppendOutcome::Appended { .. }))
}

/// Loads every planned object and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let plan_dir = required_env_value(PLAN_DIR_ENV)?;
    let source = notice_source()?;
    let objects = planned_objects(Path::new(&plan_dir), &source)?;
    refuse_repeated(&objects)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let reads_shape = committer.table_shape(DETAIL_TABLE).await?;
    let attachments_shape = committer.table_shape(ATTACHMENT_TABLE).await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        Path::new(&plan_dir),
        &LakehouseRead::from_env()?,
    )?);
    // One instant for the run, as the Spark job took one.
    let ingested_at = chrono::Utc::now();
    let attachment_contract = contract(ATTACHMENT_TABLE)?;
    let mut loaded = Vec::with_capacity(objects.len());
    for object in &objects {
        let attachment_head = head(&catalog, ATTACHMENT_TABLE).await?;
        let staged = stage_object(
            &*locked(&stage)?,
            &source,
            (&reads_shape, &attachments_shape),
            attachment_contract,
            object,
            attachment_head,
            ingested_at,
        )?;
        let attachments_appended = append(
            &committer,
            &catalog,
            ATTACHMENT_TABLE,
            staged.attachments_file.as_ref(),
            &object.object_key,
            attachment_head,
        )
        .await?;
        let detail_head = head(&catalog, DETAIL_TABLE).await?;
        let reads_appended = append(
            &committer,
            &catalog,
            DETAIL_TABLE,
            staged.reads_file.as_ref(),
            &object.object_key,
            detail_head,
        )
        .await?;
        loaded.push(json!({
            "object": object.object_key,
            "reads": staged.reads,
            "attachments": staged.attachments,
            "industrial_complex_notice_attachment_appended_rows": staged.changed_attachments,
            "industrial_complex_notice_attachment_appended": attachments_appended,
            "industrial_complex_notice_detail_appended_rows": staged.reads,
            "industrial_complex_notice_detail_appended": reads_appended,
        }));
    }
    drop(stage);
    let summary: Value = json!({
        "job": "load-industrial-complex-notice-silver",
        "engine": "duckdb",
        "status": "loaded",
        "objects": loaded,
    });
    println!("industrial-complex-notice-summary-json {summary}");
    Ok(())
}
