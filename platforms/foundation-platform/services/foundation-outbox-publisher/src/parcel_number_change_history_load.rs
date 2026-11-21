//! `load-parcel-number-change-history`: one handoff of `VWorld` 필지고유번호변동연혁 into
//! `silver.parcel_number_change_history` (root ADR-0144 §4, ADR-0145).
//!
//! `legal-dong-code-load.sh` runs it for each handoff the collection left. The publisher reads
//! each file again against its checksum and the source contract
//! (`foundation_publisher_silver::parcel_number_change_history_stage`), `DuckDB` inside the
//! publisher keeps the rows whose event the table does not hold yet, and the publisher appends
//! them in one Iceberg commit per file that records the file, even when no row is new (root
//! ADR-0318, ADR-0559, ADR-0560). It was the `load` command of the Spark job
//! `vworld_parcel_number_change_history.py`. A file out of its shape or its bounds loads nothing
//! and the command fails. The last stdout line is `parcel-number-change-history-summary-json
//! {json}`, the one the Spark job printed.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    onbid_silver_stage::write,
    parcel_number_change_history_stage::{
        read_handoff, stage_object, unheld, HistorySource, TABLE,
    },
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    onbid_silver_load::{append, contract, head, locked, seconds},
    public_data_control_support::required_env_value,
};

const HANDOFF_DIR_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_NUMBER_CHANGE_HANDOFF_DIR";
const SUMMARY_OUTPUT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_NUMBER_CHANGE_SUMMARY_OUTPUT";

/// Loads every file of the handoff and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let handoff_dir = PathBuf::from(required_env_value(HANDOFF_DIR_ENV)?);
    let source = HistorySource::compiled()?;
    // Every file is read and checked before anything is opened: a changed shape reaches no catalog.
    let objects = read_handoff(&source, &handoff_dir)?;
    let all = || objects.iter().flat_map(|object| object.rows.iter());
    let rows = all().count();
    let dong_level_rows = all().filter(|row| row.is_dong_level).count();
    let quarantined_rows = all().filter(|row| row.quarantine_reason.is_some()).count();
    let duplicate_rows_folded: i64 = all().map(|row| i64::from(row.duplicate_count) - 1).sum();

    let table_contract = contract(TABLE)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    // A table is born on its first commit (root ADR-0564 §1), as the Spark job made it.
    committer.create_table_if_absent(table_contract).await?;
    let shape = committer.table_shape(TABLE).await?;
    // One instant for the load, to the second, as the Spark job took one.
    let ingested_at = DateTime::from_timestamp(Utc::now().timestamp(), 0)
        .context("the clock names no instant")?;
    // The staged files live as long as the stage: it is kept until the last append is done.
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        &handoff_dir,
        &LakehouseRead::from_env()?,
    )?);
    let mut appended_files = 0_u64;
    let mut appended_rows = 0_u64;
    // One Bronze object is one load unit. Each is compared with the table as the object before
    // it left it, so a link two files of one handoff carry is appended once.
    for object in &objects {
        let expected_head = head(&catalog, TABLE).await?;
        let (fresh, staged) = {
            let stage = &*locked(&stage)?;
            stage_object(stage, &source, table_contract, object, ingested_at)?;
            let fresh = unheld(stage, table_contract, expected_head)?;
            let staged = write(stage, &shape, TABLE, "pnch_fresh", &object.object_key)?;
            (fresh, staged)
        };
        // Recorded by its file even when no row is new; a file the table already records
        // appends nothing.
        if append(
            &committer,
            &catalog,
            TABLE,
            staged.as_ref(),
            &object.object_key,
            expected_head,
        )
        .await?
        {
            appended_files += 1;
            appended_rows += fresh;
        }
    }
    drop(stage);
    let summary = json!({
        "job": "vworld_parcel_number_change_history",
        "engine": "duckdb",
        "contract": TABLE,
        "files": objects.len(),
        "rows": rows,
        "dong_level_rows": dong_level_rows,
        "quarantined_rows": quarantined_rows,
        "duplicate_rows_folded": duplicate_rows_folded,
        "status": "ready",
        "appended_files": appended_files,
        "appended_rows": appended_rows,
        "seconds": seconds(started),
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    if let Ok(path) = std::env::var(SUMMARY_OUTPUT_ENV) {
        write_json(Path::new(&path), &serde_json::to_string_pretty(&summary)?)?;
    }
    println!("parcel-number-change-history-summary-json {summary}");
    Ok(())
}

/// `text` and a newline into `path`.
fn write_json(path: &Path, text: &str) -> anyhow::Result<()> {
    std::fs::write(path, format!("{text}\n"))
        .with_context(|| format!("cannot write {}", path.display()))
}
