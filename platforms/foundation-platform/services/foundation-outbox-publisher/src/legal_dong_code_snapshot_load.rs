//! `load-legal-dong-code-snapshot`: one code.go.kr 법정동 전체 표 into
//! `reference.legal_dong_code_snapshot` (root ADR-0143 §2).
//!
//! `legal-dong-code-load.sh` runs it for each handoff the collector left: the table file, the day
//! it was taken and its Bronze key. The publisher reads the table and checks its shape and size
//! (`foundation_publisher_silver::legal_dong_code_snapshot_stage`), `DuckDB` inside the publisher
//! stages the rows, and the publisher appends them in one Iceberg commit that records the file,
//! so the same file is never loaded twice (root ADR-0559, ADR-0560). It was the Spark job
//! `legal_dong_code_snapshot_to_reference.py`. A table out of its shape or its bounds loads
//! nothing and the command fails. The last stdout line is
//! `legal-dong-code-snapshot-summary-json {json}`, the one the Spark job printed.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{ensure, Context};
use chrono::{DateTime, NaiveDate, Utc};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    legal_dong_code_snapshot_stage::{
        code_go_kr_table_prefixes, previous_code_go_kr_row_count, stage_snapshot,
        FullTableContract, CODE_GO_KR_TABLE_PREFIX, TABLE,
    },
    onbid_silver_stage::write,
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    onbid_silver_load::{append, contract, head, locked},
    public_data_control_support::required_env_value,
};

const INPUT_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_SNAPSHOT_INPUT";
const SNAPSHOT_DATE_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_SNAPSHOT_DATE";
const SOURCE_RECORD_ID_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_SNAPSHOT_SOURCE_RECORD_ID";
const SUMMARY_OUTPUT_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_SNAPSHOT_SUMMARY_OUTPUT";
const LATEST_MARKER_OUTPUT_ENV: &str =
    "FOUNDATION_PLATFORM_LEGAL_DONG_SNAPSHOT_LATEST_MARKER_OUTPUT";

/// `text` and a newline into `path`.
fn write_json(path: &Path, text: &str) -> anyhow::Result<()> {
    std::fs::write(path, format!("{text}\n"))
        .with_context(|| format!("cannot write {}", path.display()))
}

/// Loads the table and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let input = PathBuf::from(required_env_value(INPUT_ENV)?);
    let day = required_env_value(SNAPSHOT_DATE_ENV)?;
    let snapshot_date = NaiveDate::parse_from_str(&day, "%Y-%m-%d")
        .with_context(|| format!("{SNAPSHOT_DATE_ENV} {day:?} is not YYYY-MM-DD"))?;
    let source_record_id = required_env_value(SOURCE_RECORD_ID_ENV)?;
    // A table collected before the Bronze epoch and not loaded yet names its key under the source's
    // former id (root ADR-0626 §4).
    ensure!(
        code_go_kr_table_prefixes()
            .iter()
            .any(|prefix| source_record_id.starts_with(prefix.as_str())),
        "{SOURCE_RECORD_ID_ENV} must be the collector's Bronze key under {CODE_GO_KR_TABLE_PREFIX}"
    );
    let source = FullTableContract::compiled()?;
    // The table is read and checked before anything is opened: a changed shape reaches no catalog.
    let text = String::from_utf8(
        std::fs::read(&input).with_context(|| format!("cannot read {}", input.display()))?,
    )
    .map_err(|_| anyhow::anyhow!("{} is not UTF-8", input.display()))?;
    let rows = source.parse(&text)?;
    drop(text);
    let row_count = u64::try_from(rows.len())?;

    let table_contract = contract(TABLE)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    // A table is born on its first commit (root ADR-0564 §1), as the Spark job made it.
    committer.create_table_if_absent(table_contract).await?;
    let shape = committer.table_shape(TABLE).await?;
    let expected_head = head(&catalog, TABLE).await?;
    // One instant for the load, to the second, as the Spark job took one.
    let ingested_at = DateTime::from_timestamp(Utc::now().timestamp(), 0)
        .context("the clock names no instant")?;
    let stage_root = input
        .parent()
        .context("the input has no directory to stage in")?;
    // The staged file lives as long as the stage: it is kept until the append is done.
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        stage_root,
        &LakehouseRead::from_env()?,
    )?);
    let (previous, by_status, staged) = {
        let stage = &*locked(&stage)?;
        let previous =
            previous_code_go_kr_row_count(stage, table_contract, expected_head, snapshot_date)?;
        source.check_table_size(row_count, previous)?;
        let by_status = stage_snapshot(
            stage,
            table_contract,
            &rows,
            snapshot_date,
            &source_record_id,
            ingested_at,
        )?;
        let staged = write(
            stage,
            &shape,
            TABLE,
            "legal_dong_snapshot",
            &source_record_id,
        )?;
        (previous, by_status, staged)
    };
    // Recorded by its file: a file the table already holds appends nothing.
    let appended = append(
        &committer,
        &catalog,
        TABLE,
        staged.as_ref(),
        &source_record_id,
        expected_head,
    )
    .await?;
    drop(stage);
    if let Ok(path) = std::env::var(LATEST_MARKER_OUTPUT_ENV) {
        // Which snapshot is now the latest: the hub exports refuse a crosswalk projection built
        // from an older one (root ADR-0143 §5).
        let marker = json!({
            "row_count": row_count, "snapshot_date": day, "source_record_id": source_record_id,
        });
        write_json(Path::new(&path), &marker.to_string())?;
    }
    let summary = json!({
        "job": "legal_dong_code_snapshot_to_reference",
        "engine": "duckdb",
        "contract": TABLE,
        "snapshot_date": day,
        "input_format": "code-go-kr-html",
        "row_count": row_count,
        "by_status": by_status,
        "status": "ready",
        "previous_row_count": previous,
        "appended": appended,
        "seconds": (started.elapsed().as_secs_f64() * 10.0).round() / 10.0,
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    if let Ok(path) = std::env::var(SUMMARY_OUTPUT_ENV) {
        write_json(Path::new(&path), &serde_json::to_string_pretty(&summary)?)?;
    }
    println!("legal-dong-code-snapshot-summary-json {summary}");
    Ok(())
}
