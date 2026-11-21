//! `export-legal-dong-pairing-inputs` and `append-legal-dong-code-changes`: the lakehouse side of
//! the 법정동 code-change pairing (root ADR-0143, ADR-0145).
//!
//! `legal-dong-code-load.sh` runs the pairing's rules (`legal_dong_code_change_pairs.py`, plain
//! Python) between these two: `DuckDB` inside the publisher writes the tables the rules read into
//! the run's work folder, then appends the rows they decided to `reference.legal_dong_code_change`
//! in one Iceberg commit per load unit, and writes the table back for the projection (root
//! ADR-0559, ADR-0560). They were the Spark session of that job. Every read is pinned on the
//! snapshot the table's `main` is at when the command starts.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{bail, ensure, Context};
use chrono::NaiveDate;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    legal_dong_pairing_stage::{
        at_head, export_changes, export_official_links, export_parcels, export_snapshot,
        pairing_floor, stage_append, stage_unit, APPENDED_FILE, CHANGE_TABLE, HELD_FILE,
        HISTORY_TABLE, PARCEL_TABLE, RECORDED_FILE, SNAPSHOT_TABLE,
    },
    onbid_silver_stage::write,
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    onbid_silver_load::{append, contract, head, locked, pinned, seconds},
    public_data_control_support::required_env_value,
};

const DIR_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_PAIRING_DIR";
const PHASE_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_PAIRING_PHASE";
const SNAPSHOT_DATE_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_PAIRING_SNAPSHOT_DATE";
const SOURCE_RECORD_ID_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_PAIRING_SOURCE_RECORD_ID";
const OFFICIAL_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_PAIRING_OFFICIAL_HISTORY";

fn stage_in(directory: &Path) -> anyhow::Result<DuckDbParquetStage> {
    DuckDbParquetStage::open_reading_lakehouse(directory, &LakehouseRead::from_env()?)
}

/// Writes the files of one phase and prints the summary line.
///
/// `tables`: the snapshot, the recorded changes and, when the official history is on, its links.
/// `parcels`: the answer to each request the rules wrote from the snapshot.
pub(crate) async fn export() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let directory = PathBuf::from(required_env_value(DIR_ENV)?);
    let phase = required_env_value(PHASE_ENV)?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let written = match phase.as_str() {
        "tables" => {
            let day = required_env_value(SNAPSHOT_DATE_ENV)?;
            let snapshot_date = NaiveDate::parse_from_str(&day, "%Y-%m-%d")
                .with_context(|| format!("{SNAPSHOT_DATE_ENV} {day:?} is not YYYY-MM-DD"))?;
            let record = required_env_value(SOURCE_RECORD_ID_ENV)?;
            let official = match std::env::var(OFFICIAL_ENV).as_deref() {
                Ok("1") => true,
                Ok("0") | Err(_) => false,
                Ok(other) => bail!("{OFFICIAL_ENV} is 1 or 0, not {other:?}"),
            };
            let snapshot_head = pinned(&catalog, SNAPSHOT_TABLE).await?;
            // A change table nothing was written to holds no change.
            let change_head = catalog
                .load_main_snapshot_history(CHANGE_TABLE)
                .await?
                .and_then(|history| history.first().map(|snapshot| snapshot.snapshot_id));
            let history_head = if official {
                Some(pinned(&catalog, HISTORY_TABLE).await?)
            } else {
                None
            };
            let stage = stage_in(&directory)?;
            let snapshots = at_head(&stage, contract(SNAPSHOT_TABLE)?, snapshot_head)?;
            let rows = export_snapshot(&stage, &snapshots, snapshot_date, &record, &directory)?;
            ensure!(
                rows > 0,
                "{SNAPSHOT_TABLE} holds no rows of {record} on {day}"
            );
            let changes = contract(CHANGE_TABLE)?;
            let recorded = change_head
                .map(|head| at_head(&stage, changes, head))
                .transpose()?;
            let recorded = export_changes(
                &stage,
                changes,
                recorded.as_deref(),
                &directory,
                RECORDED_FILE,
            )?;
            let links = history_head
                .map(|head| {
                    let source = at_head(&stage, contract(HISTORY_TABLE)?, head)?;
                    export_official_links(&stage, &source, pairing_floor()?, &directory)
                })
                .transpose()?;
            json!({"snapshot_rows": rows, "recorded_changes": recorded, "official_links": links})
        }
        "parcels" => {
            let parcel_head = pinned(&catalog, PARCEL_TABLE).await?;
            let stage = stage_in(&directory)?;
            let parcels = at_head(&stage, contract(PARCEL_TABLE)?, parcel_head)?;
            let answers = export_parcels(&stage, &parcels, &directory)?;
            json!({
                "requests": answers.len(),
                "editions_not_loaded": answers.iter().filter(|answer| !answer.present).count(),
                "parcel_rows": answers.iter().map(|answer| answer.rows).sum::<u64>(),
            })
        }
        other => bail!("{PHASE_ENV} is tables or parcels, not {other:?}"),
    };
    let summary = json!({
        "job": "legal_dong_code_change_pairs", "engine": "duckdb", "phase": phase, "written": written,
        "seconds": seconds(started), "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    println!("legal-dong-pairing-inputs-summary-json {summary}");
    Ok(())
}

/// Appends the rows of `append.jsonl`, writes the table back and prints the summary line.
///
/// Each load unit (a steward decision file, then the run's changes) is its own commit, recorded
/// by its `derivation_run_id`. The rules already left out every change the table holds, so a
/// registry that answers "already loaded" means the unit collided with an earlier run's: the
/// rows would be dropped without a word, and the command fails instead.
pub(crate) async fn append_changes() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let directory = PathBuf::from(required_env_value(DIR_ENV)?);
    let changes = contract(CHANGE_TABLE)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    // A table is born on its first commit (root ADR-0564 §1), as the Spark job made it.
    committer.create_table_if_absent(changes).await?;
    let shape = committer.table_shape(CHANGE_TABLE).await?;
    // The staged files live as long as the stage: it is kept until the last append is done.
    let stage = Mutex::new(stage_in(&directory)?);
    let units = stage_append(&*locked(&stage)?, changes, &directory)?;
    let (mut steward_rows_appended, mut changes_appended) = (false, false);
    for unit in &units {
        let expected_head = head(&catalog, CHANGE_TABLE).await?;
        let staged = {
            let stage = &*locked(&stage)?;
            stage_unit(stage, changes, unit)?;
            write(stage, &shape, CHANGE_TABLE, "pair_unit", &unit.unit)?
        };
        let appended = append(
            &committer,
            &catalog,
            CHANGE_TABLE,
            staged.as_ref(),
            &unit.unit,
            expected_head,
        )
        .await?;
        ensure!(
            appended,
            "the ingest registry already holds the load unit {} of {} rows the table does not hold; they would be \
             dropped without a word. Each run must load under its own unit.",
            unit.unit,
            unit.rows
        );
        steward_rows_appended |= unit.steward;
        changes_appended |= !unit.steward;
    }
    // The projection is written from the table as it now stands, read back.
    let now_head = head(&catalog, CHANGE_TABLE).await?;
    let held = {
        let stage = &*locked(&stage)?;
        let source = now_head
            .map(|head| at_head(stage, changes, head))
            .transpose()?;
        export_changes(stage, changes, source.as_deref(), &directory, HELD_FILE)?
    };
    drop(stage);
    let appended = json!({
        // "" while the table has no snapshot, as the hub exports read it.
        "change_table_snapshot_id": now_head.map(|head| head.to_string()).unwrap_or_default(),
        "steward_rows_appended": steward_rows_appended,
        "changes_appended": changes_appended,
    });
    let path = directory.join(APPENDED_FILE);
    std::fs::write(&path, format!("{appended}\n"))
        .with_context(|| format!("cannot write {}", path.display()))?;
    let summary = json!({
        "job": "legal_dong_code_change_pairs", "engine": "duckdb", "contract": CHANGE_TABLE,
        "load_units": units.len(), "rows": units.iter().map(|unit| unit.rows).sum::<u64>(), "held_rows": held,
        "appended": appended, "seconds": seconds(started),
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    println!("legal-dong-code-changes-append-summary-json {summary}");
    Ok(())
}
