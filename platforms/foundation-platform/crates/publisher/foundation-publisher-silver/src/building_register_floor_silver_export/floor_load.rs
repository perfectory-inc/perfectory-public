//! `load-building-register-floor-silver`: one FLOOR edition appended whole (root ADR-0559).
//!
//! The publisher appends it to `silver.building_register_floors` (root ADR-0599). It was
//! `silver_scalar_handoff_to_lakehouse.py` (Spark), the script's last user.
//!
//! The native export left the edition as Parquet files (`parquet_row_writer`). `DuckDB` reads them
//! where they are, the release checks the Silver refresh lanes run judge the rows before anything is
//! staged (`release_compare_stage::check_release`: required values, checksum, normalization status,
//! one edition, the expected count), the rows are staged one file per bucket of the table's
//! `bucket(16, mgm_bldrgst_pk)` partition, and one commit appends them recording the edition's
//! identity (under its derivation, when one is given). A reader never sees half an edition, and a
//! re-run of a recorded edition appends nothing. The edition is not compared: FLOOR keeps whole
//! editions, and readers pin one (root ADR-0286).
//!
//! The run summary is the one the Spark load printed (`foundation-platform.spark_run_summary.v1`),
//! so the job's validation and its batch audit read it unchanged.

use std::{collections::BTreeMap, path::Path};

use anyhow::{ensure, Context};
use chrono::Utc;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile, TableField, TableShape,
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};
use lakehouse_infrastructure::IcebergRestCatalog;
use serde_json::{json, Value};

use super::history_gate::identity_under_derivation;
use crate::{
    duckdb_parquet_stage::{read_catalog_config, sql_path, DuckDbParquetStage, LakehouseRead},
    release_compare_stage::check_release,
};

/// The table this load appends to.
pub const TABLE: &str = "silver.building_register_floors";
/// The run summary's schema (unchanged from the Spark load).
const RUN_SUMMARY_SCHEMA: &str = "foundation-platform.spark_run_summary.v1";
/// The commit reads a staged file whole into memory: a bucket's rows are split at about this.
const STAGED_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// What one load did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FloorLoad {
    /// The summary the job validates and audits.
    pub summary: Value,
    /// Whether this run appended (false: the edition was already recorded).
    pub appended: bool,
    /// The snapshot the append committed, when it did.
    pub snapshot_id: Option<i64>,
    /// How many files the edition was staged as (one or more per bucket).
    pub staged_files: usize,
}

fn contract() -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(TABLE)
        .with_context(|| format!("no contract for {TABLE}"))
}

/// The table's shape as its contract numbers it: what a dry run stages with, having no writer to
/// ask the live schema of.
fn contract_shape(contract: &LakehouseTableContract) -> anyhow::Result<TableShape> {
    Ok(TableShape {
        location: String::new(),
        fields: contract
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Ok(TableField {
                    id: i32::try_from(index + 1)?,
                    name: column.name.to_owned(),
                    required: column.required,
                    element_id: None,
                })
            })
            .collect::<anyhow::Result<_>>()?,
    })
}

/// The handoff's rows in the table's types: the native writer keeps instants as UTC milliseconds
/// without a zone, the table as microseconds with one.
fn handoff_rows(contract: &LakehouseTableContract, handoff: &Path) -> anyhow::Result<String> {
    let root = sql_path(handoff)?;
    let columns = contract
        .columns
        .iter()
        .map(|column| match column.logical_type {
            "timestamp" => format!("CAST({0} AS TIMESTAMPTZ) AS {0}", column.name),
            "int" => format!("CAST({0} AS INTEGER) AS {0}", column.name),
            "long" => format!("CAST({0} AS BIGINT) AS {0}", column.name),
            _ => column.name.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {columns} FROM read_parquet('{root}/**/*.parquet', union_by_name = true))"
    ))
}

/// The Spark load's quality metrics over the checked rows (`collect_quality_metrics`).
fn quality_metrics(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    rows: &str,
) -> anyhow::Result<BTreeMap<String, i64>> {
    let mut expressions = vec![("row_count".to_owned(), "count(*)".to_owned())];
    for column in contract.columns.iter().filter(|column| column.required) {
        expressions.push((
            format!("{}__null_count", column.name),
            format!("count_if({} IS NULL)", column.name),
        ));
    }
    for column in contract
        .columns
        .iter()
        .filter(|column| column.required && column.logical_type == "string")
    {
        expressions.push((
            format!("{}__empty_count", column.name),
            format!("count_if(length({}) = 0)", column.name),
        ));
    }
    expressions.extend([
        (
            "invalid_checksum_count".to_owned(),
            "count_if(NOT regexp_full_match(row_checksum_sha256, '[0-9a-f]{64}'))".to_owned(),
        ),
        (
            "proposal_required_count".to_owned(),
            "count_if(normalization_status = 'proposal_required')".to_owned(),
        ),
        (
            "invalid_normalization_status_count".to_owned(),
            "count_if(normalization_status NOT IN ('accepted', 'proposal_required'))".to_owned(),
        ),
    ]);
    let select = expressions
        .iter()
        .map(|(_, expression)| format!("CAST({expression} AS BIGINT)"))
        .collect::<Vec<_>>()
        .join(", ");
    let values: Vec<i64> =
        stage
            .connection()
            .query_row(&format!("SELECT {select} FROM {rows}"), [], |row| {
                (0..expressions.len())
                    .map(|index| row.get::<_, i64>(index))
                    .collect()
            })?;
    Ok(expressions
        .into_iter()
        .map(|(name, _)| name)
        .zip(values)
        .collect())
}

/// The export's ready summary schema (`ready_summary`).
const EXPORT_SCHEMA: &str = "foundation-platform.building_register_floor_silver_handoff_export.v1";

/// What the load takes from the export's ready summary: the edition and its row count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyEdition {
    /// The edition's identity, which the append records.
    pub source_snapshot_id: String,
    /// The rows the export wrote; the release check requires exactly these.
    pub row_count: u64,
}

/// Reads the export's ready summary of one Parquet edition of this table.
///
/// # Errors
/// The summary is not ready, or is of another contract, format or edition.
pub fn ready_edition(summary: &Value) -> anyhow::Result<ReadyEdition> {
    ensure!(
        summary["schema_version"] == EXPORT_SCHEMA && summary["status"] == "ready",
        "the FLOOR export summary is not a ready {EXPORT_SCHEMA}"
    );
    let output = &summary["output"];
    ensure!(
        output["contract"] == TABLE && output["format"] == "parquet",
        "the FLOOR export wrote another contract or format"
    );
    let source = summary["source"]["source_snapshot_id"]
        .as_str()
        .context("the FLOOR export summary names no edition")?;
    ensure!(
        output["source_snapshot_ids"] == json!([source]),
        "the FLOOR export wrote another edition than its source"
    );
    Ok(ReadyEdition {
        source_snapshot_id: source.to_owned(),
        row_count: output["row_count"]
            .as_u64()
            .context("the FLOOR export summary has no row count")?,
    })
}

/// The edition's rows, checked and staged: their count, the quality metrics and the staged files.
pub(crate) struct Staged {
    pub(crate) rows: u64,
    metrics: BTreeMap<String, i64>,
    pub(crate) files: Vec<StagedFile>,
}

/// Checks the handoff's rows and stages them one file per bucket (split at [`STAGED_FILE_BYTES`]).
pub(crate) fn stage_edition(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    shape: &TableShape,
    handoff: &Path,
    source_snapshot_id: &str,
    expected_rows: Option<u64>,
) -> anyhow::Result<Staged> {
    let relation = handoff_rows(contract, handoff)?;
    let rows = check_release(
        stage,
        contract,
        &relation,
        source_snapshot_id,
        expected_rows,
    )?;
    let metrics = quality_metrics(stage, contract, &relation)?;
    let files = stage.write_bucketed(
        shape,
        &relation,
        "mgm_bldrgst_pk",
        16,
        &format!(
            "building-register-floors-{}",
            &source_snapshot_id[source_snapshot_id.len().saturating_sub(16)..]
        ),
        STAGED_FILE_BYTES,
    )?;
    Ok(Staged {
        rows,
        metrics,
        files,
    })
}

/// The run summary the Spark load printed (`foundation-platform.spark_run_summary.v1`).
fn run_summary(
    contract: &LakehouseTableContract,
    handoff: &Path,
    source_snapshot_id: &str,
    staged: Staged,
    appended: bool,
    live: bool,
) -> anyhow::Result<Value> {
    let names = contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let required = contract
        .columns
        .iter()
        .filter(|column| column.required)
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let persisted = appended.then_some(staged.rows);
    let mut quality = staged
        .metrics
        .into_iter()
        .map(|(name, value)| (name, json!(value)))
        .collect::<serde_json::Map<_, _>>();
    if let Some(persisted) = persisted {
        quality.insert("persisted_row_count".to_owned(), json!(persisted));
    }
    let (namespace, table) = TABLE.split_once('.').context("namespace.table")?;
    let mut summary = json!({
        "schema_version": RUN_SUMMARY_SCHEMA,
        "job_name": "load-building-register-floor-silver",
        "contract": TABLE,
        "created_at_utc": Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "input": {"kind": "silver_handoff_parquet", "path": handoff.to_string_lossy()},
        "target": {
            "kind": "iceberg",
            "catalog": "lakehouse",
            "namespace": namespace,
            "table": table,
            "qualified_table": format!("lakehouse.{TABLE}"),
        },
        "write_mode": "iceberg",
        "write_disposition": if live { "iceberg_append" } else { "validate_only" },
        "row_count": staged.rows,
        "persisted_row_count": persisted,
        "quality_metrics": quality,
        "column_count": names.len(),
        "columns": names,
        "required_columns": required,
        "source_snapshot_count": 1,
        "source_snapshot_ids": [source_snapshot_id],
        "source_snapshot_truncated": false,
    });
    if appended {
        let mut bytes_written = 0_u64;
        for file in &staged.files {
            bytes_written += std::fs::metadata(&file.local_path)?.len();
        }
        if let Some(object) = summary.as_object_mut() {
            object.insert("bytes_written".to_owned(), json!(bytes_written));
        }
    }
    Ok(summary)
}

/// Where a dry run keeps the files it staged, under its work directory.
///
/// The stage removes its own when it ends; the engine verdict compares these with the table (the
/// code that would commit is the code that stages).
pub const VERDICT_STAGED: &str = "verdict-staged";

fn keep_for_verdict(work: &Path, files: &[StagedFile]) -> anyhow::Result<()> {
    let kept = work.join(VERDICT_STAGED);
    std::fs::create_dir_all(&kept).with_context(|| format!("cannot create {}", kept.display()))?;
    for file in files {
        std::fs::copy(
            &file.local_path,
            kept.join(format!("{}.parquet", file.name)),
        )
        .with_context(|| format!("cannot keep {}", file.local_path.display()))?;
    }
    Ok(())
}

/// Loads the edition `source_snapshot_id` the export wrote under `handoff`, in `work`.
///
/// Without `live` nothing is committed: the rows are checked and staged with the read-only
/// credentials (`LakehouseRead::read_only_from_env`), the staged files are kept under
/// [`VERDICT_STAGED`] in `work`, and the summary says what would have been appended (the engine
/// verdict runs this).
///
/// # Errors
/// A check refuses the rows, the table records part of the batch, or the commit fails.
pub async fn load(
    handoff: &Path,
    work: &Path,
    source_snapshot_id: &str,
    derivation: Option<&str>,
    expected_rows: Option<u64>,
    live: bool,
) -> anyhow::Result<FloorLoad> {
    let contract = contract()?;
    let identity = identity_under_derivation(source_snapshot_id, derivation)?;
    let catalog = IcebergRestCatalog::new(read_catalog_config()?)?;
    let ingested = catalog
        .load_ingested_batch_objects(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?;
    if ingested.contains(&identity) {
        return Ok(FloorLoad {
            summary: json!({"status": "already_appended", "source_snapshot_ids": [source_snapshot_id]}),
            appended: false,
            snapshot_id: None,
            staged_files: 0,
        });
    }
    let head = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id);
    let committer = if live {
        Some(LakehouseDataCommitter::from_env().await?)
    } else {
        None
    };
    let shape = match &committer {
        Some(committer) => committer.table_shape(TABLE).await?,
        None => contract_shape(contract)?,
    };
    // The stage (and its staged files) lives until the commit has uploaded them.
    let stage =
        DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::read_only_from_env()?)?;
    // The staged files hold the edition's rows once more, on the stage's disk. Asked here, not in
    // `stage_edition`: its test must not depend on the disk of the machine it runs on.
    stage.require_room(0)?;
    let staged = stage_edition(
        &stage,
        contract,
        &shape,
        handoff,
        source_snapshot_id,
        expected_rows,
    )?;
    if committer.is_none() {
        keep_for_verdict(work, &staged.files)?;
    }
    let mut snapshot = None;
    if let Some(committer) = &committer {
        let outcome = committer
            .append_at_head(
                DataAppend {
                    table_name: TABLE,
                    contract,
                    files: &staged.files,
                    batch: &[identity],
                    already_ingested: &ingested,
                    summary: BTreeMap::new(),
                },
                head,
            )
            .await?;
        if let AppendOutcome::Appended {
            snapshot: committed,
            added_records,
            ..
        } = outcome
        {
            ensure!(
                added_records == staged.rows,
                "the commit of {TABLE} added {added_records} rows, the edition holds {}",
                staged.rows
            );
            snapshot = Some(committed.snapshot_id);
        }
    }
    let appended = snapshot.is_some();
    let staged_files = staged.files.len();
    let summary = run_summary(
        contract,
        handoff,
        source_snapshot_id,
        staged,
        appended,
        live,
    )?;
    drop(stage);
    Ok(FloorLoad {
        summary,
        appended,
        snapshot_id: snapshot,
        staged_files,
    })
}

#[cfg(test)]
mod tests {
    use super::{ready_edition, ReadyEdition, EXPORT_SCHEMA};

    fn summary(source: &str, written: &str) -> serde_json::Value {
        serde_json::json!({
            "schema_version": EXPORT_SCHEMA,
            "status": "ready",
            "source": {"source_snapshot_id": source},
            "output": {
                "path": "target/lakehouse/floor-runs/run/handoff",
                "format": "parquet",
                "contract": "silver.building_register_floors",
                "row_count": 7,
                "source_snapshot_ids": [written],
            },
        })
    }

    #[test]
    fn a_ready_summary_names_the_edition_and_its_rows() -> anyhow::Result<()> {
        let edition = "building-register-floor-content-v1-2099";
        assert_eq!(
            ready_edition(&summary(edition, edition))?,
            ReadyEdition {
                source_snapshot_id: edition.to_owned(),
                row_count: 7,
            }
        );
        Ok(())
    }

    #[test]
    fn a_summary_of_another_edition_format_or_not_ready_is_refused() {
        assert!(ready_edition(&summary("a", "b")).is_err());
        let mut pending = summary("a", "a");
        pending["status"] = "pending".into();
        assert!(ready_edition(&pending).is_err());
        let mut jsonl = summary("a", "a");
        jsonl["output"]["format"] = "jsonl".into();
        assert!(ready_edition(&jsonl).is_err());
    }
}
