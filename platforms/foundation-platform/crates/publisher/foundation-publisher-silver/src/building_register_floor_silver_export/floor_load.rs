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
    AppendOutcome, DataAppend, LakehouseDataCommitter, TableField, TableShape,
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use super::history_gate::identity_under_derivation;
use crate::{
    duckdb_parquet_stage::{sql_path, DuckDbParquetStage, LakehouseRead},
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

/// Loads the edition `source_snapshot_id` the export wrote under `handoff`, in `work`.
///
/// Without `live` nothing is committed: the rows are checked and staged and the summary says what
/// would have been appended (the engine verdict runs this).
///
/// # Errors
/// A check refuses the rows, the table records part of the batch, or the commit fails.
#[expect(
    clippy::too_many_lines,
    reason = "one edition's load in order; to be split by the handoff"
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the stage is locked once for the check, the metrics and the staging"
)]
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
    let batch = vec![identity.clone()];
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let ingested = catalog
        .load_ingested_batch_objects(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist"))?;
    if ingested.contains(&identity) {
        return Ok(FloorLoad {
            summary: json!({"status": "already_appended", "source_snapshot_ids": [source_snapshot_id]}),
            appended: false,
            snapshot_id: None,
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
    let stage = std::sync::Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        work,
        &LakehouseRead::with_reader_keys_from_env()?,
    )?);
    let (rows, metrics, files) = {
        let stage = stage
            .lock()
            .map_err(|_| anyhow::anyhow!("an earlier step of the load panicked"))?;
        let relation = handoff_rows(contract, handoff)?;
        let rows = check_release(
            &stage,
            contract,
            &relation,
            source_snapshot_id,
            expected_rows,
        )?;
        let metrics = quality_metrics(&stage, contract, &relation)?;
        // The staged files hold the edition's rows once more, beside the handoff.
        stage.require_room(0)?;
        let files = stage.write_bucketed(
            &shape,
            &format!("SELECT * FROM {relation}"),
            "mgm_bldrgst_pk",
            16,
            &format!(
                "building-register-floors-{}",
                &source_snapshot_id[source_snapshot_id.len().saturating_sub(16)..]
            ),
            STAGED_FILE_BYTES,
        )?;
        (rows, metrics, files)
    };
    let mut appended = false;
    let mut snapshot = None;
    if let Some(committer) = &committer {
        let outcome = committer
            .append_at_head(
                DataAppend {
                    table_name: TABLE,
                    contract,
                    files: &files,
                    batch: &batch,
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
                added_records == rows,
                "the commit of {TABLE} added {added_records} rows, the edition holds {rows}"
            );
            snapshot = Some(committed.snapshot_id);
            appended = true;
        }
    }
    drop(stage);
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
    let persisted = appended.then_some(rows);
    let mut quality = metrics
        .into_iter()
        .map(|(name, value)| (name, json!(value)))
        .collect::<serde_json::Map<_, _>>();
    if let Some(persisted) = persisted {
        quality.insert("persisted_row_count".to_owned(), json!(persisted));
    }
    let (namespace, table) = TABLE.split_once('.').context("namespace.table")?;
    let mut bytes_written = 0_u64;
    for file in &files {
        bytes_written += std::fs::metadata(&file.local_path)?.len();
    }
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
        "row_count": rows,
        "persisted_row_count": persisted,
        "quality_metrics": quality,
        "column_count": names.len(),
        "columns": names,
        "required_columns": required,
        "source_snapshot_count": 1,
        "source_snapshot_ids": [source_snapshot_id],
        "source_snapshot_truncated": false,
    });
    if let (Some(bytes), Some(object)) =
        (appended.then_some(bytes_written), summary.as_object_mut())
    {
        object.insert("bytes_written".to_owned(), json!(bytes));
    }
    Ok(FloorLoad {
        summary,
        appended,
        snapshot_id: snapshot,
    })
}
