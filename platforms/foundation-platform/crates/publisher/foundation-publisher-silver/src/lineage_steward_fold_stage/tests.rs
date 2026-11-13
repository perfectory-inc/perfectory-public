//! The fold's rows are the rows the Spark job wrote (root ADR-0559 §3).
//!
//! `fixtures/handoff.json` is a synthetic handoff (PNUs `99999…`) as the exporter writes it, and
//! `fixtures/spark_rows.txt` is what `lineage_steward_fold_to_silver.py` built from it on the
//! production Spark image before it was removed: one line per row, its columns joined by `|`,
//! ordered by `evidence_ref`. The engine-equality verdict the move was made on compared the same
//! SQL files with that job on 50,000 rows; this keeps the binary on those rows.

use anyhow::Context;
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use lakehouse_domain::industrial_complex_lakehouse_contracts;
use stewardship_domain::fold::{FoldHandoff, STEWARD_EVIDENCE_KIND};

use super::{stage_fold, LINEAGE_TABLE, REFUSALS_SQL, ROWS_SQL};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

const HANDOFF: &str = include_str!("fixtures/handoff.json");
const SPARK_ROWS: &str = include_str!("fixtures/spark_rows.txt");
/// A staged row as one line of `spark_rows.txt`, less its last field.
///
/// That field is `derived_at_utc` as epoch seconds: the bundled `DuckDB` has no ICU to read a
/// `TIMESTAMPTZ` back with, so the column is read from the file itself.
const LINE: &str = "concat_ws('|', coalesce(predecessor_pnu, 'NULL'), successor_pnu, relation,      coalesce(cardinality, 'NULL'), effective_date, grade, evidence_kind, evidence_ref,      from_snapshot_id, to_snapshot_id, rules_version, derivation_run_id)";

/// The table's columns with the field ids the lakehouse gives them (column order from 1).
fn shape() -> anyhow::Result<TableShape> {
    let contract = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == LINEAGE_TABLE)
        .context("the lineage contract")?;
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
                })
            })
            .collect::<anyhow::Result<_>>()?,
    })
}

fn derived_at() -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp(1_794_441_600, 0).context("2026-11-12T00:00:00Z")
}

fn handoff() -> anyhow::Result<FoldHandoff> {
    Ok(serde_json::from_str(HANDOFF)?)
}

#[test]
fn the_staged_rows_are_the_rows_the_spark_job_built() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let handoff = handoff()?;
    let staged = stage_fold(&stage, &shape()?, &handoff, derived_at()?)?;

    assert_eq!(
        staged.derivation_run_id,
        "steward-fold-9a1f51f60e851145d5e6b3fe"
    );
    assert_eq!(staged.decision_ids.len(), 120);
    assert_eq!(staged.decision_ids[0], handoff.rows[0].decision_id);
    let file = staged.file.context("a staged file")?;
    assert_eq!(file.name, staged.derivation_run_id);
    let path = file
        .local_path
        .to_str()
        .context("UTF-8")?
        .replace('\\', "/");
    let lines = stage
        .connection()
        .prepare(&format!(
            "SELECT {LINE} FROM read_parquet('{path}') ORDER BY evidence_ref"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let expected = SPARK_ROWS.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), expected.len());
    for (line, expected) in lines.iter().zip(expected) {
        let (row, derived_at) = expected.rsplit_once('|').context("a derived_at field")?;
        assert_eq!(line, row);
        assert_eq!(derived_at, "1794441600");
    }
    // `derived_at_utc`: every row carries the instant the caller gave, as UTC microseconds.
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&file.local_path)?,
    )?
    .build()?;
    let mut stamped = 0;
    for batch in reader {
        let batch = batch?;
        let column = batch
            .column_by_name("derived_at_utc")
            .context("derived_at_utc")?
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .context("a microsecond timestamp column")?;
        assert_eq!(column.timezone(), Some("UTC"));
        for value in column {
            assert_eq!(value, Some(1_794_441_600_000_000));
            stamped += 1;
        }
    }
    assert_eq!(stamped, 120);

    // The same decisions make the same run, whenever they are staged.
    let later = DateTime::from_timestamp(1_794_500_000, 0).context("later")?;
    let again = stage_fold(&stage, &shape()?, &handoff, later)?;
    assert_eq!(again.derivation_run_id, staged.derivation_run_id);
    Ok(())
}

type Change = fn(&mut FoldHandoff);

#[test]
fn a_handoff_the_exporter_would_not_write_is_refused() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let shape = shape()?;
    // Row 0 of the fixture is "not a link" (pending, no predecessor); row 1 is a link.
    let cases: [(Change, &str); 9] = [
        (
            |h| h.schema_version.push('x'),
            "unknown fold handoff schema",
        ),
        (|h| h.row_count = 7, "says 7 rows but holds 6"),
        (
            |h| h.rows[1].evidence_kind = "boundary".to_owned(),
            "not a steward row",
        ),
        (
            |h| h.rows[1].successor_pnu = "99999".to_owned(),
            "not a PNU",
        ),
        (
            |h| h.rows[1].predecessor_pnu = None,
            "a link names a predecessor PNU",
        ),
        (
            |h| h.rows[0].predecessor_pnu = Some("9999900000000000001".to_owned()),
            "'not a link' names no predecessor",
        ),
        (
            |h| h.rows[1].grade = "probable".to_owned(),
            "steward grade probable",
        ),
        (
            |h| h.rows[1].evidence_ref = h.rows[2].evidence_ref.clone(),
            "evidence_ref does not describe its decision",
        ),
        (
            |h| {
                h.rows[2].decision_id = h.rows[1].decision_id.clone();
                h.rows[2].evidence_ref = h.rows[1].evidence_ref.clone();
            },
            "a decision appears twice",
        ),
    ];
    for (change, reason) in cases {
        let mut handoff = handoff()?;
        handoff.rows.truncate(6);
        handoff.row_count = 6;
        change(&mut handoff);
        let outcome = stage_fold(&stage, &shape, &handoff, derived_at()?);
        let message = format!("{:#}", outcome.err().context("the handoff is refused")?);
        assert!(message.contains(reason), "{reason}: {message}");
    }
    Ok(())
}

#[test]
fn a_handoff_of_no_rows_stages_nothing() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let mut handoff = handoff()?;
    handoff.rows.clear();
    handoff.row_count = 0;
    let staged = stage_fold(&stage, &shape()?, &handoff, derived_at()?)?;
    assert!(staged.file.is_none() && staged.decision_ids.is_empty());
    Ok(())
}

#[test]
fn the_sql_names_the_evidence_kind_the_readers_key_on() {
    let literal = format!("'{STEWARD_EVIDENCE_KIND}'");
    assert!(REFUSALS_SQL.contains(&literal) && ROWS_SQL.contains(&literal));
}
