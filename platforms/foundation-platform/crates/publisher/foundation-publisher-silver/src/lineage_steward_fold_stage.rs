//! Stages standing steward decisions as `silver.parcel_lineage` rows (root ADR-0115 §9).
//!
//! Step 2 of the fold, in `DuckDB` inside the publisher (root ADR-0559, ADR-0560): it was the
//! Spark job `lineage_steward_fold_to_silver.py`, the first transform to leave Spark with its
//! commit. `export-lineage-steward-fold` wrote the decisions as lineage rows
//! (`stewardship_domain::fold::lineage_row` decides their shape); this checks the file, computes
//! the rows and their run id in SQL, and stages one Parquet file. The caller appends it once
//! (`foundation_publisher_core::lakehouse_data_commit`) and writes the summary
//! `record-lineage-steward-folds` reads back.
//!
//! The run id is the digest of the decision ids, so staging the same file again names the same run
//! and the append's re-run guard skips it.

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use duckdb::params;
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use stewardship_domain::fold::{FoldHandoff, StewardEvidence, FOLD_HANDOFF_SCHEMA_VERSION};

use crate::duckdb_parquet_stage::DuckDbParquetStage;

/// The table the fold appends to.
pub const LINEAGE_TABLE: &str = "silver.parcel_lineage";

const REFUSALS_SQL: &str = include_str!("lineage_steward_fold_stage/refusals.sql");
const ROWS_SQL: &str = include_str!("lineage_steward_fold_stage/rows.sql");
/// The one value `rows.sql` takes from its caller.
const DERIVED_AT: &str = "{derived_at_epoch_seconds}";

/// A checked handoff, staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedFold {
    /// The run every row carries: the append's batch identity (the table's load unit is the run).
    pub derivation_run_id: String,
    /// The decisions the rows fold, in the handoff's order.
    pub decision_ids: Vec<String>,
    /// The rows, as one Parquet file; `None` for a handoff of no rows.
    pub file: Option<StagedFile>,
}

/// Checks `handoff` and stages its lineage rows, each stamped `derived_at` (whole seconds).
///
/// # Errors
/// Refuses anything the exporter would not write: another schema, a count that disagrees, a row
/// that is not a steward row, a successor that is not a PNU, a link without a predecessor PNU, a
/// "not a link" with one, another grade, an `evidence_ref` that does not describe its decision,
/// and a decision that appears twice.
pub fn stage_fold(
    stage: &DuckDbParquetStage,
    shape: &TableShape,
    handoff: &FoldHandoff,
    derived_at: DateTime<Utc>,
) -> anyhow::Result<StagedFold> {
    ensure!(
        handoff.schema_version == FOLD_HANDOFF_SCHEMA_VERSION,
        "unknown fold handoff schema {:?}",
        handoff.schema_version
    );
    ensure!(
        handoff.row_count == handoff.rows.len(),
        "handoff says {} rows but holds {}",
        handoff.row_count,
        handoff.rows.len()
    );
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TABLE fold_handoff (ordinal BIGINT NOT NULL, decision_id VARCHAR NOT NULL, \
         predecessor_pnu VARCHAR, successor_pnu VARCHAR NOT NULL, relation VARCHAR NOT NULL, \
         grade VARCHAR NOT NULL, evidence_kind VARCHAR NOT NULL, evidence_ref VARCHAR NOT NULL, \
         from_snapshot_id VARCHAR NOT NULL, to_snapshot_id VARCHAR NOT NULL, \
         effective_date VARCHAR NOT NULL)",
    )?;
    let mut insert =
        connection.prepare("INSERT INTO fold_handoff VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
    for (ordinal, row) in handoff.rows.iter().enumerate() {
        let evidence: StewardEvidence =
            serde_json::from_str(&row.evidence_ref).with_context(|| {
                format!(
                    "{}: evidence_ref is not steward evidence",
                    row.successor_pnu
                )
            })?;
        ensure!(
            evidence.decision_id == row.decision_id && !evidence.evidence_etag.is_empty(),
            "{}: evidence_ref does not describe its decision",
            row.successor_pnu
        );
        insert.execute(params![
            i64::try_from(ordinal)?,
            row.decision_id,
            row.predecessor_pnu,
            row.successor_pnu,
            row.relation,
            row.grade,
            row.evidence_kind,
            row.evidence_ref,
            row.from_snapshot_id,
            row.to_snapshot_id,
            row.effective_date,
        ])?;
    }
    drop(insert);

    let refused = connection
        .prepare(REFUSALS_SQL)?
        .query_map([], |row| {
            Ok(format!(
                "{}: {}",
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if !refused.is_empty() {
        bail!(
            "the fold handoff holds {} row(s) the exporter would not write: {}",
            refused.len(),
            refused
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    let decision_ids = handoff
        .rows
        .iter()
        .map(|row| row.decision_id.clone())
        .collect::<Vec<_>>();
    if decision_ids.is_empty() {
        return Ok(StagedFold {
            derivation_run_id: String::new(),
            decision_ids,
            file: None,
        });
    }
    let rows_sql = ROWS_SQL.replace(DERIVED_AT, &derived_at.timestamp().to_string());
    let derivation_run_id: String = connection.query_row(
        &format!("SELECT DISTINCT derivation_run_id FROM ({rows_sql})"),
        [],
        |row| row.get(0),
    )?;
    let file = stage.write(shape, &rows_sql, &derivation_run_id)?;
    Ok(StagedFold {
        derivation_run_id,
        decision_ids,
        file: Some(file),
    })
}

#[cfg(test)]
mod tests;
