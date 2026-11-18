//! Moves steward decisions from the database into the lakehouse (root ADR-0115 §9), the same
//! round trip the map-edit fold makes (root ADR-0112 §7):
//!
//! 1. `export-lineage-steward-fold` writes the standing, unfolded decisions as lineage rows;
//! 2. `append-lineage-steward-fold` checks the file, stages the rows with `DuckDB` and appends them
//!    to `silver.parcel_lineage` once (root ADR-0560; it was the Spark job
//!    `lineage_steward_fold_to_silver.py`);
//! 3. `record-lineage-steward-folds` reads that step's summary and records which decisions landed.
//!
//! 법정동 pairs make the same round trip with their own reader (root ADR-0263):
//! `export-legal-dong-steward-decisions` writes the standing approvals, the pairing run
//! (`legal_dong_code_change_pairs.py`) checks and appends them to `reference.legal_dong_code_change`,
//! and `record-lineage-steward-folds` records the ones it appended.
//!
//! The row shape is `stewardship_domain::fold::lineage_row`; this module only moves files.

use std::path::Path;

use anyhow::{bail, Context};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::DuckDbParquetStage,
    lineage_steward_fold_stage::{stage_fold, LINEAGE_TABLE},
};
use lakehouse_domain::industrial_complex_lakehouse_contracts;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Deserialize;
use stewardship_domain::fold::{lineage_row, FoldHandoff, FOLD_HANDOFF_SCHEMA_VERSION};
use stewardship_infrastructure::PgLineageStewardshipStore;
use uuid::Uuid;

use crate::public_data_control_support::required_env_value;

const OUTPUT_ENV: &str = "FOUNDATION_PLATFORM_LINEAGE_STEWARD_FOLD_OUTPUT";
const LEGAL_DONG_OUTPUT_ENV: &str = "FOUNDATION_PLATFORM_LEGAL_DONG_STEWARD_DECISIONS_OUTPUT";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_LINEAGE_STEWARD_FOLD_SUMMARY";

async fn store() -> anyhow::Result<PgLineageStewardshipStore> {
    let pool = sqlx::PgPool::connect(&required_env_value("DATABASE_URL")?)
        .await
        .context("cannot connect to the Foundation database")?;
    Ok(PgLineageStewardshipStore::new(pool))
}

pub(crate) async fn export() -> anyhow::Result<()> {
    let path = required_env_value(OUTPUT_ENV)?;
    let decisions = store().await?.foldable_decisions().await?;
    let rows = decisions
        .iter()
        .map(lineage_row)
        .collect::<Result<Vec<_>, _>>()
        .context("a standing decision does not make a lineage row")?;
    let handoff = FoldHandoff {
        schema_version: FOLD_HANDOFF_SCHEMA_VERSION.to_owned(),
        row_count: rows.len(),
        rows,
    };
    std::fs::write(&path, serde_json::to_vec(&handoff)?)
        .with_context(|| format!("cannot write {path}"))?;
    println!(
        "lineage-steward-fold-export-json {}",
        serde_json::json!({"output": path, "rows": handoff.row_count})
    );
    Ok(())
}

pub(crate) async fn export_legal_dong() -> anyhow::Result<()> {
    let path = required_env_value(LEGAL_DONG_OUTPUT_ENV)?;
    let export = store().await?.legal_dong_decisions().await?;
    std::fs::write(&path, serde_json::to_vec(&export)?)
        .with_context(|| format!("cannot write {path}"))?;
    println!(
        "legal-dong-steward-decisions-export-json {}",
        serde_json::json!({
            "output": path,
            "review_items": export.review_items,
            "decisions": export.decision_count,
        })
    );
    Ok(())
}

/// Step 2: appends the exported decisions to `silver.parcel_lineage` in one snapshot, or reports
/// that `main` already records the run. Reads the export's file and writes the summary `record`
/// reads; the last stdout line is `lineage-steward-fold-json {json}`.
pub(crate) async fn append() -> anyhow::Result<()> {
    let input = required_env_value(OUTPUT_ENV)?;
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let handoff: FoldHandoff = serde_json::from_slice(
        &std::fs::read(&input).with_context(|| format!("cannot read {input}"))?,
    )
    .with_context(|| format!("{input} is not a fold handoff"))?;
    let contract = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == LINEAGE_TABLE)
        .with_context(|| format!("no contract for {LINEAGE_TABLE}"))?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let shape = committer.table_shape(LINEAGE_TABLE).await?;
    // The re-run guard, read the way every load reads it (root ADR-0069).
    let already_ingested = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?
        .load_ingested_batch_objects(LINEAGE_TABLE)
        .await?
        .with_context(|| format!("{LINEAGE_TABLE} does not exist"))?;
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = DuckDbParquetStage::open(work)?;
    let staged = stage_fold(&stage, &shape, &handoff, chrono::Utc::now())?;
    let (appended, snapshot_id) = match &staged.file {
        None => (false, None),
        Some(file) => {
            let outcome = committer
                .append(DataAppend {
                    table_name: LINEAGE_TABLE,
                    contract,
                    files: std::slice::from_ref(file),
                    batch: std::slice::from_ref(&staged.derivation_run_id),
                    already_ingested: &already_ingested,
                    summary: std::collections::BTreeMap::new(),
                })
                .await?;
            match outcome {
                AppendOutcome::Appended { snapshot, .. } => (true, Some(snapshot.snapshot_id)),
                AppendOutcome::AlreadyIngested => (false, None),
            }
        }
    };
    drop(stage);
    let summary = serde_json::json!({
        "job": "append-lineage-steward-fold",
        "derivation_run_id": staged.derivation_run_id,
        "decision_ids": staged.decision_ids,
        "rows": staged.decision_ids.len(),
        "appended": appended,
        "snapshot_id": snapshot_id,
    });
    std::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("cannot write {summary_path}"))?;
    println!("lineage-steward-fold-json {summary}");
    Ok(())
}

/// What the append step reports.
#[derive(Debug, Deserialize)]
struct FoldSummary {
    derivation_run_id: String,
    decision_ids: Vec<Uuid>,
}

pub(crate) async fn record() -> anyhow::Result<()> {
    let path = required_env_value(SUMMARY_ENV)?;
    let text = std::fs::read_to_string(&path).with_context(|| format!("cannot read {path}"))?;
    let summary: FoldSummary =
        serde_json::from_str(&text).with_context(|| format!("{path} is not a fold summary"))?;
    if summary.decision_ids.is_empty() {
        bail!("{path} names no decisions; nothing was folded");
    }
    let recorded = store()
        .await?
        .record_folds(&summary.decision_ids, &summary.derivation_run_id)
        .await?;
    println!(
        "lineage-steward-fold-record-json {}",
        serde_json::json!({
            "derivation_run_id": summary.derivation_run_id,
            "decisions": summary.decision_ids.len(),
            "newly_recorded": recorded,
        })
    );
    Ok(())
}
