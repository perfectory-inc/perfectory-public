//! Moves steward decisions from the database into the lakehouse (root ADR-0115 §9), the same
//! round trip the map-edit fold makes (root ADR-0112 §7):
//!
//! 1. `export-lineage-steward-fold` writes the standing, unfolded decisions as lineage rows;
//! 2. the Spark job `lineage_steward_fold_to_silver.py` appends them to `silver.parcel_lineage`;
//! 3. `record-lineage-steward-folds` reads the job's summary and records which decisions landed.
//!
//! The row shape is `stewardship_domain::fold::lineage_row`; this module only moves files.

use anyhow::{bail, Context};
use serde::Deserialize;
use stewardship_domain::fold::{lineage_row, FoldHandoff, FOLD_HANDOFF_SCHEMA_VERSION};
use stewardship_infrastructure::PgLineageStewardshipStore;
use uuid::Uuid;

use crate::public_data_control_support::required_env_value;

const OUTPUT_ENV: &str = "FOUNDATION_PLATFORM_LINEAGE_STEWARD_FOLD_OUTPUT";
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

/// What the Spark job reports after appending.
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
