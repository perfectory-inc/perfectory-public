//! What every Gold versions build of the publisher reads besides its own stage: the table contracts,
//! the rebuild contract's bounds, the served parcel edition and the run summary
//! (root ADR-0564, ADR-0615 §7). The builds are `kind_versions_build` (every by-PNU kind lane),
//! `place_id_versions_build` and `lineage_review_queue_versions_build`.

use anyhow::Context;
use foundation_publisher_silver::vworld_parcel_source_contract;
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use serde_json::Value;

const PARCEL_SOURCE_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");
pub(crate) const REBUILD_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/gold-panel-rebuild.contract.json");
/// Above this share of the current rows changed in one commit the run stops, unless the code or
/// the served edition moved, which explains a change of any size (root ADR-0346, ADR-0564 §3-4).
pub(crate) const MAX_CHANGED_KEY_FRACTION: f64 = 0.5;

pub(crate) fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The bound a by-PNU kind's Gold may lose rows by (`gold-panel-rebuild.contract.json`
/// `kind_tables.max_row_loss_fraction`, the kinds' one value).
fn max_row_loss_fraction() -> anyhow::Result<f64> {
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    contract["kind_tables"]["max_row_loss_fraction"]
        .as_f64()
        .context("gold-panel-rebuild.contract.json names no kind_tables.max_row_loss_fraction")
}

/// The bound `replaced`'s versions table may lose rows by: the table's own when the rebuild contract
/// lists it (`tables.<replaced>.max_row_loss_fraction`, a host panel), else the kinds' one value.
pub(crate) fn max_row_loss_fraction_of(replaced: &str) -> anyhow::Result<f64> {
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    match contract["tables"][replaced]["max_row_loss_fraction"].as_f64() {
        Some(own) => Ok(own),
        None => max_row_loss_fraction(),
    }
}

/// The served parcel edition's `silver.parcel_boundaries.source_snapshot_id`.
pub(crate) fn served_edition() -> anyhow::Result<String> {
    let view = vworld_parcel_source_contract::served_edition_view(PARCEL_SOURCE_CONTRACT)?;
    let contract: Value = serde_json::from_str(PARCEL_SOURCE_CONTRACT)?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("the parcel source contract names no snapshot_id_prefix")?;
    let edition = view["edition"]
        .as_str()
        .context("the served edition view names no edition")?;
    Ok(format!("{prefix}{edition}"))
}

/// Writes the run's summary file, what the unit's script and the cut-over's gold step read.
pub(crate) fn write_summary(path: &str, summary: &Value) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(summary)?)
        .with_context(|| format!("cannot write {path}"))
}
