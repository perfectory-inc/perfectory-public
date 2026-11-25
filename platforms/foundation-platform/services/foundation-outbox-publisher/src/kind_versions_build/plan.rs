//! What every lane's run settles before it computes (root ADR-0615 §7), and the bounds of a commit
//! of the whole table (root ADR-0564 §3-4): the pieces of `run` that know nothing of a lane's
//! stage.

use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context};
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use lakehouse_domain::{
    LakehouseTableContract, GOLD_VERSIONS_REMOVAL_COLUMN, GOLD_VERSION_SEQ_COLUMN,
};
use lakehouse_infrastructure::{
    IcebergRestCatalog, IcebergSnapshotSummary, LakehouseCatalogConfig,
};

use super::{rehearsal::Rehearsal, Serves};
use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_versions_commit as versions,
    gold_versions_support::{contract, MAX_CHANGED_KEY_FRACTION},
};

/// The bounds of root ADR-0564 §3-4 on a commit of the whole table, against its base: it may not
/// hold fewer current rows than `max_loss` allows, nor change more keys than
/// [`MAX_CHANGED_KEY_FRACTION`] unless `explain` (asked only when there is a base) says why. Returns
/// the explanation the commit records.
///
/// # Errors
/// Refuses a commit past a bound (nothing is appended), and returns `explain`'s failure.
pub(super) fn whole_table_bounds(
    table: &str,
    base: Option<&versions::Base>,
    (current_records, keys_changed): (u64, u64),
    max_loss: f64,
    explain: impl FnOnce() -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Option<String>> {
    let Some(base) = base else {
        return Ok(None);
    };
    #[allow(clippy::cast_precision_loss)]
    let floor = (1.0 - max_loss) * base.current_records as f64;
    #[allow(clippy::cast_precision_loss)]
    if (current_records as f64) < floor {
        bail!(
            "{table} would hold {current_records} current rows, fewer than the {floor:.0} its \
             {} allow (gold-panel-rebuild.contract.json max_row_loss_fraction): \
             nothing was appended",
            base.current_records
        );
    }
    #[allow(clippy::cast_precision_loss)]
    let fraction = keys_changed as f64 / (base.current_records.max(1)) as f64;
    match explain()? {
        Some(why) => Ok(Some(why)),
        None if fraction > MAX_CHANGED_KEY_FRACTION => bail!(
            "{keys_changed} of {table}'s {} current rows changed ({fraction:.3}, over \
             {MAX_CHANGED_KEY_FRACTION}) with neither the code nor the served edition moved \
             and nothing the kind declares explaining it: nothing was appended (root ADR-0564 \
             §3.4)",
            base.current_records
        ),
        None => Ok(None),
    }
}

/// The versions table's columns but the two each commit adds: the replaced table's, and the digest
/// a replaced table whose producer wrote none gains (the served boundaries).
pub(super) fn content_columns(versions_contract: &LakehouseTableContract) -> Vec<&'static str> {
    versions_contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| ![GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(name))
        .collect()
}

/// What a lane's run settles before it computes anything (root ADR-0615 §7): whether it rehearses,
/// the catalog and the committer it writes with, the versions table's history and base commit, and
/// the inputs it reads at (a shadow's are its replaced build's, root ADR-0564 §6).
pub(super) struct Plan {
    pub(super) rehearsal: Option<Rehearsal>,
    pub(super) catalog_config: LakehouseCatalogConfig,
    pub(super) catalog: IcebergRestCatalog,
    pub(super) committer: Option<LakehouseDataCommitter>,
    /// Whether this run created the versions table.
    pub(super) created: bool,
    pub(super) history: Vec<IcebergSnapshotSummary>,
    pub(super) base: Option<versions::Base>,
    pub(super) head: Option<i64>,
    pub(super) pins: BTreeMap<String, i64>,
    /// The served parcel edition the run builds against.
    pub(super) edition: String,
    pub(super) shadow: Option<versions::Shadow>,
}

/// [`Plan`] of the versions table `table`, which the lane `lane` serves and which reads `inputs`
/// against the served parcel edition `served_edition`.
///
/// # Errors
/// Returns the catalog's or the committer's failure, a versions table that does not exist after its
/// creation, and a lane the contract does not name.
pub(super) async fn plan(
    table: &'static str,
    lane: Serves,
    inputs: &[&'static str],
    served_edition: String,
) -> anyhow::Result<Plan> {
    let versions_contract = contract(table)?;
    // A rehearsal reads with the reader's catalog token and writes nothing (root ADR-0618 §9).
    let rehearsal = Rehearsal::from_env()?;
    let catalog_config = if rehearsal.is_some() {
        LakehouseCatalogConfig::reader_from_env()?
    } else {
        LakehouseCatalogConfig::from_env()?
    };
    let catalog = IcebergRestCatalog::new(catalog_config.clone())?;
    let committer = match rehearsal {
        Some(_) => None,
        None => Some(LakehouseDataCommitter::from_env().await?),
    };
    let created = match &committer {
        Some(committer) => committer.create_table_if_absent(versions_contract).await?,
        None => false,
    };

    // A rehearsal of a table that does not exist yet rehearses its first build.
    let history = catalog.load_main_snapshot_history(table).await?;
    ensure!(
        rehearsal.is_some() || history.is_some(),
        "{table} does not exist after its creation"
    );
    let history = history.unwrap_or_default();
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let served_table = match lane {
        Serves::ByPnuLane(name) => by_pnu_serving_manifest_publish::gold_table(
            ByPnuLane::named(name)?
                .with_context(|| format!("the contract names no {name} lane"))?,
        ),
        Serves::Table(served) => served,
    };
    // A rehearsal builds its candidate from the inputs' heads, never as a shadow.
    let versions::Planned {
        pins,
        served_edition: edition,
        shadow,
    } = versions::planned_inputs(
        &catalog,
        table,
        if rehearsal.is_some() {
            table
        } else {
            served_table
        },
        inputs,
        served_edition,
    )
    .await?;
    let edition = rehearsal
        .as_ref()
        .map_or(edition, |rehearsal| rehearsal.edition.clone());
    Ok(Plan {
        rehearsal,
        catalog_config,
        catalog,
        committer,
        created,
        history,
        base,
        head,
        pins,
        edition,
        shadow,
    })
}
