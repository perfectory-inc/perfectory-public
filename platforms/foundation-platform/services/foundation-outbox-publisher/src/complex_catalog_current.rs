//! The industrial-complex catalog its readers read since its takeover (root ADR-0564 §6, W103):
//! the current rows of `gold.complex_catalog_versions`, by the contract's rule (root ADR-0267).
//!
//! The canonical load and the profile export read it the way they read every Gold table, through
//! the frozen snapshot reader (root ADR-0622, open question 1: both move to `DuckDB` together with
//! that reader's removal), and take its current rows here, once.

use lakehouse_domain::GOLD_COMPLEX_CATALOG;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::gold_versions_support::contract;

/// The table the catalog is read from.
pub(crate) const CATALOG_VERSIONS: &str = "gold.complex_catalog_versions";

/// The current rows among `rows` (a snapshot of [`CATALOG_VERSIONS`]), each with only the columns
/// of the replaced table (`gold.complex_catalog`): what the readers were built on. The versions
/// table's own columns (the row digest, the version and the removal marker) are not the complex's
/// content, so a profile written before the takeover keeps its content hash.
pub(crate) fn current_rows(
    rows: &[JsonMap<String, JsonValue>],
) -> anyhow::Result<Vec<JsonMap<String, JsonValue>>> {
    let current = contract(CATALOG_VERSIONS)?.current_rows(rows)?;
    Ok(current
        .into_iter()
        .map(|row| {
            GOLD_COMPLEX_CATALOG
                .columns
                .iter()
                .filter_map(|column| {
                    row.get(column.name)
                        .map(|value| (column.name.to_owned(), value.clone()))
                })
                .collect()
        })
        .collect())
}

/// The catalog's snapshot table contract, for the snapshot reader.
pub(crate) fn versions_contract(
) -> anyhow::Result<&'static lakehouse_domain::LakehouseTableContract> {
    contract(CATALOG_VERSIONS)
}
