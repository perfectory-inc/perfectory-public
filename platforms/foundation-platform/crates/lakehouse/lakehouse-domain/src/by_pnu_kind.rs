//! The Gold table of each by-PNU data kind (root ADR-0193), derived from its lane line.
//!
//! A kind is one line of `config/r2-connections.contract.json` `by_pnu_lanes` that names a `host`:
//! its section of the host's panel and the content columns that section yields. Its Gold table is
//! `gold.<lane with - as _>`: `pnu`, those columns, `attached_via` for a parcel kind (the lineage
//! path a carried section came through, root ADR-0113 §6), then the content fingerprint and
//! lineage every by-PNU panel has. Nothing of a kind's table is written by hand.

use std::sync::OnceLock;

use serde::Deserialize;

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat, LakehouseRowKey,
    LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

const R2_CONNECTION_CONTRACT: &str =
    include_str!("../../../../config/r2-connections.contract.json");
/// The host whose kinds carry their sections through the parcel lineage.
const PARCEL_HOST: &str = "parcel_by_pnu_gateway";

#[derive(Deserialize)]
struct Contract {
    #[serde(default)]
    by_pnu_lanes: std::collections::BTreeMap<String, Lane>,
}

#[derive(Deserialize)]
struct Lane {
    host: Option<String>,
    #[serde(default)]
    columns: Vec<String>,
}

/// The Gold tables of the contract's kinds, in lane name order; empty when the contract's lane
/// lines cannot be read (`by_pnu_kind_table_error` says why, and the contract tests refuse it).
#[must_use]
pub fn by_pnu_kind_tables() -> &'static [LakehouseTableContract] {
    derived().as_ref().map_or(&[], Vec::as_slice)
}

/// Why the kind tables could not be derived, or `None`.
#[must_use]
pub fn by_pnu_kind_table_error() -> Option<&'static str> {
    derived().as_ref().err().map(String::as_str)
}

fn derived() -> &'static Result<Vec<LakehouseTableContract>, String> {
    static TABLES: OnceLock<Result<Vec<LakehouseTableContract>, String>> = OnceLock::new();
    TABLES.get_or_init(|| derive(R2_CONNECTION_CONTRACT))
}

/// The kind tables of a contract text.
///
/// # Errors
/// Names a lane line without content columns, or a column that is neither a `*_json` field nor
/// `<name>:long`.
pub fn derive(contract: &str) -> Result<Vec<LakehouseTableContract>, String> {
    let contract: Contract = serde_json::from_str(contract)
        .map_err(|error| format!("invalid R2 connection contract: {error}"))?;
    contract
        .by_pnu_lanes
        .into_iter()
        .filter_map(|(name, lane)| lane.host.map(|host| (name, host, lane.columns)))
        .map(|(name, host, columns)| table(&name, &host, &columns))
        .collect()
}

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

fn table(lane: &str, host: &str, content: &[String]) -> Result<LakehouseTableContract, String> {
    if content.is_empty() || !content.iter().any(|name| name.ends_with("_json")) {
        return Err(format!(
            "by_pnu_lanes.{lane}.columns must name the section's content columns, a *_json field first"
        ));
    }
    let mut columns = vec![column("pnu", "string", true)];
    for (index, spec) in content.iter().enumerate() {
        let (name, logical_type) = match spec.split_once(':') {
            None if spec.ends_with("_json") => (spec.as_str(), "string"),
            Some((name, "long")) if !name.ends_with("_json") => (name, "long"),
            _ => {
                return Err(format!(
                    "by_pnu_lanes.{lane}.columns: {spec} is neither a *_json field nor <name>:long"
                ))
            }
        };
        // The first column is the section itself: a row exists only where it does.
        columns.push(column(leak(name.to_owned()), logical_type, index == 0));
    }
    if host == PARCEL_HOST {
        columns.push(column("attached_via", "string", false));
    }
    columns.extend([
        column("row_digest", "string", true),
        column("source_snapshot_id", "string", true),
        column("published_at_utc", "string", true),
    ]);
    Ok(LakehouseTableContract {
        table_name: leak(format!("gold.{}", lane.replace('-', "_"))),
        layer: LakehouseLayer::Gold,
        physical_format: LakehousePhysicalFormat::Parquet,
        serving_role: LakehouseServingRole::Projection,
        current_row_predicate: None,
        row_versions: LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Columns(&["pnu"]),
        },
        columns: Box::leak(columns.into_boxed_slice()),
        partition_spec: &["source_snapshot_id"],
        sort_order: &["pnu"],
        write_distribution: LakehouseWriteDistribution::Range,
        quality_gates: &[
            "one row per pnu",
            "pnu matches the cadastral grammar",
            "every row has its section",
            "row_digest contains content only",
            "published_at_utc is present",
        ],
        load: LakehouseLoadUnit::Derived,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kind_line_is_its_gold_table() -> Result<(), String> {
        let tables = derive(
            r#"{"by_pnu_lanes": {
                "parcel-example": {"host": "parcel_by_pnu_gateway", "kind": "example",
                                   "columns": ["things_json", "thing_total:long"]},
                "building-example": {"host": "building_by_pnu_gateway", "kind": "example",
                                     "columns": ["units_json"]},
                "whole": {"pack_only": true}}}"#,
        )?;
        let shape = tables
            .iter()
            .map(|table| {
                (
                    table.table_name,
                    table
                        .columns
                        .iter()
                        .map(|column| (column.name, column.logical_type, column.required))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let lineage = [
            ("row_digest", "string", true),
            ("source_snapshot_id", "string", true),
            ("published_at_utc", "string", true),
        ];
        assert_eq!(
            shape,
            vec![
                (
                    "gold.building_example",
                    [
                        vec![("pnu", "string", true), ("units_json", "string", true)],
                        lineage.to_vec()
                    ]
                    .concat()
                ),
                (
                    "gold.parcel_example",
                    [
                        vec![
                            ("pnu", "string", true),
                            ("things_json", "string", true),
                            ("thing_total", "long", false),
                            ("attached_via", "string", false)
                        ],
                        lineage.to_vec()
                    ]
                    .concat()
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn a_kind_line_without_a_json_section_or_with_an_unknown_type_is_refused() {
        for columns in ["[]", r#"["total:long"]"#, r#"["things_json", "x:double"]"#] {
            let contract = format!(
                r#"{{"by_pnu_lanes": {{"parcel-x": {{"host": "parcel_by_pnu_gateway", "columns": {columns}}}}}}}"#
            );
            assert!(derive(&contract).is_err(), "{columns}");
        }
    }

    #[test]
    fn the_contracts_kinds_derive() {
        assert_eq!(by_pnu_kind_table_error(), None);
        assert!(by_pnu_kind_tables()
            .iter()
            .any(|table| table.table_name == "gold.parcel_zoning"));
    }
}
