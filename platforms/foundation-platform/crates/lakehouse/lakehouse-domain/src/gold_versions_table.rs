//! The table of appended row versions a Gold is born as, beside the table a `MERGE` replaces rows
//! of (root ADR-0564 §1, §2).
//!
//! `gold.<t>_versions` carries every column of `gold.<t>` as it is, then `version_seq` and
//! `row_state`. A commit appends the rows of the keys it changed, all with one `version_seq` (the
//! table's commit number, which the publisher assigns), and a key that left appends a row marked
//! `removed`. The current rows are the newest per key that are not removal markers
//! ([`LakehouseTableContract::current_row_latest`]). Nothing of the shape is written by hand: it
//! follows from the replaced table's contract.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit, LakehouseRowKey,
    LakehouseRowVersions, LakehouseTableContract,
};

/// What the versions table is named after the table it replaces.
pub const GOLD_VERSIONS_TABLE_SUFFIX: &str = "_versions";
/// The table's commit number, the same on every row of one commit.
///
/// The newest build commit's `foundation.version-seq` + 1. It orders a key's versions; neither `published_at_utc` (a string the
/// producer stamps when it starts) nor the snapshot id (not in time order) does.
pub const GOLD_VERSION_SEQ_COLUMN: &str = "version_seq";
/// `removed` on the row a key that left appends (`REMOVED_MARKER`), null on every other row: the
/// name Silver's removal markers use.
pub const GOLD_VERSIONS_REMOVAL_COLUMN: &str = "row_state";
/// The partition of a table keyed by PNU first: the 시도 (17 values).
///
/// A by-PNU shard then skips
/// other 시도's appended files by manifest bounds, and compaction merges files across versions,
/// which a partition per commit (`source_snapshot_id`) would forbid.
pub const GOLD_VERSIONS_PNU_PARTITION: &str = "truncate(2, pnu)";
/// The partition column a replaced Gold has per commit, which its versions table drops.
const PER_COMMIT_PARTITION: &str = "source_snapshot_id";

fn leak_str(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

fn leak_slice<T>(values: Vec<T>) -> &'static [T] {
    Box::leak(values.into_boxed_slice())
}

/// The versions table of `replaced`.
///
/// # Errors
/// Refuses a table that is not a Gold a load replaces rows of by a column key, one that already
/// has a `version_seq` or `row_state` column, and one left with no partition.
pub fn gold_versions_table(
    replaced: &LakehouseTableContract,
) -> Result<LakehouseTableContract, String> {
    let name = replaced.table_name;
    let LakehouseRowVersions::Replaced {
        key: LakehouseRowKey::Columns(key),
    } = replaced.row_versions
    else {
        return Err(format!(
            "{name} is not a table a load replaces rows of by a column key: it has no versions table"
        ));
    };
    if replaced.layer != LakehouseLayer::Gold || key.is_empty() {
        return Err(format!("{name} is not a keyed Gold table"));
    }
    if let Some(taken) = replaced.columns.iter().find(|column| {
        [GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(&column.name)
    }) {
        return Err(format!(
            "{name} already has a {} column: its versions table cannot add one",
            taken.name
        ));
    }
    let mut columns = replaced.columns.to_vec();
    columns.extend([
        LakehouseColumn {
            name: GOLD_VERSION_SEQ_COLUMN,
            logical_type: "long",
            required: true,
        },
        LakehouseColumn {
            name: GOLD_VERSIONS_REMOVAL_COLUMN,
            logical_type: "string",
            required: false,
        },
    ]);
    let partition_spec = if key.first() == Some(&"pnu") {
        vec![GOLD_VERSIONS_PNU_PARTITION]
    } else {
        replaced
            .partition_spec
            .iter()
            .copied()
            .filter(|column| *column != PER_COMMIT_PARTITION)
            .collect()
    };
    if partition_spec.is_empty() {
        return Err(format!(
            "{name} is partitioned by commit only and its key does not start with pnu: its versions \
             table has no partition"
        ));
    }
    // The replaced table's gates hold of the current rows; of the rows as stored, two more do.
    let mut quality_gates = vec![
        "append_only",
        leak_str(format!(
            "({}, {GOLD_VERSION_SEQ_COLUMN}) unique",
            key.join(", ")
        )),
    ];
    quality_gates.extend(
        replaced
            .quality_gates
            .iter()
            .map(|gate| leak_str(format!("of the current rows: {gate}"))),
    );
    Ok(LakehouseTableContract {
        table_name: leak_str(format!("{name}{GOLD_VERSIONS_TABLE_SUFFIX}")),
        layer: LakehouseLayer::Gold,
        physical_format: replaced.physical_format,
        serving_role: replaced.serving_role,
        current_row_predicate: None,
        row_versions: LakehouseRowVersions::LatestPerKey(LakehouseCurrentRowLatest {
            key,
            newest_first: &[GOLD_VERSION_SEQ_COLUMN],
            removal_column: Some(GOLD_VERSIONS_REMOVAL_COLUMN),
            keep_ties: false,
        }),
        columns: leak_slice(columns),
        partition_spec: leak_slice(partition_spec),
        sort_order: replaced.sort_order,
        write_distribution: replaced.write_distribution,
        quality_gates: leak_slice(quality_gates),
        load: LakehouseLoadUnit::Derived,
    })
}

/// The Golds outside the by-PNU lanes that have a versions table. Root ADR-0564 §6-9: the 실거래
/// region statistics are read by their producer alone, so the versions table is born with the
/// producer that appends to it and nothing switches over.
const VERSIONED_OUTSIDE_THE_LANES: &[&LakehouseTableContract] =
    &[&crate::real_transaction_region_stats::GOLD_REAL_TRANSACTION_REGION_STATS];

/// The versions tables of the Golds outside the by-PNU lanes; empty when one cannot be derived
/// (the test below refuses that).
#[must_use]
pub fn gold_versions_tables_outside_the_lanes() -> &'static [LakehouseTableContract] {
    static TABLES: std::sync::OnceLock<Vec<LakehouseTableContract>> = std::sync::OnceLock::new();
    TABLES.get_or_init(|| {
        VERSIONED_OUTSIDE_THE_LANES
            .iter()
            .map(|replaced| gold_versions_table(replaced))
            .collect::<Result<_, _>>()
            .unwrap_or_default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lakehouse::{LakehousePhysicalFormat, LakehouseServingRole};
    use crate::LakehouseWriteDistribution;

    const fn column(
        name: &'static str,
        logical_type: &'static str,
        required: bool,
    ) -> LakehouseColumn {
        LakehouseColumn {
            name,
            logical_type,
            required,
        }
    }

    fn replaced(
        key: &'static [&'static str],
        columns: &'static [LakehouseColumn],
        partition_spec: &'static [&'static str],
    ) -> LakehouseTableContract {
        LakehouseTableContract {
            table_name: "gold.example",
            layer: LakehouseLayer::Gold,
            physical_format: LakehousePhysicalFormat::Parquet,
            serving_role: LakehouseServingRole::Projection,
            current_row_predicate: None,
            row_versions: LakehouseRowVersions::Replaced {
                key: LakehouseRowKey::Columns(key),
            },
            columns,
            partition_spec,
            sort_order: key,
            write_distribution: LakehouseWriteDistribution::Range,
            quality_gates: &["one row per key"],
            load: LakehouseLoadUnit::Derived,
        }
    }

    const BY_PNU: &[LakehouseColumn] = &[
        column("pnu", "string", true),
        column("things_json", "string", true),
        column("row_digest", "string", true),
        column("source_snapshot_id", "string", true),
    ];

    #[test]
    fn a_pnu_keyed_gold_is_versioned_by_commit_number_and_partitioned_by_sido() -> Result<(), String>
    {
        let table = gold_versions_table(&replaced(&["pnu"], BY_PNU, &["source_snapshot_id"]))?;
        assert_eq!(table.table_name, "gold.example_versions");
        assert_eq!(
            table.current_row_latest(),
            Some(LakehouseCurrentRowLatest {
                key: &["pnu"],
                newest_first: &["version_seq"],
                removal_column: Some("row_state"),
                keep_ties: false,
            })
        );
        // Every column of the replaced table as it is (name, type, required), then the two.
        assert_eq!(&table.columns[..BY_PNU.len()], BY_PNU);
        assert_eq!(
            &table.columns[BY_PNU.len()..],
            [
                column("version_seq", "long", true),
                column("row_state", "string", false)
            ]
        );
        assert_eq!(table.partition_spec, ["truncate(2, pnu)"]);
        assert_eq!(table.sort_order, ["pnu"]);
        assert_eq!(
            table.quality_gates,
            [
                "append_only",
                "(pnu, version_seq) unique",
                "of the current rows: one row per key"
            ]
        );
        Ok(())
    }

    #[test]
    fn a_gold_keyed_otherwise_keeps_its_partition_less_the_commit() -> Result<(), String> {
        const BY_REGION: &[LakehouseColumn] = &[
            column("dataset_slug", "string", true),
            column("region_code", "string", true),
        ];
        let table = gold_versions_table(&replaced(
            &["dataset_slug", "region_code"],
            BY_REGION,
            &["dataset_slug", "source_snapshot_id"],
        ))?;
        assert_eq!(table.partition_spec, ["dataset_slug"]);
        Ok(())
    }

    #[test]
    fn the_region_statistics_have_their_versions_table() {
        let tables = gold_versions_tables_outside_the_lanes();
        assert_eq!(tables.len(), VERSIONED_OUTSIDE_THE_LANES.len());
        let table = &tables[0];
        assert_eq!(
            table.table_name,
            "gold.real_transaction_region_stats_versions"
        );
        assert_eq!(table.partition_spec, ["dataset_slug"]);
        assert_eq!(
            table.current_row_latest().map(|latest| latest.key),
            Some(
                [
                    "dataset_slug",
                    "region_level",
                    "region_code",
                    "period_kind",
                    "period"
                ]
                .as_slice()
            )
        );
    }

    #[test]
    fn a_table_that_cannot_be_versioned_is_refused() {
        const TAKEN: &[LakehouseColumn] = &[
            column("pnu", "string", true),
            column("row_state", "string", false),
        ];
        const BY_REGION: &[LakehouseColumn] = &[column("region_code", "string", true)];
        let mut silver = replaced(&["pnu"], BY_PNU, &["source_snapshot_id"]);
        silver.layer = LakehouseLayer::Silver;
        let mut keyless = replaced(&["pnu"], BY_PNU, &["source_snapshot_id"]);
        keyless.row_versions = LakehouseRowVersions::Replaced {
            key: LakehouseRowKey::Keyless { reason: "fixture" },
        };
        for table in [
            replaced(&["pnu"], TAKEN, &["source_snapshot_id"]),
            // Only the commit partition, and no PNU to partition by.
            replaced(&["region_code"], BY_REGION, &["source_snapshot_id"]),
            silver,
            keyless,
        ] {
            assert!(gold_versions_table(&table).is_err(), "{table:?}");
        }
        // Its own versions table has none: the rule is newest-per-key already.
        let versions = gold_versions_table(&replaced(&["pnu"], BY_PNU, &["source_snapshot_id"]));
        assert!(versions.is_ok_and(|table| gold_versions_table(&table).is_err()));
    }
}
