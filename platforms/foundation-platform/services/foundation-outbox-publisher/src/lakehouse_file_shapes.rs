//! What the production shape ledger asks of each table's live data files, read from the table's
//! manifests with the lakehouse read-only key (root ADR-0259, ADR-0314, ADR-0565): the sets of field
//! ids the files were written with, and, for a table read by a cluster key, each file's bounds of it.
//! No data file is opened and no engine runs; it answers what `$files` answered through Trino.
//!
//! stdin: one table per line, `<namespace>.<name>` and optionally its cluster key column.
//! stdout: one JSON object per table, in the order asked:
//! `{"table", "snapshot_id", "files", "shapes": [[field id, ...], ...], "bounds": [[lower, upper], ...]}`.
//! A table with no snapshot has `snapshot_id` null and no file; `bounds` is present only when a key
//! was asked, and leaves out a file the manifest recorded no UTF-8 bound of the key for.

use std::{collections::BTreeSet, io::BufRead as _};

use anyhow::{bail, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use foundation_publisher_core::lakehouse_snapshot_scan::{
    iceberg_scan::ScannedDataFile, scan_snapshot_data_files, LakehouseObjectReader,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Serialize;

/// One line of stdin: the table and the cluster key whose bounds are wanted.
#[derive(Debug, Eq, PartialEq)]
struct Asked {
    table: String,
    key: Option<String>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct TableFiles {
    table: String,
    snapshot_id: Option<i64>,
    files: usize,
    shapes: Vec<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bounds: Option<Vec<[String; 2]>>,
}

fn asked(line: &str) -> anyhow::Result<Option<Asked>> {
    let mut words = line.split_whitespace();
    let Some(table) = words.next() else {
        return Ok(None);
    };
    let valid = |word: &str| {
        !word.is_empty()
            && word
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.')
    };
    let key = words.next().map(str::to_owned);
    if words.next().is_some()
        || table.matches('.').count() != 1
        || !valid(table)
        || key
            .as_deref()
            .is_some_and(|key| !valid(key) || key.contains('.'))
    {
        bail!("expected `<namespace>.<name> [<key column>]`, got {line:?}");
    }
    Ok(Some(Asked {
        table: table.to_owned(),
        key,
    }))
}

/// The answer for one table from its live data files.
fn table_files(asked: &Asked, snapshot_id: Option<i64>, files: &[ScannedDataFile]) -> TableFiles {
    let shapes: BTreeSet<Vec<i64>> = files
        .iter()
        .map(|file| file.column_size_field_ids.clone())
        .collect();
    let bounds = asked.key.as_ref().map(|key| {
        files
            .iter()
            .filter_map(|file| {
                let statistics = file.column_statistics.get(key)?;
                let lower = String::from_utf8(statistics.lower_bound.clone()?).ok()?;
                let upper = String::from_utf8(statistics.upper_bound.clone()?).ok()?;
                Some([lower, upper])
            })
            .collect()
    });
    TableFiles {
        table: asked.table.clone(),
        snapshot_id,
        files: files.len(),
        shapes: shapes.into_iter().collect(),
        bounds,
    }
}

pub(crate) async fn run() -> anyhow::Result<()> {
    // The read-only catalog token and the read-only key pair: this command can change nothing.
    let catalog = IcebergRestCatalog::new(
        LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the Iceberg catalog with its read-only token")?,
    )
    .context("failed to build the Iceberg catalog client")?;
    let reader = LakehouseObjectReader::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("failed to configure lakehouse reads with the read-only key pair")?,
    );
    // Every line first: the lock on stdin is not held across a catalog call.
    let mut tables = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let line = line.context("failed to read the asked tables from stdin")?;
        tables.extend(asked(&line)?);
    }
    for asked in tables {
        let snapshot = catalog
            .load_current_snapshot_manifest_list(&asked.table)
            .await
            .with_context(|| format!("failed to read the current snapshot of {}", asked.table))?;
        let answer = match snapshot {
            None => table_files(&asked, None, &[]),
            Some(snapshot) => {
                let files = scan_snapshot_data_files(&reader, &snapshot)
                    .await
                    .with_context(|| format!("failed to read the manifests of {}", asked.table))?;
                table_files(&asked, Some(snapshot.snapshot_id), &files)
            }
        };
        println!("{}", serde_json::to_string(&answer)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use foundation_publisher_core::lakehouse_snapshot_scan::iceberg_scan::{
        ColumnStatistics, ScannedDataFile,
    };

    use super::{asked, table_files, Asked};

    fn file(ids: &[i64], key: Option<(&str, &str)>) -> ScannedDataFile {
        ScannedDataFile {
            file_path: "s3://lakehouse/x.parquet".to_owned(),
            record_count: 1,
            file_size_in_bytes: 1,
            column_statistics: key
                .map(|(lower, upper)| {
                    BTreeMap::from([(
                        "pnu".to_owned(),
                        ColumnStatistics {
                            lower_bound: Some(lower.as_bytes().to_vec()),
                            upper_bound: Some(upper.as_bytes().to_vec()),
                            null_value_count: Some(0),
                        },
                    )])
                })
                .unwrap_or_default(),
            column_size_field_ids: ids.to_vec(),
        }
    }

    #[test]
    fn a_line_names_one_table_and_at_most_its_key() -> anyhow::Result<()> {
        assert_eq!(
            asked("silver.parcels pnu")?,
            Some(Asked {
                table: "silver.parcels".to_owned(),
                key: Some("pnu".to_owned())
            })
        );
        assert_eq!(asked("   ")?, None);
        for bad in [
            "parcels",
            "a.b.c",
            "silver.parcels pnu extra",
            "silver.Parcels",
            "silver.p;x",
            "silver.p a.b",
        ] {
            assert!(asked(bad).is_err(), "{bad}");
        }
        Ok(())
    }

    #[test]
    fn shapes_are_the_distinct_column_sets_and_bounds_are_per_file_with_one() {
        let files = [
            file(&[1, 2, 3], Some(("99999100", "99999200"))),
            file(&[1, 2, 3], Some(("99999300", "99999400"))),
            file(&[1, 2], None),
        ];
        let with_key = Asked {
            table: "silver.parcels".to_owned(),
            key: Some("pnu".to_owned()),
        };
        let answer = table_files(&with_key, Some(7), &files);
        assert_eq!(answer.files, 3);
        assert_eq!(answer.shapes, vec![vec![1, 2], vec![1, 2, 3]]);
        assert_eq!(
            answer.bounds,
            Some(vec![
                ["99999100".to_owned(), "99999200".to_owned()],
                ["99999300".to_owned(), "99999400".to_owned()],
            ])
        );
        let without = Asked {
            key: None,
            ..with_key
        };
        assert_eq!(table_files(&without, Some(7), &files).bounds, None);
        let empty = table_files(&without, None, &[]);
        assert_eq!(
            (empty.snapshot_id, empty.files, empty.shapes.len()),
            (None, 0, 0)
        );
    }
}
