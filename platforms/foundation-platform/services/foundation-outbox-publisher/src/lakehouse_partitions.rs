//! Rows and files per partition of each table's `main` snapshot, read from its manifests with the
//! lakehouse read-only key (root ADR-0565): what Trino's `$partitions` answered, with no data file
//! opened and no engine. The parcel edition readiness reads Silver's rows per edition from it.
//!
//! stdin: one table per line, `<namespace>.<name>`.
//! stdout: one JSON object per table, in the order asked:
//! `{"table", "snapshot_id", "partitions": [{"partition": {field: value or null}, "record_count", "files"}]}`,
//! partitions in the order of their values. A table with no snapshot has `snapshot_id` null and none.

use std::collections::BTreeMap;
use std::io::BufRead as _;

use anyhow::{bail, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use foundation_publisher_core::lakehouse_snapshot_scan::{
    iceberg_scan::ScannedDataFile, scan_snapshot_data_files, LakehouseObjectReader,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Serialize;

type Partition = BTreeMap<String, Option<String>>;

#[derive(Debug, Eq, PartialEq, Serialize)]
struct PartitionRows {
    partition: Partition,
    record_count: u64,
    files: usize,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct TablePartitions {
    table: String,
    snapshot_id: Option<i64>,
    partitions: Vec<PartitionRows>,
}

fn table(line: &str) -> anyhow::Result<Option<String>> {
    let table = line.trim();
    if table.is_empty() {
        return Ok(None);
    }
    let valid = table.matches('.').count() == 1
        && table
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.');
    if !valid {
        bail!("expected `<namespace>.<name>`, got {line:?}");
    }
    Ok(Some(table.to_owned()))
}

/// The live files of one snapshot summed by their partition values.
fn partitions(files: &[ScannedDataFile]) -> Vec<PartitionRows> {
    let mut sums: BTreeMap<&Partition, (u64, usize)> = BTreeMap::new();
    for file in files {
        let sum = sums.entry(&file.partition).or_default();
        sum.0 += file.record_count;
        sum.1 += 1;
    }
    sums.into_iter()
        .map(|(partition, (record_count, files))| PartitionRows {
            partition: partition.clone(),
            record_count,
            files,
        })
        .collect()
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
        tables.extend(table(&line)?);
    }
    for table in tables {
        let snapshot = catalog
            .load_current_snapshot_manifest_list(&table)
            .await
            .with_context(|| format!("failed to read the current snapshot of {table}"))?;
        let answer = match snapshot {
            None => TablePartitions {
                table,
                snapshot_id: None,
                partitions: Vec::new(),
            },
            Some(snapshot) => {
                let files = scan_snapshot_data_files(&reader, &snapshot)
                    .await
                    .with_context(|| format!("failed to read the manifests of {table}"))?;
                TablePartitions {
                    table,
                    snapshot_id: Some(snapshot.snapshot_id),
                    partitions: partitions(&files),
                }
            }
        };
        println!("{}", serde_json::to_string(&answer)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use foundation_publisher_core::lakehouse_snapshot_scan::iceberg_scan::ScannedDataFile;

    use super::{partitions, table, PartitionRows};

    fn file(edition: Option<&str>, records: u64) -> ScannedDataFile {
        ScannedDataFile {
            file_path: "s3://lakehouse/x.parquet".to_owned(),
            record_count: records,
            file_size_in_bytes: 1,
            column_statistics: BTreeMap::new(),
            column_size_field_ids: Vec::new(),
            partition: BTreeMap::from([(
                "source_snapshot_id".to_owned(),
                edition.map(str::to_owned),
            )]),
        }
    }

    #[test]
    fn a_line_names_one_table() -> anyhow::Result<()> {
        assert_eq!(
            table(" silver.parcel_boundaries ")?,
            Some("silver.parcel_boundaries".to_owned())
        );
        assert_eq!(table("")?, None);
        for bad in ["parcels", "a.b.c", "silver.P", "silver.p x"] {
            assert!(table(bad).is_err(), "{bad}");
        }
        Ok(())
    }

    #[test]
    fn files_are_summed_by_their_partition_values() {
        let summed = partitions(&[
            file(Some("202609"), 10),
            file(Some("202606"), 4),
            file(Some("202609"), 5),
            file(None, 1),
        ]);
        let edition = |value: Option<&str>| {
            BTreeMap::from([("source_snapshot_id".to_owned(), value.map(str::to_owned))])
        };
        assert_eq!(
            summed,
            vec![
                PartitionRows {
                    partition: edition(None),
                    record_count: 1,
                    files: 1
                },
                PartitionRows {
                    partition: edition(Some("202606")),
                    record_count: 4,
                    files: 1
                },
                PartitionRows {
                    partition: edition(Some("202609")),
                    record_count: 15,
                    files: 2
                },
            ]
        );
    }
}
