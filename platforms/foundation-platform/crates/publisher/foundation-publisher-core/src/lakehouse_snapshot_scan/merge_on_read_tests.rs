//! A merge-on-read table reads, through this scan, exactly as Spark reads it (root ADR-0344).
//!
//! `tests/fixtures/merge_on_read` holds two Iceberg tables Spark and the pinned Iceberg runtime
//! wrote with the same appends and the same two incremental Gold `MERGE`s: one copy-on-write, one
//! merge-on-read (position delete files and delete manifests). `expected.json` is Spark's own read
//! of each. `generate.py` beside them writes all of it again. Synthetic PNUs only.

use std::{collections::BTreeSet, ops::Range, path::PathBuf};

use async_trait::async_trait;
use lakehouse_domain::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};
use lakehouse_infrastructure::IcebergSnapshotManifestList;
use serde_json::Value as JsonValue;

use super::{
    scan_snapshot_inventory, scan_snapshot_rows, scan_snapshot_rows_kept, KeptPrefix,
    LakehouseByteReader,
};

const FIXTURE: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.merge_on_read",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: &[
        LakehouseColumn {
            name: "pnu",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "row_digest",
            logical_type: "string",
            required: false,
        },
        LakehouseColumn {
            name: "value",
            logical_type: "long",
            required: false,
        },
    ],
    partition_spec: &[],
    sort_order: &["pnu"],
    write_distribution: LakehouseWriteDistribution::Range,
    quality_gates: &[],
    load: LakehouseLoadUnit::Derived,
};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/merge_on_read")
}

/// Serves the committed fixture: each location Spark wrote is the file `expected.json` names for
/// it under `files/`.
struct FixtureReader {
    files: serde_json::Map<String, JsonValue>,
}

impl FixtureReader {
    fn path(&self, location: &str) -> anyhow::Result<PathBuf> {
        let name = self
            .files
            .get(location)
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow::anyhow!("{location} is not a fixture file"))?;
        Ok(fixture_dir().join("files").join(name))
    }
}

#[async_trait]
impl LakehouseByteReader for FixtureReader {
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        Ok(std::fs::read(self.path(location)?)?)
    }

    async fn read_range(&self, location: &str, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        let bytes = std::fs::read(self.path(location)?)?;
        Ok(bytes
            .get(usize::try_from(range.start)?..usize::try_from(range.end)?)
            .ok_or_else(|| anyhow::anyhow!("range {range:?} is past the end of {location}"))?
            .to_vec())
    }
}

struct Expected {
    reader: FixtureReader,
    tables: serde_json::Map<String, JsonValue>,
}

fn expected() -> anyhow::Result<Expected> {
    let document: JsonValue =
        serde_json::from_slice(&std::fs::read(fixture_dir().join("expected.json"))?)?;
    let files = document["files"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("expected.json maps no files"))?
        .clone();
    let tables = document["tables"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("expected.json has no tables"))?
        .clone();
    Ok(Expected {
        reader: FixtureReader { files },
        tables,
    })
}

fn snapshot(table: &JsonValue) -> anyhow::Result<IcebergSnapshotManifestList> {
    Ok(IcebergSnapshotManifestList {
        table_name: FIXTURE.table_name.to_owned(),
        snapshot_id: table["snapshot_id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("no snapshot id"))?,
        snapshot_timestamp_ms: 0,
        manifest_list_location: table["manifest_list"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("no manifest list"))?
            .to_owned(),
        metadata_location: String::new(),
    })
}

/// Spark's read, as `[pnu, row_digest, value]` rows in PNU order.
fn spark_rows(table: &JsonValue) -> Vec<JsonValue> {
    table["rows"].as_array().cloned().unwrap_or_default()
}

fn scanned_rows(rows: &[serde_json::Map<String, JsonValue>]) -> Vec<JsonValue> {
    let mut rows = rows
        .iter()
        .map(|row| serde_json::json!([row["pnu"], row["row_digest"], row["value"]]))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| a[0].as_str().cmp(&b[0].as_str()));
    rows
}

#[tokio::test]
async fn a_merge_on_read_table_reads_as_spark_reads_it_and_as_its_copy_on_write_twin(
) -> anyhow::Result<()> {
    let expected = expected()?;
    let mut reads = Vec::new();
    for name in ["copy_on_write", "merge_on_read"] {
        let table = &expected.tables[name];
        let scanned = scan_snapshot_rows(&FIXTURE, &expected.reader, &snapshot(table)?).await?;
        scanned.ensure_complete()?;
        let rows = scanned_rows(&scanned.rows);
        assert_eq!(rows, spark_rows(table), "{name} reads otherwise than Spark");
        assert_eq!(
            scanned.live_record_count(),
            u64::try_from(rows.len())?,
            "{name}"
        );
        let inventory = scan_snapshot_inventory(&expected.reader, &snapshot(table)?).await?;
        assert_eq!(inventory.row_count, scanned.live_record_count(), "{name}");
        // Only the merge-on-read table carries deletes, and its data files still hold the rows.
        assert_eq!(scanned.deleted_record_count > 0, name == "merge_on_read");
        reads.push(rows);
    }
    assert_eq!(reads[0], reads[1]);
    Ok(())
}

#[tokio::test]
async fn a_shard_of_a_merge_on_read_table_keeps_its_prefix_less_the_deleted_rows(
) -> anyhow::Result<()> {
    let expected = expected()?;
    let table = &expected.tables["merge_on_read"];
    // Each ten-PNU prefix of the fixture: ranges the deletes and the merged files straddle.
    let prefixes = spark_rows(table)
        .iter()
        .filter_map(|row| row[0].as_str().map(|pnu| pnu[..14].to_owned()))
        .collect::<BTreeSet<_>>();
    for prefix in prefixes {
        let scanned = scan_snapshot_rows_kept(
            &FIXTURE,
            &expected.reader,
            &snapshot(table)?,
            |_| true,
            None,
            Some(KeptPrefix::pnu(prefix.as_str())),
        )
        .await?;
        scanned.ensure_complete()?;
        let want = spark_rows(table)
            .into_iter()
            .filter(|row| row[0].as_str().is_some_and(|pnu| pnu.starts_with(&prefix)))
            .collect::<Vec<_>>();
        assert_eq!(scanned_rows(&scanned.rows), want, "prefix {prefix}");
    }
    Ok(())
}
