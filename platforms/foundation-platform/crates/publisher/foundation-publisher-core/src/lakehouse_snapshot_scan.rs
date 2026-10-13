//! Reads the rows of one Iceberg snapshot out of the lakehouse bucket.
//!
//! The Iceberg REST catalog answers which snapshot is current and where its manifest list lives;
//! everything below that is object reads. This module owns that walk — manifest list, manifests,
//! Parquet data files — for every command that needs canonical rows, so a second implementation of
//! Iceberg manifest decoding never appears (root ADR-0040 decision 8).

pub mod iceberg_scan;
#[cfg(test)]
mod pruning_tests;

use anyhow::{ensure, Context};
use async_trait::async_trait;
use foundation_outbox::{object_storage::R2ObjectStorageConfig, R2ObjectStorage};
use lakehouse_domain::LakehouseTableContract;
use lakehouse_infrastructure::IcebergSnapshotManifestList;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Reads the lakehouse objects an Iceberg snapshot points at.
#[derive(Clone)]
pub struct LakehouseObjectReader {
    storage: R2ObjectStorage,
    bucket_name: String,
}

impl LakehouseObjectReader {
    /// Builds a reader from the configured lakehouse R2 connection.
    pub fn from_env() -> anyhow::Result<Self> {
        let config =
            R2ObjectStorageConfig::from_env().context("failed to configure lakehouse R2 reads")?;
        let bucket_name = config.bucket_name.clone();
        Ok(Self {
            storage: R2ObjectStorage::from_config(config),
            bucket_name,
        })
    }

    /// Builds a reader from an explicitly selected read-only R2 credential set.
    #[must_use]
    pub fn from_config(config: R2ObjectStorageConfig) -> Self {
        let bucket_name = config.bucket_name.clone();
        Self {
            storage: R2ObjectStorage::from_config(config),
            bucket_name,
        }
    }
}

#[async_trait]
pub trait LakehouseByteReader: Send + Sync {
    /// Reads one object named by an Iceberg storage location.
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
impl LakehouseByteReader for LakehouseObjectReader {
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        let key = lakehouse_object_key(location, self.bucket_name.as_str())?;
        // Byte ranges, each retried: the SDK retries a request but not a body that breaks off
        // midway, and on 2026-10-08 such breaks ("streaming error", a different Gold file each
        // time) failed six of 126 parcel pack shards, each after reading for up to 20 minutes.
        self.storage
            .get_object_bytes_range_retried(key.as_str())
            .await
            .with_context(|| format!("failed to read lakehouse object {location}"))
    }
}

/// Rows of one snapshot, with the counts that prove the scan reached every data file.
pub struct ScannedRows {
    /// Contract-shaped rows the keep-filter retained, in decode order.
    pub rows: Vec<JsonMap<String, JsonValue>>,
    /// Number of Parquet data files the manifests pointed at, read or skipped.
    pub data_file_count: u64,
    /// Row count the manifests declared for the whole snapshot, read or skipped.
    pub manifest_record_count: u64,
    /// Data files the scan did not open because their manifest bounds prove they hold no row
    /// under the scan's prefix (`ScannedDataFile::excludes_prefix`).
    pub skipped_data_file_count: u64,
    /// Row count the manifests declared for the skipped files.
    pub skipped_record_count: u64,
    /// Row count the manifests declared for the files the scan read.
    pub read_record_count: u64,
    /// Rows decoded across every file read, counted before any keep-filter dropped them. This is
    /// what must equal `read_record_count` — the retained rows never can when a filter is in play.
    pub decoded_row_count: u64,
    /// The scan stopped because more rows passed the keep-filter than the caller's limit. The
    /// read and decoded counts then cover only the files read so far, and `rows` holds exactly
    /// the limit.
    pub keep_limit_exceeded: bool,
}

impl ScannedRows {
    /// Refuses a scan that did not account for every row of the snapshot: each file it read was
    /// decoded to the row count its manifest declared, and the files it read and the files it
    /// skipped on proof of their bounds together are the whole snapshot (root ADR-0164).
    pub fn ensure_complete(&self) -> anyhow::Result<()> {
        ensure!(
            !self.keep_limit_exceeded,
            "the scan stopped at its keep limit, so it did not reach every data file"
        );
        ensure!(
            self.decoded_row_count == self.read_record_count,
            "scanned {} rows but the manifests declared {} for the data files read",
            self.decoded_row_count,
            self.read_record_count
        );
        ensure!(
            self.read_record_count
                .checked_add(self.skipped_record_count)
                == Some(self.manifest_record_count),
            "read {} and skipped {} rows but the manifests declared {}",
            self.read_record_count,
            self.skipped_record_count,
            self.manifest_record_count
        );
        Ok(())
    }
}

/// The rows a scan keeps when only values of one column under one prefix matter.
///
/// The scan then drops every row whose `column` is a value outside the prefix — a row without a
/// value is left to the keep-filter — and skips each data file whose manifest proves it holds no
/// row the scan would keep, so the rows returned are the same whether a file was read or skipped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeptPrefix<'a> {
    /// Contract column whose value must start with `prefix`.
    pub column: &'a str,
    /// Leading text a kept value carries.
    pub prefix: &'a str,
}

impl<'a> KeptPrefix<'a> {
    /// A by-PNU shard: the rows whose `pnu` starts with `prefix`.
    #[must_use]
    pub const fn pnu(prefix: &'a str) -> Self {
        Self {
            column: "pnu",
            prefix,
        }
    }

    fn admits(&self, row: &JsonMap<String, JsonValue>) -> bool {
        match row.get(self.column) {
            Some(JsonValue::String(value)) => value.starts_with(self.prefix),
            _ => true,
        }
    }
}

/// Snapshot-level statistics read only from Iceberg manifests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScannedSnapshotInventory {
    /// Sum of every live data file's manifest `record_count`.
    pub row_count: u64,
    /// Number of live data files reachable from the current snapshot.
    pub data_file_count: u64,
    /// Sum of every live data file's manifest `file_size_in_bytes`.
    pub total_bytes: u64,
}

/// Reads snapshot statistics without opening a Parquet data file.
pub async fn scan_snapshot_inventory(
    lakehouse: &impl LakehouseByteReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ScannedSnapshotInventory> {
    let data_files = snapshot_data_files(lakehouse, snapshot).await?;
    let mut row_count = 0_u64;
    let mut total_bytes = 0_u64;
    for data_file in &data_files {
        row_count = row_count
            .checked_add(data_file.record_count)
            .context("manifest record count overflow")?;
        total_bytes = total_bytes
            .checked_add(data_file.file_size_in_bytes)
            .context("manifest data file byte count overflow")?;
    }
    Ok(ScannedSnapshotInventory {
        row_count,
        data_file_count: u64::try_from(data_files.len()).context("data file count overflow")?,
        total_bytes,
    })
}

/// Decodes every live row of one Iceberg snapshot into contract-shaped JSON objects.
pub async fn scan_snapshot_rows(
    contract: &LakehouseTableContract,
    lakehouse: &impl LakehouseByteReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ScannedRows> {
    scan_snapshot_rows_kept(contract, lakehouse, snapshot, |_| true, None, None).await
}

/// Decodes every live row of one Iceberg snapshot, retaining only rows `keep` accepts.
///
/// The filter runs per data file, as rows decode — what a shard run keeps is what it holds in
/// memory. Every row is still decoded and counted, so the scan keeps proving it reached every
/// data file even when it retains a fraction of them.
///
/// With `max_kept`, the scan stops at the first row past the limit instead of after the last
/// file: a by-PNU shard over its row cap is refused holding the cap, not the whole shard. The
/// bake's memory cap is sized to the row cap (root ADR-0138), so a first run's unsplit shard of
/// several million parcels would otherwise be killed by it before it could refuse and split.
///
/// With `prefix`, rows outside it are dropped before `keep` sees them, and a data file whose
/// manifest bounds prove it holds none of the prefix is not opened at all. The parcel bake's 126
/// shards each read all 33 files of `gold.parcel_panel` for one prefix on 2026-10-08; once the
/// table is written in PNU order (root ADR-0164), a shard reads the few files of its own range.
/// `ScannedRows::ensure_complete` still accounts for every row of the snapshot.
pub async fn scan_snapshot_rows_kept(
    contract: &LakehouseTableContract,
    lakehouse: &impl LakehouseByteReader,
    snapshot: &IcebergSnapshotManifestList,
    keep: impl Fn(&JsonMap<String, JsonValue>) -> bool,
    max_kept: Option<usize>,
    prefix: Option<KeptPrefix<'_>>,
) -> anyhow::Result<ScannedRows> {
    if let Some(prefix) = prefix {
        ensure!(
            contract
                .columns
                .iter()
                .any(|column| column.name == prefix.column),
            "{} has no column {} to keep a prefix of",
            contract.table_name,
            prefix.column
        );
    }
    let data_files = snapshot_data_files(lakehouse, snapshot).await?;
    let mut scanned = ScannedRows {
        rows: Vec::new(),
        data_file_count: u64::try_from(data_files.len()).context("data file count overflow")?,
        manifest_record_count: 0,
        skipped_data_file_count: 0,
        skipped_record_count: 0,
        read_record_count: 0,
        decoded_row_count: 0,
        keep_limit_exceeded: false,
    };
    let mut read = Vec::with_capacity(data_files.len());
    for data_file in &data_files {
        scanned.manifest_record_count = scanned
            .manifest_record_count
            .checked_add(data_file.record_count)
            .context("manifest record count overflow")?;
        if prefix.is_some_and(|prefix| data_file.excludes_prefix(prefix.column, prefix.prefix)) {
            scanned.skipped_data_file_count += 1;
            scanned.skipped_record_count = scanned
                .skipped_record_count
                .checked_add(data_file.record_count)
                .context("skipped record count overflow")?;
        } else {
            read.push(data_file);
        }
    }
    if let Some(prefix) = prefix {
        tracing::info!(
            table = snapshot.table_name.as_str(),
            column = prefix.column,
            prefix = prefix.prefix,
            data_files = scanned.data_file_count,
            skipped_data_files = scanned.skipped_data_file_count,
            skipped_records = scanned.skipped_record_count,
            "skipped the data files whose bounds exclude the prefix"
        );
    }

    let keep = |row: &JsonMap<String, JsonValue>| {
        prefix.is_none_or(|prefix| prefix.admits(row)) && keep(row)
    };
    for data_file in read {
        scanned.read_record_count = scanned
            .read_record_count
            .checked_add(data_file.record_count)
            .context("read record count overflow")?;
        let rows = &mut scanned.rows;
        let mut past_limit = false;
        let decoded =
            iceberg_scan::decode_rows(contract, bytes_of(lakehouse, data_file).await?, |row| {
                past_limit = keep_row(rows, row, keep, max_kept);
                !past_limit
            })?;
        scanned.decoded_row_count = scanned
            .decoded_row_count
            .checked_add(decoded)
            .context("decoded row count overflow")?;
        if past_limit {
            scanned.keep_limit_exceeded = true;
            return Ok(scanned);
        }
    }
    Ok(scanned)
}

/// Appends `row` when `keep` accepts it and `rows` holds fewer than `max_kept`; a row `keep`
/// refuses is dropped here, as it decodes. Returns whether `row` is a kept row past the limit:
/// that row is dropped, never held, and the scan stops.
fn keep_row(
    rows: &mut Vec<JsonMap<String, JsonValue>>,
    row: JsonMap<String, JsonValue>,
    keep: impl Fn(&JsonMap<String, JsonValue>) -> bool,
    max_kept: Option<usize>,
) -> bool {
    if !keep(&row) {
        return false;
    }
    if max_kept.is_some_and(|max| rows.len() >= max) {
        return true;
    }
    rows.push(row);
    false
}

async fn bytes_of(
    lakehouse: &impl LakehouseByteReader,
    data_file: &iceberg_scan::ScannedDataFile,
) -> anyhow::Result<Vec<u8>> {
    lakehouse.read(data_file.file_path.as_str()).await
}

async fn snapshot_data_files(
    lakehouse: &impl LakehouseByteReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<Vec<iceberg_scan::ScannedDataFile>> {
    let manifest_list = lakehouse
        .read(snapshot.manifest_list_location.as_str())
        .await?;
    let manifest_locations = iceberg_scan::manifest_locations(&manifest_list)?;

    let mut data_files = Vec::new();
    for location in manifest_locations {
        let manifest = lakehouse.read(location.as_str()).await?;
        data_files.extend(iceberg_scan::data_files(&manifest)?);
    }
    Ok(data_files)
}

/// Maps an Iceberg storage location onto a key in the configured lakehouse bucket.
///
/// A location in another bucket is a configuration error, not something to read past: the scan
/// would silently return the wrong table's rows.
fn lakehouse_object_key(location: &str, bucket_name: &str) -> anyhow::Result<String> {
    let rest = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))
        .with_context(|| format!("lakehouse location {location} is not an S3 URI"))?;
    let (bucket, key) = rest
        .split_once('/')
        .with_context(|| format!("lakehouse location {location} carries no object key"))?;
    ensure!(
        bucket == bucket_name,
        "lakehouse location {location} lives in bucket {bucket}, not the configured {bucket_name}"
    );
    ensure!(
        !key.is_empty(),
        "lakehouse location {location} carries no object key"
    );
    Ok(key.to_owned())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_scan_over_its_keep_limit_stops_holding_the_limit() {
        let row =
            |pnu: &str| serde_json::Map::from_iter([("pnu".to_owned(), serde_json::json!(pnu))]);
        let mut rows = Vec::new();
        for pnu in ["1", "2", "3"] {
            assert!(!super::keep_row(&mut rows, row(pnu), |_| true, Some(3)));
        }
        // The fourth kept row passes the cap: it is never held.
        assert!(super::keep_row(&mut rows, row("4"), |_| true, Some(3)));
        assert_eq!(rows.len(), 3);
        // Rows the filter drops do not count toward the limit; no limit keeps everything.
        let mut kept = Vec::new();
        for pnu in ["1", "2", "3"] {
            assert!(!super::keep_row(
                &mut kept,
                row(pnu),
                |r| r["pnu"] != "2",
                Some(2)
            ));
        }
        assert_eq!(kept.len(), 2);
        for pnu in ["1", "2", "3"] {
            assert!(!super::keep_row(&mut kept, row(pnu), |_| true, None));
        }
        assert_eq!(kept.len(), 5);
    }

    use std::collections::BTreeMap;

    use apache_avro::{types::Value as AvroValue, Schema, Writer};
    use async_trait::async_trait;
    use lakehouse_infrastructure::IcebergSnapshotManifestList;

    use super::{lakehouse_object_key, scan_snapshot_inventory, LakehouseByteReader};

    struct FixtureReader {
        objects: BTreeMap<String, Vec<u8>>,
    }

    #[async_trait]
    impl LakehouseByteReader for FixtureReader {
        async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
            self.objects
                .get(location)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unexpected object read: {location}"))
        }
    }

    fn avro_container(schema: &str, record: AvroValue) -> anyhow::Result<Vec<u8>> {
        let schema = Schema::parse_str(schema)?;
        let mut writer = Writer::new(&schema, Vec::new());
        writer.append(record)?;
        Ok(writer.into_inner()?)
    }

    #[tokio::test]
    async fn snapshot_inventory_sums_manifest_metrics_without_reading_parquet() -> anyhow::Result<()>
    {
        let manifest_list_location = "s3://lakehouse/metadata/snap-1.avro";
        let manifest_location = "s3://lakehouse/metadata/manifest-1.avro";
        let parquet_location = "s3://lakehouse/silver/table/part-0.parquet";
        let manifest_list = avro_container(
            r#"{
              "type": "record",
              "name": "manifest_file",
              "fields": [
                {"name": "content", "type": "long"},
                {"name": "manifest_path", "type": "string"}
              ]
            }"#,
            AvroValue::Record(vec![
                ("content".to_owned(), AvroValue::Long(0)),
                (
                    "manifest_path".to_owned(),
                    AvroValue::String(manifest_location.to_owned()),
                ),
            ]),
        )?;
        let manifest = avro_container(
            r#"{
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "long"},
                {"name": "data_file", "type": {
                  "type": "record",
                  "name": "data_file",
                  "fields": [
                    {"name": "content", "type": "long"},
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {"name": "record_count", "type": "long"},
                    {"name": "file_size_in_bytes", "type": "long"}
                  ]
                }}
              ]
            }"#,
            AvroValue::Record(vec![
                ("status".to_owned(), AvroValue::Long(1)),
                (
                    "data_file".to_owned(),
                    AvroValue::Record(vec![
                        ("content".to_owned(), AvroValue::Long(0)),
                        (
                            "file_path".to_owned(),
                            AvroValue::String(parquet_location.to_owned()),
                        ),
                        (
                            "file_format".to_owned(),
                            AvroValue::String("PARQUET".to_owned()),
                        ),
                        ("record_count".to_owned(), AvroValue::Long(37)),
                        ("file_size_in_bytes".to_owned(), AvroValue::Long(4_096)),
                    ]),
                ),
            ]),
        )?;
        let reader = FixtureReader {
            objects: BTreeMap::from([
                (manifest_list_location.to_owned(), manifest_list),
                (manifest_location.to_owned(), manifest),
            ]),
        };
        let snapshot = IcebergSnapshotManifestList {
            table_name: "silver.fixture".to_owned(),
            snapshot_id: 42,
            snapshot_timestamp_ms: 1_777_777_777_000,
            manifest_list_location: manifest_list_location.to_owned(),
            metadata_location: "s3://lakehouse/metadata/00001.json".to_owned(),
        };

        let inventory = scan_snapshot_inventory(&reader, &snapshot).await?;

        assert_eq!(inventory.row_count, 37);
        assert_eq!(inventory.data_file_count, 1);
        assert_eq!(inventory.total_bytes, 4_096);
        Ok(())
    }

    #[test]
    fn lakehouse_locations_outside_the_configured_bucket_are_refused() -> anyhow::Result<()> {
        assert_eq!(
            lakehouse_object_key(
                "s3://foundation-platform-lakehouse-prod/metadata/snap-1.avro",
                "foundation-platform-lakehouse-prod"
            )?,
            "metadata/snap-1.avro"
        );
        assert_eq!(
            lakehouse_object_key(
                "s3a://foundation-platform-lakehouse-prod/gold/complex_catalog/part-0.parquet",
                "foundation-platform-lakehouse-prod"
            )?,
            "gold/complex_catalog/part-0.parquet"
        );
        for location in [
            "s3://another-bucket/metadata/snap-1.avro",
            "https://foundation-platform-lakehouse-prod/metadata/snap-1.avro",
            "s3://foundation-platform-lakehouse-prod",
            "s3://foundation-platform-lakehouse-prod/",
        ] {
            assert!(
                lakehouse_object_key(location, "foundation-platform-lakehouse-prod").is_err(),
                "unsafe lakehouse location was accepted: {location}"
            );
        }
        Ok(())
    }
}
