//! Reads the rows of one Iceberg snapshot out of the lakehouse bucket.
//!
//! The Iceberg REST catalog answers which snapshot is current and where its manifest list lives;
//! everything below that is object reads. This module owns that walk — manifest list, manifests,
//! Parquet data files — for every command that needs canonical rows, so a second implementation of
//! Iceberg manifest decoding never appears (root ADR-0040 decision 8).

pub(crate) mod iceberg_scan;

use anyhow::{ensure, Context};
use async_trait::async_trait;
use foundation_outbox::{object_storage::R2ObjectStorageConfig, R2ObjectStorage};
use lakehouse_domain::LakehouseTableContract;
use lakehouse_infrastructure::IcebergSnapshotManifestList;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Reads the lakehouse objects an Iceberg snapshot points at.
#[derive(Clone)]
pub(crate) struct LakehouseObjectReader {
    storage: R2ObjectStorage,
    bucket_name: String,
}

impl LakehouseObjectReader {
    /// Builds a reader from the configured lakehouse R2 connection.
    pub(crate) fn from_env() -> anyhow::Result<Self> {
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
    pub(crate) fn from_config(config: R2ObjectStorageConfig) -> Self {
        let bucket_name = config.bucket_name.clone();
        Self {
            storage: R2ObjectStorage::from_config(config),
            bucket_name,
        }
    }
}

#[async_trait]
pub(crate) trait LakehouseByteReader: Send + Sync {
    /// Reads one object named by an Iceberg storage location.
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
impl LakehouseByteReader for LakehouseObjectReader {
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        let key = lakehouse_object_key(location, self.bucket_name.as_str())?;
        self.storage
            .get_object_bytes(key.as_str())
            .await
            .with_context(|| format!("failed to read lakehouse object {location}"))
    }
}

/// Rows of one snapshot, with the counts that prove the scan reached every data file.
pub(crate) struct ScannedRows {
    /// Contract-shaped rows decoded from every live data file in the snapshot.
    pub(crate) rows: Vec<JsonMap<String, JsonValue>>,
    /// Number of Parquet data files the manifests pointed at.
    pub(crate) data_file_count: u64,
    /// Row count the manifests declared, before decoding.
    pub(crate) manifest_record_count: u64,
}

/// Snapshot-level statistics read only from Iceberg manifests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScannedSnapshotInventory {
    /// Sum of every live data file's manifest `record_count`.
    pub(crate) row_count: u64,
    /// Number of live data files reachable from the current snapshot.
    pub(crate) data_file_count: u64,
    /// Sum of every live data file's manifest `file_size_in_bytes`.
    pub(crate) total_bytes: u64,
}

/// Reads snapshot statistics without opening a Parquet data file.
pub(crate) async fn scan_snapshot_inventory(
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
pub(crate) async fn scan_snapshot_rows(
    contract: &LakehouseTableContract,
    lakehouse: &impl LakehouseByteReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ScannedRows> {
    let data_files = snapshot_data_files(lakehouse, snapshot).await?;

    let mut rows = Vec::new();
    let mut manifest_record_count = 0_u64;
    for data_file in &data_files {
        manifest_record_count = manifest_record_count
            .checked_add(data_file.record_count)
            .context("manifest record count overflow")?;
        let bytes = lakehouse.read(data_file.file_path.as_str()).await?;
        rows.extend(iceberg_scan::decode_rows(contract, bytes)?);
    }

    Ok(ScannedRows {
        rows,
        data_file_count: u64::try_from(data_files.len()).context("data file count overflow")?,
        manifest_record_count,
    })
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
