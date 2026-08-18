//! Reads the rows of one Iceberg snapshot out of the lakehouse bucket.
//!
//! The Iceberg REST catalog answers which snapshot is current and where its manifest list lives;
//! everything below that is object reads. This module owns that walk — manifest list, manifests,
//! Parquet data files — for every command that needs canonical rows, so a second implementation of
//! Iceberg manifest decoding never appears (root ADR-0040 decision 8).

pub(crate) mod iceberg_scan;

use anyhow::{ensure, Context};
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

    /// Reads one object named by an Iceberg storage location.
    pub(crate) async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
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

/// Decodes every live row of one Iceberg snapshot into contract-shaped JSON objects.
pub(crate) async fn scan_snapshot_rows(
    contract: &LakehouseTableContract,
    lakehouse: &LakehouseObjectReader,
    snapshot: &IcebergSnapshotManifestList,
) -> anyhow::Result<ScannedRows> {
    let manifest_list = lakehouse
        .read(snapshot.manifest_list_location.as_str())
        .await?;
    let manifest_locations = iceberg_scan::manifest_locations(&manifest_list)?;

    let mut data_files = Vec::new();
    for location in manifest_locations {
        let manifest = lakehouse.read(location.as_str()).await?;
        data_files.extend(iceberg_scan::data_files(&manifest)?);
    }

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
    use super::lakehouse_object_key;

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
