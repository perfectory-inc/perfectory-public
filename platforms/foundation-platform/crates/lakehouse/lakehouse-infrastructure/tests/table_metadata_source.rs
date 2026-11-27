//! A client whose table loads come from a `TableMetadataSource` (the Lakekeeper catalog through
//! iceberg-catalog-rest, root ADR-0624 P6) reads them as it reads its own HTTP answers, and makes no
//! HTTP call of its own.

use std::{error::Error, sync::Arc};

use async_trait::async_trait;
use lakehouse_domain::LakehouseError;
use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogConfig, LakehouseCatalogProvider, TableMetadataSource,
};

const TAGGED: i64 = 101;
const OTHER: i64 = 202;

#[derive(Debug)]
struct Answers;

#[async_trait]
impl TableMetadataSource for Answers {
    async fn load_table(
        &self,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, LakehouseError> {
        Ok((table_name == "silver.fixture_reads").then(|| {
            serde_json::json!({
                "metadata-location": "s3://bucket/tables/silver/fixture_reads/metadata/00002.json",
                "metadata": {
                    "table-uuid": "0190f0a4-7a54-7c3e-9d5e-1f5b9e7f0a11",
                    "current-snapshot-id": OTHER,
                    "refs": {"main": {"snapshot-id": OTHER, "type": "branch"}},
                    "snapshots": [
                        {"snapshot-id": TAGGED, "timestamp-ms": 1, "manifest-list": "s3://bucket/a.avro",
                         "summary": {"operation": "append",
                                     "foundation.ingest-batch-objects": "fixture-object-a"}},
                        {"snapshot-id": OTHER, "parent-snapshot-id": TAGGED, "timestamp-ms": 2,
                         "manifest-list": "s3://bucket/b.avro",
                         "summary": {"operation": "append",
                                     "foundation.ingest-batch-objects": "fixture-object-b"}}
                    ]
                }
            })
        }))
    }
}

fn unreachable_catalog() -> LakehouseCatalogConfig {
    // Port 9 (discard) on the loopback: any HTTP call of the client's own would fail the test.
    LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::IcebergRest,
        catalog_uri: "http://127.0.0.1:9/catalog".to_owned(),
        warehouse: "fixture-warehouse".to_owned(),
        catalog_token: None,
    }
}

#[tokio::test]
async fn the_source_answers_the_reads_and_no_http_call_is_made() -> Result<(), Box<dyn Error>> {
    let catalog = IcebergRestCatalog::with_table_source(unreachable_catalog(), Arc::new(Answers))?;
    let ingested = catalog
        .load_ingested_batch_objects("silver.fixture_reads")
        .await?
        .ok_or_else(|| std::io::Error::other("table should exist"))?;
    assert_eq!(
        ingested.into_iter().collect::<Vec<_>>(),
        ["fixture-object-a", "fixture-object-b"]
    );
    let current = catalog
        .load_current_snapshot_manifest_list("silver.fixture_reads")
        .await?
        .ok_or_else(|| std::io::Error::other("snapshot should exist"))?;
    assert_eq!(current.snapshot_id, OTHER);
    Ok(())
}

#[tokio::test]
async fn a_table_the_source_does_not_hold_is_none() -> Result<(), Box<dyn Error>> {
    let catalog = IcebergRestCatalog::with_table_source(unreachable_catalog(), Arc::new(Answers))?;
    assert!(catalog
        .load_ingested_batch_objects("silver.absent")
        .await?
        .is_none());
    Ok(())
}
