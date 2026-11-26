//! The `vended` catalog provider (root ADR-0616 A2): a run on AWS reads the current snapshots its
//! credential vendor resolved, holds no catalog token, and asks the catalog nothing.

use std::collections::BTreeMap;

use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogConfig, LakehouseCatalogConfigError,
    LakehouseCatalogProvider, VendedSnapshot,
};

const TABLES: &str = r#"{
    "silver.industrial_land_sale_notice_read": {
        "snapshot_id": 42,
        "snapshot_timestamp_ms": 1764115200000,
        "manifest_list_location": "s3://bucket/table/metadata/snap-42.avro",
        "metadata_location": "s3://bucket/table/metadata/00007.metadata.json"
    },
    "silver.never_written": null
}"#;

fn vars(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn vended(tables: Option<&str>) -> BTreeMap<String, String> {
    let mut entries = vec![
        ("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER", "vended"),
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI",
            "https://catalog.example/account/bucket",
        ),
        ("FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE", "account_bucket"),
        // A token the process happens to hold is never taken.
        (
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN",
            "synthetic-token",
        ),
    ];
    if let Some(tables) = tables {
        entries.push(("FOUNDATION_PLATFORM_LAKEHOUSE_VENDED_TABLES", tables));
    }
    vars(&entries)
}

#[test]
fn the_vended_provider_takes_the_tables_and_no_token() -> Result<(), LakehouseCatalogConfigError> {
    let config = LakehouseCatalogConfig::reader_from_vars(&vended(Some(TABLES)))?;
    assert_eq!(config.provider, LakehouseCatalogProvider::Vended);
    assert_eq!(config.catalog_token, None);
    let tables = config.vended_tables.as_ref().map(BTreeMap::len);
    assert_eq!(tables, Some(2));
    assert!(!format!("{config:?}").contains("synthetic-token"));
    Ok(())
}

#[test]
fn the_vended_provider_needs_its_tables_in_their_shape() {
    assert_eq!(
        LakehouseCatalogConfig::reader_from_vars(&vended(None)).err(),
        Some(LakehouseCatalogConfigError::MissingEnv(
            LakehouseCatalogConfig::VENDED_TABLES_ENV
        ))
    );
    assert!(matches!(
        LakehouseCatalogConfig::reader_from_vars(&vended(Some(r#"{"t": {"snapshot_id": "x"}}"#))),
        Err(LakehouseCatalogConfigError::InvalidVendedTables(_))
    ));
}

#[tokio::test]
async fn only_the_runs_tables_answer_and_nothing_asks_the_catalog(
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::reader_from_vars(&vended(
        Some(TABLES),
    ))?)?;
    let current = catalog
        .load_current_snapshot_manifest_list("silver.industrial_land_sale_notice_read")
        .await?
        .ok_or("the vended table has a current snapshot")?;
    let expected = VendedSnapshot {
        snapshot_id: 42,
        snapshot_timestamp_ms: 1_764_115_200_000,
        manifest_list_location: "s3://bucket/table/metadata/snap-42.avro".to_owned(),
        metadata_location: "s3://bucket/table/metadata/00007.metadata.json".to_owned(),
    };
    assert_eq!(
        (
            current.snapshot_id,
            current.snapshot_timestamp_ms,
            current.manifest_list_location,
            current.metadata_location
        ),
        (
            expected.snapshot_id,
            expected.snapshot_timestamp_ms,
            expected.manifest_list_location,
            expected.metadata_location
        )
    );
    assert_eq!(
        catalog
            .load_current_snapshot_manifest_list("silver.never_written")
            .await?,
        None
    );
    let refused = catalog
        .load_current_snapshot_manifest_list("bronze.court_auction")
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(refused.contains("not vended"), "{refused}");
    // Any call that would reach the catalog is refused before a request is built.
    let pinned = catalog
        .load_snapshot_manifest_list("silver.industrial_land_sale_notice_read", 42)
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(pinned.contains("needs the catalog"), "{pinned}");
    Ok(())
}
