use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use iceberg::{
    spec::{
        NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Summary, TableMetadata, TableMetadataBuilder, Type,
    },
    TableCreation,
};
use lakehouse_domain::LakehouseError;
use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogClient, LakehouseCatalogConfig, LakehouseCatalogProvider,
    TableMetadataSource,
};

use super::{load_table_answer, table_ident, CatalogSettings, STATIC_KEYS};

fn lakekeeper() -> LakehouseCatalogConfig {
    LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::IcebergRest,
        catalog_uri: "http://catalog.invalid:8181/catalog".to_owned(),
        warehouse: "fixture-warehouse".to_owned(),
        catalog_token: None,
    }
}

fn client() -> LakehouseCatalogClient {
    LakehouseCatalogClient {
        client_id: "fixture-client".to_owned(),
        client_secret: "fixture-secret-value".to_owned(),
        token_uri: "https://issuer.invalid/oauth2/token".to_owned(),
        scope: "catalog-fixture/catalog".to_owned(),
    }
}

fn nothing_set(_: &str) -> bool {
    false
}

#[test]
fn lakekeeper_mode_opens_with_the_client_and_vended_credentials_only() -> anyhow::Result<()> {
    let settings = CatalogSettings::checked(lakekeeper(), Some(client()), nothing_set)?;
    let storage = [(
        "s3.access-key-id".to_owned(),
        "fixture-static-key".to_owned(),
    )];
    let properties = settings.properties(&storage);
    assert_eq!(
        properties.get("credential").map(String::as_str),
        Some("fixture-client:fixture-secret-value")
    );
    assert_eq!(
        properties.get("oauth2-server-uri").map(String::as_str),
        Some("https://issuer.invalid/oauth2/token")
    );
    assert_eq!(
        properties.get("scope").map(String::as_str),
        Some("catalog-fixture/catalog")
    );
    assert_eq!(
        properties
            .get("header.X-Iceberg-Access-Delegation")
            .map(String::as_str),
        Some("vended-credentials")
    );
    assert!(
        properties
            .keys()
            .all(|key| !key.starts_with("s3.") && key != "token"),
        "no storage key and no token beside the client: {:?}",
        properties.keys().collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn the_current_catalog_keeps_its_token_and_storage_settings() -> anyhow::Result<()> {
    let config = LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        catalog_token: Some("fixture-token".to_owned()),
        ..lakekeeper()
    };
    let settings = CatalogSettings::checked(config, None, nothing_set)?;
    let storage = [(
        "s3.access-key-id".to_owned(),
        "fixture-static-key".to_owned(),
    )];
    let properties = settings.properties(&storage);
    assert_eq!(
        properties.get("token").map(String::as_str),
        Some("fixture-token")
    );
    assert_eq!(
        properties.get("s3.access-key-id").map(String::as_str),
        Some("fixture-static-key")
    );
    assert!(!properties.contains_key("credential"));
    Ok(())
}

#[test]
fn lakekeeper_mode_refuses_a_static_key_a_token_or_another_provider() {
    for name in STATIC_KEYS {
        let refused = CatalogSettings::checked(lakekeeper(), Some(client()), |set| set == *name);
        assert!(
            refused.is_err(),
            "{name} set beside the client must be refused"
        );
    }
    let with_token = LakehouseCatalogConfig {
        catalog_token: Some("fixture-token".to_owned()),
        ..lakekeeper()
    };
    assert!(CatalogSettings::checked(with_token, Some(client()), nothing_set).is_err());
    let basin = LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        ..lakekeeper()
    };
    assert!(CatalogSettings::checked(basin, Some(client()), nothing_set).is_err());
}

#[test]
fn a_static_key_on_the_current_catalog_is_its_business() -> anyhow::Result<()> {
    let config = LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::R2DataCatalog,
        catalog_token: Some("fixture-token".to_owned()),
        ..lakekeeper()
    };
    CatalogSettings::checked(config, None, |_| true)?;
    Ok(())
}

#[test]
fn table_names_become_iceberg_identifiers() -> anyhow::Result<()> {
    let ident = table_ident("silver.fixture_reads")?;
    assert_eq!(ident.name(), "fixture_reads");
    assert_eq!(ident.namespace().to_url_string(), "silver");
    assert!(table_ident("fixture_reads").is_err());
    assert!(table_ident("silver.").is_err());
    Ok(())
}

/// iceberg-rust's metadata of a table whose one snapshot recorded one loaded object.
fn one_load() -> anyhow::Result<TableMetadata> {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()?;
    let creation = TableCreation::builder()
        .name("fixture_reads".to_owned())
        .location("s3://fixture-bucket/tables/silver/fixture_reads".to_owned())
        .schema(schema)
        .build();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(7)
        .with_sequence_number(1)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_manifest_list("s3://fixture-bucket/tables/silver/fixture_reads/metadata/snap-7.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([(
                "foundation.ingest-batch-objects".to_owned(),
                "fixture-object".to_owned(),
            )]),
        })
        .with_schema_id(0)
        .build();
    Ok(TableMetadataBuilder::from_table_creation(creation)?
        .add_snapshot(snapshot)?
        .set_ref(
            "main",
            SnapshotReference::new(7, SnapshotRetention::branch(None, None, None)),
        )?
        .build()?
        .metadata)
}

#[derive(Debug)]
struct Loaded(serde_json::Value);

#[async_trait]
impl TableMetadataSource for Loaded {
    async fn load_table(
        &self,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, LakehouseError> {
        Ok((table_name == "silver.fixture_reads").then(|| self.0.clone()))
    }
}

#[tokio::test]
async fn iceberg_rust_metadata_reads_as_the_rest_answer() -> anyhow::Result<()> {
    let answer = load_table_answer(
        Some("s3://fixture-bucket/tables/silver/fixture_reads/metadata/00001.json"),
        &one_load()?,
    )?;
    let catalog = IcebergRestCatalog::with_table_source(lakekeeper(), Arc::new(Loaded(answer)))?;
    let ingested = catalog
        .load_ingested_batch_objects("silver.fixture_reads")
        .await?
        .ok_or_else(|| anyhow::anyhow!("the table should exist"))?;
    assert_eq!(ingested.into_iter().collect::<Vec<_>>(), ["fixture-object"]);
    let current = catalog
        .load_current_snapshot_manifest_list("silver.fixture_reads")
        .await?
        .ok_or_else(|| anyhow::anyhow!("the snapshot should exist"))?;
    assert_eq!(current.snapshot_id, 7);
    assert!(current.manifest_list_location.ends_with("snap-7.avro"));
    Ok(())
}
