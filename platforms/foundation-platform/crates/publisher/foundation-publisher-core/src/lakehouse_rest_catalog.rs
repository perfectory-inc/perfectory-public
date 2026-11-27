//! The lakehouse's Iceberg REST catalog as iceberg-rust opens it from its own properties, writing
//! through `OpenDAL` (root ADR-0624).

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

/// The REST catalog `properties` name, in iceberg-rust's own keys (`uri`, `warehouse`, `token`, or
/// `credential` with `oauth2-server-uri` and `scope`, and `s3.*` settings). With no `s3.*` key a
/// table's files are reached with the credential the catalog vends for that table on load; a key
/// given here wins over the vended one (iceberg-catalog-rest 0.11 `load_table` lays these
/// properties over the table's config), so a vending catalog is opened with none.
///
/// # Errors
/// Returns the catalog's refusal of the configuration.
pub async fn open(properties: HashMap<String, String>) -> anyhow::Result<Arc<dyn Catalog>> {
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("lakehouse", properties)
        .await
        .context("failed to open the lakehouse REST catalog")?;
    Ok(Arc::new(catalog))
}
