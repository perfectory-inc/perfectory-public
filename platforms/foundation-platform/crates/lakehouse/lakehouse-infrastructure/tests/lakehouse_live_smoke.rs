//! Optional live smoke test for the configured Iceberg REST/R2 Data Catalog.

use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_infrastructure::{
    live_lakehouse_smoke_enabled, validate_lakehouse_smoke_table_name, IcebergRestCatalog,
    LakehouseCatalogConfig, DEFAULT_LAKEHOUSE_SMOKE_TABLE,
};

const LIVE_LAKEHOUSE_SMOKE_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE";
const SMOKE_TABLE_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_SMOKE_TABLE";

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires live Iceberg REST/R2 Data Catalog credentials; read-only snapshot lookup"]
async fn lakehouse_live_smoke_reads_current_snapshot_for_configured_table() -> TestResult {
    if !live_lakehouse_smoke_enabled(std::env::var(LIVE_LAKEHOUSE_SMOKE_ENV).ok().as_deref()) {
        return Ok(());
    }

    let table_name =
        std::env::var(SMOKE_TABLE_ENV).unwrap_or_else(|_| DEFAULT_LAKEHOUSE_SMOKE_TABLE.to_owned());
    validate_lakehouse_smoke_table_name(&table_name)?;

    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let snapshot = catalog.get_current_snapshot(&table_name).await?;

    assert!(
        snapshot.is_some(),
        "live lakehouse smoke expected current snapshot for {table_name}"
    );

    Ok(())
}
