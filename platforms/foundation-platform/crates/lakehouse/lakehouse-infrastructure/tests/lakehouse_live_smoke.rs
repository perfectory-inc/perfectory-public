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
    // Ignored by default: reaching this line means the lakehouse lane was asked
    // for, so a missing opt-in is a provisioning error rather than a reason to
    // report success for a smoke that never contacted the catalog.
    let opt_in = std::env::var(LIVE_LAKEHOUSE_SMOKE_ENV).unwrap_or_default();
    assert!(
        live_lakehouse_smoke_enabled(Some(opt_in.as_str())),
        "{LIVE_LAKEHOUSE_SMOKE_ENV}=1 is required to run the lakehouse live lane; \
         `cargo xtask integration foundation lakehouse` provisions it"
    );

    let table_name =
        std::env::var(SMOKE_TABLE_ENV).unwrap_or_else(|_| DEFAULT_LAKEHOUSE_SMOKE_TABLE.to_owned());
    validate_lakehouse_smoke_table_name(&table_name)?;

    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let snapshot = catalog.get_current_snapshot(&table_name).await?;

    // `load_table` maps a catalog 404 to `Ok(None)` and a table whose metadata carries no current
    // snapshot to `Ok(None)` as well, so this absence has two very different causes and the message
    // has to name both. It said only "expected current snapshot", and the cause turned out to be
    // the first one: the configured default table does not exist in the production warehouse at
    // all, whose `silver` namespace holds `building_register_units` and
    // `building_register_unit_areas`. Reading that from the message alone was impossible.
    assert!(
        snapshot.is_some(),
        "live lakehouse smoke found no current snapshot for {table_name}. The catalog answers this \
         the same way whether the table is absent or exists with no snapshot yet — list the \
         warehouse's namespaces to tell which, and check {SMOKE_TABLE_ENV} before assuming the \
         pipeline is at fault."
    );

    Ok(())
}
