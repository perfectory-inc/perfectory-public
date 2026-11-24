//! The 실거래 Silver load's first append into its empty tables (root ADR-0389).
//!
//! On real Iceberg tables: `empty_table` makes each one in a `MemoryCatalogBuilder` catalog.

use super::*;

/// The 실거래 Silver load's first export into its two empty tables (root ADR-0389), as
/// `load-rt-molit-real-transaction-silver` appends it: the rows, and the lines not loaded.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_transaction_export_is_the_first_append_into_each_empty_table() -> anyhow::Result<()>
{
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    for table in [
        contract("silver.real_transaction")?,
        contract("silver.real_transaction_rejected")?,
    ] {
        let (catalog, shape) = empty_table(root.path(), table).await?;
        let name = format!("{}-first", short_name(table)?);
        let file = stage.write(&shape, &synthetic_row(table)?, &name)?;
        let object = "bronze/source=synthetic/period=2099-04/export--sha256-synthetic.csv";
        let first = first_append(&catalog, table, file, object).await?;
        assert_eq!(first, (None, 1), "{}", table.table_name);
    }
    Ok(())
}
