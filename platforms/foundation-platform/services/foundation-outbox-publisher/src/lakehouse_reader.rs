//! The publisher's one way to read lakehouse rows without Trino (root ADR-0559, ADR-0565): the
//! catalog's REST history names `main`'s head, and the publisher's own `DuckDB`, attached with the
//! read-only catalog token and the read-only key pair, reads the table at that snapshot. Nothing it
//! opens can write.

use std::path::Path;
use std::sync::Mutex;

use anyhow::Context;
use foundation_publisher_silver::duckdb_parquet_stage::{
    DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

/// The catalog and the `DuckDB` a command reads with. The connection is not shared between threads,
/// so it sits behind a lock taken for each query and never held across an await.
pub(crate) struct LakehouseReader {
    catalog: IcebergRestCatalog,
    stage: Mutex<DuckDbParquetStage>,
}

impl LakehouseReader {
    /// A reader whose `DuckDB` keeps its database and spill under `work`; its memory is the
    /// share every stage takes of what the process may still hold (`open_engine`).
    pub(crate) fn open(work: &Path) -> anyhow::Result<Self> {
        let catalog = LakehouseCatalogConfig::reader_from_env()
            .context("failed to configure the lakehouse catalog with its read-only token")?;
        let stage = DuckDbParquetStage::open_reading_lakehouse(
            work,
            &LakehouseRead::with_catalog(&catalog)?,
        )?;
        Ok(Self {
            catalog: IcebergRestCatalog::new(catalog)
                .context("failed to build the Iceberg catalog client")?,
            stage: Mutex::new(stage),
        })
    }

    /// `main`'s head of `table` (`<namespace>.<name>`), from the catalog's own history: `Ok(None)`
    /// when the catalog has no such table, `Ok(Some(None))` when it has no snapshot yet.
    pub(crate) async fn main_snapshot(&self, table: &str) -> anyhow::Result<Option<Option<i64>>> {
        let history = self
            .catalog
            .load_main_snapshot_history(table)
            .await
            .with_context(|| format!("failed to read the snapshot history of {table}"))?;
        Ok(history.map(|snapshots| {
            snapshots
                .first()
                .map(|snapshot| snapshot.snapshot_id)
                .filter(|snapshot| *snapshot > 0)
        }))
    }

    /// The rows `sql` answers, each one JSON object.
    pub(crate) fn json_rows(&self, sql: &str) -> anyhow::Result<Vec<serde_json::Value>> {
        let stage = self
            .stage
            .lock()
            .map_err(|_| anyhow::anyhow!("an earlier read panicked"))?;
        stage
            .json_rows(sql)?
            .iter()
            .map(|row| serde_json::from_str(row).context("DuckDB wrote a row that is not JSON"))
            .collect()
    }

    /// Each row `sql` answers, as one `T`, handed to `each` as it streams; the first error stops it.
    pub(crate) fn each_row<T: serde::de::DeserializeOwned>(
        &self,
        sql: &str,
        mut each: impl FnMut(T) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let stage = self
            .stage
            .lock()
            .map_err(|_| anyhow::anyhow!("an earlier read panicked"))?;
        stage.each_json_row(sql, |row| {
            each(serde_json::from_str(&row).context("a lakehouse row is not the expected shape")?)
        })
    }

    /// The rows `sql` answers, each one `T`.
    pub(crate) fn rows<T: serde::de::DeserializeOwned>(&self, sql: &str) -> anyhow::Result<Vec<T>> {
        self.json_rows(sql)?
            .into_iter()
            .map(|row| {
                serde_json::from_value(row).context("a lakehouse row is not the expected shape")
            })
            .collect()
    }
}

/// What a query names as `{source}`: `table` at `snapshot`, or as it is when it has none.
pub(crate) fn source(table: &str, snapshot: Option<i64>) -> String {
    match snapshot {
        Some(snapshot) => {
            foundation_publisher_silver::duckdb_parquet_stage::table_at(table, snapshot)
        }
        None => format!("{LAKEHOUSE_CATALOG}.{table}"),
    }
}
