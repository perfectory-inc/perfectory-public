// Fixture for tools/semgrep/rules/hand-written-iceberg-scan.yml (`opengrep scan --test`). Not compiled.

// ruleid: no-new-caller-of-the-hand-written-iceberg-scan
use crate::lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseObjectReader};

// ruleid: no-new-caller-of-the-hand-written-iceberg-scan
use foundation_publisher_core::lakehouse_snapshot_scan::iceberg_scan::ScannedDataFile;

async fn a_new_reader(contract: &LakehouseTableContract) -> anyhow::Result<Vec<Row>> {
    // ruleid: no-new-caller-of-the-hand-written-iceberg-scan
    let reader = foundation_publisher_core::lakehouse_snapshot_scan::LakehouseObjectReader::from_env()?;
    scan_rows(&reader, contract).await
}

fn through_duckdb(work: &Path, table: &str, snapshot: i64) -> anyhow::Result<Vec<String>> {
    // ok: no-new-caller-of-the-hand-written-iceberg-scan
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
    // ok: no-new-caller-of-the-hand-written-iceberg-scan
    stage.json_rows(&format!("SELECT * FROM {}", table_at(table, snapshot)))
}
