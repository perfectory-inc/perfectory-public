//! Read-only inventory of every table declared by the lakehouse domain contract.

use std::path::PathBuf;

use anyhow::{bail, Context};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::IcebergRestCatalog;
use serde::Serialize;

use crate::{
    lakehouse_snapshot_scan::{
        scan_snapshot_inventory, LakehouseObjectReader, ScannedSnapshotInventory,
    },
    r2_command_support::{
        env_path, lakehouse_catalog_config_from_env_file, r2_reader_config_from_env_file,
    },
};

const SCHEMA_VERSION: &str = "foundation-platform.lakehouse_inventory.v1";
const ENV_FILE_ENV: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_INVENTORY_ENV_FILE";

#[derive(Clone, Debug, Eq, PartialEq)]
enum TableInspection {
    Present {
        current_snapshot_id: Option<i64>,
        row_count: u64,
        data_file_count: u64,
        bytes: u64,
        updated_at_utc: Option<String>,
    },
    Absent,
    ReadFailed {
        exists: Option<bool>,
        error_kind: &'static str,
    },
}

#[async_trait]
trait LakehouseTableInspector: Send + Sync {
    async fn inspect(&self, contract: &LakehouseTableContract) -> TableInspection;
}

struct LiveLakehouseTableInspector {
    catalog: IcebergRestCatalog,
    reader: LakehouseObjectReader,
}

#[async_trait]
impl LakehouseTableInspector for LiveLakehouseTableInspector {
    async fn inspect(&self, contract: &LakehouseTableContract) -> TableInspection {
        let metadata = match self.catalog.get_table_metadata(contract.table_name).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return TableInspection::Absent,
            Err(_) => {
                return TableInspection::ReadFailed {
                    exists: None,
                    error_kind: "catalog_metadata_read_failed",
                };
            }
        };
        let Some(expected_snapshot_id) = metadata.current_snapshot_id else {
            return TableInspection::Present {
                current_snapshot_id: None,
                row_count: 0,
                data_file_count: 0,
                bytes: 0,
                updated_at_utc: None,
            };
        };
        let snapshot = match self
            .catalog
            .load_current_snapshot_manifest_list(contract.table_name)
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return TableInspection::ReadFailed {
                    exists: Some(true),
                    error_kind: "catalog_snapshot_disappeared",
                };
            }
            Err(_) => {
                return TableInspection::ReadFailed {
                    exists: Some(true),
                    error_kind: "catalog_snapshot_read_failed",
                };
            }
        };
        if snapshot.snapshot_id != expected_snapshot_id {
            return TableInspection::ReadFailed {
                exists: Some(true),
                error_kind: "catalog_snapshot_changed",
            };
        }
        let updated_at_utc = match timestamp_utc(snapshot.snapshot_timestamp_ms) {
            Some(timestamp) => timestamp,
            None => {
                return TableInspection::ReadFailed {
                    exists: Some(true),
                    error_kind: "snapshot_timestamp_invalid",
                };
            }
        };
        let ScannedSnapshotInventory {
            row_count,
            data_file_count,
            total_bytes,
        } = match scan_snapshot_inventory(&self.reader, &snapshot).await {
            Ok(inventory) => inventory,
            Err(_) => {
                return TableInspection::ReadFailed {
                    exists: Some(true),
                    error_kind: "snapshot_manifest_read_failed",
                };
            }
        };
        TableInspection::Present {
            current_snapshot_id: Some(snapshot.snapshot_id),
            row_count,
            data_file_count,
            bytes: total_bytes,
            updated_at_utc: Some(updated_at_utc),
        }
    }
}

fn timestamp_utc(timestamp_ms: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[derive(Debug, Serialize)]
struct LakehouseInventoryReport {
    schema_version: &'static str,
    status: &'static str,
    declared_table_count: usize,
    present_table_count: usize,
    absent_table_count: usize,
    failed_table_count: usize,
    tables: Vec<LakehouseTableInventoryReport>,
}

#[derive(Debug, Serialize)]
struct LakehouseTableInventoryReport {
    table_name: &'static str,
    state: &'static str,
    exists: Option<bool>,
    current_snapshot_id: Option<i64>,
    row_count: Option<u64>,
    data_file_count: Option<u64>,
    bytes: Option<u64>,
    updated_at_utc: Option<String>,
    error_kind: Option<&'static str>,
}

async fn collect_inventory(
    contracts: &[LakehouseTableContract],
    inspector: &impl LakehouseTableInspector,
) -> LakehouseInventoryReport {
    let mut tables = Vec::with_capacity(contracts.len());
    let mut present_table_count = 0;
    let mut absent_table_count = 0;
    let mut failed_table_count = 0;
    for contract in contracts {
        let table = match inspector.inspect(contract).await {
            TableInspection::Present {
                current_snapshot_id,
                row_count,
                data_file_count,
                bytes,
                updated_at_utc,
            } => {
                present_table_count += 1;
                LakehouseTableInventoryReport {
                    table_name: contract.table_name,
                    state: "present",
                    exists: Some(true),
                    current_snapshot_id,
                    row_count: Some(row_count),
                    data_file_count: Some(data_file_count),
                    bytes: Some(bytes),
                    updated_at_utc,
                    error_kind: None,
                }
            }
            TableInspection::Absent => {
                absent_table_count += 1;
                LakehouseTableInventoryReport {
                    table_name: contract.table_name,
                    state: "absent",
                    exists: Some(false),
                    current_snapshot_id: None,
                    row_count: None,
                    data_file_count: None,
                    bytes: None,
                    updated_at_utc: None,
                    error_kind: None,
                }
            }
            TableInspection::ReadFailed { exists, error_kind } => {
                failed_table_count += 1;
                LakehouseTableInventoryReport {
                    table_name: contract.table_name,
                    state: "read_failed",
                    exists,
                    current_snapshot_id: None,
                    row_count: None,
                    data_file_count: None,
                    bytes: None,
                    updated_at_utc: None,
                    error_kind: Some(error_kind),
                }
            }
        };
        tables.push(table);
    }
    LakehouseInventoryReport {
        schema_version: SCHEMA_VERSION,
        status: if failed_table_count == 0 {
            "complete"
        } else {
            "degraded"
        },
        declared_table_count: contracts.len(),
        present_table_count,
        absent_table_count,
        failed_table_count,
        tables,
    }
}

/// Prints the current state of every contract-declared lakehouse table as JSON.
pub(crate) async fn run() -> anyhow::Result<()> {
    let env_file = env_path(ENV_FILE_ENV, ".env.local")?;
    run_with_env_file(env_file).await
}

async fn run_with_env_file(env_file: PathBuf) -> anyhow::Result<()> {
    let catalog = IcebergRestCatalog::new(lakehouse_catalog_config_from_env_file(&env_file)?)
        .context("failed to initialise Iceberg REST catalog for lakehouse inventory")?;
    let reader = LakehouseObjectReader::from_config(r2_reader_config_from_env_file(&env_file)?);
    let inspector = LiveLakehouseTableInspector { catalog, reader };
    let report = collect_inventory(industrial_complex_lakehouse_contracts(), &inspector).await;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.failed_table_count > 0 {
        bail!("lakehouse inventory observed one or more read failures");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use lakehouse_domain::{
        LakehouseTableContract, GOLD_COMPLEX_CATALOG, SILVER_INDUSTRIAL_COMPLEXES,
        SILVER_PARCEL_BOUNDARIES,
    };
    use serde_json::json;

    use super::{collect_inventory, LakehouseTableInspector, TableInspection};

    struct FixtureInspector {
        outcomes: BTreeMap<&'static str, TableInspection>,
    }

    #[async_trait]
    impl LakehouseTableInspector for FixtureInspector {
        async fn inspect(&self, contract: &LakehouseTableContract) -> TableInspection {
            self.outcomes
                .get(contract.table_name)
                .cloned()
                .unwrap_or(TableInspection::Absent)
        }
    }

    #[tokio::test]
    async fn missing_declared_table_is_a_successful_absent_observation() -> anyhow::Result<()> {
        let inspector = FixtureInspector {
            outcomes: BTreeMap::new(),
        };

        let report = collect_inventory(&[SILVER_PARCEL_BOUNDARIES], &inspector).await;

        assert_eq!(
            serde_json::to_value(report)?,
            json!({
                "schema_version": "foundation-platform.lakehouse_inventory.v1",
                "status": "complete",
                "declared_table_count": 1,
                "present_table_count": 0,
                "absent_table_count": 1,
                "failed_table_count": 0,
                "tables": [{
                    "table_name": "silver.parcel_boundaries",
                    "state": "absent",
                    "exists": false,
                    "current_snapshot_id": null,
                    "row_count": null,
                    "data_file_count": null,
                    "bytes": null,
                    "updated_at_utc": null,
                    "error_kind": null
                }]
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_failure_is_redacted_and_does_not_hide_other_table_results() -> anyhow::Result<()>
    {
        let inspector = FixtureInspector {
            outcomes: BTreeMap::from([
                (
                    SILVER_INDUSTRIAL_COMPLEXES.table_name,
                    TableInspection::Present {
                        current_snapshot_id: Some(42),
                        row_count: 1_442,
                        data_file_count: 3,
                        bytes: 65_536,
                        updated_at_utc: Some("2026-08-25T03:04:05.000Z".to_owned()),
                    },
                ),
                (
                    GOLD_COMPLEX_CATALOG.table_name,
                    TableInspection::ReadFailed {
                        exists: Some(true),
                        error_kind: "snapshot_manifest_read_failed",
                    },
                ),
            ]),
        };

        let report = collect_inventory(
            &[SILVER_INDUSTRIAL_COMPLEXES, GOLD_COMPLEX_CATALOG],
            &inspector,
        )
        .await;

        assert_eq!(report.status, "degraded");
        assert_eq!(report.declared_table_count, 2);
        assert_eq!(report.present_table_count, 1);
        assert_eq!(report.absent_table_count, 0);
        assert_eq!(report.failed_table_count, 1);
        assert_eq!(report.tables[0].row_count, Some(1_442));
        assert_eq!(report.tables[1].state, "read_failed");
        assert_eq!(
            report.tables[1].error_kind,
            Some("snapshot_manifest_read_failed")
        );
        Ok(())
    }
}
