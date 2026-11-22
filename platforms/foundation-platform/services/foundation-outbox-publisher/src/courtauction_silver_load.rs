//! `load-courtauction-go-kr-silver`: 법원경매정보 Silver documents that left Spark (root ADR-0559).
//!
//! The documents are root ADR-0280, ADR-0540 and ADR-0575's.
//!
//! `court-auction-silver-load.sh` runs it over the plan directories of its tables, before the Spark
//! job loads the rest. For each object, oldest first, `DuckDB` inside the publisher builds the rows
//! of every table its document writes (`foundation_publisher_silver::courtauction_silver_stage`) and
//! the publisher appends them, table by table in the document's order, each in one Iceberg commit
//! that records the object even when it holds no row (root ADR-0560): a newest-per-key table's changed
//! keys, a read record's every row. These were `courtauction_go_kr.py`'s 공통코드, 감정평가서,
//! 매각물건명세서 and 배당요구종기공고. The last stdout line is
//! `courtauction-go-kr-duckdb-summary-json {json}`.
//!
//! Before it, one Dagster Pipes `report_asset_check` line per table it appended to and result check
//! (orchestration/dagster/load_checks.py; the op that ran the unit logs them): every object's rows
//! were committed whole (`silver_load_rows`), repeat no key of the table's unique gate
//! (`silver_load_key_unique`), and miss no required column (`silver_load_required_present`).

use std::{collections::BTreeMap, path::PathBuf, sync::Mutex, time::Instant};

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter,
};
use foundation_publisher_silver::{
    courtauction_silver_stage::{
        appended, planned_objects, read_object, CourtAuctionSource, PlannedObject,
    },
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    onbid_silver_stage::{plan_dirs, write},
    process_memory,
    silver_changes::{result_checks, ResultChecks},
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::onbid_silver_load::{contract, head, locked};
use crate::public_data_control_support::required_env_value;

const PLAN_ROOT_ENV: &str = "FOUNDATION_PLATFORM_COURT_AUCTION_PLAN_ROOT";
const PIPES_PROTOCOL_VERSION: &str = "0.1";

/// The result checks of one table over the run's objects.
#[derive(Default)]
struct TableChecks {
    objects: u64,
    rows: u64,
    /// Objects whose commit added another count of records than the rows staged for it.
    uncommitted: Vec<String>,
    duplicate_keys: u64,
    missing_required: u64,
}

impl TableChecks {
    fn add(&mut self, object: &str, checks: ResultChecks, added: u64) {
        self.objects += 1;
        self.rows += checks.rows;
        if added != checks.rows {
            self.uncommitted.push(object.to_owned());
        }
        self.duplicate_keys += checks.duplicate_keys;
        self.missing_required += checks.missing_required;
    }

    /// The Pipes `report_asset_check` lines of the table's three checks.
    fn pipes_lines(&self, table: &str) -> Vec<Value> {
        let line = |check: &str, passed: bool, detail: Value| {
            let mut metadata = serde_json::Map::new();
            for (name, value) in [
                ("objects", json!(self.objects)),
                ("rows", json!(self.rows)),
                ("detail", detail),
            ] {
                metadata.insert(
                    name.to_owned(),
                    json!({"raw_value": value, "type": "__infer__"}),
                );
            }
            json!({
                "__dagster_pipes_version": PIPES_PROTOCOL_VERSION,
                "method": "report_asset_check",
                "params": {
                    "asset_key": format!("iceberg/{}", table.replace('.', "/")),
                    "check_name": check,
                    "passed": passed,
                    "metadata": metadata,
                },
            })
        };
        vec![
            line(
                "silver_load_rows",
                self.uncommitted.is_empty(),
                json!(self.uncommitted.join(" ")),
            ),
            line(
                "silver_load_key_unique",
                self.duplicate_keys == 0,
                json!(self.duplicate_keys),
            ),
            line(
                "silver_load_required_present",
                self.missing_required == 0,
                json!(self.missing_required),
            ),
        ]
    }
}

/// Seconds since `started`, to a tenth.
fn seconds(started: Instant) -> f64 {
    (started.elapsed().as_secs_f64() * 10.0).round() / 10.0
}

struct Run {
    source: CourtAuctionSource,
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    ingested_at: chrono::DateTime<chrono::Utc>,
}

impl Run {
    /// Loads one object into each table its document writes; the summary line of it.
    async fn load(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        object: &PlannedObject,
        checks: &mut BTreeMap<&'static str, TableChecks>,
    ) -> anyhow::Result<Value> {
        let started = Instant::now();
        let staged = {
            let stage = &*locked(stage)?;
            read_object(stage, &self.source, object, self.ingested_at)?
        };
        let mut tables = serde_json::Map::new();
        for table in &staged {
            let table_contract = contract(table.table)?;
            // A table is born on its first commit (root ADR-0564 §1): the deploy's lakehouse
            // migration leaves an absent table to the load that makes it, as the Spark job made it.
            self.committer
                .create_table_if_absent(table_contract)
                .await?;
            let shape = self.committer.table_shape(table.table).await?;
            // Pinned before the compare, so the commit refuses a head that moved since.
            let expected_head = head(&self.catalog, table.table).await?;
            let (file, rows, result) = {
                let stage = &*locked(stage)?;
                let (rows_table, rows) = appended(stage, table_contract, expected_head, table)?;
                let result = result_checks(stage, table_contract, &rows_table)?;
                (
                    write(stage, &shape, table.table, &rows_table, &object.object_key)?,
                    rows,
                    result,
                )
            };
            let already_ingested = self
                .catalog
                .load_ingested_batch_objects(table.table)
                .await?
                .with_context(|| format!("{} does not exist", table.table))?;
            let outcome = self
                .committer
                .append_at_head(
                    DataAppend {
                        table_name: table.table,
                        contract: table_contract,
                        files: file.as_slice(),
                        batch: std::slice::from_ref(&object.object_key),
                        already_ingested: &already_ingested,
                        summary: BTreeMap::new(),
                    },
                    expected_head,
                )
                .await?;
            let (appended_commit, added) = match outcome {
                AppendOutcome::Appended { added_records, .. } => (true, added_records),
                // Recorded by an earlier run: nothing of this run to check.
                AppendOutcome::AlreadyIngested => (false, result.rows),
            };
            checks
                .entry(table.table)
                .or_default()
                .add(&object.object_key, result, added);
            let short = table.table.rsplit('.').next().unwrap_or(table.table);
            tables.insert(
                short.to_owned(),
                json!({"staged": table.rows, "appended_rows": rows, "appended": appended_commit}),
            );
        }
        Ok(json!({
            "object": object.object_key,
            "role": object.role.name(),
            "tables": tables,
            "seconds": seconds(started),
        }))
    }
}

/// Loads every planned object and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let plan_root = PathBuf::from(required_env_value(PLAN_ROOT_ENV)?);
    let source = CourtAuctionSource::compiled()?;
    let objects = planned_objects(&plan_dirs(&plan_root)?, &source)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        &plan_root,
        &LakehouseRead::from_env()?,
    )?);
    locked(&stage)?
        .connection()
        .execute_batch(&CourtAuctionSource::values_sql())
        .context("DuckDB refused the court auction value readers")?;
    // One instant for the run, as the Spark job took one.
    let ingested_at = chrono::Utc::now();
    let run = Run {
        source,
        committer,
        catalog,
        ingested_at,
    };
    let mut loaded = Vec::with_capacity(objects.len());
    let mut checks = BTreeMap::new();
    for object in &objects {
        loaded.push(run.load(&stage, object, &mut checks).await?);
    }
    drop(stage);
    for (table, table_checks) in &checks {
        for line in table_checks.pipes_lines(table) {
            println!("{line}");
        }
    }
    let summary = json!({
        "job": "load-courtauction-go-kr-silver",
        "engine": "duckdb",
        "status": "loaded",
        "objects": loaded,
        "seconds": seconds(started),
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    println!("courtauction-go-kr-duckdb-summary-json {summary}");
    Ok(())
}
