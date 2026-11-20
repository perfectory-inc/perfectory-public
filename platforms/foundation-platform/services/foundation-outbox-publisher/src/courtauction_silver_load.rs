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

use std::{path::PathBuf, sync::Mutex, time::Instant};

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{LakehouseDataCommitter, TableShape};
use foundation_publisher_silver::{
    courtauction_silver_stage::{
        appended, planned_objects, read_object, CourtAuctionSource, PlannedObject, ROLES,
    },
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    onbid_silver_stage::{plan_dirs, write},
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::onbid_silver_load::{append, contract, head, locked};
use crate::public_data_control_support::required_env_value;

const PLAN_ROOT_ENV: &str = "FOUNDATION_PLATFORM_COURT_AUCTION_PLAN_ROOT";

/// Seconds since `started`, to a tenth.
fn seconds(started: Instant) -> f64 {
    (started.elapsed().as_secs_f64() * 10.0).round() / 10.0
}

struct Run {
    source: CourtAuctionSource,
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    shapes: std::collections::BTreeMap<&'static str, TableShape>,
    ingested_at: chrono::DateTime<chrono::Utc>,
}

impl Run {
    /// Loads one object into each table its document writes; the summary line of it.
    async fn load(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        object: &PlannedObject,
    ) -> anyhow::Result<Value> {
        let started = Instant::now();
        let staged = {
            let stage = &*locked(stage)?;
            read_object(stage, &self.source, object, self.ingested_at)?
        };
        let mut tables = serde_json::Map::new();
        for table in &staged {
            let table_contract = contract(table.table)?;
            // Pinned before the compare, so the commit refuses a head that moved since.
            let expected_head = head(&self.catalog, table.table).await?;
            let (file, rows) = {
                let stage = &*locked(stage)?;
                let (rows_table, rows) = appended(stage, table_contract, expected_head, table)?;
                let shape = self
                    .shapes
                    .get(table.table)
                    .with_context(|| format!("no shape of {}", table.table))?;
                (
                    write(stage, shape, table.table, &rows_table, &object.object_key)?,
                    rows,
                )
            };
            let appended_commit = append(
                &self.committer,
                &self.catalog,
                table.table,
                file.as_ref(),
                &object.object_key,
                expected_head,
            )
            .await?;
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
    let mut shapes = std::collections::BTreeMap::new();
    for role in ROLES {
        for (table, _) in role.tables() {
            shapes.insert(*table, committer.table_shape(table).await?);
        }
    }
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
        shapes,
        ingested_at,
    };
    let mut loaded = Vec::with_capacity(objects.len());
    for object in &objects {
        loaded.push(run.load(&stage, object).await?);
    }
    drop(stage);
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
