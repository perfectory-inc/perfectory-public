//! `build-unit-official-price-versions` and `export-unit-official-price-takeover-sides`: the unit
//! prices composed by `DuckDB` beside the Spark join they replace (root ADR-0317 §6, ADR-0564 §6).
//!
//! While `gold-panel-rebuild.sh` joins each vintage with Spark (`unit_official_price.py`) into
//! `silver.unit_official_price`, and the versions migration (`vintage_versions.py`) carries each
//! of its batches into `silver.unit_official_price_versions`, this build is that join's shadow. It
//! names the batch the readers read (the newest, `unit_price_stage::batch`) and writes nothing: the
//! versions table keeps one writer until the switch. Its verdict, `takeover_equal` on the declared
//! 시도, composes that batch's vintage at the batch's own pins (the hub snapshots its id names) and
//! compares the rows, as values, with the rows Spark wrote for it (`gold-takeover-verdict.sh`,
//! `engine_equality.py`).
//!
//! The switch makes this build the writer of the versions table, appending each vintage's changed
//! keys, and deletes the Spark join.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{ensure, Context};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{table_at, with_contract_columns, DuckDbParquetStage},
    unit_official_price_stage::{self as stage, BatchPins, EXCLUSIVE_TABLE, PRICE_TABLE, REPLACED},
    unit_price_stage::batch,
};
use lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    gold_takeover_sides::sql_path,
    parcel_nearby_versions_build::{lakehouse_read, write_summary},
    public_data_control_support::{optional_env_value, required_env_value},
};

const TABLE: &str = "silver.unit_official_price_versions";
const JOB: &str = "build-unit-official-price-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
/// The verdict's environment, as `gold-takeover-verdict.sh` sets it for every export.
const PREFIX: &str = "FOUNDATION_PLATFORM_GOLD_TAKEOVER_";
/// What the verdict compares by: a batch holds one row per unit and reference date.
const KEY: [&str; 2] = ["mgm_bldrgst_pk", "base_date"];

/// The batch of [`REPLACED`] its readers read (the newest vintage's; of a vintage joined again, the
/// last appended: `unit_price_stage::batch`) and the head that records it.
async fn newest_batch(catalog: &IcebergRestCatalog) -> anyhow::Result<(BatchPins, i64)> {
    let log = catalog
        .load_ingest_commit_log(REPLACED)
        .await?
        .with_context(|| format!("{REPLACED} does not exist"))?;
    let head = log
        .head
        .with_context(|| format!("{REPLACED} holds no snapshot"))?;
    let order = batch::order_batches(&log.ingested, log.head, &log.snapshots);
    // A load identity is `unit-official-price/base-date-v2/<시도>/<batch>`.
    let ids: BTreeSet<String> = log
        .ingested
        .keys()
        .filter_map(|record| record.splitn(4, '/').nth(3))
        .filter(|id| batch::batch_vintage(id).is_some())
        .map(str::to_owned)
        .collect();
    let newest = batch::newest_batch(&ids, &order)?
        .with_context(|| format!("{REPLACED} records no batch of the join"))?;
    Ok((BatchPins::parse(&newest)?, head))
}

/// The input pins a composition of `pins` reads, and the head of [`REPLACED`] its batch is read at.
fn pins_record(pins: &BatchPins, head: i64) -> BTreeMap<&'static str, i64> {
    BTreeMap::from([
        (PRICE_TABLE, pins.price),
        (EXCLUSIVE_TABLE, pins.exclusive),
        (REPLACED, head),
    ])
}

/// Names the batch the shadow follows; writes nothing to the lakehouse.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let (pins, head) = newest_batch(&catalog).await?;
    let batch = pins.batch();
    write_summary(
        &summary_path,
        &json!({
            "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "unchanged",
            "shadow_of": {"table": REPLACED, "build": batch},
            "pins": pins_record(&pins, head), "vintage": pins.vintage,
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "gold-versions {TABLE} shadow of {REPLACED} batch {batch}: nothing is written while \
         Spark joins the vintages (root ADR-0564 §6)"
    );
    Ok(())
}

/// Writes `checks.json`, `reference.parquet` (Spark's rows of the newest batch in the 시도),
/// `candidate.parquet` (the same batch composed here at its pins) and `sides.json` for
/// `engine_equality.py`, in the verdict's work directory.
#[allow(clippy::too_many_lines)]
pub(crate) async fn export_sides() -> anyhow::Result<()> {
    let table = required_env_value(&format!("{PREFIX}TABLE"))?;
    ensure!(
        table == TABLE,
        "export-unit-official-price-takeover-sides judges {TABLE}, not {table}"
    );
    let work = required_env_value(&format!("{PREFIX}WORK_DIR"))?;
    let sido = required_env_value(&format!("{PREFIX}SIDO"))?;
    let memory_limit = optional_env_value(&format!("{PREFIX}MEMORY_LIMIT"))?;
    // A 시도 that is not two digits, or the last code, is refused before anything is read.
    let scope = stage::sido_scope(&sido)?;
    let replaced = industrial_complex_lakehouse_contract_by_table_name(REPLACED)
        .with_context(|| format!("no lakehouse contract for {REPLACED}"))?;
    let columns: Vec<&str> = replaced.columns.iter().map(|column| column.name).collect();
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let (pins, head) = newest_batch(&catalog).await?;
    let batch = pins.batch();

    let work = Path::new(&work);
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let connection = duck.connection();
    if let Some(limit) = &memory_limit {
        connection.execute("SET memory_limit = ?", [limit.as_str()])?;
    }
    stage::load(connection, &pins)?;
    let composed = stage::compose(connection, &pins, Some(scope.as_str()))?;
    // Spark's rows of the batch in the 시도: the table is partitioned by it.
    let spark = with_contract_columns(connection, replaced, &table_at(REPLACED, head))?;
    let listed = columns.join(", ");
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE side_reference AS SELECT {listed} FROM {spark} \
             WHERE sido = '{sido}' AND source_snapshot_id = '{batch}';\n\
             CREATE OR REPLACE TEMP TABLE side_candidate AS SELECT {listed} FROM unit_prices;"
        ))
        .with_context(|| format!("DuckDB failed to read {REPLACED}'s rows of {batch}"))?;
    let reference_rows: i64 =
        connection.query_row("SELECT count(*) FROM side_reference", [], |row| row.get(0))?;
    ensure!(
        reference_rows > 0,
        "{REPLACED} holds no row of {batch} in 시도 {sido}: nothing to compare with"
    );
    for name in ["reference", "candidate"] {
        let target = sql_path(&work.join(format!("{name}.parquet")))?;
        connection
            .execute_batch(&format!(
                "COPY side_{name} TO '{target}' (FORMAT parquet, COMPRESSION zstd)"
            ))
            .with_context(|| format!("DuckDB failed to write {name}.parquet"))?;
    }
    drop(duck);

    // The result checks the composition itself made: the whole dictionary was free of conflicts
    // (it refuses otherwise) and the 시도 joined rows.
    let checks = json!({
        "failed": [], "units_nationally": composed.units, "candidate_rows": composed.rows,
    });
    std::fs::write(
        work.join("checks.json"),
        serde_json::to_vec_pretty(&checks)?,
    )
    .context("cannot write checks.json")?;
    let content: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|name| !KEY.contains(name))
        .collect();
    let sides = json!({
        "table": TABLE, "replaced": REPLACED,
        "reference_snapshot": batch, "candidate_snapshot": format!("duckdb {batch}"),
        "pins": pins_record(&pins, head), "keys": KEY, "content": content,
        // No digest: neither side carries one, and the content is compared as values.
        "digest": serde_json::Value::Null,
        "sido": sido, "sido_rows": reference_rows,
        // The verdict composes the 시도 alone; the nation's rows are the switch's build.
        "rows_nationally": serde_json::Value::Null,
    });
    std::fs::write(work.join("sides.json"), serde_json::to_vec_pretty(&sides)?)
        .context("cannot write sides.json")?;
    println!(
        "unit-official-price-takeover-sides {TABLE}: reference {REPLACED}@{head} batch {batch} \
         ({reference_rows} rows in 시도 {sido}), candidate composed at its pins ({} rows); \
         keys={KEY:?} content={}",
        composed.rows,
        content.len()
    );
    Ok(())
}
