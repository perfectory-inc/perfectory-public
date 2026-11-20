//! `load-data-go-kr-onbid-silver`: the Silver half of 차세대 온비드 public sale (root ADR-0280, ADR-0340).
//!
//! `onbid-silver-load.sh` runs it after `plan-silver-load-objects` fetched, per table, the Bronze
//! objects its commits do not record. For each object, oldest first, `DuckDB` inside the publisher
//! builds its rows (`foundation_publisher_silver::onbid_silver_stage`) and the publisher appends them,
//! in one Iceberg commit that records the object even when it holds no row (root ADR-0559,
//! ADR-0560): an item list's changed conditions, placed on the served parcels
//! (`place_identity_stage`, the code first, then the address), and the removal markers of a complete
//! pass; a compared table's changed keys; every bid result and media row. It was the Spark job
//! `data_go_kr_onbid.py`. The last stdout line is `data-go-kr-onbid-summary-json {json}`, the one the
//! Spark job printed.

use std::{
    path::PathBuf,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile, TableShape,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    onbid_silver_stage::{
        compared_changes, item_changes, plan_dirs, planned_objects, read_object, write,
        OnbidSource, PlannedObject, Role, ROLES,
    },
    place_identity_stage::{
        self, PlacePins, CODE_CHANGE_TABLE, DONG_NAME_TABLE, PARCEL_TABLE, TITLE_TABLE,
    },
    process_memory,
    vworld_parcel_source_contract::served_edition_view,
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::public_data_control_support::required_env_value;

const PLAN_ROOT_ENV: &str = "FOUNDATION_PLATFORM_ONBID_PLAN_ROOT";
pub(crate) const PARCEL_SOURCE: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");

pub(crate) fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no contract for {table}"))
}

/// The snapshot `main` of `table` is at now; `None` for a table with none.
pub(crate) async fn head(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<Option<i64>> {
    Ok(catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id))
}

pub(crate) async fn pinned(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<i64> {
    head(catalog, table)
        .await?
        .with_context(|| format!("{table} has no snapshot to place rows by"))
}

/// The `silver.parcel_boundaries.source_snapshot_id` of the edition the map serves.
pub(crate) fn served_snapshot_id() -> anyhow::Result<String> {
    let contract: Value = serde_json::from_str(PARCEL_SOURCE)?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("the parcel source contract names no snapshot_id_prefix")?;
    let view = served_edition_view(PARCEL_SOURCE)?;
    let edition = view["edition"]
        .as_str()
        .context("the served edition has no name")?;
    Ok(format!("{prefix}{edition}"))
}

/// Seconds since `started`, to a tenth.
pub(crate) fn seconds(started: Instant) -> f64 {
    (started.elapsed().as_secs_f64() * 10.0).round() / 10.0
}

pub(crate) fn locked(
    stage: &Mutex<DuckDbParquetStage>,
) -> anyhow::Result<MutexGuard<'_, DuckDbParquetStage>> {
    stage
        .lock()
        .map_err(|_| anyhow::anyhow!("an earlier step of the load panicked"))
}

/// Appends one object's file (or none) to `table` on `expected_head`, recording the object.
pub(crate) async fn append(
    committer: &LakehouseDataCommitter,
    catalog: &IcebergRestCatalog,
    table: &str,
    file: Option<&StagedFile>,
    object_key: &str,
    expected_head: Option<i64>,
) -> anyhow::Result<bool> {
    let already_ingested = catalog
        .load_ingested_batch_objects(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    let outcome = committer
        .append_at_head(
            DataAppend {
                table_name: table,
                contract: contract(table)?,
                files: file.map_or(&[][..], std::slice::from_ref),
                batch: &[object_key.to_owned()],
                already_ingested: &already_ingested,
                summary: std::collections::BTreeMap::new(),
            },
            expected_head,
        )
        .await?;
    Ok(matches!(outcome, AppendOutcome::Appended { .. }))
}

/// `place_rows` of the rows `onbid_rows` holds now: their source PNU and the item name (the address
/// the provider writes first), numbered.
fn into_place_rows(stage: &DuckDbParquetStage, keep: bool) -> anyhow::Result<()> {
    let insert = if keep {
        "INSERT INTO place_rows SELECT (SELECT count(*) FROM place_rows) + row_number() OVER (), pnu, item_name, cltr_mng_no, pbct_cdtn_no FROM onbid_rows"
    } else {
        "CREATE OR REPLACE TEMP TABLE place_rows AS SELECT row_number() OVER () AS _row, pnu, item_name AS address, cltr_mng_no, pbct_cdtn_no FROM onbid_rows"
    };
    stage.connection().execute_batch(insert)?;
    Ok(())
}

struct Run {
    source: OnbidSource,
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    shapes: std::collections::BTreeMap<Role, TableShape>,
    ingested_at: chrono::DateTime<chrono::Utc>,
}

impl Run {
    /// Loads one object and appends it; the summary line of it.
    async fn load(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        object: &PlannedObject,
    ) -> anyhow::Result<Value> {
        let started = Instant::now();
        let table = object.role.table();
        let table_contract = contract(table)?;
        let expected_head = head(&self.catalog, table).await?;
        let (mut line, file) = {
            let stage = &*locked(stage)?;
            let read = read_object(
                stage,
                &self.source,
                table_contract,
                object,
                self.ingested_at,
            )?;
            let mut line = json!({
                "object": object.object_key,
                "role": object.role.name(),
                "rows": read.rows,
                "unrepresented_fields": read.unrepresented_fields,
                "unreadable_answers": read.unreadable_answers,
            });
            let staged = match object.role {
                Role::ItemList => {
                    into_place_rows(stage, false)?;
                    let grades = place_identity_stage::place(stage)?;
                    stage.connection().execute_batch(
                        "CREATE OR REPLACE TEMP TABLE onbid_staged AS \
                         SELECT r.* REPLACE (p.pnu AS pnu, p.pnu_basis AS pnu_basis, p.pnu_unresolved_reason AS pnu_unresolved_reason) \
                         FROM onbid_rows r JOIN place_rows x USING (cltr_mng_no, pbct_cdtn_no) JOIN place_result p ON p._row = x._row",
                    )?;
                    let changes = item_changes(
                        stage,
                        table_contract,
                        expected_head,
                        object,
                        read.complete,
                        self.ingested_at,
                    )?;
                    line["complete"] = json!(read.complete);
                    line["source_coded"] = json!(stage.connection().query_row(
                        "SELECT count(*) FROM onbid_rows WHERE pnu IS NOT NULL",
                        [],
                        |row| row.get::<_, i64>(0)
                    )?);
                    line["resolved_by_grade"] = json!(grades);
                    line["changes"] = json!(changes.changes);
                    line["removed"] = json!(changes.removed);
                    "onbid_changes"
                }
                role if role.compared() => {
                    line["changes"] =
                        json!(compared_changes(stage, table_contract, expected_head)?);
                    "onbid_changed"
                }
                _ => "onbid_rows",
            };
            let shape = self
                .shapes
                .get(&object.role)
                .with_context(|| format!("no shape of {table}"))?;
            (
                line,
                write(stage, shape, table, staged, &object.object_key)?,
            )
        };
        line["appended"] = json!(
            append(
                &self.committer,
                &self.catalog,
                table,
                file.as_ref(),
                &object.object_key,
                expected_head,
            )
            .await?
        );
        line["seconds"] = json!(seconds(started));
        Ok(line)
    }
}

/// Loads every planned object and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let plan_root = PathBuf::from(required_env_value(PLAN_ROOT_ENV)?);
    let source = OnbidSource::compiled()?;
    let objects = planned_objects(&plan_dirs(&plan_root)?, &source)?;
    let budget = Duration::from_secs(source.run_time_budget_seconds()?);
    let committer = LakehouseDataCommitter::from_env().await?;
    let mut shapes = std::collections::BTreeMap::new();
    for role in ROLES {
        shapes.insert(role, committer.table_shape(role.table()).await?);
    }
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        &plan_root,
        &LakehouseRead::from_env()?,
    )?);
    locked(&stage)?
        .connection()
        .execute_batch(&source.values_sql()?)
        .context("DuckDB refused the Onbid value readers")?;
    // One instant for the run, as the Spark job took one.
    let ingested_at = chrono::Utc::now();
    let mut summary = json!({
        "job": "load-data-go-kr-onbid-silver",
        "engine": "duckdb",
        "status": "loaded",
        "objects": [],
    });
    let run = Run {
        source,
        committer,
        catalog,
        shapes,
        ingested_at,
    };
    if objects.iter().any(|object| object.role == Role::ItemList) {
        // Root ADR-0288 §1: the code becomes the current pnu here; the inputs are read once a run,
        // the parcels cut to the PNUs these objects can ask about (root ADR-0390).
        let pins = PlacePins {
            code_changes: pinned(&run.catalog, CODE_CHANGE_TABLE).await?,
            dong_names: pinned(&run.catalog, DONG_NAME_TABLE).await?,
            parcels: pinned(&run.catalog, PARCEL_TABLE).await?,
            titles: pinned(&run.catalog, TITLE_TABLE).await?,
            served_snapshot_id: served_snapshot_id()?,
        };
        let stage = &*locked(&stage)?;
        // Each input is read once a run; how long each took is in the summary.
        let step = Instant::now();
        place_identity_stage::load_reference(stage, &pins)?;
        let reference_seconds = seconds(step);
        stage.connection().execute_batch(
            "CREATE OR REPLACE TEMP TABLE place_rows (_row BIGINT, pnu VARCHAR, address VARCHAR, cltr_mng_no VARCHAR, pbct_cdtn_no BIGINT)",
        )?;
        let item_contract = contract(Role::ItemList.table())?;
        let step = Instant::now();
        for object in objects
            .iter()
            .filter(|object| object.role == Role::ItemList)
        {
            read_object(stage, &run.source, item_contract, object, ingested_at)?;
            into_place_rows(stage, true)?;
        }
        let item_objects_seconds = seconds(step);
        let step = Instant::now();
        place_identity_stage::offers(stage)?;
        let offers_seconds = seconds(step);
        let step = Instant::now();
        let read = place_identity_stage::read_served(stage, &pins, contract(TITLE_TABLE)?)?;
        summary["place_seconds"] = json!({
            "reference": reference_seconds, "item_objects": item_objects_seconds,
            "offers": offers_seconds, "served_parcels_and_titles": seconds(step),
        });
        summary["parcels_read"] = json!({"asked": read.asked, "held": read.held});
        summary["place_pins"] = json!({
            "code_changes": pins.code_changes, "dong_names": pins.dong_names,
            "parcels": pins.parcels, "titles": pins.titles, "served": pins.served_snapshot_id,
        });
    }
    let mut loaded = Vec::with_capacity(objects.len());
    let mut deferred = 0_u64;
    for object in &objects {
        if started.elapsed() >= budget {
            // Not started before the run's time budget: unrecorded, so the next run's plan owes it.
            deferred += 1;
            continue;
        }
        loaded.push(run.load(&stage, object).await?);
    }
    drop(stage);
    summary["objects"] = Value::Array(loaded);
    summary["deferred"] = json!(deferred);
    summary["seconds"] = json!(seconds(started));
    // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
    summary["memory_peak_bytes"] = json!(process_memory::cgroup_peak_bytes());
    println!("data-go-kr-onbid-summary-json {summary}");
    Ok(())
}
