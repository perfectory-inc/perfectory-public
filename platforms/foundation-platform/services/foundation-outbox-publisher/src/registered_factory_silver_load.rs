//! `load-registered-factory-silver`: the Silver half of 한국산업단지공단 전국등록공장현황 (root
//! ADR-0280 §4).
//!
//! `registered-factory-cycle.sh` runs it after `plan-silver-load-objects` fetched the Bronze zips
//! no commit of `silver.registered_factory` records. For each, oldest first, `DuckDB` inside the
//! publisher reads the edition, gives each row the PNU its address resolves to
//! (`foundation_publisher_silver::registered_factory_stage`), and the publisher appends the whole
//! edition in one Iceberg commit that records the zip (root ADR-0559, ADR-0560). The lines of the
//! newest edition a 법정동 code change since the table's last follow can move are then linked
//! again (root ADR-0329 §3, ADR-0416). It was the Spark job `data_go_kr_registered_factory.py`.
//! The last stdout line is `registered-factory-summary-json {json}`, the one the Spark job printed.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::Instant,
};

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    data_go_kr_file_data_stage::{changed_prefixes, follow_record, last_followed, PlaceInputs},
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, PYTHON_VALUES_SQL},
    onbid_silver_stage::{plan_dirs, write, PLAN_SCHEMA},
    place_identity_stage::{
        self, PlacePins, CODE_CHANGE_TABLE, DONG_NAME_TABLE, PARCEL_TABLE, TITLE_TABLE,
    },
    process_memory,
    registered_factory_stage::{
        current, follow, link, link_summary, read_edition, staged_edition, FactorySource,
        SOURCE_SLUG, TABLE,
    },
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    onbid_silver_load::{append, contract, head, locked, pinned, seconds, served_snapshot_id},
    public_data_control_support::required_env_value,
};

const PLAN_ROOT_ENV: &str = "FOUNDATION_PLATFORM_REGISTERED_FACTORY_PLAN_ROOT";

/// One zip a plan fetched: its key in the lakehouse bucket and where the plan put it.
struct PlannedZip {
    object_key: String,
    path: PathBuf,
}

/// Every zip the plans in `plan_dirs` fetched, in plan order (oldest first).
fn planned_zips(plan_dirs: &[PathBuf]) -> anyhow::Result<Vec<PlannedZip>> {
    let mut zips = Vec::new();
    for plan_dir in plan_dirs {
        zips.extend(planned_in(plan_dir)?);
    }
    Ok(zips)
}

fn planned_in(plan_dir: &Path) -> anyhow::Result<Vec<PlannedZip>> {
    let plan: Value = serde_json::from_slice(
        &std::fs::read(plan_dir.join("plan.json"))
            .with_context(|| format!("cannot read the plan in {}", plan_dir.display()))?,
    )?;
    ensure!(
        plan["schema_version"] == PLAN_SCHEMA,
        "{}: not a {PLAN_SCHEMA} plan",
        plan_dir.display()
    );
    plan["objects"]
        .as_array()
        .context("the plan lists no objects")?
        .iter()
        .map(|item| {
            let key = item["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let found = item["source_slug"].as_str().unwrap_or_default();
            ensure!(
                found == SOURCE_SLUG,
                "{key}: {found} is not the source this job loads"
            );
            let local = item["local_path"]
                .as_str()
                .context("a planned object has no local path")?;
            ensure!(
                !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
                "{key}: the local path {local:?} leaves the plan directory"
            );
            Ok(PlannedZip {
                object_key: key.to_owned(),
                path: plan_dir.join(local),
            })
        })
        .collect()
}

struct Run {
    source: FactorySource,
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    pins: PlacePins,
    ingested_at: DateTime<Utc>,
}

impl Run {
    fn place(&self) -> anyhow::Result<PlaceInputs<'_>> {
        Ok(PlaceInputs {
            pins: &self.pins,
            title_contract: contract(TITLE_TABLE)?,
        })
    }

    /// Loads one edition and appends it whole; the summary line of it.
    async fn load(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        planned: &PlannedZip,
    ) -> anyhow::Result<Value> {
        let started = Instant::now();
        let table_contract = contract(TABLE)?;
        let shape = self.committer.table_shape(TABLE).await?;
        let expected_head = head(&self.catalog, TABLE).await?;
        let (mut line, staged) = {
            let stage = &*locked(stage)?;
            let edition = read_edition(stage, &self.source, &planned.path)
                .with_context(|| planned.object_key.clone())?;
            let previous = current(stage, table_contract, expected_head)?;
            self.source
                .check_bounds(edition.rows, previous)
                .with_context(|| planned.object_key.clone())?;
            link(stage, "factory_rows", self.place()?)?;
            staged_edition(
                stage,
                table_contract,
                &planned.object_key,
                &edition.snapshot_id,
                self.ingested_at,
            )?;
            let links = link_summary(stage)?;
            (
                json!({
                    "object": planned.object_key,
                    "source_snapshot_id": edition.snapshot_id,
                    "rows": links.rows,
                    "by_basis": links.by_basis,
                    "unresolved_reasons": links.unresolved_reasons,
                    "match_rate": links.match_rate,
                }),
                write(stage, &shape, TABLE, "factory_staged", &planned.object_key)?,
            )
        };
        let appended = append(
            &self.committer,
            &self.catalog,
            TABLE,
            staged.as_ref(),
            &planned.object_key,
            expected_head,
        )
        .await?;
        // A zip the table already records appends nothing.
        line["appended"] = if appended {
            line["rows"].clone()
        } else {
            json!(0)
        };
        line["seconds"] = json!(seconds(started));
        Ok(line)
    }

    /// Links again the lines of the newest edition the code changes since the table's last follow
    /// can move; the summary line of it, or `None` when its links are as of the pin.
    async fn follow(&self, stage: &Mutex<DuckDbParquetStage>) -> anyhow::Result<Option<Value>> {
        let started = Instant::now();
        let recorded = self
            .catalog
            .load_ingested_batch_objects(TABLE)
            .await?
            .with_context(|| format!("{TABLE} does not exist"))?;
        let after = self.pins.code_changes;
        let before = last_followed(recorded.iter().map(String::as_str));
        if before == Some(after) {
            return Ok(None);
        }
        let table_contract = contract(TABLE)?;
        let shape = self.committer.table_shape(TABLE).await?;
        let expected_head = head(&self.catalog, TABLE).await?;
        let record = follow_record(after, self.ingested_at);
        let (mut line, staged) = {
            let stage = &*locked(stage)?;
            // No follow recorded yet: every line is linked again, as when the earlier snapshot
            // can no longer be read.
            let prefixes = match before {
                Some(before) => changed_prefixes(stage, before, after)?,
                None => None,
            };
            current(stage, table_contract, expected_head)?;
            let found = follow(
                stage,
                table_contract,
                prefixes.is_some(),
                self.place()?,
                self.ingested_at,
            )?;
            (
                json!({
                    "from_snapshot": before,
                    "to_snapshot": after,
                    "prefixes": prefixes,
                    "checked": found.checked,
                    "relinked": found.relinked,
                    "appended": found.appended,
                }),
                write(stage, &shape, TABLE, "factory_staged", &record)?,
            )
        };
        // The record is written even when no line moved: the next follow starts from this snapshot.
        append(
            &self.committer,
            &self.catalog,
            TABLE,
            staged.as_ref(),
            &record,
            expected_head,
        )
        .await?;
        line["seconds"] = json!(seconds(started));
        Ok(Some(line))
    }
}

/// Loads every planned zip, follows the code changes, and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let plan_root = PathBuf::from(required_env_value(PLAN_ROOT_ENV)?);
    let source = FactorySource::compiled()?;
    let zips = planned_zips(&plan_dirs(&plan_root)?)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        &plan_root,
        &LakehouseRead::from_env()?,
    )?);
    // Each place-identity input is read at its head now, once a run.
    let pins = PlacePins {
        code_changes: pinned(&catalog, CODE_CHANGE_TABLE).await?,
        dong_names: pinned(&catalog, DONG_NAME_TABLE).await?,
        parcels: pinned(&catalog, PARCEL_TABLE).await?,
        titles: pinned(&catalog, TITLE_TABLE).await?,
        served_snapshot_id: served_snapshot_id()?,
    };
    {
        let stage = &*locked(&stage)?;
        stage
            .connection()
            .execute_batch(PYTHON_VALUES_SQL)
            .context("DuckDB refused the value readers")?;
        place_identity_stage::load_reference(stage, &pins)?;
    }
    // One instant for the run, to the second, as the Spark job took one.
    let ingested_at = DateTime::from_timestamp(Utc::now().timestamp(), 0)
        .context("the clock names no instant")?;
    let run = Run {
        source,
        committer,
        catalog,
        pins,
        ingested_at,
    };
    let mut editions = Vec::with_capacity(zips.len());
    for planned in &zips {
        editions.push(run.load(&stage, planned).await?);
    }
    let followed = run.follow(&stage).await?;
    drop(stage);
    let summary = json!({
        "job": "data_go_kr_registered_factory",
        "engine": "duckdb",
        "status": "loaded",
        "editions": editions,
        "follow": followed,
        "place_pins": {
            "code_changes": run.pins.code_changes, "dong_names": run.pins.dong_names,
            "parcels": run.pins.parcels, "titles": run.pins.titles, "served": run.pins.served_snapshot_id,
        },
        "seconds": seconds(started),
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    println!("registered-factory-summary-json {summary}");
    Ok(())
}
