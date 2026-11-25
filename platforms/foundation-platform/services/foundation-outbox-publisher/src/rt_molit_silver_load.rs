//! `load-rt-molit-real-transaction-silver`: the 국토교통부 실거래가 Silver load (root ADR-0348, ADR-0349,
//! ADR-0559).
//!
//! The lane's Dagster multi-asset runs it over the plan directories of `silver.real_transaction`
//! (`orchestration/dagster/rt_molit_lane.py`, root ADR-0616 §8; until the lane moves to AWS the Spark
//! job keeps loading on the data host). `DuckDB` inside the publisher reads every planned export once
//! (`foundation_publisher_silver::rt_molit_silver_stage`), reads the served parcels their lots can
//! ask about once, and then, oldest landing first, appends each object in its own Iceberg commit
//! that records it: the lines not loaded to `silver.real_transaction_rejected` first (root
//! ADR-0364), then the month's new and changed names and the removal markers of the names the
//! export no longer holds. A quarantined export is recorded with no row. An object not started
//! before the contract's run time budget is left unrecorded, so the next run's plan owes it again.
//! It was `rt_molit_real_transaction.py load` (Spark).
//!
//! Before the summary, one Dagster Pipes `report_asset_check` line per table and result check
//! (orchestration/dagster/load_checks.py): every object's rows were committed whole, repeat no key
//! of the table's unique gate and miss no required column. The last stdout line is
//! `rt-molit-real-transaction-summary-json {json}`; a run that linked fewer unmasked lots than the
//! contract's bound loads its rows and then fails, so it is seen.

use std::{collections::BTreeMap, path::PathBuf, sync::Mutex, time::Instant};

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    onbid_silver_stage::{plan_dirs, write},
    place_identity_stage::{CODE_CHANGE_TABLE, DONG_NAME_TABLE, PARCEL_TABLE},
    process_memory,
    rt_molit_silver_stage::{
        link_bound_breached, load_reference, month_changes, place_export, planned_objects,
        read_parcels, rejected_share, stage_export, Pins, PlannedObject, RtMolitSource, Staged,
        REJECTED_TABLE, TABLE,
    },
    silver_changes::{result_checks, ResultChecks},
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::courtauction_silver_load::TableChecks;
use crate::onbid_silver_load::{contract, head, locked, pinned, seconds, served_snapshot_id};
use crate::public_data_control_support::required_env_value;

const PLAN_ROOT_ENV: &str = "FOUNDATION_PLATFORM_RT_MOLIT_PLAN_ROOT";

struct Run {
    source: RtMolitSource,
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    ingested_at_epoch_seconds: i64,
}

impl Run {
    /// Commits `file` (or no file) to `table` at `expected_head`, recording `object`, and adds the
    /// rows' result checks; whether the commit appended (not a record an earlier run committed).
    async fn commit(
        &self,
        table: &'static str,
        file: Option<&StagedFile>,
        object: &str,
        expected_head: Option<i64>,
        result: ResultChecks,
        checks: &mut BTreeMap<&'static str, TableChecks>,
    ) -> anyhow::Result<bool> {
        let already_ingested = self
            .catalog
            .load_ingested_batch_objects(table)
            .await?
            .with_context(|| format!("{table} does not exist"))?;
        let outcome = self
            .committer
            .append_at_head(
                DataAppend {
                    table_name: table,
                    contract: contract(table)?,
                    files: file.map_or(&[][..], std::slice::from_ref),
                    batch: &[object.to_owned()],
                    already_ingested: &already_ingested,
                    summary: BTreeMap::new(),
                },
                expected_head,
            )
            .await?;
        let (appended, added) = match outcome {
            AppendOutcome::Appended { added_records, .. } => (true, added_records),
            // Recorded by an earlier run: nothing of this run to check.
            AppendOutcome::AlreadyIngested => (false, result.rows),
        };
        checks.entry(table).or_default().add(object, result, added);
        Ok(appended)
    }

    /// Appends the lines of object `index` that are not loaded, when it has any.
    async fn append_rejected(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        object: &PlannedObject,
        index: usize,
        checks: &mut BTreeMap<&'static str, TableChecks>,
    ) -> anyhow::Result<()> {
        let shape = self.committer.table_shape(REJECTED_TABLE).await?;
        let expected_head = head(&self.catalog, REJECTED_TABLE).await?;
        let rows = format!("rt_rejected_{index}");
        let (file, result) = {
            let stage = &*locked(stage)?;
            (
                write(stage, &shape, REJECTED_TABLE, &rows, &object.object_key)?,
                result_checks(stage, contract(REJECTED_TABLE)?, &rows)?,
            )
        };
        self.commit(
            REJECTED_TABLE,
            file.as_ref(),
            &object.object_key,
            expected_head,
            result,
            checks,
        )
        .await?;
        Ok(())
    }

    /// Loads object `index`, staged as `staged`; its summary line and whether it linked fewer
    /// unmasked lots than the contract's bound.
    #[allow(clippy::too_many_lines)] // one object: its rejected lines, its placement, its commit
    async fn load(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        object: &PlannedObject,
        staged: &Staged,
        index: usize,
        checks: &mut BTreeMap<&'static str, TableChecks>,
    ) -> anyhow::Result<(Value, bool)> {
        let started = Instant::now();
        // Root ADR-0364: the lines not loaded land first, in their own table, referencing Bronze.
        if staged.rejected > 0 {
            self.append_rejected(stage, object, index, checks).await?;
        }
        let mut line = serde_json::Map::new();
        for (name, value) in [
            ("object", json!(object.object_key)),
            ("dataset", json!(object.dataset)),
            ("month", json!(object.month)),
            ("rows", json!(staged.rows)),
            ("cancelled", json!(staged.cancelled)),
            ("rejected", json!(staged.rejected)),
            (
                "rejected_share",
                json!(rejected_share(staged.rows, staged.rejected)),
            ),
            ("refused", json!(staged.refused)),
            ("quarantined", json!(staged.quarantined)),
            ("rejected_reasons", json!(staged.rejected_reasons)),
            ("unrepresented_fields", json!(staged.unrepresented_fields)),
        ] {
            line.insert(name.to_owned(), value);
        }
        let table = contract(TABLE)?;
        let shape = self.committer.table_shape(TABLE).await?;
        // Pinned before the compare, so the commit refuses a head that moved since.
        let expected_head = head(&self.catalog, TABLE).await?;
        if staged.quarantined.is_some() {
            // Recorded with no row and no comparison: an empty export compared with the month would
            // remove every row the month holds. No later run stops on it again.
            let appended = self
                .commit(
                    TABLE,
                    None,
                    &object.object_key,
                    expected_head,
                    ResultChecks::default(),
                    checks,
                )
                .await?;
            line.insert("appended".to_owned(), json!(appended));
            line.insert("seconds".to_owned(), json!(seconds(started)));
            return Ok((Value::Object(line), false));
        }
        let (linked, changes, removed, file, result) = {
            let stage = &*locked(stage)?;
            let linked = place_export(stage, table, object, index, self.ingested_at_epoch_seconds)?;
            let (changes, removed) = month_changes(
                stage,
                table,
                expected_head,
                object,
                self.ingested_at_epoch_seconds,
            )?;
            let result = result_checks(stage, table, "rt_changes")?;
            let file = write(stage, &shape, TABLE, "rt_changes", &object.object_key)?;
            (linked, changes, removed, file, result)
        };
        let appended = self
            .commit(
                TABLE,
                file.as_ref(),
                &object.object_key,
                expected_head,
                result,
                checks,
            )
            .await?;
        let breached = link_bound_breached(
            linked.lots,
            linked.unresolved_lots,
            self.source.link_bound(&object.dataset),
        );
        #[allow(clippy::cast_precision_loss)] // lot counts of one export
        let unresolved_share = if linked.lots == 0 {
            0.0
        } else {
            linked.unresolved_lots as f64 / linked.lots as f64
        };
        for (name, value) in [
            ("changes", json!(changes)),
            ("removed", json!(removed)),
            (
                "linked",
                json!({"rows": linked.rows, "pnu": linked.pnu, "legal_dong_only": linked.legal_dong_only,
                       "unresolved": linked.unresolved}),
            ),
            ("unmasked_lots", json!(linked.lots)),
            ("unmasked_unresolved_share", json!(unresolved_share)),
            ("appended", json!(appended)),
            ("seconds", json!(seconds(started))),
        ] {
            line.insert(name.to_owned(), value);
        }
        Ok((Value::Object(line), breached))
    }
}

/// Loads every planned object and prints the summary line.
#[allow(clippy::too_many_lines)] // the run in its order: stage, read parcels, load, report
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let plan_root = PathBuf::from(required_env_value(PLAN_ROOT_ENV)?);
    let source = RtMolitSource::compiled()?;
    let objects = planned_objects(&plan_dirs(&plan_root)?, &source)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        &plan_root,
        &LakehouseRead::from_env()?,
    )?);
    // A table is born on its first commit (root ADR-0564 §1), as the Spark job made it.
    for table in [TABLE, REJECTED_TABLE] {
        committer.create_table_if_absent(contract(table)?).await?;
    }
    // One instant for the run, as the Spark job took one.
    let run = Run {
        source,
        committer,
        catalog,
        ingested_at_epoch_seconds: chrono::Utc::now().timestamp(),
    };
    let mut staged = Vec::with_capacity(objects.len());
    let mut parcels_read = Value::Null;
    if !objects.is_empty() {
        // Root ADR-0349 §5: names → codes from the newest snapshot, old codes → current through the
        // pairs in force, a PNU only when it is a parcel of the served edition. Read once a run.
        let pins = Pins {
            code_changes: pinned(&run.catalog, CODE_CHANGE_TABLE).await?,
            dong_names: pinned(&run.catalog, DONG_NAME_TABLE).await?,
            parcels: pinned(&run.catalog, PARCEL_TABLE).await?,
            served_snapshot_id: served_snapshot_id()?,
        };
        let stage = &*locked(&stage)?;
        load_reference(stage, &pins)?;
        for (index, object) in objects.iter().enumerate() {
            staged.push(stage_export(
                stage,
                contract(TABLE)?,
                object,
                index,
                (&run.source, run.ingested_at_epoch_seconds, &plan_root),
            )?);
        }
        let read = read_parcels(stage, &pins, contract(PARCEL_TABLE)?, objects.len())?;
        parcels_read = json!({"asked": read.asked, "held": read.held});
    }
    let mut loaded = Vec::with_capacity(objects.len());
    let mut checks = BTreeMap::new();
    let mut below_bound = Vec::new();
    let mut deferred = 0_u64;
    for (index, (object, staged)) in objects.iter().zip(&staged).enumerate() {
        if started.elapsed().as_secs_f64() > run.source.run_time_budget_seconds {
            deferred += 1;
            continue;
        }
        let (line, breached) = run.load(&stage, object, staged, index, &mut checks).await?;
        if breached {
            below_bound.push(object.object_key.clone());
        }
        loaded.push(line);
    }
    drop(stage);
    for (table, table_checks) in &checks {
        for line in table_checks.pipes_lines(table) {
            println!("{line}");
        }
    }
    let quarantined: Vec<&str> = objects
        .iter()
        .zip(&staged)
        .filter(|(_, staged)| staged.quarantined.is_some())
        .map(|(object, _)| object.object_key.as_str())
        .collect();
    let rejected: u64 = loaded
        .iter()
        .filter_map(|line| line["rejected"].as_u64())
        .sum();
    let summary = json!({
        "job": "load-rt-molit-real-transaction-silver",
        "engine": "duckdb",
        "status": if below_bound.is_empty() { "loaded" } else { "loaded_link_rate_below_bound" },
        "objects": loaded,
        "parcels_read": parcels_read,
        "quarantined_exports": quarantined.len(),
        // Not started before the run's time budget: unrecorded, so the next run's plan owes them.
        "deferred": deferred,
        "rejected": rejected,
        "seconds": seconds(started),
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    println!("rt-molit-real-transaction-summary-json {summary}");
    if !quarantined.is_empty() {
        eprintln!(
            "rt-molit-real-transaction: {} export(s) quarantined whole (refused, or more lines rejected than \
             max_rejected_share); their lines are in {REJECTED_TABLE}, the run goes on: {}",
            quarantined.len(),
            quarantined.join(", ")
        );
    }
    anyhow::ensure!(
        below_bound.is_empty(),
        "{} object(s) linked fewer unmasked lots than the contract's bound; loaded, failing so it is seen: {}",
        below_bound.len(),
        below_bound.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{parse_command, Command};

    #[test]
    fn the_command_is_explicit() -> anyhow::Result<()> {
        assert_eq!(
            parse_command([
                "foundation-outbox-publisher",
                "load-rt-molit-real-transaction-silver"
            ])?,
            Command::LoadRtMolitRealTransactionSilver
        );
        Ok(())
    }
}
