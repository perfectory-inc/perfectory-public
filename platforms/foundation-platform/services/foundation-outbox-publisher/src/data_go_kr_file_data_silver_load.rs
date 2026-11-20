//! `load-data-go-kr-file-data-silver`: the Silver half of the data.go.kr bulk files (root ADR-0401
//! §3–4).
//!
//! `data-go-kr-file-data-cycle.sh` runs it after `plan-silver-load-objects` fetched, per table, the
//! Bronze files its commits do not record. For each file, in plan order, `DuckDB` inside the
//! publisher builds the edition's rows (`foundation_publisher_silver::data_go_kr_file_data_stage`)
//! and the publisher appends the new and changed keys and the removal markers in one Iceberg
//! commit that records the file (root ADR-0559, ADR-0560). A table whose links a 법정동 code change
//! can move then records the change table's snapshot its links were made against; a table no file
//! was loaded into, whose last such record is an earlier snapshot, has the rows under the changed
//! codes linked again (root ADR-0329 §3). It was the Spark job `data_go_kr_file_data.py`. The last
//! stdout line is `data-go-kr-file-data-summary-json {json}`, the one the Spark job printed.

use std::{path::PathBuf, sync::Mutex, time::Instant};

use anyhow::Context;
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{LakehouseDataCommitter, TableShape};
use foundation_publisher_silver::{
    data_go_kr_file_data_stage::{
        affected_filter, changed_prefixes, changes, current, edition_filter, follow_record, keyed,
        last_followed, link, linked, planned_files, read_file, relinked, staged_edition,
        FileDataSource, FileEntry, PlaceInputs, PlannedFile,
    },
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, PYTHON_VALUES_SQL},
    onbid_silver_stage::{plan_dirs, write},
    place_identity_stage::{
        self, PlacePins, CODE_CHANGE_TABLE, DONG_NAME_TABLE, PARCEL_TABLE, TITLE_TABLE,
    },
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    onbid_silver_load::{append, contract, head, locked, pinned, seconds, served_snapshot_id},
    public_data_control_support::required_env_value,
};

const PLAN_ROOT_ENV: &str = "FOUNDATION_PLATFORM_DATA_GO_KR_FILE_DATA_PLAN_ROOT";

struct Run {
    source: FileDataSource,
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

    async fn shape(&self, table: &str) -> anyhow::Result<TableShape> {
        self.committer.table_shape(table).await
    }

    /// Loads one edition and appends it; the summary line of it.
    async fn load(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        planned: &PlannedFile,
    ) -> anyhow::Result<Value> {
        let started = Instant::now();
        let file = self.source.file(&planned.slug)?;
        let table = file.table();
        let table_contract = contract(&table)?;
        let shape = self.shape(&table).await?;
        let expected_head = head(&self.catalog, &table).await?;
        let followed = file.place_link()?.followed();
        let (mut line, staged) = {
            let stage = &*locked(stage)?;
            let rows = read_file(stage, file, &planned.path)?;
            keyed(stage, file)?;
            let previous = current(
                stage,
                file,
                table_contract,
                expected_head,
                &edition_filter(file)?,
            )?;
            file.check_bounds(rows, previous)?;
            link(stage, file, "file_keyed", self.place()?)?;
            staged_edition(stage, table_contract, planned, self.ingested_at)?;
            let found = changes(
                stage,
                file,
                table_contract,
                planned.snapshot_id(),
                self.ingested_at,
            )?;
            let mut line = json!({
                "dataset": file.slug,
                "object": planned.object_key,
                "rows": rows,
                "appended": found.appended,
                "removed": found.removed,
            });
            if followed {
                let linked = linked(stage, file.place_link()?)?;
                line["linked"] = json!(linked);
                #[allow(clippy::cast_precision_loss)]
                let rate =
                    (rows > 0).then(|| (linked as f64 / rows as f64 * 10_000.0).round() / 10_000.0);
                line["link_rate"] = json!(rate);
            }
            (
                line,
                write(stage, &shape, &table, "file_changes", &planned.object_key)?,
            )
        };
        append(
            &self.committer,
            &self.catalog,
            &table,
            staged.as_ref(),
            &planned.object_key,
            expected_head,
        )
        .await?;
        if followed {
            // The links were made against the change table at the run's pin: the next follow
            // starts there.
            let next_head = head(&self.catalog, &table).await?;
            append(
                &self.committer,
                &self.catalog,
                &table,
                None,
                &follow_record(self.pins.code_changes, self.ingested_at),
                next_head,
            )
            .await?;
        }
        line["seconds"] = json!(seconds(started));
        Ok(line)
    }

    /// Links again the rows of `file` a code change since its last record can move; the summary
    /// line of it, or `None` when the table was never loaded or its links are as of the pin.
    async fn follow(
        &self,
        stage: &Mutex<DuckDbParquetStage>,
        file: &FileEntry,
    ) -> anyhow::Result<Option<Value>> {
        let started = Instant::now();
        let table = file.table();
        let recorded = self
            .catalog
            .load_ingested_batch_objects(&table)
            .await?
            .with_context(|| format!("{table} does not exist"))?;
        let after = self.pins.code_changes;
        let Some(before) = last_followed(recorded.iter().map(String::as_str)) else {
            return Ok(None);
        };
        if before == after {
            return Ok(None);
        }
        let table_contract = contract(&table)?;
        let shape = self.shape(&table).await?;
        let expected_head = head(&self.catalog, &table).await?;
        let record = follow_record(after, self.ingested_at);
        let (mut line, staged) = {
            let stage = &*locked(stage)?;
            let prefixes = changed_prefixes(stage, before, after)?;
            let checked = current(
                stage,
                file,
                table_contract,
                expected_head,
                &affected_filter(file.place_link()?, prefixes.is_some()),
            )?;
            relinked(stage, file, table_contract, self.place()?, self.ingested_at)?;
            // The same keys on both sides: only the rows whose link moved, and no marker.
            let found = changes(stage, file, table_contract, "", self.ingested_at)?;
            anyhow::ensure!(
                found.removed == 0,
                "{table}: a follow made {} removal markers",
                found.removed
            );
            (
                json!({
                    "dataset": file.slug,
                    "from_snapshot": before,
                    "to_snapshot": after,
                    "prefixes": prefixes,
                    "checked": checked,
                    "appended": found.appended,
                }),
                write(stage, &shape, &table, "file_changes", &record)?,
            )
        };
        append(
            &self.committer,
            &self.catalog,
            &table,
            staged.as_ref(),
            &record,
            expected_head,
        )
        .await?;
        line["seconds"] = json!(seconds(started));
        Ok(Some(line))
    }
}

/// Loads every planned file, follows the code changes, and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let plan_root = PathBuf::from(required_env_value(PLAN_ROOT_ENV)?);
    let source = FileDataSource::compiled()?;
    let files = planned_files(&plan_dirs(&plan_root)?, &source)?;
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
    let mut editions = Vec::with_capacity(files.len());
    for planned in &files {
        editions.push(run.load(&stage, planned).await?);
    }
    let mut follows = Vec::new();
    for file in &run.source.files {
        let loaded = files.iter().any(|planned| planned.slug == file.slug);
        if loaded || !file.place_link()?.followed() {
            continue;
        }
        if let Some(line) = run.follow(&stage, file).await? {
            follows.push(line);
        }
    }
    drop(stage);
    let summary = json!({
        "job": "load-data-go-kr-file-data-silver",
        "engine": "duckdb",
        "status": "loaded",
        "editions": editions,
        "follows": follows,
        "place_pins": {
            "code_changes": run.pins.code_changes, "dong_names": run.pins.dong_names,
            "parcels": run.pins.parcels, "titles": run.pins.titles, "served": run.pins.served_snapshot_id,
        },
        "seconds": seconds(started),
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    println!("data-go-kr-file-data-summary-json {summary}");
    Ok(())
}
