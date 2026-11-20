// one-off: until the summary line of `load-data-go-kr-onbid-silver` reports `keys_selected` 0 for
// each of the four tables; deleted, with its stage and its call, in the PR after.
//
//! The correction step of the Onbid Silver load: re-versions the rows whose minute times the Spark
//! load misread (`foundation_publisher_silver::onbid_time_correction_stage`).
//!
//! It runs after the run's objects, inside the same time budget: per table it selects the current
//! rows that hold the pattern, reads the Bronze objects they came from again (as many as the budget
//! leaves; the rest wait for the next run), and appends the correction versions in one commit that
//! records `correction:onbid-kst-minute@<instant>`. What it found is in the load's summary line.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use foundation_outbox::{
    object_storage::{R2InventoryRequest, R2ObjectStorageConfig},
    R2ObjectStorage,
};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::DuckDbParquetStage,
    onbid_silver_stage::{read_object, snapshot_id, write, OnbidSource, PlannedObject, Role},
    onbid_time_correction_stage::{candidates, candidates_of, corrections, keep_reread},
};
use lakehouse_infrastructure::IcebergRestCatalog;
use serde_json::{json, Value};

use crate::onbid_silver_load::{append, contract, head, locked};

/// The tables with times the provider writes as twelve digits.
const ROLES: [Role; 4] = [
    Role::ItemList,
    Role::ItemBidResult,
    Role::Announcement,
    Role::AnnouncementBidResult,
];
const LIST_PAGE: i32 = 1000;

/// What the step reads and writes with: the load's own.
pub(crate) struct Correction<'a> {
    pub(crate) source: &'a OnbidSource,
    pub(crate) committer: &'a LakehouseDataCommitter,
    pub(crate) catalog: &'a IcebergRestCatalog,
    pub(crate) stage: &'a Mutex<DuckDbParquetStage>,
    /// Where the objects read again are put, and removed from.
    pub(crate) work: &'a Path,
    pub(crate) ingested_at: DateTime<Utc>,
    /// When the load started, and how long it may start new work.
    pub(crate) started: Instant,
    pub(crate) budget: Duration,
}

impl Correction<'_> {
    /// Corrects every table; one summary entry per table.
    pub(crate) async fn run(&self) -> anyhow::Result<Value> {
        let storage = R2ObjectStorage::from_config(
            R2ObjectStorageConfig::lakehouse_reader_from_env()
                .context("the correction reads Bronze with the lakehouse reader key")?,
        );
        let mut tables = Vec::with_capacity(ROLES.len());
        for role in ROLES {
            tables.push(self.table(&storage, role).await?);
        }
        Ok(Value::Array(tables))
    }

    async fn table(&self, storage: &R2ObjectStorage, role: Role) -> anyhow::Result<Value> {
        let table = role.table();
        let table_contract = contract(table)?;
        let times = self.source.minute_time_columns(role, table_contract)?;
        let Some(expected_head) = head(self.catalog, table).await? else {
            return Ok(json!({"table": table, "keys_selected": 0}));
        };
        let objects = candidates(&*locked(self.stage)?, table_contract, &times, expected_head)?;
        let mut line = json!({
            "table": table,
            "keys_selected": count(self.stage, "fix_candidates")?,
            "objects_named": objects.len(),
        });
        if objects.is_empty() {
            return Ok(line);
        }
        let prefix = format!("bronze/source={}/", self.source.source_slug(role)?);
        let listed = storage
            .inventory_audit(R2InventoryRequest::new(Some(&prefix), Some(LIST_PAGE))?)
            .await
            .with_context(|| format!("failed to list {prefix}"))?
            .objects()
            .iter()
            .map(|object| object.key.clone())
            .collect::<BTreeSet<_>>();
        let (mut reread, mut gone, mut gone_rows, mut waiting) = (0_u64, 0_u64, 0_u64, 0_u64);
        std::fs::create_dir_all(self.work)?;
        for object_key in &objects {
            if !listed.contains(object_key) {
                gone += 1;
                gone_rows += candidates_of(&*locked(self.stage)?, object_key)?;
                continue;
            }
            if self.started.elapsed() >= self.budget {
                // Not read again inside the run's budget: its rows stay candidates for the next run.
                waiting += 1;
                continue;
            }
            let path = self.work.join("object.gz");
            let bytes = storage
                .get_object_bytes(object_key)
                .await
                .with_context(|| format!("failed to read {object_key}"))?;
            std::fs::write(&path, &bytes)
                .with_context(|| format!("failed to write {}", path.display()))?;
            let planned = PlannedObject {
                object_key: object_key.clone(),
                role,
                source_snapshot_id: snapshot_id(object_key)?,
                path,
            };
            let stage = &*locked(self.stage)?;
            read_object(
                stage,
                self.source,
                table_contract,
                &planned,
                self.ingested_at,
            )?;
            keep_reread(stage, table_contract, &times, object_key)?;
            reread += 1;
        }
        std::fs::remove_dir_all(self.work).ok();
        let record = format!(
            "correction:onbid-kst-minute@{}",
            self.ingested_at.format("%Y%m%dT%H%M%SZ")
        );
        let shape = self.committer.table_shape(table).await?;
        let (found, file) = {
            let stage = &*locked(self.stage)?;
            let found = corrections(stage, table_contract, &times, self.ingested_at)?;
            (found, write(stage, &shape, table, "fix_rows", &record)?)
        };
        line["objects_re_read"] = json!(reread);
        line["objects_gone"] = json!(gone);
        line["rows_whose_object_is_gone"] = json!(gone_rows);
        line["objects_waiting"] = json!(waiting);
        line["rows_found_again"] = json!(found.found_again);
        line["rows_changed"] = json!(found.would_change);
        line["appended"] = json!(match &file {
            Some(file) => {
                append(
                    self.committer,
                    self.catalog,
                    table,
                    Some(file),
                    &record,
                    Some(expected_head),
                )
                .await?
            }
            None => false,
        });
        Ok(line)
    }
}

fn count(stage: &Mutex<DuckDbParquetStage>, table: &str) -> anyhow::Result<u64> {
    let value: i64 = locked(stage)?.connection().query_row(
        &format!("SELECT count(*) FROM {table}"),
        [],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(value)?)
}
