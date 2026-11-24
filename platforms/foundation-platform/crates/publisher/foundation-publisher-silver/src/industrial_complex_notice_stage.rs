//! Stages one Bronze object of ILIS notice detail reads as Silver rows (root ADR-0484).
//!
//! In `DuckDB` inside the publisher (root ADR-0559): it was the Spark job `industryland_or_kr.py`.
//! The collector lands, per run, gzip JSON Lines objects of notice detail reads;
//! `plan-silver-load-objects` fetches the ones the read table does not record yet. For each object,
//! oldest first, `rows.sql` builds a read row per line and an attachment row per file the media
//! store holds, and `changed.sql` keeps the attachments whose (notice, hash) is new or changed
//! against the table's current rows (the contract's rendered rule, root ADR-0564 §4). The caller
//! appends the attachments first, then the reads, each in a commit that records the object, even
//! when it holds no row (`lakehouse_data_commit::append_at_head`).

use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use lakehouse_domain::LakehouseTableContract;
use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, PYTHON_VALUES_SQL};

/// The read record a load's plan is decided from (`notice_detail.read_from`).
pub const DETAIL_TABLE: &str = "silver.industrial_complex_notice_detail";
/// The index of the attachments the media store holds.
pub const ATTACHMENT_TABLE: &str = "silver.industrial_complex_notice_attachment";
/// The plan file `plan-silver-load-objects` writes.
pub const PLAN_SCHEMA: &str = "foundation-platform.silver_load_objects.v1";
/// Columns a load writes, not the source: never compared.
const LOAD_COLUMNS: [&str; 4] = [
    "source_record_id",
    "source_line_number",
    "source_snapshot_id",
    "ingested_at_utc",
];

const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/industryland-or-kr.contract.json");
const ROWS_SQL: &str = include_str!("industrial_complex_notice_stage/rows.sql");
const CHANGED_SQL: &str = include_str!("industrial_complex_notice_stage/changed.sql");

/// What the source contract says about where a read's fields are.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NoticeSource {
    /// The Bronze source slug this load reads (`industrylandorkr__<dataset_slug>`).
    pub source_slug: String,
    result_field: String,
    files_field: String,
    path_field: String,
    name_field: String,
}

/// The source contract compiled into the publisher.
///
/// # Errors
/// Returns an error when the contract lacks a field this load reads.
pub fn notice_source() -> anyhow::Result<NoticeSource> {
    let contract: serde_json::Value = serde_json::from_str(SOURCE_CONTRACT)?;
    let text = |pointer: &str| -> anyhow::Result<String> {
        let value = contract
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("industryland-or-kr.contract.json has no {pointer}"))?;
        ensure!(
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "industryland-or-kr.contract.json {pointer} {value:?} is not a plain field name"
        );
        Ok(value.to_owned())
    };
    let result_pointer = contract
        .pointer("/notice_detail/result_pointer")
        .and_then(serde_json::Value::as_str)
        .context("industryland-or-kr.contract.json has no /notice_detail/result_pointer")?;
    let result_field = result_pointer.trim_matches('/').to_owned();
    ensure!(
        !result_field.is_empty()
            && result_field
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "result_pointer {result_pointer:?} must name one field"
    );
    Ok(NoticeSource {
        source_slug: format!("industrylandorkr__{}", text("/notice_detail/dataset_slug")?),
        result_field,
        files_field: text("/notice_detail/files_field")?,
        path_field: text("/attachments/path_field")?,
        name_field: text("/attachments/name_field")?,
    })
}

/// One Bronze object a plan fetched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedObject {
    /// Its key in the lakehouse bucket: the batch identity both tables record.
    pub object_key: String,
    /// The run id the collector put in the key (`…-20990102t030405z-000.gz` → `20990102T030405Z`).
    pub source_snapshot_id: String,
    /// Where the plan put the object.
    pub path: PathBuf,
}

/// The run id in an object key.
///
/// # Errors
/// Refuses a key that carries none.
pub fn snapshot_id(object_key: &str) -> anyhow::Result<String> {
    let stem = object_key
        .strip_suffix(".gz")
        .with_context(|| format!("{object_key} carries no run id"))?;
    let (rest, part) = stem
        .rsplit_once('-')
        .with_context(|| format!("{object_key} carries no run id"))?;
    let (_, run) = rest
        .rsplit_once('-')
        .with_context(|| format!("{object_key} carries no run id"))?;
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    ensure!(
        part.len() == 3
            && digits(part)
            && run.len() == 16
            && digits(&run[..8])
            && run.as_bytes()[8] == b't'
            && digits(&run[9..15])
            && run.as_bytes()[15] == b'z',
        "{object_key} carries no run id"
    );
    Ok(run.to_ascii_uppercase())
}

/// Every object the plan in `plan_dir` fetched, oldest run first.
///
/// # Errors
/// Refuses another plan format and an object of a source this load does not read.
pub fn planned_objects(
    plan_dir: &Path,
    source: &NoticeSource,
) -> anyhow::Result<Vec<PlannedObject>> {
    let plan: serde_json::Value = serde_json::from_slice(
        &std::fs::read(plan_dir.join("plan.json"))
            .with_context(|| format!("cannot read the plan in {}", plan_dir.display()))?,
    )?;
    ensure!(
        plan["schema_version"] == PLAN_SCHEMA,
        "{}: not a {PLAN_SCHEMA} plan",
        plan_dir.display()
    );
    let mut objects = plan["objects"]
        .as_array()
        .context("the plan lists no objects")?
        .iter()
        .map(|entry| {
            let key = entry["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let slug = entry["source_slug"].as_str().unwrap_or_default();
            ensure!(
                slug == source.source_slug,
                "{key}: {slug} is not a source this load reads"
            );
            let local = entry["local_path"]
                .as_str()
                .context("a planned object has no local path")?;
            ensure!(
                !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
                "{key}: the local path {local:?} leaves the plan directory"
            );
            Ok(PlannedObject {
                object_key: key.to_owned(),
                source_snapshot_id: snapshot_id(key)?,
                path: plan_dir.join(local),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    objects.sort_by(|a, b| {
        (&a.source_snapshot_id, &a.object_key).cmp(&(&b.source_snapshot_id, &b.object_key))
    });
    Ok(objects)
}

/// One object, staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedNotice {
    /// Read rows the object holds.
    pub reads: u64,
    /// Attachment rows it holds (one per notice and file hash).
    pub attachments: u64,
    /// Of those, the ones that are new or changed: what is appended.
    pub changed_attachments: u64,
    /// The read rows as one Parquet file; `None` when there are none.
    pub reads_file: Option<StagedFile>,
    /// The changed attachment rows as one Parquet file; `None` when there are none.
    pub attachments_file: Option<StagedFile>,
}

fn quote(text: &str) -> anyhow::Result<String> {
    ensure!(
        !text.contains('\'') && !text.contains('\\'),
        "{text:?} must not hold a quote"
    );
    Ok(text.to_owned())
}

/// The name a staged file of `table` for `object_key` gets: the same object always names the same
/// file, so a run that died leaves at most one object's files behind (root ADR-0560 §2).
fn file_name(table: &str, object_key: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(object_key.as_bytes()));
    format!(
        "{}-{}",
        table.rsplit('.').next().unwrap_or(table).replace('_', "-"),
        &digest[..24]
    )
}

/// The current-rows query of the attachment table at `head` (or of no rows when the table has no
/// snapshot), over the keys `notice_attachments` names, projecting `columns`.
fn current_attachments(
    connection: &Connection,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    columns: &[&str],
) -> anyhow::Result<String> {
    let Some(head) = head else {
        return Ok(format!(
            "SELECT {} FROM notice_attachments WHERE false",
            columns.join(", ")
        ));
    };
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{ATTACHMENT_TABLE} keeps no versions per key"))?;
    let pinned = crate::duckdb_parquet_stage::table_at(ATTACHMENT_TABLE, head);
    Ok(latest.current_rows_sql(
        &with_contract_columns(connection, contract, &pinned)?,
        columns,
        "notice_sequence IN (SELECT notice_sequence FROM notice_attachments)",
    ))
}

/// Builds the rows of `object` and stages them; the attachments are compared with the table's
/// current rows at `attachment_head` (the snapshot the append will be pinned on).
///
/// # Errors
/// Refuses a line the Spark load would have stopped on (a body or an attachment that is not an
/// object), a value too large for its column, and a `DuckDB` failure.
pub fn stage_object(
    stage: &DuckDbParquetStage,
    source: &NoticeSource,
    shapes: (&TableShape, &TableShape),
    attachment_contract: &LakehouseTableContract,
    object: &PlannedObject,
    attachment_head: Option<i64>,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<StagedNotice> {
    let (reads_shape, attachments_shape) = shapes;
    stage.load_lines(&object.path, "notice_lines")?;
    let sql = ROWS_SQL
        .replace("{result_field}", &source.result_field)
        .replace("{files_field}", &source.files_field)
        .replace("{path_field}", &source.path_field)
        .replace("{name_field}", &source.name_field)
        .replace("{object_key}", &quote(&object.object_key)?)
        .replace("{snapshot_id}", &quote(&object.source_snapshot_id)?)
        .replace(
            "{ingested_at_epoch_seconds}",
            &ingested_at.timestamp().to_string(),
        );
    let connection = stage.connection();
    connection
        .execute_batch(PYTHON_VALUES_SQL)
        .context("DuckDB refused the Python value readers")?;
    connection
        .execute_batch(&sql)
        .with_context(|| format!("DuckDB failed to read {}", object.object_key))?;

    let key = attachment_contract
        .current_row_latest()
        .with_context(|| format!("{ATTACHMENT_TABLE} keeps no versions per key"))?
        .key;
    let compared = attachment_contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !LOAD_COLUMNS.contains(name))
        .collect::<Vec<_>>();
    ensure!(
        key.iter().all(|column| compared.contains(column)),
        "{ATTACHMENT_TABLE}'s key must be among its columns"
    );
    let same_row = compared
        .iter()
        .map(|column| format!("c.{column} IS NOT DISTINCT FROM a.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    connection
        .execute_batch(
            &CHANGED_SQL
                .replace(
                    "{current}",
                    &current_attachments(
                        connection,
                        attachment_contract,
                        attachment_head,
                        &compared,
                    )?,
                )
                .replace("{matches}", &same_row),
        )
        .with_context(|| {
            format!(
                "DuckDB failed to compare the attachments of {}",
                object.object_key
            )
        })?;

    let count = |table: &str| -> anyhow::Result<u64> {
        let value: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        Ok(u64::try_from(value)?)
    };
    let (reads, attachments, changed_attachments) = (
        count("notice_reads")?,
        count("notice_attachments")?,
        count("notice_attachments_changed")?,
    );
    let write = |rows: u64, shape: &TableShape, table: &str, query: &str| {
        if rows == 0 {
            return Ok(None);
        }
        stage
            .write(shape, query, &file_name(table, &object.object_key))
            .map(Some)
    };
    Ok(StagedNotice {
        reads,
        attachments,
        changed_attachments,
        reads_file: write(
            reads,
            reads_shape,
            DETAIL_TABLE,
            "SELECT * FROM notice_reads",
        )?,
        attachments_file: write(
            changed_attachments,
            attachments_shape,
            ATTACHMENT_TABLE,
            "SELECT * FROM notice_attachments_changed",
        )?,
    })
}

/// Rejects a plan that names one object twice: the batch records would be ambiguous.
///
/// # Errors
/// When a key repeats.
pub fn refuse_repeated(objects: &[PlannedObject]) -> anyhow::Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for object in objects {
        if !seen.insert(&object.object_key) {
            bail!("the plan names {} twice", object.object_key);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
