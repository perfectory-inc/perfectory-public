//! Stages one Bronze object of industrial land sale notice reads as Silver rows (root ADR-0603).
//!
//! In `DuckDB` inside the publisher (root ADR-0559), as the ILIS notice stage does. The collector
//! lands, per source and run, gzip JSON Lines objects of notice detail reads under content-qualified
//! keys (root ADR-0152); `plan-silver-load-objects` fetches the ones the read table does not record
//! yet. For each object, oldest run first, `rows.sql` builds a read row per line and an attachment
//! row per file the media store holds, and `changed.sql` keeps the attachments whose (source,
//! notice, hash) is new or changed against the table's current rows (the contract's rendered rule,
//! root ADR-0564 §4). The caller appends the attachments first, then the reads, each in a commit
//! that records the object, even when it holds no row (`lakehouse_data_commit::append_at_head`).

use std::path::Path;

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use lakehouse_domain::LakehouseTableContract;
use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, PYTHON_VALUES_SQL};
pub use crate::industrial_complex_notice_stage::{refuse_repeated, PlannedObject, PLAN_SCHEMA};

/// The read record a load's plan is decided from (the lane contract's `read_from`).
pub const READ_TABLE: &str = "silver.industrial_land_sale_notice_read";
/// The index of the attachments the media store holds.
pub const ATTACHMENT_TABLE: &str = "silver.industrial_land_sale_notice_attachment";
/// Columns a load writes, not the source: never compared.
const LOAD_COLUMNS: [&str; 4] = [
    "source_record_id",
    "source_line_number",
    "source_snapshot_id",
    "ingested_at_utc",
];

const LANE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/industrial-land-sale-notice.contract.json");
const ROWS_SQL: &str = include_str!("industrial_land_sale_notice_stage/rows.sql");
const CHANGED_SQL: &str = include_str!("industrial_land_sale_notice_stage/changed.sql");

/// The Bronze source slugs of the detail objects this load reads: one per source of the lane
/// contract, `<provider id>__<detail_dataset_slug>`, in the contract's order.
///
/// # Errors
/// Returns an error when the contract lacks a source's provider or dataset, or names a provider
/// outside the approved domains.
pub fn detail_source_slugs() -> anyhow::Result<Vec<String>> {
    let contract: serde_json::Value = serde_json::from_str(LANE_CONTRACT)
        .context("industrial-land-sale-notice.contract.json is not valid")?;
    let sources = contract["sources"]
        .as_object()
        .context("industrial-land-sale-notice.contract.json names no sources")?;
    sources
        .iter()
        .map(|(name, source)| {
            let provider = source["provider"]
                .as_str()
                .with_context(|| format!("source {name} names no provider"))?;
            let dataset = source["detail_dataset_slug"]
                .as_str()
                .with_context(|| format!("source {name} names no detail_dataset_slug"))?;
            collection_domain::source_slug(provider, dataset)
                .with_context(|| format!("source {name} has no canonical slug"))
        })
        .collect()
}

/// The run id in a content-qualified detail object key
/// (`…-20990102t030405z-000--sha256-<hex>.gz` → `20990102T030405Z`).
///
/// # Errors
/// Refuses a key that carries no content hash or no run id.
pub fn snapshot_id(object_key: &str) -> anyhow::Result<String> {
    ensure!(
        collection_domain::bronze_content_object_key_checksum(object_key).is_some(),
        "{object_key} is not content-qualified (root ADR-0152)"
    );
    let (stem, extension) = object_key
        .rsplit_once('.')
        .with_context(|| format!("{object_key} has no extension"))?;
    let (base, _) = stem
        .rsplit_once(collection_domain::BRONZE_CONTENT_KEY_SEPARATOR)
        .with_context(|| format!("{object_key} carries no content hash"))?;
    crate::industrial_complex_notice_stage::snapshot_id(&format!("{base}.{extension}"))
}

/// Every object the plan in `plan_dir` fetched, oldest run first.
///
/// # Errors
/// Refuses another plan format, an object of a source this load does not read, and a local path
/// that leaves the plan directory.
pub fn planned_objects(plan_dir: &Path, slugs: &[String]) -> anyhow::Result<Vec<PlannedObject>> {
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
                slugs.iter().any(|known| known == slug),
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
pub struct StagedSaleNotices {
    /// Read rows the object holds.
    pub reads: u64,
    /// Attachment rows it holds (one per source, notice and file hash).
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
/// snapshot), over the notices `sale_attachments` names, projecting `columns`.
fn current_attachments(
    connection: &Connection,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    columns: &[&str],
) -> anyhow::Result<String> {
    let Some(head) = head else {
        return Ok(format!(
            "SELECT {} FROM sale_attachments WHERE false",
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
        "(notice_source, notice_id) IN (SELECT notice_source, notice_id FROM sale_attachments)",
    ))
}

/// Builds the rows of `object` and stages them; the attachments are compared with the table's
/// current rows at `attachment_head` (the snapshot the append will be pinned on).
///
/// # Errors
/// Refuses a line the collector does not write (no source, id, fingerprint or time; attachments
/// that are not a list of objects), a value too large for its column, and a `DuckDB` failure.
pub fn stage_object(
    stage: &DuckDbParquetStage,
    shapes: (&TableShape, &TableShape),
    attachment_contract: &LakehouseTableContract,
    object: &PlannedObject,
    attachment_head: Option<i64>,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<StagedSaleNotices> {
    let (reads_shape, attachments_shape) = shapes;
    stage.load_lines(&object.path, "sale_lines")?;
    let sql = ROWS_SQL
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
        count("sale_reads")?,
        count("sale_attachments")?,
        count("sale_attachments_changed")?,
    );
    let write = |rows: u64, shape: &TableShape, table: &str, query: &str| {
        if rows == 0 {
            return Ok(None);
        }
        stage
            .write(shape, query, &file_name(table, &object.object_key))
            .map(Some)
    };
    Ok(StagedSaleNotices {
        reads,
        attachments,
        changed_attachments,
        reads_file: write(reads, reads_shape, READ_TABLE, "SELECT * FROM sale_reads")?,
        attachments_file: write(
            changed_attachments,
            attachments_shape,
            ATTACHMENT_TABLE,
            "SELECT * FROM sale_attachments_changed",
        )?,
    })
}

#[cfg(test)]
mod tests;
