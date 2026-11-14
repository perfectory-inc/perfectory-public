//! The write path of a Gold of appended row versions (root ADR-0564 §3): plan from the newest commit
//! that records `foundation.version-seq`, diff the recomputed keys against the head's current rows,
//! check, append only the keys that changed (a removal marker for a key that left) while `main` is
//! still the head the diff read, and pin what the next run plans from.
//!
//! The table's producer computes its rows into a `DuckDB` table (`built`: the replaced table's
//! columns, one row per key) for the keys it recomputed; everything after that is here, so every
//! versions table is written the same way. No Spark session.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, HeadMoved, LakehouseDataCommitter, StagedFile,
};
use foundation_publisher_silver::duckdb::Connection;
use foundation_publisher_silver::duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG};
use lakehouse_domain::LakehouseTableContract;
use lakehouse_infrastructure::{IcebergRestCatalog, IcebergSnapshotSummary};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// The table's commit number (root ADR-0564 §2).
pub(crate) const VERSION_SEQ_PROPERTY: &str = "foundation.version-seq";
/// The current rows after the commit.
pub(crate) const CURRENT_RECORDS_PROPERTY: &str = "foundation.current-records";
/// The input pins, as `gold_rebuild.pins_property_value` writes them.
pub(crate) const SOURCE_SNAPSHOTS_PROPERTY: &str = "foundation.source-iceberg-snapshots";
/// The producer's code version (`gold_code_version.PROPERTY`).
pub(crate) const CODE_VERSION_PROPERTY: &str = "foundation.build-source-sha256";
/// The served parcel edition the rows were built from (`gold_rebuild.SERVED_EDITION_PROPERTY`).
pub(crate) const SERVED_EDITION_PROPERTY: &str = "foundation.served-parcel-edition";
/// The current rows' fingerprint (`gold_fingerprint.PROPERTY`).
pub(crate) const FINGERPRINT_PROPERTY: &str = "foundation.content-fingerprint";
/// The keys this commit appended (`gold_incremental.KEYS_CHANGED_PROPERTY`).
pub(crate) const KEYS_CHANGED_PROPERTY: &str = "foundation.keys-changed";
/// The tag on the commit the next run plans from (`gold_incremental.BUILD_TAG`, root ADR-0338).
pub(crate) const BUILD_TAG: &str = "foundation-gold-build";
/// The tag that keeps, on each input, the snapshot the table was last built from
/// (`gold_incremental.INPUT_TAG_PREFIX`).
pub(crate) const INPUT_TAG_PREFIX: &str = "foundation-gold-input-";
/// `gold_fingerprint.HEX_DIGITS`.
const FINGERPRINT_HEX_DIGITS: usize = 15;

/// The commit the next run plans from: the newest in `main`'s ancestry that records the version
/// sequence; only compactions follow it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Base {
    /// Its snapshot.
    pub snapshot_id: i64,
    /// Its `foundation.version-seq`.
    pub version_seq: i64,
    /// The input pins it records.
    pub pins: BTreeMap<String, i64>,
    /// Its code version.
    pub code_version: String,
    /// The served parcel edition it was built from, if it records one.
    pub served_edition: Option<String>,
    /// The current rows after it.
    pub current_records: u64,
    /// The fingerprint of the current rows after it: (rows, digest sum).
    pub fingerprint: (u64, u128),
}

/// `rows:<count>;digest_sum:<sum>`.
fn parse_fingerprint(text: &str) -> anyhow::Result<(u64, u128)> {
    let (rows, sum) = text
        .strip_prefix("rows:")
        .and_then(|rest| rest.split_once(";digest_sum:"))
        .with_context(|| format!("{text:?} is not a content fingerprint"))?;
    Ok((rows.parse()?, sum.parse()?))
}

pub(crate) fn fingerprint_text((rows, sum): (u64, u128)) -> String {
    format!("rows:{rows};digest_sum:{sum}")
}

/// The base of `history` (`main`'s lineage, newest first), or `None` for a table with no row yet.
///
/// # Errors
/// Refuses a table that holds rows and no commit that records the version sequence: its order
/// would start again at 1 (root ADR-0564 §3-1).
pub(crate) fn base(history: &[IcebergSnapshotSummary]) -> anyhow::Result<Option<Base>> {
    for snapshot in history {
        let Some(sequence) = snapshot.summary.get(VERSION_SEQ_PROPERTY) else {
            continue;
        };
        let summary = &snapshot.summary;
        let required = |key: &str| {
            summary.get(key).with_context(|| {
                format!(
                    "snapshot {} records {VERSION_SEQ_PROPERTY} and no {key}",
                    snapshot.snapshot_id
                )
            })
        };
        return Ok(Some(Base {
            snapshot_id: snapshot.snapshot_id,
            version_seq: sequence
                .parse()
                .context("foundation.version-seq is not a number")?,
            pins: snapshot.source_snapshots(),
            code_version: required(CODE_VERSION_PROPERTY)?.clone(),
            served_edition: summary.get(SERVED_EDITION_PROPERTY).cloned(),
            current_records: required(CURRENT_RECORDS_PROPERTY)?.parse()?,
            fingerprint: parse_fingerprint(required(FINGERPRINT_PROPERTY)?)?,
        }));
    }
    let rows = history
        .first()
        .and_then(|head| head.summary.get("total-records"))
        .map_or(Ok(0), |text| text.parse::<u64>())?;
    ensure!(
        rows == 0,
        "the table holds {rows} rows and no commit in main's ancestry records {VERSION_SEQ_PROPERTY}: \
         its version order cannot be continued"
    );
    Ok(None)
}

/// The run's identity: the table, its inputs, the code and the edition. A rerun of the same build
/// names the same batch, so the append's re-run guard refuses a second copy.
pub(crate) fn run_id(
    table: &str,
    pins: &BTreeMap<String, i64>,
    code_version: &str,
    served_edition: &str,
) -> String {
    let mut hash = Sha256::new();
    hash.update(table.as_bytes());
    hash.update([0]);
    for (input, snapshot) in pins {
        hash.update(format!("{input}={snapshot};").as_bytes());
    }
    hash.update([0]);
    hash.update(code_version.as_bytes());
    hash.update([0]);
    hash.update(served_edition.as_bytes());
    let digest = format!("{:x}", hash.finalize());
    format!("{}-{}", table.replace('.', "-"), &digest[..40])
}

/// The pins as the summary property holds them: a JSON object of table to snapshot id text,
/// sorted, without spaces (`gold_rebuild.pins_property_value`).
pub(crate) fn pins_property(pins: &BTreeMap<String, i64>) -> anyhow::Result<String> {
    Ok(serde_json::to_string(
        &pins
            .iter()
            .map(|(table, snapshot)| (table.clone(), snapshot.to_string()))
            .collect::<BTreeMap<_, _>>(),
    )?)
}

/// What one commit appends: the diff of `built` against the head's current rows.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub(crate) struct Diff {
    /// Keys `built` holds.
    pub built: u64,
    /// Keys new to the table.
    pub added: u64,
    /// Keys whose content changed.
    pub changed: u64,
    /// Keys the recomputed range no longer holds (a removal marker each).
    pub removed: u64,
    /// The head's current rows in the recomputed range.
    pub current_in_range: u64,
}

impl Diff {
    pub(crate) const fn keys(&self) -> u64 {
        self.added + self.changed + self.removed
    }
}

/// How a run's rows reach the table.
pub(crate) struct Commit<'a> {
    pub table: &'a str,
    pub versions: &'a LakehouseTableContract,
    /// The replaced table's columns: what `built` holds, in the table's order.
    pub content_columns: Vec<&'static str>,
    /// The key (the first content column).
    pub key: &'static str,
    /// `main`'s head the diff reads (`None`: the table has no snapshot).
    pub head: Option<i64>,
    pub base: Option<Base>,
    pub version_seq: i64,
    /// The stamps a removal marker carries (this commit's).
    pub source_snapshot_id: String,
    pub published_at_utc: String,
}

/// Diffs `built` against the current rows of the head among the keys `range` keeps (a predicate
/// on the key column) and leaves the rows to append in `appended`.
///
/// # Errors
/// Refuses two current rows of one key at the head (the SQL rule keeps ties; the write path must
/// not, root ADR-0564 §4) and a key `built` holds twice.
pub(crate) fn diff(
    connection: &Connection,
    commit: &Commit<'_>,
    range: &str,
) -> anyhow::Result<Diff> {
    let key = commit.key;
    let content = commit.content_columns.join(", ");
    let current = match commit.head {
        Some(head) => commit
            .versions
            .current_row_latest()
            .context("the versions table has no current-row rule")?
            .current_rows_sql(
                &format!(
                    "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
                    commit.table
                ),
                &commit.content_columns,
                range,
            ),
        None => format!("SELECT {content} FROM built WHERE FALSE"),
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE current_rows AS {current};"
    ))?;
    let count = |sql: &str| -> anyhow::Result<u64> {
        let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    };
    let (rows, keys) = (
        count("SELECT count(*) FROM current_rows")?,
        count(&format!("SELECT count(DISTINCT {key}) FROM current_rows"))?,
    );
    ensure!(
        rows == keys,
        "the head holds {} current rows tied on version_seq",
        rows - keys
    );
    let (built, built_keys) = (
        count("SELECT count(*) FROM built")?,
        count(&format!("SELECT count(DISTINCT {key}) FROM built"))?,
    );
    ensure!(
        built == built_keys,
        "the build holds {} keys more than once",
        built - built_keys
    );
    connection.execute_batch(&appended_sql(commit)?)?;
    Ok(Diff {
        built,
        added: count("SELECT count(*) FROM appended WHERE change = 'added'")?,
        changed: count("SELECT count(*) FROM appended WHERE change = 'changed'")?,
        removed: count("SELECT count(*) FROM appended WHERE change = 'removed'")?,
        current_in_range: rows,
    })
}

/// The statement that leaves in `appended` the rows `built` adds over `current_rows`: a new or
/// changed key's row with this commit's `version_seq`, and for a key `built` no longer holds its
/// last current row, marked `removed` and stamped with this commit's lineage (root ADR-0564 §2).
pub(crate) fn appended_sql(commit: &Commit<'_>) -> anyhow::Result<String> {
    let key = commit.key;
    let stamps = [&commit.source_snapshot_id, &commit.published_at_utc];
    ensure!(
        stamps.iter().all(|stamp| !stamp.contains(['\'', '\\'])),
        "a stamp holds a quote"
    );
    let removed_columns = commit
        .content_columns
        .iter()
        .map(|column| match *column {
            "source_snapshot_id" => {
                format!("'{}' AS source_snapshot_id", commit.source_snapshot_id)
            }
            "published_at_utc" => format!("'{}' AS published_at_utc", commit.published_at_utc),
            other => format!("c.{other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let built_columns = commit
        .content_columns
        .iter()
        .map(|column| format!("b.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sequence = commit.version_seq;
    Ok(format!(
        "CREATE OR REPLACE TABLE appended AS \
         SELECT {built_columns}, CAST({sequence} AS BIGINT) AS version_seq, CAST(NULL AS VARCHAR) AS row_state, \
                CASE WHEN c.{key} IS NULL THEN 'added' ELSE 'changed' END AS change \
         FROM built b LEFT JOIN current_rows c USING ({key}) \
         WHERE c.{key} IS NULL OR c.row_digest IS DISTINCT FROM b.row_digest \
         UNION ALL \
         SELECT {removed_columns}, CAST({sequence} AS BIGINT) AS version_seq, 'removed' AS row_state, 'removed' AS change \
         FROM current_rows c ANTI JOIN built b USING ({key});"
    ))
}

/// The fingerprint and the current rows after the commit, from the base's and the rows the commit
/// adds and hides (root ADR-0564 §5: the fingerprint sums, so it is updated by adding and
/// subtracting).
pub(crate) fn after(
    connection: &Connection,
    commit: &Commit<'_>,
    diff: &Diff,
) -> anyhow::Result<((u64, u128), u64)> {
    let key = commit.key;
    let number =
        format!("CAST(('0x' || substr(row_digest, 1, {FINGERPRINT_HEX_DIGITS})) AS HUGEINT)");
    let sum = |sql: &str| -> anyhow::Result<u128> {
        let value: String = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(value.parse()?)
    };
    let added = sum(&format!(
        "SELECT CAST(coalesce(sum({number}), 0) AS VARCHAR) FROM appended WHERE change <> 'removed'"
    ))?;
    let hidden = sum(&format!(
        "SELECT CAST(coalesce(sum({number}), 0) AS VARCHAR) FROM current_rows \
         WHERE {key} IN (SELECT {key} FROM appended)"
    ))?;
    let (rows, total, current) = commit.base.as_ref().map_or((0, 0, 0), |base| {
        (base.fingerprint.0, base.fingerprint.1, base.current_records)
    });
    ensure!(
        rows == current,
        "the base records {current} current rows and a fingerprint of {rows}"
    );
    let rows = (rows + diff.added)
        .checked_sub(diff.removed)
        .context("the commit removes more keys than the table holds")?;
    let total = (total + added)
        .checked_sub(hidden)
        .context("the commit hides digests the base fingerprint does not hold")?;
    Ok(((rows, total), rows))
}

/// Stages `appended` as one file per 시도 (the table's partition, root ADR-0564 §2), each sorted by
/// the key.
pub(crate) fn stage(
    stage: &DuckDbParquetStage,
    shape: &foundation_publisher_core::lakehouse_data_commit::TableShape,
    run: &str,
    key: &str,
) -> anyhow::Result<Vec<StagedFile>> {
    let connection = stage.connection();
    let prefixes = connection
        .prepare(&format!(
            "SELECT DISTINCT substr({key}, 1, 2) FROM appended ORDER BY 1"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    prefixes
        .iter()
        .map(|prefix| {
            ensure!(
                prefix.bytes().all(|byte| byte.is_ascii_digit()),
                "a key starts with {prefix:?}, not a 시도"
            );
            stage.write(
                shape,
                &format!(
                    "SELECT * EXCLUDE (change) FROM appended WHERE substr({key}, 1, 2) = '{prefix}' ORDER BY {key}"
                ),
                &format!("{run}-{prefix}"),
            )
        })
        .collect()
}

/// Appends the staged files while `main` is still the diff's head. A response lost after the
/// commit landed is read back from the table: the head then records this run's batch.
pub(crate) async fn append(
    committer: &LakehouseDataCommitter,
    catalog: &IcebergRestCatalog,
    commit: &Commit<'_>,
    files: &[StagedFile],
    run: &str,
    summary: BTreeMap<String, String>,
) -> anyhow::Result<i64> {
    let already_ingested = catalog
        .load_ingested_batch_objects(commit.table)
        .await?
        .with_context(|| format!("{} does not exist", commit.table))?;
    let batch = [run.to_owned()];
    let outcome = committer
        .append_at_head(
            DataAppend {
                table_name: commit.table,
                contract: commit.versions,
                files,
                batch: &batch,
                already_ingested: &already_ingested,
                summary,
            },
            commit.head,
        )
        .await;
    match outcome {
        Ok(AppendOutcome::Appended { snapshot, .. }) => Ok(snapshot.snapshot_id),
        Ok(AppendOutcome::AlreadyIngested) => head_holding(catalog, commit.table, run).await,
        Err(error) if error.downcast_ref::<HeadMoved>().is_some() => {
            // Our own commit, whose answer was lost, moved the head: the table is the record.
            match head_holding(catalog, commit.table, run).await {
                Ok(snapshot) => Ok(snapshot),
                Err(_) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// The snapshot of `main`'s lineage that records batch `run`.
async fn head_holding(catalog: &IcebergRestCatalog, table: &str, run: &str) -> anyhow::Result<i64> {
    catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .iter()
        .find(|snapshot| {
            snapshot
                .ingested_objects()
                .iter()
                .any(|object| object == run)
        })
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| format!("no snapshot of {table} records batch {run}"))
}

/// Moves `tag` of `table` to `snapshot`: a tag is never moved in place (`create_tag`), so the old
/// one is removed while it still names its snapshot, then the new one made.
async fn move_tag(
    catalog: &IcebergRestCatalog,
    table: &str,
    tag: &str,
    snapshot: i64,
) -> anyhow::Result<()> {
    let refs = catalog
        .load_snapshot_refs(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    match refs.refs.get(tag) {
        Some(existing) if existing.kind == "tag" && existing.snapshot_id == snapshot => {
            return Ok(())
        }
        Some(existing) if existing.kind == "tag" => {
            catalog.remove_tag(table, tag, existing.snapshot_id).await?;
        }
        Some(existing) => bail!("{table} has a {} named {tag}", existing.kind),
        None => {}
    }
    catalog.create_tag(table, tag, snapshot).await?;
    Ok(())
}

/// Pins what the next run plans from (root ADR-0564 §3-8): the build tag on `build`, the input tag
/// on each input's pin. Every run does it after its plan as well, so a run that died between its
/// commit and its tags is mended by the next, even one with nothing to do.
pub(crate) async fn pin(
    catalog: &IcebergRestCatalog,
    table: &str,
    build: i64,
    pins: &BTreeMap<String, i64>,
) -> anyhow::Result<Value> {
    move_tag(catalog, table, BUILD_TAG, build).await?;
    let input_tag = format!("{INPUT_TAG_PREFIX}{}", table.replace('.', "-"));
    for (input, snapshot) in pins {
        move_tag(catalog, input, &input_tag, *snapshot).await?;
    }
    Ok(json!({"build": build, "input_tag": input_tag, "inputs": pins.len()}))
}

/// The inputs whose pin moved from `base`'s, and every input `base` does not pin.
pub(crate) fn moved(base: &Base, pins: &BTreeMap<String, i64>) -> BTreeSet<String> {
    pins.iter()
        .filter(|(input, snapshot)| base.pins.get(*input) != Some(*snapshot))
        .map(|(input, _)| input.clone())
        .collect()
}

#[cfg(test)]
mod tests;
