//! The publisher's build of the steward queue as a table of appended row versions (root ADR-0564
//! §6-9, ADR-0113 §10, ADR-0483 §5): `build-lineage-review-queue-versions`.
//!
//! `DuckDB` inside the publisher computes the queue's two units from their inputs
//! (`foundation_publisher_silver::lineage_review_queue_stage`): the parcel unit from
//! `silver.parcel_lineage`, the place reference unit from every parcel source Silver the resolver
//! placed. The publisher appends the keys that changed (`gold_versions_commit`, keyed by
//! `(unit, subject_code)` and partitioned by `unit`). No Spark session.
//!
//! Partial as the Spark jobs it replaces were: the parcel unit is rebuilt when the lineage moved
//! (`lineage_review_queue_to_gold` rewrote it whole then), a source's place reference items when
//! that source moved (`place_reference_queue_to_gold`'s per-source `MERGE`), and everything when
//! the code moved. A place reference item whose evidence did not change keeps its row, as the
//! `MERGE` kept it (`WHEN MATCHED AND t.evidence_etag <> v.evidence_etag`): its snapshot pair stays
//! the one it was first found under.
//!
//! The steward cycle (`scripts/ops/lineage-stewardship-cycle.sh`) runs it after it folds the
//! stewards' decisions, at the heads of its inputs, and loads the steward database from the handoff
//! files it writes: one per unit (`<unit>.json`, `stewardship_domain::handoff`, the type the load
//! parses), from the table's current rows after the commit (root ADR-0564 §6-9, the takeover). It
//! took the queue over from the Spark jobs `lineage_review_queue_to_gold` and
//! `place_reference_queue_to_gold` on the verdict `takeover_equal` of 2026-11-25 (651 items of 시도
//! 47 and 22,580 nationally, the same on both engines). A run before the lineage exists waits.
//!
//! Of root ADR-0564 §3-4's checks it keeps the ones that hold of a queue: one row per key (the diff)
//! and each unit's statuses. It has no floor on lost rows and no bound on changed keys: an item
//! leaves when a steward decides it or stronger evidence closes it, so the queue may empty in one
//! run, and the Spark jobs it replaces had neither.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb::params,
    duckdb_parquet_stage::{
        with_contract_columns, DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG,
    },
    kind_versions_stage::ensure_no_commit_tables,
    lineage_review_queue_stage::{
        self as queue, load_review_lineage, load_unresolved, parcel_items, place_reference_items,
        QueueItem, LINEAGE_SOURCE, PARCEL_UNIT, PLACE_REFERENCE_UNIT,
    },
    parcel_nearby_stage::{NearbyPins, Source},
    process_memory,
};
use lakehouse_domain::{GOLD_VERSIONS_REMOVAL_COLUMN, GOLD_VERSION_SEQ_COLUMN};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};
use stewardship_domain::handoff::{
    parse_handoff, HandoffItem, ReviewHandoff, HANDOFF_SCHEMA_VERSION,
};

use crate::{
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY,
        VERSION_SEQ_PROPERTY,
    },
    gold_versions_support::{contract, write_summary},
    public_data_control_support::required_env_value,
};

const TABLE: &str = "gold.lineage_review_queue_versions";
/// The table the Spark jobs wrote: its columns are the queue's (the versions table adds its own).
const REPLACED: &str = "gold.lineage_review_queue";
const JOB: &str = "build-lineage-review-queue-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
/// Where the handoff files go, one per unit; unset, none is written.
const HANDOFF_DIR_ENV: &str = "FOUNDATION_PLATFORM_LINEAGE_REVIEW_HANDOFF_DIR";
/// The queue's columns a handoff carries, in the steward database's order.
const HANDOFF_COLUMNS: &str =
    "unit, subject_code, item_id, status, candidates_json, evidence_etag, from_snapshot_id, to_snapshot_id";

/// `fresh`: the items this run computed, before the place reference rule.
const FRESH_TABLE: &str = "CREATE OR REPLACE TABLE fresh (unit VARCHAR, subject_code VARCHAR, \
    item_id VARCHAR, status VARCHAR, candidates_json VARCHAR, evidence_etag VARCHAR, \
    from_snapshot_id VARCHAR, to_snapshot_id VARCHAR);";

/// `built` from `fresh` and the head's current rows (`head_rows`): a place reference item whose
/// evidence did not change keeps the snapshot pair it has. `row_digest` (the column the versions
/// table adds, root ADR-0564 §2) is the SHA-256 of `to_json` of the content but the stamp.
const BUILT_SQL: &str = "CREATE OR REPLACE TABLE built AS \
    WITH kept AS ( \
      SELECT f.unit, f.subject_code, f.item_id, f.status, f.candidates_json, f.evidence_etag, \
        CASE WHEN f.unit = '@place@' AND h.evidence_etag = f.evidence_etag \
          THEN h.from_snapshot_id ELSE f.from_snapshot_id END AS from_snapshot_id, \
        CASE WHEN f.unit = '@place@' AND h.evidence_etag = f.evidence_etag \
          THEN h.to_snapshot_id ELSE f.to_snapshot_id END AS to_snapshot_id \
      FROM fresh f LEFT JOIN head_rows h USING (unit, subject_code)) \
    SELECT *, CAST('@published_at_utc@' AS TIMESTAMPTZ) AS published_at_utc, \
      sha256(to_json({'unit': unit, 'subject_code': subject_code, 'item_id': item_id, \
        'status': status, 'candidates_json': candidates_json, 'evidence_etag': evidence_etag, \
        'from_snapshot_id': from_snapshot_id, 'to_snapshot_id': to_snapshot_id})::VARCHAR) \
        AS row_digest \
    FROM kept;";

/// The head of every input of the queue (`lineage_review_queue_stage::inputs`), or `None` while the
/// lineage does not exist yet (nothing to review). A place reference source that does not exist yet
/// has no unresolved rows.
async fn input_heads(
    catalog: &IcebergRestCatalog,
) -> anyhow::Result<Option<BTreeMap<String, i64>>> {
    let mut heads = BTreeMap::new();
    for input in queue::inputs() {
        let head = catalog
            .load_main_snapshot_history(input)
            .await?
            .and_then(|history| history.first().map(|snapshot| snapshot.snapshot_id));
        match head {
            Some(head) => {
                heads.insert(input.to_owned(), head);
            }
            None if input == LINEAGE_SOURCE => return Ok(None),
            None => {}
        }
    }
    Ok(Some(heads))
}

/// Writes the queue's current rows (`current`, a query over the queue's columns) into `dir`, one
/// handoff per unit (`<unit>.json`, both units always: a load replaces the unit's items), each held
/// to the load's own checks (`parse_handoff`) before it is written; returns what the cycle reports.
fn write_handoffs(
    connection: &foundation_publisher_silver::duckdb::Connection,
    current: &str,
    dir: &Path,
    published_at_utc: &str,
    parcel_counts: &BTreeMap<String, u64>,
) -> anyhow::Result<Value> {
    let published =
        chrono::DateTime::parse_from_rfc3339(published_at_utc)?.with_timezone(&chrono::Utc);
    let mut units: BTreeMap<String, Vec<HandoffItem>> = [PARCEL_UNIT, PLACE_REFERENCE_UNIT]
        .into_iter()
        .map(|unit| (unit.to_owned(), Vec::new()))
        .collect();
    let mut by_status: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut open_by_sido: BTreeMap<String, u64> = BTreeMap::new();
    let mut statement = connection.prepare(&format!(
        "SELECT {HANDOFF_COLUMNS} FROM ({current}) ORDER BY unit, subject_code"
    ))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    })?;
    for row in rows {
        let (unit, subject_code, item_id, status, candidates_json, evidence_etag, from, to) = row?;
        *by_status.entry((unit.clone(), status.clone())).or_default() += 1;
        if unit == PARCEL_UNIT {
            *open_by_sido
                .entry(subject_code.chars().take(2).collect())
                .or_default() += 1;
        }
        let items = units
            .get_mut(&unit)
            .with_context(|| format!("{TABLE} holds an item of the unit {unit:?}"))?;
        items.push(HandoffItem {
            item_id: item_id
                .parse()
                .with_context(|| format!("item {item_id:?} is not a UUID"))?,
            subject_code,
            status: serde_json::from_value(Value::String(status))?,
            candidates_json,
            evidence_etag,
            from_snapshot_id: from.filter(|snapshot| !snapshot.is_empty()),
            to_snapshot_id: to.filter(|snapshot| !snapshot.is_empty()),
            evidence_json: None,
        });
    }
    // Every unit is held to the load's checks before any file is written.
    let mut written = BTreeMap::new();
    let mut files = Vec::new();
    for (unit, items) in units {
        let handoff = ReviewHandoff {
            schema_version: HANDOFF_SCHEMA_VERSION.to_owned(),
            unit: unit.clone(),
            published_at_utc: published,
            item_count: items.len(),
            items,
        };
        let text = serde_json::to_string(&handoff)?;
        parse_handoff(&text)
            .map_err(|error| anyhow::anyhow!("the {unit} handoff is refused: {error}"))?;
        written.insert(unit.clone(), handoff.item_count);
        files.push((dir.join(format!("{unit}.json")), text));
    }
    std::fs::create_dir_all(dir)?;
    for (path, text) in files {
        let path: PathBuf = path;
        std::fs::write(&path, text).with_context(|| format!("cannot write {}", path.display()))?;
    }
    let count = |unit: &str, status: &str| {
        by_status
            .get(&(unit.to_owned(), status.to_owned()))
            .copied()
            .unwrap_or_default()
    };
    Ok(json!({
        "handoff_items": written,
        "open": written.get(PARCEL_UNIT).copied().unwrap_or_default(),
        "open_needs_review": count(PARCEL_UNIT, "needs_review"),
        "open_pending": count(PARCEL_UNIT, "pending"),
        "open_by_sido": open_by_sido,
        "reopened_by_new_evidence": parcel_counts.get("reopened_by_new_evidence").copied().unwrap_or_default(),
        "place_reference_open": written.get(PLACE_REFERENCE_UNIT).copied().unwrap_or_default(),
    }))
}

fn insert(
    connection: &foundation_publisher_silver::duckdb::Connection,
    items: &[QueueItem],
) -> anyhow::Result<()> {
    let mut appender = connection.appender("fresh")?;
    for item in items {
        appender.append_row(params![
            item.unit,
            item.subject_code,
            item.item_id,
            item.status.to_string(),
            item.candidates_json,
            item.evidence_etag,
            item.from_snapshot_id,
            item.to_snapshot_id,
        ])?;
    }
    appender.flush()?;
    Ok(())
}

/// Builds and appends the queue's changed items.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(TABLE)?;
    // The replaced table's columns, then the `row_digest` the versions table adds.
    let content_columns: Vec<&'static str> = versions_contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| ![GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(name))
        .collect();
    let queue_columns: Vec<&'static str> = contract(REPLACED)?
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let created = committer.create_table_if_absent(versions_contract).await?;

    // Plan.
    let history = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist after its creation"))?;
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let handoff_dir = std::env::var_os(HANDOFF_DIR_ENV).map(PathBuf::from);
    let Some(pins) = input_heads(&catalog).await? else {
        write_summary(
            &summary_path,
            &json!({
                "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "waiting",
                "waiting_for": LINEAGE_SOURCE, "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {TABLE} waiting: {LINEAGE_SOURCE} does not exist yet");
        return Ok(());
    };
    let code = queue::code_version();
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let Some(base) = &base {
        versions::pin(&catalog, TABLE, base.snapshot_id, &base.pins).await?;
    }
    let code_moved = base.as_ref().is_none_or(|base| base.code_version != code);
    let moved = base
        .as_ref()
        .map_or_else(Default::default, |base| versions::moved(base, &pins));
    if !code_moved && moved.is_empty() {
        // Nothing to append; the handoff is still the current rows, for a database that never took it.
        let queue_report = match (&handoff_dir, head) {
            (Some(dir), Some(head)) => {
                let duck = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
                let current =
                    current_rows(duck.connection(), versions_contract, head, &queue_columns)?;
                write_handoffs(
                    duck.connection(),
                    &current,
                    dir,
                    &published_at_utc,
                    &BTreeMap::new(),
                )?
            }
            _ => Value::Null,
        };
        write_summary(
            &summary_path,
            &json!({
                "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "unchanged",
                "pins": pins, "queue": queue_report, "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {TABLE} nothing to do: its inputs and code are the base commit's");
        return Ok(());
    }
    let rebuilt: Vec<&str> = pins
        .keys()
        .map(String::as_str)
        .filter(|input| code_moved || moved.contains(*input))
        .collect();

    // Compute.
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
    let memory_limit = duck.memory_limit_bytes()?;
    let connection = duck.connection();
    let new_pins = NearbyPins(pins.clone());
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &new_pins,
    };
    connection.execute_batch(FRESH_TABLE)?;
    let mut ranges = Vec::new();
    let mut parcel_counts = BTreeMap::new();
    let mut open_by_sido: BTreeMap<String, u64> = BTreeMap::new();
    if rebuilt.contains(&LINEAGE_SOURCE) {
        let rows = load_review_lineage(connection, &source)?;
        let (items, counts) = parcel_items(&rows)?;
        for item in &items {
            *open_by_sido
                .entry(item.subject_code.chars().take(2).collect())
                .or_default() += 1;
        }
        insert(connection, &items)?;
        parcel_counts = counts;
        ranges.push(format!("unit = '{PARCEL_UNIT}'"));
    }
    let mut place_read = BTreeMap::new();
    let mut place_prefixes = Vec::new();
    for table in rebuilt
        .iter()
        .copied()
        .filter(|table| *table != LINEAGE_SOURCE)
    {
        let groups = load_unresolved(connection, &source, table)?;
        let mut items = place_reference_items(table, &groups)?;
        let before = base
            .as_ref()
            .and_then(|base| base.pins.get(table))
            .map(i64::to_string);
        let now = pins[table].to_string();
        for item in &mut items {
            item.from_snapshot_id.clone_from(&before);
            item.to_snapshot_id = Some(now.clone());
        }
        insert(connection, &items)?;
        place_read.insert(
            table.to_owned(),
            json!({"snapshot": now, "items": items.len()}),
        );
        place_prefixes.push(format!("starts_with(subject_code, '{table}|')"));
    }
    if !place_prefixes.is_empty() {
        ranges.push(format!(
            "(unit = '{PLACE_REFERENCE_UNIT}' AND ({}))",
            place_prefixes.join(" OR ")
        ));
    }
    let head_rows = match head {
        Some(head) => current_rows(connection, versions_contract, head, &queue_columns)?,
        // The first build: no item has a snapshot pair to keep.
        None => format!("SELECT {HANDOFF_COLUMNS} FROM fresh WHERE false"),
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE head_rows AS {head_rows};"
    ))?;
    connection.execute_batch(
        &BUILT_SQL
            .replace("@place@", PLACE_REFERENCE_UNIT)
            .replace("@published_at_utc@", &published_at_utc),
    )?;
    connection.execute_batch("DROP TABLE fresh;")?;
    ensure_no_commit_tables(connection)?;
    // Each unit's statuses (the contract's `status is needs_review or pending` holds of the parcel
    // unit; a place reference item is a steward's or waits for data).
    let misfiled: i64 = connection.query_row(
        &format!(
            "SELECT count(*) FROM built WHERE NOT ( \
               (unit = '{PARCEL_UNIT}' AND status IN ('needs_review', 'pending')) \
               OR (unit = '{PLACE_REFERENCE_UNIT}' AND status IN ('steward', 'awaiting_data')))"
        ),
        [],
        |row| row.get(0),
    )?;
    ensure!(
        misfiled == 0,
        "{misfiled} built items hold a status their unit does not have: nothing was appended"
    );

    // Diff and check.
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table: TABLE,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: String::new(),
        published_at_utc: published_at_utc.clone(),
    };
    let range = format!("({})", ranges.join(" OR "));
    let diff = versions::diff(connection, &commit, &range)?;
    let (fingerprint, current_records) = versions::after(connection, &commit, &diff)?;
    // The rows current once this commit lands: the rebuilt range from `built`, the rest from the head.
    let queue_report = match &handoff_dir {
        Some(dir) => write_handoffs(
            connection,
            &format!(
                "SELECT {HANDOFF_COLUMNS} FROM built WHERE {range} \
                 UNION ALL SELECT {HANDOFF_COLUMNS} FROM head_rows WHERE NOT {range}"
            ),
            dir,
            &published_at_utc,
            &parcel_counts,
        )?,
        None => Value::Null,
    };

    // Commit.
    let run = versions::run_id(TABLE, &pins, &code, "");
    let shape = committer.table_shape(TABLE).await?;
    let files = versions::stage(&duck, &shape, &run, versions_contract)?;
    let properties = BTreeMap::from([
        (VERSION_SEQ_PROPERTY.to_owned(), version_seq.to_string()),
        (
            CURRENT_RECORDS_PROPERTY.to_owned(),
            current_records.to_string(),
        ),
        (
            FINGERPRINT_PROPERTY.to_owned(),
            versions::fingerprint_text(fingerprint),
        ),
        (
            SOURCE_SNAPSHOTS_PROPERTY.to_owned(),
            versions::pins_property(&pins)?,
        ),
        (KEYS_CHANGED_PROPERTY.to_owned(), diff.keys().to_string()),
        (CODE_VERSION_PROPERTY.to_owned(), code.clone()),
    ]);
    let snapshot =
        versions::append(&committer, &catalog, &commit, &files, &run, properties).await?;
    drop(duck);
    let tags = versions::pin(&catalog, TABLE, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "rebuilt": rebuilt,
            "queue": queue_report, "code_version": code, "parcel": parcel_counts, "open_by_sido": open_by_sido,
            "place_reference": place_read, "diff": diff, "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": files.len(), "tags": tags, "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "committed {TABLE}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records} in {:.0}s",
        diff.keys(),
        diff.added,
        diff.changed,
        diff.removed,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The current rows of the queue at `head`, as a query over the queue's columns.
fn current_rows(
    connection: &foundation_publisher_silver::duckdb::Connection,
    versions_contract: &'static lakehouse_domain::LakehouseTableContract,
    head: i64,
    queue_columns: &[&str],
) -> anyhow::Result<String> {
    Ok(versions_contract
        .current_row_latest()
        .context("the versions table has no current-row rule")?
        .current_rows_sql(
            &with_contract_columns(
                connection,
                versions_contract,
                &foundation_publisher_silver::duckdb_parquet_stage::table_at(TABLE, head),
            )?,
            queue_columns,
            "TRUE",
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The handoff is the queue's current rows, one file per unit (an empty unit too: a load
    /// replaces the unit's items), and the steward database's own parser reads it back.
    #[test]
    fn the_handoff_is_the_current_rows_one_file_per_unit_and_the_load_reads_it(
    ) -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let stage = DuckDbParquetStage::open(directory.path())?;
        let connection = stage.connection();
        let etag = |status| stewardship_domain::evidence_etag(status, "[]");
        let needs_review = etag(stewardship_domain::ReviewStatus::NeedsReview);
        let pending = etag(stewardship_domain::ReviewStatus::Pending);
        let first = "9999911000100010000";
        let second = "9999926000100020000";
        connection.execute_batch(&format!(
            "CREATE TABLE current AS SELECT * FROM (VALUES \
               ('{PARCEL_UNIT}', '{first}', '{}', 'needs_review', '[]', '{needs_review}', '1', '2'), \
               ('{PARCEL_UNIT}', '{second}', '{}', 'pending', '[]', '{pending}', NULL, '2')) \
             AS t(unit, subject_code, item_id, status, candidates_json, evidence_etag, from_snapshot_id, to_snapshot_id);",
            queue::item_id(PARCEL_UNIT, first),
            queue::item_id(PARCEL_UNIT, second),
        ))?;
        let out = directory.path().join("handoff");
        let reopened = BTreeMap::from([("reopened_by_new_evidence".to_owned(), 1)]);
        let report = write_handoffs(
            connection,
            "SELECT * FROM current",
            &out,
            "2099-01-02T03:04:05Z",
            &reopened,
        )?;
        assert_eq!(report["open"], json!(2));
        assert_eq!(report["open_needs_review"], json!(1));
        assert_eq!(report["open_pending"], json!(1));
        // Both synthetic PNUs lie under the code 99999.
        assert_eq!(report["open_by_sido"], json!({"99": 2}));
        assert_eq!(report["reopened_by_new_evidence"], json!(1));
        let parcel = parse_handoff(&std::fs::read_to_string(out.join("parcel.json"))?)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        assert_eq!(parcel.item_count, 2);
        assert_eq!(parcel.items[1].from_snapshot_id, None);
        let place = parse_handoff(&std::fs::read_to_string(out.join("place_reference.json"))?)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        assert_eq!(
            (place.unit.as_str(), place.item_count),
            (PLACE_REFERENCE_UNIT, 0)
        );
        Ok(())
    }

    /// An item the load would refuse is refused before any file is written.
    #[test]
    fn a_handoff_the_load_would_refuse_is_not_written() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let stage = DuckDbParquetStage::open(directory.path())?;
        let connection = stage.connection();
        let subject = "9999911000100010000";
        connection.execute_batch(&format!(
            "CREATE TABLE current AS SELECT * FROM (VALUES \
               ('{PARCEL_UNIT}', '{subject}', '{}', 'needs_review', '[]', 'not-the-etag', NULL::VARCHAR, NULL::VARCHAR)) \
             AS t(unit, subject_code, item_id, status, candidates_json, evidence_etag, from_snapshot_id, to_snapshot_id);",
            queue::item_id(PARCEL_UNIT, subject),
        ))?;
        let out = directory.path().join("handoff");
        let refused = write_handoffs(
            connection,
            "SELECT * FROM current",
            &out,
            "2099-01-02T03:04:05Z",
            &BTreeMap::new(),
        );
        assert!(refused.is_err_and(|error| error.to_string().contains("evidence_etag")));
        assert!(!out.join("parcel.json").exists());
        Ok(())
    }
}
