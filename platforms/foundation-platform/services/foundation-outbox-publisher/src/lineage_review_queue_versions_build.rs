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
//! While the Spark jobs still write `gold.lineage_review_queue` and feed the steward database, this
//! table is their shadow (root ADR-0564 §6): it reads the inputs that table last recorded
//! ([`shadow_pins`]), so the takeover verdict compares two engines over one set of inputs, and its
//! first build takes the snapshot pair of each place reference item whose evidence is unchanged from
//! that table, the history its `MERGE` kept. Nothing reads it until the verdict is equal and the
//! steward cycle loads its handoff.
//!
//! Of root ADR-0564 §3-4's checks it keeps the ones that hold of a queue: one row per key (the diff)
//! and each unit's statuses. It has no floor on lost rows and no bound on changed keys: an item
//! leaves when a steward decides it or stronger evidence closes it, so the queue may empty in one
//! run, and the Spark jobs it replaces had neither.

use std::{collections::BTreeMap, path::Path, time::Instant};

use anyhow::{ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb::params,
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG},
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
use serde_json::json;

use crate::{
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY,
        VERSION_SEQ_PROPERTY,
    },
    parcel_nearby_versions_build::{contract, lakehouse_read, write_summary, MEMORY_SHARE},
    public_data_control_support::required_env_value,
};

const TABLE: &str = "gold.lineage_review_queue_versions";
const REPLACED: &str = "gold.lineage_review_queue";
const JOB: &str = "build-lineage-review-queue-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";

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

// one-off: until the queue's takeover PR (the steward cycle loads its handoff from this table and
// the Spark jobs that write gold.lineage_review_queue are gone): `LINEAGE_SNAPSHOT_PROPERTY`,
// `SOURCE_SNAPSHOT_PREFIX`, `shadow_pins` and the first build's seed from `REPLACED` go with it.
/// The property the parcel unit's Spark job records its lineage snapshot under
/// (`lineage_review_queue_to_gold.LINEAGE_SNAPSHOT_PROPERTY`).
const LINEAGE_SNAPSHOT_PROPERTY: &str = "foundation.lineage-snapshot-id";
/// The prefix the place reference job records each source's snapshot under
/// (`place_reference_queue_to_gold.SNAPSHOT_PREFIX`).
const SOURCE_SNAPSHOT_PREFIX: &str = "foundation.place-reference-snapshot.";

/// The inputs the Spark queue last read, in place of `heads`: each input's newest recorded snapshot
/// along `gold.lineage_review_queue`'s `main`, newest first (`recorded_snapshots`). A source its
/// kept history does not name did not move since (the job reads every source that moved, each run),
/// so its head stands. Returns the pins and the replaced table's head.
///
/// # Errors
/// The replaced table does not exist or records no lineage snapshot: there is nothing to shadow.
async fn shadow_pins(
    catalog: &IcebergRestCatalog,
    heads: &BTreeMap<String, i64>,
) -> anyhow::Result<(BTreeMap<String, i64>, i64)> {
    let history = catalog
        .load_main_snapshot_history(REPLACED)
        .await?
        .with_context(|| format!("{REPLACED} does not exist: there is no queue to shadow"))?;
    let replaced_head = history
        .first()
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| format!("{REPLACED} holds no snapshot"))?;
    let mut recorded: BTreeMap<String, i64> = BTreeMap::new();
    for snapshot in &history {
        for (property, value) in &snapshot.summary {
            let input = if property == LINEAGE_SNAPSHOT_PROPERTY {
                LINEAGE_SOURCE
            } else if let Some(source) = property.strip_prefix(SOURCE_SNAPSHOT_PREFIX) {
                source
            } else {
                continue;
            };
            if !recorded.contains_key(input) {
                let pin: i64 = value.parse().with_context(|| {
                    format!(
                        "{REPLACED} snapshot {} records {property}={value}",
                        snapshot.snapshot_id
                    )
                })?;
                recorded.insert(input.to_owned(), pin);
            }
        }
    }
    ensure!(
        recorded.contains_key(LINEAGE_SOURCE),
        "{REPLACED} records no {LINEAGE_SNAPSHOT_PROPERTY} in its kept history: nothing to shadow"
    );
    let pins = heads
        .iter()
        .map(|(input, head)| (input.clone(), *recorded.get(input).unwrap_or(head)))
        .collect();
    Ok((pins, replaced_head))
}

/// Every input at its head: the lineage and each place reference source that exists.
async fn input_heads(catalog: &IcebergRestCatalog) -> anyhow::Result<BTreeMap<String, i64>> {
    let mut pins = BTreeMap::new();
    for input in std::iter::once(LINEAGE_SOURCE).chain(queue::place_reference_sources()) {
        let head = catalog
            .load_main_snapshot_history(input)
            .await?
            .and_then(|history| history.first().map(|snapshot| snapshot.snapshot_id));
        if let Some(head) = head {
            pins.insert(input.to_owned(), head);
        }
    }
    ensure!(
        pins.contains_key(LINEAGE_SOURCE),
        "{LINEAGE_SOURCE} holds no snapshot: there is no lineage to queue"
    );
    Ok(pins)
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
    let at_heads = input_heads(&catalog).await?;
    let (pins, replaced_head) = shadow_pins(&catalog, &at_heads).await?;
    let code = queue::code_version();
    if let Some(base) = &base {
        versions::pin(&catalog, TABLE, base.snapshot_id, &base.pins).await?;
    }
    let code_moved = base.as_ref().is_none_or(|base| base.code_version != code);
    let moved = base
        .as_ref()
        .map_or_else(Default::default, |base| versions::moved(base, &pins));
    if !code_moved && moved.is_empty() {
        write_summary(
            &summary_path,
            &json!({
                "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "unchanged",
                "pins": pins, "shadow_of": {"table": REPLACED, "head": replaced_head},
                "seconds": started.elapsed().as_secs_f64(),
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
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = duck.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
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
        Some(head) => versions_contract
            .current_row_latest()
            .context("the versions table has no current-row rule")?
            .current_rows_sql(
                &with_contract_columns(
                    connection,
                    versions_contract,
                    &foundation_publisher_silver::duckdb_parquet_stage::table_at(TABLE, head),
                )?,
                &queue_columns,
                "TRUE",
            ),
        // The first build: the replaced table's rows at the head its pins were read from.
        None => format!(
            "SELECT {} FROM {}",
            queue_columns.join(", "),
            with_contract_columns(
                connection,
                contract(REPLACED)?,
                &foundation_publisher_silver::duckdb_parquet_stage::table_at(
                    REPLACED,
                    replaced_head
                ),
            )?
        ),
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
            "shadow_of": {"table": REPLACED, "head": replaced_head},
            "code_version": code, "parcel": parcel_counts, "open_by_sido": open_by_sido,
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
