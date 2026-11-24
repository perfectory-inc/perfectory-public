//! A release appended to `main` as its changes without Spark and without a branch (root ADR-0599).
//!
//! The lane's export left the release as gzip JSON Lines handoffs on R2. `DuckDB` inside the
//! publisher copies them once into a local Parquet file in the lane's state directory, checks the
//! rows, compares the copy with the current rows of `main` at one pinned snapshot bucket by
//! bucket (`release_compare_stage`), and the publisher appends every bucket's changed rows in one
//! commit that records the release's objects, its change record and its edition. A reader never
//! sees half a release, and a release whose check fails is never committed: write-audit-publish
//! with the audit before the one write.
//!
//! It replaces `silver_scalar_handoff_to_lakehouse.py` (the load onto a load branch) and
//! `silver_release_compare.py` (the compare onto a changes branch) for a lane whose contract says
//! `engine: duckdb`.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile,
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};
use lakehouse_infrastructure::IcebergRestCatalog;
use sha2::{Digest, Sha256};

use super::lane::LaneContract;
use crate::{
    data_go_kr_file_data_stage::{epoch, literal},
    duckdb_parquet_stage::{sql_path, with_contract_columns, DuckDbParquetStage, LakehouseRead},
    release_compare_stage::{
        buckets, check_pnu_null_share, check_release, compare_bucket, copy_release, edition_text,
        release_relation, tolerate, BucketChanges, CompareRule, GeometryTolerance, Stamp,
    },
};

/// The commit reads a staged file whole into memory: a bucket's rows are split at about this.
const STAGED_FILE_BYTES: u64 = 256 * 1024 * 1024;
/// The summary keys a release compare's commit carries (`changelog.py`, `lakehouse_ingest.py`).
const CHANGE_RECORD_KEY: &str = "foundation.change-record";
const CHANGE_RECORD_RELEASE_COMPARE: &str = "silver-release-compare";
const CHANGED_KEY_ROWS_KEY: &str = "foundation.change-record.changed-key-rows";
const REMOVED_KEYS_KEY: &str = "foundation.change-record.removed-keys";
const EDITION_KEY: &str = "foundation.edition";
const RELEASE_VERSION_KEY: &str = "foundation.compare-release-version";
/// The file beside the copy that says the copy is whole, and of which release.
const COPY_MARKER: &str = "release.json";
const COPY_FILE: &str = "release.parquet";

/// What a release's append did, for the lane's log and evidence.
#[derive(Clone, Debug, serde::Serialize)]
pub(in crate::remote_lakehouse_job) struct ReleaseAppend {
    pub table: String,
    pub source_snapshot_id: String,
    pub engine: &'static str,
    /// `appended`, or `already_appended` when `main` recorded the release's objects already.
    pub outcome: &'static str,
    pub release_rows: u64,
    pub buckets: usize,
    pub changed_key_rows: u64,
    pub removed_keys: u64,
    /// Changed keys the lane's geometry tolerance kept unchanged.
    pub tolerated_keys: u64,
    pub staged_files: usize,
    pub staged_bytes: u64,
    pub copy_seconds: f64,
    pub compare_seconds: f64,
    pub commit_seconds: f64,
    pub snapshot_id: Option<i64>,
}

/// The release a lane hands over: its identity, the Bronze objects it is, and where its rows are.
pub(in crate::remote_lakehouse_job) struct Release<'a> {
    /// The rows' `source_snapshot_id`.
    pub identity: &'a str,
    /// The Bronze objects of the release: what `main` records as loaded.
    pub objects: &'a [String],
    /// The gzip JSON Lines handoffs, as objects the stage's keys read (`s3://bucket/key`).
    pub handoffs: &'a [String],
    /// The rows the exports said they wrote, when every export of this run said so.
    pub expected_rows: Option<u64>,
}

fn seconds(started: Instant) -> f64 {
    (started.elapsed().as_secs_f64() * 10.0).round() / 10.0
}

/// Where a lane loaded by `DuckDB` keeps its release copy, its spill and its staged files: the SSD
/// (`silver-refresh.sh` sets it), not the lane's state on the data disk, which is a slow disk.
const DUCKDB_ROOT_ENV: &str = "FOUNDATION_PLATFORM_SILVER_REFRESH_DUCKDB_ROOT";

/// This lane's directory under [`DUCKDB_ROOT_ENV`].
fn duckdb_root(contract: &LaneContract) -> anyhow::Result<PathBuf> {
    let root = std::env::var(DUCKDB_ROOT_ENV)
        .ok()
        .filter(|root| !root.trim().is_empty())
        .with_context(|| format!("{DUCKDB_ROOT_ENV} names where a DuckDB lane works"))?;
    Ok(PathBuf::from(root).join(contract.lane.id()))
}

/// Removes the stage directories an earlier run left when it was killed (a stage removes its own
/// when it ends): gigabytes of spill nothing reads again.
fn remove_dead_stages(parent: &Path) -> anyhow::Result<()> {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return Ok(());
    };
    for entry in entries {
        let path = entry?.path();
        let dead = path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".duckdb-stage-"));
        if dead && path.is_dir() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("cannot remove {}", path.display()))?;
        }
    }
    Ok(())
}

/// The copy of `release` in `directory`, made now unless a whole copy of this release is there
/// (root ADR-0599 §1). Returns the copy's path and its row count.
fn release_copy(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    release: &Release<'_>,
    directory: &Path,
) -> anyhow::Result<(PathBuf, u64)> {
    let (copy, marker) = (directory.join(COPY_FILE), directory.join(COPY_MARKER));
    if let Ok(raw) = std::fs::read(&marker) {
        let held: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
        if held["identity"] == release.identity && copy.is_file() {
            if let Some(rows) = held["rows"].as_u64() {
                println!(
                    "silver-refresh {}: the release copy of {} is whole ({rows} rows); not copied again",
                    contract.table_name, release.identity
                );
                return Ok((copy, rows));
            }
        }
    }
    // No marker, or another release's: nothing here is a copy of this release.
    if directory.exists() {
        std::fs::remove_dir_all(directory)
            .with_context(|| format!("cannot empty {}", directory.display()))?;
    }
    std::fs::create_dir_all(directory)
        .with_context(|| format!("cannot create {}", directory.display()))?;
    let rows = copy_release(stage, contract, release.handoffs, &copy)?;
    let partial = marker.with_extension("partial");
    std::fs::write(
        &partial,
        serde_json::json!({"identity": release.identity, "rows": rows}).to_string(),
    )?;
    std::fs::rename(&partial, &marker)
        .with_context(|| format!("cannot finish {}", marker.display()))?;
    Ok((copy, rows))
}

/// `main` as it stands at snapshot `head`, copied once into `directory` and read from there
/// (root ADR-0599 §2: the compare reads `main` once per pass). Returns the copy as a relation.
///
/// For a table whose files hold every compare bucket's keys (it is partitioned by a hash of the
/// key), a bucket's read of `main` prunes nothing: every pass of every bucket read all of it from
/// R2 again. 2026-11-21, `silver.building_register_unit_areas` (32 files, 8 GiB, 28 buckets): 24
/// minutes for the first bucket while R2 range requests from the host stalled for up to 171
/// seconds each; the run could not end inside its limit. A snapshot never changes, so a whole copy
/// of the same snapshot is read again by a retry; a copy of another snapshot is removed.
fn main_copy(
    stage: &DuckDbParquetStage,
    main: &str,
    head: i64,
    directory: &Path,
) -> anyhow::Result<String> {
    let copy = directory.join(format!("main-{head}.parquet"));
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("cannot list {}", directory.display()))?
    {
        let path = entry?.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with("main-") && path != copy {
            std::fs::remove_file(&path)
                .with_context(|| format!("cannot remove {}", path.display()))?;
        }
    }
    if !copy.is_file() {
        let partial = copy.with_extension("partial");
        stage
            .connection()
            .execute_batch(&format!(
                "COPY (SELECT * FROM {main}) TO '{}' (FORMAT parquet, COMPRESSION zstd)",
                sql_path(&partial)?
            ))
            .with_context(|| format!("DuckDB failed to copy main at snapshot {head}"))?;
        std::fs::rename(&partial, &copy)
            .with_context(|| format!("cannot finish {}", copy.display()))?;
    }
    release_relation(&copy)
}

/// The one edition the copy is, for a table ordered by one.
fn release_edition(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    release: &str,
    column: &str,
) -> anyhow::Result<String> {
    let mut statement = stage.connection().prepare(&format!(
        "SELECT DISTINCT {} FROM {release} LIMIT 2",
        edition_text(contract, column)?
    ))?;
    let found = statement
        .query_map([], |row| row.get::<_, Option<String>>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    match found.as_slice() {
        [Some(edition)] if !edition.is_empty() => Ok(edition.clone()),
        other => bail!("a staged release is one {column}, not {other:?}"),
    }
}

/// Refuses a release `main` cannot take in order (root ADR-0317, ADR-0328): an edition older than
/// one `main` holds, or a `main` that holds a row loaded at or after this run's instant.
fn refuse_out_of_order(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    main: &str,
    edition: Option<(&str, &str)>,
    recorded: &BTreeSet<String>,
    run_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let table = contract.table_name;
    let later: i64 = stage.connection().query_row(
        &format!(
            "SELECT count(*) FROM (SELECT 1 FROM {main} WHERE ingested_at_utc >= {} LIMIT 1)",
            epoch(run_at)
        ),
        [],
        |row| row.get(0),
    )?;
    ensure!(
        later == 0,
        "{table} holds a row loaded at or after this run's instant {run_at}; its rows would not outrank it"
    );
    let Some((column, edition)) = edition else {
        return Ok(());
    };
    let newest: Option<String> = stage.connection().query_row(
        &format!(
            "SELECT max({}) FROM {main}",
            edition_text(contract, column)?
        ),
        [],
        |row| row.get(0),
    )?;
    let newer: Vec<&String> = recorded
        .iter()
        .chain(newest.as_ref())
        .filter(|held| held.as_str() > edition)
        .collect();
    ensure!(
        newer.is_empty(),
        "edition {edition} is older than the held editions {newer:?}; versions are appended in edition order"
    );
    Ok(())
}

/// A staged file's name: the table, the release and the bucket, so a retry of the same release
/// names the same objects and never a run's instant (root ADR-0560 §2, ADR-0599 §5).
fn staged_name(table: &str, identity: &str, bucket: usize) -> String {
    let short = table.rsplit('.').next().unwrap_or(table).replace('_', "-");
    let digest = format!("{:x}", Sha256::digest(identity.as_bytes()));
    format!("{short}-{}-b{bucket:04}", &digest[..16])
}

/// `N`, when the table is partitioned by `bucket(N, key)` alone, its key one column: a release's
/// changes are then written one file set per bucket (`write_bucketed`).
fn key_buckets(contract: &LakehouseTableContract, rule: &CompareRule) -> Option<u32> {
    let [part] = contract.partition_spec else {
        return None;
    };
    let [key] = rule.key.as_slice() else {
        return None;
    };
    let (buckets, column) = part
        .strip_prefix("bucket(")?
        .strip_suffix(')')?
        .split_once(',')?;
    (column.trim() == *key)
        .then(|| buckets.trim().parse().ok())
        .flatten()
}

/// The lane's table contract and its compare rule (`None` for a lane that declares no compare: its
/// release is appended whole), when this path can load the lane.
///
/// # Errors
/// The lane declares a compare this path does not do: a partition other than the edition or one
/// hash bucket of the key, or a release appended whole on a table with the PNU null-share gate.
pub(in crate::remote_lakehouse_job) fn fits(
    contract: &LaneContract,
) -> anyhow::Result<(&'static LakehouseTableContract, Option<CompareRule>)> {
    let table = contract.table.as_str();
    let table_contract = industrial_complex_lakehouse_contract_by_table_name(table)
        .with_context(|| format!("no contract for {table}"))?;
    let rule = contract
        .compare_content
        .as_ref()
        .map(|content| CompareRule::of(table_contract, content))
        .transpose()?;
    // A release is one edition, so a table partitioned by its edition column gets one partition
    // per release; the committer refuses a file that is not (`manifest_entry`), which is also
    // what holds a release appended whole to one partition.
    ensure!(
        rule.as_ref().is_none_or(|rule| table_contract
            .partition_spec
            .iter()
            .all(|part| Some(*part) == rule.edition_column)
            || key_buckets(table_contract, rule).is_some()),
        "{table} is partitioned by {:?}: a staged file must be one partition, and this path splits by none but the edition",
        table_contract.partition_spec
    );
    ensure!(
        rule.is_some()
            || !table_contract
                .quality_gates
                .iter()
                .any(|gate| gate.contains("ordinary_land_pnu_null_share")),
        "{table} declares the PNU null-share gate, which a release appended whole is not judged by"
    );
    Ok((table_contract, rule))
}

/// Appends `release` to the lane's table as its changes, in one commit; returns what it did.
///
/// # Errors
/// The lane or its table cannot be compared this way yet (a geometry tolerance, a partitioned
/// table, a predicate), a check of the release fails, `main` moved under the compare
/// (`HeadMoved`: the next run compares again from the kept copy), or `DuckDB` or the catalog fails.
pub(in crate::remote_lakehouse_job) async fn append_release(
    contract: &LaneContract,
    catalog: &IcebergRestCatalog,
    release: &Release<'_>,
) -> anyhow::Result<ReleaseAppend> {
    let table = contract.table.as_str();
    let (table_contract, rule) = fits(contract)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    // A table is born on its first commit (root ADR-0564 §1).
    committer.create_table_if_absent(table_contract).await?;
    let ingested = catalog
        .load_ingested_batch_objects(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    let held: Vec<&String> = release
        .objects
        .iter()
        .filter(|object| ingested.contains(*object))
        .collect();
    let mut report = ReleaseAppend {
        table: table.to_owned(),
        source_snapshot_id: release.identity.to_owned(),
        engine: "duckdb",
        outcome: "already_appended",
        release_rows: 0,
        buckets: 0,
        changed_key_rows: 0,
        removed_keys: 0,
        tolerated_keys: 0,
        staged_files: 0,
        staged_bytes: 0,
        copy_seconds: 0.0,
        compare_seconds: 0.0,
        commit_seconds: 0.0,
        snapshot_id: None,
    };
    if held.len() == release.objects.len() {
        return Ok(report);
    }
    ensure!(
        held.is_empty(),
        "main records {} of the release's {} objects; a release is one commit",
        held.len(),
        release.objects.len()
    );
    let head = catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id);
    let recorded = catalog
        .load_recorded_editions(table)
        .await?
        .unwrap_or_default();
    let shape = committer.table_shape(table).await?;

    let root = duckdb_root(contract)?;
    let (stages, copies) = (root.join("stage"), root.join("release-copy"));
    remove_dead_stages(&stages)?;
    // The staged files live as long as the stage: it is kept until the append is done.
    let stage = DuckDbParquetStage::open_reading_lakehouse(&stages, &LakehouseRead::from_env()?)?;
    let started = Instant::now();
    // The copy's size is not known before it is made; its spill ceiling is.
    stage.require_room(0)?;
    let (copy, rows) = release_copy(&stage, table_contract, release, &copies)?;
    report.copy_seconds = seconds(started);
    // The staged files hold at most the copy's rows; a bucketed table gathers them once more.
    stage.require_room(std::fs::metadata(&copy)?.len().saturating_mul(2))?;
    let copied = release_relation(&copy)?;
    report.release_rows = check_release(
        &stage,
        table_contract,
        &copied,
        release.identity,
        release.expected_rows,
    )?;
    ensure!(
        rows == report.release_rows,
        "the release copy of {table} changed after it was made"
    );

    let Some(rule) = rule else {
        // No compare: the release is appended whole, as its handoff holds it.
        let started = Instant::now();
        let files = stage.write_split(
            &shape,
            &format!("SELECT * FROM {copied}"),
            &staged_name(table, release.identity, 0),
            STAGED_FILE_BYTES,
        )?;
        report.staged_files = files.len();
        for file in &files {
            report.staged_bytes += std::fs::metadata(&file.local_path)?.len();
        }
        report.changed_key_rows = report.release_rows;
        report.compare_seconds = seconds(started);
        let started = Instant::now();
        let outcome = committer
            .append_at_head_recording(
                DataAppend {
                    table_name: table,
                    contract: table_contract,
                    files: &files,
                    batch: release.objects,
                    already_ingested: &ingested,
                    summary: BTreeMap::new(),
                },
                head,
                BTreeMap::new(),
            )
            .await?;
        drop(stage);
        report.commit_seconds = seconds(started);
        let rows = report.release_rows;
        return committed(report, outcome, rows, &copies);
    };

    let main = match head {
        Some(head) => with_contract_columns(
            stage.connection(),
            table_contract,
            &crate::duckdb_parquet_stage::table_at(table, head),
        )?,
        // A table with no snapshot holds no row: every key of the release is new.
        None => format!("(SELECT * FROM {copied} WHERE false)"),
    };
    let main = match head {
        Some(head) if key_buckets(table_contract, &rule).is_some() => {
            let started = Instant::now();
            // The copy's size is not known before it is made; the stage's spill ceiling is. Asked
            // here, not in `main_copy`: its test must not depend on the disk of the machine it runs on.
            stage.require_room(0)?;
            let local = main_copy(&stage, &main, head, &copies)?;
            println!(
                "silver-refresh {table}: main at snapshot {head} copied once in {:.1} s; every bucket compares with the copy",
                seconds(started)
            );
            local
        }
        _ => main,
    };
    // Before anything of the release is staged (the Spark load judged it before its first batch).
    if let Some(outcome) =
        check_pnu_null_share(&stage, table_contract, &copied, head.map(|_| main.as_str()))?
    {
        println!("silver-refresh {table}: silver-release-pnu-null-share {outcome}");
    }
    // One instant for the run, on every appended row (root ADR-0328).
    let run_at = DateTime::from_timestamp(Utc::now().timestamp(), 0)
        .context("the clock names no instant")?;
    let edition = rule
        .edition_column
        .map(|column| release_edition(&stage, table_contract, &copied, column))
        .transpose()?;
    refuse_out_of_order(
        &stage,
        table_contract,
        &main,
        rule.edition_column.zip(edition.as_deref()),
        &recorded,
        run_at,
    )?;
    let stamp = Stamp {
        source_snapshot_id: release.identity,
        edition: edition.as_deref(),
        run_at,
    };
    let started = Instant::now();
    let sides: Vec<&str> = if contract.compare_release_buckets_only {
        vec![copied.as_str()]
    } else {
        vec![copied.as_str(), main.as_str()]
    };
    let length = contract.compare_bucket_prefix_length;
    let every = buckets(&stage, &rule, &sides, length)?;
    report.buckets = every.len();
    let mut files: Vec<StagedFile> = Vec::new();
    let tolerance = contract
        .compare_geometry_tolerance
        .as_deref()
        .map(|raw| GeometryTolerance::parse(raw, table_contract, &rule))
        .transpose()?;
    let hash_buckets = key_buckets(table_contract, &rule);
    let mut gathered = false;
    for (index, bucket) in every.iter().enumerate() {
        let in_bucket = bucket.predicate(rule.key[0], length);
        let found = compare_bucket(
            &stage,
            table_contract,
            &rule,
            &format!("(SELECT * FROM {copied} WHERE {in_bucket})"),
            &main,
            &in_bucket,
            stamp,
        )?;
        let tolerated = match &tolerance {
            Some(tolerance) => tolerate(
                &stage,
                table_contract,
                &rule,
                &format!("(SELECT * FROM {copied} WHERE {in_bucket})"),
                &main,
                &in_bucket,
                tolerance,
            )?,
            None => 0,
        };
        report.tolerated_keys += tolerated;
        let found = BucketChanges {
            changed_keys: found.changed_keys - tolerated,
            ..found
        };
        println!(
            "silver-refresh {table} bucket {}: {} changed keys ({tolerated} more within the geometry tolerance) and {} removal markers",
            bucket.label(),
            found.changed_keys,
            found.removed_keys
        );
        report.removed_keys += found.removed_keys;
        if found.changed_keys + found.removed_keys == 0 {
            continue;
        }
        if hash_buckets.is_some() {
            // Split by hash bucket once, after the last compare bucket: per compare bucket it
            // would be thirty-two small files each.
            stage.connection().execute_batch(
                "CREATE TABLE IF NOT EXISTS rc_all AS SELECT * FROM rc_changes WITH NO DATA; \
                 INSERT INTO rc_all SELECT * FROM rc_changes",
            )?;
            gathered = true;
            continue;
        }
        let staged = stage.write_split(
            &shape,
            "SELECT * FROM rc_changes",
            &staged_name(table, release.identity, index),
            STAGED_FILE_BYTES,
        )?;
        files.extend(staged);
    }
    if let (Some(buckets), true) = (hash_buckets, gathered) {
        files = stage.write_bucketed(
            &shape,
            "rc_all",
            rule.key[0],
            buckets,
            &staged_name(table, release.identity, 0),
            STAGED_FILE_BYTES,
        )?;
    }
    let mut staged_rows = 0_u64;
    for file in &files {
        report.staged_bytes += std::fs::metadata(&file.local_path)?.len();
        let rows: i64 = stage.connection().query_row(
            &format!(
                "SELECT count(*) FROM read_parquet({})",
                literal(&file.local_path.to_string_lossy().replace('\\', "/"))
            ),
            [],
            |row| row.get(0),
        )?;
        staged_rows += u64::try_from(rows)?;
    }
    report.staged_files = files.len();
    report.changed_key_rows = staged_rows
        .checked_sub(report.removed_keys)
        .context("the staged files hold fewer rows than the removal markers")?;
    report.compare_seconds = seconds(started);

    // The one write: the release's objects, its change record, its edition and its version in
    // the snapshot's summary, and the edition's durable record in the same commit (ADR-0599 §4).
    let version = edition
        .clone()
        .unwrap_or_else(|| run_at.naive_utc().to_string());
    let mut summary = BTreeMap::from([
        (
            CHANGE_RECORD_KEY.to_owned(),
            CHANGE_RECORD_RELEASE_COMPARE.to_owned(),
        ),
        (
            CHANGED_KEY_ROWS_KEY.to_owned(),
            report.changed_key_rows.to_string(),
        ),
        (REMOVED_KEYS_KEY.to_owned(), report.removed_keys.to_string()),
        (RELEASE_VERSION_KEY.to_owned(), version),
    ]);
    let mut properties = BTreeMap::new();
    if let Some(edition) = &edition {
        summary.insert(EDITION_KEY.to_owned(), edition.clone());
        // Read by its key alone (`lakehouse_ingest.read_editions`); the value says when.
        properties.insert(
            format!("{EDITION_KEY}.{edition}"),
            run_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
    }
    let started = Instant::now();
    let outcome = committer
        .append_at_head_recording(
            DataAppend {
                table_name: table,
                contract: table_contract,
                files: &files,
                batch: release.objects,
                already_ingested: &ingested,
                summary,
            },
            head,
            properties,
        )
        .await?;
    drop(stage);
    report.commit_seconds = seconds(started);
    committed(report, outcome, staged_rows, &copies)
}

/// The report of a release whose one commit returned `outcome`; its copy is removed, for the
/// release is in `main`.
fn committed(
    mut report: ReleaseAppend,
    outcome: AppendOutcome,
    staged_rows: u64,
    copies: &Path,
) -> anyhow::Result<ReleaseAppend> {
    if let AppendOutcome::Appended {
        snapshot,
        added_records,
        ..
    } = outcome
    {
        ensure!(
            added_records == staged_rows,
            "the commit of {} added {added_records} rows, not the {staged_rows} staged",
            report.table
        );
        report.outcome = "appended";
        report.snapshot_id = Some(snapshot.snapshot_id);
    }
    if copies.exists() {
        std::fs::remove_dir_all(copies)
            .with_context(|| format!("cannot remove {}", copies.display()))?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use anyhow::Context as _;
    use lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name;

    use super::{main_copy, release_copy, staged_name, Release, COPY_MARKER};
    use crate::duckdb_parquet_stage::DuckDbParquetStage;

    #[test]
    fn a_staged_file_is_named_by_its_table_release_and_bucket_never_by_the_run() {
        let name = staged_name("silver.land_use_plan", "vworldkr-land-use-plan:20991109", 7);
        assert_eq!(
            name,
            staged_name("silver.land_use_plan", "vworldkr-land-use-plan:20991109", 7)
        );
        assert!(
            name.starts_with("land-use-plan-") && name.ends_with("-b0007"),
            "{name}"
        );
        assert!(
            name.bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "{name}"
        );
        assert_ne!(
            name,
            staged_name("silver.land_use_plan", "vworldkr-land-use-plan:20991209", 7)
        );
    }

    /// `main` is read once: a copy of the snapshot is made, a whole copy of the same snapshot is
    /// read again without touching `main`, and another snapshot's copy is removed.
    #[test]
    fn main_is_copied_once_per_snapshot_and_another_snapshots_copy_is_removed() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let stage = DuckDbParquetStage::open(root.path())?;
        // Inside the stage's own directory: the only place this stage may read and write.
        let work = std::fs::read_dir(root.path())?
            .next()
            .context("the stage directory")??
            .path();
        let count = |relation: &str| -> anyhow::Result<i64> {
            Ok(stage.connection().query_row(
                &format!("SELECT count(*) FROM {relation}"),
                [],
                |row| row.get(0),
            )?)
        };
        stage
            .connection()
            .execute_batch("CREATE TABLE main_rows AS SELECT 1 AS k UNION ALL SELECT 2")?;
        let first = main_copy(&stage, "main_rows", 7, &work)?;
        assert_eq!(count(&first)?, 2);
        // Planted: the source moves; the same snapshot's copy is what is read.
        stage
            .connection()
            .execute_batch("INSERT INTO main_rows VALUES (3)")?;
        assert_eq!(main_copy(&stage, "main_rows", 7, &work)?, first);
        assert_eq!(count(&first)?, 2);
        // The next snapshot: its own copy, and the old one is gone.
        let second = main_copy(&stage, "main_rows", 8, &work)?;
        assert_eq!(count(&second)?, 3);
        assert!(!work.join("main-7.parquet").exists());
        assert!(!work.join("main-8.partial").exists());
        Ok(())
    }

    /// Root ADR-0599 §1: a whole copy of this release is not made again (a run whose commit was
    /// refused compares from it), and a copy of another release is never read as this one's.
    #[test]
    fn a_whole_copy_of_the_release_is_kept_and_another_releases_copy_is_made_again(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let stage = DuckDbParquetStage::open(root.path())?;
        let contract = industrial_complex_lakehouse_contract_by_table_name("silver.land_use_plan")
            .context("the land use plan table")?;
        // Inside the stage's own directory: the only place this stage may read and write.
        let work = std::fs::read_dir(root.path())?
            .next()
            .context("the stage directory")??
            .path();
        let handoff = work.join("handoff.jsonl.gz");
        let line = |release: &str| {
            let row: serde_json::Map<String, serde_json::Value> = contract
                .columns
                .iter()
                .map(|column| {
                    let value = match column.name {
                        "pnu" => "9999910100100010000".into(),
                        "source_snapshot_id" => release.into(),
                        "ingested_at_utc" => "2099-01-01T00:00:00Z".into(),
                        "row_state" => serde_json::Value::Null,
                        _ => "x".into(),
                    };
                    (column.name.to_owned(), value)
                })
                .collect();
            serde_json::Value::Object(row).to_string()
        };
        let write = |release: &str| -> anyhow::Result<()> {
            let mut file = flate2::write::GzEncoder::new(
                std::fs::File::create(&handoff)?,
                flate2::Compression::fast(),
            );
            writeln!(file, "{}", line(release))?;
            file.finish()?;
            Ok(())
        };
        let urls = [handoff.to_string_lossy().replace('\\', "/")];
        let objects = ["bronze/source=synthetic/a.zip".to_owned()];
        let release = |identity: &'static str| Release {
            identity,
            objects: &objects,
            handoffs: &urls,
            expected_rows: None,
        };
        let directory = work.join("release-copy");
        write("e1")?;
        let (copy, rows) = release_copy(&stage, contract, &release("e1"), &directory)?;
        assert_eq!(rows, 1);
        assert!(copy.is_file() && directory.join(COPY_MARKER).is_file());
        // The handoff is gone: a whole copy of e1 is not read from it again.
        std::fs::remove_file(&handoff)?;
        assert_eq!(
            release_copy(&stage, contract, &release("e1"), &directory)?.1,
            1
        );
        // Another release: the copy there is not its copy, and without a handoff none is made.
        assert!(release_copy(&stage, contract, &release("e2"), &directory).is_err());
        assert!(!directory.join(COPY_MARKER).exists());
        write("e2")?;
        assert_eq!(
            release_copy(&stage, contract, &release("e2"), &directory)?.1,
            1
        );
        Ok(())
    }
}
