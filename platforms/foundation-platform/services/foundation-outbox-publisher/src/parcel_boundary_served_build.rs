//! `build-parcel-boundary-served`: the served parcel boundaries, `gold.parcel_boundary_served_versions`
//! (root ADR-0133 §2, ADR-0564), and the parcel tile bake's handoff. It was the Spark jobs
//! `parcel_boundary_served_gold.py` and `parcel_boundary_fold.py`.
//!
//! One run: append the edit store's handed `parcels` edits to `silver.map_edit_ledger` once each,
//! pin the edition and the ledger at their heads, and either build every 시도 of the edition with every
//! edit applied (the default) or fold only the edits after the served build (`…_FOLD=1`, root ADR-0375
//! §2: the changed PNUs and the handoff of their neighbourhood; exit 3 when it cannot be partial).
//! The built rows are diffed against the table's current rows one 시도 at a time and the changed keys
//! appended in one commit (`gold_versions_commit`). The handoff is the v2 one the bake reads
//! (`served_gold_common`, "The v2 handoff"): create-only JSONL parts and a summary.
//!
//! `…_NO_COMMIT=1` builds, diffs and writes the handoff and summary, and appends nothing to either
//! table.
//!
//! `build-parcel-boundary-served-versions` (one-off, [`run_shadow`]) is the table's shadow while the
//! bake still serves `gold.parcel_boundary_served` from the Spark job (root ADR-0564 §6): it builds
//! from the inputs and the edition of that table's newest build, hands nothing to the bake, appends
//! no edit to the ledger, and the takeover verdict compares the two (`gold-versions-build.sh
//! parcel-boundary-served`, the check `takeover_equal`).

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile, TableShape,
};
use foundation_publisher_silver::{
    duckdb::Connection,
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG},
    parcel_boundary_served_stage::{
        self as served, build_fold_sql, build_sido_sql, change_list_line, check_ledger, fold_edits,
        handoff_line, load_edits, membership_buffer_pixels, plan, read_edit_handoff,
        silver_predicate, EditFingerprint, FoldPlan, LedgerEdit, Op, GEOMETRY_SRID,
        NEIGHBOURHOOD_ZOOM, UNIT,
    },
    process_memory, vworld_parcel_source_contract,
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseTableContract, GOLD_VERSIONS_REMOVAL_COLUMN,
    GOLD_VERSION_SEQ_COLUMN,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    gold_versions_commit::{
        self as versions, Commit, Diff, Digests, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, SERVED_EDITION_PROPERTY,
        SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    kind_versions_build::{head_sidos, sido_range},
    parcel_nearby_versions_build::write_summary,
    public_data_control_support::required_env_value,
};

/// The table this command writes.
pub(crate) const TABLE: &str = "gold.parcel_boundary_served_versions";
/// The table whose columns it carries, which the Spark job replaced on every run.
const REPLACED: &str = "gold.parcel_boundary_served";
const PARCELS: &str = "silver.parcel_boundaries";
const LEDGER: &str = "silver.map_edit_ledger";
/// The edits a build has applied, through which a fold starts.
const EDITS_THROUGH_PROPERTY: &str = "foundation.edits-through-change-seq";
const SUMMARY_SCHEMA: &str = "foundation-platform.polygon_served_gold.v2";
const JOB: &str = "parcel_boundary_served_gold";
/// The exit of a fold that cannot be partial: nothing was written and the caller builds every row.
pub(crate) const NOT_PARTIAL_EXIT: i32 = 3;
const DEFAULT_HANDOFF_PARTS: u64 = 64;
const CHANGE_LIST_NAME: &str = "change-list.jsonl";
const ENV: &str = "FOUNDATION_PLATFORM_PARCEL_SERVED_GOLD_";
/// The summary `gold-versions-build.sh` reads of a shadow run (as of every versions lane).
const SHADOW_SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
const SHADOW_JOB: &str = "build-parcel-boundary-served-versions";
const PARCEL_SOURCE_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");
/// The share of the memory left that the buffer manager takes (as the nearby build).
const MEMORY_SHARE: (u64, u64) = (3, 5);

fn env(name: &str) -> Option<String> {
    std::env::var(format!("{ENV}{name}"))
        .ok()
        .filter(|value| !value.is_empty())
}

fn flag(name: &str) -> bool {
    env(name).as_deref() == Some("1")
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// What `built` holds, in the versions table's order: its columns less the two each commit adds
/// (the replaced table's, then the `row_digest` the versions table adds to them).
fn content_columns(versions: &LakehouseTableContract) -> Vec<&'static str> {
    versions
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| ![GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(name))
        .collect()
}

/// The open rows of one edition of `silver.parcel_boundaries` at `silver`.
fn edition_sql(silver: i64, snapshot_id: &str) -> String {
    format!(
        "SELECT pnu, geometry_wkb, geometry_srid, geometry_checksum_sha256, source_snapshot_id, \
                bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y \
         FROM {LAKEHOUSE_CATALOG}.{PARCELS} AT (VERSION => {silver}) \
         WHERE source_snapshot_id = '{snapshot_id}' AND valid_to_utc IS NULL"
    )
}

/// The edited PNUs the edition holds.
fn present_pnus(
    connection: &Connection,
    edition: &str,
    edited: &BTreeSet<String>,
) -> anyhow::Result<BTreeSet<String>> {
    if edited.is_empty() {
        return Ok(BTreeSet::new());
    }
    let list = edited
        .iter()
        .map(|pnu| format!("'{pnu}'"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(connection
        .prepare(&format!(
            "SELECT pnu FROM ({edition}) e WHERE pnu IN ({list})"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?)
}

fn served_edition() -> anyhow::Result<String> {
    let view = vworld_parcel_source_contract::served_edition_view(PARCEL_SOURCE_CONTRACT)?;
    let contract: Value = serde_json::from_str(PARCEL_SOURCE_CONTRACT)?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("the parcel source contract names no snapshot_id_prefix")?;
    let edition = view["edition"]
        .as_str()
        .context("the served edition view names no edition")?;
    Ok(format!("{prefix}{edition}"))
}

/// The `DuckDB` read: the data files with the read-only pair (root ADR-0152), the catalog with the
/// run's own catalog settings (its read-only token in a verdict run).
fn reader_read(catalog: &LakehouseCatalogConfig) -> anyhow::Result<LakehouseRead> {
    let storage =
        foundation_outbox::object_storage::R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("failed to configure lakehouse R2 reads with the reader pair")?;
    Ok(LakehouseRead {
        extensions: foundation_publisher_silver::duckdb_extensions::release_directory()?,
        catalog_uri: catalog.catalog_uri.clone(),
        warehouse: catalog.warehouse.clone(),
        catalog_token: catalog
            .catalog_token
            .clone()
            .context("the lakehouse catalog token is required")?,
        s3_endpoint: storage.endpoint,
        s3_access_key_id: storage.access_key_id,
        s3_secret_access_key: storage.secret_access_key,
    })
}

async fn head_of(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<Option<i64>> {
    Ok(catalog
        .load_main_snapshot_history(table)
        .await?
        .and_then(|history| history.first().map(|snapshot| snapshot.snapshot_id)))
}

/// Refuses an edition name that is not a plain snapshot id (it is quoted into SQL).
fn ensure_snapshot_id(snapshot_id: &str) -> anyhow::Result<()> {
    ensure!(
        !snapshot_id.is_empty()
            && snapshot_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte)),
        "{snapshot_id:?} is not a snapshot id"
    );
    Ok(())
}

/// The run's inputs, from the environment.
struct Args {
    snapshot_id: String,
    fold: bool,
    no_commit: bool,
    edits_input: Option<PathBuf>,
    output_dir: PathBuf,
    summary: Option<PathBuf>,
    /// Where the `DuckDB` stage (database, spill, staged files) lives: the unit's state directory on
    /// the SSD (host-layout `duckdb_stage_reserve_bytes`); the summary's directory when unset.
    stage_root: Option<PathBuf>,
    parts: u64,
}

impl Args {
    fn from_env() -> anyhow::Result<Self> {
        let snapshot_id = env("SNAPSHOT_ID")
            .with_context(|| format!("{ENV}SNAPSHOT_ID names the edition to serve"))?;
        ensure_snapshot_id(&snapshot_id)?;
        if !flag("ALLOW_NON_SERVED_EDITION") {
            let served = served_edition()?;
            ensure!(
                snapshot_id == served,
                "{snapshot_id} is not the served edition {served}; a rehearsal passes \
                 {ENV}ALLOW_NON_SERVED_EDITION=1"
            );
        }
        let output_dir = PathBuf::from(
            env("OUTPUT_DIR").with_context(|| format!("{ENV}OUTPUT_DIR names the handoff"))?,
        );
        ensure!(
            !output_dir.exists(),
            "served-boundary bake handoff already exists and is evidence: {}",
            output_dir.display()
        );
        let parts = env("HANDOFF_PARTS")
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(DEFAULT_HANDOFF_PARTS);
        ensure!(parts > 0, "a handoff needs at least one part");
        Ok(Self {
            snapshot_id,
            fold: flag("FOLD"),
            no_commit: flag("NO_COMMIT"),
            edits_input: env("EDITS_INPUT").map(PathBuf::from),
            output_dir,
            summary: env("SUMMARY").map(PathBuf::from),
            stage_root: env("STAGE_ROOT").map(PathBuf::from),
            parts,
        })
    }
}

/// The parcels unit's ledgered edits at `head`, with their fingerprints.
fn read_ledger(
    connection: &Connection,
    head: Option<i64>,
) -> anyhow::Result<(Vec<LedgerEdit>, BTreeMap<i64, EditFingerprint>)> {
    let Some(head) = head else {
        return Ok((Vec::new(), BTreeMap::new()));
    };
    let mut statement = connection.prepare(&format!(
        "SELECT change_seq, feature_id, op, geometry_srid, geometry_wkb, geometry_checksum_sha256, \
                properties_json, editor, edited_at \
         FROM {LAKEHOUSE_CATALOG}.{LEDGER} AT (VERSION => {head}) WHERE unit = '{UNIT}'"
    ))?;
    let mut edits = Vec::new();
    let mut fingerprints = BTreeMap::new();
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let change_seq: i64 = row.get(0)?;
        let feature_id: String = row.get(1)?;
        let op: String = row.get(2)?;
        let checksum: Option<String> = row.get(5)?;
        fingerprints.insert(
            change_seq,
            (
                change_seq,
                feature_id.clone(),
                op.clone(),
                checksum.clone().filter(|sum| !sum.is_empty()),
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ),
        );
        edits.push(LedgerEdit {
            change_seq,
            feature_id,
            op: match op.as_str() {
                "upsert" => Op::Upsert,
                "delete" => Op::Delete,
                other => bail!("edit {change_seq} has op {other}"),
            },
            geometry_srid: row.get(3)?,
            geometry_wkb: row.get(4)?,
            geometry_checksum_sha256: checksum,
        });
    }
    Ok((edits, fingerprints))
}

/// The handoff's unledgered edits staged as one batch for the ledger (`None` when there are none),
/// and the ledger the run reads with them.
type LedgerBatch = Option<(
    String,
    foundation_publisher_core::lakehouse_data_commit::StagedFile,
)>;

fn ledger_with_handoff(
    args: &Args,
    stage: &DuckDbParquetStage,
    shape: Option<&foundation_publisher_core::lakehouse_data_commit::TableShape>,
    head: Option<i64>,
    now: &str,
) -> anyhow::Result<(u64, Vec<LedgerEdit>, LedgerBatch)> {
    let (mut ledger, fingerprints) = read_ledger(stage.connection(), head)?;
    let Some(path) = &args.edits_input else {
        return Ok((0, ledger, None));
    };
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read the edit handoff {}", path.display()))?;
    let handoff = read_edit_handoff(&text)?;
    let fresh = served::fresh_edits(&fingerprints, &handoff)?;
    if fresh.is_empty() {
        return Ok((0, ledger, None));
    }
    let batch = format!("map-edit-export-{}", uuid::Uuid::new_v4());
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TABLE ledger_append (unit VARCHAR, change_seq BIGINT, feature_id VARCHAR, \
           op VARCHAR, geometry_geojson VARCHAR, geometry_wkb BLOB, geometry_srid INTEGER, \
           geometry_checksum_sha256 VARCHAR, properties_json VARCHAR, editor VARCHAR, edited_at VARCHAR, \
           export_batch_id VARCHAR, ingested_at_utc TIMESTAMP);",
    )?;
    {
        let mut appender = connection.appender("ledger_append")?;
        for edit in &fresh {
            let ledgered = edit.ledger_edit()?;
            appender.append_row(foundation_publisher_silver::duckdb::params![
                UNIT,
                edit.change_seq,
                edit.feature_id,
                edit.op,
                edit.geometry_geojson,
                ledgered.geometry_wkb,
                edit.geometry_srid,
                edit.geometry_checksum_sha256,
                edit.properties_json,
                edit.editor,
                edit.edited_at,
                batch,
                now,
            ])?;
            ledger.push(ledgered);
        }
        appender.flush()?;
    }
    let Some(shape) = shape else {
        return Ok((u64::try_from(fresh.len())?, ledger, None));
    };
    let file = stage.write(shape, "SELECT * FROM ledger_append", &batch)?;
    Ok((u64::try_from(fresh.len())?, ledger, Some((batch, file))))
}

/// Appends the staged ledger batch.
async fn append_ledger(
    committer: &LakehouseDataCommitter,
    batch: &str,
    file: foundation_publisher_core::lakehouse_data_commit::StagedFile,
    already: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let outcome = committer
        .append(DataAppend {
            table_name: LEDGER,
            contract: contract(LEDGER)?,
            files: &[file],
            batch: &[batch.to_owned()],
            already_ingested: already,
            summary: BTreeMap::new(),
        })
        .await?;
    ensure!(
        matches!(outcome, AppendOutcome::Appended { .. }),
        "the ledger already records batch {batch}"
    );
    Ok(())
}

/// One part of the handoff, as the summary records it.
#[derive(serde::Serialize)]
struct Part {
    path: String,
    rows: u64,
    sha256: String,
}

/// Writes the rows `query` answers (`pnu`, `geometry_wkb`, `origin`, ordered by `pnu`) as
/// create-only parts of at most `per_part` lines, numbered from `next`.
fn write_parts(
    connection: &Connection,
    query: &str,
    directory: &Path,
    per_part: u64,
    next: &mut u32,
    parts: &mut Vec<Part>,
) -> anyhow::Result<()> {
    let mut statement = connection.prepare(query)?;
    let mut rows = statement.query([])?;
    let mut open: Option<(std::io::BufWriter<std::fs::File>, Sha256, u64, String)> = None;
    let close = |open: &mut Option<(std::io::BufWriter<std::fs::File>, Sha256, u64, String)>,
                 parts: &mut Vec<Part>|
     -> anyhow::Result<()> {
        if let Some((mut file, digest, lines, name)) = open.take() {
            file.flush()?;
            parts.push(Part {
                path: name,
                rows: lines,
                sha256: format!("{:x}", digest.finalize()),
            });
        }
        Ok(())
    };
    while let Some(row) = rows.next()? {
        let pnu: String = row.get(0)?;
        let wkb: Vec<u8> = row.get(1)?;
        let origin: String = row.get(2)?;
        if open
            .as_ref()
            .is_some_and(|(_, _, lines, _)| *lines >= per_part)
        {
            close(&mut open, parts)?;
        }
        if open.is_none() {
            let name = format!("part-{:05}.jsonl", *next);
            *next += 1;
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join(&name))
                .with_context(|| format!("cannot create the handoff part {name}"))?;
            open = Some((std::io::BufWriter::new(file), Sha256::new(), 0, name));
        }
        let (file, digest, lines, _) = open.as_mut().context("a part is open")?;
        let mut line = handoff_line(&pnu, &wkb, &origin);
        line.push('\n');
        file.write_all(line.as_bytes())?;
        digest.update(line.as_bytes());
        *lines += 1;
    }
    close(&mut open, parts)
}

/// The bytes of every file under `directory` (the stage: its database, spill and staged files).
fn directory_bytes(directory: &Path) -> anyhow::Result<u64> {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Ok(0);
    };
    for entry in entries {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            total += directory_bytes(&entry.path())?;
        } else if kind.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

/// Creates the handoff directory, which must not exist yet: a handoff is evidence (create-only).
fn create_handoff_directory(directory: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(
        directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;
    std::fs::create_dir(directory).with_context(|| {
        format!(
            "served-boundary bake handoff already exists and is evidence: {}",
            directory.display()
        )
    })
}

/// The run's outcome as the bake script reads it.
fn finish(args: &Args, summary: &Value) -> anyhow::Result<()> {
    if let Some(path) = &args.summary {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut text = serde_json::to_string_pretty(summary)?;
        text.push('\n');
        std::fs::write(path, text).with_context(|| format!("cannot write {}", path.display()))?;
    }
    println!(
        "parcel-boundary-served-gold-summary-json {}",
        serde_json::to_string(summary)?
    );
    Ok(())
}

/// The edition a full build serves and what it applies to it.
struct Edition<'a> {
    /// The `SELECT` of the edition's open rows ([`edition_sql`]).
    sql: &'a str,
    /// Its `source_snapshot_id`.
    name: &'a str,
    /// Every PNU an edit names.
    edited: &'a BTreeSet<String>,
    /// How many of those the edition holds.
    present: usize,
    /// How many end as an upsert.
    upserts: usize,
    silver_snapshot: &'a str,
    through: i64,
    now: &'a str,
}

/// Where a bake's full build writes its handoff: every row it builds, in about `parts` parts.
struct Handoff<'a> {
    directory: &'a Path,
    parts: u64,
    next: &'a mut u32,
    written: &'a mut Vec<Part>,
}

/// What a full build leaves for its commit, summed over its 시도.
struct FullBuild {
    diff: Diff,
    digests: Digests,
    staged: Vec<StagedFile>,
    edition_rows: u64,
    seconds_by_sido: BTreeMap<String, f64>,
    stage_bytes_peak: u64,
}

/// Every 시도 of the edition, of the edits and of the head, one at a time (`rules.txt`: a `DuckDB`
/// port is regional, one pass): each is built from the edition with every edit applied
/// ([`build_sido_sql`]), diffed against the head's current rows, digested and staged, and a bake's
/// handoff parts are written from `built`, before the next 시도 replaces it. The forty million rows
/// with their geometry are never held whole, copied, or read from R2 twice; the 시도's key range
/// skips the edition's files outside it.
#[allow(clippy::too_many_lines)]
fn build_every_sido(
    stage: &DuckDbParquetStage,
    shape: Option<&TableShape>,
    commit: &Commit<'_>,
    run: &str,
    edition: &Edition<'_>,
    work: &Path,
    mut handoff: Option<&mut Handoff<'_>>,
) -> anyhow::Result<FullBuild> {
    let connection = stage.connection();
    // One read of the edition's keys: its 시도 and its rows.
    let mut rows_by_sido: BTreeMap<String, i64> = connection
        .prepare(&format!(
            "SELECT substr(pnu, 1, 2), count(*) FROM ({}) e GROUP BY 1",
            edition.sql
        ))?
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    let edition_rows = u64::try_from(rows_by_sido.values().sum::<i64>())?;
    ensure!(
        edition_rows > 0,
        "{PARCELS} holds no rows of {}",
        edition.name
    );
    for pnu in edition.edited {
        rows_by_sido.entry(pnu[..2].to_owned()).or_default();
    }
    let mut sidos: BTreeSet<String> = rows_by_sido.into_keys().collect();
    if let Some(head) = commit.head {
        // A 시도 the head holds and the edition no longer does still gets its removal markers.
        sidos.extend(head_sidos(connection, commit.table, head)?);
    }
    let expected = edition_rows - u64::try_from(edition.present)? + u64::try_from(edition.upserts)?;
    // A full build's handoff is every row it builds.
    let per_part = handoff
        .as_ref()
        .map_or(1, |handoff| expected.div_ceil(handoff.parts).max(1));
    let (mut diff, mut digests, mut staged) = (Diff::default(), Digests::default(), Vec::new());
    let mut seconds_by_sido = BTreeMap::new();
    let mut stage_bytes_peak = directory_bytes(work)?;
    for (index, sido) in sidos.iter().enumerate() {
        let at = Instant::now();
        let range = sido_range(sido)?;
        connection.execute_batch(&build_sido_sql(
            edition.sql,
            &range,
            true,
            edition.silver_snapshot,
            edition.through,
            edition.now,
        )?)?;
        let part = versions::diff(connection, commit, &range)?;
        digests += versions::digests(connection, commit)?;
        if let Some(shape) = shape.filter(|_| part.keys() > 0) {
            staged.extend(versions::stage(stage, shape, run, commit.versions)?);
        }
        if let Some(handoff) = handoff.as_mut() {
            write_parts(
                connection,
                "SELECT pnu, geometry_wkb, origin FROM built ORDER BY pnu",
                handoff.directory,
                per_part,
                handoff.next,
                handoff.written,
            )?;
        }
        let stage_bytes = directory_bytes(work)?;
        stage_bytes_peak = stage_bytes_peak.max(stage_bytes);
        // The next 시도 replaces this one's tables: a checkpoint lets it reuse their blocks, or the
        // stage holds every 시도's at once (2026-11-22 22:21Z: about 43 GB by the 15th).
        stage.release_replaced_tables()?;
        // The stage shares its disk with the runtime Postgres (root ADR-0599).
        stage.require_room(0)?;
        seconds_by_sido.insert(sido.clone(), at.elapsed().as_secs_f64());
        println!(
            "parcel-boundary-served {sido} ({}/{}): {} rows, {} keys changed in {:.1}s (memory peak {:?} bytes, stage {stage_bytes} bytes)",
            index + 1,
            sidos.len(),
            part.built,
            part.keys(),
            at.elapsed().as_secs_f64(),
            process_memory::cgroup_peak_bytes()
        );
        diff += part;
    }
    ensure!(
        diff.built == expected,
        "the build holds {} rows; the edition's {edition_rows} minus the {} edited parcels it holds \
         plus the {} upserts is {expected}",
        diff.built,
        edition.present,
        edition.upserts
    );
    Ok(FullBuild {
        diff,
        digests,
        staged,
        edition_rows,
        seconds_by_sido,
        stage_bytes_peak,
    })
}

/// What a commit of the versions table records (root ADR-0564 §2, §3-8).
struct Recorded<'a> {
    version_seq: i64,
    current_records: u64,
    fingerprint: (u64, u128),
    pins: &'a BTreeMap<String, i64>,
    keys_changed: u64,
    code: &'a str,
    edition: &'a str,
    through: i64,
}

fn commit_properties(recorded: &Recorded<'_>) -> anyhow::Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        (
            VERSION_SEQ_PROPERTY.to_owned(),
            recorded.version_seq.to_string(),
        ),
        (
            CURRENT_RECORDS_PROPERTY.to_owned(),
            recorded.current_records.to_string(),
        ),
        (
            FINGERPRINT_PROPERTY.to_owned(),
            versions::fingerprint_text(recorded.fingerprint),
        ),
        (
            SOURCE_SNAPSHOTS_PROPERTY.to_owned(),
            versions::pins_property(recorded.pins)?,
        ),
        (
            KEYS_CHANGED_PROPERTY.to_owned(),
            recorded.keys_changed.to_string(),
        ),
        (CODE_VERSION_PROPERTY.to_owned(), recorded.code.to_owned()),
        (
            SERVED_EDITION_PROPERTY.to_owned(),
            recorded.edition.to_owned(),
        ),
        (
            EDITS_THROUGH_PROPERTY.to_owned(),
            recorded.through.to_string(),
        ),
    ]))
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let args = Args::from_env()?;
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(TABLE)?;
    let content_columns = content_columns(versions_contract);
    // A verdict run reads the catalog with its read-only token too (root ADR-0247).
    let catalog_config = if args.no_commit {
        LakehouseCatalogConfig::reader_from_env()?
    } else {
        LakehouseCatalogConfig::from_env()?
    };
    let catalog = IcebergRestCatalog::new(catalog_config.clone())?;
    // A verdict run (no commit) holds no write credential: the committer is the writer's.
    let committer = if args.no_commit {
        None
    } else {
        let committer = LakehouseDataCommitter::from_env().await?;
        committer.create_table_if_absent(versions_contract).await?;
        Some(committer)
    };
    let history = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .unwrap_or_default();
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let base_summary = base.as_ref().and_then(|base| {
        history
            .iter()
            .find(|snapshot| snapshot.snapshot_id == base.snapshot_id)
            .map(|snapshot| snapshot.summary.clone())
    });
    let silver_head = head_of(&catalog, PARCELS)
        .await?
        .with_context(|| format!("{PARCELS} has no snapshot"))?;
    let ledger_head = head_of(&catalog, LEDGER).await?;
    let (ledger_shape, versions_shape, ledger_ingested) = match &committer {
        Some(committer) => (
            Some(committer.table_shape(LEDGER).await?),
            Some(committer.table_shape(TABLE).await?),
            catalog
                .load_ingested_batch_objects(LEDGER)
                .await?
                .unwrap_or_default(),
        ),
        None => (None, None, BTreeSet::new()),
    };

    let work = args.stage_root.as_deref().unwrap_or_else(|| {
        args.summary
            .as_deref()
            .and_then(Path::parent)
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    });
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &reader_read(&catalog_config)?)?;
    // The stage shares its disk with the runtime Postgres: it refuses before it would eat the
    // reserve, here and after every 시도's staged file (root ADR-0599).
    stage.require_room(0)?;
    let mut stage_bytes_peak = directory_bytes(work)?;
    let memory_limit = stage.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    let connection = stage.connection();
    connection.execute_batch("SET preserve_insertion_order = false;")?;
    let (appended_edits, ledger, ledger_batch) =
        ledger_with_handoff(&args, &stage, ledger_shape.as_ref(), ledger_head, &now)?;
    let ledger = check_ledger(ledger)?;
    let through = ledger.iter().map(|edit| edit.change_seq).max().unwrap_or(0);
    let edition = edition_sql(silver_head, &args.snapshot_id);
    let edited: BTreeSet<String> = ledger.iter().map(|edit| edit.feature_id.clone()).collect();
    let buffer = membership_buffer_pixels()?;
    // A ledger with no snapshot yet is not pinned: there is no snapshot to tag.
    let mut pins = BTreeMap::from([(PARCELS.to_owned(), silver_head)]);
    if let Some(ledger_head) = ledger_head {
        pins.insert(LEDGER.to_owned(), ledger_head);
    }
    let code = served::code_version();

    // What to build: every 시도, or the fold's changed PNUs.
    let not_partial = |why: String| -> anyhow::Result<()> {
        println!(
            "parcel-boundary-fold-not-partial {}",
            serde_json::to_string(&why)?
        );
        std::process::exit(NOT_PARTIAL_EXIT);
    };
    let mut fold_scope = None;
    let present = present_pnus(connection, &edition, &edited)?;
    let (states, counts) = fold_edits(&ledger, &present);
    if args.fold {
        let Some(base) = &base else {
            return not_partial("the served table has no snapshot yet".to_owned());
        };
        let served_edition = base.served_edition.clone().unwrap_or_default();
        if served_edition != args.snapshot_id {
            return not_partial(format!(
                "the served Gold holds edition {served_edition}, not {}",
                args.snapshot_id
            ));
        }
        let served_through: i64 = base_summary
            .as_ref()
            .and_then(|summary| summary.get(EDITS_THROUGH_PROPERTY))
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(0);
        let changed: BTreeSet<&str> = ledger
            .iter()
            .filter(|edit| edit.change_seq > served_through)
            .map(|edit| edit.feature_id.as_str())
            .collect();
        if changed.is_empty() {
            return not_partial(format!(
                "no ledgered edit after the served Gold's change_seq {served_through}"
            ));
        }
        let list = changed
            .iter()
            .map(|pnu| format!("'{pnu}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let silver_boxes: BTreeMap<String, served::BoundingBox> = connection
            .prepare(&format!(
                "SELECT pnu, bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y FROM ({edition}) e \
                 WHERE pnu IN ({list}) AND bbox_min_x IS NOT NULL AND bbox_min_y IS NOT NULL \
                   AND bbox_max_x IS NOT NULL AND bbox_max_y IS NOT NULL"
            ))?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    (row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?),
                ))
            })?
            .collect::<Result<_, _>>()?;
        match plan(&ledger, served_through, &silver_boxes, buffer)? {
            FoldPlan::Full(why) => return not_partial(why),
            FoldPlan::Partial {
                after,
                tiles,
                ledger_pnus,
                edited_in_neighbourhood,
            } => {
                fold_scope = Some((
                    base.snapshot_id,
                    served_through,
                    after
                        .iter()
                        .map(|(pnu, state)| (pnu.clone(), state.is_some()))
                        .collect::<BTreeMap<_, _>>(),
                    tiles,
                    ledger_pnus,
                    edited_in_neighbourhood,
                    list,
                ));
            }
        }
    }

    // Build and diff.
    load_edits(connection, &states)?;
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table: TABLE,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: args.snapshot_id.clone(),
        published_at_utc: now.clone(),
    };
    let silver_snapshot = silver_head.to_string();
    let run = versions::run_id(TABLE, &pins, &code, &args.snapshot_id);
    let mut seconds_by_sido = BTreeMap::new();
    let mut base_rows = 0_u64;
    let mut staged = Vec::new();
    let mut parts = Vec::new();
    let mut next = 0_u32;
    create_handoff_directory(&args.output_dir)?;
    let (diff, digests) = if let Some((_, _, _, _, _, _, list)) = &fold_scope {
        connection.execute_batch(&build_fold_sql(list, &silver_snapshot, through, &now)?)?;
        let diff = versions::diff(connection, &commit, &format!("pnu IN ({list})"))?;
        if let Some(shape) = versions_shape.as_ref().filter(|_| diff.keys() > 0) {
            staged.extend(versions::stage(&stage, shape, &run, versions_contract)?);
        }
        (diff, versions::digests(connection, &commit)?)
    } else {
        let full = build_every_sido(
            &stage,
            versions_shape.as_ref(),
            &commit,
            &run,
            &Edition {
                sql: &edition,
                name: &args.snapshot_id,
                edited: &edited,
                present: present.len(),
                upserts: states.values().filter(|state| state.is_some()).count(),
                silver_snapshot: &silver_snapshot,
                through,
                now: &now,
            },
            work,
            Some(&mut Handoff {
                directory: &args.output_dir,
                parts: args.parts,
                next: &mut next,
                written: &mut parts,
            }),
        )?;
        base_rows = full.edition_rows;
        seconds_by_sido = full.seconds_by_sido;
        stage_bytes_peak = stage_bytes_peak.max(full.stage_bytes_peak);
        staged = full.staged;
        (full.diff, full.digests)
    };
    let (fingerprint, current_records) = versions::after_digests(&commit, &diff, digests)?;

    // Commit: every file is staged, so no borrow of the stage lives across an await.
    let commit_files = (versions_shape.is_some() && diff.keys() > 0).then_some(staged);
    if let (Some(committer), Some((batch, file))) = (&committer, ledger_batch) {
        append_ledger(committer, &batch, file, &ledger_ingested).await?;
    }
    let gold_snapshot = if let (Some(committer), Some(files)) = (&committer, commit_files) {
        let properties = commit_properties(&Recorded {
            version_seq,
            current_records,
            fingerprint,
            pins: &pins,
            keys_changed: diff.keys(),
            code: &code,
            edition: &args.snapshot_id,
            through,
        })?;
        let snapshot =
            versions::append(committer, &catalog, &commit, &files, &run, properties).await?;
        versions::pin(&catalog, TABLE, snapshot, &pins).await?;
        snapshot.to_string()
    } else {
        head.map_or_else(|| "none".to_owned(), |head| head.to_string())
    };
    let connection = stage.connection();

    // The handoff: the fold's neighbourhood (a full build wrote every current row above).
    let mut scope = json!({"handoff_scope": "full", "full_reason": "a new edition, or a fold that could not be partial"});
    let served_rows;
    if let Some((base_snapshot, _, after, tiles, ledger_pnus, in_neighbourhood, _)) = &fold_scope {
        let quoted = |pnus: &[String]| {
            let list = pnus
                .iter()
                .map(|pnu| format!("'{pnu}'"))
                .collect::<Vec<_>>()
                .join(", ");
            if list.is_empty() {
                "''".to_owned()
            } else {
                list
            }
        };
        let neighbourhood = format!(
            "SELECT pnu, geometry_wkb, 'source' AS origin FROM ({edition}) e \
               WHERE {} AND pnu NOT IN ({}) \
             UNION ALL SELECT pnu, geometry_wkb, 'edit' FROM served_edits WHERE pnu IN ({}) \
             ORDER BY pnu",
            silver_predicate(tiles, buffer),
            quoted(ledger_pnus),
            quoted(in_neighbourhood),
        );
        write_parts(
            connection,
            &neighbourhood,
            &args.output_dir,
            u64::MAX,
            &mut next,
            &mut parts,
        )?;
        let mut change_text = String::new();
        for (pnu, upsert) in after {
            change_text.push_str(&change_list_line(pnu, *upsert));
            change_text.push('\n');
        }
        let change_name = format!(
            "{}.{CHANGE_LIST_NAME}",
            args.output_dir
                .file_name()
                .and_then(|name| name.to_str())
                .context("the handoff directory has a name")?
        );
        let change_path = args
            .output_dir
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&change_name);
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&change_path)?
            .write_all(change_text.as_bytes())?;
        served_rows = current_records;
        scope = json!({
            "handoff_scope": "partial",
            "partial": {
                "base_snapshot": base_snapshot.to_string(),
                "neighbourhood_zoom": NEIGHBOURHOOD_ZOOM,
                "neighbourhood_tiles": tiles.iter().map(|(x, y)| [x, y]).collect::<Vec<_>>(),
                "change_list": {
                    "path": change_name,
                    "rows": after.len(),
                    "sha256": format!("{:x}", Sha256::digest(change_text.as_bytes())),
                },
            },
            "changed_parcels": {
                "upsert": after.values().filter(|upsert| **upsert).count(),
                "delete": after.values().filter(|upsert| !**upsert).count(),
            },
        });
    } else {
        served_rows = parts.iter().map(|part| part.rows).sum();
        ensure!(
            served_rows == current_records,
            "the handoff holds {served_rows} rows, the served table {current_records}"
        );
    }
    ensure!(
        !parts.is_empty() || fold_scope.is_some(),
        "the served set is empty; a bake of nothing would erase the layer"
    );

    let mut summary = json!({
        "schema_version": SUMMARY_SCHEMA,
        "job": JOB,
        "engine": "duckdb",
        "generated_at_utc": now,
        "status": "ready",
        "unit": UNIT,
        "feature_id_property": "pnu",
        "geometry_srid": GEOMETRY_SRID,
        "canonical_iceberg_snapshot_id": gold_snapshot,
        "edits_through_change_seq": through,
        "edits_in_handoff": appended_edits,
        "edits_appended_to_ledger": if args.no_commit { 0 } else { appended_edits },
        "edits_in_ledger": ledger.len(),
        "served_row_count": parts.iter().map(|part| part.rows).sum::<u64>(),
        "current_records": current_records,
        "source_row_count": if fold_scope.is_some() { served_rows } else { base_rows },
        "output_path": args.output_dir.display().to_string(),
        "upserts": counts.upserts,
        "deletes": counts.deletes,
        "deletes_of_absent_features": counts.deletes_of_absent_features,
        "silver_iceberg_snapshot_id": silver_snapshot,
        "source_snapshot_id": args.snapshot_id,
        "handoff_parts": parts,
        "table": TABLE,
        "no_commit": args.no_commit,
        "diff": diff,
        "seconds_by_sido": seconds_by_sido,
        "duckdb_memory_limit_bytes": memory_limit,
        "stage_bytes_peak": stage_bytes_peak,
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
        "seconds": started.elapsed().as_secs_f64(),
    });
    if let (Value::Object(summary), Value::Object(scope)) = (&mut summary, scope) {
        summary.extend(scope);
    }
    finish(&args, &summary)
}

// one-off: until the parcel tile bake reads gold.parcel_boundary_served_versions (its switch deletes
// the Spark job, this command and its lane).
/// The edition the replaced table's build `build` served: the one `source_snapshot_id` its source
/// rows carry (its edit rows carry the edit's).
fn replaced_edition(connection: &Connection, build: i64) -> anyhow::Result<String> {
    let editions: Vec<String> = connection
        .prepare(&format!(
            "SELECT DISTINCT source_snapshot_id FROM {LAKEHOUSE_CATALOG}.{REPLACED} \
             AT (VERSION => {build}) WHERE origin = 'source' LIMIT 2"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    let [edition] = editions.as_slice() else {
        bail!("{REPLACED} build {build} holds the editions {editions:?}, not one");
    };
    ensure_snapshot_id(edition)?;
    Ok(edition.clone())
}

/// `build-parcel-boundary-served-versions`: the versions table as the shadow of the replaced
/// table's newest build, while the parcel tile bake serves that table (root ADR-0564 §6;
/// `gold-versions-build.sh parcel-boundary-served`). The inputs are that build's, each as of its
/// commit (`gold_versions_commit::shadow_of`), and its edition is the one its rows carry, so the
/// takeover verdict compares the two engines over one set of inputs. Nothing is handed to the bake
/// and no edit is appended to the ledger: the bake's own run did both.
///
/// Nothing to do when the base commit was built from the same pins, edition and code. Otherwise
/// every 시도 is built, diffed and staged (as the bake's full build), and one commit appends the keys
/// that changed and records the pins, even with no key changed: the verdict finds the shadow commit
/// by its pins.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_shadow() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SHADOW_SUMMARY_ENV)?;
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(TABLE)?;
    let content_columns = content_columns(versions_contract);
    let catalog_config = LakehouseCatalogConfig::from_env()?;
    let catalog = IcebergRestCatalog::new(catalog_config.clone())?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let created = committer.create_table_if_absent(versions_contract).await?;
    let history = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist after its creation"))?;
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let shadow = versions::shadow_of(&catalog, TABLE, REPLACED)
        .await?
        .with_context(|| format!("{TABLE} is not a shadow of {REPLACED}"))?;
    let pins = shadow.pins.clone();
    let shadow_of = json!({"table": shadow.replaced, "build": shadow.build});
    let code = served::code_version();
    // Mends the tags a run that died between its commit and its tags left (as every versions lane).
    if let Some(base) = &base {
        versions::pin(&catalog, TABLE, base.snapshot_id, &base.pins).await?;
    }
    // Asked before the DuckDB session opens: a borrow of the session does not live across an await.
    let shape = committer.table_shape(TABLE).await?;

    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &reader_read(&catalog_config)?)?;
    // The stage shares its disk with the runtime Postgres: it refuses before it would eat the
    // reserve, here and after every 시도's staged file (root ADR-0599).
    stage.require_room(0)?;
    let memory_limit = stage.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    let connection = stage.connection();
    connection.execute_batch("SET preserve_insertion_order = false;")?;
    let edition_name = replaced_edition(connection, shadow.build)?;
    let unchanged = base.as_ref().is_some_and(|base| {
        base.pins == pins
            && base.code_version == code
            && base.served_edition.as_deref() == Some(edition_name.as_str())
    });
    if unchanged {
        write_summary(
            &summary_path,
            &json!({
                "job": SHADOW_JOB, "engine": "duckdb", "table": TABLE, "outcome": "unchanged",
                "pins": pins, "served_edition": edition_name, "shadow_of": shadow_of,
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!(
            "gold-versions {TABLE} nothing to do: the shadow of {REPLACED} build {} is the base commit",
            shadow.build
        );
        return Ok(());
    }

    let silver = *pins
        .get(PARCELS)
        .with_context(|| format!("{REPLACED} build {} read no {PARCELS}", shadow.build))?;
    let (ledger, _) = read_ledger(connection, pins.get(LEDGER).copied())?;
    let ledger = check_ledger(ledger)?;
    let through = ledger.iter().map(|edit| edit.change_seq).max().unwrap_or(0);
    let edition = edition_sql(silver, &edition_name);
    let edited: BTreeSet<String> = ledger.iter().map(|edit| edit.feature_id.clone()).collect();
    let present = present_pnus(connection, &edition, &edited)?;
    let (states, counts) = fold_edits(&ledger, &present);
    load_edits(connection, &states)?;
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table: TABLE,
        versions: versions_contract,
        content_columns,
        head,
        base,
        version_seq,
        source_snapshot_id: edition_name.clone(),
        published_at_utc: now.clone(),
    };
    let run = versions::run_id(TABLE, &pins, &code, &edition_name);
    let silver_snapshot = silver.to_string();
    let full = build_every_sido(
        &stage,
        Some(&shape),
        &commit,
        &run,
        &Edition {
            sql: &edition,
            name: &edition_name,
            edited: &edited,
            present: present.len(),
            upserts: states.values().filter(|state| state.is_some()).count(),
            silver_snapshot: &silver_snapshot,
            through,
            now: &now,
        },
        work,
        None,
    )?;
    let (fingerprint, current_records) =
        versions::after_digests(&commit, &full.diff, full.digests)?;
    let properties = commit_properties(&Recorded {
        version_seq,
        current_records,
        fingerprint,
        pins: &pins,
        keys_changed: full.diff.keys(),
        code: &code,
        edition: &edition_name,
        through,
    })?;

    // Commit: every file is staged, so no borrow of the stage lives across an await.
    let snapshot = versions::append(
        &committer,
        &catalog,
        &commit,
        &full.staged,
        &run,
        properties,
    )
    .await?;
    drop(stage);
    let tags = versions::pin(&catalog, TABLE, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": SHADOW_JOB, "engine": "duckdb", "table": TABLE, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "served_edition": edition_name,
            "shadow_of": shadow_of, "code_version": code, "edits_in_ledger": ledger.len(),
            "edits_through_change_seq": through, "upserts": counts.upserts,
            "deletes": counts.deletes, "edition_rows": full.edition_rows, "diff": full.diff,
            "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": full.staged.len(), "tags": tags, "seconds_by_sido": full.seconds_by_sido,
            "duckdb_memory_limit_bytes": memory_limit, "stage_bytes_peak": full.stage_bytes_peak,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "committed {TABLE}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records}, the shadow of \
         {REPLACED} build {} in {:.0}s",
        full.diff.keys(),
        full.diff.added,
        full.diff.changed,
        full.diff.removed,
        shadow.build,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
