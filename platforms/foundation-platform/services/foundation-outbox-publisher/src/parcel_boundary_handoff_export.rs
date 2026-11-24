//! `export-parcel-boundary-handoff`: the v2 handoff the ai-server parcel tile bake reads
//! (`served_gold_common`, "The v2 handoff": create-only JSONL parts and a summary), written from
//! what the served boundaries' build left: the current rows of a committed snapshot of
//! `gold.parcel_boundary_served_versions` (its head by default), or a rehearsal's files (root
//! ADR-0618 §9). It was the last step of `build-parcel-boundary-served`, which also wrote a fold's
//! partial handoff; nothing reads that one (the bake reads a fold as the served table's change
//! between two snapshots, root ADR-0618), and it is gone.
// one-off: until the parcel tile bake reads the 시도 partitions of the served table directly (root
// ADR-0618 §10): this command goes with the bake's move.

use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context};
use foundation_publisher_silver::{
    duckdb::Connection,
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    parcel_boundary_served_stage::{handoff_line, GEOMETRY_SRID, UNIT},
    parcel_nearby_stage::{NearbyPins, Source},
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    gold_versions_support::{contract, lakehouse_reader_read},
    kind_versions_build::{sido_range, BOUNDARY_SERVED_BUILD},
    public_data_control_support::{optional_env_value, required_env_value},
};

const ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_HANDOFF_";
const SUMMARY_SCHEMA: &str = "foundation-platform.polygon_served_gold.v2";
const JOB: &str = "parcel_boundary_served_gold";
const DEFAULT_PARTS: u64 = 64;
/// The columns of a served row the handoff carries or summarises.
const COLUMNS: [&str; 6] = [
    "pnu",
    "geometry_wkb",
    "origin",
    "source_snapshot_id",
    "silver_iceberg_snapshot_id",
    "edits_through_change_seq",
];

fn env(name: &str) -> anyhow::Result<Option<String>> {
    optional_env_value(&format!("{ENV}{name}"))
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
    type Open = Option<(std::io::BufWriter<std::fs::File>, Sha256, u64, String)>;
    let mut statement = connection.prepare(query)?;
    let mut rows = statement.query([])?;
    let mut open: Open = None;
    let close = |open: &mut Open, parts: &mut Vec<Part>| -> anyhow::Result<()> {
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

/// Writes the handoff of the served rows.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let output = PathBuf::from(required_env_value(&format!("{ENV}OUTPUT_DIR"))?);
    let parts_wanted = env("PARTS")?
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(DEFAULT_PARTS);
    ensure!(parts_wanted > 0, "a handoff needs at least one part");
    let rehearsal = env("REHEARSAL_DIR")?.map(PathBuf::from);
    let snapshot_asked = env("SNAPSHOT")?
        .map(|value| value.parse::<i64>())
        .transpose()?;
    ensure!(
        rehearsal.is_none() || snapshot_asked.is_none(),
        "a handoff is of a committed snapshot or of a rehearsal, not both"
    );
    let table = BOUNDARY_SERVED_BUILD.table;
    let catalog_config = LakehouseCatalogConfig::reader_from_env()?;
    let snapshot = match (&rehearsal, snapshot_asked) {
        (Some(_), _) => None,
        (None, Some(snapshot)) => Some(snapshot),
        (None, None) => Some(
            IcebergRestCatalog::new(catalog_config.clone())?
                .load_main_snapshot_history(table)
                .await?
                .and_then(|history| history.first().map(|snapshot| snapshot.snapshot_id))
                .with_context(|| format!("{table} holds no snapshot to hand off"))?,
        ),
    };
    std::fs::create_dir_all(
        output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;
    std::fs::create_dir(&output).with_context(|| {
        format!(
            "served-boundary bake handoff already exists and is evidence: {}",
            output.display()
        )
    })?;

    let work = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage =
        DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_reader_read(&catalog_config)?)?;
    let connection = stage.connection();
    let columns = COLUMNS.join(", ");
    // The served rows of one 시도, as a relation: the rehearsal's file, or the snapshot's current rows.
    let pins = NearbyPins(
        snapshot
            .map(|snapshot| BTreeMap::from([(table.to_owned(), snapshot)]))
            .unwrap_or_default(),
    );
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &pins,
    };
    let versions = contract(table)?;
    let latest = versions
        .current_row_latest()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    let rows_of = |sido: &str| -> anyhow::Result<String> {
        Ok(match &rehearsal {
            Some(dir) => {
                let text = dir
                    .join(format!("sido={sido}.parquet"))
                    .to_str()
                    .context("the rehearsal directory must be UTF-8")?
                    .replace('\\', "/");
                ensure!(
                    !text.contains('\''),
                    "the rehearsal directory holds a quote"
                );
                format!("SELECT {columns} FROM read_parquet('{text}')")
            }
            None => latest.current_rows_sql(&source.of(table)?, &COLUMNS, &sido_range(sido)?),
        })
    };
    // Every 시도 the served rows lie in, in PNU order.
    let sidos: Vec<String> = match &rehearsal {
        Some(dir) => {
            let mut sidos = Vec::new();
            for entry in
                std::fs::read_dir(dir).with_context(|| format!("cannot list {}", dir.display()))?
            {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if let Some(sido) = name
                    .strip_prefix("sido=")
                    .and_then(|rest| rest.strip_suffix(".parquet"))
                {
                    sidos.push(sido.to_owned());
                }
            }
            sidos.sort();
            sidos
        }
        None => connection
            .prepare(&format!(
                "SELECT DISTINCT substr(pnu, 1, 2) FROM ({}) ORDER BY 1",
                latest.current_rows_sql(&source.of(table)?, &["pnu"], "TRUE")
            ))?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<_, _>>()?,
    };
    let mut total = 0_u64;
    let (mut editions, mut through, mut silver) = (
        std::collections::BTreeSet::new(),
        0_i64,
        std::collections::BTreeSet::new(),
    );
    for sido in &sidos {
        let (rows, edition, held_through, held_silver): (
            i64,
            Option<String>,
            Option<i64>,
            Option<String>,
        ) = connection.query_row(
            &format!(
                "SELECT count(*), max(source_snapshot_id) FILTER (WHERE origin = 'source'), \
                       max(edits_through_change_seq), max(silver_iceberg_snapshot_id) FROM ({})",
                rows_of(sido)?
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        total += u64::try_from(rows)?;
        editions.extend(edition);
        through = through.max(held_through.unwrap_or(0));
        silver.extend(held_silver);
    }
    ensure!(
        total > 0,
        "the served set is empty; a bake of nothing would erase the layer"
    );
    ensure!(
        editions.len() == 1,
        "the served rows carry the editions {editions:?}, not one"
    );
    let per_part = total.div_ceil(parts_wanted).max(1);
    let (mut next, mut parts) = (0_u32, Vec::new());
    for sido in &sidos {
        write_parts(
            connection,
            &format!(
                "SELECT pnu, geometry_wkb, origin FROM ({}) ORDER BY pnu",
                rows_of(sido)?
            ),
            &output,
            per_part,
            &mut next,
            &mut parts,
        )?;
    }
    let written: u64 = parts.iter().map(|part| part.rows).sum();
    ensure!(
        written == total,
        "the handoff holds {written} rows, the served rows {total}"
    );
    let summary = json!({
        "schema_version": SUMMARY_SCHEMA,
        "job": JOB,
        "engine": "duckdb",
        "generated_at_utc": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "status": "ready",
        "unit": UNIT,
        "feature_id_property": "pnu",
        "geometry_srid": GEOMETRY_SRID,
        "canonical_iceberg_snapshot_id": snapshot.map_or_else(|| "rehearsal".to_owned(), |snapshot| snapshot.to_string()),
        "edits_through_change_seq": through,
        "served_row_count": written,
        "current_records": total,
        "output_path": output.display().to_string(),
        "silver_iceberg_snapshot_id": silver.iter().next_back(),
        "source_snapshot_id": editions.iter().next(),
        "handoff_parts": parts,
        "handoff_scope": "full",
        "table": table,
        "rehearsal_dir": rehearsal.as_ref().map(|dir| dir.display().to_string()),
    });
    if let Some(path) = env("SUMMARY")?.map(PathBuf::from) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut text = serde_json::to_string_pretty(&summary)?;
        text.push('\n');
        std::fs::write(&path, text).with_context(|| format!("cannot write {}", path.display()))?;
    }
    println!(
        "parcel-boundary-served-gold-summary-json {}",
        serde_json::to_string(&summary)?
    );
    Ok(())
}
