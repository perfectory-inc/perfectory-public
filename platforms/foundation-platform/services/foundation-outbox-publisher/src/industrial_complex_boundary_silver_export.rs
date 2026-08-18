//! Industrial-complex boundary Bronze-to-Silver handoff export command.
//!
//! `silver.industrial_complex_boundaries` had nineteen defined columns and nothing writing them.
//! This command is the reading half of that producer: it opens the provider's boundary shapefile
//! out of its Bronze zip, turns each polygon into the Silver row shape, and writes the JSONL
//! handoff that `infra/lakehouse/spark/jobs/industrial_complex_boundaries_handoff_to_silver.py`
//! loads.
//!
//! The zip holds an ESRI shapefile: `DAM_DAN.shp` carries the polygons, `DAM_DAN.dbf` carries one
//! attribute row per polygon in the same order, `DAM_DAN.prj` declares the CRS, and `DAM_DAN.cpg`
//! declares the attribute encoding. Only two attribute columns are read, both ASCII: `DAN_ID`,
//! which is the official industrial-complex code and the join key, and `DANJI_TYPE`, which is
//! counted into the summary but not carried.
//!
//! **`DANJI_TYPE` is not `boundary_kind`.** Measured across all 1,344 boundaries that join to the
//! profile, its four values agree with the profile's own `lrstt_ty` with zero exceptions —
//! 1=국가(64), 2=일반(750), 3=도시첨단(49), 4=농공(481) — so it is the *complex* kind, which
//! `silver.industrial_complexes.complex_kind` already carries. Writing it into `boundary_kind`
//! would put the same fact in two tables and would say something false: `boundary_kind` is about
//! the standing of the boundary, and its allowed values are `official`, `derived`, `corrected`,
//! and `draft`.
//!
//! The CRS is checked rather than assumed. The rows declare `geometry_srid = 5186` (root ADR-0042),
//! so a `.prj` that stops saying Korea 2000 Central Belt 2010 has to stop the run: relabelling a
//! different projection as 5186 produces coordinates nothing downstream can detect as wrong.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read as _,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use lakehouse_application::{
    build_industrial_complex_boundary_silver_handoff,
    normalize_industrial_complex_boundary_silver_rows, IndustrialComplexBoundarySilverRowsInput,
    IndustrialComplexBoundarySource, BOUNDARY_KIND_OFFICIAL, INDUSTRIAL_COMPLEX_BOUNDARY_SRID,
};
use serde_json::json;
use zip::ZipArchive;

use crate::{
    dbase_table::DbaseTable,
    industrial_complex_bronze_raw_jsonl_export::locate_single_bronze_object,
    public_data_control_support::{optional_env_value, required_env_value},
    shapefile_polygon_reader::read_shapefile_polygons,
};

const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_SILVER_HANDOFF";
const DEFAULT_SOURCE_SLUG: &str = "vworldkr__sandan_boundary";
const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_boundary_silver_handoff_export.v1";

/// Attribute column carrying the official industrial-complex code.
const COMPLEX_CODE_COLUMN: &str = "DAN_ID";

/// Attribute column carrying the complex kind. Counted, never carried — see the module docs.
const COMPLEX_TYPE_COLUMN: &str = "DANJI_TYPE";

/// Projected coordinate system name the boundary source declares in its `.prj`.
const EXPECTED_PROJECTION_NAME: &str = "Korea_2000_Korea_Central_Belt_2010";

struct BoundarySilverExportConfig {
    bronze_local_object_root: PathBuf,
    source_slug: String,
    /// Exact Bronze zip to decode; required once snapshots accumulate under the prefix.
    source_object: Option<String>,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    source_snapshot_id: Option<String>,
    valid_from_utc: Option<DateTime<Utc>>,
}

/// What one run read and what it could not turn into a row.
struct BoundarySilverExportReport {
    bronze_object_key: String,
    source_snapshot_id: String,
    shapefile_record_count: usize,
    row_count: usize,
    /// Records whose shape type is the shapefile null shape: present, but drawing nothing.
    null_shape_count: usize,
    /// Rings the reader dropped because their coordinates did not determine a winding.
    collapsed_ring_count: usize,
    /// Records that carry geometry this normalizer refused, with the reason.
    skipped: Vec<(String, String)>,
    /// How many boundaries each `DANJI_TYPE` value accounts for.
    complex_type_counts: BTreeMap<String, u64>,
    /// Area in square metres, largest first, for the handful the summary quotes.
    largest_areas: Vec<(String, f64)>,
}

/// Runs the industrial-complex boundary Bronze-to-Silver handoff export.
///
/// # Errors
/// Returns an error when the Bronze object cannot be located or decoded, when the shapefile and its
/// attribute table disagree, when the declared projection is not the expected one, or when the
/// output path already holds an earlier export.
pub fn run() -> anyhow::Result<()> {
    let config = BoundarySilverExportConfig::from_env()?;
    let report = export_boundary_handoff(&config)?;
    tracing::info!(
        bronze_object_key = %report.bronze_object_key,
        source_snapshot_id = %report.source_snapshot_id,
        shapefile_record_count = report.shapefile_record_count,
        row_count = report.row_count,
        null_shape_count = report.null_shape_count,
        collapsed_ring_count = report.collapsed_ring_count,
        skipped_count = report.skipped.len(),
        output_path = %config.output_path.display(),
        "industrial-complex boundary Silver handoff export succeeded"
    );
    Ok(())
}

fn export_boundary_handoff(
    config: &BoundarySilverExportConfig,
) -> anyhow::Result<BoundarySilverExportReport> {
    let object_path = locate_single_bronze_object(
        &config.bronze_local_object_root,
        config.source_slug.as_str(),
        config.source_object.as_deref(),
        "zip",
    )
    .with_context(|| format!("set {PREFIX}_SOURCE_OBJECT to the exact object to export"))?;
    let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, &object_path)?;
    let shapefile = read_shapefile_from_bronze_zip(&object_path)?;
    ensure_expected_projection(&shapefile.projection, &bronze_object_key)?;

    let boundaries = join_geometry_to_attributes(&shapefile)?;
    let source_snapshot_id = config
        .source_snapshot_id
        .clone()
        .unwrap_or_else(|| default_source_snapshot_id(&object_path));
    let now = Utc::now();
    let valid_from_utc = config.valid_from_utc.unwrap_or(now);

    let mut rows = Vec::with_capacity(boundaries.rows.len());
    let mut skipped = Vec::new();
    let mut complex_type_counts = BTreeMap::<String, u64>::new();
    for boundary in boundaries.rows {
        *complex_type_counts
            .entry(boundary.complex_type.clone())
            .or_insert(0) += 1;
        let source = [IndustrialComplexBoundarySource {
            official_complex_code: boundary.official_complex_code.clone(),
            geometry: boundary.geometry,
        }];
        // One record at a time, so a shape the normalizer refuses is a counted drop rather than a
        // run that loses the other 1,362 with it.
        match normalize_industrial_complex_boundary_silver_rows(
            &IndustrialComplexBoundarySilverRowsInput {
                boundaries: &source,
                source_record_id: bronze_object_key.as_str(),
                source_snapshot_id: source_snapshot_id.as_str(),
                valid_from_utc,
                ingested_at_utc: now,
            },
        ) {
            Ok(normalized) => rows.extend(normalized),
            Err(error) => skipped.push((boundary.official_complex_code, error.to_string())),
        }
    }

    ensure!(
        !rows.is_empty(),
        "the boundary source produced no rows at all; {} records were read",
        shapefile.records.len()
    );
    let handoff = build_industrial_complex_boundary_silver_handoff(&rows)
        .context("failed to build the industrial-complex boundary Silver handoff")?;

    write_handoff(&config.output_path, handoff.jsonl.as_str())?;

    let mut largest_areas = rows
        .iter()
        .map(|row| (row.official_complex_code.clone(), row.area_sqm_calculated))
        .collect::<Vec<_>>();
    largest_areas.sort_by(|left, right| right.1.total_cmp(&left.1));
    largest_areas.truncate(10);

    let report = BoundarySilverExportReport {
        bronze_object_key,
        source_snapshot_id,
        shapefile_record_count: shapefile.records.len(),
        row_count: rows.len(),
        null_shape_count: boundaries.null_shape_count,
        collapsed_ring_count: shapefile
            .records
            .iter()
            .map(|record| record.collapsed_ring_count)
            .sum(),
        skipped,
        complex_type_counts,
        largest_areas,
    };
    if let Some(summary_path) = &config.summary_path {
        write_summary(config, &handoff, &report, summary_path)?;
    }
    Ok(report)
}

impl BoundarySilverExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: PathBuf::from(required_env_value(&format!(
                "{PREFIX}_BRONZE_ROOT"
            ))?),
            source_slug: optional_env_value(&format!("{PREFIX}_SOURCE_SLUG"))?
                .unwrap_or_else(|| DEFAULT_SOURCE_SLUG.to_owned()),
            source_object: optional_env_value(&format!("{PREFIX}_SOURCE_OBJECT"))?,
            output_path: PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_PATH"))?),
            summary_path: optional_env_value(&format!("{PREFIX}_SUMMARY_PATH"))?.map(PathBuf::from),
            source_snapshot_id: optional_env_value(&format!("{PREFIX}_SOURCE_SNAPSHOT_ID"))?,
            valid_from_utc: optional_env_value(&format!("{PREFIX}_VALID_FROM_UTC"))?
                .map(|raw| parse_utc(&format!("{PREFIX}_VALID_FROM_UTC"), raw.as_str()))
                .transpose()?,
        })
    }
}

/// What the three shapefile members this command reads jointly say.
struct ShapefileBundle {
    attributes: Vec<u8>,
    projection: String,
    records: Vec<crate::shapefile_polygon_reader::ShapefilePolygonRecord>,
}

/// One boundary as the shapefile and its attribute table jointly state it.
struct JoinedBoundary {
    official_complex_code: String,
    complex_type: String,
    geometry: lakehouse_application::ParsedPolygonalGeometry,
}

struct JoinedBoundaries {
    rows: Vec<JoinedBoundary>,
    null_shape_count: usize,
}

fn read_shapefile_from_bronze_zip(object_path: &Path) -> anyhow::Result<ShapefileBundle> {
    let file = fs::File::open(object_path).with_context(|| {
        format!(
            "failed to open industrial-complex boundary Bronze zip {}",
            object_path.display()
        )
    })?;
    let mut archive = ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read industrial-complex boundary Bronze zip {}",
            object_path.display()
        )
    })?;

    let mut names_by_extension = BTreeMap::<String, Vec<(usize, String)>>::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("failed to inspect zip entry {index}"))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry.name().to_owned();
        let Some(extension) = name.rsplit('.').next().map(str::to_ascii_lowercase) else {
            continue;
        };
        names_by_extension
            .entry(extension)
            .or_default()
            .push((index, name));
    }

    let geometry_index = single_entry(&names_by_extension, "shp")?;
    let attribute_index = single_entry(&names_by_extension, "dbf")?;
    let projection_index = single_entry(&names_by_extension, "prj")?;
    let geometry = read_zip_entry(&mut archive, geometry_index)?;
    let attributes = read_zip_entry(&mut archive, attribute_index)?;
    let projection_bytes = read_zip_entry(&mut archive, projection_index)?;
    let projection = String::from_utf8(projection_bytes)
        .context("the shapefile projection sidecar is not UTF-8")?;

    let records = read_shapefile_polygons(&geometry)
        .with_context(|| format!("failed to read the polygons in {}", object_path.display()))?;
    Ok(ShapefileBundle {
        attributes,
        projection,
        records,
    })
}

fn single_entry(
    names_by_extension: &BTreeMap<String, Vec<(usize, String)>>,
    extension: &str,
) -> anyhow::Result<usize> {
    let entries = names_by_extension
        .get(extension)
        .with_context(|| format!("the boundary Bronze zip holds no .{extension} member"))?;
    let [(index, _)] = entries.as_slice() else {
        let names = entries
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("the boundary Bronze zip holds {} .{extension} members ({names}); the snapshot is ambiguous", entries.len());
    };
    Ok(*index)
}

fn read_zip_entry(archive: &mut ZipArchive<fs::File>, index: usize) -> anyhow::Result<Vec<u8>> {
    let mut entry = archive
        .by_index(index)
        .with_context(|| format!("failed to open zip entry {index}"))?;
    let mut bytes = Vec::new();
    entry
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read zip entry {index}"))?;
    Ok(bytes)
}

/// Refuses a source whose declared projection is not the one the rows claim.
fn ensure_expected_projection(projection: &str, bronze_object_key: &str) -> anyhow::Result<()> {
    ensure!(
        projection.contains(EXPECTED_PROJECTION_NAME),
        "{bronze_object_key} declares a projection that is not {EXPECTED_PROJECTION_NAME}, so its \
         coordinates are not EPSG:{INDUSTRIAL_COMPLEX_BOUNDARY_SRID}; loading them under that srid \
         would misplace every complex. The .prj says: {projection}"
    );
    Ok(())
}

/// Joins polygons to attributes by position, which is how a shapefile relates its two files.
fn join_geometry_to_attributes(shapefile: &ShapefileBundle) -> anyhow::Result<JoinedBoundaries> {
    let table = DbaseTable::open(&shapefile.attributes)
        .context("the boundary attribute table is unreadable")?;
    ensure!(
        table.record_count() == shapefile.records.len(),
        "the boundary shapefile holds {} records but its attribute table declares {}; the two are \
         joined by position and cannot be joined at all when they disagree",
        shapefile.records.len(),
        table.record_count()
    );
    let code_field = table.field(COMPLEX_CODE_COLUMN).with_context(|| {
        format!(
            "the boundary attribute table has no {COMPLEX_CODE_COLUMN} column; columns present: {}",
            table
                .fields()
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })?;
    let type_field = table.field(COMPLEX_TYPE_COLUMN).with_context(|| {
        format!("the boundary attribute table has no {COMPLEX_TYPE_COLUMN} column")
    })?;

    let mut rows = Vec::with_capacity(shapefile.records.len());
    let mut null_shape_count = 0_usize;
    let mut seen = BTreeSet::<String>::new();
    for (index, record) in shapefile.records.iter().enumerate() {
        let Some(attributes) = table.record(index)? else {
            bail!(
                "attribute record {index} is marked deleted while its polygon is not; the two \
                 files no longer line up by position"
            );
        };
        let official_complex_code =
            ascii_cell(attributes.raw(code_field), COMPLEX_CODE_COLUMN, index)?;
        ensure!(
            !official_complex_code.is_empty(),
            "attribute record {index} has an empty {COMPLEX_CODE_COLUMN}, so its polygon names no \
             complex"
        );
        ensure!(
            seen.insert(official_complex_code.clone()),
            "{COMPLEX_CODE_COLUMN} {official_complex_code} appears twice; the contract allows at \
             most one active {BOUNDARY_KIND_OFFICIAL} boundary per complex"
        );
        let complex_type = ascii_cell(attributes.raw(type_field), COMPLEX_TYPE_COLUMN, index)?;
        let Some(geometry) = record.geometry.clone() else {
            null_shape_count += 1;
            continue;
        };
        rows.push(JoinedBoundary {
            official_complex_code,
            complex_type,
            geometry,
        });
    }
    Ok(JoinedBoundaries {
        rows,
        null_shape_count,
    })
}

/// Reads one attribute cell as trimmed ASCII.
///
/// The attribute table is EUC-KR, which its `.cpg` declares, but both columns this command reads
/// are codes. A non-ASCII byte in one of them means the column is not what this command thinks it
/// is, and guessing an encoding for it would turn that into a silently wrong complex code.
fn ascii_cell(raw: Option<&[u8]>, column: &str, index: usize) -> anyhow::Result<String> {
    let raw = raw
        .with_context(|| format!("attribute record {index} is shorter than its {column} column"))?;
    let text = std::str::from_utf8(raw)
        .with_context(|| format!("attribute record {index} {column} is not ASCII"))?;
    ensure!(
        text.is_ascii(),
        "attribute record {index} {column} is not ASCII"
    );
    Ok(text.trim().to_owned())
}

/// Writes the handoff, refusing to replace an earlier one.
///
/// A handoff is evidence of what a Silver load was built from; a rerun that truncates the previous
/// file destroys that record.
fn write_handoff(output_path: &Path, jsonl: &str) -> anyhow::Result<()> {
    if output_path.exists() {
        bail!(
            "industrial-complex boundary Silver handoff already exists and is append-only \
             evidence: {}",
            output_path.display()
        );
    }
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    fs::write(output_path, jsonl)
        .with_context(|| format!("failed to write {}", output_path.display()))
}

fn write_summary(
    config: &BoundarySilverExportConfig,
    handoff: &lakehouse_application::IndustrialComplexBoundarySilverHandoff,
    report: &BoundarySilverExportReport,
    summary_path: &Path,
) -> anyhow::Result<()> {
    let mut evidence_limitations = vec![
        "local_bronze_to_jsonl_export_only",
        "does_not_run_the_spark_handoff_to_silver_job",
        "does_not_write_iceberg_table",
        // The two counts this stage cannot produce. Both need the complex list, which lives in
        // `silver.industrial_complexes`, and the writer's join is where they become knowable.
        "orphan_and_unbounded_complex_counts_come_from_the_writer_join",
        "does_not_approve_production_cutover",
    ];
    if !report.skipped.is_empty() {
        evidence_limitations.push("some_source_polygons_could_not_be_normalized");
    }
    if report.null_shape_count > 0 {
        evidence_limitations.push("some_source_records_declare_the_null_shape");
    }
    if report.collapsed_ring_count > 0 {
        // A ring whose vertices have collapsed onto each other has no winding to read, so it
        // states neither an exterior ring nor a hole and is dropped from its record's geometry.
        evidence_limitations.push("some_source_rings_collapsed_below_their_own_rounding_error");
    }

    let summary = json!({
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "contract": handoff.contract_table_name,
        "source": {
            "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
            "source_slug": config.source_slug,
            "bronze_object_key": report.bronze_object_key,
            "source_snapshot_id": report.source_snapshot_id,
            "declared_projection": EXPECTED_PROJECTION_NAME,
            "geometry_srid": INDUSTRIAL_COMPLEX_BOUNDARY_SRID,
        },
        "counts": {
            "shapefile_record_count": report.shapefile_record_count,
            "row_count": report.row_count,
            "null_shape_count": report.null_shape_count,
            "collapsed_ring_count": report.collapsed_ring_count,
            "skipped_count": report.skipped.len(),
            // Measured, not assumed: these four values agree one-for-one with the profile's
            // `lrstt_ty`, which is why `DANJI_TYPE` is not carried into `boundary_kind`.
            "source_complex_type_counts": report.complex_type_counts,
        },
        "skipped": report.skipped.iter().map(|(code, reason)| json!({
            "official_complex_code": code,
            "reason": reason,
        })).collect::<Vec<_>>(),
        "largest_area_sqm_calculated": report.largest_areas.iter().map(|(code, area)| json!({
            "official_complex_code": code,
            "area_sqm_calculated": area,
        })).collect::<Vec<_>>(),
        "quality_metrics": handoff.quality_metrics,
        "transport_columns": handoff.transport_columns,
        "table_columns": handoff.table_columns,
        "output_path": config.output_path.display().to_string(),
        "evidence_limitations": evidence_limitations,
    });

    if let Some(parent) = summary_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    fs::write(
        summary_path,
        format!("{}\n", serde_json::to_string_pretty(&summary)?),
    )
    .with_context(|| format!("failed to write {}", summary_path.display()))
}

fn bronze_object_key(
    bronze_local_object_root: &Path,
    object_path: &Path,
) -> anyhow::Result<String> {
    let relative = object_path
        .strip_prefix(bronze_local_object_root)
        .with_context(|| {
            format!(
                "{} is not under the Bronze root {}",
                object_path.display(),
                bronze_local_object_root.display()
            )
        })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn default_source_snapshot_id(object_path: &Path) -> String {
    let stem = object_path.file_stem().map_or_else(
        || "unknown".to_owned(),
        |stem| stem.to_string_lossy().into_owned(),
    );
    format!("{DEFAULT_SOURCE_SLUG}-{stem}")
}

fn parse_utc(name: &str, raw: &str) -> anyhow::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{name} must be an RFC3339 timestamp"))
}

#[cfg(test)]
mod tests;
