//! 주소정보 전자지도 base: one kind's layer of one 시도 zip to a Silver JSONL handoff (root ADR-0494
//! §6).
//!
//! The base lands once per 시도 zip in Bronze (`jusogokr__electronic_map_base`); each zip holds the
//! layers of every kind in one `<시도코드>/` folder. This export opens only the members of the one
//! kind it is given (`base.layers` of `juso-electronic-map.contract.json`, which it embeds), so a
//! kind's Silver reads its own share of each zip and no other (the owner's partial rule). The SHP
//! and DBF stream side by side feature by feature (`SequentialShapefileReader`); the DBF has no
//! `.cpg` and is CP949; the geometry is the national UTM-K grid (the zip's top-level `.prj`),
//! reprojected to EPSG:4326 by the shared reader and written as WKB hex. Every DBF column is kept
//! under its provider name in lower case, beside the row's key (`base.keys`, else the kind's
//! linkage key) and its legal-code column, which the Silver load follows through
//! `views.changed_codes` (root ADR-0329 §3). A key that repeats within the layer fails the export:
//! the replay of the daily changes is keyed by it.

use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write as _,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use chrono::Utc;
use flate2::{write::GzEncoder, Compression as GzipCompression};
use foundation_outbox::R2ObjectStorage;
use foundation_shapefile::{SequentialShapefileReader, ShapefileFeature};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest as _, Sha256};

use crate::silver_handoff_io::{
    already_exported, one_of_path_or_key, open_sink, open_source, optional_env,
    refuse_existing_outputs, required_env, HandoffSink, InputSource, OutputSink, PathOrKey,
    SeekableSource, GZIP_LEVEL,
};

/// The source contract, embedded once: the collector lands the base by the same bytes.
pub const CONTRACT_JSON: &str =
    include_str!("../../../../infra/lakehouse/contracts/juso-electronic-map.contract.json");
/// The environment names of one export.
pub const ENV_PREFIX: &str = "FOUNDATION_PLATFORM_JUSO_ELECTRONIC_MAP_BASE";
const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.juso_electronic_map_base_silver_handoff_export.v1";
/// The CRS the shared reader writes.
const GEOMETRY_SRID: i32 = 4326;
/// No `.cpg` ships with the layers; the DBF language driver is 0 and the text is CP949.
const DBF_ENCODING: &str = "cp949";

#[derive(Clone, Debug, Deserialize)]
struct KindContract {
    key: Vec<String>,
    legal_code: LegalCode,
}

#[derive(Clone, Debug, Deserialize)]
struct LegalCode {
    column: String,
    level: String,
}

#[derive(Clone, Debug, Deserialize)]
struct BaseContract {
    source: String,
    projection_member: String,
    layers: BTreeMap<String, String>,
    #[serde(default)]
    keys: BTreeMap<String, JsonValue>,
}

/// What one kind reads from a base zip, from the contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KindLayout {
    pub kind: String,
    pub layer: String,
    pub key: Vec<String>,
    pub legal_code_column: String,
    pub legal_code_level: String,
    pub projection_member: String,
    pub source: String,
}

impl KindLayout {
    /// The layout of `kind` in the embedded contract.
    ///
    /// # Errors
    /// Refuses a kind the contract does not know or that has no base layer.
    pub fn of(kind: &str) -> anyhow::Result<Self> {
        let contract: JsonValue =
            serde_json::from_str(CONTRACT_JSON).context("the juso contract is not JSON")?;
        let base: BaseContract = serde_json::from_value(contract["base"].clone())
            .context("the juso contract's base is not valid")?;
        let kind_contract: KindContract =
            serde_json::from_value(contract["kinds"][kind].clone())
                .with_context(|| format!("{kind} is no kind of the juso contract"))?;
        let layer = base
            .layers
            .get(kind)
            .with_context(|| format!("{kind} has no base layer (contract base.layers)"))?
            .clone();
        let key = match base.keys.get(kind) {
            Some(columns) => serde_json::from_value(columns.clone())
                .with_context(|| format!("base.keys.{kind} is not a list of columns"))?,
            None => kind_contract.key,
        };
        ensure!(!key.is_empty(), "{kind} has no key");
        Ok(Self {
            kind: kind.to_owned(),
            layer,
            key,
            legal_code_column: kind_contract.legal_code.column,
            legal_code_level: kind_contract.legal_code.level,
            projection_member: base.projection_member,
            source: base.source,
        })
    }

    /// The `.shp` member of this kind in a zip's member list, under its one `<시도코드>/` folder.
    fn shp_member<'a>(&self, names: &'a [String]) -> anyhow::Result<&'a str> {
        let suffix = format!("/{}.shp", self.layer);
        let found: Vec<&String> = names
            .iter()
            .filter(|name| {
                name.strip_suffix(&suffix).is_some_and(|folder| {
                    folder.len() == 5 && folder.bytes().all(|b| b.is_ascii_digit())
                })
            })
            .collect();
        let [member] = found.as_slice() else {
            bail!(
                "the zip holds {} <시도코드>/{}.shp members, not one",
                found.len(),
                self.layer
            );
        };
        Ok(member.as_str())
    }
}

/// Exports one kind's layer of one base zip.
///
/// # Errors
/// Refuses an unknown kind, a zip without the layer, an unreadable shapefile, a repeated key, and
/// failed IO.
pub async fn run() -> anyhow::Result<()> {
    let config = ExportConfig::from_env()?;
    if already_exported(&config.output).await? {
        write_summary(&config, None)?;
        tracing::info!(output = %config.output.describe(), "juso base handoff already exists; nothing to do");
        return Ok(());
    }
    let report = export_handoff(&config).await?;
    tracing::info!(
        kind = %config.layout.kind,
        folder = %report.folder,
        input_feature_count = report.input_feature_count,
        output_row_count = report.output_row_count,
        rejected_row_count = report.rejected_row_reasons.values().sum::<u64>(),
        elapsed_milliseconds = report.elapsed_milliseconds,
        "juso base Silver handoff export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug)]
struct ExportConfig {
    input: InputSource,
    output: OutputSink,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
    base_month: String,
    layout: KindLayout,
}

impl ExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let input = match one_of_path_or_key(
            &format!("{ENV_PREFIX}_INPUT_PATH"),
            &format!("{ENV_PREFIX}_INPUT_OBJECT_KEY"),
        )? {
            PathOrKey::Path(path) => InputSource::LocalPath(path),
            PathOrKey::Key(key) => InputSource::R2Object(key),
        };
        let output = match one_of_path_or_key(
            &format!("{ENV_PREFIX}_OUTPUT_PATH"),
            &format!("{ENV_PREFIX}_OUTPUT_OBJECT_KEY"),
        )? {
            PathOrKey::Path(path) => OutputSink::LocalPath(path),
            PathOrKey::Key(key) => OutputSink::R2Object(key),
        };
        let base_month = required_env(&format!("{ENV_PREFIX}_BASE_MONTH"))?;
        ensure!(
            base_month.len() == 6 && base_month.bytes().all(|b| b.is_ascii_digit()),
            "{ENV_PREFIX}_BASE_MONTH must be YYYYMM"
        );
        Ok(Self {
            input,
            output,
            summary_path: optional_env(&format!("{ENV_PREFIX}_SUMMARY_PATH"))?.map(PathBuf::from),
            source_snapshot_id: required_env(&format!("{ENV_PREFIX}_SOURCE_SNAPSHOT_ID"))?,
            base_month,
            layout: KindLayout::of(&required_env(&format!("{ENV_PREFIX}_KIND"))?)?,
        })
    }

    /// The object this run opened (root ADR-0068: derived, never passed).
    fn source_record_id(&self) -> String {
        match &self.input {
            InputSource::R2Object(key) => key.clone(),
            InputSource::LocalPath(path) => path.display().to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExportReport {
    folder: String,
    member: String,
    input_feature_count: u64,
    output_row_count: u64,
    rejected_row_reasons: BTreeMap<String, u64>,
    output_bytes: u64,
    elapsed_milliseconds: u128,
}

async fn export_handoff(config: &ExportConfig) -> anyhow::Result<ExportReport> {
    refuse_existing_outputs(&config.output, config.summary_path.as_deref())?;
    let storage = if config.input.is_r2() || config.output.is_r2() {
        Some(R2ObjectStorage::from_env().context("failed to configure R2 for a streamed handoff")?)
    } else {
        None
    };
    // Two readers of the one object: the SHP and DBF members stream side by side.
    let shapes = open_source(&config.input, storage.as_ref()).await?;
    let attributes = open_source(&config.input, storage.as_ref()).await?;
    let sink = open_sink(&config.output, storage.as_ref()).await?;
    let owned = config.clone();
    let report = tokio::task::spawn_blocking(move || convert(shapes, attributes, sink, &owned))
        .await
        .context("failed to join the juso base conversion")??;
    write_summary(config, Some(&report))?;
    Ok(report)
}

fn convert(
    shapes: SeekableSource,
    attributes: SeekableSource,
    mut sink: HandoffSink,
    config: &ExportConfig,
) -> anyhow::Result<ExportReport> {
    let started = Instant::now();
    let layout = &config.layout;
    let mut shape_zip = zip::ZipArchive::new(shapes).context("failed to open the juso base zip")?;
    let mut attribute_zip =
        zip::ZipArchive::new(attributes).context("failed to open the juso base zip")?;
    let names: Vec<String> = shape_zip
        .file_names()
        .map(|name| name.map(std::borrow::Cow::into_owned))
        .collect::<Result<_, _>>()
        .context("failed to decode the juso base zip member names")?;
    let shp_name = layout.shp_member(&names)?.to_owned();
    let stem = shp_name.trim_end_matches(".shp").to_owned();
    let folder = stem.split('/').next().unwrap_or_default().to_owned();
    let mut prj = String::new();
    shape_zip
        .by_name(&layout.projection_member)
        .with_context(|| format!("the zip has no {}", layout.projection_member))?
        .read_to_string(&mut prj)
        .context("the .prj is not text")?;
    let shp = shape_zip
        .by_name(&shp_name)
        .context("failed to open the .shp member")?;
    let dbf = attribute_zip
        .by_name(&format!("{stem}.dbf"))
        .with_context(|| format!("{stem} has no .dbf"))?;
    let mut reader = SequentialShapefileReader::new(shp, dbf, &stem, &prj, DBF_ENCODING)?;

    let mut report = ExportReport {
        folder: folder.clone(),
        member: shp_name.clone(),
        ..ExportReport::default()
    };
    let ingested_at_utc = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let source_record_id = config.source_record_id();
    let mut seen = HashSet::new();
    let mut repeated = 0_u64;
    {
        let mut writer = BufWriter::new(&mut sink);
        let mut gzip = config
            .output
            .is_compressed()
            .then(|| GzEncoder::new(Vec::new(), GzipCompression::new(GZIP_LEVEL)));
        let (mut rows, mut rejected) = (0_u64, BTreeMap::<String, u64>::new());
        report.input_feature_count = reader.for_each_feature(|feature| {
            match base_row(&feature, layout) {
                Ok((key, mut row)) => {
                    let digest: [u8; 16] = Sha256::digest(key.as_bytes())[..16]
                        .try_into()
                        .unwrap_or_default();
                    if !seen.insert(digest) {
                        repeated += 1;
                    }
                    for (column, value) in [
                        ("kind", json!(layout.kind)),
                        ("base_month", json!(config.base_month)),
                        ("sido_folder", json!(folder)),
                        ("source_record_id", json!(source_record_id)),
                        ("source_snapshot_id", json!(config.source_snapshot_id)),
                        ("ingested_at_utc", json!(ingested_at_utc)),
                    ] {
                        row.insert(column.to_owned(), value);
                    }
                    let mut line = serde_json::to_vec(&JsonValue::Object(row))?;
                    line.push(b'\n');
                    match gzip.as_mut() {
                        // Compressed in step with the rows and flushed to the sink as it fills.
                        Some(encoder) => {
                            encoder.write_all(&line)?;
                            writer.write_all(&std::mem::take(encoder.get_mut()))?;
                        }
                        None => writer.write_all(&line)?,
                    }
                    rows += 1;
                }
                Err(reason) => *rejected.entry(reason).or_insert(0) += 1,
            }
            Ok(())
        })?;
        report.output_row_count = rows;
        report.rejected_row_reasons = rejected;
        if let Some(encoder) = gzip {
            // Explicit: a dropped encoder cannot report a failed trailer.
            writer.write_all(
                &encoder
                    .finish()
                    .context("failed to finish the gzip stream")?,
            )?;
        }
        writer
            .flush()
            .context("failed to flush the juso base handoff")?;
    }
    ensure!(
        repeated == 0,
        "{stem}: {repeated} rows repeat a key ({}); the replay of the daily changes is keyed by it",
        layout.key.join("+")
    );
    report.output_bytes = sink.finish()?;
    report.elapsed_milliseconds = started.elapsed().as_millis();
    Ok(report)
}

/// One row's provider columns, key, legal code and geometry, or the reason it is refused.
fn base_row(
    feature: &ShapefileFeature,
    layout: &KindLayout,
) -> Result<(String, serde_json::Map<String, JsonValue>), String> {
    let attributes = feature.attributes();
    let text = |column: &str| match attributes.get(column) {
        Some(JsonValue::String(text)) => Some(text.clone()),
        Some(JsonValue::Number(number)) => Some(number.to_string()),
        _ => None,
    };
    let mut parts = Vec::with_capacity(layout.key.len());
    for column in &layout.key {
        let Some(part) = text(column) else {
            return Err(format!("blank_key_{}", column.to_ascii_lowercase()));
        };
        parts.push(part);
    }
    let key = parts.join("|");
    let legal_code = text(&layout.legal_code_column);
    let (wkb, bbox) = geojson_to_wkb(feature.geometry()).map_err(|reason| reason.to_owned())?;
    let mut row = serde_json::Map::new();
    for (column, value) in attributes {
        row.insert(column.to_ascii_lowercase(), value);
    }
    let hex = |bytes: &[u8]| {
        bytes
            .iter()
            .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            })
    };
    for (column, value) in [
        ("row_key", json!(key)),
        ("legal_code", json!(legal_code)),
        ("legal_code_level", json!(layout.legal_code_level)),
        (
            "geometry_type",
            json!(feature.geometry()["type"].as_str().unwrap_or_default()),
        ),
        (
            "geometry_checksum_sha256",
            json!(hex(&Sha256::digest(&wkb))),
        ),
        ("geometry_wkb", json!(hex(&wkb))),
        ("geometry_srid", json!(GEOMETRY_SRID)),
        ("bbox_min_x", json!(bbox[0])),
        ("bbox_min_y", json!(bbox[1])),
        ("bbox_max_x", json!(bbox[2])),
        ("bbox_max_y", json!(bbox[3])),
    ] {
        row.insert(column.to_owned(), value);
    }
    Ok((key, row))
}

/// Little-endian WKB of the reader's `Point`, `MultiLineString` or `MultiPolygon`, and its box.
fn geojson_to_wkb(geometry: &JsonValue) -> Result<(Vec<u8>, [f64; 4]), &'static str> {
    fn point(value: &JsonValue) -> Result<(f64, f64), &'static str> {
        match value.as_array().map(Vec::as_slice) {
            Some([x, y, ..]) => Ok((
                x.as_f64().ok_or("invalid_geometry")?,
                y.as_f64().ok_or("invalid_geometry")?,
            )),
            _ => Err("invalid_geometry"),
        }
    }
    fn array(value: &JsonValue) -> Result<&Vec<JsonValue>, &'static str> {
        value.as_array().ok_or("invalid_geometry")
    }
    fn count(bytes: &mut Vec<u8>, len: usize) -> Result<(), &'static str> {
        let len = u32::try_from(len).map_err(|_| "unencodable_geometry")?;
        bytes.extend_from_slice(&len.to_le_bytes());
        Ok(())
    }
    fn header(bytes: &mut Vec<u8>, kind: u32) {
        bytes.push(1);
        bytes.extend_from_slice(&kind.to_le_bytes());
    }
    let mut bbox = [
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    ];
    let mut points = |bytes: &mut Vec<u8>, values: &[JsonValue]| -> Result<(), &'static str> {
        count(bytes, values.len())?;
        for value in values {
            let (x, y) = point(value)?;
            bbox = [
                bbox[0].min(x),
                bbox[1].min(y),
                bbox[2].max(x),
                bbox[3].max(y),
            ];
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        }
        Ok(())
    };
    let coordinates = &geometry["coordinates"];
    let mut bytes = Vec::new();
    match geometry["type"].as_str() {
        Some("Point") => {
            header(&mut bytes, 1);
            let (x, y) = point(coordinates)?;
            bbox = [x, y, x, y];
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
        }
        Some("MultiLineString") => {
            let lines = array(coordinates)?;
            header(&mut bytes, 5);
            count(&mut bytes, lines.len())?;
            for line in lines {
                header(&mut bytes, 2);
                points(&mut bytes, array(line)?)?;
            }
        }
        Some("MultiPolygon") => {
            let polygons = array(coordinates)?;
            header(&mut bytes, 6);
            count(&mut bytes, polygons.len())?;
            for polygon in polygons {
                let rings = array(polygon)?;
                header(&mut bytes, 3);
                count(&mut bytes, rings.len())?;
                for ring in rings {
                    points(&mut bytes, array(ring)?)?;
                }
            }
        }
        _ => return Err("unsupported_geometry"),
    }
    if !bbox.iter().all(|value| value.is_finite()) {
        return Err("empty_geometry");
    }
    Ok((bytes, bbox))
}

/// The summary the Silver load reads: the outcome, the snapshot id, the rows and what was refused.
fn write_summary(config: &ExportConfig, report: Option<&ExportReport>) -> anyhow::Result<()> {
    let Some(path) = &config.summary_path else {
        return Ok(());
    };
    let summary = match report {
        Some(report) => json!({
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "outcome": "converted",
            "status": "ready",
            "source": {
                "input": config.input.describe(),
                "bronze_source": config.layout.source,
                "kind": config.layout.kind,
                "layer": config.layout.layer,
                "member": report.member,
                "folder": report.folder,
                "base_month": config.base_month,
                "source_record_id": config.source_record_id(),
                "source_snapshot_id": config.source_snapshot_id,
                "input_row_count": report.input_feature_count,
            },
            "output": {
                "destination": config.output.describe(),
                "key": config.layout.key,
                "row_count": report.output_row_count,
                "bytes": report.output_bytes,
            },
            "rejected_row_count": report.rejected_row_reasons.values().sum::<u64>(),
            "rejected_row_reasons": report.rejected_row_reasons,
            "performance": { "elapsed_milliseconds": report.elapsed_milliseconds },
        }),
        None => json!({
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "outcome": "already_present",
            "status": "ready",
            "source": {
                "input": config.input.describe(),
                "kind": config.layout.kind,
                "source_record_id": config.source_record_id(),
                "source_snapshot_id": config.source_snapshot_id,
            },
            "output": { "destination": config.output.describe() },
            "measurements_absent_because": "the handoff already exists and this run did not rebuild it",
        }),
    };
    write_json(path, &summary)
}

fn write_json(path: &Path, value: &JsonValue) -> anyhow::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, format!("{}\n", serde_json::to_string_pretty(value)?))
        .with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests;
