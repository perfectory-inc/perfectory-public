//! 용도지역지구 도형 ZIP to Silver JSONL handoff (root ADR-0362).
//!
//! One VWorld NA/24 full-data ZIP holds one layer as `AL_D<layer>_00_<YYYYMMDD>.shp/.dbf/.prj` (no
//! `.cpg`; the DBF is CP949). The Silver refresh runs this export once per layer of a release, as it
//! runs the land CSV exports once per 시도 (`land_use_silver_export`, same environment names). Both
//! members stream from the archive feature by feature (`SequentialShapefileReader`), so the largest
//! served layer never sits in memory whole; the geometry is reprojected to EPSG:4326 by the shared
//! reader and written as WKB hex, which `silver_scalar_handoff_to_lakehouse.py` decodes.

use std::{
    collections::BTreeMap,
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
use lakehouse_application::{
    geometry_bounding_box, geometry_to_wkb, parse_geojson_polygonal_geometry,
};
use lakehouse_domain::SILVER_LAND_USE_ZONE_AREA;
use regex::Regex;
use serde_json::json;
use sha2::{Digest as _, Sha256};

use crate::silver_handoff_io::{
    already_exported, one_of_path_or_key, open_sink, open_source, optional_env,
    refuse_existing_outputs, required_env, HandoffSink, InputSource, OutputSink, PathOrKey,
    SeekableSource, GZIP_LEVEL,
};

/// The environment names the Silver refresh sets for one object (`land::export_environment`).
pub const ENV_PREFIX: &str = "FOUNDATION_PLATFORM_LAND_USE_ZONE_AREA";
const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.land_use_zone_area_silver_handoff_export.v1";
/// The member the export converts: a full release of one layer. `CH_` change files are not read.
const MEMBER: &str = r"^AL_D(?P<layer>[0-9]{3})_00_(?P<vintage>[0-9]{8})\.shp$";
/// The CRS the shared reader writes (it reprojects every Korea 2000 belt to EPSG:4326).
const GEOMETRY_SRID: i32 = 4326;
/// The DBF has no `.cpg`; its text is CP949, which the provider's other land files are too.
const DBF_ENCODING: &str = "cp949";

/// Whether a ZIP member is one this export converts: any shapefile. A ZIP with two (a layer the
/// provider split, `…(2).shp`) is refused by the Silver refresh rather than half loaded.
#[must_use]
pub fn converts_member(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with(".shp")
}

/// Exports one layer's full release.
///
/// # Errors
/// Refuses a ZIP that is not one full-release layer, an unreadable shapefile, and failed IO.
pub async fn run() -> anyhow::Result<()> {
    let config = ExportConfig::from_env()?;
    if already_exported(&config.output).await? {
        write_summary(&config, None)?;
        tracing::info!(output = %config.output.describe(), "zone-area handoff already exists; nothing to do");
        return Ok(());
    }
    let report = export_handoff(&config).await?;
    tracing::info!(
        layer = %report.layer_code,
        input_feature_count = report.input_feature_count,
        output_row_count = report.output_row_count,
        rejected_row_count = report.rejected_row_reasons.values().sum::<u64>(),
        elapsed_milliseconds = report.elapsed_milliseconds,
        "zone-area Silver handoff export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportConfig {
    input: InputSource,
    output: OutputSink,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
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
        Ok(Self {
            input,
            output,
            summary_path: optional_env(&format!("{ENV_PREFIX}_SUMMARY_PATH"))?.map(PathBuf::from),
            source_snapshot_id: required_env(&format!("{ENV_PREFIX}_SOURCE_SNAPSHOT_ID"))?,
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
    layer_code: String,
    dataset_name: String,
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
        .context("failed to join the zone-area conversion")??;
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
    let mut shape_zip = zip::ZipArchive::new(shapes).context("failed to open the zone-area ZIP")?;
    let mut attribute_zip =
        zip::ZipArchive::new(attributes).context("failed to open the zone-area ZIP")?;
    let names: Vec<String> = shape_zip
        .file_names()
        .map(|name| name.map(std::borrow::Cow::into_owned))
        .collect::<Result<_, _>>()
        .context("failed to decode the zone-area ZIP member names")?;
    let shapefiles: Vec<&String> = names.iter().filter(|name| converts_member(name)).collect();
    let [shp_name] = shapefiles.as_slice() else {
        bail!(
            "the ZIP holds {} shapefiles; one layer is one shapefile",
            shapefiles.len()
        );
    };
    let member = Regex::new(MEMBER).context("the member rule is a regex")?;
    let captures = member.captures(shp_name).with_context(|| {
        format!("{shp_name} is not a full release of one layer (AL_D<layer>_00_<YYYYMMDD>.shp)")
    })?;
    let layer_code = format!("D{}", &captures["layer"]);
    let stem = shp_name.trim_end_matches(".shp");
    let mut prj = String::new();
    shape_zip
        .by_name(&format!("{stem}.prj"))
        .with_context(|| format!("{stem} has no .prj"))?
        .read_to_string(&mut prj)
        .context("the .prj is not text")?;
    let shp = shape_zip
        .by_name(shp_name)
        .context("failed to open the .shp member")?;
    let dbf = attribute_zip
        .by_name(&format!("{stem}.dbf"))
        .with_context(|| format!("{stem} has no .dbf"))?;
    let mut reader = SequentialShapefileReader::new(shp, dbf, stem, &prj, DBF_ENCODING)?;

    let mut report = ExportReport {
        layer_code: layer_code.clone(),
        dataset_name: stem.to_owned(),
        ..ExportReport::default()
    };
    let ingested_at_utc = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let source_record_id = config.source_record_id();
    {
        let mut writer = BufWriter::new(&mut sink);
        let mut gzip = config
            .output
            .is_compressed()
            .then(|| GzEncoder::new(Vec::new(), GzipCompression::new(GZIP_LEVEL)));
        let (mut rows, mut rejected) = (0_u64, BTreeMap::<String, u64>::new());
        report.input_feature_count = reader.for_each_feature(|feature| {
            match area_row(&feature, &layer_code) {
                Ok(mut row) => {
                    row.insert("source_record_id".to_owned(), json!(source_record_id));
                    row.insert(
                        "source_snapshot_id".to_owned(),
                        json!(config.source_snapshot_id),
                    );
                    row.insert("ingested_at_utc".to_owned(), json!(ingested_at_utc));
                    let mut line = serde_json::to_vec(&serde_json::Value::Object(row))?;
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
            .context("failed to flush the zone-area handoff")?;
    }
    report.output_bytes = sink.finish()?;
    report.elapsed_milliseconds = started.elapsed().as_millis();
    ensure!(
        report.output_row_count > 0,
        "{stem} produced no area at all; {} features were read",
        report.input_feature_count
    );
    Ok(report)
}

/// A DBF text cell as the provider wrote it, without its padding: the unnamed columns pad with
/// NUL bytes as well as spaces.
fn cell(feature: &ShapefileFeature, field: &str) -> anyhow::Result<Option<String>> {
    Ok(feature
        .optional_text(field)?
        .map(|value| {
            value
                .trim_matches(|c: char| c == '\0' || c.is_whitespace())
                .to_owned()
        })
        .filter(|value| !value.is_empty()))
}

/// One area's contract columns (lineage added by the caller), or the reason it is refused.
fn area_row(
    feature: &ShapefileFeature,
    layer_code: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let text =
        |field: &str| cell(feature, field).map_err(|error| format!("unreadable_{field}: {error}"));
    let Some(zone_feature_id) = text("A1")? else {
        return Err("blank_zone_feature_id".to_owned());
    };
    let geometry = parse_geojson_polygonal_geometry(feature.geometry(), &zone_feature_id)
        .map_err(|_| "invalid_geometry".to_owned())?;
    let bbox = geometry_bounding_box(&geometry).map_err(|_| "empty_geometry".to_owned())?;
    let wkb = geometry_to_wkb(&geometry).map_err(|_| "unencodable_geometry".to_owned())?;
    let hex = |bytes: &[u8]| {
        bytes
            .iter()
            .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            })
    };
    let mut row = serde_json::Map::new();
    for (column, value) in [
        ("layer_code", json!(layer_code)),
        ("zone_feature_id", json!(zone_feature_id)),
        ("layer_name", json!(text("A2")?)),
        ("sigungu_code", json!(text("A4")?)),
        ("zone_code", json!(text("A5")?)),
        ("zone_name", json!(text("A6")?)),
        ("field_a7", json!(text("A7")?)),
        ("field_a8", json!(text("A8")?)),
        ("field_a9", json!(text("A9")?)),
        (
            "geometry_checksum_sha256",
            json!(hex(&Sha256::digest(&wkb))),
        ),
        ("geometry_wkb", json!(hex(&wkb))),
        ("geometry_srid", json!(GEOMETRY_SRID)),
        ("bbox_min_x", json!(bbox.min_x)),
        ("bbox_min_y", json!(bbox.min_y)),
        ("bbox_max_x", json!(bbox.max_x)),
        ("bbox_max_y", json!(bbox.max_y)),
    ] {
        row.insert(column.to_owned(), value);
    }
    Ok(row)
}

/// The summary `land::export_rows` reads: the outcome, the snapshot id and the rows.
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
                "dataset_name": report.dataset_name,
                "layer_code": report.layer_code,
                "source_record_id": config.source_record_id(),
                "source_snapshot_id": config.source_snapshot_id,
                "input_row_count": report.input_feature_count,
            },
            "output": {
                "destination": config.output.describe(),
                "contract": SILVER_LAND_USE_ZONE_AREA.table_name,
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
                "source_record_id": config.source_record_id(),
                "source_snapshot_id": config.source_snapshot_id,
            },
            "output": {
                "destination": config.output.describe(),
                "contract": SILVER_LAND_USE_ZONE_AREA.table_name,
            },
            "measurements_absent_because": "the handoff already exists and this run did not rebuild it",
        }),
    };
    write_json(path, &summary)
}

fn write_json(path: &Path, value: &serde_json::Value) -> anyhow::Result<()> {
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
mod tests {
    use std::io::Cursor;

    use shapefile::{
        dbase::{self, encoding::EncodingRs, FieldName, FieldValue},
        Point, Polygon, PolygonRing, ShapeWriter, Writer,
    };
    use zip::{write::SimpleFileOptions, ZipWriter};

    use super::*;

    const KOREA_CENTRAL_BELT_2010: &str = concat!(
        r#"PROJCS["Korea_2000_Korea_Central_Belt_2010","#,
        r#"GEOGCS["GCS_Korea_2000",DATUM["D_Korea_2000","#,
        r#"SPHEROID["GRS_1980",6378137.0,298.257222101]],"#,
        r#"PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],"#,
        r#"PROJECTION["Transverse_Mercator"],"#,
        r#"PARAMETER["False_Easting",200000.0],"#,
        r#"PARAMETER["False_Northing",600000.0],"#,
        r#"PARAMETER["Central_Meridian",127.0],"#, // public-repository-safety: reviewed-runtime-coordinate
        r#"PARAMETER["Scale_Factor",1.0],"#,
        r#"PARAMETER["Latitude_Of_Origin",38.0],UNIT["Meter",1.0]]"#,
    );
    const FIELDS: [(&str, u8); 8] = [
        ("A1", 33),
        ("A2", 254),
        ("A4", 5),
        ("A5", 6),
        ("A6", 200),
        ("A7", 8),
        ("A8", 200),
        ("A9", 200),
    ];

    /// Synthetic layer: two areas and one without its id (no real code or place).
    fn layer_zip(stem: &str, extra_shp: bool) -> anyhow::Result<Vec<u8>> {
        let (mut shp, mut shx, mut dbf) = (
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        );
        {
            let mut table =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR));
            for (name, length) in FIELDS {
                table = table.add_character_field(
                    FieldName::try_from(name).map_err(|e| anyhow::anyhow!(e))?,
                    length,
                );
            }
            let mut writer = Writer::new(
                ShapeWriter::with_shx(&mut shp, &mut shx),
                table.build_with_dest(&mut dbf),
            );
            for (x, id) in [
                (200_000.0, "99999000099999000000UDV1000000001"),
                (200_100.0, "99999000099999000000UDV1000000002"),
                (200_200.0, ""),
            ] {
                let polygon = Polygon::new(PolygonRing::Outer(vec![
                    Point::new(x, 600_000.0),
                    Point::new(x, 600_010.0),
                    Point::new(x + 10.0, 600_010.0),
                    Point::new(x + 10.0, 600_000.0),
                    Point::new(x, 600_000.0),
                ]));
                let mut record = dbase::Record::default();
                for (name, value) in [
                    ("A1", id),
                    ("A2", "가상법/가상구역"),
                    ("A4", "99999"),
                    ("A5", "UDV100"),
                    ("A6", "가상구역"),
                    ("A7", "\0\0\0\0\0\0\0\0"),
                    ("A8", ""),
                    ("A9", "비고"),
                ] {
                    record.insert(
                        name.to_owned(),
                        FieldValue::Character(Some(value.to_owned())),
                    );
                }
                writer.write_shape_and_record(&polygon, &record)?;
            }
        }
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        let mut members = vec![
            (format!("{stem}.shp"), shp.into_inner()),
            (format!("{stem}.shx"), shx.into_inner()),
            (format!("{stem}.dbf"), dbf.into_inner()),
            (
                format!("{stem}.prj"),
                KOREA_CENTRAL_BELT_2010.as_bytes().to_vec(),
            ),
        ];
        if extra_shp {
            members.push((format!("{stem}(2).shp"), Vec::new()));
        }
        for (name, bytes) in members {
            zip.start_file(name, SimpleFileOptions::default())?;
            zip.write_all(&bytes)?;
        }
        Ok(zip.finish()?.into_inner())
    }

    async fn export(zip: Vec<u8>, gzip: bool) -> anyhow::Result<(ExportReport, String)> {
        let root = std::env::temp_dir().join(format!("zone-area-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let input = root.join("layer.zip");
        std::fs::write(&input, zip)?;
        let output = root.join(if gzip {
            "layer.jsonl.gz"
        } else {
            "layer.jsonl"
        });
        let config = ExportConfig {
            input: InputSource::LocalPath(input),
            output: OutputSink::LocalPath(output.clone()),
            summary_path: Some(root.join("summary.json")),
            source_snapshot_id: "vworldkr-land-use-zone:20991231".to_owned(),
        };
        let result = export_handoff(&config).await;
        let body = match &result {
            Ok(_) if gzip => {
                let mut text = String::new();
                flate2::read::GzDecoder::new(std::fs::File::open(&output)?)
                    .read_to_string(&mut text)?;
                text
            }
            Ok(_) => std::fs::read_to_string(&output)?,
            Err(_) => String::new(),
        };
        std::fs::remove_dir_all(&root)?;
        Ok((result?, body))
    }

    #[tokio::test]
    async fn a_layer_becomes_contract_rows_and_an_area_without_its_id_is_counted(
    ) -> anyhow::Result<()> {
        for gzip in [false, true] {
            let (report, body) = export(layer_zip("AL_D999_00_20991231", false)?, gzip).await?;
            assert_eq!(
                (report.input_feature_count, report.output_row_count),
                (3, 2)
            );
            assert_eq!(
                report.rejected_row_reasons.get("blank_zone_feature_id"),
                Some(&1)
            );
            let rows: Vec<serde_json::Value> = body
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<_, _>>()?;
            assert_eq!(rows[0]["layer_code"], "D999");
            assert_eq!(rows[0]["zone_code"], "UDV100");
            assert!(rows[0]["field_a7"].is_null(), "NUL padding is no value");
            assert!(rows[0]["field_a8"].is_null());
            assert_eq!(rows[0]["field_a9"], "비고");
            assert_eq!(rows[0]["geometry_srid"], 4326);
            let wkb = rows[0]["geometry_wkb"].as_str().unwrap_or_default();
            assert!(
                wkb.starts_with("0103000000"),
                "little-endian WKB polygon: {wkb}"
            );
            let keys: std::collections::BTreeSet<&str> = rows[0]
                .as_object()
                .map(|row| row.keys().map(String::as_str).collect())
                .unwrap_or_default();
            let contract: std::collections::BTreeSet<&str> = SILVER_LAND_USE_ZONE_AREA
                .columns
                .iter()
                .map(|column| column.name)
                .filter(|name| *name != "row_state")
                .collect();
            assert_eq!(keys, contract);
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_change_file_or_a_split_layer_is_refused() -> anyhow::Result<()> {
        let change = export(layer_zip("CH_D999_00_20991231", false)?, false).await;
        assert!(change.is_err_and(|error| error.to_string().contains("full release")));
        let split = export(layer_zip("AL_D999_00_20991231", true)?, false).await;
        assert!(split.is_err_and(|error| error.to_string().contains("2 shapefiles")));
        assert!(
            converts_member("AL_D032_00_20260609.shp")
                && !converts_member("AL_D032_00_20260609.dbf")
        );
        Ok(())
    }
}
