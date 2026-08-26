//! VWorld cadastral zipped-shapefile to Silver JSONL handoff command.

use std::{
    collections::BTreeMap,
    env, fs,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use collection_domain::VWorldCadastralDedupedFeature;
use foundation_shapefile::{ShapefileMetadata, ZipShapefileReader};
use foundation_shared_kernel::Pnu;
use lakehouse_application::{
    build_vworld_cadastral_silver_parcel_boundary_handoff,
    normalize_vworld_cadastral_silver_parcel_boundary_rows,
    VWorldCadastralSilverParcelBoundaryRowsInput,
};
use serde_json::json;
use uuid::Uuid;

const INPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_INPUT_PATH";
const OUTPUT_PATH_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_OUTPUT_PATH";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SUMMARY_PATH";
const SOURCE_RECORD_ID_ENV: &str =
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_RECORD_ID";
const SOURCE_SNAPSHOT_ID_ENV: &str =
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_SNAPSHOT_ID";
const VALID_FROM_UTC_ENV: &str = "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_VALID_FROM_UTC";

/// Runs one local `VWorld` cadastral shapefile ZIP to Silver JSONL handoff conversion.
///
/// # Errors
///
/// Returns an error when configuration, source validation, normalization, or an atomic output
/// write fails.
pub fn run() -> anyhow::Result<()> {
    let config = ExportConfig::from_env()?;
    let report = export_handoff(&config)?;
    tracing::info!(
        input_feature_count = report.input_feature_count,
        valid_pnu_count = report.valid_pnu_count,
        invalid_pnu_count = report.invalid_pnu_count,
        output_row_count = report.output_row_count,
        output_bytes = report.output_bytes,
        elapsed_milliseconds = report.elapsed_milliseconds,
        output_path = %config.output_path.display(),
        "VWorld cadastral shapefile Silver handoff export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportConfig {
    input_path: PathBuf,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    source_record_id: String,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportReport {
    input_feature_count: u64,
    valid_pnu_count: u64,
    invalid_pnu_count: u64,
    output_row_count: u64,
    output_bytes: u64,
    elapsed_milliseconds: u128,
    metadata: ShapefileMetadata,
}

struct StreamReport {
    input_feature_count: u64,
    valid_pnu_count: u64,
    invalid_pnu_count: u64,
    output_row_count: u64,
    contract_table_name: &'static str,
    quality_metrics: BTreeMap<String, u64>,
}

impl ExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            input_path: PathBuf::from(required_env(INPUT_PATH_ENV)?),
            output_path: PathBuf::from(required_env(OUTPUT_PATH_ENV)?),
            summary_path: optional_env(SUMMARY_PATH_ENV)?.map(PathBuf::from),
            source_record_id: required_env(SOURCE_RECORD_ID_ENV)?,
            source_snapshot_id: required_env(SOURCE_SNAPSHOT_ID_ENV)?,
            valid_from_utc: parse_utc_env(VALID_FROM_UTC_ENV)?,
        })
    }
}

fn export_handoff(config: &ExportConfig) -> anyhow::Result<ExportReport> {
    if config.output_path.exists() {
        bail!(
            "refusing to overwrite existing shapefile JSONL output {}",
            config.output_path.display()
        );
    }
    if config
        .summary_path
        .as_ref()
        .is_some_and(|path| path.exists())
    {
        bail!("refusing to overwrite existing shapefile summary output");
    }
    let started = Instant::now();
    let input = fs::File::open(&config.input_path).with_context(|| {
        format!(
            "failed to open VWorld cadastral shapefile ZIP {}",
            config.input_path.display()
        )
    })?;
    let mut reader = ZipShapefileReader::new(BufReader::new(input))?;
    let metadata = reader.metadata().clone();
    let mut pending_output = PendingOutput::create(&config.output_path)?;
    let mut writer = BufWriter::new(pending_output.file_mut()?);
    let mut stream_report = stream_silver_rows(&mut reader, &mut writer, config)?;
    writer.flush().context("failed to flush shapefile JSONL")?;
    drop(writer);
    let output_bytes = pending_output
        .file_mut()?
        .metadata()
        .context("failed to inspect staged shapefile JSONL")?
        .len();
    pending_output.sync_and_commit(&config.output_path)?;
    stream_report
        .quality_metrics
        .insert("row_count".to_owned(), stream_report.output_row_count);
    stream_report.quality_metrics.insert(
        "invalid_pnu_count".to_owned(),
        stream_report.invalid_pnu_count,
    );

    let report = ExportReport {
        input_feature_count: stream_report.input_feature_count,
        valid_pnu_count: stream_report.valid_pnu_count,
        invalid_pnu_count: stream_report.invalid_pnu_count,
        output_row_count: stream_report.output_row_count,
        output_bytes,
        elapsed_milliseconds: started.elapsed().as_millis(),
        metadata,
    };
    if let Some(summary_path) = &config.summary_path {
        write_summary(
            summary_path,
            config,
            &report,
            stream_report.contract_table_name,
            &stream_report.quality_metrics,
        )?;
    }
    Ok(report)
}

fn stream_silver_rows(
    reader: &mut ZipShapefileReader,
    writer: &mut impl Write,
    config: &ExportConfig,
) -> anyhow::Result<StreamReport> {
    let ingested_at_utc = Utc::now();
    let empty_handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&[])
        .context("failed to initialize Silver handoff quality metrics")?;
    let mut quality_metrics = empty_handoff.quality_metrics;
    let mut valid_pnu_count = 0_u64;
    let mut invalid_pnu_count = 0_u64;
    let mut output_row_count = 0_u64;
    let input_feature_count = reader.for_each_feature(|source_feature| {
        let Some(raw_pnu) = source_feature.optional_text("PNU")? else {
            invalid_pnu_count += 1;
            return Ok(());
        };
        let Ok(pnu) = Pnu::parse(raw_pnu.to_owned()) else {
            invalid_pnu_count += 1;
            return Ok(());
        };
        let jibun = source_feature.required_text("JIBUN")?;
        let properties = json!({
            "pnu": pnu.as_str(),
            "jibun": jibun,
            "bonbun": pnu.bonbun(),
            "bubun": pnu.bubun()
        });
        let geometry = source_feature.geometry().clone();
        let record = VWorldCadastralDedupedFeature {
            pnu: pnu.as_str().to_owned(),
            feature: json!({
                "type": "Feature",
                "properties": properties,
                "geometry": geometry
            }),
            properties,
            geometry,
            occurrence_count: 1,
        };
        let rows = normalize_vworld_cadastral_silver_parcel_boundary_rows(
            &VWorldCadastralSilverParcelBoundaryRowsInput {
                records: std::slice::from_ref(&record),
                source_record_id: &config.source_record_id,
                source_snapshot_id: &config.source_snapshot_id,
                valid_from_utc: config.valid_from_utc,
                ingested_at_utc,
            },
        )
        .context("failed to normalize shapefile feature into Silver parcel boundary")?;
        let handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&rows)
            .context("failed to serialize shapefile Silver parcel-boundary row")?;
        writer
            .write_all(handoff.jsonl.as_bytes())
            .context("failed to stream shapefile Silver JSONL row")?;
        add_quality_metrics(&mut quality_metrics, &handoff.quality_metrics);
        valid_pnu_count += 1;
        output_row_count += u64::try_from(rows.len()).context("output row count overflow")?;
        Ok(())
    })?;
    Ok(StreamReport {
        input_feature_count,
        valid_pnu_count,
        invalid_pnu_count,
        output_row_count,
        contract_table_name: empty_handoff.contract_table_name,
        quality_metrics,
    })
}

fn add_quality_metrics(target: &mut BTreeMap<String, u64>, row: &BTreeMap<String, u64>) {
    for (name, count) in row {
        *target.entry(name.clone()).or_insert(0) += count;
    }
}

fn write_summary(
    path: &Path,
    config: &ExportConfig,
    report: &ExportReport,
    contract_table_name: &str,
    quality_metrics: &BTreeMap<String, u64>,
) -> anyhow::Result<()> {
    let summary = json!({
        "schema_version": "foundation-platform.vworld_cadastral_shapefile_silver_handoff_export.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "source": {
            "kind": "vworld_shapefile_zip",
            "input_path": config.input_path.display().to_string(),
            "dataset_name": report.metadata.dataset_name,
            "cpg_label": report.metadata.cpg_label,
            "source_crs_name": report.metadata.source_crs_name,
            "shape_count": report.metadata.shape_count,
            "dbf_record_count": report.metadata.dbf_record_count,
            "seekable_member_bytes": report.metadata.seekable_member_bytes,
            "input_feature_count": report.input_feature_count,
            "source_record_id": config.source_record_id,
            "source_snapshot_id": config.source_snapshot_id
        },
        "output": {
            "path": config.output_path.display().to_string(),
            "contract": contract_table_name,
            "row_count": report.output_row_count,
            "bytes": report.output_bytes
        },
        "quality_metrics": quality_metrics,
        "performance": {
            "elapsed_milliseconds": report.elapsed_milliseconds
        },
        "evidence_limitations": [
            "local_shapefile_to_silver_handoff_only",
            "does_not_write_iceberg_table",
            "does_not_write_r2",
            "does_not_approve_national_rollout"
        ]
    });
    let bytes = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize shapefile export summary")?;
    let mut pending = PendingOutput::create(path)?;
    pending
        .file_mut()?
        .write_all(&bytes)
        .with_context(|| format!("failed to write staged summary for {}", path.display()))?;
    pending.sync_and_commit(path)
}

struct PendingOutput {
    path: PathBuf,
    file: Option<fs::File>,
    committed: bool,
}

impl PendingOutput {
    fn create(final_path: &Path) -> anyhow::Result<Self> {
        let parent = final_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
        let file_name = final_path
            .file_name()
            .context("output path must name a file")?
            .to_string_lossy();
        let path = parent.join(format!(".{file_name}.partial-{}", Uuid::new_v4()));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("failed to create staged output {}", path.display()))?;
        Ok(Self {
            path,
            file: Some(file),
            committed: false,
        })
    }

    fn file_mut(&mut self) -> anyhow::Result<&mut fs::File> {
        self.file
            .as_mut()
            .context("staged output is already closed")
    }

    fn sync_and_commit(mut self, final_path: &Path) -> anyhow::Result<()> {
        let file = self
            .file
            .take()
            .context("staged output is already closed")?;
        file.sync_all()
            .with_context(|| format!("failed to sync staged output {}", self.path.display()))?;
        drop(file);
        fs::rename(&self.path, final_path).with_context(|| {
            format!(
                "failed to promote staged output {} to {}",
                self.path.display(),
                final_path.display()
            )
        })?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(trimmed.to_owned())
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn parse_utc_env(name: &str) -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(name)?;
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("{name} must be an RFC3339 timestamp"))?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Cursor, Write as _},
    };

    use chrono::{DateTime, Utc};
    use shapefile::{
        dbase::{self, encoding::EncodingRs, FieldName, FieldValue},
        Point, Polygon, PolygonRing, ShapeWriter, Writer,
    };
    use uuid::Uuid;
    use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

    use super::{export_handoff, ExportConfig};

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

    #[test]
    fn streams_file_source_rows_through_the_existing_silver_contract() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-vworld-shapefile-export-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&root)?;
        let input_path = root.join("parcel.zip");
        fs::write(&input_path, fixture_zip()?)?;
        let output_path = root.join("parcel.jsonl");
        let summary_path = root.join("parcel.summary.json");
        let config = ExportConfig {
            input_path,
            output_path: output_path.clone(),
            summary_path: Some(summary_path.clone()),
            source_record_id: "bronze:vworldkr__parcel:fixture".to_owned(),
            source_snapshot_id: "vworldkr__parcel:202606".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")?
                .with_timezone(&Utc),
        };

        let report = export_handoff(&config)?;

        assert_eq!(report.input_feature_count, 3);
        assert_eq!(report.valid_pnu_count, 1);
        assert_eq!(report.invalid_pnu_count, 2);
        assert_eq!(report.output_row_count, 1);
        let lines = fs::read_to_string(&output_path)?
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["pnu"], "9999938029105800001");
        assert_eq!(lines[0]["sido_code"], "99");
        assert_eq!(lines[0]["sigungu_code"], "99999");
        assert_eq!(lines[0]["bjdong_code"], "9999938029");
        assert_eq!(lines[0]["jibun"], "산 580-1");
        assert_eq!(lines[0]["bonbun"], "0580");
        assert_eq!(lines[0]["bubun"], "0001");
        assert_eq!(lines[0]["geometry_srid"], 4326);
        assert_eq!(lines[0]["geometry_wkb_encoding"], "hex");
        assert_eq!(lines[0]["source_record_id"], config.source_record_id);
        let summary: serde_json::Value = serde_json::from_slice(&fs::read(summary_path)?)?;
        assert_eq!(summary["source"]["kind"], "vworld_shapefile_zip");
        assert_eq!(summary["quality_metrics"]["invalid_pnu_count"], 2);
        assert_eq!(summary["output"]["row_count"], 1);
        assert_eq!(summary["output"]["contract"], "silver.parcel_boundaries");
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn fixture_zip() -> anyhow::Result<Vec<u8>> {
        let mut shp = Cursor::new(Vec::new());
        let mut shx = Cursor::new(Vec::new());
        let mut dbf = Cursor::new(Vec::new());
        {
            let shape_writer = ShapeWriter::with_shx(&mut shp, &mut shx);
            let dbase_writer =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR))
                    .add_character_field(field_name("PNU")?, 19)
                    .add_character_field(field_name("JIBUN")?, 100)
                    .build_with_dest(&mut dbf);
            let mut writer = Writer::new(shape_writer, dbase_writer);
            writer.write_shape_and_record(
                &square(200_000.0, 600_000.0),
                &record("9999938029105800001", "산 580-1"),
            )?;
            writer.write_shape_and_record(
                &square(200_100.0, 600_100.0),
                &record("9999938029005810000", "581"),
            )?;
            writer.write_shape_and_record(
                &square(200_200.0, 600_200.0),
                &record("", "PNU 없는 행"),
            )?;
        }
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for (name, bytes) in [
            ("parcel.shp", shp.into_inner()),
            ("parcel.shx", shx.into_inner()),
            ("parcel.dbf", dbf.into_inner()),
            ("parcel.prj", KOREA_CENTRAL_BELT_2010.as_bytes().to_vec()),
            ("parcel.cpg", b"EUC-KR".to_vec()),
        ] {
            zip.start_file(name, options)?;
            zip.write_all(&bytes)?;
        }
        Ok(zip.finish()?.into_inner())
    }

    fn field_name(value: &str) -> anyhow::Result<FieldName> {
        FieldName::try_from(value).map_err(|error| anyhow::anyhow!(error))
    }

    fn square(min_x: f64, min_y: f64) -> Polygon {
        Polygon::new(PolygonRing::Outer(vec![
            Point::new(min_x, min_y),
            Point::new(min_x, min_y + 10.0),
            Point::new(min_x + 10.0, min_y + 10.0),
            Point::new(min_x + 10.0, min_y),
            Point::new(min_x, min_y),
        ]))
    }

    fn record(pnu: &str, jibun: &str) -> dbase::Record {
        let mut record = dbase::Record::default();
        record.insert(
            "PNU".to_owned(),
            FieldValue::Character(Some(pnu.to_owned())),
        );
        record.insert(
            "JIBUN".to_owned(),
            FieldValue::Character(Some(jibun.to_owned())),
        );
        record
    }
}
