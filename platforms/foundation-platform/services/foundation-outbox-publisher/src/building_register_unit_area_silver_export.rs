//! Building-register unit-area (전유공용면적) Bronze-to-Silver normalization export command.

use std::{
    collections::BTreeMap,
    env,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write as _},
    path::{Path, PathBuf},
};

mod parquet_row_writer;

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use lakehouse_application::{
    building_register_unit_area_silver_row_to_jsonl,
    normalize_building_register_unit_area_silver_rows,
    parse_building_register_unit_area_source_row_from_hub_bulk_text_line,
    BuildingRegisterUnitAreaSilverRow, BuildingRegisterUnitAreaSilverRowsInput,
};
use parquet_row_writer::ParquetUnitAreaRowWriter;
use zip::ZipArchive;

const DEFAULT_SOURCE_SLUG: &str = "hubgokr__building_register_exclusive_common_area";

struct UnitAreaExportConfig {
    bronze_local_object_root: PathBuf,
    source_slug: String,
    /// Exact Bronze zip file name to export. Required when the source prefix
    /// holds more than one zip — monthly snapshots accumulate, and a silent
    /// sorted-first pick would load an unverified month into canonical.
    source_object: Option<String>,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
    max_rows: Option<usize>,
    output_format: OutputFormat,
    chunk_rows: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UnitAreaExportReport {
    row_count: usize,
    accepted_count: u64,
    bronze_object_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputFormat {
    Jsonl,
    Parquet,
}

/// Runs the local 전유공용면적 Bronze-to-Silver normalization export.
///
/// # Errors
/// Returns an error when configuration, source decoding, normalization, or
/// output writing fails.
pub fn run() -> anyhow::Result<()> {
    let config = UnitAreaExportConfig::from_env()?;
    let report = export_handoff(&config)?;
    tracing::info!(
        row_count = report.row_count,
        accepted_count = report.accepted_count,
        bronze_object_key = %report.bronze_object_key,
        output_path = %config.output_path.display(),
        "전유공용면적 unit-area Silver normalization export succeeded"
    );
    Ok(())
}

fn export_handoff(config: &UnitAreaExportConfig) -> anyhow::Result<UnitAreaExportReport> {
    let object_path = locate_source_object(config)?;
    let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, &object_path)?;

    let mut output_writer =
        SilverRowWriter::new(&config.output_path, config.chunk_rows, config.output_format)?;
    let ingested_at_utc = Utc::now();
    let mut row_count = 0usize;
    let mut accepted_count = 0u64;
    let mut reason_counts = BTreeMap::<String, u64>::new();
    let mut area_kind_counts = BTreeMap::<String, u64>::new();

    decode_zip_lines(&object_path, config.max_rows, |line, line_number| {
        let record = parse_building_register_unit_area_source_row_from_hub_bulk_text_line(
            line,
            &bronze_object_key,
            line_number,
        )
        .with_context(|| format!("failed to parse 전유공용면적 line {line_number}"))?;
        let rows = normalize_building_register_unit_area_silver_rows(
            &BuildingRegisterUnitAreaSilverRowsInput {
                records: std::slice::from_ref(&record),
                source_snapshot_id: config.source_snapshot_id.as_str(),
                bronze_object_key: &bronze_object_key,
                valid_from_utc: config.valid_from_utc,
                ingested_at_utc,
            },
        )
        .with_context(|| {
            format!("failed to build 전유공용면적 Silver row at line {line_number}")
        })?;
        output_writer.write_rows(&rows)?;
        for row in &rows {
            row_count += 1;
            if row.normalization_status == "accepted" {
                accepted_count += 1;
            }
            *reason_counts
                .entry(row.normalization_reason.clone())
                .or_insert(0) += 1;
            *area_kind_counts.entry(row.area_kind.clone()).or_insert(0) += 1;
        }
        Ok(())
    })?;
    output_writer
        .flush()
        .context("failed to flush 전유공용면적 Silver handoff")?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(
            config,
            &bronze_object_key,
            row_count,
            accepted_count,
            &reason_counts,
            &area_kind_counts,
            summary_path,
        )?;
    }

    Ok(UnitAreaExportReport {
        row_count,
        accepted_count,
        bronze_object_key,
    })
}

impl UnitAreaExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: required_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_BRONZE_ROOT",
            )?,
            source_slug: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_SLUG",
            )?
            .unwrap_or_else(|| DEFAULT_SOURCE_SLUG.to_owned()),
            source_object: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_OBJECT",
            )?,
            output_path: required_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_PATH",
            )?,
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
            source_snapshot_id: required_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID",
            )?,
            valid_from_utc: parse_valid_from_env()?,
            max_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_MAX_ROWS",
            )?,
            output_format: OutputFormat::from_env()?,
            chunk_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_CHUNK_ROWS",
            )?,
        })
    }
}

impl OutputFormat {
    fn from_env() -> anyhow::Result<Self> {
        let Some(raw) = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_FORMAT",
        )?
        else {
            return Ok(Self::Jsonl);
        };
        match raw.as_str() {
            "jsonl" => Ok(Self::Jsonl),
            "parquet" => Ok(Self::Parquet),
            _ => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_OUTPUT_FORMAT must be one of jsonl, parquet"
            ),
        }
    }
}

fn locate_source_object(config: &UnitAreaExportConfig) -> anyhow::Result<PathBuf> {
    let source_root = config
        .bronze_local_object_root
        .join("bronze")
        .join(format!("source={}", config.source_slug));
    if !source_root.is_dir() {
        bail!(
            "전유공용면적 Bronze source directory not found: {}",
            source_root.display()
        );
    }
    if let Some(source_object) = config.source_object.as_deref() {
        let pinned = source_root.join(source_object);
        if !pinned.is_file() {
            bail!(
                "pinned 전유공용면적 Bronze object not found: {}",
                pinned.display()
            );
        }
        return Ok(pinned);
    }
    let mut zips = Vec::new();
    for entry in fs::read_dir(&source_root)
        .with_context(|| format!("failed to read {}", source_root.display()))?
    {
        let path = entry
            .with_context(|| format!("failed to read entry in {}", source_root.display()))?
            .path();
        if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
        {
            zips.push(path);
        }
    }
    zips.sort();
    if zips.len() > 1 {
        bail!(
            "전유공용면적 Bronze source {} holds {} zips — monthly snapshots are ambiguous; \
             set FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_SOURCE_OBJECT \
             (source_object) to the exact zip to export",
            source_root.display(),
            zips.len()
        );
    }
    zips.into_iter().next().with_context(|| {
        format!(
            "no 전유공용면적 Bronze zip found in {}",
            source_root.display()
        )
    })
}

fn decode_zip_lines(
    object_path: &Path,
    max_rows: Option<usize>,
    mut on_line: impl FnMut(&str, u64) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let file = File::open(object_path).with_context(|| {
        format!(
            "failed to open 전유공용면적 Bronze zip {}",
            object_path.display()
        )
    })?;
    let mut archive = ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read 전유공용면적 Bronze zip {}",
            object_path.display()
        )
    })?;
    let entry_index = single_file_entry_index(&mut archive)?;
    let entry = archive
        .by_index(entry_index)
        .with_context(|| format!("failed to open zip entry {entry_index}"))?;
    let reader = BufReader::new(entry);

    let mut decoded = 0usize;
    for (line_index, line_result) in reader.lines().enumerate() {
        if matches!(max_rows, Some(limit) if decoded >= limit) {
            break;
        }
        let line =
            line_result.with_context(|| format!("failed to read line {}", line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let line_number = u64::try_from(line_index + 1).context("line number exceeded u64")?;
        on_line(line.as_str(), line_number)?;
        decoded += 1;
    }
    Ok(())
}

fn single_file_entry_index<R: std::io::Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
) -> anyhow::Result<usize> {
    let mut file_indexes = Vec::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("failed to inspect zip entry {index}"))?;
        if !entry.is_dir() {
            file_indexes.push(index);
        }
    }
    match file_indexes.as_slice() {
        [index] => Ok(*index),
        [] => bail!("전유공용면적 Bronze zip must contain one TXT file, found none"),
        indexes => bail!(
            "전유공용면적 Bronze zip must contain one TXT file, found {}",
            indexes.len()
        ),
    }
}

fn write_summary(
    config: &UnitAreaExportConfig,
    bronze_object_key: &str,
    row_count: usize,
    accepted_count: u64,
    reason_counts: &BTreeMap<String, u64>,
    area_kind_counts: &BTreeMap<String, u64>,
    summary_path: &Path,
) -> anyhow::Result<()> {
    let proposal_count = row_count as u64 - accepted_count;
    let summary = serde_json::json!({
        "schema_version": "foundation-platform.building_register_unit_area_silver_handoff_export.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "source": {
            "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
            "source_slug": config.source_slug,
            "bronze_object_key": bronze_object_key,
            "source_snapshot_id": config.source_snapshot_id,
            "max_rows": config.max_rows,
        },
        "output": {
            "path": config.output_path.display().to_string(),
            "contract": "silver.building_register_unit_areas",
            "row_count": row_count,
            "accepted_count": accepted_count,
            "proposal_required_count": proposal_count,
            "reason_counts": reason_counts,
            "area_kind_counts": area_kind_counts,
        },
        "evidence_limitations": [
            "local_bronze_to_silver_handoff_only",
            "does_not_write_iceberg_table",
            "does_not_approve_production_cutover"
        ]
    });
    let payload = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize 전유공용면적 Silver export summary")?;
    write_file(summary_path, &payload)
}

fn bronze_object_key(root: &Path, object_path: &Path) -> anyhow::Result<String> {
    let relative = object_path.strip_prefix(root).with_context(|| {
        format!(
            "object {} is not under root {}",
            object_path.display(),
            root.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn create_file_writer(path: &Path) -> anyhow::Result<BufWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn prepare_clean_output_dir(path: &Path, label: &str) -> anyhow::Result<()> {
    if path.exists() {
        if !path.is_dir() {
            bail!(
                "{} output path is not a directory: {}",
                label,
                path.display()
            );
        }
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to clear {} directory {}", label, path.display()))?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create {} directory {}", label, path.display()))
}

fn write_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

fn parse_valid_from_env() -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_AREA_SILVER_HANDOFF_VALID_FROM_UTC",
    )?;
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("invalid valid_from_utc: {raw}"))?
        .to_utc())
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} is required"))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn required_path_env(name: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn optional_usize_env(name: &str) -> anyhow::Result<Option<usize>> {
    match optional_env(name)? {
        Some(value) => {
            Ok(Some(value.trim().parse::<usize>().with_context(|| {
                format!("{name} must be a non-negative integer")
            })?))
        }
        None => Ok(None),
    }
}

enum SilverRowWriter {
    Jsonl(JsonlRowWriter),
    Parquet(Box<ParquetUnitAreaRowWriter>),
}

impl SilverRowWriter {
    fn new(
        path: &Path,
        chunk_rows: Option<usize>,
        output_format: OutputFormat,
    ) -> anyhow::Result<Self> {
        match output_format {
            OutputFormat::Jsonl => Ok(Self::Jsonl(JsonlRowWriter::new(path, chunk_rows)?)),
            OutputFormat::Parquet => Ok(Self::Parquet(Box::new(ParquetUnitAreaRowWriter::new(
                path.to_path_buf(),
                chunk_rows,
            )?))),
        }
    }

    fn write_rows(&mut self, rows: &[BuildingRegisterUnitAreaSilverRow]) -> anyhow::Result<()> {
        match self {
            Self::Jsonl(writer) => writer.write_rows(rows),
            Self::Parquet(writer) => writer.write_rows(rows),
        }
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Jsonl(writer) => writer.flush(),
            Self::Parquet(writer) => writer.flush(),
        }
    }
}

enum JsonlRowWriter {
    Single(BufWriter<File>),
    Chunked(ChunkedJsonlRowWriter),
}

impl JsonlRowWriter {
    fn new(path: &Path, chunk_rows: Option<usize>) -> anyhow::Result<Self> {
        match chunk_rows {
            Some(chunk_rows) => Ok(Self::Chunked(ChunkedJsonlRowWriter::new(
                path.to_path_buf(),
                chunk_rows,
            )?)),
            None => Ok(Self::Single(create_file_writer(path)?)),
        }
    }

    fn write_rows(&mut self, rows: &[BuildingRegisterUnitAreaSilverRow]) -> anyhow::Result<()> {
        for row in rows {
            let jsonl = building_register_unit_area_silver_row_to_jsonl(row)
                .context("failed to serialize building-register unit-area Silver row")?;
            self.write_line(jsonl.as_str())?;
        }
        Ok(())
    }

    fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        match self {
            Self::Single(writer) => {
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
            }
            Self::Chunked(writer) => writer.write_line(line)?,
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Single(writer) => writer.flush()?,
            Self::Chunked(writer) => writer.flush()?,
        }
        Ok(())
    }
}

struct ChunkedJsonlRowWriter {
    root: PathBuf,
    chunk_rows: usize,
    current_row_count: usize,
    chunk_count: usize,
    current_writer: Option<BufWriter<File>>,
}

impl ChunkedJsonlRowWriter {
    fn new(root: PathBuf, chunk_rows: usize) -> anyhow::Result<Self> {
        prepare_clean_output_dir(&root, "chunked JSONL")?;
        Ok(Self {
            root,
            chunk_rows,
            current_row_count: 0,
            chunk_count: 0,
            current_writer: None,
        })
    }

    fn write_line(&mut self, line: &str) -> anyhow::Result<()> {
        if self.current_writer.is_none() || self.current_row_count >= self.chunk_rows {
            self.open_next_chunk()?;
        }
        let writer = self
            .current_writer
            .as_mut()
            .context("chunked JSONL writer missing current file")?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        self.current_row_count += 1;
        Ok(())
    }

    fn open_next_chunk(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.current_writer.as_mut() {
            writer.flush()?;
        }
        self.chunk_count += 1;
        self.current_row_count = 0;
        let path = self
            .root
            .join(format!("part-{:06}.jsonl", self.chunk_count));
        self.current_writer = Some(create_file_writer(&path)?);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.current_writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Array, RecordBatch};
    use uuid::Uuid;
    use zip::{write::SimpleFileOptions, ZipWriter};

    fn temp_root(name: &str) -> PathBuf {
        env::temp_dir().join(format!("{name}-{}", Uuid::new_v4()))
    }

    /// Synthetic 39-field line that exercises the provider's documented layout.
    fn area_line(pk: &str, unit: &str, area_kind_code: &str, area_m2: &str) -> String {
        let mut fields = vec![String::new(); 39];
        fields[0] = pk.to_owned();
        fields[2] = "집합".to_owned();
        fields[4] = "전유부".to_owned();
        fields[8] = "99999".to_owned();
        fields[9] = "00301".to_owned();
        fields[10] = "0".to_owned();
        fields[11] = "0171".to_owned();
        fields[12] = "0000".to_owned();
        fields[21] = "SYNTHETIC-BUILDING".to_owned();
        fields[22] = unit.to_owned();
        fields[23] = "20".to_owned();
        fields[24] = "지상".to_owned();
        fields[25] = "4".to_owned();
        fields[26] = area_kind_code.to_owned();
        fields[27] = if area_kind_code == "1" {
            "전유"
        } else {
            "공용"
        }
        .to_owned();
        fields[29] = "주건축물".to_owned();
        fields[30] = "4층".to_owned();
        fields[32] = "철골철근콘크리트구조".to_owned();
        fields[34] = "14202".to_owned();
        fields[35] = "오피스텔".to_owned();
        fields[36] = "오피스텔".to_owned();
        fields[37] = area_m2.to_owned();
        fields[38] = "20991231".to_owned();
        fields.join("|")
    }

    fn write_zip_file(path: &Path, entry_name: &str, payload: &[u8]) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut buffer = Vec::new();
        {
            let cursor = std::io::Cursor::new(&mut buffer);
            let mut writer = ZipWriter::new(cursor);
            writer.start_file(entry_name, SimpleFileOptions::default())?;
            writer.write_all(payload)?;
            writer.finish()?;
        }
        fs::write(path, buffer)?;
        Ok(())
    }

    #[test]
    fn multi_zip_source_requires_explicit_object_pin() -> anyhow::Result<()> {
        // 월별 zip이 누적된 prefix에서 sorted-first 를 조용히 고르면 검증 안 된
        // 스냅샷이 canonical 로 실린다 — 핀 없이 2개 이상이면 시끄럽게 실패한다.
        let root = temp_root("foundation-platform-building-register-unit-area-multi-zip");
        let dir = format!("bronze/source={DEFAULT_SOURCE_SLUG}");
        let payload = area_line(
            "SYNTHETIC-AREA-PK-0001",
            "SYNTHETIC-UNIT-416",
            "1",
            "42.125",
        );
        for name in ["OPN209912310000000001.zip", "OPN209912310000000002.zip"] {
            write_zip_file(
                &root.join(&dir).join(name),
                "mart_djy_06.txt",
                payload.as_bytes(),
            )?;
        }

        let mut config = UnitAreaExportConfig {
            bronze_local_object_root: root.clone(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            output_path: root.join("out.jsonl"),
            summary_path: None,
            source_snapshot_id: "synthetic-building-register-unit-area-20991231".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2099-12-31T00:00:00Z")?.to_utc(),
            max_rows: None,
            output_format: OutputFormat::Jsonl,
            chunk_rows: None,
        };
        let error = export_handoff(&config).expect_err("ambiguous source must fail");
        assert!(error.to_string().contains("source_object"), "{error}");

        // 핀을 주면 sorted-first 가 아닌 지정 zip 을 정확히 선택한다.
        config.source_object = Some("OPN209912310000000002.zip".to_owned());
        let report = export_handoff(&config)?;
        assert_eq!(report.row_count, 1);
        assert!(report
            .bronze_object_key
            .ends_with("OPN209912310000000002.zip"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_hub_bulk_zip_to_chunked_parquet_parts() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-unit-area-parquet-export");
        let object_key =
            "bronze/source=hubgokr__building_register_exclusive_common_area/OPN209912310000000002.zip";
        let payload = [
            area_line(
                "SYNTHETIC-AREA-PK-0001",
                "SYNTHETIC-UNIT-416",
                "1",
                "42.125",
            ),
            area_line("SYNTHETIC-AREA-PK-0001", "SYNTHETIC-UNIT-416", "2", "8.250"),
            area_line("SYNTHETIC-AREA-PK-INVALID", "", "9", "abc"),
        ]
        .join("\n");
        write_zip_file(
            &root.join(object_key),
            "mart_djy_06.txt",
            payload.as_bytes(),
        )?;
        let output_dir = root
            .join("silver-handoff")
            .join("building_register_unit_areas_parquet");
        let summary_path = root.join("summary").join("unit-area-summary.json");

        let report = export_handoff(&UnitAreaExportConfig {
            bronze_local_object_root: root.clone(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            output_path: output_dir.clone(),
            summary_path: Some(summary_path.clone()),
            source_snapshot_id: "synthetic-building-register-unit-area-20991231".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2099-12-31T00:00:00Z")?.to_utc(),
            max_rows: None,
            output_format: OutputFormat::Parquet,
            chunk_rows: Some(2),
        })?;

        assert_eq!(report.row_count, 3);
        assert_eq!(report.accepted_count, 2);
        assert!(output_dir.join("part-000001.parquet").is_file());
        assert!(output_dir.join("part-000002.parquet").is_file());

        let first_batch = read_first_parquet_batch(&output_dir.join("part-000001.parquet"))?;
        assert_eq!(first_batch.num_rows(), 2);
        assert_eq!(first_batch.schema().field(0).name(), "area_row_id");
        // 표준 PNU (허브 대지구분 0 → 표준 1) + 내부 조인 키는 허브 조립 유지 (ADR 0023).
        assert_eq!(string_value(&first_batch, "pnu", 0)?, "9999900301101710000");
        assert_eq!(
            string_value(&first_batch, "register_parcel_key", 0)?,
            "9999900301001710000"
        );
        assert_eq!(string_value(&first_batch, "area_kind", 0)?, "exclusive");
        assert_eq!(string_value(&first_batch, "area_kind", 1)?, "common");
        assert_eq!(
            string_value(&first_batch, "unit_designation", 0)?,
            "SYNTHETIC-UNIT-416"
        );
        assert!((f64_value(&first_batch, "area_m2", 0)? - 42.125).abs() < 1e-9);
        assert_eq!(
            string_value(&first_batch, "normalization_status", 0)?,
            "accepted"
        );

        let second_batch = read_first_parquet_batch(&output_dir.join("part-000002.parquet"))?;
        assert_eq!(second_batch.num_rows(), 1);
        assert_eq!(
            string_value(&second_batch, "normalization_status", 0)?,
            "proposal_required"
        );
        assert_eq!(
            string_value(&second_batch, "normalization_reason", 0)?,
            "invalid_area"
        );

        let summary =
            serde_json::from_str::<serde_json::Value>(&fs::read_to_string(&summary_path)?)?;
        assert_eq!(summary["output"]["row_count"], 3);
        assert_eq!(summary["output"]["accepted_count"], 2);
        assert_eq!(summary["output"]["area_kind_counts"]["exclusive"], 1);
        assert_eq!(summary["output"]["area_kind_counts"]["common"], 1);
        assert_eq!(summary["output"]["area_kind_counts"]["unknown"], 1);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn read_first_parquet_batch(path: &PathBuf) -> anyhow::Result<RecordBatch> {
        let file = File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        reader
            .next()
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("expected at least one parquet batch"))
    }

    fn string_value(batch: &RecordBatch, column_name: &str, row: usize) -> anyhow::Result<String> {
        let column_index = column_index(batch, column_name)?;
        let array = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .with_context(|| format!("column {column_name} is not Utf8"))?;
        if array.is_null(row) {
            return Ok(String::new());
        }
        Ok(array.value(row).to_owned())
    }

    fn f64_value(batch: &RecordBatch, column_name: &str, row: usize) -> anyhow::Result<f64> {
        let column_index = column_index(batch, column_name)?;
        let array = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .with_context(|| format!("column {column_name} is not Float64"))?;
        Ok(array.value(row))
    }

    fn column_index(batch: &RecordBatch, column_name: &str) -> anyhow::Result<usize> {
        batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == column_name)
            .with_context(|| format!("missing column {column_name}"))
    }
}
