//! Building-register unit Bronze-to-Silver normalization export command.

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
use foundation_normalization_application::ActiveBuildingRegisterUnitOverrideReader;
use foundation_normalization_infrastructure::PgActiveBuildingRegisterUnitOverrideReader;
use lakehouse_application::{
    building_register_unit_silver_override_from_application_snapshot,
    building_register_unit_silver_row_to_jsonl,
    normalize_building_register_unit_silver_rows_with_building_keys,
    parse_building_register_unit_source_row_from_hub_bulk_text_line,
    parse_building_title_building_link_from_hub_bulk_text_line, BuildingRegisterUnitSilverOverride,
    BuildingRegisterUnitSilverOverrideIndex, BuildingRegisterUnitSilverRow,
    BuildingRegisterUnitSilverRowsInput, BuildingTitleKeyIndex,
};
use parquet_row_writer::ParquetUnitRowWriter;
use sqlx::PgPool;
use zip::ZipArchive;

const DEFAULT_SOURCE_SLUG: &str = "hubgokr__building_register_exclusive_unit";
const DEFAULT_TITLE_SOURCE_SLUG: &str = "hubgokr__building_register_main";
const APPLY_APPROVED_OVERRIDES_ENV: &str =
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_APPLY_APPROVED_OVERRIDES";

struct UnitExportConfig {
    bronze_local_object_root: PathBuf,
    source_slug: String,
    /// Exact Bronze zip to export; required when monthly zips accumulate
    /// (silent sorted-first would load an unverified month into canonical).
    source_object: Option<String>,
    title_source_slug: Option<String>,
    /// Exact building-register title Bronze zip for the building-key index; same pin rule.
    title_source_object: Option<String>,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
    max_rows: Option<usize>,
    output_format: OutputFormat,
    chunk_rows: Option<usize>,
    active_overrides: Vec<BuildingRegisterUnitSilverOverride>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UnitExportReport {
    row_count: usize,
    accepted_count: u64,
    applied_override_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputFormat {
    Jsonl,
    Parquet,
}

/// Runs the local building-register unit Bronze-to-Silver normalization export.
pub async fn run() -> anyhow::Result<()> {
    let mut config = UnitExportConfig::from_env()?;
    config.active_overrides = load_active_unit_overrides_from_env().await?;
    let report = export_handoff(&config)?;
    tracing::info!(
        row_count = report.row_count,
        accepted_count = report.accepted_count,
        applied_override_count = report.applied_override_count,
        output_path = %config.output_path.display(),
        "building-register unit Silver normalization export succeeded"
    );
    Ok(())
}

async fn load_active_unit_overrides_from_env(
) -> anyhow::Result<Vec<BuildingRegisterUnitSilverOverride>> {
    if !optional_bool_env(APPLY_APPROVED_OVERRIDES_ENV)?.unwrap_or(true) {
        return Ok(Vec::new());
    }
    let database_url = required_env("DATABASE_URL").with_context(|| {
        format!("DATABASE_URL is required unless {APPLY_APPROVED_OVERRIDES_ENV}=0")
    })?;
    let pool = PgPool::connect(database_url.as_str())
        .await
        .context("failed to connect to DATABASE_URL for building-register unit override load")?;
    let reader = PgActiveBuildingRegisterUnitOverrideReader::new(pool);
    load_active_unit_overrides(&reader).await
}

async fn load_active_unit_overrides(
    reader: &dyn ActiveBuildingRegisterUnitOverrideReader,
) -> anyhow::Result<Vec<BuildingRegisterUnitSilverOverride>> {
    let applications = reader
        .list_active_building_register_unit_overrides()
        .await
        .context("failed to load active building-register unit normalization applications")?;
    applications
        .iter()
        .map(|application| {
            let mut override_record =
                building_register_unit_silver_override_from_application_snapshot(
                    &application.snapshot,
                )?;
            override_record.application_id = Some(application.application_id.to_string());
            Ok::<
                BuildingRegisterUnitSilverOverride,
                lakehouse_application::BuildingRegisterUnitSilverPlanError,
            >(override_record)
        })
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse active building-register unit normalization applications")
}

fn export_handoff(config: &UnitExportConfig) -> anyhow::Result<UnitExportReport> {
    let object_path = locate_source_object(config)?;
    let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, &object_path)?;
    let building_keys = load_building_key_index(config)?;
    let active_overrides =
        BuildingRegisterUnitSilverOverrideIndex::new(&config.active_overrides)
            .context("failed to build active building-register unit override index")?;

    let mut output_writer =
        SilverRowWriter::new(&config.output_path, config.chunk_rows, config.output_format)?;
    let ingested_at_utc = Utc::now();
    let mut row_count = 0usize;
    let mut accepted_count = 0u64;
    let mut applied_override_count = 0u64;
    let mut reason_counts = BTreeMap::<String, u64>::new();
    let mut link_method_counts = BTreeMap::<String, u64>::new();

    decode_zip_lines(&object_path, config.max_rows, |line, line_number| {
        let record = parse_building_register_unit_source_row_from_hub_bulk_text_line(
            line,
            &bronze_object_key,
            line_number,
        )
        .with_context(|| format!("failed to parse building-register unit line {line_number}"))?;
        let mut rows = normalize_building_register_unit_silver_rows_with_building_keys(
            &BuildingRegisterUnitSilverRowsInput {
                records: std::slice::from_ref(&record),
                source_snapshot_id: config.source_snapshot_id.as_str(),
                bronze_object_key: &bronze_object_key,
                valid_from_utc: config.valid_from_utc,
                ingested_at_utc,
            },
            &building_keys,
        )
        .context("failed to build building-register unit Silver row")?;
        for row in &mut rows {
            if active_overrides
                .apply_to_row(row)
                .context("failed to apply active building-register unit override")?
            {
                applied_override_count += 1;
            }
        }
        output_writer.write_rows(&rows)?;
        for row in &rows {
            row_count += 1;
            if row.normalization_status == "accepted" {
                accepted_count += 1;
            }
            *reason_counts
                .entry(row.normalization_reason.clone())
                .or_insert(0) += 1;
            *link_method_counts
                .entry(row.building_link_method.clone())
                .or_insert(0) += 1;
        }
        Ok(())
    })?;
    output_writer
        .flush()
        .context("failed to flush building-register unit Silver handoff")?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(
            config,
            &bronze_object_key,
            row_count,
            accepted_count,
            applied_override_count,
            &reason_counts,
            &link_method_counts,
            building_keys.len(),
            summary_path,
        )?;
    }

    Ok(UnitExportReport {
        row_count,
        accepted_count,
        applied_override_count,
    })
}

/// Streams the building-register title Bronze zip into a `(PNU + dong name) -> building key` index.
fn load_building_key_index(config: &UnitExportConfig) -> anyhow::Result<BuildingTitleKeyIndex> {
    let mut index = BuildingTitleKeyIndex::new();
    let Some(slug) = config.title_source_slug.as_deref() else {
        return Ok(index);
    };
    let source_root = config
        .bronze_local_object_root
        .join("bronze")
        .join(format!("source={slug}"));
    if !source_root.is_dir() {
        tracing::warn!(
            source_root = %source_root.display(),
            "title building-key source not found; units will be left unlinked"
        );
        return Ok(index);
    }
    let title_object = locate_zip_object(
        &config.bronze_local_object_root,
        slug,
        config.title_source_object.as_deref(),
        "title",
    )?;
    decode_zip_lines(&title_object, None, |line, _| {
        if let Some(entry) = parse_building_title_building_link_from_hub_bulk_text_line(line) {
            index.insert(entry);
        }
        Ok(())
    })?;
    Ok(index)
}

impl UnitExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: required_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_BRONZE_ROOT",
            )?,
            source_slug: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_SLUG",
            )?
            .unwrap_or_else(|| DEFAULT_SOURCE_SLUG.to_owned()),
            source_object: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_OBJECT",
            )?,
            title_source_slug: match optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_TITLE_SOURCE_SLUG",
            )? {
                Some(value) if value == "none" => None,
                Some(value) => Some(value),
                None => Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
            },
            title_source_object: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_TITLE_SOURCE_OBJECT",
            )?,
            output_path: required_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_OUTPUT_PATH",
            )?,
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
            source_snapshot_id: required_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID",
            )?,
            valid_from_utc: parse_valid_from_env()?,
            max_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_MAX_ROWS",
            )?,
            output_format: OutputFormat::from_env()?,
            chunk_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_CHUNK_ROWS",
            )?,
            active_overrides: Vec::new(),
        })
    }
}

impl OutputFormat {
    fn from_env() -> anyhow::Result<Self> {
        let Some(raw) = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_OUTPUT_FORMAT",
        )?
        else {
            return Ok(Self::Jsonl);
        };
        match raw.as_str() {
            "jsonl" => Ok(Self::Jsonl),
            "parquet" => Ok(Self::Parquet),
            _ => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_OUTPUT_FORMAT must be one of jsonl, parquet"
            ),
        }
    }
}

fn locate_source_object(config: &UnitExportConfig) -> anyhow::Result<PathBuf> {
    locate_zip_object(
        &config.bronze_local_object_root,
        &config.source_slug,
        config.source_object.as_deref(),
        "unit",
    )
}

/// Chooses one zip from a prefix that may accumulate monthly snapshots.
/// A pin selects its exact object; without a pin, only a single zip is accepted.
/// Multiple unpinned zips fail loudly instead of silently selecting the first month.
fn locate_zip_object(
    root: &Path,
    slug: &str,
    pinned_object: Option<&str>,
    label: &str,
) -> anyhow::Result<PathBuf> {
    let source_root = root.join("bronze").join(format!("source={slug}"));
    if !source_root.is_dir() {
        bail!(
            "{label} Bronze source directory not found: {}",
            source_root.display()
        );
    }
    if let Some(object) = pinned_object {
        let pinned = source_root.join(object);
        if !pinned.is_file() {
            bail!(
                "pinned {label} Bronze object not found: {}",
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
            concat!(
                "{} Bronze source {} holds {} zips; monthly snapshots are ambiguous; ",
                "set the source_object pin to the exact zip to export"
            ),
            label,
            source_root.display(),
            zips.len()
        );
    }
    zips.into_iter()
        .next()
        .with_context(|| format!("no {label} Bronze zip found in {}", source_root.display()))
}

fn decode_zip_lines(
    object_path: &Path,
    max_rows: Option<usize>,
    mut on_line: impl FnMut(&str, u64) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let file = File::open(object_path).with_context(|| {
        format!(
            "failed to open building-register unit Bronze zip {}",
            object_path.display()
        )
    })?;
    let mut archive = ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read building-register unit Bronze zip {}",
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
        [] => bail!("building-register unit Bronze zip must contain one TXT file, found none"),
        indexes => bail!(
            "building-register unit Bronze zip must contain one TXT file, found {}",
            indexes.len()
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn write_summary(
    config: &UnitExportConfig,
    bronze_object_key: &str,
    row_count: usize,
    accepted_count: u64,
    applied_override_count: u64,
    reason_counts: &BTreeMap<String, u64>,
    link_method_counts: &BTreeMap<String, u64>,
    building_key_count: usize,
    summary_path: &Path,
) -> anyhow::Result<()> {
    let proposal_count = row_count as u64 - accepted_count;
    let linked = link_method_counts
        .iter()
        .filter(|(method, _)| method.as_str() != "unresolved")
        .map(|(_, count)| count)
        .sum::<u64>();
    let summary = serde_json::json!({
        "schema_version": "foundation-platform.building_register_unit_silver_handoff_export.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "source": {
            "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
            "source_slug": config.source_slug,
            "title_source_slug": config.title_source_slug,
            "bronze_object_key": bronze_object_key,
            "source_snapshot_id": config.source_snapshot_id,
            "max_rows": config.max_rows,
            "title_building_key_count": building_key_count,
        },
        "output": {
            "path": config.output_path.display().to_string(),
            "contract": "silver.building_register_units",
            "row_count": row_count,
            "accepted_count": accepted_count,
            "applied_override_count": applied_override_count,
            "proposal_required_count": proposal_count,
            "reason_counts": reason_counts,
            "building_linked_count": linked,
            "building_link_method_counts": link_method_counts,
        },
        "evidence_limitations": [
            "local_bronze_to_silver_handoff_only",
            "does_not_write_iceberg_table",
            "fuzzy_dong_names_left_for_splink",
            "does_not_approve_production_cutover"
        ]
    });
    let payload = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize building-register unit Silver export summary")?;
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
    let raw =
        required_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_UNIT_SILVER_HANDOFF_VALID_FROM_UTC")?;
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

fn optional_bool_env(name: &str) -> anyhow::Result<Option<bool>> {
    let Some(value) = optional_env(name)? else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(Some(true)),
        "0" | "false" => Ok(Some(false)),
        _ => bail!("{name} must be one of 1, 0, true, false"),
    }
}

enum SilverRowWriter {
    Jsonl(JsonlRowWriter),
    Parquet(Box<ParquetUnitRowWriter>),
}

impl SilverRowWriter {
    fn new(
        path: &Path,
        chunk_rows: Option<usize>,
        output_format: OutputFormat,
    ) -> anyhow::Result<Self> {
        match output_format {
            OutputFormat::Jsonl => Ok(Self::Jsonl(JsonlRowWriter::new(path, chunk_rows)?)),
            OutputFormat::Parquet => Ok(Self::Parquet(Box::new(ParquetUnitRowWriter::new(
                path.to_path_buf(),
                chunk_rows,
            )?))),
        }
    }

    fn write_rows(&mut self, rows: &[BuildingRegisterUnitSilverRow]) -> anyhow::Result<()> {
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

    fn write_rows(&mut self, rows: &[BuildingRegisterUnitSilverRow]) -> anyhow::Result<()> {
        for row in rows {
            let jsonl = building_register_unit_silver_row_to_jsonl(row)
                .context("failed to serialize building-register unit Silver row")?;
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

    fn unit_line(
        pk: &str,
        dong: &str,
        unit: &str,
        floor_code: &str,
        floor_name: &str,
        floor_no: &str,
    ) -> String {
        let mut fields = vec![String::new(); 26];
        fields[0] = pk.to_owned();
        fields[8] = "99999".to_owned();
        fields[9] = "00401".to_owned();
        fields[10] = "0".to_owned();
        fields[11] = "0089".to_owned();
        fields[12] = "0004".to_owned();
        fields[21] = dong.to_owned();
        fields[22] = unit.to_owned();
        fields[23] = floor_code.to_owned();
        fields[24] = floor_name.to_owned();
        fields[25] = floor_no.to_owned();
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
    fn multi_zip_sources_require_explicit_object_pins() -> anyhow::Result<()> {
        // Monthly snapshots require an exact pin; never silently select the first zip.
        let root = temp_root("foundation-platform-building-register-unit-multi-zip");
        let unit_dir = format!("bronze/source={DEFAULT_SOURCE_SLUG}");
        let payload = unit_line("1002129933", "102", "624", "20", "above", "6");
        for name in ["OPN209912310000000003.zip", "OPN209912310000000011.zip"] {
            write_zip_file(
                &root.join(&unit_dir).join(name),
                "mart_djy_09.txt",
                payload.as_bytes(),
            )?;
        }

        let mut config = UnitExportConfig {
            bronze_local_object_root: root.clone(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            title_source_slug: None,
            title_source_object: None,
            output_path: root.join("out.jsonl"),
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-unit-20260620".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-06-20T00:00:00Z")?.to_utc(),
            max_rows: None,
            output_format: OutputFormat::Jsonl,
            chunk_rows: None,
            active_overrides: Vec::new(),
        };
        let error = export_handoff(&config).expect_err("ambiguous unit source must fail");
        assert!(error.to_string().contains("source_object"), "{error}");

        config.source_object = Some("OPN209912310000000011.zip".to_owned());
        let report = export_handoff(&config)?;
        assert_eq!(report.row_count, 1);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_hub_bulk_zip_to_chunked_parquet_parts_without_jsonl_bloat() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-unit-parquet-export");
        let object_key =
            "bronze/source=hubgokr__building_register_exclusive_unit/OPN209912310000000003.zip";
        let payload = [
            unit_line("1002129933", "102", "624", "20", "above", "6"),
            unit_line("1002129934", "", "", "20", "above", "2"),
        ]
        .join("\n");
        write_zip_file(
            &root.join(object_key),
            "mart_djy_09.txt",
            payload.as_bytes(),
        )?;
        let output_dir = root
            .join("silver-handoff")
            .join("building_register_units_parquet");

        let report = export_handoff(&UnitExportConfig {
            bronze_local_object_root: root.clone(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            title_source_slug: None,
            title_source_object: None,
            output_path: output_dir.clone(),
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-unit-20260420".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-04-20T00:00:00Z")?.to_utc(),
            max_rows: None,
            output_format: OutputFormat::Parquet,
            chunk_rows: Some(1),
            active_overrides: Vec::new(),
        })?;

        assert_eq!(report.row_count, 2);
        assert_eq!(report.accepted_count, 1);
        assert!(output_dir.join("part-000001.parquet").is_file());
        assert!(output_dir.join("part-000002.parquet").is_file());
        assert!(!output_dir.join("part-000001.jsonl").exists());

        let first_batch = read_first_parquet_batch(&output_dir.join("part-000001.parquet"))?;
        assert_eq!(first_batch.num_rows(), 1);
        assert_eq!(first_batch.schema().field(0).name(), "unit_row_id");
        // Canonical PNU maps provider land kind 0 to 1; the internal join key stays provider-shaped.
        assert_eq!(string_value(&first_batch, "pnu", 0)?, "9999900401100890004");
        assert_eq!(
            string_value(&first_batch, "register_parcel_key", 0)?,
            "9999900401000890004"
        );
        assert_eq!(string_value(&first_batch, "unit_name_raw", 0)?, "624");
        assert_eq!(string_value(&first_batch, "unit_label_ko", 0)?, "");
        assert_eq!(string_value(&first_batch, "unit_designation", 0)?, "624");
        // Title-attr columns exist even when no title zip was staged (all null).
        assert!(first_batch
            .schema()
            .column_with_name("building_main_or_annex")
            .is_some());
        assert!(first_batch
            .schema()
            .column_with_name("building_title_unit_count")
            .is_some());
        assert_eq!(
            string_value(&first_batch, "normalization_status", 0)?,
            "accepted"
        );

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn export_applies_active_unit_override_before_writing_handoff() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-unit-override-export");
        let object_key =
            "bronze/source=hubgokr__building_register_exclusive_unit/OPN209912310000000003.zip";
        let payload = unit_line("1002129933", "A", "unit", "20", "above", "1");
        write_zip_file(
            &root.join(object_key),
            "mart_djy_09.txt",
            payload.as_bytes(),
        )?;
        let output_file = root
            .join("silver-handoff")
            .join("building_register_units.jsonl");
        let target_unit_row_id = format!("building-register-unit:{object_key}#line-000001");

        let report = export_handoff(&UnitExportConfig {
            bronze_local_object_root: root.clone(),
            source_slug: DEFAULT_SOURCE_SLUG.to_owned(),
            source_object: None,
            title_source_slug: None,
            title_source_object: None,
            output_path: output_file.clone(),
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-unit-20260420".to_owned(),
            valid_from_utc: DateTime::parse_from_rfc3339("2026-04-20T00:00:00Z")?.to_utc(),
            max_rows: None,
            output_format: OutputFormat::Jsonl,
            chunk_rows: None,
            active_overrides: vec![lakehouse_application::BuildingRegisterUnitSilverOverride {
                target_unit_row_id,
                application_id: Some("normalization-application-approved-1".to_owned()),
                unit_number: Some(101),
                unit_label_ko: None,
                building_mgm_bldrgst_pk: Some("building-pk-approved".to_owned()),
                building_link_method: "canonical_dong".to_owned(),
                normalization_status: "accepted".to_owned(),
                normalization_reason: "accepted_numeric_unit".to_owned(),
            }],
        })?;

        assert_eq!(report.row_count, 1);
        assert_eq!(report.accepted_count, 1);
        assert_eq!(report.applied_override_count, 1);

        let line = fs::read_to_string(&output_file)?;
        let value = serde_json::from_str::<serde_json::Value>(line.trim())?;
        assert_eq!(value["unit_number"], 101);
        assert_eq!(value["building_mgm_bldrgst_pk"], "building-pk-approved");
        assert_eq!(value["building_link_method"], "canonical_dong");
        assert_eq!(value["normalization_status"], "accepted");
        assert_eq!(value["normalization_reason"], "accepted_numeric_unit");
        assert_eq!(
            value["normalization_application_id"],
            "normalization-application-approved-1"
        );

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn active_unit_override_loader_consumes_opaque_normalization_records(
    ) -> anyhow::Result<()> {
        use async_trait::async_trait;
        use foundation_normalization_application::{
            ActiveBuildingRegisterUnitOverride, ActiveBuildingRegisterUnitOverrideReader,
        };
        use foundation_normalization_domain::NormalizationError;

        struct StubReader {
            records: Vec<ActiveBuildingRegisterUnitOverride>,
        }

        #[async_trait]
        impl ActiveBuildingRegisterUnitOverrideReader for StubReader {
            async fn list_active_building_register_unit_overrides(
                &self,
            ) -> Result<Vec<ActiveBuildingRegisterUnitOverride>, NormalizationError> {
                Ok(self.records.clone())
            }
        }

        let application_id = Uuid::now_v7();
        let overrides = load_active_unit_overrides(&StubReader {
            records: vec![ActiveBuildingRegisterUnitOverride {
                application_id,
                snapshot: serde_json::json!({
                    "target_identity": {"raw_record_id":"unit-row-1"},
                    "proposed_record": {
                        "building_link_method":"canonical_dong",
                        "normalization_reason":"accepted_numeric_unit",
                        "normalization_status":"accepted",
                        "unit_number":101
                    }
                }),
            }],
        })
        .await?;

        assert_eq!(overrides.len(), 1);
        let expected_application_id = application_id.to_string();
        assert_eq!(
            overrides[0].application_id.as_deref(),
            Some(expected_application_id.as_str())
        );
        assert_eq!(overrides[0].unit_number, Some(101));
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
        let column_index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == column_name)
            .with_context(|| format!("missing column {column_name}"))?;
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
}
