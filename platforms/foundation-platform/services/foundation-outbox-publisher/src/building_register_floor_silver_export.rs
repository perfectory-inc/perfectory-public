//! Building-register floor Bronze-to-Silver handoff export command.

use std::{
    collections::HashMap,
    env, fs,
    fs::File,
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
};

mod hub_bulk_decoder;
mod parquet_row_writer;
mod title_counts_loader;

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use foundation_normalization_domain::BuildingFloorCounts;
use lakehouse_application::{
    build_building_register_floor_normalization_proposal_input,
    build_building_register_floor_silver_handoff,
    normalize_building_register_floor_silver_rows_from_public_data_bronze_json,
    normalize_building_register_floor_silver_rows_with_title_counts,
    BuildingRegisterFloorSilverRow, BuildingRegisterFloorSilverRowsInput,
    BuildingRegisterFloorSourceRow, PublicDataBuildingRegisterFloorBronzeJsonInput,
};
use parquet_row_writer::ParquetSilverRowWriter;
use title_counts_loader::load_building_title_floor_counts;

/// Default 표제부 Bronze source slug used as the building-title floor-count witness.
const DEFAULT_TITLE_SOURCE_SLUG: &str = "hubgokr__building_register_main";

/// Runs the local Bronze-to-Silver handoff export.
pub fn run() -> anyhow::Result<()> {
    let config = ExportConfig::from_env()?;
    let report = export_handoff(&config)?;
    tracing::info!(
        input_object_count = report.input_object_count,
        row_count = report.row_count,
        proposal_required_count = report.proposal_required_count,
        output_path = %config.output_path.display(),
        "building-register floor Silver handoff export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportConfig {
    bronze_local_object_root: PathBuf,
    source_selector: SourceSelector,
    output_path: PathBuf,
    proposal_input_path: Option<PathBuf>,
    summary_path: Option<PathBuf>,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
    max_rows: Option<usize>,
    chunk_rows: Option<usize>,
    output_format: OutputFormat,
    /// 표제부 Bronze source slug providing the building-title floor-count witness,
    /// or `None` to resolve floors from the two internal witnesses only.
    title_source_slug: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceSelector {
    Exact(String),
    Prefix(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportReport {
    input_object_count: usize,
    row_count: usize,
    proposal_required_count: u64,
    normalization_proposal_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputFormat {
    Jsonl,
    Parquet,
}

impl ExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: required_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_BRONZE_ROOT",
            )?,
            source_selector: SourceSelector::from_env()?,
            output_path: required_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_PATH",
            )?,
            proposal_input_path: optional_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_NORMALIZATION_PROPOSAL_INPUT_PATH",
            )?,
            summary_path: optional_path_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SUMMARY_PATH",
            )?,
            source_snapshot_id: required_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID",
            )?,
            valid_from_utc: parse_utc_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_VALID_FROM_UTC",
            )?,
            max_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_MAX_ROWS",
            )?,
            chunk_rows: optional_usize_env(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_CHUNK_ROWS",
            )?,
            output_format: OutputFormat::from_env()?,
            title_source_slug: title_source_slug_from_env()?,
        })
    }
}

/// Reads the 표제부 witness source slug: defaults to `hubgokr__building_register_main`,
/// and an empty or `none` value disables the building-title witness.
fn title_source_slug_from_env() -> anyhow::Result<Option<String>> {
    Ok(
        match optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_TITLE_SOURCE_SLUG",
        )? {
            Some(value) if value.trim().is_empty() || value == "none" => None,
            Some(value) => Some(value),
            None => Some(DEFAULT_TITLE_SOURCE_SLUG.to_owned()),
        },
    )
}

impl OutputFormat {
    fn from_env() -> anyhow::Result<Self> {
        let Some(raw) = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT",
        )?
        else {
            return Ok(Self::Jsonl);
        };
        match raw.as_str() {
            "jsonl" => Ok(Self::Jsonl),
            "parquet" => Ok(Self::Parquet),
            _ => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_OUTPUT_FORMAT must be one of jsonl, parquet"
            ),
        }
    }

    const fn wire_name(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Parquet => "parquet",
        }
    }
}

impl SourceSelector {
    fn from_env() -> anyhow::Result<Self> {
        let source_slug =
            optional_env("FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG")?;
        let source_slug_prefix = optional_env(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG_PREFIX",
        )?;
        match (source_slug, source_slug_prefix) {
            (Some(slug), None) => Ok(Self::Exact(slug)),
            (None, Some(prefix)) => Ok(Self::Prefix(prefix)),
            (Some(_), Some(_)) => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG and FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG_PREFIX cannot both be set"
            ),
            (None, None) => bail!(
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG or FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_HANDOFF_SOURCE_SLUG_PREFIX is required"
            ),
        }
    }

    fn source_summary(&self) -> serde_json::Value {
        match self {
            Self::Exact(source_slug) => serde_json::json!({
                "source_slug": source_slug
            }),
            Self::Prefix(source_slug_prefix) => serde_json::json!({
                "source_slug_prefix": source_slug_prefix
            }),
        }
    }
}

fn export_handoff(config: &ExportConfig) -> anyhow::Result<ExportReport> {
    let object_paths =
        collect_bronze_object_paths(&config.bronze_local_object_root, &config.source_selector)?;
    if object_paths.is_empty() {
        bail!("no building-register floor Bronze objects found for source selector");
    }

    let title_floor_counts = load_title_floor_counts(config)?;

    let mut output_writer =
        SilverRowWriter::new(&config.output_path, config.chunk_rows, config.output_format)?;
    let mut proposal_writer = config
        .proposal_input_path
        .as_ref()
        .map(|path| JsonlRowWriter::new(path, proposal_chunk_rows(path, config.chunk_rows)))
        .transpose()?;
    let mut row_count = 0usize;
    let mut proposal_required_count = 0u64;
    let mut normalization_proposal_count = 0u64;
    let mut source_snapshot_ids = Vec::<String>::new();
    let mut remaining_rows = config.max_rows;
    for object_path in &object_paths {
        if matches!(remaining_rows, Some(0)) {
            break;
        }
        let bronze_object_key = bronze_object_key(&config.bronze_local_object_root, object_path)?;
        let object_report = export_object(
            config,
            object_path,
            bronze_object_key.as_str(),
            &mut output_writer,
            proposal_writer.as_mut(),
            remaining_rows,
            &title_floor_counts,
        )?;
        row_count += object_report.row_count;
        if let Some(remaining) = remaining_rows.as_mut() {
            *remaining = remaining.saturating_sub(object_report.row_count);
        }
        proposal_required_count += object_report.proposal_required_count;
        normalization_proposal_count += object_report.normalization_proposal_count;
        for source_snapshot_id in object_report.source_snapshot_ids {
            if !source_snapshot_ids.contains(&source_snapshot_id) {
                source_snapshot_ids.push(source_snapshot_id);
            }
        }
    }
    source_snapshot_ids.sort();
    output_writer
        .flush()
        .context("failed to flush building-register floor Silver handoff")?;
    if let Some(writer) = proposal_writer.as_mut() {
        writer
            .flush()
            .context("failed to flush building-register floor proposal input")?;
    }
    if let Some(summary_path) = &config.summary_path {
        let floor_entity_context_pack_input = config.proposal_input_path.as_ref().map(|path| {
            serde_json::json!({
                "path": path.display().to_string(),
                "proposal_count": normalization_proposal_count
            })
        });
        let summary = serde_json::json!({
            "schema_version": "foundation-platform.building_register_floor_silver_handoff_export.v1",
            "generated_at_utc": Utc::now().to_rfc3339(),
            "status": "ready",
            "completion_claim_allowed": false,
            "production_cutover_allowed": false,
            "national_rollout_allowed": false,
            "source": {
                "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
                "selector": config.source_selector.source_summary(),
                "input_object_count": object_paths.len(),
                "source_snapshot_id": config.source_snapshot_id.as_str(),
                "max_rows": config.max_rows,
                "chunk_rows": config.chunk_rows,
                "output_format": config.output_format.wire_name()
            },
            "output": {
                "path": config.output_path.display().to_string(),
                "format": config.output_format.wire_name(),
                "contract": "silver.building_register_floors",
                "row_count": row_count,
                "proposal_required_count": proposal_required_count,
                "floor_entity_context_pack_input": floor_entity_context_pack_input,
                "source_snapshot_ids": source_snapshot_ids
            },
            "evidence_limitations": [
                "local_bronze_to_silver_handoff_only",
                "does_not_write_iceberg_table",
                "does_not_apply_ai_or_human_review",
                "does_not_approve_production_cutover"
            ]
        });
        let payload = serde_json::to_vec_pretty(&summary)
            .context("failed to serialize building-register floor Silver handoff export summary")?;
        write_file(summary_path, &payload)?;
    }

    Ok(ExportReport {
        input_object_count: object_paths.len(),
        row_count,
        proposal_required_count,
        normalization_proposal_count,
    })
}

fn proposal_chunk_rows(path: &Path, handoff_chunk_rows: Option<usize>) -> Option<usize> {
    if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("jsonl"))
    {
        None
    } else {
        handoff_chunk_rows
    }
}

enum SilverRowWriter {
    Jsonl(JsonlRowWriter),
    Parquet(Box<ParquetSilverRowWriter>),
}

impl SilverRowWriter {
    fn new(
        path: &Path,
        chunk_rows: Option<usize>,
        output_format: OutputFormat,
    ) -> anyhow::Result<Self> {
        match output_format {
            OutputFormat::Jsonl => Ok(Self::Jsonl(JsonlRowWriter::new(path, chunk_rows)?)),
            OutputFormat::Parquet => Ok(Self::Parquet(Box::new(ParquetSilverRowWriter::new(
                path.to_path_buf(),
                chunk_rows,
            )?))),
        }
    }

    fn write_rows(&mut self, rows: &[BuildingRegisterFloorSilverRow]) -> anyhow::Result<()> {
        match self {
            Self::Jsonl(writer) => {
                let handoff = build_building_register_floor_silver_handoff(rows)?;
                writer.write_jsonl(handoff.jsonl.as_str())
            }
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

    fn write_jsonl(&mut self, jsonl: &str) -> anyhow::Result<()> {
        for line in jsonl.lines() {
            if line.trim().is_empty() {
                continue;
            }
            self.write_line(line)?;
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectExportReport {
    row_count: usize,
    proposal_required_count: u64,
    normalization_proposal_count: u64,
    source_snapshot_ids: Vec<String>,
}

fn export_object(
    config: &ExportConfig,
    object_path: &Path,
    bronze_object_key: &str,
    output_writer: &mut SilverRowWriter,
    proposal_writer: Option<&mut JsonlRowWriter>,
    remaining_rows: Option<usize>,
    title_floor_counts: &HashMap<String, BuildingFloorCounts>,
) -> anyhow::Result<ObjectExportReport> {
    match object_path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("json") => export_public_data_json_object(
            config,
            object_path,
            bronze_object_key,
            output_writer,
            proposal_writer,
            remaining_rows,
        ),
        Some("zip") => export_hub_bulk_zip_object(
            config,
            object_path,
            bronze_object_key,
            output_writer,
            proposal_writer,
            remaining_rows,
            title_floor_counts,
        ),
        extension => bail!(
            "unsupported building-register floor Bronze object extension {:?}: {}",
            extension,
            object_path.display()
        ),
    }
}

fn export_public_data_json_object(
    config: &ExportConfig,
    object_path: &Path,
    bronze_object_key: &str,
    output_writer: &mut SilverRowWriter,
    proposal_writer: Option<&mut JsonlRowWriter>,
    remaining_rows: Option<usize>,
) -> anyhow::Result<ObjectExportReport> {
    let raw_payload = fs::read(object_path)
        .with_context(|| format!("failed to read Bronze object {}", object_path.display()))?;
    let rows = normalize_building_register_floor_silver_rows_from_public_data_bronze_json(
        &PublicDataBuildingRegisterFloorBronzeJsonInput {
            raw_payload: &raw_payload,
            source_snapshot_id: config.source_snapshot_id.as_str(),
            bronze_object_key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: Utc::now(),
        },
    )
    .with_context(|| {
        format!(
            "failed to build building-register floor Silver rows for {}",
            object_path.display()
        )
    })?;
    output_writer.write_rows(&rows).with_context(|| {
        format!(
            "failed to write Silver handoff for {}",
            object_path.display()
        )
    })?;
    let proposal_input = build_building_register_floor_normalization_proposal_input(&rows)?;
    if let Some(writer) = proposal_writer {
        writer
            .write_jsonl(proposal_input.jsonl.as_str())
            .with_context(|| {
                format!(
                    "failed to write proposal input for {}",
                    object_path.display()
                )
            })?;
    }
    let row_count = rows.len();
    let proposal_required_count = count_proposal_required(&rows);
    if let Some(limit) = remaining_rows {
        if row_count > limit {
            bail!(
                "building-register floor JSON Bronze object {} has {row_count} rows, exceeding remaining max_rows {limit}; cap the collection page size instead of truncating JSON handoff output",
                object_path.display()
            );
        }
    }
    Ok(ObjectExportReport {
        row_count,
        proposal_required_count,
        normalization_proposal_count: proposal_input.proposal_count,
        source_snapshot_ids: vec![config.source_snapshot_id.clone()],
    })
}

fn export_hub_bulk_zip_object(
    config: &ExportConfig,
    object_path: &Path,
    bronze_object_key: &str,
    output_writer: &mut SilverRowWriter,
    proposal_writer: Option<&mut JsonlRowWriter>,
    remaining_rows: Option<usize>,
    title_floor_counts: &HashMap<String, BuildingFloorCounts>,
) -> anyhow::Result<ObjectExportReport> {
    let ingested_at_utc = Utc::now();
    let mut row_count = 0usize;
    let mut proposal_required_count = 0u64;
    // Full resolved rows of every building that still holds a proposal, kept so the
    // proposal context packs reflect the building-resolved state without re-reading
    // the Bronze object.
    let mut proposal_context_rows = Vec::<BuildingRegisterFloorSilverRow>::new();
    // HUB floor rows arrive grouped by building (동); buffer the current building
    // so building-level contradiction resolution runs on the full group before
    // any of its rows are written.
    let mut buffer: Vec<BuildingRegisterFloorSourceRow> = Vec::new();
    let mut current_pk: Option<String> = None;
    hub_bulk_decoder::HubBuildingRegisterFloorBulkDecoder::decode_zip_rows(
        object_path,
        bronze_object_key,
        remaining_rows,
        |source_row| {
            if current_pk.as_deref() != Some(source_row.mgm_bldrgst_pk.as_str()) {
                flush_hub_building(
                    &mut buffer,
                    config,
                    bronze_object_key,
                    ingested_at_utc,
                    title_floor_counts,
                    output_writer,
                    &mut row_count,
                    &mut proposal_required_count,
                    &mut proposal_context_rows,
                )?;
                current_pk = Some(source_row.mgm_bldrgst_pk.clone());
            }
            buffer.push(source_row);
            Ok(())
        },
    )?;
    flush_hub_building(
        &mut buffer,
        config,
        bronze_object_key,
        ingested_at_utc,
        title_floor_counts,
        output_writer,
        &mut row_count,
        &mut proposal_required_count,
        &mut proposal_context_rows,
    )?;
    let normalization_proposal_count = write_hub_bulk_proposal_context_packs(
        &proposal_context_rows,
        proposal_writer,
        object_path,
    )?;

    Ok(ObjectExportReport {
        row_count,
        proposal_required_count,
        normalization_proposal_count,
        source_snapshot_ids: vec![config.source_snapshot_id.clone()],
    })
}

/// Normalizes and writes one buffered building (동) of HUB floor rows, running
/// building-level contradiction resolution (with the 표제부 witness) across the
/// group, then clears the buffer. Buildings that still hold a proposal keep their
/// full resolved rows for the proposal context packs.
#[allow(clippy::too_many_arguments)]
fn flush_hub_building(
    buffer: &mut Vec<BuildingRegisterFloorSourceRow>,
    config: &ExportConfig,
    bronze_object_key: &str,
    ingested_at_utc: DateTime<Utc>,
    title_floor_counts: &HashMap<String, BuildingFloorCounts>,
    output_writer: &mut SilverRowWriter,
    row_count: &mut usize,
    proposal_required_count: &mut u64,
    proposal_context_rows: &mut Vec<BuildingRegisterFloorSilverRow>,
) -> anyhow::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let rows = normalize_building_register_floor_silver_rows_with_title_counts(
        &BuildingRegisterFloorSilverRowsInput {
            records: buffer,
            source_snapshot_id: config.source_snapshot_id.as_str(),
            bronze_object_key,
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc,
        },
        title_floor_counts,
    )
    .context("failed to build HUB Silver rows for a building")?;
    output_writer
        .write_rows(&rows)
        .context("failed to write HUB Silver handoff for a building")?;
    *row_count += rows.len();
    let building_proposal_count = count_proposal_required(&rows);
    *proposal_required_count += building_proposal_count;
    if building_proposal_count > 0 {
        proposal_context_rows.extend(rows.iter().cloned());
    }
    buffer.clear();
    Ok(())
}

fn write_hub_bulk_proposal_context_packs(
    proposal_context_rows: &[BuildingRegisterFloorSilverRow],
    proposal_writer: Option<&mut JsonlRowWriter>,
    object_path: &Path,
) -> anyhow::Result<u64> {
    if proposal_context_rows.is_empty() {
        return Ok(0);
    }

    let proposal_input =
        build_building_register_floor_normalization_proposal_input(proposal_context_rows)?;
    if let Some(writer) = proposal_writer {
        writer
            .write_jsonl(proposal_input.jsonl.as_str())
            .with_context(|| {
                format!(
                    "failed to write HUB proposal input for {}",
                    object_path.display()
                )
            })?;
    }
    Ok(proposal_input.proposal_count)
}

fn count_proposal_required(rows: &[BuildingRegisterFloorSilverRow]) -> u64 {
    rows.iter()
        .filter(|row| row.normalization_status == "proposal_required")
        .count() as u64
}

fn load_title_floor_counts(
    config: &ExportConfig,
) -> anyhow::Result<HashMap<String, BuildingFloorCounts>> {
    let Some(slug) = config.title_source_slug.as_deref() else {
        return Ok(HashMap::new());
    };
    let source_root = config
        .bronze_local_object_root
        .join("bronze")
        .join(format!("source={slug}"));
    if !source_root.is_dir() {
        tracing::warn!(
            source_root = %source_root.display(),
            "표제부 floor-count witness source not found; resolving floors without the building-title witness"
        );
        return Ok(HashMap::new());
    }
    let mut object_paths = Vec::new();
    collect_json_files(&source_root, &mut object_paths)?;
    object_paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    });
    let counts = load_building_title_floor_counts(&object_paths)?;
    tracing::info!(
        buildings = counts.len(),
        source_slug = slug,
        "loaded 표제부 building-title floor-count witness"
    );
    Ok(counts)
}

fn collect_bronze_object_paths(
    root: &Path,
    selector: &SourceSelector,
) -> anyhow::Result<Vec<PathBuf>> {
    let bronze_root = root.join("bronze");
    if !bronze_root.is_dir() {
        bail!("Bronze root directory not found: {}", bronze_root.display());
    }
    let mut paths = Vec::new();
    for source_root in collect_source_roots(&bronze_root, selector)? {
        collect_json_files(&source_root, &mut paths)?;
    }
    paths.sort();
    Ok(paths)
}

fn collect_source_roots(
    bronze_root: &Path,
    selector: &SourceSelector,
) -> anyhow::Result<Vec<PathBuf>> {
    match selector {
        SourceSelector::Exact(source_slug) => {
            let source_root = bronze_root.join(format!("source={source_slug}"));
            if !source_root.is_dir() {
                bail!(
                    "Bronze source directory not found: {}",
                    source_root.display()
                );
            }
            Ok(vec![source_root])
        }
        SourceSelector::Prefix(source_slug_prefix) => {
            let expected_prefix = format!("source={source_slug_prefix}");
            let mut roots = Vec::new();
            for entry in fs::read_dir(bronze_root)
                .with_context(|| format!("failed to read Bronze root {}", bronze_root.display()))?
            {
                let entry = entry.with_context(|| {
                    format!("failed to read entry in {}", bronze_root.display())
                })?;
                let path = entry.path();
                if path.is_dir()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(&expected_prefix))
                {
                    roots.push(path);
                }
            }
            roots.sort();
            Ok(roots)
        }
    }
}

fn collect_json_files(dir: &Path, paths: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_json_files(&path, paths)?;
        } else if path.extension().is_some_and(|extension| {
            extension.eq_ignore_ascii_case("json") || extension.eq_ignore_ascii_case("zip")
        }) {
            paths.push(path);
        }
    }
    Ok(())
}

fn create_file_writer(path: &Path) -> anyhow::Result<BufWriter<File>> {
    let parent = path
        .parent()
        .context("output path must have a parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create output directory {}", parent.display()))?;
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

fn bronze_object_key(root: &Path, object_path: &Path) -> anyhow::Result<String> {
    let relative = object_path.strip_prefix(root).with_context(|| {
        format!(
            "Bronze object {} must be under local Bronze root {}",
            object_path.display(),
            root.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn write_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("output path must have a parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
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

fn required_path_env(name: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from(required_env(name)?))
}

fn optional_path_env(name: &str) -> anyhow::Result<Option<PathBuf>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(PathBuf::from(value.trim()))),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn optional_usize_env(name: &str) -> anyhow::Result<Option<usize>> {
    let Some(raw) = optional_env(name)? else {
        return Ok(None);
    };
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(Some(value))
}

fn parse_utc_env(name: &str) -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(name)?;
    Ok(DateTime::parse_from_rfc3339(raw.trim())
        .with_context(|| format!("{name} must be an RFC3339 UTC timestamp"))?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::hub_bulk_decoder::HubBuildingRegisterFloorBulkDecoder;
    use super::{export_handoff, ExportConfig, OutputFormat, SourceSelector};
    use chrono::{DateTime, Utc};
    use std::{
        fs,
        io::{Cursor, Write as _},
        path::PathBuf,
    };
    use uuid::Uuid;
    use zip::{write::SimpleFileOptions, ZipWriter};

    #[test]
    fn exports_building_register_floor_bronze_json_to_silver_handoff() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-silver-export");
        let object_key = "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json";
        let bronze_path = root.join(object_key);
        write_file(&bronze_path, sample_payload().as_bytes())?;
        let output_path = root
            .join("silver-handoff")
            .join("building_register_floors.jsonl");
        let proposal_input_path = root
            .join("ai-proposal-input")
            .join("building_register_floor_proposals.jsonl");
        let summary_path = root
            .join("audit")
            .join("building_register_floors-summary.json");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "datagokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_path.clone(),
            proposal_input_path: Some(proposal_input_path.clone()),
            summary_path: Some(summary_path.clone()),
            source_snapshot_id: "datagokr-building-register-floor-overview-11680-10300-000001"
                .to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: None,
        })?;

        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 2);
        assert_eq!(report.proposal_required_count, 1);
        let handoff = fs::read_to_string(&output_path)?;
        assert_eq!(handoff.lines().count(), 2);
        assert!(handoff.contains("\"floor_display_ko\":\"지하 1층\""));
        assert!(handoff.contains("\"normalization_status\":\"proposal_required\""));
        assert!(handoff.contains(&format!("\"bronze_object_key\":\"{object_key}\"")));
        let proposal_input = fs::read_to_string(&proposal_input_path)?;
        assert_eq!(proposal_input.lines().count(), 1);
        assert!(proposal_input
            .contains("\"schema_version\":\"foundation-platform.floor_entity_context_pack.v1\""));
        assert!(proposal_input.contains("\"target_kind\":\"building_register_floor\""));
        assert!(proposal_input.contains("\"raw_record_id\":\""));
        assert!(proposal_input.contains("\"semantic_contract\""));
        assert!(proposal_input
            .contains("\"source_slug\":\"datagokr__building_register_floor_overview\""));
        assert!(proposal_input.contains("\"field_path\":\"flrNo\""));
        assert!(proposal_input.contains("\"concept_id\":\"floor_number\""));
        assert!(proposal_input.contains("\"entity_impact\""));
        assert!(proposal_input.contains("\"entity_type\":\"building\""));

        let summary = fs::read_to_string(&summary_path)?;
        assert!(summary.contains(
            "\"schema_version\": \"foundation-platform.building_register_floor_silver_handoff_export.v1\""
        ));
        assert!(summary.contains("\"row_count\": 2"));
        assert!(summary.contains("\"proposal_required_count\": 1"));
        assert!(summary.contains("\"floor_entity_context_pack_input\""));
        assert!(summary.contains("\"proposal_count\": 1"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_prefix_matched_building_register_floor_sources() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-silver-export-prefix");
        for (bjdong, floor_label) in [("10300", "지1층"), ("10400", "지하1층")] {
            let object_key = format!(
                "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong={bjdong}/page-000001.json"
            );
            write_file(
                &root.join(object_key),
                sample_payload_with_label(floor_label).as_bytes(),
            )?;
        }
        let output_path = root
            .join("silver-handoff")
            .join("building_register_floors-prefix.jsonl");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Prefix(
                "datagokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_path.clone(),
            proposal_input_path: None,
            summary_path: None,
            source_snapshot_id: "datagokr-building-register-floor-overview-prefix-smoke".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: None,
        })?;

        assert_eq!(report.input_object_count, 2);
        assert_eq!(report.row_count, 4);
        let handoff = fs::read_to_string(&output_path)?;
        assert_eq!(handoff.lines().count(), 4);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_hub_bulk_zip_to_silver_handoff_without_json_pages() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-hub-zip-export");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        write_zip_file(
            &root.join(object_key),
            "mart_djy_04.txt",
            hub_bulk_payload().as_bytes(),
        )?;
        let output_path = root
            .join("silver-handoff")
            .join("building_register_floors-hub.jsonl");
        let proposal_input_path = root
            .join("ai-proposal-input")
            .join("building_register_floor_proposals-hub.jsonl");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "hubgokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_path.clone(),
            proposal_input_path: Some(proposal_input_path.clone()),
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: None,
        })?;

        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 2);
        assert_eq!(report.proposal_required_count, 1);
        let handoff = fs::read_to_string(&output_path)?;
        assert_eq!(handoff.lines().count(), 2);
        assert!(handoff.contains("\"floor_display_ko\":\"지하 1층\""));
        assert!(handoff.contains("\"normalization_status\":\"proposal_required\""));
        let proposal_input = fs::read_to_string(&proposal_input_path)?;
        assert_eq!(proposal_input.lines().count(), 1);
        assert!(proposal_input.contains("\"target_kind\":\"building_register_floor\""));
        assert!(proposal_input.contains("\"raw_record_id\":\""));
        assert!(proposal_input.contains("\"semantic_contract\""));
        assert!(proposal_input
            .contains("\"source_slug\":\"hubgokr__building_register_floor_overview\""));
        assert!(proposal_input.contains("\"field_path\":\"floor_number_raw\""));
        assert!(proposal_input.contains("\"concept_id\":\"floor_number\""));
        assert!(proposal_input.contains("\"entity_impact\""));
        assert!(proposal_input.contains("\"entity_type\":\"building\""));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn hub_bulk_proposal_input_keeps_same_building_floor_context() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-hub-context-pack");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        write_zip_file(
            &root.join(object_key),
            "mart_djy_04.txt",
            hub_bulk_payload_same_building().as_bytes(),
        )?;
        let output_path = root
            .join("silver-handoff")
            .join("building_register_floors-hub.jsonl");
        let proposal_input_path = root
            .join("ai-proposal-input")
            .join("building_register_floor_proposals-hub.jsonl");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "hubgokr__building_register_floor_overview".to_owned(),
            ),
            output_path,
            proposal_input_path: Some(proposal_input_path.clone()),
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: None,
        })?;

        assert_eq!(report.row_count, 2);
        assert_eq!(report.normalization_proposal_count, 1);
        let proposal_input = fs::read_to_string(&proposal_input_path)?;
        let context_pack: serde_json::Value = serde_json::from_str(
            proposal_input
                .lines()
                .next()
                .ok_or_else(|| anyhow::anyhow!("expected one context pack"))?,
        )?;
        let same_building_sequence = context_pack["same_building_floor_sequence"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("same_building_floor_sequence must be array"))?;
        assert_eq!(same_building_sequence.len(), 2);
        assert!(same_building_sequence
            .iter()
            .all(|row| row["mgm_bldrgst_pk"] == "SYNTHETIC-FLOOR-0001"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_hub_bulk_zip_respects_max_rows_for_smoke() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-hub-zip-export-limit");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        write_zip_file(
            &root.join(object_key),
            "mart_djy_04.txt",
            hub_bulk_payload().as_bytes(),
        )?;
        let output_path = root
            .join("silver-handoff")
            .join("building_register_floors-hub-limited.jsonl");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "hubgokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_path.clone(),
            proposal_input_path: None,
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: Some(1),
            chunk_rows: None,
            output_format: OutputFormat::Jsonl,
            title_source_slug: None,
        })?;

        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 1);
        let handoff = fs::read_to_string(&output_path)?;
        assert_eq!(handoff.lines().count(), 1);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_hub_bulk_zip_to_chunked_handoff_parts() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-hub-zip-export-chunked");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        write_zip_file(
            &root.join(object_key),
            "mart_djy_04.txt",
            hub_bulk_payload().as_bytes(),
        )?;
        let output_dir = root.join("silver-handoff").join("building_register_floors");
        let proposal_dir = root
            .join("ai-proposal-input")
            .join("building_register_floor_proposals");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "hubgokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_dir.clone(),
            proposal_input_path: Some(proposal_dir.clone()),
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: Some(1),
            output_format: OutputFormat::Jsonl,
            title_source_slug: None,
        })?;

        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 2);
        assert_eq!(
            fs::read_to_string(output_dir.join("part-000001.jsonl"))?
                .lines()
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(output_dir.join("part-000002.jsonl"))?
                .lines()
                .count(),
            1
        );
        assert!(!output_dir.join("part-000003.jsonl").exists());
        assert_eq!(fs::read_dir(&proposal_dir)?.count(), 1);

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn exports_hub_bulk_zip_to_chunked_parquet_parts_without_jsonl_bloat() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-hub-zip-parquet-export");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        write_zip_file(
            &root.join(object_key),
            "mart_djy_04.txt",
            hub_bulk_payload().as_bytes(),
        )?;
        let output_dir = root
            .join("silver-handoff")
            .join("building_register_floors_parquet");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "hubgokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_dir.clone(),
            proposal_input_path: None,
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: Some(1),
            output_format: OutputFormat::Parquet,
            title_source_slug: None,
        })?;

        assert_eq!(report.input_object_count, 1);
        assert_eq!(report.row_count, 2);
        assert!(output_dir.join("part-000001.parquet").is_file());
        assert!(output_dir.join("part-000002.parquet").is_file());
        assert!(!output_dir.join("part-000001.jsonl").exists());

        let first_batch = read_first_parquet_batch(&output_dir.join("part-000001.parquet"))?;
        assert_eq!(first_batch.num_rows(), 1);
        assert_eq!(first_batch.schema().field(0).name(), "floor_row_id");
        assert_eq!(string_value(&first_batch, "floor_kind", 0)?, "basement");
        assert_eq!(
            string_value(&first_batch, "normalization_status", 0)?,
            "accepted"
        );

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn chunked_parquet_handoff_keeps_proposal_input_as_single_jsonl_file() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-parquet-proposal-export");
        let object_key = "bronze/source=datagokr__building_register_floor_overview/sigungu=11680/bjdong=10300/page-000001.json";
        write_file(&root.join(object_key), sample_payload().as_bytes())?;
        let output_dir = root
            .join("silver-handoff")
            .join("building_register_floors_parquet");
        let proposal_input_path = root
            .join("ai-proposal-input")
            .join("building_register_floor_proposals.jsonl");

        let report = export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "datagokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_dir.clone(),
            proposal_input_path: Some(proposal_input_path.clone()),
            summary_path: None,
            source_snapshot_id: "datagokr-building-register-floor-overview-11680-10300-000001"
                .to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: Some(1),
            output_format: OutputFormat::Parquet,
            title_source_slug: None,
        })?;

        assert_eq!(report.row_count, 2);
        assert!(output_dir.join("part-000001.parquet").is_file());
        assert!(output_dir.join("part-000002.parquet").is_file());
        assert!(proposal_input_path.is_file());
        let proposal_input = fs::read_to_string(&proposal_input_path)?;
        assert_eq!(proposal_input.lines().count(), 1);
        assert!(!proposal_input_path.join("part-000001.jsonl").exists());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn chunked_parquet_export_removes_stale_part_files() -> anyhow::Result<()> {
        let root =
            temp_root("foundation-platform-building-register-floor-hub-zip-parquet-stale-export");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        write_zip_file(
            &root.join(object_key),
            "mart_djy_04.txt",
            hub_bulk_payload().as_bytes(),
        )?;
        let output_dir = root
            .join("silver-handoff")
            .join("building_register_floors_parquet");
        fs::create_dir_all(&output_dir)?;
        fs::write(output_dir.join("part-000003.parquet"), b"stale parquet")?;

        export_handoff(&ExportConfig {
            bronze_local_object_root: root.clone(),
            source_selector: SourceSelector::Exact(
                "hubgokr__building_register_floor_overview".to_owned(),
            ),
            output_path: output_dir.clone(),
            proposal_input_path: None,
            summary_path: None,
            source_snapshot_id: "hubgokr-building-register-floor-overview-20260620".to_owned(),
            valid_from_utc: parse_utc("2026-06-20T00:00:00Z")?,
            max_rows: None,
            chunk_rows: Some(1),
            output_format: OutputFormat::Parquet,
            title_source_slug: None,
        })?;

        assert!(output_dir.join("part-000001.parquet").is_file());
        assert!(output_dir.join("part-000002.parquet").is_file());
        assert!(!output_dir.join("part-000003.parquet").exists());

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn hub_bulk_decoder_reads_zip_rows_with_lineage_and_limit() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-building-register-floor-hub-decoder-limit");
        let object_key =
            "bronze/source=hubgokr__building_register_floor_overview/OPN209912310000000013.zip";
        let object_path = root.join(object_key);
        write_zip_file(
            &object_path,
            "mart_djy_04.txt",
            hub_bulk_payload().as_bytes(),
        )?;

        let mut rows = Vec::new();
        let decoded_count = HubBuildingRegisterFloorBulkDecoder::decode_zip_rows(
            &object_path,
            object_key,
            Some(1),
            |row| {
                rows.push(row);
                Ok(())
            },
        )?;

        assert_eq!(decoded_count, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].source_record_id,
            format!("{object_key}#line-000001")
        );
        assert_eq!(rows[0].source_line_number, Some(1));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn temp_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
    }

    fn write_file(path: &PathBuf, content: &[u8]) -> anyhow::Result<()> {
        let parent = path.parent().ok_or_else(|| anyhow::anyhow!("parent"))?;
        fs::create_dir_all(parent)?;
        fs::write(path, content)?;
        Ok(())
    }

    fn write_zip_file(path: &PathBuf, entry_name: &str, content: &[u8]) -> anyhow::Result<()> {
        let parent = path.parent().ok_or_else(|| anyhow::anyhow!("parent"))?;
        fs::create_dir_all(parent)?;
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = ZipWriter::new(&mut buffer);
            writer.start_file(entry_name, SimpleFileOptions::default())?;
            writer.write_all(content)?;
            writer.finish()?;
        }
        fs::write(path, buffer.into_inner())?;
        Ok(())
    }

    fn read_first_parquet_batch(path: &PathBuf) -> anyhow::Result<arrow_array::RecordBatch> {
        let file = fs::File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        reader
            .next()
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("expected at least one parquet batch"))
    }

    fn string_value(
        batch: &arrow_array::RecordBatch,
        column_name: &str,
        row_index: usize,
    ) -> anyhow::Result<String> {
        let column_index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == column_name)
            .ok_or_else(|| anyhow::anyhow!("missing column {column_name}"))?;
        let array = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("column {column_name} is not StringArray"))?;
        Ok(array.value(row_index).to_owned())
    }

    fn hub_bulk_payload() -> String {
        [
            "SYNTHETIC-FLOOR-0001|SYNTHETIC LOT ADDRESS 0001|SYNTHETIC ROAD ADDRESS 0001||00000|00000|0|0001|0000||||SYNTHETIC-ROAD-CODE-0001|00001|0|1|0||10|지하|1|지하층|11|벽돌구조|합성구조|01001|단독주택|주택|1.00|0|주건축물||20991231",
            "SYNTHETIC-FLOOR-0002|SYNTHETIC LOT ADDRESS 0002|SYNTHETIC ROAD ADDRESS 0002||00000|00000|0|0002|0000||||SYNTHETIC-ROAD-CODE-0002|00002|0|2|0||10|지하|1|1층|11|벽돌구조|합성구조|01001|단독주택|주택|2.00|0|주건축물||20991231",
        ]
        .join("\n")
    }

    fn hub_bulk_payload_same_building() -> String {
        let mut lines = hub_bulk_payload()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        lines[1] = lines[1].replacen("SYNTHETIC-FLOOR-0002|", "SYNTHETIC-FLOOR-0001|", 1);
        lines.join("\n")
    }

    fn sample_payload() -> String {
        sample_payload_with_label("지1층")
    }

    fn sample_payload_with_label(floor_label: &str) -> String {
        serde_json::json!({
            "response": {
                "body": {
                    "items": {
                        "item": [
                            {
                                "mgmBldrgstPk": "11680-10300-1",
                                "flrGbCd": "10",
                                "flrGbCdNm": "지하",
                                "flrNo": "1",
                                "flrNoNm": floor_label
                            },
                            {
                                "mgmBldrgstPk": "11680-10300-2",
                                "flrGbCd": "10",
                                "flrGbCdNm": "지하",
                                "flrNo": "1",
                                "flrNoNm": "1층"
                            }
                        ]
                    }
                }
            }
        })
        .to_string()
    }
}
