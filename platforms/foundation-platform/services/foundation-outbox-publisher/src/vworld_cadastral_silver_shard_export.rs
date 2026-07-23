//! Manifest-sharded `VWorld` cadastral Bronze-to-Silver handoff export command.

use std::{
    env, fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use collection_domain::VWorldCadastralFeatureDedupeAccumulator;
use foundation_outbox::{
    object_storage::{ObjectWriteMode, PutObjectRequest},
    FileObjectStorage, ObjectStorageService, R2ObjectStorage,
};
use lakehouse_application::{
    build_vworld_cadastral_silver_parcel_boundary_handoff,
    normalize_vworld_cadastral_silver_parcel_boundary_rows,
    VWorldCadastralSilverParcelBoundaryRowsInput,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

const MANIFEST_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.national_bronze_object_manifest_entry.v1";
const TARGET_PROVIDER: &str = "VWorld";
const TARGET_ENDPOINT: &str = "ingest-vworld-cadastral";
const SILVER_HANDOFF_CONTENT_TYPE: &str = "application/x-ndjson; charset=utf-8";
const SILVER_HANDOFF_CACHE_CONTROL: &str = "no-store";

/// Runs the manifest-sharded Bronze-to-Silver handoff export.
pub async fn run() -> anyhow::Result<()> {
    let config = ShardExportConfig::from_env()?;
    let report = export_handoff_shard(&config).await?;
    tracing::info!(
        selected_object_count = report.selected_object_count,
        row_count = report.row_count,
        output_storage_driver = config.output.storage_driver(),
        output_location = %config.output.location(),
        "VWorld cadastral Silver handoff shard export succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ShardExportConfig {
    manifest_path: PathBuf,
    storage: BronzeReadStorageConfig,
    output: SilverHandoffOutputConfig,
    summary_path: Option<PathBuf>,
    source_record_id: String,
    source_snapshot_id: String,
    valid_from_utc: DateTime<Utc>,
    filtered_manifest_start_index: u64,
    filtered_manifest_end_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BronzeReadStorageConfig {
    Local { root: PathBuf },
    R2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SilverHandoffOutputConfig {
    LocalFile { path: PathBuf },
    R2Object { key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestEntry {
    line_number: u64,
    filtered_index: u64,
    object_key: String,
    storage_driver: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExportReport {
    selected_object_count: usize,
    input_bytes: u64,
    row_count: usize,
    first_object_key: String,
    last_object_key: String,
}

#[derive(Clone, Debug, Deserialize)]
struct RawManifestEntry {
    schema_version: String,
    provider: String,
    endpoint: String,
    storage_driver: String,
    object_key: String,
}

impl ShardExportConfig {
    fn from_env() -> anyhow::Result<Self> {
        let storage = BronzeReadStorageConfig::from_env()?;
        let filtered_manifest_start_index = parse_positive_u64_env(
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_MANIFEST_START_INDEX",
        )?;
        let filtered_manifest_end_index = parse_positive_u64_env(
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_MANIFEST_END_INDEX",
        )?;
        if filtered_manifest_end_index < filtered_manifest_start_index {
            bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_MANIFEST_END_INDEX must be greater than or equal to START_INDEX"
            );
        }

        Ok(Self {
            manifest_path: required_path_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_BRONZE_MANIFEST_PATH",
            )?,
            storage,
            output: SilverHandoffOutputConfig::from_env()?,
            summary_path: optional_path_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SUMMARY_PATH",
            )?,
            source_record_id: required_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_RECORD_ID",
            )?,
            source_snapshot_id: required_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_SOURCE_SNAPSHOT_ID",
            )?,
            valid_from_utc: parse_utc_env(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_VALID_FROM_UTC",
            )?,
            filtered_manifest_start_index,
            filtered_manifest_end_index,
        })
    }
}

impl BronzeReadStorageConfig {
    fn from_env() -> anyhow::Result<Self> {
        let driver =
            optional_env("FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_STORAGE_DRIVER")?
                .unwrap_or_else(|| "r2".to_owned())
                .to_ascii_lowercase();
        match driver.as_str() {
            "local" => Ok(Self::Local {
                root: required_path_env(
                    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_BRONZE_ROOT",
                )?,
            }),
            "r2" => Ok(Self::R2),
            "" => bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_STORAGE_DRIVER must not be empty"
            ),
            other => bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
            ),
        }
    }

    const fn driver_name(&self) -> &'static str {
        match self {
            Self::Local { .. } => "local",
            Self::R2 => "r2",
        }
    }
}

impl SilverHandoffOutputConfig {
    fn from_env() -> anyhow::Result<Self> {
        let driver = optional_env(
            "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_STORAGE_DRIVER",
        )?
        .unwrap_or_else(|| "local".to_owned())
        .to_ascii_lowercase();
        match driver.as_str() {
            "local" => Ok(Self::LocalFile {
                path: required_path_env(
                    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_PATH",
                )?,
            }),
            "r2" => {
                let key = required_env(
                    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_OBJECT_KEY",
                )?;
                validate_provider_object_key(key.as_str())?;
                Ok(Self::R2Object { key })
            }
            "" => bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_STORAGE_DRIVER must not be empty"
            ),
            other => bail!(
                "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SILVER_HANDOFF_OUTPUT_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
            ),
        }
    }

    const fn storage_driver(&self) -> &'static str {
        match self {
            Self::LocalFile { .. } => "local",
            Self::R2Object { .. } => "r2",
        }
    }

    fn location(&self) -> String {
        match self {
            Self::LocalFile { path } => path.display().to_string(),
            Self::R2Object { key } => key.clone(),
        }
    }

    async fn write(&self, bytes: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::LocalFile { path } => write_file(path, bytes),
            Self::R2Object { key } => {
                let storage = R2ObjectStorage::from_env()
                    .context("failed to configure R2 Silver handoff output storage")?;
                storage
                    .put_object(PutObjectRequest {
                        key: key.clone(),
                        body: bytes.to_vec(),
                        content_type: SILVER_HANDOFF_CONTENT_TYPE.to_owned(),
                        cache_control: SILVER_HANDOFF_CACHE_CONTROL.to_owned(),
                        // Silver handoff output is mutable. Stays OverwriteAllowed.
                        write_mode: ObjectWriteMode::OverwriteAllowed,
                        sha256: None,
                    })
                    .await
                    .context("failed to write R2 Silver handoff output object")
            }
        }
    }
}

enum BronzeObjectReader {
    Local(FileObjectStorage),
    R2(R2ObjectStorage),
}

impl BronzeObjectReader {
    async fn from_config(config: &BronzeReadStorageConfig) -> anyhow::Result<Self> {
        match config {
            BronzeReadStorageConfig::Local { root } => {
                Ok(Self::Local(FileObjectStorage::new(root)?))
            }
            BronzeReadStorageConfig::R2 => Ok(Self::R2(R2ObjectStorage::from_env()?)),
        }
    }

    async fn read_object_bytes(&self, object_key: &str) -> anyhow::Result<Vec<u8>> {
        validate_provider_object_key(object_key)?;
        match self {
            Self::Local(storage) => storage.get_object_bytes(object_key).map_err(Into::into),
            Self::R2(storage) => storage
                .get_object_bytes(object_key)
                .await
                .map_err(Into::into),
        }
    }
}

async fn export_handoff_shard(config: &ShardExportConfig) -> anyhow::Result<ExportReport> {
    let entries = read_selected_manifest_entries(config)?;
    if entries.is_empty() {
        bail!("no VWorld cadastral Bronze manifest entries selected for shard");
    }

    let reader = BronzeObjectReader::from_config(&config.storage).await?;
    let mut accumulator =
        VWorldCadastralFeatureDedupeAccumulator::new_with_invalid_pnu_quarantine();
    let mut input_bytes = 0_u64;

    for entry in &entries {
        let bytes = reader
            .read_object_bytes(entry.object_key.as_str())
            .await
            .with_context(|| {
                format!(
                    "failed to read Bronze object from manifest line {}",
                    entry.line_number
                )
            })?;
        input_bytes += u64::try_from(bytes.len()).context("Bronze object byte size overflow")?;
        let payload: JsonValue = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "Bronze object is not valid JSON at manifest line {}",
                entry.line_number
            )
        })?;
        accumulator.ingest_payload(&payload).with_context(|| {
            format!(
                "failed to dedupe VWorld cadastral Bronze payload at filtered index {}",
                entry.filtered_index
            )
        })?;
    }

    let dedupe_report = accumulator.finish();
    let rows = normalize_vworld_cadastral_silver_parcel_boundary_rows(
        &VWorldCadastralSilverParcelBoundaryRowsInput {
            records: &dedupe_report.records,
            source_record_id: config.source_record_id.as_str(),
            source_snapshot_id: config.source_snapshot_id.as_str(),
            valid_from_utc: config.valid_from_utc,
            ingested_at_utc: Utc::now(),
        },
    )
    .context("failed to normalize VWorld cadastral Silver parcel-boundary rows")?;
    let mut handoff = build_vworld_cadastral_silver_parcel_boundary_handoff(&rows)
        .context("failed to build VWorld cadastral Silver handoff")?;
    handoff.quality_metrics.insert(
        "invalid_pnu_count".to_owned(),
        dedupe_report.invalid_pnu_feature_count,
    );

    config.output.write(handoff.jsonl.as_bytes()).await?;
    let first_object_key = entries
        .first()
        .map(|entry| entry.object_key.clone())
        .unwrap_or_default();
    let last_object_key = entries
        .last()
        .map(|entry| entry.object_key.clone())
        .unwrap_or_default();

    let report = ExportReport {
        selected_object_count: entries.len(),
        input_bytes,
        row_count: rows.len(),
        first_object_key,
        last_object_key,
    };

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, config, &report, &handoff)?;
    }

    Ok(report)
}

fn read_selected_manifest_entries(
    config: &ShardExportConfig,
) -> anyhow::Result<Vec<ManifestEntry>> {
    let file = fs::File::open(&config.manifest_path).with_context(|| {
        format!(
            "failed to open Bronze object manifest {}",
            config.manifest_path.display()
        )
    })?;
    let reader = BufReader::new(file);
    let mut filtered_index = 0_u64;
    let mut selected = Vec::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line_number = u64::try_from(line_index + 1).context("manifest line number overflow")?;
        let line = line.with_context(|| format!("failed to read manifest line {line_number}"))?;
        let line = if line_number == 1 {
            line.trim_start_matches('\u{feff}').to_owned()
        } else {
            line
        };
        if line.trim().is_empty() {
            bail!("Bronze object manifest line {line_number} must not be blank");
        }
        let row: RawManifestEntry = serde_json::from_str(&line).with_context(|| {
            format!("Bronze object manifest line {line_number} is not valid JSON")
        })?;
        if row.schema_version != MANIFEST_ENTRY_SCHEMA_VERSION {
            bail!("Bronze object manifest line {line_number} schema mismatch");
        }
        if row.provider != TARGET_PROVIDER || row.endpoint != TARGET_ENDPOINT {
            continue;
        }

        filtered_index += 1;
        if filtered_index < config.filtered_manifest_start_index
            || filtered_index > config.filtered_manifest_end_index
        {
            continue;
        }
        validate_provider_object_key(row.object_key.as_str())?;
        if row.storage_driver != config.storage.driver_name() {
            bail!(
                "manifest line {line_number} storage_driver {} does not match configured {} storage",
                row.storage_driver,
                config.storage.driver_name()
            );
        }
        selected.push(ManifestEntry {
            line_number,
            filtered_index,
            object_key: row.object_key,
            storage_driver: row.storage_driver,
        });
    }

    if filtered_index < config.filtered_manifest_end_index {
        bail!(
            "Bronze object manifest only contained {filtered_index} VWorld cadastral entries; requested end index {}",
            config.filtered_manifest_end_index
        );
    }

    let expected = config.filtered_manifest_end_index - config.filtered_manifest_start_index + 1;
    if u64::try_from(selected.len()).context("selected object count overflow")? != expected {
        bail!("selected manifest entries did not match requested shard span");
    }

    Ok(selected)
}

fn write_summary(
    path: &Path,
    config: &ShardExportConfig,
    report: &ExportReport,
    handoff: &lakehouse_application::VWorldCadastralSilverParcelBoundaryHandoff,
) -> anyhow::Result<()> {
    let summary = serde_json::json!({
        "schema_version": "foundation-platform.vworld_cadastral_silver_handoff_shard_export.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "source": {
            "bronze_object_manifest_path": config.manifest_path.display().to_string(),
            "storage_driver": config.storage.driver_name(),
            "filtered_manifest_start_index": config.filtered_manifest_start_index,
            "filtered_manifest_end_index": config.filtered_manifest_end_index,
            "selected_object_count": report.selected_object_count,
            "input_bytes": report.input_bytes,
            "invalid_pnu_feature_count": handoff
                .quality_metrics
                .get("invalid_pnu_count")
                .copied()
                .unwrap_or_default(),
            "first_object_key": report.first_object_key,
            "last_object_key": report.last_object_key,
            "source_record_id": config.source_record_id,
            "source_snapshot_id": config.source_snapshot_id
        },
        "output": {
            "storage_driver": config.output.storage_driver(),
            "location": config.output.location(),
            "path": match &config.output {
                SilverHandoffOutputConfig::LocalFile { path } => path.display().to_string(),
                SilverHandoffOutputConfig::R2Object { .. } => String::new(),
            },
            "object_key": match &config.output {
                SilverHandoffOutputConfig::LocalFile { .. } => String::new(),
                SilverHandoffOutputConfig::R2Object { key } => key.clone(),
            },
            "contract": handoff.contract_table_name,
            "row_count": report.row_count,
            "source_snapshot_ids": handoff.source_snapshot_ids,
            "source_snapshot_truncated": handoff.source_snapshot_truncated
        },
        "quality_metrics": handoff.quality_metrics,
        "evidence_limitations": [
            "manifest_shard_bronze_to_silver_handoff_only",
            "does_not_write_iceberg_table",
            "does_not_rebuild_postgis_anchor_or_pbf",
            "does_not_approve_production_cutover"
        ]
    });
    let payload = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize VWorld cadastral Silver shard export summary")?;
    write_file(path, &payload)
}

fn validate_provider_object_key(key: &str) -> anyhow::Result<()> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        bail!("provider object key must not be empty");
    }
    if trimmed != key {
        bail!("provider object key must not contain surrounding whitespace");
    }
    if trimmed.starts_with('/') || trimmed.contains('\\') || trimmed.contains("..") {
        bail!("provider object key must be a safe relative key");
    }
    if trimmed
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        bail!("provider object key must not contain empty, '.', or '..' segments");
    }
    Ok(())
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
    optional_env(name)?.ok_or_else(|| anyhow::anyhow!("{name} is required"))
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
    optional_env(name).map(|value| value.map(PathBuf::from))
}

fn parse_positive_u64_env(name: &str) -> anyhow::Result<u64> {
    let value = required_env(name)?;
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

fn parse_utc_env(name: &str) -> anyhow::Result<DateTime<Utc>> {
    let raw = required_env(name)?;
    Ok(DateTime::parse_from_rfc3339(raw.trim())
        .with_context(|| format!("{name} must be an RFC3339 UTC timestamp"))?
        .with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::{
        export_handoff_shard, BronzeReadStorageConfig, ShardExportConfig, SilverHandoffOutputConfig,
    };
    use chrono::{DateTime, Utc};
    use std::{fs, path::PathBuf};
    use uuid::Uuid;

    #[tokio::test]
    async fn exports_manifest_shard_from_local_bronze_objects() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-vworld-cadastral-shard-export");
        let manifest_path = root.join("audit").join("manifest.jsonl");
        let source_one = "vworld-cadastral-national-11110-10100";
        let source_two = "vworld-cadastral-national-11110-10200";
        let key_one = bronze_key(source_one, 1);
        let key_two = bronze_key(source_two, 1);
        write_file(
            &root.join(&key_one),
            sample_payload("9999900101100010001").as_bytes(),
        )?;
        write_file(
            &root.join(&key_two),
            sample_payload("9999900201100020001").as_bytes(),
        )?;
        write_file(
            &manifest_path,
            format!(
                "{}\n{}\n{}\n",
                manifest_line(
                    "data.go.kr",
                    "getBrTitleInfo",
                    "local",
                    "bronze/source=molit-building-register-national-11110-10100/part-000001.json"
                ),
                manifest_line("VWorld", "ingest-vworld-cadastral", "local", &key_one),
                manifest_line("VWorld", "ingest-vworld-cadastral", "local", &key_two)
            )
            .as_bytes(),
        )?;
        let output_path = root
            .join("target")
            .join("silver")
            .join("parcel_boundaries.jsonl");
        let summary_path = root.join("target").join("audit").join("summary.json");

        let report = export_handoff_shard(&ShardExportConfig {
            manifest_path,
            storage: BronzeReadStorageConfig::Local { root: root.clone() },
            output: SilverHandoffOutputConfig::LocalFile {
                path: output_path.clone(),
            },
            summary_path: Some(summary_path.clone()),
            source_record_id: "national-promotion:vworld-shard-0001".to_owned(),
            source_snapshot_id: "national-promotion:vworld-shard-0001".to_owned(),
            valid_from_utc: parse_utc("2026-05-24T00:00:00Z")?,
            filtered_manifest_start_index: 1,
            filtered_manifest_end_index: 2,
        })
        .await?;

        assert_eq!(report.selected_object_count, 2);
        assert_eq!(report.row_count, 2);
        let handoff = fs::read_to_string(&output_path)?;
        assert_eq!(handoff.lines().count(), 2);
        assert!(handoff.contains("\"pnu\":\"9999900101100010001\""));
        assert!(handoff.contains("\"pnu\":\"9999900201100020001\""));
        let summary = fs::read_to_string(&summary_path)?;
        assert!(summary.contains(
            "\"schema_version\": \"foundation-platform.vworld_cadastral_silver_handoff_shard_export.v1\""
        ));
        assert!(summary.contains("\"selected_object_count\": 2"));

        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_manifest_storage_driver_mismatch() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-vworld-cadastral-shard-export-mismatch");
        let manifest_path = root.join("audit").join("manifest.jsonl");
        let key = bronze_key("vworld-cadastral-national-11110-10100", 1);
        write_file(
            &root.join(&key),
            sample_payload("9999900101100010001").as_bytes(),
        )?;
        write_file(
            &manifest_path,
            format!(
                "{}\n",
                manifest_line("VWorld", "ingest-vworld-cadastral", "r2", &key)
            )
            .as_bytes(),
        )?;

        let error = export_handoff_shard(&ShardExportConfig {
            manifest_path,
            storage: BronzeReadStorageConfig::Local { root: root.clone() },
            output: SilverHandoffOutputConfig::LocalFile {
                path: root
                    .join("target")
                    .join("silver")
                    .join("parcel_boundaries.jsonl"),
            },
            summary_path: None,
            source_record_id: "national-promotion:vworld-shard-0001".to_owned(),
            source_snapshot_id: "national-promotion:vworld-shard-0001".to_owned(),
            valid_from_utc: parse_utc("2026-05-24T00:00:00Z")?,
            filtered_manifest_start_index: 1,
            filtered_manifest_end_index: 1,
        })
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected storage-driver mismatch"))?;

        assert!(error.to_string().contains("storage_driver r2"));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn accepts_utf8_bom_prefixed_manifest() -> anyhow::Result<()> {
        let root = temp_root("foundation-platform-vworld-cadastral-shard-export-bom");
        let manifest_path = root.join("audit").join("manifest.jsonl");
        let key = bronze_key("vworld-cadastral-national-11110-10100", 1);
        write_file(
            &root.join(&key),
            sample_payload("9999900101100010001").as_bytes(),
        )?;
        write_file(
            &manifest_path,
            format!(
                "\u{feff}{}\n",
                manifest_line("VWorld", "ingest-vworld-cadastral", "local", &key)
            )
            .as_bytes(),
        )?;

        let report = export_handoff_shard(&ShardExportConfig {
            manifest_path,
            storage: BronzeReadStorageConfig::Local { root: root.clone() },
            output: SilverHandoffOutputConfig::LocalFile {
                path: root
                    .join("target")
                    .join("silver")
                    .join("parcel_boundaries.jsonl"),
            },
            summary_path: None,
            source_record_id: "national-promotion:vworld-shard-0001".to_owned(),
            source_snapshot_id: "national-promotion:vworld-shard-0001".to_owned(),
            valid_from_utc: parse_utc("2026-05-24T00:00:00Z")?,
            filtered_manifest_start_index: 1,
            filtered_manifest_end_index: 1,
        })
        .await?;

        assert_eq!(report.selected_object_count, 1);
        assert_eq!(report.row_count, 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn temp_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
    }

    fn write_file(path: &PathBuf, content: &[u8]) -> anyhow::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("path has no parent"))?;
        fs::create_dir_all(parent)?;
        fs::write(path, content)?;
        Ok(())
    }

    fn bronze_key(source_slug: &str, page: u32) -> String {
        format!(
            "bronze/source={source_slug}/run_id=018f0000-0000-7000-8000-000000000001/partition=operation=GetFeature/dataset=LP_PA_CBND_BUBUN/filter_kind=attr/filter_sha256=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/size=001000/page={page:06}/part-{page:06}.json"
        )
    }

    fn manifest_line(
        provider: &str,
        endpoint: &str,
        storage_driver: &str,
        object_key: &str,
    ) -> String {
        serde_json::json!({
            "schema_version": "foundation-platform.national_bronze_object_manifest_entry.v1",
            "provider": provider,
            "endpoint": endpoint,
            "storage_driver": storage_driver,
            "object_key": object_key
        })
        .to_string()
    }

    fn sample_payload(pnu: &str) -> String {
        r#"{
          "response": {
            "result": {
              "featureCollection": {
                "features": [
                  {
                    "type": "Feature",
                    "properties": {
                      "pnu": "__PNU__",
                      "jibun": "1-1",
                      "bonbun": "0001",
                      "bubun": "0001"
                    },
                    "geometry": {
                      "type": "MultiPolygon",
                      "coordinates": [[[[
                        127.123470234300,
                        36.1234400
                      ], [
                        127.123470234310,
                        36.1234400
                      ], [
                        127.123470234310,
                        36.1234410
                      ], [
                        127.123470234300,
                        36.1234410
                      ], [
                        127.123470234300,
                        36.1234400
                      ]]]]
                    }
                  }
                ]
              }
            }
          }
        }"#
        .replace("__PNU__", pnu)
    }

    fn parse_utc(raw: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }
}
