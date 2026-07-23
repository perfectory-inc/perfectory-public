use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope.v1";
const SCOPE_ROW_SCHEMA_VERSION: &str = "foundation-platform.national_data_collection_scope_row.v1";
const REGISTRY_ROW_SCHEMA_VERSION: &str =
    "foundation-platform.administrative_spatial_scope_unit.v1";
const REGISTRY_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.administrative_spatial_scope_registry_evidence.v1";

pub fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    Writer::new(config)?.run()
}

struct Config {
    root: PathBuf,
    registry_path: PathBuf,
    registry_evidence_path: PathBuf,
    output_path: PathBuf,
    csv_output_path: PathBuf,
    evidence_path: PathBuf,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if !env_bool(
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_CONFIRM",
            false,
        )? {
            bail!("ConfirmNationalScopeWrite is required before writing national data collection scope");
        }
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        Ok(Self {
            registry_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_REGISTRY_PATH",
                    "target/audit/administrative-spatial-scope-registry.jsonl",
                )?,
                "RegistryPath",
            )?,
            registry_evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_REGISTRY_EVIDENCE_PATH",
                    "target/audit/administrative-spatial-scope-registry-evidence.json",
                )?,
                "RegistryEvidencePath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_OUTPUT_PATH",
                    "target/audit/national-data-collection-scope.jsonl",
                )?,
                "OutputPath",
            )?,
            csv_output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_CSV_OUTPUT_PATH",
                    "target/audit/national-data-collection-scope.csv",
                )?,
                "CsvOutputPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_EVIDENCE_PATH",
                    "target/audit/national-data-collection-scope-evidence.json",
                )?,
                "EvidencePath",
            )?,
            root,
        })
    }
}

struct Writer {
    config: Config,
}

impl Writer {
    fn new(config: Config) -> anyhow::Result<Self> {
        if !config.registry_path.is_file() {
            bail!(
                "administrative spatial scope registry missing: {}",
                repo_relative_path(&config.root, &config.registry_path)
            );
        }
        if !config.registry_evidence_path.is_file() {
            bail!(
                "administrative spatial scope registry evidence missing: {}",
                repo_relative_path(&config.root, &config.registry_evidence_path)
            );
        }
        for (path, label) in [
            (&config.output_path, "national data collection scope"),
            (
                &config.csv_output_path,
                "national data collection scope CSV projection",
            ),
            (
                &config.evidence_path,
                "national data collection scope evidence",
            ),
        ] {
            if path.is_file() {
                bail!(
                    "{label} already exists: {}",
                    repo_relative_path(&config.root, path)
                );
            }
        }
        Ok(Self { config })
    }

    fn run(&self) -> anyhow::Result<()> {
        let registry_relative_path =
            repo_relative_path(&self.config.root, &self.config.registry_path);
        let registry_sha256 = file_sha256(&self.config.registry_path)?;
        let registry_evidence = read_json(
            &self.config.registry_evidence_path,
            "administrative spatial scope registry evidence",
        )?;
        assert_registry_evidence(
            &registry_evidence,
            &registry_relative_path,
            &registry_sha256,
        )?;

        let registry_rows = read_registry_rows(&self.config.registry_path)?;
        let mut active_legal_dongs = registry_rows
            .iter()
            .filter(|row| {
                json_string(row, "schema_version") == REGISTRY_ROW_SCHEMA_VERSION
                    && json_string(row, "scope_kind") == "legal_dong"
                    && json_string(row, "status") == "active"
            })
            .collect::<Vec<_>>();
        active_legal_dongs.sort_by_key(|row| json_string(row, "canonical_code"));
        if active_legal_dongs.is_empty() {
            bail!("registry must contain at least one active legal_dong");
        }

        let scope_rows = active_legal_dongs
            .iter()
            .map(|row| scope_row_from_registry(row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        write_scope_jsonl(&self.config.output_path, &scope_rows)?;
        write_scope_csv(&self.config.csv_output_path, &scope_rows)?;

        let evidence = ScopeEvidence {
            schema_version: SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&self.config.root),
            status: "ready",
            source_kind: "administrative_spatial_scope_registry",
            registry_path: registry_relative_path,
            registry_sha256,
            registry_evidence_path: repo_relative_path(
                &self.config.root,
                &self.config.registry_evidence_path,
            ),
            output_kind: "jsonl",
            output_path: repo_relative_path(&self.config.root, &self.config.output_path),
            csv_projection_path: repo_relative_path(
                &self.config.root,
                &self.config.csv_output_path,
            ),
            scope_row_schema_version: SCOPE_ROW_SCHEMA_VERSION,
            registry_row_count: u64::try_from(registry_rows.len())
                .context("registry_row_count overflow")?,
            source_row_count: u64::try_from(scope_rows.len())
                .context("source_row_count overflow")?,
            scope_row_count: u64::try_from(scope_rows.len()).context("scope_row_count overflow")?,
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            national_rollout_blocked_reason: "scope_only_no_public_api_execution",
            evidence_limitations: vec![
                "scope_only",
                "does_not_execute_public_api_requests",
                "does_not_prove_complete_national_coverage",
                "does_not_approve_production_cutover",
            ],
            next_gates: vec![
                "national-data-collection-shard-manifest",
                "national-data-collection-shard-execution",
            ],
        };
        write_json_file(&self.config.evidence_path, &evidence)?;
        println!(
            "national-data-collection-scope-written status=ready rows={} registry_rows={} path={}",
            scope_rows.len(),
            registry_rows.len(),
            repo_relative_path(&self.config.root, &self.config.output_path)
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct ScopeRow {
    schema_version: &'static str,
    scope_unit_id: String,
    scope_kind: &'static str,
    canonical_code: String,
    scope_key: String,
    bjdong_code: String,
    sigungu_cd: String,
    bjdong_cd: String,
    geometry_srid: i64,
    bbox: ScopeBbox,
    source_provider: String,
    source_snapshot_id: String,
    source_row_count: u64,
}

#[derive(Serialize)]
struct ScopeBbox {
    min_x: String,
    min_y: String,
    max_x: String,
    max_y: String,
}

#[derive(Serialize)]
struct ScopeEvidence {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    source_kind: &'static str,
    registry_path: String,
    registry_sha256: String,
    registry_evidence_path: String,
    output_kind: &'static str,
    output_path: String,
    csv_projection_path: String,
    scope_row_schema_version: &'static str,
    registry_row_count: u64,
    source_row_count: u64,
    scope_row_count: u64,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    national_rollout_blocked_reason: &'static str,
    evidence_limitations: Vec<&'static str>,
    next_gates: Vec<&'static str>,
}

fn assert_registry_evidence(
    evidence: &JsonValue,
    registry_relative_path: &str,
    registry_sha256: &str,
) -> anyhow::Result<()> {
    if json_string(evidence, "schema_version") != REGISTRY_EVIDENCE_SCHEMA_VERSION {
        bail!("registry evidence schema mismatch");
    }
    if json_string(evidence, "status") != "ready" {
        bail!("registry evidence status must be ready");
    }
    if json_string(evidence, "registry_path") != registry_relative_path {
        bail!("registry evidence registry_path must match RegistryPath");
    }
    if json_string(evidence, "registry_sha256") != registry_sha256 {
        bail!("registry evidence registry_sha256 must match RegistryPath");
    }
    if json_bool(evidence, "national_rollout_allowed", true) {
        bail!("registry evidence must not allow national rollout");
    }
    Ok(())
}

fn read_registry_rows(path: &Path) -> anyhow::Result<Vec<JsonValue>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line_number = index + 1;
        let line = if line_number == 1 {
            raw_line.trim_start_matches('\u{feff}')
        } else {
            raw_line
        };
        if line.trim().is_empty() {
            bail!("registry line {line_number} must not be blank");
        }
        rows.push(
            serde_json::from_str(line)
                .with_context(|| format!("registry line {line_number} is not valid JSON"))?,
        );
    }
    Ok(rows)
}

fn scope_row_from_registry(row: &JsonValue) -> anyhow::Result<ScopeRow> {
    let bjdong_code = json_string(row, "canonical_code");
    if !is_fixed_digits(&bjdong_code, 10) {
        bail!("active legal_dong canonical_code must be ten digits");
    }
    let bbox = row.get("bbox").unwrap_or(&JsonValue::Null);
    Ok(ScopeRow {
        schema_version: SCOPE_ROW_SCHEMA_VERSION,
        scope_unit_id: json_string(row, "scope_unit_id"),
        scope_kind: "legal_dong",
        canonical_code: bjdong_code.clone(),
        scope_key: format!("{}:{}", &bjdong_code[0..5], &bjdong_code[5..10]),
        bjdong_code: bjdong_code.clone(),
        sigungu_cd: bjdong_code[0..5].to_owned(),
        bjdong_cd: bjdong_code[5..10].to_owned(),
        geometry_srid: json_i64(row, "geometry_srid", 0),
        bbox: ScopeBbox {
            min_x: decimal_string(bbox.get("min_x").unwrap_or(&JsonValue::Null))?,
            min_y: decimal_string(bbox.get("min_y").unwrap_or(&JsonValue::Null))?,
            max_x: decimal_string(bbox.get("max_x").unwrap_or(&JsonValue::Null))?,
            max_y: decimal_string(bbox.get("max_y").unwrap_or(&JsonValue::Null))?,
        },
        source_provider: json_string(row, "source_provider"),
        source_snapshot_id: json_string(row, "source_snapshot_id"),
        source_row_count: 1,
    })
}

fn write_scope_jsonl(path: &Path, rows: &[ScopeRow]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create scope output directory {}",
                parent.display()
            )
        })?;
    }
    let mut output = String::new();
    for row in rows {
        output.push_str(&serde_json::to_string(row).context("failed to serialize scope row")?);
        output.push('\n');
    }
    fs::write(path, output).with_context(|| format!("failed to write {}", path.display()))
}

fn write_scope_csv(path: &Path, rows: &[ScopeRow]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create scope CSV directory {}", parent.display())
        })?;
    }
    let mut output = String::from(
        "scope_unit_id,sigungu_cd,bjdong_cd,bjdong_code,bbox_min_x,bbox_min_y,bbox_max_x,bbox_max_y,source_provider,source_snapshot_id\n",
    );
    for row in rows {
        output.push_str(
            &[
                row.scope_unit_id.as_str(),
                row.sigungu_cd.as_str(),
                row.bjdong_cd.as_str(),
                row.bjdong_code.as_str(),
                row.bbox.min_x.as_str(),
                row.bbox.min_y.as_str(),
                row.bbox.max_x.as_str(),
                row.bbox.max_y.as_str(),
                row.source_provider.as_str(),
                row.source_snapshot_id.as_str(),
            ]
            .into_iter()
            .map(csv_cell)
            .collect::<Vec<_>>()
            .join(","),
        );
        output.push('\n');
    }
    fs::write(path, output).with_context(|| format!("failed to write {}", path.display()))
}

fn csv_cell(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn decimal_string(value: &JsonValue) -> anyhow::Result<String> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_f64().map(|value| value.to_string()))
        .context("bbox coordinate is required")?;
    let parsed = raw
        .parse::<f64>()
        .with_context(|| format!("bbox coordinate must be decimal: {raw}"))?;
    Ok(format!("{parsed:.6}"))
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("{digest:x}"))
}

fn json_string(value: &JsonValue, field: &str) -> String {
    value
        .get(field)
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn json_bool(value: &JsonValue, field: &str, default: bool) -> bool {
    value
        .get(field)
        .and_then(JsonValue::as_bool)
        .unwrap_or(default)
}

fn json_i64(value: &JsonValue, field: &str, default: i64) -> i64 {
    value
        .get(field)
        .and_then(|raw| {
            raw.as_i64()
                .or_else(|| raw.as_u64().and_then(|v| v.try_into().ok()))
        })
        .unwrap_or(default)
}

fn is_fixed_digits(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => bail!("{name} must be a boolean"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    const VERBATIM_PREFIX: &str = r"\\?\";
    let raw = path.to_string_lossy();
    if let Some(stripped) = raw.strip_prefix(VERBATIM_PREFIX) {
        PathBuf::from(stripped)
    } else {
        path
    }
}
