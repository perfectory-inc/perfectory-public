use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{bail, Context};
use collection_domain::building_register_dataset_slug;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::public_api_metric_writer;
use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_canonical_source_slug,
    resolve_repo_path, utc_now, write_json_file,
};

const SCHEMA_VERSION: &str = "foundation-platform.bronze_public_data_ingestion_evidence.v1";
const SCOPE_SCHEMA_VERSION: &str = "foundation-platform.bounded_live_ingestion_scope.v1";
const BOUNDED_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.bounded_live_ingestion_evidence.v1";
const DEFAULT_SCOPE_PATH: &str = "target/audit/bounded-live-ingestion-scope.json";
const DEFAULT_BOUNDED_EVIDENCE_PATH: &str = "target/audit/bounded-live-ingestion-evidence.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/bronze-public-data-ingestion-evidence.json";
const DEFAULT_LOG_PATH: &str = "target/audit/bronze-public-data-ingestion-run.log";
const DEFAULT_QUOTA_METRICS_PATH: &str =
    "target/public-api-quota/bronze-public-data-ingestion.prom";
const DEFAULT_LOCAL_OBJECT_ROOT: &str = "target/bronze-local-proof";
const MODE: &str = "building_register_local_bronze_proof";
const PREFIX: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_LOCAL_BRONZE_PROOF";

pub fn run() -> anyhow::Result<()> {
    let config = ProofConfig::from_env()?;
    let scope = read_json(&config.scope_path, "bounded live ingestion scope")
        .with_context(|| "Bounded scope artifact missing")?;
    let bounded_evidence = read_json(
        &config.bounded_evidence_path,
        "bounded live ingestion evidence",
    )
    .with_context(|| "Bounded evidence artifact missing")?;
    let request = validate_scope_and_bounded_evidence(&scope, &bounded_evidence)?;
    config.validate(&request)?;
    let source_slug = config.effective_source_slug(&request.operation)?;

    if !config.confirm_public_api_quota_impact {
        bail!("Public API quota impact must be confirmed with -ConfirmPublicApiQuotaImpact");
    }

    let dotenv = import_dotenv(&config.env_file)?;
    require_env(&dotenv, "DATABASE_URL")?;
    require_env(&dotenv, "DATA_GO_KR_SERVICE_KEY")?;

    fs::create_dir_all(&config.local_object_root).with_context(|| {
        format!(
            "failed to create local Bronze object root {}",
            config.local_object_root.display()
        )
    })?;
    if let Some(parent) = config.log_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create local Bronze proof log directory {}",
                parent.display()
            )
        })?;
    }

    write_quota_metric(
        &config.quota_metrics_path,
        "data.go.kr",
        &request.operation,
        request.max_pages,
    )?;

    let cargo = resolve_cargo(&config.cargo_exe)?;
    let child_env = building_child_env(&config, &request, &source_slug, &dotenv);
    let run = invoke_outbox_command(
        &config.root,
        &cargo,
        "ingest-building-register",
        &child_env,
        &config.log_path,
    )?;
    if run.exit_code != 0 {
        write_dependency_metric(
            &config.quota_metrics_path,
            "data.go.kr",
            &request.operation,
            run.duration,
            "failed",
            Some("bronze_ingest_error"),
        )?;
        bail!(
            "building-register Bronze local proof failed with cargo exit code {}",
            run.exit_code
        );
    }
    write_dependency_metric(
        &config.quota_metrics_path,
        "data.go.kr",
        &request.operation,
        run.duration,
        "succeeded",
        None,
    )?;

    let bronze = bronze_run_report(&config.local_object_root, run.started_at, &source_slug)?;
    let last_object_path = config.local_object_root.join(
        bronze
            .objects
            .last()
            .context("Bronze object report is empty")?
            .object_key
            .as_str(),
    );

    let report = json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "git_head": git_head(&config.root),
        "status": "ready",
        "completion_claim_allowed": false,
        "national_rollout_allowed": false,
        "national_rollout_blocked_reason": "bronze_bounded_scope_only",
        "raw_response_preserved": true,
        "evidence_paths": {
            "bounded_scope": repo_relative_path(&config.root, &config.scope_path),
            "bounded_evidence": repo_relative_path(&config.root, &config.bounded_evidence_path),
            "local_ingestion_log": repo_relative_path(&config.root, &config.log_path),
            "quota_metrics": repo_relative_path(&config.root, &config.quota_metrics_path),
            "local_bronze_object": repo_relative_path(&config.root, &last_object_path),
        },
        "source": {
            "provider": "data.go.kr",
            "operation": request.operation,
            "request_count": request.max_pages,
            "sigungu_cd": request.sigungu_cd,
            "bjdong_cd": request.bjdong_cd,
            "num_of_rows": request.num_of_rows,
            "source_slug": source_slug,
        },
        "lineage": {
            "fetched_at_utc": utc_now(),
            "source_record_count": bronze.logical_record_count,
            "ingestion_run_id": bronze.run_id,
        },
        "bronze": {
            "storage_driver": "local",
            "object_count": bronze.object_count,
            "total_size_bytes": bronze.total_size_bytes,
            "objects": bronze.objects,
        },
        "blockers": [],
        "evidence_limitations": [
            "bounded_single_administrative_dong_only",
            "local_bronze_object_storage_only",
            "does_not_run_national_collection",
            "does_not_promote_silver_or_gold"
        ],
    });
    write_json_file(&config.output_path, &report)?;

    println!(
        "building-register-local-bronze-proof-ok status=ready provider=data.go.kr objects={} bytes={} report={}",
        bronze.object_count,
        bronze.total_size_bytes,
        config.output_path.display()
    );
    println!("No secret values or raw payload were printed.");
    Ok(())
}

struct ProofConfig {
    root: PathBuf,
    env_file: PathBuf,
    scope_path: PathBuf,
    bounded_evidence_path: PathBuf,
    output_path: PathBuf,
    log_path: PathBuf,
    quota_metrics_path: PathBuf,
    local_object_root: PathBuf,
    /// Explicit `FOUNDATION_PLATFORM_..._SOURCE_SLUG` override, if set. When absent the effective slug is
    /// derived from the bounded-scope operation via the generator (ADR 0014 §6: `*-local-proof`
    /// folds to the canonical `datagokr__<dataset_slug>`; the local-FS prefix is the run-scope
    /// distinction, not a slug suffix).
    source_slug_override: Option<String>,
    cargo_exe: String,
    confirm_public_api_quota_impact: bool,
}

impl ProofConfig {
    /// Resolves the effective Bronze `source_slug` for this proof run: the explicit env override if
    /// present, otherwise the canonical generator slug for the bounded-scope operation.
    fn effective_source_slug(&self, operation: &str) -> anyhow::Result<String> {
        let dataset_slug = building_register_dataset_slug(operation).with_context(|| {
            format!("building-register operation has no registered dataset_slug: {operation}")
        })?;
        resolve_canonical_source_slug(
            &format!("{PREFIX}_SOURCE_SLUG"),
            self.source_slug_override.clone(),
            "data.go.kr",
            dataset_slug,
        )
    }
}

struct BuildingRegisterRequest {
    operation: String,
    sigungu_cd: String,
    bjdong_cd: String,
    max_pages: i64,
    num_of_rows: i64,
}

struct CommandRun {
    started_at: SystemTime,
    duration: Duration,
    exit_code: i32,
}

#[derive(Serialize)]
struct BronzeObjectReport {
    object_key: String,
    checksum_sha256: String,
    size_bytes: usize,
    logical_record_count: i64,
}

struct BronzeRunReport {
    /// Ingestion run id. After ADR 0019 the run id is no longer encoded in the Bronze object
    /// key, so this local-mirror proof cannot recover it from the path; run lineage lives in the
    /// `bronze_object` row + run manifest (control plane). `None` means "not recoverable from the
    /// local object mirror".
    run_id: Option<String>,
    object_count: i64,
    total_size_bytes: i64,
    logical_record_count: i64,
    objects: Vec<BronzeObjectReport>,
}

impl ProofConfig {
    fn from_env() -> anyhow::Result<Self> {
        let default_root = env_string("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = env_path(&format!("{PREFIX}_ROOT"), &default_root)?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let env_file = env_path(&format!("{PREFIX}_ENV_FILE"), "")?;
        let env_file = if env_file.as_os_str().is_empty() {
            root.join(".env.local")
        } else {
            resolve_repo_path(&root, &env_file, "EnvFile")?
        };

        Ok(Self {
            scope_path: resolve_repo_path(
                &root,
                &env_path(&format!("{PREFIX}_SCOPE_PATH"), DEFAULT_SCOPE_PATH)?,
                "ScopePath",
            )?,
            bounded_evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    &format!("{PREFIX}_BOUNDED_EVIDENCE_PATH"),
                    DEFAULT_BOUNDED_EVIDENCE_PATH,
                )?,
                "BoundedEvidencePath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(&format!("{PREFIX}_OUTPUT_PATH"), DEFAULT_OUTPUT_PATH)?,
                "OutputPath",
            )?,
            log_path: resolve_repo_path(
                &root,
                &env_path(&format!("{PREFIX}_LOG_PATH"), DEFAULT_LOG_PATH)?,
                "LogPath",
            )?,
            quota_metrics_path: resolve_repo_path(
                &root,
                &env_path(
                    &format!("{PREFIX}_QUOTA_METRICS_PATH"),
                    DEFAULT_QUOTA_METRICS_PATH,
                )?,
                "QuotaMetricsPath",
            )?,
            local_object_root: resolve_repo_path(
                &root,
                &env_path(
                    &format!("{PREFIX}_LOCAL_OBJECT_ROOT"),
                    DEFAULT_LOCAL_OBJECT_ROOT,
                )?,
                "LocalObjectRoot",
            )?,
            source_slug_override: optional_env_string(&format!("{PREFIX}_SOURCE_SLUG"))?,
            cargo_exe: env_string(&format!("{PREFIX}_CARGO_EXE"), "")?,
            confirm_public_api_quota_impact: env_bool(
                &format!("{PREFIX}_CONFIRM_PUBLIC_API_QUOTA_IMPACT"),
                false,
            )?,
            root,
            env_file,
        })
    }

    fn validate(&self, request: &BuildingRegisterRequest) -> anyhow::Result<()> {
        if !simple_identifier(&request.operation) {
            bail!("data_go_kr.operation must be a simple API operation identifier");
        }
        if !five_digits(&request.sigungu_cd) || !five_digits(&request.bjdong_cd) {
            bail!("data_go_kr administrative codes must be exactly five digits");
        }
        if request.max_pages < 1 || request.num_of_rows < 1 || request.num_of_rows > 100 {
            bail!("local Bronze proof must stay within bounded scope max_pages>=1 and num_of_rows<=100");
        }
        // Resolving the effective slug both validates an explicit override (relaxed charset allows
        // the canonical `__` separator) and confirms the operation maps to a registered dataset_slug.
        let effective = self.effective_source_slug(&request.operation)?;
        if !source_slug(&effective) {
            bail!("SourceSlug must be lowercase ASCII letters, digits, underscores, and hyphens");
        }
        Ok(())
    }
}

fn validate_scope_and_bounded_evidence(
    scope: &JsonValue,
    bounded_evidence: &JsonValue,
) -> anyhow::Result<BuildingRegisterRequest> {
    if string_property(scope, "schema_version") != SCOPE_SCHEMA_VERSION {
        bail!("bounded live ingestion scope schema mismatch");
    }
    if bool_property(scope, "national_rollout_allowed", false) {
        bail!("bounded live ingestion scope must keep national_rollout_allowed=false");
    }
    if string_property(bounded_evidence, "schema_version") != BOUNDED_EVIDENCE_SCHEMA_VERSION {
        bail!("bounded live ingestion evidence schema mismatch");
    }
    if string_property(bounded_evidence, "status") != "ready" {
        bail!("bounded live ingestion evidence status is not ready");
    }
    if bool_property(bounded_evidence, "national_rollout_allowed", false) {
        bail!("bounded live ingestion evidence must keep national_rollout_allowed=false");
    }

    let data_go_kr = scope
        .get("data_go_kr")
        .context("bounded live ingestion scope omitted data_go_kr")?;
    let request = BuildingRegisterRequest {
        operation: string_property(data_go_kr, "operation"),
        sigungu_cd: string_property(data_go_kr, "sigungu_cd"),
        bjdong_cd: string_property(data_go_kr, "bjdong_cd"),
        max_pages: i64_property(data_go_kr, "max_pages", 0),
        num_of_rows: i64_property(data_go_kr, "num_of_rows", 0),
    };

    let quota = bounded_evidence
        .get("quota")
        .context("bounded live ingestion evidence omitted quota")?;
    let bounded_cap = i64_property(quota, "cap", 0);
    let bounded_planned_request_count = i64_property(quota, "planned_request_count", 0);
    if bounded_cap < 1 || bounded_planned_request_count < 1 {
        bail!("bounded live ingestion evidence quota must be positive");
    }
    if request.max_pages > bounded_planned_request_count || request.max_pages > bounded_cap {
        bail!("local Bronze proof max_pages must not exceed bounded quota evidence");
    }

    Ok(request)
}

fn building_child_env(
    config: &ProofConfig,
    request: &BuildingRegisterRequest,
    source_slug: &str,
    dotenv: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut envs = dotenv.clone();
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG".to_owned(),
        source_slug.to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION".to_owned(),
        request.operation.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD".to_owned(),
        request.sigungu_cd.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD".to_owned(),
        request.bjdong_cd.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_NO".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS".to_owned(),
        request.num_of_rows.to_string(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_MAX_PAGES".to_owned(),
        request.max_pages.to_string(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_LIVE_WRITE".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER".to_owned(),
        "local".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT".to_owned(),
        config.local_object_root.to_string_lossy().to_string(),
    );
    envs.entry("RUST_LOG".to_owned())
        .or_insert_with(|| "info".to_owned());
    envs
}

fn invoke_outbox_command(
    root: &Path,
    cargo: &Path,
    command_name: &str,
    envs: &BTreeMap<String, String>,
    log_path: &Path,
) -> anyhow::Result<CommandRun> {
    let started_at = SystemTime::now();
    let timer = Instant::now();
    let mut command = outbox_subcommand(cargo, command_name);
    command.current_dir(root);
    for (name, value) in envs {
        command.env(name, value);
    }
    let output = command.output()?;
    let duration = timer.elapsed();
    let exit_code = output.status.code().unwrap_or(1);
    fs::write(log_path, String::new())?;
    append_command_output(log_path, output.stdout)?;
    append_command_output(log_path, output.stderr)?;
    Ok(CommandRun {
        started_at,
        duration,
        exit_code,
    })
}

/// Builds the command that runs an outbox-publisher `command_name` subcommand.
///
/// Prefers re-invoking the current binary directly so production never needs the Cargo toolchain;
/// falls back to `cargo run -p foundation-outbox-publisher -- <command_name>` (using the resolved
/// `cargo` path) only when the current executable cannot be resolved.
fn outbox_subcommand(cargo: &Path, command_name: &str) -> Command {
    match std::env::current_exe() {
        Ok(exe) => {
            let mut command = Command::new(exe);
            command.arg(command_name);
            command
        }
        Err(_) => {
            let mut command = Command::new(cargo);
            command.args([
                "run",
                "-p",
                "foundation-outbox-publisher",
                "--",
                command_name,
            ]);
            command
        }
    }
}

fn bronze_run_report(
    local_root: &Path,
    started_at: SystemTime,
    source_slug: &str,
) -> anyhow::Result<BronzeRunReport> {
    let bronze_root = local_root.join("bronze");
    if !bronze_root.is_dir() {
        bail!("local Bronze root does not contain a bronze/ directory");
    }
    let source_root = bronze_root.join(format!("source={source_slug}"));
    if !source_root.is_dir() {
        bail!("local Bronze run directory was not found for source={source_slug}");
    }

    // The readable Bronze key (ADR 0019) no longer encodes run_id in the path, so the
    // proof identifies the run by its write time window, not a `run_id=` directory. Report every
    // object under `source={slug}/` written at/after this proof run started.
    let mut candidates = Vec::new();
    collect_files(&source_root, &mut candidates)?;
    let mut files: Vec<PathBuf> = candidates
        .into_iter()
        .filter(|path| {
            fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .map(|modified| modified >= started_at - Duration::from_secs(1))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    if files.is_empty() {
        bail!(
            "local Bronze run directory contains no objects written by this proof run for source={source_slug}"
        );
    }
    for path in &files {
        let object_key = bronze_object_key(local_root, path)?;
        if !object_key.starts_with("bronze/") {
            bail!("Bronze object key must start with bronze/");
        }
    }
    let mut objects = Vec::new();
    let mut total_size_bytes = 0_i64;
    let mut total_logical_record_count = 0_i64;
    for path in files {
        let object_key = bronze_object_key(local_root, &path)?;
        let bytes = fs::read(&path)
            .with_context(|| format!("failed to read local Bronze object {object_key}"))?;
        if bytes.is_empty() {
            bail!("local Bronze object was empty: {object_key}");
        }
        let logical_record_count = count_logical_records(&bytes)
            .with_context(|| format!("failed to count logical records in {object_key}"))?;
        if logical_record_count < 1 {
            bail!("local Bronze object contains no logical records: {object_key}");
        }
        total_size_bytes += i64::try_from(bytes.len()).unwrap_or(i64::MAX);
        total_logical_record_count += logical_record_count;
        objects.push(BronzeObjectReport {
            object_key,
            checksum_sha256: sha256_bytes(&bytes),
            size_bytes: bytes.len(),
            logical_record_count,
        });
    }
    Ok(BronzeRunReport {
        run_id: None,
        object_count: i64::try_from(objects.len()).unwrap_or(i64::MAX),
        total_size_bytes,
        logical_record_count: total_logical_record_count,
        objects,
    })
}

fn import_dotenv(path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    if !path.is_file() {
        return Ok(values);
    }
    for raw_line in fs::read_to_string(path)
        .with_context(|| format!("failed to read .env file {}", path.display()))?
        .lines()
    {
        let mut line = raw_line.trim_start_matches('\u{feff}').trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(stripped) = line.strip_prefix("export ") {
            line = stripped.trim_start();
        }
        let Some((name, value)) = line.split_once('=') else {
            bail!("Invalid .env line in {}: {raw_line}", path.display());
        };
        let name = name.trim();
        if !valid_env_name(name) {
            bail!(
                "Invalid environment variable name in {}: {name}",
                path.display()
            );
        }
        values.insert(name.to_owned(), trim_env_value(value.trim()));
    }
    Ok(values)
}

fn require_env(dotenv: &BTreeMap<String, String>, name: &str) -> anyhow::Result<()> {
    let present = dotenv
        .get(name)
        .cloned()
        .or_else(|| env::var(name).ok())
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    if !present {
        bail!("Missing required environment variable: {name}");
    }
    Ok(())
}

fn resolve_cargo(explicit: &str) -> anyhow::Result<PathBuf> {
    if !explicit.trim().is_empty() {
        return Ok(PathBuf::from(explicit));
    }
    if let Some(path) = env::var_os("PATH") {
        for dir in env::split_paths(&path) {
            for candidate in ["cargo.exe", "cargo"] {
                let path = dir.join(candidate);
                if path.is_file() {
                    return Ok(path);
                }
            }
        }
    }
    for profile_root in [env::var_os("USERPROFILE"), env::var_os("HOME")]
        .into_iter()
        .flatten()
    {
        for candidate in ["cargo.exe", "cargo"] {
            let path = PathBuf::from(&profile_root)
                .join(".cargo")
                .join("bin")
                .join(candidate);
            if path.is_file() {
                return Ok(path);
            }
        }
    }
    bail!("cargo was not found")
}

fn write_quota_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    count: i64,
) -> anyhow::Result<()> {
    public_api_metric_writer::write_quota_metric(path, provider, endpoint, count, "attempted", MODE)
}

fn write_dependency_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    duration: Duration,
    outcome: &str,
    error_kind: Option<&str>,
) -> anyhow::Result<()> {
    public_api_metric_writer::write_dependency_metric_duration(
        path, provider, endpoint, duration, outcome, MODE, error_kind,
    )
}

fn append_command_output(path: &Path, bytes: Vec<u8>) -> anyhow::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&bytes);
    let mut content = fs::read_to_string(path).unwrap_or_default();
    content.push_str(&text);
    if !content.ends_with('\n') {
        content.push('\n');
    }
    fs::write(path, content)?;
    Ok(())
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn bronze_object_key(local_root: &Path, path: &Path) -> anyhow::Result<String> {
    Ok(path
        .strip_prefix(local_root)?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn count_logical_records(bytes: &[u8]) -> anyhow::Result<i64> {
    let json: JsonValue = serde_json::from_slice(strip_utf8_bom(bytes))?;
    let items = json
        .pointer("/response/body/items/item")
        .map(array_or_one_len)
        .unwrap_or(0);
    Ok(items)
}

fn array_or_one_len(value: &JsonValue) -> i64 {
    value
        .as_array()
        .map(|items| i64::try_from(items.len()).unwrap_or(i64::MAX))
        .unwrap_or(1)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        // Present-but-empty behaves like unset: PowerShell wrappers cannot delete env vars
        // portably ($env:X = "" removes on Windows but sets empty on Linux).
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn optional_env_string(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => bail!("invalid {name} environment variable"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn string_property(value: &JsonValue, name: &str) -> String {
    value
        .get(name)
        .map(|property| match property {
            JsonValue::String(text) => text.clone(),
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(number) => number.to_string(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

fn i64_property(value: &JsonValue, name: &str, default: i64) -> i64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_i64(),
            JsonValue::String(text) => text.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn bool_property(value: &JsonValue, name: &str, default: bool) -> bool {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Bool(flag) => Some(*flag),
            JsonValue::String(text) => text.trim().parse::<bool>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let value = path.to_string_lossy();
        if let Some(rest) = value.strip_prefix("\\\\?\\UNC\\") {
            return PathBuf::from(format!("\\\\{rest}"));
        }
        if let Some(rest) = value.strip_prefix("\\\\?\\") {
            return PathBuf::from(rest);
        }
    }
    path
}

fn trim_env_value(value: &str) -> String {
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        value[1..value.len().saturating_sub(1)].to_owned()
    } else {
        value.to_owned()
    }
}

fn valid_env_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn simple_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_alphabetic() && bytes.all(|byte| byte.is_ascii_alphanumeric())
}

fn five_digits(value: &str) -> bool {
    value.len() == 5 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn source_slug(value: &str) -> bool {
    value.len() >= 2
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.as_bytes()[value.len() - 1].is_ascii_alphanumeric()
}
