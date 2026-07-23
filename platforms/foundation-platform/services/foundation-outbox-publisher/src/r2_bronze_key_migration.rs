//! R2 Bronze object key migration from legacy date partitions to canonical run partitions.

use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::Utc;
use foundation_outbox::{
    object_storage::{validate_r2_bronze_key_migration_pair, R2ObjectStorageConfig},
    R2ObjectStorage,
};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};

const PLAN_SCHEMA_VERSION: &str = "foundation-platform.r2_bronze_key_migration_plan.v1";
const REPORT_SCHEMA_VERSION: &str = "foundation-platform.r2_bronze_key_migration_execution.v1";
const REQUIRED_CONFIRM_PHRASE: &str = "MIGRATE FOUNDATION PLATFORM R2 BRONZE KEYS";
const DEFAULT_REPORT_PATH: &str =
    "target/r2-bronze-key-migration-rust/r2-bronze-key-migration-execution.json";
const DEFAULT_CONCURRENCY: usize = 32;
const MAX_CONCURRENCY: usize = 128;

/// Runs the R2 Bronze key migration command.
pub async fn run() -> anyhow::Result<()> {
    let config = MigrationConfig::from_env()?;
    let report = execute_migration(&config).await?;
    write_report(&config.report_path, &report)?;

    tracing::info!(
        mode = report.mode,
        status = report.status,
        selected_object_count = report.selected_object_count,
        copied_count = report.copied_count,
        failed_count = report.failed_count,
        dry_run_count = report.dry_run_count,
        report_path = %config.report_path.display(),
        "R2 Bronze key migration finished"
    );

    if report.failed_count > 0 {
        bail!(
            "R2 Bronze key migration failed for {} object(s); see {}",
            report.failed_count,
            config.report_path.display()
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MigrationConfig {
    plan_path: PathBuf,
    report_path: PathBuf,
    skip_objects: usize,
    max_objects: Option<usize>,
    concurrency: usize,
    execute: bool,
    env_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct MigrationPlan {
    schema_version: String,
    status: String,
    #[serde(default)]
    source_audit: String,
    objects: Vec<MigrationPlanObject>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct MigrationPlanObject {
    old_key: String,
    new_key: String,
    size_bytes: u64,
    status: String,
}

#[derive(Debug, Serialize)]
struct MigrationExecutionReport {
    schema_version: &'static str,
    generated_at_utc: String,
    mode: &'static str,
    status: &'static str,
    source_plan: String,
    source_audit: String,
    skip_objects: usize,
    max_objects: Option<usize>,
    concurrency: usize,
    selected_object_count: usize,
    selected_total_size_bytes: u64,
    copied_count: usize,
    dry_run_count: usize,
    failed_count: usize,
    objects: Vec<MigrationObjectExecution>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MigrationObjectExecution {
    old_key: String,
    new_key: String,
    size_bytes: u64,
    status: &'static str,
    error: Option<String>,
}

impl MigrationConfig {
    fn from_env() -> anyhow::Result<Self> {
        let plan_path = PathBuf::from(required_env(
            "FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_PLAN_PATH",
        )?);
        let report_path = optional_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_REPORT_PATH")?
            .map_or_else(|| PathBuf::from(DEFAULT_REPORT_PATH), PathBuf::from);
        let skip_objects =
            optional_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_SKIP_OBJECTS")?
                .map(|raw| parse_usize(&raw, "skip objects"))
                .transpose()?
                .unwrap_or(0);
        let max_objects = optional_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_MAX_OBJECTS")?
            .map(|raw| parse_positive_usize(&raw, "max objects"))
            .transpose()?;
        let concurrency = optional_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_CONCURRENCY")?
            .map(|raw| parse_concurrency(&raw))
            .transpose()?
            .unwrap_or(DEFAULT_CONCURRENCY);
        let execute = optional_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_EXECUTE")?
            .is_some_and(|raw| raw == "1");
        let env_file = optional_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_ENV_FILE")?
            .map(PathBuf::from);

        if execute {
            let phrase =
                required_env("FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_CONFIRM_PHRASE")?;
            if phrase != REQUIRED_CONFIRM_PHRASE {
                bail!(
                    "FOUNDATION_PLATFORM_R2_BRONZE_KEY_MIGRATION_CONFIRM_PHRASE must exactly equal '{REQUIRED_CONFIRM_PHRASE}' when execute is enabled"
                );
            }
        }

        Ok(Self {
            plan_path,
            report_path,
            skip_objects,
            max_objects,
            concurrency,
            execute,
            env_file,
        })
    }
}

async fn execute_migration(config: &MigrationConfig) -> anyhow::Result<MigrationExecutionReport> {
    let plan = read_plan(&config.plan_path)?;
    let objects = select_copy_required_objects(&plan, config.skip_objects, config.max_objects);
    for object in &objects {
        validate_r2_bronze_key_migration_pair(&object.old_key, &object.new_key).with_context(
            || {
                format!(
                    "invalid R2 Bronze migration pair for old key {}",
                    object.old_key
                )
            },
        )?;
    }

    let results = if config.execute {
        let storage = r2_storage(config)
            .await
            .context("failed to configure R2 for Bronze key migration")?;
        copy_objects(storage, objects.clone(), config.concurrency).await
    } else {
        objects
            .iter()
            .map(|object| MigrationObjectExecution {
                old_key: object.old_key.clone(),
                new_key: object.new_key.clone(),
                size_bytes: object.size_bytes,
                status: "dry_run",
                error: None,
            })
            .collect()
    };

    let copied_count = results
        .iter()
        .filter(|result| result.status == "copied")
        .count();
    let dry_run_count = results
        .iter()
        .filter(|result| result.status == "dry_run")
        .count();
    let failed_count = results
        .iter()
        .filter(|result| result.status == "failed")
        .count();
    let status = if failed_count == 0 {
        if config.execute {
            "completed"
        } else {
            "ready"
        }
    } else {
        "blocked"
    };

    Ok(MigrationExecutionReport {
        schema_version: REPORT_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        mode: if config.execute { "execute" } else { "dry_run" },
        status,
        source_plan: config.plan_path.display().to_string(),
        source_audit: plan.source_audit,
        skip_objects: config.skip_objects,
        max_objects: config.max_objects,
        concurrency: config.concurrency,
        selected_object_count: objects.len(),
        selected_total_size_bytes: selected_total_size_bytes(&objects),
        copied_count,
        dry_run_count,
        failed_count,
        objects: results,
    })
}

async fn copy_objects(
    storage: R2ObjectStorage,
    objects: Vec<MigrationPlanObject>,
    concurrency: usize,
) -> Vec<MigrationObjectExecution> {
    stream::iter(objects.into_iter().map(|object| {
        let storage = storage.clone();
        async move {
            let result = storage
                .copy_legacy_date_partitioned_bronze_object(&object.old_key, &object.new_key)
                .await;
            match result {
                Ok(()) => MigrationObjectExecution {
                    old_key: object.old_key,
                    new_key: object.new_key,
                    size_bytes: object.size_bytes,
                    status: "copied",
                    error: None,
                },
                Err(error) => MigrationObjectExecution {
                    old_key: object.old_key,
                    new_key: object.new_key,
                    size_bytes: object.size_bytes,
                    status: "failed",
                    error: Some(error.to_string()),
                },
            }
        }
    }))
    .buffer_unordered(concurrency)
    .collect()
    .await
}

fn read_plan(path: &Path) -> anyhow::Result<MigrationPlan> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read migration plan {}", path.display()))?;
    read_plan_from_str(&raw)
}

fn read_plan_from_str(raw: &str) -> anyhow::Result<MigrationPlan> {
    let raw = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let plan: MigrationPlan =
        serde_json::from_str(raw).context("failed to parse R2 Bronze key migration plan")?;
    if plan.schema_version != PLAN_SCHEMA_VERSION {
        bail!(
            "unsupported R2 Bronze key migration plan schema_version: {}",
            plan.schema_version
        );
    }
    if plan.status != "ready" {
        bail!(
            "R2 Bronze key migration plan must be ready, got {}",
            plan.status
        );
    }
    Ok(plan)
}

fn select_copy_required_objects(
    plan: &MigrationPlan,
    skip_objects: usize,
    max_objects: Option<usize>,
) -> Vec<MigrationPlanObject> {
    let iter = plan
        .objects
        .iter()
        .filter(|object| object.status == "copy_required")
        .skip(skip_objects);
    match max_objects {
        Some(limit) => iter.take(limit).cloned().collect(),
        None => iter.cloned().collect(),
    }
}

fn selected_total_size_bytes(objects: &[MigrationPlanObject]) -> u64 {
    objects.iter().map(|object| object.size_bytes).sum()
}

fn write_report(path: &Path, report: &MigrationExecutionReport) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("migration report path must have a parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create migration report directory {}",
            parent.display()
        )
    })?;
    let payload =
        serde_json::to_vec_pretty(report).context("failed to serialize migration report")?;
    fs::write(path, payload)
        .with_context(|| format!("failed to write migration report {}", path.display()))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} environment variable is required"))
}

fn parse_usize(raw: &str, label: &str) -> anyhow::Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("R2 Bronze key migration {label} must be an integer"))
}

fn parse_positive_usize(raw: &str, label: &str) -> anyhow::Result<usize> {
    let value = parse_usize(raw, label)?;
    if value == 0 {
        bail!("R2 Bronze key migration {label} must be greater than zero");
    }
    Ok(value)
}

fn parse_concurrency(raw: &str) -> anyhow::Result<usize> {
    let value = parse_positive_usize(raw, "concurrency")?;
    if value > MAX_CONCURRENCY {
        bail!("R2 Bronze key migration concurrency must be at most {MAX_CONCURRENCY}");
    }
    Ok(value)
}

async fn r2_storage(config: &MigrationConfig) -> anyhow::Result<R2ObjectStorage> {
    match &config.env_file {
        Some(path) if path.as_os_str().is_empty() => Ok(R2ObjectStorage::from_env()?),
        Some(path) => Ok(R2ObjectStorage::from_config(r2_config_from_env_file(path)?)),
        None => Ok(R2ObjectStorage::from_env()?),
    }
}

fn r2_config_from_env_file(path: &Path) -> anyhow::Result<R2ObjectStorageConfig> {
    if !path.is_file() {
        bail!("Env file not found: {}", path.display());
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read env file {}", path.display()))?;
    r2_config_from_dotenv_str(&raw)
}

fn r2_config_from_dotenv_str(raw: &str) -> anyhow::Result<R2ObjectStorageConfig> {
    let values = parse_dotenv(raw);
    let endpoint = value_from_dotenv_or_env(&values, "R2_ENDPOINT")?.map_or_else(
        || {
            required_value_from_dotenv_or_env(&values, "R2_ACCOUNT_ID")
                .map(|account_id| format!("https://{account_id}.r2.cloudflarestorage.com"))
        },
        Ok,
    )?;

    Ok(R2ObjectStorageConfig {
        bucket_name: required_value_from_dotenv_or_env(&values, "R2_BUCKET_NAME")?,
        endpoint,
        region: value_from_dotenv_or_env(&values, "R2_REGION")?
            .unwrap_or_else(|| "auto".to_owned()),
        access_key_id: required_value_from_dotenv_or_env(&values, "R2_ACCESS_KEY_ID")?,
        secret_access_key: required_value_from_dotenv_or_env(&values, "R2_SECRET_ACCESS_KEY")?,
    })
}

fn parse_dotenv(raw: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        values.insert(name.trim().to_owned(), unquote(value.trim()));
    }
    values
}

fn unquote(value: &str) -> String {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

fn value_from_dotenv_or_env(
    values: &HashMap<String, String>,
    name: &str,
) -> anyhow::Result<Option<String>> {
    if let Some(value) = values.get(name).filter(|value| !value.trim().is_empty()) {
        return Ok(Some(value.trim().to_owned()));
    }
    optional_env(name)
}

fn required_value_from_dotenv_or_env(
    values: &HashMap<String, String>,
    name: &str,
) -> anyhow::Result<String> {
    value_from_dotenv_or_env(values, name)?
        .with_context(|| format!("Missing required environment variable: {name}"))
}

#[cfg(test)]
mod tests {
    use super::{
        r2_config_from_dotenv_str, read_plan_from_str, select_copy_required_objects,
        selected_total_size_bytes,
    };

    const PLAN: &str = r#"{
        "schema_version": "foundation-platform.r2_bronze_key_migration_plan.v1",
        "status": "ready",
        "source_audit": "target/audit.json",
        "objects": [
            {
                "old_key": "bronze/source=molit-building-register/ingest_date=2026-05-18/run_id=018f0000-0000-7000-8000-000000000001/partition=operation=getBrTitleInfo/page=000001/part-000001.json",
                "new_key": "bronze/source=molit-building-register/run_id=018f0000-0000-7000-8000-000000000001/partition=operation=getBrTitleInfo/page=000001/part-000001.json",
                "size_bytes": 10,
                "status": "already_canonicalized"
            },
            {
                "old_key": "bronze/source=molit-building-register/ingest_date=2026-05-18/run_id=018f0000-0000-7000-8000-000000000002/partition=operation=getBrTitleInfo/page=000001/part-000001.json",
                "new_key": "bronze/source=molit-building-register/run_id=018f0000-0000-7000-8000-000000000002/partition=operation=getBrTitleInfo/page=000001/part-000001.json",
                "size_bytes": 20,
                "status": "copy_required"
            },
            {
                "old_key": "bronze/source=molit-building-register/ingest_date=2026-05-18/run_id=018f0000-0000-7000-8000-000000000003/partition=operation=getBrTitleInfo/page=000001/part-000001.json",
                "new_key": "bronze/source=molit-building-register/run_id=018f0000-0000-7000-8000-000000000003/partition=operation=getBrTitleInfo/page=000001/part-000001.json",
                "size_bytes": 30,
                "status": "copy_required"
            }
        ]
    }"#;

    #[test]
    fn selects_only_copy_required_objects_after_skip_and_limit() -> anyhow::Result<()> {
        let plan = read_plan_from_str(PLAN)?;
        let selected = select_copy_required_objects(&plan, 1, Some(1));

        assert_eq!(selected.len(), 1);
        assert!(selected[0].old_key.contains("000000000003"));
        assert_eq!(selected_total_size_bytes(&selected), 30);
        Ok(())
    }

    #[test]
    fn rejects_blocked_migration_plan() {
        let error = read_plan_from_str(
            r#"{
                "schema_version": "foundation-platform.r2_bronze_key_migration_plan.v1",
                "status": "blocked",
                "objects": []
            }"#,
        )
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("R2 Bronze key migration plan must be ready, got blocked")
        );
    }

    #[test]
    fn accepts_utf8_bom_from_generated_plan() -> anyhow::Result<()> {
        let plan = read_plan_from_str(&format!("\u{feff}{PLAN}"))?;

        assert_eq!(plan.objects.len(), 3);
        Ok(())
    }

    #[test]
    fn r2_config_from_dotenv_prefers_file_values_and_derives_account_endpoint() -> anyhow::Result<()>
    {
        let config = r2_config_from_dotenv_str(
            r#"
            R2_BUCKET_NAME=unit-test-bucket
            R2_ACCOUNT_ID=unit-test-account
            R2_ACCESS_KEY_ID="unit-test-access"
            R2_SECRET_ACCESS_KEY='unit-test-secret'
            "#,
        )?;

        assert_eq!(config.bucket_name, "unit-test-bucket");
        assert_eq!(
            config.endpoint,
            "https://unit-test-account.r2.cloudflarestorage.com"
        );
        assert_eq!(config.region, "auto");
        assert_eq!(config.access_key_id, "unit-test-access");
        assert_eq!(config.secret_access_key, "unit-test-secret");
        Ok(())
    }
}
