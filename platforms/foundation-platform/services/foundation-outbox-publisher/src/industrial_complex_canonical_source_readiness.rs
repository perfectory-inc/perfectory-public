use std::{
    env, fs,
    path::{Component, Path, PathBuf},
    process::Command,
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::PgPool;

use crate::public_data_control_support::{env_path, read_json, utc_now, write_json_file};

const REPORT_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_canonical_source_readiness.v1";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/industrial-complex-canonical-source-readiness.json";
const CANONICAL_INPUT_PATH: &str = "target/lakehouse/canonical-input/industrial_complexes.jsonl";
const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_SOURCE_READINESS";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let stats = read_stats(&config).await?;
    let report = readiness_report(&stats, config.minimum_source_rows);
    write_json_file(&config.output_path, &report)?;
    println!(
        "industrial-complex-canonical-source-readiness-ok status={} total={} source_codes={} placeholders={} report={}",
        report.status,
        report.total,
        report.source_official_complex_codes,
        report.placeholder_official_complex_codes,
        config.output_path.display()
    );
    Ok(())
}

struct Config {
    database_url: Option<String>,
    docker_path: Option<PathBuf>,
    docker_container_name: Option<String>,
    docker_user: String,
    docker_database: String,
    source_stats_json: Option<PathBuf>,
    output_path: PathBuf,
    minimum_source_rows: i64,
}

#[derive(Debug, Serialize)]
struct ReadinessReport {
    schema_version: &'static str,
    generated_at_utc: String,
    status: &'static str,
    total: i64,
    source_official_complex_codes: i64,
    placeholder_official_complex_codes: i64,
    blank_official_complex_codes: i64,
    fixture_or_test_rows: i64,
    offending_examples: Vec<JsonValue>,
    minimum_source_rows: i64,
    canonical_input_path: &'static str,
    handoff_command: String,
    blockers: Vec<Blocker>,
}

#[derive(Debug, Serialize)]
struct Blocker {
    id: &'static str,
    message: &'static str,
    actual_count: i64,
    required_count: i64,
}

#[derive(Debug)]
struct SourceStats {
    total: i64,
    source_official_complex_codes: i64,
    placeholder_official_complex_codes: i64,
    blank_official_complex_codes: i64,
    fixture_or_test_rows: i64,
    offending_examples: Vec<JsonValue>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path(&format!("{PREFIX}_ROOT"), ".")?;
        let root = normalize_windows_verbatim_path(normalize_path(
            &fs::canonicalize(&root)
                .with_context(|| format!("Root does not exist: {}", root.display()))?,
        ));
        if !root.exists() {
            bail!("Root does not exist: {}", root.display());
        }
        let output_path = optional_env_path(&format!("{PREFIX}_OUTPUT_PATH"))?
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH));
        let minimum_source_rows = optional_env(&format!("{PREFIX}_MINIMUM_SOURCE_ROWS"))?
            .map(|value| parse_positive_i64(&value, "MinimumSourceRows"))
            .transpose()?
            .unwrap_or(1);

        Ok(Self {
            source_stats_json: optional_env_path(&format!("{PREFIX}_SOURCE_STATS_JSON"))?
                .filter(|path| !path.as_os_str().is_empty())
                .map(|path| resolve_inside_root(&root, &path, "SourceStatsJson"))
                .transpose()?,
            output_path: resolve_inside_root(&root, &output_path, "OutputPath")?,
            database_url: optional_env(&format!("{PREFIX}_DATABASE_URL"))?,
            docker_path: optional_env_path(&format!("{PREFIX}_DOCKER_PATH"))?
                .filter(|path| !path.as_os_str().is_empty()),
            docker_container_name: optional_env(&format!("{PREFIX}_DOCKER_CONTAINER_NAME"))?,
            docker_user: optional_env(&format!("{PREFIX}_DOCKER_USER"))?
                .unwrap_or_else(|| "foundation_platform".to_owned()),
            docker_database: optional_env(&format!("{PREFIX}_DOCKER_DATABASE"))?
                .unwrap_or_else(|| "foundation_platform".to_owned()),
            minimum_source_rows,
        })
    }
}

async fn read_stats(config: &Config) -> anyhow::Result<SourceStats> {
    let value = if let Some(path) = &config.source_stats_json {
        if !path.is_file() {
            bail!("SourceStatsJson does not exist");
        }
        read_json(path, "canonical source stats JSON")?
    } else if config.docker_container_name.is_some() {
        serde_json::from_str(&read_stats_from_docker(config)?)
            .context("failed to parse docker canonical source stats JSON")?
    } else {
        read_stats_from_database(config).await?
    };
    source_stats_from_value(&value)
}

async fn read_stats_from_database(config: &Config) -> anyhow::Result<JsonValue> {
    let database_url = config
        .database_url
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("DatabaseUrl must not be blank when DockerContainerName is not provided")?;
    let pool = PgPool::connect(database_url)
        .await
        .context("failed to connect for canonical source readiness query")?;
    let value = sqlx::query_scalar::<_, JsonValue>(stats_query())
        .fetch_one(&pool)
        .await
        .context("canonical source readiness query failed")?;
    Ok(value)
}

fn read_stats_from_docker(config: &Config) -> anyhow::Result<String> {
    let container = config
        .docker_container_name
        .as_deref()
        .context("DockerContainerName must use a safe docker name")?;
    if !is_safe_docker_name(container) {
        bail!("DockerContainerName must use a safe docker name");
    }
    if !is_sql_identifier(&config.docker_user) || !is_sql_identifier(&config.docker_database) {
        bail!("DockerUser and DockerDatabase must be SQL identifiers");
    }
    let docker = config
        .docker_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("docker"));
    let output = Command::new(docker)
        .args([
            "exec",
            container,
            "psql",
            "-U",
            &config.docker_user,
            "-d",
            &config.docker_database,
            "-X",
            "-q",
            "-t",
            "-A",
            "-c",
            stats_query(),
        ])
        .output()
        .context("docker is required for Docker canonical source readiness query")?;
    if !output.status.success() {
        bail!(
            "docker psql canonical source readiness query failed with exit code {}. Output: {}{}",
            output.status.code().unwrap_or(1),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn source_stats_from_value(value: &JsonValue) -> anyhow::Result<SourceStats> {
    Ok(SourceStats {
        total: int_field(value, "total"),
        source_official_complex_codes: int_field(value, "source_official_complex_codes"),
        placeholder_official_complex_codes: int_field(value, "placeholder_official_complex_codes"),
        blank_official_complex_codes: int_field(value, "blank_official_complex_codes"),
        fixture_or_test_rows: int_field(value, "fixture_or_test_rows"),
        offending_examples: value
            .get("offending_examples")
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default(),
    })
}

fn readiness_report(stats: &SourceStats, minimum_source_rows: i64) -> ReadinessReport {
    let blockers = blockers(stats, minimum_source_rows);
    ReadinessReport {
        schema_version: REPORT_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        status: if blockers.is_empty() { "ready" } else { "blocked" },
        total: stats.total,
        source_official_complex_codes: stats.source_official_complex_codes,
        placeholder_official_complex_codes: stats.placeholder_official_complex_codes,
        blank_official_complex_codes: stats.blank_official_complex_codes,
        fixture_or_test_rows: stats.fixture_or_test_rows,
        offending_examples: stats.offending_examples.clone(),
        minimum_source_rows,
        canonical_input_path: CANONICAL_INPUT_PATH,
        handoff_command: format!(
            "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_SILVER_HANDOFF_PATH={CANONICAL_INPUT_PATH} foundation-outbox-publisher export-industrial-complex-silver-handoff"
        ),
        blockers,
    }
}

fn blockers(stats: &SourceStats, minimum_source_rows: i64) -> Vec<Blocker> {
    let mut blockers = Vec::new();
    if stats.source_official_complex_codes < minimum_source_rows {
        blockers.push(Blocker {
            id: "missing_source_official_complex_codes",
            message: "source-side official_complex_code rows are required before canonical handoff export",
            actual_count: stats.source_official_complex_codes,
            required_count: minimum_source_rows,
        });
    }
    if stats.placeholder_official_complex_codes > 0 {
        blockers.push(Blocker {
            id: "placeholder_official_complex_codes",
            message:
                "foundation-platform placeholder official_complex_code values block canonical handoff export",
            actual_count: stats.placeholder_official_complex_codes,
            required_count: 0,
        });
    }
    if stats.blank_official_complex_codes > 0 {
        blockers.push(Blocker {
            id: "blank_official_complex_codes",
            message: "blank official_complex_code values block canonical handoff export",
            actual_count: stats.blank_official_complex_codes,
            required_count: 0,
        });
    }
    if stats.fixture_or_test_rows > 0 {
        blockers.push(Blocker {
            id: "fixture_or_test_rows",
            message:
                "fixture/test industrial-complex rows cannot back canonical cutover source data",
            actual_count: stats.fixture_or_test_rows,
            required_count: 0,
        });
    }
    if stats.total < minimum_source_rows {
        blockers.push(Blocker {
            id: "insufficient_catalog_rows",
            message:
                "catalog.industrial_complex must contain enough rows for the approved canonical cutover",
            actual_count: stats.total,
            required_count: minimum_source_rows,
        });
    }
    blockers
}

fn stats_query() -> &'static str {
    r"
WITH classified AS (
  SELECT
    id,
    official_complex_code,
    name,
    primary_bjdong_code,
    btrim(official_complex_code) = '' AS is_blank,
    official_complex_code LIKE 'foundation-platform:%' AS is_placeholder,
    name ILIKE '%fixture%'
      OR name ILIKE '%test%'
      OR name LIKE '%테스트%' AS is_fixture_or_test
  FROM catalog.industrial_complex
),
stats AS (
  SELECT
    count(*) AS total,
    count(*) FILTER (
      WHERE NOT is_blank
        AND NOT is_placeholder
    ) AS source_official_complex_codes,
    count(*) FILTER (WHERE is_placeholder) AS placeholder_official_complex_codes,
    count(*) FILTER (WHERE is_blank) AS blank_official_complex_codes,
    count(*) FILTER (WHERE is_fixture_or_test) AS fixture_or_test_rows
  FROM classified
),
examples AS (
  SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) AS offending_examples
  FROM (
    SELECT id, official_complex_code, name, primary_bjdong_code
    FROM classified
    WHERE is_blank OR is_placeholder OR is_fixture_or_test
    ORDER BY name, id
    LIMIT 5
  ) AS t
)
SELECT json_build_object(
  'total', stats.total,
  'source_official_complex_codes', stats.source_official_complex_codes,
  'placeholder_official_complex_codes', stats.placeholder_official_complex_codes,
  'blank_official_complex_codes', stats.blank_official_complex_codes,
  'fixture_or_test_rows', stats.fixture_or_test_rows,
  'offending_examples', examples.offending_examples
)
FROM stats, examples;
"
}

fn int_field(value: &JsonValue, field: &str) -> i64 {
    value.get(field).and_then(JsonValue::as_i64).unwrap_or(0)
}

fn parse_positive_i64(raw: &str, field: &str) -> anyhow::Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("{field} must be an integer"))?;
    if value < 1 {
        bail!("{field} must be positive");
    }
    Ok(value)
}

fn is_safe_docker_name(value: &str) -> bool {
    !value.trim().is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_sql_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

fn resolve_inside_root(root: &Path, path: &Path, field: &str) -> anyhow::Result<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let resolved = normalize_path(&candidate);
    if !path_within_root(root, &resolved) {
        bail!("{field} must stay within Root");
    }
    Ok(resolved)
}

fn path_within_root(root: &Path, path: &Path) -> bool {
    let root = comparable_path(root);
    let path = comparable_path(path);
    path == root || path.starts_with(&format!("{root}\\"))
}

fn comparable_path(path: &Path) -> String {
    let mut value = normalize_path(path).to_string_lossy().replace('/', "\\");
    while value.ends_with('\\') {
        value.pop();
    }
    if cfg!(windows) {
        value.to_ascii_lowercase()
    } else {
        value
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
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

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn optional_env_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    optional_env(name).map(|value| value.map(PathBuf::from))
}
