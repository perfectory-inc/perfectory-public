use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context};
use foundation_outbox::LakehouseLineagePublisher;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::public_data_control_support::{env_path, read_json, utc_now, write_json_file};

const PLAN_SCHEMA_VERSION: &str = "foundation-platform.lineage_publish_plan.v1";
const PREFIX: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_LINEAGE_PUBLISH";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if config.execute && !config.confirm_lineage_network_emit {
        bail!("Refusing lineage network emit without -ConfirmLineageNetworkEmit");
    }

    let event = read_json(&config.input_path, "lakehouse lineage event")?;
    LakehouseLineagePublisher::validate_event(&event).map_err(anyhow::Error::new)?;
    let payload = serde_json::to_vec(&event).context("failed to serialize lineage event")?;
    let plan = publish_plan(&config, &event, &payload)?;
    if let Some(path) = &config.plan_output_path {
        write_json_file(path, &plan)?;
    }

    if !config.execute {
        println!(
            "lineage-event-publish-plan-ok mode=dry-run job={} endpoint={}",
            event_string(&event, "job_name"),
            config.endpoint
        );
        return Ok(());
    }

    let mut builder = LakehouseLineagePublisher::builder()
        .endpoint(&config.endpoint)
        .map_err(anyhow::Error::new)?
        .timeout(Duration::from_secs(config.timeout_seconds));
    if let Some(token) = config.auth_token()? {
        builder = builder.auth_token(&token);
    }
    let publisher = builder.build().map_err(anyhow::Error::new)?;
    let status = publisher
        .publish(&event)
        .await
        .map_err(anyhow::Error::new)?;
    println!("lineage-event-publish-ok status={status}");
    Ok(())
}

struct Config {
    input_path: PathBuf,
    endpoint: String,
    plan_output_path: Option<PathBuf>,
    timeout_seconds: u64,
    auth_token_env: Option<String>,
    execute: bool,
    confirm_lineage_network_emit: bool,
}

#[derive(Debug, Serialize)]
struct PublishPlan {
    schema_version: &'static str,
    generated_at_utc: String,
    endpoint: String,
    execute: bool,
    payload_sha256: String,
    event: PublishPlanEvent,
}

#[derive(Debug, Serialize)]
struct PublishPlanEvent {
    schema_version: String,
    event_type: String,
    job_name: String,
    run_id: String,
    input_dataset: String,
    output_dataset: String,
    source_snapshot_ids: Vec<String>,
    openlineage_event_type: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = normalize_windows_verbatim_path(
            fs::canonicalize(&root)
                .with_context(|| format!("failed to resolve repo root {}", root.display()))?,
        );
        let input_path = env_path(&format!("{PREFIX}_INPUT_PATH"), "")?;
        if input_path.as_os_str().is_empty() {
            bail!("InputPath is required.");
        }
        let endpoint = required_env(&format!("{PREFIX}_ENDPOINT"))?;
        let timeout_seconds = optional_env(&format!("{PREFIX}_TIMEOUT_SECONDS"))?
            .map(|value| parse_timeout_seconds(&value))
            .transpose()?
            .unwrap_or(30);

        Ok(Self {
            input_path: resolve_path(&root, &input_path),
            endpoint,
            plan_output_path: optional_env_path(&format!("{PREFIX}_PLAN_OUTPUT_PATH"))?
                .map(|path| resolve_path(&root, &path)),
            timeout_seconds,
            auth_token_env: optional_env(&format!("{PREFIX}_AUTH_TOKEN_ENV"))?,
            execute: env_bool(&format!("{PREFIX}_EXECUTE"), false)?,
            confirm_lineage_network_emit: env_bool(
                &format!("{PREFIX}_CONFIRM_LINEAGE_NETWORK_EMIT"),
                false,
            )?,
        })
    }

    fn auth_token(&self) -> anyhow::Result<Option<String>> {
        let Some(name) = self.auth_token_env.as_deref() else {
            return Ok(None);
        };
        let value = env::var(name)
            .with_context(|| format!("Auth token environment variable is empty: {name}"))?;
        if value.trim().is_empty() {
            bail!("Auth token environment variable is empty: {name}");
        }
        Ok(Some(value))
    }
}

fn publish_plan(config: &Config, event: &JsonValue, payload: &[u8]) -> anyhow::Result<PublishPlan> {
    Ok(PublishPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        endpoint: config.endpoint.clone(),
        execute: config.execute,
        payload_sha256: sha256_hex(payload),
        event: PublishPlanEvent {
            schema_version: required_string(event, &["schema_version"])?,
            event_type: required_string(event, &["event_type"])?,
            job_name: required_string(event, &["job_name"])?,
            run_id: required_string(event, &["run_id"])?,
            input_dataset: required_string(event, &["input_dataset", "qualified_name"])?,
            output_dataset: required_string(event, &["output_dataset", "qualified_name"])?,
            source_snapshot_ids: string_array(event, "source_snapshot_ids")?,
            openlineage_event_type: required_string(event, &["openlineage_mapping", "event_type"])?,
        },
    })
}

fn required_string(value: &JsonValue, path: &[&str]) -> anyhow::Result<String> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor
            .get(*segment)
            .with_context(|| format!("lineage event missing required field: {}", path.join(".")))?;
    }
    cursor
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("lineage event missing required field: {}", path.join(".")))
}

fn string_array(value: &JsonValue, field: &str) -> anyhow::Result<Vec<String>> {
    let values = value
        .get(field)
        .and_then(JsonValue::as_array)
        .with_context(|| format!("lineage event {field} must be a non-empty array"))?;
    let mut result = Vec::with_capacity(values.len());
    for item in values {
        let Some(value) = item.as_str().filter(|value| !value.trim().is_empty()) else {
            bail!("lineage event {field} includes blank value");
        };
        result.push(value.to_owned());
    }
    if result.is_empty() {
        bail!("lineage event {field} must be a non-empty array");
    }
    Ok(result)
}

fn event_string(event: &JsonValue, field: &str) -> String {
    event
        .get(field)
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown")
        .to_owned()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn parse_timeout_seconds(raw: &str) -> anyhow::Result<u64> {
    let value = raw
        .parse::<u64>()
        .context("TimeoutSeconds must be an integer")?;
    if value == 0 {
        bail!("TimeoutSeconds must be positive.");
    }
    Ok(value)
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} is required"))
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

fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
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
