use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context};
use reqwest::Url;
use serde::Serialize;

use crate::r2_command_support::{optional_env, utc_now, write_json_file};

const PREFIX: &str = "FOUNDATION_PLATFORM_R2_BILLING_EXPORT_COLLECT";
const PLAN_SCHEMA_VERSION: &str = "foundation-platform.r2_billing_export_collection_plan.v1";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if config.execute && !config.confirm_billing_export_collection {
        bail!("Refusing R2 billing export collection without -ConfirmBillingExportCollection");
    }

    let plan = CollectionPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        export_url_redacted: redacted_url(&config.export_url),
        output_path: absolute_path_text(&config.output_path),
        auth_token_env: config.auth_token_env.clone().unwrap_or_default(),
        execute: config.execute,
    };
    if let Some(path) = &config.plan_output_path {
        write_json_file(path, &plan)?;
    }

    if !config.execute {
        write_dry_run_summary(&plan.export_url_redacted)?;
        return Ok(());
    }

    let bytes = download_export(&config).await?;
    write_atomic(&config.output_path, &bytes)?;
    write_collection_summary(bytes.len(), &config.output_path)?;
    Ok(())
}

struct Config {
    export_url: Url,
    output_path: PathBuf,
    plan_output_path: Option<PathBuf>,
    auth_token_env: Option<String>,
    timeout_seconds: u64,
    execute: bool,
    confirm_billing_export_collection: bool,
}

#[derive(Serialize)]
struct CollectionPlan {
    schema_version: &'static str,
    generated_at_utc: String,
    export_url_redacted: String,
    output_path: String,
    auth_token_env: String,
    execute: bool,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let export_url = parse_export_url(&required_env("EXPORT_URL")?)?;
        let output_path = required_path("OUTPUT_PATH")?;
        let plan_output_path = optional_env(&key("PLAN_OUTPUT_PATH"))?
            .map(|raw| resolve_path(&raw, "PlanOutputPath"))
            .transpose()?;
        let timeout_seconds = optional_env(&key("TIMEOUT_SECONDS"))?
            .map(|raw| parse_positive_u64(&raw, "TimeoutSeconds"))
            .transpose()?
            .unwrap_or(60);
        Ok(Self {
            export_url,
            output_path,
            plan_output_path,
            auth_token_env: optional_env(&key("AUTH_TOKEN_ENV"))?,
            timeout_seconds,
            execute: env_bool("EXECUTE", false)?,
            confirm_billing_export_collection: env_bool(
                "CONFIRM_BILLING_EXPORT_COLLECTION",
                false,
            )?,
        })
    }
}

async fn download_export(config: &Config) -> anyhow::Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_seconds))
        .build()
        .context("failed to build R2 billing export HTTP client")?;
    let mut request = client.get(config.export_url.clone());
    if let Some(auth_token_env) = &config.auth_token_env {
        let token = env::var(auth_token_env).with_context(|| {
            format!("Auth token environment variable is empty: {auth_token_env}")
        })?;
        if token.trim().is_empty() {
            bail!("Auth token environment variable is empty: {auth_token_env}");
        }
        request = request.bearer_auth(token);
    }

    let response = request
        .send()
        .await
        .context("failed to download R2 billing export")?;
    let status = response.status();
    if !status.is_success() {
        bail!(
            "R2 billing export collection failed with status {} from {}",
            status.as_u16(),
            redacted_url(&config.export_url)
        );
    }
    let bytes = response
        .bytes()
        .await
        .context("failed to read R2 billing export response body")?;
    Ok(bytes.to_vec())
}

fn parse_export_url(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).context("ExportUrl must be an absolute URI")?;
    if url.scheme() == "https" || (url.scheme() == "http" && is_loopback(&url)) {
        return Ok(url);
    }
    bail!("ExportUrl must use https, except loopback development URLs");
}

fn is_loopback(url: &Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
}

fn write_dry_run_summary(redacted_url: &str) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(
        stdout,
        "r2-billing-export-collect-plan-ok mode=dry-run url={redacted_url}"
    )?;
    Ok(())
}

fn write_collection_summary(byte_count: usize, output_path: &Path) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(
        stdout,
        "r2-billing-export-collect-ok bytes={} output={}",
        byte_count,
        output_path.display()
    )?;
    Ok(())
}

fn redacted_url(url: &Url) -> String {
    let mut redacted = url.clone();
    if redacted.query().is_some() {
        redacted.set_query(Some("query=redacted"));
    }
    redacted.to_string()
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create output directory {}", parent.display())
            })?;
        }
    }
    let staged_path = PathBuf::from(format!("{}.writing", path.to_string_lossy()));
    if staged_path.exists() {
        fs::remove_file(&staged_path).with_context(|| {
            format!(
                "failed to remove stale staged write file {}",
                staged_path.display()
            )
        })?;
    }
    fs::write(&staged_path, bytes)
        .with_context(|| format!("failed to write staged file {}", staged_path.display()))?;
    replace_file(&staged_path, path)
        .with_context(|| format!("failed to move staged file into {}", path.display()))
}

fn replace_file(staged_path: &Path, path: &Path) -> anyhow::Result<()> {
    match fs::rename(staged_path, path) {
        Ok(()) => Ok(()),
        Err(_) if path.exists() => {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove existing file {}", path.display()))?;
            fs::rename(staged_path, path).with_context(|| {
                format!(
                    "failed to move staged file {} into {} after removing existing file",
                    staged_path.display(),
                    path.display()
                )
            })
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to move staged file {} into {}",
                staged_path.display(),
                path.display()
            )
        }),
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(&key(name))?.map_or_else(|| bail!("{name} must not be blank"), Ok)
}

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    resolve_path(&required_env(name)?, name)
}

fn resolve_path(raw: &str, label: &str) -> anyhow::Result<PathBuf> {
    if raw.trim().is_empty() {
        bail!("{label} must not be blank");
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(env::current_dir()
        .context("failed to resolve current directory")?
        .join(path))
}

fn absolute_path_text(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn parse_positive_u64(raw: &str, label: &str) -> anyhow::Result<u64> {
    let value = raw
        .parse::<u64>()
        .with_context(|| format!("{label} must be a positive integer"))?;
    if value == 0 {
        bail!("{label} must be a positive integer");
    }
    Ok(value)
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    let Some(raw) = optional_env(&key(name))? else {
        return Ok(default);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("{name} must be a boolean"),
    }
}

fn key(name: &str) -> String {
    format!("{PREFIX}_{name}")
}
