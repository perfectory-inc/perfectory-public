use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{SecondsFormat, Utc};
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use serde::Serialize;
use serde_json::Value as JsonValue;

pub fn read_json(path: &Path, label: &str) -> anyhow::Result<JsonValue> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))?;
    serde_json::from_slice(strip_utf8_bom(&bytes))
        .with_context(|| format!("failed to parse {label} {}", path.display()))
}

pub fn write_json_file<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value).context("failed to serialize JSON report")?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

pub fn env_path(name: &str, default: &str) -> anyhow::Result<PathBuf> {
    let value = match env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw,
        Ok(_) | Err(env::VarError::NotPresent) => default.to_owned(),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    };
    Ok(PathBuf::from(value))
}

pub fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub fn canonical_path(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

pub fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

pub fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
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

pub fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

pub fn r2_config_from_env_file(path: &Path) -> anyhow::Result<R2ObjectStorageConfig> {
    if !path.is_file() {
        bail!("Env file not found: {}", path.display());
    }
    let values = read_dotenv(path)?;
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

pub fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
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

fn read_dotenv(path: &Path) -> anyhow::Result<HashMap<String, String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read env file {}", path.display()))?;
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
    Ok(values)
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

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}
