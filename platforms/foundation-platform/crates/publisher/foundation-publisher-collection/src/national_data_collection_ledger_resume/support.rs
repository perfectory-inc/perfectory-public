use std::{env, path::PathBuf};

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

pub(super) fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value.trim().to_owned()),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

pub(super) fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
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

pub(super) fn env_usize(name: &str, default: usize) -> anyhow::Result<usize> {
    env_string(name, &default.to_string())?
        .parse::<usize>()
        .with_context(|| format!("invalid {name} environment variable"))
}

pub(super) fn env_u64(name: &str, default: u64) -> anyhow::Result<u64> {
    env_string(name, &default.to_string())?
        .parse::<u64>()
        .with_context(|| format!("invalid {name} environment variable"))
}

pub(super) fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
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

pub(super) fn string_property(value: &JsonValue, name: &str) -> String {
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

pub(super) fn string_property_default(value: &JsonValue, name: &str, default: &str) -> String {
    let text = string_property(value, name);
    if text.trim().is_empty() {
        default.to_owned()
    } else {
        text
    }
}

pub(super) fn i64_property(value: &JsonValue, name: &str, default: i64) -> i64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_i64(),
            JsonValue::String(text) => text.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

pub(super) fn u64_property(value: &JsonValue, name: &str, default: u64) -> u64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_u64(),
            JsonValue::String(text) => text.trim().parse::<u64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

pub(super) fn u32_property(value: &JsonValue, name: &str, default: u32) -> u32 {
    u64_property(value, name, u64::from(default))
        .try_into()
        .unwrap_or(default)
}

pub(super) fn f64_property(value: &JsonValue, name: &str, default: f64) -> f64 {
    value
        .get(name)
        .and_then(|property| match property {
            JsonValue::Number(number) => number.as_f64(),
            JsonValue::String(text) => text.trim().parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}
