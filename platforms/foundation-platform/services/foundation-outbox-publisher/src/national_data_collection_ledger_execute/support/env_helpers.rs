use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};

use super::strip_utf8_bom;

pub(in crate::national_data_collection_ledger_execute) fn import_dotenv(
    path: &Path,
) -> anyhow::Result<BTreeMap<String, String>> {
    let mut values = env::vars().collect::<BTreeMap<_, _>>();
    if !path.is_file() {
        return Ok(values);
    }
    let bytes = fs::read(path)?;
    let raw = String::from_utf8_lossy(strip_utf8_bom(&bytes));
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((name, value)) = trimmed.split_once('=') else {
            bail!("Invalid .env line in {}: {line}", path.display());
        };
        let value = value.trim().trim_matches('"').trim_matches('\'').to_owned();
        values.insert(name.trim().to_owned(), value);
    }
    Ok(values)
}

pub(in crate::national_data_collection_ledger_execute) fn require_env(
    values: &BTreeMap<String, String>,
    name: &str,
) -> anyhow::Result<()> {
    if values
        .get(name)
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Ok(());
    }
    bail!("Missing required environment variable: {name}");
}

pub(in crate::national_data_collection_ledger_execute) fn require_r2_env(
    values: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    require_env(values, "R2_BUCKET_NAME")?;
    require_env(values, "R2_ACCESS_KEY_ID")?;
    require_env(values, "R2_SECRET_ACCESS_KEY")?;
    if values
        .get("R2_ENDPOINT")
        .is_none_or(|value| value.trim().is_empty())
        && values
            .get("R2_ACCOUNT_ID")
            .is_none_or(|value| value.trim().is_empty())
    {
        bail!("Missing required R2 addressing environment variable: R2_ENDPOINT or R2_ACCOUNT_ID");
    }
    Ok(())
}

pub(in crate::national_data_collection_ledger_execute) fn env_string(
    name: &str,
    default: &str,
) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value.trim().to_owned()),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

pub(in crate::national_data_collection_ledger_execute) fn env_bool(
    name: &str,
    default: bool,
) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            _ => bail!("invalid {name} environment variable"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

pub(in crate::national_data_collection_ledger_execute) fn env_usize(
    name: &str,
    default: usize,
) -> anyhow::Result<usize> {
    env_string(name, &default.to_string())?
        .parse::<usize>()
        .with_context(|| format!("invalid {name} environment variable"))
}

pub(in crate::national_data_collection_ledger_execute) fn env_u64(
    name: &str,
    default: u64,
) -> anyhow::Result<u64> {
    env_string(name, &default.to_string())?
        .parse::<u64>()
        .with_context(|| format!("invalid {name} environment variable"))
}

pub(in crate::national_data_collection_ledger_execute) fn env_i64(
    name: &str,
    default: i64,
) -> anyhow::Result<i64> {
    env_string(name, &default.to_string())?
        .parse::<i64>()
        .with_context(|| format!("invalid {name} environment variable"))
}

pub(in crate::national_data_collection_ledger_execute) fn optional_path_env(
    name: &str,
) -> anyhow::Result<Option<PathBuf>> {
    let value = env_string(name, "")?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(PathBuf::from(value)))
    }
}
