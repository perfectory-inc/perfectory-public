use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};

pub(super) fn import_dotenv(path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    if !path.is_file() {
        return Ok(values);
    }
    for raw_line in fs::read_to_string(path)?.lines() {
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

pub(super) fn require_env(dotenv: &BTreeMap<String, String>, name: &str) -> anyhow::Result<()> {
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

pub(super) fn resolve_cargo(explicit: &str) -> anyhow::Result<PathBuf> {
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

pub(super) fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        // Present-but-empty behaves like unset: PowerShell wrappers cannot delete env vars
        // portably ($env:X = "" removes on Windows but sets empty on Linux).
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default.to_owned()),
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

pub(super) fn env_i64(name: &str, default: i64) -> anyhow::Result<i64> {
    env_string(name, &default.to_string())?
        .parse::<i64>()
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
