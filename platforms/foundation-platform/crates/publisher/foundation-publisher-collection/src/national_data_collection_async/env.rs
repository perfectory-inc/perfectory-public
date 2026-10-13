use std::path::PathBuf;

use anyhow::{bail, Context};

// Generic env readers are shared crate-wide; re-export them so this lane keeps its short call names
// while funneling through the single canonical trimming/blank/error implementation.
pub(super) use crate::public_data_control_support::{
    optional_env_value, optional_u32_env, optional_u64_env, required_env_value,
};

/// Lane-specific infallible path reader: unlike the shared `env_path`, a blank/unset value falls
/// back to `default` and never errors (the lane treats every path as recoverable).
pub(super) fn env_path(name: &str, default: &str) -> PathBuf {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

pub(super) fn optional_usize_env(name: &str) -> anyhow::Result<Option<usize>> {
    optional_env_value(name)?
        .map(|value| {
            value
                .parse::<usize>()
                .with_context(|| format!("{name} must be a positive integer"))
        })
        .transpose()
}

pub(super) fn optional_u8_env(name: &str) -> anyhow::Result<Option<u8>> {
    optional_env_value(name)?
        .map(|value| {
            value
                .parse::<u8>()
                .with_context(|| format!("{name} must be a positive integer"))
        })
        .transpose()
}

pub(super) fn require_flag(name: &str) -> anyhow::Result<()> {
    if optional_env_value(name)?.as_deref() == Some("1") {
        return Ok(());
    }
    bail!("{name}=1 is required")
}
