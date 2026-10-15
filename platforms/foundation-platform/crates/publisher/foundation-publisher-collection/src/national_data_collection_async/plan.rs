use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde_json::Value as JsonValue;

#[derive(Clone, Debug)]
pub(super) struct PlanMetadata {
    pub(super) compiler_input_hash: String,
    pub(super) ledger_path: PathBuf,
}

pub(super) fn read_plan(path: &Path) -> anyhow::Result<PlanMetadata> {
    let raw = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read national data collection plan {}",
            path.display()
        )
    })?;
    let value: JsonValue = serde_json::from_str(raw.trim_start_matches('\u{feff}'))
        .context("failed to parse plan JSON")?;
    let compiler_input_hash = required_string(&value, "/compiler_input_hash_sha256")?;
    let ledger_path = required_string(&value, "/execution_ledger/path")?;
    Ok(PlanMetadata {
        compiler_input_hash,
        ledger_path: PathBuf::from(ledger_path),
    })
}

fn required_string(value: &JsonValue, pointer: &str) -> anyhow::Result<String> {
    value
        .pointer(pointer)
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .with_context(|| format!("required JSON string missing: {pointer}"))
}
