use std::{fs, path::Path};

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

use super::super::LEDGER_ENTRY_SCHEMA_VERSION;
use super::{string_prop, strip_utf8_bom};

pub(in crate::national_data_collection_ledger_execute) fn read_ledger_jsonl(
    path: &Path,
) -> anyhow::Result<Vec<JsonValue>> {
    let rows = read_jsonl(path, "execution ledger")?;
    for row in &rows {
        if string_prop(row, "schema_version") != LEDGER_ENTRY_SCHEMA_VERSION {
            bail!("execution ledger entry schema mismatch");
        }
    }
    Ok(rows)
}

pub(super) fn read_jsonl(path: &Path, label: &str) -> anyhow::Result<Vec<JsonValue>> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))?;
    let raw = String::from_utf8_lossy(strip_utf8_bom(&bytes));
    let mut rows = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        rows.push(
            serde_json::from_str::<JsonValue>(line)
                .with_context(|| format!("{label} line {line_number} is not valid JSON"))?,
        );
    }
    Ok(rows)
}
