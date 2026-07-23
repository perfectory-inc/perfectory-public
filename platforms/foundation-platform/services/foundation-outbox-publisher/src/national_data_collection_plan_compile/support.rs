use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::public_data_control_support::resolve_repo_path;

pub(super) fn env_resolved_manifest_path(
    root: &Path,
    value: &str,
    label: &str,
) -> anyhow::Result<PathBuf> {
    if value.trim().is_empty() {
        bail!("{label} is required");
    }
    resolve_repo_path(root, &PathBuf::from(value), label)
}

pub(super) fn require_file(path: &Path, message: &str) -> anyhow::Result<()> {
    if path.is_file() {
        Ok(())
    } else {
        bail!("{message}");
    }
}

pub(super) fn object_property<'a>(
    value: &'a JsonValue,
    name: &str,
) -> anyhow::Result<&'a JsonValue> {
    let object = value
        .get(name)
        .with_context(|| format!("{name} is required"))?;
    if object.is_object() {
        Ok(object)
    } else {
        bail!("{name} must be an object");
    }
}

pub(super) fn array_property(value: &JsonValue, name: &str) -> Vec<JsonValue> {
    value
        .get(name)
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default()
}

pub(super) fn string_property(value: &JsonValue, name: &str) -> String {
    value
        .get(name)
        .map(value_to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

pub(super) fn bool_property(value: &JsonValue, name: &str) -> Option<bool> {
    value.get(name).and_then(JsonValue::as_bool)
}

pub(super) fn u64_property(value: &JsonValue, name: &str) -> Option<u64> {
    value.get(name).and_then(json_value_to_u64)
}

pub(super) fn object<const N: usize>(items: [(&str, JsonValue); N]) -> JsonValue {
    JsonValue::Object(
        items
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<Map<_, _>>(),
    )
}

pub(super) fn str_value(value: impl AsRef<str>) -> JsonValue {
    JsonValue::String(value.as_ref().to_owned())
}

pub(super) fn u64_value(value: u64) -> JsonValue {
    JsonValue::Number(value.into())
}

pub(super) fn is_building_operation(value: &str) -> bool {
    value.starts_with("getBr") && value.chars().all(|c| c.is_ascii_alphanumeric())
}

pub(super) fn is_real_transaction_operation(value: &str) -> bool {
    value.starts_with("getRTMSDataSvc") && value.chars().all(|c| c.is_ascii_alphanumeric())
}

pub(super) fn valid_job_id(value: &str) -> bool {
    valid_building_job_id(value)
        || valid_two_code_job_id(value, "vworld-cadastral-")
        || valid_two_code_job_id(value, "vworld-land-register-")
        || valid_real_transaction_job_id(value)
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value.chars().all(|c| c.is_ascii_hexdigit())
        && value == value.to_ascii_lowercase()
}

pub(super) fn is_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.chars().all(|c| c.is_ascii_digit())
}

pub(super) fn sha256_file_hex(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(hex_lower(&Sha256::digest(bytes)))
}

pub(super) fn sha256_text(text: &str) -> String {
    hex_lower(&Sha256::digest(text.as_bytes()))
}

fn value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(raw) => raw.clone(),
        JsonValue::Number(raw) => raw.to_string(),
        JsonValue::Bool(raw) => raw.to_string(),
        JsonValue::Object(_) => "[object]".to_owned(),
        JsonValue::Array(_) => "[array]".to_owned(),
        JsonValue::Null => String::new(),
    }
}

fn json_value_to_u64(value: &JsonValue) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|raw| u64::try_from(raw).ok()))
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
}

fn valid_building_job_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("building-register-") else {
        return false;
    };
    let parts = rest.split('-').collect::<Vec<_>>();
    match parts.as_slice() {
        [sigungu, bjdong] => is_digits(sigungu, 5) && is_digits(bjdong, 5),
        [sigungu, bjdong, page, end] => {
            is_digits(sigungu, 5) && is_digits(bjdong, 5) && valid_page_suffix(page, end)
        }
        [operation, sigungu, bjdong] => {
            is_building_operation(operation) && is_digits(sigungu, 5) && is_digits(bjdong, 5)
        }
        [operation, sigungu, bjdong, page, end] => {
            is_building_operation(operation)
                && is_digits(sigungu, 5)
                && is_digits(bjdong, 5)
                && valid_page_suffix(page, end)
        }
        _ => false,
    }
}

fn valid_real_transaction_job_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("real-transaction-") else {
        return false;
    };
    let parts = rest.split('-').collect::<Vec<_>>();
    match parts.as_slice() {
        [operation, lawd_cd, deal_ymd] => {
            is_real_transaction_operation(operation)
                && is_digits(lawd_cd, 5)
                && is_digits(deal_ymd, 6)
        }
        [operation, lawd_cd, deal_ymd, page, end] => {
            is_real_transaction_operation(operation)
                && is_digits(lawd_cd, 5)
                && is_digits(deal_ymd, 6)
                && valid_page_suffix(page, end)
        }
        _ => false,
    }
}

fn valid_page_suffix(page: &str, end: &str) -> bool {
    page.len() == 7
        && page.starts_with('p')
        && page[1..].chars().all(|c| c.is_ascii_digit())
        && is_digits(end, 6)
}

fn valid_two_code_job_id(value: &str, prefix: &str) -> bool {
    let Some(rest) = value.strip_prefix(prefix) else {
        return false;
    };
    let mut parts = rest.split('-');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(sigungu), Some(bjdong), None) if is_digits(sigungu, 5) && is_digits(bjdong, 5)
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
