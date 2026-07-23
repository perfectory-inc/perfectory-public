use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{Datelike, Months, NaiveDate};
use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::public_data_control_support::{env_path, resolve_repo_path};

pub(super) fn deal_ymd_range(start: &str, end: &str) -> anyhow::Result<Vec<String>> {
    if !valid_deal_ymd(start) || !valid_deal_ymd(end) {
        bail!("RealTransactionStartDealYmd and RealTransactionEndDealYmd must use YYYYMM");
    }
    let start_date = deal_ymd_to_date(start)?;
    let end_date = deal_ymd_to_date(end)?;
    if start_date > end_date {
        bail!(
            "RealTransactionStartDealYmd must be less than or equal to RealTransactionEndDealYmd"
        );
    }
    let mut months = Vec::new();
    let mut cursor = start_date;
    while cursor <= end_date {
        months.push(format!("{:04}{:02}", cursor.year(), cursor.month()));
        cursor = cursor
            .checked_add_months(Months::new(1))
            .context("failed to advance deal month")?;
    }
    Ok(months)
}

fn deal_ymd_to_date(value: &str) -> anyhow::Result<NaiveDate> {
    let year = value[..4].parse::<i32>()?;
    let month = value[4..].parse::<u32>()?;
    NaiveDate::from_ymd_opt(year, month, 1).context("invalid deal month")
}

fn valid_deal_ymd(value: &str) -> bool {
    is_digits(value, 6)
        && value[4..]
            .parse::<u32>()
            .is_ok_and(|month| (1..=12).contains(&month))
}

// endpoint_slug intentionally retains the legacy kebab form (ADR 0014 D6); NOT a Bronze source_slug.
pub(super) fn building_endpoint_slug(operation: &str) -> String {
    format!("data-go-kr-building-register-{operation}")
}

pub(super) fn building_job_id(operation: &str, sigungu: &str, bjdong: &str) -> String {
    if operation == super::DEFAULT_BUILDING_REGISTER_OPERATION {
        format!("building-register-{sigungu}-{bjdong}")
    } else {
        format!("building-register-{operation}-{sigungu}-{bjdong}")
    }
}

// endpoint_slug intentionally retains the legacy kebab form (ADR 0014 D6); NOT a Bronze source_slug.
pub(super) fn real_transaction_endpoint_slug(operation: &str) -> String {
    format!("data-go-kr-real-transaction-{operation}")
}

pub(super) fn required_pages_for_total_count(
    provider_total_count: u64,
    effective_page_size: u64,
) -> u64 {
    if provider_total_count < 1 {
        1
    } else {
        provider_total_count.div_ceil(effective_page_size)
    }
}

pub(super) fn validate_bbox(bbox: Option<&JsonValue>, index: usize) -> anyhow::Result<()> {
    let Some(bbox) = bbox else {
        bail!("scope row {index} bbox.min_x must be a decimal number");
    };
    let min_x = decimal_property(bbox, "min_x")
        .with_context(|| format!("scope row {index} bbox.min_x must be a decimal number"))?;
    let min_y = decimal_property(bbox, "min_y")
        .with_context(|| format!("scope row {index} bbox.min_y must be a decimal number"))?;
    let max_x = decimal_property(bbox, "max_x")
        .with_context(|| format!("scope row {index} bbox.max_x must be a decimal number"))?;
    let max_y = decimal_property(bbox, "max_y")
        .with_context(|| format!("scope row {index} bbox.max_y must be a decimal number"))?;
    if min_x >= max_x || min_y >= max_y {
        bail!("scope row {index} bbox min values must be lower than max values");
    }
    Ok(())
}

fn decimal_property(value: &JsonValue, name: &str) -> anyhow::Result<f64> {
    let raw = value
        .get(name)
        .and_then(JsonValue::as_str)
        .context("missing decimal")?;
    if !is_decimal(raw) {
        bail!("invalid decimal");
    }
    raw.parse::<f64>().context("invalid decimal")
}

fn is_decimal(value: &str) -> bool {
    let rest = value.strip_prefix('-').unwrap_or(value);
    let mut parts = rest.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    parts.next().is_none()
        && !whole.is_empty()
        && whole.chars().all(|c| c.is_ascii_digit())
        && fraction
            .map(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
            .unwrap_or(true)
}

pub(super) fn json_object<const N: usize>(items: [(&str, JsonValue); N]) -> JsonValue {
    JsonValue::Object(
        items
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<Map<_, _>>(),
    )
}

pub(super) fn job_request_count(job: &JsonValue) -> u64 {
    u64_property(job, "request_count_estimate").unwrap_or(0)
}

pub(super) fn value_to_required_string(value: Option<&JsonValue>) -> String {
    match value {
        Some(JsonValue::String(raw)) => raw.clone(),
        Some(JsonValue::Number(raw)) => raw.to_string(),
        Some(JsonValue::Object(_)) => "[object]".to_owned(),
        Some(JsonValue::Array(_)) => "[array]".to_owned(),
        Some(JsonValue::Bool(raw)) => raw.to_string(),
        _ => String::new(),
    }
}

pub(super) fn env_repo_path(
    root: &Path,
    name: &str,
    default: &str,
    label: &str,
) -> anyhow::Result<PathBuf> {
    resolve_repo_path(root, &env_path(name, default)?, label)
}

pub(super) fn optional_env_path(
    root: &Path,
    name: &str,
    label: &str,
) -> anyhow::Result<Option<PathBuf>> {
    let value = env_string(name, "")?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(resolve_repo_path(root, &PathBuf::from(value), label)?))
    }
}

pub(super) fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(raw) => Ok(raw.trim().to_owned()),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

pub(super) fn env_string_list(name: &str) -> anyhow::Result<Vec<String>> {
    Ok(env_string(name, "")?
        .split([',', ';'])
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect())
}

pub(super) fn env_u64(name: &str, default: u64) -> anyhow::Result<u64> {
    let value = env_string(name, "")?;
    if value.is_empty() {
        Ok(default)
    } else {
        value
            .parse::<u64>()
            .with_context(|| format!("{name} must be an integer"))
    }
}

pub(super) fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    let value = env_string(name, "")?;
    if value.is_empty() {
        return Ok(default);
    }
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("{name} must be a boolean"),
    }
}

pub(super) fn require_file(path: &Path, message: &str) -> anyhow::Result<()> {
    if path.is_file() {
        Ok(())
    } else {
        bail!("{message}: {}", path.display())
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
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

pub(super) fn bool_property(value: &JsonValue, name: &str) -> Option<bool> {
    value.get(name).and_then(JsonValue::as_bool)
}

pub(super) fn u64_property(value: &JsonValue, name: &str) -> Option<u64> {
    value.get(name).and_then(json_value_to_u64)
}

pub(super) fn i64_property(value: &JsonValue, name: &str) -> Option<i64> {
    value.get(name).and_then(JsonValue::as_i64)
}

pub(super) fn u64_pointer(value: &JsonValue, pointer: &str) -> Option<u64> {
    value.pointer(pointer).and_then(json_value_to_u64)
}

fn json_value_to_u64(value: &JsonValue) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|raw| u64::try_from(raw).ok()))
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value.chars().all(|c| c.is_ascii_hexdigit())
        && value == value.to_ascii_lowercase()
}

pub(super) fn is_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.chars().all(|c| c.is_ascii_digit())
}

pub(super) fn is_building_operation(value: &str) -> bool {
    value.starts_with("getBr") && value.chars().all(|c| c.is_ascii_alphanumeric())
}

pub(super) fn is_real_transaction_operation(value: &str) -> bool {
    value.starts_with("getRTMSDataSvc") && value.chars().all(|c| c.is_ascii_alphanumeric())
}

pub(super) fn valid_page_count_job_id(value: &str) -> bool {
    valid_building_page_count_job_id(value)
        || valid_two_code_job_id(value, "vworld-cadastral-")
        || valid_two_code_job_id(value, "vworld-land-register-")
}

fn valid_building_page_count_job_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("building-register-") else {
        return false;
    };
    let parts = rest.split('-').collect::<Vec<_>>();
    match parts.as_slice() {
        [sigungu, bjdong] => is_digits(sigungu, 5) && is_digits(bjdong, 5),
        [operation, sigungu, bjdong] => {
            is_building_operation(operation) && is_digits(sigungu, 5) && is_digits(bjdong, 5)
        }
        _ => false,
    }
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

pub(super) fn sha256_file_hex(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(hex_lower(&Sha256::digest(bytes)))
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{deal_ymd_range, required_pages_for_total_count, valid_page_count_job_id};

    #[test]
    fn deal_ymd_range_is_inclusive_and_month_safe() -> anyhow::Result<()> {
        assert_eq!(
            deal_ymd_range("202511", "202601")?,
            ["202511", "202512", "202601"]
        );
        assert!(deal_ymd_range("202513", "202601").is_err());
        Ok(())
    }

    #[test]
    fn page_count_job_id_accepts_only_unwindowed_provider_jobs() {
        assert!(valid_page_count_job_id("building-register-11110-10100"));
        assert!(valid_page_count_job_id(
            "building-register-getBrFlrOulnInfo-11110-10100"
        ));
        assert!(valid_page_count_job_id("vworld-cadastral-11110-10100"));
        assert!(!valid_page_count_job_id(
            "building-register-11110-10100-p000001-000010"
        ));
    }

    #[test]
    fn required_pages_uses_one_page_for_empty_provider_scope() {
        assert_eq!(required_pages_for_total_count(0, 100), 1);
        assert_eq!(required_pages_for_total_count(201, 100), 3);
    }
}
