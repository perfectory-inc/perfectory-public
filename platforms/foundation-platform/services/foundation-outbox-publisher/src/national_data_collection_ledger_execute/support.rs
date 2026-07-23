use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::public_api_metric_writer;

use super::{EVENT_SCHEMA_VERSION, MODE, PLAN_SCHEMA_VERSION};

mod bronze_result;
mod config;
mod endpoint_catalog;
mod env_helpers;
mod evidence;
mod execution_validation;
mod job_environment;
mod job_outcome;
mod json_helpers;
mod ledger_jsonl;
mod ledger_selection;
mod provider_job;
mod reuse_manifest;
mod runner;

pub(super) use bronze_result::{parse_r2_run_summary, read_local_bronze_result};
pub(super) use config::{Config, StorageDriver};
pub(super) use endpoint_catalog::{load_endpoint_catalog, EndpointPolicy};
pub(super) use env_helpers::{
    env_bool, env_i64, env_string, env_u64, env_usize, import_dotenv, optional_path_env,
    require_env, require_r2_env,
};
pub(super) use evidence::{append_event_log_entry, build_execution_evidence};
pub(super) use execution_validation::validate_execution_inputs;
pub(super) use job_environment::set_job_environment;
pub(super) use job_outcome::{record_provider_empty_job, record_reused_job};
pub(super) use json_helpers::{
    bool_prop, string_at, string_prop, string_prop_default, u64_prop, value_at,
};
pub(super) use ledger_jsonl::read_ledger_jsonl;
pub(super) use ledger_selection::{
    read_planned_ledger_rows_for_job_ids, read_requested_job_ids, SelectedJobs,
};
pub(super) use provider_job::run_provider_job;
pub(super) use reuse_manifest::{validate_reuse_identity, ReuseEntry, ReuseIndex};
pub(super) use runner::Runner;

#[derive(Default)]
pub(super) struct ExecutionStats {
    pub(super) event_count: u64,
    pub(super) empty_job_count: u64,
    pub(super) reused_job_count: u64,
    pub(super) succeeded_job_count: u64,
    pub(super) failed_job_count: u64,
    pub(super) request_count_total: u64,
    pub(super) provider_request_count_total: u64,
    pub(super) source_record_count: u64,
    pub(super) bronze_total_size_bytes: u64,
}

pub(super) fn validate_plan(plan: &JsonValue) -> anyhow::Result<()> {
    if string_prop(plan, "schema_version") != PLAN_SCHEMA_VERSION
        || string_prop(plan, "status") != "ready"
    {
        bail!("national collection plan must be ready");
    }
    if string_prop(plan, "run_mode") != "national" {
        bail!("national collection plan run_mode must be national");
    }
    for flag in [
        "completion_claim_allowed",
        "production_cutover_allowed",
        "national_rollout_allowed",
    ] {
        if bool_prop(plan, flag, true) {
            bail!("national collection plan {flag} must be false");
        }
    }
    Ok(())
}

pub(super) fn is_provider_empty_job(job: &JsonValue) -> bool {
    string_prop(job, "provider_empty_reason") == "vworld_invalid_emd_code"
        && string_prop(job, "provider") == "VWorld"
        && string_prop(job, "endpoint") == "ingest-vworld-cadastral"
        && u64_prop(job, "provider_total_count", 0) == 0
}

pub(super) fn request_count(job: &JsonValue) -> u64 {
    u64_prop(job, "request_count_estimate", 0)
}

pub(super) fn base_event(
    job: &JsonValue,
    event_type: &str,
    status: &str,
    extra: JsonValue,
) -> JsonValue {
    let mut event = json!({
        "schema_version": EVENT_SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "compiler_input_hash_sha256": string_prop(job, "compiler_input_hash_sha256"),
        "request_fingerprint_schema_version": string_prop(job, "request_fingerprint_schema_version"),
        "request_fingerprint_sha256": string_prop(job, "request_fingerprint_sha256"),
        "collection_snapshot_id": string_prop(job, "collection_snapshot_id"),
        "job_id": string_prop(job, "job_id"),
        "idempotency_key": string_prop(job, "idempotency_key"),
        "scope_unit_id": string_prop(job, "scope_unit_id"),
        "shard_id": string_prop(job, "shard_id"),
        "provider": string_prop(job, "provider"),
        "endpoint": string_prop(job, "endpoint"),
        "event_type": event_type,
        "status": status
    });
    merge_object(&mut event, extra);
    event
}

fn merge_object(base: &mut JsonValue, extra: JsonValue) {
    let Some(base_map) = base.as_object_mut() else {
        return;
    };
    let Some(extra_map) = extra.as_object() else {
        return;
    };
    for (key, value) in extra_map {
        base_map.insert(key.clone(), value.clone());
    }
}

pub(super) fn write_quota_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    request_count: u64,
    outcome: &str,
) -> anyhow::Result<()> {
    let request_count = i64::try_from(request_count).context("request count exceeds i64")?;
    public_api_metric_writer::write_quota_metric(
        path,
        provider,
        endpoint,
        request_count,
        outcome,
        MODE,
    )
}

pub(super) fn write_dependency_metric(
    path: &Path,
    provider: &str,
    endpoint: &str,
    duration: Duration,
    outcome: &str,
) -> anyhow::Result<()> {
    public_api_metric_writer::write_dependency_metric_duration(
        path, provider, endpoint, duration, outcome, MODE, None,
    )
}

pub(super) fn safe_runner_error_message(output: &[String]) -> String {
    let mut message = output
        .iter()
        .filter(|line| !line.trim().is_empty())
        .rev()
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    message.reverse();
    let mut message = if message.is_empty() {
        "runner exited non-zero without output".to_owned()
    } else {
        message.join(" | ")
    };
    for token in [
        "serviceKey",
        "DATA_GO_KR_SERVICE_KEY",
        "VWORLD_API_KEY",
        "R2_SECRET_ACCESS_KEY",
        "R2_ACCESS_KEY_ID",
        "unit-test-key",
        "fake-vworld-key",
    ] {
        message = message.replace(token, "[redacted]");
    }
    if message.len() > 1000 {
        message.truncate(1000);
    }
    message
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    Ok(sha256_hex(&fs::read(path)?))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

pub(super) fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
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
