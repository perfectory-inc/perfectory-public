use std::{fs, io::Write, path::Path};

use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{git_head, repo_relative_path};

use super::super::SCHEMA_VERSION;
use super::{string_prop, utc_now, Config, ExecutionStats, SelectedJobs};

pub(in crate::national_data_collection_ledger_execute) fn append_event_log_entry(
    path: &Path,
    event: &JsonValue,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let line = serde_json::to_vec(event)?;
    file.write_all(&line)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub(in crate::national_data_collection_ledger_execute) fn build_execution_evidence(
    config: &Config,
    plan: &JsonValue,
    status: &str,
    executed: bool,
    selected: &SelectedJobs,
    stats: &ExecutionStats,
) -> JsonValue {
    json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "git_head": git_head(&config.root),
        "status": status,
        "executed": executed,
        "selected_job_count": selected.jobs.len(),
        "skipped_job_count": selected.skipped_job_count,
        "ledger_read_mode": selected.read_mode,
        "ledger_scanned_row_count": selected.scanned_row_count,
        "ledger_loaded_row_count": selected.loaded_row_count,
        "provider_min_page_interval_ms": config.provider_min_page_interval_ms,
        "bronze_storage_driver": config.bronze_storage_driver.as_str(),
        "empty_job_count": stats.empty_job_count,
        "reused_job_count": stats.reused_job_count,
        "succeeded_job_count": stats.succeeded_job_count,
        "failed_job_count": stats.failed_job_count,
        "request_count_total": stats.request_count_total,
        "provider_request_count_total": stats.provider_request_count_total,
        "raw_response_preserved": stats.provider_request_count_total > 0 || stats.reused_job_count > 0,
        "source_record_count": stats.source_record_count,
        "bronze_total_size_bytes": stats.bronze_total_size_bytes,
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "national_rollout_allowed": false,
        "plan": {
            "path": repo_relative_path(&config.root, &config.plan_path),
            "compiler_input_hash_sha256": string_prop(plan, "compiler_input_hash_sha256")
        },
        "event_log": {
            "path": repo_relative_path(&config.root, &config.event_log_path),
            "entry_count": stats.event_count
        },
        "evidence_limitations": [
            "ledger_execution_slice_only",
            "does_not_promote_silver_gold_national_tables",
            "does_not_approve_production_cutover",
            "does_not_mark_national_rollout_complete"
        ],
        "next_gates": ["silver-gold-national-promotion"]
    })
}
