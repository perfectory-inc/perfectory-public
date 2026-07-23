use std::{fs, path::Path};

use anyhow::Context;
use serde::Serialize;

pub(super) const EXECUTION_SCHEMA_VERSION: &str =
    "foundation-platform.national_data_collection_ledger_execution.v1";

#[derive(Clone, Debug, Serialize)]
pub(super) struct AdaptiveExecutionEvidence {
    pub(super) enabled: bool,
    pub(super) start_in_flight: usize,
    pub(super) final_in_flight: usize,
    pub(super) max_in_flight: usize,
    pub(super) window_count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct EvidenceCircuitBreaker {
    pub(super) failure_threshold: u32,
    pub(super) open_seconds: u64,
}

#[derive(Serialize)]
pub(super) struct ExecutionEvidence {
    pub(super) schema_version: &'static str,
    pub(super) generated_at_utc: String,
    pub(super) git_head: String,
    pub(super) status: &'static str,
    pub(super) executed: bool,
    pub(super) execution_strategy: &'static str,
    pub(super) selected_job_count: usize,
    pub(super) skipped_job_count: usize,
    pub(super) deferred_due_to_lane_budget: u64,
    pub(super) ledger_read_mode: &'static str,
    pub(super) ledger_scanned_row_count: usize,
    pub(super) ledger_loaded_row_count: usize,
    pub(super) max_in_flight: usize,
    pub(super) circuit_breaker: EvidenceCircuitBreaker,
    pub(super) adaptive_in_flight: AdaptiveExecutionEvidence,
    pub(super) empty_job_count: u64,
    pub(super) reused_job_count: u64,
    pub(super) succeeded_job_count: u64,
    pub(super) failed_job_count: u64,
    pub(super) request_count_total: u64,
    pub(super) provider_request_count_total: u64,
    pub(super) raw_response_preserved: bool,
    pub(super) source_record_count: u64,
    pub(super) bronze_total_size_bytes: u64,
    pub(super) completion_claim_allowed: bool,
    pub(super) production_cutover_allowed: bool,
    pub(super) national_rollout_allowed: bool,
    pub(super) plan: EvidencePlan,
    pub(super) event_log: EvidenceEventLog,
    pub(super) evidence_limitations: Vec<&'static str>,
    pub(super) next_gates: Vec<&'static str>,
}

#[derive(Serialize)]
pub(super) struct EvidencePlan {
    pub(super) path: String,
    pub(super) compiler_input_hash_sha256: String,
}

#[derive(Serialize)]
pub(super) struct EvidenceEventLog {
    pub(super) path: String,
    pub(super) entry_count: u64,
}

pub(super) fn write_json_file<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(value).context("failed to serialize JSON report")?;
    fs::write(path, json).with_context(|| format!("failed to write {}", path.display()))
}
