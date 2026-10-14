use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::public_provider_rate_policy::LaneState;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ProviderLaneMode {
    ProviderPolicy,
    Off,
}

pub(super) struct ResumeState {
    pub(super) succeeded_ids: std::collections::BTreeSet<String>,
    pub(super) compatible_prior_plan_succeeded_ids: std::collections::BTreeSet<String>,
}

pub(super) struct RunningChunk {
    pub(super) chunk_index: usize,
    pub(super) child: std::process::Child,
    pub(super) started_at: DateTime<Utc>,
}

pub(super) struct CompletedChunk {
    pub(super) chunk_index: usize,
    pub(super) result: ChunkExecResult,
}

pub(super) struct ChunkExecResult {
    pub(super) exit_code: i32,
    pub(super) output: Vec<String>,
    pub(super) started_at_utc: String,
    pub(super) finished_at_utc: String,
    pub(super) duration_ms: u64,
}

#[derive(Clone, Serialize)]
pub(super) struct ResumeReport {
    pub(super) schema_version: &'static str,
    pub(super) generated_at_utc: String,
    pub(super) git_head: String,
    pub(super) status: &'static str,
    pub(super) executed: bool,
    pub(super) plan: ResumePlanReport,
    pub(super) evidence: ResumeEvidenceReport,
    pub(super) coverage: ResumeCoverageReport,
    pub(super) execution_strategy: ExecutionStrategyReport,
    pub(super) chunking: ResumeChunkingReport,
    pub(super) chunks: Vec<ResumeChunkReport>,
    pub(super) provider_lanes: Vec<LaneState>,
    pub(super) provider_lane_decisions: Vec<ProviderLaneDecision>,
    pub(super) completion_claim_allowed: bool,
    pub(super) production_cutover_allowed: bool,
    pub(super) national_rollout_allowed: bool,
    pub(super) evidence_limitations: Vec<String>,
    pub(super) next_gates: Vec<String>,
}

#[derive(Clone, Serialize)]
pub(super) struct ResumePlanReport {
    pub(super) path: String,
    pub(super) compiler_input_hash_sha256: String,
    pub(super) ledger_path: String,
}

#[derive(Clone, Serialize)]
pub(super) struct ResumeEvidenceReport {
    pub(super) glob: String,
    pub(super) file_count: usize,
}

#[derive(Clone, Serialize)]
pub(super) struct ResumeCoverageReport {
    pub(super) planned_job_count: usize,
    pub(super) succeeded_job_count: usize,
    pub(super) compatible_prior_plan_succeeded_job_count: usize,
    pub(super) pending_job_count: usize,
}

#[derive(Clone, Serialize)]
pub(super) struct ExecutionStrategyReport {
    pub(super) mode: &'static str,
    pub(super) max_parallel_chunks: usize,
    pub(super) executor_isolation: &'static str,
    pub(super) provider_lane_mode: &'static str,
    pub(super) provider_rate_policy: Option<ProviderRatePolicyReport>,
    pub(super) provider_lane_seed: Option<ProviderLaneSeedReport>,
}

#[derive(Clone, Serialize)]
pub(super) struct ProviderRatePolicyReport {
    pub(super) path: String,
    pub(super) schema_version: String,
    pub(super) status: String,
    pub(super) throughput_profile: String,
}

#[derive(Clone, Serialize)]
pub(super) struct ProviderLaneSeedReport {
    pub(super) status: String,
    pub(super) path: String,
    pub(super) lane_count: usize,
}

#[derive(Clone, Serialize)]
pub(super) struct ResumeChunkingReport {
    pub(super) chunk_size: usize,
    pub(super) max_chunks: usize,
    pub(super) max_parallel_chunks: usize,
    pub(super) planned_chunks: usize,
    pub(super) executed_chunks: usize,
    pub(super) succeeded_chunks: usize,
    pub(super) failed_chunks: usize,
}

#[derive(Clone, Serialize)]
pub(super) struct ResumeChunkReport {
    pub(super) chunk_id: String,
    pub(super) job_count: usize,
    pub(super) job_ids_path: String,
    pub(super) event_log_path: String,
    pub(super) evidence_path: String,
    pub(super) status: String,
    pub(super) exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) provider: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) endpoint_groups: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_max_parallel_chunks: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_start_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_max_rps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_min_page_interval_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_start_page_interval_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_current_rps_at_start: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_current_in_flight_at_start: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_effective_parallel_chunks_at_start: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) lane_effective_rps_per_chunk_at_start: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) provider_min_page_interval_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) started_at_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) finished_at_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_tail: Option<String>,
}

#[derive(Clone, Serialize)]
pub(super) struct ProviderLaneDecision {
    pub(super) chunk_id: String,
    pub(super) lane_id: String,
    pub(super) provider: String,
    pub(super) outcome: &'static str,
    pub(super) latency_ms: u32,
    pub(super) provider_request_count: u32,
    pub(super) before_current_rps: f64,
    pub(super) after_current_rps: f64,
    pub(super) before_current_in_flight: u32,
    pub(super) after_current_in_flight: u32,
    pub(super) decision: String,
    pub(super) job_disposition: String,
    pub(super) observed_at_utc: String,
}
