use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{bail, Context};
use serde_json::Value as JsonValue;

use crate::public_data_control_support::{read_json, repo_relative_path, resolve_repo_path};
use crate::public_provider_rate_policy::{
    is_throttle_signal, new_lane_state, LanePolicy, LaneState, ProviderOutcome,
    ProviderRatePolicyDocument, POLICY_SCHEMA_VERSION,
};

use super::config::ResumeConfig;
use super::support::*;
use super::types::{ProviderLaneMode, ProviderLaneSeedReport, ResumeChunkReport};
use super::SCHEMA_VERSION;

pub(super) fn load_provider_policy_state(
    config: &ResumeConfig,
) -> anyhow::Result<(
    Option<ProviderRatePolicyDocument>,
    BTreeMap<String, LanePolicy>,
    BTreeMap<String, LaneState>,
    Option<ProviderLaneSeedReport>,
)> {
    if config.provider_lane_mode == ProviderLaneMode::Off {
        return Ok((None, BTreeMap::new(), BTreeMap::new(), None));
    }
    if !config.provider_rate_policy_path.is_file() {
        bail!(
            "ProviderRatePolicyPath file missing: {}",
            repo_relative_path(&config.root, &config.provider_rate_policy_path)
        );
    }
    let policy: ProviderRatePolicyDocument = serde_json::from_value(read_json(
        &config.provider_rate_policy_path,
        "provider rate policy",
    )?)
    .context("failed to parse provider rate policy")?;
    if policy.schema_version != POLICY_SCHEMA_VERSION || policy.status != "ready" {
        bail!("provider rate policy must be ready");
    }
    if !matches!(
        policy.throughput_profile.as_str(),
        "proof_conservative" | "max_throughput_calibration"
    ) {
        bail!("provider rate policy throughput_profile must be proof_conservative or max_throughput_calibration");
    }
    if policy.lanes.is_empty() {
        bail!("provider rate policy must include at least one lane");
    }
    let mut lane_policies = BTreeMap::new();
    let mut lane_states = BTreeMap::new();
    for lane in &policy.lanes {
        validate_lane_policy(lane)?;
        lane_policies.insert(lane.lane_id.clone(), lane.clone());
        lane_states.insert(lane.lane_id.clone(), new_lane_state(lane)?);
    }
    let seed = if let Some(path) = &config.provider_lane_seed_report_path {
        Some(load_seed_report(
            config,
            path,
            &lane_policies,
            &mut lane_states,
        )?)
    } else {
        Some(ProviderLaneSeedReport {
            status: "none".to_owned(),
            path: String::new(),
            lane_count: 0,
        })
    };
    Ok((Some(policy), lane_policies, lane_states, seed))
}

pub(super) fn provider_lane_for_job<'a>(
    job: &JsonValue,
    policy: &'a ProviderRatePolicyDocument,
) -> anyhow::Result<&'a LanePolicy> {
    crate::provider_lane::find_lane(
        policy,
        &string_property(job, "provider"),
        &string_property(job, "endpoint"),
    )
    .with_context(|| {
        format!(
            "provider rate lane lookup failed for job={}",
            string_property(job, "job_id")
        )
    })
}

pub(super) fn provider_lane_outcome(
    lane_policy: &LanePolicy,
    chunk: &ResumeChunkReport,
) -> ProviderOutcome {
    if chunk.exit_code == Some(0) {
        return ProviderOutcome::Success;
    }
    let output_tail = chunk.output_tail.as_deref().unwrap_or_default();
    if is_throttle_signal(lane_policy, 0, "", output_tail) {
        ProviderOutcome::Throttle
    } else if output_tail.to_ascii_lowercase().contains("timeout")
        || output_tail.to_ascii_lowercase().contains("timed out")
    {
        ProviderOutcome::Timeout
    } else {
        ProviderOutcome::Error
    }
}

pub(super) fn chunk_provider_latency_ms(
    config: &ResumeConfig,
    chunk: &ResumeChunkReport,
) -> anyhow::Result<u32> {
    let Some(evidence) = chunk_execution_evidence(config, chunk)? else {
        return Ok(0);
    };
    for name in [
        "provider_latency_p95_ms",
        "provider_request_latency_p95_ms",
        "provider_request_duration_p95_ms",
        "dependency_latency_p95_ms",
        "p95_latency_ms",
    ] {
        let value = u32_property(&evidence, name, 0);
        if value > 0 {
            return Ok(value);
        }
    }
    for nested_name in [
        "provider_latency",
        "provider_request_metrics",
        "public_api_quota",
        "dependency_metrics",
    ] {
        let nested = evidence.get(nested_name).unwrap_or(&JsonValue::Null);
        for name in [
            "provider_latency_p95_ms",
            "provider_request_latency_p95_ms",
            "provider_request_duration_p95_ms",
            "dependency_latency_p95_ms",
            "p95_latency_ms",
        ] {
            let value = u32_property(nested, name, 0);
            if value > 0 {
                return Ok(value);
            }
        }
    }
    Ok(0)
}

pub(super) fn chunk_provider_request_count(
    config: &ResumeConfig,
    chunk: &ResumeChunkReport,
) -> anyhow::Result<u32> {
    let Some(evidence) = chunk_execution_evidence(config, chunk)? else {
        return Ok(1);
    };
    let provider_count = u32_property(&evidence, "provider_request_count_total", 0);
    if provider_count > 0 {
        return Ok(provider_count);
    }
    Ok(u32_property(&evidence, "request_count_total", 1).max(1))
}

pub(super) fn planned_chunk_count_by_lane(chunks: &[ResumeChunkReport]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for chunk in chunks {
        if let Some(lane_id) = &chunk.lane_id {
            *counts.entry(lane_id.clone()).or_insert(0) += 1;
        }
    }
    counts
}

pub(super) fn min_page_interval_ms(rps: f64) -> anyhow::Result<u32> {
    crate::provider_lane::min_page_interval_ms(rps)
}

fn validate_lane_policy(lane: &LanePolicy) -> anyhow::Result<()> {
    let rate = lane
        .rate_window
        .as_ref()
        .with_context(|| format!("provider rate policy lane is invalid: {}", lane.lane_id))?;
    if lane.lane_id.trim().is_empty()
        || lane.provider.trim().is_empty()
        || lane.endpoint_groups.is_empty()
        || rate.start_in_flight < 1
        || rate.max_in_flight < rate.start_in_flight
    {
        bail!("provider rate policy lane is invalid: {}", lane.lane_id);
    }
    Ok(())
}

fn load_seed_report(
    config: &ResumeConfig,
    path: &std::path::Path,
    lane_policies: &BTreeMap<String, LanePolicy>,
    lane_states: &mut BTreeMap<String, LaneState>,
) -> anyhow::Result<ProviderLaneSeedReport> {
    let seed = read_json(path, "provider lane seed report")?;
    if string_property(&seed, "schema_version") != SCHEMA_VERSION {
        bail!("ProviderLaneSeedReportPath must use resume report schema");
    }
    let mut seeded = 0usize;
    for lane in seed
        .get("provider_lanes")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
    {
        let lane_id = string_property(lane, "lane_id");
        let Some(policy) = lane_policies.get(&lane_id) else {
            continue;
        };
        let rate = policy
            .rate_window
            .as_ref()
            .with_context(|| format!("provider rate policy lane is invalid: {lane_id}"))?;
        let current_rps = f64_property(lane, "current_rps", 0.0);
        let current_in_flight = u32_property(lane, "current_in_flight", 0);
        if current_rps < rate.min_rps
            || current_rps > rate.max_rps
            || current_in_flight < rate.min_in_flight
            || current_in_flight > rate.max_in_flight
        {
            bail!("provider lane seed state outside policy bounds: {lane_id}");
        }
        lane_states.insert(
            lane_id.clone(),
            LaneState {
                lane_id: lane_id.clone(),
                provider: string_property(lane, "provider"),
                current_rps,
                current_in_flight,
                accept_count: u64_property(lane, "accept_count", 0),
                throttle_count: u64_property(lane, "throttle_count", 0),
                timeout_count: u64_property(lane, "timeout_count", 0),
                success_count_since_adjustment: u32_property(
                    lane,
                    "success_count_since_adjustment",
                    0,
                ),
                p95_latency_ms: u32_property(lane, "p95_latency_ms", 0),
                cooldown_until_utc: string_property(lane, "cooldown_until_utc"),
                decision: string_property_default(lane, "decision", "seeded"),
                job_disposition: string_property_default(lane, "job_disposition", "run"),
            },
        );
        seeded += 1;
    }
    if seeded < 1 {
        bail!("ProviderLaneSeedReportPath did not contain any matching provider lanes");
    }
    Ok(ProviderLaneSeedReport {
        status: "loaded".to_owned(),
        path: repo_relative_path(&config.root, path),
        lane_count: seeded,
    })
}

fn chunk_execution_evidence(
    config: &ResumeConfig,
    chunk: &ResumeChunkReport,
) -> anyhow::Result<Option<JsonValue>> {
    let path = resolve_repo_path(
        &config.root,
        &PathBuf::from(&chunk.evidence_path),
        "chunk.evidence_path",
    )?;
    if path.is_file() {
        Ok(Some(read_json(&path, "chunk execution evidence")?))
    } else {
        Ok(None)
    }
}
