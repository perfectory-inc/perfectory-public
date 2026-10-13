use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const POLICY_SCHEMA_VERSION: &str = "foundation-platform.provider_rate_policy.v1";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProviderRatePolicyDocument {
    pub schema_version: String,
    pub status: String,
    pub owner: String,
    pub throughput_profile: String,
    pub rules: Vec<String>,
    pub lanes: Vec<LanePolicy>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LanePolicy {
    pub lane_id: String,
    pub provider: String,
    pub endpoint_slugs: Vec<String>,
    pub endpoint_groups: Vec<String>,
    pub budget: Option<LaneBudget>,
    pub rate_window: Option<RateWindow>,
    pub adaptive_control: Option<AdaptiveControl>,
    pub retry_policy: Option<RetryPolicy>,
    pub throttling_signals: Option<ThrottlingSignals>,
    pub job_disposition: Option<JobDisposition>,
    pub evidence_requirements: Option<EvidenceRequirements>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LaneBudget {
    pub daily_request_budget_env: String,
    pub request_budget_source: String,
    pub no_unbounded_budget: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RateWindow {
    pub min_rps: f64,
    pub start_rps: f64,
    pub max_rps: f64,
    pub min_in_flight: u32,
    pub start_in_flight: u32,
    pub max_in_flight: u32,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AdaptiveControl {
    pub algorithm: String,
    pub success_window_requests: u32,
    pub additive_rps_increment: f64,
    pub additive_in_flight_increment: u32,
    pub multiplicative_decrease_factor: f64,
    pub cooldown_seconds_after_throttle: i64,
    pub latency_p95_soft_ms: u32,
    pub latency_p95_hard_ms: u32,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u32,
    pub max_delay_ms: u32,
    pub jitter: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ThrottlingSignals {
    pub http_status_codes: Vec<u16>,
    pub provider_error_codes: Vec<String>,
    pub body_tokens: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct JobDisposition {
    pub on_throttle: String,
    pub on_quota_exhausted: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct EvidenceRequirements {
    pub write_window_decisions: bool,
    pub fields: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderOutcome {
    Success,
    Throttle,
    Timeout,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LaneState {
    pub lane_id: String,
    pub provider: String,
    pub current_rps: f64,
    pub current_in_flight: u32,
    pub accept_count: u64,
    pub throttle_count: u64,
    pub timeout_count: u64,
    pub success_count_since_adjustment: u32,
    pub p95_latency_ms: u32,
    pub cooldown_until_utc: String,
    pub decision: String,
    pub job_disposition: String,
}

pub fn new_lane_state(policy: &LanePolicy) -> anyhow::Result<LaneState> {
    let rate_window = policy
        .rate_window
        .as_ref()
        .with_context(|| format!("Lane policy rate_window is required: {}", policy.lane_id))?;
    Ok(LaneState {
        lane_id: policy.lane_id.clone(),
        provider: policy.provider.clone(),
        current_rps: rate_window.start_rps,
        current_in_flight: rate_window.start_in_flight,
        accept_count: 0,
        throttle_count: 0,
        timeout_count: 0,
        success_count_since_adjustment: 0,
        p95_latency_ms: 0,
        cooldown_until_utc: String::new(),
        decision: "initial".to_owned(),
        job_disposition: "run".to_owned(),
    })
}

pub fn update_lane_state(
    policy: &LanePolicy,
    state: &LaneState,
    outcome: ProviderOutcome,
    latency_ms: u32,
    observed_at_utc: DateTime<Utc>,
) -> anyhow::Result<LaneState> {
    let rate_window = policy
        .rate_window
        .as_ref()
        .with_context(|| format!("Lane policy rate_window is required: {}", policy.lane_id))?;
    let adaptive = policy.adaptive_control.as_ref().with_context(|| {
        format!(
            "Lane policy rate_window and adaptive_control are required: {}",
            policy.lane_id
        )
    })?;
    let disposition = policy.job_disposition.as_ref();

    let mut next = state.clone();
    next.p95_latency_ms = latency_ms;
    "hold".clone_into(&mut next.decision);
    "run".clone_into(&mut next.job_disposition);

    let latency_hard = latency_ms >= adaptive.latency_p95_hard_ms;
    let latency_soft = latency_ms >= adaptive.latency_p95_soft_ms;
    if matches!(
        outcome,
        ProviderOutcome::Throttle | ProviderOutcome::Timeout
    ) || latency_hard
    {
        if outcome == ProviderOutcome::Timeout {
            next.timeout_count += 1;
        } else {
            next.throttle_count += 1;
        }
        next.current_rps =
            (state.current_rps * adaptive.multiplicative_decrease_factor).max(rate_window.min_rps);
        next.current_in_flight = rate_window.min_in_flight.max(decreased_in_flight(
            state.current_in_flight,
            adaptive.multiplicative_decrease_factor,
        )?);
        next.success_count_since_adjustment = 0;
        "decrease_and_defer".clone_into(&mut next.decision);
        disposition
            .map(|value| value.on_throttle.as_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("defer_without_drop")
            .clone_into(&mut next.job_disposition);
        next.cooldown_until_utc = observed_at_utc
            .checked_add_signed(chrono::Duration::seconds(
                adaptive.cooldown_seconds_after_throttle,
            ))
            .ok_or_else(|| anyhow::anyhow!("provider rate cooldown timestamp overflow"))?
            .to_rfc3339();
    } else if outcome == ProviderOutcome::Success {
        next.accept_count += 1;
        let cooldown_active = if state.cooldown_until_utc.trim().is_empty() {
            false
        } else {
            DateTime::parse_from_rfc3339(&state.cooldown_until_utc)
                .is_ok_and(|timestamp| timestamp.with_timezone(&Utc) > observed_at_utc)
        };
        if cooldown_active {
            "hold_cooldown".clone_into(&mut next.decision);
        } else if latency_soft {
            "hold_latency".clone_into(&mut next.decision);
        } else {
            next.success_count_since_adjustment += 1;
            if next.success_count_since_adjustment >= adaptive.success_window_requests {
                next.current_rps =
                    (state.current_rps + adaptive.additive_rps_increment).min(rate_window.max_rps);
                next.current_in_flight = rate_window
                    .max_in_flight
                    .min(state.current_in_flight + adaptive.additive_in_flight_increment);
                next.success_count_since_adjustment = 0;
                "increase".clone_into(&mut next.decision);
            }
        }
    } else {
        "hold_error".clone_into(&mut next.decision);
    }

    if !next.current_rps.is_finite() {
        bail!("provider rate controller produced non-finite rps");
    }
    Ok(next)
}

pub fn is_throttle_signal(
    policy: &LanePolicy,
    http_status_code: u16,
    provider_error_code: &str,
    body_text: &str,
) -> bool {
    let Some(signals) = policy.throttling_signals.as_ref() else {
        return false;
    };
    if http_status_code > 0 && signals.http_status_codes.contains(&http_status_code) {
        return true;
    }
    if !provider_error_code.trim().is_empty()
        && signals
            .provider_error_codes
            .iter()
            .any(|code| code == provider_error_code)
    {
        return true;
    }
    signals
        .body_tokens
        .iter()
        .any(|token| !body_text.trim().is_empty() && body_text.contains(token))
}

fn decreased_in_flight(current: u32, factor: f64) -> anyhow::Result<u32> {
    if !factor.is_finite() || factor <= 0.0 {
        bail!("provider rate controller received invalid decrease factor");
    }
    let reduced = f64::from(current) * factor;
    if !reduced.is_finite() || reduced < 0.0 || reduced > f64::from(u32::MAX) {
        bail!("provider rate controller produced invalid in-flight value");
    }
    let mut low = 0_u32;
    let mut high = current;
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if f64::from(mid) <= reduced {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    Ok(low)
}
