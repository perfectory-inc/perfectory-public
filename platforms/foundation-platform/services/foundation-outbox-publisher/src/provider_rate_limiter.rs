//! In-memory adaptive provider/lane rate limiter for the async national collector (Slice 4-A).

use std::collections::BTreeMap;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context};
use chrono::{DateTime, Duration, Utc};

use crate::provider_lane::min_page_interval_ms;
use crate::public_provider_rate_policy::{
    is_throttle_signal, new_lane_state, update_lane_state, LanePolicy, LaneState, ProviderOutcome,
    ProviderRatePolicyDocument, POLICY_SCHEMA_VERSION,
};

/// Neutral fetch-outcome signal the async adapter builds from a provider call result. Deliberately
/// independent of provider error types -- keeps the limiter core provider-agnostic.
pub(crate) struct OutcomeSignal {
    pub(crate) succeeded: bool,
    /// Error chain text when `!succeeded` (original case, for token matching); empty on success.
    pub(crate) message: String,
}

impl OutcomeSignal {
    pub(crate) fn success() -> Self {
        Self {
            succeeded: true,
            message: String::new(),
        }
    }

    pub(crate) fn failure(message: String) -> Self {
        Self {
            succeeded: false,
            message,
        }
    }
}

/// Maps a (lane policy, neutral signal) to a `ProviderOutcome`. Swappable seam: a future typed-error
/// classifier replaces only this impl, with no change to the limiter core (spec decision #3).
pub(crate) trait ProviderOutcomeClassifier: Send + Sync {
    fn classify(&self, policy: &LanePolicy, signal: &OutcomeSignal) -> ProviderOutcome;
}

/// Default classifier: reads the surfaced error message because a typed provider signal is not
/// available yet.
pub(crate) struct MessageThrottleClassifier;

impl ProviderOutcomeClassifier for MessageThrottleClassifier {
    fn classify(&self, policy: &LanePolicy, signal: &OutcomeSignal) -> ProviderOutcome {
        if signal.succeeded {
            return ProviderOutcome::Success;
        }
        if is_throttle_signal(policy, 0, "", &signal.message)
            || message_contains_throttle_status(policy, &signal.message)
        {
            return ProviderOutcome::Throttle;
        }
        let lowered = signal.message.to_ascii_lowercase();
        if lowered.contains("timeout") || lowered.contains("timed out") {
            return ProviderOutcome::Timeout;
        }
        ProviderOutcome::Error
    }
}

/// True when the message text contains any of the policy's throttle HTTP status codes (e.g. "429").
fn message_contains_throttle_status(policy: &LanePolicy, message: &str) -> bool {
    let Some(signals) = policy.throttling_signals.as_ref() else {
        return false;
    };
    signals
        .http_status_codes
        .iter()
        .any(|code| message.contains(&code.to_string()))
}

/// One step of the per-page pacing/cooldown decision. Pure (clock supplied), so it is unit-tested
/// directly; the async `acquire` loop is a thin wrapper that re-evaluates this after every sleep.
pub(crate) enum AcquireStep {
    /// Clear to fetch now; the caller must commit `next_start_utc` under the same lock it read.
    Proceed { next_start_utc: DateTime<Utc> },
    /// Must sleep until `until`, then re-decide (state may have changed during the sleep).
    Wait { until: DateTime<Utc> },
}

/// Decide whether a request may proceed now. Cooldown takes priority over pacing. Never touches budget.
fn decide_acquire(
    state: &LaneState,
    next_start_utc: DateTime<Utc>,
    now: DateTime<Utc>,
) -> anyhow::Result<AcquireStep> {
    if !state.cooldown_until_utc.trim().is_empty() {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(&state.cooldown_until_utc) {
            let cooldown_end = parsed.with_timezone(&Utc);
            if cooldown_end > now {
                return Ok(AcquireStep::Wait {
                    until: cooldown_end,
                });
            }
        }
    }
    let interval = Duration::milliseconds(i64::from(min_page_interval_ms(state.current_rps)?));
    let slot = next_start_utc.max(now);
    if slot > now {
        return Ok(AcquireStep::Wait { until: slot });
    }
    Ok(AcquireStep::Proceed {
        next_start_utc: now + interval,
    })
}

/// Per-lane in-memory PACING state (one run). Not persisted (Slice 4-A; cross-run seed is deferred).
/// Budget lives separately in a shared [`BudgetPool`] keyed by budget-env name, not here.
pub(crate) struct LaneRuntime {
    state: LaneState,
    /// Earliest UTC instant the next request to this lane may START (pacing gate).
    next_start_utc: DateTime<Utc>,
}

/// Shared daily-request budget pool. One pool per budget-env name; all lanes naming the same env
/// (e.g. data.go.kr's account-wide quota) charge the SAME pool. Only `reserve` mutates `used`.
pub(crate) struct BudgetPool {
    limit: u64,
    used: u64,
}

/// Outcome of a pre-pass budget reservation for one job.
pub(crate) enum ReserveOutcome {
    Granted,
    DeferredLaneBudget,
}

/// In-memory adaptive provider/lane rate limiter shared (via `Arc`) across all concurrent page tasks.
pub(crate) struct ProviderRateLimiter {
    policies: BTreeMap<String, LanePolicy>,
    pub(crate) lanes: Mutex<BTreeMap<String, LaneRuntime>>,
    /// lane_id -> budget-env key (shared pool); None = unbounded lane.
    lane_budget_key: BTreeMap<String, Option<String>>,
    /// budget-env key -> shared pool (lanes naming the same env share one).
    pub(crate) budgets: Mutex<BTreeMap<String, BudgetPool>>,
    classifier: Box<dyn ProviderOutcomeClassifier>,
}

impl ProviderRateLimiter {
    /// Build from a loaded policy + per-lane budget limits. Fails closed: a `no_unbounded_budget`
    /// lane with no budget limit is rejected.
    pub(crate) fn new(
        policy: ProviderRatePolicyDocument,
        budget_limits: BTreeMap<String, Option<u64>>,
        classifier: Box<dyn ProviderOutcomeClassifier>,
    ) -> anyhow::Result<Self> {
        if policy.lanes.is_empty() {
            bail!("provider rate policy must include at least one lane");
        }
        let mut policies = BTreeMap::new();
        let mut lanes = BTreeMap::new();
        let mut lane_budget_key = BTreeMap::new();
        let mut budgets: BTreeMap<String, BudgetPool> = BTreeMap::new();
        for lane in &policy.lanes {
            let state = new_lane_state(lane)?;
            let budget_env = lane.budget.as_ref().and_then(|budget| {
                let trimmed = budget.daily_request_budget_env.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            });
            let resolved_limit = budget_limits.get(&lane.lane_id).copied().flatten();
            let budget_key = match (budget_env, resolved_limit) {
                (Some(env), Some(limit)) => {
                    budgets
                        .entry(env.clone())
                        .or_insert(BudgetPool { limit, used: 0 });
                    Some(env)
                }
                _ => None,
            };
            let no_unbounded = lane
                .budget
                .as_ref()
                .map(|budget| budget.no_unbounded_budget)
                .unwrap_or(false);
            if no_unbounded && budget_key.is_none() {
                bail!(
                    "provider rate lane {} declares no_unbounded_budget but no daily request budget is set",
                    lane.lane_id
                );
            }
            lane_budget_key.insert(lane.lane_id.clone(), budget_key);
            lanes.insert(
                lane.lane_id.clone(),
                LaneRuntime {
                    state,
                    next_start_utc: DateTime::<Utc>::MIN_UTC,
                },
            );
            policies.insert(lane.lane_id.clone(), lane.clone());
        }
        Ok(Self {
            policies,
            lanes: Mutex::new(lanes),
            lane_budget_key,
            budgets: Mutex::new(budgets),
            classifier,
        })
    }

    /// Resolve a job's provider + endpoint to its lane id (errors if no lane matches).
    pub(crate) fn resolve_lane(&self, provider: &str, endpoint: &str) -> anyhow::Result<String> {
        let group = crate::provider_lane::endpoint_group_for(provider, endpoint)?;
        self.policies
            .values()
            .find(|lane| lane.provider == provider && lane.endpoint_groups.contains(&group))
            .map(|lane| lane.lane_id.clone())
            .with_context(|| {
                format!("No provider rate lane for provider={provider} endpoint_group={group}")
            })
    }

    /// Pre-pass budget reservation for one job by its estimated request count. The ONLY method that
    /// charges the budget counter (pacing/`acquire` never does -- no double-count).
    pub(crate) fn reserve(
        &self,
        lane_id: &str,
        estimated_requests: u64,
    ) -> anyhow::Result<ReserveOutcome> {
        let budget_key = self
            .lane_budget_key
            .get(lane_id)
            .with_context(|| format!("unknown provider rate lane: {lane_id}"))?;
        let Some(key) = budget_key else {
            return Ok(ReserveOutcome::Granted); // unbounded lane
        };
        let mut budgets = self
            .budgets
            .lock()
            .map_err(|_| anyhow!("provider rate limiter budget mutex poisoned"))?;
        let pool = budgets
            .get_mut(key)
            .with_context(|| format!("unknown budget pool: {key}"))?;
        if pool.used.saturating_add(estimated_requests) > pool.limit {
            Ok(ReserveOutcome::DeferredLaneBudget)
        } else {
            pool.used = pool.used.saturating_add(estimated_requests);
            Ok(ReserveOutcome::Granted)
        }
    }

    /// Reflect one observed provider outcome into the lane (AIMD update via the brain).
    pub(crate) fn record(
        &self,
        lane_id: &str,
        signal: &OutcomeSignal,
        latency_ms: u32,
    ) -> anyhow::Result<()> {
        let policy = self
            .policies
            .get(lane_id)
            .with_context(|| format!("unknown provider rate lane: {lane_id}"))?;
        let outcome = self.classifier.classify(policy, signal);
        let mut lanes = self
            .lanes
            .lock()
            .map_err(|_| anyhow!("provider rate limiter mutex poisoned"))?;
        let lane = lanes
            .get_mut(lane_id)
            .with_context(|| format!("unknown provider rate lane: {lane_id}"))?;
        lane.state = update_lane_state(policy, &lane.state, outcome, latency_ms, Utc::now())?;
        Ok(())
    }

    /// Per-page pacing: wait until this lane may send, re-validating state after every sleep so a
    /// task queued before a 429/cooldown does not fire blindly (spec section 3.1, enhancement #1).
    /// Pacing only -- never touches the budget counter.
    pub(crate) async fn acquire(&self, lane_id: &str) -> anyhow::Result<()> {
        loop {
            let wait = {
                let mut lanes = self
                    .lanes
                    .lock()
                    .map_err(|_| anyhow!("provider rate limiter mutex poisoned"))?;
                let lane = lanes
                    .get_mut(lane_id)
                    .with_context(|| format!("unknown provider rate lane: {lane_id}"))?;
                let now = Utc::now();
                match decide_acquire(&lane.state, lane.next_start_utc, now)? {
                    AcquireStep::Proceed { next_start_utc } => {
                        lane.next_start_utc = next_start_utc;
                        None
                    }
                    AcquireStep::Wait { until } => {
                        Some((until - now).to_std().unwrap_or(std::time::Duration::ZERO))
                    }
                }
            }; // lock dropped here -- never held across the await below
            match wait {
                None => return Ok(()),
                Some(delay) => tokio::time::sleep(delay).await,
            }
        }
    }

    /// Pure constructor (no env, no file I/O): gate on `enabled`, resolve each lane's daily budget via
    /// `budget_lookup(env_name)`, then build. Unit-tested with an in-memory lookup.
    pub(crate) fn from_lookup(
        enabled: bool,
        policy: ProviderRatePolicyDocument,
        budget_lookup: &dyn Fn(&str) -> Option<u64>,
    ) -> anyhow::Result<Option<Self>> {
        if !enabled {
            return Ok(None);
        }
        if policy.schema_version != POLICY_SCHEMA_VERSION || policy.status != "ready" {
            bail!("provider rate policy must be schema {POLICY_SCHEMA_VERSION} and status=ready");
        }
        let mut budget_limits = BTreeMap::new();
        for lane in &policy.lanes {
            let limit = lane.budget.as_ref().and_then(|budget| {
                if budget.daily_request_budget_env.trim().is_empty() {
                    return None;
                }
                budget_lookup(&budget.daily_request_budget_env).filter(|value| *value > 0)
            });
            budget_limits.insert(lane.lane_id.clone(), limit);
        }
        Ok(Some(Self::new(
            policy,
            budget_limits,
            Box::new(MessageThrottleClassifier),
        )?))
    }

    /// Thin env wrapper: read the opt-in flag + policy file, then delegate to `from_lookup`.
    pub(crate) fn from_env() -> anyhow::Result<Option<Self>> {
        let enabled = std::env::var("FOUNDATION_PLATFORM_NATIONAL_PROVIDER_RATE_LIMIT")
            .ok()
            .as_deref()
            == Some("1");
        if !enabled {
            return Ok(None);
        }
        let path = std::env::var("FOUNDATION_PLATFORM_NATIONAL_PROVIDER_RATE_POLICY_PATH")
            .unwrap_or_else(|_| "docs/catalog/provider-rate-policy.v1.json".to_owned());
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read provider rate policy: {path}"))?;
        let policy: ProviderRatePolicyDocument = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse provider rate policy: {path}"))?;
        let lookup = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
        };
        Self::from_lookup(true, policy, &lookup)
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use crate::public_provider_rate_policy::{
        AdaptiveControl, LaneBudget, LanePolicy, ProviderRatePolicyDocument, RateWindow,
        ThrottlingSignals, POLICY_SCHEMA_VERSION,
    };

    /// One building-register lane (start 20rps, x0.5 decrease, 60s cooldown, throttle on 429 / quota).
    /// Shared by the limiter's own tests AND the async wiring tests (explicit fixture module).
    pub(crate) fn building_register_test_policy() -> ProviderRatePolicyDocument {
        ProviderRatePolicyDocument {
            schema_version: POLICY_SCHEMA_VERSION.to_owned(),
            status: "ready".to_owned(),
            throughput_profile: "max_throughput_calibration".to_owned(),
            lanes: vec![LanePolicy {
                lane_id: "data-go-kr:building-register-open-api".to_owned(),
                provider: "data.go.kr".to_owned(),
                endpoint_groups: vec!["building_register_open_api".to_owned()],
                budget: Some(LaneBudget {
                    daily_request_budget_env: "DATA_GO_KR_DAILY_REQUEST_BUDGET".to_owned(),
                    request_budget_source: "operator_portal".to_owned(),
                    no_unbounded_budget: true,
                }),
                rate_window: Some(RateWindow {
                    min_rps: 0.1,
                    start_rps: 20.0,
                    max_rps: 100.0,
                    min_in_flight: 1,
                    start_in_flight: 16,
                    max_in_flight: 64,
                }),
                adaptive_control: Some(AdaptiveControl {
                    algorithm: "aimd".to_owned(),
                    success_window_requests: 10,
                    additive_rps_increment: 5.0,
                    additive_in_flight_increment: 8,
                    multiplicative_decrease_factor: 0.5,
                    cooldown_seconds_after_throttle: 60,
                    latency_p95_soft_ms: 5000,
                    latency_p95_hard_ms: 15000,
                }),
                throttling_signals: Some(ThrottlingSignals {
                    http_status_codes: vec![429],
                    provider_error_codes: vec!["22".to_owned()],
                    body_tokens: vec!["LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR".to_owned()],
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Two data.go.kr lanes (building-register + real-transaction) that name the SAME budget env on
    /// purpose: data.go.kr's quota is account-wide, so both lanes share one budget pool.
    pub(crate) fn data_go_kr_two_lane_test_policy() -> ProviderRatePolicyDocument {
        let mut policy = building_register_test_policy();
        let mut real_transaction = policy.lanes[0].clone();
        real_transaction.lane_id = "data-go-kr:real-transaction-open-api".to_owned();
        real_transaction.endpoint_groups = vec!["real_transaction_open_api".to_owned()];
        // same budget env on purpose (account-wide quota) -> shared pool
        policy.lanes.push(real_transaction);
        policy
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::{anyhow, Context};
    use chrono::TimeZone;

    use super::*;
    use crate::provider_rate_limiter::fixtures::building_register_test_policy;
    use crate::public_provider_rate_policy::{new_lane_state, ProviderRatePolicyDocument};

    fn lane(policy: &ProviderRatePolicyDocument) -> &LanePolicy {
        &policy.lanes[0]
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        match Utc.timestamp_opt(secs, 0) {
            chrono::LocalResult::Single(value) => value,
            _ => DateTime::<Utc>::MIN_UTC,
        }
    }

    #[test]
    fn classifier_maps_success_throttle_timeout_error() {
        let policy = building_register_test_policy();
        let lane = lane(&policy);
        let c = MessageThrottleClassifier;
        assert_eq!(
            c.classify(lane, &OutcomeSignal::success()),
            ProviderOutcome::Success
        );
        assert_eq!(
            c.classify(
                lane,
                &OutcomeSignal::failure("... returned HTTP 429 ...".to_owned())
            ),
            ProviderOutcome::Throttle
        );
        assert_eq!(
            c.classify(
                lane,
                &OutcomeSignal::failure(
                    "resultCode=22 resultMsg=LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR"
                        .to_owned()
                )
            ),
            ProviderOutcome::Throttle
        );
        assert_eq!(
            c.classify(
                lane,
                &OutcomeSignal::failure("request timed out after 30s".to_owned())
            ),
            ProviderOutcome::Timeout
        );
        assert_eq!(
            c.classify(
                lane,
                &OutcomeSignal::failure("connection reset by peer".to_owned())
            ),
            ProviderOutcome::Error
        );
    }

    #[test]
    fn decide_acquire_proceeds_when_slot_is_in_the_past() -> anyhow::Result<()> {
        let policy = building_register_test_policy();
        let state = new_lane_state(lane(&policy))?; // current_rps = 20 -> interval 50ms
        let now = ts(1_000);
        match decide_acquire(&state, ts(900), now)? {
            AcquireStep::Proceed { next_start_utc } => {
                assert_eq!(next_start_utc, now + Duration::milliseconds(50));
            }
            AcquireStep::Wait { .. } => return Err(anyhow!("expected Proceed")),
        }
        Ok(())
    }

    #[test]
    fn decide_acquire_waits_for_future_pacing_slot() -> anyhow::Result<()> {
        let policy = building_register_test_policy();
        let state = new_lane_state(lane(&policy))?;
        let now = ts(1_000);
        let slot = now + Duration::milliseconds(40);
        match decide_acquire(&state, slot, now)? {
            AcquireStep::Wait { until } => assert_eq!(until, slot),
            AcquireStep::Proceed { .. } => return Err(anyhow!("expected Wait")),
        }
        Ok(())
    }

    #[test]
    fn decide_acquire_waits_for_active_cooldown_over_pacing() -> anyhow::Result<()> {
        let policy = building_register_test_policy();
        let mut state = new_lane_state(lane(&policy))?;
        let now = ts(1_000);
        let cooldown_end = now + Duration::seconds(60);
        state.cooldown_until_utc = cooldown_end.to_rfc3339();
        match decide_acquire(&state, ts(0), now)? {
            AcquireStep::Wait { until } => assert_eq!(until, cooldown_end),
            AcquireStep::Proceed { .. } => return Err(anyhow!("expected cooldown Wait")),
        }
        Ok(())
    }

    fn limiter_with_budget(limit: Option<u64>) -> anyhow::Result<ProviderRateLimiter> {
        let policy = building_register_test_policy();
        let mut budgets = BTreeMap::new();
        budgets.insert("data-go-kr:building-register-open-api".to_owned(), limit);
        ProviderRateLimiter::new(policy, budgets, Box::new(MessageThrottleClassifier))
    }

    #[test]
    fn new_fails_closed_when_no_unbounded_budget_lane_has_no_budget() {
        assert!(limiter_with_budget(None).is_err());
    }

    #[test]
    fn resolve_lane_uses_endpoint_group() -> anyhow::Result<()> {
        let limiter = limiter_with_budget(Some(100))?;
        assert_eq!(
            limiter.resolve_lane("data.go.kr", "getBrTitleInfo")?,
            "data-go-kr:building-register-open-api"
        );
        assert!(limiter.resolve_lane("data.go.kr", "getUnknownOp").is_err());
        Ok(())
    }

    #[test]
    fn reserve_grants_until_budget_then_defers() -> anyhow::Result<()> {
        let limiter = limiter_with_budget(Some(10))?;
        let lane_id = limiter.resolve_lane("data.go.kr", "getBrTitleInfo")?;
        assert!(matches!(
            limiter.reserve(&lane_id, 6)?,
            ReserveOutcome::Granted
        ));
        assert!(matches!(
            limiter.reserve(&lane_id, 5)?,
            ReserveOutcome::DeferredLaneBudget
        ));
        assert!(matches!(
            limiter.reserve(&lane_id, 4)?,
            ReserveOutcome::Granted
        ));
        Ok(())
    }

    fn two_lane_limiter(daily_budget: u64) -> anyhow::Result<ProviderRateLimiter> {
        let policy = crate::provider_rate_limiter::fixtures::data_go_kr_two_lane_test_policy();
        let mut budgets = BTreeMap::new();
        budgets.insert(
            "data-go-kr:building-register-open-api".to_owned(),
            Some(daily_budget),
        );
        budgets.insert(
            "data-go-kr:real-transaction-open-api".to_owned(),
            Some(daily_budget),
        );
        ProviderRateLimiter::new(policy, budgets, Box::new(MessageThrottleClassifier))
    }

    #[test]
    fn reserve_shares_budget_across_lanes_with_same_env() -> anyhow::Result<()> {
        let limiter = two_lane_limiter(10)?;
        let br = limiter.resolve_lane("data.go.kr", "getBrTitleInfo")?;
        let rt = limiter.resolve_lane("data.go.kr", "getRTMSDataSvcAptTradeDev")?;
        assert!(matches!(limiter.reserve(&br, 6)?, ReserveOutcome::Granted));
        // shared pool now has 6 used; the OTHER lane sees only 4 remaining.
        assert!(matches!(
            limiter.reserve(&rt, 5)?,
            ReserveOutcome::DeferredLaneBudget
        ));
        assert!(matches!(limiter.reserve(&rt, 4)?, ReserveOutcome::Granted)); // 6+4 = 10
        Ok(())
    }

    #[test]
    fn record_throttle_imposes_cooldown_that_blocks_next_acquire() -> anyhow::Result<()> {
        let limiter = limiter_with_budget(Some(1_000))?;
        let lane_id = limiter.resolve_lane("data.go.kr", "getBrTitleInfo")?;
        limiter.record(&lane_id, &OutcomeSignal::failure("HTTP 429".to_owned()), 10)?;
        let lanes = limiter.lanes.lock().map_err(|_| anyhow!("poisoned"))?;
        let runtime = lanes.get(&lane_id).context("lane present")?;
        match decide_acquire(&runtime.state, runtime.next_start_utc, Utc::now())? {
            AcquireStep::Wait { .. } => Ok(()),
            AcquireStep::Proceed { .. } => Err(anyhow!("expected cooldown Wait after throttle")),
        }
    }

    #[tokio::test]
    async fn acquire_returns_immediately_when_clear() -> anyhow::Result<()> {
        let limiter = limiter_with_budget(Some(1_000))?;
        let lane_id = limiter.resolve_lane("data.go.kr", "getBrTitleInfo")?;
        limiter.acquire(&lane_id).await?; // fresh lane: next_start = MIN_UTC, no cooldown -> no hang
        let lanes = limiter.lanes.lock().map_err(|_| anyhow!("poisoned"))?;
        let runtime = lanes.get(&lane_id).context("lane present")?;
        assert!(runtime.next_start_utc > DateTime::<Utc>::MIN_UTC);
        Ok(())
    }

    #[tokio::test]
    async fn acquire_never_changes_budget_counter() -> anyhow::Result<()> {
        // reserve charges budget; acquire must NOT (no double-count).
        let limiter = limiter_with_budget(Some(1_000))?;
        let lane_id = limiter.resolve_lane("data.go.kr", "getBrTitleInfo")?;
        assert!(matches!(
            limiter.reserve(&lane_id, 7)?,
            ReserveOutcome::Granted
        ));
        limiter.acquire(&lane_id).await?;
        limiter.acquire(&lane_id).await?;
        let budgets = limiter.budgets.lock().map_err(|_| anyhow!("poisoned"))?;
        let pool = budgets
            .get("DATA_GO_KR_DAILY_REQUEST_BUDGET")
            .context("budget pool present")?;
        assert_eq!(pool.used, 7); // reserve charged once; two acquires changed nothing
        Ok(())
    }

    #[test]
    fn from_lookup_disabled_returns_none() -> anyhow::Result<()> {
        let lookup = |_name: &str| None;
        assert!(
            ProviderRateLimiter::from_lookup(false, building_register_test_policy(), &lookup)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn from_lookup_enabled_builds_with_in_memory_budget() -> anyhow::Result<()> {
        let with_budget =
            |name: &str| (name == "DATA_GO_KR_DAILY_REQUEST_BUDGET").then_some(500_u64);
        assert!(ProviderRateLimiter::from_lookup(
            true,
            building_register_test_policy(),
            &with_budget
        )?
        .is_some());
        // Missing budget for a no_unbounded lane -> fail closed (no real env touched).
        let no_budget = |_name: &str| None;
        assert!(ProviderRateLimiter::from_lookup(
            true,
            building_register_test_policy(),
            &no_budget
        )
        .is_err());
        Ok(())
    }
}
