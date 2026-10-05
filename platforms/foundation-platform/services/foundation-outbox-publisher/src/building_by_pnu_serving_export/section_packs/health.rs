//! `check-building-gateway-version-health`: one canary step's verdict (root ADR-0151 §7, contract
//! `section_packs.canary`), read from Cloudflare analytics for the step's window.
//!
//! The new version, against the contract and against the old version it shares traffic with:
//!
//! | check | source | bound |
//! |---|---|---|
//! | enough traffic to judge | Workers invocations of the new version | `canary.min_requests_per_step` |
//! | no platform cut-off | `exceededResources` of the new version | 0 |
//! | errors | invocations that are neither `success` nor `clientDisconnected` | share ≤ 1 − `slo.availability_min` |
//! | CPU | p99 of the new version | `worker_cpu_p99_max_ms` |
//! | wall time | p50 and p99 of the new version vs the old | `slo.latency_max_increase_ms.warm` |
//! | 5xx | the live hostname's responses (every version) | share ≤ 1 − `slo.availability_min` |
//!
//! Workers analytics records no HTTP status per version (a Worker's own 503 is a `success`
//! invocation), so 5xx are read per hostname; at a small step the new version's share of them is
//! diluted, which is why each step holds long enough to count `min_requests_per_step` and the
//! steps grow. Read-only; exits non-zero on any breach, which the canary script answers with a
//! rollback.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{ensure, Context};
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;

use super::super::{optional_env, LANE};
use super::analytics::{self, AnalyticsConfig, Invocations};
use super::equality::write_evidence;
use super::gate::{WorkerCpu, EXCEEDED_RESOURCES};
use crate::by_pnu_gateway_contract::section_pack_policy;

/// One step's report.
#[derive(Debug, Serialize)]
pub(crate) struct HealthReport {
    pub(crate) script: String,
    pub(crate) new_version: String,
    pub(crate) old_version: Option<String>,
    pub(crate) window_seconds: i64,
    pub(crate) new: WorkerCpu,
    pub(crate) old: Option<WorkerCpu>,
    pub(crate) host_statuses: Option<BTreeMap<u16, u64>>,
    pub(crate) breaches: Vec<String>,
    pub(crate) passed: bool,
}

/// Runs the check.
///
/// # Errors
/// Returns an error when analytics cannot be read, or any check fails.
pub(crate) async fn run() -> anyhow::Result<()> {
    let env = |name: &str| optional_env(&LANE.env(name));
    let required = |name: &str| -> anyhow::Result<String> {
        env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
    };
    let analytics = AnalyticsConfig::from_env()?.with_context(|| {
        format!(
            "{} are required",
            AnalyticsConfig::names().unwrap_or_default()
        )
    })?;
    let canary = LANE
        .section_packs()?
        .canary
        .as_ref()
        .context("the contract names no canary for this lane")?;
    let window_seconds = match env("CANARY_WINDOW_SECONDS")? {
        Some(raw) => raw
            .parse::<i64>()
            .context("the canary window must be seconds")?,
        None => i64::try_from(canary.hold_seconds)?,
    };
    let report = check(
        &reqwest::Client::new(),
        &analytics,
        &required("CANARY_NEW_VERSION")?,
        env("CANARY_OLD_VERSION")?.as_deref(),
        optional_env(&section_pack_policy()?.cloudflare_analytics.zone_id_env)?.as_deref(),
        window_seconds,
    )
    .await?;
    if let Some(path) = env("CANARY_REPORT_PATH")? {
        write_evidence(&PathBuf::from(path), &report)?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    ensure!(
        report.passed,
        "the new version breached: {}",
        report.breaches.join("; ")
    );
    Ok(())
}

/// The step's verdict over the last `window_seconds`.
///
/// # Errors
/// Returns an error when analytics cannot be read.
pub(crate) async fn check(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    new_version: &str,
    old_version: Option<&str>,
    zone_id: Option<&str>,
    window_seconds: i64,
) -> anyhow::Result<HealthReport> {
    let gate = &section_pack_policy()?.cutover_gate;
    let policy = LANE.policy()?;
    let canary = LANE
        .section_packs()?
        .canary
        .as_ref()
        .context("the contract names no canary for this lane")?;
    let to = Utc::now();
    let from = to - ChronoDuration::seconds(window_seconds);
    let of = |version| Invocations {
        script: &policy.worker_name,
        version: Some(version),
        from,
        to,
    };
    let new = analytics::query_invocations(client, analytics, &of(new_version)).await?;
    let old = match old_version {
        Some(version) => Some(analytics::query_invocations(client, analytics, &of(version)).await?),
        None => None,
    };
    let host_statuses = match zone_id {
        Some(zone) => Some(
            analytics::responses_by_status(
                client,
                analytics,
                zone,
                &policy.public_hostname,
                from,
                to,
            )
            .await?,
        ),
        None => None,
    };
    let breaches = breaches(
        &new,
        old.as_ref(),
        host_statuses.as_ref(),
        canary.min_requests_per_step,
        gate,
    );
    Ok(HealthReport {
        script: policy.worker_name.clone(),
        new_version: new_version.to_owned(),
        old_version: old_version.map(str::to_owned),
        window_seconds,
        passed: breaches.is_empty(),
        new,
        old,
        host_statuses,
        breaches,
    })
}

/// What the new version breached; empty when it holds every bound.
pub(crate) fn breaches(
    new: &WorkerCpu,
    old: Option<&WorkerCpu>,
    host_statuses: Option<&BTreeMap<u16, u64>>,
    min_requests: u64,
    gate: &crate::by_pnu_gateway_contract::CutoverGatePolicy,
) -> Vec<String> {
    let mut breaches = Vec::new();
    let error_budget = 1.0 - gate.slo.availability_min;
    if new.requests < min_requests {
        breaches.push(format!(
            "{} requests reached the new version, fewer than the {min_requests} a step needs",
            new.requests
        ));
    }
    let cut_off = new.statuses.get(EXCEEDED_RESOURCES).copied().unwrap_or(0);
    if cut_off > 0 {
        breaches.push(format!(
            "{cut_off} invocations exceeded their resources (error 1102)"
        ));
    }
    let errors = new
        .statuses
        .iter()
        // A cut-off is its own breach above; the rest are exceptions and internal errors.
        .filter(|(status, _)| {
            !matches!(
                status.as_str(),
                "success" | "clientDisconnected" | EXCEEDED_RESOURCES
            )
        })
        .map(|(_, count)| count)
        .sum::<u64>();
    #[allow(clippy::cast_precision_loss)]
    if new.requests > 0 && errors as f64 / new.requests as f64 > error_budget {
        breaches.push(format!("{errors} of {} invocations failed", new.requests));
    }
    if new.cpu_p99_ms > gate.worker_cpu_p99_max_ms {
        breaches.push(format!(
            "CPU p99 {:.1} ms is above {:.1} ms",
            new.cpu_p99_ms, gate.worker_cpu_p99_max_ms
        ));
    }
    if let Some(old) = old.filter(|old| old.requests > 0) {
        let bound = &gate.slo.latency_max_increase_ms.warm;
        if new.wall_p50_ms - old.wall_p50_ms > bound.p50
            || new.wall_p99_ms - old.wall_p99_ms > bound.p99
        {
            breaches.push(format!(
                "wall time p50 {:.0}→{:.0} ms, p99 {:.0}→{:.0} ms is above the warm bound",
                old.wall_p50_ms, new.wall_p50_ms, old.wall_p99_ms, new.wall_p99_ms
            ));
        }
    }
    if let Some(statuses) = host_statuses {
        let total = statuses.values().sum::<u64>();
        let server_errors = statuses
            .iter()
            .filter(|(status, _)| **status >= 500)
            .map(|(_, count)| count)
            .sum::<u64>();
        #[allow(clippy::cast_precision_loss)]
        if total > 0 && server_errors as f64 / total as f64 > error_budget {
            breaches.push(format!("{server_errors} of {total} responses were 5xx"));
        }
    }
    breaches
}
