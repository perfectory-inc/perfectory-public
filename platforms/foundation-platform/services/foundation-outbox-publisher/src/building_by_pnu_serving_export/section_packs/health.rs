//! `check-building-gateway-version-health`: one canary step's verdict (root ADR-0151 §7, contract
//! `section_packs.canary`), read from pinned synthetic reads and Cloudflare analytics.
//!
//! A step first reads `canary.synthetic_load` gate-sample PNUs from the live hostname at each of
//! its two versions, pinned with Cloudflare's `Cloudflare-Workers-Version-Overrides` header: before
//! launch the live hostname carries almost no traffic, so a 1% step would never count enough real
//! requests on the new version. Then the new version is held to:
//!
//! | check | source | bound |
//! |---|---|---|
//! | every pinned read answered 200, gzip | the synthetic reads of each version | share ≥ `slo.availability_min` |
//! | the reads reached the new version | pinned answers naming the new version in `version_header` | `canary.min_requests_per_step` |
//! | analytics counted enough of them | Workers invocations of the new version / its pinned answers | ≥ `canary.analytics_min_coverage` |
//! | no platform cut-off | `exceededResources` of the new version | 0 |
//! | errors | invocations that are neither `success` nor `clientDisconnected` | share ≤ 1 − `slo.availability_min` |
//! | CPU | p99 of the new version, and its increase over the old | `worker_cpu_p99_max_ms`, `worker_cpu_p99_max_increase_ms` |
//! | wall time | p50 and p99 of the new version vs the old | `slo.latency_max_increase_ms.warm` |
//! | 5xx | the live hostname's client responses (every version) | counted at all, share ≤ 1 − `slo.availability_min` |
//!
//! Workers analytics records no HTTP status per version (a Worker's own 503 is a `success`
//! invocation), which is why the pinned reads count each version's answers themselves and the zone
//! is read too. The zone is required: a step whose 5xx cannot be counted is not judged. Whether the
//! reads reached the new version is counted from the answers themselves, each naming the version
//! that produced it (root ADR-0157): Workers analytics is sampled and minutes late (2026-10-06: 137
//! of 300 pinned answers counted when the wait ran out), so it judges CPU, cut-offs and errors only,
//! and must have counted a share of the answers first. A pin the platform did not honour (a version
//! outside the current deployment) shows as too few answers naming the new version. Read-only towards R2 and the Worker; exits non-zero on any breach, which the
//! canary script answers with a rollback.
//!
//! `--preflight` (`CANARY_PREFLIGHT=true`) runs both analytics queries once, so a missing zone id or
//! a token without both permissions refuses the rollout before anything is uploaded.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{ensure, Context};
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;

use super::super::{optional_env, LANE};
use super::analytics::{self, AnalyticsConfig, Invocations};
use super::equality::write_evidence;
use super::gate::{self, EqualityEvidence, LoadEvidence, WorkerCpu, EXCEEDED_RESOURCES};
use super::latency::REQUEST_TIMEOUT;
use super::load::{self, LoadPlan};
use crate::by_pnu_gateway_contract::{section_pack_policy, CanaryPolicy};

/// The header Cloudflare reads to pin a request to one version of a gradual deployment.
pub(crate) const VERSION_OVERRIDE_HEADER: &str = "Cloudflare-Workers-Version-Overrides";
/// Analytics lags its requests; the window reaches past the pinned reads by this much.
const ANALYTICS_SLACK_SECONDS: i64 = 120;

/// One step's report.
#[derive(Debug, Serialize)]
pub(crate) struct HealthReport {
    pub(crate) script: String,
    pub(crate) new_version: String,
    pub(crate) old_version: Option<String>,
    pub(crate) window_seconds: i64,
    pub(crate) new: WorkerCpu,
    pub(crate) old: Option<WorkerCpu>,
    pub(crate) host_statuses: BTreeMap<u16, u64>,
    /// The pinned reads answered by the new version, as each answer named it.
    pub(crate) reached_new_version: u64,
    pub(crate) pinned: Pinned,
    pub(crate) breaches: Vec<String>,
    pub(crate) passed: bool,
}

/// The synthetic reads each version answered.
#[derive(Debug, Serialize)]
pub(crate) struct Pinned {
    pub(crate) new: LoadEvidence,
    pub(crate) old: Option<LoadEvidence>,
}

/// The preflight's answer: both datasets were readable.
#[derive(Debug, Serialize)]
struct Preflight {
    script: String,
    hostname: String,
    invocations_counted: u64,
    responses_counted: u64,
}

fn canary() -> anyhow::Result<&'static CanaryPolicy> {
    LANE.section_packs()?
        .canary
        .as_ref()
        .context("the contract names no canary for this lane")
}

/// The live hostname's zone, without which a step's 5xx cannot be counted.
///
/// # Errors
/// Refuses, naming the variable and the file, when it is not set.
pub(crate) fn required_zone() -> anyhow::Result<String> {
    let names = &section_pack_policy()?.cloudflare_analytics;
    optional_env(&names.zone_id_env)?.with_context(|| {
        format!(
            "refused: a canary step counts the live hostname's 5xx from zone analytics and {} is \
             not set; add it to {} beside the token (scoped {})",
            names.zone_id_env,
            names
                .env_file()
                .unwrap_or("the runtime-secrets contract's cloudflare-analytics file"),
            names.token_scope
        )
    })
}

/// Runs the check (or the preflight).
///
/// # Errors
/// Returns an error when analytics cannot be read, or any check fails.
pub(crate) async fn run() -> anyhow::Result<()> {
    let env = |name: &str| optional_env(&LANE.env(name));
    let required = |name: &str| -> anyhow::Result<String> {
        env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
    };
    let analytics = AnalyticsConfig::required("the canary health check")?;
    let zone_id = required_zone()?;
    let client = reqwest::Client::new();
    if env("CANARY_PREFLIGHT")?.as_deref() == Some("true") {
        let answer = preflight(&client, &analytics, &zone_id).await?;
        println!("{}", serde_json::to_string_pretty(&answer)?);
        return Ok(());
    }
    let canary = canary()?;
    let window_seconds = match env("CANARY_WINDOW_SECONDS")? {
        Some(raw) => raw
            .parse::<i64>()
            .context("the canary window must be seconds")?,
        None => i64::try_from(canary.hold_seconds)?,
    };
    // The monitor's sample file is the one place the host names the gate sample.
    let (equality, _) =
        gate::read::<EqualityEvidence>(&PathBuf::from(required("MONITOR_SAMPLE_PATH")?))?;
    let base_url = match env("CANARY_BASE_URL")? {
        Some(url) => url,
        None => format!("https://{}", LANE.policy()?.public_hostname),
    };
    let new_version = required("CANARY_NEW_VERSION")?;
    let old_version = env("CANARY_OLD_VERSION")?;
    let pinned = drive(
        &LoadPlan::from_contract(&canary.synthetic_load),
        &base_url,
        &equality.sample,
        &new_version,
        old_version.as_deref(),
    )
    .await?;
    let report = check(
        &client,
        &analytics,
        &new_version,
        old_version.as_deref(),
        &zone_id,
        window_seconds,
        pinned,
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

/// Both analytics queries over the last quarter hour; an error from either refuses.
async fn preflight(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    zone_id: &str,
) -> anyhow::Result<Preflight> {
    let policy = LANE.policy()?;
    let to = Utc::now();
    let from = to - ChronoDuration::minutes(15);
    let invocations = analytics::query_invocations(
        client,
        analytics,
        &Invocations {
            script: &policy.worker_name,
            version: None,
            from,
            to,
        },
    )
    .await
    .context("refused: Workers analytics (Account Analytics: Read) cannot be read")?;
    let responses = analytics::responses_by_status(
        client,
        analytics,
        zone_id,
        &policy.public_hostname,
        from,
        to,
    )
    .await
    .context("refused: the zone's HTTP analytics (zone Analytics: Read) cannot be read")?;
    Ok(Preflight {
        script: policy.worker_name.clone(),
        hostname: policy.public_hostname.clone(),
        invocations_counted: invocations.requests,
        responses_counted: responses.values().sum(),
    })
}

/// A client whose every request Cloudflare routes to `version` of the live Worker.
///
/// # Errors
/// Returns an error when the header or the client cannot be built.
pub(crate) fn pinned_client(version: &str) -> anyhow::Result<reqwest::Client> {
    let worker = &LANE.policy()?.worker_name;
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        VERSION_OVERRIDE_HEADER,
        reqwest::header::HeaderValue::from_str(&format!("{worker}=\"{version}\""))
            .context("a version id that cannot be a header value")?,
    );
    Ok(reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .default_headers(headers)
        .build()?)
}

/// The synthetic reads (the contract's `canary.synthetic_load`), pinned to each version at once.
///
/// # Errors
/// Returns an error when the sample is empty or a client cannot be built.
pub(crate) async fn drive(
    plan: &LoadPlan,
    base_url: &str,
    sample: &[String],
    new_version: &str,
    old_version: Option<&str>,
) -> anyhow::Result<Pinned> {
    let prefix = &LANE.policy()?.request_path.prefix;
    let url_of = |pnu: &str| format!("{base_url}{prefix}{pnu}");
    let new_client = pinned_client(new_version)?;
    let new = load::run(&new_client, plan, sample, url_of, None);
    let (new, old) = match old_version {
        Some(version) => {
            let old_client = pinned_client(version)?;
            let (new, old) = tokio::join!(new, load::run(&old_client, plan, sample, url_of, None));
            (new?, Some(old?))
        }
        None => (new.await?, None),
    };
    Ok(Pinned { new, old })
}

/// The step's verdict over the last `window_seconds`, once analytics counts the pinned reads.
///
/// # Errors
/// Returns an error when analytics cannot be read.
pub(crate) async fn check(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    new_version: &str,
    old_version: Option<&str>,
    zone_id: &str,
    window_seconds: i64,
    pinned: Pinned,
) -> anyhow::Result<HealthReport> {
    let gate = &section_pack_policy()?.cutover_gate;
    let policy = LANE.policy()?;
    let canary = canary()?;
    let to = Utc::now() + ChronoDuration::seconds(5);
    // The window holds at least the pinned reads and the analytics lag behind them.
    let window_seconds = window_seconds
        .max(i64::try_from(canary.synthetic_load.duration_seconds)? + ANALYTICS_SLACK_SECONDS);
    let from = to - ChronoDuration::seconds(window_seconds);
    let of = |version| Invocations {
        script: &policy.worker_name,
        version: Some(version),
        from,
        to,
    };
    // Waits until analytics counts the reads the new version answered (it lags them).
    let reached_new_version = reached(&pinned, new_version);
    let new =
        analytics::worker_cpu(client, analytics, &of(new_version), reached_new_version).await?;
    let old = match old_version {
        Some(version) => Some(analytics::query_invocations(client, analytics, &of(version)).await?),
        None => None,
    };
    let host_statuses = analytics::responses_by_status(
        client,
        analytics,
        zone_id,
        &policy.public_hostname,
        from,
        to,
    )
    .await?;
    let breaches = breaches(
        &new,
        old.as_ref(),
        &host_statuses,
        &pinned,
        new_version,
        canary,
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
        reached_new_version,
        pinned,
        breaches,
    })
}

/// The pinned reads of the new version that `new_version` itself answered.
fn reached(pinned: &Pinned, new_version: &str) -> u64 {
    pinned.new.versions.get(new_version).copied().unwrap_or(0)
}

/// What the new version breached; empty when it holds every bound.
pub(crate) fn breaches(
    new: &WorkerCpu,
    old: Option<&WorkerCpu>,
    host_statuses: &BTreeMap<u16, u64>,
    pinned: &Pinned,
    new_version: &str,
    canary: &CanaryPolicy,
    gate: &crate::by_pnu_gateway_contract::CutoverGatePolicy,
) -> Vec<String> {
    let mut breaches = Vec::new();
    let error_budget = 1.0 - gate.slo.availability_min;
    for (label, evidence) in [("new", Some(&pinned.new)), ("old", pinned.old.as_ref())] {
        let Some(evidence) = evidence else { continue };
        if evidence.sent == 0 || evidence.availability < gate.slo.availability_min {
            breaches.push(format!(
                "the {label} version answered {} of {} pinned reads with 200 and gzip ({:?})",
                evidence.answered, evidence.sent, evidence.failures
            ));
        }
    }
    let reached = reached(pinned, new_version);
    if reached < canary.min_requests_per_step {
        breaches.push(format!(
            "{reached} pinned reads were answered by the new version {new_version}, fewer than the \
             {} a step needs (answers by version: {:?}; a version pin is honoured only for a \
             version in the current deployment)",
            canary.min_requests_per_step, pinned.new.versions
        ));
    }
    // Analytics never decides the reach; it must have counted enough of it to judge the rest.
    #[allow(clippy::cast_precision_loss)]
    let coverage = new.requests as f64 / reached.max(1) as f64;
    if reached > 0 && coverage < canary.analytics_min_coverage {
        breaches.push(format!(
            "Workers analytics counted {} invocations of the new version for its {reached} pinned \
             answers ({:.0}%), below the {:.0}% its CPU, cut-offs and errors are judged on",
            new.requests,
            coverage * 100.0,
            canary.analytics_min_coverage * 100.0
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
        if new.cpu_p99_ms - old.cpu_p99_ms > gate.worker_cpu_p99_max_increase_ms {
            breaches.push(format!(
                "CPU p99 {:.1}→{:.1} ms is more than {:.1} ms above the old version's",
                old.cpu_p99_ms, new.cpu_p99_ms, gate.worker_cpu_p99_max_increase_ms
            ));
        }
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
    let total = host_statuses.values().sum::<u64>();
    let server_errors = host_statuses
        .iter()
        .filter(|(status, _)| **status >= 500)
        .map(|(_, count)| count)
        .sum::<u64>();
    #[allow(clippy::cast_precision_loss)]
    if total == 0 {
        // The pinned reads went to this hostname: none counted means the 5xx check saw nothing.
        breaches.push(
            "the live hostname counted no client responses in the window, so its 5xx were not \
             counted"
                .to_owned(),
        );
    } else if server_errors as f64 / total as f64 > error_budget {
        breaches.push(format!("{server_errors} of {total} responses were 5xx"));
    }
    breaches
}
