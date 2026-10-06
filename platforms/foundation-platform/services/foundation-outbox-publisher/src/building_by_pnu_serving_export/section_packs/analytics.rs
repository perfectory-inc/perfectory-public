//! Cloudflare analytics (GraphQL) the cut-over reads: a Worker's invocations — status, CPU and
//! wall time — by script and version (`workersInvocationsAdaptive`), and a hostname's HTTP
//! responses by status (`httpRequestsAdaptiveGroups`).
//!
//! Workers analytics records no HTTP status per invocation: a Worker that answers 503 itself is a
//! `success` invocation. A platform cut-off (CPU or memory, error 1102) is `exceededResources`.
//! So the gate reads CPU and cut-offs per version here, and 5xx per hostname from the zone.
//!
//! `workersInvocationsAdaptive` is sampled (`avg { sampleInterval }` above 1) and its rows arrive
//! minutes late. Its `sum { requests }` is already scaled by the sample interval (2026-10-06: 304
//! counted for 300 pinned reads at an average interval of 1.2), so it is not scaled again; but at
//! the end of a step's wait it had counted 137 of them. A canary step therefore counts its reads
//! from their answers and holds analytics only to a coverage of them (root ADR-0157).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value as JsonValue};

use super::super::optional_env;
use super::gate::WorkerCpu;
use crate::by_pnu_gateway_contract::section_pack_policy;

const ENDPOINT: &str = "https://api.cloudflare.com/client/v4/graphql";
/// Analytics lags the requests by a minute or two; a reader waits this long at most for it to
/// count what was sent.
const WAIT: Duration = Duration::from_secs(600);
const POLL: Duration = Duration::from_secs(30);
/// The share of the sent requests analytics must count before its quantiles are taken.
const COVERAGE_PERCENT: u64 = 95;

/// Where and as whom the analytics are read.
#[derive(Clone, Debug)]
pub(crate) struct AnalyticsConfig {
    pub(crate) endpoint: String,
    pub(crate) account_id: String,
    pub(crate) api_token: String,
    pub(crate) wait: Duration,
    pub(crate) poll: Duration,
}

impl AnalyticsConfig {
    /// The account and its read-only token (Account Analytics: Read), from the environment
    /// variables the contract names (`by_pnu_section_packs.cloudflare_analytics`, set by the
    /// unit's `EnvironmentFile`), both or neither.
    ///
    /// # Errors
    /// Refuses one without the other.
    pub(crate) fn from_env() -> anyhow::Result<Option<Self>> {
        let names = &section_pack_policy()?.cloudflare_analytics;
        match (
            optional_env(&names.account_id_env)?,
            optional_env(&names.api_token_env)?,
        ) {
            (Some(account_id), Some(api_token)) => Ok(Some(Self {
                endpoint: ENDPOINT.to_owned(),
                account_id,
                api_token,
                wait: WAIT,
                poll: POLL,
            })),
            (None, None) => Ok(None),
            _ => bail!(
                "{} and {} are set together or not at all",
                names.account_id_env,
                names.api_token_env
            ),
        }
    }

    /// The analytics config a gate cannot judge without; refused, naming the file and the
    /// variables, when they are not set.
    ///
    /// # Errors
    /// Refuses unset or half-set variables.
    pub(crate) fn required(purpose: &str) -> anyhow::Result<Self> {
        let names = &section_pack_policy()?.cloudflare_analytics;
        Self::from_env()?.with_context(|| {
            format!(
                "refused: {purpose} reads Cloudflare Workers analytics and {} are not set. Run it \
                 in a unit whose EnvironmentFile is {} (root:root 0600, a token scoped {}); that \
                 file is missing or does not set them",
                Self::names().unwrap_or_default(),
                names
                    .env_file()
                    .unwrap_or("the runtime-secrets contract's cloudflare-analytics file"),
                names.token_scope
            )
        })
    }

    /// The variable names, for a message that asks for them.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub(crate) fn names() -> anyhow::Result<String> {
        let names = &section_pack_policy()?.cloudflare_analytics;
        Ok(format!(
            "{} and {}",
            names.account_id_env, names.api_token_env
        ))
    }
}

/// The invocations of one Worker script.
#[derive(Clone, Debug)]
pub(crate) struct Invocations<'a> {
    pub(crate) script: &'a str,
    /// One version only (a gradual deployment's step), or every version.
    pub(crate) version: Option<&'a str>,
    pub(crate) from: DateTime<Utc>,
    pub(crate) to: DateTime<Utc>,
}

fn rfc3339(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

const INVOCATIONS_QUERY: &str = "query($account: String!, $filter: AccountWorkersInvocationsAdaptiveFilter_InputObject!) { \
    viewer { accounts(filter: {accountTag: $account}) { \
    workersInvocationsAdaptive(limit: 100, filter: $filter) { sum { requests } \
    avg { sampleInterval } dimensions { status } quantiles { cpuTimeP50 cpuTimeP99 wallTimeP50 wallTimeP99 } } } } }";

/// The script's invocations over the window, waiting until analytics counts nearly every one of
/// the `expected` requests that were sent (it lags them).
///
/// # Errors
/// Returns an error when the API refuses or answers something unreadable.
pub(crate) async fn worker_cpu(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    invocations: &Invocations<'_>,
    expected: u64,
) -> anyhow::Result<WorkerCpu> {
    let waited = Instant::now();
    loop {
        let cpu = query_invocations(client, analytics, invocations).await?;
        if cpu.requests * 100 >= expected * COVERAGE_PERCENT || waited.elapsed() >= analytics.wait {
            return Ok(cpu);
        }
        tracing::info!(
            counted = cpu.requests,
            expected,
            "waiting for Workers analytics to count the requests"
        );
        tokio::time::sleep(analytics.poll).await;
    }
}

/// One read of the script's invocations over the window.
///
/// # Errors
/// Returns an error when the API refuses or answers something unreadable.
pub(crate) async fn query_invocations(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    invocations: &Invocations<'_>,
) -> anyhow::Result<WorkerCpu> {
    let (from_utc, to_utc) = (rfc3339(invocations.from), rfc3339(invocations.to));
    let mut filter = json!({
        "scriptName": invocations.script,
        "datetime_geq": from_utc,
        "datetime_leq": to_utc,
    });
    if let Some(version) = invocations.version {
        filter["scriptVersion"] = json!(version);
    }
    let answer = query(
        client,
        analytics,
        INVOCATIONS_QUERY,
        json!({"account": analytics.account_id, "filter": filter}),
    )
    .await?;
    let groups = answer
        .pointer("/data/viewer/accounts/0/workersInvocationsAdaptive")
        .and_then(JsonValue::as_array)
        .context("Workers analytics answered no invocation groups")?;
    let mut cpu = WorkerCpu {
        script: invocations.script.to_owned(),
        from_utc,
        to_utc,
        ..WorkerCpu::default()
    };
    for group in groups {
        let requests = group
            .pointer("/sum/requests")
            .and_then(JsonValue::as_u64)
            .context("an invocation group has no request count")?;
        let status = group
            .pointer("/dimensions/status")
            .and_then(JsonValue::as_str)
            .context("an invocation group has no status")?;
        let quantile = |name: &str| {
            group
                .pointer(&format!("/quantiles/{name}"))
                .and_then(JsonValue::as_f64)
                .with_context(|| format!("an invocation group has no {name}"))
        };
        // Microseconds; the largest of any group, so no status can hide a slow tail.
        cpu.cpu_p50_ms = cpu.cpu_p50_ms.max(quantile("cpuTimeP50")? / 1000.0);
        cpu.cpu_p99_ms = cpu.cpu_p99_ms.max(quantile("cpuTimeP99")? / 1000.0);
        cpu.wall_p50_ms = cpu.wall_p50_ms.max(quantile("wallTimeP50")? / 1000.0);
        cpu.wall_p99_ms = cpu.wall_p99_ms.max(quantile("wallTimeP99")? / 1000.0);
        // Absent on an older answer; the largest of any group says how sampled the window was.
        if let Some(interval) = group
            .pointer("/avg/sampleInterval")
            .and_then(JsonValue::as_f64)
        {
            cpu.sample_interval_max = cpu.sample_interval_max.max(interval);
        }
        cpu.requests += requests;
        *cpu.statuses.entry(status.to_owned()).or_default() += requests;
    }
    Ok(cpu)
}

const RESPONSES_QUERY: &str = "query($zone: String!, $host: String!, $from: Time!, $to: Time!) { \
    viewer { zones(filter: {zoneTag: $zone}) { \
    httpRequestsAdaptiveGroups(limit: 100, filter: {clientRequestHTTPHost: $host, \
    requestSource: \"eyeball\", datetime_geq: $from, datetime_leq: $to}) { count dimensions { \
    edgeResponseStatus } } } } }";

/// A hostname's responses to clients over the window, by HTTP status. Only `eyeball` requests: the
/// Worker's own Cache API calls are logged under the same hostname (a lookup that misses as a
/// GET 504, a write as a PUT 204; 2026-10-05: 908 and 873 of them beside 898 client answers) and
/// are not answers anyone received.
///
/// # Errors
/// Returns an error when the API refuses or answers something unreadable.
pub(crate) async fn responses_by_status(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    zone_id: &str,
    host: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<BTreeMap<u16, u64>> {
    let answer = query(
        client,
        analytics,
        RESPONSES_QUERY,
        json!({"zone": zone_id, "host": host, "from": rfc3339(from), "to": rfc3339(to)}),
    )
    .await?;
    let groups = answer
        .pointer("/data/viewer/zones/0/httpRequestsAdaptiveGroups")
        .and_then(JsonValue::as_array)
        .context("zone analytics answered no response groups")?;
    let mut statuses = BTreeMap::new();
    for group in groups {
        let count = group
            .get("count")
            .and_then(JsonValue::as_u64)
            .context("a response group has no count")?;
        let status = group
            .pointer("/dimensions/edgeResponseStatus")
            .and_then(JsonValue::as_u64)
            .and_then(|status| u16::try_from(status).ok())
            .context("a response group has no status")?;
        *statuses.entry(status).or_default() += count;
    }
    Ok(statuses)
}

async fn query(
    client: &reqwest::Client,
    analytics: &AnalyticsConfig,
    text: &str,
    variables: JsonValue,
) -> anyhow::Result<JsonValue> {
    let answer: JsonValue = client
        .post(&analytics.endpoint)
        .bearer_auth(&analytics.api_token)
        .json(&json!({"query": text, "variables": variables}))
        .send()
        .await
        .context("Cloudflare analytics did not answer")?
        .error_for_status()
        .context("Cloudflare analytics refused the query")?
        .json()
        .await
        .context("Cloudflare analytics answered something that is not JSON")?;
    if let Some(errors) = answer.get("errors").filter(|errors| !errors.is_null()) {
        bail!("Cloudflare analytics answered errors: {errors}");
    }
    Ok(answer)
}
