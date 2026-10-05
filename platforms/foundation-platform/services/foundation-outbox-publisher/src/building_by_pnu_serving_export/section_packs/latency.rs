//! `probe-building-by-pnu-section-pack-latency`: gate (나) of the cut-over (root ADR-0147 §6), the
//! live check after upload.
//!
//! The sample is the one the equality evidence drew (a seeded hash of the PNU over the whole bake,
//! `equality.rs`): about 10,000 PNUs, so the check costs about 10,000 live object reads and 10,000
//! preview requests, not a read of every pack. Each PNU is read once from the live route (objects)
//! and once from a preview Worker serving the unpublished pack generation
//! (`?{preview_query_parameter}=g{N}`), alternating which goes first, each timed on its own until
//! the whole body is in. The contract's `probe_concurrency` PNUs are in flight at once, so the
//! sample finishes in minutes rather than hours. Both answers must be 200 and equal in content
//! (`source` aside, numbers compared as numbers: the Worker's join prints `50` where the object
//! holds `50.0`).
//!
//! A read is cold when it is the run's first of its legal dong (the pack unit), warm otherwise.
//! The bound holds the cold reads: the pack p50 and p95 may exceed the live ones of the same PNUs
//! by at most the contract's `latency_max_increase_ms`; warm reads are reported beside them. The
//! preview's own `Server-Timing` (R2 wait, where each section's pack came from) is summarised, and
//! failures are counted by side and class.
//!
//! The preview Worker's CPU over the probe window is read from Workers analytics: the account's
//! plan cuts a request off past its CPU limit (error 1102, an HTTP 503), so the gate demands no
//! `exceededResources` and a p99 within the contract's `worker_cpu_p99_max_ms`. Without the
//! analytics credentials the evidence records no CPU and never opens the gate. Evidence of two
//! stand-in routes (loopback or private addresses) is marked `local-simulation` and never opens
//! the gate either; CI proves the probe with such stand-ins.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use chrono::Utc;
use futures_util::{stream, StreamExt as _};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use super::super::{optional_env, LANE};
use super::analytics::{self, AnalyticsConfig, Invocations};
use super::equality::write_evidence;
use super::gate::{
    self, EqualityEvidence, Increase, LatencyEvidence, ServerTimingSummary, Timings,
};
use super::load::{self, LoadPlan};
use crate::by_pnu_gateway_contract::section_pack_policy;
use crate::r2_layout::by_pnu_packs;

pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub(crate) struct LatencyConfig {
    pub(crate) generation: u64,
    pub(crate) live_base_url: String,
    pub(crate) preview_base_url: String,
    /// The equality evidence whose sample the probe reads.
    pub(crate) equality_evidence: PathBuf,
    pub(crate) evidence_path: PathBuf,
    /// PNUs in flight at once.
    pub(crate) concurrency: usize,
    /// Where the preview Worker's CPU is read; `None` records none (the gate stays shut).
    pub(crate) analytics: Option<AnalyticsConfig>,
    /// The preview Worker's script name, which analytics is asked about.
    pub(crate) preview_script: String,
    /// The paced load against the preview after the paired reads; `None` runs none (the gate
    /// stays shut).
    pub(crate) load: Option<LoadPlan>,
}

impl LatencyConfig {
    fn from_env() -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&LANE.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
        };
        let gate = &section_pack_policy()?.cutover_gate;
        let concurrency = match env("PACK_PROBE_CONCURRENCY")? {
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .with_context(|| {
                    format!(
                        "{} must be a positive integer",
                        LANE.env("PACK_PROBE_CONCURRENCY")
                    )
                })?,
            None => gate.probe_concurrency,
        };
        let analytics = AnalyticsConfig::from_env()?;
        if analytics.is_none() {
            tracing::warn!(
                "{} are not set: the evidence records no Worker CPU and cannot pass",
                AnalyticsConfig::names()?
            );
        }
        Ok(Self {
            generation: required("PACK_GENERATION")?
                .parse()
                .context("the pack generation must be a number")?,
            live_base_url: env("PACK_LIVE_BASE_URL")?.map_or_else(
                || Ok(format!("https://{}", LANE.policy()?.public_hostname)),
                Ok::<_, anyhow::Error>,
            )?,
            preview_base_url: required("PACK_PREVIEW_BASE_URL")?,
            equality_evidence: PathBuf::from(required("PACK_EQUALITY_EVIDENCE_PATH")?),
            evidence_path: PathBuf::from(required("PACK_LATENCY_EVIDENCE_PATH")?),
            concurrency,
            analytics,
            preview_script: LANE
                .section_packs()?
                .preview_worker
                .as_ref()
                .context("the contract names no preview Worker for this lane")?
                .worker_name
                .clone(),
            load: Some(LoadPlan::from_contract(&gate.load_test)),
        })
    }
}

/// Runs the probe and writes the evidence; fails when the evidence does not pass.
///
/// # Errors
/// Returns an error when the sample cannot be read or the evidence does not pass.
pub(crate) async fn run() -> anyhow::Result<()> {
    let config = LatencyConfig::from_env()?;
    let pnus = sample(&config)?;
    let evidence = probe(&config, &pnus).await?;
    write_evidence(&config.evidence_path, &evidence)?;
    tracing::info!(
        environment = %evidence.environment,
        sample_size = evidence.sample_size,
        concurrency = evidence.concurrency,
        cold_live_p50 = evidence.live_ms.p50,
        cold_live_p95 = evidence.live_ms.p95,
        cold_pack_p50 = evidence.pack_ms.p50,
        cold_pack_p95 = evidence.pack_ms.p95,
        warm_live_p50 = evidence.live_warm_ms.p50,
        warm_pack_p50 = evidence.pack_warm_ms.p50,
        failures = ?evidence.failures,
        worker_cpu = ?evidence.worker_cpu,
        passed = evidence.passed,
        "building section pack live sample probed"
    );
    ensure!(
        evidence.passed,
        "the pack route answered differently, slower or costlier than the gate allows; see {}",
        config.evidence_path.display()
    );
    Ok(())
}

/// The equality evidence's sample, for the same generation.
///
/// # Errors
/// Returns an error when the evidence cannot be read, is of another generation, or its sample
/// does not match its own digest.
pub(crate) fn sample(config: &LatencyConfig) -> anyhow::Result<Vec<String>> {
    let (equality, _) = gate::read::<EqualityEvidence>(&config.equality_evidence)?;
    ensure!(
        equality.pack_generation == config.generation,
        "the equality evidence is of generation {}, the probe of {}",
        equality.pack_generation,
        config.generation
    );
    ensure!(
        gate::sample_digest(&equality.sample) == equality.sample_sha256,
        "the equality evidence's sample does not match its digest"
    );
    for pnu in &equality.sample {
        by_pnu_packs::unit_of(pnu)?;
    }
    Ok(equality.sample)
}

/// Whether a base URL is a stand-in: not https, or a loopback or private host.
fn is_stand_in(base_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return true;
    };
    if url.scheme() != "https" {
        return true;
    }
    let Some(host) = url.host_str() else {
        return true;
    };
    match host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
    {
        Ok(std::net::IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        Ok(std::net::IpAddr::V6(ip)) => ip.is_loopback(),
        Err(_) => host == "localhost" || host.ends_with(".localhost"),
    }
}

/// One timed answer.
#[derive(Debug)]
pub(crate) struct Answer {
    pub(crate) ms: f64,
    pub(crate) body: Vec<u8>,
    pub(crate) server_timing: Option<String>,
}

/// Why a read failed, in the evidence's words, and what the client said.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) class: String,
    pub(crate) detail: String,
}

/// Both reads of one PNU.
struct Probed {
    cold: bool,
    live: Result<Answer, Failure>,
    pack: Result<Answer, Failure>,
}

/// Which reads are cold: the first of each legal dong in the sample's order.
///
/// # Errors
/// Returns an error for a PNU too short to name its dong.
pub(crate) fn cold_reads(pnus: &[String]) -> anyhow::Result<Vec<bool>> {
    let mut seen = HashSet::new();
    pnus.iter()
        .map(|pnu| Ok(seen.insert(by_pnu_packs::unit_of(pnu)?.to_owned())))
        .collect()
}

/// The preview's URL for `pnu` at the probed generation.
fn preview_url(config: &LatencyConfig, prefix: &str, parameter: &str, pnu: &str) -> String {
    format!(
        "{}{prefix}{pnu}?{parameter}=g{}",
        config.preview_base_url.trim_end_matches('/'),
        config.generation
    )
}

/// Times both routes for every sampled PNU, `concurrency` PNUs at once, then reads the preview
/// Worker's CPU for the window from Workers analytics when the config names it.
///
/// # Errors
/// Returns an error when the HTTP client cannot be built or a PNU names no dong.
pub(crate) async fn probe(
    config: &LatencyConfig,
    pnus: &[String],
) -> anyhow::Result<LatencyEvidence> {
    let policy = section_pack_policy()?;
    let prefix = LANE.policy()?.request_path.prefix.clone();
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .pool_max_idle_per_host(config.concurrency.max(1))
        .build()?;
    let cold = cold_reads(pnus)?;
    let started_at = Utc::now();
    // Owned per-PNU work: a stream of borrows trips the closure's lifetime inference.
    let jobs = pnus
        .iter()
        .zip(cold)
        .enumerate()
        .map(|(index, (pnu, cold))| {
            let live_url = format!(
                "{}{prefix}{pnu}",
                config.live_base_url.trim_end_matches('/')
            );
            let pack_url = preview_url(config, &prefix, &policy.preview_query_parameter, pnu);
            (index, cold, live_url, pack_url)
        })
        .collect::<Vec<_>>();
    let probed = stream::iter(jobs)
        .map(|(index, cold, live_url, pack_url)| {
            let client = client.clone();
            async move {
                let (live, pack) = if index % 2 == 0 {
                    let live = timed_get(&client, &live_url).await;
                    (live, timed_get(&client, &pack_url).await)
                } else {
                    let pack = timed_get(&client, &pack_url).await;
                    (timed_get(&client, &live_url).await, pack)
                };
                (index, Probed { cold, live, pack })
            }
        })
        .buffer_unordered(config.concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let ended_at = Utc::now();
    let mut probed = probed;
    probed.sort_by_key(|(index, _)| *index);

    let mut tally = Tally::default();
    for (index, read) in &probed {
        let pnu = pnus
            .get(*index)
            .context("a probed index outside the sample")?;
        tally.add(pnu, read)?;
    }
    let worker_cpu = match &config.analytics {
        Some(analytics) => Some(
            analytics::worker_cpu(
                &client,
                analytics,
                &Invocations {
                    script: &config.preview_script,
                    version: None,
                    from: started_at,
                    to: ended_at + chrono::Duration::seconds(5),
                },
                u64::try_from(pnus.len())?,
            )
            .await?,
        ),
        None => None,
    };
    let load = match &config.load {
        Some(plan) => Some(
            load::run(
                &client,
                plan,
                pnus,
                |pnu| preview_url(config, &prefix, &policy.preview_query_parameter, pnu),
                config
                    .analytics
                    .as_ref()
                    .map(|analytics| (analytics, config.preview_script.as_str())),
            )
            .await?,
        ),
        None => None,
    };
    let live_ms = timings(&mut tally.live_cold);
    let pack_ms = timings(&mut tally.pack_cold);
    let live_warm_ms = timings(&mut tally.live_warm);
    let pack_warm_ms = timings(&mut tally.pack_warm);
    let slo = &policy.cutover_gate.slo;
    let sent = u64::try_from(pnus.len())?;
    #[allow(clippy::cast_precision_loss)]
    let availability = if sent == 0 {
        0.0
    } else {
        (sent - tally.pack_failed) as f64 / sent as f64
    };
    let stand_in = is_stand_in(&config.live_base_url) || is_stand_in(&config.preview_base_url);
    let mut evidence = LatencyEvidence {
        schema_version: policy.cutover_gate.evidence_schema_version.clone(),
        kind: gate::LATENCY_KIND.to_owned(),
        lane: LANE.unit().to_owned(),
        pack_generation: config.generation,
        live_base_url: config.live_base_url.clone(),
        preview_base_url: config.preview_base_url.clone(),
        environment: if stand_in {
            "local-simulation".to_owned()
        } else {
            gate::PRODUCTION_ENVIRONMENT.to_owned()
        },
        sample_size: u64::try_from(pnus.len())?,
        sample_sha256: gate::sample_digest(pnus),
        answered: tally.answered,
        mismatched: tally.mismatched,
        failed: tally.failed,
        increase_p50_ms: pack_ms.p50 - live_ms.p50,
        increase_p95_ms: pack_ms.p95 - live_ms.p95,
        bound_p50_ms: slo.latency_max_increase_ms.cold.p50,
        bound_p95_ms: slo.latency_max_increase_ms.cold.p95,
        examples: tally.examples,
        passed: false,
        measured_at_utc: crate::by_pnu_serving_manifest_publish::now(),
        concurrency: u64::try_from(config.concurrency)?,
        cold_answered: u64::try_from(tally.pack_cold_count)?,
        warm_answered: u64::try_from(tally.pack_warm.len())?,
        increase_cold_ms: Increase::between(&live_ms, &pack_ms),
        increase_warm_ms: Increase::between(&live_warm_ms, &pack_warm_ms),
        live_warm_ms,
        pack_warm_ms,
        availability,
        load,
        failures: tally.failures,
        server_timing: ServerTimingSummary {
            answers: u64::try_from(tally.server_total.len())?,
            total_ms: timings(&mut tally.server_total),
            r2_ms: timings(&mut tally.server_r2),
            sources: tally.sources,
        },
        worker_cpu,
        bound_cpu_p99_ms: policy.cutover_gate.worker_cpu_p99_max_ms,
        live_ms,
        pack_ms,
    };
    evidence.passed = evidence.verdict()?;
    Ok(evidence)
}

/// The probe's counts as the reads come in.
#[derive(Default)]
struct Tally {
    answered: u64,
    /// Preview reads that did not answer 200.
    pack_failed: u64,
    mismatched: u64,
    failed: u64,
    examples: Vec<String>,
    live_cold: Vec<f64>,
    pack_cold: Vec<f64>,
    pack_cold_count: usize,
    live_warm: Vec<f64>,
    pack_warm: Vec<f64>,
    failures: BTreeMap<String, u64>,
    server_total: Vec<f64>,
    server_r2: Vec<f64>,
    sources: BTreeMap<String, u64>,
}

impl Tally {
    fn add(&mut self, pnu: &str, read: &Probed) -> anyhow::Result<()> {
        if let Ok(pack) = &read.pack {
            if let Some(timing) = &pack.server_timing {
                let parsed = parse_server_timing(timing);
                if let Some(total) = parsed.total_ms {
                    self.server_total.push(total);
                }
                if let Some(r2) = parsed.r2_ms {
                    self.server_r2.push(r2);
                }
                for source in parsed.sources {
                    *self.sources.entry(source).or_default() += 1;
                }
            }
        }
        match (&read.live, &read.pack) {
            (Ok(live), Ok(pack)) => {
                self.answered += 1;
                if read.cold {
                    self.live_cold.push(live.ms);
                    self.pack_cold.push(pack.ms);
                    self.pack_cold_count += 1;
                } else {
                    self.live_warm.push(live.ms);
                    self.pack_warm.push(pack.ms);
                }
                if normalized_digest(&live.body)? != normalized_digest(&pack.body)? {
                    self.mismatched += 1;
                    self.example(pnu);
                }
            }
            (live, pack) => {
                self.failed += 1;
                if pack.is_err() {
                    self.pack_failed += 1;
                }
                for (side, outcome) in [("live", live), ("pack", pack)] {
                    if let Err(failure) = outcome {
                        *self
                            .failures
                            .entry(format!("{side}:{}", failure.class))
                            .or_default() += 1;
                    }
                }
                tracing::warn!(
                    pnu = %pnu,
                    live = ?live.as_ref().err().map(|failure| &failure.detail),
                    pack = ?pack.as_ref().err().map(|failure| &failure.detail),
                    "probe read failed"
                );
                self.example(pnu);
            }
        }
        Ok(())
    }

    fn example(&mut self, pnu: &str) {
        if self.examples.len() < gate::MAX_EXAMPLES {
            self.examples.push(pnu.to_owned());
        }
    }
}

/// One GET, timed until the whole body is in; anything but 200 is a failure of its class.
pub(crate) async fn timed_get(client: &reqwest::Client, url: &str) -> Result<Answer, Failure> {
    let started = Instant::now();
    let response = client.get(url).send().await.map_err(|error| Failure {
        class: request_class(&error, "request"),
        detail: format!("GET {url}: {error:#}"),
    })?;
    let status = response.status();
    let server_timing = response
        .headers()
        .get("server-timing")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.bytes().await.map_err(|error| Failure {
        class: request_class(&error, "body"),
        detail: format!("GET {url} body: {error:#}"),
    })?;
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    if status != reqwest::StatusCode::OK {
        return Err(Failure {
            class: format!("http-{}", status.as_u16()),
            detail: format!(
                "GET {url} answered {status}{}",
                server_timing
                    .as_deref()
                    .map_or_else(String::new, |timing| format!(" ({timing})"))
            ),
        });
    }
    Ok(Answer {
        ms,
        body: body.to_vec(),
        server_timing,
    })
}

fn request_class(error: &reqwest::Error, phase: &str) -> String {
    if error.is_timeout() {
        format!("{phase}-timeout")
    } else if error.is_connect() {
        "connect".to_owned()
    } else {
        phase.to_owned()
    }
}

/// What one `Server-Timing` header says (W3C Server Timing): `total` and `r2` durations, and the
/// description of every `pack-{section}` metric.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ParsedServerTiming {
    pub(crate) total_ms: Option<f64>,
    pub(crate) r2_ms: Option<f64>,
    pub(crate) sources: Vec<String>,
}

pub(crate) fn parse_server_timing(header: &str) -> ParsedServerTiming {
    let mut parsed = ParsedServerTiming::default();
    for metric in header.split(',') {
        let mut parts = metric.split(';').map(str::trim);
        let Some(name) = parts.next() else { continue };
        let (mut duration, mut description) = (None, None);
        for parameter in parts {
            if let Some(value) = parameter.strip_prefix("dur=") {
                duration = value.parse::<f64>().ok();
            } else if let Some(value) = parameter.strip_prefix("desc=") {
                description = Some(value.trim_matches('"').to_owned());
            }
        }
        match name {
            "total" => parsed.total_ms = duration,
            "r2" => parsed.r2_ms = duration,
            _ if name.starts_with("pack-") => {
                if let Some(description) = description {
                    parsed.sources.push(description);
                }
            }
            _ => {}
        }
    }
    parsed
}

/// Nearest-rank percentiles.
pub(crate) fn timings(samples: &mut [f64]) -> Timings {
    if samples.is_empty() {
        return Timings::default();
    }
    samples.sort_by(f64::total_cmp);
    let rank = |p: f64| {
        let index = (p * samples.len() as f64).ceil() as usize;
        samples[index.clamp(1, samples.len()) - 1]
    };
    Timings {
        p50: rank(0.50),
        p95: rank(0.95),
        p99: rank(0.99),
        mean: samples.iter().sum::<f64>() / samples.len() as f64,
        max: samples[samples.len() - 1],
    }
}

/// SHA-256 of a document's content: `source` removed, keys sorted, every number as an `f64`.
pub(crate) fn normalized_digest(bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
    let mut document: JsonValue =
        serde_json::from_slice(bytes).context("the answer is not JSON")?;
    document
        .as_object_mut()
        .context("the answer is not a JSON object")?
        .remove("source");
    let mut out = String::new();
    write_normalized(&document, &mut out)?;
    Ok(Sha256::digest(out.as_bytes()).into())
}

fn write_normalized(value: &JsonValue, out: &mut String) -> anyhow::Result<()> {
    match value {
        JsonValue::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key)?);
                out.push(':');
                write_normalized(&map[key], out)?;
            }
            out.push('}');
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_normalized(item, out)?;
            }
            out.push(']');
        }
        JsonValue::Number(number) => {
            let value = number.as_f64().context("a number does not fit an f64")?;
            out.push_str(&format!("{value:?}"));
        }
        scalar => out.push_str(&serde_json::to_string(scalar)?),
    }
    Ok(())
}
