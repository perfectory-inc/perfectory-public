//! `probe-building-by-pnu-section-pack-latency`: gate (나) of the cut-over (root ADR-0147 §6), the
//! live check after upload.
//!
//! The sample is the one the equality evidence drew (a seeded hash of the PNU over the whole bake,
//! `equality.rs`): about 10,000 PNUs, so the check costs about 10,000 live object reads and 10,000
//! preview requests, not a read of every pack. Each PNU is read once from the live route (objects)
//! and once from a preview Worker version serving the unpublished pack generation
//! (`?{preview_query_parameter}=g{N}`), alternating which goes first, timed until the whole body is
//! in. Each read is that URL's first: cold at the edge cache for the packs, and at worst warm for
//! the live route — which only makes the comparison stricter. Both answers must be 200 and equal in
//! content (`source` aside, numbers compared as numbers: the Worker's join prints `50` where the
//! object holds `50.0`).
//!
//! Verdict: the pack p50 and p95 may exceed the live ones by at most the contract's
//! `latency_max_increase_ms`. Evidence of two stand-in routes (loopback or private addresses) is
//! marked `local-simulation` and never opens the gate; CI proves the probe with such stand-ins.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use super::super::{optional_env, LANE};
use super::equality::write_evidence;
use super::gate::{self, EqualityEvidence, LatencyEvidence, Timings};
use crate::by_pnu_gateway_contract::section_pack_policy;
use crate::r2_layout::by_pnu_packs;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub(crate) struct LatencyConfig {
    pub(crate) generation: u64,
    pub(crate) live_base_url: String,
    pub(crate) preview_base_url: String,
    /// The equality evidence whose sample the probe reads.
    pub(crate) equality_evidence: PathBuf,
    pub(crate) evidence_path: PathBuf,
}

impl LatencyConfig {
    fn from_env() -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&LANE.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
        };
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
        live_p50 = evidence.live_ms.p50,
        live_p95 = evidence.live_ms.p95,
        pack_p50 = evidence.pack_ms.p50,
        pack_p95 = evidence.pack_ms.p95,
        passed = evidence.passed,
        "building section pack live sample probed"
    );
    ensure!(
        evidence.passed,
        "the pack route answered differently or slower than the gate allows; see {}",
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

/// Times both routes for every sampled PNU.
///
/// # Errors
/// Returns an error when the HTTP client cannot be built.
pub(crate) async fn probe(
    config: &LatencyConfig,
    pnus: &[String],
) -> anyhow::Result<LatencyEvidence> {
    let policy = section_pack_policy()?;
    let prefix = &LANE.policy()?.request_path.prefix;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    let (mut live, mut packs) = (Vec::new(), Vec::new());
    let (mut answered, mut mismatched, mut failed) = (0_u64, 0_u64, 0_u64);
    let mut examples = Vec::new();
    for (index, pnu) in pnus.iter().enumerate() {
        let live_url = format!(
            "{}{prefix}{pnu}",
            config.live_base_url.trim_end_matches('/')
        );
        let pack_url = format!(
            "{}{prefix}{pnu}?{}=g{}",
            config.preview_base_url.trim_end_matches('/'),
            policy.preview_query_parameter,
            config.generation
        );
        let (live_read, pack_read) = if index % 2 == 0 {
            let live_read = timed_get(&client, &live_url).await;
            (live_read, timed_get(&client, &pack_url).await)
        } else {
            let pack_read = timed_get(&client, &pack_url).await;
            (timed_get(&client, &live_url).await, pack_read)
        };
        match (live_read, pack_read) {
            (Ok((live_ms, live_body)), Ok((pack_ms, pack_body))) => {
                answered += 1;
                live.push(live_ms);
                packs.push(pack_ms);
                if normalized_digest(&live_body)? != normalized_digest(&pack_body)? {
                    mismatched += 1;
                    if examples.len() < gate::MAX_EXAMPLES {
                        examples.push(pnu.clone());
                    }
                }
            }
            (live_read, pack_read) => {
                failed += 1;
                tracing::warn!(pnu = %pnu, live = ?live_read.err().map(|e| format!("{e:#}")), pack = ?pack_read.err().map(|e| format!("{e:#}")), "probe read failed");
                if examples.len() < gate::MAX_EXAMPLES {
                    examples.push(pnu.clone());
                }
            }
        }
    }
    let live_ms = timings(&mut live);
    let pack_ms = timings(&mut packs);
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
        answered,
        mismatched,
        failed,
        increase_p50_ms: pack_ms.p50 - live_ms.p50,
        increase_p95_ms: pack_ms.p95 - live_ms.p95,
        live_ms,
        pack_ms,
        bound_p50_ms: policy.cutover_gate.latency_max_increase_ms.p50,
        bound_p95_ms: policy.cutover_gate.latency_max_increase_ms.p95,
        examples,
        passed: false,
        measured_at_utc: crate::by_pnu_serving_manifest_publish::now(),
    };
    evidence.passed = evidence.verdict()?;
    Ok(evidence)
}

/// One GET, timed until the whole body is in; anything but 200 is a failure.
async fn timed_get(client: &reqwest::Client, url: &str) -> anyhow::Result<(f64, Vec<u8>)> {
    let started = Instant::now();
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("GET {url} body"))?;
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    ensure!(
        status == reqwest::StatusCode::OK,
        "GET {url} answered {status}"
    );
    Ok((elapsed, body.to_vec()))
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
