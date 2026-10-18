//! `probe-building-by-pnu-section-pack-latency`: gate (나) of the cut-over (root ADR-0147 §6), the
//! live check after upload.
//!
//! The sample is the one the equality evidence drew (a seeded hash of the PNU over the whole bake,
//! `equality.rs`): about 10,000 PNUs, so the check costs about 10,000 live object reads and 10,000
//! preview requests, not a read of every pack. Each PNU is read once from the object path and
//! once from a preview Worker serving the unpublished pack generation
//! (`?{preview_query_parameter}=g{N}`), alternating which goes first, each timed on its own until
//! the whole body is in. The contract's `probe_concurrency` PNUs are in flight at once, so the
//! sample finishes in minutes rather than hours. Both answers must be 200 and equal in content
//! (`source` aside, numbers compared as numbers: the Worker's join prints `50` where the object
//! holds `50.0`).
//!
//! The object path is the preview's own (its URL without the query) unless `PACK_LIVE_BASE_URL`
//! names another: the same Worker at the same placement, with edge copies of its own version, so
//! a fresh preview upload reads R2 on both sides, and both sides say so in `Server-Timing`. The
//! gate counts each cold read by where each side answered it from and holds both to
//! `slo.cold_reads_not_from_r2_max_share`: on 2026-10-07 the pack side was first measured against
//! its own edge copies, then against the production route's (contract `comparison_reason`).
//!
//! A read is cold when it is the run's first of its legal dong (the pack unit), warm otherwise.
//! The bound holds the cold reads: the pack p50 and p95 may exceed the live ones of the same PNUs
//! by at most the contract's `latency_max_increase_ms`; warm reads are reported beside them. The
//! preview's own `Server-Timing` (R2 wait, where each section's pack came from) is summarised, and
//! failures are counted by side and class.
//!
//! Every read asks for gzip (`Accept-Encoding: gzip`), as a browser does: the pack path's point is
//! to hand the stored member out as it is, so a pack answer without `Content-Encoding: gzip` is a
//! failure (`pack:not-gzip`), and the evidence records which encoding each side answered in and
//! which path the preview took (its `Server-Timing` outcome). A further `no_gzip_sample_size` of
//! the sample is then read from the preview with `Accept-Encoding: identity`, which the edge
//! answers by decoding the member the Worker handed out (root ADR-0233): each must answer 200,
//! uncompressed, with the live route's content.
//!
//! The preview Worker's CPU over the probe window is read from Workers analytics: the account's
//! plan cuts a request off past its CPU limit (error 1102, an HTTP 503), so the gate demands no
//! `exceededResources` and a p99 within the contract's `worker_cpu_p99_max_ms`. Without the
//! analytics credentials (the runtime-secrets contract's `cloudflare-analytics` group) the probe refuses to
//! start, naming the file; evidence that records no CPU never opens the gate either. Evidence of two
//! stand-in routes (loopback or private addresses) is marked `local-simulation` and never opens
//! the gate either; CI proves the probe with such stand-ins.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use chrono::Utc;
use futures_util::{stream, StreamExt as _};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use super::super::optional_env;
use super::analytics::{self, AnalyticsConfig, Invocations};
use super::equality::write_evidence;
use super::gate::{
    self, EqualityEvidence, Increase, LatencyEvidence, NoGzipEvidence, ServerTimingSummary, Timings,
};
use super::load::{self, LoadPlan};
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};
use crate::r2_layout::by_pnu_packs;

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub struct LatencyConfig {
    pub lane: ByPnuLane,
    pub generation: u64,
    pub live_base_url: String,
    pub preview_base_url: String,
    /// The equality evidence whose sample the probe reads.
    pub equality_evidence: PathBuf,
    pub evidence_path: PathBuf,
    /// PNUs in flight at once.
    pub concurrency: usize,
    /// Where the preview Worker's CPU is read; `None` records none (the gate stays shut).
    pub analytics: Option<AnalyticsConfig>,
    /// The preview Worker's script name, which analytics is asked about.
    pub preview_script: String,
    /// The paced load against the preview after the paired reads; `None` runs none (the gate
    /// stays shut).
    pub load: Option<LoadPlan>,
    /// How many sample PNUs are read again from the preview without gzip; 0 reads none (the gate
    /// stays shut).
    pub no_gzip_sample_size: usize,
    /// The unit of each sampled PNU, from the equality evidence (root ADR-0163): a read is cold on
    /// the first of its unit. Empty, each PNU's unit is its dong.
    pub sample_units: Vec<String>,
    /// A pack-only lane's comparison (root ADR-0190); `None` for a lane with an object path.
    pub baseline: Option<Baseline>,
    /// This probe run's token in the preview query (`g{N}-r{run}`): the preview keys its edge
    /// copies by it, so a run never reads copies an earlier run left on either side.
    pub run: String,
}

/// What a pack-only lane is measured against: it has no object path of its own, so its preview is
/// timed against the live preview of `latency_baseline_lane` reading the same PNUs (the same Worker
/// code at the same placement), and its content is held to the documents the bake wrote, read
/// back from the unpublished packs in R2.
#[derive(Clone, Debug)]
pub struct Baseline {
    pub lane: ByPnuLane,
    /// The baseline lane's request path prefix, under `live_base_url`.
    pub prefix: String,
    /// The pack generation the baseline lane serves, read from its manifest before the probe: the
    /// baseline is read as its own pack route at that generation, scoped to this run, so both
    /// sides' first reads are R2 reads (2026-10-13: the baseline's live route answered 9,502 of
    /// 9,505 cold reads from edge copies an earlier run left, against the kind lane's R2 reads).
    pub generation: Option<u64>,
    /// Each sampled PNU's document as the packs hold it; filled before the probe.
    pub expected: HashMap<String, [u8; 32]>,
}

impl LatencyConfig {
    fn from_env(lane: ByPnuLane) -> anyhow::Result<Self> {
        let env = |name: &str| optional_env(&lane.env(name));
        let required = |name: &str| -> anyhow::Result<String> {
            env(name)?.with_context(|| format!("{} is required", lane.env(name)))
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
                        lane.env("PACK_PROBE_CONCURRENCY")
                    )
                })?,
            None => gate.probe_concurrency,
        };
        // Evidence without the Worker's CPU can never open the gate, so the probe refuses before
        // spending half an hour of reads on it.
        let analytics = Some(AnalyticsConfig::required("gate (나)")?);
        let preview_base_url = required("PACK_PREVIEW_BASE_URL")?;
        let baseline = match lane.pack_lane() {
            None => None,
            Some(pack) => {
                let baseline = ByPnuLane::named(&pack.latency_baseline_lane)?
                    .with_context(|| format!("no lane is named {}", pack.latency_baseline_lane))?;
                Some(Baseline {
                    lane: baseline,
                    prefix: baseline.policy()?.request_path.prefix.clone(),
                    generation: None,
                    expected: HashMap::new(),
                })
            }
        };
        // A pack-only lane has no object path: its live side is the baseline lane's preview.
        let live_default = match &baseline {
            None => Some(preview_base_url.clone()),
            Some(baseline) => baseline
                .lane
                .section_packs()?
                .preview_worker
                .as_ref()
                .and_then(|preview| preview.public_hostname.as_ref())
                .map(|host| format!("https://{host}")),
        };
        Ok(Self {
            lane,
            generation: required("PACK_GENERATION")?
                .parse()
                .context("the pack generation must be a number")?,
            // The object side is the preview's own object path unless named: same Worker, same
            // placement, edge copies of its own version, so both sides read R2 cold and say so
            // (contract `cutover_gate.comparison_reason`).
            live_base_url: match env("PACK_LIVE_BASE_URL")? {
                Some(url) => url,
                None => live_default.with_context(|| {
                    format!(
                        "{} is required: the baseline lane names no preview hostname",
                        lane.env("PACK_LIVE_BASE_URL")
                    )
                })?,
            },
            preview_base_url,
            equality_evidence: PathBuf::from(required("PACK_EQUALITY_EVIDENCE_PATH")?),
            evidence_path: PathBuf::from(required("PACK_LATENCY_EVIDENCE_PATH")?),
            concurrency,
            analytics,
            preview_script: lane
                .section_packs()?
                .preview_worker
                .as_ref()
                .context("the contract names no preview Worker for this lane")?
                .worker_name
                .clone(),
            load: Some(LoadPlan::from_contract(
                &gate.load_test,
                load::LOAD_DRAW.to_owned(),
            )),
            no_gzip_sample_size: gate.no_gzip_sample_size,
            sample_units: Vec::new(),
            baseline,
            run: format!("{:x}", Utc::now().timestamp()),
        })
    }
}

/// Runs the probe and writes the evidence; fails when the evidence does not pass.
///
/// # Errors
/// Returns an error when the sample cannot be read or the evidence does not pass.
pub async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    let mut config = LatencyConfig::from_env(lane)?;
    let pnus = sample(&config)?;
    let (equality, _) = gate::read::<EqualityEvidence>(&config.equality_evidence)?;
    config.sample_units = equality.sample_units;
    if let Some(baseline) = &mut config.baseline {
        baseline.expected =
            expected_documents(lane, config.generation, &pnus, config.concurrency).await?;
        baseline.generation = Some(served_generation(lane, baseline.lane).await?);
    }
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
        lane = lane.unit(),
        "section pack live sample probed"
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
pub fn sample(config: &LatencyConfig) -> anyhow::Result<Vec<String>> {
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

/// Each sampled PNU's document in the unpublished generation, read back from the lane's packs as
/// the gateway would answer it (`inspect`), as the content digest the probe compares answers by.
/// The bake already held those bytes to the Gold rows (gate 가); this holds the Worker to them.
///
/// # Errors
/// Returns an error when the lane's store cannot be opened or a sampled PNU has no document.
pub async fn expected_documents(
    lane: ByPnuLane,
    generation: u64,
    pnus: &[String],
    concurrency: usize,
) -> anyhow::Result<HashMap<String, [u8; 32]>> {
    let env = |name: &str| optional_env(&lane.env(name));
    let output = crate::industrial_complex_gold_profile_store::ProfileStoreConfig::parse(
        env("OUTPUT_STORAGE_DRIVER")?
            .unwrap_or_else(|| "local".to_owned())
            .as_str(),
        crate::by_pnu_serving_store::local_root(env("OUTPUT_ROOT")?),
    )?;
    let store = crate::by_pnu_serving_store::ByPnuServingStore::open(lane, &output)?;
    let store = &store;
    let read = stream::iter(pnus.iter().cloned())
        .map(|pnu| async move {
            let report = super::inspect::inspect(store, &pnu, Some(generation)).await?;
            let document = report
                .get("document")
                .filter(|document| !document.is_null())
                .with_context(|| {
                    format!("the packs of generation {generation} hold no document of {pnu}")
                })?;
            let digest = normalized_digest(&serde_json::to_vec(document)?)?;
            anyhow::Ok((pnu, digest))
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    read.into_iter().collect()
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

/// What a read asks for in `Accept-Encoding`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Accept {
    /// What browsers send: the pack path answers with the stored member as it is.
    Gzip,
    /// A client without gzip: the edge decodes the member the Worker handed out (root ADR-0233).
    Identity,
}

impl Accept {
    fn header(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Identity => "identity",
        }
    }
}

/// One timed answer. It keeps the content digest of its body, never the body: the probe holds
/// every answer of its sample until it tallies them, and 2026-10-16 the unit price lane's 10,000
/// pairs of decoded documents took the gate (나) probe to 8 GB and the OOM killer.
#[derive(Debug)]
pub struct Answer {
    pub ms: f64,
    /// `normalized_digest` of the decoded body, or why the body has none (not a JSON object).
    pub digest: Result<[u8; 32], String>,
    pub server_timing: Option<String>,
    /// The answer's `Content-Encoding`, `None` when it had none.
    pub content_encoding: Option<String>,
    /// The Worker version that produced the answer, as it names it in the lane's
    /// `version_header` (root ADR-0157); `None` when it named none.
    pub version: Option<String>,
}

impl Answer {
    /// The answer's content digest, or the error its body gave.
    ///
    /// # Errors
    /// Returns an error when the body was not a JSON object.
    pub fn content(&self) -> anyhow::Result<[u8; 32]> {
        self.digest.clone().map_err(anyhow::Error::msg)
    }

    /// The encoding the answer came in, `identity` when it named none.
    pub fn encoding(&self) -> &str {
        self.content_encoding.as_deref().unwrap_or("identity")
    }
}

/// A pack answer that did not come gzip is a failure: the member was not passed through.
pub fn require_gzip(answer: Result<Answer, Failure>, url: &str) -> Result<Answer, Failure> {
    match answer {
        Ok(answer) if answer.content_encoding.as_deref() != Some("gzip") => Err(Failure {
            class: "not-gzip".to_owned(),
            detail: format!(
                "GET {url} asked for gzip and answered {} ({})",
                answer.encoding(),
                answer
                    .server_timing
                    .as_deref()
                    .unwrap_or("no Server-Timing")
            ),
        }),
        other => other,
    }
}

/// Why a read failed, in the evidence's words, and what the client said.
#[derive(Debug)]
pub struct Failure {
    pub class: String,
    pub detail: String,
}

/// Both reads of one PNU.
struct Probed {
    cold: bool,
    live: Result<Answer, Failure>,
    pack: Result<Answer, Failure>,
}

/// Which reads are cold: the first of each unit in the sample's order. `units` are the sample's
/// units as the equality evidence names them (a dong, or one part of it, root ADR-0163); without
/// them each PNU's unit is its dong.
///
/// # Errors
/// Returns an error for a PNU too short to name its dong, or units that are not the sample's.
pub fn cold_reads(pnus: &[String], units: &[String]) -> anyhow::Result<Vec<bool>> {
    ensure!(
        units.is_empty() || units.len() == pnus.len(),
        "the evidence names {} units for {} sample PNUs",
        units.len(),
        pnus.len()
    );
    let mut seen = HashSet::new();
    pnus.iter()
        .enumerate()
        .map(|(index, pnu)| {
            let unit = match units.get(index) {
                Some(unit) => {
                    ensure!(
                        by_pnu_packs::dong_of(unit) == by_pnu_packs::unit_of(pnu)?,
                        "unit {unit} is not of {pnu}'s dong"
                    );
                    unit.clone()
                }
                None => by_pnu_packs::unit_of(pnu)?.to_owned(),
            };
            Ok(seen.insert(unit))
        })
        .collect()
}

/// The pack generation `baseline` serves now, by its manifest in the store `lane` writes to.
///
/// # Errors
/// Returns an error when the manifest cannot be read or names no pack generation.
pub async fn served_generation(lane: ByPnuLane, baseline: ByPnuLane) -> anyhow::Result<u64> {
    let env = |name: &str| optional_env(&lane.env(name));
    let output = crate::industrial_complex_gold_profile_store::ProfileStoreConfig::parse(
        env("OUTPUT_STORAGE_DRIVER")?
            .unwrap_or_else(|| "local".to_owned())
            .as_str(),
        crate::by_pnu_serving_store::local_root(env("OUTPUT_ROOT")?),
    )?;
    let store = crate::by_pnu_serving_store::ByPnuServingStore::open(baseline, &output)?;
    let manifest = crate::by_pnu_serving_manifest::ServedManifest::parse(
        baseline,
        &store.read_manifest().await?.0,
    )?;
    let anchor = &baseline.section_packs()?.anchor_section;
    manifest
        .section_packs
        .as_ref()
        .and_then(|packs| {
            packs
                .sections
                .iter()
                .find(|section| &section.name == anchor)
        })
        .map(|section| section.generation)
        .with_context(|| {
            format!(
                "the {} lane serves no pack generation to time a pack-only lane against",
                baseline.noun()
            )
        })
}

/// The query that names `generation` on a preview, scoped to this probe run: the preview's edge
/// copies are keyed by it, so each run's first read of a document is an R2 read on both sides.
fn preview_query(config: &LatencyConfig, parameter: &str, generation: u64) -> String {
    format!("?{parameter}=g{generation}-r{}", config.run)
}

/// The preview's URL for the key `pnu` at the probed generation.
pub(super) fn preview_url(
    config: &LatencyConfig,
    prefix: &str,
    parameter: &str,
    pnu: &str,
) -> String {
    format!(
        "{}{prefix}{}{}",
        config.preview_base_url.trim_end_matches('/'),
        by_pnu_packs::request_path_of(pnu),
        preview_query(config, parameter, config.generation)
    )
}

/// The live URL gate (나) compares against for the key `pnu`: the lane's own, or its baseline
/// lane's for the key's PNU (root ADR-0190, ADR-0204: the baseline knows PNUs only).
pub(super) fn live_url(
    config: &LatencyConfig,
    live_prefix: &str,
    parameter: &str,
    pnu: &str,
) -> String {
    // A pack-only lane's baseline is the baseline lane's own pack route at the generation it
    // serves, read through its preview in this run's scope: pack route against pack route, both
    // cold on their first read. A lane with an object path is timed against that path.
    let (path, query) = match &config.baseline {
        Some(baseline) => (
            by_pnu_packs::pnu_of(pnu).to_owned(),
            baseline
                .generation
                .map(|generation| preview_query(config, parameter, generation))
                .unwrap_or_default(),
        ),
        None => (by_pnu_packs::request_path_of(pnu), String::new()),
    };
    format!(
        "{}{live_prefix}{path}{query}",
        config.live_base_url.trim_end_matches('/')
    )
}

/// Times both routes for every sampled PNU, `concurrency` PNUs at once, then reads the preview
/// Worker's CPU for the window from Workers analytics when the config names it.
///
/// # Errors
/// Returns an error when the HTTP client cannot be built or a PNU names no dong.
pub async fn probe(config: &LatencyConfig, pnus: &[String]) -> anyhow::Result<LatencyEvidence> {
    let policy = section_pack_policy()?;
    let lane = config.lane;
    let prefix = lane.policy()?.request_path.prefix.clone();
    let live_prefix = config
        .baseline
        .as_ref()
        .map_or_else(|| prefix.clone(), |baseline| baseline.prefix.clone());
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .pool_max_idle_per_host(config.concurrency.max(1))
        .build()?;
    let cold = cold_reads(pnus, &config.sample_units)?;
    let started_at = Utc::now();
    // Owned per-PNU work: a stream of borrows trips the closure's lifetime inference.
    let jobs = pnus
        .iter()
        .zip(cold)
        .enumerate()
        .map(|(index, (pnu, cold))| {
            let live_url = live_url(config, &live_prefix, &policy.preview_query_parameter, pnu);
            let pack_url = preview_url(config, &prefix, &policy.preview_query_parameter, pnu);
            (index, cold, live_url, pack_url)
        })
        .collect::<Vec<_>>();
    let probed = stream::iter(jobs)
        .map(|(index, cold, live_url, pack_url)| {
            let client = client.clone();
            async move {
                let (live, pack) = if index % 2 == 0 {
                    let live = timed_get(lane, &client, &live_url, Accept::Gzip).await;
                    (
                        live,
                        timed_get(lane, &client, &pack_url, Accept::Gzip).await,
                    )
                } else {
                    let pack = timed_get(lane, &client, &pack_url, Accept::Gzip).await;
                    (
                        timed_get(lane, &client, &live_url, Accept::Gzip).await,
                        pack,
                    )
                };
                let pack = require_gzip(pack, &pack_url);
                (index, Probed { cold, live, pack })
            }
        })
        .buffer_unordered(config.concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let ended_at = Utc::now();
    let mut probed = probed;
    probed.sort_by_key(|(index, _)| *index);

    let mut tally = Tally {
        expected: config
            .baseline
            .as_ref()
            .map(|baseline| baseline.expected.clone()),
        ..Tally::default()
    };
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
    let no_gzip = if config.no_gzip_sample_size == 0 {
        None
    } else {
        let take = config.no_gzip_sample_size.min(pnus.len());
        Some(
            read_without_gzip(
                lane,
                &client,
                &pnus[..take],
                |pnu| preview_url(config, &prefix, &policy.preview_query_parameter, pnu),
                tally.expected.as_ref().unwrap_or(&tally.live_digests),
                config.concurrency,
            )
            .await?,
        )
    };
    let load = match &config.load {
        Some(plan) => Some(
            load::run(
                lane,
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
        lane: lane.unit().to_owned(),
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
            outcomes: tally.outcomes,
        },
        encodings: tally.encodings,
        cold_read_paths: tally.cold_read_paths,
        cold_live_read_paths: tally.cold_live_read_paths,
        no_gzip,
        worker_cpu,
        bound_cpu_p99_ms: policy.cutover_gate.worker_cpu_p99_max_ms,
        baseline_lane: config
            .baseline
            .as_ref()
            .map(|baseline| baseline.lane.name().to_owned()),
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
    outcomes: BTreeMap<String, u64>,
    encodings: BTreeMap<String, u64>,
    /// The cold reads by where the preview answered them from (`gate::cold_read_path`).
    cold_read_paths: BTreeMap<String, u64>,
    /// The same on the object side, which only the preview's object path says.
    cold_live_read_paths: BTreeMap<String, u64>,
    /// The live answer's content per PNU, which the no-gzip reads are held to.
    live_digests: HashMap<String, [u8; 32]>,
    /// A pack-only lane's documents as its packs hold them: its preview's answers are held to these
    /// instead of to the baseline lane's, which are another lane's documents.
    expected: Option<HashMap<String, [u8; 32]>>,
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
                if let Some(outcome) = parsed.outcome {
                    *self.outcomes.entry(outcome).or_default() += 1;
                }
            }
        }
        for (side, outcome) in [("live", &read.live), ("pack", &read.pack)] {
            if let Ok(answer) = outcome {
                *self
                    .encodings
                    .entry(format!("{side}:{}", answer.encoding()))
                    .or_default() += 1;
            }
        }
        if let Ok(live) = &read.live {
            if let Ok(digest) = live.digest {
                self.live_digests.insert(pnu.to_owned(), digest);
            }
        }
        match (&read.live, &read.pack) {
            (Ok(live), Ok(pack)) => {
                self.answered += 1;
                if read.cold {
                    self.live_cold.push(live.ms);
                    self.pack_cold.push(pack.ms);
                    self.pack_cold_count += 1;
                    for (answer, paths) in [
                        (pack, &mut self.cold_read_paths),
                        (live, &mut self.cold_live_read_paths),
                    ] {
                        let sources = answer
                            .server_timing
                            .as_deref()
                            .map(|timing| parse_server_timing(timing).sources)
                            .unwrap_or_default();
                        *paths
                            .entry(gate::cold_read_path(&sources).to_owned())
                            .or_default() += 1;
                    }
                } else {
                    self.live_warm.push(live.ms);
                    self.pack_warm.push(pack.ms);
                }
                let wanted = match &self.expected {
                    None => Some(live.content()?),
                    Some(expected) => expected.get(pnu).copied(),
                };
                if wanted != Some(pack.content()?) {
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

/// One GET asking for `accept`, timed until the whole body is in; anything but 200 is a failure
/// of its class. A gzip body is decoded after the clock stops (the client's work, not the
/// route's); an encoding not asked for is a failure.
pub async fn timed_get(
    lane: ByPnuLane,
    client: &reqwest::Client,
    url: &str,
    accept: Accept,
) -> Result<Answer, Failure> {
    let started = Instant::now();
    let response = client
        .get(url)
        .header(reqwest::header::ACCEPT_ENCODING, accept.header())
        .send()
        .await
        .map_err(|error| Failure {
            class: request_class(&error, "request"),
            detail: format!("GET {url}: {error:#}"),
        })?;
    let status = response.status();
    let server_timing = response
        .headers()
        .get("server-timing")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    // A contract that cannot be read names no header: the answer then names no version, which a
    // canary step counts against the new version, never for it.
    let version = lane
        .policy()
        .ok()
        .and_then(|policy| policy.version_header.as_deref())
        .and_then(|name| response.headers().get(name))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let outage = crate::by_pnu_gateway_contract::section_pack_policy()
        .ok()
        .and_then(|policy| {
            response
                .headers()
                .get(policy.read_path.outage_header.as_str())
        })
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_encoding = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && value != "identity");
    let body = response.bytes().await.map_err(|error| Failure {
        class: request_class(&error, "body"),
        detail: format!("GET {url} body: {error:#}"),
    })?;
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    if status != reqwest::StatusCode::OK {
        // A 503 names its class (contract `read_path.outage_header`): `http-503:manifest-unavailable`
        // says which read failed, where `http-503` alone left the 2026-10-18 monitor failure untraced.
        return Err(Failure {
            class: outage.map_or_else(
                || format!("http-{}", status.as_u16()),
                |outage| format!("http-{}:{outage}", status.as_u16()),
            ),
            detail: format!(
                "GET {url} answered {status}{}",
                server_timing
                    .as_deref()
                    .map_or_else(String::new, |timing| format!(" ({timing})"))
            ),
        });
    }
    let body = match content_encoding.as_deref() {
        None => body.to_vec(),
        Some("gzip") => {
            let mut decoded = Vec::new();
            flate2::read::MultiGzDecoder::new(body.as_ref())
                .read_to_end(&mut decoded)
                .map_err(|error| Failure {
                    class: "gzip-decode".to_owned(),
                    detail: format!("GET {url} answered gzip that does not decode: {error}"),
                })?;
            decoded
        }
        Some(other) => {
            return Err(Failure {
                class: format!("encoding-{other}"),
                detail: format!(
                    "GET {url} asked for {} and answered {other}",
                    accept.header()
                ),
            })
        }
    };
    // Only the digest outlives this read (see `Answer`).
    let digest = normalized_digest(&body).map_err(|error| format!("{error:#}"));
    Ok(Answer {
        ms,
        digest,
        server_timing,
        content_encoding,
        version,
    })
}

/// The no-gzip sample: `pnus` read from the preview with `Accept-Encoding: identity`, each held to
/// the live answer's content for the same PNU.
///
/// # Errors
/// Returns an error when a count does not fit.
pub async fn read_without_gzip(
    lane: ByPnuLane,
    client: &reqwest::Client,
    pnus: &[String],
    url_of: impl Fn(&str) -> String,
    live_digests: &HashMap<String, [u8; 32]>,
    concurrency: usize,
) -> anyhow::Result<NoGzipEvidence> {
    let jobs = pnus
        .iter()
        .map(|pnu| (pnu.clone(), url_of(pnu)))
        .collect::<Vec<_>>();
    let answers = stream::iter(jobs)
        .map(|(pnu, url)| {
            let client = client.clone();
            async move { (pnu, timed_get(lane, &client, &url, Accept::Identity).await) }
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let mut evidence = NoGzipEvidence {
        sent: u64::try_from(pnus.len())?,
        ..NoGzipEvidence::default()
    };
    let mut latency = Vec::new();
    for (pnu, answer) in answers {
        let answer = match answer {
            Ok(answer) => answer,
            Err(failure) => {
                *evidence.failures.entry(failure.class).or_default() += 1;
                continue;
            }
        };
        evidence.answered += 1;
        latency.push(answer.ms);
        if answer.content_encoding.is_none() {
            evidence.identity += 1;
        }
        if let Some(outcome) = answer
            .server_timing
            .as_deref()
            .and_then(|timing| parse_server_timing(timing).outcome)
        {
            *evidence.outcomes.entry(outcome).or_default() += 1;
        }
        match live_digests.get(&pnu) {
            Some(live) if answer.digest.as_ref().ok() != Some(live) => {
                evidence.mismatched += 1;
            }
            Some(_) => {}
            None => evidence.not_compared += 1,
        }
    }
    evidence.latency_ms = timings(&mut latency);
    Ok(evidence)
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
pub struct ParsedServerTiming {
    /// The path the Worker took (`document`, `r2-unavailable`, …).
    pub outcome: Option<String>,
    pub total_ms: Option<f64>,
    pub r2_ms: Option<f64>,
    pub sources: Vec<String>,
}

pub fn parse_server_timing(header: &str) -> ParsedServerTiming {
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
            "outcome" => parsed.outcome = description,
            "total" => parsed.total_ms = duration,
            "r2" => parsed.r2_ms = duration,
            _ if name.starts_with("pack-") || name == "object" => {
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
pub fn timings(samples: &mut [f64]) -> Timings {
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
pub fn normalized_digest(bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
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
