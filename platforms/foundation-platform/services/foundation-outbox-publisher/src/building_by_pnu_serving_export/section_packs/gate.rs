//! The cut-over gate's evidence (root ADR-0147 §6): what the equality check and the live probe
//! write, and what the first pack publish demands of it.
//!
//! - Gate (가) is computed where the packs are baked, from the same Gold snapshot, at no R2 cost:
//!   every document the bake writes into a pack is read back from the pack bytes and compared with
//!   the object document the same row renders (`bake.rs`), and the evidence adds up every shard.
//! - Gate (나) is the live check after upload: a seeded sample, read once through the live route
//!   and once through the preview pack route, must answer the same content within the latency
//!   bound. The sample is drawn by the bake (a seeded hash of the PNU) and carried by the equality
//!   evidence, so both gates name the same PNUs and the publish can check that they do.
//!
//! The publish does not take `passed` on trust: it re-derives each verdict from the counts and the
//! timings in the file against the contract as it is now.

use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};

pub(crate) const EQUALITY_KIND: &str = "local_full_equality";
pub(crate) const LATENCY_KIND: &str = "live_sample_and_latency";
/// How many failing PNUs an evidence file names.
pub(crate) const MAX_EXAMPLES: usize = 20;

/// Gate (가): every document of a pack generation, read back from its pack, against the object
/// document the same Gold row renders.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct EqualityEvidence {
    pub(crate) schema_version: String,
    pub(crate) kind: String,
    pub(crate) lane: String,
    pub(crate) pack_generation: u64,
    pub(crate) gold_iceberg_snapshot_id: String,
    pub(crate) sections: Vec<String>,
    pub(crate) shards: u64,
    /// The Gold row count the publish states; every one of them must have been compared.
    pub(crate) expected_documents: u64,
    pub(crate) compared: u64,
    pub(crate) equal: u64,
    pub(crate) sample_seed: String,
    /// The live check's PNUs, in rank order.
    pub(crate) sample: Vec<String>,
    /// The anchor section's unit of each sample PNU (its dong, or the part of it the bake cut it
    /// into, root ADR-0163), in the sample's order: a read is cold on the first of its unit.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) sample_units: Vec<String>,
    pub(crate) sample_sha256: String,
    pub(crate) passed: bool,
    pub(crate) written_at_utc: String,
}

impl EqualityEvidence {
    /// The verdict the counts give.
    pub(crate) fn verdict(&self) -> bool {
        self.compared == self.expected_documents
            && self.equal == self.compared
            && self.compared > 0
            && self.sample_sha256 == sample_digest(&self.sample)
    }
}

/// One route's timings, in milliseconds.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct Timings {
    pub(crate) p50: f64,
    pub(crate) p95: f64,
    #[serde(default)]
    pub(crate) p99: f64,
    pub(crate) mean: f64,
    pub(crate) max: f64,
}

/// How much slower the pack route was than the live route, per percentile, in milliseconds.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct Increase {
    pub(crate) p50: f64,
    pub(crate) p95: f64,
    pub(crate) p99: f64,
}

impl Increase {
    pub(crate) fn between(live: &Timings, pack: &Timings) -> Self {
        Self {
            p50: pack.p50 - live.p50,
            p95: pack.p95 - live.p95,
            p99: pack.p99 - live.p99,
        }
    }

    fn within(&self, bound: &crate::by_pnu_gateway_contract::LatencyBound) -> bool {
        self.p50 <= bound.p50 && self.p95 <= bound.p95 && self.p99 <= bound.p99
    }
}

/// The load phase: the preview alone, paced at the contract's rate, the gate sample drawn with
/// replacement so legal dongs repeat (cold and warm reads mixed).
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct LoadEvidence {
    pub(crate) requests_per_second: u32,
    pub(crate) duration_seconds: u64,
    pub(crate) max_in_flight: u64,
    pub(crate) sent: u64,
    pub(crate) answered: u64,
    /// The requests the pacing could not start because `max_in_flight` were outstanding.
    pub(crate) shed: u64,
    pub(crate) achieved_requests_per_second: f64,
    pub(crate) availability: f64,
    pub(crate) failures: BTreeMap<String, u64>,
    pub(crate) latency_ms: Timings,
    pub(crate) server_timing: ServerTimingSummary,
    pub(crate) worker_cpu: Option<WorkerCpu>,
    /// The answers by the Worker version each named (root ADR-0157); an answer that named none is
    /// not counted here.
    #[serde(default)]
    pub(crate) versions: BTreeMap<String, u64>,
}

/// What the preview Worker said about its own answers (`Server-Timing`): its total and R2 wait,
/// and where each section's pack came from (`r2-whole`, `r2-head+range`, `edge-…`, `memory-…`).
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct ServerTimingSummary {
    pub(crate) answers: u64,
    pub(crate) total_ms: Timings,
    pub(crate) r2_ms: Timings,
    pub(crate) sources: BTreeMap<String, u64>,
    /// How the Worker answered, by its `outcome` (`document` passes the member through,
    /// `document-decompressed` gunzipped it for a client without gzip, `r2-unavailable`, …).
    #[serde(default)]
    pub(crate) outcomes: BTreeMap<String, u64>,
}

/// The preview read without gzip (`Accept-Encoding: identity`): the Worker's one path that
/// decompresses. Each answer must be 200, carry no `Content-Encoding`, and hold the content the
/// live route answered for the same PNU.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct NoGzipEvidence {
    pub(crate) sent: u64,
    pub(crate) answered: u64,
    /// Answers that came back uncompressed, as asked.
    pub(crate) identity: u64,
    pub(crate) mismatched: u64,
    /// Answers whose live read had failed, so there was nothing to compare them with.
    pub(crate) not_compared: u64,
    pub(crate) failures: BTreeMap<String, u64>,
    pub(crate) latency_ms: Timings,
    pub(crate) outcomes: BTreeMap<String, u64>,
}

impl NoGzipEvidence {
    fn holds(&self) -> bool {
        self.sent > 0
            && self.answered == self.sent
            && self.identity == self.sent
            && self.mismatched == 0
            && self.not_compared < self.sent
    }
}

/// The preview Worker's CPU over the probe window, as Workers analytics records it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct WorkerCpu {
    pub(crate) script: String,
    pub(crate) from_utc: String,
    pub(crate) to_utc: String,
    pub(crate) requests: u64,
    /// The largest p50 and p99 of any invocation status group, in milliseconds.
    pub(crate) cpu_p50_ms: f64,
    pub(crate) cpu_p99_ms: f64,
    /// Wall time per invocation, the largest of any status group, in milliseconds.
    #[serde(default)]
    pub(crate) wall_p50_ms: f64,
    #[serde(default)]
    pub(crate) wall_p99_ms: f64,
    /// Invocations per status (`success`, `exceededResources`, …).
    pub(crate) statuses: BTreeMap<String, u64>,
    /// The largest average sample interval of any status group (1 is unsampled); `requests` is
    /// already scaled by it.
    #[serde(default)]
    pub(crate) sample_interval_max: f64,
}

impl WorkerCpu {
    /// Invocations the platform cut off for their resources (error 1102, an HTTP 503).
    pub(crate) fn exceeded_resources(&self) -> u64 {
        self.statuses.get(EXCEEDED_RESOURCES).copied().unwrap_or(0)
    }
}

/// The Workers analytics status of an invocation the platform cut off (CPU or memory).
pub(crate) const EXCEEDED_RESOURCES: &str = "exceededResources";

/// Gate (나): the sample read once through the live route and once through the preview packs.
///
/// `live_ms` and `pack_ms` are the cold reads, the first of the run to touch the PNU's legal dong,
/// which the bound holds; the warm ones (`*_warm_ms`) are reported beside them.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct LatencyEvidence {
    pub(crate) schema_version: String,
    pub(crate) kind: String,
    pub(crate) lane: String,
    pub(crate) pack_generation: u64,
    pub(crate) live_base_url: String,
    pub(crate) preview_base_url: String,
    /// `local-simulation` when both routes were stand-ins: such a file never opens the gate.
    pub(crate) environment: String,
    pub(crate) sample_size: u64,
    /// The digest of the sample read; the publish holds it to the equality evidence's.
    pub(crate) sample_sha256: String,
    pub(crate) answered: u64,
    pub(crate) mismatched: u64,
    pub(crate) failed: u64,
    pub(crate) live_ms: Timings,
    pub(crate) pack_ms: Timings,
    pub(crate) increase_p50_ms: f64,
    pub(crate) increase_p95_ms: f64,
    pub(crate) bound_p50_ms: f64,
    pub(crate) bound_p95_ms: f64,
    pub(crate) examples: Vec<String>,
    pub(crate) passed: bool,
    pub(crate) measured_at_utc: String,
    /// PNUs in flight at once.
    #[serde(default)]
    pub(crate) concurrency: u64,
    #[serde(default)]
    pub(crate) cold_answered: u64,
    #[serde(default)]
    pub(crate) warm_answered: u64,
    #[serde(default)]
    pub(crate) live_warm_ms: Timings,
    #[serde(default)]
    pub(crate) pack_warm_ms: Timings,
    /// Failed reads by `{live|pack}:{class}` (`http-503`, `timeout`, `connect`, `body`, …).
    #[serde(default)]
    pub(crate) failures: BTreeMap<String, u64>,
    #[serde(default)]
    pub(crate) server_timing: ServerTimingSummary,
    /// `None` when Workers analytics was not asked; such a file never opens the gate.
    #[serde(default)]
    pub(crate) worker_cpu: Option<WorkerCpu>,
    #[serde(default)]
    pub(crate) bound_cpu_p99_ms: f64,
    /// The share of preview reads that answered 200 (the live route's failures are the network's
    /// or the object path's, not the pack path's, and are counted in `failures` only).
    #[serde(default)]
    pub(crate) availability: f64,
    #[serde(default)]
    pub(crate) increase_cold_ms: Increase,
    #[serde(default)]
    pub(crate) increase_warm_ms: Increase,
    #[serde(default)]
    pub(crate) load: Option<LoadEvidence>,
    /// Answers by `{live|pack}:{Content-Encoding}` (`identity` when none). Every read asks for
    /// gzip as a browser does; a pack answer that is not gzip is counted in `failures` as
    /// `pack:not-gzip` instead, since it means the member was not passed through.
    #[serde(default)]
    pub(crate) encodings: BTreeMap<String, u64>,
    /// `None` when the no-gzip sample was not read; such a file never opens the gate.
    #[serde(default)]
    pub(crate) no_gzip: Option<NoGzipEvidence>,
    /// The cold reads by where the preview's sections came from (`cold_read_path`). A file that
    /// counts none read from R2 (one written before this was counted) never opens the gate.
    #[serde(default)]
    pub(crate) cold_read_paths: BTreeMap<String, u64>,
    /// The same cold reads on the object side, by where its answer came from: the comparison is
    /// only fair when both sides read R2 (2026-10-07: the production route answered from edge
    /// copies earlier probes had warmed). Only the preview's object path says it, so evidence
    /// compared against the production route never opens the gate.
    #[serde(default)]
    pub(crate) cold_live_read_paths: BTreeMap<String, u64>,
}

/// The environment a production probe names.
pub(crate) const PRODUCTION_ENVIRONMENT: &str = "production-preview";

/// A cold read every section of which the preview read from R2: the first read the bound is about.
pub(crate) const COLD_PATH_R2: &str = "r2";

/// Where a cold read's answer came from, by the sources the preview's `Server-Timing` named for
/// its sections (or its object): `edge-copy` when any came from the PNU's edge copy, `r2` when
/// every one was read from R2, `memory` when some came from the isolate's memory instead, `untimed` when it named
/// none. Anything but `r2` was warmed before the run and measures a cache, not a first read.
pub(crate) fn cold_read_path(sources: &[String]) -> &'static str {
    if sources.is_empty() {
        "untimed"
    } else if sources.iter().any(|source| source.starts_with("edge-")) {
        "edge-copy"
    } else if sources.iter().all(|source| source.starts_with("r2-")) {
        COLD_PATH_R2
    } else {
        "memory"
    }
}

impl LatencyEvidence {
    /// The verdict the timings give against the contract as it is now.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub(crate) fn verdict(&self) -> anyhow::Result<bool> {
        let gate = &section_pack_policy()?.cutover_gate;
        let slo = &gate.slo;
        let cpu_within = |cpu: &Option<WorkerCpu>| {
            cpu.as_ref().is_some_and(|cpu| {
                cpu.requests > 0
                    && cpu.exceeded_resources() == 0
                    && cpu.cpu_p99_ms <= gate.worker_cpu_p99_max_ms
            })
        };
        #[allow(clippy::cast_precision_loss)]
        let enough_answered =
            self.answered as f64 >= self.sample_size as f64 * slo.availability_min;
        Ok(self.sample_size >= u64::try_from(gate.latency_sample_size)?
            && self.mismatched == 0
            && enough_answered
            && self.cold_reads_from_r2(&self.cold_read_paths, slo.cold_reads_not_from_r2_max_share)
            && self.cold_reads_from_r2(
                &self.cold_live_read_paths,
                slo.cold_reads_not_from_r2_max_share,
            )
            && self.availability >= slo.availability_min
            && self
                .increase_cold_ms
                .within(&slo.latency_max_increase_ms.cold)
            && (self.warm_answered == 0
                || self
                    .increase_warm_ms
                    .within(&slo.latency_max_increase_ms.warm))
            && cpu_within(&self.worker_cpu)
            && self.no_gzip.as_ref().is_some_and(NoGzipEvidence::holds)
            && self.load.as_ref().is_some_and(|load| {
                load.sent > 0
                    && load.availability >= slo.availability_min
                    && cpu_within(&load.worker_cpu)
            }))
    }

    /// Whether one side of the cold bound measured first reads: at most `max_share` of its cold
    /// reads were answered without reading R2.
    fn cold_reads_from_r2(&self, paths: &BTreeMap<String, u64>, max_share: f64) -> bool {
        let from_r2 = paths.get(COLD_PATH_R2).copied().unwrap_or(0);
        #[allow(clippy::cast_precision_loss)]
        let not_from_r2 = self.cold_answered.saturating_sub(from_r2) as f64;
        #[allow(clippy::cast_precision_loss)]
        let allowed = self.cold_answered as f64 * max_share;
        self.cold_answered > 0 && from_r2 <= self.cold_answered && not_from_r2 <= allowed
    }
}

/// The seeded rank of a PNU: the first eight bytes of SHA-256 over the contract's seed and the
/// PNU. The same PNU always ranks the same, whichever shard bakes it.
///
/// # Errors
/// Returns an error when the contract cannot be read.
pub(crate) fn sample_rank(pnu: &str) -> anyhow::Result<u64> {
    let seed = &section_pack_policy()?.cutover_gate.sample_seed;
    let digest = Sha256::digest(format!("{seed}:{pnu}").as_bytes());
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest[..8]);
    Ok(u64::from_be_bytes(first))
}

/// Whether a bake records `pnu` as a sample candidate: about the contract's
/// `sample_candidates_per_million` of every million PNUs, a few more than the sample needs.
///
/// # Errors
/// Returns an error when the contract cannot be read.
pub(crate) fn is_sample_candidate(pnu: &str) -> anyhow::Result<bool> {
    let per_million = section_pack_policy()?
        .cutover_gate
        .sample_candidates_per_million;
    Ok(sample_rank(pnu)? < u64::MAX / 1_000_000 * per_million)
}

/// The Gold row count of `snapshot` as the catalog's table metadata records it; a count an
/// operator `stated` must agree with it. The gate is held to the catalog, never to a typed number.
///
/// # Errors
/// Refuses an unreadable catalog, an expired snapshot or one whose summary records no row count,
/// and a stated count that differs.
pub(crate) async fn gold_record_count(
    lane: ByPnuLane,
    snapshot: &str,
    stated: Option<u64>,
) -> anyhow::Result<u64> {
    let catalog = lakehouse_infrastructure::IcebergRestCatalog::new(
        lakehouse_infrastructure::LakehouseCatalogConfig::from_env()
            .context("failed to configure the Iceberg catalog")?,
    )?;
    let table = crate::by_pnu_serving_manifest_publish::gold_table(lane);
    let recorded = catalog
        .load_snapshot_record_count(
            table,
            snapshot
                .parse::<i64>()
                .with_context(|| format!("{snapshot} is not an Iceberg snapshot id"))?,
        )
        .await?
        .with_context(|| format!("{table} does not exist in the catalog"))?;
    cross_check(lane, recorded, stated, snapshot)
}

/// The catalog's count, refused when a stated one differs.
///
/// # Errors
/// Names both counts.
pub(crate) fn cross_check(
    lane: ByPnuLane,
    recorded: u64,
    stated: Option<u64>,
    snapshot: &str,
) -> anyhow::Result<u64> {
    ensure!(
        stated.is_none_or(|stated| stated == recorded),
        "{} states {} Gold rows but the catalog records {recorded} for snapshot {snapshot}",
        lane.env("PACK_EXPECTED_DOCUMENT_COUNT"),
        stated.unwrap_or_default()
    );
    Ok(recorded)
}

/// The digest of a sample, named in both evidence files.
pub(crate) fn sample_digest(sample: &[String]) -> String {
    format!("{:x}", Sha256::digest(sample.join("\n").as_bytes()))
}

/// Reads an evidence file.
///
/// # Errors
/// Refuses an unreadable file and another schema.
pub(crate) fn read<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
) -> anyhow::Result<(T, String)> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read gate evidence {}", path.display()))?;
    let raw: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("{} is not JSON", path.display()))?;
    let schema = &section_pack_policy()?.cutover_gate.evidence_schema_version;
    ensure!(
        raw.get("schema_version")
            .and_then(serde_json::Value::as_str)
            == Some(schema.as_str()),
        "{} is not {schema} gate evidence",
        path.display()
    );
    Ok((
        serde_json::from_value(raw)
            .with_context(|| format!("{} does not parse", path.display()))?,
        format!("{:x}", Sha256::digest(&bytes)),
    ))
}

/// Refuses equality evidence that does not open the gate for `generation` of `snapshot`.
///
/// # Errors
/// Names the first unmet condition.
pub(crate) fn require_equality(
    lane: ByPnuLane,
    evidence: &EqualityEvidence,
    generation: u64,
    snapshot: &str,
    expected_documents: u64,
) -> anyhow::Result<()> {
    ensure!(
        evidence.kind == EQUALITY_KIND && evidence.lane == lane.unit(),
        "the equality evidence is a {} file of {}, not {EQUALITY_KIND} of {}",
        evidence.kind,
        evidence.lane,
        lane.unit()
    );
    ensure!(
        evidence.pack_generation == generation
            && evidence.gold_iceberg_snapshot_id == snapshot
            && evidence.sections == lane.section_packs()?.sections,
        "the equality evidence examined generation {} of Gold snapshot {} ({:?}), not generation \
         {generation} of {snapshot} in every contract section",
        evidence.pack_generation,
        evidence.gold_iceberg_snapshot_id,
        evidence.sections
    );
    ensure!(
        evidence.expected_documents == expected_documents && evidence.verdict() && evidence.passed,
        "the equality evidence does not pass: compared {} of {} expected ({} stated now), {} equal",
        evidence.compared,
        evidence.expected_documents,
        expected_documents,
        evidence.equal
    );
    Ok(())
}

/// Refuses live evidence that does not open the gate for `generation`, or that read another
/// sample than the equality evidence names.
///
/// # Errors
/// Names the first unmet condition.
#[cfg(test)]
pub(crate) fn require_latency(
    lane: ByPnuLane,
    evidence: &LatencyEvidence,
    generation: u64,
    equality: &EqualityEvidence,
) -> anyhow::Result<()> {
    require_latency_identity(lane, evidence, generation, equality)?;
    require_latency_verdict(evidence)
}

/// [`require_latency`], except that a failed verdict is accepted when the contract's
/// `cutover_gate.latency_waivers` names exactly this evidence file (its sha256), its lane and its
/// generation (root ADR-0162). A waiver accepts the measured latency and CPU only: the evidence must
/// still be of this lane, generation, sample and environment, and every answer the probe compared
/// must have matched. Returns the waiver's reason when one was used.
///
/// # Errors
/// Names the first unmet condition.
pub(crate) fn require_latency_or_waiver(
    lane: ByPnuLane,
    evidence: &LatencyEvidence,
    evidence_sha256: &str,
    generation: u64,
    equality: &EqualityEvidence,
) -> anyhow::Result<Option<String>> {
    require_latency_identity(lane, evidence, generation, equality)?;
    let Err(refusal) = require_latency_verdict(evidence) else {
        return Ok(None);
    };
    let Some(waiver) = section_pack_policy()?
        .cutover_gate
        .latency_waivers
        .iter()
        .find(|waiver| {
            waiver.lane == lane.unit()
                && waiver.generation == generation
                && waiver.latency_evidence_sha256 == evidence_sha256
        })
    else {
        return Err(refusal);
    };
    ensure!(
        evidence.mismatched == 0 && evidence.answered > 0,
        "a latency waiver accepts latency, never content: the live evidence has {} mismatched of \
         {} answered",
        evidence.mismatched,
        evidence.answered
    );
    Ok(Some(format!(
        "{} (decided {}; the gate refused: {refusal:#})",
        waiver.reason, waiver.decided_on
    )))
}

fn require_latency_identity(
    lane: ByPnuLane,
    evidence: &LatencyEvidence,
    generation: u64,
    equality: &EqualityEvidence,
) -> anyhow::Result<()> {
    ensure!(
        evidence.kind == LATENCY_KIND && evidence.lane == lane.unit(),
        "the live evidence is a {} file of {}, not {LATENCY_KIND} of {}",
        evidence.kind,
        evidence.lane,
        lane.unit()
    );
    ensure!(
        evidence.pack_generation == generation,
        "the live evidence measured generation {}, not {generation}",
        evidence.pack_generation
    );
    ensure!(
        evidence.sample_sha256 == equality.sample_sha256,
        "the live evidence read another sample than the equality evidence drew"
    );
    ensure!(
        evidence.environment == PRODUCTION_ENVIRONMENT,
        "the live evidence was measured in {:?}; only a {PRODUCTION_ENVIRONMENT} probe of the \
         live route against the preview Worker opens the gate",
        evidence.environment
    );
    Ok(())
}

fn require_latency_verdict(evidence: &LatencyEvidence) -> anyhow::Result<()> {
    ensure!(
        evidence.verdict()? && evidence.passed,
        "the live evidence does not pass: sample {} (answered {}, mismatched {}, failed {}), \
         cold p50 {:.1}→{:.1} ms, cold p95 {:.1}→{:.1} ms, worker CPU {}",
        evidence.sample_size,
        evidence.answered,
        evidence.mismatched,
        evidence.failed,
        evidence.live_ms.p50,
        evidence.pack_ms.p50,
        evidence.live_ms.p95,
        evidence.pack_ms.p95,
        evidence.worker_cpu.as_ref().map_or_else(
            || "not measured".to_owned(),
            |cpu| format!(
                "p99 {:.1} ms with {} exceededResources",
                cpu.cpu_p99_ms,
                cpu.exceeded_resources()
            )
        )
    );
    Ok(())
}
