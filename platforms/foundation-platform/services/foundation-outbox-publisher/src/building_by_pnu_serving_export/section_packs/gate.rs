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

use super::super::LANE;
use crate::by_pnu_gateway_contract::section_pack_policy;

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
    pub(crate) mean: f64,
    pub(crate) max: f64,
}

/// What the preview Worker said about its own answers (`Server-Timing`): its total and R2 wait,
/// and where each section's pack came from (`r2-whole`, `r2-head+range`, `edge-…`, `memory-…`).
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct ServerTimingSummary {
    pub(crate) answers: u64,
    pub(crate) total_ms: Timings,
    pub(crate) r2_ms: Timings,
    pub(crate) sources: BTreeMap<String, u64>,
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
    /// Invocations per status (`success`, `exceededResources`, …).
    pub(crate) statuses: BTreeMap<String, u64>,
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
}

/// The environment a production probe names.
pub(crate) const PRODUCTION_ENVIRONMENT: &str = "production-preview";

impl LatencyEvidence {
    /// The verdict the timings give against the contract as it is now.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub(crate) fn verdict(&self) -> anyhow::Result<bool> {
        let gate = &section_pack_policy()?.cutover_gate;
        Ok(self.sample_size >= u64::try_from(gate.latency_sample_size)?
            && self.answered == self.sample_size
            && self.mismatched == 0
            && self.failed == 0
            && self.pack_ms.p50 - self.live_ms.p50 <= gate.latency_max_increase_ms.p50
            && self.pack_ms.p95 - self.live_ms.p95 <= gate.latency_max_increase_ms.p95
            && self.worker_cpu.as_ref().is_some_and(|cpu| {
                cpu.requests > 0
                    && cpu.exceeded_resources() == 0
                    && cpu.cpu_p99_ms <= gate.worker_cpu_p99_max_ms
            }))
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
pub(crate) async fn gold_record_count(snapshot: &str, stated: Option<u64>) -> anyhow::Result<u64> {
    let catalog = lakehouse_infrastructure::IcebergRestCatalog::new(
        lakehouse_infrastructure::LakehouseCatalogConfig::from_env()
            .context("failed to configure the Iceberg catalog")?,
    )?;
    let table = lakehouse_domain::GOLD_BUILDING_PANEL.table_name;
    let recorded = catalog
        .load_snapshot_record_count(
            table,
            snapshot
                .parse::<i64>()
                .with_context(|| format!("{snapshot} is not an Iceberg snapshot id"))?,
        )
        .await?
        .with_context(|| format!("{table} does not exist in the catalog"))?;
    cross_check(recorded, stated, snapshot)
}

/// The catalog's count, refused when a stated one differs.
///
/// # Errors
/// Names both counts.
pub(crate) fn cross_check(
    recorded: u64,
    stated: Option<u64>,
    snapshot: &str,
) -> anyhow::Result<u64> {
    ensure!(
        stated.is_none_or(|stated| stated == recorded),
        "{} states {} Gold rows but the catalog records {recorded} for snapshot {snapshot}",
        LANE.env("PACK_EXPECTED_DOCUMENT_COUNT"),
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
    evidence: &EqualityEvidence,
    generation: u64,
    snapshot: &str,
    expected_documents: u64,
) -> anyhow::Result<()> {
    ensure!(
        evidence.kind == EQUALITY_KIND && evidence.lane == LANE.unit(),
        "the equality evidence is a {} file of {}, not {EQUALITY_KIND} of {}",
        evidence.kind,
        evidence.lane,
        LANE.unit()
    );
    ensure!(
        evidence.pack_generation == generation
            && evidence.gold_iceberg_snapshot_id == snapshot
            && evidence.sections == LANE.section_packs()?.sections,
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
pub(crate) fn require_latency(
    evidence: &LatencyEvidence,
    generation: u64,
    equality: &EqualityEvidence,
) -> anyhow::Result<()> {
    ensure!(
        evidence.kind == LATENCY_KIND && evidence.lane == LANE.unit(),
        "the live evidence is a {} file of {}, not {LATENCY_KIND} of {}",
        evidence.kind,
        evidence.lane,
        LANE.unit()
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
