//! The cut-over gate's evidence (root ADR-0147 §6): what the equality check and the latency probe
//! write, and what the first pack publish demands of it.
//!
//! The publish does not take `passed` on trust: it re-derives the verdict from the counts and the
//! timings in the file against the contract as it is now, and it ties the equality evidence to the
//! object state the manifest still serves, so evidence of an older served state cannot open the
//! gate.

use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};

use super::super::LANE;
use crate::by_pnu_gateway_contract::section_pack_policy;
use crate::by_pnu_serving_manifest::ServedManifest;

pub(crate) const EQUALITY_KIND: &str = "full_equality";
pub(crate) const LATENCY_KIND: &str = "cold_first_read_latency";
/// How many differing PNUs an evidence file names per verdict.
pub(crate) const MAX_EXAMPLES: usize = 20;

/// The object state an equality check compared against.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ServedObjects {
    pub(crate) base_generation: u64,
    pub(crate) newest_patch: u64,
    pub(crate) reflected_gold_iceberg_snapshot_id: String,
    pub(crate) object_count: u64,
}

impl ServedObjects {
    pub(crate) fn of(manifest: &ServedManifest) -> Self {
        Self {
            base_generation: manifest.base_generation,
            newest_patch: manifest.newest_patch(),
            reflected_gold_iceberg_snapshot_id: manifest.reflected_gold_iceberg_snapshot_id.clone(),
            object_count: manifest.object_count,
        }
    }
}

/// PNUs per verdict, at most [`MAX_EXAMPLES`] each.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Examples {
    pub(crate) different: Vec<String>,
    pub(crate) only_served: Vec<String>,
    pub(crate) only_packs: Vec<String>,
    pub(crate) unreadable: Vec<String>,
}

/// Gate (가): every PNU's pack answer against the object the lane serves, `source` aside.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct EqualityEvidence {
    pub(crate) schema_version: String,
    pub(crate) kind: String,
    pub(crate) lane: String,
    pub(crate) pack_generation: u64,
    pub(crate) sections: Vec<String>,
    pub(crate) served: ServedObjects,
    /// False when the run was limited to PNU prefixes: a sample cannot open the gate.
    pub(crate) complete: bool,
    pub(crate) compared: u64,
    pub(crate) equal: u64,
    pub(crate) different: u64,
    pub(crate) only_served: u64,
    pub(crate) only_packs: u64,
    pub(crate) unreadable: u64,
    pub(crate) examples: Examples,
    pub(crate) passed: bool,
    pub(crate) started_at_utc: String,
    pub(crate) finished_at_utc: String,
    pub(crate) elapsed_seconds: f64,
}

impl EqualityEvidence {
    /// The verdict the counts give.
    pub(crate) fn verdict(&self) -> bool {
        self.complete
            && self.different == 0
            && self.only_served == 0
            && self.only_packs == 0
            && self.unreadable == 0
            && self.equal == self.compared
            && self.compared == self.served.object_count
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

/// Gate (나): the cold first read of a sample, live objects against the preview of the packs.
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
            && self.pack_ms.p95 - self.live_ms.p95 <= gate.latency_max_increase_ms.p95)
    }
}

/// Reads an evidence file.
///
/// # Errors
/// Refuses an unreadable file and another schema.
pub(crate) fn read<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
) -> anyhow::Result<(T, String)> {
    use sha2::{Digest, Sha256};
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

/// Refuses equality evidence that does not open the gate for `generation` over `served`.
///
/// # Errors
/// Names the first unmet condition.
pub(crate) fn require_equality(
    evidence: &EqualityEvidence,
    generation: u64,
    served: &ServedManifest,
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
            && evidence.sections == LANE.section_packs()?.sections,
        "the equality evidence examined generation {} of {:?}, not generation {generation} of \
         every contract section",
        evidence.pack_generation,
        evidence.sections
    );
    ensure!(
        evidence.served == ServedObjects::of(served),
        "the equality evidence compared against served objects {:?}, but the manifest now serves \
         {:?}; check again",
        evidence.served,
        ServedObjects::of(served)
    );
    ensure!(
        evidence.verdict() && evidence.passed,
        "the equality evidence does not pass: complete={}, compared={}, equal={}, different={}, \
         only_served={}, only_packs={}, unreadable={}, served={}",
        evidence.complete,
        evidence.compared,
        evidence.equal,
        evidence.different,
        evidence.only_served,
        evidence.only_packs,
        evidence.unreadable,
        evidence.served.object_count
    );
    Ok(())
}

/// Refuses latency evidence that does not open the gate for `generation`.
///
/// # Errors
/// Names the first unmet condition.
pub(crate) fn require_latency(evidence: &LatencyEvidence, generation: u64) -> anyhow::Result<()> {
    ensure!(
        evidence.kind == LATENCY_KIND && evidence.lane == LANE.unit(),
        "the latency evidence is a {} file of {}, not {LATENCY_KIND} of {}",
        evidence.kind,
        evidence.lane,
        LANE.unit()
    );
    ensure!(
        evidence.pack_generation == generation,
        "the latency evidence measured generation {}, not {generation}",
        evidence.pack_generation
    );
    ensure!(
        evidence.environment == PRODUCTION_ENVIRONMENT,
        "the latency evidence was measured in {:?}; only a {PRODUCTION_ENVIRONMENT} probe of the \
         live route against the preview Worker opens the gate",
        evidence.environment
    );
    ensure!(
        evidence.verdict()? && evidence.passed,
        "the latency evidence does not pass: sample {} (answered {}, mismatched {}, failed {}), \
         p50 {:.1}→{:.1} ms, p95 {:.1}→{:.1} ms",
        evidence.sample_size,
        evidence.answered,
        evidence.mismatched,
        evidence.failed,
        evidence.live_ms.p50,
        evidence.pack_ms.p50,
        evidence.live_ms.p95,
        evidence.pack_ms.p95
    );
    Ok(())
}
