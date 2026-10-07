//! `verify-<lane>-by-pnu-section-pack-equality`: gate (가) of the cut-over (root ADR-0147 §6).
//!
//! The comparison itself happens in the bake, before any write: every document is read back from
//! the pack bytes it went into, joined the way the gateway joins it, and compared byte for byte with
//! the object document the same Gold row renders (`bake.rs`). A difference stops the bake. This
//! command adds the bake summaries of one generation up: one Gold snapshot, every contract section,
//! a base and no patch, and exactly the Gold row count compared. It reads files and the catalog's
//! table metadata only, so the gate costs no R2 read.
//!
//! It also draws the live check's sample: the PNUs the bakes recorded as candidates (a seeded hash,
//! `gate::is_sample_candidate`), the contract's `latency_sample_size` of them with the lowest rank.
//! The probe (`latency.rs`) reads exactly those, and the publish holds both files to the same
//! sample digest.
//!
//! The Gold row count every document is held to is the catalog's record of the summaries'
//! snapshot (`gate::gold_record_count`); `PACK_EXPECTED_DOCUMENT_COUNT`, when an operator states
//! it, must agree with it.

use std::path::{Path, PathBuf};

use anyhow::{ensure, Context};

use super::super::optional_env;
use super::bake::{summary_schema_version, PackExportSummary};
use super::gate::{self, EqualityEvidence};
use crate::by_pnu_gateway_contract::{section_pack_policy, ByPnuLane};

#[derive(Clone, Debug)]
pub(crate) struct EqualityConfig {
    pub(crate) lane: ByPnuLane,
    pub(crate) summary_dir: PathBuf,
    pub(crate) generation: u64,
    /// The Gold snapshot's row count, from the catalog.
    pub(crate) expected_documents: u64,
    pub(crate) evidence_path: PathBuf,
}

impl EqualityConfig {
    async fn from_env(lane: ByPnuLane) -> anyhow::Result<Self> {
        let required = |name: &str| -> anyhow::Result<String> {
            optional_env(&lane.env(name))?
                .with_context(|| format!("{} is required", lane.env(name)))
        };
        let summary_dir = PathBuf::from(required("PACK_SUMMARY_DIR")?);
        let stated = optional_env(&lane.env("PACK_EXPECTED_DOCUMENT_COUNT"))?
            .map(|raw| raw.parse::<u64>())
            .transpose()
            .context("the expected document count must be a number")?;
        let first = summary_paths(&summary_dir)?
            .first()
            .map(|path| read_summary(path))
            .transpose()?
            .with_context(|| format!("{} holds no pack export summary", summary_dir.display()))?;
        Ok(Self {
            lane,
            expected_documents: gate::gold_record_count(
                lane,
                &first.gold_iceberg_snapshot_id,
                stated,
            )
            .await?,
            summary_dir,
            generation: required("PACK_GENERATION")?
                .parse()
                .context("the pack generation must be a number")?,
            evidence_path: PathBuf::from(required("PACK_EQUALITY_EVIDENCE_PATH")?),
        })
    }
}

/// Every `*.json` file of `dir`, sorted.
pub(crate) fn summary_paths(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read {}", dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
    paths.sort();
    Ok(paths)
}

fn read_summary(path: &Path) -> anyhow::Result<PackExportSummary> {
    serde_json::from_slice(&std::fs::read(path)?)
        .with_context(|| format!("{} is not a pack export summary", path.display()))
}

/// Adds the bake summaries up and writes the evidence; fails when it does not pass.
///
/// # Errors
/// Returns an error when a summary cannot be read or the evidence does not pass.
pub(crate) async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    super::sections::check_contract_sections(lane)?;
    let config = EqualityConfig::from_env(lane).await?;
    let evidence = verify(&config)?;
    write_evidence(&config.evidence_path, &evidence)?;
    tracing::info!(
        compared = evidence.compared,
        expected = evidence.expected_documents,
        sample = evidence.sample.len(),
        passed = evidence.passed,
        lane = lane.unit(),
        "section pack equality checked"
    );
    ensure!(
        evidence.passed,
        "the bakes did not compare every Gold row of generation {}; see {}",
        config.generation,
        config.evidence_path.display()
    );
    Ok(())
}

pub(crate) fn write_evidence(path: &Path, evidence: &impl serde::Serialize) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut body = serde_json::to_vec_pretty(evidence)?;
    body.push(b'\n');
    std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

/// The evidence of one generation's bake summaries; does not write it.
///
/// # Errors
/// Refuses summaries of another kind, generation, snapshot or section list, and a patch.
pub(crate) fn verify(config: &EqualityConfig) -> anyhow::Result<EqualityEvidence> {
    let paths = summary_paths(&config.summary_dir)?;
    let sections = config.lane.section_packs()?.sections.clone();
    let mut snapshot: Option<String> = None;
    let (mut compared, mut equal) = (0_u64, 0_u64);
    let mut candidates = Vec::new();
    for path in &paths {
        let summary = read_summary(path)?;
        ensure!(
            summary.schema_version == summary_schema_version(config.lane)
                && summary.generation == config.generation
                && summary.section_generations.is_empty()
                && summary.patch.is_none()
                && summary.sections == sections,
            "{} is not a base bake of generation {} over every contract section",
            path.display(),
            config.generation
        );
        ensure!(
            snapshot
                .get_or_insert_with(|| summary.gold_iceberg_snapshot_id.clone())
                .as_str()
                == summary.gold_iceberg_snapshot_id,
            "the summaries are of more than one Gold snapshot"
        );
        ensure!(
            summary
                .gold_record_count
                .is_none_or(|count| count == config.expected_documents),
            "{} scanned a Gold snapshot of {:?} rows by its manifests, but the catalog records {}",
            path.display(),
            summary.gold_record_count,
            config.expected_documents
        );
        ensure!(
            summary.equality.compared == summary.exported_row_count,
            "{} compared {} of its {} rows",
            path.display(),
            summary.equality.compared,
            summary.exported_row_count
        );
        compared += summary.equality.compared;
        equal += summary.equality.equal;
        candidates.extend(summary.equality.sample_candidates);
    }
    let snapshot = snapshot.with_context(|| {
        format!(
            "{} holds no pack export summary",
            config.summary_dir.display()
        )
    })?;
    let gate_policy = &section_pack_policy()?.cutover_gate;
    let mut ranked = candidates
        .into_iter()
        .map(|pnu| Ok((gate::sample_rank(&pnu)?, pnu)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    ranked.sort_unstable();
    ranked.dedup();
    let sample = ranked
        .into_iter()
        .take(gate_policy.latency_sample_size)
        .map(|(_, pnu)| pnu)
        .collect::<Vec<_>>();
    let mut evidence = EqualityEvidence {
        schema_version: gate_policy.evidence_schema_version.clone(),
        kind: gate::EQUALITY_KIND.to_owned(),
        lane: config.lane.unit().to_owned(),
        pack_generation: config.generation,
        gold_iceberg_snapshot_id: snapshot,
        sections,
        shards: u64::try_from(paths.len())?,
        expected_documents: config.expected_documents,
        compared,
        equal,
        sample_seed: gate_policy.sample_seed.clone(),
        sample_sha256: gate::sample_digest(&sample),
        sample,
        passed: false,
        written_at_utc: crate::by_pnu_serving_manifest_publish::now(),
    };
    evidence.passed = evidence.verdict();
    Ok(evidence)
}
