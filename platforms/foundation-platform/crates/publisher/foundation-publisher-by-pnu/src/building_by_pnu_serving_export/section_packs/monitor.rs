//! `monitor-building-by-pnu-serving`: the hourly synthetic read of the live building hostname
//! (root ADR-0151 §6, contract `section_packs.monitor`).
//!
//! Reads the first `monitor.pnus` PNUs of the gate sample (the equality evidence on the host; the
//! real PNUs never enter the repository) from the live hostname and holds each answer to:
//!
//! - 200, within the read timeout;
//! - the document the lane serves, resolved by the publisher's own reader from R2 (the packs when
//!   the manifest names them, else the objects);
//! - a p95 within `monitor.latency_p95_max_ms`.
//!
//! With `MONITOR_PACK_GENERATION=<n>` it reads `?{preview_query_parameter}=g<n>` from a preview
//! hostname and holds each answer to generation `n`'s packs instead; with
//! `MONITOR_HISTORY_STATE=<published>-<sha256>` it reads `?{preview_query_parameter}=h<that>` and
//! holds each answer to that recorded manifest's packs and pack patches: the rollback rehearsal (root
//! ADR-0274 §5) proves the state a rollback would serve is whole and served.
//!
//! It no longer compares the answer with the per-PNU object documents once packs are served: the
//! section-pack bake does not write objects, so after a pack bake they hold an older document and
//! the comparison failed a faithful hostname (2026-10-17, parcel). Before the cut-over the objects
//! are what the lane serves, which the check above already holds.
//!
//! Any breach fails the run, so the unit's `OnFailure` reports it to Slack. Read-only.

use std::path::PathBuf;

use anyhow::{ensure, Context};
use serde::Serialize;

use super::super::optional_env;
use super::equality::write_evidence;
use super::gate::{self, EqualityEvidence, Timings};
use super::inspect;
use super::latency::{normalized_digest, timed_get, timings, Accept, REQUEST_TIMEOUT};
use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::by_pnu;

/// One run's report, written beside the unit's journal.
#[derive(Debug, Serialize)]
pub struct MonitorReport {
    pub base_url: String,
    pub pnus: usize,
    pub answered: usize,
    pub failures: Vec<String>,
    /// Answers that differ from what the lane serves (packs or objects).
    pub drifted_from_served: Vec<String>,
    pub latency_ms: Timings,
    pub latency_p95_max_ms: f64,
    pub passed: bool,
    pub checked_at_utc: String,
}

/// Runs the monitor.
///
/// # Errors
/// Returns an error when the inputs cannot be read, or any check fails.
pub async fn run(lane: ByPnuLane) -> anyhow::Result<()> {
    let env = |name: &str| optional_env(&lane.env(name));
    let required = |name: &str| -> anyhow::Result<String> {
        env(name)?.with_context(|| format!("{} is required", lane.env(name)))
    };
    let policy = lane
        .section_packs()?
        .monitor
        .as_ref()
        .context("the contract names no monitor for this lane")?;
    let (equality, _) =
        gate::read::<EqualityEvidence>(&PathBuf::from(required("MONITOR_SAMPLE_PATH")?))?;
    let pnus = equality
        .sample
        .into_iter()
        .take(policy.pnus)
        .collect::<Vec<_>>();
    ensure!(!pnus.is_empty(), "the monitor sample is empty");
    let output = ProfileStoreConfig::parse(
        env("OUTPUT_STORAGE_DRIVER")?
            .unwrap_or_else(|| "r2".to_owned())
            .as_str(),
        local_root(env("OUTPUT_ROOT")?),
    )?;
    let store = ByPnuServingStore::open(lane, &output)?;
    let base_url = match env("MONITOR_BASE_URL")? {
        Some(url) => url,
        None => format!("https://{}", lane.policy()?.public_hostname),
    };
    let generation = env("MONITOR_PACK_GENERATION")?
        .map(|raw| raw.parse::<u64>())
        .transpose()
        .with_context(|| {
            format!(
                "{} must be a generation number",
                lane.env("MONITOR_PACK_GENERATION")
            )
        })?;
    let target = match (generation, env("MONITOR_HISTORY_STATE")?) {
        (None, None) => Target::Served,
        (Some(generation), None) => Target::Generation(generation),
        (None, Some(name)) => Target::recorded(&store, &name).await?,
        (Some(_), Some(_)) => anyhow::bail!(
            "name one of {} or {}",
            lane.env("MONITOR_PACK_GENERATION"),
            lane.env("MONITOR_HISTORY_STATE")
        ),
    };
    let report = check_at(&store, &base_url, &pnus, policy.latency_p95_max_ms, &target).await?;
    if let Some(path) = env("MONITOR_REPORT_PATH")? {
        write_evidence(&PathBuf::from(path), &report)?;
    }
    tracing::info!(report = ?report, lane = lane.name(), "serving monitor");
    ensure!(
        report.passed,
        "the live {} hostname breached the monitor: {} failures, {} drifted from what the \
         lane serves, p95 {:.0} ms (bound {:.0} ms)",
        lane.noun(),
        report.failures.len(),
        report.drifted_from_served.len(),
        report.latency_ms.p95,
        report.latency_p95_max_ms
    );
    Ok(())
}

/// What a monitor run holds the answers to.
pub enum Target {
    /// What the live manifest serves.
    Served,
    /// One pack generation, read from a preview by `?packs=g<n>`.
    Generation(u64),
    /// One recorded manifest state, read from a preview by `?packs=h<name>`.
    Recorded(
        String,
        Box<crate::by_pnu_section_pack_manifest::SectionPacksState>,
    ),
}

impl Target {
    /// The recorded manifest `name` (`<published>-<sha256>`) of the lane's history.
    ///
    /// # Errors
    /// Returns an error when the name is not a history entry, or it serves no section packs.
    pub async fn recorded(store: &ByPnuServingStore, name: &str) -> anyhow::Result<Self> {
        let key = format!(
            "{}{name}.json",
            by_pnu::manifest_history_prefix(store.lane())?
        );
        ensure!(
            by_pnu::is_manifest_history_key(store.lane(), &key),
            "{name} is not a manifest history entry"
        );
        let manifest = ServedManifest::parse(store.lane(), &store.read_bytes(&key).await?)?;
        let state = manifest
            .section_packs
            .with_context(|| format!("{key} serves no section packs"))?;
        Ok(Self::Recorded(name.to_owned(), Box::new(state)))
    }
}

/// Reads every PNU from `base_url` and holds it to `target`'s documents.
///
/// # Errors
/// Returns an error when the manifest or a pack cannot be read.
pub async fn check_at(
    store: &ByPnuServingStore,
    base_url: &str,
    pnus: &[String],
    latency_p95_max_ms: f64,
    target: &Target,
) -> anyhow::Result<MonitorReport> {
    let lane = store.lane();
    let parameter = &crate::by_pnu_gateway_contract::section_pack_policy()?.preview_query_parameter;
    let query = match target {
        Target::Served => String::new(),
        Target::Generation(generation) => format!("?{parameter}=g{generation}"),
        Target::Recorded(name, _) => format!("?{parameter}=h{name}"),
    };
    let prefix = &lane.policy()?.request_path.prefix;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    let (manifest, _) = store.read_manifest().await?;
    let served = ServedManifest::parse(lane, &manifest)?;
    let mut report = MonitorReport {
        base_url: base_url.to_owned(),
        pnus: pnus.len(),
        answered: 0,
        failures: Vec::new(),
        drifted_from_served: Vec::new(),
        latency_ms: Timings::default(),
        latency_p95_max_ms,
        passed: false,
        checked_at_utc: crate::by_pnu_serving_manifest_publish::now(),
    };
    let mut latency = Vec::new();
    for pnu in pnus {
        let url = format!(
            "{}{prefix}{}{query}",
            base_url.trim_end_matches('/'),
            crate::r2_layout::by_pnu_packs::request_path_of(pnu)
        );
        let answer = match timed_get(lane, &client, &url, Accept::Gzip).await {
            Ok(answer) => answer,
            Err(failure) => {
                report.failures.push(format!("{pnu}: {}", failure.class));
                continue;
            }
        };
        report.answered += 1;
        latency.push(answer.ms);
        let live = answer.content()?;
        let served_bytes = if !matches!(target, Target::Served) || served.section_packs.is_some() {
            let inspected = match target {
                Target::Recorded(_, state) => inspect::inspect_state(store, pnu, state).await?,
                Target::Generation(generation) => {
                    inspect::inspect(store, pnu, Some(*generation)).await?
                }
                Target::Served => inspect::inspect(store, pnu, None).await?,
            };
            ensure!(
                inspected["answer"] == "document",
                "the packs do not serve {pnu} as a document: {}",
                inspected["answer"]
            );
            Some(serde_json::to_vec(&inspected["document"])?)
        } else {
            object_document(store, &served, pnu).await?
        };
        if served_bytes.as_deref().map(normalized_digest).transpose()? != Some(live) {
            report.drifted_from_served.push(pnu.clone());
        }
    }
    report.latency_ms = timings(&mut latency);
    report.passed = report.answered == pnus.len()
        && report.failures.is_empty()
        && report.drifted_from_served.is_empty()
        && report.latency_ms.p95 <= latency_p95_max_ms;
    Ok(report)
}

/// The object document of `pnu` in the manifest's base generation, or `None` when an object patch
/// covers its prefix (then the base object may not be what the object lane serves), or the lane is
/// pack-only (root ADR-0190) and never had an object.
async fn object_document(
    store: &ByPnuServingStore,
    served: &ServedManifest,
    pnu: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    if store.lane().pack_lane().is_some() {
        return Ok(None);
    }
    if let Some(length) = served.pnu_prefix_length {
        let prefix = pnu.get(..length).unwrap_or(pnu);
        if served
            .patches
            .iter()
            .any(|patch| patch.prefixes.iter().any(|covered| covered == prefix))
        {
            return Ok(None);
        }
    }
    let key = by_pnu::object_key(store.lane(), served.base_generation, pnu)?;
    Ok(Some(store.read_bytes(&key).await?))
}
