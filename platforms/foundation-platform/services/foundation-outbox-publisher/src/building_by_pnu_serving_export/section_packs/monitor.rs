//! `monitor-building-by-pnu-serving`: the hourly synthetic read of the live building hostname
//! (root ADR-0151 §6, contract `section_packs.monitor`).
//!
//! Reads the first `monitor.pnus` PNUs of the gate sample (the equality evidence on the host; the
//! real PNUs never enter the repository) from the live hostname and holds each answer to:
//!
//! - 200, within the read timeout;
//! - the document the lane serves, resolved by the publisher's own reader from R2 (the packs when
//!   the manifest names them, else the objects);
//! - while objects still exist: the object document, for a PNU no object patch covers (a covered
//!   one is reported as not compared, never guessed);
//! - a p95 within `monitor.latency_p95_max_ms`.
//!
//! Any breach fails the run, so the unit's `OnFailure` reports it to Slack. Read-only.

use std::path::PathBuf;

use anyhow::{ensure, Context};
use serde::Serialize;

use super::super::{optional_env, LANE};
use super::equality::write_evidence;
use super::gate::{self, EqualityEvidence, Timings};
use super::inspect;
use super::latency::{normalized_digest, timed_get, timings, Accept, REQUEST_TIMEOUT};
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::by_pnu;

/// One run's report, written beside the unit's journal.
#[derive(Debug, Serialize)]
pub(crate) struct MonitorReport {
    pub(crate) base_url: String,
    pub(crate) pnus: usize,
    pub(crate) answered: usize,
    pub(crate) failures: Vec<String>,
    /// Answers that differ from what the lane serves (packs or objects).
    pub(crate) drifted_from_served: Vec<String>,
    /// Answers that differ from the object document.
    pub(crate) drifted_from_objects: Vec<String>,
    /// PNUs an object patch covers, whose object was not compared.
    pub(crate) objects_not_compared: usize,
    pub(crate) latency_ms: Timings,
    pub(crate) latency_p95_max_ms: f64,
    pub(crate) passed: bool,
    pub(crate) checked_at_utc: String,
}

/// Runs the monitor.
///
/// # Errors
/// Returns an error when the inputs cannot be read, or any check fails.
pub(crate) async fn run() -> anyhow::Result<()> {
    let env = |name: &str| optional_env(&LANE.env(name));
    let required = |name: &str| -> anyhow::Result<String> {
        env(name)?.with_context(|| format!("{} is required", LANE.env(name)))
    };
    let policy = LANE
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
    let store = ByPnuServingStore::open(LANE, &output)?;
    let base_url = match env("MONITOR_BASE_URL")? {
        Some(url) => url,
        None => format!("https://{}", LANE.policy()?.public_hostname),
    };
    let report = check(&store, &base_url, &pnus, policy.latency_p95_max_ms).await?;
    if let Some(path) = env("MONITOR_REPORT_PATH")? {
        write_evidence(&PathBuf::from(path), &report)?;
    }
    tracing::info!(report = ?report, "building serving monitor");
    ensure!(
        report.passed,
        "the live building hostname breached the monitor: {} failures, {} drifted from what the \
         lane serves, {} from the objects, p95 {:.0} ms (bound {:.0} ms)",
        report.failures.len(),
        report.drifted_from_served.len(),
        report.drifted_from_objects.len(),
        report.latency_ms.p95,
        report.latency_p95_max_ms
    );
    Ok(())
}

/// Reads every PNU from `base_url` and holds it to the lane's documents.
///
/// # Errors
/// Returns an error when the manifest or a pack cannot be read.
pub(crate) async fn check(
    store: &ByPnuServingStore,
    base_url: &str,
    pnus: &[String],
    latency_p95_max_ms: f64,
) -> anyhow::Result<MonitorReport> {
    let prefix = &LANE.policy()?.request_path.prefix;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    let (manifest, _) = store.read_manifest().await?;
    let served = ServedManifest::parse(LANE, &manifest)?;
    let mut report = MonitorReport {
        base_url: base_url.to_owned(),
        pnus: pnus.len(),
        answered: 0,
        failures: Vec::new(),
        drifted_from_served: Vec::new(),
        drifted_from_objects: Vec::new(),
        objects_not_compared: 0,
        latency_ms: Timings::default(),
        latency_p95_max_ms,
        passed: false,
        checked_at_utc: crate::by_pnu_serving_manifest_publish::now(),
    };
    let mut latency = Vec::new();
    for pnu in pnus {
        let url = format!("{}{prefix}{pnu}", base_url.trim_end_matches('/'));
        let answer = match timed_get(&client, &url, Accept::Gzip).await {
            Ok(answer) => answer,
            Err(failure) => {
                report.failures.push(format!("{pnu}: {}", failure.class));
                continue;
            }
        };
        report.answered += 1;
        latency.push(answer.ms);
        let live = normalized_digest(&answer.body)?;
        let object = object_document(store, &served, pnu).await?;
        let lane = if served.section_packs.is_some() {
            let inspected = inspect::inspect(store, pnu, None).await?;
            ensure!(
                inspected["answer"] == "document",
                "the packs do not serve {pnu} as a document: {}",
                inspected["answer"]
            );
            Some(serde_json::to_vec(&inspected["document"])?)
        } else {
            object.clone()
        };
        if lane.as_deref().map(normalized_digest).transpose()? != Some(live) {
            report.drifted_from_served.push(pnu.clone());
        }
        match object {
            Some(object) if normalized_digest(&object)? != live => {
                report.drifted_from_objects.push(pnu.clone());
            }
            Some(_) => {}
            None => report.objects_not_compared += 1,
        }
    }
    report.latency_ms = timings(&mut latency);
    report.passed = report.answered == pnus.len()
        && report.failures.is_empty()
        && report.drifted_from_served.is_empty()
        && report.drifted_from_objects.is_empty()
        && report.latency_ms.p95 <= latency_p95_max_ms;
    Ok(report)
}

/// The object document of `pnu` in the manifest's base generation, or `None` when an object patch
/// covers its prefix (then the base object may not be what the object lane serves).
async fn object_document(
    store: &ByPnuServingStore,
    served: &ServedManifest,
    pnu: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
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
    let key = by_pnu::object_key(LANE, served.base_generation, pnu)?;
    Ok(Some(store.read_bytes(&key).await?))
}
