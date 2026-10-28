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
//! With neither, it also holds the served manifest's `published_at_utc` to the contract's
//! `by_pnu_section_packs.freshness_budget_hours` (root ADR-0306): from 2026-10-16 to 10-28 every
//! lane served a manifest 10-12 days old while every run of this monitor passed, since the hostname
//! was faithful to it. Staleness is kept apart from `passed`, which a Worker deploy reads as "this
//! version serves what the lane serves" (scripts/deploy/worker_autodeploy.py).
//!
//! Any breach fails the run, so the unit's `OnFailure` reports it to Slack. Read-only.

use std::path::PathBuf;

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
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
    /// When the manifest the lane serves was published, and how many hours before this run.
    pub served_published_at_utc: String,
    pub served_age_hours: f64,
    /// The budget the age was held to; `None` for a generation or recorded state, not judged.
    pub freshness_budget_hours: Option<f64>,
    /// The served manifest is older than the budget (root ADR-0306).
    pub stale: bool,
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
    let mut report = check_at(&store, &base_url, &pnus, policy.latency_p95_max_ms, &target).await?;
    if matches!(target, Target::Served) {
        judge_freshness(
            &mut report,
            crate::by_pnu_gateway_contract::section_pack_policy()?.freshness_budget_hours,
        );
    }
    if let Some(path) = env("MONITOR_REPORT_PATH")? {
        write_evidence(&PathBuf::from(path), &report)?;
    }
    tracing::info!(report = ?report, lane = lane.name(), "serving monitor");
    verdict(&report, lane.noun())
}

/// Holds the report's served age to `budget_hours` (root ADR-0306).
pub fn judge_freshness(report: &mut MonitorReport, budget_hours: f64) {
    report.freshness_budget_hours = Some(budget_hours);
    report.stale = report.served_age_hours > budget_hours;
}

/// Hours from `published_at_utc` (RFC 3339) to `now`.
///
/// # Errors
/// Returns an error when the time is not RFC 3339.
pub fn age_hours(published_at_utc: &str, now: DateTime<Utc>) -> anyhow::Result<f64> {
    let published = DateTime::parse_from_rfc3339(published_at_utc).with_context(|| {
        format!("the served manifest's published_at_utc {published_at_utc:?} is not RFC 3339")
    })?;
    #[allow(clippy::cast_precision_loss)] // seconds since a publish fit an f64 exactly
    let seconds = (now - published.with_timezone(&Utc)).num_seconds() as f64;
    Ok(seconds / 3600.0)
}

/// The run's verdict: a serving breach first, then a served manifest past its budget.
///
/// # Errors
/// Returns an error naming the breach.
pub fn verdict(report: &MonitorReport, noun: &str) -> anyhow::Result<()> {
    ensure!(
        report.passed,
        "the live {noun} hostname breached the monitor: {} failures, {} drifted from what the \
         lane serves, p95 {:.0} ms (bound {:.0} ms)",
        report.failures.len(),
        report.drifted_from_served.len(),
        report.latency_ms.p95,
        report.latency_p95_max_ms
    );
    ensure!(
        !report.stale,
        "the {noun} lane serves a manifest published {} ({:.0} h ago), past its freshness budget \
         of {:.0} h: no bake has published since, whatever the jobs reported (root ADR-0306; \
         runbook docs/runbooks/parcel-by-pnu-serving-bake.md section 9)",
        report.served_published_at_utc,
        report.served_age_hours,
        report.freshness_budget_hours.unwrap_or_default()
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
        served_age_hours: age_hours(&served.published_at_utc, Utc::now())?,
        served_published_at_utc: served.published_at_utc.clone(),
        freshness_budget_hours: None,
        stale: false,
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

#[cfg(test)]
mod tests {
    use super::{age_hours, judge_freshness, verdict, MonitorReport, Timings};
    use chrono::{DateTime, Utc};

    const BUDGET_HOURS: f64 = 48.0;
    const NOW: &str = "2026-10-28T06:17:00Z";

    /// A run in which every PNU answered with the document the lane serves, in time.
    fn faithful(published_at_utc: &str) -> anyhow::Result<MonitorReport> {
        let now = DateTime::parse_from_rfc3339(NOW)?.with_timezone(&Utc);
        Ok(MonitorReport {
            base_url: "https://lane.invalid".to_owned(),
            pnus: 20,
            answered: 20,
            failures: Vec::new(),
            drifted_from_served: Vec::new(),
            latency_ms: Timings::default(),
            latency_p95_max_ms: 3_000.0,
            passed: true,
            checked_at_utc: NOW.to_owned(),
            served_published_at_utc: published_at_utc.to_owned(),
            served_age_hours: age_hours(published_at_utc, now)?,
            freshness_budget_hours: None,
            stale: false,
        })
    }

    #[test]
    fn a_lane_published_within_its_budget_passes() -> anyhow::Result<()> {
        let mut report = faithful("2026-10-27T10:40:00Z")?;
        judge_freshness(&mut report, BUDGET_HOURS);
        assert!(!report.stale, "{report:?}");
        verdict(&report, "parcel")
    }

    #[test]
    fn a_lane_older_than_its_budget_fails() -> anyhow::Result<()> {
        let mut report = faithful("2026-10-26T06:00:00Z")?;
        judge_freshness(&mut report, BUDGET_HOURS);
        assert!(report.stale, "{report:?}");
        let refused = verdict(&report, "parcel").map_err(|error| error.to_string());
        assert!(
            matches!(&refused, Err(message) if message.contains("freshness budget")),
            "{refused:?}"
        );
        Ok(())
    }

    /// 2026-10-28: the building lane still served its 2026-10-16 manifest. Every hostname was
    /// faithful to it and every job said it succeeded, so every run of the monitor passed.
    #[test]
    fn a_faithful_hostname_serving_a_stale_manifest_fails() -> anyhow::Result<()> {
        let mut report = faithful("2026-10-16T12:10:35Z")?;
        assert!(report.served_age_hours > 280.0, "{report:?}");
        judge_freshness(&mut report, BUDGET_HOURS);
        // A Worker deploy reads `passed` as "this version serves what the lane serves": still so.
        assert!(report.passed && report.stale, "{report:?}");
        assert!(verdict(&report, "building").is_err());
        Ok(())
    }

    #[test]
    fn a_generation_or_recorded_state_is_not_judged() -> anyhow::Result<()> {
        let report = faithful("2026-10-16T12:10:35Z")?;
        assert_eq!(report.freshness_budget_hours, None);
        verdict(&report, "building")
    }

    #[test]
    fn a_published_time_that_is_not_rfc_3339_is_refused() {
        assert!(age_hours("yesterday", Utc::now()).is_err());
    }
}
