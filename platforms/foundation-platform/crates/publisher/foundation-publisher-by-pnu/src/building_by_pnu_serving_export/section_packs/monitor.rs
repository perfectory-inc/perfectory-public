//! `monitor-building-by-pnu-serving`: the hourly synthetic read of the live building hostname
//! (root ADR-0151 §6, contract `section_packs.monitor`).
//!
//! Reads `monitor.pnus` PNUs the lane serves as a document from the live hostname, half of them
//! drawn anew on every run from what its newest patches wrote, the rest from the gate sample (the
//! equality evidence on the host; the real PNUs never enter the repository; root ADR-0446), and
//! holds each answer to:
//!
//! - 200, within the read timeout, or a 404 for a PNU the lane serves as a tombstone or not at all
//!   (root ADR-0437: a lane whose keys come and go, 공매, retires sampled PNUs as their notices end);
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
//! version serves what the lane serves" (scripts/deploy/worker_autodeploy.py). The report carries the
//! age and `stale` for the series `FoundationByPnuServingStale` reads (serving_freshness_metrics.py);
//! the rule, not this run, says the lane is stale, since only the asset reconciler knows whether a
//! lane is behind its inputs (root ADR-0444: 2026-10-17..11-07 four lanes serving the newest Silver
//! there was failed this monitor hourly on a 498-hour-old manifest).
//!
//! A serving breach fails the run, so the unit's `OnFailure` reports it. Read-only.

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
use crate::by_pnu_pack::EntryState;
use crate::by_pnu_serving_manifest::ServedManifest;
use crate::by_pnu_serving_store::{local_root, ByPnuServingStore};
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;
use crate::r2_layout::{by_pnu, by_pnu_packs};

/// One run's report, written beside the unit's journal.
#[derive(Debug, Serialize)]
pub struct MonitorReport {
    pub base_url: String,
    pub pnus: usize,
    pub answered: usize,
    pub failures: Vec<String>,
    /// Answers that differ from what the lane serves (packs or objects).
    pub drifted_from_served: Vec<String>,
    /// PNUs that answered 404 because the lane serves no document for them (a tombstone, or absent
    /// from its packs): a faithful answer (root ADR-0437).
    pub absent_as_served: Vec<String>,
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
    let sampled = equality.sample.len();
    let pnus = served_sample(&store, equality.sample, policy.pnus, &target).await?;
    ensure!(
        !pnus.is_empty(),
        "the lane serves none of the {sampled} PNUs of its gate sample as a document any more; \
         take a new sample (root ADR-0437)"
    );
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

/// The run's verdict: whether the hostname serves what the lane serves. A served manifest past its
/// budget is the alert's to judge against the lane's inputs (root ADR-0444), not the run's.
///
/// # Errors
/// Returns an error naming the breach.
pub fn verdict(report: &MonitorReport, noun: &str) -> anyhow::Result<()> {
    ensure!(
        report.answered > 0 || report.absent_as_served.is_empty(),
        "the live {noun} hostname answered no sampled PNU with a document: the lane serves none of \
         the {} PNUs of its gate sample any more, so the monitor holds it to nothing; take a new \
         sample (root ADR-0437)",
        report.pnus
    );
    ensure!(
        report.passed,
        "the live {noun} hostname breached the monitor: {} failures, {} drifted from what the \
         lane serves, p95 {:.0} ms (bound {:.0} ms)",
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
        absent_as_served: Vec::new(),
        latency_ms: Timings::default(),
        latency_p95_max_ms,
        passed: false,
        checked_at_utc: crate::by_pnu_serving_manifest_publish::now(),
        served_age_hours: age_hours(&served.published_at_utc, Utc::now())?,
        served_published_at_utc: served.published_at_utc.clone(),
        freshness_budget_hours: None,
        stale: false,
    };
    let packs = !matches!(target, Target::Served) || served.section_packs.is_some();
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
                // A 404 is the answer the lane serves for a PNU its packs hold no document for.
                if failure.class == "http-404" && packs {
                    let inspected = inspect_target(store, pnu, target).await?;
                    if matches!(inspected["answer"].as_str(), Some("tombstone" | "absent")) {
                        report.absent_as_served.push(pnu.clone());
                        continue;
                    }
                }
                report.failures.push(format!("{pnu}: {}", failure.class));
                continue;
            }
        };
        report.answered += 1;
        latency.push(answer.ms);
        let live = answer.content()?;
        let served_bytes = if packs {
            let inspected = inspect_target(store, pnu, target).await?;
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
    judge_serving(&mut report);
    Ok(report)
}

/// Whether every sampled PNU got the answer the lane serves, in time: a document for at least one
/// of them, and a 404 only for the PNUs it serves no document for.
pub fn judge_serving(report: &mut MonitorReport) {
    report.passed = report.answered > 0
        && report.answered + report.absent_as_served.len() == report.pnus
        && report.failures.is_empty()
        && report.drifted_from_served.is_empty()
        && report.latency_ms.p95 <= report.latency_p95_max_ms;
}

/// The `wanted` PNUs this run reads, `target` serving each as a document. A lane whose keys come
/// and go is held to `wanted` live PNUs on every run, not to fewer as its first ones retire
/// (2026-11-07: 4 of the 공매 lane's first 20 had ended in 14 days; root ADR-0437). Served packs
/// give half of them anew on every run from the documents their newest patches wrote
/// (`recently_written`), the rest from the gate sample: the cut-over's sample alone never reads a
/// key that came after it (root ADR-0446: on 2026-11-08, 15 days after its cut-over, 1,234 of the
/// 공매 lane's 4,677 sampled PNUs had left its Gold and none of the notices published since was in
/// the sample). Either side fills what the other lacks. A lane that serves objects takes the first
/// `wanted` of the sample, as before.
pub async fn served_sample(
    store: &ByPnuServingStore,
    sample: Vec<String>,
    wanted: usize,
    target: &Target,
) -> anyhow::Result<Vec<String>> {
    let fresh = match target {
        Target::Served => {
            let served = ServedManifest::parse(store.lane(), &store.read_manifest().await?.0)?;
            let Some(state) = served.section_packs else {
                return Ok(sample.into_iter().take(wanted).collect());
            };
            recently_written(store, &state, wanted).await?
        }
        Target::Generation(_) | Target::Recorded(..) => Vec::new(),
    };
    let mut chosen = Vec::with_capacity(wanted);
    let mut rest = fresh.iter();
    for pnu in rest.by_ref().take(wanted / 2) {
        if inspect_target(store, pnu, target).await?["answer"] == "document" {
            chosen.push(pnu.clone());
        }
    }
    for pnu in sample.iter().chain(rest) {
        if chosen.len() == wanted {
            break;
        }
        if !chosen.contains(pnu)
            && inspect_target(store, pnu, target).await?["answer"] == "document"
        {
            chosen.push(pnu.clone());
        }
    }
    Ok(chosen)
}

/// Up to `wanted` PNUs the newest patches of the served anchor section wrote a document for, one
/// per unit (a legal dong, or its part), newest patch first and, within a patch, its units in the
/// gate sample's seeded order (`gate::sample_rank`), the lowest-ranked document of each: what the
/// lane changed or added last, spread over dongs as a user's reads are. A key a newer patch wrote is
/// never taken from an older one. It reads at most `4 × wanted` packs, so a run stays bounded however
/// many units a patch wrote.
async fn recently_written(
    store: &ByPnuServingStore,
    state: &crate::by_pnu_section_pack_manifest::SectionPacksState,
    wanted: usize,
) -> anyhow::Result<Vec<String>> {
    let lane = store.lane();
    let anchor = lane.section_packs()?.anchor_section.clone();
    let view = super::read::PackView::served(store, lane, state).await?;
    let Some(section) = view.sections.iter().find(|section| section.name == anchor) else {
        return Ok(Vec::new());
    };
    let mut found = Vec::with_capacity(wanted);
    let mut seen = std::collections::HashSet::new();
    let mut reads = 0;
    for (patch, units) in &section.patches {
        let mut ranked = units
            .iter()
            .map(|unit| Ok((gate::sample_rank(unit)?, unit)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        ranked.sort_unstable();
        let mut written = Vec::new();
        for (_, unit) in ranked {
            if found.len() == wanted || reads == 4 * wanted {
                return Ok(found);
            }
            reads += 1;
            let key =
                by_pnu_packs::pack_key(lane, &anchor, section.generation, Some(*patch), unit)?;
            let pack = super::read::read_pack(store, &key).await?;
            let mut documents = Vec::new();
            for entry in &pack.entries {
                if by_pnu::check_key(lane, &entry.pnu).is_err() || seen.contains(&entry.pnu) {
                    continue;
                }
                written.push(entry.pnu.clone());
                if entry.state == EntryState::Document {
                    documents.push((gate::sample_rank(&entry.pnu)?, entry.pnu.clone()));
                }
            }
            if let Some((_, pnu)) = documents.into_iter().min() {
                found.push(pnu);
            }
        }
        // An older patch's entry for a key this one wrote is not what the lane serves.
        seen.extend(written);
    }
    Ok(found)
}

/// The report of `pnu` as `target` serves it.
async fn inspect_target(
    store: &ByPnuServingStore,
    pnu: &str,
    target: &Target,
) -> anyhow::Result<serde_json::Value> {
    match target {
        Target::Recorded(_, state) => inspect::inspect_state(store, pnu, state).await,
        Target::Generation(generation) => inspect::inspect(store, pnu, Some(*generation)).await,
        Target::Served => inspect::inspect(store, pnu, None).await,
    }
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
    use super::{age_hours, judge_freshness, judge_serving, verdict, MonitorReport, Timings};
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
            absent_as_served: Vec::new(),
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

    /// 2026-10-28: the building lane still served its 2026-10-16 manifest. Every hostname was
    /// faithful to it and every job said it succeeded. The report says it is past its budget, for
    /// the alert; whether that is stale is the lane's inputs' to say (root ADR-0444: zoning's
    /// 498-hour-old manifest served the newest Silver there was), so the run itself passes.
    #[test]
    fn an_old_manifest_served_faithfully_is_reported_past_its_budget() -> anyhow::Result<()> {
        let mut report = faithful("2026-10-16T12:10:35Z")?;
        assert!(report.served_age_hours > 280.0, "{report:?}");
        judge_freshness(&mut report, BUDGET_HOURS);
        // A Worker deploy reads `passed` as "this version serves what the lane serves": still so.
        assert!(report.passed && report.stale, "{report:?}");
        verdict(&report, "building")
    }

    #[test]
    fn a_generation_or_recorded_state_is_not_judged() -> anyhow::Result<()> {
        let report = faithful("2026-10-16T12:10:35Z")?;
        assert_eq!(report.freshness_budget_hours, None);
        verdict(&report, "building")
    }

    /// 2026-11-07: the 공매 lane's 10-24 gate sample held 4 PNUs whose notices had ended; the live
    /// hostname answered them 404, as the lane serves them, and every hourly run failed.
    #[test]
    fn a_404_for_a_pnu_the_lane_serves_no_document_for_passes() -> anyhow::Result<()> {
        let mut report = faithful("2026-11-06T21:54:00Z")?;
        report.answered = 16;
        report.absent_as_served = (0..4).map(|n| format!("9999{n}")).collect();
        judge_serving(&mut report);
        assert!(report.passed, "{report:?}");
        report.failures.push("99995: http-404".to_owned());
        report.absent_as_served.pop();
        judge_serving(&mut report);
        assert!(
            !report.passed,
            "a 404 the lane does not serve still fails: {report:?}"
        );
        Ok(())
    }

    #[test]
    fn a_sample_the_lane_serves_none_of_fails() -> anyhow::Result<()> {
        let mut report = faithful("2026-11-06T21:54:00Z")?;
        report.answered = 0;
        report.absent_as_served = (0..20).map(|n| format!("9999{n:02}")).collect();
        judge_serving(&mut report);
        assert!(!report.passed, "{report:?}");
        let refused = verdict(&report, "parcel public auction").map_err(|error| error.to_string());
        assert!(
            matches!(&refused, Err(message) if message.contains("take a new sample")),
            "{refused:?}"
        );
        Ok(())
    }

    #[test]
    fn a_published_time_that_is_not_rfc_3339_is_refused() {
        assert!(age_hours("yesterday", Utc::now()).is_err());
    }
}
