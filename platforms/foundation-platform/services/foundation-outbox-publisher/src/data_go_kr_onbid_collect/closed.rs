//! Closed items (root ADR-0542): an item that left the list before its 물건상세 landed, owed one
//! until the provider answers, after the open items and within the same caps.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::Context as _;

use chrono::{DateTime, Utc};
use serde::Serialize;

use super::{Answer, Api, Fetched, Fetcher, ItemKey, LineOutcome, RunReport, Sent, State};

/// A run's closed items: seeded this run, details received, answered no data (let go), landed with
/// all their media (let go), and still owed after the run.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct ClosedReport {
    pub(crate) seeded: usize,
    pub(crate) received: usize,
    pub(crate) gone: usize,
    pub(crate) landed: usize,
    pub(crate) owed: usize,
    /// The details received, for the media step to settle; not part of the manifest.
    #[serde(skip)]
    pub(crate) fetched: Vec<Fetched>,
}

/// How often a closed item is asked before it is let go (root ADR-0542): a detail whose media did
/// not all land is asked again, an answer of no data settles it at once.
pub(crate) const CLOSED_MAX_ASKS: u32 = 3;

/// The 물건상세 of items that left the list before theirs landed (root ADR-0542), after the open
/// items and within what is left of the run's `per_run_limit` and the API's daily budget, in key
/// order. An answer of no data means the provider withdrew it: settled now. A detail joins the
/// run's media step like an open item's, and `settle_closed` lets it go once its media landed.
pub(crate) async fn collect_closed_details(
    fetcher: &mut Fetcher<'_>,
    state: &mut State,
    open: &BTreeSet<ItemKey>,
    room: usize,
    now: DateTime<Utc>,
    report: &mut RunReport,
) {
    // Listed again: the open path owns it.
    state.closed_owed.retain(|key, _| !open.contains(key));
    let owed = state.closed_owed.keys().cloned().collect::<Vec<_>>();
    for key in owed.into_iter().take(room) {
        let params = [
            ("cltrMngNo", key.cltr_mng_no.clone()),
            ("pbctCdtnNo", key.pbct_cdtn_no.to_string()),
        ];
        match fetcher.send(state, Api::ItemDetail, &params, now).await {
            Ok(Sent::OverBudget | Sent::Answer(Answer::QuotaRefused)) => {
                report
                    .stopped_by_budget
                    .push(Api::ItemDetail.name().to_owned());
                break;
            }
            Ok(Sent::Answer(Answer::NoData)) => {
                state.closed_owed.remove(&key);
                report.closed.gone += 1;
            }
            Ok(Sent::Answer(Answer::Rows { .. })) => {
                let line = fetcher
                    .lines
                    .get(&Api::ItemDetail)
                    .map_or(0, |lines| lines.len().saturating_sub(1));
                report.closed.received += 1;
                report.closed.fetched.push(Fetched {
                    key,
                    seen: BTreeSet::new(),
                    refresh: false,
                    line,
                });
            }
            Err(error) => {
                report.details_errored += 1;
                tracing::warn!(error = %error, "Onbid closed item detail fetch failed; isolated from the run");
                break;
            }
        }
    }
    report.closed.owed = state.closed_owed.len();
}

/// Lets go of the closed items whose detail's media all landed this run, and counts an ask on the
/// rest, letting go at `CLOSED_MAX_ASKS` (root ADR-0542).
pub(crate) fn settle_closed(
    state: &mut State,
    report: &mut ClosedReport,
    outcomes: &[LineOutcome],
) {
    for fetched in &report.fetched {
        let complete = outcomes
            .get(fetched.line)
            .is_some_and(|outcome| outcome.complete);
        if complete {
            state.closed_owed.remove(&fetched.key);
            report.landed += 1;
        } else if let Some(asks) = state.closed_owed.get_mut(&fetched.key) {
            *asks += 1;
            if *asks >= CLOSED_MAX_ASKS {
                state.closed_owed.remove(&fetched.key);
            }
        }
    }
    report.owed = state.closed_owed.len();
}

/// Merges the closed items a seed names (root ADR-0542 §3: those Silver holds no 물건상세 for, from
/// before the collector kept them) into `closed_owed`, skipping the listed ones. Returns how many
/// were new.
pub(crate) fn seed_closed(state: &mut State, seed: &[ItemKey]) -> usize {
    let open = state.open_items.clone().unwrap_or_default();
    let mut added = 0;
    for key in seed {
        if !open.contains(key) && !state.closed_owed.contains_key(key) {
            state.closed_owed.insert(key.clone(), 0);
            added += 1;
        }
    }
    added
}

/// The items of the previous complete pass that left the list with no 물건상세 landed, owed one as
/// closed items; an item still listed through another round, or detailed, is not (root ADR-0542 §1).
/// Runs before the new pass replaces `open_items`.
pub(crate) fn owe_vanished(state: &mut State, current: &BTreeSet<ItemKey>) {
    if let Some(previous) = &state.open_items {
        let still_listed = current
            .iter()
            .map(|key| key.cltr_mng_no.as_str())
            .collect::<BTreeSet<_>>();
        for key in previous.difference(current) {
            let cltr = key.cltr_mng_no.as_str();
            if !still_listed.contains(cltr)
                && !state.items.contains_key(cltr)
                && !state
                    .closed_owed
                    .keys()
                    .any(|owed| owed.cltr_mng_no == cltr)
            {
                state.closed_owed.insert(key.clone(), 0);
            }
        }
    }
}

/// Reads the seed list of item keys at `path`, if any, into `closed_owed` (root ADR-0542 §3).
///
/// # Errors
/// The file does not read or is not a JSON list of item keys.
pub(crate) fn load_seed(state: &mut State, path: Option<&Path>) -> anyhow::Result<usize> {
    let Some(path) = path else {
        return Ok(0);
    };
    let seed: Vec<ItemKey> = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("{} is not a list of item keys", path.display()))?;
    Ok(seed_closed(state, &seed))
}
