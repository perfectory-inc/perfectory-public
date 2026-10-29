//! Which open 공매 items are owed a 물건상세, when an item counts as complete, and the coverage the
//! collector reports per round (root ADR-0330; per item by root ADR-0339).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value as JsonValue;

use super::{Endpoint, ItemKey, ItemMark, Listed, State, KST_HOURS};

/// The node-exporter textfile the coverage series land in (root ADR-0330 §4).
const METRICS_FILE: &str = "onbid-item-detail.prom";

/// 14 KST digits (`yyyyMMddHHmmss`) of a provider date-time (`yyyyMMddHHmm` or with seconds).
pub(crate) fn kst14(value: &JsonValue) -> Option<String> {
    let text = match value {
        JsonValue::String(text) => text.trim().to_owned(),
        JsonValue::Number(number) => number.to_string(),
        _ => return None,
    };
    if !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match text.len() {
        12 => Some(format!("{text}00")),
        14 => Some(text),
        _ => None,
    }
}

/// An instant as 14 KST digits, comparable with [`kst14`].
pub(crate) fn kst14_of(at: DateTime<Utc>) -> String {
    (at + chrono::Duration::hours(KST_HOURS))
        .format("%Y%m%d%H%M%S")
        .to_string()
}

/// What the item detail needs of one list row: its bid end and the digest of its refresh fields.
pub(crate) fn listed_of(endpoint: &Endpoint, item: &JsonValue) -> Listed {
    use sha2::{Digest as _, Sha256};
    let mut hasher = Sha256::new();
    for field in &endpoint.refresh_on_list_fields {
        hasher.update(field.as_bytes());
        hasher.update([0]);
        hasher.update(item[field].to_string().as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Listed {
        bid_ends_kst: endpoint
            .bid_end_field
            .as_ref()
            .and_then(|field| kst14(&item[field])),
        digest,
    }
}

/// One open item whose detail is owed, asked through one of its rounds (root ADR-0339 §1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Owed {
    /// The round asked: the item's earliest-closing round whose bid has not ended.
    pub(crate) key: ItemKey,
    /// The list digests its rounds show now; kept on the item's mark once its media landed.
    pub(crate) seen: BTreeSet<String>,
    /// It was complete, and a round now shows a digest it was not complete at.
    pub(crate) refresh: bool,
}

/// Folds the per-round marks of root ADR-0330 into per-item marks (root ADR-0339 §1): a round's
/// media were its item's (measured 2026-10-29: every item's rounds held the same files), so a
/// complete round makes its item complete at that round's digest.
fn adopt_round_marks(state: &mut State) {
    for (key, mark) in std::mem::take(&mut state.details) {
        let item = state.items.entry(key.cltr_mng_no).or_default();
        if !mark.digest.is_empty() {
            item.seen.insert(mark.digest);
        }
        item.photos = item.photos.max(mark.photos);
    }
}

/// The open items whose detail is owed, in the order they are asked (root ADR-0330 §2 per item,
/// root ADR-0339 §1): items with a round whose bid has not ended before the rest, never-fetched before
/// changed, the earliest such bid end first (unknown last), then by key. An item is owed again when
/// one of its rounds shows a list digest the item was not complete at — a round whose `mdfcnDt`
/// changed, or a new round with a stamp none of the others had; a new round with a known stamp is not.
pub(crate) fn owed_details(
    state: &mut State,
    open: &BTreeSet<ItemKey>,
    now: DateTime<Utc>,
) -> Vec<Owed> {
    adopt_round_marks(state);
    let now = kst14_of(now);
    let mut rounds: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for key in open {
        let listed = state.listed.get(key);
        let ends = listed.and_then(|listed| listed.bid_ends_kst.clone());
        let ended = ends.as_deref().is_some_and(|ends| ends < now.as_str());
        rounds.entry(key.cltr_mng_no.as_str()).or_default().push((
            (ended, ends.is_none(), ends),
            key,
            listed.map(|listed| listed.digest.clone()),
        ));
    }
    let mut owed = Vec::new();
    for (cltr, mut item_rounds) in rounds {
        item_rounds.sort();
        let seen = item_rounds
            .iter()
            .filter_map(|(_, _, digest)| digest.clone())
            .collect::<BTreeSet<_>>();
        let refresh = match state.items.get_mut(cltr) {
            None => false,
            // A mark made before the item list was read adopts the list's digests instead of a refetch.
            Some(mark) if mark.seen.is_empty() => {
                mark.seen = seen;
                continue;
            }
            Some(mark) if seen.is_subset(&mark.seen) => continue,
            Some(_) => true,
        };
        let ((ended, unknown, ends), key, _) = item_rounds.swap_remove(0);
        owed.push((
            (ended, refresh, unknown, ends),
            Owed {
                key: key.clone(),
                seen,
                refresh,
            },
        ));
    }
    owed.sort_by(|(left, a), (right, b)| left.cmp(right).then_with(|| a.key.cmp(&b.key)));
    owed.into_iter().map(|(_, owed)| owed).collect()
}

/// A detail fetched this run: which item (through which round), at which list digests, and its
/// Bronze line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Fetched {
    pub(crate) key: ItemKey,
    pub(crate) seen: BTreeSet<String>,
    pub(crate) refresh: bool,
    /// Index of its line in `Fetcher::lines[ItemDetail]`.
    pub(crate) line: usize,
}

/// How the media of one detail line ended.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LineOutcome {
    /// Every media URL of the line is stored or permanently skipped.
    pub(crate) complete: bool,
    /// Photos of the line now stored.
    pub(crate) photos: u32,
}

/// Marks complete the items whose media all landed; the rest stay owed and are fetched again by a
/// later run (root ADR-0330 §1, per item by root ADR-0339 §1). Returns how many were marked.
pub(crate) fn settle_details(
    state: &mut State,
    fetched: &[Fetched],
    outcomes: &[LineOutcome],
) -> usize {
    let mut marked = 0;
    for fetched in fetched {
        if let Some(outcome) = outcomes
            .get(fetched.line)
            .filter(|outcome| outcome.complete)
        {
            state.items.insert(
                fetched.key.cltr_mng_no.clone(),
                ItemMark {
                    seen: fetched.seen.clone(),
                    photos: outcome.photos,
                },
            );
            marked += 1;
        }
    }
    marked
}

/// Detail-and-media coverage of the conditions whose bid has not ended (root ADR-0330 §4).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct Coverage {
    pub(crate) open: usize,
    pub(crate) covered: usize,
    pub(crate) with_photos: usize,
}

impl Coverage {
    pub(crate) fn of(state: &State, now: DateTime<Utc>) -> Self {
        let now = kst14_of(now);
        let mut coverage = Self::default();
        for key in state.open_items.iter().flatten() {
            let ends = state
                .listed
                .get(key)
                .and_then(|listed| listed.bid_ends_kst.as_deref());
            if ends.is_some_and(|ends| ends < now.as_str()) {
                continue;
            }
            coverage.open += 1;
            // A round is covered when its item is: the media belong to the item (root ADR-0339).
            if let Some(mark) = state.items.get(&key.cltr_mng_no) {
                coverage.covered += 1;
                coverage.with_photos += usize::from(mark.photos > 0);
            }
        }
        coverage
    }

    /// The node-exporter textfile series (root ADR-0321 §5's collector; rule
    /// `FoundationOnbidDetailCoverageRegressed`).
    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn prometheus(&self, now: DateTime<Utc>) -> String {
        let ratio = if self.open == 0 {
            1.0
        } else {
            self.covered as f64 / self.open as f64
        };
        format!(
            "# HELP foundation_onbid_item_detail_open_conditions Onbid item conditions listed whose bid has not ended (root ADR-0330).\n\
             # TYPE foundation_onbid_item_detail_open_conditions gauge\n\
             foundation_onbid_item_detail_open_conditions {}\n\
             # HELP foundation_onbid_item_detail_covered_conditions Of those, the ones whose detail was fetched and whose media all landed.\n\
             # TYPE foundation_onbid_item_detail_covered_conditions gauge\n\
             foundation_onbid_item_detail_covered_conditions {}\n\
             # HELP foundation_onbid_item_detail_with_photos_conditions Of the covered, the ones with at least one photo stored.\n\
             # TYPE foundation_onbid_item_detail_with_photos_conditions gauge\n\
             foundation_onbid_item_detail_with_photos_conditions {}\n\
             # HELP foundation_onbid_item_detail_coverage_ratio covered / open (1 when nothing is open).\n\
             # TYPE foundation_onbid_item_detail_coverage_ratio gauge\n\
             foundation_onbid_item_detail_coverage_ratio {ratio}\n\
             # HELP foundation_onbid_item_detail_written_timestamp_seconds When the collector last wrote these series.\n\
             # TYPE foundation_onbid_item_detail_written_timestamp_seconds gauge\n\
             foundation_onbid_item_detail_written_timestamp_seconds {}\n",
            self.open,
            self.covered,
            self.with_photos,
            now.timestamp()
        )
    }
}

/// Drops the stored-media memo of items no longer listed, so it holds the open items' media only.
pub(crate) fn prune_media_memo(state: &mut State) {
    let Some(open) = &state.open_items else {
        return;
    };
    let items = open
        .iter()
        .map(|key| key.cltr_mng_no.as_str())
        .collect::<BTreeSet<_>>();
    state
        .media
        .retain(|_, known| items.contains(known.cltr_mng_no.as_str()));
}

/// The coverage series as a node-exporter textfile, replaced whole (write, then rename).
pub(crate) fn write_metrics(
    dir: &Path,
    coverage: &Coverage,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let path = dir.join(METRICS_FILE);
    let tmp = dir.join(format!("{METRICS_FILE}.tmp"));
    std::fs::write(&tmp, coverage.prometheus(now))
        .with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("failed to install {}", path.display()))
}
