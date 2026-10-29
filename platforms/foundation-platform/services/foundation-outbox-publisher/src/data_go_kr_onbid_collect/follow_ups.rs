//! The two follow-up APIs of root ADR-0340 §3: 물건상세 입찰정보 once per open item, and 공고상세
//! once per listed announcement. Each stays within its own daily call limit and run cap, and a
//! failure is isolated like the item detail's: counted, the loop stops, the run still succeeds.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use super::{
    owed_bid_infos, Answer, Api, Fetcher, ItemKey, ItemMark, LineOutcome, Owed, RunReport, Sent,
    SourceContract, State,
};

/// 물건상세 입찰정보 (getCltrBidInf2): once per open item, the detail's unit and order, marked
/// complete on any answer (no media).
pub(crate) async fn collect_bid_infos(
    contract: &SourceContract,
    fetcher: &mut Fetcher<'_>,
    state: &mut State,
    open: &BTreeSet<ItemKey>,
    now: DateTime<Utc>,
    report: &mut RunReport,
) {
    let listed_items = open
        .iter()
        .map(|key| key.cltr_mng_no.as_str())
        .collect::<BTreeSet<_>>();
    state
        .bid_infos
        .retain(|cltr, _| listed_items.contains(cltr.as_str()));
    let per_run = contract
        .item_bid_info
        .per_run_limit
        .map_or(usize::MAX, |limit| limit as usize);
    let owed = owed_bid_infos(state, open, now);
    let owed_total = owed.len();
    for Owed { key, seen, .. } in owed.into_iter().take(per_run) {
        let params = [
            ("cltrMngNo", key.cltr_mng_no.clone()),
            ("pbctCdtnNo", key.pbct_cdtn_no.to_string()),
        ];
        match fetcher.send(state, Api::ItemBidInfo, &params, now).await {
            Ok(Sent::OverBudget | Sent::Answer(Answer::QuotaRefused)) => {
                report
                    .stopped_by_budget
                    .push(Api::ItemBidInfo.name().to_owned());
                break;
            }
            Ok(Sent::Answer(Answer::Rows { .. } | Answer::NoData)) => {
                report.bid_infos_received += 1;
                state.bid_infos.insert(
                    key.cltr_mng_no,
                    ItemMark {
                        seen,
                        ..ItemMark::default()
                    },
                );
            }
            Err(error) => {
                report.bid_infos_errored += 1;
                tracing::warn!(error = %error, "Onbid item bid info fetch failed; isolated from the run");
                break;
            }
        }
    }
    report.bid_infos_owed = owed_total - report.bid_infos_received;
}

/// 공고상세 (getPbancDtlInf2): each announcement this run's announcement list held (`held`:
/// pbancMngNo → row digest) that was never complete or whose row changed, by announcement number,
/// at most per_run_limit. Its attachments are mirrored by `run`; it is marked complete
/// ([`settle_announcements`]) only once they all landed.
pub(crate) async fn collect_announcement_details(
    contract: &SourceContract,
    fetcher: &mut Fetcher<'_>,
    state: &mut State,
    held: BTreeMap<String, String>,
    now: DateTime<Utc>,
    report: &mut RunReport,
) {
    state
        .announcements
        .retain(|number, _| held.contains_key(number));
    let per_run = contract
        .announcement_detail
        .per_run_limit
        .map_or(usize::MAX, |limit| limit as usize);
    let owed = held
        .into_iter()
        .filter(|(number, digest)| state.announcements.get(number) != Some(digest))
        .collect::<Vec<_>>();
    let owed_total = owed.len();
    for (number, digest) in owed.into_iter().take(per_run) {
        let params = [("pbancMngNo", number.clone())];
        match fetcher
            .send(state, Api::AnnouncementDetail, &params, now)
            .await
        {
            Ok(Sent::OverBudget | Sent::Answer(Answer::QuotaRefused)) => {
                report
                    .stopped_by_budget
                    .push(Api::AnnouncementDetail.name().to_owned());
                break;
            }
            Ok(Sent::Answer(Answer::Rows { .. } | Answer::NoData)) => {
                let line = fetcher
                    .lines
                    .get(&Api::AnnouncementDetail)
                    .map_or(0, |lines| lines.len().saturating_sub(1));
                report.announcements_received += 1;
                report.announcements_fetched.push((number, digest, line));
            }
            Err(error) => {
                report.announcements_errored += 1;
                tracing::warn!(error = %error, "Onbid announcement detail fetch failed; isolated from the run");
                break;
            }
        }
    }
    report.announcements_owed = owed_total - report.announcements_received;
}

/// A short digest of a whole list row: an announcement is asked again when its row changed.
pub(crate) fn row_digest(item: &JsonValue) -> String {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(item.to_string().as_bytes())[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Marks complete the announcements whose attachments all landed (root ADR-0340 §3).
pub(crate) fn settle_announcements(
    state: &mut State,
    fetched: &[(String, String, usize)],
    outcomes: &[LineOutcome],
) -> usize {
    let mut marked = 0;
    for (number, digest, line) in fetched {
        if outcomes.get(*line).is_some_and(|outcome| outcome.complete) {
            state.announcements.insert(number.clone(), digest.clone());
            marked += 1;
        }
    }
    marked
}
