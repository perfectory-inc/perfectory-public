//! Which price batch the unit price Gold reads (root ADR-0182).
//!
//! The newest vintage's, a vintage's ties broken by the order the table appended them
//! (`unit_official_price.newest_batch`, `order_batches`). The contract's edition order alone
//! (`row_versions`, newest by `vintage`) refuses those ties; a vintage joined again from newer hub
//! snapshots (2026-10-11) has a later batch, and the last appended wins.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;

/// The vintage a batch's `source_snapshot_id` names (`price:<n>|exclusive:<n>|vintage:<yyyymm>`),
/// or `None` for anything else (`unit_official_price.BATCH`).
#[must_use]
pub fn batch_vintage(id: &str) -> Option<&str> {
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    let rest = id.strip_prefix("price:")?;
    let (price, rest) = rest.split_once("|exclusive:")?;
    let (exclusive, vintage) = rest.split_once("|vintage:")?;
    (digits(price) && digits(exclusive) && digits(vintage) && vintage.len() == 6).then_some(vintage)
}

/// Batches in the order the table appended them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitOrder {
    /// The batches appended in a snapshot the table still holds, oldest first.
    pub ordered: Vec<String>,
    /// The batches whose appending snapshot has expired: expiry removes only snapshots older than
    /// every one it keeps, so each was appended before every ordered batch, in an order the table no
    /// longer records.
    pub before_retained: BTreeSet<String>,
    /// The batches whose last loads share a commit instant and a place in `main`'s chain: neither
    /// is later.
    pub tied: Vec<BTreeSet<String>>,
}

/// The batches the ingest registry records, in append order.
///
/// `ingested` maps each load identity (`unit-official-price/base-date-v2/<sido>/<batch>`) to the
/// snapshot that appended it; `snapshots` maps every snapshot the table holds to its parent and
/// commit instant (ms). A held snapshot is ordered by its instant, ties by its place in `main`'s
/// chain (a tagged one off the chain first); a batch is ordered by its last load. Batches whose last
/// loads tie are listed in [`CommitOrder::tied`], and [`newest_batch`] refuses to choose between
/// them (the Spark job takes the registry's order, which says nothing of which is later).
#[must_use]
pub fn order_batches(
    ingested: &BTreeMap<String, i64>,
    head: Option<i64>,
    snapshots: &BTreeMap<i64, (Option<i64>, i64)>,
) -> CommitOrder {
    let mut chain = Vec::new();
    let mut current = head;
    while let Some(snapshot) =
        current.filter(|id| snapshots.contains_key(id) && !chain.contains(id))
    {
        chain.push(snapshot);
        current = snapshots[&snapshot].0;
    }
    let position: BTreeMap<i64, i64> = chain
        .iter()
        .rev()
        .zip(0..)
        .map(|(snapshot, index)| (*snapshot, index))
        .collect();
    let mut last: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
    let mut expired = BTreeSet::new();
    for (record, snapshot) in ingested {
        let parts: Vec<&str> = record.splitn(4, '/').collect();
        let [_, _, _, batch] = parts[..] else {
            continue;
        };
        if batch_vintage(batch).is_none() {
            continue;
        }
        match snapshots.get(snapshot) {
            Some((_, instant)) => {
                let key = (*instant, position.get(snapshot).copied().unwrap_or(-1));
                let entry = last.entry(batch).or_insert(key);
                *entry = (*entry).max(key);
            }
            None => {
                expired.insert(batch.to_owned());
            }
        }
    }
    let mut by_key: BTreeMap<(i64, i64), BTreeSet<String>> = BTreeMap::new();
    for (batch, key) in last {
        by_key.entry(key).or_default().insert(batch.to_owned());
    }
    let ordered: Vec<String> = by_key.values().flatten().cloned().collect();
    let tied = by_key
        .into_values()
        .filter(|batches| batches.len() > 1)
        .collect();
    expired.retain(|batch| !ordered.contains(batch));
    CommitOrder {
        ordered,
        before_retained: expired,
        tied,
    }
}

/// The batch of the newest vintage among `ids`, the last appended of a tied vintage by `order`.
///
/// `None` when an id is not a batch (or there is none): which is newest is unknown, and the reader
/// takes the one id there is or refuses the mix.
///
/// # Errors
/// Returns an error when the newest vintage has several batches and `order` cannot tell which was
/// appended last.
pub fn newest_batch(ids: &BTreeSet<String>, order: &CommitOrder) -> anyhow::Result<Option<String>> {
    let vintages: Option<Vec<(&str, &str)>> = ids
        .iter()
        .map(|id| batch_vintage(id).map(|vintage| (id.as_str(), vintage)))
        .collect();
    let Some(vintages) = vintages.filter(|vintages| !vintages.is_empty()) else {
        return Ok(None);
    };
    let newest = vintages.iter().map(|(_, vintage)| *vintage).max();
    let batches: BTreeSet<&str> = vintages
        .iter()
        .filter(|(_, vintage)| Some(*vintage) == newest)
        .map(|(id, _)| *id)
        .collect();
    if let [only] = batches.iter().collect::<Vec<_>>()[..] {
        return Ok(Some((*only).to_owned()));
    }
    let ordered: Vec<&str> = order
        .ordered
        .iter()
        .map(String::as_str)
        .filter(|batch| batches.contains(batch))
        .collect();
    let covered = batches
        .iter()
        .all(|batch| ordered.contains(batch) || order.before_retained.contains(*batch));
    let tied = |last: &str| {
        order.tied.iter().any(|group| {
            group.contains(last)
                && group
                    .iter()
                    .filter(|batch| batches.contains(&batch.as_str()))
                    .count()
                    > 1
        })
    };
    match ordered.last() {
        Some(last) if covered && !tied(last) => Ok(Some((*last).to_owned())),
        _ => bail!(
            "silver.unit_official_price holds {} batches of vintage {} and their order is unknown: {:?}",
            batches.len(),
            newest.unwrap_or_default(),
            batches
        ),
    }
}

/// The batch the Gold reads among the ids the price table holds at its pin: the newest batch, else
/// the one id there is.
///
/// # Errors
/// Returns an error when a vintage's batches cannot be ordered, or the ids are not one batch set
/// and not exactly one id (`building_panel.assert_single_snapshot`).
pub fn chosen_batch(ids: &BTreeSet<String>, order: &CommitOrder) -> anyhow::Result<String> {
    if let Some(batch) = newest_batch(ids, order)? {
        return Ok(batch);
    }
    match ids.iter().collect::<Vec<_>>()[..] {
        [only] if !only.is_empty() => Ok(only.clone()),
        _ => bail!(
            "silver.unit_official_price must carry exactly one non-empty source_snapshot_id: {:?}",
            ids.iter().take(3).collect::<Vec<_>>()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(price: u32, exclusive: u32, vintage: &str) -> String {
        format!("price:{price}|exclusive:{exclusive}|vintage:{vintage}")
    }

    fn set(ids: &[&String]) -> BTreeSet<String> {
        ids.iter().map(|id| (*id).clone()).collect()
    }

    fn order(ordered: &[&String]) -> CommitOrder {
        CommitOrder {
            ordered: ordered.iter().map(|id| (*id).clone()).collect(),
            ..CommitOrder::default()
        }
    }

    /// `test_the_panel_reads_the_newest_vintages_batch`.
    #[test]
    fn the_newest_vintage_s_batch_is_read() -> anyhow::Result<()> {
        let (older, newer) = (batch(1, 2, "209907"), batch(3, 4, "209908"));
        assert_eq!(batch_vintage(&newer), Some("209908"));
        let none = CommitOrder::default();
        assert_eq!(
            newest_batch(&set(&[&older, &newer]), &none)?,
            Some(newer.clone())
        );
        assert_eq!(newest_batch(&set(&[&older]), &none)?, Some(older.clone()));
        let foreign = "older-source".to_owned();
        assert_eq!(newest_batch(&set(&[&older, &foreign]), &none)?, None);
        assert_eq!(newest_batch(&BTreeSet::new(), &none)?, None);
        assert_eq!(batch_vintage("price:1|exclusive:2|vintage:2099"), None);
        let rejoined = batch(5, 6, "209908");
        let tied = set(&[&newer, &rejoined]);
        assert_eq!(
            newest_batch(&tied, &order(&[&older, &newer, &rejoined]))?,
            Some(rejoined.clone())
        );
        assert_eq!(
            newest_batch(&tied, &order(&[&rejoined, &newer]))?,
            Some(newer.clone())
        );
        let refused = newest_batch(&tied, &none)
            .err()
            .map(|error| error.to_string());
        assert!(refused.is_some_and(
            |text| text.contains("2 batches of vintage 209908 and their order is unknown")
        ),);
        // Not batches: the one id there is, else refused.
        assert_eq!(chosen_batch(&set(&[&foreign]), &none)?, foreign);
        assert!(chosen_batch(&set(&[&older, &foreign]), &none).is_err());
        assert!(chosen_batch(&BTreeSet::new(), &none).is_err());
        Ok(())
    }

    /// `test_an_expired_or_tagged_append_still_orders_the_vintage` (2026-11-17's shape, synthetic ids).
    #[test]
    fn an_expired_or_tagged_append_still_orders_the_vintage() -> anyhow::Result<()> {
        let (expired_a, expired_b) = (batch(1, 9, "209909"), batch(2, 9, "209909"));
        let (tagged, on_main) = (batch(3, 9, "209909"), batch(3, 8, "209909"));
        let load =
            |sido: &str, batch: &str| format!("unit-official-price/base-date-v2/{sido}/{batch}");
        let ingested: BTreeMap<String, i64> = [
            (load("11", &expired_a), 71),
            (load("11", &expired_b), 72),
            (load("11", &tagged), 81),
            (load("26", &tagged), 81),
            (load("11", &on_main), 91),
            (load("26", &on_main), 91),
        ]
        .into_iter()
        .collect();
        // 90 (expired parent) <- 91 <- 92 = main; 81 is held by a tag and older than 91.
        let snapshots: BTreeMap<i64, (Option<i64>, i64)> = [
            (81, (Some(80), 1_000)),
            (91, (Some(90), 2_000)),
            (92, (Some(91), 3_000)),
        ]
        .into_iter()
        .collect();
        let found = order_batches(&ingested, Some(92), &snapshots);
        assert_eq!(found.ordered, [tagged.clone(), on_main.clone()]);
        assert_eq!(found.before_retained, set(&[&expired_a, &expired_b]));
        assert_eq!(
            newest_batch(&set(&[&expired_a, &expired_b, &tagged, &on_main]), &found)?,
            Some(on_main.clone())
        );
        assert!(newest_batch(&set(&[&expired_a, &expired_b]), &found).is_err());
        let unrecorded = batch(4, 9, "209909");
        assert!(newest_batch(&set(&[&on_main, &unrecorded]), &found).is_err());
        // Equal instants: the place in main's chain decides.
        let equal: BTreeMap<String, i64> = [(load("11", &tagged), 91), (load("11", &on_main), 92)]
            .into_iter()
            .collect();
        let chain: BTreeMap<i64, (Option<i64>, i64)> = [(91, (Some(90), 5)), (92, (Some(91), 5))]
            .into_iter()
            .collect();
        assert_eq!(
            order_batches(&equal, Some(92), &chain).ordered,
            [tagged.clone(), on_main.clone()]
        );
        // One snapshot's loads of two batches: neither is later, so the vintage is refused.
        let same: BTreeMap<String, i64> = [(load("11", &tagged), 92), (load("26", &on_main), 92)]
            .into_iter()
            .collect();
        let tied = order_batches(&same, Some(92), &chain);
        assert_eq!(tied.tied, [set(&[&tagged, &on_main])]);
        assert!(newest_batch(&set(&[&tagged, &on_main]), &tied).is_err());
        Ok(())
    }
}
