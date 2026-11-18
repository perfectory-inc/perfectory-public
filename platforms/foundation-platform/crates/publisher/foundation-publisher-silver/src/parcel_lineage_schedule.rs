//! What the scheduled parcel lineage derivation derives, and what it already holds.
//!
//! Every consecutive pair of the parcel editions the source contract holds, one derivation per
//! sido group, each derived once (root ADR-0113 §1, ADR-0148). It was the Spark job
//! `parcel_lineage_scheduled.py`; the rules of one derivation are `parcel_lineage_stage`.
//!
//! A prefix both editions hold is its own group; the prefixes only one edition holds (a sido
//! renumbered or merged, 29+46 -> 12) form one group, because their parcels pair only with each
//! other. A group is held when `silver.parcel_lineage` has a row of that pair and rules version
//! whose successor lies in it, unless official evidence withdrew a 동 pair of the group after
//! that row was derived (root ADR-0329 §3): then that group alone is derived again.

use std::collections::BTreeSet;

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use duckdb::types::{TimeUnit, ValueRef};

use crate::{
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    parcel_lineage_stage::{in_sido, LineageGroup, LineagePins, LINEAGE_TABLE, RULES_VERSION},
};

const PARCEL_SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");

/// The tables a pair's inputs are pinned on, as the pins name them.
pub const BOUNDARIES_TABLE: &str = "silver.parcel_boundaries";
/// The code change table.
pub const CODE_CHANGE_TABLE: &str = "reference.legal_dong_code_change";
/// The land movement history.
pub const HISTORY_TABLE: &str = "silver.land_transfer_history";

/// One edition of the source contract, as the lineage reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Edition {
    /// Its name, `YYYYMM`.
    pub name: String,
    /// `silver.parcel_boundaries.source_snapshot_id` of its rows.
    pub snapshot_id: String,
    /// The first day of its provider base month, `YYYY-MM-DD`.
    pub date: String,
}

/// Each edition the embedded source contract holds with the one after it, oldest first.
///
/// # Errors
/// Returns an error when the contract is not valid JSON or an edition is not named `YYYYMM`.
pub fn edition_pairs() -> anyhow::Result<Vec<(Edition, Edition)>> {
    edition_pairs_of(PARCEL_SOURCE_CONTRACT)
}

fn edition_pairs_of(contract_json: &str) -> anyhow::Result<Vec<(Edition, Edition)>> {
    let contract: serde_json::Value =
        serde_json::from_str(contract_json).context("parcel source contract is not valid JSON")?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("parcel source contract must name its snapshot_id_prefix")?;
    let mut names = contract["editions"]
        .as_object()
        .context("parcel source contract must hold its editions")?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    names.sort();
    let editions = names
        .into_iter()
        .map(|name| {
            ensure!(
                name.len() == 6
                    && name.bytes().all(|byte| byte.is_ascii_digit())
                    && matches!(
                        &name[4..],
                        "01" | "02"
                            | "03"
                            | "04"
                            | "05"
                            | "06"
                            | "07"
                            | "08"
                            | "09"
                            | "10"
                            | "11"
                            | "12"
                    ),
                "edition {name:?} is not YYYYMM"
            );
            Ok(Edition {
                snapshot_id: format!("{prefix}{name}"),
                date: format!("{}-{}-01", &name[..4], &name[4..]),
                name,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(editions
        .iter()
        .cloned()
        .zip(editions.iter().skip(1).cloned())
        .collect())
}

/// The later edition is in the source contract but not yet in `silver.parcel_boundaries`.
///
/// An edition enters the contract first and is loaded into Silver after; until then its pair
/// waits. It is not a partial load, and failing the unit every day for it would page for a step
/// that is simply next.
#[must_use]
pub fn awaits_load(before: &BTreeSet<String>, after: &BTreeSet<String>) -> bool {
    !before.is_empty() && after.is_empty()
}

/// `(from prefixes, to prefixes)` per derivation: the renumbered prefixes together first, then
/// each shared prefix alone.
///
/// First, because a reorganized region is what the lineage exists for and its group is the
/// largest: a run cut short by the deadline still derives it.
///
/// # Errors
/// Refuses an edition with no parcels, and prefixes one edition holds with no counterpart in the
/// other: a sido that vanished with nothing new is a partial load, not a reorganization.
pub fn sido_groups(
    before: &BTreeSet<String>,
    after: &BTreeSet<String>,
) -> anyhow::Result<Vec<(Vec<String>, Vec<String>)>> {
    ensure!(
        !before.is_empty() && !after.is_empty(),
        "an edition holds no parcels: before={before:?} after={after:?}"
    );
    let gone = before.difference(after).cloned().collect::<Vec<_>>();
    let new = after.difference(before).cloned().collect::<Vec<_>>();
    if gone.is_empty() != new.is_empty() {
        bail!("sido prefixes held by one edition only, with no counterpart: gone={gone:?} new={new:?}");
    }
    let mut groups = Vec::new();
    if !gone.is_empty() {
        groups.push((gone, new));
    }
    groups.extend(
        before
            .intersection(after)
            .map(|prefix| (vec![prefix.clone()], vec![prefix.clone()])),
    );
    Ok(groups)
}

/// The sido prefixes of the parcels edition `snapshot_id` holds in the pinned boundaries.
///
/// # Errors
/// Returns an error when the snapshot cannot be read.
pub fn sido_of_edition(
    stage: &DuckDbParquetStage,
    pins: LineagePins,
    snapshot_id: &str,
) -> anyhow::Result<BTreeSet<String>> {
    ensure!(
        !snapshot_id.is_empty()
            && snapshot_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'_' | b':' | b'-')),
        "an edition's snapshot id has characters a snapshot id does not use"
    );
    stage
        .connection()
        .prepare(&format!(
            "SELECT DISTINCT substr(pnu, 1, 2) FROM {LAKEHOUSE_CATALOG}.{BOUNDARIES_TABLE} AT (VERSION => {}) \
             WHERE source_snapshot_id = '{snapshot_id}' AND pnu IS NOT NULL",
            pins.boundaries
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()
        .context("DuckDB failed to read an edition's sido prefixes")
}

/// When the last 동 pair of the group was withdrawn by official evidence, to the whole second.
///
/// The newest `recorded_at_utc` of a `supersession` row of the pinned code change table whose old
/// code lies in `from_sido`, or `None`.
///
/// # Errors
/// Returns an error when the snapshot cannot be read.
pub fn superseded_since(
    stage: &DuckDbParquetStage,
    pins: LineagePins,
    from_sido: &[String],
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let sql = format!(
        "SELECT max(recorded_at_utc) FROM {LAKEHOUSE_CATALOG}.{CODE_CHANGE_TABLE} AT (VERSION => {}) \
         WHERE kind = 'supersession' AND {}",
        pins.code_changes,
        in_sido("old_code", from_sido)?
    );
    let connection = stage.connection();
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query([])?;
    let row = rows.next()?.context("an aggregate returns one row")?;
    let microseconds = match row.get_ref(0)? {
        ValueRef::Null => return Ok(None),
        ValueRef::Timestamp(TimeUnit::Second, value) => value.checked_mul(1_000_000),
        ValueRef::Timestamp(TimeUnit::Millisecond, value) => value.checked_mul(1_000),
        ValueRef::Timestamp(TimeUnit::Microsecond, value) => Some(value),
        ValueRef::Timestamp(TimeUnit::Nanosecond, value) => Some(value / 1_000),
        other => bail!(
            "recorded_at_utc is not a timestamp: {:?}",
            other.data_type()
        ),
    }
    .context("recorded_at_utc is out of range")?;
    DateTime::from_timestamp(microseconds.div_euclid(1_000_000), 0)
        .map(Some)
        .context("recorded_at_utc is out of range")
}

/// Whether `silver.parcel_lineage` already holds the group: a row of its pair and rules version
/// whose successor lies in it, derived at or after `since` when a pair of it was withdrawn.
///
/// # Errors
/// Returns an error when the table cannot be read.
pub fn is_held(
    stage: &DuckDbParquetStage,
    group: &LineageGroup,
    since: Option<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    for text in [&group.from_snapshot_id, &group.to_snapshot_id] {
        ensure!(
            !text.is_empty()
                && text.bytes().all(|byte| byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'_' | b':' | b'-')),
            "an edition's snapshot id has characters a snapshot id does not use"
        );
    }
    let after = since.map_or_else(String::new, |since| {
        format!("AND derived_at_utc >= to_timestamp({}) ", since.timestamp())
    });
    let found: i64 = stage.connection().query_row(
        &format!(
            "SELECT count(*) FROM (SELECT 1 FROM {LAKEHOUSE_CATALOG}.{LINEAGE_TABLE} \
             WHERE from_snapshot_id = '{}' AND to_snapshot_id = '{}' AND rules_version = '{RULES_VERSION}' {after}\
             AND {} LIMIT 1)",
            group.from_snapshot_id,
            group.to_snapshot_id,
            in_sido("successor_pnu", &group.to_sido)?
        ),
        [],
        |row| row.get(0),
    )?;
    Ok(found > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(prefixes: &[&str]) -> BTreeSet<String> {
        prefixes.iter().map(|&prefix| prefix.to_owned()).collect()
    }

    #[test]
    fn each_edition_pairs_with_the_next_one_oldest_first() -> anyhow::Result<()> {
        let pairs = edition_pairs_of(
            r#"{"snapshot_id_prefix": "synthetic__parcel-", "editions": {"209909": {}, "209906": {}, "209912": {}}}"#,
        )?;
        let names = pairs
            .iter()
            .map(|(from, to)| (from.name.as_str(), to.name.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(names, [("209906", "209909"), ("209909", "209912")]);
        assert_eq!(pairs[0].0.snapshot_id, "synthetic__parcel-209906");
        assert_eq!(
            (pairs[0].0.date.as_str(), pairs[0].1.date.as_str()),
            ("2099-06-01", "2099-09-01")
        );
        assert!(
            edition_pairs_of(r#"{"snapshot_id_prefix": "p-", "editions": {"209913": {}}}"#)
                .is_err()
        );
        // The repository's own contract holds at least the pair production derived.
        assert!(!edition_pairs()?.is_empty());
        Ok(())
    }

    #[test]
    fn a_shared_sido_is_its_own_group_and_the_renumbered_ones_go_together_first(
    ) -> anyhow::Result<()> {
        let groups = sido_groups(&set(&["11", "29", "46"]), &set(&["11", "12"]))?;
        assert_eq!(
            groups,
            [
                (
                    vec!["29".to_owned(), "46".to_owned()],
                    vec!["12".to_owned()]
                ),
                (vec!["11".to_owned()], vec!["11".to_owned()]),
            ]
        );
        Ok(())
    }

    #[test]
    fn an_edition_in_the_contract_but_not_loaded_yet_waits() {
        assert!(awaits_load(&set(&["11"]), &set(&[])));
        assert!(!awaits_load(&set(&["11"]), &set(&["11"])));
        assert!(!awaits_load(&set(&[]), &set(&[])));
    }

    #[test]
    fn a_sido_held_by_one_edition_without_a_counterpart_is_refused() {
        for (before, after) in [
            (set(&["11", "29"]), set(&["11"])),
            (set(&["11"]), set(&["11", "12"])),
        ] {
            let refused = sido_groups(&before, &after);
            assert!(
                refused
                    .as_ref()
                    .is_err_and(|error| error.to_string().contains("no counterpart")),
                "{refused:?}"
            );
        }
        assert!(sido_groups(&set(&[]), &set(&["11"])).is_err());
    }
}
