//! The `DuckDB` twin of `place_id_join.py`.
//!
//! Every parcel kind's Gold row carries its parcel's stable place id (root ADR-0483, ADR-0288 §5),
//! read by every kind stage that joins it.
//!
//! `place_id` is the parcel id `gold.place_id_bridge` holds for the row's PNU (unit `parcel`, the
//! release of root ADR-0481): the open period of that code, else its latest closed one. A code-pair
//! line (`legal-dong-code-change`) is a new code's form of an id, not a period the served PNU has,
//! and is not read. A PNU whose region has not been released yet has none (null).

use crate::parcel_nearby_stage::Source;

/// The place-id release.
pub const BRIDGE_SOURCE: &str = "gold.place_id_bridge";

/// `pnu, place_id` of each parcel code of the bridge at `source` among the codes `within`.
///
/// `within` is a predicate on `code` (`TRUE` for every code). A code's id is its open period's, else
/// its latest closed one's (`place_id_join.place_ids`: `valid_to` null first, then the latest
/// `valid_to`, then the id).
///
/// The one reader of the bridge: a caller names [`BRIDGE_SOURCE`] among its inputs (its pin) and
/// asks this function, so a change of where the ids live is a change here alone.
///
/// # Errors
/// Returns an error when `source` does not pin the bridge.
pub fn place_ids_sql(source: &Source<'_>, within: &str) -> anyhow::Result<String> {
    Ok(format!(
        "SELECT code AS pnu, place_id FROM (            SELECT code, place_id, row_number() OVER (PARTITION BY code              ORDER BY valid_to IS NOT NULL, valid_to DESC, place_id) AS newest            FROM {bridge}            WHERE unit = 'parcel' AND valid_from <> 'legal-dong-code-change' AND ({within}))          WHERE newest = 1",
        bridge = source.of(BRIDGE_SOURCE)?
    ))
}
