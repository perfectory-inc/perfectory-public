//! The parcel lineage's carry walk over the `lineage` rows a placing stage loaded, in `DuckDB`.
//!
//! Which current parcels a retired source PNU's value may move to, nearest first, with the path its screen
//! names (`lineage_carry`, root ADR-0113 §6), and the merge links (ADR-0113 §4). One walk for every
//! producer that places rows on parcels: the 실거래 placement (W81c) and the auctions (공매, 경매).

use std::collections::BTreeSet;

use crate::lineage_carry::{carry_candidates, Link};
use crate::lineage_steward_read::{steward_resolved, LineageRow};

fn count(connection: &duckdb::Connection, sql: &str) -> anyhow::Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value)?)
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn has_table(connection: &duckdb::Connection, name: &str) -> anyhow::Result<bool> {
    Ok(count(
        connection,
        &format!(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = {}",
            quoted(name)
        ),
    )? > 0)
}

const LINEAGE_SETUP_SQL: &str = "
INSERT INTO merges SELECT predecessor_pnu, successor_pnu, grade FROM lineage WHERE relation = 'merge';
CREATE OR REPLACE TEMP TABLE lineage_read AS
  SELECT DISTINCT * EXCLUDE (_rank, _ranked) FROM (
    SELECT *, @rank@ AS _rank,
           evidence_kind = 'same_lot_paired_dong' AND coalesce(predecessor_pnu, '') <> '' AS _ranked
    FROM lineage)
  QUALIFY NOT _ranked OR _rank = min(CASE WHEN _ranked THEN _rank END) OVER (PARTITION BY predecessor_pnu);
CREATE OR REPLACE TEMP TABLE lineage_links AS
  SELECT DISTINCT predecessor_pnu, successor_pnu FROM lineage_read
  WHERE coalesce(predecessor_pnu, '') <> '' AND predecessor_pnu <> successor_pnu;
CREATE OR REPLACE TEMP TABLE reached (pnu VARCHAR);
CREATE OR REPLACE TEMP TABLE frontier AS SELECT pnu FROM carry_sources;";

const LINEAGE_HOP_SQL: &str = "
CREATE OR REPLACE TEMP TABLE frontier AS
  SELECT DISTINCT l.successor_pnu AS pnu FROM lineage_links l
  WHERE l.predecessor_pnu IN (SELECT pnu FROM frontier)
    AND l.successor_pnu NOT IN (SELECT pnu FROM reached);
INSERT INTO reached SELECT pnu FROM frontier;";

const LINEAGE_ROWS_SQL: &str = "
SELECT DISTINCT coalesce(predecessor_pnu, ''), successor_pnu, coalesce(relation, ''), coalesce(grade, ''),
       coalesce(evidence_kind, ''), coalesce(evidence_ref, '')
FROM lineage_read WHERE successor_pnu IN (SELECT pnu FROM reached)";

/// `carry` (`successor_pnu`, `source_pnu`, `hop`, `path`) and `merges` from `lineage` for the PNUs of
/// `carry_sources(pnu)` (the caller's retired sources, which it leaves when `lineage` is loaded);
/// returns (sources, candidates found).
///
/// The walk reads only the rows of the successors at most `MAX_HOPS` links forward of a source
/// (`parcel_lineage_section.rows_reaching`), resolved by the steward decisions in force.
pub(crate) fn carry_tables(connection: &duckdb::Connection) -> anyhow::Result<(u64, u64)> {
    connection.execute_batch(
        "CREATE OR REPLACE TABLE carry (successor_pnu VARCHAR, source_pnu VARCHAR, hop INTEGER, path VARCHAR);
         CREATE OR REPLACE TABLE merges (predecessor_pnu VARCHAR, successor_pnu VARCHAR, grade VARCHAR);",
    )?;
    if !has_table(connection, "lineage")? {
        return Ok((0, 0));
    }
    let rank = crate::lineage_steward_read::EVIDENCE_RANKS.iter().fold(
        String::from("CASE"),
        |case, (prefix, rank)| {
            format!(
                "{case} WHEN starts_with(coalesce(evidence_ref, ''), {}) THEN {rank}",
                quoted(prefix)
            )
        },
    ) + &format!(
        " ELSE {} END",
        crate::lineage_steward_read::EVIDENCE_RANKS.len()
    );
    connection.execute_batch(&LINEAGE_SETUP_SQL.replace("@rank@", &rank))?;
    let sources = count(connection, "SELECT count(*) FROM carry_sources")?;
    for _hop in 0..crate::lineage_carry::MAX_HOPS {
        if count(connection, "SELECT count(*) FROM frontier")? == 0 {
            break;
        }
        connection.execute_batch(LINEAGE_HOP_SQL)?;
    }
    let rows: Vec<LineageRow> = connection
        .prepare(LINEAGE_ROWS_SQL)?
        .query_map([], |row| {
            Ok(LineageRow {
                predecessor_pnu: row.get(0)?,
                successor_pnu: row.get(1)?,
                relation: row.get(2)?,
                grade: row.get(3)?,
                evidence_kind: row.get(4)?,
                evidence_ref: row.get(5)?,
                ..LineageRow::default()
            })
        })?
        .collect::<Result<_, _>>()?;
    let wanted: BTreeSet<String> = connection
        .prepare("SELECT pnu FROM carry_sources")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    let links: Vec<Link> = steward_resolved(&rows)?
        .into_iter()
        .map(|row| Link {
            predecessor_pnu: row.predecessor_pnu,
            successor_pnu: row.successor_pnu,
            relation: row.relation,
            grade: row.grade,
        })
        .collect();
    let (candidates, _stopped) = carry_candidates(&links)?;
    let mut appender = connection.appender("carry")?;
    let mut found = 0_u64;
    for candidate in candidates.iter().filter(|c| wanted.contains(&c.source_pnu)) {
        appender.append_row(duckdb::params![
            candidate.successor_pnu,
            candidate.source_pnu,
            i32::try_from(candidate.hop)?,
            candidate.path
        ])?;
        found += 1;
    }
    Ok((sources, found))
}
