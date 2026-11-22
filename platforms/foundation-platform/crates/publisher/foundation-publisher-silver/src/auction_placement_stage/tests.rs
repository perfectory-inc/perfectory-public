//! Where an auction's rows are served, on synthetic parcels (PNUs under `99999`): under its own
//! served PNU, a lineage successor, the parcel it merged into, an earlier form of its code the served
//! edition still has, or flagged `lineage_missing` — the cases of `onbid.placed` and
//! `served_form_candidates`.

use duckdb::Connection;

use super::*;

const OWN: &str = "9999910100100010000";
const SUCCESSOR: &str = "9999910100100020000";
const MERGED_INTO: &str = "9999910100100030000";
const SPLIT_SOURCE: &str = "9999910100100050000";
const MERGED_SOURCE: &str = "9999910100100060000";
const UNREVIEWED_SOURCE: &str = "9999910100100070000";
const NO_PATH: &str = "9999910100100080000";
/// The served edition predates the pair 10200 → 10300: it still holds the lot under the old code.
const OLD_FORM: &str = "9999910200100040000";
const NEW_FORM: &str = "9999910300100040000";

fn setup(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
           effective_date VARCHAR, source VARCHAR); \
         INSERT INTO code_changes VALUES \
           ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x'); \
         CREATE TABLE served (pnu VARCHAR); \
         INSERT INTO served VALUES ('{OWN}'), ('{SUCCESSOR}'), ('{MERGED_INTO}'), ('{OLD_FORM}'); \
         CREATE TABLE rounds (round_key VARCHAR, pnu VARCHAR, ltno_pnu_raw VARCHAR); \
         INSERT INTO rounds VALUES \
           ('own', '{OWN}', NULL), \
           ('raw', NULL, '{OWN}'), \
           ('split', '{SPLIT_SOURCE}', NULL), \
           ('merged', '{MERGED_SOURCE}', NULL), \
           ('unreviewed', '{UNREVIEWED_SOURCE}', NULL), \
           ('nowhere', '{NO_PATH}', NULL), \
           ('old-code', NULL, '{OLD_FORM}'), \
           ('unplaced', NULL, '외 6필지'); \
         CREATE TABLE lineage (predecessor_pnu VARCHAR, successor_pnu VARCHAR, relation VARCHAR, \
           grade VARCHAR, evidence_kind VARCHAR, evidence_ref VARCHAR); \
         INSERT INTO lineage VALUES \
           ('{SPLIT_SOURCE}', '{SUCCESSOR}', 'split', 'official', 'official_record', 'official:a'), \
           ('{MERGED_SOURCE}', '{MERGED_INTO}', 'merge', 'official', 'official_record', 'official:b'), \
           ('{UNREVIEWED_SOURCE}', '{MERGED_INTO}', 'merge', 'needs_review', 'geometry', 'geometry:c');"
    ))?;
    Ok(())
}

fn placed(connection: &Connection) -> anyhow::Result<Vec<(String, String, Option<String>)>> {
    Ok(connection
        .prepare("SELECT round_key, pnu, attached_via FROM placed ORDER BY round_key, pnu")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?)
}

fn run(connection: &Connection) -> anyhow::Result<Placed> {
    crosswalk(connection)?;
    locate(connection, "rounds", "pnu", "ltno_pnu_raw")?;
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE carry_sources AS SELECT DISTINCT source_pnu AS pnu FROM located \
         WHERE source_pnu IS NOT NULL AND source_pnu NOT IN (SELECT pnu FROM served);",
    )?;
    crate::lineage_carry_stage::carry_tables(connection)?;
    place(connection, "served")
}

#[test]
fn every_row_is_served_where_onbid_places_it() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    setup(&connection)?;
    let counts = run(&connection)?;
    let row = |key: &str, pnu: &str, via: Option<&str>| {
        (key.to_owned(), pnu.to_owned(), via.map(str::to_owned))
    };
    assert_eq!(
        placed(&connection)?,
        [
            // A merge link not yet in effect is not followed.
            row("merged", MERGED_INTO, Some("merged:official")),
            row("nowhere", NO_PATH, Some(LINEAGE_MISSING)),
            // The source's code is carried to the current one, and the served edition still has the
            // lot under the old form.
            row("old-code", OLD_FORM, Some(SERVED_EDITION_GRADE)),
            row("own", OWN, None),
            // The source's own PNU text when the load resolved none.
            row("raw", OWN, None),
            row("split", SUCCESSOR, Some("origin:split:official")),
            row("unreviewed", UNREVIEWED_SOURCE, Some(LINEAGE_MISSING)),
        ]
    );
    assert_eq!(
        counts,
        Placed {
            located: 7,
            own: 2,
            carried: 3,
            stranded: 2
        }
    );
    // A stranded row is placed, counted and never served.
    let servable: i64 = connection.query_row(
        &format!("SELECT count(*) FROM ({SERVABLE_SQL})"),
        [],
        |row| row.get(0),
    )?;
    assert_eq!(servable, 5);
    Ok(())
}

#[test]
fn a_located_pnu_is_carried_to_the_current_code_and_prose_is_never_a_pnu() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    setup(&connection)?;
    crosswalk(&connection)?;
    locate(&connection, "rounds", "pnu", "ltno_pnu_raw")?;
    let located: Vec<(String, Option<String>)> = connection
        .prepare("SELECT round_key, source_pnu FROM located WHERE round_key IN ('old-code', 'unplaced') ORDER BY 1")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        located,
        [
            ("old-code".to_owned(), Some(NEW_FORM.to_owned())),
            ("unplaced".to_owned(), None)
        ]
    );
    assert!(locate(&connection, "rounds; DROP TABLE x", "pnu", "ltno_pnu_raw").is_err());
    Ok(())
}
