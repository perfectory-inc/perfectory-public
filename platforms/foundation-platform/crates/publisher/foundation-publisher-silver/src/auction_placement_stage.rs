//! Where an auction's rows are served, in `DuckDB`.
//!
//! The twin of `onbid_public_auction_silver_to_gold.placed`, `parcels_named` and
//! `place_identity_resolver.served_form_candidates`, which the 공매 and the 경매 Gold both place by
//! (root ADR-0287 §7, ADR-0292, ADR-0482 §1).
//!
//! A row comes with `source_pnu`, the PNU it is located by on this build (the load's resolved PNU,
//! else the source's own, carried to the current legal-dong code: [`LOCATED_PNU`]). It is served
//!
//! - under that PNU when the served parcel edition holds it (`attached_via` null);
//! - else under every current successor the parcel lineage carries it to (`lineage_carry_stage`, the
//!   path in `attached_via`), or under the served parcel an earlier form of its code names when the
//!   served edition predates the code change (`code_served_edition`);
//! - else under the current parcel it merged into (`merged:<grade>`, a link in effect only);
//! - else under itself, flagged `lineage_missing`: kept in the placement's counts, never served
//!   ([`SERVABLE_SQL`]), since no document of the host lane answers that PNU (root ADR-0292).
//!
//! The rows are an auction's (tens of thousands of rounds): they are placed once for the nation and
//! the result is split by 시도 with the kind's other spools. The served parcels are read from the
//! spool (`served/sido=*/`), cut to the PNUs a row or its lineage names, never held whole.

use anyhow::ensure;
use duckdb::Connection;

/// A located PNU's SQL (`place_identity_resolver.located` and `carried_to_current`).
///
/// `@pnu@` (the load's resolved PNU) else `@raw@` when it is 19 digits, with its legal-dong code
/// carried to the current one through `crosswalk(old_code, current_code)`; its lot is kept.
pub const LOCATED_PNU: &str = "\
CASE WHEN cw.current_code IS NOT NULL THEN cw.current_code || substr(lp.located, 11, 9) ELSE lp.located END";

/// The lineage grades a merge link must not have to be followed (`onbid.NOT_IN_EFFECT`).
pub const NOT_IN_EFFECT: [&str; 2] = crate::lineage_carry::NOT_IN_EFFECT;
/// The path of a row served under an earlier form of its code (`SERVED_EDITION_GRADE`).
pub const SERVED_EDITION_GRADE: &str = "code_served_edition";
/// The flag of a row no lineage path leaves (`onbid.LINEAGE_MISSING`).
pub const LINEAGE_MISSING: &str = "lineage_missing";

/// `located(<columns>, source_pnu)` from `@source@`: every row with its located PNU in place of
/// `@pnu@` (`rounds` renames the located `pnu` to `source_pnu`).
const LOCATED_SQL: &str = "\
CREATE OR REPLACE TEMP TABLE located AS \
SELECT lp.* EXCLUDE (located, @pnu@), @located_pnu@ AS source_pnu FROM ( \
  SELECT s.*, coalesce(s.@pnu@, CASE WHEN regexp_full_match(s.@raw@, '[0-9]{19}') THEN s.@raw@ END) AS located \
  FROM @source@ s) lp \
LEFT JOIN crosswalk cw ON substr(lp.located, 1, 10) = cw.old_code";

/// The candidates and the parcels a placement asks about, from `located`, `carry`, `merges`,
/// `served_forms(_code10, _form10, _rank)` and the served parcels `@served@`.
const NAMED_SQL: &str = "\
CREATE OR REPLACE TEMP TABLE named_sources AS \
  SELECT DISTINCT source_pnu FROM located WHERE source_pnu IS NOT NULL; \
CREATE OR REPLACE TEMP TABLE source_parcels AS \
  SELECT DISTINCT pnu FROM @served@ WHERE pnu IN (SELECT source_pnu FROM named_sources); \
CREATE OR REPLACE TEMP TABLE form_asked AS \
  SELECT source_pnu FROM named_sources \
  WHERE substr(source_pnu, 1, 10) IN (SELECT _code10 FROM served_forms) \
    AND source_pnu NOT IN (SELECT source_pnu FROM carry) \
    AND source_pnu NOT IN (SELECT pnu FROM source_parcels); \
CREATE OR REPLACE TEMP TABLE form_found AS \
  SELECT a.source_pnu, f._rank, f._form10 || substr(a.source_pnu, 11, 9) AS form \
  FROM form_asked a JOIN served_forms f ON substr(a.source_pnu, 1, 10) = f._code10; \
CREATE OR REPLACE TEMP TABLE form_held AS \
  SELECT source_pnu, _rank, count(*) AS n, min(form) AS form FROM ( \
    SELECT * FROM form_found WHERE form IN (SELECT pnu FROM @served@ WHERE pnu IN (SELECT form FROM form_found))) \
  GROUP BY source_pnu, _rank; \
CREATE OR REPLACE TEMP TABLE candidates AS \
  SELECT successor_pnu, source_pnu, hop, path FROM carry \
  UNION ALL \
  SELECT form AS successor_pnu, source_pnu, 0 AS hop, '@served_edition@' AS path FROM ( \
    SELECT source_pnu, form, n FROM form_held QUALIFY row_number() OVER (PARTITION BY source_pnu ORDER BY _rank) = 1) \
  WHERE n = 1; \
CREATE OR REPLACE TEMP TABLE named AS \
  SELECT source_pnu AS pnu FROM named_sources \
  UNION SELECT successor_pnu FROM candidates WHERE source_pnu IN (SELECT source_pnu FROM named_sources) \
  UNION SELECT successor_pnu FROM merges WHERE predecessor_pnu IN (SELECT source_pnu FROM named_sources); \
CREATE OR REPLACE TEMP TABLE current_parcels AS \
  SELECT DISTINCT pnu FROM @served@ WHERE pnu IN (SELECT pnu FROM named)";

/// `placed`: every located row on each PNU it is served under, with `attached_via` (`onbid.placed`).
const PLACED_SQL: &str = "\
CREATE OR REPLACE TEMP TABLE placed AS \
WITH known AS ( \
  SELECT l.*, l.source_pnu IN (SELECT pnu FROM current_parcels) AS _current \
  FROM located l WHERE l.source_pnu IS NOT NULL), \
own AS ( \
  SELECT * EXCLUDE (_current), source_pnu AS pnu, CAST(NULL AS VARCHAR) AS attached_via \
  FROM known WHERE _current), \
retired AS (SELECT * EXCLUDE (_current) FROM known WHERE NOT _current), \
successors AS ( \
  SELECT source_pnu, successor_pnu AS pnu, path AS attached_via FROM candidates \
  WHERE successor_pnu IN (SELECT pnu FROM current_parcels)), \
followed AS (SELECT r.*, s.pnu, s.attached_via FROM retired r JOIN successors s USING (source_pnu)), \
into_current AS ( \
  SELECT DISTINCT predecessor_pnu AS source_pnu, successor_pnu AS pnu, 'merged:' || grade AS attached_via \
  FROM merges WHERE grade NOT IN (@not_in_effect@) AND successor_pnu IN (SELECT pnu FROM current_parcels)), \
merged AS ( \
  SELECT r.*, m.pnu, m.attached_via FROM retired r JOIN into_current m USING (source_pnu) \
  WHERE r.source_pnu NOT IN (SELECT source_pnu FROM followed)), \
stranded AS ( \
  SELECT *, source_pnu AS pnu, '@missing@' AS attached_via FROM retired \
  WHERE source_pnu NOT IN (SELECT source_pnu FROM followed UNION SELECT source_pnu FROM merged)) \
SELECT * FROM own UNION ALL BY NAME SELECT * FROM followed UNION ALL BY NAME SELECT * FROM merged \
UNION ALL BY NAME SELECT * FROM stranded";

/// The placed rows a document carries: every one but `lineage_missing` (`onbid.servable`).
pub const SERVABLE_SQL: &str =
    "SELECT * FROM placed WHERE attached_via IS DISTINCT FROM 'lineage_missing'";

/// What a placement found.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct Placed {
    /// Rows with a located PNU.
    pub located: u64,
    /// Rows served under their own PNU.
    pub own: u64,
    /// Rows served under a successor, an earlier form or the parcel they merged into.
    pub carried: u64,
    /// Rows no path leaves (`lineage_missing`), counted and not served.
    pub stranded: u64,
}

fn plain(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Leaves `crosswalk(old_code, current_code)` and `served_forms(_code10, _form10, _rank)`.
///
/// They come from `code_changes` (the code change table's `kind, old_code, new_code, level,
/// effective_date, source` at the run's pin): the one place-identity resolver's pairs in force
/// (`place_identity_stage`, root ADR-0288 §1, ADR-0329), which `place_identity_resolver.crosswalk_of`
/// and `served_forms` read in Spark. No 법정동 name is read: an auction row names its code, not its
/// address.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn crosswalk(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE place_code_changes AS SELECT * FROM code_changes; \
         CREATE OR REPLACE TEMP TABLE place_dong_names \
           (region_cd VARCHAR, full_name VARCHAR, status VARCHAR);",
    )?;
    crate::place_identity_stage::reference_from_loaded(connection)?;
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE crosswalk AS SELECT old_code, current_code FROM place_remap; \
         CREATE OR REPLACE TEMP TABLE served_forms AS \
           SELECT code10 AS _code10, form10 AS _form10, rank::INTEGER AS _rank FROM place_forms;",
    )?;
    Ok(())
}

/// Leaves `located`: the rows of `source` (a table) with `source_pnu`, the PNU each is located by:
/// its `pnu` column, else its `raw` column when 19 digits, carried through `crosswalk`.
///
/// # Errors
/// Returns an error when a name is not plain or `DuckDB` refuses the statement.
pub fn locate(connection: &Connection, source: &str, pnu: &str, raw: &str) -> anyhow::Result<()> {
    ensure!(
        [source, pnu, raw].iter().all(|name| plain(name)),
        "the located table or its columns are not plain names"
    );
    connection.execute_batch(&crate::kind_versions_stage::fill(
        LOCATED_SQL,
        &[
            ("located_pnu", LOCATED_PNU),
            ("source", source),
            ("pnu", pnu),
            ("raw", raw),
        ],
    ))?;
    Ok(())
}

/// Leaves `placed` from `located`, `carry`, `merges` and `served_forms`, the served parcels read
/// through `served` (a table reference: the spool's served PNUs); returns what it found.
///
/// # Errors
/// Returns an error when `DuckDB` refuses a statement.
pub fn place(connection: &Connection, served: &str) -> anyhow::Result<Placed> {
    let not_in_effect = NOT_IN_EFFECT
        .iter()
        .map(|grade| format!("'{grade}'"))
        .collect::<Vec<_>>()
        .join(", ");
    for template in [NAMED_SQL, PLACED_SQL] {
        connection.execute_batch(&crate::kind_versions_stage::fill(
            template,
            &[
                ("served", served),
                ("served_edition", SERVED_EDITION_GRADE),
                ("not_in_effect", &not_in_effect),
                ("missing", LINEAGE_MISSING),
            ],
        ))?;
    }
    let count = |sql: &str| -> anyhow::Result<u64> {
        let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    };
    Ok(Placed {
        located: count("SELECT count(*) FROM located WHERE source_pnu IS NOT NULL")?,
        own: count("SELECT count(*) FROM placed WHERE attached_via IS NULL")?,
        carried: count(&format!(
            "SELECT count(*) FROM placed WHERE attached_via IS NOT NULL AND attached_via <> '{LINEAGE_MISSING}'"
        ))?,
        stranded: count(&format!(
            "SELECT count(*) FROM placed WHERE attached_via = '{LINEAGE_MISSING}'"
        ))?,
    })
}

/// The SQL texts a stage that places by this module folds into its code version.
pub const SQL: [&str; 4] = [LOCATED_PNU, LOCATED_SQL, NAMED_SQL, PLACED_SQL];

#[cfg(test)]
mod tests;
