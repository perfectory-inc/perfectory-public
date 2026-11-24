//! The place-id release's registry and bridge, computed by the publisher.
//!
//! The twins of `parcel_place_id_release` (unit `parcel`, one 시도 at a time, root ADR-0481) and of
//! `place_id_release.admin_release` (unit `admin`, the national 읍면동 ids, root ADR-0113 §8), for the
//! versions tables of root ADR-0564 §6-9. The code pairs a superseded code is carried by are the one
//! crosswalk every place-identity reader uses (`place_identity_stage`'s `place_remap` and
//! `place_ambiguous`, root ADR-0288 §3).
//!
//! - **Region**: the 시도 of the first PNU an id was registered with; it never changes.
//! - **Registry**: an id's newest row (a current row wins a tie of the same instant).
//! - **Bridge**: one period per (id, PNU), from the first current row to the last historic one,
//!   open while the pair's newest row is current; an open PNU under a superseded code is bridged
//!   again under its current code (`valid_from = valid_to =` [`PAIR_SOURCE`]) unless a period holds
//!   that code already or two ids would take it.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{ensure, Context};
use duckdb::Connection;

use crate::parcel_nearby_stage::Source;

/// The parcel id registry.
pub const REGISTRY_SOURCE: &str = "silver.parcel_registry";
/// The administrative boundaries, one edition per snapshot.
pub const ADMIN_SOURCE: &str = "silver.administrative_boundaries";
/// The code change record the crosswalk is read from.
pub const CODE_CHANGE_SOURCE: &str = "reference.legal_dong_code_change";
/// The unit of the parcel ids.
pub const PARCEL_UNIT: &str = "parcel";
/// The unit of the 읍면동 ids.
pub const ADMIN_UNIT: &str = "admin";
/// The admin unit's one region.
pub const ADMIN_SCOPE: &str = "national";
/// The bridge's word for a row a code pair carried, not a period (`parcel_place_id_release`).
pub const PAIR_SOURCE: &str = "legal-dong-code-change";
/// An id's status.
const CURRENT: &str = "current";
const HISTORIC: &str = "historic";
const REDIRECTED: &str = "redirected";

/// The tables each region's build leaves, the columns of the replaced Golds before their stamps.
const BUILT_SQL: &str = "CREATE OR REPLACE TABLE built_registry (unit VARCHAR, scope VARCHAR, \
    place_id VARCHAR, status VARCHAR, current_code VARCHAR, redirect_to VARCHAR); \
    CREATE OR REPLACE TABLE built_bridge (unit VARCHAR, scope VARCHAR, place_id VARCHAR, code VARCHAR, \
    valid_from VARCHAR, valid_to VARCHAR);";

/// `registry_rows`: the registry rows of the served edition and the editions before it, each with
/// its id's region, spooled under `@dir@` so a region reads only its own rows (`ORDER BY scope`: the
/// row groups a region skips). The lakehouse is read once. The region is the 시도 of the id's first
/// row by (`valid_from` first, none last; then `recorded_at_utc`, then `pnu`), as
/// `parcel_place_id_release.with_scope` orders them, an aggregate that spills rather than a sort of
/// the 40M rows in memory.
const REGISTRY_ROWS_SQL: &str = "\
COPY (SELECT parcel_id, pnu, status, valid_from, valid_to, redirect_to, recorded_at_utc \
  FROM @registry@ WHERE to_snapshot_id IN (@served@)) TO '@dir@/registry_read.parquet' (FORMAT parquet); \
COPY (WITH spooled AS (SELECT * FROM read_parquet('@dir@/registry_read.parquet')), \
  scoped AS ( \
    SELECT parcel_id, substr(arg_min(pnu, {'unknown': valid_from IS NULL, 'valid_from': coalesce(valid_from, ''), \
      'recorded': recorded_at_utc IS NOT NULL, 'recorded_at': coalesce(epoch_us(recorded_at_utc), 0), \
      'named': pnu IS NOT NULL, 'pnu': coalesce(pnu, '')}), 1, 2) AS scope \
    FROM spooled GROUP BY parcel_id) \
  SELECT r.*, s.scope FROM spooled r JOIN scoped s USING (parcel_id) ORDER BY s.scope) \
  TO '@dir@/registry_rows.parquet' (FORMAT parquet); \
CREATE OR REPLACE VIEW registry_rows AS SELECT * FROM read_parquet('@dir@/registry_rows.parquet');";

/// One region's registry and bridge from `registry_rows`, inserted into the built tables.
const PARCEL_SCOPE_SQL: &str = "\
INSERT INTO built_registry \
SELECT '@unit@', scope, parcel_id, status, \
  CASE WHEN status = '@current@' THEN pnu END, CASE WHEN status = '@redirected@' THEN redirect_to END \
FROM (SELECT *, row_number() OVER (PARTITION BY parcel_id ORDER BY recorded_at_utc DESC NULLS LAST, \
        CASE WHEN status = '@current@' THEN 0 ELSE 1 END, coalesce(valid_to, valid_from) DESC NULLS LAST) AS n \
      FROM registry_rows WHERE scope = '@scope@') \
WHERE n = 1; \
CREATE OR REPLACE TEMP TABLE scope_periods AS \
SELECT scope, parcel_id AS place_id, pnu AS code, opened AS valid_from, \
  CASE WHEN newest <> '@current@' THEN closed END AS valid_to \
FROM (SELECT scope, parcel_id, pnu, \
        min(CASE WHEN status = '@current@' THEN valid_from END) AS opened, \
        max(CASE WHEN status <> '@current@' THEN coalesce(valid_to, valid_from) END) AS closed, \
        arg_max(status, recorded_at_utc) AS newest \
      FROM registry_rows WHERE scope = '@scope@' GROUP BY scope, parcel_id, pnu) \
WHERE opened IS NOT NULL; \
INSERT INTO built_bridge SELECT '@unit@', scope, place_id, code, valid_from, valid_to FROM scope_periods; \
INSERT INTO built_bridge \
WITH carried AS ( \
  SELECT p.scope, p.place_id, r.current_code || substr(p.code, 11, 9) AS code \
  FROM scope_periods p JOIN place_remap r ON substr(p.code, 1, 10) = r.old_code \
  WHERE p.valid_to IS NULL), \
fresh AS (SELECT * FROM carried c WHERE NOT EXISTS (SELECT 1 FROM scope_periods p WHERE p.code = c.code)), \
alone AS (SELECT code FROM fresh GROUP BY code HAVING count(DISTINCT place_id) = 1) \
SELECT DISTINCT '@unit@', scope, place_id, code, '@pair@', '@pair@' FROM fresh WHERE code IN (SELECT code FROM alone); \
DROP TABLE scope_periods;";

/// `template` with each `@name@` in it replaced by its value.
fn fill(template: &str, values: &[(&str, &str)]) -> String {
    values
        .iter()
        .fold(template.to_owned(), |text, (name, value)| {
            text.replace(&format!("@{name}@"), value)
        })
}

/// Whether `text` is safe inside a SQL literal: letters, digits, `_`, `-`, `.`.
fn is_plain(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// Leaves `place_remap(old_code, current_code)` and `place_ambiguous(code)` from `source`'s pairs.
///
/// The crosswalk needs only the pairs; the address readings the same SQL builds are left empty.
///
/// # Errors
/// Returns an error when the record cannot be read.
pub fn load_remap(connection: &Connection, source: &Source<'_>) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE place_code_changes AS \
         SELECT kind, old_code, new_code, level, effective_date, source FROM {}; \
         CREATE OR REPLACE TEMP TABLE place_dong_names \
           (region_cd VARCHAR, full_name VARCHAR, status VARCHAR);",
        source.of(CODE_CHANGE_SOURCE)?
    ))?;
    crate::place_identity_stage::reference_from_loaded(connection)
}

/// Creates the built tables empty, for one region or unit at a time.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn create_built(connection: &Connection) -> anyhow::Result<()> {
    Ok(connection.execute_batch(BUILT_SQL)?)
}

/// Leaves `registry_rows` (the registry as of the served edition, `snapshot_ids_through`, each row
/// with its id's region), spooled under `dir`; returns each region's number of ids.
///
/// # Errors
/// Returns an error when an edition id is not plain or the registry cannot be read.
pub fn load_parcel_rows(
    connection: &Connection,
    source: &Source<'_>,
    served_snapshots: &[String],
    dir: &Path,
) -> anyhow::Result<BTreeMap<String, u64>> {
    let dir_text = dir
        .to_str()
        .context("the spool directory must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !dir_text.contains('\''),
        "the spool directory must not contain a quote"
    );
    ensure!(
        !served_snapshots.is_empty() && served_snapshots.iter().all(|id| is_plain(id)),
        "the served editions {served_snapshots:?} are not plain snapshot ids"
    );
    let served = served_snapshots
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&fill(
        REGISTRY_ROWS_SQL,
        &[
            ("registry", &source.of(REGISTRY_SOURCE)?),
            ("served", &served),
            ("dir", &dir_text),
        ],
    ))?;
    let regions = connection
        .prepare(
            "SELECT scope, CAST(count(DISTINCT parcel_id) AS UBIGINT) FROM registry_rows \
             WHERE scope IS NOT NULL GROUP BY scope ORDER BY scope",
        )?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(regions)
}

/// Builds region `scope` of the parcel unit into the built tables ([`load_remap`],
/// [`create_built`] and [`load_parcel_rows`] first).
///
/// # Errors
/// Returns an error when `scope` is not two digits or `DuckDB` refuses the statement.
pub fn build_parcel_scope(connection: &Connection, scope: &str) -> anyhow::Result<()> {
    ensure!(
        scope.len() == 2 && scope.bytes().all(|byte| byte.is_ascii_digit()),
        "the region {scope:?} is not two digits"
    );
    connection.execute_batch(&fill(
        PARCEL_SCOPE_SQL,
        &[
            ("unit", PARCEL_UNIT),
            ("scope", scope),
            ("current", CURRENT),
            ("redirected", REDIRECTED),
            ("pair", PAIR_SOURCE),
        ],
    ))?;
    Ok(())
}

/// One snapshot of the administrative units: its label and `{unit id: canonical code}`.
pub type AdminSnapshot = (String, BTreeMap<String, String>);

/// A registry row `[place_id, status, current_code, redirect_to]` or a bridge row
/// `[place_id, code, valid_from, valid_to]` of the admin unit.
pub type ReleaseRow = [Option<String>; 4];

/// The administrative boundaries at `source`, one entry per snapshot, oldest first (by the
/// snapshot's newest ingestion, then its label).
///
/// # Errors
/// Returns an error when the table cannot be read.
pub fn load_admin_snapshots(
    connection: &Connection,
    source: &Source<'_>,
) -> anyhow::Result<Vec<AdminSnapshot>> {
    let table = source.of(ADMIN_SOURCE)?;
    let rows = connection
        .prepare(&format!(
            "WITH units AS (SELECT source_snapshot_id AS label, administrative_unit_id AS id, \
               canonical_code AS code, ingested_at_utc FROM {table}), \
             ordered AS (SELECT label, max(ingested_at_utc) AS at FROM units GROUP BY label) \
             SELECT u.label, u.id, u.code FROM units u JOIN ordered o USING (label) \
             ORDER BY o.at ASC NULLS FIRST, u.label ASC NULLS FIRST, u.id"
        ))?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut snapshots: Vec<AdminSnapshot> = Vec::new();
    for (label, id, code) in rows {
        if snapshots.last().is_none_or(|(held, _)| *held != label) {
            snapshots.push((label.clone(), BTreeMap::new()));
        }
        if let Some((_, units)) = snapshots.last_mut() {
            units.insert(id, code);
        }
    }
    Ok(snapshots)
}

/// The admin unit's registry and bridge rows (`place_id_release.admin_release`).
///
/// The registry is every id any snapshot served, current when the newest serves it. The bridge holds
/// each (id, code) from the first snapshot it appeared in until the snapshot that replaced it, and
/// each code the pairs carry to a current code the newest snapshot holds, unless the code is
/// ambiguous or bridged already.
#[must_use]
pub fn admin_release(
    snapshots: &[AdminSnapshot],
    remap: &BTreeMap<String, String>,
    ambiguous: &BTreeSet<String>,
) -> (Vec<ReleaseRow>, Vec<ReleaseRow>) {
    let Some((_, newest)) = snapshots.last() else {
        return (Vec::new(), Vec::new());
    };
    let ids: BTreeSet<&String> = snapshots
        .iter()
        .flat_map(|(_, units)| units.keys())
        .collect();
    let registry = ids
        .into_iter()
        .map(|id| {
            newest.get(id).map_or_else(
                || [Some(id.clone()), Some(HISTORIC.to_owned()), None, None],
                |code| {
                    [
                        Some(id.clone()),
                        Some(CURRENT.to_owned()),
                        Some(code.clone()),
                        None,
                    ]
                },
            )
        })
        .collect();
    let mut bridge: Vec<ReleaseRow> = Vec::new();
    let mut opened: BTreeMap<(String, String), String> = BTreeMap::new();
    for (label, units) in snapshots {
        let closing: Vec<(String, String)> = opened
            .keys()
            .filter(|(id, code)| units.get(id) != Some(code))
            .cloned()
            .collect();
        for key in closing {
            if let Some(start) = opened.remove(&key) {
                bridge.push([Some(key.0), Some(key.1), Some(start), Some(label.clone())]);
            }
        }
        for (id, code) in units {
            opened
                .entry((id.clone(), code.clone()))
                .or_insert_with(|| label.clone());
        }
    }
    bridge.extend(
        opened
            .into_iter()
            .map(|((id, code), start)| [Some(id), Some(code), Some(start), None]),
    );
    let current_to_id: BTreeMap<&String, &String> =
        newest.iter().map(|(id, code)| (code, id)).collect();
    let bridged: BTreeSet<String> = bridge.iter().filter_map(|row| row[1].clone()).collect();
    for (old, current) in remap {
        if let Some(id) = current_to_id.get(current) {
            if !ambiguous.contains(old) && !bridged.contains(old) {
                bridge.push([
                    Some((*id).clone()),
                    Some(old.clone()),
                    Some(PAIR_SOURCE.to_owned()),
                    Some(PAIR_SOURCE.to_owned()),
                ]);
            }
        }
    }
    (registry, bridge)
}

/// `place_remap` and `place_ambiguous` as Rust maps (after [`load_remap`]).
///
/// # Errors
/// Returns an error when `DuckDB` cannot read them.
pub fn remap_of(
    connection: &Connection,
) -> anyhow::Result<(BTreeMap<String, String>, BTreeSet<String>)> {
    let remap = connection
        .prepare("SELECT old_code, current_code FROM place_remap")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let ambiguous = connection
        .prepare("SELECT code FROM place_ambiguous")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok((remap, ambiguous))
}

/// Inserts the admin unit's rows into the built tables (empty tables first when they are absent).
///
/// # Errors
/// Returns an error when `DuckDB` refuses a row.
pub fn insert_admin(
    connection: &Connection,
    registry: &[ReleaseRow],
    bridge: &[ReleaseRow],
) -> anyhow::Result<()> {
    let mut appender = connection.appender("built_registry")?;
    for [id, status, code, redirect] in registry {
        appender.append_row(duckdb::params![
            ADMIN_UNIT,
            ADMIN_SCOPE,
            id,
            status,
            code,
            redirect
        ])?;
    }
    appender.flush()?;
    let mut appender = connection.appender("built_bridge")?;
    for [id, code, from, to] in bridge {
        appender.append_row(duckdb::params![ADMIN_UNIT, ADMIN_SCOPE, id, code, from, to])?;
    }
    appender.flush()?;
    Ok(())
}

/// The rows the built tables hold: (registry, bridge).
///
/// # Errors
/// Returns an error when `DuckDB` cannot count them.
pub fn built_counts(connection: &Connection) -> anyhow::Result<(u64, u64)> {
    let count = |table: &str| -> anyhow::Result<u64> {
        let value: i64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .with_context(|| format!("no {table}"))?;
        Ok(u64::try_from(value)?)
    };
    Ok((count("built_registry")?, count("built_bridge")?))
}

/// The SHA-256 of the rules: this file and the crosswalk SQL the pairs are read with.
#[must_use]
pub fn code_version() -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for text in [
        include_str!("place_id_release_stage.rs"),
        include_str!("place_identity_stage/reference.sql"),
    ] {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

#[cfg(test)]
mod tests;
