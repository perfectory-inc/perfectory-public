//! The parcel 총괄표제부 kind's Gold in `DuckDB` (root ADR-0450, ADR-0564 §6).
//!
//! It builds `gold.parcel_register_masters_versions`, the twin of
//! `building_register_master_silver_to_gold.py`, which it replaces once the takeover verdict says
//! the two are `equal`.
//!
//! A PNU's document (`register_masters_json`) is the parcel's current 총괄표제부 rows (one per
//! 관리건축물대장PK), newest first by the latest of 사용승인일, 착공일 and 허가일 (YYYYMMDD sorts as
//! text; a site without a date sorts last; the remaining fields and the key make the order total), at
//! most [`RECORD_BOUND`]; `register_master_total` counts them all. Each field is the provider's
//! value with its spaces trimmed, an empty one null. A row keyed under a code a reorganization
//! renumbered is read under the served code (root ADR-0329 §3), and only PNUs of the served parcel
//! edition have a document (ADR-0292).
//!
//! The bytes are Spark's: `to_json` of `{"records": [...]}` with every field written, a null as
//! `null` (`ignoreNullFields` false), and `row_digest` the SHA-256 of `to_json` of the content
//! columns with the null `attached_via` left out (Spark's default `ignoreNullFields`).
//!
//! Partial (root ADR-0312 §2): a run rebuilds the PNUs whose current rows differ between the
//! base's pins and the new ones (both directions, so a site whose PNU moved leaves its old parcel)
//! and the parcels that entered or left the served edition; a moved code change table rebuilds
//! every PNU, as every own-producer parcel kind does.

use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use duckdb::Connection;
use sha2::{Digest, Sha256};

use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};
use crate::parcel_nearby_stage::{current_rows, Source};

/// The 총괄표제부 Silver, keyed by 관리건축물대장PK.
pub const MASTER_SOURCE: &str = "silver.building_register_master";
/// The parcels; the served edition's are the universe.
pub const PARCEL_SOURCE: &str = crate::parcel_nearby_stage::PARCEL_SOURCE;
/// The code change table a renumbering is read from.
pub const CODE_CHANGE_SOURCE: &str = crate::parcel_nearby_stage::CODE_CHANGE_SOURCE;
/// Records a document holds at most (measured 2026-11-09: at most 70 sites on one PNU, 99% one).
pub const RECORD_BOUND: u32 = 50;
/// The record's fields, in the document's order (hub-building-register-master-source-objects.json).
pub const RECORD_FIELDS: [&str; 10] = [
    "plot_area_m2",
    "building_area_m2",
    "total_floor_area_m2",
    "main_purpose_name",
    "main_building_count",
    "annex_building_count",
    "parking_count",
    "permit_date",
    "start_date",
    "use_approval_date",
];
/// What the rules are; part of the build's code version, with the SQL texts.
pub const RULES_VERSION: &str = "parcel-register-masters-duckdb-v1";

/// `masters`' rows ranked and folded into one row per PNU of `targets`, inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH ranked AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY \
    greatest(coalesce(use_approval_date, ''), coalesce(start_date, ''), coalesce(permit_date, '')) DESC, \
    @field_order@, mgmt_key ASC NULLS FIRST) AS n \
  FROM masters WHERE @targets@), \
per_pnu AS ( \
  SELECT pnu, \
    list({@record@} ORDER BY n) FILTER (WHERE n <= @bound@) AS records, \
    count(*) AS total \
  FROM ranked GROUP BY pnu), \
documents AS ( \
  SELECT pnu, to_json({'records': records})::VARCHAR AS register_masters_json, \
    CAST(total AS BIGINT) AS register_master_total FROM per_pnu) \
SELECT pnu, register_masters_json, register_master_total, CAST(NULL AS VARCHAR) AS attached_via, \
  sha256(to_json({'pnu': pnu, 'register_masters_json': register_masters_json, \
    'register_master_total': register_master_total})::VARCHAR) AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

const BUILT_TABLE: &str =
    "CREATE OR REPLACE TABLE built (pnu VARCHAR, register_masters_json VARCHAR, \
    register_master_total BIGINT, attached_via VARCHAR, row_digest VARCHAR, \
    source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// Every input of the build, in the order the summary names them.
#[must_use]
pub fn inputs() -> Vec<&'static str> {
    vec![PARCEL_SOURCE, MASTER_SOURCE, CODE_CHANGE_SOURCE]
}

/// The inputs whose change may move any parcel's document: the code change table.
#[must_use]
pub fn whole_table_inputs() -> Vec<&'static str> {
    vec![CODE_CHANGE_SOURCE]
}

/// The build's code version: the rules version, the SQL texts and the shared code views it runs.
/// A change of any of them rebuilds every PNU.
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [
        RULES_VERSION,
        DOCUMENT_SQL,
        MASTERS_SQL,
        PAIR_ROWS_SQL,
        RENUMBERED_INTO_SQL,
    ] {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// `masters<suffix>`: the current 총괄표제부 rows, re-keyed to the served code, of served PNUs only,
/// each field trimmed and an empty one null, one row per distinct site.
const MASTERS_SQL: &str = "\
CREATE OR REPLACE TABLE masters@suffix@ AS \
SELECT DISTINCT * FROM ( \
  SELECT CASE WHEN r.dst IS NULL THEN m.pnu ELSE r.dst || substr(m.pnu, 11, 9) END AS pnu, \
    m.mgmt_key, @fields@ \
  FROM current_masters@suffix@ m LEFT JOIN renumbering@suffix@ r ON substr(m.pnu, 1, 10) = r.src \
  WHERE m.pnu IS NOT NULL) \
WHERE pnu IN (SELECT pnu FROM served@suffix@)";

fn is_plain(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// What one read left behind: how many served parcels and sites, and how long each input took.
#[derive(Clone, Debug, Default)]
pub struct MasterInputs {
    /// PNUs of the served edition.
    pub served_parcels: u64,
    /// Distinct sites on served PNUs.
    pub sites: u64,
    /// Milliseconds each input took to read, by table name.
    pub read_millis: BTreeMap<String, u64>,
}

/// Reads the inputs at `source` for the served `edition`.
///
/// It leaves `served<suffix>(pnu)`, `renumbering<suffix>(src, dst)` and `masters<suffix>`; with an
/// empty suffix also an empty `built`. A suffix (`_old`) reads the base's pins beside the new ones
/// for the partial diff.
///
/// # Errors
/// Returns an error when a snapshot cannot be read, the edition is not a plain id or holds no
/// served parcel.
pub fn load_inputs(
    connection: &Connection,
    source: &Source<'_>,
    edition: &str,
    suffix: &str,
) -> anyhow::Result<MasterInputs> {
    ensure!(
        is_plain(edition),
        "the edition {edition:?} is not a plain snapshot id"
    );
    ensure!(
        suffix.is_empty()
            || suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
        "the suffix {suffix:?} is not a plain name"
    );
    let mut read_millis = BTreeMap::new();
    let at = std::time::Instant::now();
    let parcels = lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == PARCEL_SOURCE)
        .context("no lakehouse contract for the parcels")?;
    let predicate = parcels
        .current_row_predicate
        .context("silver.parcel_boundaries names its current rows by a predicate")?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE served{suffix} AS SELECT DISTINCT pnu FROM {table} \
         WHERE source_snapshot_id = '{edition}' AND ({predicate}) AND pnu IS NOT NULL;",
        table = source.of(PARCEL_SOURCE)?
    ))?;
    read_millis.insert(PARCEL_SOURCE.to_owned(), elapsed(at));
    let served_parcels: i64 =
        connection.query_row(&format!("SELECT count(*) FROM served{suffix}"), [], |row| {
            row.get(0)
        })?;
    ensure!(
        served_parcels > 0,
        "edition {edition} holds no served parcel"
    );

    // The code change table's pairs in force, and the codes the served edition holds of them.
    let at = std::time::Instant::now();
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE code_changes AS \
         SELECT kind, old_code, new_code, level, effective_date, source \
         FROM {changes} WHERE kind IN ('pair', 'supersession');",
        changes = source.of(CODE_CHANGE_SOURCE)?
    ))?;
    read_millis.insert(CODE_CHANGE_SOURCE.to_owned(), elapsed(at));
    connection.execute_batch(PAIR_ROWS_SQL)?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE served_codes AS \
         SELECT DISTINCT substr(pnu, 1, 10) AS code FROM served{suffix} WHERE substr(pnu, 1, 10) IN ( \
           SELECT old_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri') \
           UNION SELECT new_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri'));"
    ))?;
    connection.execute_batch(RENUMBERED_INTO_SQL)?;
    if !suffix.is_empty() {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE renumbering{suffix} AS SELECT src, dst FROM renumbering;"
        ))?;
    }

    let at = std::time::Instant::now();
    let columns: Vec<&str> = ["pnu", "mgmt_key"]
        .into_iter()
        .chain(RECORD_FIELDS)
        .collect();
    let rows = current_rows(
        connection,
        source,
        MASTER_SOURCE,
        &columns,
        &format!("current_masters{suffix}"),
    )?;
    ensure!(
        rows == format!("SELECT * FROM current_masters{suffix}"),
        "the current rows were not left in current_masters{suffix}"
    );
    read_millis.insert(MASTER_SOURCE.to_owned(), elapsed(at));
    let fields = RECORD_FIELDS
        .iter()
        .map(|field| {
            format!("CASE WHEN trim(m.{field}, ' ') = '' THEN NULL ELSE trim(m.{field}, ' ') END AS {field}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(
        &MASTERS_SQL
            .replace("@suffix@", suffix)
            .replace("@fields@", &fields),
    )?;
    let sites: i64 = connection.query_row(
        &format!("SELECT count(*) FROM masters{suffix}"),
        [],
        |row| row.get(0),
    )?;
    if suffix.is_empty() {
        connection.execute_batch(BUILT_TABLE)?;
    }
    Ok(MasterInputs {
        served_parcels: u64::try_from(served_parcels)?,
        sites: u64::try_from(sites)?,
        read_millis,
    })
}

fn elapsed(at: std::time::Instant) -> u64 {
    u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Leaves `targets(pnu)` and returns how many: the PNUs to rebuild.
///
/// They are the PNUs whose sites differ between `masters_old` and `masters` (either way, on the
/// fields as the document reads them; so a site that moved PNU leaves the old one) and the parcels
/// that entered or left the served edition. Both sides must have been read ([`load_inputs`] with and
/// without the `_old` suffix).
///
/// # Errors
/// Returns an error when a side has not been read.
pub fn changed_pnus(connection: &Connection) -> anyhow::Result<u64> {
    let fields = std::iter::once("mgmt_key")
        .chain(RECORD_FIELDS)
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE targets AS SELECT DISTINCT pnu FROM ( \
           (SELECT pnu, {fields} FROM masters EXCEPT SELECT pnu, {fields} FROM masters_old) \
           UNION ALL (SELECT pnu, {fields} FROM masters_old EXCEPT SELECT pnu, {fields} FROM masters) \
           UNION ALL (SELECT pnu, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM ( \
             (SELECT pnu FROM served EXCEPT SELECT pnu FROM served_old) \
             UNION ALL (SELECT pnu FROM served_old EXCEPT SELECT pnu FROM served))));"
    ))?;
    let count: i64 = connection.query_row("SELECT count(*) FROM targets", [], |row| row.get(0))?;
    Ok(u64::try_from(count)?)
}

/// Builds the documents of every PNU (`all`) or of `targets` into `built`.
///
/// # Errors
/// Returns an error when the stamps are not plain ids or `DuckDB` refuses the statement.
pub fn build(
    connection: &Connection,
    all: bool,
    source_snapshot_id: &str,
    published_at_utc: &str,
) -> anyhow::Result<()> {
    ensure!(
        is_plain(source_snapshot_id) && is_plain(published_at_utc),
        "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
    );
    let field_order = RECORD_FIELDS
        .iter()
        .map(|field| format!("{field} ASC NULLS FIRST"))
        .collect::<Vec<_>>()
        .join(", ");
    let record = RECORD_FIELDS
        .iter()
        .map(|field| format!("'{field}': {field}"))
        .collect::<Vec<_>>()
        .join(", ");
    let targets = if all {
        "TRUE"
    } else {
        "pnu IN (SELECT pnu FROM targets)"
    };
    let sql = DOCUMENT_SQL
        .replace("@field_order@", &field_order)
        .replace("@targets@", targets)
        .replace("@bound@", &RECORD_BOUND.to_string())
        .replace("@source_snapshot_id@", source_snapshot_id)
        .replace("@published_at_utc@", published_at_utc)
        .replace("@record@", &record);
    connection
        .execute_batch(&sql)
        .context("DuckDB refused the 총괄표제부 document statement")?;
    Ok(())
}

#[cfg(test)]
mod tests;
