//! The twin of `land_ownership_silver_to_gold.py`.
//!
//! Root ADR-0361, ADR-0564 §6.
//!
//! A PNU's document is its current owner rows (every row of the newest release that holds the
//! parcel, the contract's `keep_ties`) folded into one `ownership` object:
//!
//! - `owners`: the parcel's own owners, the rows whose 집합건물일련번호 is empty or all zeros, sorted
//!   on their fields in [`OWNER_FIELDS`] order (Spark's `array_sort` of the structs: each field
//!   ascending, a null first), at most [`OWNER_PAGE_BOUND`]; `owner_total` counts them all.
//! - `unit_owner_total` and `unit_owner_kinds`: the collective building's unit rows, counted per
//!   소유구분명, most first, then the name (a null first).
//! - `latest_changed_on`, `base_year_month`, `data_reference_date`: the newest of each over all rows.
//!
//! Only PNUs of the served parcel edition have a document, a row under a renumbered code is read
//! under the served one; the lineage is not followed.
//!
//! The bytes are Spark's: `to_json` with every field written, a null as `null`
//! (`ignoreNullFields` false), and `row_digest` the SHA-256 of `to_json` of the content columns with
//! the null `attached_via` left out (Spark's default `ignoreNullFields`).

use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, KindVersionsStage, RowSet, CODE_CHANGE_SOURCE, PARCEL_SOURCE,
    SHARED_SQL,
};

/// The owner Silver.
pub const OWNERSHIP_SOURCE: &str = "silver.land_ownership";
/// A parcel's listed owners: past this the document keeps the first ones (measured 2026-10-30 on
/// 세종: at most 815 owner rows on one parcel's own land).
pub const OWNER_PAGE_BOUND: u32 = 100;
/// The provider's land-level rows name no collective building: 집합건물일련번호 is all zeros.
pub const LAND_ROW_SERIAL: &str = "^0*$";
/// An owner's fields, in the document's order (and the order Spark sorts the owners on).
pub const OWNER_FIELDS: [&str; 11] = [
    "co_owner_serial_no",
    "ownership_kind_code",
    "ownership_kind_name",
    "residence_kind_code",
    "residence_kind_name",
    "state_agency_kind_code",
    "state_agency_kind_name",
    "ownership_change_reason_code",
    "ownership_change_reason",
    "ownership_changed_on",
    "co_owner_count",
];
/// The other columns a row keeps beside its PNU.
const ROW_FIELDS: [&str; 3] = [
    "collective_building_serial_no",
    "base_year_month",
    "data_reference_date",
];
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-ownership-duckdb-v1";

/// The rows of the PNUs of `targets` folded into one document each, inserted into `built`. The
/// 공유인수 is read as the integer the Silver holds: a 시도 the spool holds nothing of loads every
/// column as text.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH picked AS ( \
  SELECT * REPLACE (CAST(co_owner_count AS INTEGER) AS co_owner_count) \
  FROM ownership_rows WHERE @targets@), \
land AS ( \
  SELECT * FROM picked \
  WHERE collective_building_serial_no IS NULL OR regexp_matches(collective_building_serial_no, '@land@')), \
units AS ( \
  SELECT * FROM picked \
  WHERE collective_building_serial_no IS NOT NULL \
    AND NOT regexp_matches(collective_building_serial_no, '@land@')), \
owners AS ( \
  SELECT pnu, list({@owner@} ORDER BY @owner_order@) AS owners, count(*) AS owner_total \
  FROM land GROUP BY pnu), \
kinds AS ( \
  SELECT pnu, \
    list({'ownership_kind_name': ownership_kind_name, 'count': n} \
      ORDER BY n DESC, ownership_kind_name ASC NULLS FIRST) AS kinds, \
    CAST(sum(n) AS BIGINT) AS unit_owner_total \
  FROM (SELECT pnu, ownership_kind_name, count(*) AS n FROM units GROUP BY pnu, ownership_kind_name) \
  GROUP BY pnu), \
newest AS ( \
  SELECT pnu, max(ownership_changed_on) AS latest_changed_on, max(base_year_month) AS base_year_month, \
    max(data_reference_date) AS data_reference_date \
  FROM picked GROUP BY pnu), \
documents AS ( \
  SELECT n.pnu, \
    to_json({ \
      'owners': coalesce(list_slice(o.owners, 1, @bound@), CAST([] AS @owner_type@[])), \
      'unit_owner_total': coalesce(k.unit_owner_total, 0), \
      'unit_owner_kinds': coalesce(k.kinds, \
        CAST([] AS STRUCT(ownership_kind_name VARCHAR, count BIGINT)[])), \
      'latest_changed_on': n.latest_changed_on, \
      'base_year_month': n.base_year_month, \
      'data_reference_date': n.data_reference_date})::VARCHAR AS ownership_json, \
    CAST(coalesce(o.owner_total, 0) AS BIGINT) AS owner_total \
  FROM newest n LEFT JOIN owners o ON o.pnu = n.pnu LEFT JOIN kinds k ON k.pnu = n.pnu) \
SELECT pnu, ownership_json, owner_total, CAST(NULL AS VARCHAR) AS attached_via, \
  sha256(to_json({'pnu': pnu, 'ownership_json': ownership_json, 'owner_total': owner_total})::VARCHAR) \
    AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, ownership_json VARCHAR, \
    owner_total BIGINT, attached_via VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, \
    published_at_utc VARCHAR);";

/// The columns a row keeps beside its PNU.
fn kept() -> Vec<&'static str> {
    ROW_FIELDS
        .iter()
        .chain(OWNER_FIELDS.iter())
        .copied()
        .collect()
}

/// The owner struct's type: every field text but the 공유인수 (`int` in the Silver contract).
fn owner_type() -> String {
    let fields = OWNER_FIELDS
        .iter()
        .map(|field| {
            let kind = if *field == "co_owner_count" {
                "INTEGER"
            } else {
                "VARCHAR"
            };
            format!("{field} {kind}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("STRUCT({fields})")
}

/// The ownership kind.
#[derive(Clone, Copy, Debug)]
pub struct OwnershipKind;

/// The one ownership kind.
pub const OWNERSHIP: OwnershipKind = OwnershipKind;

impl KindVersionsStage for OwnershipKind {
    fn section_column(&self) -> &'static str {
        "ownership_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![PARCEL_SOURCE, OWNERSHIP_SOURCE, CODE_CHANGE_SOURCE]
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let bound = OWNER_PAGE_BOUND.to_string();
        let fields = OWNER_FIELDS.join(",");
        for text in [
            RULES_VERSION,
            LAND_ROW_SERIAL,
            &bound,
            &fields,
            DOCUMENT_SQL,
        ]
        .into_iter()
        .chain(SHARED_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        format!("{:x}", hash.finalize())
    }

    fn row_sets(&self) -> Vec<RowSet> {
        // Every owner row counts (`owner_total`), so the rows are not made distinct.
        vec![RowSet {
            name: "ownership_rows",
            source: OWNERSHIP_SOURCE,
            pnu: "pnu",
            reads: kept(),
            columns: kept()
                .into_iter()
                .map(|column| (format!("m.{column}"), column))
                .collect(),
            joins: Vec::new(),
            prepared: false,
            distinct: false,
        }]
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        let owner = OWNER_FIELDS
            .iter()
            .map(|field| format!("'{field}': {field}"))
            .collect::<Vec<_>>()
            .join(", ");
        let owner_order = OWNER_FIELDS
            .iter()
            .map(|field| format!("{field} ASC NULLS FIRST"))
            .collect::<Vec<_>>()
            .join(", ");
        let targets = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        let bound = OWNER_PAGE_BOUND.to_string();
        let at = std::time::Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("targets", targets),
                    ("land", LAND_ROW_SERIAL),
                    ("owner", &owner),
                    ("owner_order", &owner_order),
                    ("owner_type", &owner_type()),
                    ("bound", &bound),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the ownership document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
