//! The parcel registered-factory kind's Gold in `DuckDB` (root ADR-0449, ADR-0564 §6): the twin of
//! `registered_factory_silver_to_gold.py`.
//!
//! A PNU's document is `{"records": [...]}`: the parcel's distinct factories of the newest edition
//! of `silver.registered_factory` (한국산업단지공단 전국등록공장현황; the edition chosen by the
//! contract's one rendering, `newest_edition_sql`), each field in the provider's words with its
//! spaces trimmed and an empty one null, ordered by 회사명, 단지명, 생산품 (a missing value last), at
//! most [`RECORD_BOUND`]; `factory_total` counts them all. Only PNUs of the served parcel edition
//! have a document, a row under a renumbered code is read under the served one, and each row carries
//! its parcel's place id (`place_id_join`, root ADR-0483).
//!
//! A partial run rebuilds the PNUs whose factory rows differ between the base's pins and the new
//! ones (both directions: a factory that left the file loses its document), the parcels that
//! entered or left the served edition, and the PNUs whose place id moved; a change of the code change
//! table rebuilds every PNU, as every own-producer parcel kind.
//!
//! The bytes are Spark's: `to_json` with every field written, a null as `null`, and `row_digest` the
//! SHA-256 of `to_json` of the content columns with a null `attached_via` or `place_id` left out
//! (Spark's default `ignoreNullFields`).

use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, KindVersionsStage, RowSet, CODE_CHANGE_SOURCE, PARCEL_SOURCE,
    PLACE_IDS, SHARED_SQL,
};
use crate::parcel_nearby_stage::Source;
use crate::place_id_join::{place_ids_sql, BRIDGE_SOURCE};
use crate::record_kind_stage::text;

/// The registered-factory Silver.
pub const FACTORY_SOURCE: &str = "silver.registered_factory";
/// The record's fields, in the document's order (`RECORD_FIELDS`).
pub const RECORD_FIELDS: [&str; 3] = ["company_name", "complex_name", "products"];
/// Factories a document holds at most.
///
/// Measured 2026-11-08 (edition 20251231): 123,828 parcels, at most 528 factories on one (a
/// complex's shared address), 99% at most 6; 생산품 runs to 3,778 characters, so 50 keeps a
/// document well inside the 256 KiB pack budget.
pub const RECORD_BOUND: u32 = 50;
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-factories-duckdb-v1";

/// `rows`' rows ranked by name and folded into one row per PNU of `targets`, with its place id,
/// inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH ranked AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY @order@) AS n \
  FROM rows WHERE @targets@), \
per_pnu AS ( \
  SELECT pnu, list({@record@} ORDER BY n) FILTER (WHERE n <= @bound@) AS records, count(*) AS total \
  FROM ranked GROUP BY pnu), \
documents AS ( \
  SELECT d.pnu, to_json({'records': d.records})::VARCHAR AS factories_json, \
    CAST(d.total AS BIGINT) AS factory_total, p.place_id \
  FROM per_pnu d LEFT JOIN @place_ids@ p USING (pnu)) \
SELECT pnu, factories_json, factory_total, CAST(NULL AS VARCHAR) AS attached_via, place_id, \
  CASE WHEN place_id IS NULL \
    THEN sha256(to_json({'pnu': pnu, 'factories_json': factories_json, 'factory_total': factory_total})::VARCHAR) \
    ELSE sha256(to_json({'pnu': pnu, 'factories_json': factories_json, 'factory_total': factory_total, \
                         'place_id': place_id})::VARCHAR) END AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, factories_json VARCHAR, \
    factory_total BIGINT, attached_via VARCHAR, place_id VARCHAR, row_digest VARCHAR, \
    source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// The registered-factory kind.
#[derive(Clone, Copy, Debug)]
pub struct FactoryKind;

/// The one registered-factory kind.
pub const FACTORIES: FactoryKind = FactoryKind;

impl KindVersionsStage for FactoryKind {
    fn section_column(&self) -> &'static str {
        "factories_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![
            PARCEL_SOURCE,
            FACTORY_SOURCE,
            CODE_CHANGE_SOURCE,
            BRIDGE_SOURCE,
        ]
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let place_ids = place_ids_sql(&Source::Local, "TRUE").unwrap_or_default();
        let fields = RECORD_FIELDS.join(",");
        for text in [
            RULES_VERSION,
            DOCUMENT_SQL,
            place_ids.as_str(),
            fields.as_str(),
        ]
        .into_iter()
        .chain(SHARED_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        hash.update(RECORD_BOUND.to_le_bytes());
        format!("{:x}", hash.finalize())
    }

    fn row_sets(&self) -> Vec<RowSet> {
        // The newest edition's rows (the contract's rule), each field trimmed, distinct: the same
        // factory listed twice (its own 순번) is one factory.
        vec![RowSet {
            name: "rows",
            source: FACTORY_SOURCE,
            pnu: "pnu",
            reads: RECORD_FIELDS.to_vec(),
            columns: RECORD_FIELDS
                .iter()
                .map(|field| (text(&format!("m.{field}")), *field))
                .collect(),
            joins: Vec::new(),
            prepared: false,
            distinct: true,
        }]
    }

    fn carries_place_id(&self) -> bool {
        true
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    fn changed_pnus(&self, connection: &Connection) -> anyhow::Result<u64> {
        let columns = RECORD_FIELDS.join(", ");
        let mut parts = Vec::new();
        for (a, b) in [("", "_old"), ("_old", "")] {
            parts.push(format!(
                "(SELECT pnu FROM (SELECT pnu, {columns} FROM rows{a} \
                 EXCEPT ALL SELECT pnu, {columns} FROM rows{b}))"
            ));
            parts.push(format!(
                "(SELECT pnu FROM served{a} EXCEPT SELECT pnu FROM served{b})"
            ));
            parts.push(format!(
                "(SELECT pnu FROM (SELECT pnu, place_id FROM {PLACE_IDS}{a} \
                 EXCEPT SELECT pnu, place_id FROM {PLACE_IDS}{b}))"
            ));
        }
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE targets AS SELECT DISTINCT pnu FROM ({});",
            parts.join(" UNION ALL ")
        ))?;
        let count: i64 =
            connection.query_row("SELECT count(*) FROM targets", [], |row| row.get(0))?;
        Ok(u64::try_from(count)?)
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
        let order = RECORD_FIELDS
            .iter()
            .map(|field| format!("{field} ASC NULLS LAST"))
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
        let bound = RECORD_BOUND.to_string();
        let at = std::time::Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("order", &order),
                    ("place_ids", PLACE_IDS),
                    ("targets", targets),
                    ("record", &record),
                    ("bound", &bound),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the factory document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
