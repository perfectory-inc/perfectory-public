//! The parcel kinds whose document is a register's records on the parcel, in `DuckDB` (root
//! ADR-0564 §6).
//!
//! Each [`RecordKind`] is the twin of one Spark job: [`REGISTER_MASTERS`] of
//! `building_register_master_silver_to_gold.py` (root ADR-0450) and [`DEMOLITIONS`] of
//! `building_demolition_silver_to_gold.py` (root ADR-0399). A PNU's document is `{"records": [...]}`:
//! the parcel's distinct current rows, newest first by the latest of three dates (YYYYMMDD sorts as
//! text; a row without a date sorts last; the fields, then the key, make the order total), at most
//! the kind's bound, each field with its spaces trimmed and an empty one null; the total column
//! counts them all. Only PNUs of the served parcel edition have a document, a row under a renumbered
//! code is read under the served one.
//!
//! The two Spark jobs differ in one place, which [`RecordKind::trim_before_rank`] carries: the
//! 총괄표제부 trims each field before it removes duplicates and ranks, the demolition reports
//! remove duplicates and rank on the provider's text and trim only what the document writes.
//!
//! The bytes are Spark's: `to_json` with every field written, a null as `null` (`ignoreNullFields`
//! false), and `row_digest` the SHA-256 of `to_json` of the content columns with the null
//! `attached_via` left out (Spark's default `ignoreNullFields`).

use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, KindVersionsStage, RowSet, CODE_CHANGE_SOURCE, PARCEL_SOURCE,
    SHARED_SQL,
};

/// One register kind: its Silver, its record fields and how its Spark job folds them.
#[derive(Clone, Copy, Debug)]
pub struct RecordKind {
    /// What the rules are; part of the build's code version.
    pub rules_version: &'static str,
    /// The register Silver.
    pub source: &'static str,
    /// The key a row is one of (one 총괄표제부 per 관리건축물대장PK); `None` when rows are distinct on
    /// their fields alone.
    pub key: Option<&'static str>,
    /// The record's fields, in the document's order.
    pub fields: &'static [&'static str],
    /// The three dates whose latest orders the records, in `greatest`'s order.
    pub dates: [&'static str; 3],
    /// Records a document holds at most.
    pub bound: u32,
    /// Whether duplicates are removed and rows ranked on trimmed fields (else on the provider's text).
    pub trim_before_rank: bool,
    /// The document column.
    pub json_column: &'static str,
    /// The column that counts every record.
    pub total_column: &'static str,
}

/// The 총괄표제부 (root ADR-0450; measured 2026-11-09: at most 70 sites on one PNU, 99% one).
pub const REGISTER_MASTERS: RecordKind = RecordKind {
    rules_version: "parcel-register-masters-duckdb-v1",
    source: "silver.building_register_master",
    key: Some("mgmt_key"),
    fields: &[
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
    ],
    dates: ["use_approval_date", "start_date", "permit_date"],
    bound: 50,
    trim_before_rank: true,
    json_column: "register_masters_json",
    total_column: "register_master_total",
};

/// The 철거·멸실 reports (root ADR-0399; measured 2026-10: 425,040 parcels, at most 194 on one).
pub const DEMOLITIONS: RecordKind = RecordKind {
    rules_version: "parcel-demolitions-duckdb-v1",
    source: "silver.building_permit_demolition_loss",
    key: None,
    fields: &[
        "demolition_kind_name",
        "demolition_start_date",
        "demolition_end_date",
        "demolition_loss_date",
        "total_floor_area_m2",
        "building_count",
        "main_purpose_name",
        "structure_name",
    ],
    dates: [
        "demolition_loss_date",
        "demolition_end_date",
        "demolition_start_date",
    ],
    bound: 50,
    trim_before_rank: false,
    json_column: "demolitions_json",
    total_column: "demolition_total",
};

/// `rows`' rows ranked and folded into one row per PNU of `targets`, inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH ranked AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY @latest@ DESC, @order@) AS n \
  FROM rows WHERE @targets@), \
per_pnu AS ( \
  SELECT pnu, list({@record@} ORDER BY n) FILTER (WHERE n <= @bound@) AS records, count(*) AS total \
  FROM ranked GROUP BY pnu), \
documents AS ( \
  SELECT pnu, to_json({'records': records})::VARCHAR AS @json@, CAST(total AS BIGINT) AS @total@ \
  FROM per_pnu) \
SELECT pnu, @json@, @total@, CAST(NULL AS VARCHAR) AS attached_via, \
  sha256(to_json({'pnu': pnu, '@json@': @json@, '@total@': @total@})::VARCHAR) AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

/// A field as the document writes it: trimmed, an empty one null.
fn text(field: &str) -> String {
    format!("CASE WHEN trim({field}, ' ') = '' THEN NULL ELSE trim({field}, ' ') END")
}

impl RecordKind {
    /// The columns a row keeps beside its PNU: the key, then the fields.
    fn kept(&self) -> Vec<&'static str> {
        self.key
            .into_iter()
            .chain(self.fields.iter().copied())
            .collect()
    }
}

impl KindVersionsStage for RecordKind {
    fn section_column(&self) -> &'static str {
        self.json_column
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![PARCEL_SOURCE, self.source, CODE_CHANGE_SOURCE]
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let spec = format!("{self:?}");
        for text in [self.rules_version, &spec, DOCUMENT_SQL]
            .into_iter()
            .chain(SHARED_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        format!("{:x}", hash.finalize())
    }

    fn row_sets(&self) -> Vec<RowSet> {
        let columns = self
            .kept()
            .into_iter()
            .map(|column| {
                let value = format!("m.{column}");
                if self.trim_before_rank && Some(column) != self.key {
                    (text(&value), column)
                } else {
                    (value, column)
                }
            })
            .collect();
        vec![RowSet {
            name: "rows",
            source: self.source,
            pnu: "pnu",
            reads: self.kept(),
            columns,
            joins: Vec::new(),
            prepared: false,
            distinct: true,
        }]
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        let mut built = vec![
            "pnu VARCHAR".to_owned(),
            format!("{} VARCHAR", self.json_column),
            format!("{} BIGINT", self.total_column),
        ];
        built.extend(
            [
                "attached_via",
                "row_digest",
                "source_snapshot_id",
                "published_at_utc",
            ]
            .map(|column| format!("{column} VARCHAR")),
        );
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE built ({});",
            built.join(", ")
        ))?;
        Ok(())
    }

    fn changed_pnus(&self, connection: &Connection) -> anyhow::Result<u64> {
        let kept = self.kept();
        let columns = kept.join(", ");
        let nulls = kept.iter().map(|_| "NULL").collect::<Vec<_>>().join(", ");
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE targets AS SELECT DISTINCT pnu FROM ( \
               (SELECT pnu, {columns} FROM rows EXCEPT ALL SELECT pnu, {columns} FROM rows_old) \
               UNION ALL (SELECT pnu, {columns} FROM rows_old EXCEPT ALL SELECT pnu, {columns} FROM rows) \
               UNION ALL (SELECT pnu, {nulls} FROM ( \
                 (SELECT pnu FROM served EXCEPT SELECT pnu FROM served_old) \
                 UNION ALL (SELECT pnu FROM served_old EXCEPT SELECT pnu FROM served))));"
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
        let latest = format!(
            "greatest({})",
            self.dates
                .iter()
                .map(|date| format!("coalesce(trim({date}, ' '), '')"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let order = self
            .fields
            .iter()
            .chain(self.key.iter())
            .map(|column| format!("{column} ASC NULLS FIRST"))
            .collect::<Vec<_>>()
            .join(", ");
        let record = self
            .fields
            .iter()
            .map(|field| format!("'{field}': {}", text(field)))
            .collect::<Vec<_>>()
            .join(", ");
        let targets = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        let bound = self.bound.to_string();
        let at = std::time::Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("latest", &latest),
                    ("order", &order),
                    ("targets", targets),
                    ("record", &record),
                    ("bound", &bound),
                    ("json", self.json_column),
                    ("total", self.total_column),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the {} document statement after {} ms: {error}",
                    self.source,
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
