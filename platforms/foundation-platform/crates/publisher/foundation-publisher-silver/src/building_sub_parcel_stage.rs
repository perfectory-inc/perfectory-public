//! The parcel sub-parcel kind's Gold in `DuckDB` (root ADR-0395, ADR-0399, ADR-0564 §6).
//!
//! It carries the rules of the Spark job `building_sub_parcel_silver_to_gold.py`, which it replaced
//! when the lane was taken over.
//!
//! A 부속지번 row ties two parcels: the register's own 대지 (`pnu`) and another parcel its building
//! also stands on (`sub_pnu`). A PNU's document reads the same rows from both ends:
//! `on_other_parcels` (registers whose 대지 is this parcel, with the other parcel each also stands on)
//! and `from_other_parcels` (registers whose 대지 is another parcel and whose building also stands
//! here). Each entry is the register (관리건축물대장PK, 대장종류명), the other parcel's PNU and its
//! 지번 as the provider wrote it (시군구-법정동-대지구분-번-지; a 시군구 the crosswalk refused leaves
//! the PNU null and the 지번 shown), at most [`ENTRY_BOUND`] of each, newest register key first; the
//! document counts every distinct entry. A row naming the parcel itself as its other parcel is not a
//! link. Both PNUs of a row under a renumbered code are read under the served code (root ADR-0329
//! §3); only PNUs of the served parcel edition have a document (ADR-0292).
//!
//! The rows are read once into two row sets, one per direction, each on the parcel it serves: a
//! link's `from_other` entry lies in its 부속 지번's 시도, which need not be its 대지's
//! (`RowSet::pnu`). A partial run rebuilds the PNUs of every entry that differs between the two
//! reads (both directions), so a register whose set lost a parcel leaves that parcel's document
//! too, and the parcels that entered or left the served edition.
//!
//! The bytes are Spark's: `to_json` with every field written, a null as `null` (`ignoreNullFields`
//! false), and `row_digest` the SHA-256 of `to_json` of the content columns with the null
//! `attached_via` left out (Spark's default `ignoreNullFields`).

use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, renumbering_join, served_pnu, KindVersionsStage, RowSet,
    CODE_CHANGE_SOURCE, PARCEL_SOURCE, SHARED_SQL,
};

/// The 부속지번 Silver.
pub const SUB_PARCEL_SOURCE: &str = "silver.building_register_sub_parcel";
/// Entries a direction holds at most (measured 2026-10: one register carries up to 774 rows; the
/// panel lists the first of each direction).
pub const ENTRY_BOUND: u32 = 100;
/// The other parcel's 지번 parts as the provider spells them.
pub const JIBUN_PARTS: [&str; 5] = [
    "sub_sigungu_cd",
    "sub_bjdong_cd",
    "sub_san_gubun",
    "sub_bonbeon",
    "sub_bubeon",
];
/// The register's own 대지's 지번 parts.
pub const SITE_PARTS: [&str; 5] = ["sigungu_cd", "bjdong_cd", "san_gubun", "bonbeon", "bubeon"];
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-sub-parcels-duckdb-v1";
/// The two directions, each a row set on the parcel it serves.
///
/// Each is (row set, the column of the parcel it serves, the other parcel's column, the other
/// parcel's 지번 parts). `on_other`: this parcel is the 대지 and the other parcel the row's 부속 지번.
/// `from_other`: this parcel is the 부속 지번 and the other parcel the register's 대지.
pub const DIRECTIONS: [(&str, &str, &str, [&str; 5]); 2] = [
    ("on_other", "pnu", "sub_pnu", JIBUN_PARTS),
    ("from_other", "sub_pnu", "pnu", SITE_PARTS),
];

/// One direction's entries of the PNUs of `targets`, ranked and folded: `@name@_folded`. A row
/// naming its own parcel as the other is not a link.
const DIRECTION_SQL: &str = "\
@name@_ranked AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY mgmt_key DESC NULLS LAST, \
    other_pnu ASC NULLS LAST, other_jibun ASC NULLS LAST, register_kind_name ASC NULLS LAST) AS n \
  FROM @name@ WHERE (other_pnu IS NULL OR other_pnu <> pnu) AND @targets@), \
@name@_folded AS ( \
  SELECT pnu, list({'mgmt_key': mgmt_key, 'register_kind_name': register_kind_name, \
      'other_pnu': other_pnu, 'other_jibun': other_jibun} ORDER BY n) FILTER (WHERE n <= @bound@) \
      AS entries, \
    count(*) AS total \
  FROM @name@_ranked GROUP BY pnu)";

/// The type of an empty direction.
const ENTRY_LIST: &str = "STRUCT(mgmt_key VARCHAR, register_kind_name VARCHAR, other_pnu VARCHAR, \
    other_jibun VARCHAR)[]";

/// Both directions folded into one document per PNU, inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH @on_other@, @from_other@, \
documents AS ( \
  SELECT coalesce(o.pnu, f.pnu) AS pnu, to_json({ \
      'on_other_parcels': coalesce(o.entries, CAST([] AS @entry_list@)), \
      'on_other_total': CAST(coalesce(o.total, 0) AS BIGINT), \
      'from_other_parcels': coalesce(f.entries, CAST([] AS @entry_list@)), \
      'from_other_total': CAST(coalesce(f.total, 0) AS BIGINT)})::VARCHAR AS sub_parcels_json \
  FROM on_other_folded o FULL OUTER JOIN from_other_folded f ON o.pnu = f.pnu) \
SELECT pnu, sub_parcels_json, CAST(NULL AS VARCHAR) AS attached_via, \
  sha256(to_json({'pnu': pnu, 'sub_parcels_json': sub_parcels_json})::VARCHAR) AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, sub_parcels_json VARCHAR, \
    attached_via VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, published_at_utc VARCHAR);";

/// `parts` joined the way Spark's `concat_ws("-", trim(...))` does: each part's spaces trimmed, a
/// null part skipped.
fn jibun(parts: &[&str]) -> String {
    let trimmed = parts
        .iter()
        .map(|part| format!("trim(m.{part}, ' ')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("concat_ws('-', {trimmed})")
}

/// The sub-parcel kind.
#[derive(Clone, Copy, Debug)]
pub struct SubParcelKind;

/// The one sub-parcel kind.
pub const SUB_PARCELS: SubParcelKind = SubParcelKind;

impl KindVersionsStage for SubParcelKind {
    fn section_column(&self) -> &'static str {
        "sub_parcels_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![PARCEL_SOURCE, SUB_PARCEL_SOURCE, CODE_CHANGE_SOURCE]
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let sets = format!("{:?}", self.row_sets());
        for text in [
            RULES_VERSION,
            &sets,
            DIRECTION_SQL,
            ENTRY_LIST,
            DOCUMENT_SQL,
        ]
        .into_iter()
        .chain(SHARED_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        hash.update(ENTRY_BOUND.to_le_bytes());
        format!("{:x}", hash.finalize())
    }

    fn row_sets(&self) -> Vec<RowSet> {
        DIRECTIONS
            .iter()
            .map(|&(name, here, other, parts)| {
                let other_column = format!("m.{other}");
                let mut reads = vec![other, "mgmt_key", "register_kind_name"];
                reads.extend(parts);
                RowSet {
                    name,
                    source: SUB_PARCEL_SOURCE,
                    pnu: here,
                    reads,
                    columns: vec![
                        ("m.mgmt_key".to_owned(), "mgmt_key"),
                        ("m.register_kind_name".to_owned(), "register_kind_name"),
                        // The other parcel under the served code too, by this read's renumbering.
                        (served_pnu(&other_column, "o"), "other_pnu"),
                        (jibun(&parts), "other_jibun"),
                    ],
                    joins: vec![renumbering_join(&other_column, "o", "@suffix@")],
                    prepared: false,
                    distinct: true,
                }
            })
            .collect()
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
        let bound = ENTRY_BOUND.to_string();
        let targets = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        let direction = |name: &str| {
            fill(
                DIRECTION_SQL,
                &[("name", name), ("targets", targets), ("bound", &bound)],
            )
        };
        let at = std::time::Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("on_other", &direction("on_other")),
                    ("from_other", &direction("from_other")),
                    ("entry_list", ENTRY_LIST),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the sub-parcel document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
