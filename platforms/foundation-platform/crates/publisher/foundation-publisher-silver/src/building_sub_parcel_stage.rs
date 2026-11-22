//! The parcel sub-parcel kind's Gold in `DuckDB` (root ADR-0395, ADR-0399, ADR-0564 §6): the twin of
//! `building_sub_parcel_silver_to_gold.py`.
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
//! A partial run rebuilds both ends of every link that differs between the two reads (both
//! directions), so a register whose set lost a parcel leaves that parcel's document too, and the
//! parcels that entered or left the served edition.
//!
//! The bytes are Spark's: `to_json` with every field written, a null as `null` (`ignoreNullFields`
//! false), and `row_digest` the SHA-256 of `to_json` of the content columns with the null
//! `attached_via` left out (Spark's default `ignoreNullFields`).

use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, load_current, load_served_and_renumbering, KindInputs,
    KindVersionsStage, CODE_CHANGE_SOURCE, PARCEL_SOURCE, SHARED_SQL,
};
use crate::parcel_nearby_stage::Source;

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

/// `links<suffix>`: the current rows with both PNUs under the served code and both 지번 as the
/// provider spells them, a row naming its own parcel left out.
const LINKS_SQL: &str = "\
CREATE OR REPLACE TABLE links@suffix@ AS \
SELECT DISTINCT * FROM ( \
  SELECT CASE WHEN r.dst IS NULL THEN m.pnu ELSE r.dst || substr(m.pnu, 11, 9) END AS pnu, \
    CASE WHEN s.dst IS NULL THEN m.sub_pnu ELSE s.dst || substr(m.sub_pnu, 11, 9) END AS sub_pnu, \
    m.mgmt_key, m.register_kind_name, @sub_jibun@ AS sub_jibun, @site_jibun@ AS site_jibun \
  FROM source_rows@suffix@ m \
  LEFT JOIN renumbering@suffix@ r ON substr(m.pnu, 1, 10) = r.src \
  LEFT JOIN renumbering@suffix@ s ON substr(m.sub_pnu, 1, 10) = s.src) \
WHERE pnu IS NULL OR sub_pnu IS NULL OR pnu <> sub_pnu";

/// One direction's entries of the served PNUs of `targets`, ranked: `@name@_ranked`.
const DIRECTION_SQL: &str = "\
@name@_entries AS ( \
  SELECT DISTINCT @here@ AS pnu, mgmt_key, register_kind_name, @other@ AS other_pnu, \
    @other_jibun@ AS other_jibun \
  FROM links WHERE @here@ IS NOT NULL AND @here@ IN (SELECT pnu FROM served) AND @targets@), \
@name@_ranked AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY mgmt_key DESC NULLS LAST, \
    other_pnu ASC NULLS LAST, other_jibun ASC NULLS LAST, register_kind_name ASC NULLS LAST) AS n \
  FROM @name@_entries), \
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
        let parts = format!("{JIBUN_PARTS:?}{SITE_PARTS:?}");
        for text in [
            RULES_VERSION,
            &parts,
            LINKS_SQL,
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

    fn load_inputs(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        edition: &str,
        suffix: &str,
    ) -> anyhow::Result<KindInputs> {
        let mut read_millis = std::collections::BTreeMap::new();
        let served_parcels =
            load_served_and_renumbering(connection, source, edition, suffix, &mut read_millis)?;
        let mut columns = vec!["pnu", "sub_pnu", "mgmt_key", "register_kind_name"];
        columns.extend(JIBUN_PARTS);
        columns.extend(SITE_PARTS);
        let millis = load_current(
            connection,
            source,
            SUB_PARCEL_SOURCE,
            &columns,
            &format!("source_rows{suffix}"),
        )?;
        read_millis.insert(SUB_PARCEL_SOURCE.to_owned(), millis);
        // The renumbering of this read: an empty suffix leaves it in `renumbering` itself.
        connection.execute_batch(&fill(
            LINKS_SQL,
            &[
                ("suffix", suffix),
                ("sub_jibun", &jibun(&JIBUN_PARTS)),
                ("site_jibun", &jibun(&SITE_PARTS)),
            ],
        ))?;
        let rows: i64 =
            connection.query_row(&format!("SELECT count(*) FROM links{suffix}"), [], |row| {
                row.get(0)
            })?;
        if suffix.is_empty() {
            connection.execute_batch(BUILT_TABLE)?;
        }
        Ok(KindInputs {
            served_parcels,
            rows: u64::try_from(rows)?,
            read_millis,
        })
    }

    fn changed_pnus(&self, connection: &Connection) -> anyhow::Result<u64> {
        connection.execute_batch(
            "CREATE OR REPLACE TABLE targets AS \
             WITH moved AS ( \
               (SELECT * FROM links EXCEPT ALL SELECT * FROM links_old) \
               UNION ALL (SELECT * FROM links_old EXCEPT ALL SELECT * FROM links)) \
             SELECT DISTINCT pnu FROM ( \
               SELECT pnu FROM moved UNION ALL SELECT sub_pnu AS pnu FROM moved \
               UNION ALL (SELECT pnu FROM served EXCEPT SELECT pnu FROM served_old) \
               UNION ALL (SELECT pnu FROM served_old EXCEPT SELECT pnu FROM served)) \
             WHERE pnu IS NOT NULL;",
        )?;
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
        let bound = ENTRY_BOUND.to_string();
        let direction = |name: &str, here: &str, other: &str, other_jibun: &str| {
            let targets = if all {
                "TRUE".to_owned()
            } else {
                format!("{here} IN (SELECT pnu FROM targets)")
            };
            fill(
                DIRECTION_SQL,
                &[
                    ("name", name),
                    ("here", here),
                    ("other", other),
                    ("other_jibun", other_jibun),
                    ("targets", &targets),
                    ("bound", &bound),
                ],
            )
        };
        // This parcel is the 대지: the other parcel is the row's 부속 지번.
        let on_other = direction("on_other", "pnu", "sub_pnu", "sub_jibun");
        // This parcel is the 부속 지번: the other parcel is the register's 대지.
        let from_other = direction("from_other", "sub_pnu", "pnu", "site_jibun");
        let at = std::time::Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("on_other", &on_other),
                    ("from_other", &from_other),
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
