//! The twin of `parcel_panel_silver_to_gold.py` as the scheduled rebuild runs it.
//!
//! One row per PNU of the served parcel edition, each section a JSON column shaped like the response
//! DTO the parcel document carries (`parcel_by_pnu_serving_export::parcel_document`). The sections:
//!
//! - `zonings_json`: 용도지역 verdicts of inclusion `1`·`2` (a `3` 접함 is dropped), each zone code
//!   resolved to its anchor by walking `silver.land_use_zone_code`'s parents (at most 16, the anchors
//!   [`ZONING_ANCHORS`], [`ZONING_URBAN_ROOT`] standing for itself; a code whose walk dies is
//!   dropped), collapsed per (PNU, zone code) with the lowest inclusion and the greatest trimmed
//!   name, ordered by inclusion then zone code.
//! - `price_json`: the newest 공시지가 by (year, month, price), the year, month and price read as
//!   integers as Spark's cast reads them (a fractional part dropped) and a row with a month outside
//!   1..=12, a year not above 0 or a negative price skipped.
//! - `characteristics_json`, `forest_ledger_json`: the parcel's one row with a finite, positive area
//!   (the forest's co-owner count absent or not negative); a PNU with two such rows is refused.
//! - `transfer_history_json`: one entry per (사유 순번, 필지 이력 순번), the smallest by its fields
//!   with a missing one first, ordered by the move date newest first (a missing one last), then both
//!   sequences descending; a row without a 필지 이력 순번 is refused.
//! - `land_rights_json`, `land_right_total`: one entry per (권리 일련번호, 동, 층, 호, 실) with a blank
//!   designation as `''`, the smallest by its other fields, the first [`LAND_RIGHT_PAGE_BOUND`] in that
//!   key's order and the count of all.
//! - `legal_dong_json`: the name and lowest name the newest `reference.legal_dong_code_snapshot`
//!   gives the PNU's 법정동 code as existing (`존재`); none when it does not.
//!
//! The lineage is not read: the rebuild contract holds `silver.parcel_lineage` under
//! `unmeasured_inputs` (`gold-panel-rebuild.contract.json`), so the Spark build this table shadows
//! runs without it (`--no-carry-lineage`) and `lineage_json` and `attached_via_json` are null on
//! every row. Reading it is a decision of its own, measured on this engine.
//!
//! Every row set keys its versions by the PNU, so its current rows are chosen per 시도
//! ([`KindVersionsStage::current_rows_per_region`]): the land use plan alone is 402,659,127 rows
//! (2026-11-22). The documents are written as `DuckDB` writes JSON; the takeover verdict compares
//! them with Spark's as values (root ADR-0564 Revision 2026-11-22 (둘째)), not as bytes.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use lakehouse_domain::GOLD_PARCEL_PANEL;
use sha2::{Digest, Sha256};

use crate::duckdb_parquet_stage::with_contract_columns;
use crate::kind_versions_stage::{
    current_rows_query, elapsed, fill, is_plain, KindVersionsStage, RowSet, CODE_CHANGE_SOURCE,
    PARCEL_SOURCE, REGIONAL_SQL, SHARED_SQL,
};
use crate::legal_dong_code_snapshot_stage::EXISTS;
use crate::parcel_nearby_stage::Source;

/// 용도지역·지구 verdicts per parcel.
pub const ZONING_SOURCE: &str = "silver.land_use_plan";
/// The zone code list whose parent edges resolve an anchor.
pub const ZONE_CODE_SOURCE: &str = "silver.land_use_zone_code";
/// 개별공시지가.
pub const PRICE_SOURCE: &str = "silver.land_individual_price";
/// 토지특성.
pub const CHARACTERISTIC_SOURCE: &str = "silver.land_characteristic";
/// 임야대장.
pub const FOREST_SOURCE: &str = "silver.land_forest_ledger";
/// 토지이동이력.
pub const TRANSFER_SOURCE: &str = "silver.land_transfer_history";
/// 토지 대지권 등록.
pub const LAND_RIGHT_SOURCE: &str = "silver.land_right_registration";
/// The 법정동 code list whose newest snapshot names a PNU's 동·리.
pub const LEGAL_DONG_SOURCE: &str = "reference.legal_dong_code_snapshot";

/// The resolved endpoints of a zone code's parent walk (`parcel_zoning_catalog_projection_load.rs`
/// `anchor_for`).
pub const ZONING_ANCHORS: [&str; 7] = [
    "UQA100", "UQA200", "UQA300", "UQA400", "UQB001", "UQC001", "UQD001",
];
/// The urban root, which stands for itself.
pub const ZONING_URBAN_ROOT: &str = "UQA001";
/// The longest parent walk (the loader's cycle guard).
pub const ZONING_ANCHOR_MAX_DEPTH: usize = 16;
/// The land rights a document lists at most (the API's page bound); the total counts them all.
pub const LAND_RIGHT_PAGE_BOUND: u32 = 200;
/// The zone codes a prepared table may hold: the list is about 2,540 codes (2026-11-22).
const ZONE_CODE_BOUND: i64 = 100_000;
/// The 법정동 codes a prepared table may hold: about 20,000 exist at a time.
const LEGAL_DONG_BOUND: i64 = 1_000_000;

/// The parcel panel.
#[derive(Clone, Copy, Debug)]
pub struct ParcelPanel {
    /// What the rules are; part of the build's code version.
    pub rules_version: &'static str,
}

/// The parcel panel as the scheduled rebuild builds it (`parcel_panel_silver_to_gold.py`).
pub const PANEL: ParcelPanel = ParcelPanel {
    rules_version: "parcel-panel-duckdb-v1",
};

/// The legal dong names of the newest code list, as a parcel's section: the code, and its name and
/// lowest name (`null` when the list has none) as `{name, lowest_name}`.
const LEGAL_DONG_SQL: &str = "\
CREATE OR REPLACE TABLE legal_dong_names@suffix@ AS \
SELECT region_cd AS code, to_json({'name': name, 'lowest_name': lowest_name})::VARCHAR AS legal_dong_json \
FROM (SELECT region_cd, \
        CASE WHEN trim(full_name, ' ') = '' THEN NULL ELSE trim(full_name, ' ') END AS name, \
        CASE WHEN trim(lowest_name, ' ') = '' THEN NULL ELSE trim(lowest_name, ' ') END AS lowest_name \
      FROM @names@ WHERE snapshot_date = (SELECT max(snapshot_date) FROM @names@) AND status = '@exists@') \
WHERE name IS NOT NULL;";

/// The anchors a prepare leaves, one row per zone code that resolves.
const ZONE_ANCHORS_SQL: &str =
    "CREATE OR REPLACE TABLE zone_anchors@suffix@ (zone_code VARCHAR, anchor_code VARCHAR);";

/// A text field as the sections read it: trimmed of spaces, an empty one null.
const TRIMMED_OR_NULL: &str = "CASE WHEN trim(@x@, ' ') = '' THEN NULL ELSE trim(@x@, ' ') END";

/// An integer field written as text, read as Spark's string cast reads it: digits with a sign, and a
/// fractional part dropped (`1.5` is 1; `TRY_CAST` alone would round it to 2); null otherwise.
const INTEGER_TEXT: &str =
    "CASE WHEN regexp_full_match(trim(@x@, ' '), '[+-]?[0-9]+([.][0-9]*)?') \
    THEN TRY_CAST(split_part(trim(@x@, ' '), '.', 1) AS @type@) END";

/// The documents of `targets` (or every served PNU), one row each, inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH z AS ( \
  SELECT pnu, zone_code, max(@zone_name@) AS zone_name, max(a.anchor_code) AS anchor_code, \
    min(inclusion_code) AS inclusion_code \
  FROM zonings JOIN zone_anchors a USING (zone_code) \
  WHERE inclusion_code IN ('1', '2') AND @targets@ GROUP BY pnu, zone_code), \
zonings_doc AS ( \
  SELECT pnu, to_json(list({'zone_code': zone_code, 'zone_name': zone_name, 'anchor_code': anchor_code, \
    'inclusion_code': inclusion_code} ORDER BY inclusion_code, zone_code))::VARCHAR AS zonings_json \
  FROM z GROUP BY pnu), \
p AS ( \
  SELECT pnu, base_year, base_month, price_per_m2, announced_date, row_number() OVER ( \
    PARTITION BY pnu ORDER BY base_year DESC, base_month DESC, price_per_m2 DESC, \
    announced_date ASC NULLS FIRST) AS n \
  FROM (SELECT pnu, @year@ AS base_year, @month@ AS base_month, @price@ AS price_per_m2, \
          @announced@ AS announced_date FROM prices WHERE @targets@) \
  WHERE base_year > 0 AND base_month BETWEEN 1 AND 12 AND price_per_m2 >= 0), \
price_doc AS ( \
  SELECT pnu, to_json({'price_per_m2': price_per_m2, 'base_year': base_year, 'base_month': base_month, \
    'announced_date': announced_date})::VARCHAR AS price_json FROM p WHERE n = 1), \
characteristics_doc AS ( \
  SELECT pnu, to_json({'land_category': land_category, 'area_m2': area_m2, \
    'land_use_situation': land_use_situation, 'terrain_height': terrain_height, \
    'terrain_shape': terrain_shape, 'road_contact': road_contact})::VARCHAR AS characteristics_json \
  FROM valid_characteristics), \
forest_doc AS ( \
  SELECT pnu, to_json({'land_category': land_category, 'area_m2': area_m2, \
    'ownership_kind': ownership_kind, 'ownership_kind_name': ownership_kind_name, \
    'co_owner_count': co_owner_count})::VARCHAR AS forest_ledger_json \
  FROM valid_forests), \
t AS ( \
  SELECT pnu, CAST(transfer_history_seq AS BIGINT) AS history_seq, \
    trim(parcel_history_seq, ' ') AS parcel_history_seq, reason_code, reason, moved_at, erased_at, \
    land_category, CAST(area_m2 AS DOUBLE) AS area_m2, closure_seq, row_number() OVER ( \
      PARTITION BY pnu, CAST(transfer_history_seq AS BIGINT), trim(parcel_history_seq, ' ') \
      ORDER BY reason_code ASC NULLS FIRST, reason ASC NULLS FIRST, moved_at ASC NULLS FIRST, \
        erased_at ASC NULLS FIRST, land_category ASC NULLS FIRST, \
        CAST(area_m2 AS DOUBLE) ASC NULLS FIRST, closure_seq ASC NULLS FIRST) AS n \
  FROM transfers WHERE @targets@), \
transfer_doc AS ( \
  SELECT pnu, to_json(list({'reason': reason, 'reason_code': reason_code, 'moved_at': moved_at, \
    'erased_at': erased_at, 'land_category': land_category, 'area_m2': area_m2, \
    'history_seq': history_seq, 'parcel_history_seq': parcel_history_seq, 'closure_seq': closure_seq} \
    ORDER BY moved_at DESC NULLS LAST, history_seq DESC, parcel_history_seq DESC))::VARCHAR \
    AS transfer_history_json \
  FROM t WHERE n = 1 GROUP BY pnu), \
r AS ( \
  SELECT pnu, right_serial_no, building_name, coalesce(dong_name, '') AS dong_name, \
    coalesce(floor_name, '') AS floor_name, coalesce(ho_name, '') AS ho_name, \
    coalesce(room_name, '') AS room_name, right_ratio, closure_kind_name AS closure_kind, \
    closure_kind_code \
  FROM land_rights WHERE @targets@), \
r1 AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu, right_serial_no, dong_name, floor_name, ho_name, \
    room_name ORDER BY building_name ASC NULLS FIRST, right_ratio ASC NULLS FIRST, \
    closure_kind_code ASC NULLS FIRST, closure_kind ASC NULLS FIRST) AS n FROM r), \
rights_doc AS ( \
  SELECT pnu, to_json(list_slice(list({'right_serial_no': right_serial_no, \
    'building_name': building_name, 'dong_name': dong_name, 'floor_name': floor_name, \
    'ho_name': ho_name, 'room_name': room_name, 'right_ratio': right_ratio, \
    'closure_kind': closure_kind, 'closure_kind_code': closure_kind_code} \
    ORDER BY right_serial_no ASC NULLS FIRST, dong_name, floor_name, ho_name, room_name), \
    1, @page@))::VARCHAR AS land_rights_json, count(*) AS land_right_total \
  FROM r1 WHERE n = 1 GROUP BY pnu), \
served_doc AS (SELECT DISTINCT pnu FROM served WHERE @targets@), \
documents AS ( \
  SELECT s.pnu, CAST(NULL AS VARCHAR) AS kind, CAST(NULL AS BIGINT) AS area_m2, \
    coalesce(zonings_doc.zonings_json, '[]') AS zonings_json, price_doc.price_json, \
    characteristics_doc.characteristics_json, forest_doc.forest_ledger_json, \
    coalesce(transfer_doc.transfer_history_json, '[]') AS transfer_history_json, \
    coalesce(rights_doc.land_rights_json, '[]') AS land_rights_json, \
    CAST(coalesce(rights_doc.land_right_total, 0) AS BIGINT) AS land_right_total, \
    CAST(NULL AS VARCHAR) AS lineage_json, CAST(NULL AS VARCHAR) AS attached_via_json, \
    names.legal_dong_json \
  FROM served_doc s \
  LEFT JOIN zonings_doc USING (pnu) LEFT JOIN price_doc USING (pnu) \
  LEFT JOIN characteristics_doc USING (pnu) LEFT JOIN forest_doc USING (pnu) \
  LEFT JOIN transfer_doc USING (pnu) LEFT JOIN rights_doc USING (pnu) \
  LEFT JOIN legal_dong_names names ON substr(s.pnu, 1, 10) = names.code) \
SELECT @columns@, @digest@ AS row_digest, '@source_snapshot_id@' AS source_snapshot_id, \
  '@published_at_utc@' AS published_at_utc \
FROM documents";

/// The rows of the two one-per-parcel sections the build reads, valid ones only.
const VALID_SQL: &str = "\
CREATE OR REPLACE TEMP VIEW valid_characteristics AS \
  SELECT pnu, land_category, CAST(area_m2 AS DOUBLE) AS area_m2, land_use_situation, terrain_height, \
    terrain_shape, road_contact \
  FROM characteristics \
  WHERE @targets@ AND NOT isnan(CAST(area_m2 AS DOUBLE)) AND CAST(area_m2 AS DOUBLE) > 0; \
CREATE OR REPLACE TEMP VIEW valid_forests AS \
  SELECT pnu, land_category, CAST(area_m2 AS DOUBLE) AS area_m2, ownership_kind_code AS ownership_kind, \
    ownership_kind_name, CAST(co_owner_count AS INTEGER) AS co_owner_count \
  FROM forests \
  WHERE @targets@ AND NOT isnan(CAST(area_m2 AS DOUBLE)) AND CAST(area_m2 AS DOUBLE) > 0 \
    AND (co_owner_count IS NULL OR CAST(co_owner_count AS INTEGER) >= 0);";

/// What the build refuses before it writes: a PNU with two valid characteristic or forest rows, and
/// a transfer row without its 필지 이력 순번 (the loaders' identity, root ADR-0090).
const REFUSED_SQL: &str = "\
SELECT (SELECT count(*) - count(DISTINCT pnu) FROM valid_characteristics), \
  (SELECT count(*) - count(DISTINCT pnu) FROM valid_forests), \
  (SELECT count(*) FROM transfers WHERE @targets@ \
     AND (parcel_history_seq IS NULL OR trim(parcel_history_seq, ' ') = ''))";

/// The PNUs a partial run rebuilds beside those whose rows or served parcel differ
/// ([`KindVersionsStage::changed_keys_sql`]): the parcels zoned by a zone code whose anchor moved
/// and the served parcels of a 법정동 whose name moved, on either side. It reads the two sides'
/// spools nationally (`@dir@`) and the lookups `prepare` left for both.
const CHANGED_KEYS_SQL: &str = "\
WITH moved_anchors AS (SELECT DISTINCT zone_code FROM ( \
  (SELECT * FROM zone_anchors EXCEPT SELECT * FROM zone_anchors_old) \
  UNION ALL (SELECT * FROM zone_anchors_old EXCEPT SELECT * FROM zone_anchors))), \
moved_names AS (SELECT DISTINCT code FROM ( \
  (SELECT * FROM legal_dong_names EXCEPT SELECT * FROM legal_dong_names_old) \
  UNION ALL (SELECT * FROM legal_dong_names_old EXCEPT SELECT * FROM legal_dong_names))) \
SELECT pnu FROM read_parquet('@dir@/zonings/*/*.parquet', hive_partitioning = false) \
  WHERE zone_code IN (SELECT zone_code FROM moved_anchors) \
UNION ALL SELECT pnu FROM read_parquet('@dir@/zonings_old/*/*.parquet', hive_partitioning = false) \
  WHERE zone_code IN (SELECT zone_code FROM moved_anchors) \
UNION ALL SELECT pnu FROM read_parquet('@dir@/served/*/*.parquet', hive_partitioning = false) \
  WHERE substr(pnu, 1, 10) IN (SELECT code FROM moved_names) \
UNION ALL SELECT pnu FROM read_parquet('@dir@/served_old/*/*.parquet', hive_partitioning = false) \
  WHERE substr(pnu, 1, 10) IN (SELECT code FROM moved_names)";

/// One row set of the panel: its table, its Silver and the columns it keeps, each as it is.
fn row_set(
    name: &'static str,
    source: &'static str,
    columns: &[&'static str],
    distinct: bool,
) -> RowSet {
    RowSet {
        name,
        source,
        pnu: "pnu",
        reads: columns.to_vec(),
        columns: columns
            .iter()
            .map(|column| (format!("m.{column}"), *column))
            .collect(),
        joins: Vec::new(),
        prepared: false,
        distinct,
    }
}

/// The `row_digest` of a built row.
///
/// The SHA-256 of `to_json` of every content column of `gold.parcel_panel` (all but the digest and
/// the two per-commit stamps), so a content column the contract gains is in it without a change here.
#[must_use]
pub fn row_digest_sql() -> String {
    let fields = content_columns()
        .iter()
        .map(|column| format!("'{column}': {column}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("sha256(to_json({{{fields}}})::VARCHAR)")
}

/// The content columns of `gold.parcel_panel`, in its order.
fn content_columns() -> Vec<&'static str> {
    GOLD_PARCEL_PANEL
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| !["row_digest", "source_snapshot_id", "published_at_utc"].contains(name))
        .collect()
}

/// The zone code's anchor by the loader's walk: the first anchor (or the urban root) up its
/// parents within [`ZONING_ANCHOR_MAX_DEPTH`] steps; `None` when the walk dies.
fn anchor_of(code: &str, parents: &BTreeMap<String, Option<String>>) -> Option<String> {
    let mut current = code.to_owned();
    for _ in 0..ZONING_ANCHOR_MAX_DEPTH {
        if ZONING_ANCHORS.contains(&current.as_str()) || current == ZONING_URBAN_ROOT {
            return Some(current);
        }
        current = parents.get(&current)?.clone()?;
    }
    None
}

impl KindVersionsStage for ParcelPanel {
    fn section_column(&self) -> &'static str {
        "zonings_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![
            PARCEL_SOURCE,
            ZONING_SOURCE,
            ZONE_CODE_SOURCE,
            PRICE_SOURCE,
            CHARACTERISTIC_SOURCE,
            FOREST_SOURCE,
            TRANSFER_SOURCE,
            LAND_RIGHT_SOURCE,
            CODE_CHANGE_SOURCE,
            LEGAL_DONG_SOURCE,
        ]
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let sets = format!("{:?}", self.row_sets());
        let anchors = format!("{ZONING_ANCHORS:?}{ZONING_URBAN_ROOT}{ZONING_ANCHOR_MAX_DEPTH}");
        for text in [
            self.rules_version,
            &sets,
            &anchors,
            LEGAL_DONG_SQL,
            ZONE_ANCHORS_SQL,
            DOCUMENT_SQL,
            VALID_SQL,
            REFUSED_SQL,
            CHANGED_KEYS_SQL,
            TRIMMED_OR_NULL,
            INTEGER_TEXT,
            &row_digest_sql(),
        ]
        .into_iter()
        .chain(SHARED_SQL)
        .chain(REGIONAL_SQL)
        {
            hash.update(text.as_bytes());
            hash.update([0]);
        }
        format!("{:x}", hash.finalize())
    }

    fn current_rows_per_region(&self) -> bool {
        true
    }

    fn changed_keys_sql(&self) -> Option<String> {
        Some(CHANGED_KEYS_SQL.to_owned())
    }

    fn row_sets(&self) -> Vec<RowSet> {
        vec![
            row_set(
                "zonings",
                ZONING_SOURCE,
                &["inclusion_code", "zone_code", "zone_name"],
                true,
            ),
            row_set(
                "prices",
                PRICE_SOURCE,
                &["base_year", "base_month", "price_per_m2", "announced_date"],
                true,
            ),
            // Not distinct: two equal rows of a parcel are two rows, and the build refuses them.
            row_set(
                "characteristics",
                CHARACTERISTIC_SOURCE,
                &[
                    "land_category",
                    "area_m2",
                    "land_use_situation",
                    "terrain_height",
                    "terrain_shape",
                    "road_contact",
                ],
                false,
            ),
            row_set(
                "forests",
                FOREST_SOURCE,
                &[
                    "land_category",
                    "area_m2",
                    "ownership_kind_code",
                    "ownership_kind_name",
                    "co_owner_count",
                ],
                false,
            ),
            row_set(
                "transfers",
                TRANSFER_SOURCE,
                &[
                    "transfer_history_seq",
                    "parcel_history_seq",
                    "reason_code",
                    "reason",
                    "moved_at",
                    "erased_at",
                    "land_category",
                    "area_m2",
                    "closure_seq",
                ],
                true,
            ),
            row_set(
                "land_rights",
                LAND_RIGHT_SOURCE,
                &[
                    "right_serial_no",
                    "building_name",
                    "dong_name",
                    "floor_name",
                    "ho_name",
                    "room_name",
                    "right_ratio",
                    "closure_kind_name",
                    "closure_kind_code",
                ],
                true,
            ),
        ]
    }

    fn prepare(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let mut read_millis = BTreeMap::new();

        // The zone codes' anchors, walked here: the list is a few thousand codes.
        let at = Instant::now();
        let mut statement = connection.prepare(&format!(
            "SELECT ucode, parent_ucode FROM ({}) WHERE ucode IS NOT NULL \
             ORDER BY ucode, parent_ucode NULLS FIRST",
            current_rows_query(
                connection,
                source,
                ZONE_CODE_SOURCE,
                &["ucode", "parent_ucode"],
                "TRUE"
            )?
        ))?;
        let codes: Vec<(String, Option<String>)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        ensure!(
            i64::try_from(codes.len())? <= ZONE_CODE_BOUND,
            "{ZONE_CODE_SOURCE} holds {} current codes, more than the {ZONE_CODE_BOUND} a prepared \
             table may hold",
            codes.len()
        );
        // A code the list repeats takes its last parent, in the order read.
        let parents: BTreeMap<String, Option<String>> = codes
            .into_iter()
            .map(|(code, parent)| {
                let parent = parent
                    .map(|parent| parent.trim_matches(' ').to_owned())
                    .filter(|parent| !parent.is_empty() && parent != "000000");
                (code, parent)
            })
            .collect();
        connection.execute_batch(&fill(ZONE_ANCHORS_SQL, &[("suffix", suffix)]))?;
        let mut appender = connection.appender(&format!("zone_anchors{suffix}"))?;
        for code in parents.keys() {
            if let Some(anchor) = anchor_of(code, &parents) {
                appender.append_row([code.as_str(), anchor.as_str()])?;
            }
        }
        appender.flush()?;
        drop(appender);
        read_millis.insert(ZONE_CODE_SOURCE.to_owned(), elapsed(at));

        // The newest code list's names, by code.
        let at = Instant::now();
        let contract = lakehouse_domain::industrial_complex_lakehouse_contracts()
            .iter()
            .find(|contract| contract.table_name == LEGAL_DONG_SOURCE)
            .with_context(|| format!("no lakehouse contract for {LEGAL_DONG_SOURCE}"))?;
        // A snapshot that predates the lowest name reads it as NULL.
        let names = with_contract_columns(connection, contract, &source.of(LEGAL_DONG_SOURCE)?)?;
        connection.execute_batch(&fill(
            LEGAL_DONG_SQL,
            &[("suffix", suffix), ("names", &names), ("exists", EXISTS)],
        ))?;
        let held: i64 = connection.query_row(
            &format!("SELECT count(*) FROM legal_dong_names{suffix}"),
            [],
            |row| row.get(0),
        )?;
        ensure!(
            held <= LEGAL_DONG_BOUND,
            "{LEGAL_DONG_SOURCE}'s newest snapshot names {held} codes, more than the \
             {LEGAL_DONG_BOUND} a prepared table may hold"
        );
        read_millis.insert(LEGAL_DONG_SOURCE.to_owned(), elapsed(at));
        Ok(read_millis)
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        let columns = GOLD_PARCEL_PANEL
            .columns
            .iter()
            .map(|column| {
                let sql_type = match column.logical_type {
                    "string" => "VARCHAR",
                    "long" => "BIGINT",
                    other => bail!("gold.parcel_panel's {} is a {other}", column.name),
                };
                Ok(format!("{} {sql_type}", column.name))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE built ({});",
            columns.join(", ")
        ))?;
        Ok(())
    }

    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        let targets = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        connection.execute_batch(&fill(VALID_SQL, &[("targets", targets)]))?;
        let (characteristics, forests, transfers): (i64, i64, i64) =
            connection.query_row(&fill(REFUSED_SQL, &[("targets", targets)]), [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        ensure!(
            characteristics == 0 && forests == 0,
            "{CHARACTERISTIC_SOURCE} carries {characteristics} and {FOREST_SOURCE} {forests} \
             duplicate PNU rows inside one snapshot: the loaders break such ties by md5(row(...)), \
             which is not portable, so the panel refuses them"
        );
        ensure!(
            transfers == 0,
            "{TRANSFER_SOURCE} carries {transfers} rows without parcel_history_seq; the loader \
             refuses these (ADR-0090 three-part identity) and so does the panel"
        );
        let text = |field: &str| fill(TRIMMED_OR_NULL, &[("x", field)]);
        let integer =
            |field: &str, sql_type: &str| fill(INTEGER_TEXT, &[("x", field), ("type", sql_type)]);
        let page = LAND_RIGHT_PAGE_BOUND.to_string();
        let columns = content_columns().join(", ");
        let at = Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("zone_name", &text("zone_name")),
                    ("year", &integer("base_year", "INTEGER")),
                    ("month", &integer("base_month", "INTEGER")),
                    ("price", &integer("price_per_m2", "BIGINT")),
                    ("announced", &text("announced_date")),
                    ("page", &page),
                    ("targets", targets),
                    ("columns", &columns),
                    ("digest", &row_digest_sql()),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the parcel panel's document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        connection.execute_batch("DROP VIEW valid_characteristics; DROP VIEW valid_forests;")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
