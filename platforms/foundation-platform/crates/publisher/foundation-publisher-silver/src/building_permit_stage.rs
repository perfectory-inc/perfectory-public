//! The parcel building-permit kind's Gold in `DuckDB` (root ADR-0356, ADR-0451, ADR-0564 §6): the
//! twin of `building_permit_silver_to_gold.py`.
//!
//! A PNU's document holds, from the parcel's current rows:
//!
//! - `records`: its 건축인허가 (one per 관리허가대장PK; a tied repeat is one permit), newest first by the
//!   latest of [`LATEST_DATES`] (YYYYMMDD sorts as text; a permit without a date sorts last; the
//!   fields, then the key, make the order total), at most [`RECORD_BOUND`]; `building_permit_total`
//!   (the Gold column) counts them all. Each carries its 주차장 대수 from the parking file by the same
//!   관리허가대장PK (a tied repeat folded to its smallest value per field; null without a row).
//! - one list per other file of the permit family ([`DETAILS`]), `{total, records}` in the provider's
//!   words, at most [`RECORD_BOUND`] records newest first by the file's own date when it has one, then
//!   by the fields and the key; null when the parcel has none. Each row is on the parcel its own PNU
//!   names, except the 정기점검이력: its rows reach the parcel of the one 표제부 their
//!   관리건축물대장PK names (root ADR-0404 §2), read every run from the 표제부's current rows.
//!
//! A parcel with any of them has a document. Every value is the provider's text with its spaces
//! trimmed, an empty one null. A row under a renumbered code is read under the served one (root
//! ADR-0329 §3); only PNUs of the served parcel edition have a document (ADR-0292).
//!
//! Each input is read once and split by 시도 (`kind_versions_stage::spool`). The parking rows join
//! their permit by key nationally, in the permits' spool, through a view [`KindVersionsStage::prepare`]
//! leaves: a parking row lies wherever its permit does. The inspections' link to a 표제부 is decided
//! nationally there too, since "exactly one 표제부" counts over every 시도; the links are a small
//! table ([`MAX_INSPECTION_ROWS`]). A partial run compares each row set between the two reads (both
//! directions, so a row whose PNU moved leaves its old parcel; a changed parking row is a changed
//! permit; a changed 표제부 is a change of the inspections it links) and rebuilds those PNUs and the
//! parcels that entered or left the served edition.
//!
//! The bytes are Spark's: `to_json` with every field written, a null as `null` (`ignoreNullFields`
//! false), and `row_digest` the SHA-256 of `to_json` of the content columns with the null
//! `attached_via` left out (Spark's default `ignoreNullFields`).

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::ensure;
use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::kind_versions_stage::{
    current_rows_query, elapsed, fill, is_plain, KindVersionsStage, RowSet, CODE_CHANGE_SOURCE,
    PARCEL_SOURCE, SHARED_SQL,
};
use crate::parcel_nearby_stage::Source;
use crate::record_kind_stage::text;

/// The 건축인허가 기본개요 Silver.
pub const PERMIT_SOURCE: &str = "silver.building_permit_basis_outline";
/// The 건축인허가 주차장 Silver, joined by 관리허가대장PK (root ADR-0452).
pub const PARKING_SOURCE: &str = "silver.building_permit_parking";
/// The 건축물 정기점검이력 Silver, whose rows carry no PNU.
pub const INSPECTION_SOURCE: &str = "silver.building_maintenance_regular_inspection_history";
/// The 표제부 an inspection reaches its parcel through.
pub const TITLE_SOURCE: &str = "silver.building_register_titles";
/// Records a list holds at most (measured 2026-11-09, vintage 202610: 6,527,521 permits on
/// 3,742,220 PNUs, at most 11,076 on one, 99% at most 8, 99.9% at most 22).
pub const RECORD_BOUND: u32 = 50;
/// The current inspections the prepared link table may hold: it holds the nation, so it must stay
/// small (measured 2026-11-22: 65,482 rows of every vintage in the Silver).
pub const MAX_INSPECTION_ROWS: i64 = 1_000_000;
/// A permit's fields in the provider's words (hub-building-permit-basis-outline-source-objects.json).
pub const RECORD_FIELDS: [&str; 10] = [
    "arch_kind_name",
    "main_purpose_name",
    "plot_area_m2",
    "building_area_m2",
    "total_floor_area_m2",
    "permit_date",
    "start_scheduled_date",
    "start_delayed_date",
    "start_actual_date",
    "use_approval_date",
];
/// A permit's parking counts (measured 2026-11-09: 2,835,072 parking rows, every key distinct).
pub const PARKING_FIELDS: [&str; 3] = [
    "indoor_self_parking_count",
    "outdoor_self_parking_count",
    "indoor_mechanical_parking_count",
];
/// The five dates whose latest orders the permits, in `greatest`'s order.
pub const LATEST_DATES: [&str; 5] = [
    "use_approval_date",
    "start_actual_date",
    "start_delayed_date",
    "start_scheduled_date",
    "permit_date",
];
/// The inspection's columns that name its 표제부.
pub const INSPECTION_COLUMNS: [&str; 6] = [
    "mgmt_key",
    "sigungu_cd",
    "bjdong_cd",
    "bonbeon",
    "bubeon",
    "building_register_key",
];
/// What the rules are; part of the build's code version.
pub const RULES_VERSION: &str = "parcel-building-permits-duckdb-v1";

/// One other file of the permit family (root ADR-0402, ADR-0464, ADR-0511).
#[derive(Clone, Copy, Debug)]
pub struct Detail {
    /// Its Silver.
    pub source: &'static str,
    /// Its key in the document.
    pub key: &'static str,
    /// Its row set: `detail_<key>`.
    pub set: &'static str,
    /// Its record fields in the provider's words. Not carried: the keys, 건물명·대지위치·특수지명
    /// (people's names, ADR-0402), 동명 of the inspection.
    pub fields: &'static [&'static str],
    /// The field its records are newest first by; `None` for the fields' own order.
    pub newest: Option<&'static str>,
}

/// The permit family's other files, in the document's order.
pub const DETAILS: [Detail; 9] = [
    Detail {
        source: "silver.building_permit_building_outline",
        key: "building_outlines",
        set: "detail_building_outlines",
        fields: &[
            "main_annex_name",
            "main_purpose_name",
            "structure_name",
            "roof_name",
            "building_area_m2",
            "total_floor_area_m2",
            "far_floor_area_m2",
            "unit_count",
            "family_count",
            "household_count",
            "passenger_elevator_count",
            "emergency_elevator_count",
        ],
        newest: None,
    },
    Detail {
        source: "silver.building_permit_floor_overview",
        key: "floor_overviews",
        set: "detail_floor_overviews",
        fields: &[
            "floor_kind_name",
            "floor_no",
            "main_purpose_name",
            "structure_name",
            "floor_area_m2",
            "arch_kind_name",
        ],
        newest: None,
    },
    Detail {
        source: "silver.building_permit_site_location",
        key: "site_locations",
        set: "detail_site_locations",
        fields: &[
            "representative_flag",
            "main_building_name",
            "land_category_name",
        ],
        newest: None,
    },
    Detail {
        source: "silver.building_permit_major_repair",
        key: "major_repairs",
        set: "detail_major_repairs",
        fields: &["major_repair_kind_name", "major_repair_change_name"],
        newest: None,
    },
    Detail {
        source: "silver.building_permit_temporary_building",
        key: "temporary_buildings",
        set: "detail_temporary_buildings",
        fields: &[
            "main_purpose_name",
            "structure_name",
            "building_area_m2",
            "total_floor_area_m2",
            "plot_area_m2",
            "ground_floor_count",
            "existence_expiry_date",
        ],
        newest: Some("existence_expiry_date"),
    },
    Detail {
        source: "silver.building_permit_annex_parking",
        key: "annex_parkings",
        set: "detail_annex_parkings",
        fields: &["land_category_name", "related_jibun_name"],
        newest: None,
    },
    Detail {
        source: "silver.building_permit_structure_ledger",
        key: "structures",
        set: "detail_structures",
        fields: &[
            "facility_kind_name",
            "structure_name",
            "length",
            "height",
            "area_m2",
            "building_coverage_ratio",
            "zone_name",
            "district_name",
            "precinct_name",
        ],
        newest: None,
    },
    // 주택인허가 기본개요 (root ADR-0511 §2): a housing project's approval on the parcel its own PNU
    // names, newest approval first. Not carried: 건물명·대지위치 (ADR-0356).
    Detail {
        source: "silver.housing_permit_basis_outline",
        key: "housing_permits",
        set: "detail_housing_permits",
        fields: &[
            "purpose_name",
            "structure_name",
            "main_building_count",
            "total_floor_area_m2",
            "total_household_count",
            "approval_date",
            "start_scheduled_date",
            "start_actual_date",
            "use_inspection_scheduled_date",
            "use_inspection_date",
        ],
        newest: Some("approval_date"),
    },
    Detail {
        source: INSPECTION_SOURCE,
        key: "regular_inspections",
        set: "detail_regular_inspections",
        fields: &[
            "inspection_agency_name",
            "inspection_start_date",
            "processed_date",
        ],
        newest: Some("inspection_start_date"),
    },
];

/// The prepared table the inspections' row set reads (`<name><suffix>`).
const INSPECTION_LINKS: &str = "inspection_links";
/// The view the permits' row set joins (`<name><suffix>`).
const PARKING_BY_KEY: &str = "parking_by_key";

/// `parking_by_key<suffix>`: each 관리허가대장PK's parking, a tied repeat folded to its smallest
/// value per field.
const PARKING_SQL: &str = "\
CREATE OR REPLACE TEMP VIEW parking_by_key@suffix@ AS \
SELECT p.mgmt_key, @folds@ FROM (@parking@) p GROUP BY p.mgmt_key";

/// `inspection_links<suffix>`: the inspections on the parcel of the one 표제부 their 관리건축물대장PK
/// names (root ADR-0404 §2). `S-N` names the 표제부 whose key is four characters, `1`, then N, and
/// whose PNU starts with S or has this row's 법정동·번·지. A row that names no 표제부, or more than
/// one, reaches no parcel. The 표제부 are read only where their key is one an inspection names.
const INSPECTION_LINKS_SQL: &str = "\
CREATE OR REPLACE TEMP TABLE inspection_links@suffix@ AS \
WITH keyed AS ( \
  SELECT *, trim(building_register_key, ' ') AS register_key FROM inspection_rows@suffix@), \
named AS ( \
  SELECT *, substr(register_key, 1, instr(register_key, '-') - 1) AS key_sigungu, \
    '1' || substr(register_key, instr(register_key, '-') + 1) AS key_suffix \
  FROM keyed WHERE instr(register_key, '-') > 1), \
register AS ( \
  SELECT substr(mgm_bldrgst_pk, 5) AS key_suffix, pnu AS title_pnu FROM (@titles@) t \
  WHERE pnu IS NOT NULL), \
matched AS ( \
  SELECT named.*, register.title_pnu FROM named JOIN register ON named.key_suffix = register.key_suffix \
  WHERE substr(title_pnu, 1, 5) = key_sigungu \
    OR (substr(title_pnu, 6, 5) = trim(bjdong_cd, ' ') AND substr(title_pnu, 12, 4) = trim(bonbeon, ' ') \
      AND substr(title_pnu, 16, 4) = trim(bubeon, ' '))) \
SELECT title_pnu AS pnu, mgmt_key, @fields@ FROM matched \
QUALIFY count(*) OVER (PARTITION BY mgmt_key) = 1";

/// The 표제부 an inspection may name: a key longer than five whose suffix one names.
const TITLE_FILTER: &str = "length(mgm_bldrgst_pk) > 5 AND substr(mgm_bldrgst_pk, 5) IN ( \
    SELECT '1' || substr(trim(building_register_key, ' '), instr(trim(building_register_key, ' '), '-') + 1) \
    FROM inspection_rows@suffix@ WHERE instr(trim(building_register_key, ' '), '-') > 1)";

/// One list's rows of the PNUs of `targets` ranked and folded: `<name>_folded(pnu, records, total)`.
const FOLD_SQL: &str = "\
@name@_ranked AS ( \
  SELECT *, row_number() OVER (PARTITION BY pnu ORDER BY @order@) AS n FROM @table@ WHERE @targets@), \
@name@_folded AS ( \
  SELECT pnu, list({@record@} ORDER BY n) FILTER (WHERE n <= @bound@) AS records, \
    CAST(count(*) AS BIGINT) AS total \
  FROM @name@_ranked GROUP BY pnu)";

/// Every list folded into one document per PNU that has any, inserted into `built`.
const DOCUMENT_SQL: &str = "\
INSERT INTO built \
WITH @folds@, \
universe AS (@universe@), \
documents AS ( \
  SELECT u.pnu, to_json({'records': coalesce(permit.records, CAST([] AS @record_list@)), @details@})::VARCHAR \
      AS building_permits_json, \
    CAST(coalesce(permit.total, 0) AS BIGINT) AS building_permit_total \
  FROM universe u LEFT JOIN permit_folded permit ON permit.pnu = u.pnu @detail_joins@) \
SELECT pnu, building_permits_json, building_permit_total, CAST(NULL AS VARCHAR) AS attached_via, \
  sha256(to_json({'pnu': pnu, 'building_permits_json': building_permits_json, \
    'building_permit_total': building_permit_total})::VARCHAR) AS row_digest, \
  '@source_snapshot_id@' AS source_snapshot_id, '@published_at_utc@' AS published_at_utc \
FROM documents";

const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, building_permits_json VARCHAR, \
    building_permit_total BIGINT, attached_via VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, \
    published_at_utc VARCHAR);";

/// The fields ascending, a null first, then the key: what makes a list's order total.
fn field_order(fields: &[&str]) -> String {
    fields
        .iter()
        .chain(std::iter::once(&"mgmt_key"))
        .map(|column| format!("{column} ASC NULLS FIRST"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A record struct of `fields`, in their order.
fn record(fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| format!("'{field}': {field}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The fields of a permit record: its own, then its parking.
fn document_fields() -> Vec<&'static str> {
    RECORD_FIELDS
        .iter()
        .chain(PARKING_FIELDS.iter())
        .copied()
        .collect()
}

/// The building-permit kind.
#[derive(Clone, Copy, Debug)]
pub struct PermitKind;

/// The one building-permit kind.
pub const BUILDING_PERMITS: PermitKind = PermitKind;

impl KindVersionsStage for PermitKind {
    fn section_column(&self) -> &'static str {
        "building_permits_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        let mut inputs = vec![PARCEL_SOURCE, PERMIT_SOURCE, PARKING_SOURCE];
        inputs.extend(DETAILS.iter().map(|detail| detail.source));
        inputs.extend([TITLE_SOURCE, CODE_CHANGE_SOURCE]);
        inputs
    }

    fn code_version(&self) -> String {
        let mut hash = Sha256::new();
        let sets = format!("{:?}", self.row_sets());
        let spec = format!("{LATEST_DATES:?}{INSPECTION_COLUMNS:?}{DETAILS:?}");
        for text in [
            RULES_VERSION,
            &sets,
            &spec,
            PARKING_SQL,
            INSPECTION_LINKS_SQL,
            TITLE_FILTER,
            FOLD_SQL,
            DOCUMENT_SQL,
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
        let mut permit_reads = vec!["mgmt_key"];
        permit_reads.extend(RECORD_FIELDS);
        let mut permit_columns = provider_columns(&RECORD_FIELDS);
        permit_columns.extend(
            PARKING_FIELDS
                .iter()
                .map(|field| (format!("k.{field}"), *field)),
        );
        let mut sets = vec![RowSet {
            name: "permits",
            source: PERMIT_SOURCE,
            pnu: "pnu",
            reads: permit_reads,
            columns: permit_columns,
            joins: vec![format!(
                "LEFT JOIN {PARKING_BY_KEY}@suffix@ k ON k.mgmt_key = m.mgmt_key"
            )],
            prepared: false,
            distinct: true,
        }];
        for detail in &DETAILS {
            let mut reads = vec!["mgmt_key"];
            reads.extend(detail.fields);
            let inspections = detail.source == INSPECTION_SOURCE;
            sets.push(RowSet {
                name: detail.set,
                source: if inspections {
                    INSPECTION_LINKS
                } else {
                    detail.source
                },
                pnu: "pnu",
                reads,
                columns: provider_columns(detail.fields),
                joins: Vec::new(),
                prepared: inspections,
                distinct: true,
            });
        }
        sets
    }

    fn prepare(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let mut read_millis = BTreeMap::new();
        // The parking a permit's spool joins: a view, read once, in that spool.
        let mut parking_columns = vec!["mgmt_key"];
        parking_columns.extend(PARKING_FIELDS);
        let folds = PARKING_FIELDS
            .iter()
            .map(|field| format!("min({}) AS {field}", text(&format!("p.{field}"))))
            .collect::<Vec<_>>()
            .join(", ");
        connection.execute_batch(&fill(
            PARKING_SQL,
            &[
                ("suffix", suffix),
                ("folds", &folds),
                (
                    "parking",
                    &current_rows_query(
                        connection,
                        source,
                        PARKING_SOURCE,
                        &parking_columns,
                        "TRUE",
                    )?,
                ),
            ],
        ))?;

        // The inspections and the 표제부 they name: small, so read whole and linked here.
        let at = Instant::now();
        let inspection = DETAILS
            .iter()
            .find(|detail| detail.source == INSPECTION_SOURCE)
            .map(|detail| detail.fields)
            .unwrap_or_default();
        let mut inspection_columns = INSPECTION_COLUMNS.to_vec();
        inspection_columns.extend(inspection);
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE inspection_rows{suffix} AS {};",
            current_rows_query(
                connection,
                source,
                INSPECTION_SOURCE,
                &inspection_columns,
                "TRUE"
            )?
        ))?;
        let rows: i64 = connection.query_row(
            &format!("SELECT count(*) FROM inspection_rows{suffix}"),
            [],
            |row| row.get(0),
        )?;
        ensure!(
            rows <= MAX_INSPECTION_ROWS,
            "{INSPECTION_SOURCE} holds {rows} current rows, more than the {MAX_INSPECTION_ROWS} its \
             national link table may hold: read it as a row set"
        );
        read_millis.insert(INSPECTION_SOURCE.to_owned(), elapsed(at));
        let at = Instant::now();
        let titles = current_rows_query(
            connection,
            source,
            TITLE_SOURCE,
            &["mgm_bldrgst_pk", "pnu"],
            &fill(TITLE_FILTER, &[("suffix", suffix)]),
        )?;
        let fields = inspection.join(", ");
        connection.execute_batch(&fill(
            INSPECTION_LINKS_SQL,
            &[("suffix", suffix), ("titles", &titles), ("fields", &fields)],
        ))?;
        connection.execute_batch(&format!("DROP TABLE inspection_rows{suffix};"))?;
        read_millis.insert(TITLE_SOURCE.to_owned(), elapsed(at));
        Ok(read_millis)
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    fn changed_pnus(&self, connection: &Connection) -> anyhow::Result<u64> {
        let mut parts = Vec::new();
        for set in self.row_sets() {
            let table = set.name;
            for (a, b) in [("", "_old"), ("_old", "")] {
                parts.push(format!(
                    "(SELECT pnu FROM (SELECT * FROM {table}{a} EXCEPT ALL SELECT * FROM {table}{b}))"
                ));
            }
        }
        for (a, b) in [("", "_old"), ("_old", "")] {
            parts.push(format!(
                "(SELECT pnu FROM served{a} EXCEPT SELECT pnu FROM served{b})"
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
        ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        let targets = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        let bound = RECORD_BOUND.to_string();
        let fold = |name: &str, table: &str, order: &str, record: &str| {
            fill(
                FOLD_SQL,
                &[
                    ("name", name),
                    ("table", table),
                    ("order", order),
                    ("targets", targets),
                    ("record", record),
                    ("bound", &bound),
                ],
            )
        };
        let latest = format!(
            "greatest({}) DESC",
            LATEST_DATES
                .iter()
                .map(|date| format!("coalesce({date}, '')"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut folds = vec![fold(
            "permit",
            "permits",
            &format!("{latest}, {}", field_order(&RECORD_FIELDS)),
            &record(&document_fields()),
        )];
        let mut details = Vec::new();
        let mut joins = Vec::new();
        let mut universe = vec!["SELECT pnu FROM permit_folded".to_owned()];
        for detail in &DETAILS {
            let order = detail.newest.map_or_else(
                || field_order(detail.fields),
                |newest| {
                    format!(
                        "coalesce({newest}, '') DESC, {}",
                        field_order(detail.fields)
                    )
                },
            );
            let name = format!("d_{}", detail.key);
            folds.push(fold(&name, detail.set, &order, &record(detail.fields)));
            details.push(format!(
                "'{key}': CASE WHEN {name}.pnu IS NULL THEN NULL \
                 ELSE {{'total': {name}.total, 'records': {name}.records}} END",
                key = detail.key
            ));
            joins.push(format!(
                "LEFT JOIN {name}_folded {name} ON {name}.pnu = u.pnu"
            ));
            universe.push(format!("SELECT pnu FROM {name}_folded"));
        }
        let record_list = format!(
            "STRUCT({})[]",
            document_fields()
                .iter()
                .map(|field| format!("{field} VARCHAR"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let at = Instant::now();
        connection
            .execute_batch(&fill(
                DOCUMENT_SQL,
                &[
                    ("folds", &folds.join(", ")),
                    ("universe", &universe.join(" UNION ")),
                    ("record_list", &record_list),
                    ("details", &details.join(", ")),
                    ("detail_joins", &joins.join(" ")),
                    ("source_snapshot_id", source_snapshot_id),
                    ("published_at_utc", published_at_utc),
                ],
            ))
            .map_err(|error| {
                anyhow::anyhow!(
                    "DuckDB refused the building-permit document statement after {} ms: {error}",
                    elapsed(at)
                )
            })?;
        Ok(())
    }
}

/// A row set's columns: the key, then `fields` as the document writes them.
fn provider_columns(fields: &[&'static str]) -> Vec<(String, &'static str)> {
    std::iter::once(("m.mgmt_key".to_owned(), "mgmt_key"))
        .chain(
            fields
                .iter()
                .map(|field| (text(&format!("m.{field}")), *field)),
        )
        .collect()
}

#[cfg(test)]
mod tests;
