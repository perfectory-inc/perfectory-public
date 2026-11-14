//! The parcel nearby kind's Gold in `DuckDB` inside the publisher (root ADR-0465, ADR-0559,
//! ADR-0564 §9 turn 2).
//!
//! What is near each served parcel, from its boundary's centroid: the nearest 공항, 철도역,
//! 도시철도역, 초중등학교 and 대학 with the distance on the WGS84 spheroid, the 상가(상권) stores in the
//! H3 cells within [`STORE_H3_RING`] rings of the parcel's cell, and the 고속도로 출입시설 of its
//! 시군구. It was the Spark job `parcel_nearby_silver_to_gold.py` (Sedona `ST_KNN`, k = 1), which
//! never committed: four national builds ran to the 160-minute unit bound (2026-11-12..13). Judged on
//! 경기 (5,208,587 parcels) against that job on the same pins: no parcel on one side only, every
//! differing value either nearer here (at most 216 m) or an equal-distance tie Sedona broke
//! arbitrarily, nearer in Spark never. The owner chose this result (2026-11-12, root ADR-0559
//! exception ⓑ) and its tie rule: the nearest by distance, then the facility's `row_key`
//! (`nearest.sql`).
//!
//! The caller pins every input once ([`NearbyPins`]); [`load_inputs`] reads them, and
//! [`build_sido`] computes one 시도 into `built`. A 시도 at a time keeps the peak inside the stage's
//! memory: nationally 39,861,511 parcels in 445.6 s with a 5.01 GiB peak (W81b, 2026-11-13, 4
//! threads, a 5 GB `DuckDB` limit). Nothing here commits.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{ensure, Context};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};

const NEAREST_SQL: &str = include_str!("parcel_nearby_stage/nearest.sql");
const DOCUMENT_SQL: &str = include_str!("parcel_nearby_stage/document.sql");

/// The served parcels.
pub const PARCEL_SOURCE: &str = "silver.parcel_boundaries";
/// The 상가(상권) stores.
pub const STORE_SOURCE: &str = "silver.commercial_district";
/// The 대학, placed at the centroid of the parcel their address resolved to.
pub const UNIVERSITY_SOURCE: &str = "silver.university";
/// The 고속도로 출입시설, placed by the 법정동 code their names resolved to.
pub const HIGHWAY_SOURCE: &str = "silver.highway_access_point";
/// The code change table: a code under a reorganization is read under the served code.
pub const CODE_CHANGE_SOURCE: &str = "reference.legal_dong_code_change";
/// Each parcel's stable place id (root ADR-0483).
pub const BRIDGE_SOURCE: &str = "gold.place_id_bridge";

/// A point file: its table, the document key, its latitude and longitude headers, and its
/// `(document field, header)` pairs, as `data-go-kr-file-data.contract.json` names the headers.
pub struct PointFile {
    /// The Silver table.
    pub table: &'static str,
    /// The document key.
    pub key: &'static str,
    /// The latitude header.
    pub latitude: &'static str,
    /// The longitude header.
    pub longitude: &'static str,
    /// The document fields and the headers they are read from, in the document's order.
    pub fields: &'static [(&'static str, &'static str)],
}

/// The files whose rows carry coordinates, in the document's order.
pub const POINT_FILES: [PointFile; 4] = [
    PointFile {
        table: "silver.airport",
        key: "airport",
        latitude: "위도(WGS84좌표)",
        longitude: "경도(WGS84좌표)",
        fields: &[("name", "공항명")],
    },
    PointFile {
        table: "silver.railway_station",
        key: "railway_station",
        latitude: "위도좌표",
        longitude: "경도좌표",
        fields: &[("name", "역이름")],
    },
    PointFile {
        table: "silver.subway_station",
        key: "subway_station",
        latitude: "LATITUDE",
        longitude: "LONGITUDE",
        fields: &[("name", "STATN_NM"), ("route_name", "ROUTE_NM")],
    },
    PointFile {
        table: "silver.school_location",
        key: "school",
        latitude: "LATITUDE",
        longitude: "LONGITUDE",
        fields: &[("name", "SCHOOL_NM"), ("school_level", "SCHOOL_SE")],
    },
];
/// The 대학's document key and fields.
pub const UNIVERSITY: (&str, &[(&str, &str)]) = (
    "university",
    &[("name", "SCHL_NM"), ("school_kind", "SCHL_SE_NM")],
);
/// The highway list's fields.
pub const HIGHWAY_FIELDS: [(&str, &str); 2] =
    [("route_name", "노선명"), ("facility_name", "출입시설")];
/// The stores' coordinate headers.
pub const STORE_COORDINATES: (&str, &str) = ("위도", "경도");
/// Resolution 9 cells average 0.105 km² (edge about 174 m, h3geo.org); two rings span about five
/// cell edges, about 500 m around the parcel.
pub const STORE_H3_RESOLUTION: u8 = 9;
/// The rings around a parcel's cell whose stores it counts.
pub const STORE_H3_RING: u8 = 2;
/// At most this many highway access points are listed (the total is always given).
pub const RECORD_BOUND: u32 = 20;
/// The mainland, Jeju, Ulleung and Dokdo, with margin (degrees): a coordinate outside it, or not a
/// number, is the source's error and not a candidate.
pub const KOREA_LATITUDE: (f64, f64) = (32.5, 39.0); // public-repository-safety: reviewed-runtime-coordinate
/// See [`KOREA_LATITUDE`].
pub const KOREA_LONGITUDE: (f64, f64) = (124.0, 132.0); // public-repository-safety: reviewed-runtime-coordinate
/// The coarse and fine candidate cells, in degrees (`nearest.sql`).
const COARSE_DEGREES: f64 = 0.05;
const FINE_DEGREES: f64 = 0.005;

/// What the rules are; part of the build's code version, with the SQL texts.
pub const RULES_VERSION: &str = "parcel-nearby-duckdb-v1";

/// Every input of the build, in the order the summary names them.
#[must_use]
pub fn inputs() -> Vec<&'static str> {
    let mut tables = vec![PARCEL_SOURCE];
    tables.extend(POINT_FILES.iter().map(|file| file.table));
    tables.extend([
        UNIVERSITY_SOURCE,
        STORE_SOURCE,
        HIGHWAY_SOURCE,
        CODE_CHANGE_SOURCE,
        BRIDGE_SOURCE,
    ]);
    tables
}

/// The inputs whose change may move any parcel's document.
///
/// A facility file (any parcel's nearest point), the highway file and the code change table. A
/// change of the parcels, the stores or the place ids moves only the 시도 it touches.
#[must_use]
pub fn whole_table_inputs() -> Vec<&'static str> {
    let mut tables: Vec<&'static str> = POINT_FILES.iter().map(|file| file.table).collect();
    tables.extend([UNIVERSITY_SOURCE, HIGHWAY_SOURCE, CODE_CHANGE_SOURCE]);
    tables
}

/// The build's code version: the rules version, the SQL texts and the shared code views it runs.
/// A change of any of them recomputes every 시도.
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [
        RULES_VERSION,
        NEAREST_SQL,
        DOCUMENT_SQL,
        PAIR_ROWS_SQL,
        RENUMBERED_INTO_SQL,
    ] {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// The snapshot each input is read at, by table name.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NearbyPins(pub BTreeMap<String, i64>);

impl NearbyPins {
    /// The pin of `table`.
    ///
    /// # Errors
    /// Returns an error when `table` is not pinned.
    pub fn of(&self, table: &str) -> anyhow::Result<i64> {
        self.0
            .get(table)
            .copied()
            .with_context(|| format!("the nearby build has no pin for {table}"))
    }
}

/// Where the build reads each input: a table reference for `DuckDB`.
pub enum Source<'a> {
    /// The lakehouse attached as `catalog`, each table at its pin.
    Pinned {
        /// The attached catalog's name.
        catalog: &'a str,
        /// The snapshot of each input.
        pins: &'a NearbyPins,
    },
    /// Tables of the connection itself, named `<namespace>_<table>` (the tests' inputs).
    Local,
}

impl Source<'_> {
    /// The reference `DuckDB` reads `table` through.
    ///
    /// # Errors
    /// Returns an error when the catalog is not a plain identifier or `table` is not pinned.
    pub fn of(&self, table: &str) -> anyhow::Result<String> {
        ensure!(
            table.split('.').count() == 2
                && table
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.'),
            "{table} is not namespace.table"
        );
        match self {
            Self::Pinned { catalog, pins } => {
                ensure!(
                    !catalog.is_empty()
                        && catalog
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
                    "the catalog must be a plain identifier"
                );
                Ok(format!(
                    "{catalog}.{table} AT (VERSION => {})",
                    pins.of(table)?
                ))
            }
            Self::Local => Ok(table.replace('.', "_")),
        }
    }
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// `template` with each `{name}` in it replaced by its value.
fn fill(template: &str, values: &[(&str, &str)]) -> String {
    values
        .iter()
        .fold(template.to_owned(), |text, (name, value)| {
            text.replace(&format!("{{{name}}}"), value)
        })
}

fn is_plain(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// A header as a JSON path into `provider_fields_json` (Spark: `from_json(…, map<string,string>)[h]`).
fn field(header: &str) -> anyhow::Result<String> {
    ensure!(
        !header.contains(['\'', '"', '\\']),
        "a header holds a quote: {header}"
    );
    Ok(format!(
        "json_extract_string(provider_fields_json, '$.\"{header}\"')"
    ))
}

/// The current rows of `table` at `pin`, projecting `columns`, by its contract's one rendering
/// (root ADR-0564 §4). A table that must not tie is refused when two current rows of a key do.
fn current_rows(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
) -> anyhow::Result<String> {
    let contract = contract(table)?;
    ensure!(
        contract.current_row_predicate.is_none(),
        "{table} selects its current rows by a predicate too; this reader applies only the newest-per-key rule"
    );
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    let source = source.of(table)?;
    let query = latest.current_rows_sql(&source, columns, "TRUE");
    if !latest.keep_ties {
        let key = latest.key.join(", ");
        let (rows, keys): (i64, i64) = connection.query_row(
            &format!(
                "SELECT count(*), count(DISTINCT ({key})) FROM ({})",
                latest.current_rows_sql(&source, latest.key, "TRUE")
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows == keys,
            "{table} ({source}) holds {} current rows tied on their version order",
            rows - keys
        );
    }
    Ok(query)
}

/// The spheroid distance in metres between two (longitude, latitude) points, as a macro.
///
/// `DuckDB` 2.0 refuses a spheroid distance until `geometry_always_xy` is set: true reads a point as
/// (longitude, latitude), and 2.1 makes it the default. A build that read the points the other way
/// would answer every distance wrong without failing, so the order is checked on two known points:
/// 서울시청 → 부산시청 is about 325 km.
fn define_spheroid(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(
        "SET geometry_always_xy = true; \
         CREATE OR REPLACE MACRO nearby_spheroid_m(lon1, lat1, lon2, lat2) AS \
         ST_Distance_Spheroid(ST_Point(lon1, lat1), ST_Point(lon2, lat2));",
    )?;
    let metres: f64 = connection.query_row(
        "SELECT nearby_spheroid_m(126.9779, 37.5663, 129.0750, 35.1798)", // public-repository-safety: reviewed-runtime-coordinate
        [],
        |row| row.get(0),
    )?;
    ensure!(
        (320_000.0..330_000.0).contains(&metres),
        "the spheroid distance from 서울시청 to 부산시청 is {metres} m, not about 325 km: the spatial \
         extension reads its points in another axis order"
    );
    Ok(())
}

fn inside(latitude: &str, longitude: &str) -> String {
    format!(
        "{latitude} BETWEEN {} AND {} AND {longitude} BETWEEN {} AND {}",
        KOREA_LATITUDE.0, KOREA_LATITUDE.1, KOREA_LONGITUDE.0, KOREA_LONGITUDE.1
    )
}

/// What [`load_inputs`] read.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct NearbyInputs {
    /// Served parcels with a centroid, by 시도.
    pub parcels_by_sido: BTreeMap<String, u64>,
    /// Located facilities per document key.
    pub facilities: BTreeMap<String, u64>,
    /// Located stores.
    pub stores: u64,
    /// 시군구 with highway access points.
    pub highway_sigungu: u64,
    /// Codes the renumbering translates.
    pub renumbered_codes: u64,
    /// The located stores per H3 cell, which [`build_sido`] counts the rings of.
    #[serde(skip)]
    pub store_cells: StoreCells,
}

/// The H3 cell (resolution [`STORE_H3_RESOLUTION`]) of a point, as Sedona's `ST_H3CellIDs` and the
/// h3 library give it (H3 v4).
fn cell(lat: f64, lon: f64) -> anyhow::Result<h3o::CellIndex> {
    let resolution = h3o::Resolution::try_from(STORE_H3_RESOLUTION)?;
    Ok(h3o::LatLng::new(lat, lon)
        .with_context(|| format!("({lat}, {lon}) is no point"))?
        .to_cell(resolution))
}

/// The located stores per H3 cell.
///
/// H3 is computed in the publisher with h3o (a Rust implementation of the H3 library): `DuckDB`'s
/// community h3 extension loads only with unsigned extensions allowed, which the release refuses.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreCells(HashMap<u64, u64>);

impl StoreCells {
    /// The stores at `points` (latitude, longitude), counted per cell.
    ///
    /// # Errors
    /// Returns an error when a point is not a coordinate.
    pub fn from_points(points: &[(f64, f64)]) -> anyhow::Result<Self> {
        let mut cells = HashMap::new();
        for (lat, lon) in points {
            *cells.entry(u64::from(cell(*lat, *lon)?)).or_insert(0) += 1;
        }
        Ok(Self(cells))
    }

    /// Every store counted.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.0.values().sum()
    }

    /// The stores in the cells within [`STORE_H3_RING`] rings of the cell of (`lat`, `lon`).
    ///
    /// # Errors
    /// Returns an error when the point is not a coordinate.
    pub fn ring_count(&self, lat: f64, lon: f64) -> anyhow::Result<u64> {
        Ok(cell(lat, lon)?
            .grid_disk::<Vec<_>>(u32::from(STORE_H3_RING))
            .into_iter()
            .filter_map(|ring| self.0.get(&u64::from(ring)))
            .sum())
    }
}

/// The (latitude, longitude) rows `query` returns.
fn points(connection: &Connection, query: &str) -> anyhow::Result<Vec<(f64, f64)>> {
    connection
        .prepare(query)?
        .query_map([], |row| Ok((row.get::<_, f64>(0)?, row.get::<_, f64>(1)?)))?
        .collect::<Result<Vec<_>, _>>()
        .context("DuckDB failed to read the points")
}

/// Reads every input from `source` into `connection`.
///
/// It defines the macros the build runs and leaves `parcels(pnu, lon, lat)` of the served
/// `edition`, a `f_<key>` table per nearest kind, the stores per H3 cell, `highway(sigungu,
/// highway_access_points)`, `place_ids(pnu, place_id)` and an empty `built`.
///
/// # Errors
/// Returns an error when a snapshot cannot be read, an input ties where its contract forbids it, a
/// served parcel appears twice, or the spatial extension measures distances wrong.
#[allow(clippy::too_many_lines)]
pub fn load_inputs(
    connection: &Connection,
    source: &Source<'_>,
    edition: &str,
) -> anyhow::Result<NearbyInputs> {
    ensure!(
        is_plain(edition),
        "the edition {edition:?} is not a plain snapshot id"
    );
    define_spheroid(connection)?;
    let parcels = contract(PARCEL_SOURCE)?;
    let predicate = parcels
        .current_row_predicate
        .context("silver.parcel_boundaries names its current rows by a predicate")?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE parcels AS \
         SELECT pnu, ST_X(g) AS lon, ST_Y(g) AS lat \
         FROM (SELECT pnu, ST_Centroid(ST_GeomFromWKB(geometry_wkb)) AS g \
               FROM {parcels_table} \
               WHERE source_snapshot_id = '{edition}' AND ({predicate}) AND geometry_wkb IS NOT NULL);",
        parcels_table = source.of(PARCEL_SOURCE)?
    ))?;
    let (rows, keys): (i64, i64) = connection.query_row(
        "SELECT count(*), count(DISTINCT pnu) FROM parcels",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(rows > 0, "edition {edition} holds no served parcel");
    ensure!(
        rows == keys,
        "edition {edition} holds {} parcels more than once",
        rows - keys
    );

    // The code change table's pairs in force, and the codes the served edition holds of them.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE code_changes AS \
         SELECT kind, old_code, new_code, level, effective_date, source \
         FROM {changes} WHERE kind IN ('pair', 'supersession');",
        changes = source.of(CODE_CHANGE_SOURCE)?
    ))?;
    connection.execute_batch(PAIR_ROWS_SQL)?;
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE served_codes AS \
         SELECT DISTINCT substr(pnu, 1, 10) AS code FROM parcels WHERE substr(pnu, 1, 10) IN ( \
           SELECT old_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri') \
           UNION SELECT new_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri'));",
    )?;
    connection.execute_batch(RENUMBERED_INTO_SQL)?;

    let mut facilities = BTreeMap::new();
    for file in &POINT_FILES {
        let mut columns = vec![
            "row_key".to_owned(),
            format!("TRY_CAST(trim({}) AS DOUBLE) AS lat", field(file.latitude)?),
            format!(
                "TRY_CAST(trim({}) AS DOUBLE) AS lon",
                field(file.longitude)?
            ),
        ];
        for (name, header) in file.fields {
            columns.push(format!("{} AS {name}", field(header)?));
        }
        let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let rows = current_rows(connection, source, file.table, &columns)?;
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE f_{key} AS SELECT * FROM ({rows}) WHERE {inside};",
            key = file.key,
            inside = inside("lat", "lon")
        ))?;
    }
    let (university_key, university_fields) = UNIVERSITY;
    let mut columns = vec!["row_key".to_owned(), "pnu".to_owned()];
    for (name, header) in university_fields {
        columns.push(format!("{} AS {name}", field(header)?));
    }
    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let rows = current_rows(connection, source, UNIVERSITY_SOURCE, &columns)?;
    let picked = university_fields
        .iter()
        .map(|(name, _)| format!("u.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    // A 대학 stands at the centroid of its (renumbered) parcel, read from every served parcel.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE f_{university_key} AS \
         SELECT u.row_key, p.lat, p.lon, {picked} FROM ( \
           SELECT r.* EXCLUDE (pnu), coalesce(n.dst || substr(r.pnu, 11, 9), r.pnu) AS pnu \
           FROM ({rows}) r LEFT JOIN renumbering n ON substr(r.pnu, 1, 10) = n.src WHERE r.pnu IS NOT NULL \
         ) u JOIN parcels p USING (pnu) WHERE {inside};",
        inside = inside("p.lat", "p.lon")
    ))?;
    for key in POINT_FILES
        .iter()
        .map(|file| file.key)
        .chain([university_key])
    {
        let located: i64 =
            connection.query_row(&format!("SELECT count(*) FROM f_{key}"), [], |row| {
                row.get(0)
            })?;
        ensure!(located > 0, "no located {key}: the build would name none");
        facilities.insert(key.to_owned(), u64::try_from(located)?);
    }

    let (store_lat, store_lon) = STORE_COORDINATES;
    let store_columns = [
        format!("TRY_CAST(trim({}) AS DOUBLE) AS lat", field(store_lat)?),
        format!("TRY_CAST(trim({}) AS DOUBLE) AS lon", field(store_lon)?),
    ];
    let store_columns = store_columns.iter().map(String::as_str).collect::<Vec<_>>();
    let rows = current_rows(connection, source, STORE_SOURCE, &store_columns)?;
    let store_cells = StoreCells::from_points(&points(
        connection,
        &format!(
            "SELECT lat, lon FROM ({rows}) WHERE {}",
            inside("lat", "lon")
        ),
    )?)?;

    let mut highway_columns = vec!["legal_dong_code".to_owned()];
    for (name, header) in HIGHWAY_FIELDS {
        highway_columns.push(format!("{} AS {name}", field(header)?));
    }
    let highway_columns = highway_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let rows = current_rows(connection, source, HIGHWAY_SOURCE, &highway_columns)?;
    let record = HIGHWAY_FIELDS
        .iter()
        .map(|(name, _)| format!("'{name}': {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let distinct = HIGHWAY_FIELDS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE highway AS \
         WITH k AS (SELECT DISTINCT substr(coalesce(n.dst, h.legal_dong_code), 1, 5) AS sigungu, {distinct} \
                    FROM ({rows}) h LEFT JOIN renumbering n ON h.legal_dong_code = n.src \
                    WHERE h.legal_dong_code IS NOT NULL) \
         SELECT sigungu, {{'total': count(*)::BIGINT, \
                           'records': list_slice(list_sort(list({{{record}}})), 1, {RECORD_BOUND})}} AS highway_access_points \
         FROM k GROUP BY sigungu;"
    ))?;

    // The parcel place ids: each code's open period, else its latest closed one; a code-pair line
    // is a new code's form of an id, not a period (place_id_join.place_ids).
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE place_ids AS SELECT code AS pnu, place_id FROM ( \
           SELECT code, place_id, row_number() OVER (PARTITION BY code \
             ORDER BY valid_to IS NOT NULL, valid_to DESC, place_id) AS newest \
           FROM {bridge} \
           WHERE unit = 'parcel' AND valid_from <> 'legal-dong-code-change') WHERE newest = 1;",
        bridge = source.of(BRIDGE_SOURCE)?
    ))?;
    connection.execute_batch(BUILT_TABLE)?;

    let count = |sql: &str| -> anyhow::Result<u64> {
        let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    };
    let parcels_by_sido = connection
        .prepare("SELECT substr(pnu, 1, 2), count(*) FROM parcels GROUP BY 1 ORDER BY 1")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .map(|row| {
            let (sido, rows) = row?;
            Ok((sido, u64::try_from(rows)?))
        })
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    Ok(NearbyInputs {
        parcels_by_sido,
        facilities,
        stores: store_cells.total(),
        highway_sigungu: count("SELECT count(*) FROM highway")?,
        renumbered_codes: count("SELECT count(*) FROM renumbering")?,
        store_cells,
    })
}

/// The 시도 whose documents a move of the parcels, the stores or the place ids can change.
///
/// Between `old` and `new` (the inputs [`load_inputs`] read): a parcel whose boundary or place id
/// differs, and the parcels within [`STORE_H3_RING`] rings of a cell a store left or entered. Every
/// other input is whole-table ([`whole_table_inputs`]): the caller recomputes every 시도 for those.
///
/// # Errors
/// Returns an error when a snapshot cannot be read.
pub fn changed_sidos(
    connection: &Connection,
    old: &Source<'_>,
    new: &Source<'_>,
    moved: &BTreeSet<String>,
    edition: &str,
) -> anyhow::Result<BTreeSet<String>> {
    ensure!(
        is_plain(edition),
        "the edition {edition:?} is not a plain snapshot id"
    );
    let mut parts = Vec::new();
    if moved.contains(PARCEL_SOURCE) {
        let predicate = contract(PARCEL_SOURCE)?
            .current_row_predicate
            .context("silver.parcel_boundaries names its current rows by a predicate")?;
        let side = |source: &Source<'_>| -> anyhow::Result<String> {
            Ok(format!(
                "SELECT pnu, md5(geometry_wkb) AS shape FROM {} WHERE source_snapshot_id = '{edition}' AND ({predicate})",
                source.of(PARCEL_SOURCE)?
            ))
        };
        let (before, after) = (side(old)?, side(new)?);
        parts.push(format!(
            "SELECT substr(pnu, 1, 2) AS sido FROM (({after}) EXCEPT ({before}))              UNION SELECT substr(pnu, 1, 2) FROM (({before}) EXCEPT ({after}))"
        ));
    }
    if moved.contains(BRIDGE_SOURCE) {
        let side = |source: &Source<'_>| -> anyhow::Result<String> {
            Ok(format!(
                "SELECT code AS pnu, place_id FROM (SELECT code, place_id, row_number() OVER (PARTITION BY code                  ORDER BY valid_to IS NOT NULL, valid_to DESC, place_id) AS newest FROM {}                  WHERE unit = 'parcel' AND valid_from <> 'legal-dong-code-change') WHERE newest = 1",
                source.of(BRIDGE_SOURCE)?
            ))
        };
        let (before, after) = (side(old)?, side(new)?);
        parts.push(format!(
            "SELECT substr(pnu, 1, 2) FROM (({after}) EXCEPT ({before}))              UNION SELECT substr(pnu, 1, 2) FROM (({before}) EXCEPT ({after}))"
        ));
    }
    let mut found = BTreeSet::new();
    if moved.contains(STORE_SOURCE) {
        let (lat, lon) = STORE_COORDINATES;
        let columns = [
            "row_key".to_owned(),
            format!("TRY_CAST(trim({}) AS DOUBLE) AS lat", field(lat)?),
            format!("TRY_CAST(trim({}) AS DOUBLE) AS lon", field(lon)?),
        ];
        let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let side = |source: &Source<'_>| -> anyhow::Result<BTreeSet<(String, u64)>> {
            let query = format!(
                "SELECT row_key, lat, lon FROM ({}) WHERE {}",
                current_rows(connection, source, STORE_SOURCE, &columns)?,
                inside("lat", "lon")
            );
            connection
                .prepare(&query)?
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, f64>(2)?,
                    ))
                })?
                .map(|row| {
                    let (key, lat, lon) = row?;
                    Ok((key, u64::from(cell(lat, lon)?)))
                })
                .collect()
        };
        let (before, after) = (side(old)?, side(new)?);
        // The cells a store left or entered, and every cell within the rings of one.
        let mut touched = BTreeSet::new();
        for (_, moved_cell) in before.symmetric_difference(&after) {
            let center = h3o::CellIndex::try_from(*moved_cell)?;
            for ring in center.grid_disk::<Vec<_>>(u32::from(STORE_H3_RING)) {
                touched.insert(u64::from(ring));
            }
        }
        if !touched.is_empty() {
            let mut statement = connection.prepare("SELECT pnu, lat, lon FROM parcels")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let pnu: String = row.get(0)?;
                let prefix = pnu.get(..2).unwrap_or_default().to_owned();
                if !found.contains(&prefix)
                    && touched.contains(&u64::from(cell(row.get(1)?, row.get(2)?)?))
                {
                    found.insert(prefix);
                }
            }
        }
    }
    if !parts.is_empty() {
        let sidos = connection
            .prepare(&format!(
                "SELECT DISTINCT * FROM ({}) ORDER BY 1",
                parts.join(" UNION ")
            ))?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()
            .context("DuckDB failed to find the 시도 the moved inputs touch")?;
        found.extend(sidos);
    }
    Ok(found)
}

/// The table [`build_sido`] appends to: the Gold's columns but `version_seq` and `row_state`.
const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, nearby_json VARCHAR, \
    attached_via VARCHAR, place_id VARCHAR, row_digest VARCHAR, source_snapshot_id VARCHAR, \
    published_at_utc VARCHAR);";

/// The nearest kinds in the document's order: the point files, then the 대학.
fn nearest_kinds() -> Vec<(&'static str, &'static [(&'static str, &'static str)])> {
    POINT_FILES
        .iter()
        .map(|file| (file.key, file.fields))
        .chain([UNIVERSITY])
        .collect()
}

/// Whether `sido` is a 시도 prefix (two digits).
#[must_use]
pub fn is_sido(sido: &str) -> bool {
    sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit())
}

/// Computes the rows of the parcels of `sido` into `built`: their documents, place ids and digests,
/// each stamped `source_snapshot_id` (the parcels' pin) and `published_at_utc`. Returns the rows it
/// added.
///
/// # Errors
/// Returns an error when `sido` is not two digits, a stamp is not plain text, or `DuckDB` fails.
pub fn build_sido(
    connection: &Connection,
    stores: &StoreCells,
    sido: &str,
    source_snapshot_id: &str,
    published_at_utc: &str,
) -> anyhow::Result<u64> {
    ensure!(is_sido(sido), "{sido:?} is not a 시도 prefix");
    ensure!(
        is_plain(source_snapshot_id) && is_plain(published_at_utc),
        "a stamp holds characters a snapshot id or a timestamp does not use"
    );
    let before: i64 = connection.query_row("SELECT count(*) FROM built", [], |row| row.get(0))?;
    // Tables live in the stage's database file: a temporary table stays in memory and cannot spill.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE sp AS SELECT *, floor(lon / {COARSE_DEGREES}) AS cx, floor(lat / {COARSE_DEGREES}) AS cy, \
         floor(lon / {FINE_DEGREES}) AS fx, floor(lat / {FINE_DEGREES}) AS fy \
         FROM parcels WHERE pnu >= '{sido}' AND pnu < '{sido}:';"
    ))?;
    let mut joins = Vec::new();
    let mut document = Vec::new();
    for (kind, fields) in nearest_kinds() {
        let names = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        let fields_of_f = names
            .iter()
            .map(|name| format!("f.{name}"))
            .collect::<Vec<_>>()
            .join(", ");
        let struct_fields = names
            .iter()
            .map(|name| format!("'{name}': {name}"))
            .collect::<Vec<_>>()
            .join(", ");
        let fields_order = names
            .iter()
            .map(|name| format!("{name} NULLS FIRST"))
            .collect::<Vec<_>>()
            .join(", ");
        let (coarse, fine) = (COARSE_DEGREES.to_string(), FINE_DEGREES.to_string());
        let sql = fill(
            NEAREST_SQL,
            &[
                ("kind", kind),
                ("coarse", &coarse),
                ("fine", &fine),
                ("fields_of_f", &fields_of_f),
                ("struct_fields", &struct_fields),
                ("fields_order", &fields_order),
            ],
        );
        connection
            .execute_batch(&sql)
            .with_context(|| format!("DuckDB failed to find the nearest {kind} in {sido}"))?;
        joins.push(format!("LEFT JOIN n_{kind} USING (pnu)"));
        document.push(format!("'{kind}': {kind}"));
    }
    document.push("'commercial_store_count': coalesce(r.store_count, 0)".to_owned());
    document.push("'highway_access_points': h.highway_access_points".to_owned());
    // Each parcel's stores within the rings of its cell.
    connection.execute_batch("CREATE OR REPLACE TABLE ring (pnu VARCHAR, store_count BIGINT);")?;
    {
        let mut appender = connection.appender("ring")?;
        let mut statement = connection.prepare("SELECT pnu, lat, lon FROM sp")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let pnu: String = row.get(0)?;
            let count = i64::try_from(stores.ring_count(row.get(1)?, row.get(2)?)?)?;
            if count > 0 {
                appender.append_row(duckdb::params![pnu, count])?;
            }
        }
        appender.flush()?;
    }
    let (joins, document) = (joins.join(" "), format!("{{{}}}", document.join(", ")));
    let sql = fill(
        DOCUMENT_SQL,
        &[
            ("joins", &joins),
            ("document", &document),
            ("source_snapshot_id", source_snapshot_id),
            ("published_at_utc", published_at_utc),
        ],
    );
    connection
        .execute_batch(&sql)
        .with_context(|| format!("DuckDB failed to assemble the documents of {sido}"))?;
    let after: i64 = connection.query_row("SELECT count(*) FROM built", [], |row| row.get(0))?;
    let parcels: i64 = connection.query_row("SELECT count(*) FROM sp", [], |row| row.get(0))?;
    // Root ADR-0272: every served parcel has the section.
    ensure!(
        after - before == parcels,
        "{sido}: {parcels} served parcels but {} documents",
        after - before
    );
    Ok(u64::try_from(after - before)?)
}

#[cfg(test)]
mod tests;
