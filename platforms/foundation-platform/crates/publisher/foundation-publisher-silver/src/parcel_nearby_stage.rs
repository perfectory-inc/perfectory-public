//! The parcel nearby kind's Gold in `DuckDB` inside the publisher.
//!
//! Root ADR-0465, ADR-0559, ADR-0564 §9 turn 2; on the shared Gold versions driver
//! (`kind_versions_build`, root ADR-0615 §7).
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
//! What the lane gives the shared driver ([`NEARBY`]): the facilities, the highway list and the
//! stores' ring counts, small, read once nationally ([`KindVersionsStage::prepare`]); the served
//! parcels at their centroids, spooled by 시도 by the shared split ([`KindVersionsStage::spool_more`]);
//! and its build, which the shared build shows one 시군구 at a time: a parcel's nearest facility is
//! searched over the national facilities, so a part needs no other part's rows. A partial run
//! rebuilds the parcels whose centroid or place id changed (the shared comparison of the two reads)
//! and those near a cell whose store count moved ([`KindVersionsStage::changed_keys_sql`]); a commit
//! that only store counts changed is recorded as such ([`KindVersionsStage::change_explanation`],
//! root ADR-0596). Nationally 39,861,511 parcels took 445.6 s with a 5.01 GiB peak a 시도 at a time
//! (W81b, 2026-11-13, 4 threads, a 5 GB `DuckDB` limit).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Instant;

use anyhow::{ensure, Context};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use sha2::{Digest, Sha256};

use duckdb::Connection;

use crate::duckdb_parquet_stage::with_contract_columns;
use crate::kind_versions_stage::{
    elapsed, spool_by_sido, ChangeExplanation, KindVersionsStage, RowSet, BASE_SUFFIX, SHARED_SQL,
};

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
pub use crate::place_id_join::BRIDGE_SOURCE;

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
/// The H3 resolution the stores are counted at.
///
/// Resolution 9 cells average 0.105 km² (edge about 174 m, h3geo.org); the parcel's cell and the
/// two rings around it are 19 cells, about 2.0 km², as much as a circle of about 800 m radius,
/// centred on the parcel's cell (not on the parcel).
pub const STORE_H3_RESOLUTION: u8 = 9;
/// The rings around a parcel's cell whose stores it counts.
pub const STORE_H3_RING: u8 = 2;
/// At most this many highway access points are listed (the total is always given).
pub const RECORD_BOUND: u32 = 20;
/// The document field a store-only commit may change, and nothing else (root ADR-0596).
pub const STORE_COUNT_FIELD: &str = "commercial_store_count";
/// The mainland, Jeju, Ulleung and Dokdo, with margin (degrees): a coordinate outside it, or not a
/// number, is the source's error and not a candidate.
pub const KOREA_LATITUDE: (f64, f64) = (32.5, 39.0); // public-repository-safety: reviewed-runtime-coordinate
/// See [`KOREA_LATITUDE`].
pub const KOREA_LONGITUDE: (f64, f64) = (124.0, 132.0); // public-repository-safety: reviewed-runtime-coordinate
/// The coarse and fine candidate cells, in degrees (`nearest.sql`); the fine ones also bucket the
/// parcels a partial run looks up near a moved store.
const COARSE_DEGREES: f64 = 0.05;
const FINE_DEGREES: f64 = 0.005;

/// What the rules are; part of the build's code version, with the SQL texts.
pub const RULES_VERSION: &str = "parcel-nearby-duckdb-v2";

/// The spool, and a 시도's table, of the served parcels at their centroids.
const PARCELS: &str = "parcels";
/// The served parcels of an edition at their boundaries' centroids: the [`PARCELS`] spool's rows.
const PARCEL_POINTS_SQL: &str = "SELECT pnu, ST_X(g) AS lon, ST_Y(g) AS lat \
    FROM (SELECT pnu, ST_Centroid(ST_GeomFromWKB(geometry_wkb)) AS g FROM {parcels} \
          WHERE source_snapshot_id = '{edition}' AND ({predicate}) AND geometry_wkb IS NOT NULL)";
/// The stores within [`STORE_H3_RING`] rings of each cell, `(cell, store_count)`, of a read.
const STORE_RINGS: &str = "nearby_store_rings";
/// The located stores of a read, `(stores)`.
const LOCATED_STORES: &str = "nearby_located_stores";
/// The cells whose ring count differs between the two reads of a partial run.
const MOVED_CELLS: &str = "nearby_store_cells_moved";
/// The fine buckets ([`FINE_DEGREES`]) those cells cover, with a bucket of margin.
const MOVED_BUCKETS: &str = "nearby_store_buckets_moved";
/// Each built parcel's stores within the rings of its cell: the cells (counted in the publisher)
/// joined to the ring counts.
const RING_SQL: &str = "CREATE OR REPLACE TABLE ring AS \
    SELECT c.pnu, r.store_count FROM nearby_parcel_cells c JOIN nearby_store_rings r USING (cell);";
/// The parcels a partial run rebuilds near a cell whose store count moved: every parcel of a fine
/// bucket over such a cell (a parcel named for nothing builds the document it had).
const NEAR_MOVED_STORES_SQL: &str = "SELECT p.pnu \
    FROM read_parquet('@dir@/parcels/*/*.parquet', hive_partitioning = false) p \
    JOIN nearby_store_buckets_moved b \
      ON CAST(floor(p.lon / {fine}) AS BIGINT) = b.fx AND CAST(floor(p.lat / {fine}) AS BIGINT) = b.fy";
/// What a store-only commit gathers of each 시도 (root ADR-0596): its changed rows that differ from
/// the head's in more than [`STORE_COUNT_FIELD`], and the points of its changed parcels.
const STORE_ONLY_SETUP_SQL: &str = "\
CREATE OR REPLACE TABLE nearby_store_only_breaks (n BIGINT); \
CREATE OR REPLACE TABLE nearby_changed_points (pnu VARCHAR, lat DOUBLE, lon DOUBLE);";
const STORE_ONLY_GATHER_SQL: &str = "\
INSERT INTO nearby_store_only_breaks \
  SELECT count(*) FROM appended a JOIN current_rows c USING (pnu) WHERE a.change = 'changed' \
  AND (json_extract(a.nearby_json, '$.{field}') IS NOT DISTINCT FROM json_extract(c.nearby_json, '$.{field}') \
       OR json_merge_patch(a.nearby_json, '{\"{field}\": null}')::VARCHAR \
          IS DISTINCT FROM json_merge_patch(c.nearby_json, '{\"{field}\": null}')::VARCHAR \
       OR a.place_id IS DISTINCT FROM c.place_id OR a.attached_via IS DISTINCT FROM c.attached_via); \
INSERT INTO nearby_changed_points \
  SELECT a.pnu, p.lat, p.lon FROM appended a LEFT JOIN parcels p USING (pnu) WHERE a.change = 'changed';";

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
/// change of the parcels, the stores or the place ids moves only the parcels it touches.
#[must_use]
pub fn whole_table_inputs() -> Vec<&'static str> {
    let mut tables: Vec<&'static str> = POINT_FILES.iter().map(|file| file.table).collect();
    tables.extend([UNIVERSITY_SOURCE, HIGHWAY_SOURCE, CODE_CHANGE_SOURCE]);
    tables
}

/// The build's code version: the rules version, the SQL texts and the shared code views and split
/// it runs. A change of any of them recomputes every parcel.
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [
        RULES_VERSION,
        NEAREST_SQL,
        DOCUMENT_SQL,
        PARCEL_POINTS_SQL,
        RING_SQL,
    ]
    .into_iter()
    .chain(SHARED_SQL)
    {
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
                Ok(crate::duckdb_parquet_stage::table_at_in(
                    catalog,
                    table,
                    pins.of(table)?,
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

fn count(connection: &Connection, sql: &str) -> anyhow::Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value)?)
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
/// (root ADR-0564 §4), built once into the temporary table `into`; returns the query that reads it.
///
/// The pinned table is read once: a table that must not tie is refused, on `into`, when two current
/// rows of a key do. Until 2026-11-19 the tie check rendered the rule a second time and the first
/// production run read every such input from R2 twice (46 minutes of a one-hour start, root ADR-0564).
pub(crate) fn current_rows(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
    into: &str,
) -> anyhow::Result<String> {
    let contract = contract(table)?;
    ensure!(
        contract.current_row_predicate.is_none(),
        "{table} selects its current rows by a predicate too; this reader applies only the newest-per-key rule"
    );
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    let pinned = source.of(table)?;
    // A column the table gained after the pinned snapshot reads as NULL there.
    let read = with_contract_columns(connection, contract, &pinned)?;
    // The key rides along for the tie check and leaves after it.
    let extra: Vec<&str> = if latest.keep_ties {
        Vec::new()
    } else {
        latest
            .key
            .iter()
            .copied()
            .filter(|column| !columns.contains(column))
            .collect()
    };
    let projected: Vec<&str> = columns
        .iter()
        .copied()
        .chain(extra.iter().copied())
        .collect();
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {into} AS {}",
        latest.current_rows_sql(&read, &projected, "TRUE")
    ))?;
    if !latest.keep_ties {
        let key = latest.key.join(", ");
        let (rows, keys): (i64, i64) = connection.query_row(
            &format!("SELECT count(*), count(DISTINCT ({key})) FROM {into}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows == keys,
            "{table} ({pinned}) holds {} current rows tied on their version order",
            rows - keys
        );
    }
    for column in extra {
        connection.execute_batch(&format!("ALTER TABLE {into} DROP COLUMN {column}"))?;
    }
    Ok(format!("SELECT * FROM {into}"))
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

/// The H3 cell (resolution [`STORE_H3_RESOLUTION`]) of a point, as Sedona's `ST_H3CellIDs` and the
/// h3 library give it (H3 v4).
///
/// H3 is computed in the publisher with h3o (a Rust implementation of the H3 library): `DuckDB`'s
/// community h3 extension loads only with unsigned extensions allowed, which the release refuses.
fn cell(lat: f64, lon: f64) -> anyhow::Result<h3o::CellIndex> {
    let resolution = h3o::Resolution::try_from(STORE_H3_RESOLUTION)?;
    Ok(h3o::LatLng::new(lat, lon)
        .with_context(|| format!("({lat}, {lon}) is no point"))?
        .to_cell(resolution))
}

/// The store cell ([`STORE_H3_RESOLUTION`]) of a point.
///
/// # Errors
/// Returns an error when the point is not a coordinate.
pub fn store_cell(lat: f64, lon: f64) -> anyhow::Result<u64> {
    Ok(u64::from(cell(lat, lon)?))
}

/// The stores within [`STORE_H3_RING`] rings of each cell, from the stores at `points` (latitude,
/// longitude): a cell's count is every store whose cell is within the rings of it.
///
/// It equals counting, for each parcel, the stores in the rings around its cell (the Spark job's
/// `ST_H3KRing`): the grid distance is symmetric, so a store lies in the rings of a parcel's cell
/// exactly when that cell lies in the rings of the store's.
///
/// # Errors
/// Returns an error when a point is not a coordinate.
pub fn store_rings(points: &[(f64, f64)]) -> anyhow::Result<HashMap<u64, u64>> {
    let mut stores: HashMap<u64, u64> = HashMap::new();
    for (lat, lon) in points {
        *stores.entry(store_cell(*lat, *lon)?).or_insert(0) += 1;
    }
    let mut rings: HashMap<u64, u64> = HashMap::new();
    for (center, held) in &stores {
        for ring in h3o::CellIndex::try_from(*center)?.grid_disk::<Vec<_>>(u32::from(STORE_H3_RING))
        {
            *rings.entry(u64::from(ring)).or_insert(0) += held;
        }
    }
    Ok(rings)
}

/// The fine buckets (`floor(degrees / FINE_DEGREES)` of longitude and latitude) that cover `cells`.
///
/// One bucket of margin goes around each cell's bounds: every parcel whose centroid lies in one of
/// the cells lies in one of them.
///
/// # Errors
/// Returns an error when a cell is not an H3 index.
pub fn buckets_over(cells: &[u64]) -> anyhow::Result<BTreeSet<(i64, i64)>> {
    let bucket = |degrees: f64| -> anyhow::Result<i64> {
        let value = (degrees / FINE_DEGREES).floor();
        ensure!(value.is_finite(), "{degrees} is no coordinate");
        // A coordinate inside Korea is a few tens of thousands of buckets: no truncation.
        #[allow(clippy::cast_possible_truncation)]
        let bucketed = value as i64;
        Ok(bucketed)
    };
    let mut buckets = BTreeSet::new();
    for cell in cells {
        let boundary = h3o::CellIndex::try_from(*cell)?.boundary();
        let (mut lat_min, mut lat_max) = (f64::MAX, f64::MIN);
        let (mut lon_min, mut lon_max) = (f64::MAX, f64::MIN);
        for vertex in boundary.iter() {
            lat_min = lat_min.min(vertex.lat());
            lat_max = lat_max.max(vertex.lat());
            lon_min = lon_min.min(vertex.lng());
            lon_max = lon_max.max(vertex.lng());
        }
        for fx in (bucket(lon_min)? - 1)..=(bucket(lon_max)? + 1) {
            for fy in (bucket(lat_min)? - 1)..=(bucket(lat_max)? + 1) {
                buckets.insert((fx, fy));
            }
        }
    }
    Ok(buckets)
}

/// The (latitude, longitude) rows `query` returns.
fn points(connection: &Connection, query: &str) -> anyhow::Result<Vec<(f64, f64)>> {
    connection
        .prepare(query)?
        .query_map([], |row| Ok((row.get::<_, f64>(0)?, row.get::<_, f64>(1)?)))?
        .collect::<Result<Vec<_>, _>>()
        .context("DuckDB failed to read the points")
}

/// Reads the facility files and the 대학's rows into `f_<key>` (the 대학's placed in
/// [`place_universities`], once the parcels are spooled) and the highway list into `highway`;
/// returns each input's read time.
fn read_facilities(
    connection: &Connection,
    source: &Source<'_>,
) -> anyhow::Result<BTreeMap<String, u64>> {
    let mut read_millis = BTreeMap::new();
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
        let at = Instant::now();
        let rows = current_rows(
            connection,
            source,
            file.table,
            &columns,
            &format!("current_{}", file.key),
        )?;
        read_millis.insert(file.table.to_owned(), elapsed(at));
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE f_{key} AS SELECT * FROM ({rows}) WHERE {inside};",
            key = file.key,
            inside = inside("lat", "lon")
        ))?;
    }
    let (_, university_fields) = UNIVERSITY;
    let mut columns = vec!["row_key".to_owned(), "pnu".to_owned()];
    for (name, header) in university_fields {
        columns.push(format!("{} AS {name}", field(header)?));
    }
    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let at = Instant::now();
    current_rows(
        connection,
        source,
        UNIVERSITY_SOURCE,
        &columns,
        "current_university",
    )?;
    read_millis.insert(UNIVERSITY_SOURCE.to_owned(), elapsed(at));

    let mut highway_columns = vec!["legal_dong_code".to_owned()];
    for (name, header) in HIGHWAY_FIELDS {
        highway_columns.push(format!("{} AS {name}", field(header)?));
    }
    let highway_columns = highway_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let at = Instant::now();
    let rows = current_rows(
        connection,
        source,
        HIGHWAY_SOURCE,
        &highway_columns,
        "current_highway",
    )?;
    read_millis.insert(HIGHWAY_SOURCE.to_owned(), elapsed(at));
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
    Ok(read_millis)
}

/// Places each 대학 at the centroid of its (renumbered) parcel, read from the parcels spooled under
/// `dir_text`, as `f_university`, and refuses a build with no located facility of a kind.
fn place_universities(connection: &Connection, dir_text: &str) -> anyhow::Result<()> {
    let (university_key, university_fields) = UNIVERSITY;
    let picked = university_fields
        .iter()
        .map(|(name, _)| format!("u.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE f_{university_key} AS \
         SELECT u.row_key, p.lat, p.lon, {picked} FROM ( \
           SELECT r.* EXCLUDE (pnu), coalesce(n.dst || substr(r.pnu, 11, 9), r.pnu) AS pnu \
           FROM current_university r LEFT JOIN renumbering n ON substr(r.pnu, 1, 10) = n.src \
           WHERE r.pnu IS NOT NULL \
         ) u JOIN read_parquet('{dir_text}/{PARCELS}/*/*.parquet', hive_partitioning = false) p \
           USING (pnu) WHERE {inside};",
        inside = inside("p.lat", "p.lon")
    ))?;
    for key in POINT_FILES
        .iter()
        .map(|file| file.key)
        .chain([university_key])
    {
        ensure!(
            count(connection, &format!("SELECT count(*) FROM f_{key}"))? > 0,
            "no located {key}: the build would name none"
        );
    }
    Ok(())
}

/// Reads the stores at `source` and leaves their ring counts as `nearby_store_rings<suffix>` and
/// how many were located as `nearby_located_stores<suffix>`; returns the read time.
fn read_stores(connection: &Connection, source: &Source<'_>, suffix: &str) -> anyhow::Result<u64> {
    let (store_lat, store_lon) = STORE_COORDINATES;
    let store_columns = [
        format!("TRY_CAST(trim({}) AS DOUBLE) AS lat", field(store_lat)?),
        format!("TRY_CAST(trim({}) AS DOUBLE) AS lon", field(store_lon)?),
    ];
    let store_columns = store_columns.iter().map(String::as_str).collect::<Vec<_>>();
    let at = Instant::now();
    let rows = current_rows(
        connection,
        source,
        STORE_SOURCE,
        &store_columns,
        &format!("current_stores{suffix}"),
    )?;
    let located = points(
        connection,
        &format!(
            "SELECT lat, lon FROM ({rows}) WHERE {}",
            inside("lat", "lon")
        ),
    )?;
    let read = elapsed(at);
    let rings = store_rings(&located)?;
    let table = format!("{STORE_RINGS}{suffix}");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {table} (cell UBIGINT, store_count BIGINT); \
         CREATE OR REPLACE TABLE {LOCATED_STORES}{suffix} AS SELECT CAST({} AS BIGINT) AS stores;",
        located.len()
    ))?;
    {
        let mut appender = connection.appender(&table)?;
        for (ring_cell, stores) in &rings {
            appender.append_row(duckdb::params![*ring_cell, i64::try_from(*stores)?])?;
        }
        appender.flush()?;
    }
    Ok(read)
}

/// Leaves the cells whose ring count differs between the new read and the base's (`suffix`) as
/// [`MOVED_CELLS`] and the fine buckets over them as [`MOVED_BUCKETS`].
fn moved_store_cells(connection: &Connection, suffix: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {MOVED_CELLS} AS \
         SELECT coalesce(n.cell, o.cell) AS cell FROM {STORE_RINGS} n \
         FULL JOIN {STORE_RINGS}{suffix} o ON n.cell = o.cell \
         WHERE n.store_count IS DISTINCT FROM o.store_count; \
         CREATE OR REPLACE TABLE {MOVED_BUCKETS} (fx BIGINT, fy BIGINT);"
    ))?;
    let cells = connection
        .prepare(&format!("SELECT cell FROM {MOVED_CELLS}"))?
        .query_map([], |row| row.get::<_, u64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut appender = connection.appender(MOVED_BUCKETS)?;
    for (fx, fy) in buckets_over(&cells)? {
        appender.append_row(duckdb::params![fx, fy])?;
    }
    appender.flush()?;
    Ok(())
}

/// How many of the points (latitude, longitude; a null for a parcel the 시도 did not hold) `query`
/// returns lie in none of `cells`.
fn points_outside(
    connection: &Connection,
    cells: &HashSet<u64>,
    query: &str,
) -> anyhow::Result<u64> {
    let mut statement = connection.prepare(query)?;
    let mut rows = statement.query([])?;
    let mut outside = 0;
    while let Some(row) = rows.next()? {
        let point: (Option<f64>, Option<f64>) = (row.get(0)?, row.get(1)?);
        let within = match point {
            (Some(lat), Some(lon)) => cells.contains(&store_cell(lat, lon)?),
            _ => false,
        };
        if !within {
            outside += 1;
        }
    }
    Ok(outside)
}

/// Why a commit that only the stores moved is the stores' doing, or `None` (root ADR-0596).
///
/// It is when no key came or went, the located stores did not fall to none or move by more than `max_loss` of the
/// base's (a broken or emptied store file would otherwise move every count), every changed row
/// differs from the head's in [`STORE_COUNT_FIELD`] and in nothing else, and every changed parcel
/// lies in a cell whose store count moved.
///
/// # Errors
/// Returns an error when the gathered tables cannot be read or a point is not a coordinate.
pub fn store_only(
    connection: &Connection,
    (added, changed, removed): (u64, u64, u64),
    max_loss: f64,
) -> anyhow::Result<Option<String>> {
    // A run that read no base side (every parcel rebuilt) compared no stores.
    let compared: i64 = connection.query_row(
        &format!(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name IN \
             ('{LOCATED_STORES}{BASE_SUFFIX}', '{MOVED_CELLS}')"
        ),
        [],
        |row| row.get(0),
    )?;
    if added != 0 || removed != 0 || compared != 2 {
        return Ok(None);
    }
    let before = count(
        connection,
        &format!("SELECT stores FROM {LOCATED_STORES}{BASE_SUFFIX}"),
    )?;
    let after = count(connection, &format!("SELECT stores FROM {LOCATED_STORES}"))?;
    #[allow(clippy::cast_precision_loss)]
    let moved_share = before.abs_diff(after) as f64 / before.max(1) as f64;
    if before == 0 || after == 0 || moved_share > max_loss {
        return Ok(None);
    }
    if count(
        connection,
        "SELECT CAST(coalesce(sum(n), 0) AS BIGINT) FROM nearby_store_only_breaks",
    )? != 0
    {
        return Ok(None);
    }
    let cells = connection
        .prepare(&format!("SELECT cell FROM {MOVED_CELLS}"))?
        .query_map([], |row| row.get::<_, u64>(0))?
        .collect::<Result<HashSet<_>, _>>()?;
    if points_outside(
        connection,
        &cells,
        "SELECT lat, lon FROM nearby_changed_points",
    )? != 0
    {
        return Ok(None);
    }
    Ok(Some(format!(
        "store-only: {changed} changed keys, each differing from the head only in {STORE_COUNT_FIELD}, \
         every one in one of the {} H3 cells whose stores within {STORE_H3_RING} rings moved in \
         {STORE_SOURCE} ({before} -> {after} located stores)",
        cells.len()
    )))
}

/// The table the build appends to: the Gold's columns but `version_seq` and `row_state`.
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

/// Computes the rows of the loaded parcels (every one, or those `targets` names) into `built`.
///
/// Their documents, place ids and digests, each stamped `source_snapshot_id` (the parcels' pin) and
/// `published_at_utc`. Returns the rows it added.
///
/// # Errors
/// Returns an error when a stamp is not plain text, a point is not a coordinate, or `DuckDB` fails.
pub fn build(
    connection: &Connection,
    all: bool,
    source_snapshot_id: &str,
    published_at_utc: &str,
) -> anyhow::Result<u64> {
    ensure!(
        is_plain(source_snapshot_id) && is_plain(published_at_utc),
        "a stamp holds characters a snapshot id or a timestamp does not use"
    );
    let before = count(connection, "SELECT count(*) FROM built")?;
    let targeted = if all {
        ""
    } else {
        " WHERE pnu IN (SELECT pnu FROM targets)"
    };
    // Tables live in the stage's database file: a temporary table stays in memory and cannot spill.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE sp AS SELECT *, floor(lon / {COARSE_DEGREES}) AS cx, floor(lat / {COARSE_DEGREES}) AS cy, \
         floor(lon / {FINE_DEGREES}) AS fx, floor(lat / {FINE_DEGREES}) AS fy \
         FROM {PARCELS}{targeted};"
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
            .with_context(|| format!("DuckDB failed to find the nearest {kind}"))?;
        joins.push(format!("LEFT JOIN n_{kind} USING (pnu)"));
        document.push(format!("'{kind}': {kind}"));
    }
    document.push(format!("'{STORE_COUNT_FIELD}': coalesce(r.store_count, 0)"));
    document.push("'highway_access_points': h.highway_access_points".to_owned());
    // Each parcel's cell, counted here (h3o), then its stores within the rings of it.
    connection.execute_batch(
        "CREATE OR REPLACE TABLE nearby_parcel_cells (pnu VARCHAR, cell UBIGINT);",
    )?;
    {
        let mut appender = connection.appender("nearby_parcel_cells")?;
        let mut statement = connection.prepare("SELECT pnu, lat, lon FROM sp")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let pnu: String = row.get(0)?;
            appender.append_row(duckdb::params![pnu, store_cell(row.get(1)?, row.get(2)?)?])?;
        }
        appender.flush()?;
    }
    connection.execute_batch(RING_SQL)?;
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
        .with_context(|| "DuckDB failed to assemble the documents")?;
    let after = count(connection, "SELECT count(*) FROM built")?;
    let parcels = count(connection, "SELECT count(*) FROM sp")?;
    // Root ADR-0272: every served parcel has the section.
    ensure!(
        after - before == parcels,
        "{parcels} served parcels but {} documents",
        after - before
    );
    Ok(after - before)
}

/// The nearby lane as the shared driver builds it (`kind_versions_build` `NEARBY_BUILD`).
#[derive(Clone, Copy, Debug)]
pub struct ParcelNearby;

/// The one nearby lane.
pub const NEARBY: ParcelNearby = ParcelNearby;

impl KindVersionsStage for ParcelNearby {
    fn section_column(&self) -> &'static str {
        "nearby_json"
    }

    fn inputs(&self) -> Vec<&'static str> {
        inputs()
    }

    fn whole_table_inputs(&self) -> Vec<&'static str> {
        whole_table_inputs()
    }

    fn code_version(&self) -> String {
        code_version()
    }

    // The parcels are read by the served edition, not as a Silver's current rows, and the
    // facilities are national: no Silver is a row set of it.
    fn row_sets(&self) -> Vec<RowSet> {
        Vec::new()
    }

    fn carries_place_id(&self) -> bool {
        true
    }

    // Small and national: the facilities (thousands of points a kind), the highway list (one row a
    // 시군구) and the stores' ring counts (at most 19 rows a store cell). The base side of a partial
    // run reads only the stores, to find the cells whose count moved.
    fn prepare(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let mut read_millis = BTreeMap::new();
        if suffix.is_empty() {
            define_spheroid(connection)?;
            read_millis.extend(read_facilities(connection, source)?);
        }
        read_millis.insert(
            STORE_SOURCE.to_owned(),
            read_stores(connection, source, suffix)?,
        );
        if !suffix.is_empty() {
            moved_store_cells(connection, suffix)?;
        }
        Ok(read_millis)
    }

    // The served parcels at their centroids, once, split by the shared spool; then the 대학 at
    // theirs.
    fn spool_more(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        edition: &str,
        suffix: &str,
        dir_text: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        ensure!(
            is_plain(edition),
            "the edition {edition:?} is not a plain snapshot id"
        );
        let predicate = contract(PARCEL_SOURCE)?
            .current_row_predicate
            .context("silver.parcel_boundaries names its current rows by a predicate")?;
        let at = Instant::now();
        let name = format!("{PARCELS}{suffix}");
        spool_by_sido(
            connection,
            &fill(
                PARCEL_POINTS_SQL,
                &[
                    ("parcels", &source.of(PARCEL_SOURCE)?),
                    ("edition", edition),
                    ("predicate", predicate),
                ],
            ),
            dir_text,
            &name,
        )?;
        let read = elapsed(at);
        let every =
            format!("read_parquet('{dir_text}/{name}/*/*.parquet', hive_partitioning = false)");
        let (rows, keys): (i64, i64) = connection.query_row(
            &format!("SELECT count(*), count(DISTINCT pnu) FROM {every}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows > 0,
            "edition {edition} holds no served parcel with a boundary"
        );
        ensure!(
            rows == keys,
            "edition {edition} holds {} parcels more than once",
            rows - keys
        );
        if suffix.is_empty() {
            place_universities(connection, dir_text)?;
        }
        Ok(BTreeMap::from([(PARCEL_SOURCE.to_owned(), read)]))
    }

    fn load_more(
        &self,
        connection: &Connection,
        read: &dyn Fn(&str) -> Option<String>,
        suffix: &str,
    ) -> anyhow::Result<u64> {
        let name = format!("{PARCELS}{suffix}");
        let body = read(&name).map_or_else(
            || {
                "SELECT CAST(NULL AS VARCHAR) AS pnu, CAST(NULL AS DOUBLE) AS lon, \
                 CAST(NULL AS DOUBLE) AS lat WHERE FALSE"
                    .to_owned()
            },
            |held| format!("SELECT pnu, lon, lat FROM {held}"),
        );
        connection.execute_batch(&format!("CREATE OR REPLACE TABLE {name} AS {body};"))?;
        count(connection, &format!("SELECT count(*) FROM {name}"))
    }

    fn pnu_spools(&self) -> Vec<&'static str> {
        vec![PARCELS]
    }

    // The parcels and the place ids are compared by the shared rule; a store's move reaches the
    // parcels near the cells whose count it moved.
    fn changed_keys_sql(&self) -> Option<String> {
        Some(fill(
            NEAR_MOVED_STORES_SQL,
            &[("fine", &FINE_DEGREES.to_string())],
        ))
    }

    fn change_explanation(&self) -> Option<ChangeExplanation> {
        Some(ChangeExplanation {
            moved: vec![STORE_SOURCE],
            setup_sql: STORE_ONLY_SETUP_SQL.to_owned(),
            gather_sql: fill(STORE_ONLY_GATHER_SQL, &[("field", STORE_COUNT_FIELD)]),
            explain: store_only,
        })
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
        build(connection, all, source_snapshot_id, published_at_utc).map(|_| ())
    }
}

#[cfg(test)]
mod tests;
