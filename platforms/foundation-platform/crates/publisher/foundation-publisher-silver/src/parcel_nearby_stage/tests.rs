//! The nearby rules on synthetic inputs (PNUs under `99999`, longitudes in the repository's reserved
//! synthetic range, latitudes spread so the candidate cells differ), read as the shared driver reads
//! them. The national judgement against the Spark job is the module's doc; these hold the rules a
//! verdict cannot see on real data: the tie rule, a nearer facility in another cell, a coordinate
//! outside Korea, the store rings, the highway list under a renumbered code, the 대학 at its parcel,
//! the digest, a partial run near a moved store and the store-only explanation.

use duckdb::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::*;
use crate::kind_versions_stage::{load_region, partial_for_test, spool};

const EDITION: &str = "synthetic__parcel-209906";
const FIRST: &str = "9999910100100010000";

/// A connection with the spatial extension, as the release loads it by path.
fn connection() -> anyhow::Result<Connection> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("INSTALL spatial; LOAD spatial;")?;
    Ok(connection)
}

/// A square parcel of side `half * 2` degrees around (`lon`, `lat`), as WKB.
fn parcel(pnu: &str, lon: f64, lat: f64) -> String {
    let half = 0.00005;
    format!(
        "('{pnu}', ST_AsWKB(ST_GeomFromText('POLYGON(({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))')), '{EDITION}', NULL)",
        w = lon - half,
        e = lon + half,
        s = lat - half,
        n = lat + half
    )
}

fn point(row_key: &str, fields: &str, ingested: &str) -> String {
    format!("('{row_key}', '{fields}', TIMESTAMP '{ingested}', NULL)")
}

#[allow(clippy::too_many_lines)]
fn load(connection: &Connection) -> anyhow::Result<()> {
    let lon = 127.1235;
    // 시도 99, 시군구 99999, two 동 (10100 served; 10200 renumbered into 10300 by the code table).
    let parcels = [
        parcel(FIRST, lon, 37.0),
        parcel("9999910100100020000", lon, 37.3),
        parcel("9999910300100010000", lon, 38.0),
    ];
    connection.execute_batch(&format!(
        "CREATE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
         INSERT INTO silver_parcel_boundaries VALUES {}, \
           ('9999910100100030000', ST_AsWKB(ST_GeomFromText('POINT(127.1235 37.1)')), 'synthetic__parcel-209903', NULL), \
           ('9999910100100040000', ST_AsWKB(ST_GeomFromText('POINT(127.1235 37.2)')), '{EDITION}', TIMESTAMP '2099-01-01');",
        parcels.join(", ")
    ))?;
    let file = |table: &str, rows: &[String]| -> anyhow::Result<()> {
        connection.execute_batch(&format!(
            "CREATE TABLE {table} (row_key VARCHAR, provider_fields_json VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); \
             INSERT INTO {table} VALUES {};",
            rows.join(", ")
        ))?;
        Ok(())
    };
    // Two airports at one point: the tie goes to the smaller row_key. An older version of `a` stood
    // elsewhere: only the current one counts.
    file(
        "silver_airport",
        &[
            point(
                "b",
                r#"{"위도(WGS84좌표)": "37.01", "경도(WGS84좌표)": "127.1235", "공항명": "B"}"#,
                "2099-01-02",
            ),
            point(
                "a",
                r#"{"위도(WGS84좌표)": "37.01", "경도(WGS84좌표)": "127.1235", "공항명": "A"}"#,
                "2099-01-02",
            ),
            point(
                "a",
                r#"{"위도(WGS84좌표)": "36.5", "경도(WGS84좌표)": "127.1235", "공항명": "A-old"}"#,
                "2099-01-01",
            ),
        ],
    )?;
    // One station per parcel's latitude; one outside Korea, which is no candidate.
    file(
        "silver_railway_station",
        &[
            point(
                "r1",
                r#"{"위도좌표": "37.02", "경도좌표": "127.1235", "역이름": "R1"}"#,
                "2099-01-01",
            ),
            point(
                "r2",
                r#"{"위도좌표": "38.02", "경도좌표": "127.1235", "역이름": "R2"}"#,
                "2099-01-01",
            ),
            point(
                "r3",
                r#"{"위도좌표": "40.0", "경도좌표": "127.1235", "역이름": "R3"}"#,
                "2099-01-01",
            ),
            point(
                "r4",
                r#"{"위도좌표": "x", "경도좌표": "127.1235", "역이름": "R4"}"#,
                "2099-01-01",
            ),
        ],
    )?;
    file(
        "silver_subway_station",
        &[point(
            "s1",
            r#"{"LATITUDE": "37.29", "LONGITUDE": "127.1235", "STATN_NM": "S1", "ROUTE_NM": "L1"}"#,
            "2099-01-01",
        )],
    )?;
    file(
        "silver_school_location",
        &[
            point(
                "k1",
                r#"{"LATITUDE": "36.9", "LONGITUDE": "127.1235", "SCHOOL_NM": "K1", "SCHOOL_SE": "초등학교"}"#,
                "2099-01-01",
            ),
            point(
                "k2",
                r#"{"LATITUDE": "37.9", "LONGITUDE": "127.1235", "SCHOOL_NM": "K2"}"#,
                "2099-01-01",
            ),
        ],
    )?;
    connection.execute_batch(
        "CREATE TABLE silver_university (row_key VARCHAR, provider_fields_json VARCHAR, pnu VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); \
         INSERT INTO silver_university VALUES \
           ('u1', '{\"SCHL_NM\": \"U1\", \"SCHL_SE_NM\": \"대학교\"}', '9999910200100010000', TIMESTAMP '2099-01-01', NULL), \
           ('u2', '{\"SCHL_NM\": \"U2\"}', NULL, TIMESTAMP '2099-01-01', NULL);",
    )?;
    // Three stores in the first parcel's cell, one far away.
    file(
        "silver_commercial_district",
        &[
            point(
                "c1",
                r#"{"위도": "37.0", "경도": "127.1235"}"#,
                "2099-01-01",
            ),
            point(
                "c2",
                r#"{"위도": "37.0", "경도": "127.1235"}"#,
                "2099-01-01",
            ),
            point(
                "c3",
                r#"{"위도": " 37.0 ", "경도": "127.1235"}"#,
                "2099-01-01",
            ),
            point(
                "c4",
                r#"{"위도": "38.5", "경도": "127.1235"}"#,
                "2099-01-01",
            ),
        ],
    )?;
    connection.execute_batch(
        "CREATE TABLE silver_highway_access_point (row_key VARCHAR, provider_fields_json VARCHAR, legal_dong_code VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); \
         INSERT INTO silver_highway_access_point VALUES \
           ('h1', '{\"노선명\": \"N1\", \"출입시설\": \"IC2\"}', '9999910100', TIMESTAMP '2099-01-01', NULL), \
           ('h2', '{\"노선명\": \"N1\", \"출입시설\": \"IC1\"}', '9999910200', TIMESTAMP '2099-01-01', NULL), \
           ('h3', '{\"노선명\": \"N1\", \"출입시설\": \"IC1\"}', '9999910100', TIMESTAMP '2099-01-01', NULL), \
           ('h4', '{\"노선명\": \"N2\", \"출입시설\": \"IC3\"}', '9999820100', TIMESTAMP '2099-01-01', NULL); \
         CREATE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         INSERT INTO reference_legal_dong_code_change VALUES ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x'); \
         CREATE TABLE gold_place_id_bridge (unit VARCHAR, scope VARCHAR, place_id VARCHAR, code VARCHAR, valid_from VARCHAR, valid_to VARCHAR); \
         INSERT INTO gold_place_id_bridge VALUES \
           ('parcel', '99', 'p-old', '9999910100100010000', '2099-01-01', '2099-02-01'), \
           ('parcel', '99', 'p-open', '9999910100100010000', '2099-02-01', NULL), \
           ('parcel', '99', 'p-pair', '9999910100100020000', 'legal-dong-code-change', NULL);",
    )?;
    Ok(())
}

/// One built row: the PNU, its parsed section, its place id and its digest.
type Document = (String, Value, Option<String>, String);

fn documents(connection: &Connection) -> anyhow::Result<Vec<Document>> {
    connection
        .prepare("SELECT pnu, nearby_json, place_id, row_digest FROM built ORDER BY pnu")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .map(|row| {
            let (pnu, json, place_id, digest) = row?;
            Ok((pnu, serde_json::from_str(&json)?, place_id, digest))
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

fn held(connection: &Connection, sql: &str) -> anyhow::Result<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

#[test]
fn every_served_parcel_gets_its_nearest_facilities_stores_and_highways() -> anyhow::Result<()> {
    let connection = connection()?;
    load(&connection)?;
    let dir = tempfile::tempdir()?;
    let spooled = spool(
        &NEARBY,
        &connection,
        &Source::Local,
        EDITION,
        "",
        dir.path(),
    )?;
    // The other edition's parcel and the closed one are not served.
    assert_eq!(spooled.served_parcels, 3);
    assert_eq!(spooled.sidos, BTreeSet::from(["99".to_owned()]));
    assert_eq!(held(&connection, "SELECT count(*) FROM renumbering")?, 1);
    // Every pinned input is read once and timed (the first production run read each tie-checked
    // input twice and spent 46 of its 60 minutes reading).
    for input in inputs() {
        assert!(
            spooled.read_millis.contains_key(input),
            "{input} is not timed"
        );
    }
    assert_eq!(
        held(&connection, "SELECT count(*) FROM f_railway_station")?,
        2
    );
    assert_eq!(held(&connection, "SELECT count(*) FROM f_university")?, 1);
    assert_eq!(load_region(&NEARBY, &connection, dir.path(), "", "99")?, 3);
    assert_eq!(build(&connection, true, "123", "2099-01-01T00:00:00Z")?, 3);
    let rows = documents(&connection)?;
    let (pnu, first, place_id, digest) = &rows[0];
    assert_eq!(pnu, FIRST);
    // The tie between two airports at one point goes to the smaller row_key.
    assert_eq!(first["airport"]["name"], "A");
    assert_eq!(first["railway_station"]["name"], "R1");
    assert_eq!(first["school"]["name"], "K1");
    // The school 0.1° south beats the one 0.9° north, whatever cell each is in.
    assert!(first["school"]["distance_m"]
        .as_i64()
        .is_some_and(|m| (10_000..12_000).contains(&m)));
    // The 대학's parcel was renumbered 10200 -> 10300: it stands at the third parcel.
    assert_eq!(first["university"]["name"], "U1");
    assert_eq!(first[STORE_COUNT_FIELD], 3);
    // 10200's access point is read under 10300, the same 시군구; the list is distinct and sorted.
    assert_eq!(first["highway_access_points"]["total"], 2);
    assert_eq!(
        first["highway_access_points"]["records"],
        serde_json::json!([
            {"route_name": "N1", "facility_name": "IC1"},
            {"route_name": "N1", "facility_name": "IC2"}
        ])
    );
    assert_eq!(place_id.as_deref(), Some("p-open"));
    // The section's keys in the Gold's order, a missing field written null.
    let text: String = connection.query_row(
        &format!("SELECT nearby_json FROM built WHERE pnu = '{FIRST}'"),
        [],
        |row| row.get(0),
    )?;
    let keys = [
        "\"airport\":",
        "\"railway_station\":",
        "\"subway_station\":",
        "\"school\":",
        "\"university\":",
        "\"commercial_store_count\":",
        "\"highway_access_points\":",
    ];
    let positions = keys
        .iter()
        .map(|key| text.find(key))
        .collect::<Option<Vec<_>>>()
        .context("a section key is missing")?;
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{text}");
    let third = &rows[2].1;
    assert_eq!(third["school"]["school_level"], Value::Null);
    assert_eq!(third[STORE_COUNT_FIELD], 0);
    // The digest is Spark's: `to_json(struct(pnu, nearby_json, attached_via, place_id))` in that
    // order, a null field left out.
    let spark_digest = |pnu: &str, text: &str, place_id: Option<&str>| -> anyhow::Result<String> {
        let place = place_id
            .map(|place_id| serde_json::to_string(place_id).map(|id| format!(",\"place_id\":{id}")))
            .transpose()?
            .unwrap_or_default();
        let json = format!(
            "{{\"pnu\":{},\"nearby_json\":{}{place}}}",
            serde_json::to_string(pnu)?,
            serde_json::to_string(text)?
        );
        Ok(format!("{:x}", Sha256::digest(json.as_bytes())))
    };
    assert_eq!(digest, &spark_digest(pnu, &text, Some("p-open"))?);
    let (pnu, _, place_id, digest) = &rows[1];
    assert_eq!(place_id, &None, "a code-pair line is no period of the PNU");
    let text: String = connection.query_row(
        "SELECT nearby_json FROM built WHERE pnu = ?",
        [pnu],
        |row| row.get(0),
    )?;
    assert_eq!(digest, &spark_digest(pnu, &text, None)?);
    Ok(())
}

#[test]
fn a_tie_the_contract_forbids_is_refused() -> anyhow::Result<()> {
    let connection = connection()?;
    load(&connection)?;
    connection.execute_batch(
        "INSERT INTO silver_commercial_district VALUES ('c1', '{\"위도\": \"37.0\", \"경도\": \"127.1235\"}', TIMESTAMP '2099-01-01', NULL);",
    )?;
    let dir = tempfile::tempdir()?;
    let refused = spool(
        &NEARBY,
        &connection,
        &Source::Local,
        EDITION,
        "",
        dir.path(),
    );
    let keeps_ties = contract(STORE_SOURCE)?
        .current_row_latest()
        .is_some_and(|latest| latest.keep_ties);
    assert_eq!(refused.is_err(), !keeps_ties, "{refused:?}");
    Ok(())
}

#[test]
fn the_inputs_and_the_code_version_are_the_builds() {
    assert_eq!(inputs().len(), 10);
    assert!(whole_table_inputs()
        .iter()
        .all(|table| inputs().contains(table)));
    assert!(!whole_table_inputs().contains(&PARCEL_SOURCE));
    assert!(!whole_table_inputs().contains(&STORE_SOURCE));
    assert_eq!(code_version().len(), 64);
    assert!(is_sido("11") && !is_sido("1") && !is_sido("1a"));
}

/// Every field path of a JSON value under `at`, in the records' grammar (`a.b`, `a[].b`).
fn json_paths(value: &Value, at: &str, paths: &mut std::collections::BTreeSet<String>) {
    match value {
        Value::Object(fields) => {
            for (name, inner) in fields {
                let path = format!("{at}.{name}");
                paths.insert(path.clone());
                json_paths(inner, &path, paths);
            }
        }
        Value::Array(items) => {
            for item in items {
                json_paths(item, &format!("{at}[]"), paths);
            }
        }
        _ => {}
    }
}

/// Every property path a JSON Schema declares under `at`, in the same grammar.
fn schema_paths(schema: &Value, at: &str, paths: &mut std::collections::BTreeSet<String>) {
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, inner) in properties {
            let path = format!("{at}.{name}");
            paths.insert(path.clone());
            schema_paths(inner, &path, paths);
        }
    }
    if let Some(items) = schema.get("items") {
        schema_paths(items, &format!("{at}[]"), paths);
    }
}

#[test]
fn a_document_carries_exactly_the_registry_s_fields() -> anyhow::Result<()> {
    let schema: Value = serde_json::from_str(include_str!(
        "../../../../../contracts/schema-registry/parcel_nearby_by_pnu/1/1.schema.json"
    ))?;
    let mut recorded = std::collections::BTreeSet::new();
    schema_paths(&schema["properties"]["nearby"], "nearby", &mut recorded);
    let connection = connection()?;
    load(&connection)?;
    crate::kind_versions_stage::read_for_test(&NEARBY, &connection, EDITION, "")?;
    // The shared build, a 시군구 at a time, as the driver runs it.
    crate::kind_versions_parts::build_in_parts(
        &NEARBY,
        &connection,
        true,
        "123",
        "2099-01-01T00:00:00Z",
    )?;
    // The first parcel's document names every facility, a store count and a highway list.
    let (_, first, _, _) = documents(&connection)?
        .into_iter()
        .next()
        .context("a document")?;
    let mut written = std::collections::BTreeSet::new();
    json_paths(&first, "nearby", &mut written);
    assert_eq!(written, recorded);
    Ok(())
}

#[test]
fn the_headers_are_the_file_contract_s() -> anyhow::Result<()> {
    let contract: Value = serde_json::from_str(include_str!(
        "../../../../../infra/lakehouse/contracts/data-go-kr-file-data.contract.json"
    ))?;
    let header = |table: &str| -> anyhow::Result<std::collections::BTreeSet<String>> {
        let file = &contract["files"][table.trim_start_matches("silver.")];
        let header = file
            .get("header")
            .or_else(|| file["member"].get("header"))
            .and_then(Value::as_array)
            .with_context(|| format!("no header for {table}"))?;
        Ok(header
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect())
    };
    for file in &POINT_FILES {
        let held = header(file.table)?;
        for name in [file.latitude, file.longitude]
            .into_iter()
            .chain(file.fields.iter().map(|(_, header)| *header))
        {
            assert!(held.contains(name), "{}: {name}", file.table);
        }
    }
    let university = header(UNIVERSITY_SOURCE)?;
    assert!(UNIVERSITY
        .1
        .iter()
        .all(|(_, name)| university.contains(*name)));
    let stores = header(STORE_SOURCE)?;
    assert!(stores.contains(STORE_COORDINATES.0) && stores.contains(STORE_COORDINATES.1));
    let highway = header(HIGHWAY_SOURCE)?;
    assert!(HIGHWAY_FIELDS
        .iter()
        .all(|(_, name)| highway.contains(*name)));
    Ok(())
}

/// A cell's ring count is what counting the stores around each parcel's cell gave (the Spark
/// job's rule): the stores in the 19 cells of its disk.
#[test]
fn a_cell_s_ring_count_is_the_stores_within_its_rings() -> anyhow::Result<()> {
    let resolution = h3o::Resolution::try_from(STORE_H3_RESOLUTION)?;
    let center = h3o::LatLng::new(37.0, 127.1235)?.to_cell(resolution);
    let ring_cell = |k: u32| -> anyhow::Result<h3o::LatLng> {
        Ok(h3o::LatLng::from(
            center
                .grid_ring_fast(k)
                .flatten()
                .next()
                .context("a ring cell")?,
        ))
    };
    let (two, three) = (ring_cell(2)?, ring_cell(3)?);
    // Two stores at the centre, one two rings out, one three rings out.
    let stores = [
        (37.0, 127.1235),
        (37.0, 127.1235),
        (two.lat(), two.lng()),
        (three.lat(), three.lng()),
    ];
    let rings = store_rings(&stores)?;
    assert_eq!(rings.get(&u64::from(center)), Some(&3));
    // Counting it the Spark job's way, around each cell, gives the same for every cell.
    for (cell, count) in &rings {
        let around: usize = h3o::CellIndex::try_from(*cell)?
            .grid_disk::<Vec<_>>(u32::from(STORE_H3_RING))
            .into_iter()
            .map(|near| {
                stores
                    .iter()
                    .filter(|(lat, lon)| {
                        store_cell(*lat, *lon).is_ok_and(|at| at == u64::from(near))
                    })
                    .count()
            })
            .sum();
        assert_eq!(u64::try_from(around)?, *count, "cell {cell}");
    }
    // A cell's buckets hold every point of the cell.
    let buckets = buckets_over(&[u64::from(center)])?;
    #[allow(clippy::cast_possible_truncation)]
    let bucket = |degrees: f64| (degrees / FINE_DEGREES).floor() as i64;
    for vertex in center.boundary().iter() {
        assert!(buckets.contains(&(bucket(vertex.lng()), bucket(vertex.lat()))));
    }
    assert!(buckets.contains(&(bucket(127.1235), bucket(37.0))));
    Ok(())
}

/// A partial run in which only the stores moved rebuilds the parcels near the moved store, and no
/// other.
#[test]
fn a_moved_store_reaches_the_parcels_near_it_and_no_other() -> anyhow::Result<()> {
    let connection = connection()?;
    load(&connection)?;
    let (_, targets) = partial_for_test(
        &NEARBY,
        &connection,
        EDITION,
        &[STORE_SOURCE],
        &|| {
            connection
                .execute_batch("DELETE FROM silver_commercial_district WHERE row_key = 'c5';")?;
            Ok(())
        },
        &|| {
            connection.execute_batch(
                "INSERT INTO silver_commercial_district VALUES \
                 ('c5', '{\"위도\": \"37.0\", \"경도\": \"127.1235\"}', TIMESTAMP '2099-01-03', NULL);",
            )?;
            Ok(())
        },
    )?;
    assert_eq!(targets, 1);
    let named: String = connection.query_row("SELECT pnu FROM targets", [], |row| row.get(0))?;
    assert_eq!(named, FIRST);
    // The partial build builds the named parcel only, with the new count.
    assert_eq!(build(&connection, false, "123", "2099-01-01T00:00:00Z")?, 1);
    let (pnu, document, _, _) = documents(&connection)?
        .into_iter()
        .next()
        .context("a document")?;
    assert_eq!(
        (pnu.as_str(), &document[STORE_COUNT_FIELD]),
        (FIRST, &Value::from(4))
    );
    Ok(())
}

const HEAD: &str = r#"{"airport":{"name":"A","distance_m":9000},"commercial_store_count":3}"#;
const COUNT_ONLY: &str = r#"{"airport":{"name":"A","distance_m":9000},"commercial_store_count":5}"#;
const LOSS: f64 = 0.1;
/// The parcel the store-only cases change, and where it lies.
const CHANGED: (&str, f64, f64) = ("9999900000100000000", 37.5, 127.1235);

/// What a run gathered of a store-only commit of one changed parcel: the head's row and the built
/// row as the diff leaves them, the located stores `before` -> `after` and the cells whose count
/// moved, gathered as the driver gathers them.
fn gathered(
    built: &str,
    place_id: &str,
    attached_via: &str,
    (before, after): (u64, u64),
    moved_cell: (f64, f64),
) -> anyhow::Result<Connection> {
    let connection = Connection::open_in_memory()?;
    let (pnu, lat, lon) = CHANGED;
    connection.execute_batch(&format!(
        "CREATE TABLE parcels AS SELECT '{pnu}' AS pnu, {lon}::DOUBLE AS lon, {lat}::DOUBLE AS lat; \
         CREATE TABLE current_rows AS SELECT '{pnu}' AS pnu, '{HEAD}' AS nearby_json, \
           NULL::VARCHAR AS attached_via, 'P1' AS place_id; \
         CREATE TABLE appended AS SELECT '{pnu}' AS pnu, '{built}' AS nearby_json, \
           {attached_via} AS attached_via, '{place_id}' AS place_id, 'changed' AS change; \
         CREATE TABLE {LOCATED_STORES}{BASE_SUFFIX} AS SELECT CAST({before} AS BIGINT) AS stores; \
         CREATE TABLE {LOCATED_STORES} AS SELECT CAST({after} AS BIGINT) AS stores; \
         CREATE TABLE {MOVED_CELLS} AS SELECT CAST({} AS UBIGINT) AS cell;",
        store_cell(moved_cell.0, moved_cell.1)?
    ))?;
    let explanation = NEARBY.change_explanation().context("an explanation")?;
    connection.execute_batch(&explanation.setup_sql)?;
    connection.execute_batch(&explanation.gather_sql)?;
    Ok(connection)
}

#[test]
fn a_change_of_the_store_count_alone_in_the_moved_cells_is_explained() -> anyhow::Result<()> {
    // Root ADR-0596: a quarter's 상가 file moves the count around most parcels (11-19 Gold: 80.38%
    // of parcels count a store), past the bound, and nothing else.
    let here = (CHANGED.1, CHANGED.2);
    let connection = gathered(COUNT_ONLY, "P1", "NULL", (100, 95), here)?;
    let why = store_only(&connection, (0, 1, 0), LOSS)?.context("explained")?;
    assert!(why.starts_with("store-only: 1 changed keys"), "{why}");
    assert!(why.contains("(100 -> 95 located stores)"), "{why}");
    assert!(
        !why.contains("  "),
        "no run of spaces enters the recorded text: {why}"
    );
    Ok(())
}

#[test]
fn anything_but_the_store_count_moving_is_not_store_only() -> anyhow::Result<()> {
    let here = (CHANGED.1, CHANGED.2);
    let distance = r#"{"airport":{"name":"A","distance_m":8000},"commercial_store_count":5}"#;
    for (case, built, place_id, attached_via) in [
        ("a distance moved", distance, "P1", "NULL"),
        ("the place id moved", COUNT_ONLY, "P2", "NULL"),
        ("attached_via moved", COUNT_ONLY, "P1", "'lineage'"),
        ("the count stayed (a digest-only drift)", HEAD, "P1", "NULL"),
    ] {
        let connection = gathered(built, place_id, attached_via, (100, 95), here)?;
        assert_eq!(store_only(&connection, (0, 1, 0), LOSS)?, None, "{case}");
    }
    let connection = gathered(COUNT_ONLY, "P1", "NULL", (100, 95), here)?;
    assert_eq!(
        store_only(&connection, (0, 1, 1), LOSS)?,
        None,
        "a key left"
    );
    assert_eq!(
        store_only(&connection, (1, 1, 0), LOSS)?,
        None,
        "a key came"
    );
    let elsewhere = gathered(COUNT_ONLY, "P1", "NULL", (100, 95), (38.0, 127.1235))?;
    assert_eq!(
        store_only(&elsewhere, (0, 1, 0), LOSS)?,
        None,
        "outside the cells"
    );
    Ok(())
}

#[test]
fn an_emptied_or_halved_store_file_is_not_store_only() -> anyhow::Result<()> {
    // A file whose coordinates do not parse, or half loaded, moves every count near a store.
    let here = (CHANGED.1, CHANGED.2);
    for (before, after) in [(2, 0), (0, 2), (100, 50), (100, 150)] {
        let connection = gathered(COUNT_ONLY, "P1", "NULL", (before, after), here)?;
        assert_eq!(
            store_only(&connection, (0, 1, 0), LOSS)?,
            None,
            "{before} -> {after}"
        );
    }
    // A run that read no base side compared no stores.
    let connection = gathered(COUNT_ONLY, "P1", "NULL", (100, 95), here)?;
    connection.execute_batch(&format!("DROP TABLE {LOCATED_STORES}{BASE_SUFFIX};"))?;
    assert_eq!(store_only(&connection, (0, 1, 0), LOSS)?, None);
    Ok(())
}
