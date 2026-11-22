//! The ownership kind's rules on synthetic inputs (PNUs under `99999`): the newest release's rows
//! with their ties, owners sorted on their fields with a null first, the page bound, unit owners
//! counted per 소유구분명, a renumbered code on the served parcel, an unserved PNU and a removed one
//! left out, the bytes Spark writes, and the partial diff. The judgement against the Spark job is its
//! takeover verdict.

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::*;
use crate::kind_versions_stage::KindVersionsStage;

const EDITION: &str = "synthetic__parcel-209906";
const FACTORY: &str = "9999910100100010000";
const COMPLEX: &str = "9999910100100020000";
const MANY: &str = "9999910100100030000";
const REMOVED: &str = "9999910100100040000";
const UNSERVED: &str = "9999910100100050000";
const MOVED: &str = "9999910300100010000";
const OLD_CODE: &str = "9999910200100010000";

/// One owner row: (pnu, 집합건물일련번호, 공유인일련번호, 소유구분코드, 소유구분명, 변동일자, 공유인수,
/// release, ingested, row_state).
struct Row {
    pnu: &'static str,
    serial: Option<&'static str>,
    co_owner: String,
    kind_code: Option<&'static str>,
    kind_name: Option<&'static str>,
    changed_on: &'static str,
    count: i32,
    release: &'static str,
    ingested: &'static str,
    state: Option<&'static str>,
}

fn text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| format!("'{value}'"))
}

impl Row {
    fn values(&self) -> String {
        format!(
            "('{pnu}', {serial}, '202610', {code}, {name}, '1', '거주', NULL, NULL, '03', '매매', \
             '{changed}', {count}, '20261001', '{co_owner}', 'r-{co_owner}', '{release}', \
             TIMESTAMP '{ingested}', {state})",
            pnu = self.pnu,
            serial = text(self.serial),
            code = text(self.kind_code),
            name = text(self.kind_name),
            changed = self.changed_on,
            count = self.count,
            co_owner = self.co_owner,
            release = self.release,
            ingested = self.ingested,
            state = text(self.state),
        )
    }
}

fn row(pnu: &'static str, serial: Option<&'static str>, co_owner: &str) -> Row {
    Row {
        pnu,
        serial,
        co_owner: co_owner.to_owned(),
        kind_code: Some("01"),
        kind_name: Some("개인"),
        changed_on: "20200101",
        count: 1,
        release: "release-209902",
        ingested: "2099-02-01",
        state: None,
    }
}

fn load(connection: &Connection, rows: &[Row]) -> anyhow::Result<()> {
    let served = [FACTORY, COMPLEX, MANY, REMOVED, MOVED]
        .map(|pnu| format!("('{pnu}', NULL, '{EDITION}', NULL)"))
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, \
           source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
         INSERT INTO silver_parcel_boundaries VALUES {served}, \
           ('{UNSERVED}', NULL, 'synthetic__parcel-209903', NULL); \
         CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         INSERT INTO reference_legal_dong_code_change VALUES \
           ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x'); \
         CREATE OR REPLACE TABLE silver_land_ownership (pnu VARCHAR, collective_building_serial_no VARCHAR, \
           base_year_month VARCHAR, ownership_kind_code VARCHAR, ownership_kind_name VARCHAR, \
           residence_kind_code VARCHAR, residence_kind_name VARCHAR, state_agency_kind_code VARCHAR, \
           state_agency_kind_name VARCHAR, ownership_change_reason_code VARCHAR, \
           ownership_change_reason VARCHAR, ownership_changed_on VARCHAR, co_owner_count INTEGER, \
           data_reference_date VARCHAR, co_owner_serial_no VARCHAR, source_record_id VARCHAR, \
           source_snapshot_id VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); \
         INSERT INTO silver_land_ownership VALUES {};",
        rows.iter().map(Row::values).collect::<Vec<_>>().join(", ")
    ))?;
    Ok(())
}

fn rows() -> Vec<Row> {
    let mut rows = vec![
        // An older release of the factory: only the newest release counts.
        Row {
            release: "release-209901",
            ingested: "2099-01-01",
            ..row(FACTORY, Some("0000"), "000009")
        },
        Row {
            changed_on: "20200101",
            count: 2,
            ..row(FACTORY, Some("0000"), "000002")
        },
        Row {
            changed_on: "20190101",
            count: 2,
            ..row(FACTORY, None, "000001")
        },
        // The same 공유인 without a 소유구분코드: a null sorts first.
        Row {
            kind_code: None,
            changed_on: "20210101",
            ..row(FACTORY, Some(""), "000001")
        },
        // Unit owners of a collective building: counted per 소유구분명, most first, a null name first
        // among equals.
        Row {
            kind_name: Some("법인"),
            ..row(COMPLEX, Some("0001"), "000001")
        },
        row(COMPLEX, Some("0002"), "000001"),
        row(COMPLEX, Some("0003"), "000001"),
        Row {
            kind_name: None,
            ..row(COMPLEX, Some("0004"), "000001")
        },
        // Removed in the newest release: no document.
        Row {
            release: "release-209901",
            ingested: "2099-01-01",
            ..row(REMOVED, None, "000001")
        },
        Row {
            state: Some("removed"),
            ..row(REMOVED, None, "000001")
        },
        // Under the code a reorganization renumbered into the served 10300.
        row(OLD_CODE, None, "000001"),
        // A PNU the served edition does not hold.
        row(UNSERVED, None, "000001"),
    ];
    for serial in 1..=OWNER_PAGE_BOUND + 5 {
        rows.push(row(MANY, None, &format!("{serial:06}")));
    }
    rows
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, String, i64, String)>> {
    Ok(connection
        .prepare("SELECT pnu, ownership_json, owner_total, row_digest FROM built ORDER BY pnu")?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

/// An owner as Spark's `to_json` writes it (every field, a null as `null`).
fn owner(co_owner: &str, kind_code: Option<&str>, changed_on: &str, count: i32) -> String {
    let code = kind_code.map_or_else(|| "null".to_owned(), |code| format!("\"{code}\""));
    format!(
        "{{\"co_owner_serial_no\":\"{co_owner}\",\"ownership_kind_code\":{code},\
         \"ownership_kind_name\":\"개인\",\"residence_kind_code\":\"1\",\"residence_kind_name\":\"거주\",\
         \"state_agency_kind_code\":null,\"state_agency_kind_name\":null,\
         \"ownership_change_reason_code\":\"03\",\"ownership_change_reason\":\"매매\",\
         \"ownership_changed_on\":\"{changed_on}\",\"co_owner_count\":{count}}}"
    )
}

fn document(owners: &[String], unit_total: i64, kinds: &str, latest: &str) -> String {
    format!(
        "{{\"owners\":[{}],\"unit_owner_total\":{unit_total},\"unit_owner_kinds\":[{kinds}],\
         \"latest_changed_on\":\"{latest}\",\"base_year_month\":\"202610\",\
         \"data_reference_date\":\"20261001\"}}",
        owners.join(",")
    )
}

fn digest(pnu: &str, document: &str, total: i64) -> anyhow::Result<String> {
    // Spark's row_digest: to_json of the content columns, the null attached_via left out.
    let content = format!(
        "{{\"pnu\":\"{pnu}\",\"ownership_json\":{},\"owner_total\":{total}}}",
        serde_json::to_string(document)?
    );
    Ok(format!("{:x}", Sha256::digest(content.as_bytes())))
}

fn build_all(connection: &Connection) -> anyhow::Result<Vec<(String, String, i64, String)>> {
    OWNERSHIP.build(
        connection,
        true,
        "999990000000000001",
        "2099-03-01T00:00:00Z",
    )?;
    crate::kind_versions_stage::ensure_no_commit_tables(connection)?;
    built(connection)
}

#[test]
fn a_document_is_spark_s_bytes_with_owners_sorted_null_first() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &rows())?;
    let read = crate::kind_versions_stage::read_for_test(&OWNERSHIP, &connection, EDITION, "")?;
    // The factory's three newest rows, four unit rows, the bounded parcel's 105 and the moved one.
    assert_eq!(
        (read.served_parcels, read.rows),
        (5, 3 + 4 + u64::from(OWNER_PAGE_BOUND) + 5 + 1)
    );
    let rows = build_all(&connection)?;
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    // The removed and the unserved parcels have none; the old code lands on the served one.
    assert_eq!(pnus, [FACTORY, COMPLEX, MANY, MOVED]);

    let (_, factory, total, row_digest) = &rows[0];
    let expected = document(
        &[
            owner("000001", None, "20210101", 1),
            owner("000001", Some("01"), "20190101", 2),
            owner("000002", Some("01"), "20200101", 2),
        ],
        0,
        "",
        "20210101",
    );
    assert_eq!(factory, &expected);
    assert_eq!(*total, 3);
    assert_eq!(row_digest, &digest(FACTORY, &expected, 3)?);
    Ok(())
}

#[test]
fn unit_owners_are_counted_most_first_and_a_null_name_first_among_equals() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &rows())?;
    crate::kind_versions_stage::read_for_test(&OWNERSHIP, &connection, EDITION, "")?;
    let rows = build_all(&connection)?;
    let (_, complex, total, _) = &rows[1];
    let expected = document(
        &[],
        4,
        "{\"ownership_kind_name\":\"개인\",\"count\":2},\
         {\"ownership_kind_name\":null,\"count\":1},\
         {\"ownership_kind_name\":\"법인\",\"count\":1}",
        "20200101",
    );
    assert_eq!(complex, &expected);
    assert_eq!(*total, 0);
    Ok(())
}

#[test]
fn a_parcel_past_the_bound_keeps_the_first_owners_and_counts_all() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &rows())?;
    crate::kind_versions_stage::read_for_test(&OWNERSHIP, &connection, EDITION, "")?;
    let rows = build_all(&connection)?;
    let (_, many, total, _) = &rows[2];
    let owners: Vec<String> = (1..=OWNER_PAGE_BOUND)
        .map(|serial| owner(&format!("{serial:06}"), Some("01"), "20200101", 1))
        .collect();
    assert_eq!(many, &document(&owners, 0, "", "20200101"));
    assert_eq!(*total, i64::from(OWNER_PAGE_BOUND) + 5);
    Ok(())
}

#[test]
fn a_partial_run_names_only_the_parcels_whose_rows_or_service_changed() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &rows())?;
    crate::kind_versions_stage::read_for_test(&OWNERSHIP, &connection, EDITION, "_old")?;
    // A new release changes one factory owner's 변동일자; nothing else moves.
    let mut newer = rows();
    newer.push(Row {
        release: "release-209903",
        ingested: "2099-03-01",
        changed_on: "20990101",
        ..row(FACTORY, None, "000001")
    });
    load(&connection, &newer)?;
    crate::kind_versions_stage::read_for_test(&OWNERSHIP, &connection, EDITION, "")?;
    assert_eq!(OWNERSHIP.changed_pnus(&connection)?, 1);
    OWNERSHIP.build(
        &connection,
        false,
        "999990000000000002",
        "2099-03-02T00:00:00Z",
    )?;
    let rows = built(&connection)?;
    assert_eq!(rows.len(), 1);
    let (pnu, factory, total, _) = &rows[0];
    assert_eq!(pnu, FACTORY);
    // The new release holds the parcel: its one row is now every owner.
    assert_eq!(
        factory,
        &document(
            &[owner("000001", Some("01"), "20990101", 1)],
            0,
            "",
            "20990101"
        )
    );
    assert_eq!(*total, 1);
    Ok(())
}
