use super::*;
use crate::kind_versions_stage::{load_ranged, partial_for_test, read_for_test};

/// The ISO WKB type of a polygon.
const WKB_POLYGON: u32 = 3;

/// A little-endian ISO WKB polygon of one ring.
fn polygon(points: &[(f64, f64)]) -> Vec<u8> {
    let mut wkb = vec![1];
    wkb.extend(WKB_POLYGON.to_le_bytes());
    wkb.extend(1_u32.to_le_bytes());
    wkb.extend(
        u32::try_from(points.len())
            .unwrap_or_default()
            .to_le_bytes(),
    );
    for (x, y) in points {
        wkb.extend(x.to_le_bytes());
        wkb.extend(y.to_le_bytes());
    }
    wkb
}

fn square(x: f64, y: f64, side: f64) -> Vec<u8> {
    polygon(&[
        (x, y),
        (x + side, y),
        (x + side, y + side),
        (x, y + side),
        (x, y),
    ])
}

const SUM: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn edit(seq: i64, pnu: &str, op: Op, wkb: Option<Vec<u8>>) -> LedgerEdit {
    LedgerEdit {
        change_seq: seq,
        feature_id: pnu.to_owned(),
        op,
        geometry_srid: GEOMETRY_SRID,
        geometry_checksum_sha256: wkb.as_ref().map(|_| SUM.to_owned()),
        geometry_wkb: wkb,
    }
}

#[test]
fn the_ledger_is_ordered_and_an_edit_the_table_cannot_hold_is_refused() -> anyhow::Result<()> {
    let ledger = check_ledger(vec![
        edit(2, "9999910100100020000", Op::Delete, None),
        edit(
            1,
            "9999910100100010000",
            Op::Upsert,
            Some(square(0.0, 0.0, 1.0)),
        ),
    ])?;
    assert_eq!(
        ledger.iter().map(|e| e.change_seq).collect::<Vec<_>>(),
        [1, 2]
    );
    let refused = |edits: Vec<LedgerEdit>| check_ledger(edits).is_err();
    assert!(refused(vec![
        edit(1, "9999910100100010000", Op::Delete, None),
        edit(1, "9999910100100020000", Op::Delete, None),
    ]));
    assert!(refused(vec![edit(
        0,
        "9999910100100010000",
        Op::Delete,
        None
    )]));
    assert!(refused(vec![edit(
        1,
        "99999101001000X0000",
        Op::Delete,
        None
    )]));
    assert!(refused(vec![edit(
        1,
        "9999910100100010000",
        Op::Upsert,
        None
    )]));
    let mut other_crs = edit(1, "9999910100100010000", Op::Delete, None);
    other_crs.geometry_srid = 5186;
    assert!(refused(vec![other_crs]));
    let mut no_sum = edit(
        1,
        "9999910100100010000",
        Op::Upsert,
        Some(square(0.0, 0.0, 1.0)),
    );
    no_sum.geometry_checksum_sha256 = Some("ABC".to_owned());
    assert!(refused(vec![no_sum]));
    Ok(())
}

#[test]
fn each_edited_parcel_ends_as_its_last_edit_and_a_delete_of_nothing_is_counted() {
    let ledger = vec![
        edit(
            1,
            "9999910100100010000",
            Op::Upsert,
            Some(square(0.0, 0.0, 1.0)),
        ),
        edit(2, "9999910100100010000", Op::Delete, None),
        edit(3, "9999910100100020000", Op::Delete, None),
        edit(4, "9999910100100030000", Op::Delete, None),
        edit(
            5,
            "9999910100100030000",
            Op::Upsert,
            Some(square(2.0, 2.0, 1.0)),
        ),
    ];
    let present = BTreeSet::from(["9999910100100030000".to_owned()]);
    let (states, counts) = fold_edits(&ledger, &present);
    assert!(states["9999910100100010000"].is_none());
    assert!(states["9999910100100020000"].is_none());
    assert_eq!(states["9999910100100030000"].map(|e| e.change_seq), Some(5));
    assert_eq!(
        counts,
        EditCounts {
            upserts: 2,
            deletes: 3,
            deletes_of_absent_features: 1
        }
    );
}

#[test]
fn a_handoff_line_is_the_one_the_spark_job_wrote() {
    assert_eq!(
        handoff_line("9999910100100010000", &[0x01, 0x03, 0, 0, 0], "edit"),
        r#"{"feature_id":"9999910100100010000","geometry_srid":4326,"geometry_wkb_hex":"0103000000","origin":"edit","properties":{}}"#
    );
}

/// The edition of the fixtures, its parcels under 시도 99, and an edition that is not served.
fn inputs(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(
        "CREATE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB,
           geometry_checksum_sha256 VARCHAR, source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP);
         INSERT INTO silver_parcel_boundaries VALUES
           ('9999910100100010000', '\\x01'::BLOB, 'aa', 'edition-b', NULL),
           ('9999910100100020000', '\\x02'::BLOB, 'bb', 'edition-b', NULL),
           ('9999920200100010000', '\\x03'::BLOB, 'cc', 'edition-b', NULL),
           ('9999920200100020000', '\\x04'::BLOB, 'dd', 'edition-a', NULL);
         CREATE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR,
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR);
         CREATE TABLE silver_map_edit_ledger (unit VARCHAR, change_seq BIGINT, feature_id VARCHAR,
           op VARCHAR, geometry_srid INTEGER, geometry_wkb BLOB, geometry_checksum_sha256 VARCHAR,
           properties_json VARCHAR, editor VARCHAR, edited_at VARCHAR);",
    )?;
    Ok(())
}

/// Ledgers `edit` as the parcels unit's.
fn ledger(connection: &Connection, edit: &LedgerEdit) -> anyhow::Result<()> {
    connection.execute(
        "INSERT INTO silver_map_edit_ledger VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'e', '2099-01-01')",
        duckdb::params![
            UNIT,
            edit.change_seq,
            edit.feature_id,
            if edit.op == Op::Upsert {
                "upsert"
            } else {
                "delete"
            },
            edit.geometry_srid,
            edit.geometry_wkb.as_deref(),
            edit.geometry_checksum_sha256.as_deref(),
        ],
    )?;
    Ok(())
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, String, String)>> {
    Ok(connection
        .prepare("SELECT pnu, origin, source_snapshot_id FROM built ORDER BY pnu")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<_, _>>()?)
}

#[test]
fn the_built_rows_are_the_edition_but_its_edited_parcels_and_the_digest_is_the_sql_s(
) -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    inputs(&connection)?;
    ledger(
        &connection,
        &edit(1, "9999910100100010000", Op::Delete, None),
    )?;
    ledger(
        &connection,
        &edit(
            2,
            "9999910100100030000",
            Op::Upsert,
            Some(square(0.0, 0.0, 1.0)),
        ),
    )?;
    // As the shared driver reads a 시도: the spools, then the edition by the 시도's range.
    read_for_test(&SERVED_BOUNDARIES, &connection, "edition-b", "")?;
    assert_eq!(
        load_ranged(
            &SERVED_BOUNDARIES,
            &connection,
            &Source::Local,
            "edition-b",
            "(pnu >= '99')"
        )?,
        3
    );
    crate::kind_versions_parts::build_in_parts(
        &SERVED_BOUNDARIES,
        &connection,
        true,
        "42",
        "2099-01-01T00:00:00Z",
    )?;
    assert_eq!(
        built(&connection)?,
        [
            (
                "9999910100100020000".into(),
                "source".into(),
                "edition-b".into()
            ),
            (
                "9999910100100030000".into(),
                "edit".into(),
                "map-edit-2".into()
            ),
            (
                "9999920200100010000".into(),
                "source".into(),
                "edition-b".into()
            ),
        ]
    );
    let (digest, through): (String, i64) = connection.query_row(
        "SELECT row_digest, edits_through_change_seq FROM built WHERE pnu = '9999910100100020000'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(digest, row_digest("9999910100100020000", "bb", "source"));
    assert_eq!(through, 2);
    assert!(SERVED_BOUNDARIES
        .build(&connection, true, "1'", "x")
        .is_err());
    Ok(())
}

/// A fold: only the ledger moved, and the run rebuilds the parcel whose final state moved, an
/// upsert of a parcel the edition does not hold among them.
#[test]
fn a_ledger_only_move_rebuilds_the_parcels_whose_final_state_moved() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    inputs(&connection)?;
    ledger(
        &connection,
        &edit(1, "9999910100100010000", Op::Delete, None),
    )?;
    let (_, targets) = partial_for_test(
        &SERVED_BOUNDARIES,
        &connection,
        "edition-b",
        &[LEDGER_SOURCE],
        &|| {
            connection.execute_batch("DELETE FROM silver_map_edit_ledger WHERE change_seq = 2;")?;
            Ok(())
        },
        &|| {
            ledger(
                &connection,
                &edit(
                    2,
                    "9999910100100030000",
                    Op::Upsert,
                    Some(square(0.0, 0.0, 1.0)),
                ),
            )
        },
    )?;
    assert_eq!(targets, 1);
    load_ranged(
        &SERVED_BOUNDARIES,
        &connection,
        &Source::Local,
        "edition-b",
        "(pnu >= '99')",
    )?;
    crate::kind_versions_parts::build_in_parts(
        &SERVED_BOUNDARIES,
        &connection,
        false,
        "42",
        "2099-01-01T00:00:00Z",
    )?;
    assert_eq!(
        built(&connection)?,
        [(
            "9999910100100030000".into(),
            "edit".into(),
            "map-edit-2".into()
        )]
    );
    Ok(())
}

fn handoff_line_of(seq: i64, op: &str, carries: bool) -> String {
    let geometry = if carries {
        format!(
            r#""geometry_geojson":"{{}}","geometry_wkb_hex":"01","geometry_checksum_sha256":"{SUM}""#
        )
    } else {
        r#""geometry_geojson":null,"geometry_wkb_hex":null,"geometry_checksum_sha256":null"#
            .to_owned()
    };
    format!(
        r#"{{"unit":"parcels","change_seq":{seq},"feature_id":"9999910100100010000","op":"{op}",{geometry},"geometry_srid":4326,"properties_json":"{{}}","editor":"e","edited_at":"2099-01-01T00:00:00Z"}}"#
    )
}

#[test]
fn the_edit_handoff_is_read_as_the_ledger_contract_reads_it() -> anyhow::Result<()> {
    let text = format!(
        "{}\n\n{}\n",
        handoff_line_of(2, "delete", false),
        handoff_line_of(1, "upsert", true)
    );
    let rows = read_edit_handoff(&text)?;
    assert_eq!(
        rows.iter().map(|r| r.change_seq).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(rows[0].ledger_edit()?.geometry_wkb, Some(vec![1]));
    let refused = |text: String| read_edit_handoff(&text).is_err();
    assert!(
        refused(handoff_line_of(1, "upsert", false)),
        "an upsert without geometry"
    );
    assert!(
        refused(handoff_line_of(1, "delete", true)),
        "a delete with one"
    );
    assert!(refused(handoff_line_of(1, "move", false)));
    assert!(refused(handoff_line_of(0, "delete", false)));
    assert!(refused(format!(
        "{}\n{}",
        handoff_line_of(1, "delete", false),
        handoff_line_of(1, "delete", false)
    )));
    assert!(refused(
        handoff_line_of(1, "delete", false).replace("parcels", "admin")
    ));
    assert!(refused(
        handoff_line_of(1, "delete", false).replace("4326", "5186")
    ));
    assert!(refused(handoff_line_of(1, "delete", false).replace(
        r#""properties_json":"{}""#,
        r#""properties_json":"[]""#
    )));
    Ok(())
}

#[test]
fn only_unledgered_edits_are_fresh_and_a_rewritten_one_is_refused() -> anyhow::Result<()> {
    let rows = read_edit_handoff(&format!(
        "{}\n{}",
        handoff_line_of(1, "upsert", true),
        handoff_line_of(2, "delete", false)
    ))?;
    let ledgered = BTreeMap::from([(1, rows[0].fingerprint())]);
    let fresh = fresh_edits(&ledgered, &rows)?;
    assert_eq!(fresh.iter().map(|r| r.change_seq).collect::<Vec<_>>(), [2]);
    let mut other = rows[0].fingerprint();
    other.5 = "someone else".to_owned();
    assert!(fresh_edits(&BTreeMap::from([(1, other)]), &rows).is_err());
    Ok(())
}
