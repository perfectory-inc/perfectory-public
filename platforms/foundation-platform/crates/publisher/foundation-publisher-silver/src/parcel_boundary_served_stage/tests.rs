use super::*;
use crate::kind_versions_stage::{load_ranged, read_for_test};
use crate::parcel_nearby_stage::Source;

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
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR);",
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
fn the_built_rows_are_the_edition_as_the_source_holds_it_and_the_digest_is_the_sql_s(
) -> anyhow::Result<()> {
    // Root ADR-0621 §1: parcel geometry takes no edits, so every row is the edition's.
    let connection = Connection::open_in_memory()?;
    inputs(&connection)?;
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
                "9999910100100010000".into(),
                "source".into(),
                "edition-b".into()
            ),
            (
                "9999910100100020000".into(),
                "source".into(),
                "edition-b".into()
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
    assert_eq!(through, 0);
    assert!(SERVED_BOUNDARIES
        .build(&connection, true, "1'", "x")
        .is_err());
    Ok(())
}
