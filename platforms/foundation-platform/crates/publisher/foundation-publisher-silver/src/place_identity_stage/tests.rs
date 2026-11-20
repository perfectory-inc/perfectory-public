//! The place-identity stage places rows as the Spark resolver did (root ADR-0288 §1, ADR-0482 §2).
//!
//! The case is the one the Spark job's own test held (`OnbidPlacementTest`, removed with the job):
//! the code first, then the item name's jibun, then its road address, each naming how; prose reads
//! as nothing and keeps the code path's reason. Synthetic codes under 99999 only.

use anyhow::Context;

use super::{offers, read_served_from, ParcelsRead, PLACE_SQL, REFERENCE_SQL, TITLE_TABLE};
use crate::duckdb_parquet_stage::{DuckDbParquetStage, PYTHON_VALUES_SQL};

fn stage() -> anyhow::Result<(tempfile::TempDir, DuckDbParquetStage)> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(PYTHON_VALUES_SQL)?;
    Ok((directory, stage))
}

#[test]
fn the_code_comes_first_then_the_jibun_then_the_road_and_each_says_how() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    let connection = stage.connection();
    connection.execute_batch(&format!(
        "CREATE TABLE place_code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
         effective_date VARCHAR, source VARCHAR);
         CREATE TABLE place_dong_names AS SELECT * FROM (VALUES
           ('9900000000', '합성도', '존재'), ('9999910100', '합성도 가상시 가나동', '존재')) t(region_cd, full_name, status);
         {REFERENCE_SQL}
         CREATE TABLE place_rows AS SELECT * FROM (VALUES
           (0, '9999910100100070000', '합성도 가상시 가나동 8'),
           (1, NULL, '합성도 가상시 가나동 8, 101호 근린생활시설'),
           (2, NULL, '합성도 가상시 가나대로 3 상가'),
           (3, NULL, '합성도 가상시 가나동 9구역9블록'),
           (4, '9999910100100090000', '합성도 가상시 가나동 9')) t(_row, pnu, address);"
    ))?;
    offers(&stage)?;
    connection.execute_batch(&format!(
        "CREATE TABLE place_parcels AS SELECT * FROM (VALUES ('9999910100100070000'), ('9999910100100080000')) t(pnu);
         CREATE TABLE place_titles AS SELECT * FROM (VALUES ('9999910100100070000', '합성도 가상시 가나대로 3')) t(pnu, road_address_raw);
         {PLACE_SQL}"
    ))?;
    let mut query = connection.prepare(
        "SELECT _row, coalesce(pnu, '-'), coalesce(pnu_basis, '-'), coalesce(pnu_unresolved_reason, '-') \
         FROM place_result ORDER BY _row",
    )?;
    let found = query
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let expected = [
        (0, "9999910100100070000", "legal_dong_code", "-"),
        (1, "9999910100100080000", "jibun_address", "-"),
        (2, "9999910100100070000", "road_address", "-"),
        (3, "-", "-", "no_source_code"),
        (4, "-", "-", "parcel_absent"),
    ]
    .map(|(row, pnu, basis, reason)| (row, pnu.to_owned(), basis.to_owned(), reason.to_owned()));
    assert_eq!(found, expected);
    Ok(())
}

#[test]
fn a_superseded_code_is_carried_and_a_split_or_withdrawn_pair_is_not() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    let connection = stage.connection();
    connection.execute_batch(&format!(
        "CREATE TABLE place_code_changes AS SELECT * FROM (VALUES
           ('pair', '9999910100', '9999920100', 'eupmyeondong', '2099-01-01', 'derived:x'),
           ('pair', '9999920100', '9999921100', 'eupmyeondong', '2099-02-01', 'derived:x'),
           ('pair', '9999930100', '9999940100', 'eupmyeondong', '2099-01-01', 'derived:x'),
           ('pair', '9999930100', '9999950100', 'eupmyeondong', '2099-01-01', 'derived:x'),
           ('pair', '9999960100', '9999970100', 'eupmyeondong', '2099-01-01', 'derived:x'),
           ('supersession', '9999960100', '9999970100', 'eupmyeondong', '2099-01-02', 'official:parcel-history'),
           ('pair', '9999980100', '9999990100', 'eupmyeondong', '2099-01-01', 'steward:someone'),
           ('supersession', '9999980100', '9999990100', 'eupmyeondong', '2099-01-02', 'official:parcel-history'))
           t(kind, old_code, new_code, level, effective_date, source);
         CREATE TABLE place_dong_names (region_cd VARCHAR, full_name VARCHAR, status VARCHAR);
         {REFERENCE_SQL}"
    ))?;
    let mut remap =
        connection.prepare("SELECT old_code, current_code FROM place_remap ORDER BY 1")?;
    let found = remap
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        found,
        [
            ("9999910100", "9999921100"),
            ("9999920100", "9999921100"),
            ("9999980100", "9999990100"),
        ]
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
    );
    let ambiguous: String = connection.query_row(
        "SELECT string_agg(code, ',' ORDER BY code) FROM place_ambiguous",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(ambiguous, "9999930100");
    Ok(())
}

/// The titles as a snapshot from before `row_state` holds them.
///
/// A read pinned by snapshot id has that snapshot's schema. On 2026-11-19 the first production
/// load refused them ("Referenced column `row_state` not found"): the contract's current-row query
/// names the column.
#[test]
fn titles_pinned_before_a_column_the_contract_gained_are_read_with_it_null() -> anyhow::Result<()> {
    let (_directory, stage) = stage()?;
    let connection = stage.connection();
    connection.execute_batch(&format!(
        "CREATE TABLE place_code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, level VARCHAR, \
         effective_date VARCHAR, source VARCHAR);
         CREATE TABLE place_dong_names AS SELECT * FROM (VALUES
           ('9900000000', '합성도', '존재'), ('9999910100', '합성도 가상시 가나동', '존재')) t(region_cd, full_name, status);
         {REFERENCE_SQL}
         CREATE TABLE place_rows AS SELECT * FROM (VALUES (0, NULL, '합성도 가상시 가나대로 3 상가')) t(_row, pnu, address);"
    ))?;
    offers(&stage)?;
    // Two versions of one building: the newer one is current. No `row_state` column.
    connection.execute_batch(
        "CREATE TABLE old_parcels AS SELECT * FROM (VALUES ('9999910100100070000', 'e1', NULL::TIMESTAMPTZ)) \
           t(pnu, source_snapshot_id, valid_to_utc);
         CREATE TABLE old_titles AS SELECT * FROM (VALUES
           ('k1', '9999910100100060000', '합성도 가상시 가나대로 3', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 00:00:00+00'),
           ('k1', '9999910100100070000', '합성도 가상시 가나대로 3', TIMESTAMPTZ '2026-02-01 00:00:00+00', TIMESTAMPTZ '2026-02-01 00:00:00+00'))
           t(mgm_bldrgst_pk, pnu, road_address_raw, valid_from_utc, ingested_at_utc);",
    )?;
    let contract =
        lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(TITLE_TABLE)
            .context("the titles have a contract")?;
    let rule = contract
        .current_rows_sql_template()
        .context("the titles keep versions per key")?;
    assert!(
        rule.contains("row_state"),
        "the rule names the column the old snapshot lacks: {rule}"
    );
    let unwrapped = rule
        .replace("{columns}", "pnu")
        .replace("{source}", "old_titles")
        .replace("{filter}", "TRUE");
    let Err(refused) = connection.execute_batch(&unwrapped) else {
        anyhow::bail!("the bare rule read a schema without row_state");
    };
    assert!(refused.to_string().contains("row_state"), "{refused}");

    let read = read_served_from(&stage, "old_parcels", "old_titles", "e1", contract)?;
    assert_eq!(read, ParcelsRead { asked: 0, held: 0 });
    let titles: Vec<String> = connection
        .prepare("SELECT pnu FROM place_titles ORDER BY pnu")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        titles,
        ["9999910100100070000"],
        "the building's current title, once"
    );

    // A source that holds every column is read as it is.
    connection.execute_batch("ALTER TABLE old_titles ADD COLUMN row_state VARCHAR; UPDATE old_titles SET row_state = 'removed' WHERE pnu = '9999910100100070000';")?;
    read_served_from(&stage, "old_parcels", "old_titles", "e1", contract)?;
    let left: i64 =
        connection.query_row("SELECT count(*) FROM place_titles", [], |row| row.get(0))?;
    assert_eq!(
        left, 0,
        "a building whose newest title is a removal marker has no current title"
    );
    Ok(())
}
