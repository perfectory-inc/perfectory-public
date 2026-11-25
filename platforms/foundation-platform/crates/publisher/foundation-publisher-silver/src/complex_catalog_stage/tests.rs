//! The catalog's rules on synthetic inputs (codes under `98`·`99`, made-up names): the codes carried
//! through a pair, a split code dropped, the finest level naming the levels above, the 시도's name,
//! the gates, and a digest that ignores the build's stamps. The national judgement against the
//! Spark job is its takeover verdict.

use duckdb::Connection;

use super::*;

/// One complex: id, 시도, 시군구, 법정동, name (SQL literals).
type Complex<'a> = [&'a str; 5];

fn load_local(connection: &Connection, complexes: &[Complex<'_>]) -> anyhow::Result<()> {
    let rows = complexes
        .iter()
        .map(|[id, sido, sigungu, bjdong, name]| {
            format!(
                "({id}, 'code-' || {id}, {name}, '일반', '완료', NULL, NULL, NULL, {sido}, {sigungu}, \
                 {bjdong}, NULL, NULL, NULL, DATE '2099-01-02', NULL, NULL, 1234.5, 99.5, NULL, NULL, \
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'synthetic-2099')"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE complex_current (complex_id VARCHAR, official_complex_code VARCHAR, \
           complex_name VARCHAR, complex_kind VARCHAR, status VARCHAR, development_stage VARCHAR, \
           complex_kind_raw VARCHAR, status_raw VARCHAR, sido_code VARCHAR, sigungu_code VARCHAR, \
           primary_bjdong_code VARCHAR, address_text VARCHAR, management_agency_name VARCHAR, \
           developer_name VARCHAR, designated_date DATE, construction_start_date DATE, \
           completion_date DATE, official_area_sqm DECIMAL(18, 2), \
           development_progress_percent DECIMAL(5, 2), lot_sales_status VARCHAR, \
           lot_sales_status_raw VARCHAR, business_period_raw VARCHAR, \
           business_period_start_month VARCHAR, business_period_end_month VARCHAR, \
           designation_basis_law_raw VARCHAR, development_method_raw VARCHAR, \
           development_purpose_raw VARCHAR, invited_industries_raw VARCHAR, source_snapshot_id VARCHAR); \
         INSERT INTO complex_current VALUES {rows}; \
         CREATE OR REPLACE TABLE place_code_changes (kind VARCHAR, old_code VARCHAR, new_code VARCHAR, \
           level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         INSERT INTO place_code_changes VALUES \
           ('pair', '9999910100', '9899910100', 'eupmyeondong', '2099-01-01', 'official:x'), \
           ('pair', '9999200000', '9999300000', 'sigungu', '2099-01-01', 'official:x'), \
           ('pair', '9999200000', '9999400000', 'sigungu', '2099-01-01', 'official:x'); \
         CREATE OR REPLACE TABLE place_dong_names (region_cd VARCHAR, full_name VARCHAR, status VARCHAR); \
         INSERT INTO place_dong_names VALUES \
           ('9900000000', '가상도', '존재'), ('9999100000', '가상도 하나시', '존재'), \
           ('9800000000', '옛가상도', '폐지'), ('9811000000', '새가상도 둘시', '존재'), \
           ('9899910100', '새가상도 셋시 넷동', '존재');"
    ))?;
    Ok(())
}

type Region = (Option<String>, Option<String>, Option<String>);

fn region(connection: &Connection, id: &str) -> anyhow::Result<Region> {
    Ok(connection.query_row(
        "SELECT sido_code, sigungu_code, sido_name FROM built WHERE complex_id = ?",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?)
}

fn some(sido: &str, sigungu: Option<&str>, name: &str) -> Region {
    (
        Some(sido.to_owned()),
        sigungu.map(str::to_owned),
        Some(name.to_owned()),
    )
}

#[test]
fn the_codes_are_carried_to_the_current_ones_and_named() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load_local(
        &connection,
        &[
            // The 법정동 moved 시도: its 시군구 and 시도 follow it.
            ["'moved'", "'99'", "'99999'", "'9999910100'", "'하나단지'"],
            // The 시군구 split: no 시군구, the 시도 its own code.
            ["'split'", "'99'", "'99992'", "NULL", "'둘단지'"],
            // No pair reaches it.
            ["'kept'", "'99'", "'99991'", "'9999110200'", "'셋단지'"],
        ],
    )?;
    assert_eq!(build(&connection, 7, "2099-01-03T00:00:00Z")?, 3);
    // 98 has no 시도 row in force: the smallest code names it.
    assert_eq!(
        region(&connection, "moved")?,
        some("98", Some("98999"), "새가상도")
    );
    assert_eq!(region(&connection, "split")?, some("99", None, "가상도"));
    assert_eq!(
        region(&connection, "kept")?,
        some("99", Some("99991"), "가상도")
    );
    let (snapshot, parcels, published): (String, i64, String) = connection.query_row(
        "SELECT iceberg_snapshot_id, parcel_count, CAST(published_at_utc AS VARCHAR) FROM built \
         WHERE complex_id = 'kept'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(
        (snapshot.as_str(), parcels, published.as_str()),
        ("7", 0, "2099-01-03 00:00:00")
    );
    Ok(())
}

#[test]
fn the_digest_is_the_content_and_not_the_build_s_stamps() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load_local(
        &connection,
        &[["'kept'", "'99'", "'99991'", "'9999110200'", "'셋단지'"]],
    )?;
    let digest = |connection: &Connection| -> anyhow::Result<String> {
        Ok(connection.query_row("SELECT row_digest FROM built", [], |row| row.get(0))?)
    };
    build(&connection, 7, "2099-01-03T00:00:00Z")?;
    let first = digest(&connection)?;
    build(&connection, 8, "2099-02-03T00:00:00Z")?;
    assert_eq!(digest(&connection)?, first);
    connection.execute_batch("UPDATE complex_current SET complex_name = '넷단지'")?;
    build(&connection, 8, "2099-02-03T00:00:00Z")?;
    assert_ne!(digest(&connection)?, first);
    Ok(())
}

#[test]
fn a_row_the_spark_gates_refused_is_refused() -> anyhow::Result<()> {
    for name in ["''", "NULL"] {
        let connection = Connection::open_in_memory()?;
        load_local(
            &connection,
            &[["'blank'", "'99'", "'99991'", "'9999110200'", name]],
        )?;
        let refused = build(&connection, 7, "2099-01-03T00:00:00Z")
            .err()
            .map(|error| error.to_string());
        assert!(
            refused.is_some_and(|text| text.contains("name is")),
            "{name}"
        );
    }
    let connection = Connection::open_in_memory()?;
    load_local(
        &connection,
        &[["'kept'", "'99'", "'99991'", "'9999110200'", "'셋단지'"]],
    )?;
    assert!(build(&connection, 7, "2099-01-03'; DROP TABLE built; --").is_err());
    Ok(())
}

#[test]
fn the_code_version_names_the_sql_it_runs() {
    let version = code_version();
    assert_eq!(version.len(), 64);
    assert_eq!(version, code_version());
}
