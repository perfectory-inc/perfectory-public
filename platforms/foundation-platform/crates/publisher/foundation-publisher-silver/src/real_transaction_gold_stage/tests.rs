//! The region statistics' rules, on synthetic deals (법정동 `9999910100`).
//!
//! The national verdict (module docs) is what moved the transform; this keeps the binary on the
//! rules that verdict could only cover by the data it met: the quartile interpolation and its
//! HALF_UP rounding, a cancelled deal counted but not priced, no building price for 토지, the
//! trailing window's bounds, the 시군구 level, and Java's text of a double past 10⁷ in the digest.

use chrono::NaiveDate;

use super::{define_macros, region_stats_query};

const INDUSTRIAL: &str = "real_transaction_industrial_trade";
const LAND: &str = "real_transaction_land_trade";

fn connection() -> anyhow::Result<duckdb::Connection> {
    let connection = duckdb::Connection::open_in_memory()?;
    define_macros(&connection)?;
    connection.execute_batch(
        "CREATE TABLE deals (dataset_slug VARCHAR, legal_dong VARCHAR, deal_date DATE, \
         deal_amount_10k_krw BIGINT, land_area_m2 DOUBLE, building_area_m2 DOUBLE, cancelled BOOLEAN)",
    )?;
    let deals = [
        (INDUSTRIAL, "2026-10-05", 100_i64, 10.0, 4.0, false),
        (INDUSTRIAL, "2026-10-20", 300, 20.0, 0.0, false),
        // Counted, not priced.
        (INDUSTRIAL, "2026-10-25", 50, 2.0, 2.0, true),
        // 토지 has no building price.
        (LAND, "2026-10-07", 200, 8.0, 8.0, false),
        // Before the trailing window; its price is past 10⁷.
        (INDUSTRIAL, "2025-10-31", 120_000_000, 1.0, 0.0, false),
        // The build month itself is not in the trailing window.
        (INDUSTRIAL, "2026-11-01", 9, 3.0, 0.0, false),
    ];
    for (dataset, day, amount, land, building, cancelled) in deals {
        connection.execute(
            "INSERT INTO deals VALUES (?, '9999910100', CAST(? AS DATE), ?, ?, ?, ?)",
            duckdb::params![dataset, day, amount, land, building, cancelled],
        )?;
    }
    // A deal without its current 법정동 is in no region.
    connection.execute_batch(&format!(
        "INSERT INTO deals VALUES ('{INDUSTRIAL}', NULL, DATE '2026-10-01', 1, 1, 1, false)"
    ))?;
    Ok(connection)
}

type Row = (
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);

fn rows(connection: &duckdb::Connection, as_of: NaiveDate) -> anyhow::Result<Vec<Row>> {
    let query = region_stats_query(as_of)?;
    let mut statement = connection.prepare(&format!(
        "SELECT dataset_slug, region_level, region_code, period_kind, period, deal_count, \
         cancelled_count, land_price_per_m2_p25, land_price_per_m2_median, land_price_per_m2_p75, \
         building_price_per_m2_median FROM ({query}) ORDER BY ALL"
    ))?;
    let found = statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(found)
}

#[test]
fn the_statistics_are_the_spark_jobs() -> anyhow::Result<()> {
    let connection = connection()?;
    let found = rows(
        &connection,
        NaiveDate::from_ymd_opt(2026, 11, 15).unwrap_or_default(),
    )?;
    let row = |dataset: &str,
               level: &str,
               kind: &str,
               period: &str,
               counts: (i64, i64),
               land: [Option<f64>; 3],
               building: Option<f64>|
     -> Row {
        let code = if level == "legal_dong" {
            "9999910100"
        } else {
            "9999900000"
        };
        (
            dataset.to_owned(),
            level.to_owned(),
            code.to_owned(),
            kind.to_owned(),
            period.to_owned(),
            counts.0,
            counts.1,
            land[0],
            land[1],
            land[2],
            building,
        )
    };
    let mut expected = Vec::new();
    for level in ["legal_dong", "sigungu"] {
        // [10, 15]: 11.25 → 11.3 and 13.75 → 13.8 (HALF_UP), the median between them.
        let october = [Some(11.3), Some(12.5), Some(13.8)];
        expected.extend([
            row(
                INDUSTRIAL,
                level,
                "month",
                "2025-10",
                (1, 0),
                [Some(1.2e8); 3],
                None,
            ),
            row(
                INDUSTRIAL,
                level,
                "month",
                "2026-10",
                (3, 1),
                october,
                Some(25.0),
            ),
            row(
                INDUSTRIAL,
                level,
                "month",
                "2026-11",
                (1, 0),
                [Some(3.0); 3],
                None,
            ),
            row(
                INDUSTRIAL,
                level,
                "trailing_12_months",
                "2026-11",
                (3, 1),
                october,
                Some(25.0),
            ),
            row(
                LAND,
                level,
                "month",
                "2026-10",
                (1, 0),
                [Some(25.0); 3],
                None,
            ),
            row(
                LAND,
                level,
                "trailing_12_months",
                "2026-11",
                (1, 0),
                [Some(25.0); 3],
                None,
            ),
        ]);
    }
    expected.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    assert_eq!(found, expected);
    Ok(())
}

#[test]
fn the_digest_is_sparks_to_json_of_the_content() -> anyhow::Result<()> {
    let connection = connection()?;
    let query = region_stats_query(NaiveDate::from_ymd_opt(2026, 11, 1).unwrap_or_default())?;
    // Spark's to_json: contract order, nulls left out, a double as Java writes it.
    for (period, text) in [
        (
            "2026-10",
            r#"{"dataset_slug":"real_transaction_industrial_trade","region_level":"legal_dong","region_code":"9999910100","period_kind":"month","period":"2026-10","deal_count":3,"cancelled_count":1,"land_price_per_m2_p25":11.3,"land_price_per_m2_median":12.5,"land_price_per_m2_p75":13.8,"building_price_per_m2_p25":25.0,"building_price_per_m2_median":25.0,"building_price_per_m2_p75":25.0}"#,
        ),
        (
            "2025-10",
            r#"{"dataset_slug":"real_transaction_industrial_trade","region_level":"legal_dong","region_code":"9999910100","period_kind":"month","period":"2025-10","deal_count":1,"cancelled_count":0,"land_price_per_m2_p25":1.2E8,"land_price_per_m2_median":1.2E8,"land_price_per_m2_p75":1.2E8}"#,
        ),
    ] {
        let (digest, expected): (String, String) = connection.query_row(
            &format!(
                "SELECT row_digest, sha256(?) FROM ({query}) WHERE dataset_slug = '{INDUSTRIAL}' \
                 AND region_level = 'legal_dong' AND period_kind = 'month' AND period = ?"
            ),
            duckdb::params![text, period],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(digest, expected, "{period}");
    }
    Ok(())
}

#[test]
fn the_double_text_is_javas() -> anyhow::Result<()> {
    let connection = duckdb::Connection::open_in_memory()?;
    define_macros(&connection)?;
    for (value, text) in [
        (0.0, "0.0"),
        (12.5, "12.5"),
        (9_999_999.9, "9999999.9"),
        (10_000_000.0, "1.0E7"),
        (12_345_678.9, "1.23456789E7"),
        (120_000_000.0, "1.2E8"),
    ] {
        let found: String = connection.query_row(
            "SELECT spark_double_text(CAST(? AS DOUBLE))",
            [value],
            |row| row.get(0),
        )?;
        assert_eq!(found, text, "{value}");
    }
    Ok(())
}
