//! The 법정동 full table is read as the Spark job's Python read it (root ADR-0143 §2).
//!
//! `fixtures/full_table.html` is a synthetic page in the shape code.go.kr serves (unclosed `<tr>`,
//! nested layout tables, `&nbsp` without its semicolon, a `<table>` inside a script's text); the
//! two JSON files beside it are what `code_go_kr_legal_dong.html_tables` and
//! `parse_full_table_html` made of it before the job was removed (the rows as arrays in the
//! contract's header order). Codes under 99 only.

use anyhow::Context as _;
use chrono::NaiveDate;
use serde_json::Value;

use super::{
    html_tables, previous_row_count_in, stage_snapshot, unescape, CodeRow, FullTableContract, TABLE,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

fn fixture(name: &str) -> anyhow::Result<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/legal_dong_code_snapshot_stage/fixtures")
        .join(name);
    std::fs::read_to_string(&path).with_context(|| format!("cannot read {}", path.display()))
}

fn texts(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|cells| {
            cells
                .iter()
                .map(|cell| cell.as_str().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn every_table_of_the_page_is_read_as_python_read_it() -> anyhow::Result<()> {
    let expected: Vec<Value> = serde_json::from_str(&fixture("full_table.tables.json")?)?;
    let found = html_tables(&fixture("full_table.html")?)?;
    assert_eq!(found.len(), expected.len());
    for (table, expected) in found.iter().zip(&expected) {
        let head = (!expected["head"].is_null()).then(|| texts(&expected["head"]));
        assert_eq!(table.head, head);
        let rows = expected["rows"]
            .as_array()
            .context("rows")?
            .iter()
            .map(texts)
            .collect::<Vec<_>>();
        assert_eq!(table.rows, rows);
    }
    Ok(())
}

#[test]
fn the_full_table_is_the_rows_python_read() -> anyhow::Result<()> {
    let expected: Vec<Value> = serde_json::from_str(&fixture("full_table.rows.json")?)?;
    let rows = FullTableContract::compiled()?.parse(&fixture("full_table.html")?)?;
    // One array per code, in the contract's header order: code, name, status, parent, rank,
    // created, abolished, last work, lowest name, 주민 code, 지적 code.
    let expected = expected
        .iter()
        .map(|row| {
            let cell = |at: usize| row[at].as_str().unwrap_or_default().to_owned();
            CodeRow {
                region_cd: cell(0),
                full_name: cell(1),
                status: cell(2),
                parent_cd: cell(3),
                created_date: cell(5),
                abolished_date: cell(6),
                lowest_name: cell(8),
                jumin_cd: cell(9),
                jijuk_cd: cell(10),
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
    Ok(())
}

/// The page with `from` replaced by `to` once, refused with `message`.
fn refused(from: &str, to: &str, message: &str) -> anyhow::Result<()> {
    let page = fixture("full_table.html")?;
    anyhow::ensure!(page.contains(from), "the fixture holds no {from:?}");
    let Err(error) = FullTableContract::compiled()?.parse(&page.replacen(from, to, 1)) else {
        anyhow::bail!("{message}: the page was read");
    };
    assert!(format!("{error:#}").contains(message), "{error:#}");
    Ok(())
}

#[test]
fn a_changed_shape_refuses_the_whole_table() -> anyhow::Result<()> {
    refused(">9999910100<", ">999991010<", "is not a 10-digit")?;
    refused(">9999910200<", ">9999910100<", "appears twice")?;
    refused(">20990117<", ">20991317<", "is not a YYYYMMDD date")?;
    refused(
        "<center>폐지</center>",
        "<center>없음</center>",
        "is none of the contract's words",
    )?;
    refused(">99999<", ">구십구<", "is not a code")?;
    refused(
        "<td class=\"table_left\">9999910300</td>",
        "",
        "10 cells, expected 11",
    )?;
    refused(">생성일<", ">만든날<", "columns changed")?;
    refused("\t</thead>", "", "holds no table headed")?;
    refused(
        "마바 &amp; 사동",
        "마바 &middot; 사동",
        "does not know that name",
    )?;
    Ok(())
}

#[test]
fn a_reference_is_read_as_html_unescape_reads_it() -> anyhow::Result<()> {
    assert_eq!(unescape("a&nbspb&nbsp;c")?, "a\u{a0}b\u{a0}c");
    assert_eq!(
        unescape("1 &lt; 2 &amp 3 &#38; &#x3E; &apos;")?,
        "1 < 2 & 3 & > '"
    );
    assert_eq!(unescape("a & b &# c &;")?, "a & b &# c &;");
    assert_eq!(unescape("?a=1&pageSize=10")?, "?a=1&pageSize=10");
    Ok(())
}

#[test]
fn a_table_under_the_floor_or_shrunk_is_refused() -> anyhow::Result<()> {
    let contract = FullTableContract::compiled()?;
    let floor = contract.min_rows;
    contract.check_table_size(floor, None)?;
    assert!(contract.check_table_size(floor - 1, None).is_err());
    contract.check_table_size(floor * 2, Some(floor * 2))?;
    assert!(contract.check_table_size(floor, Some(floor * 2)).is_err());
    Ok(())
}

#[test]
fn the_snapshot_is_staged_as_the_tables_columns_with_empty_fields_null() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let rows = FullTableContract::compiled()?.parse(&fixture("full_table.html")?)?;
    let contract = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(TABLE)
        .context("the snapshot table has a contract")?;
    let at = chrono::DateTime::from_timestamp(4_070_000_000, 0).context("an instant")?;
    let by_status = stage_snapshot(
        &stage,
        contract,
        &rows,
        NaiveDate::from_ymd_opt(2099, 1, 2).context("a day")?,
        "bronze/source=codegokr__legal_dong_code/synthetic.html",
        at,
    )?;
    assert_eq!(
        by_status.into_iter().collect::<Vec<_>>(),
        [("존재".to_owned(), 4), ("폐지".to_owned(), 1)]
    );
    let found: Vec<(String, Option<String>, Option<String>, String)> = stage
        .connection()
        .prepare(
            "SELECT region_cd, parent_cd, abolished_date, CAST(snapshot_date AS VARCHAR) \
             FROM legal_dong_snapshot ORDER BY region_cd",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<_, _>>()?;
    let day = "2099-01-02".to_owned();
    assert_eq!(found.len(), 5);
    assert_eq!(
        found[3],
        (
            "9999910200".to_owned(),
            None,
            Some("20990117".to_owned()),
            day
        )
    );
    let columns: Vec<String> = stage
        .connection()
        .prepare("SELECT column_name FROM (DESCRIBE legal_dong_snapshot)")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        columns,
        contract
            .columns
            .iter()
            .map(|column| column.name.to_owned())
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// 2026-11-04~07: a changed-since window with no change returned the site's one-cell notice, and
/// the reader refused it as a changed shape. The notice is no rows; any other one-cell row is
/// still a changed shape.
#[test]
fn the_sites_empty_result_notice_is_no_rows() -> anyhow::Result<()> {
    let contract = FullTableContract::compiled()?;
    let head = contract
        .headers
        .iter()
        .map(|header| format!("<td class=\"table_header_center01_blue01\">{header}</td>"))
        .collect::<Vec<_>>()
        .concat();
    let page = |notice: &str| {
        format!(
            "<table><tr><td colspan=\"2\"><table class=\"table\" width=\"770\"><thead><tr>{head}</tr></thead>
 <tr> 
             <td class=\"table_left\" colspan=\"10\" align=\"center\" ><div align=\"center\">{notice}</div></td>
</tr>
             </table></td></tr><tr><td width=\"100\" align=\"right\">총 <strong>0</strong>건</td></tr></table>"
        )
    };
    assert_eq!(contract.parse(&page(&contract.empty_text))?, []);
    let Err(error) = contract.parse(&page("점검 중입니다.")) else {
        anyhow::bail!("another notice was read as no rows");
    };
    assert!(format!("{error:#}").contains("1 cells"), "{error:#}");
    // An empty full table is still refused by its floor.
    assert!(contract.check_table_size(0, None).is_err());
    Ok(())
}

/// A day that holds two loads (a retry) is one snapshot: counting every row of that day would
/// double the baseline and refuse every later table as shrunken. A load before the Bronze epoch
/// recorded the source's former id (root ADR-0626 §4) and is still the source's.
#[test]
fn the_shrink_baseline_is_the_latest_earlier_code_go_kr_load() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    stage.connection().execute_batch(
        "CREATE TABLE held AS
         SELECT 'c' || i AS region_cd, DATE '2099-01-01' AS snapshot_date,
                'bronze/source=codegokr__legal_dong_code_table/k-1' AS source_record_id, to_timestamp(4070000000) AS ingested_at_utc
         FROM range(5) t(i)
         UNION ALL SELECT 'c' || i, DATE '2099-01-02', 'bronze/source=codegokr__legal_dong_code/k-2a', to_timestamp(4070090000)
         FROM range(6) t(i)
         UNION ALL SELECT 'c' || i, DATE '2099-01-02', 'bronze/source=codegokr__legal_dong_code/k-2b', to_timestamp(4070095000)
         FROM range(7) t(i)
         UNION ALL SELECT 'c' || i, DATE '2099-01-02', 'another-source:full:2099-01-02', to_timestamp(4070099000)
         FROM range(9) t(i)",
    )?;
    let day = |day| NaiveDate::from_ymd_opt(2099, 1, day).context("a day");
    assert_eq!(previous_row_count_in(&stage, "held", day(3)?)?, Some(7));
    assert_eq!(previous_row_count_in(&stage, "held", day(2)?)?, Some(5));
    assert_eq!(previous_row_count_in(&stage, "held", day(1)?)?, None);
    Ok(())
}
