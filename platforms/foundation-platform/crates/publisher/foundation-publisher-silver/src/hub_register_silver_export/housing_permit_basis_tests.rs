use super::*;

/// A synthetic 주택인허가 기본개요 row: 29 columns, the provider's approval key in column 0.
fn row(key: &str, san: &str) -> String {
    let mut fields = vec![""; 29];
    fields[0] = key;
    fields[1] = "가상 대지위치";
    fields[3] = "99999";
    fields[4] = "00101";
    fields[5] = san;
    fields[6] = "0012";
    fields[7] = "0003";
    fields[11] = "02000";
    fields[12] = "공동주택";
    fields[15] = "3";
    fields[16] = "12000.5";
    fields[17] = "150";
    fields[23] = "20240115";
    fields[24] = "20240601";
    fields[25] = "20240710";
    fields[26] = "20270301";
    fields[28] = "20261020";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::housing_permit_basis_outline_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn an_approval_keeps_its_own_key_its_dates_and_a_pnu_only_on_a_land_lot() -> anyhow::Result<()> {
    let text = [
        row("synthetic-approval-1", "0"),
        row("synthetic-approval-2", "1"),
        row("synthetic-approval-3", "2"),
    ]
    .join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 2);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    assert_eq!(rows[0]["mgmt_key"], "synthetic-approval-1");
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[1]["pnu"], "9999900101200120003");
    // HUB 대지구분 2 (블록) names no cadastral lot (FP-ADR-0023).
    assert!(rows[2]["pnu"].is_null());
    assert_eq!(rows[0]["purpose_name"], "공동주택");
    assert_eq!(rows[0]["total_household_count"], "150");
    assert_eq!(rows[0]["approval_date"], "20240115");
    assert_eq!(rows[0]["start_scheduled_date"], "20240601");
    assert_eq!(rows[0]["start_actual_date"], "20240710");
    assert_eq!(rows[0]["use_inspection_scheduled_date"], "20270301");
    // Values stay the provider's text: a project not yet inspected keeps the empty cell.
    assert_eq!(rows[0]["use_inspection_date"], "");
    assert_eq!(rows[0]["raw_columns"][28], "20261020");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 29);
    Ok(())
}
