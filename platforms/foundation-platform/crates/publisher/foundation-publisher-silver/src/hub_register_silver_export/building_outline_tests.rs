use super::*;

/// A synthetic 동별개요 row: 29 columns, the provider's building key in column 0.
fn row(key: &str, san: &str) -> String {
    let mut fields = vec![""; 29];
    fields[0] = key;
    fields[1] = "synthetic-permit";
    fields[2] = "가상 대지위치";
    fields[4] = "99999";
    fields[5] = "00101";
    fields[6] = san;
    fields[7] = "0012";
    fields[8] = "0003";
    fields[12] = "0";
    fields[13] = "주건축물";
    fields[14] = "17000";
    fields[15] = "공장";
    fields[19] = "31";
    fields[20] = "일반철골구조";
    fields[23] = "100";
    fields[24] = "200";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_building_outline_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_building_keeps_its_own_key_its_permit_and_a_pnu_only_on_a_land_lot() -> anyhow::Result<()> {
    let text = [
        row("synthetic-dong-1", "0"),
        row("synthetic-dong-2", "1"),
        row("synthetic-dong-3", "2"),
    ]
    .join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 2);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    assert_eq!(rows[0]["mgmt_key"], "synthetic-dong-1");
    assert_eq!(rows[0]["permit_mgmt_key"], "synthetic-permit");
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[1]["pnu"], "9999900101200120003");
    // HUB 대지구분 2 (블록) names no cadastral lot (FP-ADR-0023).
    assert!(rows[2]["pnu"].is_null());
    assert_eq!(rows[0]["main_annex_name"], "주건축물");
    assert_eq!(rows[0]["main_purpose_name"], "공장");
    assert_eq!(rows[0]["total_floor_area_m2"], "200");
    assert_eq!(rows[0]["raw_columns"][2], "가상 대지위치");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 29);
    Ok(())
}
