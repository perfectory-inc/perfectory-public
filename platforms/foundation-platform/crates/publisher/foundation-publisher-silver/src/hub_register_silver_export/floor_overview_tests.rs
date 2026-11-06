use super::*;

/// A synthetic 층별개요 row: 24 columns, the provider's floor key in column 0.
fn row(key: &str, floor_kind: &str, floor_no: &str) -> String {
    let mut fields = vec![""; 24];
    fields[0] = key;
    fields[1] = "synthetic-dong";
    fields[2] = "synthetic-permit";
    fields[3] = "가상 대지위치";
    fields[5] = "99999";
    fields[6] = "00101";
    fields[7] = "0";
    fields[8] = "0012";
    fields[9] = "0003";
    fields[13] = "31";
    fields[14] = "일반철골구조";
    fields[15] = "17000";
    fields[16] = "공장";
    fields[17] = floor_no;
    fields[18] = "250.5";
    fields[19] = floor_kind;
    fields[20] = if floor_kind == "10" {
        "지하"
    } else {
        "지상"
    };
    fields[21] = "0100";
    fields[22] = "신축";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_floor_overview_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_floor_keeps_its_own_key_its_building_and_its_permit() -> anyhow::Result<()> {
    let text = [
        row("synthetic-floor-1", "10", "1"),
        row("synthetic-floor-2", "20", "1"),
    ]
    .join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 2);
    assert_eq!(report["pnu_ok"], 2);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    assert_eq!(rows[0]["mgmt_key"], "synthetic-floor-1");
    assert_eq!(rows[1]["mgmt_key"], "synthetic-floor-2");
    assert_eq!(rows[0]["building_mgmt_key"], "synthetic-dong");
    assert_eq!(rows[0]["permit_mgmt_key"], "synthetic-permit");
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    // The same floor number above and below ground is two floors, told apart by 층구분.
    assert_eq!(rows[0]["floor_no"], rows[1]["floor_no"]);
    assert_eq!(rows[0]["floor_kind_name"], "지하");
    assert_eq!(rows[1]["floor_kind_name"], "지상");
    assert_eq!(rows[0]["floor_area_m2"], "250.5");
    assert_eq!(rows[0]["main_purpose_name"], "공장");
    assert_eq!(rows[0]["raw_columns"][3], "가상 대지위치");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 24);
    Ok(())
}
