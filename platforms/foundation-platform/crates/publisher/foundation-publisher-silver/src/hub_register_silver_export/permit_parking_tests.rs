use super::*;

/// A synthetic 주차장 row: 관리허가대장PK first, then the 지번 and the parking spaces by kind.
fn row(key: &str, outdoor_self: &str) -> String {
    let mut fields = vec!["0"; 24];
    fields[0] = key;
    fields[1] = "가상 대지위치";
    fields[2] = "99999";
    fields[3] = "00101";
    fields[4] = "1";
    fields[5] = "0012";
    fields[6] = "0003";
    fields[7] = "";
    fields[8] = "";
    fields[9] = "";
    fields[10] = "2";
    fields[11] = "25";
    fields[12] = outdoor_self;
    fields[13] = "23";
    fields[23] = "20990909";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_parking_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_permit_parking_row_keeps_its_key_and_spaces_by_kind() -> anyhow::Result<()> {
    let text = [row("9900000000001", "2"), row("9900000000002", "0")].join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 2);
    assert_eq!(report["pnu_ok"], 2);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    assert_eq!(rows[0]["mgmt_key"], "9900000000001");
    // HUB 대지구분 1 (산) is the standard PNU digit 2 (FP-ADR-0023).
    assert_eq!(rows[0]["pnu"], "9999900101200120003");
    assert_eq!(rows[0]["indoor_self_parking_count"], "2");
    assert_eq!(rows[0]["indoor_self_parking_area_m2"], "25");
    assert_eq!(rows[0]["outdoor_self_parking_count"], "2");
    assert_eq!(rows[1]["outdoor_self_parking_count"], "0");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 24);
    Ok(())
}
