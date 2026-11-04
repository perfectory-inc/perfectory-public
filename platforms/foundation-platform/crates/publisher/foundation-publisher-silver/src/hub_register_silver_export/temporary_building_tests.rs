use super::*;

/// A synthetic 가설건축물 row. One permit holds several rows, so its key column (0) repeats.
fn row(value: &str) -> String {
    let mut fields = vec![""; 23];
    fields[0] = "SYNTHETIC-PERMIT-1";
    fields[1] = "가상 대지위치";
    fields[3] = "99999";
    fields[4] = "00101";
    fields[5] = "0";
    fields[6] = "0012";
    fields[7] = "0003";
    fields[11] = "30";
    fields[12] = "30";
    fields[13] = "500";
    fields[15] = "74";
    fields[16] = "컨테이너조";
    fields[17] = "18000";
    fields[19] = "30";
    fields[20] = "30";
    fields[21] = "1";
    fields[18] = value;
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_temporary_building_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_permit_with_several_rows_keys_each_row_by_its_own_content() -> anyhow::Result<()> {
    let first = "창고시설";
    let text = [row(first), row(first), row("공장")].join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 3);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    let key = |index: usize| rows[index]["mgmt_key"].as_str().map(str::to_owned);
    let expected = format!("{:x}", sha2::Sha256::digest(row(first).as_bytes()));
    // The same row twice is one key; the permit's other row is another key under the same permit.
    assert_eq!(key(0).as_deref(), Some(expected.as_str()));
    assert_eq!(key(1), key(0));
    assert_ne!(key(2), key(0));
    assert_eq!(rows[2]["permit_mgmt_key"], rows[0]["permit_mgmt_key"]);
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[0]["main_purpose_name"], "창고시설");
    assert_eq!(rows[0]["structure_name"], "컨테이너조");
    assert_eq!(rows[0]["ground_floor_count"], "1");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 23);
    Ok(())
}
