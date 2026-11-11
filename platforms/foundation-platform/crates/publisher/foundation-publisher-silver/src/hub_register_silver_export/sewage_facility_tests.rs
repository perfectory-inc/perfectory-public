use super::*;

/// A synthetic 오수정화시설 row. A building lists several facilities, so its key column (0) repeats.
fn row(method_code: &str, method_name: &str) -> String {
    let mut fields = vec![""; 29];
    fields[0] = "SYNTHETIC-BUILDING-1";
    fields[3] = "2";
    fields[4] = "일반건축물";
    fields[5] = "가상 대지위치";
    fields[8] = "99999";
    fields[9] = "00101";
    fields[10] = "0";
    fields[11] = "0012";
    fields[12] = "0003";
    fields[21] = method_code;
    fields[22] = method_name;
    fields[26] = "10";
    fields[27] = "0";
    fields[28] = "20990101";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_register_sewage_facility_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_building_with_several_facilities_keys_each_row_by_its_own_content() -> anyhow::Result<()> {
    let tank = row("201", "부패탱크방법");
    let text = [tank.clone(), tank.clone(), row("300", "하수종말처리장연결")].join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 3);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    let key = |index: usize| rows[index]["mgmt_key"].as_str().map(str::to_owned);
    let expected = format!("{:x}", sha2::Sha256::digest(tank.as_bytes()));
    // The same row twice is one key; the building's other facility is another key under the same building.
    assert_eq!(key(0).as_deref(), Some(expected.as_str()));
    assert_eq!(key(1), key(0));
    assert_ne!(key(2), key(0));
    assert_eq!(rows[2]["building_mgmt_key"], rows[0]["building_mgmt_key"]);
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[0]["treatment_method_name"], "부패탱크방법");
    assert_eq!(rows[2]["treatment_method_code"], "300");
    assert_eq!(rows[0]["register_kind_name"], "일반건축물");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 29);
    Ok(())
}
