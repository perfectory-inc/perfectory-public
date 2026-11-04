use super::*;

/// A synthetic 대수선 row. One permit holds several rows, so its key column (0) repeats.
fn row(value: &str) -> String {
    let mut fields = vec![""; 16];
    fields[0] = "SYNTHETIC-PERMIT-1";
    fields[1] = "가상 대지위치";
    fields[3] = "99999";
    fields[4] = "00101";
    fields[5] = "0";
    fields[6] = "0012";
    fields[7] = "0003";
    fields[11] = "04";
    fields[13] = "4";
    fields[14] = "변경";
    fields[12] = value;
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_major_repair_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_permit_with_several_rows_keys_each_row_by_its_own_content() -> anyhow::Result<()> {
    let first = "내력벽";
    let text = [row(first), row(first), row("기둥")].join("\n");
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
    assert_eq!(rows[0]["major_repair_kind_name"], "내력벽");
    assert_eq!(rows[0]["major_repair_change_name"], "변경");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 16);
    Ok(())
}
