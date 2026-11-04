use super::*;

/// A synthetic 부설주차장 row: one permit (column 0) may hold several annex rows.
fn row(permit: &str, related: &str) -> String {
    let mut fields = vec![""; 12];
    fields[0] = permit;
    fields[1] = "가상 대지위치";
    fields[2] = "99999";
    fields[3] = "00100";
    fields[4] = "00101";
    fields[5] = "0";
    fields[6] = "0012";
    fields[7] = "0003";
    fields[8] = "08";
    fields[9] = "대";
    fields[10] = related;
    fields[11] = "20990909";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_annex_parking_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn annex_rows_of_one_permit_are_keyed_by_their_own_content() -> anyhow::Result<()> {
    let text = [
        row("990001", "1-1"),
        row("990001", "2-1"),
        row("990001", "2-1"),
    ]
    .join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 3);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    let key = |index: usize| rows[index]["mgmt_key"].as_str().map(str::to_owned);
    let expected = format!(
        "{:x}",
        sha2::Sha256::digest(row("990001", "1-1").as_bytes())
    );
    // One permit, two annex rows: two keys; the repeated row is one key's set.
    assert_eq!(key(0).as_deref(), Some(expected.as_str()));
    assert_ne!(key(1), key(0));
    assert_eq!(key(2), key(1));
    assert_eq!(rows[0]["permit_mgmt_key"], "990001");
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[0]["land_category_name"], "대");
    assert_eq!(rows[1]["related_jibun_name"], "2-1");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 12);
    Ok(())
}
