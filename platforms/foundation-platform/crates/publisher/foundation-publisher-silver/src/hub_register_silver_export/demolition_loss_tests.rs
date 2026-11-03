use super::*;

/// A synthetic 철거멸실 row: the provider's key column (0) is empty, as on every real row.
fn row(kind: &str, purpose: &str) -> String {
    let mut fields = vec![""; 34];
    fields[1] = "가상 대지위치";
    fields[3] = "99999";
    fields[4] = "00101";
    fields[5] = "0";
    fields[6] = "0012";
    fields[7] = "0003";
    fields[11] = kind;
    fields[12] = "철거";
    fields[13] = "20990801";
    fields[14] = "20990815";
    fields[18] = "17100";
    fields[19] = purpose;
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_demolition_loss_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_row_without_a_provider_key_is_keyed_by_its_own_content() -> anyhow::Result<()> {
    let text = [row("1", "공장"), row("1", "공장"), row("1", "창고시설")].join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 3);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    let key = |index: usize| rows[index]["mgmt_key"].as_str().map(str::to_owned);
    let expected = format!("{:x}", sha2::Sha256::digest(row("1", "공장").as_bytes()));
    // The same row twice is one key (its set holds the repeat); a different row is another key.
    assert_eq!(key(0).as_deref(), Some(expected.as_str()));
    assert_eq!(key(1), key(0));
    assert_ne!(key(2), key(0));
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[0]["main_purpose_name"], "공장");
    assert_eq!(rows[0]["demolition_kind_name"], "철거");
    assert_eq!(rows[0]["raw_columns"][0], "");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 34);
    Ok(())
}

#[test]
fn a_layout_takes_its_key_from_a_column_or_from_the_row_not_both() -> anyhow::Result<()> {
    let mut layout = crate::building_permit_demolition_loss_silver_export::layout()?;
    layout.columns.insert(
        "mgmt_key".into(),
        serde_json::from_value(json!({"index": 0}))?,
    );
    assert!(layout.validate().is_err());
    let mut master = crate::building_register_master_silver_export::layout()?;
    master.row_key = Some(layout::RowKey::Sha256OfRow);
    assert!(master.validate().is_err());
    Ok(())
}
