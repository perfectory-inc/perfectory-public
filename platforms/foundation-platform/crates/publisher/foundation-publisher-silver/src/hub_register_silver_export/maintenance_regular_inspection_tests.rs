use super::*;

/// A synthetic 정기점검 row: 11 columns, no 대지구분, the register key in the provider's form.
fn row(inspection: &str, register: &str) -> String {
    let mut fields = vec![""; 11];
    fields[0] = "99999";
    fields[1] = "00101";
    fields[2] = "0012";
    fields[3] = "0003";
    fields[4] = "가상 건물";
    fields[5] = "가동";
    fields[6] = inspection;
    fields[7] = register;
    fields[8] = "가상 점검기관";
    fields[9] = "20990801";
    fields[10] = "20990815";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_maintenance_regular_inspection_history_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn an_inspection_row_keeps_its_register_key_and_composes_no_pnu() -> anyhow::Result<()> {
    // One inspection over two buildings: the inspection key repeats, the rows do not.
    let text = [row("123", "99999-1"), row("123", "99999-2")].join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 2);
    // No row carries a PNU: every row counts as pnu_bad, so the manifest accounting holds.
    assert_eq!(
        (report["pnu_ok"].clone(), report["pnu_bad"].clone()),
        (json!(0), json!(2))
    );
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    let expected = format!(
        "{:x}",
        sha2::Sha256::digest(row("123", "99999-1").as_bytes())
    );
    assert_eq!(rows[0]["mgmt_key"], expected.as_str());
    assert_ne!(rows[1]["mgmt_key"], rows[0]["mgmt_key"]);
    assert_eq!(rows[0]["building_register_key"], "99999-1");
    assert_eq!(rows[0]["inspection_key"], "123");
    assert!(rows[0].get("pnu").is_none());
    assert_eq!(rows[0]["raw_columns"][4], "가상 건물");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 11);
    Ok(())
}

#[test]
fn a_layout_reaches_its_parcel_by_pnu_inputs_or_by_a_link_exactly_one() -> anyhow::Result<()> {
    let linked = crate::building_maintenance_regular_inspection_history_silver_export::layout()?;
    // Neither: a linked layout without its link.
    let mut neither = linked.clone();
    neither.parcel_link = None;
    assert!(neither.validate().is_err());
    // A link must name a consumed column.
    let mut dangling = linked.clone();
    if let Some(link) = dangling.parcel_link.as_mut() {
        link.column = "no_such_column".into();
    }
    assert!(dangling.validate().is_err());
    // Both: a PNU-composing layout cannot also take a link.
    let mut master = crate::building_register_master_silver_export::layout()?;
    master.parcel_link = linked.parcel_link.clone();
    assert!(master.validate().is_err());
    Ok(())
}
