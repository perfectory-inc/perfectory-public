use super::*;

/// A synthetic 부속지번 row: the register's own 지번 at 8–12 and the other parcel's at 23–27.
fn row(key: &str, sub_san: &str, sub_bon: &str) -> String {
    let mut fields = vec![""; 33];
    fields[0] = key;
    fields[3] = "2";
    fields[4] = "일반건축물";
    fields[7] = "가상 건물";
    fields[8] = "99999";
    fields[9] = "00101";
    fields[10] = "0";
    fields[11] = "0012";
    fields[12] = "0003";
    fields[23] = "99999";
    fields[24] = "00101";
    fields[25] = sub_san;
    fields[26] = sub_bon;
    fields[27] = "0000";
    fields[32] = "20990801";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_register_sub_parcel_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn each_row_carries_its_register_parcel_and_the_other_parcel() -> anyhow::Result<()> {
    let text = [
        row("synthetic-1", "0", "0013"),
        row("synthetic-1", "1", "0014"),
        // A 대지구분 the standard PNU has no digit for (실측 3–9·'-'·빈칸) composes no sub_pnu.
        row("synthetic-2", "-", "0015"),
    ]
    .join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 3);
    assert_eq!(report["extra_pnu_ok"], json!({"sub_pnu": 2}));
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    for emitted in rows {
        assert_eq!(emitted["pnu"], "9999900101100120003");
        assert_eq!(emitted["register_kind_name"], "일반건축물");
        assert_eq!(emitted["raw_columns"].as_array().context("raw")?.len(), 33);
        assert!(emitted.get("building_name").is_none());
    }
    assert_eq!(rows[0]["sub_pnu"], "9999900101100130000");
    assert_eq!(rows[1]["sub_pnu"], "9999900101200140000");
    assert!(rows[2]["sub_pnu"].is_null());
    assert_eq!(rows[2]["sub_san_gubun"], "-");
    Ok(())
}

#[test]
fn a_refused_sigungu_on_the_other_parcel_leaves_it_null_and_the_run_whole() -> anyhow::Result<()> {
    // The real crosswalk declares 99999 a hub placeholder: the register's own PNU and the other
    // parcel's are both null, counted, and the export still completes.
    let layout = crate::building_register_sub_parcel_silver_export::layout()?;
    let zip = test_support::fixture_named(&row("synthetic-1", "0", "0013"), &layout.inner_file)?;
    let crosswalk = crate::sigungu_crosswalk::baseline_crosswalk()?;
    let report = test_support::convert_fixture_via(zip, layout, 2, &crosswalk)?;
    assert_eq!(report["pnu_bad"], 1);
    assert_eq!(report["extra_pnu_ok"], json!({"sub_pnu": 0}));
    assert!(report["decoded_rows"][0]["sub_pnu"].is_null());
    Ok(())
}

#[test]
fn an_extra_pnu_must_name_consumed_columns() -> anyhow::Result<()> {
    let mut layout = crate::building_register_sub_parcel_silver_export::layout()?;
    if let Some(inputs) = layout.extra_pnus.get_mut("sub_pnu") {
        inputs.bonbeon = "not_a_column".into();
    }
    assert!(layout.validate().is_err());
    Ok(())
}
