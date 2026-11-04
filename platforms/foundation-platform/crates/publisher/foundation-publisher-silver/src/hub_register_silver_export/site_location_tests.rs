use super::*;

/// A synthetic 대지위치 row: 20 columns, the provider's site key in column 0, 시군구 in 5 and 법정동
/// in 9 with the 행정동 between them.
fn row(key: &str, permit: &str, building: &str, san: &str) -> String {
    let mut fields = vec![""; 20];
    fields[0] = key;
    fields[1] = permit;
    fields[2] = building;
    fields[3] = "1";
    fields[4] = "가상 대지위치";
    fields[5] = "99999";
    fields[6] = "0";
    fields[7] = "주건축물";
    fields[8] = "55500";
    fields[9] = "00101";
    fields[10] = san;
    fields[11] = "0012";
    fields[12] = "0003";
    fields[16] = "09";
    fields[17] = "공장용지";
    fields.join("|")
}

fn convert_text(text: &str) -> anyhow::Result<Value> {
    let layout = crate::building_permit_site_location_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_site_row_composes_its_pnu_from_the_legal_dong_not_the_administrative_one() -> anyhow::Result<()>
{
    let text = [
        row("synthetic-site-1", "synthetic-permit", "", "0"),
        row("synthetic-site-2", "", "synthetic-dong", "1"),
        row("synthetic-site-3", "synthetic-permit", "", ""),
    ]
    .join("\n");
    let report = convert_text(&text)?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 2);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    assert_eq!(rows[0]["mgmt_key"], "synthetic-site-1");
    assert_eq!(rows[0]["permit_mgmt_key"], "synthetic-permit");
    assert_eq!(rows[1]["building_mgmt_key"], "synthetic-dong");
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[1]["pnu"], "9999900101200120003");
    assert_eq!(rows[0]["admin_dong_cd"], "55500");
    // An empty HUB 대지구분 names no lot kind, so no PNU is guessed (FP-ADR-0023).
    assert!(rows[2]["pnu"].is_null());
    assert_eq!(rows[0]["representative_flag"], "1");
    assert_eq!(rows[0]["land_category_name"], "공장용지");
    assert_eq!(rows[0]["raw_columns"][4], "가상 대지위치");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 20);
    Ok(())
}
