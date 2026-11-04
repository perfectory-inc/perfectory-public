use super::*;

/// A synthetic 월별 전기 row: usage month, 순번, 지번, a road address and the usage.
fn row(seq: &str, bon: &str, usage: &str) -> String {
    format!("209908|{seq}|99999|00101|가상시|가상구|가상동|0|{bon}|0003|999990000001|가상로|0|지상|1|0|{usage}")
}

fn convert_text(text: &str, member: &str) -> anyhow::Result<Value> {
    let layout = crate::building_energy_monthly_silver_export::electricity_layout()?;
    let zip = test_support::fixture_named(text, member)?;
    test_support::convert_fixture(zip, layout, 2)
}

#[test]
fn a_parcels_rows_of_a_month_share_one_key_whatever_their_sequence() -> anyhow::Result<()> {
    let text = [
        row("1", "0012", "100"),
        row("2", "0012", "250"),
        row("1", "0013", "7"),
    ]
    .join("\n");
    let report = convert_text(&text, "eais_elcty_209908.txt")?;
    assert_eq!(report["rows_emitted"], 3);
    assert_eq!(report["pnu_ok"], 3);
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    // The key starts with the usage month, which the compare buckets by (root ADR-0396).
    assert_eq!(rows[0]["mgmt_key"], "209908|99999|00101|0|0012|0003");
    assert_eq!(rows[1]["mgmt_key"], rows[0]["mgmt_key"]);
    assert_eq!(rows[2]["mgmt_key"], "209908|99999|00101|0|0013|0003");
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[1]["usage_amount"], "250");
    assert_eq!(rows[0]["use_ym"], "209908");
    assert_eq!(rows[0]["raw_columns"].as_array().context("raw")?.len(), 17);
    Ok(())
}

#[test]
fn the_member_is_named_by_its_usage_month() -> anyhow::Result<()> {
    let layout = crate::building_energy_monthly_silver_export::electricity_layout()?;
    assert!(layout.accepts_member("eais_elcty_202606.txt"));
    assert!(!layout.accepts_member("eais_elcty_20260.txt"));
    assert!(!layout.accepts_member("eais_gas_202606.txt"));
    assert!(!layout.accepts_member("eais_elcty_2026ab.txt"));
    let gas = crate::building_energy_monthly_silver_export::gas_layout()?;
    assert!(gas.accepts_member("eais_gas_202606.txt"));
    // A fixed member name stays exact.
    let master = crate::building_register_master_silver_export::layout()?;
    assert!(master.accepts_member("mart_djy_02.txt"));
    assert!(!master.accepts_member("mart_djy_02.txt.bak"));
    assert!(convert_text(&row("1", "0012", "1"), "eais_gas_209908.txt").is_err());
    Ok(())
}
