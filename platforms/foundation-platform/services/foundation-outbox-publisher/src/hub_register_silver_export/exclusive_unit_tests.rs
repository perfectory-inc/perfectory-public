use super::*;

fn row(san: &str) -> String {
    format!("synthetic-unit|2|집합|4|전유부|가상주소|가상도로| 이름,\"원문\" |99999|00101|{san}|0012|0003||||road|code|0|461||102동|628호|20|지상|6|20220813")
}

fn convert_text(text: &str, rows_per_part: u64) -> anyhow::Result<Value> {
    let layout = crate::building_register_exclusive_unit_silver_export::layout()?;
    let zip = test_support::fixture_named(text, &layout.inner_file)?;
    test_support::convert_fixture(zip, layout, rows_per_part)
}

#[test]
fn exclusive_unit_stream_preserves_labels_raw_fields_and_invalid_pnus() -> anyhow::Result<()> {
    let text = format!(
        "{}\r\n{}\n{}\n{}",
        row("0"),
        row("1"),
        row("2"),
        row("0").replace("99999", "bad")
    );
    let report = convert_text(&text, 2)?;
    assert_eq!(report["rows_read"], 4);
    assert_eq!(report["rows_emitted"], 4);
    assert_eq!(report["pnu_ok"], 2);
    assert_eq!(report["pnu_bad"], 2);
    assert_eq!(report["rejected_rows"], 0);
    assert_eq!(report["vintage"], "202608");
    let rows = report["decoded_rows"].as_array().context("expected rows")?;
    let parts = report["parts"].as_array().context("expected parts")?;
    assert_eq!(parts.len(), 2);
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[1]["pnu"], "9999900101200120003");
    assert!(rows[2]["pnu"].is_null());
    assert!(rows[3]["pnu"].is_null());
    for (index, emitted) in rows.iter().enumerate() {
        let source_line = text.lines().nth(index).context("expected source line")?;
        assert_eq!(
            emitted["raw_columns"],
            json!(source_line.split('|').collect::<Vec<_>>())
        );
        assert_eq!(
            emitted["raw_columns"]
                .as_array()
                .context("expected array")?
                .len(),
            27
        );
        assert_eq!(emitted["mgmt_key"], "synthetic-unit");
        assert_eq!(emitted["dong_name"], "102동");
        assert_eq!(emitted["ho_name"], "628호");
        assert_eq!(emitted["floor_kind"], "지상");
        assert_eq!(emitted["floor_no"], "6");
        assert_eq!(emitted["source_line_number"], index + 1);
        assert_eq!(emitted["source_record_id"], report["input_object_key"]);
        assert_eq!(emitted["source_part_id"], parts[index / 2]["object_key"]);
        assert_eq!(emitted["source_snapshot_id"], "SYNTHETIC-test");
        assert!(chrono::DateTime::parse_from_rfc3339(
            emitted["ingested_at_utc"].as_str().context("timestamp")?
        )
        .is_ok());
        let names: std::collections::BTreeSet<_> = emitted
            .as_object()
            .context("row object")?
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            names,
            lakehouse_domain::SILVER_BUILDING_REGISTER_EXCLUSIVE_UNIT
                .columns
                .iter()
                .map(|c| c.name)
                .collect()
        );
    }
    Ok(())
}

#[test]
fn exclusive_unit_refuses_26_and_28_columns_before_publication() -> anyhow::Result<()> {
    let original = row("0");
    let short = original.rsplit_once('|').context("last field")?.0;
    for text in [short.to_owned(), format!("{original}|extra")] {
        let error = convert_text(&text, 1)
            .err()
            .context("layout drift must fail")?;
        assert!(
            error.to_string().contains("first row column count"),
            "{error:#}"
        );
    }
    Ok(())
}

#[test]
fn exclusive_unit_rejects_later_wrong_width_and_retains_raw_floor_text() -> anyhow::Result<()> {
    let text = format!(
        "{}\nshort|row\n{}",
        row("0"),
        row("2").replace("|지상|6|", "|지하|B1|")
    );
    let report = convert_text(&text, 1)?;
    assert_eq!(report["rows_read"], 3);
    assert_eq!(report["rows_emitted"], 2);
    assert_eq!(report["rejected_rows"], 1);
    assert_eq!(report["pnu_bad"], 1);
    assert_eq!(report["decoded_rows"][1]["source_line_number"], 3);
    assert_eq!(report["decoded_rows"][1]["floor_kind"], "지하");
    assert_eq!(report["decoded_rows"][1]["floor_no"], "B1");
    Ok(())
}

#[test]
fn exclusive_unit_layout_cannot_drop_labels_or_consume_price_fields() -> anyhow::Result<()> {
    let original = crate::building_register_exclusive_unit_silver_export::layout()?;
    for name in ["dong_name", "ho_name", "floor_kind", "floor_no"] {
        let mut missing = original.clone();
        missing.columns.remove(name);
        assert!(missing.validate().is_err());
    }
    let mut wrong_lane = original.clone();
    let column = wrong_lane.columns.remove("ho_name").context("ho_name")?;
    wrong_lane.columns.insert("price_won".into(), column);
    assert!(wrong_lane.validate().is_err());
    let mut duplicate = original.clone();
    duplicate
        .columns
        .get_mut("ho_name")
        .context("ho_name")?
        .index = 21;
    assert!(duplicate.validate().is_err());
    let mut out_of_range = original;
    out_of_range
        .columns
        .get_mut("ho_name")
        .context("ho_name")?
        .index = 27;
    assert!(out_of_range.validate().is_err());
    Ok(())
}

#[test]
fn exclusive_unit_integrity_failure_never_publishes_a_completed_manifest() -> anyhow::Result<()> {
    let layout = crate::building_register_exclusive_unit_silver_export::layout()?;
    let mut zip =
        test_support::fixture_named(&format!("{}\n{}", row("0"), row("1")), &layout.inner_file)?;
    let central = zip
        .windows(4)
        .position(|w| w == b"PK\x01\x02")
        .context("ZIP directory")?;
    zip[central + 16] ^= 1;
    let error = test_support::convert_fixture(zip, layout, 1)
        .err()
        .context("CRC must fail")?;
    assert!(error.to_string().contains("CRC/size"), "{error:#}");
    Ok(())
}
