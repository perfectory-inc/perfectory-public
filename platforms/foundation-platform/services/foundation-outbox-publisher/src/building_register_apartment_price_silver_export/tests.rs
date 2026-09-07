use super::*;
use std::io::{Cursor, SeekFrom, Write};

struct Fragmented(Cursor<Vec<u8>>);

impl Read for Fragmented {
    fn read(&mut self, target: &mut [u8]) -> std::io::Result<usize> {
        let count = target.len().min(7);
        self.0.read(&mut target[..count])
    }
}

impl Seek for Fragmented {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(position)
    }
}

fn convert_fixture(zip: Vec<u8>, rows_per_part: u64) -> anyhow::Result<Value> {
    let temp = std::env::temp_dir().join(format!("hub-price-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temp)?;
    let result = (|| {
        let mut layout = Layout::embedded()?;
        layout.rows_per_part = rows_per_part;
        let config = Config {
            input_object_key: layout.selected()?.object_key.clone(),
            output_prefix: OutputSink::R2Object(
                "silver-handoff/hubgokr__building_register_apartment_price".into(),
            ),
            source_snapshot_id: "SYNTHETIC-test".into(),
            summary_path: Some(temp.join("summary.json")),
        };
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let result = convert(Fragmented(Cursor::new(zip)), &config, &layout, |output| {
            runtime.block_on(open_sink(
                &OutputSink::LocalPath(temp.join(output_name(output))),
                None,
            ))
        });
        let manifest = temp.join(output_name(
            &config.output(&format!("{}/manifest.json", config.object_stem()?)),
        ));
        if result.is_err() {
            assert!(!manifest.exists(), "failed conversion published a manifest");
            assert!(
                !temp.join("summary.json").exists(),
                "failed conversion published summary"
            );
        }
        let report = result?;
        let mut value = serde_json::to_value(&report)?;
        let persisted: Value = serde_json::from_slice(&std::fs::read(manifest)?)?;
        assert_eq!(persisted, value);
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(temp.join("summary.json"))?)?,
            value
        );
        let mut rows = Vec::<Value>::new();
        for part in &report.parts {
            let file = std::fs::File::open(temp.join(&part.object_key))?;
            let mut text = String::new();
            flate2::read::GzDecoder::new(file).read_to_string(&mut text)?;
            let decoded = text
                .lines()
                .map(serde_json::from_str::<Value>)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(decoded.len() as u64, part.rows);
            rows.extend(decoded);
        }
        value["decoded_rows"] = json!(rows);
        Ok(value)
    })();
    std::fs::remove_dir_all(temp)?;
    result
}

fn row(san: &str) -> String {
    format!("synthetic|2|집합|4|전유부|가상주소|가상도로| 이름,\"원문\" |99999|00101|{san}|0012|0003||||0|road|code|0|120||20090101|208000000|20220625")
}

fn fixture(text: &str) -> anyhow::Result<Vec<u8>> {
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    archive.start_file(
        "mart_djy_08.txt",
        zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .large_file(true),
    )?;
    archive.write_all(text.as_bytes())?;
    Ok(archive.finish()?.into_inner())
}

#[test]
fn streams_zip64_preserves_raw_columns_counts_pnus_and_rotates() -> anyhow::Result<()> {
    let text = format!(
        "{}\r\n{}\n{}\n{}\n",
        row("0"),
        row("1"),
        row("2"),
        row("0").replace("99999", "bad")
    );
    let result = convert_fixture(fixture(&text)?, 2)?;
    assert_eq!(result["rows_read"], 4);
    assert_eq!(result["rows_emitted"], 4);
    assert_eq!(result["pnu_ok"], 2);
    assert_eq!(result["pnu_bad"], 2);
    assert_eq!(result["rejected_rows"], 0);
    assert_eq!(
        result["parts"]
            .as_array()
            .context("expected JSON array")?
            .len(),
        2
    );
    let rows = result["decoded_rows"]
        .as_array()
        .context("expected JSON array")?;
    assert_eq!(rows[0]["pnu"], "9999900101100120003");
    assert_eq!(rows[1]["pnu"], "9999900101200120003");
    assert!(rows[2]["pnu"].is_null());
    assert!(rows[3]["pnu"].is_null());
    assert_eq!(rows[0]["raw_columns"][7], " 이름,\"원문\" ");
    assert_eq!(
        rows[0]["raw_columns"]
            .as_array()
            .context("expected JSON array")?
            .len(),
        25
    );
    assert_eq!(rows[0]["raw_columns"][13], "");
    assert_eq!(rows[0]["source_part_id"], rows[1]["source_part_id"]);
    assert_ne!(rows[1]["source_part_id"], rows[2]["source_part_id"]);
    assert_eq!(rows[3]["source_line_number"], 4);
    let columns: std::collections::BTreeSet<_> = rows[0]
        .as_object()
        .context("expected JSON object")?
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        columns,
        lakehouse_domain::SILVER_BUILDING_REGISTER_APARTMENT_PRICE
            .columns
            .iter()
            .map(|c| c.name)
            .collect()
    );
    Ok(())
}

#[test]
fn refuses_first_row_layout_drift() -> anyhow::Result<()> {
    for text in [format!("{}|extra\n", row("0")), row("0").replace('|', ",")] {
        let error = convert_fixture(fixture(&text)?, 2)
            .err()
            .context("first row must refuse the file")?;
        assert!(
            error.to_string().contains("first row column count"),
            "{error:#}"
        );
    }
    Ok(())
}

#[test]
fn rejects_later_bad_width_without_losing_pnu_bad_rows() -> anyhow::Result<()> {
    let result = convert_fixture(
        fixture(&format!("{}\nshort|row\n{}", row("0"), row("2")))?,
        2,
    )?;
    assert_eq!(result["rows_read"], 3);
    assert_eq!(result["rows_emitted"], 2);
    assert_eq!(result["rejected_rows"], 1);
    assert_eq!(result["pnu_bad"], 1);
    assert_eq!(result["decoded_rows"][1]["source_line_number"], 3);
    assert_eq!(
        result["parts"]
            .as_array()
            .context("expected JSON array")?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn crc_failure_after_rotation_does_not_publish_manifest() -> anyhow::Result<()> {
    let mut zip = fixture(&format!("{}\n{}\n{}\n", row("0"), row("0"), row("1")))?;
    let central = zip
        .windows(4)
        .position(|w| w == b"PK\x01\x02")
        .context("expected fixture value")?;
    zip[central + 16] ^= 1;
    let error = convert_fixture(zip, 1)
        .err()
        .context("corrupt CRC must fail after writing parts")?;
    assert!(error.to_string().contains("CRC/size"), "{error:#}");
    Ok(())
}

#[test]
fn refuses_truncated_wrong_member_invalid_utf8_and_oversized_rows() -> anyhow::Result<()> {
    let mut truncated = fixture(&row("0"))?;
    truncated.truncate(truncated.len() - 22);
    assert!(convert_fixture(truncated, 2).is_err());
    let mut wrong_name = fixture(&row("0"))?;
    wrong_name[30] = b'x';
    assert!(convert_fixture(wrong_name, 2).is_err());
    let error = convert_fixture(fixture(&"x".repeat(1024 * 1024 + 1))?, 2)
        .err()
        .context("expected conversion failure")?;
    assert!(error.to_string().contains("max_row_bytes"));
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    zip.start_file(
        "mart_djy_08.txt",
        zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated),
    )?;
    zip.write_all(&[0xff, b'\n'])?;
    let error = convert_fixture(zip.finish()?.into_inner(), 2)
        .err()
        .context("expected conversion failure")?;
    assert!(error.to_string().contains("UTF-8"));
    Ok(())
}

#[test]
fn layout_refuses_duplicate_or_missing_positions_and_old_selected_vintage() -> anyhow::Result<()> {
    let original = Layout::embedded()?;
    let mut duplicate = original.clone();
    duplicate
        .columns
        .get_mut("price_won")
        .context("expected fixture value")?
        .index = 0;
    assert!(duplicate.validate().is_err());
    let mut out_of_range = original.clone();
    out_of_range
        .columns
        .get_mut("price_won")
        .context("expected fixture value")?
        .index = out_of_range.column_count;
    assert!(out_of_range.validate().is_err());
    let mut missing = original.clone();
    missing.columns.remove("price_won");
    assert!(missing.validate().is_err());
    let mut old = original;
    old.selected_vintage = "202607".into();
    assert!(old.validate().is_err());
    Ok(())
}

#[test]
fn empty_and_malformed_pnu_components_preserve_the_row() -> anyhow::Result<()> {
    let rows = ["|99999|00101|", "|0012|0003|"];
    for (needle, replacements) in [
        (rows[0], vec!["||00101|", "|999999|00101|", "|99999|한글|"]),
        (rows[1], vec!["||0003|", "|12345|0003|", "|0012||"]),
    ] {
        for replacement in replacements {
            let result = convert_fixture(fixture(&row("0").replace(needle, replacement))?, 2)?;
            assert_eq!(result["pnu_bad"], 1);
            assert_eq!(result["rows_emitted"], 1);
            assert!(result["decoded_rows"][0]["pnu"].is_null());
        }
    }
    Ok(())
}
