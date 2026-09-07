use super::*;

const HEADER: &str = "고유번호|대지권일련번호|법정동코드|법정동명|지번|대장구분코드|대장구분명|건축물명|동명|층명|호명|실명|대지권비율|폐쇄구분코드|폐쇄구분명|관련토지소재지코드|데이터기준일자|원천시도시군구코드";
const ROW: &str = r#"9999938029104450003|0001|9999938029|가상도 가상군 가상리|445-3|1|토지대장|" 가상|건물,명 ""별관"" "| 동01 | 지하1층 |001호| 실A | 3분의1 |1|폐쇄|9999938029104450003|2026-06-09|99999"#;

pub(crate) async fn fixture_handoff() -> anyhow::Result<String> {
    let second = ROW
        .replace("|0001|", "|214748364800000000000|")
        .replace("| 실A |", "||")
        .replace("|1|폐쇄|", "|0|미폐쇄|");
    Ok(export_csv(
        HEADER,
        &format!("{ROW}\n{second}"),
        "AL_D006_99_20260609.csv",
    )
    .await?
    .1)
}

async fn export_csv(
    header: &str,
    rows: &str,
    member: &str,
) -> anyhow::Result<(ExportReport, String)> {
    let temp = std::env::temp_dir().join(format!("land-right-csv-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp)?;
    let result = async {
        let input = temp.join("source.zip");
        let output = temp.join("handoff.jsonl.gz");
        let mut zip = zip::ZipWriter::new(fs::File::create(&input)?);
        zip.start_file::<_, ()>(member, zip::write::FileOptions::default())?;
        let text = format!("{header}\r\n{rows}\r\n");
        let (bytes, _, malformed) = encoding_rs::EUC_KR.encode(&text);
        assert!(!malformed);
        zip.write_all(&bytes)?;
        zip.finish()?;
        let config = ExportConfig {
            input: InputSource::LocalPath(input),
            output: OutputSink::LocalPath(output.clone()),
            summary_path: None,
            source_snapshot_id: "SYNTHETIC-test".into(),
        };
        let report = export_handoff(&config, &LAND_RIGHT_LANE).await?;
        let mut body = String::new();
        flate2::read::GzDecoder::new(fs::File::open(output)?).read_to_string(&mut body)?;
        Ok((report, body))
    }
    .await;
    fs::remove_dir_all(temp)?;
    result
}

#[test]
fn pipe_delimited_records_preserve_quotes_commas_newlines_and_empty_fields() -> anyhow::Result<()> {
    let mut records = CsvRecords::new(&b"a|b|c\r\n\"x|,\"\"y\nend\"|2|\ntail|1|"[..], b'|');
    assert_eq!(
        records.next_record()?.unwrap(),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(
        records.next_record()?.unwrap(),
        vec![b"x|,\"y\nend".to_vec(), b"2".to_vec(), vec![]]
    );
    assert_eq!(
        records.next_record()?.unwrap(),
        vec![b"tail".to_vec(), b"1".to_vec(), vec![]]
    );
    assert!(records.next_record()?.is_none());
    let mut broken = CsvRecords::new(&b"a|\"broken"[..], b'|');
    assert!(broken.next_record().is_err());
    Ok(())
}

#[tokio::test]
async fn land_right_csv_preserves_every_registration_and_verbatim_registry_field(
) -> anyhow::Result<()> {
    let body = fixture_handoff().await?;
    let rows: Vec<serde_json::Value> = body
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["pnu"], rows[1]["pnu"]);
    assert_eq!(rows[1]["right_serial_no"], "214748364800000000000");
    assert!(rows[1]["room_name"].is_null());
    for (key, value) in [
        ("pnu", "9999938029104450003"),
        ("right_serial_no", "0001"),
        ("legal_dong_code", "9999938029"),
        ("legal_dong_name", "가상도 가상군 가상리"),
        ("jibun", "445-3"),
        ("ledger_kind_code", "1"),
        ("ledger_kind_name", "토지대장"),
        ("building_name", " 가상|건물,명 \"별관\" "),
        ("dong_name", " 동01 "),
        ("floor_name", " 지하1층 "),
        ("ho_name", "001호"),
        ("room_name", " 실A "),
        ("right_ratio", " 3분의1 "),
        ("closure_kind_code", "1"),
        ("closure_kind_name", "폐쇄"),
        ("related_parcel_code", "9999938029104450003"),
        ("data_reference_date", "2026-06-09"),
        ("source_sigungu_code", "99999"),
        ("source_snapshot_id", "SYNTHETIC-test"),
    ] {
        assert_eq!(rows[0][key], value, "{key}");
    }
    let keys: std::collections::BTreeSet<_> = rows[0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        SILVER_LAND_RIGHT_REGISTRATION
            .columns
            .iter()
            .map(|c| c.name)
            .collect()
    );
    Ok(())
}

#[tokio::test]
async fn land_right_csv_refuses_header_drift_wrong_delimiter_and_change_feed() {
    for header in [
        HEADER.replace("대지권일련번호", "일련번호"),
        HEADER.replace("동명|층명", "층명|동명"),
        HEADER.replace('|', ","),
    ] {
        let error = export_csv(&header, ROW, "AL_D006_99_20260609.csv")
            .await
            .expect_err("header drift");
        assert!(error.to_string().contains("header"), "{error}");
    }
    let error = export_csv(HEADER, ROW, "CH_D006_99_20260609.csv")
        .await
        .expect_err("change feed");
    assert!(error.to_string().contains("no member"), "{error}");
}

#[tokio::test]
async fn land_right_csv_rejects_non_digit_keys_without_parsing_identifiers_or_ratios(
) -> anyhow::Result<()> {
    let rows = [
        ROW.to_owned(),
        ROW.replace("9999938029104450003", "invalid"),
    ]
    .into_iter()
    .chain(
        ["", "1.5", "-1", " 0001 ", "일"]
            .map(|serial| ROW.replace("|0001|", &format!("|{serial}|"))),
    )
    .collect::<Vec<_>>()
    .join("\n");
    let (report, _) = export_csv(HEADER, &rows, "AL_D006_99_20260609.csv").await?;
    assert_eq!(
        (
            report.input_row_count,
            report.output_row_count,
            report.rejected_row_count
        ),
        (7, 1, 6)
    );
    assert_eq!(
        report.rejected_row_reasons.get("invalid_right_serial_no"),
        Some(&4)
    );
    assert_eq!(
        report.rejected_row_reasons.get("blank_right_serial_no"),
        Some(&1)
    );
    for ratio in ["0", "1/3", "", "   "] {
        let (_, body) = export_csv(
            HEADER,
            &ROW.replace(" 3분의1 ", ratio),
            "AL_D006_99_20260609.csv",
        )
        .await?;
        let row: serde_json::Value = serde_json::from_str(body.trim())?;
        assert_eq!(
            row["right_ratio"],
            if ratio.is_empty() {
                serde_json::Value::Null
            } else {
                json!(ratio)
            }
        );
    }
    Ok(())
}
