use super::*;

const HEADER: &str = "고유번호,법정동코드,법정동명,지번,대장구분코드,대장구분명,지목코드,지목명,면적,소유구분코드,소유구분명,소유(공유)인수,축척구분코드,축척구분명,데이터기준일자,원천시도시군구코드";
const ROW: &str = "9999938029204450003,9999938029,가상도 가상군 가상리,산445-3,2,임야대장,05,임야,123.45,01,개인,2,03,6000분의1,2026-06-07,99999";

pub(crate) async fn fixture_handoff() -> anyhow::Result<String> {
    Ok(export_csv(HEADER, ROW, "AL_D003_99_20260607.csv").await?.1)
}

async fn export_csv(
    header: &str,
    rows: &str,
    member: &str,
) -> anyhow::Result<(ExportReport, String)> {
    let temp = std::env::temp_dir().join(format!("forest-csv-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp)?;
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
    let result = async {
        let report = export_handoff(&config, &LAND_FOREST_LANE).await?;
        let mut body = String::new();
        flate2::read::GzDecoder::new(fs::File::open(output)?).read_to_string(&mut body)?;
        Ok((report, body))
    }
    .await;
    fs::remove_dir_all(temp)?;
    result
}

#[tokio::test]
async fn land_forest_csv_preserves_every_named_field_and_numeric_values() -> anyhow::Result<()> {
    let (report, body) = export_csv(HEADER, ROW, "AL_D003_99_20260607.csv").await?;
    assert_eq!(
        (
            report.input_row_count,
            report.output_row_count,
            report.rejected_row_count
        ),
        (1, 1, 0)
    );
    let row: serde_json::Value = serde_json::from_str(body.trim())?;
    for (key, value) in [
        ("pnu", "9999938029204450003"),
        ("legal_dong_code", "9999938029"),
        ("legal_dong_name", "가상도 가상군 가상리"),
        ("jibun", "산445-3"),
        ("ledger_kind_code", "2"),
        ("ledger_kind_name", "임야대장"),
        ("land_category_code", "05"),
        ("land_category", "임야"),
        ("ownership_kind_code", "01"),
        ("ownership_kind_name", "개인"),
        ("scale_code", "03"),
        ("scale_name", "6000분의1"),
        ("data_reference_date", "2026-06-07"),
        ("source_sigungu_code", "99999"),
        ("source_snapshot_id", "SYNTHETIC-test"),
    ] {
        assert_eq!(row[key], value, "{key}");
    }
    assert_eq!(row["area_m2"], 123.45);
    assert_eq!(row["co_owner_count"], 2);
    let keys: std::collections::BTreeSet<_> = row
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        SILVER_LAND_FOREST_LEDGER
            .columns
            .iter()
            .map(|c| c.name)
            .collect()
    );
    Ok(())
}

#[tokio::test]
async fn land_forest_csv_refuses_header_drift_and_change_feed() {
    for header in [
        HEADER.replace("지목명", "지목"),
        HEADER.replace("소유(공유)인수", "소유자"),
        HEADER.replace("법정동명,지번", "지번,법정동명"),
    ] {
        let error = export_csv(&header, ROW, "AL_D003_99_20260607.csv")
            .await
            .expect_err("header drift");
        assert!(error.to_string().contains("header"), "{error}");
    }
    let error = export_csv(HEADER, ROW, "CH_D003_99_20260607.csv")
        .await
        .expect_err("change feed");
    assert!(error.to_string().contains("no member"), "{error}");
}

#[tokio::test]
async fn land_forest_csv_counts_invalid_pnu_area_and_co_owner_count() -> anyhow::Result<()> {
    let rows = [
        ROW.to_owned(),
        ROW.replace("9999938029204450003", "invalid"),
        ROW.replace("123.45", "NaN"),
        ROW.replace("123.45", "0"),
        ROW.replace("123.45", "-1"),
        ROW.replace("123.45", ""),
        ROW.replace(",2,03,", ",-1,03,"),
        ROW.replace(",2,03,", ",1.5,03,"),
        ROW.replace(",2,03,", ",2147483648,03,"),
        ROW.replace(",2,03,", ",bad,03,"),
    ]
    .join("\n");
    let (report, _) = export_csv(HEADER, &rows, "AL_D003_99_20260607.csv").await?;
    assert_eq!(
        (
            report.input_row_count,
            report.output_row_count,
            report.rejected_row_count
        ),
        (10, 1, 9)
    );
    assert_eq!(report.rejected_row_reasons.get("invalid_area_m2"), Some(&3));
    assert_eq!(report.rejected_row_reasons.get("blank_area_m2"), Some(&1));
    assert_eq!(
        report.rejected_row_reasons.get("invalid_co_owner_count"),
        Some(&4)
    );
    Ok(())
}

#[tokio::test]
async fn land_forest_csv_preserves_blank_and_zero_co_owner_counts() -> anyhow::Result<()> {
    for (replacement, expected) in [("", serde_json::Value::Null), ("0", json!(0))] {
        let (_, body) = export_csv(
            HEADER,
            &ROW.replace(",2,03,", &format!(",{replacement},03,")),
            "AL_D003_99_20260607.csv",
        )
        .await?;
        let row: serde_json::Value = serde_json::from_str(body.trim())?;
        assert_eq!(row["co_owner_count"], expected);
    }
    Ok(())
}
