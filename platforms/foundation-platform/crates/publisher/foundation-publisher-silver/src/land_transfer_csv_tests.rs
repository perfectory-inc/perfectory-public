use super::*;

const HEADER: &str = "고유번호,법정동코드,법정동명,대장구분코드,대장구분명,지번,토지이동이력순번,폐쇄순번,지목코드,지목,토지면적,토지이동사유코드,토지이동사유,토지이동일자,토지이동말소일자,토지이력순번,데이터기준일자,원천시도시군구코드";
const ROW: &str = "9999938029104450003,9999938029,가상도 가상군 가상리,1,토지대장,445-3,1,01,08,대,123.45,01,95번에서 분할,2002-05-23,2005-06-08,007,2026-05-31,99999";

pub(crate) async fn fixture_handoff() -> anyhow::Result<String> {
    let second = ROW.replace(",1,01,08,", ",2147483648,02,08,").replace(
        "95번에서 분할,2002-05-23,2005-06-08",
        "구획정리 시행신고,2005-06-08,",
    );
    Ok(export_csv(
        HEADER,
        &format!("{ROW}\n{second}"),
        "AL_D157_99_20260531.csv",
    )
    .await?
    .1)
}

async fn export_csv(
    header: &str,
    rows: &str,
    member: &str,
) -> anyhow::Result<(ExportReport, String)> {
    let temp = std::env::temp_dir().join(format!("transfer-csv-{}", uuid::Uuid::new_v4()));
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
        let report = export_handoff(&config, &LAND_TRANSFER_LANE).await?;
        let mut body = String::new();
        flate2::read::GzDecoder::new(fs::File::open(output)?).read_to_string(&mut body)?;
        Ok((report, body))
    }
    .await;
    fs::remove_dir_all(temp)?;
    result
}

#[tokio::test]
async fn land_transfer_csv_preserves_every_event_and_named_field() -> anyhow::Result<()> {
    let body = fixture_handoff().await?;
    let rows: Vec<serde_json::Value> = body
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["pnu"], rows[1]["pnu"]);
    assert_eq!(rows[0]["transfer_history_seq"], 1);
    assert_eq!(rows[1]["transfer_history_seq"], 2_147_483_648_i64);
    assert_eq!(rows[1]["reason"], "구획정리 시행신고");
    assert_eq!(rows[1]["moved_at"], "2005-06-08");
    assert!(rows[1]["erased_at"].is_null());
    for (key, value) in [
        ("pnu", "9999938029104450003"),
        ("legal_dong_code", "9999938029"),
        ("legal_dong_name", "가상도 가상군 가상리"),
        ("ledger_kind_code", "1"),
        ("ledger_kind_name", "토지대장"),
        ("jibun", "445-3"),
        ("closure_seq", "01"),
        ("land_category_code", "08"),
        ("land_category", "대"),
        ("reason_code", "01"),
        ("reason", "95번에서 분할"),
        ("moved_at", "2002-05-23"),
        ("erased_at", "2005-06-08"),
        ("parcel_history_seq", "007"),
        ("data_reference_date", "2026-05-31"),
        ("source_sigungu_code", "99999"),
        ("source_snapshot_id", "SYNTHETIC-test"),
    ] {
        assert_eq!(rows[0][key], value, "{key}");
    }
    assert_eq!(rows[0]["area_m2"], 123.45);
    let keys: std::collections::BTreeSet<_> = rows[0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        SILVER_LAND_TRANSFER_HISTORY
            .columns
            .iter()
            .map(|c| c.name)
            .collect()
    );
    let mapped: Vec<_> = LAND_TRANSFER_LANE
        .csv_columns
        .iter()
        .flatten()
        .copied()
        .chain(LINEAGE_COLUMNS)
        .collect();
    assert_eq!(
        mapped,
        SILVER_LAND_TRANSFER_HISTORY
            .columns
            .iter()
            .map(|c| c.name)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn land_transfer_csv_refuses_header_drift_and_change_feed() {
    for header in [
        HEADER.replace("지목,", "지목명,"),
        HEADER.replace("폐쇄순번", "폐쇄번호"),
        HEADER.replace("법정동코드,법정동명", "법정동명,법정동코드"),
    ] {
        let error = export_csv(&header, ROW, "AL_D157_99_20260531.csv")
            .await
            .expect_err("header drift");
        assert!(error.to_string().contains("header"), "{error}");
    }
    let error = export_csv(HEADER, ROW, "CH_D157_99_20260531.csv")
        .await
        .expect_err("change feed");
    assert!(error.to_string().contains("no member"), "{error}");
}

#[tokio::test]
async fn land_transfer_csv_preserves_empty_and_zero_historic_area() -> anyhow::Result<()> {
    for (area, expected) in [("", serde_json::Value::Null), ("0", json!(0.0))] {
        let (report, body) = export_csv(
            HEADER,
            &ROW.replace("123.45", area),
            "AL_D157_99_20260531.csv",
        )
        .await?;
        assert_eq!(report.output_row_count, 1);
        let row: serde_json::Value = serde_json::from_str(body.trim())?;
        assert_eq!(row["area_m2"], expected);
        assert_eq!(row["closure_seq"], "01");
        assert_eq!(row["erased_at"], "2005-06-08");
    }
    Ok(())
}

#[tokio::test]
async fn land_transfer_csv_counts_invalid_keys_and_non_finite_area() -> anyhow::Result<()> {
    let rows = [
        ROW.to_owned(),
        ROW.replace("9999938029104450003", "invalid"),
        ROW.replace(",1,01,08,", ",,01,08,"),
        ROW.replace(",1,01,08,", ",1.5,01,08,"),
        ROW.replace(
            ",1,01,08,",
            &format!(",{},01,08,", i128::from(i64::MAX) + 1),
        ),
        ROW.replace("123.45", "NaN"),
    ]
    .join("\n");
    let (report, _) = export_csv(HEADER, &rows, "AL_D157_99_20260531.csv").await?;
    assert_eq!(
        (
            report.input_row_count,
            report.output_row_count,
            report.rejected_row_count
        ),
        (6, 1, 5)
    );
    assert_eq!(
        report
            .rejected_row_reasons
            .get("invalid_transfer_history_seq"),
        Some(&2)
    );
    assert_eq!(
        report
            .rejected_row_reasons
            .get("blank_transfer_history_seq"),
        Some(&1)
    );
    Ok(())
}
