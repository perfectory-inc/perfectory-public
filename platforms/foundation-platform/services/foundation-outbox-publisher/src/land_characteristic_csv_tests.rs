use super::*;

const HEADER: &str = "고유번호,법정동코드,법정동명,대장구분코드,대장구분명,지번,토지일련번호,기준연도,기준월,지목코드,지목명,토지면적,용도지역코드1,용도지역명1,용도지역코드2,용도지역명2,토지이용상황코드,토지이용상황,지형높이코드,지형높이,지형형상코드,지형형상,도로접면코드,도로접면,공시지가,데이터기준일자";
const ROW: &str = "9999938029104450003,9999938029,가상도 가상군 가상리,1,토지대장,445-3,123,2025,01,08,대,123.45,100,제1종일반주거지역,200,기타,300,공업용,400,평지,500,정방형,600,광대로,81700,2026-05-19";

pub(crate) async fn fixture_handoff() -> anyhow::Result<String> {
    Ok(export_csv(HEADER, ROW, "AL_D195_99_20260519.csv").await?.1)
}

async fn export_csv(
    header: &str,
    rows: &str,
    member: &str,
) -> anyhow::Result<(ExportReport, String)> {
    let temp = std::env::temp_dir().join(format!("characteristic-csv-{}", uuid::Uuid::new_v4()));
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
        source_snapshot_id: "characteristic:test-20260519".into(),
    };
    let result = async {
        let report = export_handoff(&config, &LAND_CHARACTERISTIC_LANE).await?;
        let mut body = String::new();
        flate2::read::GzDecoder::new(fs::File::open(output)?).read_to_string(&mut body)?;
        Ok((report, body))
    }
    .await;
    fs::remove_dir_all(temp)?;
    result
}

#[tokio::test]
async fn land_characteristic_csv_preserves_named_fields_and_numeric_area_without_price(
) -> anyhow::Result<()> {
    let (report, body) = export_csv(HEADER, ROW, "AL_D195_99_20260519.csv").await?;
    assert_eq!(
        (
            report.input_row_count,
            report.output_row_count,
            report.rejected_row_count
        ),
        (1, 1, 0)
    );
    let row: serde_json::Value = serde_json::from_str(body.trim())?;
    assert_eq!(row["pnu"], "9999938029104450003");
    assert_eq!(row["land_category"], "대");
    assert_eq!(row["area_m2"], 123.45);
    assert_eq!(row["zone_code_1"], "100");
    assert_eq!(row["zone_name_2"], "기타");
    assert_eq!(row["land_use_situation"], "공업용");
    assert_eq!(row["terrain_height"], "평지");
    assert_eq!(row["terrain_shape"], "정방형");
    assert_eq!(row["road_contact"], "광대로");
    assert_eq!(row["data_reference_date"], "2026-05-19");
    assert_eq!(row["source_snapshot_id"], "characteristic:test-20260519");
    for column in [
        "official_price",
        "verification_price_per_m2",
        "source_vintage",
    ] {
        assert!(row.get(column).is_none(), "unexpected column {column}");
    }
    let keys: std::collections::BTreeSet<_> = row
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        SILVER_LAND_CHARACTERISTIC
            .columns
            .iter()
            .map(|c| c.name)
            .collect()
    );
    Ok(())
}

#[tokio::test]
async fn land_characteristic_csv_refuses_header_drift_and_shapefile_siblings() {
    for header in [
        HEADER.replace("지목명", "지목"),
        HEADER.replace("공시지가", "새열"),
        HEADER.replace("지형높이,지형형상코드", "지형형상코드,지형높이"),
    ] {
        let error = export_csv(&header, ROW, "AL_D195_99_20260519.csv")
            .await
            .expect_err("header drift");
        assert!(error.to_string().contains("header"), "{error}");
    }
    let error = export_csv(HEADER, ROW, "AL_D194_99999_20260519.dbf")
        .await
        .expect_err("SHP sibling");
    assert!(error.to_string().contains("no member"), "{error}");
}

#[tokio::test]
async fn land_characteristic_csv_counts_invalid_pnu_and_area() -> anyhow::Result<()> {
    let rows = [
        ROW.to_owned(),
        ROW.replace("9999938029104450003", "invalid"),
        ROW.replace("123.45", "bad"),
        ROW.replace("123.45", "NaN"),
        ROW.replace("123.45", "0"),
        ROW.replace("123.45", "-1"),
        ROW.replace("123.45", ""),
    ]
    .join("\n");
    let (report, _) = export_csv(HEADER, &rows, "AL_D195_99_20260519.csv").await?;
    assert_eq!(
        (
            report.input_row_count,
            report.output_row_count,
            report.rejected_row_count
        ),
        (7, 1, 6)
    );
    assert_eq!(report.rejected_row_reasons.get("invalid_area_m2"), Some(&4));
    assert_eq!(report.rejected_row_reasons.get("blank_area_m2"), Some(&1));
    Ok(())
}
