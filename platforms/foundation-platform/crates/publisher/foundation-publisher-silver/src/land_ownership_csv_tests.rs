use super::*;

const HEADER: &str = "고유번호,법정동코드,법정동명,대장구분코드,대장구분명,지번,집합건물일련번호,건물동명,건물층명,건물호명,건물실명,공유인일련번호,기준연월,지목코드,지목,토지면적,공시지가,소유구분코드,소유구분,거주지구분코드,거주지구분,국가기관구분코드,국가기관구분,소유권변동원인코드,소유권변동원인,소유권변동일자,공유인수,데이터기준일자,원천시도시군구코드";
// Synthetic only (root rule: no real PNU or address in a fixture): a parcel owned by a company,
// then a unit of a collective building on it owned by a person.
const LAND_ROW: &str = "9999938029104450003,9999938029,가상도 가상군 가상리,1,토지대장,445-3,0000,,,,,000001,2024-12,09,공장용지,1650.0,812000,03,법인,03,읍면동내,ZZ,구분없음,03,소유권이전,2019-11-26,1,2025-08-26,99999";
const UNIT_ROW: &str = "9999938029104450003,9999938029,가상도 가상군 가상리,1,토지대장,445-3,0012, 110 ,2,204,0000,000003,2024-12,09,공장용지,40.40,812000,01,개인,04,시군구외,ZZ,구분없음,03,소유권이전,2024-09-13,2,2025-08-26,99999";

async fn export_csv(
    header: &str,
    rows: &str,
    member: &str,
) -> anyhow::Result<(ExportReport, String)> {
    let temp = std::env::temp_dir().join(format!("land-ownership-csv-{}", uuid::Uuid::new_v4()));
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
        let report = export_handoff(&config, &LAND_OWNERSHIP_LANE).await?;
        let mut body = String::new();
        flate2::read::GzDecoder::new(fs::File::open(output)?).read_to_string(&mut body)?;
        Ok((report, body))
    }
    .await;
    fs::remove_dir_all(temp)?;
    result
}

#[tokio::test]
async fn ownership_rows_keep_owner_facts_and_drop_the_facts_other_lanes_own() -> anyhow::Result<()>
{
    let negative = UNIT_ROW.replace(",2,2025-08-26,", ",-1,2025-08-26,");
    let (report, body) = export_csv(
        HEADER,
        &format!("{LAND_ROW}\n{UNIT_ROW}\n{negative}"),
        "AL_D161_99_20250831.csv",
    )
    .await?;
    assert_eq!(report.output_row_count, 2);
    assert_eq!(
        report.rejected_row_reasons.get("invalid_co_owner_count"),
        Some(&1)
    );
    let rows: Vec<serde_json::Value> = body
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    for (key, value) in [
        ("pnu", "9999938029104450003"),
        ("collective_building_serial_no", "0000"),
        ("co_owner_serial_no", "000001"),
        ("base_year_month", "2024-12"),
        ("ownership_kind_code", "03"),
        ("ownership_kind_name", "법인"),
        ("residence_kind_name", "읍면동내"),
        ("state_agency_kind_code", "ZZ"),
        ("ownership_change_reason", "소유권이전"),
        ("ownership_changed_on", "2019-11-26"),
        ("source_sigungu_code", "99999"),
    ] {
        assert_eq!(rows[0][key], value, "{key}");
    }
    assert_eq!(rows[0]["co_owner_count"], 1);
    assert!(rows[0]["dong_name"].is_null());
    // A unit's designations are registry facts, kept verbatim as the land-rights lane keeps them.
    assert_eq!(rows[1]["dong_name"], " 110 ");
    assert_eq!(rows[1]["ho_name"], "204");
    assert_eq!(rows[1]["ownership_kind_name"], "개인");
    let keys: std::collections::BTreeSet<_> = rows[0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        SILVER_LAND_OWNERSHIP
            .columns
            .iter()
            .map(|c| c.name)
            .filter(|name| *name != "row_state")
            .collect()
    );
    for dropped in ["land_category", "area_m2", "price_per_m2"] {
        assert!(!keys.contains(dropped), "{dropped}");
    }
    Ok(())
}

#[tokio::test]
async fn ownership_export_refuses_header_drift_and_the_shape_sibling() {
    for header in [
        HEADER.replace("공유인일련번호", "일련번호"),
        HEADER.replace("소유구분코드,소유구분", "소유구분,소유구분코드"),
    ] {
        let error = export_csv(&header, LAND_ROW, "AL_D161_99_20250831.csv")
            .await
            .expect_err("header drift");
        assert!(error.to_string().contains("header"), "{error}");
    }
    let error = export_csv(HEADER, LAND_ROW, "AL_D160_99_20251103.dbf")
        .await
        .expect_err("the shape sibling is not this lane's member");
    assert!(error.to_string().contains("no member"), "{error}");
}
