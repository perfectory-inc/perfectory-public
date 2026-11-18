use super::*;

const OFFICE_HEADER: &str = "법정동코드,법정동명,등록번호,사업자상호,중개업자명,상태구분코드,상태구분명,등록일자,전화번호,보증설정시작일,보증설정종료일,데이터기준일자,지번주소,도로명주소,도로명주소코드";
// Synthetic only (root rule: no real name, number or address in a fixture). The provider quotes
// every field; a name holding a comma is one field, not two.
const OFFICE_ROW: &str = "\"99999\",\"가상도 가상군\",\"99999-2026-00001\",\"가상,공인중개사사무소\",\"홍길동\",\"1\",\"영업중\",\"2026-11-02\",\"\",\"2026-11-02\",\"2027-11-01\",\"2026-11-10\",\"가상도 가상군 가상리 1-1\",\"가상도 가상군 가상로 1\",\"99999000000000000000001\"";

async fn export_csv(
    lane: &'static Lane,
    header: &str,
    rows: &str,
    member: &str,
) -> anyhow::Result<(ExportReport, String)> {
    let temp = std::env::temp_dir().join(format!("broker-csv-{}", uuid::Uuid::new_v4()));
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
        let report = export_handoff(&config, lane).await?;
        let mut body = String::new();
        flate2::read::GzDecoder::new(fs::File::open(output)?).read_to_string(&mut body)?;
        Ok((report, body))
    }
    .await;
    fs::remove_dir_all(temp)?;
    result
}

#[tokio::test]
async fn an_office_row_keeps_its_quoted_comma_and_a_blank_number_is_refused() -> anyhow::Result<()>
{
    let unnumbered = OFFICE_ROW.replace("\"99999-2026-00001\"", "\"\"");
    let (report, body) = export_csv(
        &REAL_ESTATE_BROKER_OFFICE_LANE,
        OFFICE_HEADER,
        &format!("{OFFICE_ROW}\r\n{unnumbered}"),
        "AL_D171_00_20261109.csv",
    )
    .await?;
    assert_eq!(report.output_row_count, 1);
    assert_eq!(
        report.rejected_row_reasons.get("blank_registration_no"),
        Some(&1)
    );
    let row: serde_json::Value = serde_json::from_str(body.trim())?;
    for (key, value) in [
        ("sigungu_code", "99999"),
        ("registration_no", "99999-2026-00001"),
        ("office_name", "가상,공인중개사사무소"),
        ("representative_name", "홍길동"),
        ("status_code", "1"),
        ("status_name", "영업중"),
        ("registered_on", "2026-11-02"),
        ("guarantee_end_on", "2027-11-01"),
        ("road_address_code", "99999000000000000000001"),
    ] {
        assert_eq!(row[key], value, "{key}");
    }
    assert!(row["phone_number"].is_null());
    Ok(())
}

#[tokio::test]
async fn the_office_lane_refuses_header_drift_and_the_member_roster_sibling() {
    let error = export_csv(
        &REAL_ESTATE_BROKER_OFFICE_LANE,
        &OFFICE_HEADER.replace("등록번호,사업자상호", "사업자상호,등록번호"),
        OFFICE_ROW,
        "AL_D171_00_20261109.csv",
    )
    .await
    .expect_err("header drift");
    assert!(error.to_string().contains("header"), "{error}");
    let error = export_csv(
        &REAL_ESTATE_BROKER_OFFICE_LANE,
        OFFICE_HEADER,
        OFFICE_ROW,
        "AL_D172_00_20261109.csv",
    )
    .await
    .expect_err("the roster is not this lane's member");
    assert!(error.to_string().contains("no member"), "{error}");
}

const MEMBER_HEADER: &str = "법정동코드,법정동명,등록번호,사업자상호,중개업자명,중개업자종별코드,중개업자종별명,자격증번호,자격증취득일,직위구분코드,직위구분명,데이터기준일자";
// Synthetic only: the representative with a license, then an assistant, who holds none.
const BROKER_ROW: &str = "99999,가상도 가상군,99999-2026-00001,\"가상,공인중개사사무소\",홍길동,2,공인중개사,99-2026-00001,2026-01-02,01,대표,2026-11-10";
const ASSISTANT_ROW: &str = "99999,가상도 가상군,99999-2026-00001,\"가상,공인중개사사무소\",김가상,4,중개보조원,,,04,일반,2026-11-10";

#[tokio::test]
async fn a_roster_keeps_an_assistant_without_a_license_and_refuses_a_row_without_its_office(
) -> anyhow::Result<()> {
    let officeless = BROKER_ROW.replace("99999-2026-00001", "");
    let (report, body) = export_csv(
        &REAL_ESTATE_BROKER_MEMBER_LANE,
        MEMBER_HEADER,
        &format!("{BROKER_ROW}\n{ASSISTANT_ROW}\n{officeless}"),
        "AL_D172_00_20261109.csv",
    )
    .await?;
    assert_eq!(report.output_row_count, 2);
    assert_eq!(
        report.rejected_row_reasons.get("blank_registration_no"),
        Some(&1)
    );
    let rows: Vec<serde_json::Value> = body
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    for (key, value) in [
        ("registration_no", "99999-2026-00001"),
        ("office_name", "가상,공인중개사사무소"),
        ("member_name", "홍길동"),
        ("member_kind_name", "공인중개사"),
        ("license_no", "99-2026-00001"),
        ("license_acquired_on", "2026-01-02"),
        ("position_name", "대표"),
    ] {
        assert_eq!(rows[0][key], value, "{key}");
    }
    assert_eq!(rows[1]["member_kind_name"], "중개보조원");
    assert!(rows[1]["license_no"].is_null());
    Ok(())
}
