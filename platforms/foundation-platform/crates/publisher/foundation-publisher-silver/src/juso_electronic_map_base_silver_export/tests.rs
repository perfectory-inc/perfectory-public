use std::io::Cursor;

use shapefile::{
    dbase::{self, encoding::EncodingRs, FieldName, FieldValue},
    Point, Polygon, PolygonRing, ShapeWriter, Writer,
};
use zip::{write::SimpleFileOptions, ZipWriter};

use super::*;

// Synthetic only: the made-up 시도 folder 99000, codes of 9s and no real place (public repository).

/// The PRJ the 주소정보 전자지도 download ships beside its layers (`GRS80_UTMK.prj`).
const UTM_K: &str = concat!(
    r#"PROJCS["PCS_ITRF2000_TM",GEOGCS["GCS_ITRF_2000",DATUM["D_ITRF_2000","#,
    r#"SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],"#,
    r#"UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],"#,
    r#"PARAMETER["False_Easting",1000000.0],PARAMETER["False_Northing",2000000.0],"#,
    r#"PARAMETER["Central_Meridian",127.5],PARAMETER["Scale_Factor",0.9996],"#, // public-repository-safety: reviewed-runtime-coordinate
    r#"PARAMETER["Latitude_Of_Origin",38.0],UNIT["Meter",1.0]]"#,
);

fn name(text: &str) -> anyhow::Result<FieldName> {
    FieldName::try_from(text).map_err(|error| anyhow::anyhow!("{error:?}"))
}

/// A 시도 zip holding `TL_SCCO_LI` (리 polygons, key LI_CD) and `TL_SPBD_ENTRC` (출입구 points,
/// key SIG_CD+ENT_MAN_NO). `ri_codes` are the 리 rows' codes, so a test can repeat one.
fn region_zip(ri_codes: &[&str]) -> anyhow::Result<Vec<u8>> {
    let mut members = vec![("GRS80_UTMK.prj".to_owned(), UTM_K.as_bytes().to_vec())];
    {
        let (mut shp, mut shx, mut dbf) = (
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        );
        {
            let table =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR))
                    .add_character_field(name("LI_CD")?, 10)
                    .add_character_field(name("LI_KOR_NM")?, 40);
            let mut writer = Writer::new(
                ShapeWriter::with_shx(&mut shp, &mut shx),
                table.build_with_dest(&mut dbf),
            );
            for (index, code) in ri_codes.iter().enumerate() {
                #[allow(clippy::cast_precision_loss)]
                let x = 1_000_000.0 + 100.0 * index as f64;
                let polygon = Polygon::new(PolygonRing::Outer(vec![
                    Point::new(x, 2_000_000.0),
                    Point::new(x, 2_000_010.0),
                    Point::new(x + 10.0, 2_000_010.0),
                    Point::new(x + 10.0, 2_000_000.0),
                    Point::new(x, 2_000_000.0),
                ]));
                let mut record = dbase::Record::default();
                record.insert(
                    "LI_CD".to_owned(),
                    FieldValue::Character(Some((*code).to_owned())),
                );
                record.insert(
                    "LI_KOR_NM".to_owned(),
                    FieldValue::Character(Some("가상리".to_owned())),
                );
                writer.write_shape_and_record(&polygon, &record)?;
            }
        }
        for (part, bytes) in [("shp", shp), ("shx", shx), ("dbf", dbf)] {
            members.push((format!("99000/TL_SCCO_LI.{part}"), bytes.into_inner()));
        }
    }
    {
        let (mut shp, mut shx, mut dbf) = (
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        );
        {
            let table =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR))
                    .add_character_field(name("SIG_CD")?, 5)
                    .add_numeric_field(name("ENT_MAN_NO")?, 10, 0);
            let mut writer = Writer::new(
                ShapeWriter::with_shx(&mut shp, &mut shx),
                table.build_with_dest(&mut dbf),
            );
            for number in [1.0, 2.0] {
                let mut record = dbase::Record::default();
                record.insert(
                    "SIG_CD".to_owned(),
                    FieldValue::Character(Some("99999".to_owned())),
                );
                record.insert("ENT_MAN_NO".to_owned(), FieldValue::Numeric(Some(number)));
                writer.write_shape_and_record(
                    &Point::new(1_000_000.0 + number, 2_000_000.0),
                    &record,
                )?;
            }
        }
        for (part, bytes) in [("shp", shp), ("shx", shx), ("dbf", dbf)] {
            members.push((format!("99000/TL_SPBD_ENTRC.{part}"), bytes.into_inner()));
        }
    }
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    for (member, bytes) in members {
        zip.start_file(member, SimpleFileOptions::default())?;
        zip.write_all(&bytes)?;
    }
    Ok(zip.finish()?.into_inner())
}

async fn export(zip: Vec<u8>, kind: &str) -> anyhow::Result<(ExportReport, Vec<JsonValue>)> {
    let root = std::env::temp_dir().join(format!("juso-base-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root)?;
    let input = root.join("99000.zip");
    std::fs::write(&input, zip)?;
    let output = root.join("rows.jsonl.gz");
    let config = ExportConfig {
        input: InputSource::LocalPath(input),
        output: OutputSink::LocalPath(output.clone()),
        summary_path: Some(root.join("summary.json")),
        source_snapshot_id: "jusogokr-electronic-map-base:209910".to_owned(),
        base_month: "209910".to_owned(),
        layout: KindLayout::of(kind)?,
    };
    let result = export_handoff(&config).await;
    let mut rows = Vec::new();
    if result.is_ok() {
        let mut text = String::new();
        flate2::read::GzDecoder::new(std::fs::File::open(&output)?).read_to_string(&mut text)?;
        rows = text
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
    }
    std::fs::remove_dir_all(&root)?;
    Ok((result?, rows))
}

#[test]
fn every_kind_with_a_layer_reads_its_layer_key_and_legal_code_from_the_contract(
) -> anyhow::Result<()> {
    let building = KindLayout::of("building")?;
    assert_eq!(building.layer, "TL_SPBD_BULD");
    // The base's measured key, not the linkage's (base.keys).
    assert_eq!(building.key, ["BD_MGT_SN"]);
    assert_eq!(building.source, "electronic_map_base");
    let entrance = KindLayout::of("building_entrance")?;
    assert_eq!(entrance.key, ["SIG_CD", "ENT_MAN_NO"]);
    assert_eq!(entrance.legal_code_column, "SIG_CD");
    assert!(KindLayout::of("building_group").is_err(), "no base layer");
    assert!(KindLayout::of("synthetic").is_err());
    Ok(())
}

#[tokio::test]
async fn a_kind_reads_only_its_layer_and_keeps_every_provider_column() -> anyhow::Result<()> {
    let (report, rows) = export(region_zip(&["9999999999", "9999999998"])?, "legal_ri").await?;
    assert_eq!(
        (report.input_feature_count, report.output_row_count),
        (2, 2)
    );
    assert_eq!(report.member, "99000/TL_SCCO_LI.shp");
    assert_eq!(rows[0]["kind"], "legal_ri");
    assert_eq!(rows[0]["li_cd"], "9999999999");
    assert_eq!(rows[0]["li_kor_nm"], "가상리");
    assert_eq!(rows[0]["row_key"], "9999999999");
    assert_eq!(rows[0]["legal_code"], "9999999999");
    assert_eq!(rows[0]["sido_folder"], "99000");
    assert_eq!(rows[0]["base_month"], "209910");
    let wkb = rows[0]["geometry_wkb"].as_str().unwrap_or_default();
    assert!(wkb.starts_with("0106000000"), "multipolygon: {wkb}");
    // UTM-K's false origin is its central meridian.
    let min_x = rows[0]["bbox_min_x"].as_f64().unwrap_or_default();
    assert!((min_x - 127.5).abs() < 1e-6, "{min_x}"); // public-repository-safety: reviewed-runtime-coordinate

    let (report, rows) = export(region_zip(&["9999999999"])?, "building_entrance").await?;
    assert_eq!(report.output_row_count, 2);
    assert_eq!(rows[1]["row_key"], "99999|2");
    assert_eq!(rows[1]["ent_man_no"], 2, "a whole number stays an integer");
    assert_eq!(rows[1]["geometry_type"], "Point");
    assert!(rows[1]["geometry_wkb"]
        .as_str()
        .is_some_and(|wkb| wkb.starts_with("0101000000")));
    Ok(())
}

#[tokio::test]
async fn a_repeated_key_or_a_missing_layer_fails_the_export() -> anyhow::Result<()> {
    let repeated = export(region_zip(&["9999999999", "9999999999"])?, "legal_ri").await;
    assert!(repeated.is_err_and(|error| error.to_string().contains("repeat a key")));
    let missing = export(region_zip(&["9999999999"])?, "legal_emd").await;
    assert!(missing.is_err_and(|error| error.to_string().contains("TL_SCCO_EMD.shp")));
    Ok(())
}
