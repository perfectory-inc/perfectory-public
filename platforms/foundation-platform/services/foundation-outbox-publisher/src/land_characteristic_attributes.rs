//! `AL_D194`'s measured anonymous DBF mapping. No geometry or serving price is emitted.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use anyhow::{bail, Context};
use chrono::{NaiveDate, Utc};
use foundation_shapefile::{for_each_dbf_record, DbfRecord, DbfValue};
use foundation_shared_kernel::Pnu;
use serde_json::{json, Map, Value};

use super::{ExportConfig, StreamReport};

const SOURCE_FIELDS: &[&str] = &[
    "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10", "A11", "A12", "A13", "A14",
    "A15", "A16", "A17", "A18", "A19", "A20", "A21", "A22", "A23", "A24", "A25", "A26",
];

const TEXT_ATTRIBUTES: &[(&str, &str)] = &[
    ("land_category", "A11"),
    ("land_use_situation", "A18"),
    ("terrain_height", "A20"),
    ("terrain_shape", "A22"),
    ("road_contact", "A24"),
];

pub(super) fn stream_rows(
    member: impl Read,
    writer: &mut impl Write,
    config: &ExportConfig,
    dataset_name: &str,
) -> anyhow::Result<StreamReport> {
    let vintage = source_vintage(dataset_name)?;
    let source_record_id = config.source_record_id();
    let ingested = Utc::now().to_rfc3339();
    let count = for_each_dbf_record(member, "EUC-KR", SOURCE_FIELDS, |record| {
        let mut row = map_record(&record)?;
        row.insert("source_vintage".to_owned(), json!(vintage));
        row.insert("source_record_id".to_owned(), json!(source_record_id));
        row.insert(
            "source_snapshot_id".to_owned(),
            json!(config.source_snapshot_id),
        );
        row.insert("ingested_at_utc".to_owned(), json!(ingested));
        serde_json::to_writer(&mut *writer, &row)?;
        writer.write_all(b"\n")?;
        Ok(())
    })?;
    if count == 0 {
        bail!("land_characteristic_empty_source: {dataset_name}");
    }
    Ok(StreamReport {
        input_row_count: count,
        output_row_count: count,
        rejected_row_count: 0,
        rejected_row_reasons: BTreeMap::new(),
    })
}

fn source_vintage(dataset_name: &str) -> anyhow::Result<&str> {
    let stem = dataset_name
        .strip_prefix("AL_D194_")
        .and_then(|s| s.strip_suffix(".dbf"))
        .context("land_characteristic_dataset_name: expected AL_D194_<sigungu>_<yyyymmdd>.dbf")?;
    let (region, vintage) = stem
        .split_once('_')
        .context("land_characteristic_dataset_name: region/vintage absent")?;
    if region.len() != 5
        || !region.bytes().all(|b| b.is_ascii_digit())
        || vintage.len() != 8
        || !vintage.bytes().all(|b| b.is_ascii_digit())
    {
        bail!("land_characteristic_dataset_name: invalid region/vintage");
    }
    NaiveDate::parse_from_str(vintage, "%Y%m%d")
        .context("land_characteristic_dataset_name: invalid vintage date")?;
    Ok(vintage)
}

fn map_record(record: &DbfRecord) -> anyhow::Result<Map<String, Value>> {
    let raw_pnu = text(record, "A1")?.context("land_characteristic_invalid_pnu: A1 blank")?;
    let pnu = Pnu::parse(raw_pnu).context("land_characteristic_invalid_pnu: A1")?;
    let area = number(record, "A12")?;
    if !area.is_finite() || area <= 0.0 {
        bail!("land_characteristic_invalid_area: A12 must be finite and positive");
    }
    let price = number(record, "A25")?;
    if !price.is_finite() || price < 0.0 || price.fract() != 0.0 || price > 9_007_199_254_740_991.0
    {
        bail!("land_characteristic_invalid_verification_price: A25 must be an exact nonnegative integer");
    }
    let mut row = Map::new();
    row.insert("pnu".to_owned(), json!(pnu.as_str()));
    row.insert("area_m2".to_owned(), json!(area));
    row.insert(
        "verification_price_per_m2".to_owned(),
        json!(format!("{price:.0}")),
    );
    for (column, field) in [("base_year", "A8"), ("base_month", "A9")] {
        let value = match record.get(field) {
            Some(DbfValue::Character(Some(value))) => value.trim().to_owned(),
            _ => number(record, field)?.to_string(),
        };
        row.insert(column.to_owned(), json!(value));
    }
    for (column, field) in TEXT_ATTRIBUTES {
        row.insert((*column).to_owned(), json!(text(record, field)?));
    }
    Ok(row)
}

fn text<'a>(record: &'a DbfRecord, field: &str) -> anyhow::Result<Option<&'a str>> {
    match record.get(field) {
        Some(DbfValue::Character(Some(value))) => {
            Ok((!value.trim().is_empty()).then_some(value.trim()))
        }
        Some(DbfValue::Character(None)) => Ok(None),
        _ => bail!("land_characteristic_field_type: {field} must be character data"),
    }
}

fn number(record: &DbfRecord, field: &str) -> anyhow::Result<f64> {
    match record.get(field) {
        Some(DbfValue::Numeric(Some(value)) | DbfValue::Double(value)) => Ok(*value),
        Some(DbfValue::Float(Some(value))) => Ok(f64::from(*value)),
        Some(DbfValue::Integer(value)) => Ok(f64::from(*value)),
        _ => bail!("land_characteristic_field_type: {field} must be nonnull numeric data"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> DbfRecord {
        let mut row = DbfRecord::default();
        row.insert(
            "A1".into(),
            DbfValue::Character(Some("9999938029104450003".into())),
        );
        row.insert("A8".into(), DbfValue::Character(Some("2025".into())));
        row.insert("A9".into(), DbfValue::Character(Some("1".into())));
        row.insert("A12".into(), DbfValue::Numeric(Some(123.45)));
        row.insert("A25".into(), DbfValue::Numeric(Some(81_700.0)));
        for ((_, field), value) in
            TEXT_ATTRIBUTES
                .iter()
                .zip(["대", "공업용", "평지", "정방형", "광대로"])
        {
            row.insert((*field).into(), DbfValue::Character(Some(value.into())));
        }
        row
    }

    #[test]
    fn mapped_attributes_cover_the_scalar_contract_without_geometry_or_serving_price(
    ) -> anyhow::Result<()> {
        let row = map_record(&record())?;
        assert_eq!(row["land_category"], "대");
        assert_eq!(row["area_m2"], 123.45);
        assert_eq!(row["land_use_situation"], "공업용");
        assert_eq!(row["terrain_height"], "평지");
        assert_eq!(row["terrain_shape"], "정방형");
        assert_eq!(row["road_contact"], "광대로");
        assert_eq!(row["verification_price_per_m2"], "81700");
        let mut keys: Vec<&str> = row
            .keys()
            .map(String::as_str)
            .chain([
                "source_vintage",
                "source_record_id",
                "source_snapshot_id",
                "ingested_at_utc",
            ])
            .collect();
        keys.sort_unstable();
        let mut declared: Vec<&str> = lakehouse_domain::SILVER_LAND_CHARACTERISTIC
            .columns
            .iter()
            .map(|c| c.name)
            .collect();
        declared.sort_unstable();
        assert_eq!(keys, declared);
        assert!(!row.contains_key("geometry"));
        assert!(!row.contains_key("price_per_m2"));
        Ok(())
    }

    #[test]
    fn shifted_pnu_area_and_label_types_refuse() {
        for (field, value) in [
            ("A1", DbfValue::Character(Some("invalid".into()))),
            ("A12", DbfValue::Numeric(Some(0.0))),
            ("A12", DbfValue::Numeric(Some(f64::NAN))),
            ("A11", DbfValue::Numeric(Some(1.0))),
            ("A25", DbfValue::Numeric(Some(1.5))),
        ] {
            let mut row = record();
            row.insert(field.into(), value);
            assert!(map_record(&row).is_err(), "{field} must refuse");
        }
    }

    #[test]
    fn vintage_is_taken_from_the_opened_member_not_the_operator() {
        assert_eq!(
            source_vintage("AL_D194_99999_20260526.dbf").ok(),
            Some("20260526")
        );
        assert!(source_vintage("AL_D194_99999_20260230.dbf").is_err());
        assert!(source_vintage("AL_D151_99999_20260526.dbf").is_err());
    }

    #[tokio::test]
    async fn dbf_only_zip_uses_shared_transport_and_finishes_gzip() -> anyhow::Result<()> {
        use super::super::{export_handoff, InputSource, OutputSink, CHARACTERISTIC_LANE};
        use shapefile::dbase::{encoding::EncodingRs, FieldName, TableWriterBuilder};
        let root =
            std::env::temp_dir().join(format!("characteristic-export-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let mut dbf = std::io::Cursor::new(Vec::new());
        let mut builder = TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR));
        let mut source = record();
        for name in SOURCE_FIELDS {
            let field = FieldName::try_from(*name).map_err(|e| anyhow::anyhow!(e))?;
            if matches!(*name, "A12" | "A25") {
                builder = builder.add_numeric_field(field, 18, 2);
            } else {
                builder = builder.add_character_field(field, 64);
                if source.get(name).is_none() {
                    source.insert((*name).into(), DbfValue::Character(None));
                }
            }
        }
        builder.build_with_dest(&mut dbf).write_record(&source)?;
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        zip.start_file::<_, ()>(
            "AL_D194_99999_20260526.dbf",
            zip::write::FileOptions::default(),
        )?;
        zip.write_all(&dbf.into_inner())?;
        let input = root.join("attributes.zip");
        std::fs::write(&input, zip.finish()?.into_inner())?;
        let output = root.join("attributes.jsonl.gz");
        let summary = root.join("summary.json");
        let config = ExportConfig {
            input: InputSource::LocalPath(input),
            output: OutputSink::LocalPath(output.clone()),
            summary_path: Some(summary.clone()),
            source_snapshot_id: "characteristic:test".into(),
        };
        let report = export_handoff(&config, &CHARACTERISTIC_LANE).await?;
        assert_eq!(report.output_row_count, 1);
        let mut decoded = String::new();
        flate2::read::GzDecoder::new(std::fs::File::open(output)?).read_to_string(&mut decoded)?;
        let row: Value = serde_json::from_str(&decoded)?;
        assert_eq!(row["area_m2"], 123.45);
        assert_eq!(row["land_category"], "대");
        assert_eq!(row["base_year"], "2025");
        assert_eq!(row["source_vintage"], "20260526");
        let summary: Value = serde_json::from_slice(&std::fs::read(summary)?)?;
        assert_eq!(summary["output"]["contract"], "silver.land_characteristic");
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
