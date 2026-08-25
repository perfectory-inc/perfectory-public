//! Row scan of one Iceberg snapshot: manifest list -> manifests -> Parquet data files.
//!
//! The Iceberg REST catalog answers which snapshot is current and where its manifest list lives.
//! Everything below that is object reads, so this module stays byte-oriented: it takes the Avro and
//! Parquet bytes an object reader produced and returns rows shaped by the Lakehouse table contract.
//! Keeping the column set on `LakehouseTableContract` means the projection cannot silently disagree
//! with the canonical table it reads.

use anyhow::{bail, ensure, Context};
use apache_avro::{types::Value as AvroValue, Reader as AvroReader};
use arrow_array::{
    Array, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, SecondsFormat, TimeDelta, Utc};
use lakehouse_domain::LakehouseTableContract;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Iceberg `manifest_file.content` / `data_file.content` value for row data.
const CONTENT_DATA: i64 = 0;
/// Iceberg `manifest_entry.status` value for an entry removed by this snapshot.
const STATUS_DELETED: i64 = 2;
const PARQUET_FILE_FORMAT: &str = "PARQUET";

/// One Parquet data file reachable from the scanned snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScannedDataFile {
    /// Storage location the manifest recorded for the data file.
    pub(crate) file_path: String,
    /// Row count the manifest recorded for the data file.
    pub(crate) record_count: u64,
    /// Compressed byte size the manifest recorded for the data file.
    pub(crate) file_size_in_bytes: u64,
}

/// Returns the manifest locations a snapshot's manifest list points at.
pub(crate) fn manifest_locations(manifest_list_avro: &[u8]) -> anyhow::Result<Vec<String>> {
    let mut locations = Vec::new();
    for record in
        avro_records(manifest_list_avro).context("failed to read Iceberg manifest list")?
    {
        let content = optional_integer_field(&record, "content")?.unwrap_or(CONTENT_DATA);
        ensure!(
            content == CONTENT_DATA,
            "Iceberg snapshot carries a delete manifest; this scan reads row data only"
        );
        locations.push(string_field(&record, "manifest_path")?);
    }
    Ok(locations)
}

/// Returns the live Parquet data files one manifest points at.
pub(crate) fn data_files(manifest_avro: &[u8]) -> anyhow::Result<Vec<ScannedDataFile>> {
    let mut data_files = Vec::new();
    for entry in avro_records(manifest_avro).context("failed to read Iceberg manifest")? {
        let status = optional_integer_field(&entry, "status")?.unwrap_or(CONTENT_DATA);
        if status == STATUS_DELETED {
            continue;
        }
        let data_file = resolve(
            field(&entry, "data_file")
                .context("Iceberg manifest entry is missing its data_file record")?,
        );
        let content = optional_integer_field(data_file, "content")?.unwrap_or(CONTENT_DATA);
        ensure!(
            content == CONTENT_DATA,
            "Iceberg manifest carries a delete file; this scan reads row data only"
        );
        let file_format = string_field(data_file, "file_format")?;
        ensure!(
            file_format.eq_ignore_ascii_case(PARQUET_FILE_FORMAT),
            "Iceberg data file format {file_format} is not supported by this scan"
        );
        let record_count = optional_integer_field(data_file, "record_count")?
            .context("Iceberg data file is missing its record_count")?;
        let file_size_in_bytes = optional_integer_field(data_file, "file_size_in_bytes")?
            .context("Iceberg data file is missing its file_size_in_bytes")?;
        data_files.push(ScannedDataFile {
            file_path: string_field(data_file, "file_path")?,
            record_count: u64::try_from(record_count)
                .context("Iceberg data file record_count must not be negative")?,
            file_size_in_bytes: u64::try_from(file_size_in_bytes)
                .context("Iceberg data file file_size_in_bytes must not be negative")?,
        });
    }
    Ok(data_files)
}

/// Decodes one Parquet data file into contract-shaped rows.
pub(crate) fn decode_rows(
    contract: &LakehouseTableContract,
    parquet_bytes: Vec<u8>,
) -> anyhow::Result<Vec<JsonMap<String, JsonValue>>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes))
        .context("failed to open Iceberg Parquet data file")?
        .build()
        .context("failed to build the Iceberg Parquet reader")?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.context("failed to read an Iceberg Parquet record batch")?;
        assert_batch_matches_contract(contract, &batch)?;
        for index in 0..batch.num_rows() {
            rows.push(decode_row(contract, &batch, index)?);
        }
    }
    Ok(rows)
}

fn assert_batch_matches_contract(
    contract: &LakehouseTableContract,
    batch: &RecordBatch,
) -> anyhow::Result<()> {
    let mut actual = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    ensure!(
        actual == expected,
        "{} data file columns {actual:?} do not match the table contract {expected:?}",
        contract.table_name
    );
    Ok(())
}

fn decode_row(
    contract: &LakehouseTableContract,
    batch: &RecordBatch,
    index: usize,
) -> anyhow::Result<JsonMap<String, JsonValue>> {
    let mut row = JsonMap::new();
    for column in contract.columns {
        let array = batch
            .column_by_name(column.name)
            .with_context(|| format!("data file is missing column {}", column.name))?;
        let value = decode_value(column.logical_type, column.name, array.as_ref(), index)?;
        ensure!(
            !(column.required && value.is_null()),
            "{} column {} is required by the table contract but the data file row is null",
            contract.table_name,
            column.name
        );
        row.insert(column.name.to_owned(), value);
    }
    Ok(row)
}

fn decode_value(
    logical_type: &str,
    column_name: &str,
    array: &dyn Array,
    index: usize,
) -> anyhow::Result<JsonValue> {
    if array.is_null(index) {
        return Ok(JsonValue::Null);
    }
    match logical_type {
        "string" => decode_string(column_name, array, index),
        "long" => decode_long(column_name, array, index),
        "date" => decode_date(column_name, array, index),
        "timestamp" => decode_timestamp(column_name, array, index),
        decimal if decimal.starts_with("decimal(") => {
            decode_decimal(decimal, column_name, array, index)
        }
        other => bail!("column {column_name} has unsupported contract type {other}"),
    }
}

fn decode_string(column_name: &str, array: &dyn Array, index: usize) -> anyhow::Result<JsonValue> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column {column_name} is not a Parquet string column"))?;
    Ok(JsonValue::String(values.value(index).to_owned()))
}

fn decode_long(column_name: &str, array: &dyn Array, index: usize) -> anyhow::Result<JsonValue> {
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column {column_name} is not a Parquet 64-bit integer column"))?;
    Ok(JsonValue::Number(values.value(index).into()))
}

/// Renders an Iceberg `date` as an ISO-8601 calendar date.
///
/// Iceberg stores a date as days since the epoch, which is a number a JSON reader would have to
/// know the encoding of to interpret. The scan hands on `YYYY-MM-DD` for the same reason it hands
/// on decimals as exact text: the reader should not need the storage encoding to read the value.
fn decode_date(column_name: &str, array: &dyn Array, index: usize) -> anyhow::Result<JsonValue> {
    match array.data_type() {
        DataType::Date32 => {}
        other => bail!("column {column_name} is a {other} rather than a 32-bit date"),
    }
    let values = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .with_context(|| format!("column {column_name} is not a Parquet date column"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).context("1970-01-01 must parse as a date")?;
    let date = epoch
        .checked_add_signed(TimeDelta::days(i64::from(values.value(index))))
        .with_context(|| format!("column {column_name} carries an out-of-range date"))?;
    Ok(JsonValue::String(date.format("%Y-%m-%d").to_string()))
}

fn decode_timestamp(
    column_name: &str,
    array: &dyn Array,
    index: usize,
) -> anyhow::Result<JsonValue> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {}
        other => bail!("column {column_name} is a {other} rather than a microsecond timestamp"),
    }
    let values = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .with_context(|| format!("column {column_name} is not a Parquet timestamp column"))?;
    let timestamp: DateTime<Utc> = DateTime::from_timestamp_micros(values.value(index))
        .with_context(|| format!("column {column_name} carries an out-of-range timestamp"))?;
    Ok(JsonValue::String(
        timestamp.to_rfc3339_opts(SecondsFormat::Secs, true),
    ))
}

fn decode_decimal(
    logical_type: &str,
    column_name: &str,
    array: &dyn Array,
    index: usize,
) -> anyhow::Result<JsonValue> {
    let (precision, scale) = parse_decimal_type(logical_type)
        .with_context(|| format!("column {column_name} has an unreadable decimal contract type"))?;
    match *array.data_type() {
        DataType::Decimal128(actual_precision, actual_scale)
            if actual_precision == precision && i64::from(actual_scale) == i64::from(scale) => {}
        ref other => {
            bail!("column {column_name} is a {other} rather than the contract's {logical_type}")
        }
    }
    let values = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .with_context(|| format!("column {column_name} is not a Parquet decimal column"))?;
    Ok(JsonValue::String(format_decimal(
        values.value(index),
        u32::from(scale.unsigned_abs()),
    )))
}

fn parse_decimal_type(logical_type: &str) -> Option<(u8, i8)> {
    let arguments = logical_type
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))?;
    let (precision, scale) = arguments.split_once(',')?;
    Some((precision.trim().parse().ok()?, scale.trim().parse().ok()?))
}

/// Renders a decimal as its exact base-10 text, so no float ever touches a published area.
fn format_decimal(value: i128, scale: u32) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    if scale == 0 {
        return format!("{sign}{magnitude}");
    }
    let divisor = 10_u128.pow(scale);
    let integer = magnitude / divisor;
    let fraction = magnitude % divisor;
    let width = scale as usize;
    format!("{sign}{integer}.{fraction:0width$}")
}

fn avro_records(bytes: &[u8]) -> anyhow::Result<Vec<AvroValue>> {
    let reader = AvroReader::new(bytes).context("failed to open the Avro container")?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .context("failed to decode an Avro record")
}

fn field<'a>(record: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    match resolve(record) {
        AvroValue::Record(fields) => fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value),
        _ => None,
    }
}

/// Unwraps Avro unions so callers always see the resolved branch.
fn resolve(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, inner) => resolve(inner),
        other => other,
    }
}

fn string_field(record: &AvroValue, name: &str) -> anyhow::Result<String> {
    match field(record, name).map(resolve) {
        Some(AvroValue::String(value)) => Ok(value.clone()),
        Some(other) => bail!("Avro field {name} is {other:?} rather than a string"),
        None => bail!("Avro record is missing field {name}"),
    }
}

fn optional_integer_field(record: &AvroValue, name: &str) -> anyhow::Result<Option<i64>> {
    match field(record, name).map(resolve) {
        Some(AvroValue::Int(value)) => Ok(Some(i64::from(*value))),
        Some(AvroValue::Long(value)) => Ok(Some(*value)),
        Some(AvroValue::Null) | None => Ok(None),
        Some(other) => bail!("Avro field {name} is {other:?} rather than an integer"),
    }
}

#[cfg(test)]
mod tests {
    use super::{data_files, format_decimal, parse_decimal_type, resolve};
    use apache_avro::{types::Value as AvroValue, Schema, Writer};

    #[test]
    fn data_files_read_manifest_record_count_and_size_without_opening_parquet() -> anyhow::Result<()>
    {
        let schema = Schema::parse_str(
            r#"{
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "long"},
                {"name": "data_file", "type": {
                  "type": "record",
                  "name": "data_file",
                  "fields": [
                    {"name": "content", "type": "long"},
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {"name": "record_count", "type": "long"},
                    {"name": "file_size_in_bytes", "type": "long"}
                  ]
                }}
              ]
            }"#,
        )?;
        let mut writer = Writer::new(&schema, Vec::new());
        writer.append(AvroValue::Record(vec![
            ("status".to_owned(), AvroValue::Long(1)),
            (
                "data_file".to_owned(),
                AvroValue::Record(vec![
                    ("content".to_owned(), AvroValue::Long(0)),
                    (
                        "file_path".to_owned(),
                        AvroValue::String("s3://lakehouse/silver/table/part-0.parquet".to_owned()),
                    ),
                    (
                        "file_format".to_owned(),
                        AvroValue::String("PARQUET".to_owned()),
                    ),
                    ("record_count".to_owned(), AvroValue::Long(37)),
                    ("file_size_in_bytes".to_owned(), AvroValue::Long(4_096)),
                ]),
            ),
        ]))?;
        let bytes = writer.into_inner()?;

        let files = data_files(&bytes)?;

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 37);
        assert_eq!(files[0].file_size_in_bytes, 4_096);
        Ok(())
    }

    #[test]
    fn decimals_render_as_exact_base_ten_text() {
        assert_eq!(format_decimal(123_456, 2), "1234.56");
        assert_eq!(format_decimal(-5, 2), "-0.05");
        assert_eq!(format_decimal(0, 2), "0.00");
        assert_eq!(format_decimal(700, 0), "700");
    }

    #[test]
    fn decimal_contract_types_are_parsed_into_precision_and_scale() {
        assert_eq!(parse_decimal_type("decimal(18,2)"), Some((18, 2)));
        assert_eq!(parse_decimal_type("decimal(9, 4)"), Some((9, 4)));
        assert_eq!(parse_decimal_type("string"), None);
        assert_eq!(parse_decimal_type("decimal(18)"), None);
    }

    #[test]
    fn unions_resolve_to_their_selected_branch() {
        let value = AvroValue::Union(1, Box::new(AvroValue::Long(7)));
        assert_eq!(resolve(&value), &AvroValue::Long(7));
        assert_eq!(resolve(&AvroValue::Null), &AvroValue::Null);
    }
}
