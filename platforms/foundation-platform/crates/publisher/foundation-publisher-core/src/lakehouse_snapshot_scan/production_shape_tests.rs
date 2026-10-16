//! The scan reads every data file shape a production table can hold (root ADR-0259 §3).
//!
//! `lakehouse-production-shape.json` is production's catalog: each table's columns now and every
//! earlier set of columns its Iceberg metadata records, the shapes its data files were written
//! under. For each table the scan reads, a Parquet file of each shape is written here with
//! synthetic values (the reserved `99999` region, never a real PNU) and read with the table's
//! current contract. On 2026-10-15 a file of `reference.legal_dong_code_snapshot`'s first shape (6
//! of its 12 columns) stopped every VWorld land Silver refresh; no test had a file of that shape.

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use parquet::arrow::ArrowWriter;
use serde::Deserialize;

/// Read when the test runs, not compiled in: the ledger is data, refreshed without a code change.
const LEDGER: &str = "../../../infra/lakehouse/contracts/lakehouse-production-shape.json";

/// The column types this scan decodes (`iceberg_scan::decode_value`). A table with a column of any
/// other type is not one the scan reads.
fn scan_reads(logical_type: &str) -> bool {
    matches!(logical_type, "string" | "long" | "date" | "timestamp")
        || logical_type.starts_with("decimal(")
}

#[derive(Deserialize)]
struct Ledger {
    tables: BTreeMap<String, TableShape>,
}

#[derive(Deserialize)]
struct TableShape {
    /// `[id, name, logical type, nullable]` of every field the table ever had.
    fields: Vec<(u32, String, String, bool)>,
    columns: Vec<u32>,
    earlier: Vec<Vec<u32>>,
}

/// One synthetic value of `logical_type`, as Spark's Iceberg writer lays it out in Parquet.
fn synthetic_column(logical_type: &str) -> anyhow::Result<(DataType, ArrayRef)> {
    Ok(match logical_type {
        "string" => (
            DataType::Utf8,
            Arc::new(StringArray::from(vec!["9999910100"])),
        ),
        "long" => (DataType::Int64, Arc::new(Int64Array::from(vec![1_i64]))),
        "int" => (DataType::Int32, Arc::new(Int32Array::from(vec![1]))),
        "double" => (DataType::Float64, Arc::new(Float64Array::from(vec![1.5]))),
        "boolean" => (DataType::Boolean, Arc::new(BooleanArray::from(vec![true]))),
        "binary" => (
            DataType::Binary,
            Arc::new(BinaryArray::from(vec![b"x".as_slice()])),
        ),
        "date" => (DataType::Date32, Arc::new(Date32Array::from(vec![20_000]))),
        "timestamp" => (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_700_000_000_000_000]).with_timezone("UTC"),
            ),
        ),
        "array<string>" => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            builder.values().append_value("9999910100");
            builder.append(true);
            let array = builder.finish();
            (array.data_type().clone(), Arc::new(array))
        }
        decimal if decimal.starts_with("decimal(") => {
            let (precision, scale) = decimal
                .trim_start_matches("decimal(")
                .trim_end_matches(')')
                .split_once(',')
                .ok_or_else(|| anyhow::anyhow!("unreadable decimal type {decimal}"))?;
            let (precision, scale) = (precision.trim().parse()?, scale.trim().parse()?);
            let array =
                Decimal128Array::from(vec![1_i128]).with_precision_and_scale(precision, scale)?;
            (DataType::Decimal128(precision, scale), Arc::new(array))
        }
        other => anyhow::bail!("the ledger holds a type this test cannot write: {other}"),
    })
}

/// A one-row Parquet data file holding exactly the `columns` (field ids) of `shape`.
fn data_file(shape: &TableShape, columns: &[u32]) -> anyhow::Result<Vec<u8>> {
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for id in columns {
        let (_, name, logical_type, _) = shape
            .fields
            .iter()
            .find(|field| field.0 == *id)
            .ok_or_else(|| anyhow::anyhow!("the ledger names no field {id}"))?;
        let (data_type, array) = synthetic_column(logical_type)?;
        fields.push(Field::new(name, data_type, true));
        arrays.push(array);
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

/// Every shape of `shape` the scan cannot read under `contract`, with its reason.
fn unreadable_shapes(
    contract: &LakehouseTableContract,
    shape: &TableShape,
) -> anyhow::Result<Vec<String>> {
    let mut refusals = Vec::new();
    for columns in std::iter::once(&shape.columns).chain(&shape.earlier) {
        let mut rows = 0;
        let decoded =
            super::iceberg_scan::decode_rows(contract, data_file(shape, columns)?, |_| {
                rows += 1;
                true
            });
        match decoded {
            Ok(_) if rows == 1 => {}
            Ok(_) => refusals.push(format!("{} {columns:?}: {rows} rows", contract.table_name)),
            Err(error) => refusals.push(format!("{} {columns:?}: {error:#}", contract.table_name)),
        }
    }
    Ok(refusals)
}

fn scanned_tables() -> anyhow::Result<Vec<(&'static LakehouseTableContract, TableShape)>> {
    // `cargo test` runs a test binary in its package's directory.
    let mut ledger: Ledger = serde_json::from_str(&std::fs::read_to_string(LEDGER)?)?;
    Ok(industrial_complex_lakehouse_contracts()
        .iter()
        .filter(|contract| contract.columns.iter().all(|c| scan_reads(c.logical_type)))
        .filter_map(|contract| Some((contract, ledger.tables.remove(contract.table_name)?)))
        .collect())
}

#[test]
fn every_shape_of_every_production_table_the_scan_reads_is_read() -> anyhow::Result<()> {
    let tables = scanned_tables()?;
    assert!(
        tables
            .iter()
            .any(|(contract, _)| contract.table_name == "reference.legal_dong_code_snapshot"),
        "the scan's tables include the one that failed on 2026-10-15"
    );
    let mut refusals = Vec::new();
    for (contract, shape) in &tables {
        refusals.extend(unreadable_shapes(contract, shape)?);
    }
    assert!(refusals.is_empty(), "{}", refusals.join("\n"));
    Ok(())
}

#[test]
fn the_legal_dong_snapshot_holds_files_of_its_first_six_column_shape() -> anyhow::Result<()> {
    // The replay of 2026-10-15 (#522): production holds files written before the code.go.kr
    // columns were added. Without this shape in the ledger the test above would prove nothing.
    let tables = scanned_tables()?;
    let (_, shape) = tables
        .iter()
        .find(|(contract, _)| contract.table_name == "reference.legal_dong_code_snapshot")
        .ok_or_else(|| anyhow::anyhow!("no legal-dong snapshot shape"))?;
    assert!(shape.earlier.iter().any(|columns| columns.len() == 6));
    Ok(())
}

#[test]
fn a_shape_with_a_column_the_contract_does_not_name_is_reported() -> anyhow::Result<()> {
    // The check can fail: an earlier file holding a column the contract no longer names (a drop).
    let tables = scanned_tables()?;
    let (contract, shape) = tables
        .iter()
        .find(|(contract, _)| contract.table_name == "reference.legal_dong_code_snapshot")
        .ok_or_else(|| anyhow::anyhow!("no legal-dong snapshot shape"))?;
    let mut dropped = TableShape {
        fields: shape.fields.clone(),
        columns: shape.columns.clone(),
        earlier: vec![shape.columns.clone()],
    };
    dropped
        .fields
        .push((9_999, "dropped_since".into(), "string".into(), true));
    dropped.earlier[0].push(9_999);
    let refusals = unreadable_shapes(contract, &dropped)?;
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(refusals[0].contains("dropped_since"), "{refusals:?}");
    Ok(())
}
