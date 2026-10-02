//! Typed access to the fixed scratch SELECT schemas; no Arrow 58/59 type mixing.
use anyhow::{ensure, Context};
use duckdb::arrow::{
    array::{Array, BinaryArray, Int64Array, StringArray},
    record_batch::RecordBatch,
};

pub(super) fn integer(batch: &RecordBatch, column: usize, row: usize) -> anyhow::Result<i64> {
    optional_integer(batch, column, row)?.context("NULL in required floor integer")
}

pub(super) fn optional_integer(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> anyhow::Result<Option<i64>> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("floor scratch expected BIGINT")?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(super) fn text(batch: &RecordBatch, column: usize, row: usize) -> anyhow::Result<&str> {
    optional_text(batch, column, row)?.context("NULL in required floor text")
}

pub(super) fn optional_text(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> anyhow::Result<Option<&str>> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("floor scratch expected VARCHAR")?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

pub(super) fn blob(batch: &RecordBatch, column: usize, row: usize) -> anyhow::Result<Vec<u8>> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("floor scratch expected BLOB")?;
    ensure!(!values.is_null(row), "NULL in required floor blob");
    Ok(values.value(row).to_vec())
}
