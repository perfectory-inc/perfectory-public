use std::{fs::File, io::BufWriter, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use arrow_array::{
    builder::{Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use lakehouse_application::BuildingRegisterFloorSilverRow;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use super::{create_file_writer, prepare_clean_output_dir};

const DEFAULT_BATCH_ROWS: usize = 8192;

pub(crate) struct ParquetSilverRowWriter {
    mode: ParquetOutputMode,
    schema: Arc<Schema>,
    batch_rows: usize,
    buffer: Vec<BuildingRegisterFloorSilverRow>,
    current_writer: Option<ArrowWriter<BufWriter<File>>>,
    current_row_count: usize,
    chunk_count: usize,
}

enum ParquetOutputMode {
    Single(PathBuf),
    Chunked { root: PathBuf, chunk_rows: usize },
}

impl ParquetSilverRowWriter {
    pub(crate) fn new(path: PathBuf, chunk_rows: Option<usize>) -> Result<Self> {
        if let Some(chunk_rows) = chunk_rows {
            prepare_clean_output_dir(&path, "chunked Parquet")?;
            return Ok(Self {
                mode: ParquetOutputMode::Chunked {
                    root: path,
                    chunk_rows,
                },
                schema: Arc::new(build_schema()),
                batch_rows: DEFAULT_BATCH_ROWS,
                buffer: Vec::with_capacity(DEFAULT_BATCH_ROWS),
                current_writer: None,
                current_row_count: 0,
                chunk_count: 0,
            });
        }

        Ok(Self {
            mode: ParquetOutputMode::Single(path),
            schema: Arc::new(build_schema()),
            batch_rows: DEFAULT_BATCH_ROWS,
            buffer: Vec::with_capacity(DEFAULT_BATCH_ROWS),
            current_writer: None,
            current_row_count: 0,
            chunk_count: 0,
        })
    }

    pub(crate) fn write_rows(&mut self, rows: &[BuildingRegisterFloorSilverRow]) -> Result<()> {
        for row in rows {
            self.ensure_writer()?;
            self.buffer.push(row.clone());
            self.current_row_count += 1;

            if self.buffer.len() >= self.batch_rows || self.chunk_boundary_reached() {
                self.flush_batch()?;
            }
            if self.chunk_boundary_reached() {
                self.close_current_writer()?;
            }
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        self.flush_batch()?;
        self.close_current_writer()
    }

    fn ensure_writer(&mut self) -> Result<()> {
        if self.current_writer.is_some() {
            return Ok(());
        }

        let path = match &self.mode {
            ParquetOutputMode::Single(path) => path.clone(),
            ParquetOutputMode::Chunked { root, .. } => {
                self.chunk_count += 1;
                self.current_row_count = 0;
                root.join(format!("part-{:06}.parquet", self.chunk_count))
            }
        };
        let writer = create_file_writer(&path)?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        self.current_writer = Some(
            ArrowWriter::try_new(writer, Arc::clone(&self.schema), Some(properties))
                .with_context(|| format!("failed to create Parquet writer {}", path.display()))?,
        );
        Ok(())
    }

    fn chunk_boundary_reached(&self) -> bool {
        matches!(
            self.mode,
            ParquetOutputMode::Chunked { chunk_rows, .. } if self.current_row_count >= chunk_rows
        )
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = rows_to_batch(&self.buffer, Arc::clone(&self.schema))?;
        let writer = self
            .current_writer
            .as_mut()
            .context("Parquet writer is not open")?;
        writer
            .write(&batch)
            .context("failed to write Parquet batch")?;
        self.buffer.clear();
        Ok(())
    }

    fn close_current_writer(&mut self) -> Result<()> {
        self.flush_batch()?;
        if let Some(writer) = self.current_writer.take() {
            writer.close().context("failed to close Parquet writer")?;
        }
        Ok(())
    }
}

fn build_schema() -> Schema {
    Schema::new(vec![
        Field::new("floor_row_id", DataType::Utf8, false),
        Field::new("mgm_bldrgst_pk", DataType::Utf8, false),
        Field::new("floor_type_code_raw", DataType::Utf8, true),
        Field::new("floor_type_name_raw", DataType::Utf8, true),
        Field::new("floor_number_raw", DataType::Utf8, true),
        Field::new("floor_label_raw", DataType::Utf8, true),
        Field::new("floor_kind", DataType::Utf8, false),
        Field::new("floor_number", DataType::Int32, true),
        Field::new("floor_index", DataType::Int32, true),
        Field::new("floor_display_ko", DataType::Utf8, true),
        Field::new("normalization_status", DataType::Utf8, false),
        Field::new("normalization_reason", DataType::Utf8, false),
        Field::new("source_record_id", DataType::Utf8, false),
        Field::new("source_snapshot_id", DataType::Utf8, false),
        Field::new("bronze_object_key", DataType::Utf8, false),
        Field::new("source_line_number", DataType::Int64, true),
        Field::new(
            "valid_from_utc",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "valid_to_utc",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "ingested_at_utc",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("row_checksum_sha256", DataType::Utf8, false),
    ])
}

fn rows_to_batch(
    rows: &[BuildingRegisterFloorSilverRow],
    schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let mut floor_row_id = StringBuilder::new();
    let mut mgm_bldrgst_pk = StringBuilder::new();
    let mut floor_type_code_raw = StringBuilder::new();
    let mut floor_type_name_raw = StringBuilder::new();
    let mut floor_number_raw = StringBuilder::new();
    let mut floor_label_raw = StringBuilder::new();
    let mut floor_kind = StringBuilder::new();
    let mut floor_number = Int32Builder::new();
    let mut floor_index = Int32Builder::new();
    let mut floor_display_ko = StringBuilder::new();
    let mut normalization_status = StringBuilder::new();
    let mut normalization_reason = StringBuilder::new();
    let mut source_record_id = StringBuilder::new();
    let mut source_snapshot_id = StringBuilder::new();
    let mut bronze_object_key = StringBuilder::new();
    let mut source_line_number = Int64Builder::new();
    let mut valid_from_utc = TimestampMillisecondBuilder::new();
    let mut valid_to_utc = TimestampMillisecondBuilder::new();
    let mut ingested_at_utc = TimestampMillisecondBuilder::new();
    let mut row_checksum_sha256 = StringBuilder::new();

    for row in rows {
        floor_row_id.append_value(&row.floor_row_id);
        mgm_bldrgst_pk.append_value(&row.mgm_bldrgst_pk);
        append_optional_string(&mut floor_type_code_raw, Some(&row.floor_type_code_raw));
        append_optional_string(&mut floor_type_name_raw, Some(&row.floor_type_name_raw));
        append_optional_string(&mut floor_number_raw, Some(&row.floor_number_raw));
        append_optional_string(&mut floor_label_raw, row.floor_label_raw.as_ref());
        floor_kind.append_value(&row.floor_kind);
        append_optional_i32(&mut floor_number, row.floor_number.map(i32::from));
        append_optional_i32(&mut floor_index, row.floor_index.map(i32::from));
        append_optional_string(&mut floor_display_ko, row.floor_display_ko.as_ref());
        normalization_status.append_value(&row.normalization_status);
        normalization_reason.append_value(&row.normalization_reason);
        source_record_id.append_value(&row.source_record_id);
        source_snapshot_id.append_value(&row.source_snapshot_id);
        bronze_object_key.append_value(&row.bronze_object_key);
        append_optional_i64(
            &mut source_line_number,
            row.source_line_number
                .and_then(|value| i64::try_from(value).ok()),
        );
        valid_from_utc.append_value(row.valid_from_utc.timestamp_millis());
        match row.valid_to_utc {
            Some(value) => valid_to_utc.append_value(value.timestamp_millis()),
            None => valid_to_utc.append_null(),
        }
        ingested_at_utc.append_value(row.ingested_at_utc.timestamp_millis());
        row_checksum_sha256.append_value(&row.row_checksum_sha256);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(floor_row_id.finish()) as ArrayRef,
            Arc::new(mgm_bldrgst_pk.finish()),
            Arc::new(floor_type_code_raw.finish()),
            Arc::new(floor_type_name_raw.finish()),
            Arc::new(floor_number_raw.finish()),
            Arc::new(floor_label_raw.finish()),
            Arc::new(floor_kind.finish()),
            Arc::new(floor_number.finish()),
            Arc::new(floor_index.finish()),
            Arc::new(floor_display_ko.finish()),
            Arc::new(normalization_status.finish()),
            Arc::new(normalization_reason.finish()),
            Arc::new(source_record_id.finish()),
            Arc::new(source_snapshot_id.finish()),
            Arc::new(bronze_object_key.finish()),
            Arc::new(source_line_number.finish()),
            Arc::new(valid_from_utc.finish()),
            Arc::new(valid_to_utc.finish()),
            Arc::new(ingested_at_utc.finish()),
            Arc::new(row_checksum_sha256.finish()),
        ],
    )
    .context("failed to build building-register floor Parquet batch")
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<&String>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_i64(builder: &mut Int64Builder, value: Option<i64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}
