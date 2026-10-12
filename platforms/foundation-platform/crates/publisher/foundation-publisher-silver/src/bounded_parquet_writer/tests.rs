use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

use arrow_array::StringArray;
use arrow_schema::{DataType, Field};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::*;

#[derive(Serialize)]
struct TrackedRow {
    value: String,
    #[serde(skip)]
    clones: Arc<AtomicUsize>,
}

impl Clone for TrackedRow {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            value: self.value.clone(),
            clones: Arc::clone(&self.clones),
        }
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]))
}

fn rows_to_batch(rows: &[TrackedRow], schema: Arc<Schema>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.value.as_str()),
        ))],
    )?)
}

fn bounded_rows_to_batch(rows: &[TrackedRow], schema: Arc<Schema>) -> Result<RecordBatch> {
    let target = serving_parquet_policy()?.row_group_target_bytes()?;
    let mut admitted_bytes = 0;
    for row in rows {
        admitted_bytes += size_of::<TrackedRow>() + serde_json::to_vec(row)?.len();
    }
    assert!(
        admitted_bytes <= target,
        "converter received {admitted_bytes} input bytes above target {target}"
    );
    rows_to_batch(rows, schema)
}

fn create_file(path: &Path) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(File::create(path)?))
}

#[allow(
    clippy::panic,
    reason = "Test sentinel must fail immediately; returning Err would satisfy the rejection assertion"
)]
fn unexpected_cleanup(_: &Path, _: &str) -> Result<()> {
    panic!("invalid configuration must be rejected before output cleanup")
}

fn policy(target: i64, maximum: i64) -> Result<ServingParquetPolicy> {
    Ok(serde_json::from_value(serde_json::json!({
        "row_group_target_bytes": target,
        "max_row_group_bytes": maximum,
        "row_group_check_min_records": 1,
        "row_group_check_max_records": 16,
    }))?)
}

#[test]
fn rejects_json_expansion_before_cloning_or_opening_a_file() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = directory.path().join("expanded.parquet");
    let target = serving_parquet_policy()?.row_group_target_bytes()?;
    let clones = Arc::new(AtomicUsize::new(0));
    let row = TrackedRow {
        value: "\0".repeat(target / 4),
        clones: Arc::clone(&clones),
    };
    let mut writer = BoundedParquetWriter::new(
        output.clone(),
        None,
        schema(),
        rows_to_batch,
        create_file,
        unexpected_cleanup,
    )?;

    assert!(row.value.len() < target);
    assert!(writer.write_rows(&[row]).is_err());
    assert_eq!(clones.load(Ordering::SeqCst), 0);
    assert!(!output.exists());
    assert!(
        writer.flush().is_err(),
        "a rejected export cannot become ready"
    );
    Ok(())
}

#[test]
fn bounds_compressible_input_and_open_group_buffers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = directory.path().join("compressible.parquet");
    let target = serving_parquet_policy()?.row_group_target_bytes()?;
    let row = TrackedRow {
        value: "x".repeat(target / 16),
        clones: Arc::new(AtomicUsize::new(0)),
    };
    let mut writer = BoundedParquetWriter::new(
        output.clone(),
        None,
        schema(),
        bounded_rows_to_batch,
        create_file,
        unexpected_cleanup,
    )?;
    for _ in 0..40 {
        writer.write_rows(std::slice::from_ref(&row))?;
        assert!(writer.buffer_bytes <= target);
        assert!(writer.buffer.len() * row.value.len() <= target);
        assert!(writer
            .current_writer
            .as_ref()
            .is_none_or(|open| open.memory_size() < target));
    }
    writer.flush()?;
    assert_eq!(row.clones.load(Ordering::SeqCst), 40);

    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(output)?)?
        .with_batch_size(7)
        .build()?;
    let mut read_rows = 0;
    for batch in reader {
        let batch = batch?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("fixture must contain a string column")?;
        for value in values {
            assert!(value == Some(row.value.as_str()));
            read_rows += 1;
        }
    }
    assert_eq!(read_rows, 40);
    Ok(())
}

#[test]
fn validates_policy_and_chunk_size_before_cleanup() -> Result<()> {
    for (policy, chunk_rows) in [
        (policy(0, 256)?, Some(1)),
        (policy(256, 256)?, Some(1)),
        (*serving_parquet_policy()?, Some(0)),
    ] {
        assert!(BoundedParquetWriter::with_policy(
            PathBuf::from("unused"),
            chunk_rows,
            schema(),
            rows_to_batch,
            create_file,
            unexpected_cleanup,
            policy,
        )
        .is_err());
    }
    Ok(())
}

struct FlushFailure;

impl Write for FlushFailure {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("fixture final sink flush failure"))
    }
}

#[test]
fn final_sink_flush_failure_cannot_be_retried_as_success() -> Result<()> {
    let mut writer = BoundedParquetWriter::new(
        PathBuf::from("unused"),
        None,
        schema(),
        rows_to_batch,
        |_| Ok(FlushFailure),
        unexpected_cleanup,
    )?;
    writer.write_rows(&[TrackedRow {
        value: "small tail".to_owned(),
        clones: Arc::new(AtomicUsize::new(0)),
    }])?;
    let error = format!("{:#}", writer.flush().err().context("flush must fail")?);
    assert!(error.contains("failed to close Parquet writer"));
    assert!(error.contains("fixture final sink flush failure"));
    assert!(writer.flush().is_err());
    assert!(writer.write_rows(&[]).is_err());
    Ok(())
}

#[test]
fn chunk_boundary_close_failure_is_returned_by_write_rows() -> Result<()> {
    let mut writer = BoundedParquetWriter::new(
        PathBuf::from("unused"),
        Some(2),
        schema(),
        rows_to_batch,
        |_| Ok(FlushFailure),
        |_, _| Ok(()),
    )?;
    let row = TrackedRow {
        value: "small chunk".to_owned(),
        clones: Arc::new(AtomicUsize::new(0)),
    };
    writer.write_rows(std::slice::from_ref(&row))?;
    let error = format!(
        "{:#}",
        writer
            .write_rows(std::slice::from_ref(&row))
            .err()
            .context("chunk boundary close must fail")?
    );
    assert!(error.contains("failed to close Parquet writer"));
    assert!(error.contains("fixture final sink flush failure"));
    assert!(writer.flush().is_err());
    Ok(())
}

#[test]
fn validates_row_groups_first_emitted_by_close() -> Result<()> {
    let schema = schema();
    let mut writer = BoundedParquetWriter::with_policy(
        PathBuf::from("unused"),
        None,
        Arc::clone(&schema),
        rows_to_batch,
        |_| Ok(Vec::<u8>::new()),
        unexpected_cleanup,
        policy(128, 256)?,
    )?;
    // Inject an open upstream writer whose final group violates the footer
    // policy. This isolates the close-tail gate from input/auto-flush guards.
    let mut upstream = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), None)?;
    upstream.write(&RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["x".repeat(1024)]))],
    )?)?;
    assert!(upstream.flushed_row_groups().is_empty());
    writer.current_writer = Some(upstream);

    let error = format!("{:#}", writer.flush().err().context("flush must fail")?);
    assert!(error.contains("Parquet row group 0 exceeds scan byte bound"));
    assert!(writer.flush().is_err());
    Ok(())
}
