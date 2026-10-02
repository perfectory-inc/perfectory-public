//! Shared native Parquet buffering, file chunks, and writer/reader byte policy.
use std::{
    fs::File,
    io::{BufWriter, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{ensure, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::{metadata::RowGroupMetaData, properties::WriterProperties},
};
use serde::Serialize;

use crate::{
    bounded_bytes::BoundedByteCount,
    lakehouse_engine_contract::{serving_parquet_policy, ServingParquetPolicy},
};

/// 공통 바이트 예산과 footer 검증을 적용하는 Parquet 출력기.
pub struct BoundedParquetWriter<T, W: Write + Send = BufWriter<File>> {
    mode: OutputMode,
    schema: Arc<Schema>,
    rows_to_batch: fn(&[T], Arc<Schema>) -> Result<RecordBatch>,
    create_file_writer: fn(&Path) -> Result<W>,
    policy: ServingParquetPolicy,
    target_bytes: usize,
    buffer: Vec<T>,
    buffer_bytes: usize,
    current_writer: Option<ArrowWriter<W>>,
    current_row_count: usize,
    chunk_count: usize,
    validated_row_groups: usize,
    failed: bool,
}

enum OutputMode {
    Single(PathBuf),
    Chunked { root: PathBuf, chunk_rows: usize },
}

impl<T: Clone + Serialize, W: Write + Send> BoundedParquetWriter<T, W> {
    /// 공통 계약을 읽어 단일 파일 또는 분할 출력을 준비한다.
    /// # Errors
    /// 계약·출력 설정이 잘못되었거나 출력 디렉터리를 준비할 수 없으면 실패한다.
    pub fn new(
        path: PathBuf,
        chunk_rows: Option<usize>,
        schema: Arc<Schema>,
        rows_to_batch: fn(&[T], Arc<Schema>) -> Result<RecordBatch>,
        create_file_writer: fn(&Path) -> Result<W>,
        prepare_clean_output_dir: fn(&Path, &str) -> Result<()>,
    ) -> Result<Self> {
        Self::with_policy(
            path,
            chunk_rows,
            schema,
            rows_to_batch,
            create_file_writer,
            prepare_clean_output_dir,
            *serving_parquet_policy()?,
        )
    }

    fn with_policy(
        path: PathBuf,
        chunk_rows: Option<usize>,
        schema: Arc<Schema>,
        rows_to_batch: fn(&[T], Arc<Schema>) -> Result<RecordBatch>,
        create_file_writer: fn(&Path) -> Result<W>,
        prepare_clean_output_dir: fn(&Path, &str) -> Result<()>,
        policy: ServingParquetPolicy,
    ) -> Result<Self> {
        // No destructive output preparation until all configuration is valid.
        let target_bytes = policy.row_group_target_bytes()?;
        ensure!(chunk_rows != Some(0), "Parquet chunk_rows must be positive");
        let mode = if let Some(chunk_rows) = chunk_rows {
            prepare_clean_output_dir(&path, "chunked Parquet")?;
            OutputMode::Chunked {
                root: path,
                chunk_rows,
            }
        } else {
            OutputMode::Single(path)
        };
        Ok(Self {
            mode,
            schema,
            rows_to_batch,
            create_file_writer,
            policy,
            target_bytes,
            buffer: Vec::new(),
            buffer_bytes: 0,
            current_writer: None,
            current_row_count: 0,
            chunk_count: 0,
            validated_row_groups: 0,
            failed: false,
        })
    }

    /// 행을 제한된 버퍼에 추가하고 필요한 파일·행 그룹을 기록한다.
    /// # Errors
    /// 이미 실패한 출력기, 초과한 행 크기, 변환 또는 파일 쓰기 오류를 반환한다.
    pub fn write_rows(&mut self, rows: &[T]) -> Result<()> {
        ensure!(!self.failed, "Parquet writer has already failed");
        let result = self.write_rows_inner(rows);
        self.failed = result.is_err();
        result
    }

    fn write_rows_inner(&mut self, rows: &[T]) -> Result<()> {
        for row in rows {
            // Flat Silver rows hold their variable data in strings. Count JSON
            // escaping plus the inline row before cloning or opening a file.
            let mut count = BoundedByteCount::with_error(
                self.target_bytes,
                "Parquet input row exceeds byte target",
            );
            count.add_bytes(size_of::<T>())?;
            serde_json::to_writer(&mut count, row)?;
            let row_bytes = count.bytes_written();
            if row_bytes > self.target_bytes - self.buffer_bytes {
                self.flush_batch()?;
            }
            self.ensure_writer()?;
            self.buffer.push(row.clone());
            self.buffer_bytes += row_bytes;
            self.current_row_count = self
                .current_row_count
                .checked_add(1)
                .context("Parquet file row count overflow")?;
            if self.buffer_bytes >= self.target_bytes || self.chunk_boundary_reached() {
                self.flush_batch()?;
            }
            if self.chunk_boundary_reached() {
                self.close_current_writer()?;
            }
        }
        Ok(())
    }

    /// 남은 행을 쓰고 파일을 닫으며 행 그룹의 크기를 검증한다.
    /// # Errors
    /// 쓰기·종료·footer 검증 중 하나라도 실패하면 오류를 반환한다.
    pub fn flush(&mut self) -> Result<()> {
        ensure!(!self.failed, "Parquet writer has already failed");
        let result = self.close_current_writer();
        self.failed = result.is_err();
        result
    }

    fn ensure_writer(&mut self) -> Result<()> {
        if self.current_writer.is_some() {
            return Ok(());
        }
        let path = match &self.mode {
            OutputMode::Single(path) => path.clone(),
            OutputMode::Chunked { root, .. } => {
                self.chunk_count = self
                    .chunk_count
                    .checked_add(1)
                    .context("Parquet chunk count overflow")?;
                self.current_row_count = 0;
                root.join(format!("part-{:06}.parquet", self.chunk_count))
            }
        };
        let writer = (self.create_file_writer)(&path)?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_bytes(Some(self.target_bytes))
            .build();
        self.current_writer = Some(
            ArrowWriter::try_new(writer, Arc::clone(&self.schema), Some(properties))
                .with_context(|| format!("failed to create Parquet writer {}", path.display()))?,
        );
        self.validated_row_groups = 0;
        Ok(())
    }

    const fn chunk_boundary_reached(&self) -> bool {
        matches!(
            self.mode,
            OutputMode::Chunked { chunk_rows, .. } if self.current_row_count >= chunk_rows
        )
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = (self.rows_to_batch)(&self.buffer, Arc::clone(&self.schema))?;
        let writer = self
            .current_writer
            .as_mut()
            .context("Parquet writer is not open")?;
        writer
            .write(&batch)
            .context("failed to write Parquet batch")?;
        // Encoded size can stay small under dictionary/SNAPPY compression while
        // live column buffers grow. Inspect memory after every bounded batch.
        if writer.memory_size() >= self.target_bytes {
            writer
                .flush()
                .context("failed to flush Parquet row group")?;
        }
        validate_new_row_groups(
            self.policy,
            writer.flushed_row_groups(),
            &mut self.validated_row_groups,
        )?;
        self.buffer.clear();
        self.buffer_bytes = 0;
        Ok(())
    }

    fn close_current_writer(&mut self) -> Result<()> {
        self.flush_batch()?;
        if let Some(writer) = self.current_writer.take() {
            let metadata = writer.close().context("failed to close Parquet writer")?;
            // close() emits the final partial row group and flushes the sink.
            validate_new_row_groups(
                self.policy,
                metadata.row_groups(),
                &mut self.validated_row_groups,
            )?;
        }
        Ok(())
    }
}

fn validate_new_row_groups(
    policy: ServingParquetPolicy,
    groups: &[RowGroupMetaData],
    validated: &mut usize,
) -> Result<()> {
    for (index, group) in groups.iter().enumerate().skip(*validated) {
        policy.validate_row_group(index, group.total_byte_size(), group.compressed_size())?;
    }
    *validated = groups.len();
    Ok(())
}

#[cfg(test)]
mod tests;
