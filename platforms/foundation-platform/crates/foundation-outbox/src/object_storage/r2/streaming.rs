//! Bounded random-access reads and atomic multipart writes over the R2 S3 client.

use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use tokio::runtime::Handle;

use crate::errors::PublishError;

use super::{r2_range_header, R2ObjectStorage, R2_RANGE_READ_MAX_ATTEMPTS};

const MIN_MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024;
const MAX_MULTIPART_PART_BYTES: usize = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_PARTS: i32 = 10_000;

trait RangeBackend: Send + Sync + std::fmt::Debug {
    fn len(&self) -> u64;
    fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>>;
}

/// Shared counters for one logical R2 object read, including all cloned seek readers.
#[derive(Clone, Debug, Default)]
pub struct R2ReadRequestMetrics {
    head_requests: Arc<AtomicU64>,
    get_requests: Arc<AtomicU64>,
}

impl R2ReadRequestMetrics {
    /// Returns the actual number of S3 `HeadObject` requests attempted.
    #[must_use]
    pub fn head_request_count(&self) -> u64 {
        self.head_requests.load(Ordering::Relaxed)
    }

    /// Returns the actual number of S3 ranged `GetObject` requests attempted, including retries.
    #[must_use]
    pub fn get_request_count(&self) -> u64 {
        self.get_requests.load(Ordering::Relaxed)
    }

    /// Returns all R2 read requests attempted by the logical reader.
    #[must_use]
    pub fn total_request_count(&self) -> u64 {
        self.head_request_count() + self.get_request_count()
    }
}

/// A cloneable `Read + Seek` facade backed by bounded object byte ranges.
///
/// Clones share request counters and immutable object identity, but never clone the cached range.
/// Seeking changes only the logical position; an R2 request occurs on the next read miss.
pub struct R2SeekableObjectReader {
    backend: Arc<dyn RangeBackend>,
    chunk_bytes: usize,
    position: u64,
    buffer_start: u64,
    buffer: Vec<u8>,
    metrics: R2ReadRequestMetrics,
}

impl std::fmt::Debug for R2SeekableObjectReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("R2SeekableObjectReader")
            .field("object_bytes", &self.backend.len())
            .field("chunk_bytes", &self.chunk_bytes)
            .field("position", &self.position)
            .field("buffered_bytes", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

impl Clone for R2SeekableObjectReader {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            chunk_bytes: self.chunk_bytes,
            position: self.position,
            buffer_start: 0,
            buffer: Vec::new(),
            metrics: self.metrics.clone(),
        }
    }
}

impl R2SeekableObjectReader {
    fn from_backend(
        backend: Arc<dyn RangeBackend>,
        chunk_bytes: usize,
    ) -> Result<Self, PublishError> {
        if chunk_bytes == 0 {
            return Err(PublishError::Infrastructure(
                "R2 seek reader chunk size must be positive".to_owned(),
            ));
        }
        Ok(Self {
            backend,
            chunk_bytes,
            position: 0,
            buffer_start: 0,
            buffer: Vec::new(),
            metrics: R2ReadRequestMetrics::default(),
        })
    }

    fn from_r2_backend(
        backend: Arc<dyn RangeBackend>,
        chunk_bytes: usize,
        metrics: R2ReadRequestMetrics,
    ) -> Result<Self, PublishError> {
        let mut reader = Self::from_backend(backend, chunk_bytes)?;
        reader.metrics = metrics;
        Ok(reader)
    }

    /// Returns the immutable object length observed before the first range request.
    #[must_use]
    pub fn object_bytes(&self) -> u64 {
        self.backend.len()
    }

    /// Returns shared request counters for this reader and every clone derived from it.
    #[must_use]
    pub fn request_metrics(&self) -> R2ReadRequestMetrics {
        self.metrics.clone()
    }

    fn buffer_contains_position(&self) -> bool {
        self.position >= self.buffer_start
            && self.position < self.buffer_start + self.buffer.len() as u64
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        let object_bytes = self.backend.len();
        if self.position >= object_bytes {
            self.buffer.clear();
            return Ok(());
        }
        let chunk_bytes = self.chunk_bytes as u64;
        let start = (self.position / chunk_bytes) * chunk_bytes;
        let end = start
            .checked_add(chunk_bytes - 1)
            .unwrap_or(u64::MAX)
            .min(object_bytes - 1);
        let bytes = self.backend.read_range(start, end)?;
        let expected = usize::try_from(end - start + 1)
            .map_err(|_| io::Error::other("R2 byte range length does not fit usize"))?;
        if bytes.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "R2 byte range {start}-{end} returned {} bytes, expected {expected}",
                    bytes.len()
                ),
            ));
        }
        self.buffer_start = start;
        self.buffer = bytes;
        Ok(())
    }
}

impl Read for R2SeekableObjectReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.position >= self.backend.len() {
            return Ok(0);
        }
        if !self.buffer_contains_position() {
            self.fill_buffer()?;
        }
        let offset = usize::try_from(self.position - self.buffer_start)
            .map_err(|_| io::Error::other("R2 buffer offset does not fit usize"))?;
        let available = &self.buffer[offset..];
        let copied = output.len().min(available.len());
        output[..copied].copy_from_slice(&available[..copied]);
        self.position += copied as u64;
        Ok(copied)
    }
}

impl Seek for R2SeekableObjectReader {
    fn seek(&mut self, seek_from: SeekFrom) -> io::Result<u64> {
        let object_bytes = self.backend.len();
        let position = match seek_from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::End(offset) => object_bytes.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "R2 seek overflow"))?;
        if position > object_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "R2 seek exceeds object length",
            ));
        }
        self.position = position;
        Ok(position)
    }
}

#[derive(Debug)]
struct R2RangeBackend {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    object_bytes: u64,
    e_tag: Option<String>,
    handle: Handle,
    metrics: R2ReadRequestMetrics,
}

impl RangeBackend for R2RangeBackend {
    fn len(&self) -> u64 {
        self.object_bytes
    }

    fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let start = i64::try_from(start)
            .map_err(|_| io::Error::other("R2 byte range start does not fit i64"))?;
        let end = i64::try_from(end)
            .map_err(|_| io::Error::other("R2 byte range end does not fit i64"))?;
        let range = r2_range_header(start, end).map_err(publish_io_error)?;
        for attempt in 1..=R2_RANGE_READ_MAX_ATTEMPTS {
            self.metrics.get_requests.fetch_add(1, Ordering::Relaxed);
            let mut request = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .range(range.clone());
            if let Some(e_tag) = &self.e_tag {
                request = request.if_match(e_tag);
            }
            let result = self.handle.block_on(request.send());
            match result {
                Ok(output) => match self.handle.block_on(output.body.collect()) {
                    Ok(body) => return Ok(body.into_bytes().to_vec()),
                    Err(error) if attempt < R2_RANGE_READ_MAX_ATTEMPTS => {
                        tracing::warn!(
                            key = self.key,
                            range,
                            attempt,
                            error = %error,
                            "retrying bounded R2 stream body"
                        );
                    }
                    Err(error) => {
                        return Err(io::Error::other(format!(
                            "failed to read R2 stream body {} {range}: {error}",
                            self.key
                        )))
                    }
                },
                Err(error) if attempt < R2_RANGE_READ_MAX_ATTEMPTS => {
                    tracing::warn!(
                        key = self.key,
                        range,
                        attempt,
                        error = %error,
                        "retrying bounded R2 stream range"
                    );
                }
                Err(error) => {
                    return Err(io::Error::other(format!(
                        "failed to read R2 stream range {} {range}: {error}",
                        self.key
                    )))
                }
            }
        }
        Err(io::Error::other("R2 range retry loop ended unexpectedly"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UploadedPart {
    part_number: i32,
    e_tag: String,
}

trait MultipartBackend: Send + Sync + std::fmt::Debug {
    fn upload_part(&self, part_number: i32, body: Vec<u8>) -> Result<String, String>;
    fn complete(&self, parts: &[UploadedPart]) -> Result<(), String>;
    fn abort(&self) -> Result<(), String>;
}

/// Request and byte counts returned after an R2 multipart upload is atomically completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct R2MultipartUploadReport {
    /// Exact bytes accepted from the synchronous writer.
    pub output_bytes: u64,
    /// Number of `UploadPart` requests made.
    pub upload_part_request_count: u64,
    /// Number of completed parts.
    pub part_count: u64,
}

/// Synchronous bounded-memory writer for one atomic R2 multipart object.
///
/// Full equal-sized parts are uploaded as the caller writes. The target object becomes visible
/// only after [`Self::complete`]; dropping any unfinished writer aborts the multipart upload.
pub struct R2MultipartUploadWriter {
    backend: Arc<dyn MultipartBackend>,
    part_bytes: usize,
    buffer: Vec<u8>,
    parts: Vec<UploadedPart>,
    output_bytes: u64,
    upload_part_request_count: u64,
    finalized: bool,
}

impl std::fmt::Debug for R2MultipartUploadWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("R2MultipartUploadWriter")
            .field("part_bytes", &self.part_bytes)
            .field("buffered_bytes", &self.buffer.len())
            .field("completed_parts", &self.parts.len())
            .field("output_bytes", &self.output_bytes)
            .field("finalized", &self.finalized)
            .finish_non_exhaustive()
    }
}

impl R2MultipartUploadWriter {
    fn from_backend(
        backend: Arc<dyn MultipartBackend>,
        part_bytes: usize,
    ) -> Result<Self, PublishError> {
        if part_bytes == 0 {
            return Err(PublishError::Infrastructure(
                "multipart part size must be positive".to_owned(),
            ));
        }
        Ok(Self {
            backend,
            part_bytes,
            buffer: Vec::with_capacity(part_bytes),
            parts: Vec::new(),
            output_bytes: 0,
            upload_part_request_count: 0,
            finalized: false,
        })
    }

    fn upload_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() && !self.parts.is_empty() {
            return Ok(());
        }
        let part_number = i32::try_from(self.parts.len() + 1)
            .map_err(|_| io::Error::other("multipart part number overflow"))?;
        if part_number > MAX_MULTIPART_PARTS {
            return Err(io::Error::other("multipart upload exceeds 10,000 parts"));
        }
        let body = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.part_bytes));
        self.upload_part_request_count += 1;
        let e_tag = self
            .backend
            .upload_part(part_number, body)
            .map_err(io::Error::other)?;
        self.parts.push(UploadedPart {
            part_number,
            e_tag,
        });
        Ok(())
    }

    /// Uploads the trailing part and atomically exposes the completed object.
    ///
    /// # Errors
    /// Returns `PublishError` when the last part or completion request fails. In that case the
    /// multipart upload is aborted before returning whenever R2 remains reachable.
    pub fn complete(mut self) -> Result<R2MultipartUploadReport, PublishError> {
        if let Err(error) = self.upload_buffer() {
            self.abort_best_effort();
            return Err(PublishError::Broadcaster(format!(
                "failed to upload final R2 multipart part: {error}"
            )));
        }
        if let Err(error) = self.backend.complete(&self.parts) {
            self.abort_best_effort();
            return Err(PublishError::Broadcaster(format!(
                "failed to complete R2 multipart upload: {error}"
            )));
        }
        self.finalized = true;
        Ok(R2MultipartUploadReport {
            output_bytes: self.output_bytes,
            upload_part_request_count: self.upload_part_request_count,
            part_count: self.parts.len() as u64,
        })
    }

    fn abort_best_effort(&mut self) {
        if !self.finalized {
            if let Err(error) = self.backend.abort() {
                tracing::error!(error, "failed to abort incomplete R2 multipart upload");
            }
            self.finalized = true;
        }
    }
}

impl Write for R2MultipartUploadWriter {
    fn write(&mut self, mut input: &[u8]) -> io::Result<usize> {
        if self.finalized {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "R2 multipart writer is finalized",
            ));
        }
        let input_len = input.len();
        while !input.is_empty() {
            let available = self.part_bytes - self.buffer.len();
            let copied = available.min(input.len());
            self.buffer.extend_from_slice(&input[..copied]);
            self.output_bytes = self
                .output_bytes
                .checked_add(copied as u64)
                .ok_or_else(|| io::Error::other("multipart output byte count overflow"))?;
            input = &input[copied..];
            if self.buffer.len() == self.part_bytes {
                if let Err(error) = self.upload_buffer() {
                    self.abort_best_effort();
                    return Err(error);
                }
            }
        }
        Ok(input_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for R2MultipartUploadWriter {
    fn drop(&mut self) {
        self.abort_best_effort();
    }
}

#[derive(Debug)]
struct R2MultipartBackend {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    upload_id: String,
    handle: Handle,
}

impl MultipartBackend for R2MultipartBackend {
    fn upload_part(&self, part_number: i32, body: Vec<u8>) -> Result<String, String> {
        let output = self
            .handle
            .block_on(
                self.client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(&self.upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(body))
                    .send(),
            )
            .map_err(|error| format!("failed to upload R2 part {part_number}: {error}"))?;
        output
            .e_tag()
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("R2 part {part_number} response omitted ETag"))
    }

    fn complete(&self, parts: &[UploadedPart]) -> Result<(), String> {
        let completed = parts
            .iter()
            .map(|part| {
                CompletedPart::builder()
                    .part_number(part.part_number)
                    .e_tag(&part.e_tag)
                    .build()
            })
            .collect::<Vec<_>>();
        let multipart = CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        self.handle
            .block_on(
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(&self.upload_id)
                    .multipart_upload(multipart)
                    .if_none_match("*")
                    .send(),
            )
            .map_err(|error| format!("failed to complete R2 object {}: {error}", self.key))?;
        Ok(())
    }

    fn abort(&self) -> Result<(), String> {
        self.handle
            .block_on(
                self.client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(&self.upload_id)
                    .send(),
            )
            .map_err(|error| format!("failed to abort R2 object {}: {error}", self.key))?;
        Ok(())
    }
}

impl R2ObjectStorage {
    /// Opens an immutable R2 object as a bounded-cache `Read + Seek` source.
    ///
    /// The initial `HeadObject` fixes content length and ETag. Every subsequent ranged GET carries
    /// `If-Match` when R2 supplied an ETag, so a source mutation fails closed.
    ///
    /// # Errors
    /// Returns `PublishError` when the key or chunk size is invalid, R2 rejects the head request,
    /// or the object length cannot be represented.
    pub async fn open_seekable_object(
        &self,
        key: &str,
        chunk_bytes: usize,
    ) -> Result<R2SeekableObjectReader, PublishError> {
        super::validate_relative_r2_object_key(key, "key")?;
        if chunk_bytes == 0 {
            return Err(PublishError::Infrastructure(
                "R2 seek reader chunk size must be positive".to_owned(),
            ));
        }
        let metrics = R2ReadRequestMetrics::default();
        metrics.head_requests.fetch_add(1, Ordering::Relaxed);
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!("failed to head R2 stream object {key}: {error}"))
            })?;
        let object_bytes = head.content_length().ok_or_else(|| {
            PublishError::Broadcaster(format!("R2 stream object {key} omitted content length"))
        })?;
        let object_bytes = u64::try_from(object_bytes).map_err(|_| {
            PublishError::Broadcaster(format!(
                "R2 stream object {key} reported negative content length"
            ))
        })?;
        let backend = Arc::new(R2RangeBackend {
            client: self.client.clone(),
            bucket: self.bucket_name.clone(),
            key: key.to_owned(),
            object_bytes,
            e_tag: head.e_tag().map(ToOwned::to_owned),
            handle: Handle::current(),
            metrics: metrics.clone(),
        });
        R2SeekableObjectReader::from_r2_backend(backend, chunk_bytes, metrics)
    }

    /// Starts a create-only multipart upload and returns a bounded synchronous writer.
    ///
    /// # Errors
    /// Returns `PublishError` for an unsafe key, an R2-invalid part size, or a rejected
    /// `CreateMultipartUpload` request.
    pub async fn start_create_only_multipart_upload(
        &self,
        key: &str,
        content_type: &str,
        cache_control: &str,
        part_bytes: usize,
    ) -> Result<R2MultipartUploadWriter, PublishError> {
        super::validate_relative_r2_object_key(key, "key")?;
        if !(MIN_MULTIPART_PART_BYTES..=MAX_MULTIPART_PART_BYTES).contains(&part_bytes) {
            return Err(PublishError::Infrastructure(format!(
                "R2 multipart part size must be between {MIN_MULTIPART_PART_BYTES} and {MAX_MULTIPART_PART_BYTES} bytes"
            )));
        }
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(key)
            .content_type(content_type)
            .cache_control(cache_control)
            .send()
            .await
            .map_err(|error| {
                PublishError::Broadcaster(format!(
                    "failed to create R2 multipart upload {key}: {error}"
                ))
            })?;
        let upload_id = output.upload_id().ok_or_else(|| {
            PublishError::Broadcaster(format!(
                "R2 create multipart response omitted upload id for {key}"
            ))
        })?;
        let backend = Arc::new(R2MultipartBackend {
            client: self.client.clone(),
            bucket: self.bucket_name.clone(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            handle: Handle::current(),
        });
        R2MultipartUploadWriter::from_backend(backend, part_bytes)
    }
}

fn publish_io_error(error: PublishError) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read as _, Seek as _, SeekFrom, Write as _},
        sync::{Arc, Mutex},
    };

    use super::{
        MultipartBackend, R2MultipartUploadWriter, R2SeekableObjectReader, RangeBackend,
        UploadedPart,
    };

    #[derive(Debug)]
    struct MemoryRangeBackend {
        bytes: Vec<u8>,
        calls: Mutex<Vec<(u64, u64)>>,
    }

    impl RangeBackend for MemoryRangeBackend {
        fn len(&self) -> u64 {
            self.bytes.len() as u64
        }

        fn read_range(&self, start: u64, end: u64) -> std::io::Result<Vec<u8>> {
            self.calls.lock().map_err(lock_error)?.push((start, end));
            Ok(self.bytes[start as usize..=end as usize].to_vec())
        }
    }

    #[test]
    fn seekable_reader_fetches_only_bounded_ranges_and_clones_without_buffers() -> TestResult {
        let backend = Arc::new(MemoryRangeBackend {
            bytes: b"0123456789abcdef".to_vec(),
            calls: Mutex::new(Vec::new()),
        });
        let mut reader = R2SeekableObjectReader::from_backend(backend.clone(), 4)?;

        reader.seek(SeekFrom::End(-4))?;
        let mut tail = [0_u8; 4];
        reader.read_exact(&mut tail)?;
        assert_eq!(&tail, b"cdef");

        let mut clone = reader.clone();
        clone.seek(SeekFrom::Start(1))?;
        let mut prefix = [0_u8; 5];
        clone.read_exact(&mut prefix)?;
        assert_eq!(&prefix, b"12345");
        assert_eq!(
            *backend.calls.lock().map_err(lock_error)?,
            [(12, 15), (0, 3), (4, 7)]
        );
        Ok(())
    }

    #[derive(Debug, Default)]
    struct RecordingMultipartBackend {
        uploaded: Mutex<Vec<(i32, Vec<u8>)>>,
        completed: Mutex<Vec<Vec<UploadedPart>>>,
        abort_count: Mutex<u64>,
    }

    impl MultipartBackend for RecordingMultipartBackend {
        fn upload_part(&self, part_number: i32, body: Vec<u8>) -> Result<String, String> {
            self.uploaded
                .lock()
                .map_err(|error| error.to_string())?
                .push((part_number, body));
            Ok(format!("etag-{part_number}"))
        }

        fn complete(&self, parts: &[UploadedPart]) -> Result<(), String> {
            self.completed
                .lock()
                .map_err(|error| error.to_string())?
                .push(parts.to_vec());
            Ok(())
        }

        fn abort(&self) -> Result<(), String> {
            *self
                .abort_count
                .lock()
                .map_err(|error| error.to_string())? += 1;
            Ok(())
        }
    }

    #[test]
    fn multipart_writer_uploads_uniform_parts_and_exposes_only_on_complete() -> TestResult {
        let backend = Arc::new(RecordingMultipartBackend::default());
        let mut writer = R2MultipartUploadWriter::from_backend(backend.clone(), 4)?;

        writer.write_all(b"abcdefghij")?;
        assert!(backend.completed.lock().map_err(lock_error)?.is_empty());
        let report = writer.complete()?;

        assert_eq!(report.output_bytes, 10);
        assert_eq!(report.upload_part_request_count, 3);
        assert_eq!(
            *backend.uploaded.lock().map_err(lock_error)?,
            [
                (1, b"abcd".to_vec()),
                (2, b"efgh".to_vec()),
                (3, b"ij".to_vec())
            ]
        );
        assert_eq!(backend.completed.lock().map_err(lock_error)?.len(), 1);
        assert_eq!(*backend.abort_count.lock().map_err(lock_error)?, 0);
        Ok(())
    }

    #[test]
    fn dropping_unfinished_multipart_writer_aborts_upload() -> TestResult {
        let backend = Arc::new(RecordingMultipartBackend::default());
        {
            let mut writer = R2MultipartUploadWriter::from_backend(backend.clone(), 4)?;
            writer.write_all(b"abc")?;
        }

        assert!(backend.completed.lock().map_err(lock_error)?.is_empty());
        assert_eq!(*backend.abort_count.lock().map_err(lock_error)?, 1);
        Ok(())
    }

    fn lock_error<T>(error: std::sync::PoisonError<T>) -> std::io::Error {
        std::io::Error::other(error.to_string())
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;
}
