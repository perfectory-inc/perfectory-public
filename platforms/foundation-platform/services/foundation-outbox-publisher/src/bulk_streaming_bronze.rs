//! Shared bounded-memory streaming write port for large Bronze bulk files.
//!
//! This is the streaming half of the Bronze commit boundary (ADR 0016): it implements the
//! committer's narrow [`BronzeStreamingRawObjectWriter`] seam so the large-file streaming write
//! (which computes sha256 INCREMENTALLY as bytes flow to R2) and its 412 GET-rehash recovery flow
//! through the same single committer the in-memory page lanes use. `catalog-application` never touches the
//! provider bytes — the body stream is captured here, around the storage adapter.

use std::{
    fmt::Write as _,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
};

use anyhow::{bail, Context};
use async_trait::async_trait;
use bytes::Bytes;
use collection_application::{
    BronzeStorageError, BronzeStreamingRawObjectWriter, BronzeStreamingWriteOutcome,
    BronzeStreamingWriteRequest, StreamedObjectRehash,
};
use collection_domain::CollectionError;
use foundation_outbox::{
    object_storage::{ByteStream, ObjectWriteMode, StreamingPutObjectRequest},
    ObjectStorageStreamingService, PublishError,
};
use futures_util::{
    stream::{self, BoxStream},
    Stream, StreamExt as _,
};
use http_body::Frame;
use sha2::{Digest, Sha256};

const BRONZE_CACHE_CONTROL: &str = "no-store, max-age=0";

/// Services-layer streaming write port bridging the low-level [`ObjectStorageStreamingService`] to
/// the committer's [`BronzeStreamingRawObjectWriter`] seam.
///
/// Holds the provider byte source (taken on first write) plus the storage adapter, so the committer
/// can request a write-once streaming `CreateOnly` put + a recovery GET-rehash without ever touching
/// the bytes itself. The body is consumed at most once: a `CreateOnly` 412 collision is rejected by
/// `If-None-Match: *` before the body is read, so the committer's recovery path GET-rehashes the
/// existing object instead of re-streaming (the provider is not re-downloaded).
pub(crate) struct BronzeStreamingObjectStorageWriter<'a, Storage: ?Sized> {
    storage: &'a Storage,
    content_type: String,
    body: Mutex<Option<BoxStream<'static, Result<Bytes, CollectionError>>>>,
}

impl<'a, Storage> BronzeStreamingObjectStorageWriter<'a, Storage>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    /// Wraps a storage adapter + a provider byte source as a committer streaming write port.
    pub(crate) fn new(
        storage: &'a Storage,
        content_type: String,
        body: BoxStream<'static, Result<Bytes, CollectionError>>,
    ) -> Self {
        Self {
            storage,
            content_type,
            body: Mutex::new(Some(body)),
        }
    }

    fn take_body(
        &self,
    ) -> Result<BoxStream<'static, Result<Bytes, CollectionError>>, BronzeStorageError> {
        self.body
            .lock()
            .map_err(|_| BronzeStorageError("streaming body mutex poisoned".to_owned()))?
            .take()
            .ok_or_else(|| {
                BronzeStorageError(
                    "streaming Bronze body already consumed; the committer must stream at most once"
                        .to_owned(),
                )
            })
    }
}

#[async_trait]
impl<Storage> BronzeStreamingRawObjectWriter for BronzeStreamingObjectStorageWriter<'_, Storage>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    async fn write_streaming_object(
        &self,
        request: BronzeStreamingWriteRequest,
    ) -> Result<BronzeStreamingWriteOutcome, BronzeStorageError> {
        let body = self.take_body()?;
        match stream_bronze_object_create_only(self.storage, &request, &self.content_type, body)
            .await
        {
            Ok(outcome) => Ok(outcome),
            // Preserve the full anyhow context chain (`{:#}`) so the underlying storage cause (e.g.
            // an R2 outage message) survives into the committer's `Storage` error and the run's
            // recorded failure message, instead of collapsing to only the outer context.
            Err(error) => Err(BronzeStorageError(format!("{error:#}"))),
        }
    }

    async fn read_object_sha256_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamedObjectRehash>, BronzeStorageError> {
        self.storage
            .read_object_sha256_and_size_by_rehash(key)
            .await
            .map(|rehash| {
                rehash.map(|rehash| StreamedObjectRehash {
                    checksum_sha256: rehash.checksum_sha256,
                    size_bytes: rehash.size_bytes,
                })
            })
            .map_err(|error| BronzeStorageError(error.to_string()))
    }
}

/// Streams a provider byte source to `request.key` write-once (`CreateOnly` / `If-None-Match: *`),
/// computing sha256 + size INCREMENTALLY as the bytes flow to storage.
///
/// On success returns [`BronzeStreamingWriteOutcome::Written`] with the in-flight checksum + size
/// (after asserting the streamed length equals the declared `Content-Length`). A `CreateOnly`
/// collision (the storage maps R2's `412` to [`PublishError::ObjectAlreadyExists`]) returns
/// [`BronzeStreamingWriteOutcome::AlreadyExists`] — NOT an error — so the committer reconciles by
/// GET-rehash. An empty body or an HTML-instead-of-file payload fails loud.
async fn stream_bronze_object_create_only<Storage>(
    storage: &Storage,
    request: &BronzeStreamingWriteRequest,
    content_type: &str,
    mut source_body: BoxStream<'static, Result<Bytes, CollectionError>>,
) -> anyhow::Result<BronzeStreamingWriteOutcome>
where
    Storage: ObjectStorageStreamingService + ?Sized,
{
    let expected_size_bytes = request.expected_size_bytes;
    let object_key = request.key.clone();

    let first_chunk = source_body
        .next()
        .await
        .transpose()
        .context("failed to read first provider bulk file chunk")?
        .with_context(|| format!("provider file {object_key} response body was empty"))?;
    if is_html_payload(content_type, &first_chunk) {
        bail!("provider file {object_key} returned HTML instead of a provider file");
    }

    let digest_state = Arc::new(Mutex::new(StreamingDigestState {
        hasher: Sha256::new(),
        size_bytes: 0,
    }));
    let body_digest_state = Arc::clone(&digest_state);
    let body = stream::once(async move { Ok(first_chunk) })
        .chain(source_body)
        .map(move |chunk| {
            let bytes = chunk.map_err(|error| io::Error::other(error.to_string()))?;
            let mut state = body_digest_state
                .lock()
                .map_err(|_| io::Error::other("streaming digest state mutex poisoned"))?;
            state.hasher.update(&bytes);
            state.size_bytes = state
                .size_bytes
                .checked_add(
                    u64::try_from(bytes.len())
                        .map_err(|_| io::Error::other("provider chunk length overflowed u64"))?,
                )
                .ok_or_else(|| io::Error::other("provider stream length overflowed u64"))?;
            Ok::<Frame<Bytes>, io::Error>(Frame::data(bytes))
        })
        .boxed();
    let byte_stream = ByteStream::from_body_1_x(SyncUploadBody::new(body));

    // Bronze raw streaming write: CreateOnly (`If-None-Match: *`). The sha is known only AFTER the
    // stream completes, so it cannot be stamped as x-amz-meta-sha256 at write time — a later 412
    // collision is reconciled by GET-rehash, not HEAD metadata (the committer's streaming recovery).
    let put_result = storage
        .put_streaming_object(StreamingPutObjectRequest {
            key: object_key.clone(),
            content_type: content_type.to_owned(),
            cache_control: BRONZE_CACHE_CONTROL.to_owned(),
            size_bytes: expected_size_bytes,
            body: byte_stream,
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await;

    match put_result {
        Ok(()) => {}
        // CreateOnly collision is NOT a failure: report it so the committer reconciles by GET-rehash.
        Err(PublishError::ObjectAlreadyExists { .. }) => {
            return Ok(BronzeStreamingWriteOutcome::AlreadyExists)
        }
        Err(error) => {
            return Err(anyhow::Error::new(error))
                .with_context(|| format!("failed to stream Bronze object: {object_key}"))
        }
    }

    let (actual_size_bytes, checksum_sha256) = finish_digest(&digest_state)?;
    if actual_size_bytes != expected_size_bytes {
        bail!(
            "provider file {object_key} streamed {actual_size_bytes} bytes but Content-Length declared {expected_size_bytes}"
        );
    }
    Ok(BronzeStreamingWriteOutcome::Written {
        checksum_sha256,
        size_bytes: actual_size_bytes,
    })
}

struct StreamingDigestState {
    hasher: Sha256,
    size_bytes: u64,
}

type UploadFrameStream = Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, io::Error>> + Send>>;

struct SyncUploadBody {
    stream: Arc<Mutex<UploadFrameStream>>,
}

impl SyncUploadBody {
    fn new(stream: UploadFrameStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

impl http_body::Body for SyncUploadBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.stream.lock() {
            Ok(mut stream) => stream.as_mut().poll_next(cx),
            Err(_) => Poll::Ready(Some(Err(io::Error::other(
                "upload frame stream mutex poisoned",
            )))),
        }
    }
}

fn finish_digest(state: &Arc<Mutex<StreamingDigestState>>) -> anyhow::Result<(u64, String)> {
    let state = state
        .lock()
        .map_err(|_| anyhow::anyhow!("streaming digest state mutex poisoned"))?;
    Ok((
        state.size_bytes,
        sha256_hex(&state.hasher.clone().finalize()),
    ))
}

fn sha256_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}

fn is_html_payload(content_type: &str, first_chunk: &[u8]) -> bool {
    let normalized_content_type = content_type.to_ascii_lowercase();
    normalized_content_type.contains("text/html")
        || normalized_content_type.contains("application/xhtml")
        || first_chunk
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'<')
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use async_trait::async_trait;
    use bytes::Bytes;
    use collection_application::{
        BronzeStreamingRawObjectWriter, BronzeStreamingWriteOutcome, BronzeStreamingWriteRequest,
    };
    use collection_domain::CollectionError;
    use foundation_outbox::{
        object_storage::{
            ObjectWriteMode, PutObjectRequest, StreamingObjectRehash, StreamingPutObjectRequest,
        },
        ObjectStorageService, ObjectStorageStreamingService, PublishError,
    };
    use futures_util::{stream, StreamExt as _};
    use sha2::{Digest, Sha256};

    use super::BronzeStreamingObjectStorageWriter;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// The writer streams the provider bytes to storage with `CreateOnly` and reports the in-flight
    /// checksum + size to the committer (the seam the committer records the row from).
    #[tokio::test]
    async fn write_streaming_object_streams_create_only_and_reports_inflight_checksum() -> TestResult
    {
        let body = stream::iter([
            Ok::<Bytes, CollectionError>(Bytes::from_static(b"PK\x03\x04")),
            Ok(Bytes::from_static(b"provider zip bytes")),
        ])
        .boxed();
        let expected_bytes = b"PK\x03\x04provider zip bytes".to_vec();
        let storage = RecordingObjectStorage::default();
        let writer =
            BronzeStreamingObjectStorageWriter::new(&storage, "application/zip".to_owned(), body);

        let outcome = writer
            .write_streaming_object(BronzeStreamingWriteRequest {
                key: "bronze/source=hubgokr__building_register_main/OPN1.zip".to_owned(),
                content_type: "application/zip".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
                expected_size_bytes: expected_bytes.len() as u64,
            })
            .await?;

        assert_eq!(storage.writes()?.len(), 0);
        let streaming_writes = storage.streaming_writes()?;
        assert_eq!(streaming_writes.len(), 1);
        assert_eq!(streaming_writes[0].body, expected_bytes);
        assert_eq!(streaming_writes[0].size_bytes, expected_bytes.len() as u64);
        // The committer's write-once policy: streaming bulk writes are CreateOnly.
        assert_eq!(streaming_writes[0].write_mode, ObjectWriteMode::CreateOnly);
        assert_eq!(streaming_writes[0].content_type, "application/zip");
        match outcome {
            BronzeStreamingWriteOutcome::Written {
                checksum_sha256,
                size_bytes,
            } => {
                assert_eq!(checksum_sha256, sha256_hex(&expected_bytes));
                assert_eq!(size_bytes, expected_bytes.len() as u64);
            }
            other => return Err(format!("expected Written, got {other:?}").into()),
        }
        Ok(())
    }

    /// A `CreateOnly` collision maps to `AlreadyExists` (not an error) so the committer reconciles
    /// by GET-rehash, and the read-back rehash bridges the storage rehash to the committer seam.
    #[tokio::test]
    async fn write_streaming_object_maps_already_exists_and_bridges_rehash() -> TestResult {
        let body = stream::iter([Ok::<Bytes, CollectionError>(Bytes::from_static(
            b"PK\x03\x04zip",
        ))])
        .boxed();
        let storage = RecordingObjectStorage::already_exists(StreamingObjectRehash {
            checksum_sha256: "a".repeat(64),
            size_bytes: 7,
            observed_e_tag: None,
            observed_last_modified: None,
        });
        let writer =
            BronzeStreamingObjectStorageWriter::new(&storage, "application/zip".to_owned(), body);

        let outcome = writer
            .write_streaming_object(BronzeStreamingWriteRequest {
                key: "bronze/source=hubgokr__building_register_main/OPN1.zip".to_owned(),
                content_type: "application/zip".to_owned(),
                cache_control: "no-store, max-age=0".to_owned(),
                expected_size_bytes: 7,
            })
            .await?;
        assert!(matches!(
            outcome,
            BronzeStreamingWriteOutcome::AlreadyExists
        ));

        let rehash = writer
            .read_object_sha256_by_rehash("bronze/source=hubgokr__building_register_main/OPN1.zip")
            .await?
            .ok_or("expected a rehash for the existing object")?;
        assert_eq!(rehash.checksum_sha256, "a".repeat(64));
        assert_eq!(rehash.size_bytes, 7);
        Ok(())
    }

    #[derive(Debug, Default)]
    struct RecordingObjectStorage {
        writes: Mutex<Vec<PutObjectRequest>>,
        streaming_writes: Mutex<Vec<StreamingWriteRecord>>,
        already_exists: bool,
        rehash: Option<StreamingObjectRehash>,
    }

    impl RecordingObjectStorage {
        fn already_exists(rehash: StreamingObjectRehash) -> Self {
            Self {
                already_exists: true,
                rehash: Some(rehash),
                ..Self::default()
            }
        }

        fn writes(&self) -> Result<Vec<PutObjectRequest>, PublishError> {
            Ok(lock(&self.writes, "writes")?.clone())
        }

        fn streaming_writes(&self) -> Result<Vec<StreamingWriteRecord>, PublishError> {
            Ok(lock(&self.streaming_writes, "streaming_writes")?.clone())
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StreamingWriteRecord {
        key: String,
        content_type: String,
        write_mode: ObjectWriteMode,
        body: Vec<u8>,
        size_bytes: u64,
    }

    #[async_trait]
    impl ObjectStorageService for RecordingObjectStorage {
        async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
            lock(&self.writes, "writes")?.push(request);
            Ok(())
        }

        async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl ObjectStorageStreamingService for RecordingObjectStorage {
        async fn put_streaming_object(
            &self,
            request: StreamingPutObjectRequest,
        ) -> Result<(), PublishError> {
            if self.already_exists {
                return Err(PublishError::ObjectAlreadyExists { key: request.key });
            }
            let key = request.key;
            let content_type = request.content_type;
            let write_mode = request.write_mode;
            let size_bytes = request.size_bytes;
            let body = request
                .body
                .collect()
                .await
                .map_err(|error| {
                    PublishError::Infrastructure(format!(
                        "streaming test body read failed: {error}"
                    ))
                })?
                .into_bytes()
                .to_vec();
            lock(&self.streaming_writes, "streaming_writes")?.push(StreamingWriteRecord {
                key,
                content_type,
                write_mode,
                body,
                size_bytes,
            });
            Ok(())
        }

        async fn read_object_sha256_and_size_by_rehash(
            &self,
            _key: &str,
        ) -> Result<Option<StreamingObjectRehash>, PublishError> {
            Ok(self.rehash.clone())
        }
    }

    fn lock<'a, T>(
        mutex: &'a Mutex<T>,
        name: &'static str,
    ) -> Result<MutexGuard<'a, T>, PublishError> {
        mutex
            .lock()
            .map_err(|_| PublishError::Infrastructure(format!("{name} mutex poisoned")))
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .fold(String::with_capacity(64), |mut checksum, byte| {
                use std::fmt::Write as _;

                let _ = write!(&mut checksum, "{byte:02x}");
                checksum
            })
    }
}
