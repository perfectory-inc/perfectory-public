//! Disk spool for content-addressed Bronze keys (root ADR-0168, amending ADR-0152).
//!
//! A content-addressed key names the SHA-256 of the bytes, so the bytes must be read whole before the
//! key is known. Holding them in memory capped a file at 256 MiB; the swept VWorld land datasets list
//! files up to about 1.5 GiB. The body is therefore written to a temporary file in a declared spool
//! directory while it is hashed, then uploaded from that file (the storage adapter switches to its
//! multipart path by size), and the file is removed when the [`SpoolFile`] is dropped — on success and
//! on every failure.
//!
//! Before a body is read the spool reserves its declared length against the free space of the spool
//! filesystem, counting every file still in flight, and keeps [`SPOOL_MIN_FREE_BYTES`] free besides.
//! A file that does not fit is refused before its body is read.

use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context};
use bytes::Bytes;
use collection_domain::CollectionError;
use futures_util::{
    stream::{self, BoxStream},
    StreamExt as _,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Free space the spool leaves on its filesystem beyond every reservation.
pub(crate) const SPOOL_MIN_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Read size when the spooled file is streamed to storage.
const SPOOL_READ_CHUNK_BYTES: usize = 1024 * 1024;

type FreeBytes = fn(&Path) -> anyhow::Result<u64>;

/// A directory the content-addressed path spools provider bodies into, with the bytes reserved by
/// the files in flight. Clones share the reservation.
#[derive(Clone)]
pub(crate) struct SpoolDir {
    path: PathBuf,
    reserved: Arc<AtomicU64>,
    free_bytes: FreeBytes,
}

impl SpoolDir {
    /// The spool at `path`, created if missing, measured with the host's free-space call.
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        Self::with_free_bytes(path, crate::lakehouse_tile_bake::free_bytes)
    }

    pub(crate) fn with_free_bytes(path: &Path, free_bytes: FreeBytes) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)
            .with_context(|| format!("failed to create the spool directory {}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            reserved: Arc::new(AtomicU64::new(0)),
            free_bytes,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Reserves `size_bytes` if the filesystem has room for it, every other reservation, and
    /// [`SPOOL_MIN_FREE_BYTES`]. The reservation is released when the returned file is dropped.
    fn reserve(&self, size_bytes: u64, provider_file_id: &str) -> anyhow::Result<Reservation> {
        let free = (self.free_bytes)(&self.path)?;
        let previous = self.reserved.fetch_add(size_bytes, Ordering::SeqCst);
        let reservation = Reservation {
            reserved: Arc::clone(&self.reserved),
            size_bytes,
        };
        let needed = previous
            .saturating_add(size_bytes)
            .saturating_add(SPOOL_MIN_FREE_BYTES);
        if free < needed {
            bail!(
                "provider file {provider_file_id} declares {size_bytes} bytes; the spool {} has {free} bytes free and needs {needed} (files in flight {previous}, kept free {SPOOL_MIN_FREE_BYTES}) — refused before its body was read",
                self.path.display()
            );
        }
        Ok(reservation)
    }
}

struct Reservation {
    reserved: Arc<AtomicU64>,
    size_bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.reserved.fetch_sub(self.size_bytes, Ordering::SeqCst);
    }
}

/// A provider body written whole to the spool, with the SHA-256 that names it. Dropping it removes
/// the file and releases its reservation.
pub(crate) struct SpoolFile {
    file: tempfile::TempPath,
    _reservation: Reservation,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: u64,
}

/// Where an upload body stands in its spooled file. The open handle is dropped before the last
/// owner of the [`SpoolFile`], so the file is closed before it is removed (Windows refuses otherwise).
enum ReadState {
    Unopened(Arc<SpoolFile>),
    Open {
        handle: tokio::fs::File,
        spool: Arc<SpoolFile>,
    },
}

impl SpoolFile {
    pub(crate) fn path(&self) -> &Path {
        &self.file
    }

    /// The spooled bytes as an upload body, read from disk in bounded chunks. The body owns the
    /// file: it is removed when the body and every other owner are gone, whether or not the body
    /// was read.
    pub(crate) fn body_stream(
        self: Arc<Self>,
    ) -> BoxStream<'static, Result<Bytes, CollectionError>> {
        stream::unfold(Some(ReadState::Unopened(self)), |state| async move {
            let (mut handle, spool) = match state? {
                ReadState::Unopened(spool) => match tokio::fs::File::open(spool.path()).await {
                    Ok(handle) => (handle, spool),
                    Err(error) => return Some((Err(spool_read_error(&error)), None)),
                },
                ReadState::Open { handle, spool } => (handle, spool),
            };
            let mut buffer = vec![0_u8; SPOOL_READ_CHUNK_BYTES];
            let read = handle.read(&mut buffer).await;
            match read {
                Ok(read) if read > 0 => {
                    buffer.truncate(read);
                    Some((
                        Ok(Bytes::from(buffer)),
                        Some(ReadState::Open { handle, spool }),
                    ))
                }
                ended => {
                    // Close before the last owner removes the file.
                    drop(handle);
                    drop(spool);
                    ended
                        .err()
                        .map(|error| (Err(spool_read_error(&error)), None))
                }
            }
        })
        // An upload reads until the body ends and may ask once more: `unfold` panics when polled
        // after it ended (2026-10-10 the daily sweep aborted on its first spooled file), a fused
        // stream keeps answering `None`.
        .fuse()
        .boxed()
    }
}

fn spool_read_error(error: &io::Error) -> CollectionError {
    CollectionError::Infrastructure(format!("failed to read spooled provider file: {error}"))
}

/// Writes a provider body to the spool while hashing it.
///
/// The body must be exactly `expected_size_bytes` (the provider's `Content-Length`) long and not an
/// HTML page. The space is reserved before the first byte is read; any failure removes the file.
pub(crate) async fn spool_payload(
    spool: &SpoolDir,
    mut body: BoxStream<'static, Result<Bytes, CollectionError>>,
    content_type: &str,
    expected_size_bytes: u64,
    provider_file_id: &str,
) -> anyhow::Result<SpoolFile> {
    let reservation = spool.reserve(expected_size_bytes, provider_file_id)?;
    let temp = tempfile::Builder::new()
        .prefix(".provider-")
        .suffix(".part")
        .tempfile_in(spool.path())
        .with_context(|| {
            format!(
                "failed to create a spool file in {}",
                spool.path().display()
            )
        })?;
    let (std_file, path) = temp.into_parts();
    let mut file = tokio::fs::File::from_std(std_file);
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut first = true;
    while let Some(chunk) = body.next().await {
        let chunk =
            chunk.with_context(|| format!("failed to read provider file {provider_file_id}"))?;
        if chunk.is_empty() {
            continue;
        }
        if first {
            if crate::bulk_streaming_bronze::is_html_payload(content_type, &chunk) {
                bail!("provider file {provider_file_id} returned HTML instead of a provider file");
            }
            first = false;
        }
        size_bytes = size_bytes.saturating_add(chunk.len() as u64);
        if size_bytes > expected_size_bytes {
            bail!(
                "provider file {provider_file_id} is longer than its Content-Length {expected_size_bytes}"
            );
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to spool provider file {provider_file_id}"))?;
    }
    if size_bytes == 0 {
        bail!("provider file {provider_file_id} response body was empty");
    }
    if size_bytes != expected_size_bytes {
        bail!(
            "provider file {provider_file_id} sent {size_bytes} bytes but Content-Length declared {expected_size_bytes}"
        );
    }
    file.sync_all()
        .await
        .with_context(|| format!("failed to flush spooled provider file {provider_file_id}"))?;
    drop(file);
    Ok(SpoolFile {
        file: path,
        _reservation: reservation,
        checksum_sha256: crate::bulk_streaming_bronze::sha256_hex(&hasher.finalize()),
        size_bytes,
    })
}

#[cfg(test)]
mod tests;
