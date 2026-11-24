//! The one content-addressed media store every photo source writes to (root ADR-0298, ADR-0427 §7):
//! 공매 downloads (`onbid_media.rs`) and 경매 photos (root ADR-0384) both end in [`store_bytes`], so
//! the key, the format sniffing, the size cap, the dedup and the cache policy cannot differ between
//! sources. Each stored image then gets its display ladder from `media_renditions::ensure_ladder`.
//!
//! A raster no screen reads (BMP, TIFF; root ADR-0503) is kept byte for byte under
//! `<prefix>/sources/sha256/`, and its lossless PNG display master is what lands in the served store:
//! the photo index, the ladder, the edge and every screen see only the master.

use anyhow::Context as _;
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{
    ByteStream, ObjectStorageStreamingService, ObjectWriteMode, PutObjectRequest,
    StreamingPutObjectRequest,
};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{
    content_addressed_key, source_key, MediaFormat, ObjectKey, ObjectKeyPrefix, Sha256Hex,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::media_renditions;

/// The content-addressed facts a stored (or already-present) media file yields for a Silver photo
/// index. Identical bytes always give the same three, so a re-seen or deduplicated file still maps.
pub(crate) struct StoredMedia {
    pub(crate) sha256: String,
    /// `MediaFormat::extension` of the stored file.
    pub(crate) media_format: String,
    pub(crate) size_bytes: u64,
    /// The longest side its header declares, when stored this run and readable.
    pub(crate) longest_px: Option<u32>,
    /// The file as received, when it is not one the screens read and this is its display master
    /// (root ADR-0503): kept under `<prefix>/sources/sha256/`, named in the Bronze line beside it.
    pub(crate) source: Option<SourceMedia>,
}

/// A received file kept as is beside its display master (root ADR-0503).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SourceMedia {
    pub(crate) sha256: String,
    /// `MediaFormat::extension` of the received file (`bmp`, `tif`).
    pub(crate) media_format: String,
    pub(crate) size_bytes: u64,
}

/// What storing one file came to.
pub(crate) enum Stored {
    New(StoredMedia),
    Deduplicated(StoredMedia),
    Oversize,
    /// The bytes are no format the store reads (an expired link's HTML page), or a raster no
    /// screen reads that does not decode into a display master (kept as received all the same).
    UnknownFormat,
    /// A fetching caller's answer, never [`store_bytes`]'s: the provider answered 4xx, the link is
    /// gone and asking again will not bring it back.
    Gone,
}

/// Stores media bytes already in hand once, under their content hash (`<prefix>/sha256/<hex>.<ext>`,
/// write-once): an over-cap file or an unknown format is not stored, an existing key is a dedup. A
/// raster no screen reads is kept as received under [`source_key`] and its display master is stored
/// in its place (root ADR-0503): the answer names the master, with the received file as `source`.
///
/// # Errors
/// The store refused the HEAD or the write (retried by a later run).
pub(crate) async fn store_bytes(
    bytes: &[u8],
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
    max_file_bytes: u64,
) -> anyhow::Result<Stored> {
    // A chunked response carries no length; the same cap still holds once the bytes are in hand.
    if bytes.len() as u64 > max_file_bytes {
        return Ok(Stored::Oversize);
    }
    let Some(format) = MediaFormat::sniff(bytes) else {
        return Ok(Stored::UnknownFormat);
    };
    if format.is_served() {
        return store_served(bytes, format, prefix, storage).await;
    }
    let hash = Sha256Hex::of(bytes);
    let kept = source_key(prefix, &hash, format).context("media source key is invalid")?;
    put_once(storage, &kept, bytes.to_vec(), format, &hash).await?;
    let source = SourceMedia {
        sha256: hash.as_str().to_owned(),
        media_format: format.extension().to_owned(),
        size_bytes: bytes.len() as u64,
    };
    let max_pixels = media_renditions::spec()?.max_source_pixels;
    let received = bytes.to_vec();
    // Decoding and PNG encoding are CPU work: off the async workers.
    let master = match tokio::task::spawn_blocking(move || {
        foundation_media_renditions::display_master(&received, max_pixels)
    })
    .await
    .context("the display master task panicked")?
    {
        Ok(master) => master,
        Err(error) => {
            tracing::warn!(error = %error, sha256 = source.sha256, "a kept source gives no display master");
            return Ok(Stored::UnknownFormat);
        }
    };
    Ok(
        match store_served(&master, MediaFormat::Png, prefix, storage).await? {
            Stored::New(media) => Stored::New(StoredMedia {
                source: Some(source),
                ..media
            }),
            Stored::Deduplicated(media) => Stored::Deduplicated(StoredMedia {
                source: Some(source),
                ..media
            }),
            other => other,
        },
    )
}

/// The two ports an attachment is stored through: the HEAD that finds its content key already there,
/// so a file seen before is not sent again, and the streaming put that sends a spooled file from disk.
#[derive(Clone, Copy)]
pub(crate) struct MediaStore<'a> {
    pub(crate) head: &'a dyn ObjectStorageService,
    pub(crate) stream: &'a dyn ObjectStorageStreamingService,
}

#[cfg(test)]
impl<'a> MediaStore<'a> {
    /// Both ports of one adapter that is both (the file store the tests write to).
    pub(crate) fn of<Storage>(storage: &'a Storage) -> Self
    where
        Storage: ObjectStorageService + ObjectStorageStreamingService,
    {
        Self {
            head: storage,
            stream: storage,
        }
    }
}

/// How much of a spooled file stays in memory for its format and image size: every signature but
/// HWP's is a prefix (the spool watches for that one across the whole file), and an image's header
/// lies in its first kilobytes.
const SPOOL_HEAD_BYTES: usize = 1 << 20;
/// How much of a spooled file's start is kept beside it, for a caller that reads what a file that is
/// not the file says (a CAPTCHA or error page).
const SPOOL_START_BYTES: usize = 4096;

/// A file written to disk as it arrives and hashed chunk by chunk (root ADR-0603, 2026-11-23
/// revision §3), so a file of any size passes through one chunk of memory: holding a notice
/// attachment whole, and a copy of it for the put, took Factory-on's first run to 977.6 MB under its
/// 1 GB cap (2026-11-22). Bytes past `max_bytes` are hashed and counted but not kept: the file is then
/// `oversize` and is not stored.
pub(crate) struct Spool {
    file: tokio::fs::File,
    path: tempfile::TempPath,
    hasher: Sha256,
    size_bytes: u64,
    max_bytes: u64,
    head: Vec<u8>,
    /// The last bytes seen, so the HWP signature split across two chunks is still found.
    tail: Vec<u8>,
    has_hwp_signature: bool,
}

/// A spooled file: its hash, length and format, and the file on disk, removed when this drops.
pub(crate) struct Spooled {
    path: tempfile::TempPath,
    pub(crate) sha256: Sha256Hex,
    pub(crate) size_bytes: u64,
    pub(crate) format: MediaFormat,
    /// Whether [`MediaFormat::sniff`] names the format: a photo source stores only such a file.
    pub(crate) shown: bool,
    /// The file's first bytes (at most [`SPOOL_START_BYTES`]).
    // Read by the court auction collector, which is built on unix only.
    #[cfg_attr(not(any(unix, test)), allow(dead_code))]
    pub(crate) start: Vec<u8>,
    pub(crate) oversize: bool,
    longest_px: Option<u32>,
}

impl Spool {
    /// A spool under the process's temporary directory (the unit's private `/tmp`).
    ///
    /// # Errors
    /// The temporary file could not be made.
    pub(crate) fn create(max_bytes: u64) -> anyhow::Result<Self> {
        let (file, path) = tempfile::NamedTempFile::new()
            .context("failed to create a spool file")?
            .into_parts();
        Ok(Self {
            file: tokio::fs::File::from_std(file),
            path,
            hasher: Sha256::new(),
            size_bytes: 0,
            max_bytes,
            head: Vec::new(),
            tail: Vec::new(),
            has_hwp_signature: false,
        })
    }

    /// Takes the next chunk of the file.
    ///
    /// # Errors
    /// The spool file could not be written.
    pub(crate) async fn push(&mut self, chunk: &[u8]) -> anyhow::Result<()> {
        self.hasher.update(chunk);
        let room = usize::try_from(self.max_bytes.saturating_sub(self.size_bytes))
            .unwrap_or(usize::MAX)
            .min(chunk.len());
        self.file
            .write_all(&chunk[..room])
            .await
            .context("failed to write the spool file")?;
        self.size_bytes += chunk.len() as u64;
        let head_room = SPOOL_HEAD_BYTES
            .saturating_sub(self.head.len())
            .min(chunk.len());
        self.head.extend_from_slice(&chunk[..head_room]);
        if !self.has_hwp_signature {
            self.watch(chunk);
        }
        Ok(())
    }

    /// Whether [`MediaFormat::HWP_SIGNATURE`] went by, inside this chunk or across its edge.
    fn watch(&mut self, chunk: &[u8]) {
        let signature = MediaFormat::HWP_SIGNATURE;
        let edge = signature.len() - 1;
        let mut window = std::mem::take(&mut self.tail);
        window.extend_from_slice(&chunk[..chunk.len().min(edge)]);
        self.has_hwp_signature = contains(&window, signature) || contains(chunk, signature);
        self.tail = if chunk.len() >= edge {
            chunk[chunk.len() - edge..].to_vec()
        } else {
            window[window.len().saturating_sub(edge)..].to_vec()
        };
    }

    /// The file as spooled.
    ///
    /// # Errors
    /// The spool file could not be flushed.
    pub(crate) async fn finish(mut self) -> anyhow::Result<Spooled> {
        self.file
            .flush()
            .await
            .context("failed to flush the spool file")?;
        let format = MediaFormat::sniff_document_head(&self.head, self.has_hwp_signature);
        let longest_px = if format.is_image() {
            foundation_media_renditions::dimensions(&self.head)
                .map(|(width, height)| width.max(height))
        } else {
            None
        };
        let sha256 = Sha256Hex::parse(&format!("{:x}", self.hasher.finalize()))
            .context("the spool's digest is not a sha256")?;
        Ok(Spooled {
            path: self.path,
            sha256,
            size_bytes: self.size_bytes,
            format,
            shown: MediaFormat::sniff(&self.head).is_some(),
            start: self.head[..self.head.len().min(SPOOL_START_BYTES)].to_vec(),
            oversize: self.size_bytes > self.max_bytes,
            longest_px,
        })
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Spools a response's body as it arrives ([`Spool`]).
///
/// # Errors
/// The body broke off, or the spool file could not be written.
pub(crate) async fn spool_response(
    mut response: reqwest::Response,
    max_bytes: u64,
) -> anyhow::Result<Spooled> {
    let mut spool = Spool::create(max_bytes)?;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| anyhow::anyhow!("the file body failed: {}", error.without_url()))?
    {
        spool.push(&chunk).await?;
    }
    spool.finish().await
}

/// Stores a spooled file as received under `<prefix>/sha256/<hex>.<ext>`, whatever its format, sent
/// from disk (in parts when it is large): an over-cap file is not stored, and a key already there is a
/// dedup with nothing sent.
///
/// # Errors
/// The store refused the HEAD or the write (retried by a later run).
pub(crate) async fn store_spooled(
    spooled: &Spooled,
    prefix: &ObjectKeyPrefix,
    store: MediaStore<'_>,
) -> anyhow::Result<Stored> {
    if spooled.oversize {
        return Ok(Stored::Oversize);
    }
    let key = content_addressed_key(prefix, &spooled.sha256, spooled.format)
        .context("media object key is invalid")?;
    let media = StoredMedia {
        sha256: spooled.sha256.as_str().to_owned(),
        media_format: spooled.format.extension().to_owned(),
        size_bytes: spooled.size_bytes,
        longest_px: spooled.longest_px,
        source: None,
    };
    if store
        .head
        .read_object_sha256(key.as_str())
        .await
        .context("media head failed")?
        .is_some()
    {
        return Ok(Stored::Deduplicated(media));
    }
    let body = ByteStream::read_from()
        .path(&spooled.path)
        .build()
        .await
        .context("failed to open the spool file")?;
    match store
        .stream
        .put_streaming_object(StreamingPutObjectRequest {
            key: key.as_str().to_owned(),
            content_type: spooled.format.content_type().to_owned(),
            // Never changes once stored (the key is its content hash): the edge's one cache policy.
            cache_control: media_renditions::cache_control()?.to_owned(),
            size_bytes: spooled.size_bytes,
            body,
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(spooled.sha256.as_str().to_owned()),
        })
        .await
    {
        Ok(()) => Ok(Stored::New(media)),
        // A concurrent run stored the same content first: identical bytes, so it is a dedup.
        Err(PublishError::ObjectAlreadyExists { .. }) => Ok(Stored::Deduplicated(media)),
        Err(error) => Err(error).context("media streaming put failed"),
    }
}

/// Stores a spooled photo-source file as [`store_bytes`] stores one in hand, without holding it:
/// an over-cap file or a format [`MediaFormat::sniff`] does not name is not stored, a format every
/// screen reads is sent from disk ([`store_spooled`]). Only a raster no screen reads (BMP, TIFF) is
/// read back, since its display master is made from its pixels (root ADR-0503).
///
/// Every source that fetches media over HTTP stores through here ([`spool_response`] first):
/// holding each response whole, and a copy of it for the put, is what a photo source's memory was
/// bounded by (Onbid: up to `max_file_bytes` 134 MB, six at a time).
///
/// # Errors
/// The store refused the HEAD or the write (retried by a later run), or the spool could not be read.
pub(crate) async fn store_spooled_shown(
    spooled: &Spooled,
    prefix: &ObjectKeyPrefix,
    store: MediaStore<'_>,
) -> anyhow::Result<Stored> {
    if spooled.oversize {
        return Ok(Stored::Oversize);
    }
    if !spooled.shown {
        return Ok(Stored::UnknownFormat);
    }
    if spooled.format.is_served() {
        return store_spooled(spooled, prefix, store).await;
    }
    let bytes = tokio::fs::read(&spooled.path)
        .await
        .context("failed to read the spool file")?;
    store_bytes(&bytes, prefix, store.head, u64::MAX).await
}

/// Stores a file as received under `<prefix>/sha256/<hex>.<ext>`.
async fn store_served(
    bytes: &[u8],
    format: MediaFormat,
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
) -> anyhow::Result<Stored> {
    let hash = Sha256Hex::of(bytes);
    let key =
        content_addressed_key(prefix, &hash, format).context("media object key is invalid")?;
    let media = StoredMedia {
        sha256: hash.as_str().to_owned(),
        media_format: format.extension().to_owned(),
        size_bytes: bytes.len() as u64,
        longest_px: foundation_media_renditions::dimensions(bytes)
            .map(|(width, height)| width.max(height)),
        source: None,
    };
    Ok(
        if put_once(storage, &key, bytes.to_vec(), format, &hash).await? {
            Stored::New(media)
        } else {
            Stored::Deduplicated(media)
        },
    )
}

/// Writes `body` once at its content key; `false` when the key was already there (the same bytes).
async fn put_once(
    storage: &dyn ObjectStorageService,
    key: &ObjectKey,
    body: Vec<u8>,
    format: MediaFormat,
    hash: &Sha256Hex,
) -> anyhow::Result<bool> {
    // The key is the content hash: if it exists, the stored bytes are identical, so skip the write.
    if storage
        .read_object_sha256(key.as_str())
        .await
        .context("media head failed")?
        .is_some()
    {
        return Ok(false);
    }
    match storage
        .put_object(PutObjectRequest {
            key: key.as_str().to_owned(),
            body,
            content_type: format.content_type().to_owned(),
            // Never changes once stored (the key is its content hash): the edge's one cache policy.
            cache_control: media_renditions::cache_control()?.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(hash.as_str().to_owned()),
        })
        .await
    {
        Ok(()) => Ok(true),
        // A concurrent run stored the same content first: identical bytes, so it is a dedup, not a fault.
        Err(PublishError::ObjectAlreadyExists { .. }) => Ok(false),
        Err(error) => Err(error).context("media put_object failed"),
    }
}

#[cfg(test)]
mod tests {
    use foundation_outbox::object_storage::FileObjectStorage;
    use foundation_shared_kernel::{
        content_addressed_key, MediaFormat, ObjectKeyPrefix, Sha256Hex,
    };

    use super::{store_spooled, store_spooled_shown, MediaStore, Spool, Stored};
    use foundation_outbox::ObjectStorageService;

    fn pattern(length: usize) -> Vec<u8> {
        (0..length)
            .map(|at| u8::try_from(at % 251).unwrap_or(0))
            .collect()
    }

    #[tokio::test]
    async fn a_file_is_hashed_as_it_arrives_sent_from_disk_once_and_found_the_next_time(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let storage = FileObjectStorage::new(root.path())?;
        let store = MediaStore {
            head: &storage,
            stream: &storage,
        };
        let prefix = ObjectKeyPrefix::parse("media")?;
        let mut body = b"%PDF-1.7\n".to_vec();
        body.extend(pattern(3 * 1024 * 1024));
        let mut spool = Spool::create(u64::MAX)?;
        for chunk in body.chunks(7_777) {
            spool.push(chunk).await?;
        }
        let spooled = spool.finish().await?;
        assert_eq!(spooled.sha256, Sha256Hex::of(&body));
        assert_eq!(spooled.size_bytes, body.len() as u64);
        assert_eq!(spooled.format, MediaFormat::Pdf);
        assert!(matches!(
            store_spooled(&spooled, &prefix, store).await?,
            Stored::New(_)
        ));
        let key = content_addressed_key(&prefix, &spooled.sha256, MediaFormat::Pdf)?;
        assert_eq!(storage.read_object_bytes(key.as_str()).await?, body);
        assert!(matches!(
            store_spooled(&spooled, &prefix, store).await?,
            Stored::Deduplicated(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn past_the_cap_a_file_is_hashed_and_counted_not_kept_or_stored() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let storage = FileObjectStorage::new(root.path())?;
        let store = MediaStore {
            head: &storage,
            stream: &storage,
        };
        let body = pattern(10_000);
        let mut spool = Spool::create(1_000)?;
        for chunk in body.chunks(999) {
            spool.push(chunk).await?;
        }
        let spooled = spool.finish().await?;
        assert!(spooled.oversize);
        assert_eq!(spooled.sha256, Sha256Hex::of(&body));
        assert_eq!(spooled.size_bytes, 10_000);
        assert_eq!(std::fs::metadata(&*spooled.path)?.len(), 1_000);
        let prefix = ObjectKeyPrefix::parse("media")?;
        assert!(matches!(
            store_spooled(&spooled, &prefix, store).await?,
            Stored::Oversize
        ));
        Ok(())
    }

    #[tokio::test]
    async fn an_hwp_container_is_named_by_a_signature_past_its_head_even_split_across_chunks(
    ) -> anyhow::Result<()> {
        let mut body = vec![0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        body.extend(vec![0; 2 * 1024 * 1024]);
        let at = body.len();
        body.extend_from_slice(MediaFormat::HWP_SIGNATURE);
        body.extend(vec![0; 100]);
        let mut spool = Spool::create(u64::MAX)?;
        // The signature arrives in three pieces, the middle one shorter than it.
        spool.push(&body[..at + 3]).await?;
        spool.push(&body[at + 3..at + 9]).await?;
        spool.push(&body[at + 9..]).await?;
        let spooled = spool.finish().await?;
        assert_eq!(spooled.format, MediaFormat::Hwp);
        assert_eq!(MediaFormat::sniff_document(&body), MediaFormat::Hwp);
        Ok(())
    }

    #[tokio::test]
    async fn a_photo_source_stores_from_disk_only_what_a_screen_can_show() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let storage = FileObjectStorage::new(root.path())?;
        let store = MediaStore::of(&storage);
        let prefix = ObjectKeyPrefix::parse("media")?;
        // A page in place of the file (an expired link, a CAPTCHA): not stored, and its start is
        // kept for the caller that reads what it says.
        let mut page = b"<html>synthetic notice</html>".to_vec();
        page.extend(pattern(10_000));
        let mut spool = Spool::create(u64::MAX)?;
        spool.push(&page).await?;
        let spooled = spool.finish().await?;
        assert!(!spooled.shown);
        assert_eq!(spooled.start, page[..4096]);
        assert!(matches!(
            store_spooled_shown(&spooled, &prefix, store).await?,
            Stored::UnknownFormat
        ));
        // A format a screen reads is sent from disk under its content key, once.
        let mut body = b"%PDF-1.7\n".to_vec();
        body.extend(pattern(2 * 1024 * 1024));
        let mut spool = Spool::create(u64::MAX)?;
        for chunk in body.chunks(50_000) {
            spool.push(chunk).await?;
        }
        let spooled = spool.finish().await?;
        assert!(spooled.shown);
        assert!(matches!(
            store_spooled_shown(&spooled, &prefix, store).await?,
            Stored::New(_)
        ));
        let key = content_addressed_key(&prefix, &spooled.sha256, MediaFormat::Pdf)?;
        assert_eq!(storage.read_object_bytes(key.as_str()).await?, body);
        assert!(matches!(
            store_spooled_shown(&spooled, &prefix, store).await?,
            Stored::Deduplicated(_)
        ));
        // Past the cap nothing is stored, whatever the format.
        let mut spool = Spool::create(100)?;
        spool.push(&body[..1_000]).await?;
        assert!(matches!(
            store_spooled_shown(&spool.finish().await?, &prefix, store).await?,
            Stored::Oversize
        ));
        Ok(())
    }
}
