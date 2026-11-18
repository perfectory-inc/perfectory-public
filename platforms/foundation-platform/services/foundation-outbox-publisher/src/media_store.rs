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
use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{
    content_addressed_key, source_key, MediaFormat, ObjectKey, ObjectKeyPrefix, Sha256Hex,
};

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

/// [`store_bytes`] for bytes whose format the caller already named, stored as received whatever it
/// is: a notice attachment (`MediaFormat::sniff_document`, root ADR-0484). Photos go through
/// [`store_bytes`], which keeps a raster no screen reads out of the served store (root ADR-0503).
///
/// # Errors
/// The store refused the HEAD or the write (retried by a later run).
pub(crate) async fn store_bytes_as(
    bytes: &[u8],
    format: MediaFormat,
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
    max_file_bytes: u64,
) -> anyhow::Result<Stored> {
    // A chunked response carries no length; the same cap still holds once the bytes are in hand.
    if bytes.len() as u64 > max_file_bytes {
        return Ok(Stored::Oversize);
    }
    store_served(bytes, format, prefix, storage).await
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
