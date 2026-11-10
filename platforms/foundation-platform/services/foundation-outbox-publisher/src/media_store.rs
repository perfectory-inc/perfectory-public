//! The one content-addressed media store every photo source writes to (root ADR-0298, ADR-0427 §7):
//! 공매 downloads (`onbid_media.rs`) and 경매 photos (root ADR-0384) both end in [`store_bytes`], so
//! the key, the format sniffing, the size cap, the dedup and the cache policy cannot differ between
//! sources. Each stored image then gets its display ladder from `media_renditions::ensure_ladder`.

use anyhow::Context as _;
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{content_addressed_key, MediaFormat, ObjectKeyPrefix, Sha256Hex};

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
}

/// What storing one file came to.
pub(crate) enum Stored {
    New(StoredMedia),
    Deduplicated(StoredMedia),
    Oversize,
    UnknownFormat,
    /// A fetching caller's answer, never [`store_bytes`]'s: the provider answered 4xx, the link is
    /// gone and asking again will not bring it back.
    Gone,
}

/// Stores media bytes already in hand once, under their content hash (`<prefix>/sha256/<hex>.<ext>`,
/// write-once): an over-cap file or an unknown format is not stored, an existing key is a dedup.
///
/// # Errors
/// The store refused the HEAD or the write (retried by a later run).
pub(crate) async fn store_bytes(
    bytes: &[u8],
    prefix: &ObjectKeyPrefix,
    storage: &dyn ObjectStorageService,
    max_file_bytes: u64,
) -> anyhow::Result<Stored> {
    let Some(format) = MediaFormat::sniff(bytes) else {
        return Ok(Stored::UnknownFormat);
    };
    store_bytes_as(bytes, format, prefix, storage, max_file_bytes).await
}

/// [`store_bytes`] for bytes whose format the caller already named: a notice attachment is stored
/// whatever it is (`MediaFormat::sniff_document`, root ADR-0484), a photo only when it is a picture.
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
    let hash = Sha256Hex::of(bytes);
    let hash_hex = hash.as_str().to_owned();
    let key =
        content_addressed_key(prefix, &hash, format).context("media object key is invalid")?;
    let media = StoredMedia {
        sha256: hash_hex.clone(),
        media_format: format.extension().to_owned(),
        size_bytes: bytes.len() as u64,
        longest_px: foundation_media_renditions::dimensions(bytes)
            .map(|(width, height)| width.max(height)),
    };
    // The key is the content hash: if it exists, the stored bytes are identical, so skip the write.
    if storage
        .read_object_sha256(key.as_str())
        .await
        .context("media head failed")?
        .is_some()
    {
        return Ok(Stored::Deduplicated(media));
    }
    match storage
        .put_object(PutObjectRequest {
            key: key.as_str().to_owned(),
            body: bytes.to_vec(),
            content_type: format.content_type().to_owned(),
            // Never changes once stored (the key is its content hash): the edge's one cache policy.
            cache_control: media_renditions::cache_control()?.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(hash_hex),
        })
        .await
    {
        Ok(()) => Ok(Stored::New(media)),
        // A concurrent run stored the same content first: identical bytes, so it is a dedup, not a fault.
        Err(PublishError::ObjectAlreadyExists { .. }) => Ok(Stored::Deduplicated(media)),
        Err(error) => Err(error).context("media put_object failed"),
    }
}
