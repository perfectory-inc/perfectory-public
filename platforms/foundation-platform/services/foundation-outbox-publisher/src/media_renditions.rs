//! A stored photo's rendition ladder, written beside it in the shared content-addressed media store
//! (root ADR-0426, ADR-0297 §2). 공매 (`onbid_media.rs`) and 경매 photos both end here.
//!
//! The ladder is `config/r2-connections.contract.json` `parcel_media.renditions` and is rendered by
//! the `foundation-media-renditions` crate from the original's stored bytes. Each rendition lands at
//! `<prefix>/renditions/<version>/<source sha256>/<width>.<ext>` (write-once, immutable), then the
//! manifest `<prefix>/renditions/<version>/<source sha256>.json` last: a manifest means the whole
//! ladder is there, so a source with one is never rendered again (partial: only new originals, or
//! every source once after a version change).

use std::sync::OnceLock;

use anyhow::Context as _;
use foundation_media_renditions::{render, Spec};
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{ObjectWriteMode, PutObjectRequest};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::{
    content_addressed_key, rendition_key, rendition_manifest_key, MediaFormat, ObjectKeyPrefix,
    Sha256Hex,
};
use serde::Deserialize;
use serde_json::json;

const CONNECTIONS: &str = include_str!("../../../config/r2-connections.contract.json");

/// `parcel_media` of the connection contract: how the edge serves the media store, and its ladder.
#[derive(Debug, Deserialize)]
struct ParcelMedia {
    cache_control: String,
    renditions: Spec,
}

fn parcel_media() -> anyhow::Result<&'static ParcelMedia> {
    static POLICY: OnceLock<Result<ParcelMedia, String>> = OnceLock::new();
    POLICY
        .get_or_init(|| {
            serde_json::from_str::<serde_json::Value>(CONNECTIONS)
                .and_then(|contract| serde_json::from_value(contract["parcel_media"].clone()))
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("parcel_media does not parse: {error}"))
}

/// The contract's ladder, parsed once.
pub(crate) fn spec() -> anyhow::Result<&'static Spec> {
    parcel_media().map(|policy| &policy.renditions)
}

/// The one `Cache-Control` of every media object (originals and renditions): the edge's
/// `parcel_media.cache_control`, so what is stored and what is served cannot differ.
pub(crate) fn cache_control() -> anyhow::Result<&'static str> {
    parcel_media().map(|policy| policy.cache_control.as_str())
}

/// Whether a stored file of this extension gets a ladder.
pub(crate) fn is_source(media_format: &str) -> bool {
    MediaFormat::from_extension(media_format).is_some_and(MediaFormat::is_image)
}

/// What one source came to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Laddered {
    /// Its manifest was already there.
    Present,
    /// Rendered and stored now: the bytes written.
    Written(u64),
    /// The stored bytes do not decode, or are over the pixel cap: rendering again will not help,
    /// so the screen shows the source itself.
    Unrenderable,
}

/// Makes sure the ladder of the stored source `<prefix>/sha256/<source_sha256>.<media_format>`
/// exists at the contract's version.
///
/// # Errors
/// The store refused a read or a write (retried by a later run).
pub(crate) async fn ensure_ladder(
    prefix: &str,
    source_sha256: &str,
    media_format: &str,
    spec: &'static Spec,
    storage: &dyn ObjectStorageService,
) -> anyhow::Result<Laddered> {
    let prefix = ObjectKeyPrefix::parse(prefix).context("media prefix is invalid")?;
    let source_hash = Sha256Hex::parse(source_sha256).context("source sha-256 is not canonical")?;
    let source_format = MediaFormat::from_extension(media_format)
        .with_context(|| format!("{media_format} is not a media format"))?;
    let manifest = rendition_manifest_key(&prefix, &spec.version, &source_hash)?;
    if storage
        .read_object_sha256(manifest.as_str())
        .await
        .context("rendition manifest head failed")?
        .is_some()
    {
        return Ok(Laddered::Present);
    }
    let source = storage
        .read_object_bytes(content_addressed_key(&prefix, &source_hash, source_format)?.as_str())
        .await
        .context("reading the stored original failed")?;
    // Decoding and AV1 encoding are CPU work: off the async workers.
    let rendered = match tokio::task::spawn_blocking(move || render(&source, spec))
        .await
        .context("the rendition task panicked")?
    {
        Ok(rendered) => rendered,
        Err(error) => {
            tracing::warn!(error = %error, source_sha256, "a stored photo gives no rendition ladder");
            return Ok(Laddered::Unrenderable);
        }
    };
    let mut written: u64 = 0;
    let mut entries = Vec::new();
    for rendition in rendered.renditions {
        let key = rendition_key(
            &prefix,
            &spec.version,
            &source_hash,
            rendition.rung,
            rendition.format,
        )?;
        let sha256 = Sha256Hex::of(&rendition.bytes);
        let size = rendition.bytes.len() as u64;
        entries.push(json!({
            "width": rendition.rung,
            "format": rendition.format.extension(),
            "pixel_width": rendition.width,
            "pixel_height": rendition.height,
            "sha256": sha256.as_str(),
            "size_bytes": size,
        }));
        put_once(
            storage,
            key.as_str(),
            rendition.bytes,
            rendition.format.content_type(),
            sha256,
        )
        .await?;
        written += size;
    }
    let body = serde_json::to_vec(&json!({
        "version": spec.version,
        "source": {
            "sha256": source_sha256,
            "media_format": media_format,
            "pixel_width": rendered.source_width,
            "pixel_height": rendered.source_height,
        },
        "renditions": entries,
    }))
    .context("the rendition manifest does not serialize")?;
    let sha256 = Sha256Hex::of(&body);
    put_once(storage, manifest.as_str(), body, "application/json", sha256).await?;
    Ok(Laddered::Written(written))
}

/// Write-once: an existing key is the same rendition of the same source and version (a concurrent
/// or interrupted earlier run), so it is kept.
async fn put_once(
    storage: &dyn ObjectStorageService,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
    sha256: Sha256Hex,
) -> anyhow::Result<()> {
    match storage
        .put_object(PutObjectRequest {
            key: key.to_owned(),
            body,
            content_type: content_type.to_owned(),
            cache_control: cache_control()?.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(sha256.as_str().to_owned()),
        })
        .await
    {
        Ok(()) | Err(PublishError::ObjectAlreadyExists { .. }) => Ok(()),
        Err(error) => Err(error).context("rendition put_object failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundation_outbox::object_storage::FileObjectStorage;

    fn jpeg(width: u32, height: u32) -> Vec<u8> {
        let mut out = Vec::new();
        let pixels = vec![120_u8; usize::try_from(width * height * 3).unwrap_or_default()];
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 90)
            .encode(&pixels, width, height, image::ExtendedColorType::Rgb8)
            .unwrap_or_default();
        out
    }

    #[test]
    fn the_contract_ladder_and_keys_match_the_edge_path() -> anyhow::Result<()> {
        let spec = spec()?;
        let contract: serde_json::Value = serde_json::from_str(CONNECTIONS)?;
        let pattern = regex::Regex::new(&format!(
            "^(?:{})$",
            contract["parcel_media"]["path_pattern"]
                .as_str()
                .unwrap_or_default()
        ))?;
        let prefix = ObjectKeyPrefix::parse("onbid/media")?;
        let sha = Sha256Hex::of(b"source");
        for width in &spec.widths {
            for format in &spec.formats {
                let key =
                    rendition_key(&prefix, &spec.version, &sha, *width, format.media_format())?;
                let key = key.as_str();
                assert!(
                    pattern.is_match(key),
                    "{key} is not served by the parcel edge"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_ladder_is_written_once_then_left_alone() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let storage = FileObjectStorage::new(root.path())?;
        let source = jpeg(400, 300);
        let sha = Sha256Hex::of(&source).as_str().to_owned();
        storage
            .put_object(PutObjectRequest {
                key: format!("onbid/media/sha256/{sha}.jpg"),
                body: source,
                content_type: "image/jpeg".to_owned(),
                cache_control: cache_control()?.to_owned(),
                write_mode: ObjectWriteMode::CreateOnly,
                sha256: None,
            })
            .await?;
        let spec = spec()?;
        let first = ensure_ladder("onbid/media", &sha, "jpg", spec, &storage).await?;
        assert!(matches!(first, Laddered::Written(bytes) if bytes > 0));
        let manifest: serde_json::Value = serde_json::from_slice(
            &storage.get_object_bytes(
                rendition_manifest_key(
                    &ObjectKeyPrefix::parse("onbid/media")?,
                    &spec.version,
                    &Sha256Hex::parse(&sha)?,
                )?
                .as_str(),
            )?,
        )?;
        let entries = manifest["renditions"].as_array().map_or(0, Vec::len);
        assert_eq!(entries, spec.widths.len() * spec.formats.len());
        // A 400px source is never enlarged: every rung at or above it is 400 wide.
        assert!(manifest["renditions"]
            .as_array()
            .into_iter()
            .flatten()
            .all(|entry| entry["pixel_width"].as_u64() <= Some(400)));
        assert_eq!(
            ensure_ladder("onbid/media", &sha, "jpg", spec, &storage).await?,
            Laddered::Present
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_stored_file_that_does_not_decode_is_unrenderable_not_an_error() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let storage = FileObjectStorage::new(root.path())?;
        let sha = "b".repeat(64);
        storage
            .put_object(PutObjectRequest {
                key: format!("onbid/media/sha256/{sha}.jpg"),
                body: vec![0xFF, 0xD8, 0xFF, 0x00, 0x01],
                content_type: "image/jpeg".to_owned(),
                cache_control: cache_control()?.to_owned(),
                write_mode: ObjectWriteMode::CreateOnly,
                sha256: None,
            })
            .await?;
        assert_eq!(
            ensure_ladder("onbid/media", &sha, "jpg", spec()?, &storage).await?,
            Laddered::Unrenderable
        );
        Ok(())
    }
}
