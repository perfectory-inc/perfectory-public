//! Media (image / document) format detection and content-addressed object keys.
//!
//! A source's photos and attachments are stored once by the hash of their bytes, under one store
//! shared by every parcel source (root ADR-0290 §7 for court auction, ADR-0296 §2 for public
//! auction). This module is the pure part — magic-byte format detection and the key scheme
//! `<prefix>/sha256/<hash>.<ext>` — with no I/O. The capture side differs per source (base64 decode
//! for court auction, file-URL download for public auction). The write is a content-addressed
//! write-once object (`put_object` CreateOnly + sha256 metadata), NOT a per-object Bronze ledger row
//! (ADR-0298); the silver photo table is the reference index. Both sources reuse the format and key
//! defined here.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::object_key::{ObjectKey, ObjectKeyError, ObjectKeyPrefix};

/// A media format recognized from the leading bytes of a payload.
///
/// Only the formats the providers actually serve for parcel sources are listed: KAMCO Onbid photos
/// and appraisal attachments, and court-auction photos. A payload whose leading bytes match none of
/// these is not stored as media.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaFormat {
    /// JPEG image (`FF D8 FF`).
    Jpeg,
    /// PNG image (`89 50 4E 47 0D 0A 1A 0A`).
    Png,
    /// GIF image (`GIF87a` / `GIF89a`).
    Gif,
    /// PDF document (`%PDF-`), e.g. an appraisal attachment.
    Pdf,
}

impl MediaFormat {
    /// The canonical lowercase file extension, used in the content-addressed key.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
            Self::Gif => "gif",
            Self::Pdf => "pdf",
        }
    }

    /// The media type to serve the object with.
    #[must_use]
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::Gif => "image/gif",
            Self::Pdf => "application/pdf",
        }
    }

    /// Detects the format from the payload's leading bytes.
    ///
    /// Returns `None` when the bytes match no known format — the provider's declared content type is
    /// not trusted (KAMCO serves expired links as HTML error pages with an image content type), so a
    /// payload that does not begin with a known signature is never stored as media.
    #[must_use]
    pub fn sniff(bytes: &[u8]) -> Option<Self> {
        if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
            Some(Self::Jpeg)
        } else if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
            Some(Self::Png)
        } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
            Some(Self::Gif)
        } else if bytes.starts_with(b"%PDF-") {
            Some(Self::Pdf)
        } else {
            None
        }
    }
}

/// A lowercase hex SHA-256 digest (exactly 64 characters, `0-9a-f`).
///
/// Content addressing is only deterministic if the digest is canonical, so the key builder takes
/// this validated value rather than a raw string: an uppercase, truncated, or path-like value cannot
/// reach the key scheme.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sha256Hex(String);

impl Sha256Hex {
    /// Parses a lowercase hex SHA-256 digest.
    ///
    /// # Errors
    /// Returns [`Sha256HexError`] when the value is not exactly 64 lowercase hex characters.
    pub fn parse(raw: &str) -> Result<Self, Sha256HexError> {
        if raw.len() != 64 {
            return Err(Sha256HexError::Length(raw.len()));
        }
        if !raw
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Sha256HexError::NotLowercaseHex);
        }
        Ok(Self(raw.to_owned()))
    }

    /// The digest as a lowercase hex string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validation errors returned while parsing a SHA-256 digest.
#[derive(Debug, Error)]
pub enum Sha256HexError {
    /// Digest was not 64 characters long.
    #[error("sha-256 hex digest must be 64 characters, got {0}")]
    Length(usize),
    /// Digest contained a character outside lowercase hex.
    #[error("sha-256 hex digest must be lowercase hex (0-9a-f)")]
    NotLowercaseHex,
}

/// Builds the content-addressed media object key `<prefix>/sha256/<hash>.<ext>` (ADR-0290 §7).
///
/// The same bytes always resolve to the same key, so a photo seen in many rounds is stored once.
///
/// # Errors
/// Returns [`ObjectKeyError`] when the composed key is not a valid object key.
pub fn content_addressed_key(
    prefix: &ObjectKeyPrefix,
    hash: &Sha256Hex,
    format: MediaFormat,
) -> Result<ObjectKey, ObjectKeyError> {
    let prefix = prefix.as_str().trim_end_matches('/');
    ObjectKey::parse(&format!(
        "{prefix}/sha256/{}.{}",
        hash.as_str(),
        format.extension()
    ))
}

#[cfg(test)]
mod tests {
    use super::{content_addressed_key, MediaFormat, Sha256Hex};
    use crate::object_key::ObjectKeyPrefix;

    #[test]
    fn sniffs_each_known_format_from_its_signature() {
        assert_eq!(
            MediaFormat::sniff(&[0xFF, 0xD8, 0xFF, 0xE0, 0x00]),
            Some(MediaFormat::Jpeg)
        );
        assert_eq!(
            MediaFormat::sniff(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00]),
            Some(MediaFormat::Png),
        );
        assert_eq!(MediaFormat::sniff(b"GIF89a....."), Some(MediaFormat::Gif));
        assert_eq!(MediaFormat::sniff(b"GIF87a....."), Some(MediaFormat::Gif));
        assert_eq!(MediaFormat::sniff(b"%PDF-1.4 ..."), Some(MediaFormat::Pdf));
    }

    #[test]
    fn rejects_unknown_or_empty_bytes_as_media() {
        assert_eq!(MediaFormat::sniff(b"<html>not an image</html>"), None);
        assert_eq!(MediaFormat::sniff(&[]), None);
        // A truthful content type would say image/jpeg, but the bytes are not a JPEG.
        assert_eq!(MediaFormat::sniff(&[0x00, 0xD8, 0xFF]), None);
    }

    #[test]
    fn maps_format_to_extension_and_content_type() {
        assert_eq!(MediaFormat::Jpeg.extension(), "jpg");
        assert_eq!(MediaFormat::Pdf.extension(), "pdf");
        assert_eq!(MediaFormat::Png.content_type(), "image/png");
        assert_eq!(MediaFormat::Pdf.content_type(), "application/pdf");
    }

    #[test]
    fn parses_only_canonical_lowercase_hex_digests() {
        let good = "a".repeat(64);
        assert!(Sha256Hex::parse(&good).is_ok());
        assert!(
            Sha256Hex::parse(&"A".repeat(64)).is_err(),
            "uppercase is not canonical"
        );
        assert!(Sha256Hex::parse(&"a".repeat(63)).is_err(), "too short");
        assert!(Sha256Hex::parse(&"a".repeat(65)).is_err(), "too long");
        assert!(Sha256Hex::parse(&"g".repeat(64)).is_err(), "non-hex");
    }

    #[test]
    fn builds_the_content_addressed_key_once_per_content() {
        let h = "0123456789abcdef".repeat(4); // 64 hex chars
        let key = ObjectKeyPrefix::parse("onbid/media")
            .ok()
            .zip(Sha256Hex::parse(&h).ok())
            .and_then(|(prefix, hash)| {
                content_addressed_key(&prefix, &hash, MediaFormat::Jpeg).ok()
            })
            .map(|k| k.as_str().to_owned());
        assert_eq!(key, Some(format!("onbid/media/sha256/{h}.jpg")));
    }

    #[test]
    fn a_trailing_slash_on_the_prefix_does_not_double() {
        let h = "0123456789abcdef".repeat(4);
        let key = ObjectKeyPrefix::parse("onbid/media/")
            .ok()
            .zip(Sha256Hex::parse(&h).ok())
            .and_then(|(prefix, hash)| content_addressed_key(&prefix, &hash, MediaFormat::Pdf).ok())
            .map(|k| k.as_str().to_owned());
        assert_eq!(key, Some(format!("onbid/media/sha256/{h}.pdf")));
    }
}
