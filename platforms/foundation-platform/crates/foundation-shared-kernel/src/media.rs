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
/// The photo formats ([`Self::sniff`]) are the ones the parcel sources serve: KAMCO Onbid photos and
/// appraisal attachments, and court-auction photos; a photo payload matching none of them is not
/// stored. The document formats ([`Self::sniff_document`], root ADR-0484) are what a notice may be
/// attached as: every attachment is stored, [`Self::Binary`] when no signature names it.
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
    /// WebP image (`RIFF....WEBP`): a provider's or one of our display renditions (root ADR-0427).
    Webp,
    /// AVIF image (an ISO-BMFF `ftyp` box of brand `avif`/`avis`): our display renditions.
    Avif,
    /// 한글(HWP) document: HWP 5 (an OLE compound file carrying the `HWP Document File` header) or
    /// HWP 3 (that header at the start). A notice attachment (root ADR-0484).
    Hwp,
    /// HWPX document: a ZIP whose first entry is `mimetype` = `application/hwp+zip` (KS X 6101).
    Hwpx,
    /// Any other ZIP archive (`PK\x03\x04`).
    Zip,
    /// Bytes no signature names. Only [`Self::sniff_document`] gives it: an attachment is kept
    /// whatever it is, and a later reader decides what it holds.
    Binary,
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
            Self::Webp => "webp",
            Self::Avif => "avif",
            Self::Hwp => "hwp",
            Self::Hwpx => "hwpx",
            Self::Zip => "zip",
            Self::Binary => "bin",
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
            Self::Webp => "image/webp",
            Self::Avif => "image/avif",
            Self::Hwp => "application/x-hwp",
            Self::Hwpx => "application/hwp+zip",
            Self::Zip => "application/zip",
            Self::Binary => "application/octet-stream",
        }
    }

    /// The format a canonical extension names (the inverse of [`Self::extension`]).
    #[must_use]
    pub fn from_extension(extension: &str) -> Option<Self> {
        [
            Self::Jpeg,
            Self::Png,
            Self::Gif,
            Self::Pdf,
            Self::Webp,
            Self::Avif,
            Self::Hwp,
            Self::Hwpx,
            Self::Zip,
            Self::Binary,
        ]
        .into_iter()
        .find(|format| format.extension() == extension)
    }

    /// Whether it is a picture a display ladder can be made from (root ADR-0427): not a document.
    #[must_use]
    pub const fn is_image(self) -> bool {
        matches!(
            self,
            Self::Jpeg | Self::Png | Self::Gif | Self::Webp | Self::Avif
        )
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
        } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
            Some(Self::Webp)
        } else if bytes.get(4..8) == Some(b"ftyp")
            && matches!(bytes.get(8..12), Some(b"avif" | b"avis"))
        {
            Some(Self::Avif)
        } else {
            None
        }
    }

    /// Detects a notice attachment's format (root ADR-0484); never fails, [`Self::Binary`] when no
    /// signature names it. [`Self::sniff`] is unchanged by it, so a photo source still stores only
    /// what it can show.
    #[must_use]
    pub fn sniff_document(bytes: &[u8]) -> Self {
        const CFB: &[u8] = &[0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        const HWP_HEADER: &[u8] = b"HWP Document File";
        if let Some(format) = Self::sniff(bytes) {
            format
        } else if bytes.starts_with(HWP_HEADER)
            || (bytes.starts_with(CFB) && contains(bytes, HWP_HEADER))
        {
            Self::Hwp
        } else if bytes.starts_with(b"PK\x03\x04") {
            // OCF packaging (as ODF and EPUB): a stored first entry `mimetype` holding the type.
            if bytes.get(30..38) == Some(b"mimetype")
                && bytes
                    .get(38..)
                    .is_some_and(|rest| rest.starts_with(b"application/hwp+zip"))
            {
                Self::Hwpx
            } else {
                Self::Zip
            }
        } else {
            Self::Binary
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
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

    /// The digest of `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        use sha2::{Digest as _, Sha256};
        use std::fmt::Write as _;
        Self(
            Sha256::digest(bytes)
                .iter()
                .fold(String::with_capacity(64), |mut hex, byte| {
                    let _ = write!(hex, "{byte:02x}");
                    hex
                }),
        )
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

/// The key of one display rendition of a stored source (root ADR-0427).
///
/// `<prefix>/renditions/<version>/<source sha256>/<width>.<ext>`: a rendition is named by its source
/// and the ladder version, not by its own bytes, so a page can name it from the source's hash alone.
///
/// # Errors
/// Returns [`ObjectKeyError`] when the composed key is not a valid object key.
pub fn rendition_key(
    prefix: &ObjectKeyPrefix,
    version: &str,
    source: &Sha256Hex,
    width: u32,
    format: MediaFormat,
) -> Result<ObjectKey, ObjectKeyError> {
    let prefix = prefix.as_str().trim_end_matches('/');
    ObjectKey::parse(&format!(
        "{prefix}/renditions/{version}/{}/{width}.{}",
        source.as_str(),
        format.extension()
    ))
}

/// The key of a source's ladder manifest, written after every rendition (root ADR-0427).
///
/// `<prefix>/renditions/<version>/<source sha256>.json`: its presence means the ladder is whole.
///
/// # Errors
/// Returns [`ObjectKeyError`] when the composed key is not a valid object key.
pub fn rendition_manifest_key(
    prefix: &ObjectKeyPrefix,
    version: &str,
    source: &Sha256Hex,
) -> Result<ObjectKey, ObjectKeyError> {
    let prefix = prefix.as_str().trim_end_matches('/');
    ObjectKey::parse(&format!(
        "{prefix}/renditions/{version}/{}.json",
        source.as_str()
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
        assert_eq!(
            MediaFormat::sniff(b"RIFF\x10\0\0\0WEBPVP8 "),
            Some(MediaFormat::Webp)
        );
        assert_eq!(
            MediaFormat::sniff(b"\0\0\0\x1cftypavif\0\0\0\0"),
            Some(MediaFormat::Avif)
        );
        assert_eq!(MediaFormat::sniff(b"RIFF\x10\0\0\0WAVEfmt "), None);
    }

    #[test]
    fn names_a_rendition_by_its_source_and_version() -> Result<(), Box<dyn std::error::Error>> {
        let prefix = ObjectKeyPrefix::parse("onbid/media")?;
        let source = Sha256Hex::of(b"source");
        assert_eq!(source, Sha256Hex::parse(source.as_str())?);
        assert_eq!(
            super::rendition_key(&prefix, "r1", &source, 400, MediaFormat::Avif)?.as_str(),
            format!("onbid/media/renditions/r1/{}/400.avif", source.as_str())
        );
        assert_eq!(
            super::rendition_manifest_key(&prefix, "r1", &source)?.as_str(),
            format!("onbid/media/renditions/r1/{}.json", source.as_str())
        );
        for format in [
            MediaFormat::Jpeg,
            MediaFormat::Webp,
            MediaFormat::Avif,
            MediaFormat::Pdf,
        ] {
            assert_eq!(
                MediaFormat::from_extension(format.extension()),
                Some(format)
            );
        }
        assert!(!MediaFormat::Pdf.is_image());
        Ok(())
    }

    #[test]
    fn sniffs_every_notice_attachment_and_keeps_photo_sniffing_as_it_was() {
        let mut hwp5 = vec![0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        hwp5.extend_from_slice(&[0; 600]);
        hwp5.extend_from_slice(b"HWP Document File\0\0");
        let mut hwpx = b"PK\x03\x04".to_vec();
        hwpx.extend_from_slice(&[0; 26]);
        hwpx.extend_from_slice(b"mimetypeapplication/hwp+zipPK");
        let mut xls = vec![0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        xls.extend_from_slice(b"Workbook");
        let cases: [(&[u8], MediaFormat); 7] = [
            (&hwp5, MediaFormat::Hwp),
            (b"HWP Document File V3.00 \x1a\x01\x02", MediaFormat::Hwp),
            (&hwpx, MediaFormat::Hwpx),
            (b"PK\x03\x04\x14\0\0\0word/document.xml", MediaFormat::Zip),
            (b"%PDF-1.7", MediaFormat::Pdf),
            (&xls, MediaFormat::Binary),
            (b"", MediaFormat::Binary),
        ];
        for (bytes, expected) in cases {
            assert_eq!(MediaFormat::sniff_document(bytes), expected);
            // A photo source never stores a document it cannot show (the screens' schemas list
            // jpg/png/gif/pdf only): the photo sniff does not see these formats.
            if !matches!(expected, MediaFormat::Pdf) {
                assert_eq!(MediaFormat::sniff(bytes), None);
            }
            assert_eq!(
                MediaFormat::from_extension(expected.extension()),
                Some(expected)
            );
            assert!(!expected.is_image());
        }
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
