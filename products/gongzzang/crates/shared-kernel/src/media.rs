//! 사진 형식과 내용주소 키 (root ADR-0427, ADR-0298).
//!
//! 공짱의 모든 사진은 바이트의 sha256 으로 한 번 저장되고(`<prefix>/sha256/<hex>.<ext>`), 그 옆에
//! 원본에서 만든 표시 사다리(`<prefix>/renditions/<version>/<hex>/<width>.<ext>`, 다 쓰면 마지막에
//! `<prefix>/renditions/<version>/<hex>.json`)가 있다. 공매·경매와 같은 키 규칙·형식 판정이다.
//!
//! 영역 간 Cargo path 의존은 금지(AGENTS.md)라 foundation `foundation-shared-kernel` `media.rs` 와
//! `config/r2-connections.contract.json` `parcel_media.renditions` 의 사본이고, 시험
//! (`tests/media_mirrors_foundation.rs`)이 그 둘과 같은지 본다.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 매물 사진이 쓰는 내용주소 저장소의 키 접두사 (공짱 R2 버킷 안).
pub const LISTING_PHOTO_MEDIA_PREFIX: &str = "media/listing-photo";

/// 표시 사다리: version·긴 변·형식 (foundation `parcel_media.renditions` 의 사본).
pub const RENDITION_VERSION: &str = "r1";
/// 사다리 단계의 긴 변(px).
pub const RENDITION_WIDTHS: [u32; 3] = [400, 960, 1920];
/// 사다리 형식, 순서대로.
pub const RENDITION_FORMATS: [MediaFormat; 2] = [MediaFormat::Avif, MediaFormat::Webp];

/// 앞 바이트로 판정한 미디어 형식.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaFormat {
    /// JPEG (`FF D8 FF`).
    Jpeg,
    /// PNG (`89 50 4E 47 0D 0A 1A 0A`).
    Png,
    /// GIF (`GIF87a` / `GIF89a`).
    Gif,
    /// PDF (`%PDF-`).
    Pdf,
    /// WebP (`RIFF....WEBP`).
    Webp,
    /// AVIF (`ftyp` 상자, brand `avif`/`avis`).
    Avif,
}

impl MediaFormat {
    /// 모든 형식.
    pub const ALL: [Self; 6] = [
        Self::Jpeg,
        Self::Png,
        Self::Gif,
        Self::Pdf,
        Self::Webp,
        Self::Avif,
    ];

    /// 내용주소 키에 쓰는 소문자 확장자.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
            Self::Gif => "gif",
            Self::Pdf => "pdf",
            Self::Webp => "webp",
            Self::Avif => "avif",
        }
    }

    /// 내보내는 media type (`DB` `content_type` 열의 값).
    #[must_use]
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Png => "image/png",
            Self::Gif => "image/gif",
            Self::Pdf => "application/pdf",
            Self::Webp => "image/webp",
            Self::Avif => "image/avif",
        }
    }

    /// 확장자가 이름 대는 형식.
    #[must_use]
    pub fn from_extension(extension: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|format| format.extension() == extension)
    }

    /// media type 이 이름 대는 형식.
    #[must_use]
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|format| format.content_type() == content_type)
    }

    /// 표시 사다리를 만들 수 있는 그림인가 (PDF 가 아님).
    #[must_use]
    pub const fn is_image(self) -> bool {
        !matches!(self, Self::Pdf)
    }

    /// 앞 바이트로 형식을 판정한다. 아는 서명이 없으면 `None`.
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
}

impl fmt::Display for MediaFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.content_type())
    }
}

/// 소문자 hex sha256 (정확히 64자, `0-9a-f`). 키를 결정적으로 만드는 정규형.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Sha256Hex(String);

/// [`Sha256Hex`] 파싱 에러.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Sha256HexError {
    /// 64자가 아님.
    #[error("sha-256 hex digest must be 64 characters, got {0}")]
    Length(usize),
    /// 소문자 hex 가 아닌 글자.
    #[error("sha-256 hex digest must be lowercase hex (0-9a-f)")]
    NotLowercaseHex,
}

impl Sha256Hex {
    /// 소문자 hex sha256 을 파싱한다.
    ///
    /// # Errors
    /// 64자 소문자 hex 가 아니면 [`Sha256HexError`].
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

    /// 소문자 hex 문자열.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Sha256Hex {
    type Error = Sha256HexError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<Sha256Hex> for String {
    fn from(value: Sha256Hex) -> Self {
        value.0
    }
}

/// 내용주소 키 `<prefix>/sha256/<hex>.<ext>`.
#[must_use]
pub fn content_addressed_key(prefix: &str, hash: &Sha256Hex, format: MediaFormat) -> String {
    format!(
        "{}/sha256/{}.{}",
        prefix.trim_end_matches('/'),
        hash.as_str(),
        format.extension()
    )
}

/// 사다리 한 단계의 키 `<prefix>/renditions/<version>/<source hex>/<width>.<ext>`.
#[must_use]
pub fn rendition_key(
    prefix: &str,
    version: &str,
    source: &Sha256Hex,
    width: u32,
    format: MediaFormat,
) -> String {
    format!(
        "{}/renditions/{version}/{}/{width}.{}",
        prefix.trim_end_matches('/'),
        source.as_str(),
        format.extension()
    )
}

/// 사다리 manifest 키 `<prefix>/renditions/<version>/<source hex>.json` — 있으면 사다리가 다 있다.
#[must_use]
pub fn rendition_manifest_key(prefix: &str, version: &str, source: &Sha256Hex) -> String {
    format!(
        "{}/renditions/{version}/{}.json",
        prefix.trim_end_matches('/'),
        source.as_str()
    )
}

#[cfg(test)]
mod tests {
    use super::{
        content_addressed_key, rendition_key, rendition_manifest_key, MediaFormat, Sha256Hex,
        LISTING_PHOTO_MEDIA_PREFIX,
    };

    #[test]
    fn sniffs_and_round_trips_every_format() {
        assert_eq!(
            MediaFormat::sniff(&[0xFF, 0xD8, 0xFF, 0xE0]),
            Some(MediaFormat::Jpeg)
        );
        assert_eq!(
            MediaFormat::sniff(b"RIFF\x10\0\0\0WEBPVP8 "),
            Some(MediaFormat::Webp)
        );
        assert_eq!(MediaFormat::sniff(b"<html>"), None);
        for format in MediaFormat::ALL {
            assert_eq!(
                MediaFormat::from_extension(format.extension()),
                Some(format)
            );
            assert_eq!(
                MediaFormat::from_content_type(format.content_type()),
                Some(format)
            );
        }
        assert!(!MediaFormat::Pdf.is_image());
    }

    #[test]
    fn names_the_original_and_its_ladder_by_the_hash() -> Result<(), super::Sha256HexError> {
        let hash = Sha256Hex::parse(&"0123456789abcdef".repeat(4))?;
        let h = hash.as_str().to_owned();
        assert_eq!(
            content_addressed_key(LISTING_PHOTO_MEDIA_PREFIX, &hash, MediaFormat::Jpeg),
            format!("media/listing-photo/sha256/{h}.jpg")
        );
        assert_eq!(
            rendition_key("media/listing-photo/", "r1", &hash, 400, MediaFormat::Avif),
            format!("media/listing-photo/renditions/r1/{h}/400.avif")
        );
        assert_eq!(
            rendition_manifest_key(LISTING_PHOTO_MEDIA_PREFIX, "r1", &hash),
            format!("media/listing-photo/renditions/r1/{h}.json")
        );
        assert!(Sha256Hex::parse(&"A".repeat(64)).is_err());
        assert!(Sha256Hex::parse(&"a".repeat(63)).is_err());
        Ok(())
    }
}
