//! Provider-neutral file assets owned by the Catalog context.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{FileAssetId, SourceRecordId};
use foundation_shared_kernel::ObjectKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// File visibility for Catalog-managed assets.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileAssetVisibility {
    /// Publicly readable file.
    Public,
    /// Internal staff-readable file.
    Internal,
    /// Private restricted file.
    Private,
}

impl FileAssetVisibility {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Private => "private",
        }
    }

    /// Parses a stable wire value into a domain visibility.
    ///
    /// # Errors
    /// Returns `ParseFileAssetVisibilityError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseFileAssetVisibilityError> {
        match raw {
            "public" => Ok(Self::Public),
            "internal" => Ok(Self::Internal),
            "private" => Ok(Self::Private),
            other => Err(ParseFileAssetVisibilityError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing file asset visibility.
#[derive(Debug, Error)]
pub enum ParseFileAssetVisibilityError {
    /// Unsupported wire value.
    #[error("unknown FileAssetVisibility wire value: {0:?}")]
    Unknown(String),
}

/// File asset classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileAssetKind {
    /// Official image asset.
    OfficialImage,
    /// Official document asset.
    OfficialDocument,
    /// Blueprint or drawing asset.
    Blueprint,
    /// Notice attachment asset.
    NoticeAttachment,
    /// Digital twin asset.
    DigitalTwin,
    /// Raw source snapshot asset.
    RawSnapshot,
    /// Other or unknown file asset kind.
    Other,
}

impl FileAssetKind {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::OfficialImage => "official_image",
            Self::OfficialDocument => "official_document",
            Self::Blueprint => "blueprint",
            Self::NoticeAttachment => "notice_attachment",
            Self::DigitalTwin => "digital_twin",
            Self::RawSnapshot => "raw_snapshot",
            Self::Other => "other",
        }
    }

    /// Parses a stable wire value into a domain file asset kind.
    ///
    /// # Errors
    /// Returns `ParseFileAssetKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseFileAssetKindError> {
        match raw {
            "official_image" => Ok(Self::OfficialImage),
            "official_document" => Ok(Self::OfficialDocument),
            "blueprint" => Ok(Self::Blueprint),
            "notice_attachment" => Ok(Self::NoticeAttachment),
            "digital_twin" => Ok(Self::DigitalTwin),
            "raw_snapshot" => Ok(Self::RawSnapshot),
            "other" => Ok(Self::Other),
            other => Err(ParseFileAssetKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing file asset kind.
#[derive(Debug, Error)]
pub enum ParseFileAssetKindError {
    /// Unsupported wire value.
    #[error("unknown FileAssetKind wire value: {0:?}")]
    Unknown(String),
}

/// Provider-neutral file metadata owned by Catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileAsset {
    /// Stable foundation-platform file asset identifier.
    pub id: FileAssetId,
    /// Provider-neutral object key.
    pub object_key: ObjectKey,
    /// MIME type recorded for the object.
    pub mime_type: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional SHA-256 checksum in lowercase hexadecimal form.
    pub checksum_sha256: Option<String>,
    /// Optional display title for UI surfaces.
    pub title: Option<String>,
    /// Optional source record that produced this asset.
    pub source_record_id: Option<SourceRecordId>,
    /// File visibility.
    pub visibility: FileAssetVisibility,
    /// UTC timestamp when the file asset was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the file asset was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
