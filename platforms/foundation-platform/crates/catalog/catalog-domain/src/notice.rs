//! Official notices and attachments belonging to industrial complexes.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{ComplexId, FileAssetId, NoticeId, SourceRecordId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical notice classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NoticeType {
    /// General notice.
    Notice,
    /// Official announcement.
    Announcement,
    /// Sale or supply notice.
    Sale,
    /// Regulation notice.
    Regulation,
    /// Maintenance notice.
    Maintenance,
    /// Other or unknown notice type.
    Other,
}

impl NoticeType {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Notice => "notice",
            Self::Announcement => "announcement",
            Self::Sale => "sale",
            Self::Regulation => "regulation",
            Self::Maintenance => "maintenance",
            Self::Other => "other",
        }
    }

    /// Parses a stable wire value into a domain notice type.
    ///
    /// # Errors
    /// Returns `ParseNoticeTypeError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseNoticeTypeError> {
        match raw {
            "notice" => Ok(Self::Notice),
            "announcement" => Ok(Self::Announcement),
            "sale" => Ok(Self::Sale),
            "regulation" => Ok(Self::Regulation),
            "maintenance" => Ok(Self::Maintenance),
            "other" => Ok(Self::Other),
            other => Err(ParseNoticeTypeError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing a notice type.
#[derive(Debug, Error)]
pub enum ParseNoticeTypeError {
    /// Unsupported wire value.
    #[error("unknown NoticeType wire value: {0:?}")]
    Unknown(String),
}

/// Official notice or announcement attached to an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComplexNotice {
    /// Stable foundation-platform notice identifier.
    pub id: NoticeId,
    /// Industrial complex that owns this notice.
    pub complex_id: ComplexId,
    /// Canonical notice type.
    pub notice_type: NoticeType,
    /// Notice title.
    pub title: String,
    /// Optional short summary prepared for list views.
    pub summary: Option<String>,
    /// Publication timestamp when provided by the source.
    pub published_at: Option<DateTime<Utc>>,
    /// Optional source record that produced this notice.
    pub source_record_id: Option<SourceRecordId>,
    /// UTC timestamp when the notice was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the notice was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

/// Join row between a notice and an attached file asset.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NoticeAttachment {
    /// Notice that owns the attachment.
    pub notice_id: NoticeId,
    /// Attached file asset.
    pub file_asset_id: FileAssetId,
    /// Sort order within the notice attachment list.
    pub display_order: i32,
}
