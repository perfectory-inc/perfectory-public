//! Industry taxonomy and parcel-level assignment rules.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{
    ComplexId, IndustryAssignmentId, IndustryGroupId, ParcelId, SourceRecordId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Industry code system used by Catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndustryCodeSystem {
    /// Korean Standard Industrial Classification.
    Ksic,
}

impl IndustryCodeSystem {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Ksic => "ksic",
        }
    }

    /// Parses a stable wire value into an industry code system.
    ///
    /// # Errors
    /// Returns `ParseIndustryCodeSystemError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseIndustryCodeSystemError> {
        match raw {
            "ksic" => Ok(Self::Ksic),
            other => Err(ParseIndustryCodeSystemError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing an industry code system.
#[derive(Debug, Error)]
pub enum ParseIndustryCodeSystemError {
    /// Unsupported wire value.
    #[error("unknown IndustryCodeSystem wire value: {0:?}")]
    Unknown(String),
}

/// Relationship kind between an industry group and a parcel or complex rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndustryAssignmentKind {
    /// Industry is allowed.
    Allowed,
    /// Industry is recommended.
    Recommended,
    /// Industry is restricted.
    Restricted,
}

impl IndustryAssignmentKind {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Recommended => "recommended",
            Self::Restricted => "restricted",
        }
    }

    /// Parses a stable wire value into an assignment kind.
    ///
    /// # Errors
    /// Returns `ParseIndustryAssignmentKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseIndustryAssignmentKindError> {
        match raw {
            "allowed" => Ok(Self::Allowed),
            "recommended" => Ok(Self::Recommended),
            "restricted" => Ok(Self::Restricted),
            other => Err(ParseIndustryAssignmentKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing an industry assignment kind.
#[derive(Debug, Error)]
pub enum ParseIndustryAssignmentKindError {
    /// Unsupported wire value.
    #[error("unknown IndustryAssignmentKind wire value: {0:?}")]
    Unknown(String),
}

/// Named group of industry codes within an industrial complex.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustryGroup {
    /// Stable foundation-platform industry group identifier.
    pub id: IndustryGroupId,
    /// Industrial complex that owns this industry group.
    pub complex_id: ComplexId,
    /// Industry group display name.
    pub name: String,
    /// Optional source-provided description.
    pub description: Option<String>,
    /// UTC timestamp when the group was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the group was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

/// Single industry code inside an industry group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustryGroupMember {
    /// Industry group that owns this member.
    pub industry_group_id: IndustryGroupId,
    /// Industry code value.
    pub industry_code: String,
    /// Industry code system.
    pub industry_code_system: IndustryCodeSystem,
}

/// Complex-level industry rule.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllowedIndustry {
    /// Stable foundation-platform rule identifier.
    pub id: IndustryAssignmentId,
    /// Industrial complex that owns the rule.
    pub complex_id: ComplexId,
    /// Industry group controlled by the rule.
    pub industry_group_id: IndustryGroupId,
    /// Rule kind.
    pub rule_kind: IndustryAssignmentKind,
    /// Optional source record that produced this rule.
    pub source_record_id: Option<SourceRecordId>,
    /// UTC timestamp when the rule was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

/// Parcel-level industry assignment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParcelIndustryAssignment {
    /// Stable foundation-platform assignment identifier.
    pub id: IndustryAssignmentId,
    /// Parcel receiving the assignment.
    pub parcel_id: ParcelId,
    /// Industry group assigned to the parcel.
    pub industry_group_id: IndustryGroupId,
    /// Assignment kind.
    pub assignment_kind: IndustryAssignmentKind,
    /// Optional source record that produced this assignment.
    pub source_record_id: Option<SourceRecordId>,
    /// UTC timestamp when the assignment was last updated.
    pub updated_at: DateTime<Utc>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}
