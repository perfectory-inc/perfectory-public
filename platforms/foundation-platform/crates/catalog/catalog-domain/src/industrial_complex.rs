//! Industrial complex aggregate and wire classification.

use chrono::{DateTime, NaiveDate, Utc};
use foundation_shared_kernel::events::catalog_v1::{
    IndustrialComplexArchivedV1, IndustrialComplexCreatedV3, IndustrialComplexUpdatedV1,
};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId, StaffId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical industrial complex classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndustrialComplexKind {
    /// National industrial complex.
    National,
    /// General industrial complex.
    General,
    /// Agricultural industrial complex.
    Agricultural,
    /// Urban high-tech industrial complex.
    UrbanHighTech,
}

impl IndustrialComplexKind {
    /// Returns the stable wire value used by DB rows, outbox payloads, and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::National => "national",
            Self::General => "general",
            Self::Agricultural => "agricultural",
            Self::UrbanHighTech => "urban_high_tech",
        }
    }

    /// Parses a stable wire value into a domain kind.
    ///
    /// # Errors
    /// Returns `ParseIndustrialComplexKindError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseIndustrialComplexKindError> {
        match raw {
            "national" => Ok(Self::National),
            "general" => Ok(Self::General),
            "agricultural" => Ok(Self::Agricultural),
            "urban_high_tech" => Ok(Self::UrbanHighTech),
            other => Err(ParseIndustrialComplexKindError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing an industrial complex kind.
#[derive(Debug, Error)]
pub enum ParseIndustrialComplexKindError {
    /// Unsupported wire value.
    #[error("unknown IndustrialComplexKind wire value: {0:?}")]
    Unknown(String),
}

/// Where an industrial complex is in its development lifecycle.
///
/// The wire values are the ones `silver.industrial_complexes` already admits. That table's exported
/// contract carries the same six strings, and the `industrial_complex_bronze_raw_rows` test pins
/// that list to this enum, the way it already pins the classification. Two spellings of one domain
/// is how the two drift apart, so the domain has exactly one owner — this enum — and the exported
/// list is checked against it rather than maintained beside it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndustrialComplexStatus {
    /// Designated or under compensation; no site formation has started.
    Planned,
    /// Site formation is under way.
    Developing,
    /// Site formation is complete and the complex is in service.
    Operating,
    /// The designation changed and the current shape is not yet settled.
    Changed,
    /// The designation was withdrawn.
    Abolished,
    /// The source stated a lifecycle label this domain does not recognize.
    ///
    /// Distinct from `None`: `None` means the source said nothing, `Unknown` means it said
    /// something unreadable. Collapsing the two would delete the difference.
    Unknown,
}

impl IndustrialComplexStatus {
    /// Returns the stable wire value used by DB rows and HTTP DTOs.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Developing => "developing",
            Self::Operating => "operating",
            Self::Changed => "changed",
            Self::Abolished => "abolished",
            Self::Unknown => "unknown",
        }
    }

    /// Parses a stable wire value into a domain status.
    ///
    /// # Errors
    /// Returns `ParseIndustrialComplexStatusError::Unknown` for unsupported wire values.
    pub fn from_wire(raw: &str) -> Result<Self, ParseIndustrialComplexStatusError> {
        match raw {
            "planned" => Ok(Self::Planned),
            "developing" => Ok(Self::Developing),
            "operating" => Ok(Self::Operating),
            "changed" => Ok(Self::Changed),
            "abolished" => Ok(Self::Abolished),
            "unknown" => Ok(Self::Unknown),
            other => Err(ParseIndustrialComplexStatusError::Unknown(other.to_owned())),
        }
    }
}

/// Error returned while parsing an industrial complex status.
#[derive(Debug, Error)]
pub enum ParseIndustrialComplexStatusError {
    /// Unsupported wire value.
    #[error("unknown IndustrialComplexStatus wire value: {0:?}")]
    Unknown(String),
}

/// Canonical industrial complex aggregate root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplex {
    /// Stable foundation-platform complex identifier.
    pub id: ComplexId,
    /// Identifier the same complex carries in the lakehouse, when it was sourced from there.
    ///
    /// [`Self::id`] is minted locally with `Uuid::now_v7()`, so it is unrelated to the `UUIDv5` the
    /// Bronze-to-Silver job derives — and Gold profile object keys are computed from the latter.
    /// Without this column a canonical row names no object in R2, which is the only reason it
    /// exists; it is not a second primary key.
    ///
    /// `None` for a complex registered through the API rather than loaded from a Gold snapshot:
    /// such a complex is in no lakehouse table and has no artifact to address.
    pub lakehouse_complex_id: Option<LakehouseComplexId>,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Canonical industrial complex classification.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code that defines the complex parcel scope, when one was sourced.
    ///
    /// `None` is a fact, not a gap to be filled: the sourced industrial complexes resolve to
    /// sigungu granularity at best (root ADR-0034), and root ADR-0040 dropped the column's
    /// `NOT NULL` rather than let a producer invent ten digits.
    pub primary_bjdong_code: Option<String>,
    /// Official complex area in square meters.
    pub area_m2: u64,
    /// Where the complex is in its development lifecycle, when the source stated one.
    ///
    /// Every field from here to `completion_date` is sourced description rather than identity: the
    /// Gold projection carries it, the canonical row records it, and nothing derives it. All are
    /// optional because the source workbook leaves cells blank, and a blank cell is `None` — never
    /// `""`, never a zero, never a substituted neighbour value.
    pub status: Option<IndustrialComplexStatus>,
    /// Province-level administrative code, when the address resolution produced one.
    pub sido_code: Option<String>,
    /// City/county/district administrative code, when the address resolution produced one.
    pub sigungu_code: Option<String>,
    /// Address the source stated for the complex, verbatim.
    pub address_text: Option<String>,
    /// Organization that manages the complex.
    pub management_agency_name: Option<String>,
    /// Organization that developed the complex.
    pub developer_name: Option<String>,
    /// Date the complex was officially designated.
    pub designated_date: Option<NaiveDate>,
    /// Date the complex's site formation was approved as complete.
    pub completion_date: Option<NaiveDate>,
    /// UTC timestamp when the complex was created.
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when the complex was last updated.
    pub updated_at: DateTime<Utc>,
    /// UTC timestamp when the complex was archived, if no longer active.
    pub archived_at: Option<DateTime<Utc>>,
    /// Monotonic version used for optimistic concurrency.
    pub version: i64,
}

impl IndustrialComplex {
    /// Returns whether the official area differs from another area value.
    #[must_use]
    pub const fn area_differs_from(&self, other: u64) -> bool {
        self.area_m2 != other
    }

    /// Builds the creation outbox payload for this aggregate.
    #[must_use]
    pub fn created_event(&self) -> IndustrialComplexCreatedV3 {
        IndustrialComplexCreatedV3 {
            schema_version: 3,
            complex_id: self.id,
            official_complex_code: self.official_complex_code.clone(),
            name: self.name.clone(),
            primary_bjdong_code: self.primary_bjdong_code.clone(),
            created_at: self.created_at,
        }
    }

    /// Builds the update outbox payload for the given mutation.
    #[must_use]
    pub fn updated_event(&self, mutation: &ComplexMutation) -> IndustrialComplexUpdatedV1 {
        self.updated_fields_event(mutation.changed_fields())
    }

    /// Builds the update outbox payload for explicit changed-field names.
    #[must_use]
    pub const fn updated_fields_event(
        &self,
        changed_fields: Vec<String>,
    ) -> IndustrialComplexUpdatedV1 {
        IndustrialComplexUpdatedV1 {
            schema_version: 1,
            complex_id: self.id,
            changed_fields,
            updated_at: self.updated_at,
        }
    }

    /// Builds the archive outbox payload for this aggregate.
    #[must_use]
    pub fn archived_event(
        &self,
        operator_staff_id: StaffId,
        reason: Option<String>,
        request_id: Option<String>,
    ) -> IndustrialComplexArchivedV1 {
        IndustrialComplexArchivedV1 {
            schema_version: 1,
            complex_id: self.id,
            operator_staff_id,
            request_id,
            reason,
            archived_at: self.archived_at.unwrap_or(self.updated_at),
        }
    }
}

/// Partial industrial complex mutation and changed-field extractor.
#[derive(Clone, Debug, Default)]
pub struct ComplexMutation {
    /// Optional replacement name.
    pub name: Option<String>,
    /// Optional replacement area in square meters.
    pub area_m2: Option<u64>,
}

impl ComplexMutation {
    /// Returns the wire field names changed by this mutation.
    #[must_use]
    pub fn changed_fields(&self) -> Vec<String> {
        let mut v = Vec::with_capacity(2);
        if self.name.is_some() {
            v.push("name".to_owned());
        }
        if self.area_m2.is_some() {
            v.push("area_m2".to_owned());
        }
        v
    }
}
