//! Industrial complex aggregate and wire classification.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::events::catalog_v1::{
    IndustrialComplexArchivedV1, IndustrialComplexCreatedV2, IndustrialComplexUpdatedV1,
};
use foundation_shared_kernel::ids::{ComplexId, StaffId};
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

/// Canonical industrial complex aggregate root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndustrialComplex {
    /// Stable foundation-platform complex identifier.
    pub id: ComplexId,
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Human-readable industrial complex name.
    pub name: String,
    /// Canonical industrial complex classification.
    pub kind: IndustrialComplexKind,
    /// primary legal-dong code that defines the complex parcel scope.
    pub primary_bjdong_code: String,
    /// Official complex area in square meters.
    pub area_m2: u64,
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
    pub fn created_event(&self) -> IndustrialComplexCreatedV2 {
        IndustrialComplexCreatedV2 {
            schema_version: 2,
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
