//! Building-register floor label normalization rules.

mod entity_resolver;
mod normalizer;

use serde::{Deserialize, Serialize};

pub use entity_resolver::{resolve_building_floors, BuildingFloorCounts, FloorRowEvidence};
pub use normalizer::{
    building_register_floor_evidence_numbers, building_register_floor_label_is_attic,
    normalize_building_register_floor,
};

/// Raw floor fields from building-register records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawBuildingRegisterFloor<'a> {
    /// Provider floor-kind code, for example `10`, `20`, `30`, or `40`.
    pub floor_type_code: &'a str,
    /// Provider floor-kind name, for example `지하`, `지상`, `옥탑`, or `각층`.
    pub floor_type_name: &'a str,
    /// Provider floor number field. It is kept as text until validated.
    pub floor_number: &'a str,
    /// Provider floor label when the source dataset has one.
    pub floor_label: Option<&'a str>,
}

/// Canonical floor kind interpreted from provider code/name fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BuildingRegisterFloorKind {
    /// Normal above-ground floor.
    AboveGround,
    /// Basement floor.
    Basement,
    /// Rooftop floor or rooftop structure row.
    Rooftop,
    /// Provider row that describes all floors rather than one numbered floor.
    AllFloors,
    /// Provider row that describes the lower side of a multi-floor unit.
    MultiFloorLower,
    /// Provider row that describes the upper side of a multi-floor unit.
    MultiFloorUpper,
    /// Unknown or unsupported provider floor kind.
    Unknown,
}

impl BuildingRegisterFloorKind {
    /// Stable wire representation for Silver rows and API payloads.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::AboveGround => "above_ground",
            Self::Basement => "basement",
            Self::Rooftop => "rooftop",
            Self::AllFloors => "all_floors",
            Self::MultiFloorLower => "multi_floor_lower",
            Self::MultiFloorUpper => "multi_floor_upper",
            Self::Unknown => "unknown",
        }
    }
}

/// Whether deterministic normalization can safely apply this record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BuildingRegisterFloorNormalizationStatus {
    /// Deterministic parser accepted the row.
    Accepted,
    /// Deterministic parser refused to guess; a proposal/review path may handle it.
    ProposalRequired,
}

impl BuildingRegisterFloorNormalizationStatus {
    /// Stable wire representation for Silver rows and API payloads.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::ProposalRequired => "proposal_required",
        }
    }
}

/// Machine-readable reason for the normalization decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BuildingRegisterFloorNormalizationReason {
    /// Label and number exactly described the provider kind.
    AcceptedExactLabel,
    /// Basement shorthand such as `지1` or `지1층` was accepted.
    AcceptedBasementShorthand,
    /// Generic basement label such as `지층` used the numeric floor field.
    AcceptedBasementGenericLabelWithNumber,
    /// Source lacks a label field, so code/name/number were used.
    AcceptedLabelAbsentUsingTypeAndNumber,
    /// Special non-numbered provider floor kind was accepted.
    AcceptedSpecialFloorKind,
    /// Label was present in the source but empty after trimming.
    EmptyLabel,
    /// Floor number was empty or not a valid unsigned integer.
    InvalidFloorNumber,
    /// Floor number exceeded the deterministic parser's safe range.
    FloorNumberOutOfRange,
    /// Label described a different floor kind than the provider code/name.
    LabelKindMismatch,
    /// Label number disagreed with the provider numeric floor field.
    LabelNumberMismatch,
    /// Provider numeric floor field was invalid, but the label carried a safe floor number.
    AcceptedLabelNumberUsingInvalidProviderNumber,
    /// A rooftop-kind row whose label denotes a rooftop structure (옥탑/옥상/지붕/탑)
    /// was accepted as a rooftop even though the label and provider floor numbers
    /// disagreed: the rooftop level is nominal, so the disagreement is not a real
    /// floor conflict. 다락 (attic) / 계단 (stairwell) labels are intentionally excluded.
    AcceptedRooftopStructure,
    /// Provider used a special high floor number such as `901` or `9001`.
    SpecialProviderFloorNumber,
    /// Provider code and provider name did not describe the same floor kind.
    TypeCodeNameMismatch,
    /// Provider floor-kind code/name was unknown.
    UnknownFloorType,
    /// A per-row num-vs-label conflict was resolved by building-level witness
    /// majority: the source (provider number or label number) that forms the
    /// building's clean contiguous floor sequence was chosen.
    ResolvedByBuildingWitnessMajority,
    /// A 다락 (attic) row was classified as the building's top above-ground floor
    /// because the 표제부 지상층수 exceeds the observed numbered floors by one, so
    /// the 다락 accounts for the missing top floor.
    ResolvedAtticAsTopFloor,
    /// A 다락 (attic) row was classified as a rooftop structure because the
    /// observed numbered floors already account for the full 표제부 지상층수, so the
    /// 다락 sits above the counted floors.
    ResolvedAtticAsRooftop,
}

impl BuildingRegisterFloorNormalizationReason {
    /// Stable wire representation for Silver rows and proposal evidence.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::AcceptedExactLabel => "accepted_exact_label",
            Self::AcceptedBasementShorthand => "accepted_basement_shorthand",
            Self::AcceptedBasementGenericLabelWithNumber => {
                "accepted_basement_generic_label_with_number"
            }
            Self::AcceptedLabelAbsentUsingTypeAndNumber => {
                "accepted_label_absent_using_type_and_number"
            }
            Self::AcceptedSpecialFloorKind => "accepted_special_floor_kind",
            Self::EmptyLabel => "empty_label",
            Self::InvalidFloorNumber => "invalid_floor_number",
            Self::FloorNumberOutOfRange => "floor_number_out_of_range",
            Self::LabelKindMismatch => "label_kind_mismatch",
            Self::LabelNumberMismatch => "label_number_mismatch",
            Self::AcceptedLabelNumberUsingInvalidProviderNumber => {
                "accepted_label_number_using_invalid_provider_number"
            }
            Self::AcceptedRooftopStructure => "accepted_rooftop_structure",
            Self::SpecialProviderFloorNumber => "special_provider_floor_number",
            Self::TypeCodeNameMismatch => "type_code_name_mismatch",
            Self::UnknownFloorType => "unknown_floor_type",
            Self::ResolvedByBuildingWitnessMajority => "resolved_by_building_witness_majority",
            Self::ResolvedAtticAsTopFloor => "resolved_attic_as_top_floor",
            Self::ResolvedAtticAsRooftop => "resolved_attic_as_rooftop",
        }
    }
}

/// Deterministic interpretation of one building-register floor row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedBuildingRegisterFloor {
    /// Canonical floor kind.
    pub kind: BuildingRegisterFloorKind,
    /// Canonical floor number when the row is a numbered floor.
    pub floor_number: Option<u16>,
    /// Signed floor position: above-ground positive, basement negative.
    pub floor_index: Option<i16>,
    /// Korean display label derived from deterministic rules.
    pub display_ko: Option<String>,
    /// Deterministic normalization status.
    pub status: BuildingRegisterFloorNormalizationStatus,
    /// Machine-readable reason for the status.
    pub reason: BuildingRegisterFloorNormalizationReason,
}
