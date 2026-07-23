//! Pure deterministic Normalization rules and semantic contracts.
//!
//! This crate owns source-value interpretation and semantic impact decisions. It deliberately has
//! no Catalog, Lakehouse, Collection, database, HTTP, or model-runtime dependency.

/// Building-register floor interpretation and entity-context resolution.
pub mod building_register_floor;

/// Building-register unit interpretation.
pub mod building_register_unit;

/// Normalization capability errors.
pub mod errors;

/// Proposal identity and lifecycle contracts.
pub mod proposal;

/// Entity-impact detection from semantic metadata.
pub mod semantic_entity_impact;

/// Semantic metadata for source fields and consistency domains.
pub mod semantic_metadata;

pub use building_register_floor::{
    building_register_floor_evidence_numbers, building_register_floor_label_is_attic,
    normalize_building_register_floor, resolve_building_floors, BuildingFloorCounts,
    BuildingRegisterFloorKind, BuildingRegisterFloorNormalizationReason,
    BuildingRegisterFloorNormalizationStatus, FloorRowEvidence, NormalizedBuildingRegisterFloor,
    RawBuildingRegisterFloor,
};
pub use building_register_unit::{
    building_register_unit_designation, canonical_dong_join_key, normalize_building_register_unit,
    validate_building_register_unit_proposal, validate_building_register_unit_target_identity,
    validate_building_register_unit_target_identity_matches, BuildingRegisterUnitReason,
    BuildingRegisterUnitStatus, NormalizedBuildingRegisterUnit, RawBuildingRegisterUnit,
    BUILDING_REGISTER_UNIT_SCHEMA_VERSION,
};
pub use errors::NormalizationError;
pub use proposal::{
    compute_normalization_proposal_content_hash, compute_normalization_proposal_key,
    validate_normalization_json_object, NormalizationProposal, NormalizationProposalContentHash,
    NormalizationProposalKeyInput, NormalizationProposalStatus, NormalizationReviewDecision,
    NormalizationTargetKind,
};
pub use semantic_entity_impact::{detect_entity_impacts, DetectedEntityImpact};
pub use semantic_metadata::{
    entity_impact_mappings_for_source, field_semantic_mappings_for_source, ConsistencyDomainId,
    EntityImpactMapping, EntityTypeId, FieldSemanticMapping, SemanticConceptId, SourceFieldRef,
};
