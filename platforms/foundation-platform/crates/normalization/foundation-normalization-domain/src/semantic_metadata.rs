//! Semantic metadata contracts for source fields and entity impact.

/// Platform-wide semantic concepts used to interpret provider fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticConceptId {
    /// Provider building-register management key.
    BuildingRegistryKey,
    /// The floor number of the row's represented floor.
    FloorNumber,
    /// Provider code that classifies floor kind, such as basement or above-ground.
    FloorKindCode,
    /// Provider display name for floor kind.
    FloorKindName,
    /// Provider display label for the floor.
    FloorLabel,
    /// Whole-building above-ground floor count.
    AboveGroundFloorCount,
    /// Whole-building basement floor count.
    BelowGroundFloorCount,
}

impl SemanticConceptId {
    /// Returns the stable wire identifier for this semantic concept.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BuildingRegistryKey => "building_registry_key",
            Self::FloorNumber => "floor_number",
            Self::FloorKindCode => "floor_kind_code",
            Self::FloorKindName => "floor_kind_name",
            Self::FloorLabel => "floor_label",
            Self::AboveGroundFloorCount => "above_ground_floor_count",
            Self::BelowGroundFloorCount => "below_ground_floor_count",
        }
    }
}

/// Entity types affected by source rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityTypeId {
    /// Building entity.
    Building,
    /// Floor entity.
    Floor,
}

impl EntityTypeId {
    /// Returns the stable wire identifier for this entity type.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Floor => "floor",
        }
    }
}

/// Consistency domains that may need recalculation when source rows change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsistencyDomainId {
    /// Floor label normalization.
    FloorLabelNormalization,
    /// Floor kind consistency.
    FloorKindConsistency,
    /// Same-building floor sequence consistency.
    BuildingFloorSequenceConsistency,
}

impl ConsistencyDomainId {
    /// Returns the stable wire identifier for this consistency domain.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FloorLabelNormalization => "floor_label_normalization",
            Self::FloorKindConsistency => "floor_kind_consistency",
            Self::BuildingFloorSequenceConsistency => "building_floor_sequence_consistency",
        }
    }
}

/// A source field reference inside a provider dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceFieldRef {
    /// Canonical Bronze source slug.
    pub source_slug: &'static str,
    /// Provider field path or stable parser alias.
    pub field_path: &'static str,
}

/// Mapping from one source field to one platform semantic concept.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldSemanticMapping {
    /// Source field being interpreted.
    pub field: SourceFieldRef,
    /// Platform semantic concept.
    pub concept_id: SemanticConceptId,
    /// Whether this field is required when building entity context packs.
    pub required_for_entity_context: bool,
}

/// Mapping from source rows to impacted entity consistency domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityImpactMapping {
    /// Canonical Bronze source slug.
    pub source_slug: &'static str,
    /// Entity type impacted by this source.
    pub entity_type: EntityTypeId,
    /// Source fields that form the entity key in normalized source-row space.
    pub entity_key_fields: &'static [&'static str],
    /// Consistency domains to recalculate for the entity.
    pub consistency_domains: &'static [ConsistencyDomainId],
}

const HUB_BUILDING_REGISTER_FLOOR_OVERVIEW: &str = "hubgokr__building_register_floor_overview";
const DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW: &str =
    "datagokr__building_register_floor_overview";

const HUB_FLOOR_FIELD_MAPPINGS: &[FieldSemanticMapping] = &[
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: HUB_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "mgm_bldrgst_pk",
        },
        concept_id: SemanticConceptId::BuildingRegistryKey,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: HUB_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "floor_number_raw",
        },
        concept_id: SemanticConceptId::FloorNumber,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: HUB_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "floor_type_code_raw",
        },
        concept_id: SemanticConceptId::FloorKindCode,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: HUB_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "floor_type_name_raw",
        },
        concept_id: SemanticConceptId::FloorKindName,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: HUB_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "floor_label_raw",
        },
        concept_id: SemanticConceptId::FloorLabel,
        required_for_entity_context: false,
    },
];

const DATAGOKR_FLOOR_FIELD_MAPPINGS: &[FieldSemanticMapping] = &[
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "mgmBldrgstPk",
        },
        concept_id: SemanticConceptId::BuildingRegistryKey,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "flrNo",
        },
        concept_id: SemanticConceptId::FloorNumber,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "flrGbCd",
        },
        concept_id: SemanticConceptId::FloorKindCode,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "flrGbCdNm",
        },
        concept_id: SemanticConceptId::FloorKindName,
        required_for_entity_context: true,
    },
    FieldSemanticMapping {
        field: SourceFieldRef {
            source_slug: DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW,
            field_path: "flrNoNm",
        },
        concept_id: SemanticConceptId::FloorLabel,
        required_for_entity_context: false,
    },
];

const FLOOR_CONSISTENCY_DOMAINS: &[ConsistencyDomainId] = &[
    ConsistencyDomainId::FloorLabelNormalization,
    ConsistencyDomainId::FloorKindConsistency,
    ConsistencyDomainId::BuildingFloorSequenceConsistency,
];

const HUB_FLOOR_ENTITY_IMPACTS: &[EntityImpactMapping] = &[EntityImpactMapping {
    source_slug: HUB_BUILDING_REGISTER_FLOOR_OVERVIEW,
    entity_type: EntityTypeId::Building,
    entity_key_fields: &["mgm_bldrgst_pk"],
    consistency_domains: FLOOR_CONSISTENCY_DOMAINS,
}];

const DATAGOKR_FLOOR_ENTITY_IMPACTS: &[EntityImpactMapping] = &[EntityImpactMapping {
    source_slug: DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW,
    entity_type: EntityTypeId::Building,
    entity_key_fields: &["mgm_bldrgst_pk"],
    consistency_domains: FLOOR_CONSISTENCY_DOMAINS,
}];

/// Returns field semantic mappings for a canonical Bronze source slug.
#[must_use]
pub fn field_semantic_mappings_for_source(source_slug: &str) -> &'static [FieldSemanticMapping] {
    match source_slug {
        HUB_BUILDING_REGISTER_FLOOR_OVERVIEW => HUB_FLOOR_FIELD_MAPPINGS,
        DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW => DATAGOKR_FLOOR_FIELD_MAPPINGS,
        _ => &[],
    }
}

/// Returns entity impact mappings for a canonical Bronze source slug.
#[must_use]
pub fn entity_impact_mappings_for_source(source_slug: &str) -> &'static [EntityImpactMapping] {
    match source_slug {
        HUB_BUILDING_REGISTER_FLOOR_OVERVIEW => HUB_FLOOR_ENTITY_IMPACTS,
        DATAGOKR_BUILDING_REGISTER_FLOOR_OVERVIEW => DATAGOKR_FLOOR_ENTITY_IMPACTS,
        _ => &[],
    }
}

#[cfg(test)]
mod tests;
