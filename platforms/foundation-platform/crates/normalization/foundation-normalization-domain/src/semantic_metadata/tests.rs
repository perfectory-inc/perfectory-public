use super::*;

#[test]
fn maps_hub_floor_overview_fields_to_shared_floor_concepts() {
    let mappings = field_semantic_mappings_for_source("hubgokr__building_register_floor_overview");

    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "mgm_bldrgst_pk"
            && mapping.concept_id == SemanticConceptId::BuildingRegistryKey
    }));
    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "floor_number_raw"
            && mapping.concept_id == SemanticConceptId::FloorNumber
    }));
    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "floor_type_code_raw"
            && mapping.concept_id == SemanticConceptId::FloorKindCode
    }));
    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "floor_type_name_raw"
            && mapping.concept_id == SemanticConceptId::FloorKindName
    }));
}

#[test]
fn maps_public_data_floor_overview_fields_to_shared_floor_concepts() {
    let mappings = field_semantic_mappings_for_source("datagokr__building_register_floor_overview");

    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "mgmBldrgstPk"
            && mapping.concept_id == SemanticConceptId::BuildingRegistryKey
    }));
    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "flrNo" && mapping.concept_id == SemanticConceptId::FloorNumber
    }));
    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "flrGbCd"
            && mapping.concept_id == SemanticConceptId::FloorKindCode
    }));
    assert!(mappings.iter().any(|mapping| {
        mapping.field.field_path == "flrGbCdNm"
            && mapping.concept_id == SemanticConceptId::FloorKindName
    }));
}

#[test]
fn maps_floor_overview_sources_to_building_floor_consistency_impact() {
    for source_slug in [
        "hubgokr__building_register_floor_overview",
        "datagokr__building_register_floor_overview",
    ] {
        let impacts = entity_impact_mappings_for_source(source_slug);

        assert_eq!(impacts.len(), 1);
        assert_eq!(impacts[0].entity_type, EntityTypeId::Building);
        assert_eq!(impacts[0].entity_key_fields, &["mgm_bldrgst_pk"]);
        assert_eq!(
            impacts[0].consistency_domains,
            &[
                ConsistencyDomainId::FloorLabelNormalization,
                ConsistencyDomainId::FloorKindConsistency,
                ConsistencyDomainId::BuildingFloorSequenceConsistency,
            ]
        );
    }
}
