use super::*;
use std::collections::BTreeMap;

#[test]
fn detects_building_impact_from_hub_floor_overview_record() {
    let mut fields = BTreeMap::new();
    fields.insert("mgm_bldrgst_pk".to_owned(), "11680-12345".to_owned());

    let impacts = detect_entity_impacts("hubgokr__building_register_floor_overview", &fields);

    assert_eq!(impacts.len(), 1);
    assert_eq!(impacts[0].entity_type, "building");
    assert_eq!(impacts[0].entity_key, "11680-12345");
    assert_eq!(
        impacts[0].consistency_domains,
        vec![
            "floor_label_normalization".to_owned(),
            "floor_kind_consistency".to_owned(),
            "building_floor_sequence_consistency".to_owned(),
        ]
    );
}

#[test]
fn returns_no_impact_when_required_entity_key_is_blank() {
    let mut fields = BTreeMap::new();
    fields.insert("mgm_bldrgst_pk".to_owned(), " ".to_owned());

    let impacts = detect_entity_impacts("hubgokr__building_register_floor_overview", &fields);

    assert!(impacts.is_empty());
}
