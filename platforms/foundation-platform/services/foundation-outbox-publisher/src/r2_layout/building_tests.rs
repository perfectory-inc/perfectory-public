use super::*;
const PNU: &str = "9999900000100000000";
#[test]
fn building_serving_key_pins_generation_directory_and_pnu_grammar() -> anyhow::Result<()> {
    assert_eq!(
        building_by_pnu_serving_object_key(1, PNU)?,
        "serving/buildings/by-pnu/v1/9999900000100000000.json"
    );
    assert_eq!(
        building_by_pnu_serving_manifest_key()?,
        "serving/buildings/by-pnu/manifest.json"
    );

    assert!(building_by_pnu_serving_object_key(0, PNU).is_err());
    for invalid_pnu in [
        "999990000010000000",   // 18 digits
        "99999000001000000001", // 20 digits
        "999990000010000000a",  // non-digit
        "9999900000000000000",  // 11th digit outside the cadastral-register kinds [1289]
        "",
    ] {
        assert!(
            building_by_pnu_serving_object_key(1, invalid_pnu).is_err(),
            "PNU {invalid_pnu:?} must be refused"
        );
    }
    Ok(())
}

#[test]
fn a_generation_prefix_contains_its_keys_and_nothing_else() -> anyhow::Result<()> {
    let prefix = building_by_pnu_serving_generation_prefix(7)?;
    assert_eq!(prefix, "serving/buildings/by-pnu/v7/");
    assert!(building_by_pnu_serving_object_key(7, PNU)?.starts_with(&prefix));
    assert!(!building_by_pnu_serving_object_key(8, PNU)?.starts_with(&prefix));
    assert!(!building_by_pnu_serving_manifest_key()?.starts_with(prefix.as_str()));
    assert!(building_by_pnu_serving_generation_prefix(0).is_err());
    Ok(())
}

#[test]
fn only_a_canonical_building_serving_key_is_recognised_as_one() -> anyhow::Result<()> {
    let key = building_by_pnu_serving_object_key(7, PNU)?;

    assert!(is_building_by_pnu_serving_object_key(&key));
    for other in [
        "serving/buildings/by-pnu/manifest.json",
        "serving/buildings/by-pnu/v01/9999900000100000000.json",
        "serving/buildings/by-pnu/v0/9999900000100000000.json",
        "serving/buildings/by-pnu/v1/999990000010000000.json",
        "serving/buildings/by-pnu/v1/nested/9999900000100000000.json",
        "serving/buildings/by-pnu/v1/9999900000100000000.json.bak",
        "serving/buildings/by-pnu/9999900000100000000.json",
        "serving/other/v1/9999900000100000000.json",
        "gold/industrial-complex/profiles/018f0000-0000-7000-8000-000000000001.json",
    ] {
        assert!(
            !is_building_by_pnu_serving_object_key(other),
            "non-canonical key was recognised as a building serving object: {other}"
        );
    }

    assert!(is_building_by_pnu_serving_manifest_key(
        "serving/buildings/by-pnu/manifest.json"
    ));
    assert!(!is_building_by_pnu_serving_manifest_key(&key));
    assert!(!is_industrial_complex_gold_profile_key(&key));
    Ok(())
}
