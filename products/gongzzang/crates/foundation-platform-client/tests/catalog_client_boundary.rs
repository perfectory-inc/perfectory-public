//! Catalog transport ownership regression coverage.

use std::error::Error;
use std::path::Path;

#[test]
fn catalog_paths_and_wire_dtos_live_only_in_the_foundation_client() -> Result<(), Box<dyn Error>> {
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let client = std::fs::read_to_string(
        repository_root.join("crates/foundation-platform-client/src/catalog.rs"),
    )?;
    let parcel_adapter = std::fs::read_to_string(
        repository_root.join("services/gongzzang-api/src/foundation_parcel_lookup.rs"),
    )?;
    let building_adapter = std::fs::read_to_string(
        repository_root.join("services/gongzzang-api/src/building_reader.rs"),
    )?;

    let parcel_production = parcel_adapter
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(&parcel_adapter);
    let building_production = building_adapter
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(&building_adapter);

    for contract_token in [
        "catalog/v1/parcels/by-pnu/",
        "pub struct CatalogParcelResponse",
        "pub struct CatalogBuildingResponse",
    ] {
        assert!(
            client.contains(contract_token),
            "Foundation client must own {contract_token}"
        );
    }
    assert!(!parcel_production.contains("catalog/v1/parcels/by-pnu/"));
    assert!(!building_production.contains("catalog/v1/parcels/by-pnu/"));
    assert!(!parcel_production.contains("struct FoundationPlatformParcelResponse"));
    assert!(!building_production.contains("struct FoundationPlatformBuildingResponse"));
    Ok(())
}
