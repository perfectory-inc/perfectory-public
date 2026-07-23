//! Contract tests for industrial-complex lakehouse table definitions.

use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseColumn, LakehouseLayer,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    GOLD_COMPLEX_SPATIAL_LOCATOR, SILVER_BUILDING_REGISTER_FLOORS, SILVER_BUILDING_REGISTER_UNITS,
    SILVER_BUILDING_REGISTER_UNIT_AREAS, SILVER_COMPLEX_PARCEL_MEMBERSHIPS,
    SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES, SILVER_PARCEL_BOUNDARIES,
};

fn has_column(contract: &LakehouseTableContract, name: &str) -> bool {
    contract.columns.iter().any(|column| column.name == name)
}

fn column_required(contract: &LakehouseTableContract, name: &str) -> Option<bool> {
    contract
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.required)
}

fn required_columns(contract: &LakehouseTableContract) -> impl Iterator<Item = &LakehouseColumn> {
    contract.columns.iter().filter(|column| column.required)
}

#[test]
fn industrial_complex_contract_set_is_complete() {
    let contracts = industrial_complex_lakehouse_contracts();

    assert_eq!(contracts.len(), 9);
    assert!(contracts
        .iter()
        .all(|contract| !contract.table_name.is_empty()));
    assert!(contracts
        .iter()
        .all(|contract| required_columns(contract).count() > 0));
    assert!(contracts.contains(&SILVER_BUILDING_REGISTER_FLOORS));
    assert!(contracts.contains(&SILVER_BUILDING_REGISTER_UNITS));
    assert!(contracts.contains(&SILVER_BUILDING_REGISTER_UNIT_AREAS));
}

#[test]
fn silver_contracts_are_canonical() {
    let contracts = industrial_complex_lakehouse_contracts();

    assert!(contracts
        .iter()
        .filter(|contract| contract.layer == LakehouseLayer::Silver)
        .all(|contract| contract.serving_role == LakehouseServingRole::Canonical));
}

#[test]
fn boundary_contract_is_geoparquet_with_geometry_pruning_columns() {
    let contract = SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES;

    assert_eq!(
        contract.physical_format,
        LakehousePhysicalFormat::GeoParquet
    );
    assert!(has_column(&contract, "geometry_wkb"));
    assert!(has_column(&contract, "geometry_srid"));
    assert!(has_column(&contract, "bbox_min_x"));
    assert!(has_column(&contract, "bbox_min_y"));
    assert!(has_column(&contract, "bbox_max_x"));
    assert!(has_column(&contract, "bbox_max_y"));
    assert!(has_column(&contract, "geometry_checksum_sha256"));
    assert!(contract.partition_spec.contains(&"sido_code"));
    assert!(contract.partition_spec.contains(&"bucket(32, complex_id)"));
}

#[test]
fn parcel_boundary_contract_is_canonical_geoparquet_partitioned_for_pnu_lookup() {
    let contract = SILVER_PARCEL_BOUNDARIES;

    assert_eq!(contract.table_name, "silver.parcel_boundaries");
    assert_eq!(contract.layer, LakehouseLayer::Silver);
    assert_eq!(
        contract.physical_format,
        LakehousePhysicalFormat::GeoParquet
    );
    assert_eq!(contract.serving_role, LakehouseServingRole::Canonical);
    assert!(has_column(&contract, "boundary_id"));
    assert!(has_column(&contract, "pnu"));
    assert!(has_column(&contract, "sido_code"));
    assert!(has_column(&contract, "sigungu_code"));
    assert!(has_column(&contract, "bjdong_code"));
    assert!(has_column(&contract, "geometry_wkb"));
    assert!(has_column(&contract, "geometry_srid"));
    assert!(has_column(&contract, "bbox_min_x"));
    assert!(has_column(&contract, "bbox_min_y"));
    assert!(has_column(&contract, "bbox_max_x"));
    assert!(has_column(&contract, "bbox_max_y"));
    assert!(has_column(&contract, "geometry_checksum_sha256"));
    assert!(contract.partition_spec.contains(&"sigungu_code"));
    assert!(contract.partition_spec.contains(&"bucket(256, pnu)"));
    assert_eq!(contract.sort_order, &["pnu", "valid_from_utc"]);
}

#[test]
fn membership_contract_partitions_for_pnu_lookup() {
    let contract = SILVER_COMPLEX_PARCEL_MEMBERSHIPS;

    assert_eq!(contract.physical_format, LakehousePhysicalFormat::Parquet);
    assert!(has_column(&contract, "complex_id"));
    assert!(has_column(&contract, "pnu"));
    assert!(has_column(&contract, "sigungu_code"));
    assert!(contract.partition_spec.contains(&"sigungu_code"));
    assert!(contract.partition_spec.contains(&"bucket(256, pnu)"));
    assert_eq!(
        contract.sort_order,
        &["complex_id", "pnu", "membership_kind"]
    );
}

#[test]
fn building_register_units_contract_is_canonical_and_entity_keyed() {
    let contract = SILVER_BUILDING_REGISTER_UNITS;

    assert_eq!(contract.table_name, "silver.building_register_units");
    assert_eq!(contract.layer, LakehouseLayer::Silver);
    assert_eq!(contract.physical_format, LakehousePhysicalFormat::Parquet);
    assert_eq!(contract.serving_role, LakehouseServingRole::Canonical);
    assert!(has_column(&contract, "unit_row_id"));
    assert!(has_column(&contract, "mgm_bldrgst_pk"));
    assert!(has_column(&contract, "pnu"));
    assert!(has_column(&contract, "dong_join_name"));
    assert!(has_column(&contract, "unit_number"));
    assert!(has_column(&contract, "floor_index"));
    assert!(has_column(&contract, "building_mgm_bldrgst_pk"));
    assert!(has_column(&contract, "building_link_method"));
    assert!(has_column(&contract, "normalization_status"));
    assert!(has_column(&contract, "source_snapshot_id"));
    assert!(has_column(&contract, "bronze_object_key"));
    assert!(has_column(&contract, "row_checksum_sha256"));
    assert!(contract.partition_spec.contains(&"bucket(256, pnu)"));
    assert_eq!(
        contract.sort_order,
        &[
            "pnu",
            "building_mgm_bldrgst_pk",
            "floor_index",
            "unit_number",
            "unit_row_id"
        ]
    );
    assert!(contract
        .quality_gates
        .contains(&"proposal_required_rows_preserved"));
    assert!(contract
        .quality_gates
        .contains(&"building_link_method_in_allowed_values"));
}

#[test]
fn building_register_unit_dong_name_raw_is_optional_source_evidence() {
    let contract = SILVER_BUILDING_REGISTER_UNITS;

    assert_eq!(column_required(&contract, "dong_name_raw"), Some(false));
    assert_eq!(column_required(&contract, "unit_name_raw"), Some(false));
    assert_eq!(column_required(&contract, "dong_join_name"), Some(false));
    assert_eq!(
        column_required(&contract, "building_link_method"),
        Some(true)
    );
}

#[test]
fn gold_spatial_locator_points_back_to_iceberg_artifacts() {
    let contract = GOLD_COMPLEX_SPATIAL_LOCATOR;

    assert_eq!(contract.layer, LakehouseLayer::Gold);
    assert_eq!(contract.serving_role, LakehouseServingRole::SpatialLocator);
    assert!(has_column(&contract, "object_key"));
    assert!(has_column(&contract, "iceberg_snapshot_id"));
    assert!(has_column(&contract, "geometry_checksum_sha256"));
}

#[test]
fn contracts_do_not_name_postgis_as_canonical_storage() {
    let contracts = industrial_complex_lakehouse_contracts();

    for contract in contracts {
        let table_text = contract.table_name.to_ascii_lowercase();
        assert!(!table_text.contains("postgis"));

        for gate in contract.quality_gates {
            let gate_text = gate.to_ascii_lowercase();
            assert!(!gate_text.contains("postgis"));
        }
    }
}
