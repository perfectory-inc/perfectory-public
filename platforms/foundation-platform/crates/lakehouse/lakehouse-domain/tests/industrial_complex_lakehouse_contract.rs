//! Contract tests for industrial-complex lakehouse table definitions.

use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseColumn, LakehouseLayer,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract, GOLD_COMPLEX_CATALOG,
    GOLD_COMPLEX_SPATIAL_LOCATOR, SILVER_BUILDING_REGISTER_FLOORS, SILVER_BUILDING_REGISTER_UNITS,
    SILVER_BUILDING_REGISTER_UNIT_AREAS, SILVER_COMPLEX_PARCEL_MEMBERSHIPS,
    SILVER_INDUSTRIAL_COMPLEXES, SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES, SILVER_PARCEL_BOUNDARIES,
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

/// A partition key is written into the path or the manifest of every row, so a null one has no
/// value to write. Any contract that partitions on a column it does not require is unwritable for
/// the rows the same contract says are legal, which is exactly the state
/// `silver.industrial_complexes` was in once `sido_code` stopped being required (root ADR-0035).
#[test]
fn no_contract_partitions_on_a_column_it_does_not_require() {
    for contract in industrial_complex_lakehouse_contracts() {
        for entry in contract.partition_spec {
            let Some(required) = column_required(contract, entry) else {
                // A transform entry such as `bucket(32, complex_id)` names no column directly; the
                // column it wraps is covered by the identity entries the same spec lists.
                continue;
            };
            assert!(
                required,
                "{} partitions on optional column {entry}",
                contract.table_name
            );
        }
    }
}

/// The owner deferred per-region industrial-complex work, so neither the canonical table nor its
/// projection may require a region or lean on one for physical layout (root ADR-0035). One complex
/// in 1,442 has no district code from any source, and this is what lets it through without a code
/// being invented for it.
#[test]
fn the_industrial_complex_tables_neither_require_nor_partition_on_a_region() {
    for contract in [SILVER_INDUSTRIAL_COMPLEXES, GOLD_COMPLEX_CATALOG] {
        for region in ["sido_code", "sigungu_code"] {
            assert_eq!(
                column_required(&contract, region),
                Some(false),
                "{} still requires {region}",
                contract.table_name
            );
        }
        assert_eq!(contract.partition_spec, &["source_snapshot_id"]);
        for entry in contract.sort_order {
            assert!(
                !entry.contains("sido_code") && !entry.contains("sigungu_code"),
                "{} still sorts on a region: {entry}",
                contract.table_name
            );
        }
    }
    // The dong code was already optional and stays optional; nothing about this decision puts a
    // district code back into that column (root ADR-0034).
    assert_eq!(
        column_required(&SILVER_INDUSTRIAL_COMPLEXES, "primary_bjdong_code"),
        Some(false)
    );
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
    // Unpartitioned on purpose. Splitting 1,343 rows across 371 partitions produced 371 files of
    // twenty kilobytes, and compaction could not merge them because it never crosses a partition
    // (root ADR-0066). The sort order does the pruning a table this size needs.
    assert_eq!(contract.partition_spec, &[] as &[&str]);
    assert_eq!(
        contract.sort_order,
        &["complex_id", "boundary_kind", "valid_from_utc"]
    );
}

/// A partition must hold enough data to be worth the file it forces into existence.
///
/// Iceberg writes at least one file per partition, so the partition count is the floor on the
/// file count no matter how often compaction runs. Databricks calls a partition under a gigabyte
/// over-partitioned; this asserts the weaker thing we can check without live sizes — that a table
/// whose whole contents fit in one file is not split at all.
#[test]
fn a_table_that_fits_in_one_file_declares_no_partitions() {
    // Measured 2026-08-29 from `.files` on the live tables.
    const MEASURED_BYTES: [(&str, u64); 2] = [
        ("silver.industrial_complex_boundaries", 8_000_000),
        ("silver.industrial_complexes", 360_000),
    ];
    const TARGET_FILE_BYTES: u64 = 512 * 1024 * 1024;

    for (table_name, bytes) in MEASURED_BYTES {
        let contract = industrial_complex_lakehouse_contracts()
            .iter()
            .find(|candidate| candidate.table_name == table_name)
            .unwrap_or_else(|| panic!("{table_name} is missing from the contract set"));

        if bytes < TARGET_FILE_BYTES {
            assert!(
                contract.partition_spec.len() <= 1,
                "{table_name} holds {bytes} bytes, under one target file, yet declares {} \
                 partition fields; every field multiplies the floor on its file count",
                contract.partition_spec.len()
            );
        }
    }
}

#[test]
fn parcel_boundary_contract_is_canonical_geoparquet_partitioned_by_sigungu() {
    let contract = SILVER_PARCEL_BOUNDARIES;

    assert_eq!(contract.table_name, "silver.parcel_boundaries");
    assert_eq!(contract.current_row_predicate, Some("valid_to_utc IS NULL"));
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
    // Sigungu and nothing beside it. A PNU begins with its sigungu code, so a second
    // partition field derived from `pnu` cannot narrow a PNU lookup this one has not narrowed
    // already; the sort order below carries the search the rest of the way. What such a field
    // did do was multiply 255 partitions into 65,280, which measured out at 0.28 MB per file
    // against a 128 MB target (root ADR-0063).
    assert_eq!(contract.partition_spec, &["sigungu_code"]);
    assert_eq!(contract.sort_order, &["pnu", "valid_from_utc"]);
}

/// A partition field must be able to exclude rows the other fields would have read.
///
/// `bucket(256, pnu)` sat beside `sigungu_code` here for exactly as long as nobody divided the
/// row count by the partition count. It excluded nothing — a PNU's leading digits *are* the
/// sigungu code — and cost 256x the files. This is the arithmetic that would have caught it.
#[test]
fn parcel_boundary_partitions_hold_enough_rows_to_fill_a_file() {
    const NATIONAL_PARCEL_ROWS: u64 = 39_861_511;
    const SIGUNGU_COUNT: u64 = 255;
    // Below this, a partition cannot fill even a small Parquet file and the per-file footers
    // start to outweigh the rows they describe.
    const MIN_ROWS_PER_PARTITION: u64 = 50_000;

    let contract = SILVER_PARCEL_BOUNDARIES;
    let bucket_multiplier: u64 = contract
        .partition_spec
        .iter()
        .filter_map(|field| field.strip_prefix("bucket("))
        .filter_map(|rest| rest.split(',').next())
        .filter_map(|count| count.trim().parse::<u64>().ok())
        .product::<u64>()
        .max(1);

    let partitions = SIGUNGU_COUNT * bucket_multiplier;
    let rows_per_partition = NATIONAL_PARCEL_ROWS / partitions;

    assert!(
        rows_per_partition >= MIN_ROWS_PER_PARTITION,
        "{} partitions hold {rows_per_partition} rows each; a partition field that cannot \
         narrow a search only splits files (root ADR-0063)",
        partitions
    );
}

#[test]
fn only_parcel_boundaries_define_a_current_row_predicate() {
    for contract in industrial_complex_lakehouse_contracts() {
        if contract.table_name == SILVER_PARCEL_BOUNDARIES.table_name {
            assert_eq!(contract.current_row_predicate, Some("valid_to_utc IS NULL"));
        } else {
            assert_eq!(
                contract.current_row_predicate, None,
                "{} must not define a current-row predicate",
                contract.table_name
            );
        }
    }
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
