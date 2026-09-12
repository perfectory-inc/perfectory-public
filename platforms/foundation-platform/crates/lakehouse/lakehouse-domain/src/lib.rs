//! Provider-neutral Lakehouse contracts and domain rules.

/// Lakehouse domain errors.
pub mod errors;

/// Current Gold data pointers for industrial-complex artifacts.
pub mod industrial_complex_gold_pointer;

/// JSONL transport contract for the industrial-complex Bronze-to-Silver job.
pub mod industrial_complex_jsonl_transport;

mod building_panel;
mod building_register_apartment_price;
mod building_register_exclusive_unit;
mod land_characteristic;
mod land_forest_ledger;
mod land_right_registration;
mod land_transfer_history;
mod parcel_panel;
mod unit_official_price;

/// Provider-neutral table contracts.
pub mod lakehouse;

/// Lakehouse maintenance planning contracts.
pub mod lakehouse_maintenance;

/// Provider-neutral Lakehouse lineage event contract.
pub mod lakehouse_lineage_event;

/// Provider-neutral Lakehouse quality-rule contract and evaluator.
pub mod lakehouse_quality;

/// Lakehouse Registry ownership and artifact metadata.
pub mod lakehouse_registry;

/// Spark batch run summary handoff contracts.
pub mod lakehouse_run_summary;

pub use building_register_apartment_price::SILVER_BUILDING_REGISTER_APARTMENT_PRICE;
pub use building_register_exclusive_unit::SILVER_BUILDING_REGISTER_EXCLUSIVE_UNIT;
pub use errors::LakehouseError;
pub use industrial_complex_gold_pointer::{
    IndustrialComplexGoldPointer, IndustrialComplexGoldPointerPublished,
};
pub use industrial_complex_jsonl_transport::{
    bronze_industrial_complexes_raw_jsonl_columns, BRONZE_INDUSTRIAL_COMPLEXES_RAW_JSONL,
    INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES, INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES,
    INDUSTRIAL_COMPLEX_SILVER_JOB_DERIVED_COLUMNS, INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
};
pub use lakehouse::{
    industrial_complex_lakehouse_contract_by_table_name, industrial_complex_lakehouse_contracts,
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, GOLD_BUILDING_PANEL, GOLD_COMPLEX_CATALOG,
    GOLD_COMPLEX_SPATIAL_LOCATOR, GOLD_PARCEL_PANEL, REFERENCE_LEGAL_DONG_CODE,
    REFERENCE_SIGUNGU_CANONICAL_CROSSWALK, SILVER_BUILDING_REGISTER_FLOORS,
    SILVER_BUILDING_REGISTER_TITLES, SILVER_BUILDING_REGISTER_UNITS,
    SILVER_BUILDING_REGISTER_UNIT_AREAS, SILVER_COMPLEX_PARCEL_MEMBERSHIPS,
    SILVER_INDUSTRIAL_COMPLEXES, SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES, SILVER_LAND_CHARACTERISTIC,
    SILVER_LAND_FOREST_LEDGER, SILVER_LAND_INDIVIDUAL_PRICE, SILVER_LAND_RIGHT_REGISTRATION,
    SILVER_LAND_TRANSFER_HISTORY, SILVER_LAND_USE_PLAN, SILVER_LAND_USE_ZONE_CODES,
    SILVER_PARCEL_BOUNDARIES,
};
pub use lakehouse_lineage_event::{
    validate_lakehouse_lineage_event, LakehouseLineageEventError,
    LAKEHOUSE_LINEAGE_EVENT_SCHEMA_VERSION, LAKEHOUSE_LINEAGE_EVENT_TYPE,
};
pub use lakehouse_maintenance::{
    plan_lakehouse_maintenance, BasisPoints, LakehouseMaintenanceAction,
    LakehouseMaintenanceActionKind, LakehouseMaintenancePlan, LakehouseMaintenancePolicy,
    LakehouseTableHealth, LakehouseTableHealthError, LakehouseTableHealthMetrics,
};
pub use lakehouse_quality::{
    evaluate_lakehouse_quality_rules, LakehouseQualityError, LakehouseQualityEvaluation,
    LakehouseQualityRules, LAKEHOUSE_QUALITY_RULES_SCHEMA_VERSION,
};
pub use lakehouse_registry::{
    LakehouseArtifactFormat, LakehouseAssetKind, LakehouseAssetStatus, LakehouseCatalogProvider,
    LakehouseDataAsset, LakehouseDatasetVersion, LakehouseDatasetVersionState,
    LakehouseEnvironment, LakehouseNamespaceStatus, LakehouseObjectArtifact, LakehouseOwnerService,
    LakehouseRegistryLayer, LakehouseStorageNamespace, LakehouseStorageProvider,
    ParseLakehouseRegistryWireError,
};
pub use lakehouse_run_summary::{
    required_quality_metric_names, SparkRunIcebergReadbackValidation, SparkRunInput,
    SparkRunSummary, SparkRunSummaryError, SparkRunTarget, SparkRunWriteDisposition,
    SparkRunWriteMode, SPARK_RUN_SUMMARY_SCHEMA_VERSION,
};
pub use unit_official_price::SILVER_UNIT_OFFICIAL_PRICE;
