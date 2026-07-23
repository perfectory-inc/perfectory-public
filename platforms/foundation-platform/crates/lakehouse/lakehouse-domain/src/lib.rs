//! Provider-neutral Lakehouse contracts and domain rules.

/// Lakehouse domain errors.
pub mod errors;

/// Current Gold data pointers for industrial-complex artifacts.
pub mod industrial_complex_gold_pointer;

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

pub use errors::LakehouseError;
pub use industrial_complex_gold_pointer::{
    IndustrialComplexGoldPointer, IndustrialComplexGoldPointerPublished,
};
pub use lakehouse::{
    industrial_complex_lakehouse_contract_by_table_name, industrial_complex_lakehouse_contracts,
    LakehouseColumn, LakehouseLayer, LakehousePhysicalFormat, LakehouseServingRole,
    LakehouseTableContract, GOLD_COMPLEX_CATALOG, GOLD_COMPLEX_SPATIAL_LOCATOR,
    SILVER_BUILDING_REGISTER_FLOORS, SILVER_BUILDING_REGISTER_UNITS,
    SILVER_BUILDING_REGISTER_UNIT_AREAS, SILVER_COMPLEX_PARCEL_MEMBERSHIPS,
    SILVER_INDUSTRIAL_COMPLEXES, SILVER_INDUSTRIAL_COMPLEX_BOUNDARIES, SILVER_PARCEL_BOUNDARIES,
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
    SparkRunIcebergReadbackValidation, SparkRunInput, SparkRunSummary, SparkRunSummaryError,
    SparkRunTarget, SparkRunWriteDisposition, SparkRunWriteMode, SPARK_RUN_SUMMARY_SCHEMA_VERSION,
};
