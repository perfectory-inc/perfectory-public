//! Lakehouse application use cases and outbound ports.

#![deny(missing_docs)]

/// Silver normalization helpers for official building-register floor rows.
pub mod building_register_floor_silver_plan;

/// Building-register main title parsing for floor-count and building-link witnesses.
pub mod building_register_title;

/// Silver normalization helpers for official building-register unit-area rows.
pub mod building_register_unit_area_silver_plan;

/// Silver normalization helpers for official building-register unit rows.
pub mod building_register_unit_silver_plan;

/// Use case for building canonical industrial-complex Silver handoff JSONL.
pub mod build_industrial_complex_silver_handoff;

/// Silver handoff helpers for canonical industrial-complex rows.
pub mod industrial_complex_silver_plan;

/// Outbound ports implemented by Lakehouse infrastructure.
pub mod ports;

/// Use case for selecting validated Lakehouse batch promotion candidates.
pub mod get_lakehouse_promotion_candidate;

/// Industrial-complex Gold pointer publication.
pub mod publish_industrial_complex_gold_pointer;

/// Use case for recording validated Lakehouse batch run summaries.
pub mod record_lakehouse_batch_run;

/// Governed Registry artifact registration.
pub mod register_lakehouse_object_artifact;

/// Silver normalization helpers for `VWorld` cadastral parcel-boundary rows.
pub mod vworld_cadastral_silver_plan;

pub use build_industrial_complex_silver_handoff::{
    BuildIndustrialComplexSilverHandoff, BuildIndustrialComplexSilverHandoffInput,
};
pub use building_register_floor_silver_plan::{
    build_building_register_floor_entity_context_pack_input,
    build_building_register_floor_normalization_proposal_input,
    build_building_register_floor_silver_handoff,
    build_building_register_floor_silver_handoff_from_public_data_bronze_json,
    build_building_register_floor_silver_outputs_from_public_data_bronze_json,
    normalize_building_register_floor_silver_rows,
    normalize_building_register_floor_silver_rows_from_public_data_bronze_json,
    normalize_building_register_floor_silver_rows_with_title_counts,
    parse_building_register_floor_source_row_from_hub_bulk_text_line,
    parse_building_register_floor_source_rows_from_public_data_json,
    BuildingRegisterFloorEntityContextPackInput, BuildingRegisterFloorNormalizationProposalInput,
    BuildingRegisterFloorSilverHandoff, BuildingRegisterFloorSilverOutputs,
    BuildingRegisterFloorSilverPlanError, BuildingRegisterFloorSilverRow,
    BuildingRegisterFloorSilverRowsInput, BuildingRegisterFloorSourceRow,
    PublicDataBuildingRegisterFloorBronzeJsonInput,
};
pub use building_register_title::{
    parse_building_title_building_link_from_hub_bulk_text_line,
    parse_building_title_floor_counts_from_hub_bulk_text_line, BuildingLink, BuildingTitleKeyIndex,
};
pub use building_register_unit_area_silver_plan::{
    building_register_unit_area_silver_row_to_jsonl,
    normalize_building_register_unit_area_silver_rows,
    parse_building_register_unit_area_source_row_from_hub_bulk_text_line,
    BuildingRegisterUnitAreaSilverRow, BuildingRegisterUnitAreaSilverRowsInput,
    BuildingRegisterUnitAreaSourceRow,
};
pub use building_register_unit_silver_plan::{
    apply_building_register_unit_silver_overrides,
    building_register_unit_silver_override_from_application_snapshot,
    building_register_unit_silver_row_to_jsonl, normalize_building_register_unit_silver_rows,
    normalize_building_register_unit_silver_rows_with_building_keys,
    parse_building_register_unit_source_row_from_hub_bulk_text_line,
    BuildingRegisterUnitSilverOverride, BuildingRegisterUnitSilverOverrideIndex,
    BuildingRegisterUnitSilverPlanError, BuildingRegisterUnitSilverRow,
    BuildingRegisterUnitSilverRowsInput, BuildingRegisterUnitSourceRow,
};
pub use industrial_complex_silver_plan::{
    build_industrial_complex_silver_handoff, normalize_industrial_complex_silver_rows,
    IndustrialComplexSilverHandoff, IndustrialComplexSilverPlanError, IndustrialComplexSilverRow,
    IndustrialComplexSilverRowsInput,
};

pub use get_lakehouse_promotion_candidate::GetLakehousePromotionCandidate;
pub use publish_industrial_complex_gold_pointer::{
    PublishIndustrialComplexGoldPointer, PublishIndustrialComplexGoldPointerCommand,
    PublishIndustrialComplexGoldPointerInput,
};
pub use record_lakehouse_batch_run::{RecordLakehouseBatchRun, RecordLakehouseBatchRunInput};
pub use register_lakehouse_object_artifact::{
    RegisterLakehouseObjectArtifact, RegisterLakehouseObjectArtifactCommand,
    RegisterLakehouseObjectArtifactInput, RegisterLakehouseObjectArtifactReceipt,
};
pub use vworld_cadastral_silver_plan::{
    build_vworld_cadastral_silver_parcel_boundary_handoff,
    normalize_vworld_cadastral_silver_parcel_boundary_rows, VWorldCadastralBoundingBox,
    VWorldCadastralSilverParcelBoundaryHandoff, VWorldCadastralSilverParcelBoundaryRow,
    VWorldCadastralSilverParcelBoundaryRowsInput, VWorldCadastralSilverPlanError,
};
