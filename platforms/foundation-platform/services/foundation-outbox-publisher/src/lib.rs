//! Focused reusable command modules for the Foundation outbox-publisher executable.

/// Shared transport plumbing for Silver handoff exporters.
pub mod silver_handoff_io;

/// 시군구 canonical crosswalk seed loader (ADR-0103 geography identity Wave 1).
pub mod sigungu_crosswalk;

/// The code.go.kr 법정동 source contract (root ADR-0143), embedded once.
pub mod code_go_kr_legal_dong_contract;

/// Headerless HUB apartment-price ZIP to partitioned Silver handoff.
pub mod building_register_apartment_price_silver_export;

/// VWorld cadastral zipped-shapefile to Silver JSONL handoff command.
pub mod vworld_cadastral_shapefile_silver_export;

mod hub_register_silver_export;

/// Headerless HUB exclusive-unit ZIP to partitioned Silver handoff.
pub mod building_register_exclusive_unit_silver_export;

/// Shared FLOOR producer and native resource contract support.
pub mod building_register_floor_silver_export;

/// Shared FLOOR producer and native resource contract support.
pub mod bounded_bytes;

/// Shared FLOOR producer and native resource contract support.
pub mod bounded_parquet_writer;

/// Shared FLOOR producer and native resource contract support.
pub mod building_register_zip_lines;

/// Shared FLOOR producer and native resource contract support.
pub mod building_register_snapshot;

/// Shared FLOOR producer and native resource contract support.
pub mod building_register_source_role;

/// Shared FLOOR producer and native resource contract support.
pub mod serving_scratch;

/// Shared FLOOR producer and native resource contract support.
pub mod lakehouse_engine_contract;
