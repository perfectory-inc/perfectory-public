//! The outbox-publisher Silver lane (root ADR-0200): HUB register and `VWorld` land Silver handoff
//! exports, the remote lakehouse job runner that drives them, and the source contracts they read.

/// Shared transport plumbing for Silver handoff exporters.
pub mod silver_handoff_io;

/// 시군구 canonical crosswalk seed loader (ADR-0103 geography identity Wave 1).
pub mod sigungu_crosswalk;

/// The code.go.kr 법정동 source contract (root ADR-0143), embedded once.
pub mod code_go_kr_legal_dong_contract;

/// The served edition of the parcel source contract (root ADR-0148).
pub mod vworld_parcel_source_contract;

/// Headerless HUB apartment-price ZIP to partitioned Silver handoff.
pub mod building_register_apartment_price_silver_export;

/// VWorld cadastral zipped-shapefile to Silver JSONL handoff command.
pub mod vworld_cadastral_shapefile_silver_export;

mod hub_register_silver_export;

/// Headerless HUB exclusive-unit ZIP to partitioned Silver handoff.
pub mod building_register_exclusive_unit_silver_export;

/// Headerless HUB 건축인허가 기본개요 ZIP to partitioned Silver handoff (root ADR-0356).
pub mod building_permit_basis_outline_silver_export;

/// Headerless HUB 월별 건물 전기·가스 ZIPs to partitioned Silver handoffs (root ADR-0396).
pub mod building_energy_monthly_silver_export;

/// Headerless HUB 철거멸실관리대장 ZIP to partitioned Silver handoff, keyed by row (root ADR-0397).
pub mod building_permit_demolition_loss_silver_export;

/// Headerless HUB 건축인허가 주차장 ZIP to partitioned Silver handoff (root ADR-0402).
pub mod building_permit_parking_silver_export;

/// Headerless HUB 건축인허가 부설주차장 ZIP to partitioned Silver handoff, keyed by row (root ADR-0402).
pub mod building_permit_annex_parking_silver_export;

/// Headerless HUB 건축인허가 동별개요 ZIP to partitioned Silver handoff (root ADR-0402).
pub mod building_permit_building_outline_silver_export;

/// Headerless HUB 건축인허가 대수선 ZIP to partitioned Silver handoff, keyed by row (root ADR-0402).
pub mod building_permit_major_repair_silver_export;

/// Headerless HUB 공작물관리대장 ZIP to partitioned Silver handoff, keyed by row (root ADR-0402).
pub mod building_permit_structure_ledger_silver_export;

/// Headerless HUB 건축인허가 층별개요 ZIP to partitioned Silver handoff (root ADR-0402).
pub mod building_permit_floor_overview_silver_export;

/// Headerless HUB 건축인허가 가설건축물 ZIP to partitioned Silver handoff, keyed by row (root ADR-0402).
pub mod building_permit_temporary_building_silver_export;

/// Headerless HUB 건축인허가 대지위치 ZIP to partitioned Silver handoff (root ADR-0402).
pub mod building_permit_site_location_silver_export;

/// Headerless HUB 주택인허가 기본개요 ZIP to partitioned Silver handoff, keyed by 관리주택대장PK (root ADR-0511).
pub mod housing_permit_basis_outline_silver_export;
/// Every HUB family lane's headerless ZIP to partitioned Silver handoff (root ADR-0551).
pub mod hub_family_silver_export;

/// Headerless HUB 건축물 정기점검이력 ZIP to partitioned Silver handoff (root ADR-0402).
pub mod building_maintenance_regular_inspection_history_silver_export;

/// Headerless HUB 총괄표제부 ZIP to partitioned Silver handoff (root ADR-0357).
pub mod building_register_master_silver_export;

/// Headerless HUB 부속지번 ZIP to partitioned Silver handoff, two PNUs a row (root ADR-0395).
pub mod building_register_sub_parcel_silver_export;

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

/// The one definition of a hub.go.kr Bronze file's provider day, month and release.
pub mod hub_provider_release;

/// How much memory the process may still allocate, from its cgroup and the host.
pub mod process_memory;

/// Shared FLOOR producer and native resource contract support.
pub mod building_register_source_role;

/// Shared FLOOR producer and native resource contract support.
pub mod serving_scratch;

/// Shared FLOOR producer and native resource contract support.
pub mod lakehouse_engine_contract;

/// `DuckDB` computes a transform's rows and stages them as Parquet an Iceberg table can adopt
/// (root ADR-0559, ADR-0560).
pub mod duckdb_parquet_stage;

/// The `DuckDB` extensions a release carries (root ADR-0559 §6).
pub mod duckdb_extensions;

/// Stages standing steward decisions as `silver.parcel_lineage` rows (root ADR-0115 §9).
///
/// The first transform that left Spark with its commit (root ADR-0560).
pub mod lineage_steward_fold_stage;

/// Derives `silver.parcel_lineage` for one pair of parcel editions and one sido group (root ADR-0113).
///
/// It was the Spark jobs `parcel_lineage_to_silver.py` and `parcel_lineage_scheduled.py`.
pub mod parcel_lineage_stage;

/// Which parcel lineage groups the schedule derives and which it already holds (root ADR-0148).
pub mod parcel_lineage_schedule;

/// The `DuckDB` the stages run in, for the caller that diffs and commits a stage's rows (root
/// ADR-0564 §3): one build of it in the executable.
pub use duckdb;

/// The code change table's views a `DuckDB` stage reads (root ADR-0145, ADR-0329).
pub mod legal_dong_code_change_stage;

/// The parcel nearby kind's Gold in `DuckDB` (root ADR-0465, ADR-0564 §9).
pub mod parcel_nearby_stage;

/// The 실거래 Gold's transforms in `DuckDB` (root ADR-0559 §3, ADR-0564 §9).
pub mod real_transaction_gold_stage;

/// The remote lakehouse job runner (Silver refresh, FLOOR cycle). It lived in the executable and
/// keeps the executable's lint posture (root ADR-0200 §4).
#[allow(
    clippy::nursery,
    clippy::pedantic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    missing_docs
)]
#[cfg_attr(
    test,
    allow(
        clippy::err_expect,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used
    )
)]
pub mod remote_lakehouse_job;

/// `VWorld` land Silver handoff exports. It lived in the executable and keeps its lint posture.
#[allow(
    clippy::nursery,
    clippy::pedantic,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    missing_docs
)]
#[cfg_attr(
    test,
    allow(
        clippy::err_expect,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used
    )
)]
pub mod land_use_silver_export;

/// 용도지역지구 도형 Silver handoff export (root ADR-0362), run by the Silver refresh like the land
/// CSV exports above.
#[allow(
    clippy::nursery,
    clippy::pedantic,
    clippy::print_stderr,
    clippy::print_stdout,
    missing_docs
)]
#[cfg_attr(test, allow(clippy::panic, clippy::unwrap_used))]
pub mod land_use_zone_area_silver_export;
