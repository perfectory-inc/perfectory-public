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

/// Stages ILIS notice detail reads and their stored attachments as Silver rows (root ADR-0484).
pub mod industrial_complex_notice_stage;
/// Stages one industrial-complex profile export as `silver.industrial_complexes` rows (root ADR-0033).
pub mod industrial_complex_silver_stage;
/// Stages industrial land sale notice reads and their stored attachments as Silver rows (root ADR-0603).
pub mod industrial_land_sale_notice_stage;
/// The `DuckDB` the stages run in, for the caller that diffs and commits a stage's rows (root
/// ADR-0564 §3): one build of it in the executable.
pub use duckdb;

/// The code change table's views a `DuckDB` stage reads (root ADR-0145, ADR-0329).
pub mod legal_dong_code_change_stage;
pub mod legal_dong_code_snapshot_stage;

/// The building energy kind's Gold in `DuckDB` (root ADR-0398).
pub mod building_energy_stage;
pub mod building_permit_stage;
pub mod building_sub_parcel_stage;
/// The registered-factory kind's Gold in `DuckDB` (root ADR-0449).
pub mod factory_kind_stage;
pub mod intelligence_documents_stage;
/// What every parcel kind Gold of appended row versions shares (root ADR-0564 §6).
pub mod kind_versions_stage;
/// The parcel ownership kind's Gold in `DuckDB` (root ADR-0361).
pub mod land_ownership_stage;
pub mod legal_dong_pairing_stage;
/// The parcel nearby kind's Gold in `DuckDB` (root ADR-0465, ADR-0564 §9).
pub mod parcel_nearby_stage;
pub mod parcel_number_change_history_stage;
/// The parcel panel's Gold in `DuckDB` (root ADR-0096, ADR-0564 §9 차례 5).
pub mod parcel_panel_stage;
pub mod parcel_registry_stage;
/// The parcel place ids every kind row carries, by one rule (root ADR-0483).
pub mod place_id_join;
/// The register kinds' Gold in `DuckDB`: 총괄표제부, 철거·멸실 (root ADR-0450, ADR-0399).
pub mod record_kind_stage;

/// Stages the data.go.kr bulk files as Silver rows, one table per file (root ADR-0401 §3–4).
pub mod data_go_kr_file_data_stage;

/// Stages 차세대 온비드 public sale Bronze objects as Silver rows (root ADR-0280, ADR-0340).
pub mod onbid_silver_stage;

/// The parcel matching gate (root ADR-0113 §7) before a cadastral edition is baked.
pub mod parcel_matching_gate_stage;
/// What a Silver load that left Spark appends of a newest-per-key table: its changed keys (root
/// ADR-0311, ADR-0559).
pub mod silver_changes;

/// Stages 법원경매정보 Bronze objects as Silver rows, the roles that left Spark (root ADR-0540,
/// ADR-0575).
pub mod courtauction_silver_stage;

/// The served parcel boundaries and the fold of staff edits (root ADR-0133 §2, ADR-0375 §2).
pub mod parcel_boundary_served_stage;

/// The one place-identity resolver every parcel source calls at ingestion (root ADR-0288 §1).
pub mod place_identity_stage;

/// The parcel lineage's carry walk every placing producer shares (root ADR-0113 §6).
pub mod lineage_carry;

/// The parcel lineage as every consumer reads it: steward decisions in force (root ADR-0115 §9).
pub mod lineage_steward_read;

/// The 실거래 Gold's transforms in `DuckDB` (root ADR-0559 §3, ADR-0564 §9).
pub mod real_transaction_gold_stage;
pub mod registered_factory_stage;

/// Where each 실거래 deal belongs, in `DuckDB` (root ADR-0350 §1, ADR-0351, ADR-0405).
pub mod real_transaction_placement_stage;

pub mod release_compare_stage;
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

/// 주소정보 전자지도 base Silver handoff export (root ADR-0494 §6): one kind's layer of one 시도 zip.
#[allow(
    clippy::nursery,
    clippy::pedantic,
    clippy::print_stderr,
    clippy::print_stdout,
    missing_docs
)]
#[cfg_attr(test, allow(clippy::panic, clippy::unwrap_used))]
pub mod juso_electronic_map_base_silver_export;
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
