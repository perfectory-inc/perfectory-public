//! `VWorld` 필지고유번호변동연혁 (MK/30527): the provider's official old PNU -> new PNU record, as it ships
//! (root ADR-0144 §4, ADR-0145).
//!
//! A source table, not a derived pair store: the 법정동 code pairing reads its dated links as the
//! official evidence, and records what it decides in `reference.legal_dong_code_change`. A row whose
//! 대장구분 is 0 and 본번·부번 0000 moves a whole 법정동 (`is_dong_level`). A row with no readable
//! 토지이동일자 keeps `changed_on` null and names why in `quarantine_reason`; it is never evidence. A row
//! the file repeats exactly is kept once, at its first line, with `duplicate_count`.

use crate::lakehouse::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

const SILVER_PARCEL_NUMBER_CHANGE_HISTORY_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "old_pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "new_pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "is_dong_level",
        logical_type: "boolean",
        required: true,
    },
    LakehouseColumn {
        name: "reason_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "changed_on",
        logical_type: "date",
        required: false,
    },
    LakehouseColumn {
        name: "changed_on_raw",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "quarantine_reason",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_sigungu_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source_line_number",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "duplicate_count",
        logical_type: "int",
        required: true,
    },
    LakehouseColumn {
        name: "source_file_name",
        logical_type: "string",
        required: true,
    },
    // The 시도 code of the province file the row came from (its member name): each 시도 file is
    // re-released whole, so this is the group whose newest edition is current (root ADR-0286).
    LakehouseColumn {
        name: "source_region_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "source_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "provider_updated_on",
        logical_type: "date",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Every row of every 30527 province file, with the Bronze object it came from.
pub const SILVER_PARCEL_NUMBER_CHANGE_HISTORY: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.parcel_number_change_history",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // 사건이다. 판이 아니다. 적재는 표가 갖지 않은 사건의 행만 붙이므로(루트 ADR-0318 §8) 한 시도의 가장 새
    // 파일이 붙인 행은 그 달에 새로 생긴 몇 건뿐이다: "시도마다 가장 새 판"으로 읽으면 그 몇 건만 남는다
    // (2026-11-15 적재 뒤 7개 시도, 짝 맞추기 창의 연결 3,702 건 중 1,669 건이 가려졌다). 모든 행이 현재다.
    row_versions: LakehouseRowVersions::Events {
        canonical_reader: Some("vworld_parcel_number_change_history.official_links"),
    },
    columns: SILVER_PARCEL_NUMBER_CHANGE_HISTORY_COLUMNS,
    partition_spec: &[],
    sort_order: &["changed_on", "old_pnu"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "old_pnu and new_pnu are 19 digits unless quarantine_reason is malformed_pnu",
        "(source_record_id, source_line_number) unique",
        "changed_on is null only where quarantine_reason is set",
    ],
    // 제공자 파일 하나(Bronze 객체 하나)가 한 번의 적재다. 제공자가 같은 파일 번호에 새 판을 올리면
    // Bronze 키가 달라 새 적재가 된다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
