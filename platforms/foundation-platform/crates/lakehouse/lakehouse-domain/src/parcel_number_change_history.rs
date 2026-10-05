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
    LakehouseServingRole, LakehouseTableContract,
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
    columns: SILVER_PARCEL_NUMBER_CHANGE_HISTORY_COLUMNS,
    partition_spec: &[],
    sort_order: &["changed_on", "old_pnu"],
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
