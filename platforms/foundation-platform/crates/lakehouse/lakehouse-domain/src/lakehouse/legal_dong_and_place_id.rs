//! The legal-dong code reference tables, parcel lineage and registry, and the place-id tables.
//! Moved out of `lakehouse.rs` unchanged.

use super::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};

pub(super) const REFERENCE_LEGAL_DONG_CODE_SNAPSHOT_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "region_cd",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "full_name",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "snapshot_date",
        logical_type: "date",
        required: true,
    },
    LakehouseColumn {
        name: "source_record_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "ingested_at_utc",
        logical_type: "timestamp",
        required: true,
    },
    // code.go.kr 전체 표가 더 주는 칸 (root ADR-0143 §2). 옛 전체자료 파일 적재분에는 없으므로 비어 있다.
    LakehouseColumn {
        name: "parent_cd",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "created_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "abolished_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "lowest_name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "jumin_cd",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "jijuk_cd",
        logical_type: "string",
        required: false,
    },
];

/// Every snapshot of the official 법정동 code list.
///
/// The 전체자료 file (root ADR-0113 §5) carries code, full name and 존재/폐지 only; the code.go.kr full table (root ADR-0143 §2) adds the parent code,
/// 생성일, 폐지일, the lowest name and the 주민/지적 codes. Dates are `YYYYMMDD` text, blank when the
/// source leaves them blank.
pub const REFERENCE_LEGAL_DONG_CODE_SNAPSHOT: LakehouseTableContract = LakehouseTableContract {
    table_name: "reference.legal_dong_code_snapshot",
    layer: LakehouseLayer::Reference,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: REFERENCE_LEGAL_DONG_CODE_SNAPSHOT_COLUMNS,
    partition_spec: &[],
    sort_order: &["snapshot_date", "region_cd"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(snapshot_date, region_cd) unique",
        "region_cd is 10 digits",
        "status is 존재 or 폐지",
    ],
    // 스냅숏 파일 하나가 한 번의 적재다. 같은 파일을 두 번 넣지 않는다.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

pub(super) const REFERENCE_LEGAL_DONG_CODE_CHANGE_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "change_key",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "old_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "new_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "level",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "effective_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "source",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "rule_verdict",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "detail",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "derivation_run_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "recorded_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Every 법정동 code change the data implies, old code → new code.
///
/// Root ADR-0143 §3–4, ADR-0144: from downloaded data only. `source` names the evidence:
/// `derived:code-go-kr:date+name:<day>` (the date + name rule over the full table),
/// `derived:parcel-jibun:<snapshots>` (the 지번 sets of two parcel snapshots, ADR-0113 §5),
/// `official:parcel-history` (`silver.parcel_number_change_history`, the 필지고유번호변동연혁 links), `derived:children` (a 시군구 or
/// 시도 rolled up from its 동) or `steward:<who>`. Append-only: a change is recorded once, keyed by
/// `change_key`; a load is one derivation run or one steward decision.
pub const REFERENCE_LEGAL_DONG_CODE_CHANGE: LakehouseTableContract = LakehouseTableContract {
    table_name: "reference.legal_dong_code_change",
    layer: LakehouseLayer::Reference,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: REFERENCE_LEGAL_DONG_CODE_CHANGE_COLUMNS,
    partition_spec: &[],
    sort_order: &["effective_date", "old_code"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "change_key unique",
        "kind is pair",
        "a pair names old_code and new_code",
    ],
    // 도출 실행 한 번(표 키 + 실행 시각), 또는 스튜어드 결정 파일 하나가 한 번의 적재다. 이미 있는
    // change_key 는 적재 전에 걸러 내므로 같은 변경을 다시 도출해도 다시 넣지 않는다.
    load: LakehouseLoadUnit::Object {
        column: "derivation_run_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

pub(super) const SILVER_PARCEL_LINEAGE_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "predecessor_pnu",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "successor_pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "relation",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "cardinality",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "effective_date",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "grade",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "evidence_kind",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "evidence_ref",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "from_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "to_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "rules_version",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "derivation_run_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "derived_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Old parcel -> new parcel, with relation, grade and evidence, for one pair of cadastral snapshots
/// (root ADR-0113 §1·§4). Append-only; the effective link is the highest grade per parcel.
pub const SILVER_PARCEL_LINEAGE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.parcel_lineage",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_PARCEL_LINEAGE_COLUMNS,
    partition_spec: &[],
    sort_order: &["to_snapshot_id", "successor_pnu"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "grade is official, code_derived, evidence_strong, evidence_weak, needs_review or pending",
        "relation is code_change, jurisdiction_transfer, registration_conversion, merge, split, resurvey or other",
        "predecessor_pnu is empty only for pending",
        "polygon overlap is never evidence",
    ],
    // 스냅숏 쌍·규칙 판마다 도출 한 번이 한 번의 적재다. 같은 도출을 두 번 쌓지 않는다.
    load: LakehouseLoadUnit::Run {
        column: "derivation_run_id",
    },
};

pub(super) const SILVER_PARCEL_REGISTRY_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "parcel_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "pnu",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "valid_from",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "valid_to",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "redirect_to",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "to_snapshot_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "id_rules_version",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "derivation_run_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "recorded_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Bitemporal parcel registry: which stable `parcel_id` a PNU carried, from when to when (root ADR-0113
/// §2·§3). Rows only open, close or redirect; folding them in date order gives the registry now.
pub const SILVER_PARCEL_REGISTRY: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.parcel_registry",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: None,
    columns: SILVER_PARCEL_REGISTRY_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "valid_from"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "status is current, historic or redirected",
        "redirect_to is set only for redirected",
        "a parcel_id is never issued for a second parcel",
        "every parcel of a promoted snapshot has exactly one current parcel_id",
    ],
    // 스냅숏 전이(또는 등급 상승 재실행) 한 번이 한 번의 적재다.
    load: LakehouseLoadUnit::Run {
        column: "derivation_run_id",
    },
};

pub(super) const GOLD_PLACE_ID_REGISTRY_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "unit",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "scope",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "place_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "current_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "redirect_to",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "release_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Every id a unit ever served and its status now (root ADR-0113 §8, the GERS registry).
pub const GOLD_PLACE_ID_REGISTRY: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.place_id_registry",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_PLACE_ID_REGISTRY_COLUMNS,
    partition_spec: &["unit", "scope"],
    sort_order: &["place_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "one row per (unit, scope, place_id)",
        "status is current, historic or redirected",
        "redirect_to is set only for redirected",
    ],
    // silver.parcel_registry · silver.administrative_boundaries 에서 발행마다 (unit, scope) 단위로 다시 쓴다.
    load: LakehouseLoadUnit::Derived,
};

pub(super) const GOLD_PLACE_ID_BRIDGE_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "unit",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "scope",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "place_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "valid_from",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "valid_to",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "release_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// Id <-> source code (PNU or legal-dong code) with the period the pair held (root ADR-0113 §8).
pub const GOLD_PLACE_ID_BRIDGE: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.place_id_bridge",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_PLACE_ID_BRIDGE_COLUMNS,
    partition_spec: &["unit", "scope"],
    sort_order: &["place_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "a code maps to one place_id at any time",
        "valid_to is empty only for the open period",
    ],
    // silver.parcel_registry · silver.administrative_boundaries 에서 발행마다 (unit, scope) 단위로 다시 쓴다.
    load: LakehouseLoadUnit::Derived,
};

pub(super) const GOLD_PLACE_ID_CHANGELOG_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "unit",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "scope",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "place_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "change",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "from_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "to_code",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "redirect_to",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "previous_release_id",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "release_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// What each release changed: added, retired, renumbered, redirected ids (root ADR-0113 §8).
pub const GOLD_PLACE_ID_CHANGELOG: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.place_id_changelog",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_PLACE_ID_CHANGELOG_COLUMNS,
    partition_spec: &["unit", "scope"],
    sort_order: &["place_id"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "change is added, retired, renumbered or redirected",
    ],
    // 발행 한 번이 한 번의 적재다. 같은 발행을 두 번 쌓지 않는다.
    load: LakehouseLoadUnit::Run {
        column: "release_id",
    },
};

pub(super) const GOLD_LINEAGE_REVIEW_QUEUE_COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "unit",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "subject_code",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "item_id",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "status",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "candidates_json",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "evidence_etag",
        logical_type: "string",
        required: true,
    },
    LakehouseColumn {
        name: "from_snapshot_id",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "to_snapshot_id",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "published_at_utc",
        logical_type: "timestamp",
        required: true,
    },
];

/// The parcels a person has to decide: best lineage grade `needs_review` or `pending`, no steward
/// row yet (root ADR-0113 §10).
pub const GOLD_LINEAGE_REVIEW_QUEUE: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.lineage_review_queue",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
    columns: GOLD_LINEAGE_REVIEW_QUEUE_COLUMNS,
    partition_spec: &["unit"],
    sort_order: &["subject_code"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "one row per (unit, subject_code)",
        "status is needs_review or pending",
        "item_id is the UUIDv5 of lineage-review:<unit>:<subject_code>",
        "evidence_etag is the sha256 of status, a newline, and candidates_json (ADR-0115)",
    ],
    // silver.parcel_lineage 에서 매번 다시 쓴다. 결정은 이 표가 아니라 계보 행으로 쌓인다.
    load: LakehouseLoadUnit::Derived,
};
