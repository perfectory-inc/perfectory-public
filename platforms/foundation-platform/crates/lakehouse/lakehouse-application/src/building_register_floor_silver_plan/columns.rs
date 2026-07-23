//! Column lists for the building-register floor Silver handoff.

use lakehouse_domain::LakehouseTableContract;

pub(super) fn column_names(contract: &LakehouseTableContract) -> Vec<String> {
    contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect()
}

pub(super) fn building_register_floor_transport_columns() -> Vec<String> {
    [
        "floor_row_id",
        "mgm_bldrgst_pk",
        "floor_type_code_raw",
        "floor_type_name_raw",
        "floor_number_raw",
        "floor_label_raw",
        "floor_kind",
        "floor_number",
        "floor_index",
        "floor_display_ko",
        "normalization_status",
        "normalization_reason",
        "source_record_id",
        "source_snapshot_id",
        "bronze_object_key",
        "source_line_number",
        "valid_from_utc",
        "valid_to_utc",
        "ingested_at_utc",
        "row_checksum_sha256",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
