select
    unit_row_id as building_unit_id,
    source_snapshot_id as target_snapshot_id,
    pnu,
    register_parcel_key,
    building_mgm_bldrgst_pk,
    nullif(dong_join_name, '') as dong_name,
    nullif(unit_name_raw, '') as unit_name_raw,
    nullif(unit_label_ko, '') as unit_label_ko,
    nullif(unit_designation, '') as unit_designation,
    coalesce(
        nullif(unit_label_ko, ''),
        nullif(unit_designation, ''),
        nullif(unit_name_raw, '')
    ) as unit_label,
    unit_number,
    floor_number,
    floor_index,
    case
        when floor_kind = 'basement' and floor_number is not null
            then concat('B', cast(floor_number as varchar))
        when floor_kind = 'above_ground' and floor_number is not null
            then cast(floor_number as varchar)
        when floor_kind = 'all_floors'
            then 'all_floors'
        else nullif(floor_kind, '')
    end as floor_label,
    source_snapshot_id as lineage_run_id,
    mgm_bldrgst_pk
from {{ source('foundation', 'building_register_unit') }}
