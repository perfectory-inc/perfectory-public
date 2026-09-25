select
    source_record_id,
    'vworld-ned-land-right-registration' as source_system,
    pnu,
    right_serial_no,
    nullif(dong_name, '') as dong_name,
    nullif(floor_name, '') as floor_name,
    nullif(ho_name, '') as ho_name,
    nullif(room_name, '') as room_name,
    nullif(closure_kind_name, '') as closure_kind_name,
    source_snapshot_id,
    source_snapshot_id as lineage_run_id
from {{ source('foundation', 'land_right_registration') }}
where nullif(ho_name, '') is not null
  and nullif(pnu, '') is not null
