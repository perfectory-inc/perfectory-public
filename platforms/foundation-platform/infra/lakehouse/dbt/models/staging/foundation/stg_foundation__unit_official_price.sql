select
    source_record_id,
    'hubgokr-unit-official-price' as source_system,
    pnu,
    mgm_bldrgst_pk,
    nullif(dong_name, '') as dong_name,
    nullif(ho_name, '') as ho_name,
    base_date,
    source_snapshot_id,
    source_snapshot_id as lineage_run_id
from {{ source('foundation', 'unit_official_price') }}
where nullif(ho_name, '') is not null
  and nullif(pnu, '') is not null
