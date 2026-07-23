select
    concat(
        'court_auction:',
        court_office_code,
        ':',
        case_no,
        ':',
        cast(gds_seq as varchar),
        ':',
        cast(objct_seq as varchar)
    ) as source_observation_id,
    'court_auction' as source_system,
    concat(
        court_office_code,
        ':',
        case_no,
        ':',
        cast(gds_seq as varchar),
        ':',
        cast(objct_seq as varchar)
    ) as source_record_id,
    cast({{ required_env_var_sql_literal('FOUNDATION_DBT_COURT_AUCTION_SOURCE_SNAPSHOT_ID') }} as varchar) as source_snapshot_id,
    pnu,
    dong_name,
    unit_designation as unit_label,
    {{ foundation_unit_number_from_designation('unit_designation') }} as unit_number,
    {{ foundation_floor_designation_hint_from_designation('unit_designation') }} as floor_from_designation,
    exclusive_area as exclusive_area_sqm,
    ltno_addr as lot_address_raw,
    road_addr as road_address_raw,
    print_addr_raw,
    x_crd as x_coordinate,
    y_crd as y_coordinate,
    'pii_excluded' as privacy_classification,
    cast({{ required_env_var_sql_literal('FOUNDATION_DBT_COURT_AUCTION_LINEAGE_RUN_ID') }} as varchar) as lineage_run_id
from {{ source('gongzzang', 'court_auction_property') }}
