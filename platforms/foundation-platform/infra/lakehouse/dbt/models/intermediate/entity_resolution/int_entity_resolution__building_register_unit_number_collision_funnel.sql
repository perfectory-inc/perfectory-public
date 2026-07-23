{{ config(materialized='table', tags=['full_quality']) }}

with building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

scope_number_rollup as (
    select
        coalesce(register_parcel_key, pnu) as parcel_scope_key,
        coalesce(building_mgm_bldrgst_pk, '') as building_scope_key,
        coalesce(dong_name, '') as dong_name_key,
        coalesce(cast(floor_index as varchar), 'unknown') as floor_index_key,
        unit_number,
        count(*) as unit_row_count
    from building_unit
    where unit_number is not null
    group by 1, 2, 3, 4, 5
),

collision_candidates as (
    select * from {{ ref('int_entity_resolution__building_register_unit_number_collision_candidates') }}
),

source_total as (
    select
        1 as stage_order,
        'source_total' as diagnostic_stage,
        cast(null as bigint) as affected_group_count,
        count(*) as affected_row_count
    from building_unit
),

unit_number_present as (
    select
        2 as stage_order,
        'unit_number_present' as diagnostic_stage,
        cast(null as bigint) as affected_group_count,
        count(*) as affected_row_count
    from building_unit
    where unit_number is not null
),

unit_number_collision_groups as (
    select
        3 as stage_order,
        'unit_number_collision_groups' as diagnostic_stage,
        count(*) as affected_group_count,
        coalesce(sum(unit_row_count), 0) as affected_row_count
    from scope_number_rollup
    where unit_row_count > 1
),

designation_collision_groups as (
    select
        4 as stage_order,
        'designation_collision_groups' as diagnostic_stage,
        count(*) as affected_group_count,
        coalesce(sum(unit_row_count), 0) as affected_row_count
    from collision_candidates
),

hyphenated_designation_collision_groups as (
    select
        5 as stage_order,
        'hyphenated_designation_collision_groups' as diagnostic_stage,
        count(*) as affected_group_count,
        coalesce(sum(unit_row_count), 0) as affected_row_count
    from collision_candidates
    where hyphenated_designation_count > 0
),

parenthesized_suffix_collision_groups as (
    select
        6 as stage_order,
        'parenthesized_suffix_collision_groups' as diagnostic_stage,
        count(*) as affected_group_count,
        coalesce(sum(unit_row_count), 0) as affected_row_count
    from collision_candidates
    where parenthesized_suffix_count > 0
)

select * from source_total
union all
select * from unit_number_present
union all
select * from unit_number_collision_groups
union all
select * from designation_collision_groups
union all
select * from hyphenated_designation_collision_groups
union all
select * from parenthesized_suffix_collision_groups
