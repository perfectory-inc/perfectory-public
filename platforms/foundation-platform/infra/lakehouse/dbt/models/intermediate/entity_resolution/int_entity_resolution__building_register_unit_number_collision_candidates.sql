{{ config(materialized='table', tags=['full_quality']) }}

with building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

scoped_units as (
    select
        building_unit_id,
        coalesce(register_parcel_key, pnu) as parcel_scope_key,
        coalesce(building_mgm_bldrgst_pk, '') as building_scope_key,
        coalesce(dong_name, '') as dong_name_key,
        coalesce(cast(floor_index as varchar), 'unknown') as floor_index_key,
        unit_number,
        unit_designation,
        unit_label
    from building_unit
    where unit_number is not null
),

scope_rollup as (
    select
        parcel_scope_key,
        building_scope_key,
        dong_name_key,
        floor_index_key,
        unit_number,
        count(*) as unit_row_count,
        count(distinct building_unit_id) as distinct_unit_count,
        count(distinct coalesce(unit_designation, '<null>')) as distinct_designation_count,
        count(
            case
                when regexp_like(coalesce(unit_designation, ''), '[0-9]+\s*-\s*[0-9]+')
                    then 1
            end
        ) as hyphenated_designation_count,
        count(
            case
                when regexp_like(coalesce(unit_designation, ''), '^[0-9]+\s*\([^)]*\)\s*$')
                    then 1
            end
        ) as parenthesized_suffix_count,
        array_join(
            slice(
                array_sort(array_distinct(array_agg(coalesce(unit_designation, '<null>')))),
                1,
                10
            ),
            ' | '
        ) as sample_unit_designations,
        array_join(
            slice(
                array_sort(array_distinct(array_agg(coalesce(unit_label, '<null>')))),
                1,
                10
            ),
            ' | '
        ) as sample_unit_labels
    from scoped_units
    group by 1, 2, 3, 4, 5
    having count(*) > 1
       and count(distinct coalesce(unit_designation, '<null>')) > 1
),

classified as (
    select
        concat(
            parcel_scope_key,
            '|',
            building_scope_key,
            '|',
            dong_name_key,
            '|',
            floor_index_key,
            '|',
            cast(unit_number as varchar)
        ) as collision_scope_id,
        parcel_scope_key,
        building_scope_key,
        dong_name_key,
        floor_index_key,
        unit_number,
        unit_row_count,
        distinct_unit_count,
        distinct_designation_count,
        hyphenated_designation_count,
        parenthesized_suffix_count,
        sample_unit_designations,
        sample_unit_labels,
        case
            when parenthesized_suffix_count > 0 then 'parenthesized_suffix'
            when hyphenated_designation_count > 0 then 'hyphenated_designation'
            else 'mixed_designation'
        end as collision_family
    from scope_rollup
)

select * from classified
