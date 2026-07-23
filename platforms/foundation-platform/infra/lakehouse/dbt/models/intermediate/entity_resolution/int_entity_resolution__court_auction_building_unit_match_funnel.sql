{{ config(materialized='table', tags=['full_quality']) }}

with court_auction as (
    select * from {{ ref('stg_gongzzang__court_auction_observation') }}
),

building_unit as (
    select * from {{ ref('stg_foundation__building_register_unit') }}
),

court_pnu as (
    select distinct pnu
    from court_auction
    where nullif(pnu, '') is not null
),

building_relevant as (
    select building_unit.*
    from building_unit
    join court_pnu
      on building_unit.pnu = court_pnu.pnu
),

building_unit_pnu_dong_label_rollup as (
    select
        pnu,
        coalesce(dong_name, '') as dong_name_key,
        coalesce(unit_label, '') as unit_label_key,
        count(*) as unit_count
    from building_relevant
    group by 1, 2, 3
),

building_unit_pnu_dong_number_rollup as (
    select
        pnu,
        coalesce(dong_name, '') as dong_name_key,
        unit_number,
        count(*) as unit_count
    from building_relevant
    where unit_number is not null
    group by 1, 2, 3
),

building_unit_pnu_dong_number_unique_rollup as (
    select *
    from building_unit_pnu_dong_number_rollup
    where unit_count = 1
),

building_unit_pnu_dong_rollup as (
    select
        pnu,
        dong_name_key,
        sum(unit_count) as unit_count
    from building_unit_pnu_dong_label_rollup
    group by 1, 2
),

building_unit_pnu_rollup as (
    select
        pnu,
        sum(unit_count) as unit_count
    from building_unit_pnu_dong_label_rollup
    group by 1
),

candidates as (
    select * from {{ ref('int_entity_resolution__court_auction_building_unit_candidates') }}
),

candidate_counts as (
    select
        count(distinct source_observation_id) as source_observation_count,
        count(*) as candidate_pair_count,
        count(
            distinct case
                when match_path = 'PNU_DONG_UNIT_LABEL_AREA' then source_observation_id
            end
        ) as label_area_source_observation_count,
        count(
            case
                when match_path = 'PNU_DONG_UNIT_LABEL_AREA' then 1
            end
        ) as label_area_candidate_pair_count,
        count(
            distinct case
                when match_path = 'PNU_DONG_UNIT_NUMBER_AREA' then source_observation_id
            end
        ) as number_area_source_observation_count,
        count(
            case
                when match_path = 'PNU_DONG_UNIT_NUMBER_AREA' then 1
            end
        ) as number_area_candidate_pair_count,
        count(
            distinct case
                when match_path = 'PNU_DONG_UNIT_NUMBER_UNIQUE' then source_observation_id
            end
        ) as number_unique_source_observation_count,
        count(
            case
                when match_path = 'PNU_DONG_UNIT_NUMBER_UNIQUE' then 1
            end
        ) as number_unique_candidate_pair_count
    from candidates
),

source_total as (
    select
        1 as stage_order,
        'source_total' as match_stage,
        count(distinct source_observation_id) as source_observation_count,
        count(*) as candidate_pair_count
    from court_auction
),

source_with_pnu as (
    select
        2 as stage_order,
        'source_with_pnu' as match_stage,
        count(distinct source_observation_id) as source_observation_count,
        count(*) as candidate_pair_count
    from court_auction
    where nullif(pnu, '') is not null
),

pnu_match as (
    select
        3 as stage_order,
        'pnu_match' as match_stage,
        count(
            distinct case
                when building_unit_pnu_rollup.unit_count > 0
                    then court_auction.source_observation_id
            end
        ) as source_observation_count,
        sum(coalesce(building_unit_pnu_rollup.unit_count, 0)) as candidate_pair_count
    from court_auction
    left join building_unit_pnu_rollup
      on court_auction.pnu = building_unit_pnu_rollup.pnu
),

pnu_dong_match as (
    select
        4 as stage_order,
        'pnu_dong_match' as match_stage,
        count(
            distinct case
                when building_unit_pnu_dong_rollup.unit_count > 0
                    then court_auction.source_observation_id
            end
        ) as source_observation_count,
        sum(coalesce(building_unit_pnu_dong_rollup.unit_count, 0)) as candidate_pair_count
    from court_auction
    left join building_unit_pnu_dong_rollup
      on court_auction.pnu = building_unit_pnu_dong_rollup.pnu
     and coalesce(court_auction.dong_name, '') = building_unit_pnu_dong_rollup.dong_name_key
),

pnu_dong_unit_label_match as (
    select
        5 as stage_order,
        'pnu_dong_unit_label_match' as match_stage,
        count(
            distinct case
                when building_unit_pnu_dong_label_rollup.unit_count > 0
                    then court_auction.source_observation_id
            end
        ) as source_observation_count,
        sum(coalesce(building_unit_pnu_dong_label_rollup.unit_count, 0)) as candidate_pair_count
    from court_auction
    left join building_unit_pnu_dong_label_rollup
      on court_auction.pnu = building_unit_pnu_dong_label_rollup.pnu
     and coalesce(court_auction.dong_name, '') = building_unit_pnu_dong_label_rollup.dong_name_key
     and coalesce(court_auction.unit_label, '') = building_unit_pnu_dong_label_rollup.unit_label_key
),

pnu_dong_unit_number_match as (
    select
        6 as stage_order,
        'pnu_dong_unit_number_match' as match_stage,
        count(
            distinct case
                when building_unit_pnu_dong_number_rollup.unit_count > 0
                    then court_auction.source_observation_id
            end
        ) as source_observation_count,
        sum(coalesce(building_unit_pnu_dong_number_rollup.unit_count, 0)) as candidate_pair_count
    from court_auction
    left join building_unit_pnu_dong_number_rollup
      on court_auction.pnu = building_unit_pnu_dong_number_rollup.pnu
     and coalesce(court_auction.dong_name, '') = building_unit_pnu_dong_number_rollup.dong_name_key
     and court_auction.unit_number = building_unit_pnu_dong_number_rollup.unit_number
),

pnu_dong_unit_number_unique as (
    select
        7 as stage_order,
        'pnu_dong_unit_number_unique' as match_stage,
        count(
            distinct case
                when building_unit_pnu_dong_number_unique_rollup.unit_count > 0
                    then court_auction.source_observation_id
            end
        ) as source_observation_count,
        sum(coalesce(building_unit_pnu_dong_number_unique_rollup.unit_count, 0)) as candidate_pair_count
    from court_auction
    left join building_unit_pnu_dong_number_unique_rollup
      on court_auction.pnu = building_unit_pnu_dong_number_unique_rollup.pnu
     and coalesce(court_auction.dong_name, '') = building_unit_pnu_dong_number_unique_rollup.dong_name_key
     and court_auction.unit_number = building_unit_pnu_dong_number_unique_rollup.unit_number
),

pnu_dong_unit_label_area_match as (
    select
        8 as stage_order,
        'pnu_dong_unit_label_area_match' as match_stage,
        label_area_source_observation_count as source_observation_count,
        label_area_candidate_pair_count as candidate_pair_count
    from candidate_counts
),

pnu_dong_unit_number_area_match as (
    select
        9 as stage_order,
        'pnu_dong_unit_number_area_match' as match_stage,
        number_area_source_observation_count as source_observation_count,
        number_area_candidate_pair_count as candidate_pair_count
    from candidate_counts
),

candidate_output as (
    select
        10 as stage_order,
        'candidate_output' as match_stage,
        source_observation_count,
        candidate_pair_count
    from candidate_counts
)

select * from source_total
union all
select * from source_with_pnu
union all
select * from pnu_match
union all
select * from pnu_dong_match
union all
select * from pnu_dong_unit_label_match
union all
select * from pnu_dong_unit_number_match
union all
select * from pnu_dong_unit_number_unique
union all
select * from pnu_dong_unit_label_area_match
union all
select * from pnu_dong_unit_number_area_match
union all
select * from candidate_output
